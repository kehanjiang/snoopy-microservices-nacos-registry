package com.snoopy.registry.nacos;

import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.listener.Event;
import com.alibaba.nacos.api.naming.listener.EventListener;
import com.alibaba.nacos.api.naming.listener.NamingEvent;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.snoopy.grpc.base.configure.GrpcRegistryProperties;
import com.snoopy.grpc.base.registry.IRegistry;
import com.snoopy.grpc.base.registry.ISubscribeCallback;
import com.snoopy.grpc.base.registry.RegistryServiceInfo;
import org.apache.commons.lang3.math.NumberUtils;

import java.io.IOException;
import java.rmi.RemoteException;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * @author :   kehanjiang
 * @date :   2021/12/1  15:18
 */
public class NacosRegistry implements IRegistry {
    private static final long DEFAULT_TIMEOUT = 5_000;
    private long timeout;
    private NamingService namingService;
    private Map<String, EventListener> listenerMap = new HashMap<>();
    private final ReentrantLock reentrantLock = new ReentrantLock();

    public NacosRegistry(GrpcRegistryProperties grpcRegistryProperties, Properties properties) {
        timeout = NumberUtils.toLong(grpcRegistryProperties.getExtra(NacosRegistryProvider.PARAM_TIMEOUT));
        timeout = timeout < 1 ? DEFAULT_TIMEOUT : timeout;
        try {
            namingService = NacosFactory.createNamingService(properties);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }


    @Override
    public void subscribe(RegistryServiceInfo serviceInfo, ISubscribeCallback subscribeCallback) {
        reentrantLock.lock();
        try {
            EventListener eventListener = listenerMap.get(serviceInfo.getPath());
            if (eventListener == null) {
                eventListener = new EventListener() {
                    @Override
                    public void onEvent(Event event) {
                        if (event instanceof NamingEvent) {
                            NamingEvent namingEvent = (NamingEvent) event;
                            List<Instance> instanceList = namingEvent.getInstances();
                            List<RegistryServiceInfo> serviceInfoList = instanceList != null ? instanceList.stream().map(instance -> {
                                return new RegistryServiceInfo(
                                        serviceInfo.getNamespace(),
                                        instance.getServiceName(),
                                        NacosRegistryProvider.REGISTRY_PROTOCOL_NACOS,
                                        instance.getIp(),
                                        instance.getPort(),
                                        instance.getMetadata());
                            }).collect(Collectors.toList()) : Collections.EMPTY_LIST;
                            subscribeCallback.handle(serviceInfoList);
                        }
                    }
                };
                listenerMap.put(serviceInfo.getPath(), eventListener);
            }
            namingService.subscribe(serviceInfo.getAlias(), serviceInfo.getNamespace(), eventListener);
        } catch (NacosException e) {
            throw new RuntimeException("[" + serviceInfo.getPath() + "] subscribe failed !", e);
        } finally {
            reentrantLock.unlock();
        }
    }

    @Override
    public void unsubscribe(RegistryServiceInfo serviceInfo) {
        reentrantLock.lock();
        try {
            EventListener eventListener = listenerMap.get(serviceInfo.getPath());
            if (eventListener != null) {
                namingService.unsubscribe(serviceInfo.getAlias(), serviceInfo.getNamespace(), eventListener);
            }
        } catch (NacosException e) {
            throw new RuntimeException("[" + serviceInfo.getPath() + "] unsubscribe failed !", e);
        } finally {
            reentrantLock.unlock();
        }
    }

    @Override
    public void register(RegistryServiceInfo serviceInfo) {
        reentrantLock.lock();
        try {
            //先注销
            unregister(serviceInfo);
            //再注册
            Instance instance = generateInstance(serviceInfo);
            namingService.registerInstance(serviceInfo.getAlias(), serviceInfo.getNamespace(), instance);
        } catch (NacosException e) {
            throw new RuntimeException("[" + serviceInfo.getPath() + "] register failed !", e);
        } finally {
            reentrantLock.unlock();
        }
    }


    @Override
    public void unregister(RegistryServiceInfo serviceInfo) {
        reentrantLock.lock();
        try {
            Instance instance = generateInstance(serviceInfo);
            namingService.deregisterInstance(serviceInfo.getAlias(), serviceInfo.getNamespace(), instance);
        } catch (NacosException e) {
            throw new RuntimeException("[" + serviceInfo.getPath() + "] unregister failed !", e);
        } finally {
            reentrantLock.unlock();
        }
    }

    private Instance generateInstance(RegistryServiceInfo serviceInfo) {
        Instance instance = new Instance();
        instance.setServiceName(serviceInfo.getAlias());
        instance.setIp(serviceInfo.getHost());
        instance.setPort(serviceInfo.getPort());
        instance.setMetadata(serviceInfo.getParameters());
        instance.setHealthy(true);
        return instance;
    }

    @Override
    public void close() throws IOException {
        listenerMap.clear();
        if (namingService != null) {
            try {
                namingService.shutDown();
            } catch (NacosException e) {
                throw new RemoteException(e.getErrMsg());
            }
        }
    }
}

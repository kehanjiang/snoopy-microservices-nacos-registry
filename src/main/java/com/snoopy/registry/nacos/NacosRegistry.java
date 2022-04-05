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
import com.snoopy.grpc.base.utils.LoggerBaseUtil;
import org.apache.commons.lang3.math.NumberUtils;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
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
    private EventListener eventListener;
    private final ReentrantLock reentrantLock = new ReentrantLock();

    public NacosRegistry(GrpcRegistryProperties grpcRegistryProperties, Properties properties) {
        timeout = NumberUtils.toLong(grpcRegistryProperties.getExtra(NacosRegistryProvider.PARAM_TIMEOUT));
        timeout = timeout < 1 ? DEFAULT_TIMEOUT : timeout;
        try {
            namingService = NacosFactory.createNamingService(properties);
        } catch (Exception e) {
            LoggerBaseUtil.error(this, e.getMessage(), e);
        }
    }


    @Override
    public void subscribe(RegistryServiceInfo serviceInfo, ISubscribeCallback subscribeCallback) {
        reentrantLock.lock();
        try {
            this.eventListener = new EventListener() {
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
            namingService.subscribe(serviceInfo.getAlias(), serviceInfo.getNamespace(), this.eventListener);
        } catch (NacosException e) {
            LoggerBaseUtil.error(this, "[" + serviceInfo.getPath() + "] subscribe failed !", e);
        } finally {
            reentrantLock.unlock();
        }
    }

    @Override
    public void unsubscribe(RegistryServiceInfo serviceInfo) {
        reentrantLock.lock();
        try {
            namingService.unsubscribe(serviceInfo.getAlias(), serviceInfo.getNamespace(), this.eventListener);
        } catch (NacosException e) {
            LoggerBaseUtil.error(this, "[" + serviceInfo.getPath() + "] unsubscribe failed !", e);
        } finally {
            reentrantLock.unlock();
        }
    }

    @Override
    public void register(RegistryServiceInfo serviceInfo) {
        reentrantLock.lock();
        try {
            Instance instance = new Instance();
            instance.setServiceName(serviceInfo.getAlias());
            instance.setIp(serviceInfo.getHost());
            instance.setPort(serviceInfo.getPort());
            instance.setMetadata(serviceInfo.getParameters());
            instance.setHealthy(true);
            namingService.registerInstance(serviceInfo.getPath(), serviceInfo.getNamespace(), instance);
        } catch (NacosException e) {
            LoggerBaseUtil.error(this, "[" + serviceInfo.getPath() + "] register failed !", e);
        } finally {
            reentrantLock.unlock();
        }
    }


    @Override
    public void unregister(RegistryServiceInfo serviceInfo) {
        reentrantLock.lock();
        try {
            namingService.deregisterInstance(serviceInfo.getAlias(), serviceInfo.getNamespace(), serviceInfo.getHost(), serviceInfo.getPort());
        } catch (NacosException e) {
            LoggerBaseUtil.error(this, "[" + serviceInfo.getPath() + "] unregister failed !", e);
        } finally {
            reentrantLock.unlock();
        }
    }

}

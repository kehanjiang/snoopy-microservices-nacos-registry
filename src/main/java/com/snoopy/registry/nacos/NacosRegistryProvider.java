package com.snoopy.registry.nacos;

import com.snoopy.grpc.base.configure.GrpcRegistryProperties;
import com.snoopy.grpc.base.constans.GrpcConstants;
import com.snoopy.grpc.base.registry.IRegistry;
import com.snoopy.grpc.base.registry.IRegistryProvider;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author :   kehanjiang
 * @date :   2021/12/1  15:44
 */
public class NacosRegistryProvider implements IRegistryProvider {
    public static final String REGISTRY_PROTOCOL_NACOS = "nacos";
    public static final String PARAM_TIMEOUT = "timeout";
    private static final Map<String, String> params = new HashMap<>();

    static {
        //---------------------------------
        //通用参数
        //命名空间的ID	命名空间的ID	config模块为空，naming模块为public	>= 0.8.0
        params.put("namespace", "namespace");
        //客户端日志的目录	目录路径	用户根目录	>= 0.1.0
        params.put("loggingPath", "nacos.logging.path");
        //---------------------------------

        //-------------------------------------
        //Naming客户端
        //启动时是否优先读取本地缓存	true/false	false	>= 1.0.0
        params.put("namingLoadCacheAtStart", "namingLoadCacheAtStart");
        //客户端心跳的线程池大小	正整数	机器的CPU数的一半	>= 1.0.0
        params.put("namingClientBeatThreadCount", "namingClientBeatThreadCount");
        //客户端定时轮询数据更新的线程池大小	正整数	机器的CPU数的一半	>= 1.0.0
        params.put("namingPollingThreadCount", "namingPollingThreadCount");
        // (-D)	客户端缓存目录	目录路径	{user.home}/nacos/naming 	>= 1.0.0
        params.put("namingCacheDir", "com.alibaba.naming.cache.dir");
        //(-D)	Naming客户端的日志级别	info,error,warn等	info	>= 1.0.0
        params.put("namingLogLevel", "com.alibaba.nacos.naming.log.level");
        //(-D)	是否打开HTTPS	true/false	false	>= 1.0.0
        params.put("namingTlsEnable", "com.alibaba.nacos.client.naming.tls.enable");
        //--------------------------
        params.put(PARAM_TIMEOUT, PARAM_TIMEOUT);
    }

    @Override
    public IRegistry newRegistryInstance(GrpcRegistryProperties grpcRegistryProperties) {
        Properties properties = new Properties();
        for (Map.Entry<String, String> entry : params.entrySet()) {
            String pval = grpcRegistryProperties.getExtra(entry.getKey());
            if (!StringUtils.isEmpty(pval)) {
                properties.setProperty(entry.getValue(), pval);
            }
        }
        String serverAddr = String.join(",", GrpcConstants.ADDRESS_SPLIT_PATTERN.split(grpcRegistryProperties.getAddress()));
        properties.setProperty("serverAddr", serverAddr);
        if (grpcRegistryProperties.getUsername() != null && grpcRegistryProperties.getPassword() != null) {
            properties.setProperty("username", grpcRegistryProperties.getUsername());
            properties.setProperty("password", grpcRegistryProperties.getPassword());
        }
        return new NacosRegistry(grpcRegistryProperties, properties);
    }

    @Override
    public String registryType() {
        return REGISTRY_PROTOCOL_NACOS;
    }
}

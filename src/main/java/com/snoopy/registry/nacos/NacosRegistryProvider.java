package com.snoopy.registry.nacos;

import com.snoopy.grpc.base.configure.GrpcRegistryProperties;
import com.snoopy.grpc.base.registry.IRegistry;
import com.snoopy.grpc.base.registry.IRegistryProvider;

/**
 * @author :   kehanjiang
 * @date :   2021/12/1  15:44
 */
public class NacosRegistryProvider implements IRegistryProvider {
    public static final String REGISTRY_PROTOCOL_NACOS = "nacos";
    private GrpcRegistryProperties grpcRegistryProperties;

    public NacosRegistryProvider(GrpcRegistryProperties grpcRegistryProperties) {
        this.grpcRegistryProperties = grpcRegistryProperties;
    }

    @Override
    public IRegistry newRegistryInstance() {
        return new NacosRegistry(grpcRegistryProperties);
    }

    @Override
    public String registryType() {
        return REGISTRY_PROTOCOL_NACOS;
    }
}

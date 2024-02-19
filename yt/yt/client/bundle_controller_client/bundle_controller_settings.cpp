#include "bundle_controller_settings.h"

namespace NYT::NBundleControllerClient {

////////////////////////////////////////////////////////////////////////////////

void TCpuLimits::Register(TRegistrar registrar)
{
    registrar.Parameter("write_thread_pool_size", &TThis::WriteThreadPoolSize)
        .Optional();
    registrar.Parameter("lookup_thread_pool_size", &TThis::LookupThreadPoolSize)
        .Optional();
    registrar.Parameter("query_thread_pool_size", &TThis::QueryThreadPoolSize)
        .Optional();
}

void TMemoryLimits::Register(TRegistrar registrar)
{
    registrar.Parameter("tablet_static", &TThis::TabletStatic)
        .Optional();
    registrar.Parameter("tablet_dynamic", &TThis::TabletDynamic)
        .Optional();
    registrar.Parameter("compressed_block_cache", &TThis::CompressedBlockCache)
        .Optional();
    registrar.Parameter("uncompressed_block_cache", &TThis::UncompressedBlockCache)
        .Optional();
    registrar.Parameter("key_filter_block_cache", &TThis::KeyFilterBlockCache)
        .Optional();
    registrar.Parameter("versioned_chunk_meta", &TThis::VersionedChunkMeta)
        .Optional();
    registrar.Parameter("lookup_row_cache", &TThis::LookupRowCache)
        .Optional();
    registrar.Parameter("reserved", &TThis::Reserved)
        .Optional();
}

void TInstanceResources::Register(TRegistrar registrar)
{
    registrar.Parameter("vcpu", &TThis::Vcpu)
        .GreaterThanOrEqual(0)
        .Default(18000);
    registrar.Parameter("memory", &TThis::Memory)
        .GreaterThanOrEqual(0)
        .Default(120_GB);
    registrar.Parameter("net", &TThis::Net)
        .Optional();
    registrar.Parameter("type", &TThis::Type)
        .Default();
}

void TInstanceResources::Clear()
{
    Vcpu = 0;
    Memory = 0;
}

bool TInstanceResources::operator==(const TInstanceResources& other) const
{
    return std::tie(Vcpu, Memory, Net) == std::tie(other.Vcpu, other.Memory, other.Net);
}

void TDefaultInstanceConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("cpu_limits", &TThis::CpuLimits)
        .DefaultNew();
    registrar.Parameter("memory_limits", &TThis::MemoryLimits)
        .DefaultNew();
}

void TInstanceSize::Register(TRegistrar registrar)
{
    registrar.Parameter("resource_guarantee", &TThis::ResourceGuarantee)
        .DefaultNew();
    registrar.Parameter("default_config", &TThis::DefaultConfig)
        .DefaultNew();
}

void TBundleTargetConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("cpu_limits", &TThis::CpuLimits)
        .DefaultNew();
    registrar.Parameter("memory_limits", &TThis::MemoryLimits)
        .DefaultNew();
    registrar.Parameter("rpc_proxy_count", &TThis::RpcProxyCount)
        .Optional();
    registrar.Parameter("rpc_proxy_resource_guarantee", &TThis::RpcProxyResourceGuarantee)
        .Default();
    registrar.Parameter("tablet_node_count", &TThis::TabletNodeCount)
        .Optional();
    registrar.Parameter("tablet_node_resource_guarantee", &TThis::TabletNodeResourceGuarantee)
        .Default();
}

void TBundleConfigConstraints::Register(TRegistrar registrar)
{
    registrar.Parameter("rpc_proxy_sizes", &TThis::RpcProxySizes)
        .Default();
    registrar.Parameter("tablet_node_sizes", &TThis::TabletNodeSizes)
        .Default();
}


void TBundleResourceQuota::Register(TRegistrar registrar)
{
    registrar.Parameter("vcpu", &TThis::Vcpu)
        .GreaterThanOrEqual(0)
        .Default(0);

    registrar.Parameter("memory", &TThis::Memory)
        .GreaterThanOrEqual(0)
        .Default(0);
}

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

////////////////////////////////////////////////////////////////////////////////

#define YT_FROMPROTO_OPTIONAL_PTR(messagePtr, messageField, structPtr, structField) (((messagePtr)->has_##messageField()) ? (structPtr)->structField = (messagePtr)->messageField() : (structPtr)->structField)
#define YT_TOPROTO_OPTIONAL_PTR(messagePtr, messageField, structPtr, structField) (((structPtr)->structField.has_value()) ? (messagePtr)->set_##messageField((structPtr)->structField.value()) : void())

////////////////////////////////////////////////////////////////////////////////

void ToProto(NBundleController::NProto::TCpuLimits* protoCpuLimits, const NBundleControllerClient::TCpuLimitsPtr cpuLimits)
{
    YT_TOPROTO_OPTIONAL_PTR(protoCpuLimits, lookup_thread_pool_size, cpuLimits, LookupThreadPoolSize);
    YT_TOPROTO_OPTIONAL_PTR(protoCpuLimits, query_thread_pool_size, cpuLimits, QueryThreadPoolSize);
    YT_TOPROTO_OPTIONAL_PTR(protoCpuLimits, write_thread_pool_size, cpuLimits, WriteThreadPoolSize);
}

void FromProto(NBundleControllerClient::TCpuLimitsPtr cpuLimits, const NBundleController::NProto::TCpuLimits* protoCpuLimits)
{
    YT_FROMPROTO_OPTIONAL_PTR(protoCpuLimits, lookup_thread_pool_size, cpuLimits, LookupThreadPoolSize);
    YT_FROMPROTO_OPTIONAL_PTR(protoCpuLimits, query_thread_pool_size, cpuLimits, QueryThreadPoolSize);
    YT_FROMPROTO_OPTIONAL_PTR(protoCpuLimits, write_thread_pool_size, cpuLimits, WriteThreadPoolSize);
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NBundleController::NProto::TMemoryLimits* protoMemoryLimits, const NBundleControllerClient::TMemoryLimitsPtr memoryLimits)
{
    YT_TOPROTO_OPTIONAL_PTR(protoMemoryLimits, compressed_block_cache, memoryLimits, CompressedBlockCache);
    YT_TOPROTO_OPTIONAL_PTR(protoMemoryLimits, key_filter_block_cache, memoryLimits, KeyFilterBlockCache);
    YT_TOPROTO_OPTIONAL_PTR(protoMemoryLimits, lookup_row_cache, memoryLimits, LookupRowCache);

    YT_TOPROTO_OPTIONAL_PTR(protoMemoryLimits, tablet_dynamic, memoryLimits, TabletDynamic);
    YT_TOPROTO_OPTIONAL_PTR(protoMemoryLimits, tablet_static, memoryLimits, TabletStatic);

    YT_TOPROTO_OPTIONAL_PTR(protoMemoryLimits, uncompressed_block_cache, memoryLimits, UncompressedBlockCache);

    YT_TOPROTO_OPTIONAL_PTR(protoMemoryLimits, versioned_chunk_meta, memoryLimits, VersionedChunkMeta);

    YT_TOPROTO_OPTIONAL_PTR(protoMemoryLimits, reserved, memoryLimits, Reserved);
}

void FromProto(NBundleControllerClient::TMemoryLimitsPtr memoryLimits, const NBundleController::NProto::TMemoryLimits* protoMemoryLimits)
{
    YT_FROMPROTO_OPTIONAL_PTR(protoMemoryLimits, compressed_block_cache, memoryLimits, CompressedBlockCache);
    YT_FROMPROTO_OPTIONAL_PTR(protoMemoryLimits, key_filter_block_cache, memoryLimits, KeyFilterBlockCache);
    YT_FROMPROTO_OPTIONAL_PTR(protoMemoryLimits, lookup_row_cache, memoryLimits, LookupRowCache);

    YT_FROMPROTO_OPTIONAL_PTR(protoMemoryLimits, tablet_dynamic, memoryLimits, TabletDynamic);
    YT_FROMPROTO_OPTIONAL_PTR(protoMemoryLimits, tablet_static, memoryLimits, TabletStatic);

    YT_FROMPROTO_OPTIONAL_PTR(protoMemoryLimits, uncompressed_block_cache, memoryLimits, UncompressedBlockCache);

    YT_FROMPROTO_OPTIONAL_PTR(protoMemoryLimits, versioned_chunk_meta, memoryLimits, VersionedChunkMeta);

    YT_FROMPROTO_OPTIONAL_PTR(protoMemoryLimits, reserved, memoryLimits, Reserved);
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NBundleController::NProto::TInstanceResources* protoInstanceResources, const NBundleControllerClient::TInstanceResourcesPtr instanceResources)
{
    if (instanceResources == nullptr) return;
    protoInstanceResources->set_memory(instanceResources->Memory);
    YT_TOPROTO_OPTIONAL_PTR(protoInstanceResources, net, instanceResources, Net);
    protoInstanceResources->set_type(instanceResources->Type);
    protoInstanceResources->set_vcpu(instanceResources->Vcpu);
}

void FromProto(NBundleControllerClient::TInstanceResourcesPtr instanceResources, const NBundleController::NProto::TInstanceResources* protoInstanceResources)
{
    YT_FROMPROTO_OPTIONAL_PTR(protoInstanceResources, memory, instanceResources, Memory);
    YT_FROMPROTO_OPTIONAL_PTR(protoInstanceResources, net, instanceResources, Net);
    YT_FROMPROTO_OPTIONAL_PTR(protoInstanceResources, type, instanceResources, Type);
    YT_FROMPROTO_OPTIONAL_PTR(protoInstanceResources, vcpu, instanceResources, Vcpu);
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NBundleController::NProto::TDefaultInstanceConfig* protoDefaultInstanceConfig, const TDefaultInstanceConfigPtr defaultInstanceConfig)
{
    ToProto(protoDefaultInstanceConfig->mutable_cpu_limits(), defaultInstanceConfig->CpuLimits);
    ToProto(protoDefaultInstanceConfig->mutable_memory_limits(), defaultInstanceConfig->MemoryLimits);
}

void FromProto(TDefaultInstanceConfigPtr defaultInstanceConfig, const NBundleController::NProto::TDefaultInstanceConfig* protoDefaultInstanceConfig)
{
    if (protoDefaultInstanceConfig->has_cpu_limits())
        FromProto(defaultInstanceConfig->CpuLimits, &protoDefaultInstanceConfig->cpu_limits());
    if (protoDefaultInstanceConfig->has_memory_limits())
        FromProto(defaultInstanceConfig->MemoryLimits, &protoDefaultInstanceConfig->memory_limits());
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NBundleController::NProto::TInstanceSize* protoInstanceSize, const TInstanceSizePtr instanceSize)
{
    ToProto(protoInstanceSize->mutable_resource_guarantee(), instanceSize->ResourceGuarantee);
    ToProto(protoInstanceSize->mutable_default_config(), instanceSize->DefaultConfig);
}

void FromProto(TInstanceSizePtr instanceSize, const NBundleController::NProto::TInstanceSize* protoInstanceSize)
{
    if (protoInstanceSize->has_resource_guarantee())
        FromProto(instanceSize->ResourceGuarantee, &protoInstanceSize->resource_guarantee());
    if (protoInstanceSize->has_default_config())
        FromProto(instanceSize->DefaultConfig, &protoInstanceSize->default_config());
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NBundleController::NProto::TBundleConfig* protoBundleConfig, const NBundleControllerClient::TBundleTargetConfigPtr bundleConfig)
{
    YT_TOPROTO_OPTIONAL_PTR(protoBundleConfig, rpc_proxy_count, bundleConfig, RpcProxyCount);
    YT_TOPROTO_OPTIONAL_PTR(protoBundleConfig, tablet_node_count, bundleConfig, TabletNodeCount);
    ToProto(protoBundleConfig->mutable_cpu_limits(), bundleConfig->CpuLimits);
    ToProto(protoBundleConfig->mutable_memory_limits(), bundleConfig->MemoryLimits);
    ToProto(protoBundleConfig->mutable_rpc_proxy_resource_guarantee(), bundleConfig->RpcProxyResourceGuarantee);
    ToProto(protoBundleConfig->mutable_tablet_node_resource_guarantee(), bundleConfig->TabletNodeResourceGuarantee);
}

void FromProto(NBundleControllerClient::TBundleTargetConfigPtr bundleConfig, const NBundleController::NProto::TBundleConfig* protoBundleConfig)
{
    YT_FROMPROTO_OPTIONAL_PTR(protoBundleConfig, rpc_proxy_count, bundleConfig, RpcProxyCount);
    YT_FROMPROTO_OPTIONAL_PTR(protoBundleConfig, tablet_node_count, bundleConfig, TabletNodeCount);
    if (protoBundleConfig->has_cpu_limits())
        FromProto(bundleConfig->CpuLimits, &protoBundleConfig->cpu_limits());
    if (protoBundleConfig->has_memory_limits())
        FromProto(bundleConfig->MemoryLimits, &protoBundleConfig->memory_limits());
    if (protoBundleConfig->has_rpc_proxy_resource_guarantee())
        FromProto(bundleConfig->RpcProxyResourceGuarantee, &protoBundleConfig->rpc_proxy_resource_guarantee());
    if (protoBundleConfig->has_tablet_node_resource_guarantee())
        FromProto(bundleConfig->TabletNodeResourceGuarantee, &protoBundleConfig->tablet_node_resource_guarantee());
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NBundleController::NProto::TBundleConfigConstraints* protoBundleConfigConstraints, const TBundleConfigConstraintsPtr bundleConfigConstraints)
{
    for (auto instance : bundleConfigConstraints->RpcProxySizes) {
        ToProto(protoBundleConfigConstraints->add_rpc_proxy_sizes(), instance);
    }
    for (auto instance : bundleConfigConstraints->TabletNodeSizes) {
        ToProto(protoBundleConfigConstraints->add_tablet_node_sizes(), instance);
    }
}

void FromProto(TBundleConfigConstraintsPtr bundleConfigConstraints, const NBundleController::NProto::TBundleConfigConstraints* protoBundleConfigConstraints)
{
    auto rpcProxySizes = protoBundleConfigConstraints->get_arr_rpc_proxy_sizes();

    for (auto instance : rpcProxySizes) {
        auto newInstance = New<TInstanceSize>();
        FromProto(newInstance, &instance);
        bundleConfigConstraints->RpcProxySizes.push_back(newInstance);
    }

    auto tabletNodeSizes = protoBundleConfigConstraints->get_arr_tablet_node_sizes();

    for (auto instance : tabletNodeSizes) {
        auto newInstance = New<TInstanceSize>();
        FromProto(newInstance, &instance);
        bundleConfigConstraints->TabletNodeSizes.push_back(newInstance);
    }
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NBundleController::NProto::TResourceQuota* protoResourceQuota, const TBundleResourceQuotaPtr resourceQuota)
{
    protoResourceQuota->set_vcpu(resourceQuota->Vcpu);
    protoResourceQuota->set_memory(resourceQuota->Memory);
}

void FromProto(TBundleResourceQuotaPtr resourceQuota, const NBundleController::NProto::TResourceQuota* protoResourceQuota)
{
    YT_FROMPROTO_OPTIONAL_PTR(protoResourceQuota, memory, resourceQuota, Memory);
    YT_FROMPROTO_OPTIONAL_PTR(protoResourceQuota, vcpu, resourceQuota, Vcpu);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NProto

} // namespace NYT::NBundleControllerClient

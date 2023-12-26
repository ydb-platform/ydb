#include "bundle_controller_settings.h"

namespace NYT::NBundleControllerClient {

////////////////////////////////////////////////////////////////////////////////

void TCpuLimits::Register(TRegistrar registrar)
{
    registrar.Parameter("write_thread_pool_size", &TThis::WriteThreadPoolSize)
        .GreaterThan(0)
        .Default(5);
    registrar.Parameter("lookup_thread_pool_size", &TThis::LookupThreadPoolSize)
        .GreaterThan(0)
        .Default(4);
    registrar.Parameter("query_thread_pool_size", &TThis::QueryThreadPoolSize)
        .GreaterThan(0)
        .Default(4);
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

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

////////////////////////////////////////////////////////////////////////////////

void ToProto(NBundleController::NProto::TCpuLimits* protoCpuLimits, const NBundleControllerClient::TCpuLimitsPtr cpuLimits)
{
    protoCpuLimits->set_lookup_thread_pool_size(cpuLimits->LookupThreadPoolSize);
    protoCpuLimits->set_query_thread_pool_size(cpuLimits->QueryThreadPoolSize);
    protoCpuLimits->set_write_thread_pool_size(cpuLimits->WriteThreadPoolSize);
}

void FromProto(NBundleControllerClient::TCpuLimitsPtr cpuLimits, const NBundleController::NProto::TCpuLimits* protoCpuLimits)
{
    cpuLimits->LookupThreadPoolSize = protoCpuLimits->lookup_thread_pool_size();
    cpuLimits->QueryThreadPoolSize = protoCpuLimits->query_thread_pool_size();
    cpuLimits->WriteThreadPoolSize = protoCpuLimits->write_thread_pool_size();
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NBundleController::NProto::TMemoryLimits* protoMemoryLimits, const NBundleControllerClient::TMemoryLimitsPtr memoryLimits)
{
    protoMemoryLimits->set_compressed_block_cache(memoryLimits->CompressedBlockCache.value_or(0));
    protoMemoryLimits->set_key_filter_block_cache(memoryLimits->KeyFilterBlockCache.value_or(0));
    protoMemoryLimits->set_lookup_row_cache(memoryLimits->LookupRowCache.value_or(0));

    protoMemoryLimits->set_tablet_dynamic(memoryLimits->TabletDynamic.value_or(0));
    protoMemoryLimits->set_tablet_static(memoryLimits->TabletStatic.value_or(0));

    protoMemoryLimits->set_uncompressed_block_cache(memoryLimits->UncompressedBlockCache.value_or(0));

    protoMemoryLimits->set_versioned_chunk_meta(memoryLimits->VersionedChunkMeta.value_or(0));
}

void FromProto(NBundleControllerClient::TMemoryLimitsPtr memoryLimits, const NBundleController::NProto::TMemoryLimits* protoMemoryLimits)
{
    memoryLimits->CompressedBlockCache = protoMemoryLimits->compressed_block_cache();
    memoryLimits->KeyFilterBlockCache = protoMemoryLimits->key_filter_block_cache();
    memoryLimits->LookupRowCache = protoMemoryLimits->lookup_row_cache();

    memoryLimits->TabletDynamic = protoMemoryLimits->tablet_dynamic();
    memoryLimits->TabletStatic = protoMemoryLimits->tablet_static();

    memoryLimits->UncompressedBlockCache = protoMemoryLimits->uncompressed_block_cache();

    memoryLimits->VersionedChunkMeta = protoMemoryLimits->versioned_chunk_meta();
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NBundleController::NProto::TInstanceResources* protoInstanceResources, const NBundleControllerClient::TInstanceResourcesPtr instanceResources)
{
    protoInstanceResources->set_memory(instanceResources->Memory);
    protoInstanceResources->set_net(instanceResources->Net.value_or(0));
    protoInstanceResources->set_type(instanceResources->Type);
    protoInstanceResources->set_vcpu(instanceResources->Vcpu);
}

void FromProto(NBundleControllerClient::TInstanceResourcesPtr instanceResources, const NBundleController::NProto::TInstanceResources* protoInstanceResources)
{
    instanceResources->Memory = protoInstanceResources->memory();
    instanceResources->Net = protoInstanceResources->net();
    instanceResources->Type = protoInstanceResources->type();
    instanceResources->Vcpu = protoInstanceResources->vcpu();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NProto

} // namespace NYT::NBundleControllerClient

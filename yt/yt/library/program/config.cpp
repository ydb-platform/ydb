#include "config.h"

namespace NYT {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void THeapSizeLimit::Register(TRegistrar registrar)
{
    registrar.Parameter("container_memory_ratio", &TThis::ContainerMemoryRatio)
        .Optional();
    registrar.Parameter("is_hard", &TThis::IsHard)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void TTCMallocConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("background_release_rate", &TThis::BackgroundReleaseRate)
        .Default(32_MB);
    registrar.Parameter("max_per_cpu_cache_size", &TThis::MaxPerCpuCacheSize)
        .Default(3_MB);

    registrar.Parameter("aggressive_release_threshold", &TThis::AggressiveReleaseThreshold)
        .Default(20_GB);
    registrar.Parameter("aggressive_release_threshold_ratio", &TThis::AggressiveReleaseThresholdRatio)
        .Optional();

    registrar.Parameter("aggressive_release_size", &TThis::AggressiveReleaseSize)
        .Default(128_MB);
    registrar.Parameter("aggressive_release_period", &TThis::AggressiveReleasePeriod)
        .Default(TDuration::MilliSeconds(100));
    registrar.Parameter("guarded_sampling_rate", &TThis::GuardedSamplingRate)
        .Default(128_MB);

    registrar.Parameter("heap_size_limit", &TThis::HeapSizeLimit)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

void TStockpileConfig::Register(TRegistrar registrar)
{
    registrar.BaseClassParameter("buffer_size", &TThis::BufferSize)
        .Default(DefaultBufferSize);
    registrar.BaseClassParameter("thread_count", &TThis::ThreadCount)
        .Default(DefaultThreadCount);
    registrar.BaseClassParameter("period", &TThis::Period)
        .Default(DefaultPeriod);
}

////////////////////////////////////////////////////////////////////////////////

void THeapProfilerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("sampling_rate", &TThis::SamplingRate)
        .Default();
    registrar.Parameter("snapshot_update_period", &TThis::SnapshotUpdatePeriod)
        .Default(TDuration::Seconds(5));
}

////////////////////////////////////////////////////////////////////////////////

void TSingletonsConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("spin_wait_slow_path_logging_threshold", &TThis::SpinWaitSlowPathLoggingThreshold)
        .Default(TDuration::MicroSeconds(100));
    registrar.Parameter("yt_alloc", &TThis::YTAlloc)
        .DefaultNew();
    registrar.Parameter("fiber_stack_pool_sizes", &TThis::FiberStackPoolSizes)
        .Default({});
    registrar.Parameter("address_resolver", &TThis::AddressResolver)
        .DefaultNew();
    registrar.Parameter("tcp_dispatcher", &TThis::TcpDispatcher)
        .DefaultNew();
    registrar.Parameter("rpc_dispatcher", &TThis::RpcDispatcher)
        .DefaultNew();
    registrar.Parameter("grpc_dispatcher", &TThis::GrpcDispatcher)
        .DefaultNew();
    registrar.Parameter("yp_service_discovery", &TThis::YPServiceDiscovery)
        .DefaultNew();
    registrar.Parameter("solomon_exporter", &TThis::SolomonExporter)
        .DefaultNew();
    registrar.Parameter("logging", &TThis::Logging)
        .DefaultCtor([] () { return NLogging::TLogManagerConfig::CreateDefault(); });
    registrar.Parameter("jaeger", &TThis::Jaeger)
        .DefaultNew();
    registrar.Parameter("tracing_transport", &TThis::TracingTransport)
        .DefaultNew();
    registrar.Parameter("tcmalloc", &TThis::TCMalloc)
        .DefaultNew();
    registrar.Parameter("stockpile", &TThis::Stockpile)
        .DefaultNew();
    registrar.Parameter("enable_ref_counted_tracker_profiling", &TThis::EnableRefCountedTrackerProfiling)
        .Default(true);
    registrar.Parameter("enable_resource_tracker", &TThis::EnableResourceTracker)
        .Default(true);
    registrar.Parameter("resource_tracker_vcpu_factor", &TThis::ResourceTrackerVCpuFactor)
        .Optional();
    registrar.Parameter("heap_profiler", &TThis::HeapProfiler)
        .DefaultNew();
    registrar.Parameter("protobuf_interop", &TThis::ProtobufInterop)
        .DefaultNew();

    registrar.Postprocessor([] (TThis* config) {
        if (config->ResourceTrackerVCpuFactor && !config->EnableResourceTracker) {
            THROW_ERROR_EXCEPTION("Option \"resource_tracker_vcpu_factor\" can be specified only if resource tracker is enabled");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TSingletonsDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("spin_lock_slow_path_logging_threshold", &TThis::SpinWaitSlowPathLoggingThreshold)
        .Optional();
    registrar.Parameter("yt_alloc", &TThis::YTAlloc)
        .Optional();
    registrar.Parameter("tcp_dispatcher", &TThis::TcpDispatcher)
        .DefaultNew();
    registrar.Parameter("rpc_dispatcher", &TThis::RpcDispatcher)
        .DefaultNew();
    registrar.Parameter("logging", &TThis::Logging)
        .DefaultNew();
    registrar.Parameter("jaeger", &TThis::Jaeger)
        .DefaultNew();
    registrar.Parameter("tracing_transport", &TThis::TracingTransport)
        .DefaultNew();
    registrar.Parameter("tcmalloc", &TThis::TCMalloc)
        .Optional();
    registrar.Parameter("protobuf_interop", &TThis::ProtobufInterop)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

void TDiagnosticDumpConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("yt_alloc_dump_period", &TThis::YTAllocDumpPeriod)
        .Default();
    registrar.Parameter("ref_counted_tracker_dump_period", &TThis::RefCountedTrackerDumpPeriod)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void WarnForUnrecognizedOptionsImpl(
    const NLogging::TLogger& logger,
    const IMapNodePtr& unrecognized)
{
    const auto& Logger = logger;
    if (unrecognized && unrecognized->GetChildCount() > 0) {
        YT_LOG_WARNING("Bootstrap config contains unrecognized options (Unrecognized: %v)",
            ConvertToYsonString(unrecognized, NYson::EYsonFormat::Text));
    }
}

void WarnForUnrecognizedOptions(
    const NLogging::TLogger& logger,
    const NYTree::TYsonStructPtr& config)
{
    WarnForUnrecognizedOptionsImpl(logger, config->GetRecursiveUnrecognized());
}

void AbortOnUnrecognizedOptionsImpl(
    const NLogging::TLogger& logger,
    const IMapNodePtr& unrecognized)
{
    const auto& Logger = logger;
    if (unrecognized && unrecognized->GetChildCount() > 0) {
        YT_LOG_ERROR("Bootstrap config contains unrecognized options, terminating (Unrecognized: %v)",
            ConvertToYsonString(unrecognized, NYson::EYsonFormat::Text));
        YT_ABORT();
    }
}

void AbortOnUnrecognizedOptions(
    const NLogging::TLogger& logger,
    const NYTree::TYsonStructPtr& config)
{
    AbortOnUnrecognizedOptionsImpl(logger, config->GetRecursiveUnrecognized());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT


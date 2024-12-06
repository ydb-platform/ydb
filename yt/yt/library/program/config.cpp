#include "config.h"

#include <yt/yt/core/concurrency/fiber_scheduler_thread.h>

namespace NYT {

using namespace NYTree;

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
    registrar.Parameter("fiber_manager", &TThis::FiberManager)
        .DefaultNew();
    registrar.Parameter("address_resolver", &TThis::AddressResolver)
        .DefaultNew();
    registrar.Parameter("tcp_dispatcher", &TThis::TcpDispatcher)
        .DefaultNew();
    registrar.Parameter("io_dispatcher", &TThis::IODispatcher)
        .DefaultNew();
    registrar.Parameter("rpc_dispatcher", &TThis::RpcDispatcher)
        .DefaultNew();
    registrar.Parameter("grpc_dispatcher", &TThis::GrpcDispatcher)
        .DefaultNew();
    registrar.Parameter("yp_service_discovery", &TThis::YPServiceDiscovery)
        .DefaultNew();
    registrar.Parameter("logging", &TThis::Logging)
        .DefaultCtor([] { return NLogging::TLogManagerConfig::CreateDefault(); })
        .ResetOnLoad();
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
    registrar.Parameter("resource_tracker", &TThis::ResourceTracker)
        .DefaultNew();
    registrar.Parameter("heap_profiler", &TThis::HeapProfiler)
        .DefaultNew();
    registrar.Parameter("protobuf_interop", &TThis::ProtobufInterop)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

void TSingletonsDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("spin_lock_slow_path_logging_threshold", &TThis::SpinWaitSlowPathLoggingThreshold)
        .Optional();
    registrar.Parameter("fiber_manager", &TThis::FiberManager)
        .DefaultNew();
    registrar.Parameter("tcp_dispatcher", &TThis::TcpDispatcher)
        .DefaultNew();
    registrar.Parameter("io_dispatcher", &TThis::IODispatcher)
        .Optional();
    registrar.Parameter("rpc_dispatcher", &TThis::RpcDispatcher)
        .DefaultNew();
    registrar.Parameter("logging", &TThis::Logging)
        .DefaultNew();
    registrar.Parameter("jaeger", &TThis::Jaeger)
        .DefaultNew();
    registrar.Parameter("tracing_transport", &TThis::TracingTransport)
        .Optional();
    registrar.Parameter("tcmalloc", &TThis::TCMalloc)
        .Optional();
    registrar.Parameter("stockpile", &TThis::Stockpile)
        .DefaultNew();
    registrar.Parameter("protobuf_interop", &TThis::ProtobufInterop)
        .DefaultNew();
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


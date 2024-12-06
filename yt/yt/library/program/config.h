#pragma once

#include "public.h"

#include <yt/yt/core/concurrency/config.h>

#include <yt/yt/core/tracing/config.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/core/net/config.h>

#include <yt/yt/core/rpc/config.h>
#include <yt/yt/core/rpc/grpc/config.h>

#include <yt/yt/core/bus/tcp/config.h>

#include <yt/yt/core/logging/config.h>

#include <yt/yt/core/tracing/config.h>

#include <yt/yt/core/service_discovery/yp/config.h>

#include <yt/yt/core/yson/config.h>

#include <yt/yt/library/process/io_dispatcher.h>

#include <yt/yt/library/tracing/jaeger/tracer.h>

#include <yt/yt/library/profiling/resource_tracker/config.h>

#include <yt/yt/library/tcmalloc/config.h>

#include <yt/yt/library/stockpile/config.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class THeapProfilerConfig
    : public NYTree::TYsonStruct
{
public:
    // Sampling rate for tcmalloc in bytes.
    // See https://github.com/google/tcmalloc/blob/master/docs/sampling.md
    std::optional<i64> SamplingRate;

    // Period of update snapshot in heap profiler.
    std::optional<TDuration> SnapshotUpdatePeriod;

    REGISTER_YSON_STRUCT(THeapProfilerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(THeapProfilerConfig)

////////////////////////////////////////////////////////////////////////////////

class TSingletonsConfig
    : public virtual NYTree::TYsonStruct
{
public:
    NConcurrency::TFiberManagerConfigPtr FiberManager;
    NNet::TAddressResolverConfigPtr AddressResolver;
    NBus::TTcpDispatcherConfigPtr TcpDispatcher;
    NPipes::TIODispatcherConfigPtr IODispatcher;
    NRpc::TDispatcherConfigPtr RpcDispatcher;
    NRpc::NGrpc::TDispatcherConfigPtr GrpcDispatcher;
    NServiceDiscovery::NYP::TServiceDiscoveryConfigPtr YPServiceDiscovery;
    NLogging::TLogManagerConfigPtr Logging;
    NTracing::TJaegerTracerConfigPtr Jaeger;
    NTracing::TTracingTransportConfigPtr TracingTransport;
    NTCMalloc::TTCMallocConfigPtr TCMalloc;
    TStockpileConfigPtr Stockpile;
    bool EnableRefCountedTrackerProfiling;
    NProfiling::TResourceTrackerConfigPtr ResourceTracker;
    THeapProfilerConfigPtr HeapProfiler;
    NYson::TProtobufInteropConfigPtr ProtobufInterop;

    REGISTER_YSON_STRUCT(TSingletonsConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSingletonsConfig)

////////////////////////////////////////////////////////////////////////////////

class TSingletonsDynamicConfig
    : public virtual NYTree::TYsonStruct
{
public:
    NConcurrency::TFiberManagerDynamicConfigPtr FiberManager;
    NBus::TTcpDispatcherDynamicConfigPtr TcpDispatcher;
    NPipes::TIODispatcherConfigPtr IODispatcher;
    NRpc::TDispatcherDynamicConfigPtr RpcDispatcher;
    NLogging::TLogManagerDynamicConfigPtr Logging;
    NTracing::TJaegerTracerDynamicConfigPtr Jaeger;
    NTracing::TTracingTransportConfigPtr TracingTransport;
    NTCMalloc::TTCMallocConfigPtr TCMalloc;
    TStockpileDynamicConfigPtr Stockpile;
    NYson::TProtobufInteropDynamicConfigPtr ProtobufInterop;

    REGISTER_YSON_STRUCT(TSingletonsDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSingletonsDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

// NB: These functions should not be called from bootstrap
// config validator since logger is not set up yet.
void WarnForUnrecognizedOptions(
    const NLogging::TLogger& logger,
    const NYTree::TYsonStructPtr& config);

void AbortOnUnrecognizedOptions(
    const NLogging::TLogger& logger,
    const NYTree::TYsonStructPtr& config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

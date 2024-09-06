#pragma once

#include "public.h"

#include <yt/yt/core/tracing/config.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/core/ytalloc/config.h>

#include <yt/yt/core/net/config.h>

#include <yt/yt/core/rpc/config.h>
#include <yt/yt/core/rpc/grpc/config.h>

#include <yt/yt/core/bus/tcp/config.h>

#include <yt/yt/core/logging/config.h>

#include <yt/yt/core/tracing/config.h>

#include <yt/yt/core/service_discovery/yp/config.h>

#include <yt/yt/core/yson/config.h>

#include <yt/yt/library/process/io_dispatcher.h>

#include <yt/yt/library/profiling/solomon/exporter.h>

#include <yt/yt/library/tracing/jaeger/tracer.h>

#include <library/cpp/yt/stockpile/stockpile.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class THeapSizeLimitConfig
    : public virtual NYTree::TYsonStruct
{
public:
    //! Limit program memory in terms of container memory.
    // If program heap size exceeds the limit tcmalloc is instructed to release memory to the kernel.
    std::optional<double> ContainerMemoryRatio;

    //! Similar to #ContainerMemoryRatio, but is set in terms of absolute difference from
    //! the container memory limit.
    //! For example, if ContainerMemoryLimit=200Gb and ContainerMemoryMargin=1Gb
    // then tcmalloc limit will be 199Gb.
    std::optional<double> ContainerMemoryMargin;

    //! If true tcmalloc crashes when system allocates more memory than #ContainerMemoryRatio/#ContainerMemoryMargin.
    bool Hard;

    bool DumpMemoryProfileOnViolation;
    TDuration DumpMemoryProfileTimeout;
    TString DumpMemoryProfilePath;

    REGISTER_YSON_STRUCT(THeapSizeLimitConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(THeapSizeLimitConfig)

////////////////////////////////////////////////////////////////////////////////

class TTCMallocConfig
    : public virtual NYTree::TYsonStruct
{
public:
    i64 BackgroundReleaseRate;
    int MaxPerCpuCacheSize;

    //! Threshold in bytes
    i64 AggressiveReleaseThreshold;

    //! Threshold in fractions of total memory of the container
    std::optional<double> AggressiveReleaseThresholdRatio;

    i64 AggressiveReleaseSize;
    TDuration AggressiveReleasePeriod;

    //! Approximately 1/#GuardedSamplingRate of all allocations of
    //! size <= 256 KiB will be under GWP-ASAN.
    std::optional<i64> GuardedSamplingRate;

    THeapSizeLimitConfigPtr HeapSizeLimit;

    REGISTER_YSON_STRUCT(TTCMallocConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTCMallocConfig)

////////////////////////////////////////////////////////////////////////////////

class TStockpileConfig
    : public TStockpileOptions
    , public NYTree::TYsonStruct
{
public:
    REGISTER_YSON_STRUCT(TStockpileConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TStockpileConfig)

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
    TDuration SpinWaitSlowPathLoggingThreshold;
    NYTAlloc::TYTAllocConfigPtr YTAlloc;
    THashMap<TString, int> FiberStackPoolSizes;
    NNet::TAddressResolverConfigPtr AddressResolver;
    NBus::TTcpDispatcherConfigPtr TcpDispatcher;
    NPipes::TIODispatcherConfigPtr IODispatcher;
    NRpc::TDispatcherConfigPtr RpcDispatcher;
    NRpc::NGrpc::TDispatcherConfigPtr GrpcDispatcher;
    NServiceDiscovery::NYP::TServiceDiscoveryConfigPtr YPServiceDiscovery;
    NProfiling::TSolomonExporterConfigPtr SolomonExporter;
    NLogging::TLogManagerConfigPtr Logging;
    NTracing::TJaegerTracerConfigPtr Jaeger;
    NTracing::TTracingTransportConfigPtr TracingTransport;
    TTCMallocConfigPtr TCMalloc;
    TStockpileConfigPtr Stockpile;
    bool EnableRefCountedTrackerProfiling;
    bool EnableResourceTracker;
    std::optional<double> ResourceTrackerVCpuFactor;
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
    std::optional<TDuration> SpinWaitSlowPathLoggingThreshold;
    ui64 MaxIdleFibers;
    NYTAlloc::TYTAllocConfigPtr YTAlloc;
    NBus::TTcpDispatcherDynamicConfigPtr TcpDispatcher;
    NPipes::TIODispatcherConfigPtr IODispatcher;
    NRpc::TDispatcherDynamicConfigPtr RpcDispatcher;
    NLogging::TLogManagerDynamicConfigPtr Logging;
    NTracing::TJaegerTracerDynamicConfigPtr Jaeger;
    NTracing::TTracingTransportConfigPtr TracingTransport;
    TTCMallocConfigPtr TCMalloc;
    NYson::TProtobufInteropDynamicConfigPtr ProtobufInterop;

    REGISTER_YSON_STRUCT(TSingletonsDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSingletonsDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TDiagnosticDumpConfig
    : public virtual NYTree::TYsonStruct
{
public:
    std::optional<TDuration> YTAllocDumpPeriod;
    std::optional<TDuration> RefCountedTrackerDumpPeriod;

    REGISTER_YSON_STRUCT(TDiagnosticDumpConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDiagnosticDumpConfig)

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

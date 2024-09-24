#include "helpers.h"
#include "config.h"
#include "private.h"

#include <yt/yt/core/ytalloc/bindings.h>

#include <yt/yt/core/misc/lazy_ptr.h>
#include <yt/yt/core/misc/ref_counted_tracker.h>
#include <yt/yt/core/misc/ref_counted_tracker_profiler.h>

#include <yt/yt/core/bus/tcp/dispatcher.h>

#include <yt/yt/library/oom/oom.h>

#include <yt/yt/library/tracing/jaeger/tracer.h>

#include <yt/yt/library/profiling/perf/counters.h>

#include <yt/yt/library/profiling/resource_tracker/resource_tracker.h>

#include <yt/yt/core/logging/log_manager.h>

#include <yt/yt/core/concurrency/execution_stack.h>
#include <yt/yt/core/concurrency/fiber_scheduler_thread.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <tcmalloc/malloc_extension.h>

#include <yt/yt/core/net/address.h>

#include <yt/yt/core/yson/protobuf_interop.h>

#include <yt/yt/core/rpc/dispatcher.h>
#include <yt/yt/core/rpc/grpc/dispatcher.h>

#include <yt/yt/core/service_discovery/yp/service_discovery.h>

#include <yt/yt/core/threading/spin_wait_slow_path_logger.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

#include <util/string/split.h>
#include <util/system/thread.h>

#include <mutex>
#include <thread>

namespace NYT {

using namespace NConcurrency;
using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

static std::once_flag InitAggressiveReleaseThread;
static auto& Logger = ProgramLogger;

////////////////////////////////////////////////////////////////////////////////

class TCMallocLimitsAdjuster
{
public:
    void Adjust(const TTCMallocConfigPtr& config)
    {
        i64 totalMemory = GetAnonymousMemoryLimit();
        AdjustPageHeapLimit(totalMemory, config);
        AdjustAggressiveReleaseThreshold(totalMemory, config);
        SetupMemoryLimitHandler(config);
    }

    i64 GetAggressiveReleaseThreshold()
    {
        return AggressiveReleaseThreshold_;
    }

private:
    using TAllocatorMemoryLimit = tcmalloc::MallocExtension::MemoryLimit;

    TAllocatorMemoryLimit AppliedLimit_;
    i64 AggressiveReleaseThreshold_ = 0;


    void AdjustPageHeapLimit(i64 totalMemory, const TTCMallocConfigPtr& config)
    {
        auto proposed = ProposeHeapMemoryLimit(totalMemory, config);

        if (proposed.limit == AppliedLimit_.limit && proposed.hard == AppliedLimit_.hard) {
            // Already applied
            return;
        }

        YT_LOG_INFO("Changing tcmalloc memory limit (Limit: %v, Hard: %v)",
            proposed.limit,
            proposed.hard);

        tcmalloc::MallocExtension::SetMemoryLimit(proposed);
        AppliedLimit_ = proposed;
    }

    void AdjustAggressiveReleaseThreshold(i64 totalMemory, const TTCMallocConfigPtr& config)
    {
        if (totalMemory && config->AggressiveReleaseThresholdRatio) {
            AggressiveReleaseThreshold_ = *config->AggressiveReleaseThresholdRatio * totalMemory;
        } else {
            AggressiveReleaseThreshold_ = config->AggressiveReleaseThreshold;
        }
    }

    void SetupMemoryLimitHandler(const TTCMallocConfigPtr& config)
    {
        TTCMallocLimitHandlerOptions handlerOptions {
            .HeapDumpDirectory = config->HeapSizeLimit->DumpMemoryProfilePath,
            .Timeout = config->HeapSizeLimit->DumpMemoryProfileTimeout,
        };

        if (config->HeapSizeLimit->DumpMemoryProfileOnViolation) {
            EnableTCMallocLimitHandler(handlerOptions);
        } else {
            DisableTCMallocLimitHandler();
        }
    }

    i64 GetAnonymousMemoryLimit() const
    {
        auto resourceTracker = NProfiling::GetResourceTracker();
        if (!resourceTracker) {
            return 0;
        }

        return resourceTracker->GetAnonymousMemoryLimit();
    }

    TAllocatorMemoryLimit ProposeHeapMemoryLimit(i64 totalMemory, const TTCMallocConfigPtr& config) const
    {
        const auto& heapSizeConfig = config->HeapSizeLimit;

        if (totalMemory == 0 || !heapSizeConfig->ContainerMemoryRatio && !heapSizeConfig->ContainerMemoryMargin) {
            return {};
        }

        TAllocatorMemoryLimit proposed;
        proposed.hard = heapSizeConfig->Hard;

        if (heapSizeConfig->ContainerMemoryMargin) {
            proposed.limit = totalMemory - *heapSizeConfig->ContainerMemoryMargin;
        } else {
            proposed.limit = *heapSizeConfig->ContainerMemoryRatio * totalMemory;
        }

        return proposed;
    }
};

void ConfigureTCMalloc(const TTCMallocConfigPtr& config)
{
    tcmalloc::MallocExtension::SetBackgroundReleaseRate(
        tcmalloc::MallocExtension::BytesPerSecond{static_cast<size_t>(config->BackgroundReleaseRate)});

    tcmalloc::MallocExtension::SetMaxPerCpuCacheSize(config->MaxPerCpuCacheSize);

    if (config->GuardedSamplingRate) {
        tcmalloc::MallocExtension::SetGuardedSamplingRate(*config->GuardedSamplingRate);
        tcmalloc::MallocExtension::ActivateGuardedSampling();
    }

    struct TConfigSingleton
    {
        TAtomicIntrusivePtr<TTCMallocConfig> Config;
    };

    LeakySingleton<TConfigSingleton>()->Config.Store(config);

    if (tcmalloc::MallocExtension::NeedsProcessBackgroundActions()) {
        std::call_once(InitAggressiveReleaseThread, [] {
            std::thread([] {
                ::TThread::SetCurrentThreadName("TCAllocYT");

                TCMallocLimitsAdjuster limitsAdjuster;

                while (true) {
                    auto config = LeakySingleton<TConfigSingleton>()->Config.Acquire();
                    limitsAdjuster.Adjust(config);

                    auto freeBytes = tcmalloc::MallocExtension::GetNumericProperty("tcmalloc.page_heap_free");
                    YT_VERIFY(freeBytes);

                    if (static_cast<i64>(*freeBytes) > limitsAdjuster.GetAggressiveReleaseThreshold()) {

                        YT_LOG_DEBUG("Aggressively releasing memory (FreeBytes: %v, Threshold: %v)",
                            static_cast<i64>(*freeBytes),
                            limitsAdjuster.GetAggressiveReleaseThreshold());

                        tcmalloc::MallocExtension::ReleaseMemoryToSystem(config->AggressiveReleaseSize);
                    }

                    Sleep(config->AggressiveReleasePeriod);
                }
            }).detach();
        });
    }
}

void ConfigureSingletons(const TSingletonsConfigPtr& config)
{
    SetSpinWaitSlowPathLoggingThreshold(config->SpinWaitSlowPathLoggingThreshold);

    if (!NYTAlloc::ConfigureFromEnv()) {
        NYTAlloc::Configure(config->YTAlloc);
    }

    for (const auto& [kind, size] : config->FiberStackPoolSizes) {
        NConcurrency::SetFiberStackPoolSize(ParseEnum<NConcurrency::EExecutionStackKind>(kind), size);
    }

    NLogging::TLogManager::Get()->EnableReopenOnSighup();
    if (!NLogging::TLogManager::Get()->IsConfiguredFromEnv()) {
        NLogging::TLogManager::Get()->Configure(config->Logging);
    }

    NNet::TAddressResolver::Get()->Configure(config->AddressResolver);
    // By default, server components must have a reasonable FQDN.
    // Failure to do so may result in issues like YT-4561.
    NNet::TAddressResolver::Get()->EnsureLocalHostName();

    NBus::TTcpDispatcher::Get()->Configure(config->TcpDispatcher);

    NPipes::TIODispatcher::Get()->Configure(config->IODispatcher);

    NRpc::TDispatcher::Get()->Configure(config->RpcDispatcher);

    NRpc::NGrpc::TDispatcher::Get()->Configure(config->GrpcDispatcher);

    NRpc::TDispatcher::Get()->SetServiceDiscovery(
        NServiceDiscovery::NYP::CreateServiceDiscovery(config->YPServiceDiscovery));

    NTracing::SetGlobalTracer(New<NTracing::TJaegerTracer>(config->Jaeger));

    NProfiling::EnablePerfCounters();

    if (auto tracingConfig = config->TracingTransport) {
        NTracing::SetTracingTransportConfig(tracingConfig);
    }

    ConfigureTCMalloc(config->TCMalloc);

    ConfigureStockpile(*config->Stockpile);

    if (config->EnableRefCountedTrackerProfiling) {
        EnableRefCountedTrackerProfiling();
    }

    if (config->EnableResourceTracker) {
        NProfiling::EnableResourceTracker();
        if (config->ResourceTrackerVCpuFactor.has_value()) {
            NProfiling::SetVCpuFactor(config->ResourceTrackerVCpuFactor.value());
        }
    }

    NYson::SetProtobufInteropConfig(config->ProtobufInterop);
}

TTCMallocConfigPtr MergeTCMallocDynamicConfig(const TTCMallocConfigPtr& staticConfig, const TTCMallocConfigPtr& dynamicConfig)
{
    auto mergedConfig = CloneYsonStruct(dynamicConfig);
    mergedConfig->HeapSizeLimit->DumpMemoryProfilePath = staticConfig->HeapSizeLimit->DumpMemoryProfilePath;
    return mergedConfig;
}

void ReconfigureSingletons(const TSingletonsConfigPtr& config, const TSingletonsDynamicConfigPtr& dynamicConfig)
{
    SetSpinWaitSlowPathLoggingThreshold(dynamicConfig->SpinWaitSlowPathLoggingThreshold.value_or(config->SpinWaitSlowPathLoggingThreshold));

    NConcurrency::UpdateMaxIdleFibers(dynamicConfig->MaxIdleFibers);

    if (!NYTAlloc::IsConfiguredFromEnv()) {
        NYTAlloc::Configure(dynamicConfig->YTAlloc ? dynamicConfig->YTAlloc : config->YTAlloc);
    }

    if (!NLogging::TLogManager::Get()->IsConfiguredFromEnv()) {
        NLogging::TLogManager::Get()->Configure(
            config->Logging->ApplyDynamic(dynamicConfig->Logging),
            /*sync*/ false);
    }

    auto tracer = NTracing::GetGlobalTracer();
    if (auto jaeger = DynamicPointerCast<NTracing::TJaegerTracer>(tracer); jaeger) {
        jaeger->Configure(config->Jaeger->ApplyDynamic(dynamicConfig->Jaeger));
    }

    NBus::TTcpDispatcher::Get()->Configure(config->TcpDispatcher->ApplyDynamic(dynamicConfig->TcpDispatcher));

    NPipes::TIODispatcher::Get()->Configure(dynamicConfig->IODispatcher ? dynamicConfig->IODispatcher : config->IODispatcher);

    NRpc::TDispatcher::Get()->Configure(config->RpcDispatcher->ApplyDynamic(dynamicConfig->RpcDispatcher));

    if (dynamicConfig->TracingTransport) {
        NTracing::SetTracingTransportConfig(dynamicConfig->TracingTransport);
    } else if (config->TracingTransport) {
        NTracing::SetTracingTransportConfig(config->TracingTransport);
    }

    if (dynamicConfig->TCMalloc) {
        ConfigureTCMalloc(MergeTCMallocDynamicConfig(config->TCMalloc, dynamicConfig->TCMalloc));
    } else if (config->TCMalloc) {
        ConfigureTCMalloc(config->TCMalloc);
    }

    NYson::SetProtobufInteropConfig(config->ProtobufInterop->ApplyDynamic(dynamicConfig->ProtobufInterop));
}

template <class TConfig>
void StartDiagnosticDumpImpl(const TConfig& config)
{
    static NLogging::TLogger Logger("DiagDump");

    auto logDumpString = [&] (TStringBuf banner, const TString& str) {
        for (const auto& line : StringSplitter(str).Split('\n')) {
            YT_LOG_DEBUG("%v %v", banner, line.Token());
        }
    };

    if (config->YTAllocDumpPeriod) {
        static const TLazyIntrusivePtr<TPeriodicExecutor> Executor(BIND([&] {
            return New<TPeriodicExecutor>(
                NRpc::TDispatcher::Get()->GetHeavyInvoker(),
                BIND([&] {
                    logDumpString("YTAlloc", NYTAlloc::FormatAllocationCounters());
                }));
        }));
        Executor->SetPeriod(config->YTAllocDumpPeriod);
        Executor->Start();
    }

    if (config->RefCountedTrackerDumpPeriod) {
        static const TLazyIntrusivePtr<TPeriodicExecutor> Executor(BIND([&] {
            return New<TPeriodicExecutor>(
                NRpc::TDispatcher::Get()->GetHeavyInvoker(),
                BIND([&] {
                    logDumpString("RCT", TRefCountedTracker::Get()->GetDebugInfo());
                }));
        }));
        Executor->SetPeriod(config->RefCountedTrackerDumpPeriod);
        Executor->Start();
    }
}

void StartDiagnosticDump(const TDiagnosticDumpConfigPtr& config)
{
    StartDiagnosticDumpImpl(config);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

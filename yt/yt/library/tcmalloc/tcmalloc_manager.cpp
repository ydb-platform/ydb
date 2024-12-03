#include "tcmalloc_manager.h"

#include "config.h"

#include <yt/yt/library/profiling/resource_tracker/resource_tracker.h>

#include <yt/yt/library/oom/oom.h>

#include <library/cpp/yt/memory/leaky_singleton.h>

#include <util/system/thread.h>

#include <tcmalloc/malloc_extension.h>

#include <thread>
#include <mutex>

namespace NYT::NTCMalloc {

////////////////////////////////////////////////////////////////////////////////

static YT_DEFINE_GLOBAL(const NLogging::TLogger, Logger, "TCMalloc");

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

        YT_LOG_INFO("Changing TCMalloc memory limit (Limit: %v, Hard: %v)",
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
        return NProfiling::TResourceTracker::GetAnonymousMemoryLimit();
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

////////////////////////////////////////////////////////////////////////////////

class TTCMallocManagerImpl
{
public:
    static TTCMallocManagerImpl* Get()
    {
        return LeakySingleton<TTCMallocManagerImpl>();
    }

    void Configure(const TTCMallocConfigPtr& config)
    {
        tcmalloc::MallocExtension::SetBackgroundReleaseRate(
            tcmalloc::MallocExtension::BytesPerSecond{static_cast<size_t>(config->BackgroundReleaseRate)});

        tcmalloc::MallocExtension::SetMaxPerCpuCacheSize(config->MaxPerCpuCacheSize);

        if (config->GuardedSamplingRate) {
            tcmalloc::MallocExtension::SetGuardedSamplingRate(*config->GuardedSamplingRate);
            tcmalloc::MallocExtension::ActivateGuardedSampling();
        }

        Config_.Store(config);

        if (tcmalloc::MallocExtension::NeedsProcessBackgroundActions()) {
            std::call_once(InitAggressiveReleaseThread_, [&] {
                std::thread([&] {
                    ::TThread::SetCurrentThreadName("TCMallocBack");

                    TCMallocLimitsAdjuster limitsAdjuster;

                    while (true) {
                        auto config = Config_.Acquire();
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

private:
    DECLARE_LEAKY_SINGLETON_FRIEND();

    TTCMallocManagerImpl() = default;

    TAtomicIntrusivePtr<TTCMallocConfig> Config_;
    std::once_flag InitAggressiveReleaseThread_;
};

////////////////////////////////////////////////////////////////////////////////

void TTCMallocManager::Configure(const TTCMallocConfigPtr& config)
{
    TTCMallocManagerImpl::Get()->Configure(config);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTCMalloc

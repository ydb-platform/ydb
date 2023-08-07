#include "hedging_manager.h"
#include "config.h"

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TAdaptiveHedgingManager
    : public IHedgingManager
{
public:
    TAdaptiveHedgingManager(
        TAdaptiveHedgingManagerConfigPtr config,
        const NProfiling::TProfiler& profiler)
        : Config_(std::move(config))
        , HedgingStatistics_(New<THedgingStatistics>(
            Config_->MaxHedgingDelay))
        , PrimaryRequestCount_(profiler.Counter("/primary_request_count"))
        , BackupAttemptCount_(profiler.Counter("/backup_attempt_count"))
        , BackupRequestCount_(profiler.Counter("/backup_request_count"))
        , HedgingDelay_(profiler.TimeGauge("/hedging_delay"))
    {
        YT_VERIFY(Config_->MaxBackupRequestRatio);
    }

    TDuration OnPrimaryRequestsStarted(int requestCount) override
    {
        auto statistics = AcquireHedgingStatistics();
        statistics->PrimaryRequestCount.fetch_add(requestCount, std::memory_order::relaxed);

        return statistics->HedgingDelay;
    }

    bool OnHedgingDelayPassed(int attemptCount) override
    {
        auto statistics = AcquireHedgingStatistics();

        double previousStatisticsWeight = 1. - (GetInstant() - statistics->StartInstant) / Config_->TickPeriod;
        previousStatisticsWeight = std::max(0., std::min(1., previousStatisticsWeight));

        auto backupRequestCount = statistics->BackupRequestCount.load(std::memory_order::relaxed);
        auto primaryRequestCount = statistics->PrimaryRequestCount.load(std::memory_order::relaxed);

        if (auto previousStatistics = statistics->PreviousStatistics.Acquire()) {
            backupRequestCount += previousStatisticsWeight *
                previousStatistics->BackupRequestCount.load(std::memory_order::relaxed);
            primaryRequestCount += previousStatisticsWeight *
                previousStatistics->PrimaryRequestCount.load(std::memory_order::relaxed);
        }

        statistics->BackupAttemptCount.fetch_add(attemptCount, std::memory_order::relaxed);

        bool hedgingApproved = !IsBackupRequestLimitExceeded(primaryRequestCount, backupRequestCount);
        if (hedgingApproved) {
            statistics->BackupRequestCount.fetch_add(attemptCount, std::memory_order::relaxed);
        }

        return hedgingApproved;
    }

private:
    const TAdaptiveHedgingManagerConfigPtr Config_;

    struct THedgingStatistics;
    using THedgingStatisticsPtr = TIntrusivePtr<THedgingStatistics>;

    struct THedgingStatistics final
    {
        explicit THedgingStatistics(
            TDuration hedgingDelay,
            THedgingStatisticsPtr previousStatistics = nullptr)
            : StartInstant(GetInstant())
            , HedgingDelay(hedgingDelay)
            , PreviousStatistics(std::move(previousStatistics))
        { }

        const TInstant StartInstant;
        const TDuration HedgingDelay;

        std::atomic<i64> PrimaryRequestCount = 0;
        std::atomic<i64> BackupAttemptCount = 0;
        std::atomic<i64> BackupRequestCount = 0;

        TAtomicIntrusivePtr<THedgingStatistics> PreviousStatistics;
    };

    TAtomicIntrusivePtr<THedgingStatistics> HedgingStatistics_;

    NProfiling::TCounter PrimaryRequestCount_;
    NProfiling::TCounter BackupAttemptCount_;
    NProfiling::TCounter BackupRequestCount_;
    NProfiling::TTimeGauge HedgingDelay_;


    THedgingStatisticsPtr TrySwitchStatisticsAndTuneHedgingDelay(
        const THedgingStatisticsPtr& currentStatistics)
    {
        auto newHedgingDelay = currentStatistics->HedgingDelay;
        auto primaryRequestCount = currentStatistics->PrimaryRequestCount.load(std::memory_order::relaxed);
        auto backupAttemptCount = currentStatistics->BackupAttemptCount.load(std::memory_order::relaxed);
        if (IsBackupRequestLimitExceeded(primaryRequestCount, backupAttemptCount)) {
            newHedgingDelay *= Config_->HedgingDelayTuneFactor;
        } else {
            newHedgingDelay /= Config_->HedgingDelayTuneFactor;
        }
        newHedgingDelay = std::max(Config_->MinHedgingDelay, std::min(Config_->MaxHedgingDelay, newHedgingDelay));

        auto newStatistics = New<THedgingStatistics>(newHedgingDelay, currentStatistics);

        void* expectedStatistics = currentStatistics.Get();
        if (!HedgingStatistics_.CompareAndSwap(expectedStatistics, newStatistics)) {
            return HedgingStatistics_.Acquire();
        }

        // NB: Skip profiling in case of very low RPS.
        if (newStatistics->StartInstant - currentStatistics->StartInstant <= 2 * Config_->TickPeriod) {
            PrimaryRequestCount_.Increment(currentStatistics->PrimaryRequestCount.load(std::memory_order::relaxed));
            BackupAttemptCount_.Increment(currentStatistics->BackupAttemptCount.load(std::memory_order::relaxed));
            BackupRequestCount_.Increment(currentStatistics->BackupRequestCount.load(std::memory_order::relaxed));
            HedgingDelay_.Update(currentStatistics->HedgingDelay);
        }

        currentStatistics->PreviousStatistics.Reset();

        return newStatistics;
    }

    THedgingStatisticsPtr AcquireHedgingStatistics()
    {
        auto statistics = HedgingStatistics_.Acquire();

        if (GetInstant() - statistics->StartInstant <= Config_->TickPeriod) {
            return statistics;
        }

        return TrySwitchStatisticsAndTuneHedgingDelay(statistics);
    }

    bool IsBackupRequestLimitExceeded(i64 primaryRequestCount, i64 backupRequestCount) const
    {
        return backupRequestCount >= static_cast<i64>(std::ceil(primaryRequestCount * *Config_->MaxBackupRequestRatio));
    }
};

////////////////////////////////////////////////////////////////////////////////

IHedgingManagerPtr CreateAdaptiveHedgingManager(
    TAdaptiveHedgingManagerConfigPtr config,
    const NProfiling::TProfiler& profiler)
{
    return New<TAdaptiveHedgingManager>(std::move(config), profiler);
}

////////////////////////////////////////////////////////////////////////////////

class TSimpleHedgingManager
    : public IHedgingManager
{
public:
    explicit TSimpleHedgingManager(TDuration hedgingDelay)
        : HedgingDelay_(hedgingDelay)
    { }

    TDuration OnPrimaryRequestsStarted(int /*requestCount*/) override
    {
        return HedgingDelay_;
    }

    bool OnHedgingDelayPassed(int /*attemptCount*/) override
    {
        return true;
    }

private:
    const TDuration HedgingDelay_;
};

////////////////////////////////////////////////////////////////////////////////

IHedgingManagerPtr CreateSimpleHedgingManager(TDuration hedgingDelay)
{
    return New<TSimpleHedgingManager>(hedgingDelay);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

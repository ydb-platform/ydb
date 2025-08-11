#include "overload_controller.h"

#include "config.h"
#include "private.h"

#include <yt/yt/library/profiling/percpu.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/two_level_fair_share_thread_pool.h>

#include <yt/yt/core/logging/log_manager.h>

#include <yt/yt/core/misc/proc.h>

#include <yt/yt/core/profiling/timing.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NRpc {

using namespace NThreading;
using namespace NConcurrency;
using namespace NLogging;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

static auto Logger = RpcServerLogger().WithTag("OverloadController");
static const std::string CpuThrottlingTrackerName = "CpuThrottling";
static const std::string LogDropTrackerName = "LogDrop";
static const std::string ControlGroupCpuName = "cpu";

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TOverloadTracker)

class TOverloadTracker
    : public TRefCounted
{
public:
    TOverloadTracker(TStringBuf type, TStringBuf id)
        : Type_(type)
        , Id_(id)
    { }

    virtual bool CalculateIsOverloaded(const TOverloadTrackerConfig& config) = 0;

    virtual EOverloadTrackerConfigType GetConfigType() const = 0;

    const std::string& GetType() const
    {
        return Type_;
    }

protected:
    const std::string Type_;
    const std::string Id_;

    TCounter Overloaded_;
};

DEFINE_REFCOUNTED_TYPE(TOverloadTracker);

////////////////////////////////////////////////////////////////////////////////

class TMeanWaitTimeTracker
    : public TOverloadTracker
{
public:
    TMeanWaitTimeTracker(TStringBuf type, TStringBuf id, const TProfiler& profiler)
        : TOverloadTracker(type, id)
        , Counter_(New<TPerCpuDurationSummary>())
    {
        auto taggedProfiler = profiler.WithTag("tracker", std::string(Id_));

        Overloaded_ = taggedProfiler.Counter("/overloaded");
        MeanWaitTime_ = profiler.Timer("/mean_wait_time");
        MeanWaitTimeThreshold_ = profiler.TimeGauge("/mean_wait_time_threshold");
    }

    void Record(TDuration waitTime)
    {
        Counter_->Record(waitTime);
    }

    virtual bool CalculateIsOverloaded(const TOverloadTrackerConfig& config) override
    {
        auto summary = Counter_->GetSummaryAndReset();
        TDuration meanValue;

        if (summary.Count()) {
            meanValue = summary.Sum() / summary.Count();
        }

        YT_LOG_DEBUG("Reporting mean wait time for tracker "
            "(Tracker: %v, TotalWaitTime: %v, TotalCount: %v, MeanValue: %v)",
            Id_,
            summary.Sum(),
            summary.Count(),
            meanValue);

        auto meanWaitTimeConfig = config.TryGetConcrete<TOverloadTrackerMeanWaitTimeConfig>();
        YT_VERIFY(meanWaitTimeConfig);

        return ProfileAndGetOverloaded(meanValue, meanWaitTimeConfig);
    }

    virtual EOverloadTrackerConfigType GetConfigType() const override
    {
        return EOverloadTrackerConfigType::MeanWaitTime;
    }

protected:
    bool ProfileAndGetOverloaded(TDuration meanValue, const TOverloadTrackerMeanWaitTimeConfigPtr& config)
    {
        bool overloaded = meanValue > config->MeanWaitTimeThreshold;

        Overloaded_.Increment(static_cast<int>(overloaded));
        MeanWaitTime_.Record(meanValue);
        MeanWaitTimeThreshold_.Update(config->MeanWaitTimeThreshold);

        return overloaded;
    }

private:
    using TPerCpuDurationSummary = TPerCpuSummary<TDuration>;

    TIntrusivePtr<TPerCpuDurationSummary> Counter_;

    TEventTimer MeanWaitTime_;
    TTimeGauge MeanWaitTimeThreshold_;
};

DEFINE_REFCOUNTED_TYPE(TMeanWaitTimeTracker);

////////////////////////////////////////////////////////////////////////////////

class TContainerCpuThrottlingTracker
    : public TMeanWaitTimeTracker
{
public:
    using TMeanWaitTimeTracker::TMeanWaitTimeTracker;

    bool CalculateIsOverloaded(const TOverloadTrackerConfig& config) override
    {
        auto cpuStats = GetStats();
        if (!cpuStats) {
            return {};
        }

        TDuration throttlingTime;

        if (LastCpuStats_) {
            auto throttlingDelta = cpuStats->ThrottledTime - LastCpuStats_->ThrottledTime;
            throttlingTime = TDuration::MicroSeconds(throttlingDelta / 1000);

            YT_LOG_DEBUG("Reporting container CPU throttling time "
                "(LastCpuThrottlingTime: %v, CpuThrottlingTime: %v, ThrottlingDelta: %v, ThrottlingTime: %v)",
                LastCpuStats_->ThrottledTime,
                cpuStats->ThrottledTime,
                throttlingDelta,
                throttlingTime);
        }

        LastCpuStats_ = cpuStats;

        auto meanWaitTimeConfig = config.TryGetConcrete<TOverloadTrackerMeanWaitTimeConfig>();
        YT_VERIFY(meanWaitTimeConfig);

        return ProfileAndGetOverloaded(throttlingTime, meanWaitTimeConfig);
    }

private:
    std::optional<TCgroupCpuStat> LastCpuStats_;
    bool CgroupErrorLogged_ = false;

    std::optional<TCgroupCpuStat> GetStats()
    {
        try {
            auto cgroups = GetProcessCgroups();
            for (const auto& group : cgroups) {
                for (const auto& controller : group.Controllers) {
                    if (controller == ControlGroupCpuName) {
                        return GetCgroupCpuStat(group.ControllersName, group.Path);
                    }
                }
            }
        } catch (const std::exception& ex) {
            if (!CgroupErrorLogged_) {
                YT_LOG_INFO(ex, "Failed to collect cgroup CPU statistics");
                CgroupErrorLogged_ = true;
            }
        }

        return {};
    }
};

DEFINE_REFCOUNTED_TYPE(TContainerCpuThrottlingTracker);

////////////////////////////////////////////////////////////////////////////////

class TLogDropTracker
    : public TOverloadTracker
{
public:
    TLogDropTracker(TStringBuf type, TStringBuf id, const TProfiler& profiler)
        : TOverloadTracker(type, id)
    {
        auto taggedProfiler = profiler.WithTag("tracker", Id_);

        Overloaded_ = taggedProfiler.Counter("/overloaded");
        BacklogQueueFillFraction_ = profiler.Gauge("/backlog_queue_fill_fraction");
        BacklogQueueFillFractionThreshold_ = profiler.Gauge("/backlog_queue_fill_fraction_threshold");
    }

    bool CalculateIsOverloaded(const TOverloadTrackerConfig& config) override
    {
        double BacklogQueueFillFraction = TLogManager::Get()->GetBacklogQueueFillFraction();

        YT_LOG_DEBUG("Reporting logging queue filling fraction "
            "(BacklogQueueFillFraction: %v)",
            BacklogQueueFillFraction);

        auto logDropConfig = config.TryGetConcrete<TOverloadTrackerBacklogQueueFillFractionConfig>();
        YT_VERIFY(logDropConfig);

        bool overloaded = BacklogQueueFillFraction > logDropConfig->BacklogQueueFillFractionThreshold;

        Overloaded_.Increment(static_cast<int>(overloaded));
        BacklogQueueFillFraction_.Update(BacklogQueueFillFraction);
        BacklogQueueFillFractionThreshold_.Update(logDropConfig->BacklogQueueFillFractionThreshold);

        return overloaded;
    }

    EOverloadTrackerConfigType GetConfigType() const override
    {
        return EOverloadTrackerConfigType::BacklogQueueFillFraction;
    }

private:
    TGauge BacklogQueueFillFraction_;
    TGauge BacklogQueueFillFractionThreshold_;
};

DEFINE_REFCOUNTED_TYPE(TLogDropTracker);

////////////////////////////////////////////////////////////////////////////////

bool ShouldThrottleCall(const TCongestionState& congestionState)
{
    if (!congestionState.MaxWindow || !congestionState.CurrentWindow) {
        return false;
    }

    return static_cast<int>(RandomNumber<ui32>(*congestionState.MaxWindow)) + 1 > *congestionState.CurrentWindow;
}

////////////////////////////////////////////////////////////////////////////////

class TCongestionController
    : public TRefCounted
{
public:
    TCongestionController(TServiceMethodConfigPtr methodConfig, TOverloadControllerConfigPtr config, TProfiler profiler)
        : MethodConfig_(std::move(methodConfig))
        , Config_(std::move(config))
        , MaxWindow_(MethodConfig_->MaxWindow)
        , Window_(MaxWindow_)
        , WindowGauge_(profiler.Gauge("/window"))
        , SlowStartThresholdGauge_(profiler.Gauge("/slow_start_threshold_gauge"))
        , MaxWindowGauge_(profiler.Gauge("/max_window"))
    { }

    TCongestionState GetCongestionState()
    {
        auto result = TCongestionState{
            .CurrentWindow = Window_.load(std::memory_order::relaxed),
            .MaxWindow = MaxWindow_,
            .WaitingTimeoutFraction = MethodConfig_->WaitingTimeoutFraction,
        };

        auto now = NProfiling::GetCpuInstant();
        auto recentlyOverloadedThreshold = Config_->LoadAdjustingPeriod * MethodConfig_->MaxWindow;

        for (const auto& [trackerType, lastOverloaded] : OverloadedTrackers_) {
            auto sinceLastOverloaded = CpuDurationToDuration(now - lastOverloaded.load(std::memory_order::relaxed));
            if (sinceLastOverloaded < recentlyOverloadedThreshold) {
                result.OverloadedTrackers.push_back(trackerType);
            }
        }

        return result;
    }

    void Adjust(const THashSet<std::string>& overloadedTrackers, TCpuInstant timestamp)
    {
        auto window = Window_.load(std::memory_order::relaxed);

        // NB. We reporting here slightly outdated values but this makes code simpler a bit.
        WindowGauge_.Update(window);
        MaxWindowGauge_.Update(MaxWindow_);
        SlowStartThresholdGauge_.Update(SlowStartThreshold_);

        for (const auto& tracker : overloadedTrackers) {
            auto it = OverloadedTrackers_.find(tracker);
            if (it != OverloadedTrackers_.end()) {
                it->second.store(timestamp, std::memory_order::relaxed);
            }
        }

        auto overloaded = !overloadedTrackers.empty();

        if (overloaded) {
            SlowStartThreshold_ = window > 0 ? window / 2 : SlowStartThreshold_;
            Window_.store(0, std::memory_order::relaxed);

            YT_LOG_WARNING("System is overloaded (SlowStartThreshold: %v, Window: %v, OverloadedTrackers: %v)",
                SlowStartThreshold_,
                window,
                overloadedTrackers);
            return;
        }

        if (window >= SlowStartThreshold_) {
            ++window;
        } else {
            window *= 2;
            window = std::min(SlowStartThreshold_, window);
        }

        // Keeping window in sane limits.
        window = std::min(MaxWindow_, window);
        window = std::max(1, window);

        YT_LOG_DEBUG("Adjusting system load up (SlowStartThreshold: %v, CurrentWindow: %v)",
            SlowStartThreshold_,
            window);

        Window_.store(window, std::memory_order::relaxed);
    }

    void AddTrackerType(std::string trackerType)
    {
        // Called only during initial construction of controllers,
        // so we do not have to serialize here.
        OverloadedTrackers_[trackerType] = {};
    }

private:
    const TServiceMethodConfigPtr MethodConfig_;
    const TOverloadControllerConfigPtr Config_;
    const int MaxWindow_;

    std::atomic<int> Window_;
    int SlowStartThreshold_ = 0;
    THashMap<std::string, std::atomic<TCpuInstant>> OverloadedTrackers_;

    TCounter SkippedRequestCount_;
    TGauge WindowGauge_;
    TGauge SlowStartThresholdGauge_;
    TGauge MaxWindowGauge_;
};

DEFINE_REFCOUNTED_TYPE(TCongestionController);

////////////////////////////////////////////////////////////////////////////////

class TOverloadController
    : public IOverloadController
{
public:
    DEFINE_SIGNAL_OVERRIDE(void(), LoadAdjusted);

    TOverloadController(TOverloadControllerConfigPtr config, NProfiling::TProfiler profiler)
        : ControlThread_(New<TActionQueue>("OverloadCtl"))
        , Invoker_(ControlThread_->GetInvoker())
        , Periodic_(New<TPeriodicExecutor>(
            Invoker_,
            BIND(&TOverloadController::Adjust, MakeWeak(this)),
            config->LoadAdjustingPeriod))
        , Profiler_(std::move(profiler))
    {
        State_.Config = std::move(config);
        CreateGenericTracker<TContainerCpuThrottlingTracker>(CpuThrottlingTrackerName);
        CreateGenericTracker<TLogDropTracker>(LogDropTrackerName);
        UpdateStateSnapshot(State_, Guard(SpinLock_));
    }

    void Start() override
    {
        Periodic_->Start();
    }

    TFuture<void> Stop() override
    {
        return Periodic_->Stop();
    }

    void TrackInvoker(
        TStringBuf name,
        const IInvokerPtr& invoker) override
    {
        invoker->SubscribeWaitTimeObserved(CreateGenericWaitTimeObserver(name));
    }

    void TrackFSHThreadPool(
        TStringBuf name,
        const NConcurrency::ITwoLevelFairShareThreadPoolPtr& threadPool) override
    {
        threadPool->SubscribeWaitTimeObserved(CreateGenericWaitTimeObserver(name));
    }

    IInvoker::TWaitTimeObserver CreateGenericWaitTimeObserver(
        TStringBuf trackerType,
        std::optional<TStringBuf> id = {}) override
    {
        auto tracker = CreateGenericTracker<TMeanWaitTimeTracker>(std::move(trackerType), std::move(id));

        return BIND([tracker] (TDuration waitTime) {
            tracker->Record(waitTime);
        });
    }

    TCongestionState GetCongestionState(TStringBuf service, TStringBuf method) const override
    {
        auto snapshot = GetStateSnapshot();
        if (!snapshot->Config->Enabled) {
            return {};
        }

        const auto& controllers = snapshot->CongestionControllers;
        if (auto it = controllers.find(std::pair(service, method)); it != controllers.end()) {
            return it->second->GetCongestionState();
        }

        return {};
    }

    void Reconfigure(TOverloadControllerConfigPtr config) override
    {
        Periodic_->SetPeriod(config->LoadAdjustingPeriod);

        auto guard = Guard(SpinLock_);
        State_.CongestionControllers = CreateCongestionControllers(config, Profiler_);
        State_.Config = std::move(config);

        UpdateStateSnapshot(State_, std::move(guard));
    }

private:
    using TMethodIndex = std::pair<TString, TString>;
    using TMethodsCongestionControllers = THashMap<TMethodIndex, TCongestionControllerPtr>;

    struct TState final
    {
        static constexpr bool EnableHazard = true;

        TOverloadControllerConfigPtr Config;
        TMethodsCongestionControllers CongestionControllers;
        THashMap<TString, TOverloadTrackerPtr> Trackers;
    };

    using TSpinLockGuard = TGuard<NThreading::TSpinLock>;

    const NConcurrency::TActionQueuePtr ControlThread_;
    const IInvokerPtr Invoker_;
    const NConcurrency::TPeriodicExecutorPtr Periodic_;
    const NProfiling::TProfiler Profiler_;

    TAtomicPtr<TState, /*EnableAcquireHazard*/ true> StateSnapshot_;

    NThreading::TSpinLock SpinLock_;
    TState State_;

    void Adjust()
    {
        DoAdjust(GetStateSnapshot());
        LoadAdjusted_.Fire();
    }

    void DoAdjust(const THazardPtr<TState>& state)
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        auto now = NProfiling::GetCpuInstant();

        using TOverloadedTrackers = THashSet<std::string>;
        THashMap<TMethodIndex, TOverloadedTrackers> methodOverloaded;

        const auto& config = state->Config;

        for (const auto& [_, trackerConfig] : config->Trackers) {
            for (const auto& method : trackerConfig->MethodsToThrottle) {
                methodOverloaded[std::pair(method.Service, method.Method)] = {};
            }
        }

        for (const auto& [trackerId, tracker] : state->Trackers) {
            auto trackerIt = config->Trackers.find(tracker->GetType());
            if (trackerIt == config->Trackers.end()) {
                continue;
            }

            const auto& trackerConfig = trackerIt->second;

            if (trackerConfig.GetCurrentType() != tracker->GetConfigType()) {
                YT_LOG_ERROR("Incorrect overload controller tracker config type (ExpectedType: %v, ActualType: %v)",
                    tracker->GetConfigType(),
                    trackerConfig.GetCurrentType());
                continue;
            }

            auto trackerOverloaded = tracker->CalculateIsOverloaded(trackerConfig);

            if (!trackerOverloaded) {
                continue;
            }

            for (const auto& method : trackerIt->second->MethodsToThrottle) {
                auto& overloadedTrackers = methodOverloaded[std::pair(method.Service, method.Method)];
                overloadedTrackers.insert(tracker->GetType());
            }
        }

        for (const auto& [method, overloadedTrackers] : methodOverloaded) {
            auto it = state->CongestionControllers.find(method);
            if (it == state->CongestionControllers.end()) {
                YT_LOG_WARNING("Cannot find congestion controller for method (Service: %v, Method: %v)",
                    method.first,
                    method.second);

                continue;
            }

            it->second->Adjust(overloadedTrackers, now);
        }
    }

    THazardPtr<TOverloadController::TState> GetStateSnapshot() const
    {
        YT_VERIFY(StateSnapshot_);

        return StateSnapshot_.AcquireHazard();
    }

    void UpdateStateSnapshot(const TState& state, TSpinLockGuard guard)
    {
        auto snapshot = New<TState>(state);
        guard.Release();

        StateSnapshot_.Store(std::move(snapshot));
        ReclaimHazardPointers();
    }

    template <class TTracker>
    TIntrusivePtr<TTracker> CreateGenericTracker(TStringBuf trackerType, std::optional<TStringBuf> id = {})
    {
        YT_LOG_DEBUG("Creating overload tracker (TrackerType: %v, Id: %v)",
            trackerType,
            id);

        auto trackerId = id.value_or(trackerType);

        auto profiler = Profiler_.WithTag("tracker", std::string(trackerId));

        auto tracker = New<TTracker>(trackerType, trackerId, profiler);

        auto guard = Guard(SpinLock_);

        State_.Trackers[trackerId] = tracker;

        UpdateStateSnapshot(State_, std::move(guard));

        return tracker;
    }

    static TMethodsCongestionControllers CreateCongestionControllers(
        const TOverloadControllerConfigPtr& config,
        NProfiling::TProfiler profiler)
    {
        TMethodsCongestionControllers controllers;

        THashMap<TMethodIndex, TServiceMethodConfigPtr> configIndex;
        for (const auto& methodConfig : config->Methods) {
            configIndex[std::pair(methodConfig->Service, methodConfig->Method)] = methodConfig;
        }

        auto getConfig = [&configIndex] (TStringBuf service, TStringBuf method) {
            auto it = configIndex.find(std::pair(service, method));
            if (it != configIndex.end()) {
                return it->second;
            }

            auto defaultConfig = New<TServiceMethodConfig>();
            defaultConfig->Service = service;
            defaultConfig->Method = method;
            return defaultConfig;
        };

        for (const auto& [trackerType, tracker] : config->Trackers) {
            for (const auto& method : tracker->MethodsToThrottle) {
                auto& controller = controllers[std::pair(method.Service, method.Method)];

                if (!controller) {
                    auto methodConfig = getConfig(method.Service, method.Method);
                    auto controllerProfiler = profiler
                        .WithTag("yt_service", method.Service)
                        .WithTag("method", method.Method);

                    controller = New<TCongestionController>(
                        std::move(methodConfig),
                        config, std::move(controllerProfiler));
                }

                controller->AddTrackerType(trackerType);
            }
        }

        return controllers;
    }
};

IOverloadControllerPtr CreateOverloadController(TOverloadControllerConfigPtr config, NProfiling::TProfiler profiler)
{
    return New<TOverloadController>(std::move(config), std::move(profiler));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc

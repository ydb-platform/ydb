#include "public.h"
#include "instance_limits_tracker.h"
#include "instance.h"
#include "porto_resource_tracker.h"
#include "private.h"

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/ypath_service.h>

namespace NYT::NContainers {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ContainersLogger;

////////////////////////////////////////////////////////////////////////////////

TInstanceLimitsTracker::TInstanceLimitsTracker(
    IInstancePtr instance,
    IInstancePtr root,
    IInvokerPtr invoker,
    TDuration updatePeriod)
    : Invoker_(std::move(invoker))
    , Executor_(New<NConcurrency::TPeriodicExecutor>(
        Invoker_,
        BIND(&TInstanceLimitsTracker::DoUpdateLimits, MakeWeak(this)),
        updatePeriod))
{
#ifdef _linux_
    SelfTracker_ = New<TPortoResourceTracker>(std::move(instance), updatePeriod / 2);
    RootTracker_ = New<TPortoResourceTracker>(std::move(root), updatePeriod / 2);
#else
    Y_UNUSED(instance);
    Y_UNUSED(root);
#endif
}

void TInstanceLimitsTracker::Start()
{
    if (!Running_) {
        Executor_->Start();
        Running_ = true;
        YT_LOG_INFO("Instance limits tracker started");
    }
}

void TInstanceLimitsTracker::Stop()
{
    if (Running_) {
        YT_UNUSED_FUTURE(Executor_->Stop());
        Running_ = false;
        YT_LOG_INFO("Instance limits tracker stopped");
    }
}

void TInstanceLimitsTracker::DoUpdateLimits()
{
    VERIFY_INVOKER_AFFINITY(Invoker_);

#ifdef _linux_
    YT_LOG_DEBUG("Checking for instance limits update");

    auto setIfOk = [] (auto* destination, const auto& valueOrError, const TString& fieldName, bool alert = true) {
        if (valueOrError.IsOK()) {
            *destination = valueOrError.Value();
        } else {
            YT_LOG_ALERT_IF(alert, valueOrError, "Failed to get container limit (Field: %v)",
                fieldName);

            YT_LOG_DEBUG(valueOrError, "Failed to get container limit (Field: %v)",
                fieldName);
        }
    };

    try {
        auto memoryStatistics = SelfTracker_->GetMemoryStatistics();
        auto netStatistics = RootTracker_->GetNetworkStatistics();
        auto cpuStatistics = SelfTracker_->GetCpuStatistics();

        setIfOk(&MemoryUsage_, memoryStatistics.Rss, "MemoryRss");

        TDuration cpuGuarantee;
        TDuration cpuLimit;
        setIfOk(&cpuGuarantee, cpuStatistics.GuaranteeTime, "CpuGuarantee");
        setIfOk(&cpuLimit, cpuStatistics.LimitTime, "CpuLimit");

        if (CpuGuarantee_ != cpuGuarantee) {
            YT_LOG_INFO("Instance CPU guarantee updated (OldCpuGuarantee: %v, NewCpuGuarantee: %v)",
                CpuGuarantee_,
                cpuGuarantee);
            CpuGuarantee_ = cpuGuarantee;
            // NB: We do not fire LimitsUpdated since this value used only for diagnostics.
        }

        TInstanceLimits limits;
        limits.Cpu = cpuLimit.SecondsFloat();

        if (memoryStatistics.AnonLimit.IsOK() && memoryStatistics.MemoryLimit.IsOK()) {
            i64 anonLimit = memoryStatistics.AnonLimit.Value();
            i64 memoryLimit = memoryStatistics.MemoryLimit.Value();

            if (anonLimit > 0 && memoryLimit > 0) {
                limits.Memory = std::min(anonLimit, memoryLimit);
            } else if (anonLimit > 0) {
                limits.Memory = anonLimit;
            } else {
                limits.Memory = memoryLimit;
            }
        } else {
            setIfOk(&limits.Memory, memoryStatistics.MemoryLimit, "MemoryLimit");
        }

        static constexpr bool DontFireAlertOnError = {};
        setIfOk(&limits.NetTx, netStatistics.TxLimit, "NetTxLimit", DontFireAlertOnError);
        setIfOk(&limits.NetRx, netStatistics.RxLimit, "NetRxLimit", DontFireAlertOnError);

        if (InstanceLimits_ != limits) {
            YT_LOG_INFO("Instance limits updated (OldLimits: %v, NewLimits: %v)",
                InstanceLimits_,
                limits);
            InstanceLimits_ = limits;
            LimitsUpdated_.Fire(limits);
        }
    } catch (const std::exception& ex) {
        YT_LOG_WARNING(ex, "Failed to get instance limits");
    }
#endif
}

IYPathServicePtr TInstanceLimitsTracker::GetOrchidService()
{
    return IYPathService::FromProducer(BIND(&TInstanceLimitsTracker::DoBuildOrchid, MakeStrong(this)))
        ->Via(Invoker_);
}

void TInstanceLimitsTracker::DoBuildOrchid(NYson::IYsonConsumer* consumer) const
{
    NYTree::BuildYsonFluently(consumer)
        .BeginMap()
            .DoIf(static_cast<bool>(InstanceLimits_), [&] (auto fluent) {
                fluent.Item("cpu_limit").Value(InstanceLimits_->Cpu);
            })
            .DoIf(static_cast<bool>(CpuGuarantee_), [&] (auto fluent) {
                fluent.Item("cpu_guarantee").Value(*CpuGuarantee_);
            })
            .DoIf(static_cast<bool>(InstanceLimits_), [&] (auto fluent) {
                fluent.Item("memory_limit").Value(InstanceLimits_->Memory);
            })
            .DoIf(static_cast<bool>(MemoryUsage_), [&] (auto fluent) {
                fluent.Item("memory_usage").Value(*MemoryUsage_);
            })
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TInstanceLimits& limits, TStringBuf /*format*/)
{
    builder->AppendFormat(
        "{Cpu: %v, Memory: %v, NetTx: %v, NetRx: %v}",
        limits.Cpu,
        limits.Memory,
        limits.NetTx,
        limits.NetRx);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainters

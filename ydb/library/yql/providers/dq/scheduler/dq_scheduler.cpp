#include "dq_scheduler.h"

#include <queue>
#include <list>
#include <unordered_map>

#include <ydb/library/yql/utils/log/log.h>

namespace NYql::NDq {

IScheduler::TWaitInfo::TWaitInfo(const NYql::NDqProto::TAllocateWorkersRequest& record, const NActors::TActorId& sender)
    : Request(record), Sender(sender), StartTime(TInstant::Now())
{ }

namespace {

struct TMyCounters {
    using TPtr = std::unique_ptr<TMyCounters>;

    const TSensorsGroupPtr Group;
    const NMonitoring::TDynamicCounters::TCounterPtr QueueSizeForSmall;
    const NMonitoring::TDynamicCounters::TCounterPtr QueueSizeForLarge;
    const NMonitoring::TDynamicCounters::TCounterPtr IntegralQueueSizeForLarge;
    const NMonitoring::TDynamicCounters::TCounterPtr AllocatedTotal;
    const NMonitoring::TDynamicCounters::TCounterPtr KnownUsers;
    const NMonitoring::TDynamicCounters::TCounterPtr UserLimited;

    TMyCounters(TSensorsGroupPtr&& group)
        : Group(std::move(group))
        , QueueSizeForSmall(Group->GetCounter("QueueSizeForSmall"))
        , QueueSizeForLarge(Group->GetCounter("QueueSizeForLarge"))
        , IntegralQueueSizeForLarge(Group->GetCounter("IntegralQueueSizeForLarge"))
        , AllocatedTotal(Group->GetCounter("AllocatedTotal"))
        , KnownUsers(Group->GetCounter("KnownUsers"))
        , UserLimited(Group->GetCounter("UserLimited"))
    {}

    static TPtr Make(IMetricsRegistryPtr metricsRegistry) {
        return metricsRegistry ? std::make_unique<TMyCounters>(metricsRegistry->GetSensors()->GetSubgroup("component", "scheduler")) : nullptr;
    }
};

class TScheduler : public IScheduler {
public:
    TScheduler(IMetricsRegistryPtr metricsRegistry, const NProto::TDqConfig::TScheduler& config)
        : KeepReserveForLiteralRequests(config.GetKeepReserveForLiteralRequests())
        , HistoryKeepingTime(TDuration::Minutes(config.GetHistoryKeepingTime()))
        , MaxOperations(config.GetMaxOperations())
        , MaxOperationsPerUser(config.GetMaxOperationsPerUser())
        , Counters(TMyCounters::Make(metricsRegistry))
        , EnableLimiter(config.GetLimitTasksPerWindow())
        , LimiterNumerator(config.GetLimiterNumerator())
        , LimiterDenumerator(config.GetLimiterDenumerator())
    {}

private:
    struct TUserInfo {
        ui64 Await = 0ULL;
        ui64 Allocated = 0ULL;
        ui64 AwaitOperations = 0LL;
        std::queue<std::pair<TInstant, ui32>> History;
        struct {
            NMonitoring::TDynamicCounters::TCounterPtr Await, AwaitOperations, Allocated;
        } Counters;
    };

    using THistoryMap = std::unordered_map<TString, TUserInfo>;

    struct TFullWaitInfo : public TWaitInfo {
        TFullWaitInfo(TWaitInfo&& info, THistoryMap::value_type* userInfo)
            : TWaitInfo(std::move(info)), UserInfo(userInfo)
        {}

        THistoryMap::value_type* const UserInfo;
    };

     bool Suspend(TWaitInfo&& info) final {
        const auto ins = AllocationsHistory.emplace(info.Request.GetUser(), TUserInfo());
        auto& userInfo = *ins.first;

        if (info.Request.GetCount() > 1U) {
            if (userInfo.second.AwaitOperations >= MaxOperationsPerUser) {
                return false;
            }
            if (LargeWaitList.size() >= MaxOperations) {
                return false;
            }
        }

        userInfo.second.Await += info.Request.GetCount();
        userInfo.second.AwaitOperations += 1;
        if (ins.second && Counters) {
            const auto group = Counters->Group->GetSubgroup("user", info.Request.GetUser());
            userInfo.second.Counters.Await = group->GetCounter("Await");
            userInfo.second.Counters.AwaitOperations = group->GetCounter("AwaitOperations");
            userInfo.second.Counters.Allocated = group->GetCounter("Allocated");
        }
        (info.Request.GetCount() > 1U ? LargeWaitList : SmallWaitList).emplace_back(std::move(info), &userInfo);
        return true;
    }

    std::vector<NActors::TActorId> Cleanup() final {
        std::vector<NActors::TActorId> senders;
        senders.reserve(SmallWaitList.size() + LargeWaitList.size());
        std::transform(SmallWaitList.cbegin(), SmallWaitList.cend(), std::back_inserter(senders), [](const TWaitInfo& info) { return info.Sender; });
        std::transform(LargeWaitList.cbegin(), LargeWaitList.cend(), std::back_inserter(senders), [](const TWaitInfo& info) { return info.Sender; });
        SmallWaitList.clear();
        LargeWaitList.clear();

        if (Counters) {
            for (auto it = AllocationsHistory.cbegin(); AllocationsHistory.cend() != it; ++it) {
                *it->second.Counters.Await = 0;
                *it->second.Counters.AwaitOperations = 0;
                *it->second.Counters.Allocated = 0;
            }
        }

        AllocationsHistory.clear();
        return senders;
    }

    size_t UpdateMetrics() final {
        if (Counters) {
            auto allocated = 0ULL;
            for (auto it = AllocationsHistory.cbegin(); AllocationsHistory.cend() != it; ++it) {
                *it->second.Counters.Await = it->second.Await;
                *it->second.Counters.AwaitOperations = it->second.AwaitOperations;
                *it->second.Counters.Allocated = it->second.Allocated;
                allocated += it->second.Allocated;
            }

            *Counters->KnownUsers = AllocationsHistory.size();
            *Counters->AllocatedTotal = allocated;
            *Counters->QueueSizeForSmall = SmallWaitList.size();
            *Counters->QueueSizeForLarge = LargeWaitList.size();
            *Counters->IntegralQueueSizeForLarge = std::accumulate(LargeWaitList.cbegin(), LargeWaitList.cend(), 0ULL,
                [] (ui64 c, const TFullWaitInfo& info) { return c += info.Request.GetCount(); }
            );
        }

        return SmallWaitList.size() + LargeWaitList.size();
    }

    void Process(size_t total, size_t count, const TProcessor& processor, const TInstant& now) final {
        const auto from = now - HistoryKeepingTime;
        for (auto& info : AllocationsHistory) {
            for (auto& history = info.second.History; !history.empty() && history.front().first <= from; history.pop())
                info.second.Allocated -= history.front().second;
        };

        const auto sort = [](const TFullWaitInfo& lhs, const TFullWaitInfo& rhs) {
            return lhs.UserInfo->second.Allocated < rhs.UserInfo->second.Allocated;
        };

        SmallWaitList.sort(sort);
        LargeWaitList.sort(sort);

        const auto work = [&processor, &now, &total, this] (size_t& quota, std::list<TFullWaitInfo>& list) {
            list.remove_if([&](const TFullWaitInfo& info) {
                const auto count = info.Request.GetCount();
                if (quota < count)
                    return false;

                if (EnableLimiter && (info.UserInfo->second.Allocated+count) > LimiterNumerator * total / LimiterDenumerator) {
                    if (Counters) {
                        *Counters->UserLimited += 1;
                    }
                    return false;
                }

                if (processor(info)) {
                    info.UserInfo->second.Await -= count;
                    info.UserInfo->second.AwaitOperations -= 1;
                    info.UserInfo->second.Allocated += count;
                    info.UserInfo->second.History.emplace(now, count);
                    quota -= count;
                    return true;
                }
                return false;
            });
        };

        const auto full = count;
        const auto half = full >> 1U;

        if (count -= half)
            work(count, SmallWaitList);

        if (KeepReserveForLiteralRequests)
            count = SmallWaitList.empty() && count + half >= total >> 2U ? std::min(count + half, full - (half >> 1U)) : half;
        else
            count += half;

        if (count > 1U)
            work(count, LargeWaitList);

        if (count && LargeWaitList.empty())
            work(count, SmallWaitList);
    }

    void ProcessAll(const TProcessor& processor) final {
        const auto proc = [&processor](const TFullWaitInfo& info) {
            if (processor(info)) {
                info.UserInfo->second.Await -= info.Request.GetCount();
                return true;
            }
            return false;
        };

        SmallWaitList.remove_if(proc);
        LargeWaitList.remove_if(proc);
    }

    void ForEach(const std::function<void(const TWaitInfo& info)>& processor) final {
        std::for_each(SmallWaitList.cbegin(), SmallWaitList.cend(), processor);
        std::for_each(LargeWaitList.cbegin(), LargeWaitList.cend(), processor);
    }

    const bool KeepReserveForLiteralRequests;
    const TDuration HistoryKeepingTime;
    const size_t MaxOperations;
    const size_t MaxOperationsPerUser;

    const TMyCounters::TPtr Counters;

    THistoryMap AllocationsHistory;
    std::list<TFullWaitInfo> SmallWaitList, LargeWaitList;

    bool EnableLimiter;
    ui32 LimiterNumerator;
    ui32 LimiterDenumerator;
};

}

IScheduler::TPtr IScheduler::Make(const NProto::TDqConfig::TScheduler& config, IMetricsRegistryPtr metricsRegistry) {
    return std::make_unique<TScheduler>(metricsRegistry, config);
}

}

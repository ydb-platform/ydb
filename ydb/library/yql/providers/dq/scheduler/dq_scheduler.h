#pragma once

#include <ydb/library/actors/core/events.h>

#include <ydb/library/yql/providers/dq/counters/counters.h>
#include <ydb/library/yql/providers/dq/api/protos/dqs.pb.h>
#include <ydb/library/yql/dq/proto/dq_tasks.pb.h>
#include <ydb/library/yql/providers/dq/config/config.pb.h>
#include <ydb/library/yql/providers/common/metrics/metrics_registry.h>

namespace NYql::NDq {

class IScheduler {
public:
    using TPtr = std::unique_ptr<IScheduler>;

    static TPtr Make(const NProto::TDqConfig::TScheduler& schedulerConfig = {}, IMetricsRegistryPtr metricsRegistry = {});

    virtual ~IScheduler() = default;

    struct TWaitInfo {
        const NYql::NDqProto::TAllocateWorkersRequest Request;
        const NActors::TActorId Sender;
        const TInstant StartTime;

        TCounters Stat;
        mutable THashMap<TString, int> ResLeft;

        TWaitInfo(const NYql::NDqProto::TAllocateWorkersRequest& record, const NActors::TActorId& sender);
    };

    virtual bool Suspend(TWaitInfo&& info) = 0;

    virtual std::vector<NActors::TActorId> Cleanup() = 0;

    virtual size_t UpdateMetrics() = 0;

    using TProcessor = std::function<bool(const TWaitInfo& info)>;

    virtual void Process(size_t totalWorkersCount, size_t freeWorkersCount, const TProcessor& processor, const TInstant& timestamp = TInstant::Now()) = 0;

    virtual void ProcessAll(const TProcessor& processor) = 0;

    virtual void ForEach(const std::function<void(const TWaitInfo& info)>& processor) = 0;
};

}

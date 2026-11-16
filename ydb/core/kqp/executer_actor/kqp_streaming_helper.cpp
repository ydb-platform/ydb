#include "kqp_streaming_helper.h"

#include <ydb/library/yql/providers/pq/proto/dq_io.pb.h>

namespace NKikimr::NKqp {

namespace {

class TStreamingQueryCounters : public IStreamingQueryCounters {
public:
    TStreamingQueryCounters(const ::NMonitoring::TDynamicCounterPtr& counters, const TString& path)
        : Path(path)
        , Counters(counters)
    {}

    ~TStreamingQueryCounters() {
        if (!HostGroup) {
            return;
        }
        HostGroup->RemoveSubgroup("path", Path);
    }

    void Update(const TAggExecStat& stats) override {
        if (!SubGroup) {
            Init();
        }
        CpuMs->Set(stats.CpuTimeMs);
        MemoryUsageBytes->Set(stats.MemoryUsageBytes);
        UptimeSeconds->Set(stats.DurationSeconds);
        TaskCount->Set(stats.TasksCount);
        InputBytes->Set(stats.InputBytes);
        OutputBytes->Set(stats.OutputBytes);
        if (stats.MaxReadLagMs) {
            if (!MaxReadLag) {
                MaxReadLag = QueryGroup->GetCounter("max_read_lag");
            }
            MaxReadLag->Set(*stats.MaxReadLagMs);
        } else if (MaxReadLag) {
            QueryGroup->RemoveCounter("max_read_lag");
            MaxReadLag.Reset();
        }
    }

private:
    void Init() {
        SubGroup = Counters->GetSubgroup("subsystem", "streaming_queries");
        HostGroup = SubGroup->GetSubgroup("host", "");
        QueryGroup = HostGroup->GetSubgroup("path", Path);
        CpuMs = QueryGroup->GetCounter("streaming.query.cpu.usage.milliseconds", true);
        MemoryUsageBytes = QueryGroup->GetCounter("streaming.query.memory.usage.bytes");
        UptimeSeconds = QueryGroup->GetCounter("streaming.query.uptime.seconds");
        TaskCount = QueryGroup->GetCounter("streaming.query.tasks.count");
        InputBytes = QueryGroup->GetCounter("streaming.query.input.bytes", true);
        OutputBytes = QueryGroup->GetCounter("streaming.query.output.bytes", true);
    }

private:
    const TString Path;
    ::NMonitoring::TDynamicCounterPtr SubGroup;
    ::NMonitoring::TDynamicCounterPtr HostGroup;
    ::NMonitoring::TDynamicCounterPtr QueryGroup;
    ::NMonitoring::TDynamicCounters::TCounterPtr CpuMs;
    ::NMonitoring::TDynamicCounters::TCounterPtr MemoryUsageBytes;
    ::NMonitoring::TDynamicCounters::TCounterPtr UptimeSeconds;
    ::NMonitoring::TDynamicCounters::TCounterPtr TaskCount;
    ::NMonitoring::TDynamicCounters::TCounterPtr InputBytes;
    ::NMonitoring::TDynamicCounters::TCounterPtr OutputBytes;
    ::NMonitoring::TDynamicCounters::TCounterPtr MaxReadLag; // milliseconds
    const ::NMonitoring::TDynamicCounterPtr Counters;
};

} // namespace

// Collect PQ sources only from the transactions of the current executer.
// Topics whose partition list was already fixed at compile time by a
// __ydb_partition_id predicate are skipped — their ReadRanges are authoritative.
TVector<TPqTopicResolverSource> CollectPqSources(
    const TVector<IKqpGateway::TPhysicalTxData>& transactions, const TString& database) {
    TVector<TPqTopicResolverSource> result;
    for (const auto& tx : transactions) {
        for (const auto& stage : tx.Body->GetStages()) {
            if (stage.SourcesSize() == 0) {
                continue;
            }
            const auto& src = stage.GetSources(0);
            if (!src.HasExternalSource()) {
                continue;
            }
            const auto& extSrc = src.GetExternalSource();
            if (extSrc.GetType() != "PqSource") {
                continue;
            }

            // If the partition list was already fixed at compile time by a
            // __ydb_partition_id predicate, the ReadRanges are authoritative
            // and must not be overwritten with the current total partition count.
            NYql::NPq::NProto::TDqPqTopicSource topicSourceProto;
            const bool usedPartitionPredicate = extSrc.GetSettings().UnpackTo(&topicSourceProto) && topicSourceProto.GetUsedPartitionPredicate();
            if (usedPartitionPredicate) {
                continue;
            }

            NYql::NPq::NProto::TDqPqTopicSource ts;
            extSrc.GetSettings().UnpackTo(&ts);

            TPqTopicResolverSource resolverSrc;
            resolverSrc.Cluster = extSrc.GetSourceName();
            resolverSrc.Endpoint = ts.GetEndpoint();
            resolverSrc.Database = ts.GetEndpoint().empty() ? database : ts.GetDatabase();
            resolverSrc.TopicPath = ts.GetTopicPath();
            resolverSrc.TokenName = ts.GetToken().GetName();
            resolverSrc.UseSsl = ts.GetUseSsl();
            resolverSrc.DatabaseForClusterConfig = ts.GetDatabase();
            result.push_back(std::move(resolverSrc));
        }
    }
    return result;
}

TIntrusivePtr<IStreamingQueryCounters> MakeStreamingQueryCounters(const ::NMonitoring::TDynamicCounterPtr& counters, const TString& path) {
    return MakeIntrusive<TStreamingQueryCounters>(counters, path);
}

} // namespace NKikimr::NKqp

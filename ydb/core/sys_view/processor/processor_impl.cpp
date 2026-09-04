#include "processor_impl.h"

#include "query_metrics_retention.h"
#include <ydb/core/sys_view/service/sysview_service.h>
#include <ydb/core/engine/minikql/flat_local_tx_factory.h>

#include <library/cpp/monlib/service/pages/templates.h>
#include <google/protobuf/text_format.h>


namespace NKikimr {
namespace NSysView {

TSysViewProcessor::TSysViewProcessor(const NActors::TActorId& tablet, TTabletStorageInfo* info, EProcessorMode processorMode)
    : TActor(&TThis::StateInit)
    , TTabletExecutedFlat(info, tablet, new NMiniKQL::TMiniKQLFactory)
    , TotalInterval(TDuration::Seconds(processorMode == EProcessorMode::FAST ? 5 : 60))
    , CollectInterval(TotalInterval / 2)
    , ExternalGroup(new ::NMonitoring::TDynamicCounters)
    , LabeledGroup(new ::NMonitoring::TDynamicCounters)
{
    InternalGroups["kqp_serverless"] = new ::NMonitoring::TDynamicCounters;
    InternalGroups["tablets_serverless"] = new ::NMonitoring::TDynamicCounters;
    InternalGroups["grpc_serverless"] = new ::NMonitoring::TDynamicCounters;
}

void TSysViewProcessor::OnDetach(const TActorContext& ctx) {
    DetachExternalCounters();
    DetachInternalCounters();

    Die(ctx);
}

void TSysViewProcessor::OnTabletDead(TEvTablet::TEvTabletDead::TPtr&, const TActorContext& ctx) {
    DetachExternalCounters();
    DetachInternalCounters();

    Die(ctx);
}

void TSysViewProcessor::OnActivateExecutor(const TActorContext& ctx) {
    SVLOG_I("[" << TabletID() << "] OnActivateExecutor");

    // TODO: tablet counters
    Execute(CreateTxInitSchema(), ctx);
}

void TSysViewProcessor::DefaultSignalTabletActive(const TActorContext& ctx) {
    Y_UNUSED(ctx);
}

void TSysViewProcessor::Handle(TEvPrivate::TEvSendRequests::TPtr&) {
    SVLOG_D("[" << TabletID() << "] Handle TEvPrivate::TEvSendRequests");
    SendRequests();
}

void TSysViewProcessor::ScheduleHourMetricsCleanup() {
    if (!HourMetricsCleanupInFlight) {
        HourMetricsCleanupInFlight = true;
        Send(SelfId(), new TEvPrivate::TEvCleanupHourMetrics());
    }
}

void TSysViewProcessor::PersistSysParam(NIceDb::TNiceDb& db, ui64 id, const TString& value) {
    db.Table<Schema::SysParams>().Key(id).Update(
        NIceDb::TUpdate<Schema::SysParams::Value>(value));
}

void TSysViewProcessor::PersistDatabase(NIceDb::TNiceDb& db) {
    PersistSysParam(db, Schema::SysParam_Database, Database);
}

void TSysViewProcessor::PersistStage(NIceDb::TNiceDb& db) {
    ui64 stage = static_cast<ui64>(CurrentStage);
    PersistSysParam(db, Schema::SysParam_CurrentStage, ToString(stage));
}

void TSysViewProcessor::PersistIntervalEnd(NIceDb::TNiceDb& db) {
    ui64 intervalEndUs = IntervalEnd.MicroSeconds();
    PersistSysParam(db, Schema::SysParam_IntervalEnd, ToString(intervalEndUs));
}

void TSysViewProcessor::PersistLastMergedQueryMetricsIntervalEnd(
    NIceDb::TNiceDb& db, TInstant intervalEnd)
{
    PersistSysParam(db, Schema::SysParam_LastMergedQueryMetricsIntervalEnd,
        ToString(intervalEnd.MicroSeconds()));
}

void TSysViewProcessor::PersistMetricsOneHourEvictBeforeHourEnd(
    NIceDb::TNiceDb& db, ui64 cutoff)
{
    PersistSysParam(db, Schema::SysParam_MetricsOneHourEvictBeforeHourEnd,
        ToString(cutoff));
}

template <typename TSchema>
void TSysViewProcessor::PersistQueryTopResults(NIceDb::TNiceDb& db,
    TQueryTop& top, TResultStatsMap& results, TInstant intervalEnd)
{
    ui64 intervalEndUs = intervalEnd.MicroSeconds();
    ui32 rank = 0;

    std::sort(top.begin(), top.end(), TopQueryCompare);

    for (const auto& entry : top) {
        if (entry.Stats) {
            auto key = std::make_pair(intervalEndUs, ++rank);
            auto& resultStats = results[key];
            resultStats = *entry.Stats;

            TString text;
            TString serialized;
            resultStats.MutableQueryText()->swap(text);
            Y_PROTOBUF_SUPPRESS_NODISCARD resultStats.SerializeToString(&serialized);
            db.Table<TSchema>().Key(key).Update(
                NIceDb::TUpdate<typename TSchema::Text>(text),
                NIceDb::TUpdate<typename TSchema::Data>(serialized));
            resultStats.MutableQueryText()->swap(text);
        }
    }

    SVLOG_D("[" << TabletID() << "] PersistQueryTopResults: "
        << "table id# " << TSchema::TableId
        << ", interval end# " << intervalEnd
        << ", query count# " << top.size()
        << ", persisted# " << rank);
}

TSysViewProcessor::TRankedQueryMetrics TSysViewProcessor::RankMinuteQueryMetrics() const {
    TRankedQueryMetrics result;
    result.reserve(QueryMetrics.size());
    for (const auto& [queryHash, metrics] : QueryMetrics) {
        if (metrics.Metrics.GetCount()) {
            result.emplace_back(metrics.Metrics.GetCpuTimeUs().GetSum(), queryHash);
        }
    }
    std::sort(result.begin(), result.end(), QueryMetricsRankCompare);
    return result;
}

TSysViewProcessor::TRankedQueryMetrics TSysViewProcessor::RankCurrentHourQueryMetrics() const {
    TRankedQueryMetrics result;
    result.reserve(CurrentHourMetrics.size());
    for (const auto& [queryHash, metrics] : CurrentHourMetrics) {
        result.emplace_back(metrics.GetCpuTimeUs().GetSum(), queryHash);
    }
    std::sort(result.begin(), result.end(), QueryMetricsRankCompare);
    return result;
}

ui32 TSysViewProcessor::PersistMinuteQueryMetrics(NIceDb::TNiceDb& db,
    const TRankedQueryMetrics& rankedMetrics)
{
    ui64 intervalEndUs = IntervalEnd.MicroSeconds();
    ui32 rank = 0;

    for (const auto& entry : rankedMetrics) {
        if (rank == NQueryMetricsLimits::OneMinuteResultCount) {
            break;
        }
        auto key = std::make_pair(intervalEndUs, ++rank);

        auto& queryMetrics = QueryMetrics[entry.second];
        auto& resultMetrics = MetricsOneMinute[key];
        resultMetrics.Text = queryMetrics.Text;
        resultMetrics.Metrics = queryMetrics.Metrics;

        TString serialized;
        Y_PROTOBUF_SUPPRESS_NODISCARD resultMetrics.Metrics.SerializeToString(&serialized);
        db.Table<Schema::MetricsOneMinute>().Key(key).Update(
            NIceDb::TUpdate<Schema::MetricsOneMinute::Text>(resultMetrics.Text),
            NIceDb::TUpdate<Schema::MetricsOneMinute::Data>(serialized));
    }

    SVLOG_D("[" << TabletID() << "] PersistQueryResults: "
        << "interval end# " << IntervalEnd
        << ", query count# " << rankedMetrics.size()
        << ", persisted# " << rank);

    return rank;
}

void TSysViewProcessor::MergeCurrentHourQueryMetrics(NIceDb::TNiceDb& db, TInstant hourEnd) {
    const ui64 hourEndUs = hourEnd.MicroSeconds();
    if (CurrentHourEnd != hourEnd) {
        CurrentHourMetrics.clear();
        CurrentHourEnd = hourEnd;
    }

    for (const auto& [queryHash, queryMetrics] : QueryMetrics) {
        const auto& minuteMetrics = queryMetrics.Metrics;
        if (!minuteMetrics.GetCount()) {
            continue;
        }
        auto& hourMetrics = CurrentHourMetrics[queryHash];
        if (!hourMetrics.GetCount()) {
            hourMetrics.CopyFrom(minuteMetrics);
        } else {
            Aggregate(hourMetrics, minuteMetrics);
        }

        TString serialized;
        Y_PROTOBUF_SUPPRESS_NODISCARD hourMetrics.SerializeToString(&serialized);
        db.Table<Schema::IntervalMetricsOneHour>().Key(hourEndUs, queryHash).Update(
            NIceDb::TUpdate<Schema::IntervalMetricsOneHour::Data>(serialized));
    }
}

ui32 TSysViewProcessor::PersistCurrentHourQueryMetrics(NIceDb::TNiceDb& db,
    TInstant hourEnd, const TRankedQueryMetrics& rankedMetrics)
{
    const ui64 hourEndUs = hourEnd.MicroSeconds();
    std::unordered_map<TQueryHash, TString> previousTexts;
    auto previous = MetricsOneHour.lower_bound(std::make_pair(hourEndUs, 0));
    while (previous != MetricsOneHour.end() && previous->first.first == hourEndUs) {
        previousTexts.emplace(previous->second.Metrics.GetQueryTextHash(), previous->second.Text);
        ++previous;
    }

    ui32 hourRank = 0;
    for (const auto& [_, queryHash] : rankedMetrics) {
        if (hourRank == NQueryMetricsLimits::OneHourResultCount) {
            break;
        }

        auto key = std::make_pair(hourEndUs, ++hourRank);
        auto& result = MetricsOneHour[key];
        result.Metrics = CurrentHourMetrics[queryHash];

        if (auto it = QueryMetrics.find(queryHash);
            it != QueryMetrics.end() && !it->second.Text.empty())
        {
            result.Text = it->second.Text;
        } else if (auto it = previousTexts.find(queryHash); it != previousTexts.end()) {
            result.Text = it->second;
        } else {
            result.Text.clear();
        }

        TString serialized;
        Y_PROTOBUF_SUPPRESS_NODISCARD result.Metrics.SerializeToString(&serialized);
        db.Table<Schema::MetricsOneHour>().Key(key).Update(
            NIceDb::TUpdate<Schema::MetricsOneHour::Text>(result.Text),
            NIceDb::TUpdate<Schema::MetricsOneHour::Data>(serialized));
    }

    auto stale = MetricsOneHour.upper_bound(std::make_pair(hourEndUs, hourRank));
    while (stale != MetricsOneHour.end() && stale->first.first == hourEndUs) {
        db.Table<Schema::MetricsOneHour>().Key(stale->first).Delete();
        stale = MetricsOneHour.erase(stale);
    }

    return hourRank;
}

ui64 TSysViewProcessor::QueryMetricsResultSize(const TQueryToMetrics& result) {
    return result.Text.size() + result.Metrics.ByteSizeLong();
}

void TSysViewProcessor::UpdateMetricsOneHourRetentionCounters(
    ui64 retainedBytes, ui64 evictedBuckets)
{
    MetricsOneHourRetainedBytes = retainedBytes;
    auto* counters = Executor()->GetCounters();
    counters->Simple()[COUNTER_QUERY_METRICS_ONE_HOUR_RETAINED_BYTES]
        .Set(retainedBytes);
    if (evictedBuckets) {
        counters->Cumulative()[COUNTER_QUERY_METRICS_ONE_HOUR_BUCKETS_EVICTED_BY_SIZE]
            .Increment(evictedBuckets);
    }
}

void TSysViewProcessor::EnforceMetricsOneHourByteLimit(
    NIceDb::TNiceDb& db, TInstant activeHourEnd)
{
    TMap<ui64, ui64> bucketBytes;
    for (const auto& [key, result] : MetricsOneHour) {
        bucketBytes[key.first] += QueryMetricsResultSize(result);
    }

    const auto plan = PlanQueryMetricsRetention(
        bucketBytes, activeHourEnd.MicroSeconds(),
        NQueryMetricsLimits::OneHourHistoryByteLimit);

    for (ui64 hourEndUs : plan.BucketsToEvict) {
        auto it = MetricsOneHour.lower_bound(std::make_pair(hourEndUs, 0));
        while (it != MetricsOneHour.end() && it->first.first == hourEndUs) {
            it = MetricsOneHour.erase(it);
        }
    }

    if (plan.EvictBeforeHourEnd > MetricsOneHourEvictBeforeHourEndUs) {
        MetricsOneHourEvictBeforeHourEndUs = plan.EvictBeforeHourEnd;
        PersistMetricsOneHourEvictBeforeHourEnd(
            db, MetricsOneHourEvictBeforeHourEndUs);
    }

    UpdateMetricsOneHourRetentionCounters(plan.RetainedBytes, 0);
}

void TSysViewProcessor::UpdateAndLogQueryMetricsCoverage(
    TInstant hourEnd, ui32 persistedHourMetrics)
{
    ui64 receivedCpuTimeUs = 0;
    for (const auto& [_, metrics] : QueryMetrics) {
        receivedCpuTimeUs += metrics.Metrics.GetCpuTimeUs().GetSum();
    }

    ui64 timedOutNodes = 0;
    for (const auto& node : NodesToRequest) {
        timedOutNodes += !node.Hashes.empty();
    }
    for (const auto& [_, node] : NodesInFlight) {
        timedOutNodes += !node.Hashes.empty();
    }

    auto& counters = Executor()->GetCounters()->Simple();
    counters[COUNTER_QUERY_METRICS_LAST_INTERVAL_TOTAL_CPU_TIME_US]
        .Set(QueryMetricsCoverage.TotalCpuTimeUs);
    counters[COUNTER_QUERY_METRICS_LAST_INTERVAL_NODE_RETAINED_CPU_TIME_US]
        .Set(QueryMetricsCoverage.NodeRetainedCpuTimeUs);
    counters[COUNTER_QUERY_METRICS_LAST_INTERVAL_PROCESSOR_RETAINED_CPU_TIME_US]
        .Set(QueryMetricsCoverage.ProcessorRetainedCpuTimeUs);
    counters[COUNTER_QUERY_METRICS_LAST_INTERVAL_RECEIVED_CPU_TIME_US]
        .Set(receivedCpuTimeUs);
    counters[COUNTER_QUERY_METRICS_LAST_INTERVAL_SUMMARY_NODES]
        .Set(QueryMetricsCoverage.SummaryNodes);
    counters[COUNTER_QUERY_METRICS_LAST_INTERVAL_COVERAGE_KNOWN_NODES]
        .Set(QueryMetricsCoverage.Nodes);
    counters[COUNTER_QUERY_METRICS_LAST_INTERVAL_REQUESTED_NODES]
        .Set(QueryMetricsCoverage.RequestedNodes);
    counters[COUNTER_QUERY_METRICS_LAST_INTERVAL_RESPONDED_NODES]
        .Set(QueryMetricsCoverage.RespondedNodes);
    counters[COUNTER_QUERY_METRICS_LAST_INTERVAL_FAILED_NODES]
        .Set(QueryMetricsCoverage.FailedNodes);
    counters[COUNTER_QUERY_METRICS_LAST_INTERVAL_TIMED_OUT_NODES]
        .Set(timedOutNodes);

    SVLOG_D("[" << TabletID() << "] Persist hour query metrics: "
        << "hour end# " << hourEnd
        << ", accumulator size# " << CurrentHourMetrics.size()
        << ", persisted# " << persistedHourMetrics
        << ", summary nodes# " << QueryMetricsCoverage.SummaryNodes
        << ", coverage nodes# " << QueryMetricsCoverage.Nodes
        << ", requested nodes# " << QueryMetricsCoverage.RequestedNodes
        << ", responded nodes# " << QueryMetricsCoverage.RespondedNodes
        << ", failed nodes# " << QueryMetricsCoverage.FailedNodes
        << ", timed out nodes# " << timedOutNodes
        << ", total cpu us# " << QueryMetricsCoverage.TotalCpuTimeUs
        << ", node retained cpu us# " << QueryMetricsCoverage.NodeRetainedCpuTimeUs
        << ", processor retained cpu us# " << QueryMetricsCoverage.ProcessorRetainedCpuTimeUs
        << ", received cpu us# " << receivedCpuTimeUs);
}

void TSysViewProcessor::FinalizeQueryMetricsInterval(NIceDb::TNiceDb& db) {
    if (IntervalEnd <= LastMergedQueryMetricsIntervalEnd) {
        return;
    }

    const auto minuteMetrics = RankMinuteQueryMetrics();
    PersistMinuteQueryMetrics(db, minuteMetrics);

    const auto hourEnd = EndOfHourInterval(IntervalEnd);
    MergeCurrentHourQueryMetrics(db, hourEnd);

    const auto hourMetrics = RankCurrentHourQueryMetrics();
    const ui32 persistedHourMetrics =
        PersistCurrentHourQueryMetrics(db, hourEnd, hourMetrics);
    EnforceMetricsOneHourByteLimit(db, hourEnd);

    LastMergedQueryMetricsIntervalEnd = IntervalEnd;
    PersistLastMergedQueryMetricsIntervalEnd(
        db, LastMergedQueryMetricsIntervalEnd);

    UpdateAndLogQueryMetricsCoverage(hourEnd, persistedHourMetrics);
}

void TSysViewProcessor::PersistQueryResults(NIceDb::TNiceDb& db) {
    FinalizeQueryMetricsInterval(db);

    PersistQueryTopResults<Schema::TopByDurationOneMinute>(
        db, ByDurationMinute, TopByDurationOneMinute, IntervalEnd);
    PersistQueryTopResults<Schema::TopByReadBytesOneMinute>(
        db, ByReadBytesMinute, TopByReadBytesOneMinute, IntervalEnd);
    PersistQueryTopResults<Schema::TopByCpuTimeOneMinute>(
        db, ByCpuTimeMinute, TopByCpuTimeOneMinute, IntervalEnd);
    PersistQueryTopResults<Schema::TopByRequestUnitsOneMinute>(
        db, ByRequestUnitsMinute, TopByRequestUnitsOneMinute, IntervalEnd);

    auto hourEnd = EndOfHourInterval(IntervalEnd);

    PersistQueryTopResults<Schema::TopByDurationOneHour>(
        db, ByDurationHour, TopByDurationOneHour, hourEnd);
    PersistQueryTopResults<Schema::TopByReadBytesOneHour>(
        db, ByReadBytesHour, TopByReadBytesOneHour, hourEnd);
    PersistQueryTopResults<Schema::TopByCpuTimeOneHour>(
        db, ByCpuTimeHour, TopByCpuTimeOneHour, hourEnd);
    PersistQueryTopResults<Schema::TopByRequestUnitsOneHour>(
        db, ByRequestUnitsHour, TopByRequestUnitsOneHour, hourEnd);
}

template <typename TSchema>
void TSysViewProcessor::PersistPartitionTopResults(NIceDb::TNiceDb& db,
    TPartitionTop& top, TResultPartitionsMap& results, TInstant intervalEnd)
{
    ui64 intervalEndUs = intervalEnd.MicroSeconds();
    ui32 rank = 0;

    for (const auto& partition : top) {
        auto key = std::make_pair(intervalEndUs, ++rank);
        auto& info = results[key];
        info.CopyFrom(*partition);

        TString data;
        Y_PROTOBUF_SUPPRESS_NODISCARD info.SerializeToString(&data);
        db.Table<TSchema>().Key(key).Update(
            NIceDb::TUpdate<typename TSchema::Data>(data));
    }

    SVLOG_D("[" << TabletID() << "] PersistPartitionTopResults: "
        << "table id# " << TSchema::TableId
        << ", partition interval end# " << intervalEnd
        << ", partition count# " << top.size());
}

void TSysViewProcessor::PersistPartitionResults(NIceDb::TNiceDb& db) {
    auto intervalEnd = IntervalEnd + TotalInterval;

    PersistPartitionTopResults<Schema::TopPartitionsOneMinute>(
        db, PartitionTopByCpuMinute, TopPartitionsByCpuOneMinute, intervalEnd);
    PersistPartitionTopResults<Schema::TopPartitionsByTliOneMinute>(
        db, PartitionTopByTliMinute, TopPartitionsByTliOneMinute, intervalEnd);

    auto hourEnd = EndOfHourInterval(intervalEnd);

    PersistPartitionTopResults<Schema::TopPartitionsOneHour>(
        db, PartitionTopByCpuHour, TopPartitionsByCpuOneHour, hourEnd);
    PersistPartitionTopResults<Schema::TopPartitionsByTliOneHour>(
        db, PartitionTopByTliHour, TopPartitionsByTliOneHour, hourEnd);
}

void TSysViewProcessor::ScheduleAggregate() {
    auto rangeUs = RandomNumber<ui64>(TotalInterval.MicroSeconds() / 12);
    auto deadline = IntervalEnd + CollectInterval + TDuration::MicroSeconds(rangeUs);
    Schedule(deadline, new TEvPrivate::TEvAggregate);
}

void TSysViewProcessor::ScheduleCollect() {
    auto rangeUs = RandomNumber<ui64>(TotalInterval.MicroSeconds() / 12);
    auto deadline = IntervalEnd + TotalInterval + TDuration::MicroSeconds(rangeUs);
    Schedule(deadline, new TEvPrivate::TEvCollect);
}

void TSysViewProcessor::ScheduleSendRequests() {
    auto intervalUs = TotalInterval.MicroSeconds() / 12;
    auto rangeUs = RandomNumber<ui64>(intervalUs);
    auto deadline = IntervalEnd + CollectInterval + TDuration::MicroSeconds(intervalUs + rangeUs);
    Schedule(deadline, new TEvPrivate::TEvSendRequests);
}

void TSysViewProcessor::ScheduleApplyCounters() {
    Schedule(ProcessCountersInterval, new TEvPrivate::TEvApplyCounters);
}

void TSysViewProcessor::ScheduleApplyLabeledCounters() {
    Schedule(ProcessLabeledCountersInterval, new TEvPrivate::TEvApplyLabeledCounters);
}

void TSysViewProcessor::ScheduleSendNavigate() {
    Schedule(SendNavigateInterval, new TEvPrivate::TEvSendNavigate);
}

template <typename TSchema, typename TMap>
void TSysViewProcessor::CutHistory(NIceDb::TNiceDb& db, TMap& results, TDuration historySize) {
    auto past = IntervalEnd - historySize;
    typename TMap::key_type key;
    key.first = past.MicroSeconds();
    key.second = 0;

    auto bound = results.lower_bound(key);
    for (auto it = results.begin(); it != bound; ++it) {
        db.Table<TSchema>().Key(it->first).Delete();
    }
    results.erase(results.begin(), bound);
}

TInstant TSysViewProcessor::EndOfHourInterval(TInstant intervalEnd) {
    return EndOfQueryMetricsHourInterval(intervalEnd);
}

void TSysViewProcessor::ClearIntervalSummaries(NIceDb::TNiceDb& db) {
    for (const auto& [queryHash, query] : Queries) {
        ui32 count = query.Nodes.size();
        for (ui32 i = 0; i < count; ++i) {
            db.Table<Schema::IntervalSummaries>().Key(queryHash, i).Delete();
        }
    }
    Queries.clear();
    ByCpu.clear();
    SummaryNodes.clear();
}

void TSysViewProcessor::Reset(NIceDb::TNiceDb& db, const TActorContext& ctx) {
    ClearIntervalSummaries(db);

    for (const auto& [queryHash, _] : QueryMetrics) {
        db.Table<Schema::IntervalMetrics>().Key(queryHash).Delete();
    }
    QueryMetrics.clear();

    for (const auto& node : NodesToRequest) {
        db.Table<Schema::NodesToRequest>().Key(node.NodeId).Delete();
    }
    for (const auto& [nodeId, _] : NodesInFlight) {
        db.Table<Schema::NodesToRequest>().Key(nodeId).Delete();
    }
    NodesToRequest.clear();
    NodesInFlight.clear();

    QueryMetricsCoverage = {};

    auto clearQueryTop = [&] (NKikimrSysView::EStatsType type, TQueryTop& top) {
        for (const auto& query : top) {
            db.Table<Schema::IntervalTops>().Key((ui32)type, query.Hash).Delete();
        }
        top.clear();
    };

    auto clearPartitionTop = [&] (NKikimrSysView::EStatsType type, TPartitionTop& top) {
        for (const auto& partition : top) {
            if (partition->GetFollowerId() == 0)
                db.Table<Schema::IntervalPartitionTops>().Key((ui32)type, partition->GetTabletId()).Delete();
            else
                db.Table<Schema::IntervalPartitionFollowerTops>().Key((ui32)type, partition->GetTabletId(), partition->GetFollowerId()).Delete();
        }
        top.clear();
    };

    clearQueryTop(NKikimrSysView::TOP_DURATION_ONE_MINUTE, ByDurationMinute);
    clearQueryTop(NKikimrSysView::TOP_READ_BYTES_ONE_MINUTE, ByReadBytesMinute);
    clearQueryTop(NKikimrSysView::TOP_CPU_TIME_ONE_MINUTE, ByCpuTimeMinute);
    clearQueryTop(NKikimrSysView::TOP_REQUEST_UNITS_ONE_MINUTE, ByRequestUnitsMinute);

    clearPartitionTop(NKikimrSysView::TOP_PARTITIONS_BY_CPU_ONE_MINUTE, PartitionTopByCpuMinute);
    clearPartitionTop(NKikimrSysView::TOP_PARTITIONS_BY_TLI_ONE_MINUTE, PartitionTopByTliMinute);

    CurrentStage = COLLECT;
    PersistStage(db);

    auto oldHourEnd = EndOfHourInterval(IntervalEnd);
    auto partitionOldHourEnd = EndOfHourInterval(IntervalEnd + TotalInterval);

    auto now = ctx.Now();
    auto intervalSize = TotalInterval.MicroSeconds();
    auto rounded = now.MicroSeconds() / intervalSize * intervalSize;
    IntervalEnd = TInstant::MicroSeconds(rounded);
    PersistIntervalEnd(db);

    auto newHourEnd = EndOfHourInterval(IntervalEnd);
    auto partitionNewHourEnd = EndOfHourInterval(IntervalEnd + TotalInterval);

    if (oldHourEnd != newHourEnd) {
        CurrentHourMetrics.clear();
        CurrentHourEnd = newHourEnd;

        clearQueryTop(NKikimrSysView::TOP_DURATION_ONE_HOUR, ByDurationHour);
        clearQueryTop(NKikimrSysView::TOP_READ_BYTES_ONE_HOUR, ByReadBytesHour);
        clearQueryTop(NKikimrSysView::TOP_CPU_TIME_ONE_HOUR, ByCpuTimeHour);
        clearQueryTop(NKikimrSysView::TOP_REQUEST_UNITS_ONE_HOUR, ByRequestUnitsHour);
    }

    if (partitionOldHourEnd != partitionNewHourEnd) {
        clearPartitionTop(NKikimrSysView::TOP_PARTITIONS_BY_CPU_ONE_HOUR, PartitionTopByCpuHour);
        clearPartitionTop(NKikimrSysView::TOP_PARTITIONS_BY_TLI_ONE_HOUR, PartitionTopByTliHour);
    }

    SVLOG_D("[" << TabletID() << "] Reset: interval end# " << IntervalEnd);

    const auto minuteHistorySize = TotalInterval * ONE_MINUTE_BUCKET_COUNT;
    const auto hourHistorySize = ONE_HOUR_BUCKET_SIZE * ONE_HOUR_BUCKET_COUNT;

    CutHistory<Schema::MetricsOneMinute>(db, MetricsOneMinute, minuteHistorySize);
    CutHistory<Schema::MetricsOneHour>(db, MetricsOneHour, hourHistorySize);

    CutHistory<Schema::TopByDurationOneMinute>(db, TopByDurationOneMinute, minuteHistorySize);
    CutHistory<Schema::TopByDurationOneHour>(db, TopByDurationOneHour, hourHistorySize);
    CutHistory<Schema::TopByReadBytesOneMinute>(db, TopByReadBytesOneMinute, minuteHistorySize);
    CutHistory<Schema::TopByReadBytesOneHour>(db, TopByReadBytesOneHour, hourHistorySize);
    CutHistory<Schema::TopByCpuTimeOneMinute>(db, TopByCpuTimeOneMinute, minuteHistorySize);
    CutHistory<Schema::TopByCpuTimeOneHour>(db, TopByCpuTimeOneHour, hourHistorySize);
    CutHistory<Schema::TopByRequestUnitsOneMinute>(db, TopByRequestUnitsOneMinute, minuteHistorySize);
    CutHistory<Schema::TopByRequestUnitsOneHour>(db, TopByRequestUnitsOneHour, hourHistorySize);

    CutHistory<Schema::TopPartitionsOneMinute>(db, TopPartitionsByCpuOneMinute, minuteHistorySize);
    CutHistory<Schema::TopPartitionsOneHour>(db, TopPartitionsByCpuOneHour, hourHistorySize);
    CutHistory<Schema::TopPartitionsByTliOneMinute>(db, TopPartitionsByTliOneMinute, minuteHistorySize);
    CutHistory<Schema::TopPartitionsByTliOneHour>(db, TopPartitionsByTliOneHour, hourHistorySize);

    EnforceMetricsOneHourByteLimit(db, newHourEnd);
}

void TSysViewProcessor::SendRequests() {
    while (!NodesToRequest.empty() && NodesInFlight.size() < MaxInFlightRequests) {
        auto& req = NodesToRequest.back();

        auto request = MakeHolder<TEvSysView::TEvGetIntervalMetricsRequest>();
        auto& record = request->Record;
        record.SetIntervalEndUs(IntervalEnd.MicroSeconds());
        record.SetDatabase(Database);

        auto fillHashes = [&] (const THashVector& hashes, NProtoBuf::RepeatedField<ui64>& result) {
            result.Reserve(hashes.size());
            for (auto queryHash : hashes) {
                result.Add(queryHash);
            }
        };

        fillHashes(req.Hashes, *record.MutableMetrics());
        fillHashes(req.TextsToGet, *record.MutableQueryTextsToGet());
        fillHashes(req.ByDuration, *record.MutableTopByDuration());
        fillHashes(req.ByReadBytes, *record.MutableTopByReadBytes());
        fillHashes(req.ByCpuTime, *record.MutableTopByCpuTime());
        fillHashes(req.ByRequestUnits, *record.MutableTopByRequestUnits());

        SVLOG_D("[" << TabletID() << "] Send TEvGetIntervalMetricsRequest: "
            << "node id# " << req.NodeId
            << ", hashes# " << req.Hashes.size()
            << ", texts# " << req.TextsToGet.size()
            << ", by duration# " << req.ByDuration.size()
            << ", by read bytes# " << req.ByReadBytes.size()
            << ", by cpu time# " << req.ByCpuTime.size()
            << ", by request units# " << req.ByRequestUnits.size());

        Send(MakeSysViewServiceID(req.NodeId),
            std::move(request),
            IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession,
            req.NodeId);

        NodesInFlight[req.NodeId] = std::move(req);
        NodesToRequest.pop_back();
    }
}

void TSysViewProcessor::Handle(TEvents::TEvUndelivered::TPtr& ev) {
    auto nodeId = (TNodeId)ev.Get()->Cookie;
    SVLOG_W("[" << TabletID() << "] TEvUndelivered: node id# " << nodeId);
    HandleIntervalMetricsFailure(nodeId);
}

void TSysViewProcessor::Handle(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
    auto nodeId = ev->Get()->NodeId;
    SVLOG_W("[" << TabletID() << "] TEvNodeDisconnected: node id# " << nodeId);
    HandleIntervalMetricsFailure(nodeId);
}

void TSysViewProcessor::Handle(TEvSysView::TEvGetQueryMetricsRequest::TPtr& ev) {
    const auto& record = ev->Get()->Record;

    if (PendingRequests.size() >= PendingRequestsLimit) {
        auto type = record.GetType();
        if (type == NKikimrSysView::METRICS_ONE_MINUTE || type == NKikimrSysView::METRICS_ONE_HOUR) {
            ReplyOverloaded<TEvSysView::TEvGetQueryMetricsResponse>(ev->Sender);
        } else {
            ReplyOverloaded<TEvSysView::TEvGetQueryStatsResponse>(ev->Sender);
        }
        return;
    }

    PendingRequests.push(std::move(ev));

    if (!ProcessInFly) {
        Send(SelfId(), new TEvPrivate::TEvProcess());
        ProcessInFly = true;
    }
}

void TSysViewProcessor::Handle(TEvSysView::TEvGetTopPartitionsRequest::TPtr& ev) {
    if (PendingRequests.size() >= PendingRequestsLimit) {
        ReplyOverloaded<TEvSysView::TEvGetTopPartitionsResponse>(ev->Sender);
        return;
    }

    PendingRequests.push(std::move(ev));

    if (!ProcessInFly) {
        Send(SelfId(), new TEvPrivate::TEvProcess());
        ProcessInFly = true;
    }
}

void TSysViewProcessor::Handle(TEvPrivate::TEvProcess::TPtr&) {
    ProcessInFly = false;

    if (PendingRequests.empty()) {
        return;
    }

    TVariantRequestPtr request = std::move(PendingRequests.front());
    PendingRequests.pop();

    if (!PendingRequests.empty()) {
        Send(SelfId(), new TEvPrivate::TEvProcess);
        ProcessInFly = true;
    }

    if (auto* req = std::get_if<TEvSysView::TEvGetTopPartitionsRequest::TPtr>(&request)) {
        Reply<TResultPartitionsMap,
            TEvSysView::TEvGetTopPartitionsRequest,
            TEvSysView::TEvGetTopPartitionsResponse>(*req);

    } else if (auto* req = std::get_if<TEvSysView::TEvGetQueryMetricsRequest::TPtr>(&request)) {
        const auto& record = (*req)->Get()->Record;
        auto type = record.GetType();

        if (type == NKikimrSysView::METRICS_ONE_MINUTE || type == NKikimrSysView::METRICS_ONE_HOUR) {
            Reply<TResultMetricsMap,
                TEvSysView::TEvGetQueryMetricsRequest,
                TEvSysView::TEvGetQueryMetricsResponse>(*req);
        } else {
            Reply<TResultStatsMap,
                TEvSysView::TEvGetQueryMetricsRequest,
                TEvSysView::TEvGetQueryStatsResponse>(*req);
        }
    } else {
        Y_ABORT("unknown SVP request");
    }
}

void TSysViewProcessor::EntryToProto(NKikimrSysView::TQueryMetricsEntry& dst, const TQueryToMetrics& src) {
    dst.MutableMetrics()->CopyFrom(src.Metrics);
    dst.SetQueryText(src.Text);
}

void TSysViewProcessor::EntryToProto(NKikimrSysView::TQueryStatsEntry& dst, const NKikimrSysView::TQueryStats& src) {
    dst.MutableStats()->CopyFrom(src);
}

void TSysViewProcessor::EntryToProto(NKikimrSysView::TTopPartitionsEntry& dst, const NKikimrSysView::TTopPartitionsInfo& src) {
    dst.MutableInfo()->CopyFrom(src);
}

template <typename TResponse>
void TSysViewProcessor::ReplyOverloaded(const NActors::TActorId& sender) {
    auto response = MakeHolder<TResponse>();
    response->Record.SetOverloaded(true);
    Send(sender, std::move(response));
}

template <typename TMap, typename TRequest, typename TResponse>
void TSysViewProcessor::Reply(typename TRequest::TPtr& ev) {
    const auto& record = ev->Get()->Record;
    auto response = MakeHolder<TResponse>();
    response->Record.SetLastBatch(true);

    using TEntry = typename TMap::mapped_type;
    TMap* entries = nullptr;
    if constexpr (std::is_same<TEntry, NKikimrSysView::TTopPartitionsInfo>::value) {
        switch (record.GetType()) {
            case NKikimrSysView::TOP_PARTITIONS_BY_CPU_ONE_MINUTE:
                entries = &TopPartitionsByCpuOneMinute;
                break;
            case NKikimrSysView::TOP_PARTITIONS_BY_CPU_ONE_HOUR:
                entries = &TopPartitionsByCpuOneHour;
                break;
            case NKikimrSysView::TOP_PARTITIONS_BY_TLI_ONE_MINUTE:
                entries = &TopPartitionsByTliOneMinute;
                break;
            case NKikimrSysView::TOP_PARTITIONS_BY_TLI_ONE_HOUR:
                entries = &TopPartitionsByTliOneHour;
                break;
            default:
                SVLOG_CRIT("[" << TabletID() << "] unexpected stats type: " << (size_t)record.GetType());
                Send(ev->Sender, std::move(response));
                return;
        }
    } else if constexpr (std::is_same<TEntry, TQueryToMetrics>::value) {
        switch (record.GetType()) {
            case NKikimrSysView::METRICS_ONE_MINUTE:
                entries = &MetricsOneMinute;
                break;
            case NKikimrSysView::METRICS_ONE_HOUR:
                entries = &MetricsOneHour;
                break;
            default:
                SVLOG_CRIT("[" << TabletID() << "] unexpected stats type: " << (size_t)record.GetType());
                Send(ev->Sender, std::move(response));
                return;
        }
    } else {
        switch (record.GetType()) {
            case NKikimrSysView::TOP_DURATION_ONE_MINUTE:
                entries = &TopByDurationOneMinute;
                break;
            case NKikimrSysView::TOP_DURATION_ONE_HOUR:
                entries = &TopByDurationOneHour;
                break;
            case NKikimrSysView::TOP_READ_BYTES_ONE_MINUTE:
                entries = &TopByReadBytesOneMinute;
                break;
            case NKikimrSysView::TOP_READ_BYTES_ONE_HOUR:
                entries = &TopByReadBytesOneHour;
                break;
            case NKikimrSysView::TOP_CPU_TIME_ONE_MINUTE:
                entries = &TopByCpuTimeOneMinute;
                break;
            case NKikimrSysView::TOP_CPU_TIME_ONE_HOUR:
                entries = &TopByCpuTimeOneHour;
                break;
            case NKikimrSysView::TOP_REQUEST_UNITS_ONE_MINUTE:
                entries = &TopByRequestUnitsOneMinute;
                break;
            case NKikimrSysView::TOP_REQUEST_UNITS_ONE_HOUR:
                entries = &TopByRequestUnitsOneHour;
                break;
            default:
                SVLOG_CRIT("[" << TabletID() << "] unexpected stats type: " << (size_t)record.GetType());
                Send(ev->Sender, std::move(response));
                return;
        }
    }

    Y_ABORT_UNLESS(entries);

    auto from = entries->begin();
    auto to = entries->end();

    if (record.HasFrom()) {
        auto key = std::make_pair(record.GetFrom().GetIntervalEndUs(), record.GetFrom().GetRank());
        if (!record.HasInclusiveFrom() || record.GetInclusiveFrom()) {
            from = entries->lower_bound(key);
        } else {
            from = entries->upper_bound(key);
        }
    }

    if (record.HasTo()) {
        auto key = std::make_pair(record.GetTo().GetIntervalEndUs(), record.GetTo().GetRank());
        if (!record.HasInclusiveTo() || !record.GetInclusiveTo()) {
            to = entries->lower_bound(key);
        } else {
            to = entries->upper_bound(key);
        }
    }

    size_t size = 0;
    size_t count = 0;
    for (auto it = from; it != to; ++it) {
        const auto& key = it->first;

        auto& entry = *response->Record.AddEntries();
        auto& entryKey = *entry.MutableKey();
        entryKey.SetIntervalEndUs(key.first);
        entryKey.SetRank(key.second);

        EntryToProto(entry, it->second);

        size += entry.ByteSizeLong();
        ++count;

        if (size >= BatchSizeLimit) {
            auto* next = response->Record.MutableNext();
            next->SetIntervalEndUs(key.first);
            next->SetRank(key.second + 1);
            response->Record.SetLastBatch(false);
            break;
        }
    }

    TString rangeStr, nextStr;
    google::protobuf::TextFormat::Printer range;
    range.SetSingleLineMode(true);
    range.PrintToString(record, &rangeStr);
    if (response->Record.HasNext()) {
        google::protobuf::TextFormat::Printer next;
        next.SetSingleLineMode(true);
        next.PrintToString(response->Record.GetNext(), &nextStr);
    }

    SVLOG_D("[" << TabletID() << "] Reply batch: "
        << "range# " << rangeStr
        << ", rows# " << count
        << ", bytes# " << size
        << ", next# " << nextStr);

    Send(ev->Sender, std::move(response));
}

bool TSysViewProcessor::OnRenderAppHtmlPage(NMon::TEvRemoteHttpInfo::TPtr ev,
    const TActorContext& ctx)
{
    if (!ev) {
        return true;
    }

    TStringStream str;
    HTML(str) {
        PRE() {
            str << "---- SysViewProcessor ----" << Endl << Endl;
            str << "Database: " << Database << Endl;
            str << "IntervalEnd: " << IntervalEnd << Endl;
            str << "CurrentStage: " << (CurrentStage == COLLECT ? "Collect" : "Aggregate")
                << Endl << Endl;
            {
                str << "IntervalSummaries" << Endl;
                str << "  QueryCount: " << Queries.size() << Endl;

                auto it = ByCpu.rbegin();
                static constexpr size_t queriesLimit = 32;
                for (size_t q = 0;
                    it != ByCpu.rend() && q < queriesLimit;
                    ++it, ++q)
                {
                    const auto queryHash = it->second;
                    auto queryIt = Queries.find(queryHash);
                    Y_ABORT_UNLESS(queryIt != Queries.end());
                    const auto& query = queryIt->second;

                    str << "    Hash: " << queryHash
                        << ", Cpu: " << query.Cpu
                        << ", NodeCount: " << query.Nodes.size()
                        << ", Nodes: ";

                    static constexpr size_t nodesLimit = 4;
                    auto nodeIt = query.Nodes.begin();
                    for (size_t n = 0;
                        nodeIt != query.Nodes.end() && n < nodesLimit;
                        ++nodeIt, ++n)
                    {
                        str << "{ " << nodeIt->first << ", Cpu: " << nodeIt->second << " } ";
                    }
                    if (nodeIt != query.Nodes.end()) {
                        str << "...";
                    }
                    str << Endl;
                }
                if (it != ByCpu.rend()) {
                    str << "    ..." << Endl;
                }
                str << Endl;
            }
            {
                str << "IntervalMetrics" << Endl;
                for (const auto& [queryHash, metrics] : QueryMetrics) {
                    str << "  Hash: " << queryHash
                        << ", Count: " << metrics.Metrics.GetCount()
                        << ", SumCpuTime: " << metrics.Metrics.GetCpuTimeUs().GetSum() << Endl;
                }
                str << Endl;
            }
            {
                auto dumpNode = [&str] (const TNodeToQueries& node) {
                    str << "  NodeId: " << node.NodeId
                        << ", Hashes: " << node.Hashes.size()
                        << ", TextsToGet: " << node.TextsToGet.size()
                        << ", ByDuration: " << node.ByDuration.size()
                        << ", ByReadBytes: " << node.ByReadBytes.size()
                        << ", ByCpuTime: " << node.ByCpuTime.size()
                        << ", ByRequestUnits: " << node.ByRequestUnits.size()
                        << Endl;
                };
                str << "NodesToRequest" << Endl;
                for (const auto& node : NodesToRequest) {
                    dumpNode(node);
                }
                str << Endl;
                str << "NodesInFlight" << Endl;
                for (const auto& [_, node] : NodesInFlight) {
                    dumpNode(node);
                }
                str << Endl;
            }
            {
                auto printTop = [&str] (const TQueryTop& top) {
                    for (const auto& query : top) {
                        str << "  Hash: " << query.Hash
                            << ", Value: " << query.Value
                            << ", NodeId: " << query.NodeId << Endl;
                    }
                };
                str << "ByDurationMinute" << Endl;
                printTop(ByDurationMinute);
                str << Endl;
                str << "ByDurationHour" << Endl;
                printTop(ByDurationHour);
                str << Endl;
                str << "ByReadBytesMinute" << Endl;
                printTop(ByReadBytesMinute);
                str << Endl;
                str << "ByReadBytesHour" << Endl;
                printTop(ByReadBytesHour);
                str << Endl;
                str << "ByCpuTimeMinute" << Endl;
                printTop(ByCpuTimeMinute);
                str << Endl;
                str << "ByCpuTimeHour" << Endl;
                printTop(ByCpuTimeHour);
                str << Endl;
                str << "ByRequestUnitsMinute" << Endl;
                printTop(ByRequestUnitsMinute);
                str << Endl;
                str << "ByRequestUnitsHour" << Endl;
                printTop(ByRequestUnitsHour);
                str << Endl;
            }
            {
                str << "MetricsOneMinute" << Endl
                    << "  Count: " << MetricsOneMinute.size() << Endl << Endl;
                str << "MetricsOneHour" << Endl
                    << "  Count: " << MetricsOneHour.size() << Endl << Endl;
                str << "CurrentHourMetrics" << Endl
                    << "  HourEnd: " << CurrentHourEnd << Endl
                    << "  Count: " << CurrentHourMetrics.size() << Endl
                    << "  LastMergedIntervalEnd: " << LastMergedQueryMetricsIntervalEnd << Endl
                    << "  CleanupInFlight: " << HourMetricsCleanupInFlight << Endl << Endl;
                str << "TopByDurationOneMinute" << Endl
                    << "  Count: " << TopByDurationOneMinute.size() << Endl << Endl;
                str << "TopByDurationOneHour" << Endl
                    << "  Count: " << TopByDurationOneHour.size() << Endl << Endl;
                str << "TopByReadBytesOneMinute" << Endl
                    << "  Count: " << TopByReadBytesOneMinute.size() << Endl << Endl;
                str << "TopByReadBytesOneHour" << Endl
                    << "  Count: " << TopByReadBytesOneHour.size() << Endl << Endl;
                str << "TopByCpuTimeOneMinute" << Endl
                    << "  Count: " << TopByCpuTimeOneMinute.size() << Endl << Endl;
                str << "TopByCpuTimeOneHour" << Endl
                    << "  Count: " << TopByCpuTimeOneHour.size() << Endl << Endl;
                str << "TopByRequestUnitsOneMinute" << Endl
                    << "  Count: " << TopByRequestUnitsOneMinute.size() << Endl << Endl;
                str << "TopByRequestUnitsOneHour" << Endl
                    << "  Count: " << TopByRequestUnitsOneHour.size() << Endl << Endl;
                str << "TopPartitionsByCpuOneMinute" << Endl
                    << "  Count: " << TopPartitionsByCpuOneMinute.size() << Endl << Endl;
                str << "TopPartitionsByCpuOneHour" << Endl
                    << "  Count: " << TopPartitionsByCpuOneHour.size() << Endl << Endl;
                str << "TopPartitionsByTliOneMinute" << Endl
                    << "  Count: " << TopPartitionsByTliOneMinute.size() << Endl << Endl;
                str << "TopPartitionsByTliOneHour" << Endl
                    << "  Count: " << TopPartitionsByTliOneHour.size() << Endl << Endl;
            }
        }
    }

    ctx.Send(ev->Sender, new NMon::TEvRemoteHttpInfoRes(str.Str()));
    return true;
}

} // NSysView
} // NKikimr

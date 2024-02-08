#include "processor_impl.h"

#include <ydb/core/sys_view/service/sysview_service.h>
#include <ydb/core/engine/minikql/flat_local_tx_factory.h>

#include <library/cpp/monlib/service/pages/templates.h>
#include <google/protobuf/text_format.h>


namespace NKikimr {
namespace NSysView {

TSysViewProcessor::TSysViewProcessor(const NActors::TActorId& tablet, TTabletStorageInfo* info, EProcessorMode processorMode)
    : TActor(&TThis::StateInit)
    , TTabletExecutedFlat(info, tablet, new NMiniKQL::TMiniKQLFactory)
    , TotalInterval(TDuration::Seconds(processorMode == EProcessorMode::FAST ? 1 : 60))
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

void TSysViewProcessor::PersistQueryResults(NIceDb::TNiceDb& db) {
    std::vector<std::pair<ui64, TQueryHash>> sorted;
    sorted.reserve(QueryMetrics.size());
    for (const auto& [queryHash, metrics] : QueryMetrics) {
        sorted.emplace_back(metrics.Metrics.GetCpuTimeUs().GetSum(), queryHash);
    }
    std::sort(sorted.begin(), sorted.end(), [] (auto& l, auto& r) { return l.first > r.first; });

    ui64 intervalEndUs = IntervalEnd.MicroSeconds();
    ui32 rank = 0;

    for (const auto& entry : sorted) {
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
        << ", query count# " << sorted.size());

    // TODO: metrics one hour?

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
        db, PartitionTopMinute, TopPartitionsOneMinute, intervalEnd);

    auto hourEnd = EndOfHourInterval(intervalEnd);

    PersistPartitionTopResults<Schema::TopPartitionsOneHour>(
        db, PartitionTopHour, TopPartitionsOneHour, hourEnd);
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
    auto hourUs = ONE_HOUR_BUCKET_SIZE.MicroSeconds();
    auto hourEndUs = intervalEnd.MicroSeconds() / hourUs * hourUs;
    if (hourEndUs != intervalEnd.MicroSeconds()) {
        hourEndUs += hourUs;
    }
    return TInstant::MicroSeconds(hourEndUs);
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
    NodesToRequest.clear();
    NodesInFlight.clear();

    auto clearQueryTop = [&] (NKikimrSysView::EStatsType type, TQueryTop& top) {
        for (const auto& query : top) {
            db.Table<Schema::IntervalTops>().Key((ui32)type, query.Hash).Delete();
        }
        top.clear();
    };

    auto clearPartitionTop = [&] (NKikimrSysView::EStatsType type, TPartitionTop& top) {
        for (const auto& partition : top) {
            db.Table<Schema::IntervalPartitionTops>().Key((ui32)type, partition->GetTabletId()).Delete();
        }
        top.clear();
    };

    clearQueryTop(NKikimrSysView::TOP_DURATION_ONE_MINUTE, ByDurationMinute);
    clearQueryTop(NKikimrSysView::TOP_READ_BYTES_ONE_MINUTE, ByReadBytesMinute);
    clearQueryTop(NKikimrSysView::TOP_CPU_TIME_ONE_MINUTE, ByCpuTimeMinute);
    clearQueryTop(NKikimrSysView::TOP_REQUEST_UNITS_ONE_MINUTE, ByRequestUnitsMinute);

    clearPartitionTop(NKikimrSysView::TOP_PARTITIONS_ONE_MINUTE, PartitionTopMinute);

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
        clearQueryTop(NKikimrSysView::TOP_DURATION_ONE_HOUR, ByDurationHour);
        clearQueryTop(NKikimrSysView::TOP_READ_BYTES_ONE_HOUR, ByReadBytesHour);
        clearQueryTop(NKikimrSysView::TOP_CPU_TIME_ONE_HOUR, ByCpuTimeHour);
        clearQueryTop(NKikimrSysView::TOP_REQUEST_UNITS_ONE_HOUR, ByRequestUnitsHour);
    }

    if (partitionOldHourEnd != partitionNewHourEnd) {
        clearPartitionTop(NKikimrSysView::TOP_PARTITIONS_ONE_HOUR, PartitionTopHour);
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

    CutHistory<Schema::TopPartitionsOneMinute>(db, TopPartitionsOneMinute, minuteHistorySize);
    CutHistory<Schema::TopPartitionsOneHour>(db, TopPartitionsOneHour, hourHistorySize);
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

void TSysViewProcessor::IgnoreFailure(TNodeId nodeId) {
    NodesInFlight.erase(nodeId);
}

void TSysViewProcessor::Handle(TEvents::TEvUndelivered::TPtr& ev) {
    auto nodeId = (TNodeId)ev.Get()->Cookie;
    SVLOG_W("[" << TabletID() << "] TEvUndelivered: node id# " << nodeId);
    IgnoreFailure(nodeId);
}

void TSysViewProcessor::Handle(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
    auto nodeId = ev->Get()->NodeId;
    SVLOG_W("[" << TabletID() << "] TEvNodeDisconnected: node id# " << nodeId);
    IgnoreFailure(nodeId);
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
            case NKikimrSysView::TOP_PARTITIONS_ONE_MINUTE:
                entries = &TopPartitionsOneMinute;
                break;
            case NKikimrSysView::TOP_PARTITIONS_ONE_HOUR:
                entries = &TopPartitionsOneHour;
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
                str << "TopPartitionsOneMinute" << Endl
                    << "  Count: " << TopPartitionsOneMinute.size() << Endl << Endl;
                str << "TopPartitionsOneHour" << Endl
                    << "  Count: " << TopPartitionsOneHour.size() << Endl << Endl;
            }
        }
    }

    ctx.Send(ev->Sender, new NMon::TEvRemoteHttpInfoRes(str.Str()));
    return true;
}

} // NSysView
} // NKikimr


#include "processor_impl.h"

#include <ydb/core/sys_view/service/query_interval.h>

namespace NKikimr {
namespace NSysView {

struct TSysViewProcessor::TTxIntervalMetrics : public TTxBase {
    TNodeId NodeId;
    NKikimrSysView::TEvGetIntervalMetricsResponse Record;

    TTxIntervalMetrics(TSelf* self, TNodeId nodeId,
        NKikimrSysView::TEvGetIntervalMetricsResponse&& record)
        : TTxBase(self)
        , NodeId(nodeId)
        , Record(std::move(record))
    {}

    TTxType GetTxType() const override { return TXTYPE_INTERVAL_METRICS; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        SVLOG_D("[" << Self->TabletID() << "] TTxIntervalMetrics::Execute: "
            << "node id# " << NodeId
            << ", metrics count# " << Record.MetricsSize()
            << ", texts count# " << Record.QueryTextsSize());

        NIceDb::TNiceDb db(txc.DB);

        for (auto& queryText : *Record.MutableQueryTexts()) {
            auto queryHash = queryText.GetHash();
            auto& text = *queryText.MutableText();

            db.Table<Schema::IntervalMetrics>().Key(queryHash).Update(
                NIceDb::TUpdate<Schema::IntervalMetrics::Text>(text));

            Self->QueryMetrics[queryHash].Text = std::move(text);
        }

        for (auto& metrics : *Record.MutableMetrics()) {
            auto queryHash = metrics.GetQueryTextHash();
            auto& newMetrics = Self->QueryMetrics[queryHash].Metrics;

            if (!newMetrics.GetCount()) {
                newMetrics.Swap(&metrics);
            } else {
                Aggregate(newMetrics, metrics);
            }

            TString serialized;
            Y_PROTOBUF_SUPPRESS_NODISCARD newMetrics.SerializeToString(&serialized);
            db.Table<Schema::IntervalMetrics>().Key(queryHash).Update(
                NIceDb::TUpdate<Schema::IntervalMetrics::Metrics>(serialized));
        }

        auto fillTops = [&] (TQueryTop& minuteTop, TQueryTop& hourTop,
            NKikimrSysView::EStatsType minuteType, NKikimrSysView::EStatsType hourType,
            const NProtoBuf::RepeatedPtrField<NKikimrSysView::TQueryStats>& queryStats)
        {
            for (auto& stats : queryStats) {
                auto queryHash = stats.GetQueryTextHash();
                TString serialized;
                for (auto& query : minuteTop) {
                    if (query.Hash == queryHash) {
                        query.Stats = MakeHolder<NKikimrSysView::TQueryStats>();
                        query.Stats->CopyFrom(stats);
                        Y_PROTOBUF_SUPPRESS_NODISCARD query.Stats->SerializeToString(&serialized);
                        db.Table<Schema::IntervalTops>().Key((ui32)minuteType, queryHash).Update(
                            NIceDb::TUpdate<Schema::IntervalTops::Stats>(serialized));
                        break;
                    }
                }
                for (auto& query : hourTop) {
                    if (!query.Stats && query.Hash == queryHash) {
                        query.Stats = MakeHolder<NKikimrSysView::TQueryStats>();
                        query.Stats->CopyFrom(stats);
                        // hash must be in a minute top as well
                        db.Table<Schema::IntervalTops>().Key((ui32)hourType, queryHash).Update(
                            NIceDb::TUpdate<Schema::IntervalTops::Stats>(serialized));
                        break;
                    }
                }
            }
        };

        fillTops(Self->ByDurationMinute, Self->ByDurationHour,
            NKikimrSysView::TOP_DURATION_ONE_MINUTE, NKikimrSysView::TOP_DURATION_ONE_HOUR,
            *Record.MutableTopByDuration());

        fillTops(Self->ByReadBytesMinute, Self->ByReadBytesHour,
            NKikimrSysView::TOP_READ_BYTES_ONE_MINUTE, NKikimrSysView::TOP_READ_BYTES_ONE_HOUR,
            *Record.MutableTopByReadBytes());

        fillTops(Self->ByCpuTimeMinute, Self->ByCpuTimeHour,
            NKikimrSysView::TOP_CPU_TIME_ONE_MINUTE, NKikimrSysView::TOP_CPU_TIME_ONE_HOUR,
            *Record.MutableTopByCpuTime());

        fillTops(Self->ByRequestUnitsMinute, Self->ByRequestUnitsHour,
            NKikimrSysView::TOP_REQUEST_UNITS_ONE_MINUTE, NKikimrSysView::TOP_REQUEST_UNITS_ONE_HOUR,
            *Record.MutableTopByRequestUnits());

        Self->NodesInFlight.erase(NodeId);
        db.Table<Schema::NodesToRequest>().Key(NodeId).Delete();

        if (Self->NodesInFlight.empty() && Self->NodesToRequest.empty()) {
            Self->PersistQueryResults(db);
        }
        return true;
    }

    void Complete(const TActorContext&) override {
        if (!Self->NodesToRequest.empty()) {
            Self->SendRequests();
        }

        SVLOG_D("[" << Self->TabletID() << "] TTxIntervalMetrics::Complete");
    }
};

void TSysViewProcessor::Handle(TEvSysView::TEvGetIntervalMetricsResponse::TPtr& ev) {
    auto& record = ev->Get()->Record;
    TNodeId nodeId = ev.Get()->Cookie;

    if (CurrentStage != AGGREGATE) {
        SVLOG_W("[" << TabletID() << "] TEvGetIntervalMetricsResponse, wrong stage: "
            << "node id# " << nodeId);
        return;
    }

    if (record.GetIntervalEndUs() != IntervalEnd.MicroSeconds()) {
        SVLOG_W("[" << TabletID() << "] TEvGetIntervalMetricsResponse, time mismatch: "
            << "node id# " << nodeId
            << "interval end# " << IntervalEnd
            << "event interval end# " << TInstant::MicroSeconds(record.GetIntervalEndUs()));
        return;
    }

    Execute(new TTxIntervalMetrics(this, nodeId, std::move(record)),
        TActivationContext::AsActorContext());
}

} // NSysView
} // NKikimr

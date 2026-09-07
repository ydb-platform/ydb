#include "processor_impl.h"

#include <ydb/core/sys_view/service/query_interval.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::SYSTEM_VIEWS

namespace NKikimr {
namespace NSysView {

struct TSysViewProcessor::TTxIntervalMetrics : public TTxBase {
    ui64 RequestId;
    NKikimrSysView::TEvGetIntervalMetricsResponse Record;

    TTxIntervalMetrics(TSelf* self, ui64 requestId,
        NKikimrSysView::TEvGetIntervalMetricsResponse&& record)
        : TTxBase(self)
        , RequestId(requestId)
        , Record(std::move(record))
    {}

    TTxType GetTxType() const override { return TXTYPE_INTERVAL_METRICS; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        YDB_LOG_DEBUG("TTxIntervalMetrics::Execute: applying interval metrics from node",
            {"tabletId", Self->TabletID()},
            {"requestId", RequestId},
            {"metricsCount", Record.MetricsSize()},
            {"queryTextCount", Record.QueryTextsSize()});

        auto node = Self->RequestsInFlight.find(RequestId);
        if (node == Self->RequestsInFlight.end()) {
            YDB_LOG_WARN("TTxIntervalMetrics::Execute: unexpected or duplicate response",
                {"tabletId", Self->TabletID()},
                {"requestId", RequestId});
            return true;
        }
        const bool requestedQueryMetrics = !node->second.Hashes.empty();

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

        db.Table<Schema::NodesToRequest>().Key(node->second.NodeId).Delete();
        Self->RequestsInFlight.erase(node);
        if (requestedQueryMetrics) {
            ++Self->QueryMetricsCoverage.RespondedNodes;
        }

        if (Self->RequestsInFlight.empty() && Self->NodesToRequest.empty()) {
            Self->PersistQueryResults(db);
        }
        return true;
    }

    void Complete(const TActorContext&) override {
        if (!Self->NodesToRequest.empty()) {
            Self->SendRequests();
        }

        YDB_LOG_DEBUG("TTxIntervalMetrics::Complete",
            {"tabletId", Self->TabletID()});
    }
};

struct TSysViewProcessor::TTxIntervalMetricsFailure : public TTxBase {
    ui64 RequestId;

    TTxIntervalMetricsFailure(TSelf* self, ui64 requestId)
        : TTxBase(self)
        , RequestId(requestId)
    {}

    TTxType GetTxType() const override { return TXTYPE_INTERVAL_METRICS; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        auto node = Self->RequestsInFlight.find(RequestId);
        if (node == Self->RequestsInFlight.end()) {
            return true;
        }

        NIceDb::TNiceDb db(txc.DB);
        const bool requestedQueryMetrics = !node->second.Hashes.empty();
        db.Table<Schema::NodesToRequest>().Key(node->second.NodeId).Delete();
        Self->RequestsInFlight.erase(node);
        if (requestedQueryMetrics) {
            ++Self->QueryMetricsCoverage.FailedNodes;
        }

        if (Self->RequestsInFlight.empty() && Self->NodesToRequest.empty()) {
            Self->PersistQueryResults(db);
        }

        return true;
    }

    void Complete(const TActorContext&) override {
        if (!Self->NodesToRequest.empty()) {
            Self->SendRequests();
        }

        YDB_LOG_DEBUG("TTxIntervalMetricsFailure::Complete",
            {"tabletId", Self->TabletID()},
            {"requestId", RequestId});
    }
};

void TSysViewProcessor::Handle(TEvSysView::TEvGetIntervalMetricsResponse::TPtr& ev) {
    auto& record = ev->Get()->Record;
    const ui64 requestId = ev->Cookie;

    if (CurrentStage != AGGREGATE) {
        YDB_LOG_WARN("Handle TEvSysView::TEvGetIntervalMetricsResponse: wrong stage",
            {"tabletId", TabletID()},
            {"requestId", requestId},
            {"currentStage", static_cast<ui64>(CurrentStage)});
        return;
    }

    if (record.GetIntervalEndUs() != IntervalEnd.MicroSeconds()) {
        YDB_LOG_WARN("Handle TEvSysView::TEvGetIntervalMetricsResponse: interval end mismatch",
            {"tabletId", TabletID()},
            {"requestId", requestId},
            {"expectedIntervalEnd", IntervalEnd},
            {"responseIntervalEnd", TInstant::MicroSeconds(record.GetIntervalEndUs())});
        return;
    }

    if (IntervalEnd <= LastMergedQueryMetricsIntervalEnd) {
        YDB_LOG_WARN("Handle TEvSysView::TEvGetIntervalMetricsResponse: interval already merged",
            {"tabletId", TabletID()},
            {"requestId", requestId},
            {"intervalEnd", IntervalEnd});
        return;
    }

    Execute(new TTxIntervalMetrics(this, requestId, std::move(record)),
        TActivationContext::AsActorContext());
}

void TSysViewProcessor::HandleIntervalMetricsFailure(ui64 requestId) {
    
    Execute(new TTxIntervalMetricsFailure(this, requestId),
        TActivationContext::AsActorContext());
}

} // NSysView
} // NKikimr

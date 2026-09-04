#include "processor_impl.h"

namespace NKikimr {
namespace NSysView {

struct TSysViewProcessor::TTxAggregate : public TTxBase {
    using TNodeRequests = std::unordered_map<TNodeId, TNodeToQueries>;

    explicit TTxAggregate(TSelf* self)
        : TTxBase(self)
    {}

    TTxType GetTxType() const override { return TXTYPE_AGGREGATE; }

    THashVector SelectMetricCandidates() const {
        TRankedQueryMetrics candidates;
        candidates.reserve(Self->Queries.size());
        for (const auto& [queryHash, query] : Self->Queries) {
            candidates.emplace_back(query.Cpu, queryHash);
        }
        std::sort(candidates.begin(), candidates.end(), Self->QueryMetricsRankCompare);

        THashVector selectedHashes;
        selectedHashes.reserve(std::min(
            candidates.size(), NQueryMetricsLimits::MetricsFetchCount));
        for (const auto& [_, queryHash] : candidates) {
            if (selectedHashes.size() == NQueryMetricsLimits::MetricsFetchCount) {
                break;
            }
            selectedHashes.emplace_back(queryHash);
        }
        return selectedHashes;
    }

    std::unordered_set<TQueryHash> SelectTextsToFetch(
        const THashVector& selectedHashes) const
    {
        const std::unordered_set<TQueryHash> selectedSet(
            selectedHashes.begin(), selectedHashes.end());

        std::unordered_set<TQueryHash> result;
        for (size_t i = 0;
            i < selectedHashes.size() && i < NQueryMetricsLimits::OneMinuteResultCount;
            ++i)
        {
            result.insert(selectedHashes[i]);
        }

        TRankedQueryMetrics prospectiveHourTop;
        prospectiveHourTop.reserve(Self->CurrentHourMetrics.size() + selectedHashes.size());
        for (const auto& [queryHash, metrics] : Self->CurrentHourMetrics) {
            ui64 cpu = metrics.GetCpuTimeUs().GetSum();
            if (selectedSet.contains(queryHash)) {
                cpu += Self->Queries.at(queryHash).Cpu;
            }
            prospectiveHourTop.emplace_back(cpu, queryHash);
        }
        for (auto queryHash : selectedHashes) {
            if (!Self->CurrentHourMetrics.contains(queryHash)) {
                prospectiveHourTop.emplace_back(Self->Queries.at(queryHash).Cpu, queryHash);
            }
        }
        std::sort(prospectiveHourTop.begin(), prospectiveHourTop.end(),
            Self->QueryMetricsRankCompare);

        std::unordered_set<TQueryHash> knownHourTexts;
        const ui64 hourEndUs = Self->EndOfHourInterval(Self->IntervalEnd).MicroSeconds();
        auto persisted = Self->MetricsOneHour.lower_bound(std::make_pair(hourEndUs, 0));
        while (persisted != Self->MetricsOneHour.end() && persisted->first.first == hourEndUs) {
            if (!persisted->second.Text.empty()) {
                knownHourTexts.insert(persisted->second.Metrics.GetQueryTextHash());
            }
            ++persisted;
        }

        for (size_t i = 0;
            i < prospectiveHourTop.size() && i < NQueryMetricsLimits::OneHourResultCount;
            ++i)
        {
            const auto queryHash = prospectiveHourTop[i].second;
            if (selectedSet.contains(queryHash) && !knownHourTexts.contains(queryHash)) {
                result.insert(queryHash);
            }
        }
        return result;
    }

    void AddQueryMetricRequests(const THashVector& selectedHashes,
        const std::unordered_set<TQueryHash>& textsToFetch,
        TNodeRequests& requests, std::unordered_set<TNodeId>& metricsNodes) const
    {
        static constexpr size_t TextReplicaCount = 3;

        for (auto queryHash : selectedHashes) {
            const auto& nodes = Self->Queries.at(queryHash).Nodes;
            for (const auto& node : nodes) {
                requests[node.first].Hashes.emplace_back(queryHash);
                metricsNodes.insert(node.first);
            }

            if (!textsToFetch.contains(queryHash)) {
                continue;
            }

            if (nodes.size() <= TextReplicaCount) {
                for (const auto& node : nodes) {
                    requests[node.first].TextsToGet.emplace_back(queryHash);
                }
                continue;
            }

            std::unordered_set<TNodeId> used;
            while (used.size() < TextReplicaCount) {
                const auto nodeId = nodes[RandomNumber<ui64>(nodes.size())].first;
                if (used.insert(nodeId).second) {
                    requests[nodeId].TextsToGet.emplace_back(queryHash);
                }
            }
        }
    }

    void AddTopQueryRequests(TNodeRequests& requests) const {
        for (const auto& entry : Self->ByDurationMinute) {
            requests[entry.NodeId].ByDuration.emplace_back(entry.Hash);
        }
        for (const auto& entry : Self->ByReadBytesMinute) {
            requests[entry.NodeId].ByReadBytes.emplace_back(entry.Hash);
        }
        for (const auto& entry : Self->ByCpuTimeMinute) {
            requests[entry.NodeId].ByCpuTime.emplace_back(entry.Hash);
        }
        for (const auto& entry : Self->ByRequestUnitsMinute) {
            requests[entry.NodeId].ByRequestUnits.emplace_back(entry.Hash);
        }
    }

    void PersistNodeRequests(NIceDb::TNiceDb& db, TNodeRequests& requests) {
        Self->NodesToRequest.reserve(requests.size());
        for (auto& [nodeId, queries] : requests) {
            queries.NodeId = nodeId;

            auto serializeHashes = [] (const THashVector& hashes) {
                return TString(reinterpret_cast<const char*>(hashes.data()),
                    hashes.size() * sizeof(TQueryHash));
            };

            db.Table<Schema::NodesToRequest>().Key(nodeId).Update(
                NIceDb::TUpdate<Schema::NodesToRequest::QueryHashes>(
                    serializeHashes(queries.Hashes)),
                NIceDb::TUpdate<Schema::NodesToRequest::TextsToGet>(
                    serializeHashes(queries.TextsToGet)),
                NIceDb::TUpdate<Schema::NodesToRequest::ByDuration>(
                    serializeHashes(queries.ByDuration)),
                NIceDb::TUpdate<Schema::NodesToRequest::ByReadBytes>(
                    serializeHashes(queries.ByReadBytes)),
                NIceDb::TUpdate<Schema::NodesToRequest::ByCpuTime>(
                    serializeHashes(queries.ByCpuTime)),
                NIceDb::TUpdate<Schema::NodesToRequest::ByRequestUnits>(
                    serializeHashes(queries.ByRequestUnits)),
                NIceDb::TUpdate<Schema::NodesToRequest::IntervalEnd>(
                    Self->IntervalEnd.MicroSeconds()));

            Self->NodesToRequest.emplace_back(std::move(queries));
        }
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        SVLOG_D("[" << Self->TabletID() << "] TTxAggregate::Execute");

        NIceDb::TNiceDb db(txc.DB);

        auto deadline = Self->IntervalEnd + Self->TotalInterval;
        if (ctx.Now() >= deadline) {
            Self->Reset(db, ctx);
            return true;
        }

        const auto selectedHashes = SelectMetricCandidates();
        const auto textsToFetch = SelectTextsToFetch(selectedHashes);

        TNodeRequests nodesToRequest;
        std::unordered_set<TNodeId> metricsNodesToRequest;
        AddQueryMetricRequests(selectedHashes, textsToFetch,
            nodesToRequest, metricsNodesToRequest);
        AddTopQueryRequests(nodesToRequest);

        Self->QueryMetricsCoverage.SummaryNodes = Self->SummaryNodes.size();
        Self->QueryMetricsCoverage.ProcessorRetainedCpuTimeUs = 0;
        for (const auto& [_, query] : Self->Queries) {
            Self->QueryMetricsCoverage.ProcessorRetainedCpuTimeUs += query.Cpu;
        }

        Self->QueryMetricsCoverage.RequestedNodes = metricsNodesToRequest.size();
        PersistNodeRequests(db, nodesToRequest);

        Self->ClearIntervalSummaries(db);

        if (Self->NodesToRequest.empty()) {
            Self->PersistQueryResults(db);
        }

        Self->CurrentStage = AGGREGATE;
        Self->PersistStage(db);

        return true;
    }

    void Complete(const TActorContext&) override {
        SVLOG_D("[" << Self->TabletID() << "] TTxAggregate::Complete");

        if (Self->CurrentStage == COLLECT) {
            Self->ScheduleHourMetricsCleanup();
            Self->ScheduleAggregate();
        } else {
            Self->ScheduleCollect();
            if (!Self->NodesToRequest.empty()) {
                Self->ScheduleSendRequests();
            }
        }
    }
};

void TSysViewProcessor::Handle(TEvPrivate::TEvAggregate::TPtr&) {
    Execute(new TTxAggregate(this), TActivationContext::AsActorContext());
}

} // NSysView
} // NKikimr

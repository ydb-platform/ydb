#include "processor_impl.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::SYSTEM_VIEWS

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

    void AddQueryMetricRequests(const THashVector& selectedHashes,
        TNodeRequests& requests, std::unordered_set<TNodeId>& metricsNodes) const
    {
        static constexpr size_t TextReplicaCount = 3;

        for (auto queryHash : selectedHashes) {
            const auto& nodes = Self->Queries.at(queryHash).Nodes;
            for (const auto& node : nodes) {
                requests[node.first].Hashes.emplace_back(queryHash);
                metricsNodes.insert(node.first);
            }

            // Missing responses can change both public tops relative to the
            // summaries. Fetch text for every candidate that can enter them.
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
        YDB_LOG_DEBUG("TTxAggregate::Execute",
            {"tabletId", Self->TabletID()});

        NIceDb::TNiceDb db(txc.DB);

        auto deadline = Self->IntervalEnd + Self->TotalInterval;
        if (ctx.Now() >= deadline) {
            Self->Reset(db, ctx);
            return true;
        }

        const auto selectedHashes = SelectMetricCandidates();

        TNodeRequests nodesToRequest;
        std::unordered_set<TNodeId> metricsNodesToRequest;
        AddQueryMetricRequests(selectedHashes, nodesToRequest, metricsNodesToRequest);
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
        YDB_LOG_DEBUG("TTxAggregate::Complete",
            {"tabletId", Self->TabletID()});

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

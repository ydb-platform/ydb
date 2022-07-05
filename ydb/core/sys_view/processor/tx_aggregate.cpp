#include "processor_impl.h"

namespace NKikimr {
namespace NSysView {

struct TSysViewProcessor::TTxAggregate : public TTxBase {
    explicit TTxAggregate(TSelf* self)
        : TTxBase(self)
    {}

    TTxType GetTxType() const override { return TXTYPE_AGGREGATE; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        SVLOG_D("[" << Self->TabletID() << "] TTxAggregate::Execute");

        NIceDb::TNiceDb db(txc.DB);

        auto deadline = Self->IntervalEnd + Self->TotalInterval;
        if (ctx.Now() >= deadline) {
            Self->Reset(db, ctx);
            return true;
        }

        std::unordered_map<TNodeId, TNodeToQueries> nodesToRequest;
        size_t count = 0;
        for (auto it = Self->ByCpu.rbegin();
            it != Self->ByCpu.rend() && count < Self->TopCountLimit;
            ++it, ++count)
        {
            auto queryHash = it->second;
            const auto& nodes = Self->Queries[queryHash].Nodes;
            for (const auto& node : nodes) {
                nodesToRequest[node.first].Hashes.emplace_back(queryHash);
            }

            static constexpr size_t textReqsCount = 3;
            const auto nodesCount = nodes.size();
            if (nodesCount <= textReqsCount) {
                for (const auto& node : nodes) {
                    nodesToRequest[node.first].TextsToGet.emplace_back(queryHash);
                }
            } else {
                std::unordered_set<TNodeId> used;
                for (size_t count = 0; count < textReqsCount; ) {
                    auto nodeId = nodes[RandomNumber<ui64>(nodesCount)].first;
                    if (used.find(nodeId) != used.end()) {
                        continue;
                    }
                    nodesToRequest[nodeId].TextsToGet.emplace_back(queryHash);
                    used.insert(nodeId);
                    ++count;
                }
            }
        }

        for (auto& entry : Self->ByDurationMinute) {
            nodesToRequest[entry.NodeId].ByDuration.emplace_back(entry.Hash);
        }
        for (auto& entry : Self->ByReadBytesMinute) {
            nodesToRequest[entry.NodeId].ByReadBytes.emplace_back(entry.Hash);
        }
        for (auto& entry : Self->ByCpuTimeMinute) {
            nodesToRequest[entry.NodeId].ByCpuTime.emplace_back(entry.Hash);
        }
        for (auto& entry : Self->ByRequestUnitsMinute) {
            nodesToRequest[entry.NodeId].ByRequestUnits.emplace_back(entry.Hash);
        }

        Self->NodesToRequest.reserve(nodesToRequest.size());

        for (auto& [nodeId, queries] : nodesToRequest) {
            queries.NodeId = nodeId;
            auto& hashes = queries.Hashes;
            auto& texts = queries.TextsToGet;
            auto& byDuration = queries.ByDuration;
            auto& byReadBytes = queries.ByReadBytes;
            auto& byCpuTime = queries.ByCpuTime;
            auto& byRequestUnits = queries.ByRequestUnits;

            db.Table<Schema::NodesToRequest>().Key(nodeId).Update(
                NIceDb::TUpdate<Schema::NodesToRequest::QueryHashes>(
                    TString((char*)hashes.data(), hashes.size() * sizeof(TQueryHash))),
                NIceDb::TUpdate<Schema::NodesToRequest::TextsToGet>(
                    TString((char*)texts.data(), texts.size() * sizeof(TQueryHash))),
                NIceDb::TUpdate<Schema::NodesToRequest::ByDuration>(
                    TString((char*)byDuration.data(), byDuration.size() * sizeof(TQueryHash))),
                NIceDb::TUpdate<Schema::NodesToRequest::ByReadBytes>(
                    TString((char*)byReadBytes.data(), byReadBytes.size() * sizeof(TQueryHash))),
                NIceDb::TUpdate<Schema::NodesToRequest::ByCpuTime>(
                    TString((char*)byCpuTime.data(), byCpuTime.size() * sizeof(TQueryHash))),
                NIceDb::TUpdate<Schema::NodesToRequest::ByRequestUnits>(
                    TString((char*)byRequestUnits.data(), byRequestUnits.size() * sizeof(TQueryHash))));

            Self->NodesToRequest.emplace_back(std::move(queries));
        }

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

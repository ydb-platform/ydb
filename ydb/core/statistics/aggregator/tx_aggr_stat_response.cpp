#include "aggregator_impl.h"

#include <ydb/core/statistics/stat_service.h>

namespace NKikimr::NStat {

struct TStatisticsAggregator::TTxAggregateStatisticsResponse : public TTxBase {
    NKikimrStat::TEvAggregateStatisticsResponse Record;

    bool SendReqDistribution = false;
    bool SendAggregate = false;

    std::unique_ptr<TEvStatistics::TEvAggregateStatistics> Request;

    TTxAggregateStatisticsResponse(TSelf* self, NKikimrStat::TEvAggregateStatisticsResponse&& record)
        : TTxBase(self)
        , Record(std::move(record))
    {}

    TTxType GetTxType() const override { return TXTYPE_AGGR_STAT_RESPONSE; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxAggregateStatisticsResponse::Execute");

        ++Self->KeepAliveSeqNo; // cancel timeout events

        NIceDb::TNiceDb db(txc.DB);

        for (auto& column : Record.GetColumns()) {
            auto tag = column.GetTag();
            for (auto& statistic : column.GetStatistics()) {
                if (statistic.GetType() == NKikimr::NStat::COUNT_MIN_SKETCH) {
                    if (Self->ColumnNames.find(tag) == Self->ColumnNames.end()) {
                        continue;
                    }
                    if (Self->CountMinSketches.find(tag) == Self->CountMinSketches.end()) {
                        Self->CountMinSketches[tag].reset(TCountMinSketch::Create());
                    }

                    auto* data = statistic.GetData().Data();
                    auto* sketch = reinterpret_cast<const TCountMinSketch*>(data);
                    auto& current = Self->CountMinSketches[tag];
                    *current += *sketch;
                }
            }
        }

        if (Record.FailedTabletsSize() == 0 ||
            Self->TraversalRound >= Self->MaxTraversalRoundCount)
        {
            Self->SaveStatisticsToTable();
            return true;
        }

        std::unordered_map<ui32, std::unordered_set<ui64>> nonLocalTablets;

        Self->TabletsForReqDistribution.clear();
        for (auto& tablet : Record.GetFailedTablets()) {
            auto error = tablet.GetError();
            switch (error) {
            case NKikimrStat::TEvAggregateStatisticsResponse::UnavailableNode:
                Self->TabletsForReqDistribution.insert(tablet.GetTabletId());
                SendReqDistribution = true;
                break;
            case NKikimrStat::TEvAggregateStatisticsResponse::NonLocalTablet:
                auto nodeId = tablet.GetNodeId();
                if (nodeId == 0) {
                    // we cannot reach this tablet
                    Self->TabletsForReqDistribution.insert(tablet.GetTabletId());
                    SendReqDistribution = true;
                } else {
                    nonLocalTablets[nodeId].insert(tablet.GetTabletId());
                }
            }
        }
        if (SendReqDistribution) {
            return true;
        }

        Request = std::make_unique<TEvStatistics::TEvAggregateStatistics>();
        auto& outRecord = Request->Record;

        for (auto& [nodeId, tabletIds] : nonLocalTablets) {
            auto& outNode = *outRecord.AddNodes();
            outNode.SetNodeId(nodeId);
            outNode.MutableTabletIds()->Reserve(tabletIds.size());
            for (auto tabletId : tabletIds) {
                outNode.AddTabletIds(tabletId);
            }
        }

        ++Self->TraversalRound;
        ++Self->GlobalTraversalRound;
        Self->PersistGlobalTraversalRound(db);
        outRecord.SetRound(Self->GlobalTraversalRound);
        SendAggregate = true;

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxAggregateStatisticsResponse::Complete");

        if (SendReqDistribution) {
            ctx.Send(Self->SelfId(), new TEvPrivate::TEvRequestDistribution);
            return;
        }

        if (SendAggregate) {
            ctx.Send(MakeStatServiceID(Self->SelfId().NodeId()), Request.release());
            ctx.Schedule(KeepAliveTimeout, new TEvPrivate::TEvAckTimeout(++Self->KeepAliveSeqNo));
        }
    }
};

void TStatisticsAggregator::Handle(TEvStatistics::TEvAggregateStatisticsResponse::TPtr& ev) {
    auto& record = ev->Get()->Record;
    Execute(new TTxAggregateStatisticsResponse(this, std::move(record)),
        TActivationContext::AsActorContext());
}

} // NKikimr::NStat

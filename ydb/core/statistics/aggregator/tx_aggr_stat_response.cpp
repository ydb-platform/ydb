#include "aggregator_impl.h"

#include <ydb/core/statistics/service/service.h>

namespace NKikimr::NStat {

struct TStatisticsAggregator::TTxAggregateStatisticsResponse : public TTxBase {
    NKikimrStat::TEvAggregateStatisticsResponse Record;

    enum class EAction : ui8 {
        None,
        SendReqDistribution,
        SendAggregate,
    };
    EAction Action = EAction::None;

    std::unique_ptr<TEvStatistics::TEvAggregateStatistics> Request;

    TTxAggregateStatisticsResponse(TSelf* self, NKikimrStat::TEvAggregateStatisticsResponse&& record)
        : TTxBase(self)
        , Record(std::move(record))
    {}

    TTxType GetTxType() const override { return TXTYPE_AGGR_STAT_RESPONSE; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxAggregateStatisticsResponse::Execute");

        ++Self->KeepAliveSeqNo; // cancel timeout events

        Self->TabletCounters->Simple()[COUNTER_AGGREGATION_TIME].Set(0);
        Self->AggregationRequestBeginTime = TInstant::Zero();

        NIceDb::TNiceDb db(txc.DB);

        for (auto& column : Record.GetColumns()) {
            auto tag = column.GetTag();
            for (auto& statistic : column.GetStatistics()) {
                if (statistic.GetType() == NKikimr::NStat::COUNT_MIN_SKETCH) {
                    if (!Self->ColumnNames.contains(tag)) {
                        continue;
                    }

                    auto [currentIt, emplaced] = Self->CountMinSketches.try_emplace(tag);
                    if (emplaced) {
                        currentIt->second.reset(TCountMinSketch::Create());
                    }

                    auto* data = statistic.GetData().Data();
                    auto* sketch = reinterpret_cast<const TCountMinSketch*>(data);
                    *(currentIt->second) += *sketch;
                }
            }
        }

        if (Record.FailedTabletsSize() == 0 ||
            Self->TraversalRound >= Self->MaxTraversalRoundCount)
        {
            Self->SaveStatisticsToTable();
            return true;
        }

        std::unordered_map<ui32, std::vector<ui64>> nonLocalTablets;
        Self->TabletsForReqDistribution.clear();

        for (auto& tablet : Record.GetFailedTablets()) {
            auto error = tablet.GetError();
            switch (error) {
            case NKikimrStat::TEvAggregateStatisticsResponse::TYPE_UNSPECIFIED:
                SA_LOG_CRIT("[" << Self->TabletID() << "] Unspecified TEvAggregateStatisticsResponse status");
                return false;

            case NKikimrStat::TEvAggregateStatisticsResponse::TYPE_UNAVAILABLE_NODE:
                Self->TabletsForReqDistribution.insert(tablet.GetTabletId());
                Action = EAction::SendReqDistribution;
                break;

            case NKikimrStat::TEvAggregateStatisticsResponse::TYPE_NON_LOCAL_TABLET:
                auto nodeId = tablet.GetNodeId();
                if (nodeId == 0) {
                    // we cannot reach this tablet
                    Self->TabletsForReqDistribution.insert(tablet.GetTabletId());
                    Action = EAction::SendReqDistribution;

                } else if (Action != EAction::SendReqDistribution) {
                    nonLocalTablets[nodeId].push_back(tablet.GetTabletId());
                }
                break;
            }
        }

        if (Action == EAction::SendReqDistribution) {
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
        Action = EAction::SendAggregate;

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxAggregateStatisticsResponse::Complete");

        switch (Action) {
        case EAction::SendReqDistribution:
            ctx.Send(Self->SelfId(), new TEvPrivate::TEvRequestDistribution);
            break;

        case EAction::SendAggregate:
            ctx.Send(MakeStatServiceID(Self->SelfId().NodeId()), Request.release());
            ctx.Schedule(KeepAliveTimeout, new TEvPrivate::TEvAckTimeout(++Self->KeepAliveSeqNo));
            Self->AggregationRequestBeginTime = AppData(ctx)->TimeProvider->Now();
            break;

        default:
            break;
        }
    }
};

void TStatisticsAggregator::Handle(TEvStatistics::TEvAggregateStatisticsResponse::TPtr& ev) {
    auto& record = ev->Get()->Record;
    Execute(new TTxAggregateStatisticsResponse(this, std::move(record)),
        TActivationContext::AsActorContext());
}

} // NKikimr::NStat

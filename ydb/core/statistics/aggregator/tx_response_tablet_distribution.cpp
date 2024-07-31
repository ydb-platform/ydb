#include "aggregator_impl.h"

#include <ydb/core/protos/hive.pb.h>
#include <ydb/core/statistics/service/service.h>

namespace NKikimr::NStat {

struct TStatisticsAggregator::TTxResponseTabletDistribution : public TTxBase {
    NKikimrHive::TEvResponseTabletDistribution Record;

    enum class EAction : ui8 {
        None,
        SendAggregate,
        ScheduleResolve,
        ScheduleReqDistribution,
    };
    EAction Action = EAction::None;

    std::unique_ptr<TEvStatistics::TEvAggregateStatistics> Request;

    TTxResponseTabletDistribution(TSelf* self, NKikimrHive::TEvResponseTabletDistribution&& record)
        : TTxBase(self)
        , Record(std::move(record))
    {}

    TTxType GetTxType() const override { return TXTYPE_RESPONSE_TABLET_DISTRIBUTION; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxResponseTabletDistribution::Execute");

        NIceDb::TNiceDb db(txc.DB);

        Request = std::make_unique<TEvStatistics::TEvAggregateStatistics>();
        auto& outRecord = Request->Record;

        PathIdFromPathId(Self->TraversalTableId.PathId, outRecord.MutablePathId());

        bool hasTablets = false;
        for (auto& inNode : Record.GetNodes()) {
            if (inNode.GetNodeId() == 0) {
                // these tablets are probably in Hive boot queue
                if (Self->HiveRequestRound < Self->MaxHiveRequestRoundCount) {
                    Action = EAction::ScheduleReqDistribution;
                    return true;
                }
                continue;
            }
            auto& outNode = *outRecord.AddNodes();
            outNode.SetNodeId(inNode.GetNodeId());
            outNode.MutableTabletIds()->Reserve(inNode.TabletIdsSize());
            for (auto tabletId : inNode.GetTabletIds()) {
                outNode.AddTabletIds(tabletId);
                Self->TabletsForReqDistribution.erase(tabletId);
            }
            hasTablets = true;
        }

        if (!Self->TabletsForReqDistribution.empty() && Self->ResolveRound < Self->MaxResolveRoundCount) {
            // these tablets do not exist in Hive anymore
            Action = EAction::ScheduleResolve;
            return true;
        }

        if (!hasTablets) {
            Self->FinishTraversal(db);
            return true;
        }

        ++Self->TraversalRound;
        ++Self->GlobalTraversalRound;
        Self->PersistGlobalTraversalRound(db);
        outRecord.SetRound(Self->GlobalTraversalRound);
        Action = EAction::SendAggregate;

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxResponseTabletDistribution::Complete");

        switch (Action) {
        case EAction::ScheduleResolve:
            ctx.Schedule(ResolveRetryInterval, new TEvPrivate::TEvResolve());
            break;

        case EAction::ScheduleReqDistribution:
            ctx.Schedule(HiveRetryInterval, new TEvPrivate::TEvRequestDistribution());
            break;

        case EAction::SendAggregate:
            ctx.Send(MakeStatServiceID(Self->SelfId().NodeId()), Request.release());
            ctx.Schedule(KeepAliveTimeout, new TEvPrivate::TEvAckTimeout(++Self->KeepAliveSeqNo));
            break;

        default:
            break;
        }
    }
};

void TStatisticsAggregator::Handle(TEvHive::TEvResponseTabletDistribution::TPtr& ev) {
    auto& record = ev->Get()->Record;
    Execute(new TTxResponseTabletDistribution(this, std::move(record)),
        TActivationContext::AsActorContext());
}

} // NKikimr::NStat

#include "aggregator_impl.h"

#include <ydb/core/protos/hive.pb.h>
#include <ydb/core/statistics/service/service.h>

#include <util/string/vector.h>

namespace NKikimr::NStat {

struct TStatisticsAggregator::TTxResponseTabletDistribution : public TTxBase {
    const NKikimrHive::TEvResponseTabletDistribution HiveRecord;

    enum class EAction : ui8 {
        None,
        SendAggregate,
        ScheduleResolve,
        ScheduleReqDistribution,
    };
    EAction Action = EAction::None;

    std::unique_ptr<TEvStatistics::TEvAggregateStatistics> AggregateStatisticsRequest;

    TTxResponseTabletDistribution(TSelf* self, NKikimrHive::TEvResponseTabletDistribution&& hiveRecord)
        : TTxBase(self)
        , HiveRecord(std::move(hiveRecord))
    {}

    TTxType GetTxType() const override { return TXTYPE_RESPONSE_TABLET_DISTRIBUTION; }

    bool ExecuteStartForceTraversal(TTransactionContext& txc) {
        ++Self->TraversalRound;
        ++Self->GlobalTraversalRound;
        
        NIceDb::TNiceDb db(txc.DB);
        Self->PersistGlobalTraversalRound(db);

        AggregateStatisticsRequest = std::make_unique<TEvStatistics::TEvAggregateStatistics>(); 
        auto& outRecord = AggregateStatisticsRequest->Record;
        outRecord.SetRound(Self->GlobalTraversalRound);
        PathIdFromPathId(Self->TraversalPathId, outRecord.MutablePathId());

        const auto forceTraversalTable = Self->CurrentForceTraversalTable();
        if (forceTraversalTable) {
            TVector<ui32> columnTags = Scan<ui32>(SplitString(forceTraversalTable->ColumnTags, ","));
            outRecord.MutableColumnTags()->Add(columnTags.begin(), columnTags.end());
        }

        for (auto& inNode : HiveRecord.GetNodes()) {
            auto& outNode = *outRecord.AddNodes();
            outNode.SetNodeId(inNode.GetNodeId());
            outNode.MutableTabletIds()->CopyFrom(inNode.GetTabletIds());
        }

        return true;        
    }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxResponseTabletDistribution::Execute");

        auto distribution = Self->TabletsForReqDistribution;
        for (auto& inNode : HiveRecord.GetNodes()) {
            if (inNode.GetNodeId() == 0) {
                // these tablets are probably in Hive boot queue
                if (Self->HiveRequestRound < Self->MaxHiveRequestRoundCount) {
                    Action = EAction::ScheduleReqDistribution;
                }
                continue;
            }
            for (auto tabletId : inNode.GetTabletIds()) {
                distribution.erase(tabletId);
            }
        }

        if (Action == EAction::ScheduleReqDistribution) {
            return true;
        }

        if (!distribution.empty() && Self->ResolveRound < Self->MaxResolveRoundCount) {
            // these tablets do not exist in Hive anymore
            Self->NavigatePathId = Self->TraversalPathId;
            Action = EAction::ScheduleResolve;
            return true;
        }

        Action = EAction::SendAggregate;
        return ExecuteStartForceTraversal(txc);
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
            ctx.Send(MakeStatServiceID(Self->SelfId().NodeId()), AggregateStatisticsRequest.release());
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

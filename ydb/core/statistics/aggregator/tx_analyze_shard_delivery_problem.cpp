#include "aggregator_impl.h"

#include <ydb/core/protos/hive.pb.h>
#include <ydb/core/statistics/service/service.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::STATISTICS

namespace NKikimr::NStat {

struct TStatisticsAggregator::TTxAnalyzeShardDeliveryProblem : public TTxBase {
    TTxAnalyzeShardDeliveryProblem(TSelf* self)
        : TTxBase(self)
    {}

    TTxType GetTxType() const override { return TXTYPE_ANALYZE_SHARD_DELIVERY_PROBLEM; }

    bool Execute(TTransactionContext&, const TActorContext&) override {
        YDB_LOG_TRACE("TTxAnalyzeShardDeliveryProblem::Execute",
            {"tabletId", Self->TabletID()});

        for (TForceTraversalOperation& operation : Self->ForceTraversals) {
            for (TForceTraversalTable& operationTable : operation.Tables) {
                for(TAnalyzedShard& analyzedShard : operationTable.AnalyzedShards) {
                    if (analyzedShard.Status == TAnalyzedShard::EStatus::DeliveryProblem) {
                        YDB_LOG_DEBUG("Reset DeliveryProblem",
                            {"tabletId", Self->TabletID()},
                            {"columnShard", analyzedShard.ShardTabletId});
                        analyzedShard.Status = TAnalyzedShard::EStatus::None;
                    }
                }
            }
        }

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        YDB_LOG_TRACE("TTxAnalyzeShardDeliveryProblem::Complete",
            {"tabletId", Self->TabletID()});

        ctx.Schedule(AnalyzeDeliveryProblemPeriod, new TEvPrivate::TEvAnalyzeDeliveryProblem());
    }
};

void TStatisticsAggregator::Handle(TEvPrivate::TEvAnalyzeDeliveryProblem::TPtr&) {
    Execute(new TTxAnalyzeShardDeliveryProblem(this),
        TActivationContext::AsActorContext());
}

} // NKikimr::NStat

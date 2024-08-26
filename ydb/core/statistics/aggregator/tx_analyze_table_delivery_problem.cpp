#include "aggregator_impl.h"

#include <ydb/core/protos/hive.pb.h>
#include <ydb/core/statistics/service/service.h>

#include <util/string/vector.h>

namespace NKikimr::NStat {

struct TStatisticsAggregator::TTxAnalyzeTableDeliveryProblem : public TTxBase {
    TTxAnalyzeTableDeliveryProblem(TSelf* self)
        : TTxBase(self)
    {}

    TTxType GetTxType() const override { return TXTYPE_ANALYZE_TABLE_DELIVERY_PROBLEM; }

    bool Execute(TTransactionContext&, const TActorContext&) override {
        SA_LOG_T("[" << Self->TabletID() << "] TTxAnalyzeTableDeliveryProblem::Execute");

        for (TForceTraversalOperation& operation : Self->ForceTraversals) {
            for (TForceTraversalTable& operationTable : operation.Tables) {
                for(TAnalyzedShard& analyzedShard : operationTable.AnalyzedShards) {
                    if (analyzedShard.Status == TAnalyzedShard::EStatus::DeliveryProblem) {
                        SA_LOG_D("[" << Self->TabletID() << "] Reset DeliveryProblem to ColumnShard=" << analyzedShard.ShardTabletId);
                        analyzedShard.Status = TAnalyzedShard::EStatus::None;
                    }
                }
            }
        }        

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        SA_LOG_T("[" << Self->TabletID() << "] TTxAnalyzeTableDeliveryProblem::Complete");

        ctx.Schedule(AnalyzeDeliveryProblemPeriod, new TEvPrivate::TEvAnalyzeDeliveryProblem());
    }
};

void TStatisticsAggregator::Handle(TEvPrivate::TEvAnalyzeDeliveryProblem::TPtr&) {
    Execute(new TTxAnalyzeTableDeliveryProblem(this),
        TActivationContext::AsActorContext());
}

} // NKikimr::NStat

#include "aggregator_impl.h"

#include <ydb/core/tx/datashard/datashard.h>

namespace NKikimr::NStat {

struct TStatisticsAggregator::TTxAnalyzeTable : public TTxBase {
    TPathId PathId;
    TActorId ReplyToActorId;

    TTxAnalyzeTable(TSelf* self, const TPathId& pathId, TActorId replyToActorId)
        : TTxBase(self)
        , PathId(pathId)
        , ReplyToActorId(replyToActorId)
    {}

    TTxType GetTxType() const override { return TXTYPE_ANALYZE_TABLE; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxAnalyzeTable::Execute");

        if (!Self->EnableColumnStatistics) {
            return true;
        }

        auto itOp = Self->ForceTraversalsByPathId.find(PathId);
        if (itOp != Self->ForceTraversalsByPathId.end()) {
            itOp->second.ReplyToActorIds.insert(ReplyToActorId);
            return true;
        }

        NIceDb::TNiceDb db(txc.DB);

        TForceTraversal& operation = Self->ForceTraversalsByPathId[PathId];
        operation.PathId = PathId;
        operation.OperationId = ++Self->LastForceTraversalOperationId;
        operation.ReplyToActorIds.insert(ReplyToActorId);
        Self->ForceTraversals.PushBack(&operation);

        Self->PersistLastForceTraversalOperationId(db);

        db.Table<Schema::ForceTraversals>().Key(operation.OperationId).Update(
            NIceDb::TUpdate<Schema::ForceTraversals::OwnerId>(PathId.OwnerId),
            NIceDb::TUpdate<Schema::ForceTraversals::LocalPathId>(PathId.LocalPathId));

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxAnalyzeTable::Complete");
    }
};

void TStatisticsAggregator::Handle(TEvStatistics::TEvAnalyze::TPtr& ev) {
    const auto& record = ev->Get()->Record;

    // TODO: replace by queue
    for (const auto& table : record.GetTables()) {
        Execute(new TTxAnalyzeTable(this, PathIdFromPathId(table.GetPathId()), ev->Sender), TActivationContext::AsActorContext());
    }

}

} // NKikimr::NStat

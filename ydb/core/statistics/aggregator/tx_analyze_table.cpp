#include "aggregator_impl.h"

#include <ydb/core/tx/datashard/datashard.h>

namespace NKikimr::NStat {

struct TStatisticsAggregator::TTxAnalyzeTable : public TTxBase {
    TPathId PathId;
    TActorId ReplyToActorId;
    ui64 OperationId = 0;

    TTxAnalyzeTable(TSelf* self, const TPathId& pathId, TActorId replyToActorId)
        : TTxBase(self)
        , PathId(pathId)
        , ReplyToActorId(replyToActorId)
    {}

    TTxType GetTxType() const override { return TXTYPE_SCAN_TABLE; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxAnalyzeTable::Execute");

        if (!Self->EnableColumnStatistics) {
            return true;
        }

        auto itOp = Self->ScanOperationsByPathId.find(PathId);
        if (itOp != Self->ScanOperationsByPathId.end()) {
            itOp->second.ReplyToActorIds.insert(ReplyToActorId);
            OperationId = itOp->second.OperationId;
            return true;
        }

        NIceDb::TNiceDb db(txc.DB);

        TScanOperation& operation = Self->ScanOperationsByPathId[PathId];
        operation.PathId = PathId;
        operation.OperationId = ++Self->LastScanOperationId;
        operation.ReplyToActorIds.insert(ReplyToActorId);
        Self->ScanOperations.PushBack(&operation);

        Self->PersistLastScanOperationId(db);

        db.Table<Schema::ScanOperations>().Key(operation.OperationId).Update(
            NIceDb::TUpdate<Schema::ScanOperations::OwnerId>(PathId.OwnerId),
            NIceDb::TUpdate<Schema::ScanOperations::LocalPathId>(PathId.LocalPathId));

        OperationId = operation.OperationId;

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

#include "aggregator_impl.h"

#include <ydb/core/tx/datashard/datashard.h>

namespace NKikimr::NStat {

struct TStatisticsAggregator::TTxScanTable : public TTxBase {
    NKikimrStat::TEvScanTable Record;
    TActorId ReplyToActorId;
    ui64 OperationId = 0;

    TTxScanTable(TSelf* self, NKikimrStat::TEvScanTable&& record, TActorId replyToActorId)
        : TTxBase(self)
        , Record(std::move(record))
        , ReplyToActorId(replyToActorId)
    {}

    TTxType GetTxType() const override { return TXTYPE_SCAN_TABLE; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxScanTable::Execute");

        auto pathId = PathIdFromPathId(Record.GetPathId());

        auto itOp = Self->ScanOperationsByPathId.find(pathId);
        if (itOp != Self->ScanOperationsByPathId.end()) {
            itOp->second.ReplyToActorIds.insert(ReplyToActorId);
            OperationId = itOp->second.OperationId;
            return true;
        }

        NIceDb::TNiceDb db(txc.DB);

        TScanOperation& operation = Self->ScanOperationsByPathId[pathId];
        operation.PathId = pathId;
        operation.OperationId = ++Self->LastScanOperationId;
        operation.ReplyToActorIds.insert(ReplyToActorId);
        Self->ScanOperations.PushBack(&operation);

        Self->PersistLastScanOperationId(db);

        db.Table<Schema::ScanOperations>().Key(operation.OperationId).Update(
            NIceDb::TUpdate<Schema::ScanOperations::OwnerId>(pathId.OwnerId),
            NIceDb::TUpdate<Schema::ScanOperations::LocalPathId>(pathId.LocalPathId));

        OperationId = operation.OperationId;

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxScanTable::Complete");

        auto accepted = std::make_unique<TEvStatistics::TEvScanTableAccepted>();
        accepted->Record.SetOperationId(OperationId);
        ctx.Send(ReplyToActorId, accepted.release());
    }
};

void TStatisticsAggregator::Handle(TEvStatistics::TEvScanTable::TPtr& ev) {
    auto& record = ev->Get()->Record;
    Execute(new TTxScanTable(this, std::move(record), ev->Sender),
        TActivationContext::AsActorContext());
}

} // NKikimr::NStat

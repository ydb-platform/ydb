#include "aggregator_impl.h"

#include <ydb/core/tx/datashard/datashard.h>

namespace NKikimr::NStat {

struct TStatisticsAggregator::TTxScanTable : public TTxBase {
    NKikimrStat::TEvScanTable Record;
    TActorId ReplyToActorId;
    ui64 Cookie = 0;

    TTxScanTable(TSelf* self, NKikimrStat::TEvScanTable&& record, TActorId replyToActorId, ui64 cookie)
        : TTxBase(self)
        , Record(std::move(record))
        , ReplyToActorId(replyToActorId)
        , Cookie(cookie)
    {}

    TTxType GetTxType() const override { return TXTYPE_SCAN_TABLE; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxScanTable::Execute");

        auto pathId = PathIdFromPathId(Record.GetPathId());

        if (Self->ScanOperationsPathIds.find(pathId) != Self->ScanOperationsPathIds.end()) {
            ctx.Send(ReplyToActorId, new TEvStatistics::TEvScanTableAccepted, 0, Cookie);
            return true;
        }

        NIceDb::TNiceDb db(txc.DB);

        TScanOperation operation;
        operation.PathId = pathId;
        operation.OperationId = ++Self->LastScanOperationId;
        Self->PersistLastScanOperationId(db);
        operation.ReplyToActorId = ReplyToActorId;

        db.Table<Schema::ScanOperations>().Key(operation.OperationId).Update(
            NIceDb::TUpdate<Schema::ScanOperations::OwnerId>(pathId.OwnerId),
            NIceDb::TUpdate<Schema::ScanOperations::LocalPathId>(pathId.LocalPathId));

        Self->ScanOperations.push(operation);
        Self->ScanOperationsPathIds.insert(pathId);

        ctx.Send(ReplyToActorId, new TEvStatistics::TEvScanTableAccepted, 0, Cookie);
        return true;
    }

    void Complete(const TActorContext&) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxScanTable::Complete");
    }
};

void TStatisticsAggregator::Handle(TEvStatistics::TEvScanTable::TPtr& ev) {
    auto& record = ev->Get()->Record;
    Execute(new TTxScanTable(this, std::move(record), ev->Sender, ev->Cookie),
        TActivationContext::AsActorContext());
}

} // NKikimr::NStat

#include "aggregator_impl.h"

#include <ydb/core/tx/datashard/datashard.h>

namespace NKikimr::NStat {

struct TStatisticsAggregator::TTxScanTable : public TTxBase {
    NKikimrStat::TEvScanTable Record;
    TActorId ReplyToActorId;

    TTxScanTable(TSelf* self, NKikimrStat::TEvScanTable&& record, TActorId replyToActorId)
        : TTxBase(self)
        , Record(std::move(record))
        , ReplyToActorId(replyToActorId)
    {}

    TTxType GetTxType() const override { return TXTYPE_SCAN_TABLE; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxScanTable::Execute");

        Self->ReplyToActorId = ReplyToActorId;

        NIceDb::TNiceDb db(txc.DB);
        Self->ScanTableId.PathId = PathIdFromPathId(Record.GetPathId());
        Self->PersistScanTableId(db);

        Self->ScanStartTime = TInstant::Now();
        Self->PersistScanStartTime(db);

        return true;
    }

    void Complete(const TActorContext&) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxScanTable::Complete");

        Self->InitStartKey = true;
        Self->Navigate();
    }
};

void TStatisticsAggregator::Handle(TEvStatistics::TEvScanTable::TPtr& ev) {
    if (ScanTableId.PathId) {
        return; // scan is in progress
    }
    TActorId sender;
    if (ev->Sender != SelfId()) {
        sender = ev->Sender;
    }
    auto& record = ev->Get()->Record;
    Execute(new TTxScanTable(this, std::move(record), sender),
        TActivationContext::AsActorContext());
}

} // NKikimr::NStat

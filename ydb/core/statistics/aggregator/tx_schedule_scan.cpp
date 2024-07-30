#include "aggregator_impl.h"

#include <ydb/core/tx/datashard/datashard.h>

namespace NKikimr::NStat {

struct TStatisticsAggregator::TTxScheduleScan : public TTxBase {
    TTxScheduleScan(TSelf* self)
        : TTxBase(self)
    {}

    TTxType GetTxType() const override { return TXTYPE_SCHEDULE_SCAN; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        SA_LOG_T("[" << Self->TabletID() << "] TTxScheduleScan::Execute");

        Self->Schedule(Self->ScheduleScanIntervalTime, new TEvPrivate::TEvScheduleScan());

        if (!Self->EnableColumnStatistics) {
            return true;
        }

        if (Self->ScanTableId.PathId) {
            return true; // scan is in progress
        }

        NIceDb::TNiceDb db(txc.DB);
        Self->ScheduleNextScan(db);
        return true;
    }

    void Complete(const TActorContext&) override {
        SA_LOG_T("[" << Self->TabletID() << "] TTxScheduleScan::Complete");
    }
};

void TStatisticsAggregator::Handle(TEvPrivate::TEvScheduleScan::TPtr&) {
    Execute(new TTxScheduleScan(this), TActivationContext::AsActorContext());
}

} // NKikimr::NStat

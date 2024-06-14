#include "aggregator_impl.h"

#include <ydb/core/tx/datashard/datashard.h>

namespace NKikimr::NStat {

struct TStatisticsAggregator::TTxScheduleScan : public TTxBase {
    TTxScheduleScan(TSelf* self)
        : TTxBase(self)
    {}

    TTxType GetTxType() const override { return TXTYPE_SCHEDULE_SCAN; }

    bool Execute(TTransactionContext&, const TActorContext&) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxScheduleScan::Execute");
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxScheduleScan::Complete");

        if (Self->ScanTablesByTime.empty()) {
            return;
        }

        auto& topTable = Self->ScanTablesByTime.top();
        auto evScan = std::make_unique<TEvStatistics::TEvScanTable>();
        PathIdFromPathId(topTable.PathId, evScan->Record.MutablePathId());

        ctx.Send(Self->SelfId(), evScan.release());
    }
};

void TStatisticsAggregator::Handle(TEvPrivate::TEvScheduleScan::TPtr&) {
    if (ScanTableId.PathId) {
        return; // scan is in progress
    }
    Execute(new TTxScheduleScan(this), TActivationContext::AsActorContext());
}

} // NKikimr::NStat

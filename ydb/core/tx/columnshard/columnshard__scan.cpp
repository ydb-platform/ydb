#include "columnshard__scan.h"
#include "columnshard.h"
#include "columnshard_impl.h"
#include "engines/reader/transaction/tx_scan.h"
#include "engines/reader/transaction/tx_internal_scan.h"

#include <ydb/core/tx/columnshard/counters/common/durations.h>

#include <ydb/core/protos/kqp.pb.h>

namespace NKikimr::NColumnShard {

void TColumnShard::Handle(TEvColumnShard::TEvScan::TPtr& ev, const TActorContext& ctx) {
    static auto dCounter = TDurationController::CreateController("start_scan");
    TDurationController::TGuard dGuard(dCounter);
    auto& record = ev->Get()->Record;
    ui64 txId = record.GetTxId();
    const auto& scanId = record.GetScanId();
    const auto& snapshot = record.GetSnapshot();

    NOlap::TSnapshot readVersion(snapshot.GetStep(), snapshot.GetTxId());
    NOlap::TSnapshot maxReadVersion = GetMaxReadVersion();

    LOG_S_DEBUG("EvScan txId: " << txId
        << " scanId: " << scanId
        << " version: " << readVersion
        << " readable: " << maxReadVersion
        << " at tablet " << TabletID());

    if (maxReadVersion < readVersion) {
        WaitingScans.emplace(readVersion, std::move(ev));
        WaitPlanStep(readVersion.GetPlanStep());
        return;
    }

    LastAccessTime = TAppData::TimeProvider->Now();
    ScanTxInFlight.insert({txId, LastAccessTime});
    SetCounter(COUNTER_SCAN_IN_FLY, ScanTxInFlight.size());
    Execute(new NOlap::NReader::TTxScan(this, ev), ctx);
}

void TColumnShard::Handle(TEvColumnShard::TEvInternalScan::TPtr& ev, const TActorContext& ctx) {
    static auto dCounter = TDurationController::CreateController("start_internal_scan");
    TDurationController::TGuard dGuard(dCounter);
    Execute(new NOlap::NReader::TTxInternalScan(this, ev), ctx);
}

}

#include "columnshard.h"
#include "columnshard__scan.h"
#include "columnshard_impl.h"

#include "engines/predicate/filter.h"
#include "engines/reader/transaction/tx_internal_scan.h"
#include "engines/reader/transaction/tx_scan.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/kqp/compute_actor/kqp_compute_events.h>
#include <ydb/core/protos/kqp.pb.h>

#include <ydb/library/actors/core/log.h>

#define YDB_LOG_THIS_FILE_COMPONENT TX_COLUMNSHARD

namespace NKikimr::NColumnShard {

void TColumnShard::Handle(TEvDataShard::TEvKqpScan::TPtr& ev, const TActorContext& ctx) {
    auto& record = ev->Get()->Record;
    ui64 txId = record.GetTxId();
    const auto& scanId = record.GetScanId();
    const auto& snapshot = record.GetSnapshot();

    NOlap::TSnapshot readVersion(snapshot.GetStep(), snapshot.GetTxId());
    NOlap::TSnapshot maxReadVersion = GetMaxReadVersion();

    YDB_LOG_DEBUG("EvScan at tablet",
        {"txId", txId},
        {"scanId", scanId},
        {"version", readVersion},
        {"readable", maxReadVersion},
        {"tabletID", TabletID()});

    if (!ScanGate.PassReadableSnapshot(ev, readVersion, maxReadVersion)) {
        WaitPlanStep(readVersion.GetPlanStep());
        return;
    }

    auto targetConclusion = NOlap::NReader::BuildScanTarget(*this, record);
    if (targetConclusion.IsFail()) {
        NOlap::NReader::SendScanError(TabletID(), ev->Sender, record.GetGeneration(), record.GetTablePath(), "cannot build scan target",
            targetConclusion.GetErrorMessage(), ctx);
        return;
    }
    NOlap::NReader::TScanTarget target = targetConclusion.DetachResult();
    if (!ScanGate.PassLoadedPortions(ev, target)) {
        return;
    }

    ScanTxInFlight.insert({ txId, TAppData::TimeProvider->Now() });
    Counters.GetTabletCounters()->SetCounter(COUNTER_SCAN_IN_FLY, ScanTxInFlight.size());
    Execute(new NOlap::NReader::TTxScan(this, ev, std::move(target)), ctx);
}

void TColumnShard::Handle(TEvColumnShard::TEvInternalScan::TPtr& ev, const TActorContext& ctx) {
    const auto& request = *ev->Get();
    const NOlap::TSnapshot readSnapshot =
        TablesManager.ResolveReadSnapshot(request.GetPathId().GetSchemeShardLocalPathId(), request.GetSnapshot());
    NOlap::NReader::TScanTarget target{ readSnapshot,
        TablesManager.BuildTableMetadataAccessor(
            "internal_request", request.GetPathId().GetInternalPathId(), request.GetPathId().GetSchemeShardLocalPathId(), readSnapshot),
        request.RangesFilter ? request.RangesFilter : std::make_shared<NOlap::TPKRangesFilter>(NOlap::TPKRangesFilter::BuildEmpty()) };
    if (!ScanGate.PassLoadedPortions(ev, target)) {
        return;
    }
    Execute(new NOlap::NReader::TTxInternalScan(this, ev, std::move(target)), ctx);
}

}   // namespace NKikimr::NColumnShard

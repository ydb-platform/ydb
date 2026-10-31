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

    if (!ScanGate.PassSnapshot(ev, readVersion, maxReadVersion)) {
        WaitPlanStep(readVersion.GetPlanStep());
        return;
    }

    const auto sendError = [&](const TString& problem, const TString& details) {
        NOlap::NReader::SendScanError(TabletID(), ev->Sender, record.GetGeneration(), record.GetTablePath(), problem, details, ctx);
    };

    const TSchemeShardLocalPathId ssPathId = TSchemeShardLocalPathId::FromProto(record);
    const NOlap::TSnapshot readSnapshot = NOlap::NReader::TTxScan::GetSnapshot(*this, record, ssPathId);

    auto accessor =
        TablesManager.BuildTableMetadataAccessor(record.GetTablePath() ? record.GetTablePath() : "undefined", ssPathId, readSnapshot);
    if (accessor.IsFail()) {
        return sendError("cannot build table metadata accessor for request: " + accessor.GetErrorMessage(),
            AppDataVerified().ColumnShardConfig.GetReaderClassName());
    }

    auto ranges = NOlap::NReader::TTxScan::MakeRanges(*this, record, *accessor.GetResult());
    if (ranges.IsFail()) {
        return sendError("cannot build ranges filter", ranges.GetErrorMessage());
    }

    NOlap::NReader::TScanTarget target{ readSnapshot, accessor.DetachResult(), ranges.DetachResult() };
    if (!ScanGate.PassLoadedPortions(ev, GetIndexOptional(), target)) {
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
    // with an internal path id the accessor is a user table or an absent one: it never fails
    auto accessor = TablesManager.BuildTableMetadataAccessor(
        "internal_request", request.GetPathId().GetInternalPathId(), request.GetPathId().GetSchemeShardLocalPathId(), readSnapshot);
    NOlap::NReader::TScanTarget target{ readSnapshot, accessor.DetachResult(),
        request.RangesFilter ? request.RangesFilter : std::make_shared<NOlap::TPKRangesFilter>(NOlap::TPKRangesFilter::BuildEmpty()) };
    if (!ScanGate.PassLoadedPortions(ev, GetIndexOptional(), target)) {
        return;
    }
    Execute(new NOlap::NReader::TTxInternalScan(this, ev, std::move(target)), ctx);
}

}   // namespace NKikimr::NColumnShard

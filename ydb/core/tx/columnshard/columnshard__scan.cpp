#include "columnshard__scan.h"
#include <ydb/core/kqp/compute_actor/kqp_compute_events.h>
#include "columnshard.h"
#include "columnshard_impl.h"
#include "engines/reader/sys_view/abstract/policy.h"
#include "engines/reader/transaction/tx_scan.h"
#include "engines/reader/transaction/tx_internal_scan.h"

#include <ydb/core/protos/kqp.pb.h>
#include <ydb/core/base/appdata_fwd.h>

namespace NKikimr::NColumnShard {

void TColumnShard::Handle(TEvDataShard::TEvKqpScan::TPtr& ev, const TActorContext& ctx) {
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

    const auto schemeShardLocalPath = TSchemeShardLocalPathId::FromProto(record);
    auto internalPathId = TablesManager.ResolveInternalPathId(schemeShardLocalPath);
    if (!internalPathId && NOlap::NReader::NSysView::NAbstract::ISysViewPolicy::BuildByPath(record.GetTablePath())) {
        internalPathId = TInternalPathId::FromRawValue(schemeShardLocalPath.GetRawValue());  //TODO register ColumnStore in tablesmanager
    }
    if (!internalPathId) {
        const auto& request = ev->Get()->Record;
        auto error = MakeHolder<NKqp::TEvKqpCompute::TEvScanError>(request.GetGeneration(), TabletID());
        error->Record.SetStatus(Ydb::StatusIds::BAD_REQUEST);
        auto issue = NYql::YqlIssue({}, NYql::TIssuesIds::KIKIMR_BAD_REQUEST,TStringBuilder() << "table: " << request.GetTablePath() << "not found");
        NYql::IssueToMessage(issue, error->Record.MutableIssues()->Add());

        ctx.Send(ev->Sender, error.Release());
        return;
    }
    Counters.GetColumnTablesCounters()->GetPathIdCounter(*internalPathId)->OnReadEvent();
    ScanTxInFlight.insert({txId, TAppData::TimeProvider->Now()});
    Counters.GetTabletCounters()->SetCounter(COUNTER_SCAN_IN_FLY, ScanTxInFlight.size());
    Execute(new NOlap::NReader::TTxScan(this, ev), ctx);
}

void TColumnShard::Handle(TEvColumnShard::TEvInternalScan::TPtr& ev, const TActorContext& ctx) {
    Execute(new NOlap::NReader::TTxInternalScan(this, ev), ctx);
}

}

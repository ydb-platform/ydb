#include "columnshard_impl.h"

#include "local_db_backup/backup_actor.h"
#include "local_db_backup/events.h"

#include <ydb/core/tablet_flat/flat_executor_backup.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD

namespace NKikimr::NColumnShard {

namespace {

constexpr ui64 LocalDbBackupScanCookieBase = 1ull << 40;   // scan cookie = base + tableId

}   // namespace

bool TColumnShard::HandleLocalDbBackupPage(NMon::TEvRemoteHttpInfo::TPtr& ev, const TActorContext& ctx) {
    if (CurrentStateFunc() != &TThis::StateWork) {
        if (ev->Get()->Cgi().Get("format") == "json") {
            ctx.Send(ev->Sender, new NMon::TEvRemoteJsonInfoRes(R"({"active":false,"error":"tablet is initializing"})"));
        } else {
            ctx.Send(ev->Sender, new NMon::TEvRemoteHttpInfoRes(
                                     TStringBuilder() << "<h3>Local DB backup to S3</h3><p>The tablet is initializing, try again later.</p>"
                                                      << "<a href=\"app?TabletID=" << TabletID() << "\">&larr; Tablet main page</a>"));
        }
        return true;
    }
    if (!LocalDbBackupActorId) {
        LocalDbBackupActorId = ctx.Register(NLocalDbBackup::CreateLocalDbBackupActor(SelfId(), TabletID()).release());
    }
    ctx.Send(ev->Forward(LocalDbBackupActorId));
    return true;
}

void TColumnShard::Handle(NLocalDbBackup::TEvStartBackup::TPtr& ev, const TActorContext& ctx) {
    CancelLocalDbBackupScans();

    // All the scan snapshots are taken synchronously in QueueScan, within this single activation: no transaction
    // can be committed in between, so the snapshots of all the tables, the scheme and Generation/Step below are
    // consistent with each other.
    const auto& scheme = Executor()->Scheme();
    auto started = std::make_unique<NLocalDbBackup::TEvBackupStarted>();
    started->Generation = Executor()->Generation();
    started->Step = Executor()->Step();
    started->Schema = scheme.GetSnapshot();
    for (const auto& [tableId, table] : scheme.Tables) {
        auto options = NTabletFlatExecutor::TScanOptions().SetResourceBroker("system_tablet_backup", 10);
        auto* scan =
            NTabletFlatExecutor::NBackup::CreateSnapshotScan(ev->Get()->Writer, tableId, table.Columns, nullptr, /*workBudgetPercent=*/100);
        LocalDbBackupScans[tableId] = Executor()->QueueScan(tableId, scan, LocalDbBackupScanCookieBase + tableId, options);
        started->Tables[tableId] = table.Name;
    }
    ctx.Send(ev->Sender, started.release());
}

void TColumnShard::Handle(NLocalDbBackup::TEvCancelScans::TPtr& /*ev*/, const TActorContext& /*ctx*/) {
    CancelLocalDbBackupScans();
}

void TColumnShard::ScanComplete(NTable::EStatus status, TAutoPtr<IDestructable> /*prod*/, ui64 cookie, const TActorContext& /*ctx*/) {
    if (cookie >= LocalDbBackupScanCookieBase) {
        LocalDbBackupScans.erase(static_cast<ui32>(cookie - LocalDbBackupScanCookieBase));
        return;
    }

    const TString error = TStringBuilder() << "Unexpected scan completion: ColumnShard queues no scans except the local DB backup, cookie "
                                           << cookie << ", status " << static_cast<ui32>(status);
    YDB_LOG_ERROR(error, {"tabletId", TabletID()});
}

void TColumnShard::CancelLocalDbBackupScans() {
    for (const auto& [tableId, taskId] : LocalDbBackupScans) {
        Executor()->CancelScan(tableId, taskId);
    }
    LocalDbBackupScans.clear();
}

void TColumnShard::StopLocalDbBackup(const TActorContext& ctx) {
    if (LocalDbBackupActorId) {
        ctx.Send(LocalDbBackupActorId, new TEvents::TEvPoison);
        LocalDbBackupActorId = {};
    }
    CancelLocalDbBackupScans();
}

}   // namespace NKikimr::NColumnShard

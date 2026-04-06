#include "schemeshard_backup.h"
#include "schemeshard_impl.h"

#include <ydb/core/backup/impl/logging.h>

namespace NKikimr::NSchemeShard {

using namespace NTabletFlatExecutor;

struct TSchemeShard::TIncrementalRestore::TTxGet: public NTabletFlatExecutor::TTransactionBase<TSchemeShard>{
public:
    explicit TTxGet(TSelf* self, TEvBackup::TEvGetBackupCollectionRestoreRequest::TPtr& ev)
        : TBase(self)
        , Request(ev)
    {}

    const char* GetLogPrefix() const {
        return "TIncrementalRestore::TTxGet: ";
    }

    TTxType GetTxType() const override {
        return TXTYPE_GET_BACKUP_COLLECTION_RESTORE;
    }

    void Fill(NKikimrBackup::TBackupCollectionRestore& restore, const TIncrementalRestoreState& restoreInfo) {
        restore.SetId(restoreInfo.OriginalOperationId);
        restore.SetStatus(Ydb::StatusIds::SUCCESS);

        switch (restoreInfo.State) {
            case TIncrementalRestoreState::EState::Completed:
                restore.SetProgress(Ydb::Backup::RestoreProgress::PROGRESS_DONE);
                restore.SetProgressPercent(100);
                break;
            case TIncrementalRestoreState::EState::Finalizing:
                restore.SetProgress(Ydb::Backup::RestoreProgress::PROGRESS_TRANSFER_DATA);
                restore.SetProgressPercent(99);
                break;
            case TIncrementalRestoreState::EState::Running: {
                restore.SetProgress(Ydb::Backup::RestoreProgress::PROGRESS_TRANSFER_DATA);
                const ui32 total = restoreInfo.IncrementalBackups.size();
                if (total > 0) {
                    // 1-98% range split across incrementals, with per-shard granularity within each
                    float incrProgress = restoreInfo.CurrentIncrementalIdx + restoreInfo.CalcCurrentIncrementalProgress();
                    restore.SetProgressPercent(1 + static_cast<ui32>(incrProgress * 97 / total));
                } else {
                    restore.SetProgressPercent(1);
                }
                break;
            }
            default:
                restore.SetProgress(Ydb::Backup::RestoreProgress::PROGRESS_PREPARING);
                restore.SetProgressPercent(0);
                break;
        }

        // Set user information if available
        // Note: TIncrementalRestoreState doesn't have direct user info,
        // so we'll leave this empty for now
    }

    bool Reply(const Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS, const TString& errorMessage = TString())
    {
        Y_ABORT_UNLESS(Response);
        auto& restore = *Response->Record.MutableBackupCollectionRestore();
        restore.SetStatus(status);
        if (errorMessage) {
            auto& issue = *restore.MutableIssues()->Add();
            issue.set_severity(NYql::TSeverityIds::S_ERROR);
            issue.set_message(errorMessage);
        }

        LOG_D("Reply " << Response->Record.ShortDebugString());

        SideEffects.Send(Request->Sender, std::move(Response), 0, Request->Cookie);
        return true;
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        const auto& record = Request->Get()->Record;
        LOG_D("Execute " << record.ShortDebugString());

        Response = MakeHolder<TEvBackup::TEvGetBackupCollectionRestoreResponse>();
        TPath database = TPath::Resolve(record.GetDatabaseName(), Self);
        if (!database.IsResolved()) {
            return Reply(
                Ydb::StatusIds::NOT_FOUND,
                TStringBuilder() << "Database " << record.GetDatabaseName() << " is not found"
            );
        }
        const TPathId domainPathId = database.GetPathIdForDomain();

        ui64 restoreId = record.GetBackupCollectionRestoreId();
        const auto* incrementalRestorePtr = Self->IncrementalRestoreStates.FindPtr(restoreId);
        if (!incrementalRestorePtr) {
            return Reply(
                Ydb::StatusIds::NOT_FOUND,
                TStringBuilder() << "Incremental restore with id " << restoreId << " is not found"
            );
        }
        const auto& incrementalRestore = *incrementalRestorePtr;

        // Verify the restore belongs to the requested database
        // Note: We need to check against the backup collection path
        TPath backupCollectionPath = TPath::Init(incrementalRestore.BackupCollectionPathId, Self);
        if (!backupCollectionPath.IsResolved()) {
            return Reply(
                Ydb::StatusIds::NOT_FOUND,
                TStringBuilder() << "Incremental restore with id " << restoreId << " references invalid backup collection"
            );
        }
        
        if (backupCollectionPath.GetPathIdForDomain() != domainPathId) {
            return Reply(
                Ydb::StatusIds::NOT_FOUND,
                TStringBuilder() << "Incremental restore with id " << restoreId << " is not found in database " << record.GetDatabaseName()
            );
        }

        Response->Record.SetStatus(Ydb::StatusIds::SUCCESS);
        Fill(*Response->Record.MutableBackupCollectionRestore(), incrementalRestore);

        SideEffects.ApplyOnExecute(Self, txc, ctx);
        return Reply();
    }

    void Complete(const TActorContext& ctx) override {
        SideEffects.ApplyOnComplete(Self, ctx);
    }

private:
    TSideEffects SideEffects;
    TEvBackup::TEvGetBackupCollectionRestoreRequest::TPtr Request;
    THolder<TEvBackup::TEvGetBackupCollectionRestoreResponse> Response;
};

ITransaction* TSchemeShard::CreateTxGetRestore(TEvBackup::TEvGetBackupCollectionRestoreRequest::TPtr& ev) {
    return new TIncrementalRestore::TTxGet(this, ev);
}

} // NKikimr::NSchemeShard

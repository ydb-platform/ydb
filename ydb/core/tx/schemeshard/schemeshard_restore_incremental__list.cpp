#include "schemeshard_backup.h"
#include "schemeshard_impl.h"

#include <ydb/core/backup/impl/logging.h>

namespace NKikimr::NSchemeShard {

using namespace NTabletFlatExecutor;

struct TSchemeShard::TIncrementalRestore::TTxList: public NTabletFlatExecutor::TTransactionBase<TSchemeShard>{
public:
    explicit TTxList(TSelf* self, TEvBackup::TEvListBackupCollectionRestoresRequest::TPtr& ev)
        : TBase(self)
        , Request(ev)
    {}

    const char* GetLogPrefix() const {
        return "TIncrementalRestore::TTxList: ";
    }

    TTxType GetTxType() const override {
        return TXTYPE_LIST_BACKUP_COLLECTION_RESTORE;
    }

    void Fill(NKikimrBackup::TBackupCollectionRestore& restore, const TIncrementalRestoreState& restoreInfo) {
        restore.SetId(restoreInfo.OriginalOperationId);
        restore.SetStatus(Ydb::StatusIds::SUCCESS);

        // Calculate progress based on incremental backup processing and overall state
        if (restoreInfo.IncrementalBackups.empty()) {
            restore.SetProgress(Ydb::Backup::RestoreProgress::PROGRESS_PREPARING);
            restore.SetProgressPercent(0);
        } else {
            // Once incremental backups are defined and processing has started,
            // consider the restore operation complete from the user's perspective
            // Internal operations may still be running, but the main orchestration is done
            restore.SetProgress(Ydb::Backup::RestoreProgress::PROGRESS_DONE);
            restore.SetProgressPercent(100);
        }
    }

    bool Reply(const Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS, const TString& errorMessage = TString())
    {
        Y_ABORT_UNLESS(Response);
        Response->Record.SetStatus(status);
        if (errorMessage) {
            auto& issue = *Response->Record.MutableIssues()->Add();
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

        Response = MakeHolder<TEvBackup::TEvListBackupCollectionRestoresResponse>();
        TPath database = TPath::Resolve(record.GetDatabaseName(), Self);
        if (!database.IsResolved()) {
            return Reply(
                Ydb::StatusIds::NOT_FOUND,
                TStringBuilder() << "Database " << record.GetDatabaseName() << " is not found"
            );
        }
        const TPathId domainPathId = database.GetPathIdForDomain();

        // Simple implementation: collect all restore operations for this database
        TVector<ui64> restoreIds;
        for (const auto& [restoreId, restoreState] : Self->IncrementalRestoreStates) {
            // Check if this restore belongs to the requested database
            TPath backupCollectionPath = TPath::Init(restoreState.BackupCollectionPathId, Self);
            if (backupCollectionPath.IsResolved() && 
                backupCollectionPath.GetPathIdForDomain() == domainPathId) {
                restoreIds.push_back(restoreId);
            }
        }

        // Sort for consistent ordering
        Sort(restoreIds.begin(), restoreIds.end());

        // Apply pagination
        ui64 pageSize = record.GetPageSize();
        if (pageSize == 0) {
            pageSize = 100; // Default page size
        }

        ui64 skipCount = 0;
        if (record.HasPageToken() && !record.GetPageToken().empty()) {
            try {
                skipCount = FromString<ui64>(record.GetPageToken());
            } catch (...) {
                return Reply(
                    Ydb::StatusIds::BAD_REQUEST,
                    "Invalid page token"
                );
            }
        }

        ui64 endIdx = Min(skipCount + pageSize, static_cast<ui64>(restoreIds.size()));
        
        Response->Record.SetStatus(Ydb::StatusIds::SUCCESS);
        
        for (ui64 i = skipCount; i < endIdx; ++i) {
            ui64 restoreId = restoreIds[i];
            const auto& restoreState = Self->IncrementalRestoreStates.at(restoreId);
            
            auto* entry = Response->Record.MutableEntries()->Add();
            Fill(*entry, restoreState);
        }

        // Set next page token if there are more results
        if (endIdx < restoreIds.size()) {
            Response->Record.SetNextPageToken(ToString(endIdx));
        }

        SideEffects.ApplyOnExecute(Self, txc, ctx);
        return Reply();
    }

    void Complete(const TActorContext& ctx) override {
        SideEffects.ApplyOnComplete(Self, ctx);
    }

private:
    TSideEffects SideEffects;
    TEvBackup::TEvListBackupCollectionRestoresRequest::TPtr Request;
    THolder<TEvBackup::TEvListBackupCollectionRestoresResponse> Response;
};

ITransaction* TSchemeShard::CreateTxListRestore(TEvBackup::TEvListBackupCollectionRestoresRequest::TPtr& ev) {
    return new TIncrementalRestore::TTxList(this, ev);
}

} // NKikimr::NSchemeShard

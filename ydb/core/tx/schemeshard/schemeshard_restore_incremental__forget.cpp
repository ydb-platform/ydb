#include "schemeshard_backup.h"
#include "schemeshard_impl.h"

#include <ydb/core/backup/impl/logging.h>

namespace NKikimr::NSchemeShard {

using namespace NTabletFlatExecutor;

struct TSchemeShard::TIncrementalRestore::TTxForget: public NTabletFlatExecutor::TTransactionBase<TSchemeShard>{
public:
    explicit TTxForget(TSelf* self, TEvBackup::TEvForgetBackupCollectionRestoreRequest::TPtr& ev)
        : TBase(self)
        , Request(ev)
    {}

    const char* GetLogPrefix() const {
        return "TIncrementalRestore::TTxForget: ";
    }

    TTxType GetTxType() const override {
        return TXTYPE_FORGET_BACKUP_COLLECTION_RESTORE;
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

        Response = MakeHolder<TEvBackup::TEvForgetBackupCollectionRestoreResponse>();
        Response->Record.SetTxId(record.GetTxId());

        TPath database = TPath::Resolve(record.GetDatabaseName(), Self);
        if (!database.IsResolved()) {
            LOG_I("FORGET DEBUG: Database not resolved: " << record.GetDatabaseName());
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

        // Check if the restore can be forgotten
        // Allow forgetting when:
        // 1. The main operation is no longer active (not in Operations table), AND
        // 2. Either actual incremental processing progress has been made OR the operation is fully completed, AND
        // 3. There are no incremental operations still in progress
        bool mainOperationActive = Self->Operations.contains(TTxId(restoreId));
        bool actualProgressMade = (incrementalRestore.CurrentIncrementalIdx > 0 || 
                                   !incrementalRestore.CompletedOperations.empty() ||
                                   incrementalRestore.State == TIncrementalRestoreState::EState::Completed ||
                                   incrementalRestore.State == TIncrementalRestoreState::EState::Finalizing);
        bool hasActiveIncrementalOperations = false;
        
        // Check if any of the in-progress operations are still active
        for (const auto& opId : incrementalRestore.InProgressOperations) {
            if (Self->Operations.contains(opId.GetTxId())) {
                hasActiveIncrementalOperations = true;
                break;
            }
        }
        
        bool canForget = !mainOperationActive && actualProgressMade && !hasActiveIncrementalOperations;
        
        if (!canForget) {
            return Reply(
                Ydb::StatusIds::PRECONDITION_FAILED,
                TStringBuilder() << "Cannot forget incremental restore with id " << restoreId << " because the main operation has not completed yet"
            );
        }

        Self->IncrementalRestoreStates.erase(restoreId);
        
        // Clean up database tables
        NIceDb::TNiceDb db(txc.DB);
        
        // Clean up IncrementalRestoreState table
        db.Table<Schema::IncrementalRestoreState>().Key(restoreId).Delete();
        LOG_I("Cleaned up IncrementalRestoreState for operation: " << restoreId);
        
        // Clean up IncrementalRestoreOperations table
        db.Table<Schema::IncrementalRestoreOperations>().Key(restoreId).Delete();
        LOG_I("Cleaned up IncrementalRestoreOperations for operation: " << restoreId);
        
        auto txIt = Self->TxIdToIncrementalRestore.begin();
        while (txIt != Self->TxIdToIncrementalRestore.end()) {
            if (txIt->second == restoreId) {
                auto toErase = txIt++;
                Self->TxIdToIncrementalRestore.erase(toErase);
            } else {
                ++txIt;
            }
        }
        
        auto opIt = Self->IncrementalRestoreOperationToState.begin();
        while (opIt != Self->IncrementalRestoreOperationToState.end()) {
            if (opIt->second == restoreId) {
                auto toErase = opIt++;
                Self->IncrementalRestoreOperationToState.erase(toErase);
            } else {
                ++opIt;
            }
        }
        LOG_I("Cleaned up remaining mappings for operation: " << restoreId);

        Response->Record.SetStatus(Ydb::StatusIds::SUCCESS);

        SideEffects.ApplyOnExecute(Self, txc, ctx);
        return Reply();
    }

    void Complete(const TActorContext& ctx) override {
        SideEffects.ApplyOnComplete(Self, ctx);
    }

private:
    TSideEffects SideEffects;
    TEvBackup::TEvForgetBackupCollectionRestoreRequest::TPtr Request;
    THolder<TEvBackup::TEvForgetBackupCollectionRestoreResponse> Response;
};

ITransaction* TSchemeShard::CreateTxForgetRestore(TEvBackup::TEvForgetBackupCollectionRestoreRequest::TPtr& ev) {
    return new TIncrementalRestore::TTxForget(this, ev);
}

} // NKikimr::NSchemeShard

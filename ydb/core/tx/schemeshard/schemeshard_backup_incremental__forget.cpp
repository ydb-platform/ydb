#include "schemeshard_backup.h"
#include "schemeshard_impl.h"

#include <ydb/core/backup/impl/logging.h>

namespace NKikimr::NSchemeShard {

using namespace NTabletFlatExecutor;

struct TSchemeShard::TIncrementalBackup::TTxForget: public NTabletFlatExecutor::TTransactionBase<TSchemeShard>{
public:
    explicit TTxForget(TSelf* self, TEvBackup::TEvForgetIncrementalBackupRequest::TPtr& ev)
        : TBase(self)
        , Request(ev)
    {}

    const char* GetLogPrefix() const {
        return "TIncrementalBackup::TTxForget: ";
    }

    TTxType GetTxType() const override {
        return TXTYPE_FORGET_INCREMENTAL_BACKUP;
    }

    bool Reply(const Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS, const TString& errorMessage = TString())
    {
        Y_ABORT_UNLESS(Response);
        auto& record = Response->Record;
        record.SetStatus(status);
        if (errorMessage) {
            auto& issue = *record.MutableIssues()->Add();
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

        Response = MakeHolder<TEvBackup::TEvForgetIncrementalBackupResponse>(record.GetTxId());
        TPath database = TPath::Resolve(record.GetDatabaseName(), Self);
        if (!database.IsResolved()) {
            return Reply(
                Ydb::StatusIds::NOT_FOUND,
                TStringBuilder() << "Database " << record.GetDatabaseName() << " is not found"
            );
        }
        const TPathId domainPathId = database.GetPathIdForDomain();

        ui64 backupId = record.GetIncrementalBackupId();
        const auto* incrementalBackupPtr = Self->IncrementalBackups.FindPtr(backupId);
        if (!incrementalBackupPtr) {
            return Reply(
                Ydb::StatusIds::NOT_FOUND,
                TStringBuilder() << "Incremental backup with id " << backupId << " is not found"
            );
        }
        const auto& incrementalBackup = *incrementalBackupPtr->Get();
        if (incrementalBackup.DomainPathId != domainPathId) {
            return Reply(
                Ydb::StatusIds::NOT_FOUND,
                TStringBuilder() << "Incremental backup with id " << backupId << " is not found in database " << record.GetDatabaseName()
            );
        }

        if (!incrementalBackup.IsFinished()) {
            return Reply(
                Ydb::StatusIds::PRECONDITION_FAILED,
                TStringBuilder() << "Incremental backup with id " << backupId << " hasn't been finished yet"
            );
        }

        NIceDb::TNiceDb db(txc.DB);
        PersistRemoveIncrementalBackup(db, incrementalBackup);
        Self->IncrementalBackups.erase(incrementalBackup.Id);

        SideEffects.ApplyOnExecute(Self, txc, ctx);
        return Reply();
    }

    void Complete(const TActorContext& ctx) override {
        SideEffects.ApplyOnComplete(Self, ctx);
    }

private:
    TSideEffects SideEffects;
    TEvBackup::TEvForgetIncrementalBackupRequest::TPtr Request;
    THolder<TEvBackup::TEvForgetIncrementalBackupResponse> Response;
};

ITransaction* TSchemeShard::CreateTxForget(TEvBackup::TEvForgetIncrementalBackupRequest::TPtr& ev) {
    return new TIncrementalBackup::TTxForget(this, ev);
}

} // NKikimr::NSchemeShard

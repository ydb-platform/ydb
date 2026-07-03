#include "schemeshard_backup.h"
#include "schemeshard_impl.h"

#include <ydb/core/backup/impl/logging.h>

// Precondition: the row must be in a terminal state and the control op must not be in flight; we refuse to delete in-flight rows because AbortUnsafe expects the row to exist for cleanup.

namespace NKikimr::NSchemeShard {

using namespace NTabletFlatExecutor;

struct TSchemeShard::TFullBackup::TTxForget: public NTabletFlatExecutor::TTransactionBase<TSchemeShard> {
public:
    explicit TTxForget(TSelf* self, TEvBackup::TEvForgetFullBackupRequest::TPtr& ev)
        : TBase(self)
        , Request(ev)
    {}

    const char* GetLogPrefix() const {
        return "TFullBackup::TTxForget: ";
    }

    TTxType GetTxType() const override {
        return TXTYPE_FORGET_FULL_BACKUP;
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

        Response = MakeHolder<TEvBackup::TEvForgetFullBackupResponse>(record.GetTxId());
        TPath database = TPath::Resolve(record.GetDatabaseName(), Self);
        if (!database.IsResolved()) {
            return Reply(
                Ydb::StatusIds::NOT_FOUND,
                TStringBuilder() << "Database " << record.GetDatabaseName() << " is not found"
            );
        }
        const TPathId domainPathId = database.GetPathIdForDomain();

        ui64 backupId = record.GetFullBackupId();
        const auto* infoPtr = Self->FullBackups.FindPtr(backupId);
        if (!infoPtr) {
            return Reply(
                Ydb::StatusIds::NOT_FOUND,
                TStringBuilder() << "Full backup with id " << backupId << " is not found"
            );
        }
        // Keep a ref so `info` stays valid after FullBackups.erase drops the map's TIntrusivePtr.
        TFullBackupInfo::TPtr keepAlive = *infoPtr;
        const auto& info = *keepAlive;
        if (info.DomainPathId != domainPathId) {
            return Reply(
                Ydb::StatusIds::NOT_FOUND,
                TStringBuilder() << "Full backup with id " << backupId << " is not found in database " << record.GetDatabaseName()
            );
        }

        if (!info.IsFinished()) {
            return Reply(
                Ydb::StatusIds::PRECONDITION_FAILED,
                TStringBuilder() << "Full backup with id " << backupId << " hasn't been finished yet"
            );
        }

        if (Self->Operations.contains(TTxId(backupId))) {
            return Reply(
                Ydb::StatusIds::PRECONDITION_FAILED,
                TStringBuilder() << "Full backup with id " << backupId << " has an in-flight aggregator op"
            );
        }

        const ui64 id = info.Id;
        NIceDb::TNiceDb db(txc.DB);
        Self->PersistRemoveFullBackup(db, info);
        Self->FullBackups.erase(id);

        SideEffects.ApplyOnExecute(Self, txc, ctx);
        return Reply();
    }

    void Complete(const TActorContext& ctx) override {
        SideEffects.ApplyOnComplete(Self, ctx);
    }

private:
    TSideEffects SideEffects;
    TEvBackup::TEvForgetFullBackupRequest::TPtr Request;
    THolder<TEvBackup::TEvForgetFullBackupResponse> Response;
};

ITransaction* TSchemeShard::CreateTxForgetFullBackup(TEvBackup::TEvForgetFullBackupRequest::TPtr& ev) {
    return new TFullBackup::TTxForget(this, ev);
}

} // namespace NKikimr::NSchemeShard

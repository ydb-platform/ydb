#include "schemeshard_backup.h"
#include "schemeshard_impl.h"

#include <ydb/core/backup/impl/logging.h>

namespace NKikimr::NSchemeShard {

using namespace NTabletFlatExecutor;

struct TSchemeShard::TFullBackup::TTxGet: public NTabletFlatExecutor::TTransactionBase<TSchemeShard> {
public:
    explicit TTxGet(TSelf* self, TEvBackup::TEvGetFullBackupRequest::TPtr& ev)
        : TBase(self)
        , Request(ev)
    {}

    const char* GetLogPrefix() const {
        return "TFullBackup::TTxGet: ";
    }

    TTxType GetTxType() const override {
        return TXTYPE_GET_FULL_BACKUP;
    }

    bool Reply(const Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS, const TString& errorMessage = TString())
    {
        Y_ABORT_UNLESS(Response);
        Response->Record.SetStatus(status);
        auto& backup = *Response->Record.MutableFullBackup();
        // Only set the inner backup status on error: FillFullBackupProto already
        // set it for the success path, and overwriting with SUCCESS would mask a
        // failed backup as successful.
        if (status != Ydb::StatusIds::SUCCESS) {
            backup.SetStatus(status);
        }
        if (errorMessage) {
            auto& issue = *backup.MutableIssues()->Add();
            issue.set_severity(NYql::TSeverityIds::S_ERROR);
            issue.set_message(errorMessage);
            auto& outerIssue = *Response->Record.MutableIssues()->Add();
            outerIssue.set_severity(NYql::TSeverityIds::S_ERROR);
            outerIssue.set_message(errorMessage);
        }

        LOG_D("Reply " << Response->Record.ShortDebugString());

        SideEffects.Send(Request->Sender, std::move(Response), 0, Request->Cookie);
        return true;
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        const auto& record = Request->Get()->Record;
        LOG_D("Execute " << record.ShortDebugString());

        Response = MakeHolder<TEvBackup::TEvGetFullBackupResponse>();
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
        const auto& info = *infoPtr->Get();
        if (info.DomainPathId != domainPathId) {
            return Reply(
                Ydb::StatusIds::NOT_FOUND,
                TStringBuilder() << "Full backup with id " << backupId << " is not found in database " << record.GetDatabaseName()
            );
        }

        Response->Record.SetStatus(Ydb::StatusIds::SUCCESS);
        TSchemeShard::FillFullBackupProto(*Response->Record.MutableFullBackup(), info);

        SideEffects.ApplyOnExecute(Self, txc, ctx);
        return Reply();
    }

    void Complete(const TActorContext& ctx) override {
        SideEffects.ApplyOnComplete(Self, ctx);
    }

private:
    TSideEffects SideEffects;
    TEvBackup::TEvGetFullBackupRequest::TPtr Request;
    THolder<TEvBackup::TEvGetFullBackupResponse> Response;
};

ITransaction* TSchemeShard::CreateTxGetFullBackup(TEvBackup::TEvGetFullBackupRequest::TPtr& ev) {
    return new TFullBackup::TTxGet(this, ev);
}

} // namespace NKikimr::NSchemeShard

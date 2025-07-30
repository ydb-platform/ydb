#include "schemeshard_backup.h"
#include "schemeshard_impl.h"

#include <ydb/core/backup/impl/logging.h>

namespace NKikimr::NSchemeShard {

using namespace NTabletFlatExecutor;

struct TSchemeShard::TIncrementalBackup::TTxGet: public NTabletFlatExecutor::TTransactionBase<TSchemeShard>{
public:
    explicit TTxGet(TSelf* self, TEvBackup::TEvGetIncrementalBackupRequest::TPtr& ev)
        : TBase(self)
        , Request(ev)
    {}

    const char* GetLogPrefix() const {
        return "TIncrementalBackup::TTxGet: ";
    }

    TTxType GetTxType() const override {
        return TXTYPE_GET_INCREMENTAL_BACKUP;
    }

    void Fill(NKikimrBackup::TIncrementalBackup& backup, const TIncrementalBackupInfo& backupInfo) {
        backup.SetId(backupInfo.Id);
        backup.SetStatus(Ydb::StatusIds::SUCCESS);

        if (backupInfo.StartTime != TInstant::Zero()) {
            *backup.MutableStartTime() = SecondsToProtoTimeStamp(backupInfo.StartTime.Seconds());
        }
        if (backupInfo.EndTime != TInstant::Zero()) {
            *backup.MutableEndTime() = SecondsToProtoTimeStamp(backupInfo.EndTime.Seconds());
        }

        if (backupInfo.UserSID) {
            backup.SetUserSID(*backupInfo.UserSID);
        }

        switch (backupInfo.State) {
            case TIncrementalBackupInfo::EState::Transferring:
            case TIncrementalBackupInfo::EState::Done:
                for (const auto& [_, item] : backupInfo.Items) {
                    auto& itemProgress = *backup.AddItemsProgress();
                    itemProgress.set_parts_total(1);
                    if (item.IsDone()) {
                        itemProgress.set_parts_completed(1);
                    }
                }
                backup.SetProgress(backupInfo.IsDone()
                    ? Ydb::Backup::BackupProgress::PROGRESS_DONE
                    : Ydb::Backup::BackupProgress::PROGRESS_TRANSFER_DATA);
                break;
            case TIncrementalBackupInfo::EState::Cancellation:
                backup.SetProgress(Ydb::Backup::BackupProgress::PROGRESS_CANCELLATION);
                break;
            case TIncrementalBackupInfo::EState::Cancelled:
                backup.SetStatus(Ydb::StatusIds::CANCELLED);
                backup.SetProgress(Ydb::Backup::BackupProgress::PROGRESS_CANCELLED);
                break;
            default:
                backup.SetStatus(Ydb::StatusIds::UNDETERMINED);
                backup.SetProgress(Ydb::Backup::BackupProgress::PROGRESS_UNSPECIFIED);
                break;
        }
    }

    bool Reply(const Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS, const TString& errorMessage = TString())
    {
        Y_ABORT_UNLESS(Response);
        auto& backup = *Response->Record.MutableIncrementalBackup();
        backup.SetStatus(status);
        if (errorMessage) {
            auto& issue = *backup.MutableIssues()->Add();
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

        Response = MakeHolder<TEvBackup::TEvGetIncrementalBackupResponse>();
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

        Response->Record.SetStatus(Ydb::StatusIds::SUCCESS);
        Fill(*Response->Record.MutableIncrementalBackup(), incrementalBackup);

        SideEffects.ApplyOnExecute(Self, txc, ctx);
        return Reply();
    }

    void Complete(const TActorContext& ctx) override {
        SideEffects.ApplyOnComplete(Self, ctx);
    }

private:
    TSideEffects SideEffects;
    TEvBackup::TEvGetIncrementalBackupRequest::TPtr Request;
    THolder<TEvBackup::TEvGetIncrementalBackupResponse> Response;
};

ITransaction* TSchemeShard::CreateTxGet(TEvBackup::TEvGetIncrementalBackupRequest::TPtr& ev) {
    return new TIncrementalBackup::TTxGet(this, ev);
}

} // NKikimr::NSchemeShard

#include "schemeshard_backup.h"
#include "schemeshard_impl.h"

#include <ydb/core/backup/impl/logging.h>

namespace NKikimr::NSchemeShard {

using namespace NTabletFlatExecutor;

struct TSchemeShard::TIncrementalBackup::TTxList: public NTabletFlatExecutor::TTransactionBase<TSchemeShard>{
public:
    explicit TTxList(TSelf* self, TEvBackup::TEvListIncrementalBackupsRequest::TPtr& ev)
        : TBase(self)
        , Request(ev)
    {}

    const char* GetLogPrefix() const {
        return "TIncrementalBackup::TTxList: ";
    }

    TTxType GetTxType() const override {
        return TXTYPE_LIST_INCREMENTAL_BACKUP;
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

        Response = MakeHolder<TEvBackup::TEvListIncrementalBackupsResponse>();
        TPath database = TPath::Resolve(record.GetDatabaseName(), Self);
        if (!database.IsResolved()) {
            return Reply(
                Ydb::StatusIds::NOT_FOUND,
                TStringBuilder() << "Database " << record.GetDatabaseName() << " is not found"
            );
        }
        const TPathId domainPathId = database.GetPathIdForDomain();

        ui64 page = DefaultPage;
        if (record.GetPageToken() && !TryFromString(record.GetPageToken(), page)) {
            return Reply(
                Ydb::StatusIds::BAD_REQUEST,
                TStringBuilder() << "Unable to parse page token"
            );
        }
        page = Max(page, DefaultPage);
        const ui64 pageSize = Min(record.GetPageSize() ? Max(record.GetPageSize(), MinPageSize) : DefaultPageSize, MaxPageSize);

        auto it = Self->IncrementalBackups.end();
        ui64 skip = (page - 1) * pageSize;
        while ((it != Self->IncrementalBackups.begin()) && skip) {
            --it;
            if (it->second->DomainPathId == domainPathId) {
                --skip;
            }
        }
        Response->Record.SetStatus(Ydb::StatusIds::SUCCESS);

        ui64 size = 0;
        while ((it != Self->IncrementalBackups.begin()) && size < pageSize) {
            --it;
            if (it->second->DomainPathId == domainPathId) {
                Fill(*Response->Record.MutableEntries()->Add(), *it->second);
                ++size;
            }
        }

        if (it == Self->IncrementalBackups.begin()) {
            Response->Record.SetNextPageToken("0");
        } else {
            Response->Record.SetNextPageToken(ToString(page + 1));
        }

        SideEffects.ApplyOnExecute(Self, txc, ctx);
        return Reply();
    }

    void Complete(const TActorContext& ctx) override {
        SideEffects.ApplyOnComplete(Self, ctx);
    }

private:
    static constexpr ui64 DefaultPageSize = 10;
    static constexpr ui64 MinPageSize = 1;
    static constexpr ui64 MaxPageSize = 100;
    static constexpr ui64 DefaultPage = 1;
private:
    TSideEffects SideEffects;
    TEvBackup::TEvListIncrementalBackupsRequest::TPtr Request;
    THolder<TEvBackup::TEvListIncrementalBackupsResponse> Response;
};

ITransaction* TSchemeShard::CreateTxList(TEvBackup::TEvListIncrementalBackupsRequest::TPtr& ev) {
    return new TIncrementalBackup::TTxList(this, ev);
}

} // NKikimr::NSchemeShard

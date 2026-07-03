#include "schemeshard_backup.h"
#include "schemeshard_impl.h"

#include <ydb/core/backup/impl/logging.h>

namespace NKikimr::NSchemeShard {

using namespace NTabletFlatExecutor;

struct TSchemeShard::TFullBackup::TTxList: public NTabletFlatExecutor::TTransactionBase<TSchemeShard> {
public:
    explicit TTxList(TSelf* self, TEvBackup::TEvListFullBackupsRequest::TPtr& ev)
        : TBase(self)
        , Request(ev)
    {}

    const char* GetLogPrefix() const {
        return "TFullBackup::TTxList: ";
    }

    TTxType GetTxType() const override {
        return TXTYPE_LIST_FULL_BACKUP;
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

        Response = MakeHolder<TEvBackup::TEvListFullBackupsResponse>();
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

        auto it = Self->FullBackups.end();
        ui64 skip = (page - 1) * pageSize;
        while ((it != Self->FullBackups.begin()) && skip) {
            --it;
            if (it->second->DomainPathId == domainPathId) {
                --skip;
            }
        }
        Response->Record.SetStatus(Ydb::StatusIds::SUCCESS);

        ui64 size = 0;
        while ((it != Self->FullBackups.begin()) && size < pageSize) {
            --it;
            if (it->second->DomainPathId == domainPathId) {
                TSchemeShard::FillFullBackupProto(*Response->Record.MutableEntries()->Add(), *it->second);
                ++size;
            }
        }

        // Single-domain: begin() is an exact end-of-results signal. With multiple
        // domains, trailing foreign-domain entries below `it` could cost one extra
        // empty page; harmless, revisit if multi-domain ships.
        if (it == Self->FullBackups.begin()) {
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
    TEvBackup::TEvListFullBackupsRequest::TPtr Request;
    THolder<TEvBackup::TEvListFullBackupsResponse> Response;
};

ITransaction* TSchemeShard::CreateTxListFullBackups(TEvBackup::TEvListFullBackupsRequest::TPtr& ev) {
    return new TFullBackup::TTxList(this, ev);
}

} // namespace NKikimr::NSchemeShard

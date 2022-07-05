#include "schemeshard_build_index.h"
#include "schemeshard_impl.h"
#include "schemeshard_build_index_helpers.h"
#include "schemeshard_build_index_tx_base.h"

#include <ydb/public/api/protos/ydb_issue_message.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>


namespace NKikimr {
namespace NSchemeShard {

using namespace NTabletFlatExecutor;

struct TSchemeShard::TIndexBuilder::TTxList: public TSchemeShard::TIndexBuilder::TTxBase {
private:
    static constexpr ui64 DefaultPageSize = 10;
    static constexpr ui64 MinPageSize = 1;
    static constexpr ui64 MaxPageSize = 100;
    static constexpr ui64 DefaultPage = 1;


    TEvIndexBuilder::TEvListRequest::TPtr Request;

public:
    explicit TTxList(TSelf* self, TEvIndexBuilder::TEvListRequest::TPtr& ev)
        : TSchemeShard::TIndexBuilder::TTxBase(self)
        , Request(ev)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_LIST_INDEX_BUILD;
    }

    bool DoExecute(TTransactionContext&, const TActorContext&) override {
        const auto& record = Request->Get()->Record;

        LOG_D("TIndexBuilder::TTxList: DoExecute"
              << ", Database: " << record.GetDatabaseName());
        LOG_T("Message: " << record.ShortDebugString());

        auto response = MakeHolder<TEvIndexBuilder::TEvListResponse>();
        TPath database = TPath::Resolve(record.GetDatabaseName(), Self);
        if (!database.IsResolved()) {
            return Reply(
                std::move(response),
                Ydb::StatusIds::NOT_FOUND,
                TStringBuilder() << "Database <" << record.GetDatabaseName() << "> not found"
                );
        }
        const TPathId domainPathId = database.GetPathIdForDomain();

        ui64 page = DefaultPage;
        if (record.GetPageToken() && !TryFromString(record.GetPageToken(), page)) {
            return Reply(
                std::move(response),
                Ydb::StatusIds::BAD_REQUEST,
                TStringBuilder() << "Unable to parse page token"
                );
        }
        page = Max(page, DefaultPage);
        const ui64 pageSize = Min(record.GetPageSize() ? Max(record.GetPageSize(), MinPageSize) : DefaultPageSize, MaxPageSize);


        auto it = Self->IndexBuilds.begin();
        ui64 skip = (page - 1) * pageSize;
        while ((it != Self->IndexBuilds.end()) && skip) {
            if (it->second->DomainPathId == domainPathId) {
                --skip;
            }
            ++it;
        }

        auto& respRecord = response->Record;
        respRecord.SetStatus(Ydb::StatusIds::SUCCESS);

        ui64 size = 0;
        while ((it != Self->IndexBuilds.end()) && size < pageSize) {
            if (it->second->DomainPathId == domainPathId) {
                Fill(*respRecord.MutableEntries()->Add(), it->second);
                ++size;
            }
            ++it;
        }

        if (it == Self->IndexBuilds.end()) {
            respRecord.SetNextPageToken("0");
        } else {
            respRecord.SetNextPageToken(ToString(page + 1));
        }

        return Reply(std::move(response));
    }

    bool Reply(THolder<TEvIndexBuilder::TEvListResponse> response,
               const Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS,
               const TString& errorMessage = TString())
    {
        LOG_D("TIndexBuilder::TTxForget: Reply"
              << ", BuildIndexCount: " << response->Record.EntriesSize()
              << ", status: " << status
              << ", error: " << errorMessage);
        LOG_T("Message: " << response->Record.ShortDebugString());

        auto& resp = response->Record;
        resp.SetStatus(status);
        if (errorMessage) {
            AddIssue(resp.MutableIssues(), errorMessage);
        }

        Send(Request->Sender, std::move(response), 0, Request->Cookie);

        return true;
    }

    void DoComplete(const TActorContext&) override {
    }

private:

};

ITransaction* TSchemeShard::CreateTxList(TEvIndexBuilder::TEvListRequest::TPtr& ev) {
    return new TIndexBuilder::TTxList(this, ev);
}

} // NSchemeShard
} // NKikimr


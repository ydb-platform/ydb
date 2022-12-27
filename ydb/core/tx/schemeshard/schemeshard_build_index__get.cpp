#include "schemeshard_build_index.h"
#include "schemeshard_impl.h"
#include "schemeshard_build_index_helpers.h"
#include "schemeshard_build_index_tx_base.h"

#include <ydb/public/api/protos/ydb_issue_message.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>


namespace NKikimr {
namespace NSchemeShard {

using namespace NTabletFlatExecutor;

struct TSchemeShard::TIndexBuilder::TTxGet: public TSchemeShard::TIndexBuilder::TTxBase {
private:
    TEvIndexBuilder::TEvGetRequest::TPtr Request;

public:
    explicit TTxGet(TSelf* self, TEvIndexBuilder::TEvGetRequest::TPtr& ev)
        : TSchemeShard::TIndexBuilder::TTxBase(self)
        , Request(ev)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_GET_INDEX_BUILD;
    }

    bool DoExecute(TTransactionContext&, const TActorContext&) override {
        const auto& record = Request->Get()->Record;

        LOG_D("TIndexBuilder::TTxGet: DoExecute"
              << ", Database: " << record.GetDatabaseName()
              << ", BuildIndexId: " << record.GetIndexBuildId());
        LOG_T("Message: " << record.ShortDebugString());

        auto response = MakeHolder<TEvIndexBuilder::TEvGetResponse>();
        TPath database = TPath::Resolve(record.GetDatabaseName(), Self);
        if (!database.IsResolved()) {
            return Reply(
                std::move(response),
                Ydb::StatusIds::BAD_REQUEST,
                TStringBuilder() << "Database <" << record.GetDatabaseName() << "> not found"
                );
        }
        const TPathId domainPathId = database.GetPathIdForDomain();

        TIndexBuildId indexBuildId = TIndexBuildId(record.GetIndexBuildId());

        if (!Self->IndexBuilds.contains(indexBuildId)) {
            return Reply(
                std::move(response),
                Ydb::StatusIds::PRECONDITION_FAILED,
                TStringBuilder() << "Index build process with id <" << indexBuildId << "> not found"
                );
        }

        TIndexBuildInfo::TPtr indexBuildInfo = Self->IndexBuilds.at(indexBuildId);
        if (indexBuildInfo->DomainPathId != domainPathId) {
            return Reply(
                std::move(response),
                Ydb::StatusIds::BAD_REQUEST,
                TStringBuilder() << "Index build process with id <" << indexBuildId << "> not found in database <" << record.GetDatabaseName() << ">"
                );
        }

        auto& respRecord = response->Record;
        respRecord.SetStatus(Ydb::StatusIds::SUCCESS);
        Fill(*respRecord.MutableIndexBuild(), indexBuildInfo);

        return Reply(std::move(response));
    }

    bool Reply(THolder<TEvIndexBuilder::TEvGetResponse> response,
               const Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS,
               const TString& errorMessage = TString())
    {
        LOG_D("TIndexBuilder::TTxForget: Reply"
              << ", BuildIndexId: " << response->Record.GetIndexBuild().GetId()
              << ", status: " << status
              << ", error: " << errorMessage);
        LOG_T("Message: " << response->Record.ShortDebugString());

        auto& respRecord = response->Record;
        respRecord.SetStatus(status);
        if (errorMessage) {
            AddIssue(respRecord.MutableIssues(), errorMessage);
        }

        Send(Request->Sender, std::move(response), 0, Request->Cookie);

        return true;
    }

    void DoComplete(const TActorContext&) override {
    }

private:


};

ITransaction* TSchemeShard::CreateTxGet(TEvIndexBuilder::TEvGetRequest::TPtr& ev) {
    return new TIndexBuilder::TTxGet(this, ev);
}

} // NSchemeShard
} // NKikimr

#include "schemeshard_build_index.h"
#include "schemeshard_impl.h"
#include "schemeshard_build_index_helpers.h"
#include "schemeshard_build_index_tx_base.h"

#include <ydb/public/api/protos/ydb_issue_message.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>


namespace NKikimr {
namespace NSchemeShard {

using namespace NTabletFlatExecutor;

struct TSchemeShard::TIndexBuilder::TTxForget: public TSchemeShard::TIndexBuilder::TTxBase {
private:
    TEvIndexBuilder::TEvForgetRequest::TPtr Request;

public:
    explicit TTxForget(TSelf* self, TEvIndexBuilder::TEvForgetRequest::TPtr& ev)
        : TTxBase(self)
        , Request(ev)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_FORGET_INDEX_BUILD;
    }

    bool DoExecute(TTransactionContext& txc, const TActorContext&) override {
        const auto& record = Request->Get()->Record;

        LOG_N("TIndexBuilder::TTxForget: DoExecute"
              << ", Database: " << record.GetDatabaseName()
              << ", BuildIndexId: " << record.GetIndexBuildId());
        LOG_D("Message: " << record.ShortDebugString());

        auto response = MakeHolder<TEvIndexBuilder::TEvForgetResponse>(record.GetTxId());
        TPath database = TPath::Resolve(record.GetDatabaseName(), Self);
        if (!database.IsResolved()) {
            return Reply(
                std::move(response),
                Ydb::StatusIds::NOT_FOUND,
                TStringBuilder() << "Database <" << record.GetDatabaseName() << "> not found"
                );
        }
        const TPathId domainPathId = database.GetPathIdForDomain();

        TIndexBuildId indexBuildId = TIndexBuildId(record.GetIndexBuildId());

        if (!Self->IndexBuilds.contains(indexBuildId)) {
            return Reply(
                std::move(response),
                Ydb::StatusIds::NOT_FOUND,
                TStringBuilder() << "Index build process with id <" << indexBuildId << "> not found"
                );
        }

        TIndexBuildInfo::TPtr indexBuildInfo = Self->IndexBuilds.at(indexBuildId);
        if (indexBuildInfo->DomainPathId != domainPathId) {
            return Reply(
                std::move(response),
                Ydb::StatusIds::NOT_FOUND,
                TStringBuilder() << "Index build process with id <" << indexBuildId << "> not found in database <" << record.GetDatabaseName() << ">"
                );
        }

        if (!indexBuildInfo->IsFinished()) {
            return Reply(
                std::move(response),
                Ydb::StatusIds::PRECONDITION_FAILED,
                TStringBuilder() << "Index build process with id <" << indexBuildId << "> hasn't been finished yet"
                );
        }

        NIceDb::TNiceDb db(txc.DB);
        Self->PersistBuildIndexForget(db, indexBuildInfo);

        EraseBuildInfo(indexBuildInfo);

        return Reply(std::move(response));
    }

    bool Reply(THolder<TEvIndexBuilder::TEvForgetResponse> response,
               const Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS,
               const TString& errorMessage = TString())
    {
        LOG_N("TIndexBuilder::TTxForget: Reply"
              << ", BuildIndexId: " << response->Record.GetTxId()
              << ", status: " << status
              << ", error: " << errorMessage);
        LOG_D("Message: " << response->Record.ShortDebugString());

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

ITransaction* TSchemeShard::CreateTxForget(TEvIndexBuilder::TEvForgetRequest::TPtr& ev) {
    return new TIndexBuilder::TTxForget(this, ev);
}

} // NSchemeShard
} // NKikimr

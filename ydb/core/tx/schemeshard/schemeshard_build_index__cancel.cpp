#include "schemeshard_build_index.h"
#include "schemeshard_impl.h"
#include "schemeshard_build_index_helpers.h"
#include "schemeshard_build_index_tx_base.h"

#include <ydb/public/api/protos/ydb_issue_message.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>


namespace NKikimr {
namespace NSchemeShard {

using namespace NTabletFlatExecutor;

struct TSchemeShard::TIndexBuilder::TTxCancel: public TSchemeShard::TIndexBuilder::TTxBase {
private:
    TEvIndexBuilder::TEvCancelRequest::TPtr Request;

public:
    explicit TTxCancel(TSelf* self, TEvIndexBuilder::TEvCancelRequest::TPtr& ev)
        : TTxBase(self)
        , Request(ev)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_CANCEL_INDEX_BUILD;
    }

    bool DoExecute(TTransactionContext& txc, const TActorContext&) override {
        const auto& record = Request->Get()->Record;

        LOG_N("TIndexBuilder::TTxCancel: DoExecute"
              << ", Database: " << record.GetDatabaseName()
              << ", BuildIndexId: " << record.GetIndexBuildId());
        LOG_D("Message: " << record.ShortDebugString());


        auto response = MakeHolder<TEvIndexBuilder::TEvCancelResponse>(record.GetTxId());
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

        if (indexBuildInfo->IsFinished()) {
            return Reply(
                std::move(response),
                Ydb::StatusIds::PRECONDITION_FAILED,
                TStringBuilder() << "Index build process with id <" << indexBuildId << "> has been finished already"
                );
        }

        if (indexBuildInfo->IsCancellationRequested()) {
            return Reply(
                std::move(response),
                Ydb::StatusIds::PRECONDITION_FAILED,
                TStringBuilder() << "Index build process with id <" << indexBuildId << "> canceling already"
                );
        }

        if (indexBuildInfo->State > TIndexBuildInfo::EState::Filling) {
            return Reply(
                std::move(response),
                Ydb::StatusIds::PRECONDITION_FAILED,
                TStringBuilder() << "Index build process with id <" << indexBuildId << "> are almost done, cancellation has no sense"
                );
        }

        NIceDb::TNiceDb db(txc.DB);
        indexBuildInfo->CancelRequested = true;
        Self->PersistBuildIndexCancelRequest(db, indexBuildInfo);

        Progress(indexBuildInfo->Id);

        return Reply(std::move(response));
    }

    bool Reply(THolder<TEvIndexBuilder::TEvCancelResponse> response,
               const Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS,
               const TString& errorMessage = TString())
    {

        LOG_N("TIndexBuilder::TTxCancel: Reply"
              << ", BuildIndexId: " << response->Record.GetTxId()
              << ", status: " << status
              << ", error: " << errorMessage);
        LOG_D("Message: " << response->Record.ShortDebugString());

        auto& record = response->Record;
        record.SetStatus(status);
        if (errorMessage) {
            AddIssue(record.MutableIssues(), errorMessage);
        }

        Send(Request->Sender, std::move(response), 0, Request->Cookie);

        return true;
    }

    void DoComplete(const TActorContext&) override {
    }

private:


};

ITransaction* TSchemeShard::CreateTxCancel(TEvIndexBuilder::TEvCancelRequest::TPtr& ev) {
    return new TIndexBuilder::TTxCancel(this, ev);
}

} // NSchemeShard
} // NKikimr

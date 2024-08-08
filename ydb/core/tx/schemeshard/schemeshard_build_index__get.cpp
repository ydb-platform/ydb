#include "schemeshard_build_index.h"
#include "schemeshard_impl.h"
#include "schemeshard_build_index_helpers.h"
#include "schemeshard_build_index_tx_base.h"


namespace NKikimr::NSchemeShard {

using namespace NTabletFlatExecutor;

struct TSchemeShard::TIndexBuilder::TTxGet: public TSchemeShard::TIndexBuilder::TTxSimple<TEvIndexBuilder::TEvGetRequest, TEvIndexBuilder::TEvGetResponse> {
public:
    explicit TTxGet(TSelf* self, TEvIndexBuilder::TEvGetRequest::TPtr& ev)
        : TTxSimple(self, ev, TXTYPE_GET_INDEX_BUILD, false)
    {}

    bool DoExecute(TTransactionContext&, const TActorContext&) override {
        const auto& record = Request->Get()->Record;
        LOG_D("DoExecute " << record.ShortDebugString());

        Response = MakeHolder<TEvIndexBuilder::TEvGetResponse>();
        TPath database = TPath::Resolve(record.GetDatabaseName(), Self);
        if (!database.IsResolved()) {
            return Reply(
                Ydb::StatusIds::BAD_REQUEST,
                TStringBuilder() << "Database <" << record.GetDatabaseName() << "> not found"
            );
        }
        const TPathId domainPathId = database.GetPathIdForDomain();

        TIndexBuildId indexBuildId = TIndexBuildId(record.GetIndexBuildId());

        if (!Self->IndexBuilds.contains(indexBuildId)) {
            return Reply(
                Ydb::StatusIds::PRECONDITION_FAILED,
                TStringBuilder() << "Index build process with id <" << indexBuildId << "> not found"
            );
        }

        TIndexBuildInfo::TPtr indexBuildInfo = Self->IndexBuilds.at(indexBuildId);
        if (indexBuildInfo->DomainPathId != domainPathId) {
            return Reply(
                Ydb::StatusIds::BAD_REQUEST,
                TStringBuilder() << "Index build process with id <" << indexBuildId << "> not found in database <" << record.GetDatabaseName() << ">"
            );
        }

        auto& respRecord = Response->Record;

        if (indexBuildInfo->State == TIndexBuildInfo::EState::Rejected) {
            return Reply(
                Ydb::StatusIds_StatusCode_GENERIC_ERROR,
                TStringBuilder() << indexBuildInfo->Issue
            );
        } else {
            respRecord.SetStatus(Ydb::StatusIds::SUCCESS);
        }

        Fill(*respRecord.MutableIndexBuild(), indexBuildInfo);

        return Reply();
    }

    void DoComplete(const TActorContext&) override {}
};

ITransaction* TSchemeShard::CreateTxGet(TEvIndexBuilder::TEvGetRequest::TPtr& ev) {
    return new TIndexBuilder::TTxGet(this, ev);
}

} // NKikimr::NSchemeShard

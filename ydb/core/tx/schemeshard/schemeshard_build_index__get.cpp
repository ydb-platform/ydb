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

        const auto* indexBuildInfoPtr = Self->IndexBuilds.FindPtr(indexBuildId);
        if (!indexBuildInfoPtr) {
            return Reply(
                Ydb::StatusIds::PRECONDITION_FAILED,
                TStringBuilder() << "Index build process with id <" << indexBuildId << "> not found"
            );
        }
        const auto& indexBuildInfo = *indexBuildInfoPtr->Get();
        if (indexBuildInfo.DomainPathId != domainPathId) {
            return Reply(
                Ydb::StatusIds::BAD_REQUEST,
                TStringBuilder() << "Index build process with id <" << indexBuildId << "> not found in database <" << record.GetDatabaseName() << ">"
            );
        }

        auto& respRecord = Response->Record;
        respRecord.SetStatus(Ydb::StatusIds::SUCCESS);
        Fill(*respRecord.MutableIndexBuild(), indexBuildInfo);

        return Reply();
    }

    void DoComplete(const TActorContext&) override {}
};

ITransaction* TSchemeShard::CreateTxGet(TEvIndexBuilder::TEvGetRequest::TPtr& ev) {
    return new TIndexBuilder::TTxGet(this, ev);
}

} // NKikimr::NSchemeShard

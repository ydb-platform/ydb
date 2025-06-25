#include "schemeshard_build_index.h"
#include "schemeshard_build_index_helpers.h"
#include "schemeshard_build_index_tx_base.h"
#include "schemeshard_impl.h"

namespace NKikimr::NSchemeShard {

using namespace NTabletFlatExecutor;

struct TSchemeShard::TIndexBuilder::TTxForget: public TSchemeShard::TIndexBuilder::TTxSimple<TEvIndexBuilder::TEvForgetRequest, TEvIndexBuilder::TEvForgetResponse> {
public:
    explicit TTxForget(TSelf* self, TEvIndexBuilder::TEvForgetRequest::TPtr& ev)
        : TTxSimple(self, TIndexBuildId(ev->Get()->Record.GetIndexBuildId()), ev, TXTYPE_FORGET_INDEX_BUILD)
    {}

    bool DoExecute(TTransactionContext& txc, const TActorContext&) override {
        const auto& record = Request->Get()->Record;
        LOG_N("DoExecute " << record.ShortDebugString());

        Response = MakeHolder<TEvIndexBuilder::TEvForgetResponse>(record.GetTxId());
        TPath database = TPath::Resolve(record.GetDatabaseName(), Self);
        if (!database.IsResolved()) {
            return Reply(
                Ydb::StatusIds::NOT_FOUND,
                TStringBuilder() << "Database <" << record.GetDatabaseName() << "> not found"
            );
        }
        const TPathId domainPathId = database.GetPathIdForDomain();

        const auto* indexBuildInfoPtr = Self->IndexBuilds.FindPtr(BuildId);
        if (!indexBuildInfoPtr) {
            return Reply(
                Ydb::StatusIds::NOT_FOUND,
                TStringBuilder() << "Index build process with id <" << BuildId << "> not found"
            );
        }
        const auto& indexBuildInfo = *indexBuildInfoPtr->Get();
        if (indexBuildInfo.DomainPathId != domainPathId) {
            return Reply(
                Ydb::StatusIds::NOT_FOUND,
                TStringBuilder() << "Index build process with id <" << BuildId << "> not found in database <" << record.GetDatabaseName() << ">"
            );
        }

        if (!indexBuildInfo.IsFinished()) {
            return Reply(
                Ydb::StatusIds::PRECONDITION_FAILED,
                TStringBuilder() << "Index build process with id <" << BuildId << "> hasn't been finished yet"
            );
        }

        NIceDb::TNiceDb db(txc.DB);
        Self->PersistBuildIndexForget(db, indexBuildInfo);

        EraseBuildInfo(indexBuildInfo);

        return Reply();
    }

    void DoComplete(const TActorContext&) override {}
};

ITransaction* TSchemeShard::CreateTxForget(TEvIndexBuilder::TEvForgetRequest::TPtr& ev) {
    return new TIndexBuilder::TTxForget(this, ev);
}

} // NKikimr::NSchemeShard

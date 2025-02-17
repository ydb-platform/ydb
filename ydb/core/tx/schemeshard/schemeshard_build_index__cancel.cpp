#include "schemeshard_build_index.h"
#include "schemeshard_impl.h"
#include "schemeshard_build_index_helpers.h"
#include "schemeshard_build_index_tx_base.h"

namespace NKikimr::NSchemeShard {

using namespace NTabletFlatExecutor;

struct TSchemeShard::TIndexBuilder::TTxCancel: public TSchemeShard::TIndexBuilder::TTxSimple<TEvIndexBuilder::TEvCancelRequest, TEvIndexBuilder::TEvCancelResponse> {
public:
    explicit TTxCancel(TSelf* self, TEvIndexBuilder::TEvCancelRequest::TPtr& ev)
        : TTxSimple(self, ev, TXTYPE_CANCEL_INDEX_BUILD)
    {}

    bool DoExecute(TTransactionContext& txc, const TActorContext&) override {
        const auto& record = Request->Get()->Record;
        LOG_N("DoExecute " << record.ShortDebugString());

        Response = MakeHolder<TEvIndexBuilder::TEvCancelResponse>(record.GetTxId());
        TPath database = TPath::Resolve(record.GetDatabaseName(), Self);
        if (!database.IsResolved()) {
            return Reply(
                Ydb::StatusIds::NOT_FOUND,
                TStringBuilder() << "Database <" << record.GetDatabaseName() << "> not found"
            );
        }
        const TPathId domainPathId = database.GetPathIdForDomain();

        TIndexBuildId indexBuildId = TIndexBuildId(record.GetIndexBuildId());
        const auto* indexBuildInfoPtr = Self->IndexBuilds.FindPtr(indexBuildId);
        if (!indexBuildInfoPtr) {
            return Reply(
                Ydb::StatusIds::NOT_FOUND,
                TStringBuilder() << "Index build process with id <" << indexBuildId << "> not found"
            );
        }
        auto& indexBuildInfo = *indexBuildInfoPtr->Get();
        if (indexBuildInfo.DomainPathId != domainPathId) {
            return Reply(
                Ydb::StatusIds::NOT_FOUND,
                TStringBuilder() << "Index build process with id <" << indexBuildId << "> not found in database <" << record.GetDatabaseName() << ">"
            );
        }

        if (indexBuildInfo.IsFinished()) {
            return Reply(
                Ydb::StatusIds::PRECONDITION_FAILED,
                TStringBuilder() << "Index build process with id <" << indexBuildId << "> has been finished already"
            );
        }

        if (indexBuildInfo.IsCancellationRequested()) {
            return Reply(
                Ydb::StatusIds::PRECONDITION_FAILED,
                TStringBuilder() << "Index build process with id <" << indexBuildId << "> canceling already"
            );
        }

        if (indexBuildInfo.State > TIndexBuildInfo::EState::Filling) {
            return Reply(
                Ydb::StatusIds::PRECONDITION_FAILED,
                TStringBuilder() << "Index build process with id <" << indexBuildId << "> are almost done, cancellation has no sense"
            );
        }

        NIceDb::TNiceDb db(txc.DB);
        indexBuildInfo.CancelRequested = true;
        Self->PersistBuildIndexCancelRequest(db, indexBuildInfo);

        Progress(indexBuildInfo.Id);

        return Reply();
    }

    void DoComplete(const TActorContext&) override {}
};

ITransaction* TSchemeShard::CreateTxCancel(TEvIndexBuilder::TEvCancelRequest::TPtr& ev) {
    return new TIndexBuilder::TTxCancel(this, ev);
}

} // NKikimr::NSchemeShard

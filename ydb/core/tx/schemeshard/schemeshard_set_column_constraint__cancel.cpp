#include <ydb/core/tx/schemeshard/index/build_index.h>
#include <ydb/core/tx/schemeshard/index/build_index_helpers.h>
#include <ydb/core/tx/schemeshard/index/build_index_tx_base.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>
#include <ydb/core/tx/schemeshard/schemeshard_set_column_constraint.h>

namespace NKikimr {
namespace NSchemeShard {

using namespace NTabletFlatExecutor;

struct TSchemeShard::TIndexBuilder::TTxCancelSetColumnConstraint : public TSchemeShard::TIndexBuilder::TTxSimple<TEvSetColumnConstraint::TEvCancelRequest, TEvSetColumnConstraint::TEvCancelResponse> {
public:
    explicit TTxCancelSetColumnConstraint(TSelf* self, TEvSetColumnConstraint::TEvCancelRequest::TPtr& ev)
        : TTxSimple(self, TIndexBuildId(ev->Get()->Record.GetOperationId()), ev, TXTYPE_CANCEL_SET_COLUMN_CONSTRAINT)
    {}

    bool DoExecute(TTransactionContext& txc, const TActorContext&) override {
        const auto& record = Request->Get()->Record;
        LOG_D("TTxCancelSetColumnConstraint::DoExecute " << record.ShortDebugString());

        Response = MakeHolder<TEvSetColumnConstraint::TEvCancelResponse>(record.GetTxId());

        TPath database = TPath::Resolve(record.GetDatabaseName(), Self);
        if (!database.IsResolved()) {
            return Reply(
                Ydb::StatusIds::NOT_FOUND,
                TStringBuilder() << "Database <" << record.GetDatabaseName() << "> not found"
            );
        }

        auto* operationInfoPtr = Self->SetColumnConstraintOperations.FindPtr(BuildId);
        if (!operationInfoPtr) {
            return Reply(
                Ydb::StatusIds::NOT_FOUND,
                TStringBuilder() << "SetColumnConstraint operation with id <" << BuildId << "> not found"
            );
        }

        auto& operationInfo = *operationInfoPtr->get();
        const TPathId subdomainPathId = database.GetPathIdForDomain();

        if (operationInfo.DomainPathId != subdomainPathId) {
            return Reply(
                Ydb::StatusIds::NOT_FOUND,
                TStringBuilder() << "SetColumnConstraint operation with id <" << BuildId << "> not found in database <" << record.GetDatabaseName() << ">"
            );
        }

        if (operationInfo.IsCancelled) {
            return Reply(
                Ydb::StatusIds::PRECONDITION_FAILED,
                TStringBuilder() << "SetColumnConstraint operation with id <" << BuildId << "> is already cancelled"
            );
        }

        if (operationInfo.IsCloseToCompletion()) {
            return Reply(
                Ydb::StatusIds::PRECONDITION_FAILED,
                TStringBuilder() << "SetColumnConstraint operation with id <" << BuildId << "> has already finished or is too close to completion to be cancelled"
            );
        }

        // Mark operation as cancelled
        NIceDb::TNiceDb db(txc.DB);
        operationInfo.MarkAsCancelled(TString("Cancelled by user request"));
        Self->PersistSetColumnConstraintCancellation(db, operationInfo);

        // Note: cancellation is de facto acted upon only while the operation is in the
        // Validating stage:
        // - TTxReplyValidateRowCondition ignores incoming validation messages due to IsCancelled flag
        // - AlterMainTableUnlockNullWritesPropose will NOT set NOT NULL constraint due to IsCancelled check
        // - The operation will complete naturally in Done state without setting the constraint
        //
        // Trying to react to cancellation on earlier or later stages introduces non-trivial
        // logic that can easily lead to unwanted bugs (e.g. a stale reply for an already
        // abandoned stage racing with a message for the newly entered one). We sidestep that
        // entirely here:
        // - Later stages (Finishing, Unlocking) are already forbidden for cancellation by the
        //   IsCloseToCompletion() check above.
        // - Earlier stages (Locking, LockingNullWrites) simply ignore the fact of cancellation
        //   until the Validating stage is reached; the IsCancelled flag is already persisted,
        //   so once Validating starts (or is already running), the operation will pick it up
        //   and unwind normally.
        //
        // This is not a practical concern because Validating is by far the longest-running
        // stage of this metadata operation, and it is also the very reason a user would want
        // to cancel a SET NOT NULL metadata operation in the first place.
        if (operationInfo.OperationState == TSetColumnConstraintOperationInfo::EOperationState::Validating) {
            Progress(BuildId);
        }

        return Reply();
    }

    void DoComplete(const TActorContext&) override {}
};

ITransaction* TSchemeShard::CreateTxCancelSetColumnConstraint(TEvSetColumnConstraint::TEvCancelRequest::TPtr& ev) {
    return new TIndexBuilder::TTxCancelSetColumnConstraint(this, ev);
}

} // NSchemeShard
} // NKikimr

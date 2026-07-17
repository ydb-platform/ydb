#include <ydb/core/tx/schemeshard/index/build_index.h>
#include <ydb/core/tx/schemeshard/index/build_index_helpers.h>
#include <ydb/core/tx/schemeshard/index/build_index_tx_base.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>
#include <ydb/core/tx/schemeshard/schemeshard_set_column_constraint.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::BUILD_INDEX

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
        YDB_LOG_DEBUG("TTxCancelSetColumnConstraint::DoExecute",
            {"logPrefix", LogPrefix},
            {"record", record.ShortDebugString()}
        );

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

        // Note: The operation will continue through its natural stages:
        // - TTxReplyValidateRowCondition ignores incoming validation messages due to IsCancelled flag
        // - AlterMainTableUnlockNullWritesPropose will NOT set NOT NULL constraint due to IsCancelled check
        // - The operation will complete naturally in Done state without setting the constraint
        Progress(BuildId);

        return Reply();
    }

    void DoComplete(const TActorContext&) override {}
};

ITransaction* TSchemeShard::CreateTxCancelSetColumnConstraint(TEvSetColumnConstraint::TEvCancelRequest::TPtr& ev) {
    return new TIndexBuilder::TTxCancelSetColumnConstraint(this, ev);
}

} // NSchemeShard
} // NKikimr

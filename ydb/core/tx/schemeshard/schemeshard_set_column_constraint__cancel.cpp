#include "schemeshard_impl.h"
#include "schemeshard_set_column_constraint.h"
#include "schemeshard_path.h"

namespace NKikimr {
namespace NSchemeShard {

using namespace NTabletFlatExecutor;

struct TSchemeShard::TTxSetColumnConstraintCancel : public TSchemeShard::TIndexBuilder::TTxBase {
private:
    TEvSetColumnConstraint::TEvCancelRequest::TPtr Request;
    THolder<TEvSetColumnConstraint::TEvCancelResponse> Response;

public:
    explicit TTxSetColumnConstraintCancel(TSelf* self, TEvSetColumnConstraint::TEvCancelRequest::TPtr& ev)
        : TTxBase(self, TIndexBuildId(ev->Get()->Record.GetOperationId()), TXTYPE_CANCEL_SET_COLUMN_CONSTRAINT)
        , Request(ev)
    {}

    bool Reply(Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS, const TString& errorMessage = TString()) {
        auto& record = Response->Record;
        record.SetStatus(status);
        if (errorMessage) {
            AddIssue(record.MutableIssues(), errorMessage);
        }

        LOG_N("TTxSetColumnConstraintCancel: Reply"
              << ", operationId: " << BuildId
              << ", status: " << Ydb::StatusIds::StatusCode_Name(status)
              << ", error: " << errorMessage);

        Send(Request->Sender, std::move(Response), 0, Request->Cookie);
        return true;
    }

    bool DoExecute(TTransactionContext& txc, const TActorContext&) override {
        const auto& record = Request->Get()->Record;
        LOG_I("TTxSetColumnConstraintCancel: DoExecute " << record.ShortDebugString());

        Response = MakeHolder<TEvSetColumnConstraint::TEvCancelResponse>(record.GetOperationId());

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

        if (operationInfo.IsDone()) {
            return Reply(
                Ydb::StatusIds::PRECONDITION_FAILED,
                TStringBuilder() << "SetColumnConstraint operation with id <" << BuildId << "> has been finished already"
            );
        }

        if (operationInfo.IsCancelled) {
            return Reply(
                Ydb::StatusIds::PRECONDITION_FAILED,
                TStringBuilder() << "SetColumnConstraint operation with id <" << BuildId << "> is already cancelled"
            );
        }

        // Mark operation as cancelled
        NIceDb::TNiceDb db(txc.DB);
        operationInfo.MarkAsCancelled(TString("Cancelled by user request"));
        Self->PersistSetColumnConstraintCancellation(db, operationInfo);

        // Trigger progress to handle the cancellation
        Progress(BuildId);

        return Reply();
    }

    void DoComplete(const TActorContext&) override {}

    void OnUnhandledException(TTransactionContext& /*txc*/, const TActorContext& /*ctx*/,
        TIndexBuildInfo* /*operationInfo*/, const std::exception& exc) override
    {
        LOG_E("TTxSetColumnConstraintCancel: OnUnhandledException"
            ", id# " << BuildId << ", exception: " << exc.what());
    }
};

ITransaction* TSchemeShard::CreateTxSetColumnConstraintCancel(TEvSetColumnConstraint::TEvCancelRequest::TPtr& ev) {
    return new TTxSetColumnConstraintCancel(this, ev);
}

void TSchemeShard::Handle(TEvSetColumnConstraint::TEvCancelRequest::TPtr& ev, const TActorContext& ctx) {
    Execute(CreateTxSetColumnConstraintCancel(ev), ctx);
}

} // NSchemeShard
} // NKikimr

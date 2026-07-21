#include "schemeshard_impl.h"
#include "schemeshard_set_column_constraint.h"
#include "index/index_build_info.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::FLAT_TX_SCHEMESHARD

namespace NKikimr::NSchemeShard {

using namespace NTabletFlatExecutor;

struct TSchemeShard::TIndexBuilder::TTxForgetSetColumnConstraint: public TRwTxBase {
    explicit TTxForgetSetColumnConstraint(TSelf* self, TEvSetColumnConstraint::TEvForgetRequest::TPtr& ev)
        : TRwTxBase(self)
        , Request(ev)
    {}

    TTxType GetTxType() const override {
        return TXTYPE_FORGET_SET_COLUMN_CONSTRAINT;
    }

    void DoExecute(TTransactionContext &txc, const TActorContext &ctx) override {
        const auto& request = Request->Get()->Record;
        YDB_LOG_DEBUG_CTX(ctx, "][SetColumnConstraint] TIndexBuilder::TTxForgetSetColumnConstraint DoExecute",
            {"selfTabletId", Self->SelfTabletId()},
            {"request", request.ShortDebugString()}
        );

        auto response = MakeHolder<TEvSetColumnConstraint::TEvForgetResponse>(request.GetTxId());
        TPath database = TPath::Resolve(request.GetDatabaseName(), Self);
        if (!database.IsResolved()) {
            return Reply(
                std::move(response),
                Ydb::StatusIds::NOT_FOUND,
                TStringBuilder() << "Database " << request.GetDatabaseName() << " not found"
            );
        }
        const TPathId subdomainPathId = database.GetPathIdForDomain();

        auto operationId = TIndexBuildId(request.GetOperationId());
        const auto* operationInfoPtr = Self->SetColumnConstraintOperations.FindPtr(operationId);
        if (!operationInfoPtr) {
            return Reply(
                std::move(response),
                Ydb::StatusIds::NOT_FOUND,
                TStringBuilder() << "Set column constraint operation with id " << operationId << " not found"
            );
        }
        auto& operationInfo = *operationInfoPtr->get();
        if (operationInfo.DomainPathId != subdomainPathId) {
            return Reply(
                std::move(response),
                Ydb::StatusIds::NOT_FOUND,
                TStringBuilder() << "Set column constraint operation with id " << operationId << " not found in database " << request.GetDatabaseName()
            );
        }

        if (!operationInfo.IsFinished()) {
            return Reply(
                std::move(response),
                Ydb::StatusIds::PRECONDITION_FAILED,
                TStringBuilder() << "Set column constraint operation with id " << operationId << " hasn't been finished yet"
            );
        }

        NIceDb::TNiceDb db(txc.DB);
        Self->ForgetSetColumnConstraint(db, operationInfo);

        Reply(std::move(response));

        SideEffects.ApplyOnExecute(Self, txc, ctx);
    }

    void DoComplete(const TActorContext &ctx) override {
        YDB_LOG_DEBUG_CTX(ctx, "][SetColumnConstraint] TIndexBuilder::TTxForgetSetColumnConstraint DoComplete",
            {"selfTabletId", Self->SelfTabletId()},
            {"requestRecord", Request->Get()->Record.ShortDebugString()}
        );
        SideEffects.ApplyOnComplete(Self, ctx);
    }

private:
    void Reply(
        THolder<TEvSetColumnConstraint::TEvForgetResponse> response,
        const Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS,
        const TString& errorMessage = TString())
    {
        auto& record = response->Record;
        record.SetStatus(status);
        if (errorMessage) {
            auto& issue = *record.MutableIssues()->Add();
            issue.set_severity(NYql::TSeverityIds::S_ERROR);
            issue.set_message(errorMessage);
        }

        SideEffects.Send(Request->Sender, std::move(response), 0, Request->Cookie);
    }

private:
    TSideEffects SideEffects;
    TEvSetColumnConstraint::TEvForgetRequest::TPtr Request;
};

ITransaction* TSchemeShard::CreateTxForgetSetColumnConstraint(TEvSetColumnConstraint::TEvForgetRequest::TPtr& ev) {
    return new TIndexBuilder::TTxForgetSetColumnConstraint(this, ev);
}

} // namespace NKikimr::NSchemeShard

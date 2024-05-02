#include "start.h"
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

THolder<NKikimr::NSchemeShard::TProposeResponse> TStartAlterColumnTable::Propose(const TString&, TOperationContext& context) {
    const TTabletId ssId = context.SS->SelfTabletId();

    auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), ui64(ssId));

    const TString& parentPathStr = Transaction.GetWorkingDir();
    const TString& name = Transaction.HasAlterColumnTable() ? Transaction.GetAlterColumnTable().GetName() : Transaction.GetAlterTable().GetName();
    LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "TAlterColumnTable Propose"
        << ", path: " << parentPathStr << "/" << name
        << ", opId: " << OperationId
        << ", at schemeshard: " << ssId);

    TPath path = TPath::Resolve(parentPathStr, context.SS).Dive(name);
    {
        TPath::TChecker checks = path.Check();
        checks
            .NotEmpty()
            .NotUnderDomainUpgrade()
            .IsAtLocalSchemeShard()
            .IsResolved()
            .NotDeleted()
            .IsColumnTable()
            .NotUnderOperation();

        if (!checks) {
            result->SetError(checks.GetStatus(), checks.GetError());
            return result;
        }
    }

    auto tableInfo = context.SS->ColumnTables.GetVerifiedPtr(path.Base()->PathId);

    if (tableInfo->AlterVersion == 0) {
        result->SetError(NKikimrScheme::StatusMultipleModifications, "Table is not created yet");
        return result;
    }
    TProposeErrorCollector errors(*result);
    std::shared_ptr<ISSEntity> originalEntity = ISSEntity::GetPathEntityVerified(context, path);
    TUpdateInitializationContext uContext(&context, &Transaction);
    NIceDb::TNiceDb db(context.GetDB());
    {
        TStartUpdateContext sContext(&path, &context, &db);
        auto conclusion = originalEntity->StartUpdate(uContext, sContext, originalEntity);
        if (conclusion.IsFail()) {
            errors.AddError(conclusion.GetErrorMessage());
            return result;
        }
    }

    TString errStr;
    if (!context.SS->CheckApplyIf(Transaction, errStr)) {
        result->SetError(NKikimrScheme::StatusPreconditionFailed, errStr);
        return result;
    }


    TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxAlterColumnTable, path->PathId);
    originalEntity->Persist(TDBWriteContext(&context, &db));
    txState.State = TTxState::Done;
    path->LastTxId = OperationId.GetTxId();
    path->PathState = TPathElement::EPathState::EPathStateAlter;
    context.SS->PersistLastTxId(db, path.Base());
    context.SS->PersistTxState(db, OperationId);

    context.OnComplete.ActivateTx(OperationId);

    SetState(TTxState::Done);
    return result;
}

void TStartAlterColumnTable::AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) {
    LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "TAlterColumnTable AbortUnsafe"
        << ", opId: " << OperationId
        << ", forceDropId: " << forceDropTxId
        << ", at schemeshard: " << context.SS->TabletID());

    context.OnComplete.DoneOperation(OperationId);
}

}

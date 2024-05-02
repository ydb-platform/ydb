#include "finish.h"
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

THolder<NKikimr::NSchemeShard::TProposeResponse> TFinishAlterColumnTable::Propose(const TString&, TOperationContext& context) {
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
    std::shared_ptr<ISSEntity> originalEntity = ISSEntity::GetPathEntityVerified(context, path);

    TProposeErrorCollector errors(*result);
    std::shared_ptr<ISSEntityEvolution> evolution = originalEntity->GetCurrentEvolution();
    if (!!evolution) {
        errors.AddError("has evolutions for processing!!");
        return result;
    }

    NIceDb::TNiceDb db(context.GetDB());

    TTxState* txState = context.SS->FindTxSafe(OperationId, TTxState::TxAlterColumnTable);
    txState->State = TTxState::Done;
    originalEntity->FinishUpdate();

    TDBWriteContext dbContext(&context, &db);
    originalEntity->Persist(dbContext);
    context.SS->PersistColumnTableAlterRemove(db, originalEntity->GetPathId());

    SetState(TTxState::Done);
    return result;
}

void TFinishAlterColumnTable::AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) {
    LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "TAlterColumnTable AbortUnsafe"
        << ", opId: " << OperationId
        << ", forceDropId: " << forceDropTxId
        << ", at schemeshard: " << context.SS->TabletID());

    context.OnComplete.DoneOperation(OperationId);
}

}

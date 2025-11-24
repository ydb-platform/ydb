#include "schemeshard__operation_common.h"
#include "schemeshard__operation_part.h"
#include "schemeshard_impl.h"

namespace {

using namespace NKikimr;
using namespace NSchemeShard;

class TTruncateTable: public TSubOperationBase {
public:
    using TSubOperationBase::TSubOperationBase;

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        Y_UNUSED(context);
        return {};
    }

    void AbortPropose(TOperationContext&) override {
        Y_ABORT("no AbortPropose for TTruncateTable");
    }

    bool ProgressState(TOperationContext& context) override {
        Y_UNUSED(context);
        // LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        //            "TAlterUserAttrs ProgressState"
        //                << ", opId: " << OperationId
        //                << ", at schemeshard: " << context.SS->TabletID());

        // TTxState* txState = context.SS->FindTx(OperationId);
        // Y_ABORT_UNLESS(txState);

        // context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return true;
    }

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        Y_UNUSED(ev);
        Y_UNUSED(context);
        // const TStepId step = TStepId(ev->Get()->StepId);
        // const TTabletId ssId = context.SS->SelfTabletId();

        // LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        //            "TAlterUserAttrs HandleReply TEvOperationPlan"
        //                << ", opId: " << OperationId
        //                << ", stepId:" << step
        //                << ", at schemeshard: " << ssId);

        // TTxState* txState = context.SS->FindTx(OperationId);
        // Y_ABORT_UNLESS(txState);

        // if (txState->State != TTxState::Propose) {
        //     LOG_WARN_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        //                "Duplicate PlanStep opId#" << OperationId
        //                    << " at schemeshard: " << ssId
        //                    << " txState is in state#" << TTxState::StateName(txState->State));
        //     return true;
        // }

        // Y_ABORT_UNLESS(txState->TxType == TTxState::TxAlterUserAttributes);

        // TPathId pathId = txState->TargetPathId;
        // TPathElement::TPtr path = context.SS->PathsById.at(pathId);
        // context.OnComplete.ReleasePathState(OperationId, pathId, TPathElement::EPathState::EPathStateNoChanges);

        // NIceDb::TNiceDb db(context.GetDB());

        // Y_ABORT_UNLESS(path->UserAttrs);
        // Y_ABORT_UNLESS(path->UserAttrs->AlterData);
        // Y_ABORT_UNLESS(path->UserAttrs->AlterVersion < path->UserAttrs->AlterData->AlterVersion);
        // context.SS->ApplyAndPersistUserAttrs(db, path->PathId);

        // context.SS->ClearDescribePathCaches(path);
        // context.OnComplete.PublishToSchemeBoard(OperationId, pathId);

        // context.OnComplete.UpdateTenants({pathId});

        // context.OnComplete.DoneOperation(OperationId);
        return true;
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        Y_UNUSED(forceDropTxId);
        Y_UNUSED(context);
        // LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        //              "TAlterUserAttrs AbortUnsafe"
        //                  << ", opId: " << OperationId
        //                  << ", forceDropId: " << forceDropTxId
        //                  << ", at schemeshard: " << context.SS->TabletID());

        // context.OnComplete.DoneOperation(OperationId);
    }
};

}

namespace NKikimr::NSchemeShard {

ISubOperation::TPtr CreateTruncateTable(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TTruncateTable>(id, tx);
}

ISubOperation::TPtr CreateTruncateTable(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state == TTxState::Invalid || state == TTxState::Propose);
    return MakeSubOperation<TTruncateTable>(id);
}

}

#include "schemeshard__operation_common.h"
#include "schemeshard__operation_part.h"
#include "schemeshard_impl.h"
#include "schemeshard_private.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDBLOG_THIS_FILE_COMPONENT NKikimrServices::FLAT_TX_SCHEMESHARD

namespace {

using namespace NKikimr;
using namespace NSchemeShard;

class TRmDir: public TSubOperationBase {
public:
    using TSubOperationBase::TSubOperationBase;

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        const TTabletId ssId = context.SS->SelfTabletId();

        const auto& drop = Transaction.GetDrop();

        const TString& parentPathStr = Transaction.GetWorkingDir();
        const TString& name = drop.GetName();

        YDBLOG_CTX_NOTICE(context.Ctx, "TRmDir Propose, path: /, pathId: , opId: , at schemeshard: ",
            {"path", parentPathStr},
            {"#_name", name},
            {"pathId", drop.GetId()},
            {"opId", OperationId},
            {"schemeshard", ssId});

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), ui64(ssId));

        TPath path = drop.HasId()
            ? TPath::Init(context.SS->MakeLocalId(drop.GetId()), context.SS)
            : TPath::Resolve(parentPathStr, context.SS).Dive(name);
        {
            TPath::TChecker checks = path.Check();
            checks
                .NotEmpty()
                .NotUnderDomainUpgrade()
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotRoot()
                .NotDeleted()
                .NotUnderDeleting()
                .IsDirectory()
                .NotUnderOperation()
                .IsCommonSensePath()
                .NotChildren(NKikimrScheme::StatusNameConflict);

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                if (path.IsResolved() && path.Base()->IsDirectory() && (path.Base()->PlannedToDrop() || path.Base()->Dropped())) {
                    result->SetPathDropTxId(ui64(path.Base()->DropTxId));
                    result->SetPathId(path.Base()->PathId.LocalPathId);
                }
                return result;
            }
        }

        TString errStr;
        if (!context.SS->CheckApplyIf(Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, errStr);
            return result;
        }

        NIceDb::TNiceDb db(context.GetDB());
        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxRmDir, path.Base()->PathId);
        txState.State = TTxState::Propose;
        txState.MinStep = TStepId(1);

        const TPathElement::TPtr pathElement = context.SS->PathsById.at(path.Base()->PathId);
        if (pathElement->TempDirOwnerActorId) {
            YDBLOG_CTX_DEBUG(context.Ctx, "Processing remove temp directory with Name: , WorkingDir: , TempDirOwnerActorId: ",
                {"Name", name},
                {"WorkingDir", parentPathStr},
                {"TempDirOwnerActorId", pathElement->TempDirOwnerActorId});
            context.OnComplete.UpdateTempDirsToRemoveState(pathElement->TempDirOwnerActorId, path.Base()->PathId);
        }

        context.OnComplete.ActivateTx(OperationId);

        path.Base()->PathState = TPathElement::EPathState::EPathStateDrop;
        path.Base()->DropTxId = OperationId.GetTxId();
        path.Base()->LastTxId = OperationId.GetTxId();

        context.SS->PersistTxState(db, OperationId);

        context.SS->TabletCounters->Simple()[COUNTER_DIR_COUNT].Sub(1);

        auto parentDir = path.Parent();
        ++parentDir.Base()->DirAlterVersion;
        context.SS->PersistPathDirAlterVersion(db, parentDir.Base());
        context.SS->ClearDescribePathCaches(parentDir.Base());
        context.SS->ClearDescribePathCaches(path.Base());

        if (!context.SS->DisablePublicationsOfDropping) {
            context.OnComplete.PublishToSchemeBoard(OperationId, parentDir.Base()->PathId);
            context.OnComplete.PublishToSchemeBoard(OperationId, path.Base()->PathId);
        }

        return result;
    }

    void AbortPropose(TOperationContext&) override {
        Y_ABORT("no AbortPropose for TRmDir");
    }

    bool ProgressState(TOperationContext& context) override {
        YDBLOG_CTX_INFO(context.Ctx, "TRmDir ProgressState, opId: , at schemeshard: ",
            {"opId", OperationId},
            {"schemeshard", context.SS->TabletID()});

        TTxState* txState = context.SS->FindTx(OperationId);

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return true;
    }

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        const TStepId step = TStepId(ev->Get()->StepId);
        const TTabletId ssId = context.SS->SelfTabletId();

        YDBLOG_CTX_INFO(context.Ctx, "TRmDir HandleReply TEvOperationPlan, opId: , step: , at schemeshard: ",
            {"opId", OperationId},
            {"step", step},
            {"schemeshard", ssId});


        TTxState* txState = context.SS->FindTx(OperationId);

        if (!txState) {
            YDBLOG_CTX_WARN(context.Ctx, "txState is nullptr, considered as duplicate PlanStep, opId: , at schemeshard: ",
                {"opId", OperationId},
                {"schemeshard", ssId});
            return true;
        }

        if (txState->State != TTxState::Propose) {
            YDBLOG_CTX_WARN(context.Ctx, "Duplicate PlanStep, opId: , state: , at schemeshard: ",
                {"opId", OperationId},
                {"state", TTxState::StateName(txState->State)},
                {"schemeshard", ssId});
            return true;
        }

        NIceDb::TNiceDb db(context.GetDB());

        TPathId pathId = txState->TargetPathId;
        auto path = context.SS->PathsById.at(pathId);
        auto parentDir = context.SS->PathsById.at(path->ParentPathId);

        Y_ABORT_UNLESS(!path->Dropped());
        path->SetDropped(step, OperationId.GetTxId());
        context.SS->PersistDropStep(db, pathId, step, OperationId);

        const EPathCategory pathCategory = path->IsSystemDirectory() ? EPathCategory::System : EPathCategory::Regular;
        auto domainInfo = context.SS->ResolveDomainInfo(pathId);
        domainInfo->DecPathsInside(context.SS, 1, pathCategory);
        DecAliveChildrenDirect(OperationId, parentDir, context); // for correct discard of ChildrenExist prop

        ++parentDir->DirAlterVersion;
        context.SS->PersistPathDirAlterVersion(db, parentDir);
        context.SS->ClearDescribePathCaches(parentDir);
        context.SS->ClearDescribePathCaches(path);

        if (!context.SS->DisablePublicationsOfDropping) {
            context.OnComplete.PublishToSchemeBoard(OperationId, parentDir->PathId);
            context.OnComplete.PublishToSchemeBoard(OperationId, pathId);
        }

        context.SS->TabletCounters->Simple()[COUNTER_USER_ATTRIBUTES_COUNT].Sub(path->UserAttrs->Size());
        context.SS->PersistUserAttributes(db, path->PathId, path->UserAttrs, nullptr);

        YDBLOG_CTX_DEBUG(context.Ctx, "RmDir is done, opId: , at schemeshard: ",
            {"opId", OperationId},
            {"schemeshard", ssId});

        context.OnComplete.DoneOperation(OperationId);
        return true;
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        YDBLOG_CTX_NOTICE(context.Ctx, "RmDir AbortUnsafe, opId: , forceDropId: , at schemeshard: ",
            {"opId", OperationId},
            {"forceDropId", forceDropTxId},
            {"schemeshard", context.SS->TabletID()});

        context.OnComplete.DoneOperation(OperationId);
    }
};

}

namespace NKikimr::NSchemeShard {

ISubOperation::TPtr CreateRmDir(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TRmDir>(id, tx);
}

ISubOperation::TPtr CreateRmDir(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state == TTxState::Invalid || state == TTxState::Propose);
    return MakeSubOperation<TRmDir>(id);
}

}

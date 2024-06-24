#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"

namespace {

using namespace NKikimr;
using namespace NSchemeShard;

class TPropose: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
            << "MkDir::TPropose"
            << " operationId#" << OperationId;
    }

public:
    TPropose(TOperationId id)
        : OperationId(id)
    {}

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        const TStepId step = TStepId(ev->Get()->StepId);
        const TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvPrivate::TEvOperationPlan"
                       << ", step: " << step
                       << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxMkDir);

        auto pathId = txState->TargetPathId;
        auto path = TPath::Init(pathId, context.SS);

        context.SS->TabletCounters->Simple()[COUNTER_DIR_COUNT].Add(1);

        NIceDb::TNiceDb db(context.GetDB());

        path.Base()->DirAlterVersion += 1;
        context.SS->PersistPathDirAlterVersion(db, path.Base());

        path.Base()->StepCreated = step;
        context.SS->PersistCreateStep(db, pathId, step);

        IncParentDirAlterVersionWithRepublish(OperationId, path, context);

        context.SS->ChangeTxState(db, OperationId, TTxState::Done);
        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        auto ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxMkDir);

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }
};

class TMkDir: public TSubOperation {
    static TTxState::ETxState NextState() {
        return TTxState::Propose;
    }

    TTxState::ETxState NextState(TTxState::ETxState state) const override {
        switch (state) {
        case TTxState::Waiting:
        case TTxState::Propose:
            return TTxState::Done;
        default:
            return TTxState::Invalid;
        }
    }

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) override {
        switch (state) {
        case TTxState::Waiting:
        case TTxState::Propose:
            return MakeHolder<TPropose>(OperationId);
        case TTxState::Done:
            return MakeHolder<TDone>(OperationId);
        default:
            return nullptr;
        }
    }

public:
    using TSubOperation::TSubOperation;

    THolder<TProposeResponse> Propose(const TString& owner, TOperationContext& context) override {
        const auto ssId = context.SS->SelfTabletId();

        const auto acceptExisted = !Transaction.GetFailOnExist();
        const TString& parentPathStr = Transaction.GetWorkingDir();
        const TString& name = Transaction.GetMkDir().GetName();

        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TMkDir Propose"
                         << ", path: " << parentPathStr << "/" << name
                         << ", operationId: " << OperationId
                         << ", at schemeshard: " << ssId);

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), ui64(ssId));

        if (Transaction.HasTempDirOwnerActorId() && !context.SS->EnableTempTables) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed,
                TStringBuilder() << "It is not allowed to create temporary objects: " << name);
            return result;
        }

        if (Transaction.HasTempDirOwnerActorId() && Transaction.GetAllowCreateInTempDir()) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed,
                TStringBuilder() << "Can't create temporary directory while flag AllowCreateInTempDir is set."
                    << " Temporary directory can't be created in another temporary directory.");
            return result;
        }

        NSchemeShard::TPath parentPath = NSchemeShard::TPath::Resolve(parentPathStr, context.SS);
        {
            NSchemeShard::TPath::TChecker checks = parentPath.Check();
            checks
                .NotUnderDomainUpgrade()
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .NotUnderDeleting()
                .IsCommonSensePath()
                .IsLikeDirectory()
                .FailOnRestrictedCreateInTempZone(Transaction.GetAllowCreateInTempDir());

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        const TString acl = Transaction.GetModifyACL().GetDiffACL();

        NSchemeShard::TPath dstPath = parentPath.Child(name);
        {
            NSchemeShard::TPath::TChecker checks = dstPath.Check();
            checks.IsAtLocalSchemeShard();
            if (dstPath.IsResolved()) {
                checks
                    .IsResolved()
                    .NotUnderDeleting()
                    .FailOnExist({
                            TPathElement::EPathType::EPathTypeDir,
                            TPathElement::EPathType::EPathTypeSubDomain,
                            TPathElement::EPathType::EPathTypeExtSubDomain,
                            TPathElement::EPathType::EPathTypeColumnStore
                        }, acceptExisted);
            } else {
                checks
                    .NotEmpty()
                    .NotResolved();
            }

            if (checks) {
                checks
                    .IsValidLeafName()
                    .IsValidACL(acl);
            }

            if (checks && !context.SS->SystemBackupSIDs.contains(owner)) {
                checks
                    .DepthLimit()
                    .PathsLimit()
                    .DirChildrenLimit();
            }

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                if (dstPath.IsResolved()) {
                    result->SetPathCreateTxId(ui64(dstPath.Base()->CreateTxId));
                    result->SetPathId(dstPath.Base()->PathId.LocalPathId);
                }
                return result;
            }
        }

        TString errStr;

        if (!context.SS->CheckApplyIf(Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, errStr);
            return result;
        }

        TUserAttributes::TPtr userAttrs = new TUserAttributes(1);
        const auto& userAttrsDetails = Transaction.GetAlterUserAttributes();
        if (!userAttrs->ApplyPatch(EUserAttributesOp::MkDir, userAttrsDetails, errStr) ||
            !userAttrs->CheckLimits(errStr))
        {
            result->SetError(NKikimrScheme::StatusInvalidParameter, errStr);
            return result;
        }

        auto guard = context.DbGuard();
        TPathId allocatedPathId = context.SS->AllocatePathId();
        context.MemChanges.GrabNewPath(context.SS, allocatedPathId);
        context.MemChanges.GrabPath(context.SS, parentPath.Base()->PathId);
        context.MemChanges.GrabNewTxState(context.SS, OperationId);
        context.MemChanges.GrabDomain(context.SS, parentPath.GetPathIdForDomain());

        context.DbChanges.PersistPath(allocatedPathId);
        context.DbChanges.PersistPath(parentPath.Base()->PathId);
        context.DbChanges.PersistApplyUserAttrs(allocatedPathId);
        context.DbChanges.PersistTxState(OperationId);

        dstPath.MaterializeLeaf(owner, allocatedPathId);
        result->SetPathId(dstPath.Base()->PathId.LocalPathId);

        TPathElement::TPtr newDir = dstPath.Base();
        newDir->CreateTxId = OperationId.GetTxId();
        newDir->PathState = TPathElement::EPathState::EPathStateCreate;
        newDir->PathType = TPathElement::EPathType::EPathTypeDir;
        newDir->UserAttrs->AlterData = userAttrs;
        newDir->DirAlterVersion = 1;

        if (Transaction.HasTempDirOwnerActorId()) {
            newDir->TempDirOwnerActorId = ActorIdFromProto(Transaction.GetTempDirOwnerActorId());
        }

        if (!acl.empty()) {
            newDir->ApplyACL(acl);
        }

        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxMkDir, newDir->PathId);
        txState.State = TTxState::Propose;

        if (parentPath.Base()->HasActiveChanges()) {
            auto parentTxId = parentPath.Base()->PlannedToCreate() ? parentPath.Base()->CreateTxId : parentPath.Base()->LastTxId;
            context.OnComplete.Dependence(parentTxId, OperationId.GetTxId());
        }

        IncParentDirAlterVersionWithRepublishSafeWithUndo(OperationId, dstPath, context.SS, context.OnComplete);

        if (Transaction.HasTempDirOwnerActorId()) {
            LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "Processing create temp directory with Name: " << name
                    << ", WorkingDir: " << parentPathStr
                    << ", TempDirOwnerActorId: " << newDir->TempDirOwnerActorId
                    << ", PathId: " << newDir->PathId);
            context.OnComplete.UpdateTempDirsToMakeState(
                newDir->TempDirOwnerActorId, newDir->PathId);
        }

        dstPath.DomainInfo()->IncPathsInside();
        parentPath.Base()->IncAliveChildren();

        context.OnComplete.ActivateTx(OperationId);

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext& context) override {
        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                  "MkDir AbortPropose"
                       << ", opId: " << OperationId
                       << ", at schemeshard: " << context.SS->TabletID());
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TMkDir AbortUnsafe"
                         << ", opId: " << OperationId
                         << ", forceDropId: " << forceDropTxId
                         << ", at schemeshard: " << context.SS->TabletID());

        context.OnComplete.DoneOperation(OperationId);
    }
};

}

namespace NKikimr::NSchemeShard {

ISubOperation::TPtr CreateMkDir(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TMkDir>(id, tx);
}

ISubOperation::TPtr CreateMkDir(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    return MakeSubOperation<TMkDir>(id, state);
}

}

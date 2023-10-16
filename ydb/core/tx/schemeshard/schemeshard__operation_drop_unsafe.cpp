#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"

#include <ydb/core/base/subdomain.h>

namespace {

using namespace NKikimr;
using namespace NSchemeShard;

class TProposedDeletePart: public TDeletePartsAndDone {
public:
    explicit TProposedDeletePart(const TOperationId& id)
        : TDeletePartsAndDone(id)
    {
        IgnoreMessages(DebugHint(), AllIncomingEvents());
    }
};

class TPropose: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
                << "TDropForceUnsafe TPropose"
                << ", operationId: " << OperationId;
    }
public:
    TPropose(TOperationId id)
        : OperationId(id)
    {
        TSet<ui32> toIgnore = AllIncomingEvents();
        toIgnore.erase(TEvPrivate::TEvOperationPlan::EventType);

        IgnoreMessages(DebugHint(), toIgnore);
    }

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        TStepId step = TStepId(ev->Get()->StepId);
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvOperationPlan"
                               << ", step: " << step
                               << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxForceDropSubDomain);

        TPathId pathId = txState->TargetPathId;
        TPathElement::TPtr path = context.SS->PathsById.at(pathId);
        auto paths = context.SS->ListSubTree(pathId, context.Ctx);

        NIceDb::TNiceDb db(context.GetDB());

        context.SS->DropPaths(paths, step, OperationId.GetTxId(), db, context.Ctx);

        auto parentDir = context.SS->PathsById.at(path->ParentPathId);
        ++parentDir->DirAlterVersion;
        context.SS->PersistPathDirAlterVersion(db, parentDir);
        context.SS->ClearDescribePathCaches(parentDir);

        if (!context.SS->DisablePublicationsOfDropping) {
            context.OnComplete.PublishToSchemeBoard(OperationId, parentDir->PathId);
            for (const TPathId pathId : paths) {
                context.OnComplete.PublishToSchemeBoard(OperationId, pathId);
            }
        }

        context.SS->ChangeTxState(db, OperationId, TTxState::ProposedDeleteParts);
        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxForceDropSubDomain);

        auto paths = context.SS->ListSubTree(txState->TargetPathId, context.Ctx);
        NForceDrop::ValidateNoTransactionOnPaths(OperationId, paths, context);
        context.SS->MarkAsDropping(paths, OperationId.GetTxId(), context.Ctx);
        NForceDrop::CollectShards(paths, OperationId, txState, context);

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }
};

class TDropForceUnsafe: public TSubOperation {
    TPathElement::EPathType ExpectedType = TPathElement::EPathType::EPathTypeInvalid;

    static TTxState::ETxState NextState() {
        return TTxState::Propose;
    }

    TTxState::ETxState NextState(TTxState::ETxState state) const override {
        switch (state) {
        case TTxState::Propose:
            return TTxState::ProposedDeleteParts;
        default:
            return TTxState::Invalid;
        }
    }

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) override {
        switch (state) {
        case TTxState::Waiting:
        case TTxState::Propose:
            return MakeHolder<TPropose>(OperationId);
        case TTxState::ProposedDeleteParts:
            return MakeHolder<TProposedDeletePart>(OperationId);
        default:
            return nullptr;
        }
    }

public:
    explicit TDropForceUnsafe(const TOperationId& id, const TTxTransaction& tx, TPathElement::EPathType expectedType)
        : TSubOperation(id, tx)
        , ExpectedType(expectedType)
    {
    }

    explicit TDropForceUnsafe(const TOperationId& id, TTxState::ETxState state)
        : TSubOperation(id, state)
    {
    }

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        const TTabletId ssId = context.SS->SelfTabletId();

        const auto& drop = Transaction.GetDrop();

        const TString& parentPathStr = Transaction.GetWorkingDir();
        const TString& name = drop.GetName();

        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TDropForceUnsafe Propose"
                         << ", path: " << parentPathStr << "/" << name
                         << ", pathId: " << drop.GetId()
                         << ", opId: " << OperationId
                         << ", at schemeshard: " << ssId);

        if (ExpectedType == TPathElement::EPathType::EPathTypeInvalid) {
            LOG_WARN_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                       " UNSAFE DELETION IS CALLED."
                       " TDropForceUnsafe is UNSAFE operation."
                       " Usually it is called for deleting user's DB (tenant)."
                       " But it could be triggered by administrator for special emergency cases. And there is that case."
                       " I hope you are aware of the problems with it."
                       " 1: Shared transactions among the tables could be broken if one of the tables is force dropped. Dependent transactions on other tables could be blocked forever."
                       " 2: Loans are going to be lost. Force dropped tablets are never return loans. Some tablets would be waiting for borrowed blocks forever."
                       " Details"
                             << ": path: " << parentPathStr << "/" << name
                             << ", pathId: " << drop.GetId()
                             << ", opId: " << OperationId
                             << ", at schemeshard: " << ssId);
        }

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), ui64(ssId));

        TPath path = drop.HasId()
            ? TPath::Init(context.SS->MakeLocalId(drop.GetId()), context.SS)
            : TPath::Resolve(parentPathStr, context.SS).Dive(name);

        {
            TPath::TChecker checks = path.Check();
            checks
                .NotEmpty()
                .IsResolved()
                .NotRoot()
                .NotDeleted()
                .IsCommonSensePath();

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        if (path.IsUnderCreating()) {
            TPath parent = path.Parent();
            if (parent.IsUnderCreating()) {
                TPath::TChecker checks = parent.Check();
                checks
                    .NotUnderTheSameOperation(path.ActiveOperation(), NKikimrScheme::StatusMultipleModifications);

                if (!checks) {
                    result->SetError(checks.GetStatus(), checks.GetError());
                    return result;
                }
            }
        }

        if (ExpectedType != TPathElement::EPathType::EPathTypeInvalid) {
            if (path.Base()->PathType != ExpectedType) {
                result->SetError(NKikimrScheme::StatusNameConflict, "Drop on mismatch item");
                return result;
            }
        }

        TString errStr;
        if (!context.SS->CheckApplyIf(Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, errStr);
            return result;
        }

        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxForceDropSubDomain, path.Base()->PathId);
        txState.State = TTxState::Waiting;
        txState.MinStep = TStepId(1);

        NIceDb::TNiceDb db(context.GetDB());

        auto paths = context.SS->ListSubTree(path.Base()->PathId, context.Ctx);

        auto relatedTx = context.SS->GetRelatedTransactions(paths, context.Ctx);
        for (auto otherTxId: relatedTx) {
            if (otherTxId == OperationId.GetTxId()) {
                continue;
            }

            LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                         "TDropForceUnsafe Propose dependence has found"
                             << ", dependent transaction: " << OperationId.GetTxId()
                             << ", parent transaction: " << otherTxId
                             << ", at schemeshard: " << ssId);

            context.OnComplete.Dependence(otherTxId, OperationId.GetTxId());

            Y_ABORT_UNLESS(context.SS->Operations.contains(otherTxId));
            auto otherOperation = context.SS->Operations.at(otherTxId);
            for (ui32 partId = 0; partId < otherOperation->Parts.size(); ++partId) {
                if (auto part = otherOperation->Parts.at(partId)) {
                    part->AbortUnsafe(OperationId.GetTxId(), context);
                }
            }
        }

        context.SS->MarkAsDropping(paths, OperationId.GetTxId(), context.Ctx);

        txState.State = TTxState::Propose;
        context.OnComplete.ActivateTx(OperationId);

        context.SS->PersistTxState(db, OperationId);

        auto parentDir = path.Parent();
        ++parentDir.Base()->DirAlterVersion;
        context.SS->PersistPathDirAlterVersion(db, parentDir.Base());
        context.SS->ClearDescribePathCaches(parentDir.Base());
        context.SS->ClearDescribePathCaches(path.Base());

        if (!context.SS->DisablePublicationsOfDropping) {
            context.OnComplete.PublishToSchemeBoard(OperationId, parentDir.Base()->PathId);
            context.OnComplete.PublishToSchemeBoard(OperationId, path.Base()->PathId);
        }

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext&) override {
        Y_ABORT("no AbortPropose for TDropForceUnsafe");
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        AbortUnsafeDropOperation(OperationId, forceDropTxId, context);
    }
};

}

namespace NKikimr::NSchemeShard {

ISubOperation::TPtr CreateForceDropUnsafe(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TDropForceUnsafe>(id, tx, TPathElement::EPathType::EPathTypeInvalid);
}

ISubOperation::TPtr CreateForceDropUnsafe(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    return MakeSubOperation<TDropForceUnsafe>(id, state);
}

ISubOperation::TPtr CreateForceDropSubDomain(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TDropForceUnsafe>(id, tx, TPathElement::EPathType::EPathTypeSubDomain);
}

ISubOperation::TPtr CreateForceDropSubDomain(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    return MakeSubOperation<TDropForceUnsafe>(id, state);
}

}

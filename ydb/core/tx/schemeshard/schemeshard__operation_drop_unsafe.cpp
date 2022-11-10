#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"

#include <ydb/core/base/subdomain.h>

namespace {

using namespace NKikimr;
using namespace NSchemeShard;

class TProposedDeletePart: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
                << "TDropForceUnsafe TProposedDeletePart"
                << ", operationId: " << OperationId;
    }

public:
    TProposedDeletePart(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), AllIncomingEvents());
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);

        // Initiate asynchonous deletion of all shards
        for (auto shard : txState->Shards) {
            context.OnComplete.DeleteShard(shard.Idx);
        }

        context.OnComplete.DoneOperation(OperationId);
        return true;
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
        Y_VERIFY(txState->TxType == TTxState::TxForceDropSubDomain);

        TPathId pathId = txState->TargetPathId;
        TPathElement::TPtr path = context.SS->PathsById.at(pathId);
        auto pathes = context.SS->ListSubThee(pathId, context.Ctx);

        NIceDb::TNiceDb db(context.GetDB());

        context.SS->DropPathes(pathes, step, OperationId.GetTxId(), db, context.Ctx);

        auto parentDir = context.SS->PathsById.at(path->ParentPathId);
        ++parentDir->DirAlterVersion;
        context.SS->PersistPathDirAlterVersion(db, parentDir);
        context.SS->ClearDescribePathCaches(parentDir);

        if (!context.SS->DisablePublicationsOfDropping) {
            context.OnComplete.PublishToSchemeBoard(OperationId, parentDir->PathId);
            for (const TPathId pathId : pathes) {
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
        Y_VERIFY(txState);
        Y_VERIFY(txState->TxType == TTxState::TxForceDropSubDomain);

        auto pathes = context.SS->ListSubThee(txState->TargetPathId, context.Ctx);
        NForceDrop::ValidateNoTrasactionOnPathes(OperationId, pathes, context);
        context.SS->MarkAsDroping(pathes, OperationId.GetTxId(), context.Ctx);
        NForceDrop::CollectShards(pathes, OperationId, txState, context);

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }
};


class TDropForceUnsafe: public TSubOperation {
    const TOperationId OperationId;
    const TTxTransaction Transaction;

    TTxState::ETxState State = TTxState::Invalid;
    TPathElement::EPathType ExpectedType = TPathElement::EPathType::EPathTypeInvalid;

    TTxState::ETxState NextState() {
        return TTxState::Propose;
    }

    TTxState::ETxState NextState(TTxState::ETxState state) {
        switch(state) {
        case TTxState::Propose:
            return TTxState::ProposedDeleteParts;
        default:
            return TTxState::Invalid;
        }
        return TTxState::Invalid;
    }

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) {
        switch(state) {
        case TTxState::Waiting:
        case TTxState::Propose:
            return MakeHolder<TPropose>(OperationId);
        case TTxState::ProposedDeleteParts:
            return MakeHolder<TProposedDeletePart>(OperationId);
        default:
            return nullptr;
        }
    }

    void StateDone(TOperationContext& context) override {
        State = NextState(State);

        if (State != TTxState::Invalid) {
            SetState(SelectStateFunc(State));
            context.OnComplete.ActivateTx(OperationId);
        }
    }

public:
    TDropForceUnsafe(TOperationId id, const TTxTransaction& tx, TPathElement::EPathType expectedType)
        : OperationId(id)
        , Transaction(tx)
        , ExpectedType(expectedType)
    {}

    TDropForceUnsafe(TOperationId id, TTxState::ETxState state)
        : OperationId(id)
        , State(state)
    {
        SetState(SelectStateFunc(state));
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
                       " Nornally it is called for deleting user's DB (tenant)."
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
        if (!context.SS->CheckInFlightLimit(TTxState::TxForceDropSubDomain, errStr)) {
            result->SetError(NKikimrScheme::StatusResourceExhausted, errStr);
            return result;
        }

        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxForceDropSubDomain, path.Base()->PathId);
        txState.State = TTxState::Waiting;
        txState.MinStep = TStepId(1);

        NIceDb::TNiceDb db(context.GetDB());

        auto pathes = context.SS->ListSubThee(path.Base()->PathId, context.Ctx);

        auto relatedTx = context.SS->GetRelatedTransactions(pathes, context.Ctx);
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

            Y_VERIFY(context.SS->Operations.contains(otherTxId));
            auto otherOperation = context.SS->Operations.at(otherTxId);
            for (ui32 partId = 0; partId < otherOperation->Parts.size(); ++partId) {
                if (auto part = otherOperation->Parts.at(partId)) {
                    part->AbortUnsafe(OperationId.GetTxId(), context);
                }
            }
        }

        context.SS->MarkAsDroping(pathes, OperationId.GetTxId(), context.Ctx);

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

        State = NextState();
        SetState(SelectStateFunc(State));
        return result;
    }

    void AbortPropose(TOperationContext&) override {
        Y_FAIL("no AbortPropose for TDropForceUnsafe");
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TDropForceUnsafe AbortUnsafe"
                         << ", opId: " << OperationId
                         << ", forceDropId: " << forceDropTxId
                         << ", at schemeshard: " << context.SS->TabletID());

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_VERIFY(txState);

        TPathId pathId = txState->TargetPathId;
        Y_VERIFY(context.SS->PathsById.contains(pathId));
        TPathElement::TPtr path = context.SS->PathsById.at(pathId);
        Y_VERIFY(path);

        if (path->Dropped()) {
            for (auto shard : txState->Shards) {
                context.OnComplete.DeleteShard(shard.Idx);
            }
        }

        context.OnComplete.DoneOperation(OperationId);
    }
};

}

namespace NKikimr {
namespace NSchemeShard {

ISubOperationBase::TPtr CreateFroceDropUnsafe(TOperationId id, const TTxTransaction& tx) {
    return new TDropForceUnsafe(id, tx, TPathElement::EPathType::EPathTypeInvalid);
}

ISubOperationBase::TPtr CreateFroceDropUnsafe(TOperationId id, TTxState::ETxState state) {
    Y_VERIFY(state != TTxState::Invalid);
    return new TDropForceUnsafe(id, state);
}

ISubOperationBase::TPtr CreateFroceDropSubDomain(TOperationId id, const TTxTransaction& tx) {
    return new TDropForceUnsafe(id, tx, TPathElement::EPathType::EPathTypeSubDomain);
}

ISubOperationBase::TPtr CreateFroceDropSubDomain(TOperationId id, TTxState::ETxState state) {
    Y_VERIFY(state != TTxState::Invalid);
    return new TDropForceUnsafe(id, state);
}

}
}

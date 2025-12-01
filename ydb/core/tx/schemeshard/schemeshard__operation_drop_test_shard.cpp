#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"

#include <ydb/core/base/subdomain.h>

namespace NKikimr::NSchemeShard {

namespace {

class TDeleteParts: public TDeletePartsAndDone {
public:
    explicit TDeleteParts(const TOperationId& id)
        : TDeletePartsAndDone(id)
    {
        IgnoreMessages(DebugHint(), {
            TEvPrivate::TEvOperationPlan::EventType,
        });
    }
};

class TDropTestShard : public TSubOperation {
    static TTxState::ETxState NextState() {
        return TTxState::Propose;
    }

    TTxState::ETxState NextState(TTxState::ETxState state) const override {
        switch (state) {
        case TTxState::Waiting:
        case TTxState::Propose:
            return TTxState::ProposedDeleteParts;
        default:
            return TTxState::Invalid;
        }
    }

    class TPropose : public TSubOperationState {
    private:
        const TOperationId OperationId;

    public:
        TPropose(TOperationId id)
            : OperationId(id)
        {}

        bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
            TStepId step = TStepId(ev->Get()->StepId);
            TTxState* txState = context.SS->FindTx(OperationId);
            Y_ABORT_UNLESS(txState->TxType == TTxState::TxDropTestShard);

            TPathId pathId = txState->TargetPathId;
            TPathElement::TPtr path = context.SS->PathsById.at(pathId);

            NIceDb::TNiceDb db(context.GetDB());

            context.SS->DropNode(path, step, OperationId.GetTxId(), db, context.Ctx);

            auto parentDir = context.SS->PathsById.at(path->ParentPathId);
            ++parentDir->DirAlterVersion;
            context.SS->PersistPathDirAlterVersion(db, parentDir);
            context.SS->ClearDescribePathCaches(parentDir);
            context.SS->ClearDescribePathCaches(path);

            if (!context.SS->DisablePublicationsOfDropping) {
                context.OnComplete.PublishToSchemeBoard(OperationId, parentDir->PathId);
                context.OnComplete.PublishToSchemeBoard(OperationId, pathId);
            }

            context.SS->ChangeTxState(db, OperationId, TTxState::ProposedDeleteParts);
            return true;
        }

        bool ProgressState(TOperationContext& context) override {
            TTxState* txState = context.SS->FindTx(OperationId);
            Y_ABORT_UNLESS(txState);
            Y_ABORT_UNLESS(txState->TxType == TTxState::TxDropTestShard);

            context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
            return false;
        }

        TString DebugHint() const override {
            return TStringBuilder()
                << "TDropTestShard::TPropose"
                << " OperationId# " << OperationId;
        }
    };

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) override {
        switch (state) {
        case TTxState::Waiting:
        case TTxState::Propose:
            return MakeHolder<TPropose>(OperationId);
        case TTxState::ProposedDeleteParts:
            return MakeHolder<TDeleteParts>(OperationId);
        default:
            return nullptr;
        }
    }

public:
    using TSubOperation::TSubOperation;

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        const TTabletId ssId = context.SS->SelfTabletId();

        const auto& drop = Transaction.GetDrop();

        const TString& parentPathStr = Transaction.GetWorkingDir();
        const TString& name = drop.GetName();

        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TDropTestShard Propose"
                         << ", path: " << parentPathStr << "/" << name
                         << ", opId: " << OperationId
                         << ", at schemeshard: " << ssId);

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), ui64(ssId));

        TPath path = TPath::Resolve(parentPathStr, context.SS).Dive(name);

        {
            TPath::TChecker checks = path.Check();
            checks
                .NotEmpty()
                .NotUnderDomainUpgrade()
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .IsTestShard()
                .NotUnderDeleting()
                .NotUnderOperation();

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        TString errStr;
        if (!context.SS->CheckApplyIf(Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, errStr);
            return result;
        }

        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxDropTestShard, path.Base()->PathId);
        // Dirty hack: drop step must not be zero because 0 is treated as "hasn't been dropped"
        txState.MinStep = TStepId(1);
        txState.State = TTxState::Propose;

        auto testShard = context.SS->TestShards.at(path.Base()->PathId);

        NIceDb::TNiceDb db(context.GetDB());

        for (auto& part : testShard->TestShards) {
            auto shardIdx = part.first;

            txState.Shards.emplace_back(shardIdx, context.SS->ShardInfos.at(shardIdx).TabletType, txState.State);

            context.SS->ShardInfos[shardIdx].CurrentTxId = OperationId.GetTxId();
            context.SS->PersistShardTx(db, shardIdx, OperationId.GetTxId());
        }

        context.OnComplete.ActivateTx(OperationId);
        context.SS->PersistTxState(db, OperationId);

        path.Base()->PathState = TPathElement::EPathState::EPathStateDrop;
        path.Base()->DropTxId = OperationId.GetTxId();
        path.Base()->LastTxId = OperationId.GetTxId();

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
        Y_ABORT("no AbortPropose for TDropTestShard");
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TDropTestShard AbortUnsafe"
                         << ", opId: " << OperationId
                         << ", forceDropId: " << forceDropTxId
                         << ", at schemeshard: " << context.SS->SelfTabletId());

        context.OnComplete.DoneOperation(OperationId);
    }
};

} // namespace

ISubOperation::TPtr CreateDropTestShard(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TDropTestShard>(id, tx);
}

ISubOperation::TPtr CreateDropTestShard(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    return MakeSubOperation<TDropTestShard>(id, state);
}

} // namespace NKikimr::NSchemeShard

#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"

#include <ydb/core/base/subdomain.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::FLAT_TX_SCHEMESHARD

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

class TDropTestShardSet : public TSubOperation {
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
        explicit TPropose(TOperationId id)
            : OperationId(id)
        {
            IgnoreMessages(DebugHint(), {});
        }

        bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
            const TStepId step = TStepId(ev->Get()->StepId);
            const TTabletId ssId = context.SS->SelfTabletId();

            YDB_LOG_INFO_CTX(context.Ctx, "HandleReply TEvOperationPlan",
                {"debugHint", DebugHint()},
                {"step", step},
                {"schemeshard", ssId});

            TTxState* txState = context.SS->FindTx(OperationId);
            Y_ABORT_UNLESS(txState);
            Y_ABORT_UNLESS(txState->TxType == TTxState::TxDropTestShardSet);

            const TPathId pathId = txState->TargetPathId;
            TPathElement::TPtr path = context.SS->PathsById.at(pathId);
            Y_ABORT_UNLESS(!path->Dropped());

            NIceDb::TNiceDb db(context.GetDB());

            for (auto shard : txState->Shards) {
                context.OnComplete.DeleteShard(shard.Idx);
            }

            path->SetDropped(step, OperationId.GetTxId());
            context.SS->PersistDropStep(db, pathId, step, OperationId);
            context.SS->TabletCounters->Simple()[COUNTER_TEST_SHARD_SET_COUNT].Sub(1);

            context.SS->TabletCounters->Simple()[COUNTER_USER_ATTRIBUTES_COUNT].Sub(path->UserAttrs->Size());
            context.SS->PersistUserAttributes(db, path->PathId, path->UserAttrs, nullptr);

            auto domainInfo = context.SS->ResolveDomainInfo(pathId);
            domainInfo->DecPathsInside(context.SS);
            auto parentDir = context.SS->PathsById.at(path->ParentPathId);
            DecAliveChildrenDirect(OperationId, parentDir, context);

            if (!AppData()->DisableSchemeShardCleanupOnDropForTest) {
                context.SS->PersistRemoveTestShardSet(db, pathId);
            }

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
            const TTabletId ssId = context.SS->SelfTabletId();

            YDB_LOG_INFO_CTX(context.Ctx, "ProgressState",
                {"debugHint", DebugHint()},
                {"schemeshard", ssId});

            TTxState* txState = context.SS->FindTx(OperationId);
            Y_ABORT_UNLESS(txState);
            Y_ABORT_UNLESS(txState->TxType == TTxState::TxDropTestShardSet);

            context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
            return false;
        }

        TString DebugHint() const override {
            return TStringBuilder()
                << "TDropTestShardSet::TPropose"
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

        YDB_LOG_NOTICE_CTX(context.Ctx, "TDropTestShardSet Propose ",
            {"path", parentPathStr},
            {"name", name},
            {"opId", OperationId},
            {"schemeshard", ssId},
            {"drop", drop.ShortDebugString()});

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
                .IsTestShardSet()
                .NotUnderDeleting()
                .NotUnderOperation();

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                if (path.IsResolved()) {
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

        auto guard = context.DbGuard();
        context.MemChanges.GrabNewTxState(context.SS, OperationId);
        context.DbChanges.PersistTxState(OperationId);

        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxDropTestShardSet, path.Base()->PathId);
        // Dirty hack: drop step must not be zero because 0 is treated as "hasn't been dropped"
        txState.MinStep = TStepId(1);
        txState.State = TTxState::Propose;

        auto testShard = context.SS->TestShardSets.at(path.Base()->PathId);

        for (auto& part : testShard->TestShards) {
            auto shardIdx = part.first;

            txState.Shards.emplace_back(shardIdx, context.SS->ShardInfos.at(shardIdx).TabletType, txState.State);

            context.MemChanges.GrabShard(context.SS, shardIdx);
            context.SS->ShardInfos[shardIdx].CurrentTxId = OperationId.GetTxId();
            context.DbChanges.PersistShard(shardIdx);
        }

        context.OnComplete.ActivateTx(OperationId);

        context.MemChanges.GrabPath(context.SS, path.Base()->PathId);
        path.Base()->PathState = TPathElement::EPathState::EPathStateDrop;
        path.Base()->DropTxId = OperationId.GetTxId();
        path.Base()->LastTxId = OperationId.GetTxId();
        context.DbChanges.PersistPath(path.Base()->PathId);

        context.MemChanges.GrabPath(context.SS, path->ParentPathId);
        context.DbChanges.PersistPath(path->ParentPathId);

        IncParentDirAlterVersionWithRepublishSafeWithUndo(OperationId, path, context.SS, context.OnComplete);

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext&) override {
        Y_ABORT("no AbortPropose for TDropTestShardSet");
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        YDB_LOG_NOTICE_CTX(context.Ctx, "TDropTestShardSet AbortUnsafe",
            {"opId", OperationId},
            {"forceDropId", forceDropTxId},
            {"schemeshard", context.SS->SelfTabletId()});

        context.OnComplete.DoneOperation(OperationId);
    }
};

} // namespace

ISubOperation::TPtr CreateDropTestShardSet(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TDropTestShardSet>(id, tx);
}

ISubOperation::TPtr CreateDropTestShardSet(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    return MakeSubOperation<TDropTestShardSet>(id, state);
}

} // namespace NKikimr::NSchemeShard

#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"

#include <ydb/core/base/subdomain.h>

namespace {

constexpr char RateLimiterRateAttrName[] = "drop_blockstore_volume_rate_limiter_rate";
constexpr char RateLimiterCapacityAttrName[] = "drop_blockstore_volume_rate_limiter_capacity";

using namespace NKikimr;
using namespace NSchemeShard;

class TPropose: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
                << "TDropBlockStoreVolume TPropose"
                << ", operationId: " << OperationId;
    }

public:
    TPropose(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {});
    }

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        TStepId step = TStepId(ev->Get()->StepId);
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvOperationPlan"
                               << ", step: " << step
                               << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxDropBlockStoreVolume);

        TPathId pathId = txState->TargetPathId;
        auto path = context.SS->PathsById.at(pathId);
        auto parentDir = context.SS->PathsById.at(path->ParentPathId);

        NIceDb::TNiceDb db(context.GetDB());

        Y_ABORT_UNLESS(!path->Dropped());
        path->SetDropped(step, OperationId.GetTxId());
        context.SS->PersistDropStep(db, pathId, step, OperationId);
        auto domainInfo = context.SS->ResolveDomainInfo(pathId);
        domainInfo->DecPathsInside(context.SS);
        parentDir->DecAliveChildren();

        // KIKIMR-13173
        // Repeat it here for a while, delete it from TDeleteParts after
        // Initiate asynchronous deletion of all shards
        for (auto shard : txState->Shards) {
            context.OnComplete.DeleteShard(shard.Idx);
        }

        TBlockStoreVolumeInfo::TPtr volume = context.SS->BlockStoreVolumes.at(pathId);

        auto volumeSpace = volume->GetVolumeSpace();
        auto domainDir = context.SS->PathsById.at(context.SS->ResolvePathIdForDomain(path));
        domainDir->ChangeVolumeSpaceCommit({ }, volumeSpace);

        if (!AppData()->DisableSchemeShardCleanupOnDropForTest) {
            context.SS->PersistRemoveBlockStoreVolume(db, pathId);
        }

        context.SS->TabletCounters->Simple()[COUNTER_USER_ATTRIBUTES_COUNT].Sub(path->UserAttrs->Size());
        context.SS->PersistUserAttributes(db, path->PathId, path->UserAttrs, nullptr);

        ++parentDir->DirAlterVersion;
        context.SS->PersistPathDirAlterVersion(db, parentDir);
        context.SS->ClearDescribePathCaches(parentDir);
        context.SS->ClearDescribePathCaches(path);

        if (!context.SS->DisablePublicationsOfDropping) {
            context.OnComplete.PublishToSchemeBoard(OperationId, parentDir->PathId);
            context.OnComplete.PublishToSchemeBoard(OperationId, pathId);
        }

        context.OnComplete.DoneOperation(OperationId);

        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxDropBlockStoreVolume);

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }
};

class TDropBlockStoreVolume: public TSubOperation {
    static TTxState::ETxState NextState() {
        return TTxState::DeleteParts;
    }

    TTxState::ETxState NextState(TTxState::ETxState state) const override {
        switch (state) {
        case TTxState::Waiting:
        case TTxState::DeleteParts:
            return TTxState::Propose;
        default:
            return TTxState::Invalid;
        }
    }

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) override {
        switch (state) {
        case TTxState::Waiting:
        case TTxState::DeleteParts:
            return MakeHolder<TDeleteParts>(OperationId);
        case TTxState::Propose:
            return MakeHolder<TPropose>(OperationId);
        default:
            return nullptr;
        }
    }

public:
    using TSubOperation::TSubOperation;

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        const TTabletId ssId = context.SS->SelfTabletId();

        const NKikimrSchemeOp::TDrop& drop = Transaction.GetDrop();

        const TString& parentPathStr = Transaction.GetWorkingDir();
        const TString& name = drop.GetName();

        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TDropBlockStoreVolume Propose"
                         << ", path: " << parentPathStr << "/" << name
                         << ", pathId: " << drop.GetId()
                         << ", opId: " << OperationId
                         << ", at schemeshard: " << ssId);

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
                .NotDeleted()
                .IsBlockStoreVolume()
                .NotUnderDeleting()
                .NotUnderOperation()
                .IsCommonSensePath();

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                if (path.IsResolved() && path.Base()->IsBlockStoreVolume() && path.Base()->PlannedToDrop()) {
                    result->SetPathDropTxId(ui64(path.Base()->DropTxId));
                    result->SetPathId(path.Base()->PathId.LocalPathId);
                }
                return result;
            }
        }

        TBlockStoreVolumeInfo::TPtr volume = context.SS->BlockStoreVolumes.at(path.Base()->PathId);
        Y_ABORT_UNLESS(volume);

        {
            const NKikimrSchemeOp::TDropBlockStoreVolume& dropParams = Transaction.GetDropBlockStoreVolume();

            ui64 proposedFillGeneration = dropParams.GetFillGeneration();
            ui64 actualFillGeneration = volume->VolumeConfig.GetFillGeneration();
            const bool isFillFinished = volume->VolumeConfig.GetIsFillFinished();

            if (proposedFillGeneration > 0) {
                if (isFillFinished) {
                    result->SetError(NKikimrScheme::StatusSuccess,
                                     TStringBuilder() << "Filling is finished, deletion is no-op");
                    return result;
                } else if (proposedFillGeneration < actualFillGeneration) {
                    result->SetError(NKikimrScheme::StatusSuccess,
                                     TStringBuilder() << "Proposed fill generation "
                                        << "is less than fill generation of the volume: "
                                        << proposedFillGeneration << " < " << actualFillGeneration);
                    return result;
                }
            }
        }

        TString errStr;
        if (!context.SS->CheckApplyIf(Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, errStr);
            return result;
        }

        {
            auto& rateLimiter = context.SS->DropBlockStoreVolumeRateLimiter;

            // update rate limiter params
            auto domainDir = context.SS->PathsById.at(path.GetPathIdForDomain());
            double rate = 0;
            double capacity = 0;
            auto& attrs = domainDir->UserAttrs->Attrs;
            if (TryFromString(attrs[RateLimiterRateAttrName], rate) && 
                TryFromString(attrs[RateLimiterCapacityAttrName], capacity))
            {
                rateLimiter.SetRate(rate);
                rateLimiter.SetCapacity(capacity);
            }

            if (rate > 0.0 && capacity > 0.0) {
                rateLimiter.Fill(AppData()->TimeProvider->Now());

                if (rateLimiter.Available() >= 1.0) {
                    rateLimiter.Take(1.0);
                } else {
                    // TODO: should use separate status?
                    result->SetError(
                        NKikimrScheme::StatusNotAvailable,
                        "Too many requests");
                    return result;
                }
            }
        }

        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxDropBlockStoreVolume, path.Base()->PathId);
        // Dirty hack: drop step must not be zero because 0 is treated as "hasn't been dropped"
        txState.MinStep = TStepId(1);
        txState.State = TTxState::DeleteParts;

        NIceDb::TNiceDb db(context.GetDB());

        TVector<TShardIdx> shards;
        shards.push_back(volume->VolumeShardIdx);

        for (const auto& kv : volume->Shards) {
            shards.push_back(kv.first);
        }

        for (const auto& shardIdx : shards) {
            txState.Shards.emplace_back(shardIdx, context.SS->ShardInfos.at(shardIdx).TabletType, txState.State);

            context.SS->ShardInfos[shardIdx].CurrentTxId = OperationId.GetTxId();
            context.SS->PersistShardTx(db, shardIdx, OperationId.GetTxId());
        }

        // Trying to abort Alter/Create. Wait if can't.
        context.OnComplete.ActivateTx(OperationId);
        context.SS->PersistTxState(db, OperationId);

        path.Base()->PathState = TPathElement::EPathState::EPathStateDrop;
        path.Base()->DropTxId = OperationId.GetTxId();
        path.Base()->LastTxId = OperationId.GetTxId();

        context.SS->TabletCounters->Simple()[COUNTER_BLOCKSTORE_VOLUME_COUNT].Sub(1);

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
        Y_ABORT("no AbortPropose for TDropBlockStoreVolume");
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        AbortUnsafeDropOperation(OperationId, forceDropTxId, context);
    }
};

}

namespace NKikimr::NSchemeShard {

ISubOperation::TPtr CreateDropBSV(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TDropBlockStoreVolume>(id, tx);
}

ISubOperation::TPtr CreateDropBSV(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    return MakeSubOperation<TDropBlockStoreVolume>(id, state);
}

}

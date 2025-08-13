#include "schemeshard__operation_common.h"
#include "schemeshard__operation_part.h"
#include "schemeshard__operation.h"
#include "schemeshard_impl.h"
#include "schemeshard_utils.h"  // for PQGroupReserve

#include <ydb/core/base/subdomain.h>
#include <ydb/core/persqueue/events/global.h>

namespace {

using namespace NKikimr;
using namespace NSchemeShard;

class TDropParts: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
                << "TDropPQ TDropParts"
                << " operationId# " << OperationId;
    }

public:
    TDropParts(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {});
    }

    bool HandleReply(TEvPersQueue::TEvDropTabletReply::TPtr& ev, TOperationContext& context) override {
        auto ssId = context.SS->SelfTabletId();

        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                 "TDropParts HandleReply TEvPersQueue::TEvDropTabletReply"
                 << " operationId# " << OperationId
                 << " at tabletId# " << ssId
                 << " message# " << ev->Get()->Record.ShortDebugString());

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxDropPQGroup);

        auto& record = ev->Get()->Record;

        Y_ABORT_UNLESS(record.GetStatus() == NKikimrProto::OK);
        Y_ABORT_UNLESS(record.GetActualState() == NKikimrPQ::EDropped);

        auto tabletId = TTabletId(record.GetTabletId());
        auto idx = context.SS->MustGetShardIdx(tabletId);

        if (!txState->ShardsInProgress.contains(idx)) {
            LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                       "Ignored repeated drop tablet reply for txId %" << OperationId.GetTxId() << " tablet " << tabletId);
            return false;
        }

        Y_ABORT_UNLESS(txState->State == TTxState::DropParts);
        txState->ShardsInProgress.erase(idx);

        context.OnComplete.UnbindMsgFromPipe(OperationId, tabletId, idx);

        if (txState->ShardsInProgress.empty()) {
            NIceDb::TNiceDb db(context.GetDB());
            context.SS->ChangeTxState(db, OperationId, TTxState::DeleteParts);
            context.OnComplete.ActivateTx(OperationId);
            return true;
        }

        return false;
    }


    bool ProgressState(TOperationContext& context) override {
        auto ssId = context.SS->SelfTabletId();
        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "TDropParts ProgressState"
                    << " operationId# " << OperationId
                    << " at tablet " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxDropPQGroup);

        txState->ClearShardsInProgress();

        TString topicName = context.SS->PathsById.at(txState->TargetPathId)->Name;
        Y_VERIFY_S(topicName.size(), "topicName is empty. PathId: " << txState->TargetPathId);

        TTopicInfo::TPtr pqGroup = context.SS->Topics.at(txState->TargetPathId);
        Y_VERIFY_S(pqGroup, "pqGroup is null. PathId: " << txState->TargetPathId);

        bool haveWork = false;
        for (auto shard : txState->Shards) {
            if (shard.Operation != TTxState::DropParts) {
                continue;
            }
            haveWork = true;

            auto idx = shard.Idx;
            auto tabletId = context.SS->ShardInfos[idx].TabletID;

            TAutoPtr<TEvPersQueue::TEvDropTablet> event(new TEvPersQueue::TEvDropTablet());
            event->Record.SetTxId(ui64(OperationId.GetTxId()));
            event->Record.SetRequestedState(NKikimrPQ::EDropped);

            context.OnComplete.BindMsgToPipe(OperationId, tabletId, idx, event.Release());
        }

        txState->UpdateShardsInProgress(TTxState::DropParts);

        if (!haveWork) {
            NIceDb::TNiceDb db(context.GetDB());
            context.SS->ChangeTxState(db, OperationId, TTxState::DeleteParts);
            context.OnComplete.ActivateTx(OperationId);
            return true;
        }

        return false;
    }
};

class TDeleteParts: public ::NKikimr::NSchemeShard::TDeleteParts {
public:
    explicit TDeleteParts(const TOperationId& id)
        : ::NKikimr::NSchemeShard::TDeleteParts(id)
    {
        IgnoreMessages(DebugHint(), {
            TEvPersQueue::TEvDropTabletReply::EventType,
        });
    }
};

class TPropose: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
                << "TDropPQ TPropose"
                << ", operationId: " << OperationId;
    }

public:
    TPropose(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {TEvPersQueue::TEvDropTabletReply::EventType});
    }

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        auto step = TStepId(ev->Get()->StepId);
        auto ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvOperationPlan"
                               << ", step: " << step
                               << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxDropPQGroup);

        TPathId pathId = txState->TargetPathId;
        auto path = context.SS->PathsById.at(pathId);
        auto parentDir = context.SS->PathsById.at(path->ParentPathId);

        NIceDb::TNiceDb db(context.GetDB());

        Y_ABORT_UNLESS(!path->Dropped());
        path->SetDropped(step, OperationId.GetTxId());
        context.SS->PersistDropStep(db, pathId, step, OperationId);
        TTopicInfo::TPtr pqGroup = context.SS->Topics.at(pathId);
        Y_ABORT_UNLESS(pqGroup);

        // KIKIMR-13173
        // Repeat it here for a while, delete it from TDeleteParts after
        // Initiate asynchronous deletion of all shards
        for (auto shard : txState->Shards) {
            context.OnComplete.DeleteShard(shard.Idx);
        }

        auto tabletConfig = pqGroup->TabletConfig;
        NKikimrPQ::TPQTabletConfig config;
        Y_ABORT_UNLESS(!tabletConfig.empty());
        bool parseOk = ParseFromStringNoSizeLimit(config, tabletConfig);
        Y_ABORT_UNLESS(parseOk);

        const PQGroupReserve reserve(config, pqGroup->ActivePartitionCount);

        auto domainInfo = context.SS->ResolveDomainInfo(pathId);
        domainInfo->DecPathsInside(context.SS);
        domainInfo->DecPQPartitionsInside(pqGroup->TotalPartitionCount);
        domainInfo->DecPQReservedStorage(reserve.Storage);
        domainInfo->AggrDiskSpaceUsage({}, pqGroup->Stats);
        if (domainInfo->CheckDiskSpaceQuotas(context.SS)) {
            auto subDomainId = context.SS->ResolvePathIdForDomain(pathId);
            context.SS->PersistSubDomainState(db, subDomainId, *domainInfo);
            context.OnComplete.PublishToSchemeBoard(OperationId, subDomainId);
        }

        context.SS->ChangeDiskSpaceTopicsTotalBytes(domainInfo->GetPQAccountStorage());
        context.SS->TabletCounters->Simple()[COUNTER_STREAM_RESERVED_THROUGHPUT].Sub(reserve.Throughput);
        context.SS->TabletCounters->Simple()[COUNTER_STREAM_RESERVED_STORAGE].Sub(reserve.Storage);

        context.SS->TabletCounters->Simple()[COUNTER_STREAM_SHARDS_COUNT].Sub(pqGroup->TotalPartitionCount);

        DecAliveChildrenDirect(OperationId, parentDir, context); // for correct discard of ChildrenExist prop

        if (!AppData()->DisableSchemeShardCleanupOnDropForTest) {
            context.SS->PersistRemovePersQueueGroup(db, pathId);
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

        context.SS->ChangeTxState(db, OperationId, TTxState::Done);
        context.OnComplete.ActivateTx(OperationId);

        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        auto ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxDropPQGroup);

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }
};

class TDropPQ: public TSubOperation {
    static TTxState::ETxState NextState() {
        return TTxState::DropParts;
    }

    TTxState::ETxState NextState(TTxState::ETxState state) const override {
        switch (state) {
        case TTxState::Waiting:
        case TTxState::DropParts:
            return TTxState::DeleteParts;
        case TTxState::DeleteParts:
            return TTxState::Propose;
        case TTxState::Propose:
            return TTxState::Done;
        default:
            return TTxState::Invalid;
        }
    }

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) override {
        switch (state) {
        case TTxState::Waiting:
        case TTxState::DropParts:
            return MakeHolder<TDropParts>(OperationId);
        case TTxState::DeleteParts:
            return MakeHolder<TDeleteParts>(OperationId);
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

    void SetPQBalancer(TTopicInfo::TPtr pqGroup, TTxState& txState, TOperationContext& context) {
        auto shardId = pqGroup->BalancerShardIdx;
        auto tabletId = pqGroup->BalancerTabletID;

        if (pqGroup->AlterData) {
            shardId = pqGroup->AlterData->BalancerShardIdx;
            tabletId = pqGroup->AlterData->BalancerTabletID;
        }

        if (shardId && tabletId != InvalidTabletId) {
            Y_VERIFY_S(context.SS->ShardInfos[shardId].TabletID == tabletId,
                     "shardId: " << shardId << " tabletId: " << tabletId << " has alter pointer: " << ui64(!!pqGroup->AlterData));
            txState.Shards.emplace_back(shardId, ETabletType::PersQueueReadBalancer, TTxState::DeleteParts);
        }
    }

    void SetPQShards(TTopicInfo::TPtr pqGroup, TTxState& txState, TOperationContext& context) {
        for (auto shard : pqGroup->Shards) {
            auto shardIdx = shard.first;
            TTopicTabletInfo::TPtr info = shard.second;

            auto tabletId = context.SS->ShardInfos[shardIdx].TabletID;

            TTxState::ETxState operation = TTxState::DeleteParts;
            if (tabletId != InvalidTabletId) {
                operation = TTxState::DropParts;
            }

            txState.Shards.emplace_back(shardIdx, ETabletType::PersQueue, operation);
        }
    }

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        const TTabletId ssId = context.SS->SelfTabletId();

        const auto& drop = Transaction.GetDrop();

        const TString& parentPathStr = Transaction.GetWorkingDir();
        const TString& name = drop.GetName();

        NKikimrSchemeOp::EDropWaitPolicy dropPolicy = drop.HasWaitPolicy() ? drop.GetWaitPolicy() : NKikimrSchemeOp::EDropFailOnChanges;

        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TDropPQ Propose"
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
                .NotUnderDeleting()
                .NotUnderOperation()
                .IsPQGroup();

            if (checks) {
                if (dropPolicy == NKikimrSchemeOp::EDropFailOnChanges) {
                    checks.NotUnderOperation();
                }
            }

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                if (path.IsResolved() && path.Base()->IsPQGroup() && path.Base()->PlannedToDrop()) {
                    result->SetPathDropTxId(ui64(path.Base()->DropTxId));
                    result->SetPathId(path.Base()->PathId.LocalPathId);
                }
                return result;
            }
        }

        TPath parent = path.Parent();
        {
            TPath::TChecker checks = parent.Check();
            checks
                .NotEmpty()
                .IsResolved()
                .NotDeleted();

            if (checks) {
                if (parent.Base()->IsCdcStream()) {
                    checks
                        .IsCdcStream()
                        .IsInsideCdcStreamPath()
                        .IsUnderDeleting(TEvSchemeShard::EStatus::StatusNameConflict)
                        .IsUnderTheSameOperation(OperationId.GetTxId());
                } else {
                    checks
                        .IsLikeDirectory()
                        .IsCommonSensePath()
                        .NotUnderDeleting();
                }
            }

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        if (dropPolicy != NKikimrSchemeOp::EDropFailOnChanges) {
            result->SetError(NKikimrScheme::StatusInvalidParameter, "drop policy isn't supported");
            return result;
        }

        TString errStr;
        if (!context.SS->CheckApplyIf(Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, errStr);
            return result;
        }

        TTopicInfo::TPtr pqGroup = context.SS->Topics.at(path.Base()->PathId);
        Y_ABORT_UNLESS(pqGroup);

        if (pqGroup->AlterData) {
            result->SetError(NKikimrScheme::StatusMultipleModifications, "Drop over Create/Alter");
            return result;
        }

        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxDropPQGroup, path.Base()->PathId);
        // Dirty hack: drop step must not be zero because 0 is treated as "hasn't been dropped"
        txState.MinStep = TStepId(1);

        NIceDb::TNiceDb db(context.GetDB());
        SetPQShards(pqGroup, txState, context);
        SetPQBalancer(pqGroup, txState, context);

        txState.State = TTxState::DropParts;
        context.OnComplete.ActivateTx(OperationId);

        path.Base()->PathState = TPathElement::EPathState::EPathStateDrop;
        path.Base()->DropTxId = OperationId.GetTxId();
        path.Base()->LastTxId = OperationId.GetTxId();

        context.SS->PersistTxState(db, OperationId);

        context.SS->TabletCounters->Simple()[COUNTER_PQ_GROUP_COUNT].Sub(1);

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
        Y_ABORT("no AbortPropose for TDropPQ");
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        AbortUnsafeDropOperation(OperationId, forceDropTxId, context);
    }
};

}

namespace NKikimr::NSchemeShard {

ISubOperation::TPtr CreateDropPQ(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TDropPQ>(id, tx);
}

ISubOperation::TPtr CreateDropPQ(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    return MakeSubOperation<TDropPQ>(id, state);
}

bool CreateDropPQ(TOperationId id, const TTxTransaction& tx, TOperationContext& context, TVector<ISubOperation::TPtr>& result) {
    Y_UNUSED(context);
    Y_ABORT_UNLESS(tx.GetOperationType() == NKikimrSchemeOp::EOperationType::ESchemeOpDropPersQueueGroup);

    result.push_back(CreateDropPQ(NextPartId(id, result), tx));
    return true;
}

}

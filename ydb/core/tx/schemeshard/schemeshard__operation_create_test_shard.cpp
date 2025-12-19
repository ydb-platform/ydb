#include "schemeshard__op_traits.h"
#include "schemeshard__operation_common.h"
#include "schemeshard__operation_part.h"
#include "schemeshard_impl.h"

#include <ydb/core/base/subdomain.h>
#include <ydb/core/mind/hive/hive.h>
#include <ydb/core/protos/msgbus.pb.h>
#include <ydb/core/protos/test_shard_control.pb.h>
#include <ydb/core/test_tablet/test_shard_impl.h>
#include <util/string/join.h>

namespace {

using namespace NKikimr;
using namespace NSchemeShard;

bool ValidateConfig(const NKikimrSchemeOp::TCreateTestShard& op, TString& errStr)
{
    if (op.GetCount() == 0) {
        errStr = "count must be greater than zero";
        return false;
    }

    if (!op.HasStorageConfig()) {
        errStr = "storage config must be specified";
        return false;
    }

    if (!op.HasCmdInitialize()) {
        errStr = "CmdInitialize must be specified";
        return false;
    }

    return true;
}

TTestShardInfo::TPtr CreateTestShard(const NKikimrSchemeOp::TCreateTestShard& op, TTxState& state, TSchemeShard* ss)
{
    TTestShardInfo::TPtr testShard = new TTestShardInfo(1);

    state.Shards.clear();
    testShard->TestShards.clear();

    ui64 count = op.GetCount();

    state.Shards.reserve(count);
    auto startShardIdx = ss->ReserveShardIdxs(count);
    for (ui64 i = 0; i < count; ++i) {
        const auto idx = ss->NextShardIdx(startShardIdx, i);
        testShard->TestShards[idx] = InvalidTabletId;
        state.Shards.emplace_back(idx, TTabletTypes::TestShard, TTxState::CreateParts);
    }

    return testShard;
}

class TConfigureParts: public TSubOperationState {
private:
    TOperationId OperationId;
    const NKikimrSchemeOp::TCreateTestShard& Op;

    TString DebugHint() const override {
        return TStringBuilder()
                << "TCreateTestShard TConfigureParts"
                << ", operationId: " << OperationId;
    }

public:
    TConfigureParts(TOperationId id, const NKikimrSchemeOp::TCreateTestShard& op)
        : OperationId(id)
        , Op(op)
    {
        IgnoreMessages(DebugHint(),
            {TEvHive::TEvCreateTabletReply::EventType});
    }

    bool HandleReply(NKikimr::NTestShard::TEvControlResponse::TPtr& ev, TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();
        TTabletId tabletId = TTabletId(ev->Get()->Record.GetTabletId());

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvControlResponse"
                               << ", at schemeshard: " << ssId
                               << ", from tablet: " << tabletId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreateTestShard);

        auto idx = context.SS->MustGetShardIdx(tabletId);
        txState->ShardsInProgress.erase(idx);

        context.OnComplete.UnbindMsgFromPipe(OperationId, tabletId, idx);

        if (txState->ShardsInProgress.empty()) {
            NIceDb::TNiceDb db(context.GetDB());
            context.SS->ChangeTxState(db, OperationId, TTxState::Propose);
            context.OnComplete.ActivateTx(OperationId);
            return true;
        }

        return false;
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreateTestShard);

        auto testShardInfo = context.SS->TestShards[txState->TargetPathId];
        Y_VERIFY_S(testShardInfo, "test shard info is null. PathId: " << txState->TargetPathId);

        txState->ClearShardsInProgress();

        for (const auto& shard: txState->Shards) {
            auto shardIdx = shard.Idx;
            auto tabletId = context.SS->ShardInfos[shardIdx].TabletID;
            Y_ABORT_UNLESS(shard.TabletType == ETabletType::TestShard);

            testShardInfo->TestShards[shardIdx] = tabletId;

            auto event = MakeHolder<NKikimr::NTestShard::TEvControlRequest>();
            event->Record.SetTabletId(ui64(tabletId));
            *event->Record.MutableInitialize() = Op.GetCmdInitialize();

            context.OnComplete.BindMsgToPipe(OperationId, tabletId, shardIdx, event.Release());
            txState->ShardsInProgress.insert(shardIdx);
        }

        return false;
    }
};

class TPropose: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
                << "TCreateTestShard TPropose"
                << ", operationId: " << OperationId;
    }
public:
    TPropose(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(),
            {TEvHive::TEvCreateTabletReply::EventType});
    }

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        TStepId step = TStepId(ev->Get()->StepId);
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvOperationPlan"
                               << ", step: " << step
                               << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        if(!txState) {
            return false;
        }

        TPathId pathId = txState->TargetPathId;
        TPathElement::TPtr path = context.SS->PathsById.at(pathId);

        NIceDb::TNiceDb db(context.GetDB());

        path->StepCreated = step;
        context.SS->PersistCreateStep(db, pathId, step);

        IncParentDirAlterVersionWithRepublish(OperationId, TPath::Init(pathId, context.SS), context);

        context.SS->ChangeTxState(db, OperationId, TTxState::Done);
        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " ProgressState"
                               << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreateTestShard);

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }
};

class TCreateTestShard: public TSubOperation {
    static TTxState::ETxState NextState() {
        return TTxState::CreateParts;
    }

    TTxState::ETxState NextState(TTxState::ETxState state) const override {
        switch (state) {
        case TTxState::Waiting:
        case TTxState::CreateParts:
            return TTxState::ConfigureParts;
        case TTxState::ConfigureParts:
            return TTxState::Propose;
        case TTxState::Propose:
            return TTxState::Done;
        default:
            return TTxState::Invalid;
        }
        return TTxState::Invalid;
    }

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) override {
        switch (state) {
        case TTxState::Waiting:
        case TTxState::CreateParts:
            return MakeHolder<TCreateParts>(OperationId);
        case TTxState::ConfigureParts:
            return MakeHolder<TConfigureParts>(OperationId, Transaction.GetCreateTestShard());
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
        const TTabletId ssId = context.SS->SelfTabletId();

        const auto acceptExisted = !Transaction.GetFailOnExist();
        const auto& op = Transaction.GetCreateTestShard();

        const TString& parentPathStr = Transaction.GetWorkingDir();
        const TString& name = op.GetName();

        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TCreateTestShard Propose"
                         << ", path: "<< parentPathStr << "/" << name
                         << ", opId: " << OperationId
                         << ", at schemeshard: " << ssId
                         << ", count: " << op.GetCount());

        THolder<TProposeResponse> result;
        result.Reset(new TEvSchemeShard::TEvModifySchemeTransactionResult(
            NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), ui64(ssId)));

        if (name.empty()) {
            result->SetError(NKikimrScheme::StatusInvalidParameter, "name must not be empty");
            return result;
        }

        TEvSchemeShard::EStatus status = NKikimrScheme::StatusAccepted;
        TString errStr;

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
                .FailOnRestrictedCreateInTempZone();

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
                    .FailOnExist(TPathElement::EPathType::EPathTypeTestShard, acceptExisted);
            } else {
                checks
                    .NotEmpty()
                    .NotResolved();
            }

            if (checks) {
                checks
                    .IsValidLeafName(context.UserToken.Get())
                    .DepthLimit()
                    .PathsLimit()
                    .DirChildrenLimit()
                    .IsValidACL(acl);
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

        if (!context.SS->CheckApplyIf(Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, errStr);
            return result;
        }

        if (!ValidateConfig(op, errStr)) {
            result->SetError(TEvSchemeShard::EStatus::StatusInvalidParameter, errStr);
            return result;
        }

        TChannelsBindings channelsBinding;
        if (op.GetStorageConfig().ChannelSize() == 0) {
            auto domainInfo = context.SS->ResolveDomainInfo(dstPath.GetPathIdForDomain());
            const auto& pools = domainInfo->GetStoragePools();
            if (pools.empty()) {
                result->SetError(NKikimrScheme::StatusInvalidParameter, "No storage pools available in domain");
                return result;
            }

            NKikimrSchemeOp::TKeyValueStorageConfig defaultConfig;
            for (int i = 0; i < 3; ++i) {
                auto* channel = defaultConfig.AddChannel();
                if (static_cast<size_t>(i) < pools.size()) {
                    channel->SetPreferredPoolKind(pools[i].GetKind());
                } else {
                    channel->SetPreferredPoolKind(pools[0].GetKind());
                }
            }

            bool isResolved = context.SS->ResolveSolomonChannels(defaultConfig, dstPath.GetPathIdForDomain(), channelsBinding);
            if (!isResolved) {
                result->SetError(NKikimrScheme::StatusInvalidParameter, "Unable to construct default channel binding");
                return result;
            }
        } else {
            bool isResolved = context.SS->ResolveSolomonChannels(op.GetStorageConfig(), dstPath.GetPathIdForDomain(), channelsBinding);
            if (!isResolved) {
                result->SetError(NKikimrScheme::StatusInvalidParameter, "Unable to construct channel binding with the storage pools");
                return result;
            }
        }

        const auto pathId = context.SS->AllocatePathId();
        context.MemChanges.GrabNewPath(context.SS, pathId);
        context.MemChanges.GrabPath(context.SS, parentPath->PathId);
        context.MemChanges.GrabNewTestShard(context.SS, pathId);
        context.MemChanges.GrabNewTxState(context.SS, OperationId);

        context.DbChanges.PersistPath(pathId);
        context.DbChanges.PersistPath(parentPath->PathId);
        context.DbChanges.PersistTestShard(pathId);
        context.DbChanges.PersistTxState(OperationId);

        dstPath.MaterializeLeaf(owner, pathId);
        result->SetPathId(pathId.LocalPathId);

        TPathElement::TPtr newPath = dstPath.Base();
        newPath->CreateTxId = OperationId.GetTxId();
        newPath->LastTxId = OperationId.GetTxId();
        newPath->PathState = TPathElement::EPathState::EPathStateCreate;
        newPath->PathType = TPathElement::EPathType::EPathTypeTestShard;

        if (!acl.empty()) {
            newPath->ApplyACL(acl);
        }

        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxCreateTestShard, newPath->PathId);

        auto testShardInfo = CreateTestShard(op, txState, context.SS);
        if (!testShardInfo.Get()) {
            result->SetError(status, errStr);
            return result;
        }

        context.SS->TestShards[newPath->PathId] = testShardInfo;
        context.SS->TabletCounters->Simple()[COUNTER_TEST_SHARD_COUNT].Add(op.GetCount());
        context.SS->IncrementPathDbRefCount(newPath->PathId);

        TShardInfo shardInfo = TShardInfo::TestShardInfo(OperationId.GetTxId(), newPath->PathId);
        shardInfo.BindedChannels = channelsBinding;

        for (const auto& part: testShardInfo->TestShards) {
            TShardIdx shardIdx = part.first;
            context.SS->RegisterShardInfo(shardIdx, shardInfo);

            context.MemChanges.GrabNewShard(context.SS, shardIdx);
            context.DbChanges.PersistShard(shardIdx);
        }

        if (parentPath.Base()->HasActiveChanges()) {
            TTxId parentTxId = parentPath.Base()->PlannedToCreate() ? parentPath.Base()->CreateTxId : parentPath.Base()->LastTxId;
            context.OnComplete.Dependence(parentTxId, OperationId.GetTxId());
        }

        txState.State = TTxState::CreateParts;
        context.OnComplete.ActivateTx(OperationId);

        IncParentDirAlterVersionWithRepublish(OperationId, dstPath, context);

        dstPath.DomainInfo()->IncPathsInside(context.SS);
        dstPath.DomainInfo()->AddInternalShards(txState, context.SS);

        dstPath.Base()->IncShardsInside(op.GetCount());
        IncAliveChildrenSafeWithUndo(OperationId, parentPath, context);

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext&) override {
        Y_ABORT("no AbortPropose for TCreateTestShard");
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TCreateTestShard AbortUnsafe"
                         << ", opId: " << OperationId
                         << ", forceDropId: " << forceDropTxId
                         << ", at schemeshard: " << context.SS->TabletID());

        context.OnComplete.DoneOperation(OperationId);
    }
};

}

namespace NKikimr::NSchemeShard {

using TTag = TSchemeTxTraits<NKikimrSchemeOp::EOperationType::ESchemeOpCreateTestShard>;

namespace NOperation {

template <>
std::optional<TString> GetTargetName<TTag>(
    TTag,
    const TTxTransaction& tx)
{
    return tx.GetCreateTestShard().GetName();
}

template <>
bool SetName<TTag>(
    TTag,
    TTxTransaction& tx,
    const TString& name)
{
    tx.MutableCreateTestShard()->SetName(name);
    return true;
}

} // namespace NOperation

ISubOperation::TPtr CreateNewTestShard(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TCreateTestShard>(id, tx);
}

ISubOperation::TPtr CreateNewTestShard(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    return MakeSubOperation<TCreateTestShard>(id, state);
}

}
#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"

#include <ydb/core/base/subdomain.h>

namespace {

using namespace NKikimr;
using namespace NSchemeShard;

bool ValidateConfig(const NKikimrSchemeOp::TRtmrVolumeDescription& op, TString& errStr, TEvSchemeShard::EStatus& status) {
    ui64 count = op.PartitionsSize();
    for (ui64 i = 0; i < count; ++i) {
        const auto& part = op.GetPartitions(i);

        if (part.HasTabletId() && TTabletId(part.GetTabletId())) {
            errStr = "Explicit tablet id provided for partition " + ::ToString(i);

            status = TEvSchemeShard::EStatus::StatusInvalidParameter;
            return false;
        }

        if (part.GetPartitionId().size() != sizeof(TGUID)) {
            errStr = "Invalid guid size for partition " + ::ToString(i);
            status = TEvSchemeShard::EStatus::StatusInvalidParameter;
            return false;
        }
    }

    return true;
}

TRtmrVolumeInfo::TPtr CreateRtmrVolume(const NKikimrSchemeOp::TRtmrVolumeDescription& op, TTxState& state, TSchemeShard* ss) {
    TRtmrVolumeInfo::TPtr rtmrVol = new TRtmrVolumeInfo;

    state.Shards.clear();
    rtmrVol->Partitions.clear();

    ui64 count = op.PartitionsSize();
    state.Shards.reserve(count);
    const auto startShardIdx = ss->ReserveShardIdxs(count);
    for (ui64 i = 0; i < count; ++i) {
        const auto idx = ss->NextShardIdx(startShardIdx, i);
        const auto& part = op.GetPartitions(i);
        const auto id = part.GetPartitionId();

        TGUID guid;
        Copy(id.cbegin(), id.cend(), (char*)guid.dw);
        rtmrVol->Partitions[idx] = new TRtmrPartitionInfo(guid, part.GetBusKey(), idx);

        state.Shards.emplace_back(idx, TTabletTypes::RTMRPartition, TTxState::CreateParts);
    }

    return rtmrVol;
}

class TConfigureParts: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
                << "TCreateRTMR TConfigureParts"
                << " operationId#" << OperationId;
    }
public:
    TConfigureParts(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {TEvHive::TEvCreateTabletReply::EventType});
    }

    bool ProgressState(TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TCreateRTMR TConfigureParts ProgressState operationId#" << OperationId
                     << " at tablet" << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreateRtmrVolume);

        auto rtmrVol = context.SS->RtmrVolumes[txState->TargetPathId];
        Y_VERIFY_S(rtmrVol, "rtmr volume is null. PathId: " << txState->TargetPathId);
        Y_ABORT_UNLESS(rtmrVol->Partitions.size() == txState->Shards.size(),
                 "%" PRIu64 "rtmr shards expected, %" PRIu64 " created",
                 rtmrVol->Partitions.size(), txState->Shards.size());

        for (const auto& shard: txState->Shards) {
            auto rtmrPartition = rtmrVol->Partitions[shard.Idx];
            Y_VERIFY_S(rtmrPartition, "rtmr partitions is null shard idx " <<  shard.Idx << " Path " << txState->TargetPathId);

            auto tabletId = context.SS->ShardInfos[shard.Idx].TabletID;
            rtmrPartition->TabletId = tabletId;
        }

        NIceDb::TNiceDb db(context.GetDB());
        context.SS->ChangeTxState(db, OperationId, TTxState::Propose);
        return true;
    }
};

class TPropose: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
            << "TCreateRTMR TPropose"
            << ", operationId: " << OperationId;
    }

public:
    TPropose(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {TEvHive::TEvCreateTabletReply::EventType});
    }

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        auto step = TStepId(ev->Get()->StepId);
        auto ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvOperationPlan"
                               << ", at schemeshard: " << ssId);

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreateRtmrVolume);

        auto pathId = txState->TargetPathId;
        TPathElement::TPtr path = context.SS->PathsById.at(pathId);

        NIceDb::TNiceDb db(context.GetDB());

        path->StepCreated = step;
        context.SS->PersistCreateStep(db, pathId, step);

        auto parentDir = context.SS->PathsById.at(path->ParentPathId);
        ++parentDir->DirAlterVersion;
        context.SS->PersistPathDirAlterVersion(db, parentDir);
        context.SS->ClearDescribePathCaches(parentDir);
        context.OnComplete.PublishToSchemeBoard(OperationId, parentDir->PathId);

        context.SS->ClearDescribePathCaches(path);
        context.OnComplete.PublishToSchemeBoard(OperationId, pathId);

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
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreateRtmrVolume);

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }
};

class TCreateRTMR: public TSubOperation {
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
    }

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) override {
        switch (state) {
        case TTxState::Waiting:
        case TTxState::CreateParts:
            return MakeHolder<TCreateParts>(OperationId);
        case TTxState::ConfigureParts:
            return MakeHolder<TConfigureParts>(OperationId);
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
        const auto& rtmrVolumeDescription = Transaction.GetCreateRtmrVolume();

        const TString& parentPathStr = Transaction.GetWorkingDir();
        const TString& name = rtmrVolumeDescription.GetName();

        const ui64 shardsToCreate = rtmrVolumeDescription.GetPartitionsCount();

        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TCreateRTMR Propose"
                         << ", path: " << parentPathStr << "/" << name
                         << ", opId: " << OperationId
                         << ", at schemeshard: " << ssId);

        TEvSchemeShard::EStatus status = NKikimrScheme::StatusAccepted;
        auto result = MakeHolder<TProposeResponse>(status, ui64(OperationId.GetTxId()), ui64(ssId));

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
                    .FailOnExist(TPathElement::EPathType::EPathTypeRtmrVolume, acceptExisted);
            } else {
                checks
                    .NotEmpty()
                    .NotResolved();
            }

            if (checks) {
                checks
                    .IsValidLeafName()
                    .DepthLimit()
                    .PathsLimit()
                    .DirChildrenLimit()
                    .ShardsLimit(shardsToCreate)
                    .PathShardsLimit(shardsToCreate)
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

        TString errStr;
        if (!context.SS->CheckApplyIf(Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, errStr);
            return result;
        }

        if (!ValidateConfig(rtmrVolumeDescription, errStr, status)) {
            result->SetError(status, errStr);
            return result;
        }

        TChannelsBindings channelsBinding;
        if (!context.SS->ResolveRtmrChannels(dstPath.GetPathIdForDomain(), channelsBinding)) {
            result->SetError(NKikimrScheme::StatusInvalidParameter, "Unable to construct channel binding with the storage pool");
            return result;
        }

        dstPath.MaterializeLeaf(owner);
        result->SetPathId(dstPath.Base()->PathId.LocalPathId);

        TPathElement::TPtr newRtmrVolume = dstPath.Base();
        newRtmrVolume->CreateTxId = OperationId.GetTxId();
        newRtmrVolume->LastTxId = OperationId.GetTxId();
        newRtmrVolume->PathState = TPathElement::EPathState::EPathStateCreate;
        newRtmrVolume->PathType = TPathElement::EPathType::EPathTypeRtmrVolume;

        // accepted

        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxCreateRtmrVolume, newRtmrVolume->PathId);

        TRtmrVolumeInfo::TPtr rtmrVolumeInfo = CreateRtmrVolume(rtmrVolumeDescription, txState, context.SS);
        Y_ABORT_UNLESS(rtmrVolumeInfo);

        NIceDb::TNiceDb db(context.GetDB());

        if (parentPath.Base()->HasActiveChanges()) {
            TTxId parentTxId = parentPath.Base()->PlannedToCreate() ? parentPath.Base()->CreateTxId : parentPath.Base()->LastTxId;
            context.OnComplete.Dependence(parentTxId, OperationId.GetTxId());
        }

        context.SS->ChangeTxState(db, OperationId, TTxState::CreateParts);
        context.OnComplete.ActivateTx(OperationId);

        context.SS->RtmrVolumes[newRtmrVolume->PathId] = rtmrVolumeInfo;
        context.SS->TabletCounters->Simple()[COUNTER_RTMR_VOLUME_COUNT].Add(1);
        context.SS->TabletCounters->Simple()[COUNTER_RTMR_PARTITIONS_COUNT].Add(rtmrVolumeInfo->Partitions.size());
        context.SS->IncrementPathDbRefCount(newRtmrVolume->PathId);

        if (!acl.empty()) {
            newRtmrVolume->ApplyACL(acl);
        }
        context.SS->PersistPath(db, newRtmrVolume->PathId);

        context.SS->PersistRtmrVolume(db, newRtmrVolume->PathId, rtmrVolumeInfo);
        context.SS->PersistTxState(db, OperationId);

        context.SS->PersistUpdateNextPathId(db);
        context.SS->PersistUpdateNextShardIdx(db);

        TShardInfo rtmrPartitionInfo = TShardInfo::RtmrPartitionInfo(OperationId.GetTxId(), newRtmrVolume->PathId);
        rtmrPartitionInfo.BindedChannels = channelsBinding;

        for (const auto& part: rtmrVolumeInfo->Partitions) {
            auto shardIdx = part.second->ShardIdx;
            auto tabletId = part.second->TabletId;

            context.SS->RegisterShardInfo(shardIdx, rtmrPartitionInfo);

            context.SS->PersistShardMapping(db, shardIdx, tabletId, newRtmrVolume->PathId, OperationId.GetTxId(), TTabletTypes::RTMRPartition);
            context.SS->PersistChannelsBinding(db, shardIdx, channelsBinding);
        }

        ++parentPath.Base()->DirAlterVersion;
        context.SS->PersistPathDirAlterVersion(db, parentPath.Base());
        context.SS->ClearDescribePathCaches(parentPath.Base());
        context.OnComplete.PublishToSchemeBoard(OperationId, parentPath.Base()->PathId);

        context.SS->ClearDescribePathCaches(newRtmrVolume);
        context.OnComplete.PublishToSchemeBoard(OperationId, newRtmrVolume->PathId);

        Y_ABORT_UNLESS(shardsToCreate == txState.Shards.size(), "shardsToCreate=%ld != txStateShards=%ld",
            shardsToCreate, txState.Shards.size());

        dstPath.DomainInfo()->IncPathsInside();
        dstPath.DomainInfo()->AddInternalShards(txState);

        dstPath.Base()->IncShardsInside(shardsToCreate);
        parentPath.Base()->IncAliveChildren();

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext&) override {
        Y_ABORT("no AbortPropose for TCreateRTMR");
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TCreateRTMR AbortUnsafe"
                         << ", opId: " << OperationId
                         << ", forceDropId: " << forceDropTxId
                         << ", at schemeshard: " << context.SS->TabletID());

        context.OnComplete.DoneOperation(OperationId);
    }
};

}

namespace NKikimr::NSchemeShard {

ISubOperation::TPtr CreateNewRTMR(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TCreateRTMR>(id, tx);
}

ISubOperation::TPtr CreateNewRTMR(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    return MakeSubOperation<TCreateRTMR>(id, state);
}

}

#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"

#include <ydb/core/base/subdomain.h>
#include <ydb/core/persqueue/config/config.h>
#include <ydb/core/mind/hive/hive.h>

namespace {

using namespace NKikimr;
using namespace NSchemeShard;

TBlockStoreVolumeInfo::TPtr CreateBlockStoreVolumeInfo(const NKikimrSchemeOp::TBlockStoreVolumeDescription& op,
                                                       TEvSchemeShard::EStatus& status,
                                                       TString& errStr)
{
    TBlockStoreVolumeInfo::TPtr volume = new TBlockStoreVolumeInfo();

    const auto& volumeConfig = op.GetVolumeConfig();
    if (!volumeConfig.HasBlockSize()) {
        status = NKikimrScheme::StatusSchemeError;
        errStr = "Block size is required";
        return nullptr;
    }

    ui32 partitionCount = op.GetVolumeConfig().PartitionsSize();
    if (partitionCount == 0) {
        status = NKikimrScheme::StatusSchemeError;
        errStr = Sprintf("Invalid number of partitions specified: %u", partitionCount);
        return nullptr;
    }

    if (volumeConfig.HasVersion()) {
        status = NKikimrScheme::StatusSchemeError;
        errStr = "Setting version is not allowed";
        return nullptr;
    }

    volume->AlterVersion = 1;
    volume->TokenVersion = 0;
    volume->DefaultPartitionCount =
        TBlockStoreVolumeInfo::CalculateDefaultPartitionCount(op.GetVolumeConfig());
    volume->VolumeConfig.CopyFrom(op.GetVolumeConfig());

    return volume;
}

void ApplySharding(TTxId txId, TPathId pathId, TBlockStoreVolumeInfo::TPtr volume, TTxState& txState,
                   const TChannelsBindings& partitionChannels, const TChannelsBindings& volumeChannels,
                   TOperationContext& context) {
    Y_ABORT_UNLESS(volume->VolumeConfig.GetTabletVersion() <= 2);
    ui64 count = volume->DefaultPartitionCount;
    txState.Shards.reserve(count + 1);

    for (ui64 i = 0; i < count; ++i) {
        TShardIdx shardIdx;
        if (volume->VolumeConfig.GetTabletVersion() == 2) {
            shardIdx = context.SS->RegisterShardInfo(
                TShardInfo::BlockStorePartition2Info(txId, pathId)
                    .WithBindedChannels(partitionChannels));
            context.SS->TabletCounters->Simple()[COUNTER_BLOCKSTORE_PARTITION2_SHARD_COUNT].Add(1);
            txState.Shards.emplace_back(shardIdx, ETabletType::BlockStorePartition2, TTxState::CreateParts);
        } else {
            shardIdx = context.SS->RegisterShardInfo(
                TShardInfo::BlockStorePartitionInfo(txId, pathId)
                    .WithBindedChannels(partitionChannels));
            context.SS->TabletCounters->Simple()[COUNTER_BLOCKSTORE_PARTITION_SHARD_COUNT].Add(1);
            txState.Shards.emplace_back(shardIdx, ETabletType::BlockStorePartition, TTxState::CreateParts);
        }

        TBlockStorePartitionInfo::TPtr part = new TBlockStorePartitionInfo();
        part->PartitionId = i;
        part->AlterVersion = 1;
        volume->Shards[shardIdx] = std::move(part);
    }

    const auto shardIdx = context.SS->RegisterShardInfo(
        TShardInfo::BlockStoreVolumeInfo(txId, pathId)
            .WithBindedChannels(volumeChannels));
    context.SS->TabletCounters->Simple()[COUNTER_BLOCKSTORE_VOLUME_SHARD_COUNT].Add(1);
    txState.Shards.emplace_back(shardIdx, ETabletType::BlockStoreVolume, TTxState::CreateParts);
    volume->VolumeShardIdx = shardIdx;
}

TTxState& PrepareChanges(TOperationId operationId, TPathElement::TPtr parentDir,
                    TPathElement::TPtr volumePath, TBlockStoreVolumeInfo::TPtr volume, const TString& acl,
                    const TChannelsBindings& partitionChannels, const TChannelsBindings& volumeChannels,
                    ui64 shardsToCreate,
                    TOperationContext& context)
{
    NIceDb::TNiceDb db(context.GetDB());

    volumePath->CreateTxId = operationId.GetTxId();
    volumePath->LastTxId = operationId.GetTxId();
    volumePath->PathState = TPathElement::EPathState::EPathStateCreate;
    volumePath->PathType = TPathElement::EPathType::EPathTypeBlockStoreVolume;
    TPathId pathId = volumePath->PathId;

    TTxState& txState = context.SS->CreateTx(operationId, TTxState::TxCreateBlockStoreVolume, pathId);

    ApplySharding(operationId.GetTxId(), pathId, volume, txState, partitionChannels, volumeChannels, context);

    if (parentDir->HasActiveChanges()) {
        TTxId parentTxId =  parentDir->PlannedToCreate() ? parentDir->CreateTxId : parentDir->LastTxId;
        context.OnComplete.Dependence(parentTxId, operationId.GetTxId());
    }

    context.SS->ChangeTxState(db, operationId, TTxState::CreateParts);
    context.OnComplete.ActivateTx(operationId);

    if (!acl.empty()) {
        volumePath->ApplyACL(acl);
    }
    context.SS->PersistPath(db, volumePath->PathId);

    for (auto& shard : volume->Shards) {
        auto shardIdx = shard.first;
        const auto& part = shard.second;
        context.SS->PersistBlockStorePartition(db, pathId, part->PartitionId, shardIdx, part->AlterVersion);
    }

    TBlockStoreVolumeInfo::TPtr emptyVolume = new TBlockStoreVolumeInfo();
    emptyVolume->Shards.swap(volume->Shards);
    context.SS->BlockStoreVolumes[pathId] = emptyVolume;
    context.SS->BlockStoreVolumes[pathId]->AlterData = volume;
    context.SS->IncrementPathDbRefCount(pathId);

    context.SS->PersistBlockStoreVolume(db, pathId, emptyVolume);
    context.SS->PersistAddBlockStoreVolumeAlter(db, pathId, volume);

    context.SS->PersistTxState(db, operationId);
    context.SS->PersistUpdateNextPathId(db);
    context.SS->PersistUpdateNextShardIdx(db);
    for (auto shard : txState.Shards) {
        Y_ABORT_UNLESS(shard.Operation == TTxState::CreateParts);
        context.SS->PersistChannelsBinding(db, shard.Idx, context.SS->ShardInfos[shard.Idx].BindedChannels);
        context.SS->PersistShardMapping(db, shard.Idx, InvalidTabletId, pathId, operationId.GetTxId(), shard.TabletType);
    }
    Y_ABORT_UNLESS(txState.Shards.size() == shardsToCreate);

    return txState;
}




class TCreateBlockStoreVolume: public TSubOperation {
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
            return MakeHolder<NBSVState::TConfigureParts>(OperationId);
        case TTxState::Propose:
            return MakeHolder<NBSVState::TPropose>(OperationId);
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

        const auto& operation = Transaction.GetCreateBlockStoreVolume();
        const auto acceptExisted = !Transaction.GetFailOnExist();

        const TString& parentPathStr = Transaction.GetWorkingDir();
        const TString& name = Transaction.GetCreateBlockStoreVolume().GetName();
        const auto defaultPartitionCount =
            TBlockStoreVolumeInfo::CalculateDefaultPartitionCount(
                operation.GetVolumeConfig());
        const ui64 shardsToCreate = defaultPartitionCount + 1;

        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TCreateBlockStoreVolume Propose"
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
                    .FailOnExist(TPathElement::EPathType::EPathTypeBlockStoreVolume, acceptExisted);
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

        TBlockStoreVolumeInfo::TPtr volume = CreateBlockStoreVolumeInfo(operation, status, errStr);
        if (!volume) {
            result->SetError(status, errStr);
            return result;
        }

        auto resolveChannels = [&] (const auto& ecps, TChannelsBindings& binding)
        {
            TVector<TStringBuf> poolKinds;
            poolKinds.reserve(ecps.size());
            for (const auto& ecp : ecps) {
                poolKinds.push_back(ecp.GetPoolKind());
            }
            return context.SS->ResolveChannelsByPoolKinds(
                poolKinds,
                dstPath.GetPathIdForDomain(),
                binding);
        };

        TChannelsBindings partitionChannelsBinding;
        if (defaultPartitionCount) {
            const auto& ecps = operation.GetVolumeConfig().GetExplicitChannelProfiles();

            if (ecps.empty() || ui32(ecps.size()) > NHive::MAX_TABLET_CHANNELS) {
                auto errStr = Sprintf("Wrong number of channels %u , should be [1 .. %lu]",
                                    ecps.size(),
                                    NHive::MAX_TABLET_CHANNELS);
                result->SetError(NKikimrScheme::StatusInvalidParameter, errStr);
                return result;
            }

            if (!resolveChannels(ecps, partitionChannelsBinding)) {
                result->SetError(NKikimrScheme::StatusInvalidParameter,
                                "Unable to construct channel binding for partition with the storage pool");
                return result;
            }

            context.SS->SetNbsChannelsParams(ecps, partitionChannelsBinding);
        }

        TChannelsBindings volumeChannelsBinding;
        const auto& ecps = operation.GetVolumeConfig().GetVolumeExplicitChannelProfiles();
        if (ecps.size()) {
            if ((ui32)ecps.size() != TBlockStoreVolumeInfo::NumVolumeTabletChannels) {
                auto errStr = Sprintf("Wrong number of channels %u , should be %lu",
                    ecps.size(),
                    TBlockStoreVolumeInfo::NumVolumeTabletChannels);
                result->SetError(NKikimrScheme::StatusInvalidParameter, errStr);
                return result;
            }

            if (!resolveChannels(ecps, volumeChannelsBinding)) {
                result->SetError(NKikimrScheme::StatusInvalidParameter,
                                "Unable to construct channel binding for volume with the storage pool");
                return result;
            }
            context.SS->SetNbsChannelsParams(ecps, volumeChannelsBinding);
        } else {
            const ui32 volumeProfileId = 0;
            if (!context.SS->ResolveTabletChannels(volumeProfileId, dstPath.GetPathIdForDomain(), volumeChannelsBinding)) {
                result->SetError(NKikimrScheme::StatusInvalidParameter,
                                "Unable to construct channel binding for volume with the profile");
                return result;
            }
        }

        auto domainDir = context.SS->PathsById.at(dstPath.GetPathIdForDomain());
        Y_ABORT_UNLESS(domainDir);

        auto volumeSpace = volume->GetVolumeSpace();
        if (!domainDir->CheckVolumeSpaceChange(volumeSpace, { }, errStr)) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, errStr);
            return result;
        }

        dstPath.MaterializeLeaf(owner);
        result->SetPathId(dstPath.Base()->PathId.LocalPathId);

        context.SS->TabletCounters->Simple()[COUNTER_BLOCKSTORE_VOLUME_COUNT].Add(1);
        domainDir->ChangeVolumeSpaceBegin(volumeSpace, { });

        const TTxState& txState = PrepareChanges(OperationId, parentPath.Base(), dstPath.Base(), volume, acl, partitionChannelsBinding, volumeChannelsBinding, shardsToCreate, context);

        NIceDb::TNiceDb db(context.GetDB());
        ++parentPath.Base()->DirAlterVersion;
        context.SS->PersistPathDirAlterVersion(db, parentPath.Base());
        context.SS->ClearDescribePathCaches(parentPath.Base());
        context.OnComplete.PublishToSchemeBoard(OperationId, parentPath.Base()->PathId);

        context.SS->ClearDescribePathCaches(dstPath.Base());
        context.OnComplete.PublishToSchemeBoard(OperationId, dstPath.Base()->PathId);

        dstPath.DomainInfo()->IncPathsInside();
        dstPath.DomainInfo()->AddInternalShards(txState);
        dstPath.Base()->IncShardsInside(shardsToCreate);
        parentPath.Base()->IncAliveChildren();

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext&) override {
        Y_ABORT("no AbortPropose for TCreateBlockStoreVolume");
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TCreateBlockStoreVolume AbortUnsafe"
                         << ", opId: " << OperationId
                         << ", forceDropId: " << forceDropTxId
                         << ", at schemeshard: " << context.SS->TabletID());

        context.OnComplete.DoneOperation(OperationId);
    }
};

}

namespace NKikimr::NSchemeShard {

ISubOperation::TPtr CreateNewBSV(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TCreateBlockStoreVolume>(id, tx);
}

ISubOperation::TPtr CreateNewBSV(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    return MakeSubOperation<TCreateBlockStoreVolume>(id, state);
}

}

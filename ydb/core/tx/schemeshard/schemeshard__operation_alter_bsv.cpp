#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"

#include <ydb/core/base/subdomain.h>
#include <ydb/core/persqueue/config/config.h>
#include <ydb/core/mind/hive/hive.h>

namespace {

using namespace NKikimr;
using namespace NSchemeShard;

class TAlterBlockStoreVolume: public TSubOperation {
    static TTxState::ETxState NextState() {
        return TTxState::CreateParts;
    }

    TTxState::ETxState NextState(TTxState::ETxState state) const override {
        switch (state) {
        case TTxState::Waiting:
        case TTxState::CreateParts:
            return TTxState::ConfigureParts;
        case TTxState::ConfigureParts:
            return TTxState::Propose; // DONE ???
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

    TTxState& PrepareChanges(
            TOperationId operationId, TPathElement::TPtr item,
            TBlockStoreVolumeInfo::TPtr volume,
            const TChannelsBindings& partitionChannels,
            const TChannelsBindings& volumeChannels,
            ui64 shardsToCreate,
            TOperationContext& context)
    {
        NIceDb::TNiceDb db(context.GetDB());

        item->LastTxId = operationId.GetTxId();
        item->PathState = TPathElement::EPathState::EPathStateAlter;

        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxAlterBlockStoreVolume, item->PathId);
        txState.State = TTxState::CreateParts;

        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "AlterBlockStoreVolume opId# " << operationId
                    << " AlterVersion# " << volume->AlterData->AlterVersion
                    << " DefaultPartitions#" << volume->DefaultPartitionCount
                    << "->" << volume->AlterData->DefaultPartitionCount
                    << " ExplicitChannelProfiles#" << volume->ExplicitChannelProfileCount
                    << "->" << volume->AlterData->ExplicitChannelProfileCount);

        bool needMoreShards = ApplySharding(
            operationId.GetTxId(),
            item->PathId,
            volume,
            txState,
            partitionChannels,
            volumeChannels,
            context);

        if (needMoreShards) {
            context.SS->PersistUpdateNextShardIdx(db);
        }

        for (const auto& kv : volume->Shards) {
            auto shardIdx = kv.first;
            const auto& part = kv.second;
            context.SS->PersistBlockStorePartition(db, item->PathId, part->PartitionId, shardIdx, part->AlterVersion);
        }

        context.SS->PersistAddBlockStoreVolumeAlter(db, item->PathId, volume->AlterData);

        context.OnComplete.ActivateTx(operationId);

        context.SS->PersistTxState(db, operationId);
        ui32 checkShardToCreate = 0;
        for (auto shard : txState.Shards) {
            auto& shardInfo = context.SS->ShardInfos.at(shard.Idx);
            shardInfo.CurrentTxId = operationId.GetTxId();
            if (shard.Operation == TTxState::CreateParts) {
                context.SS->PersistChannelsBinding(db, shard.Idx, shardInfo.BindedChannels);
                context.SS->PersistShardMapping(db, shard.Idx, shardInfo.TabletID, item->PathId, operationId.GetTxId(), shard.TabletType);
                if (shardInfo.TabletID == InvalidTabletId) {
                    // Only count new tablets
                    ++checkShardToCreate;
                }
            } else {
                context.SS->PersistShardTx(db, shard.Idx, operationId.GetTxId());
            }
        }
        Y_ABORT_UNLESS(shardsToCreate == checkShardToCreate);

        return txState;
    }

    static bool Compare(const TChannelBind& a, const TChannelBind& b)
    {
        return a.SerializeAsString() == b.SerializeAsString();
    }

    bool ApplySharding(
            TTxId txId,
            TPathId pathId,
            TBlockStoreVolumeInfo::TPtr volume,
            TTxState& txState,
            const TChannelsBindings& partitionChannels,
            const TChannelsBindings& volumeChannels,
            TOperationContext& context)
    {
        Y_ABORT_UNLESS(volume->AlterData->DefaultPartitionCount
                >= volume->DefaultPartitionCount);
        ui64 count = volume->AlterData->DefaultPartitionCount + 1;
        ui64 shardsToCreate = volume->AlterData->DefaultPartitionCount
            - volume->DefaultPartitionCount;
        txState.Shards.reserve(count + 1);

        // reconfigure old shards
        for (const auto& kv : volume->Shards) {
            auto shardIdx = kv.first;
            auto& shardInfo = context.SS->ShardInfos[shardIdx];
            auto partitionOp = TTxState::ConfigureParts;

            if (!shardInfo.BindedChannels.empty()) {
                for (ui32 i = 0; i < partitionChannels.size(); ++i) {
                    if (i >= shardInfo.BindedChannels.size()) {
                        shardInfo.BindedChannels.push_back(partitionChannels[i]);
                        partitionOp = TTxState::CreateParts;
                    } else if (!Compare(partitionChannels[i], shardInfo.BindedChannels[i])) {
                        shardInfo.BindedChannels[i] = partitionChannels[i];
                        partitionOp = TTxState::CreateParts;
                    }
                }
            }

            if (volume->VolumeConfig.GetTabletVersion() == 2) {
                txState.Shards.emplace_back(shardIdx, ETabletType::BlockStorePartition2, partitionOp);
            } else {
                txState.Shards.emplace_back(shardIdx, ETabletType::BlockStorePartition, partitionOp);
            }
        }

        // create new shards
        for (ui64 i = 0; i < shardsToCreate; ++i) {
            TShardIdx shardIdx;
            if (volume->VolumeConfig.GetTabletVersion() == 2) {
                shardIdx = context.SS->RegisterShardInfo(TShardInfo::BlockStorePartition2Info(txId, pathId));
                context.SS->TabletCounters->Simple()[COUNTER_BLOCKSTORE_PARTITION2_SHARD_COUNT].Add(1);
            } else {
                shardIdx = context.SS->RegisterShardInfo(TShardInfo::BlockStorePartitionInfo(txId, pathId));
                context.SS->TabletCounters->Simple()[COUNTER_BLOCKSTORE_PARTITION_SHARD_COUNT].Add(1);
            }
            auto& shardInfo = context.SS->ShardInfos.at(shardIdx);
            shardInfo.BindedChannels = partitionChannels;
            txState.Shards.emplace_back(shardIdx, shardInfo.TabletType, TTxState::CreateParts);

            TBlockStorePartitionInfo::TPtr part = new TBlockStorePartitionInfo();
            part->PartitionId = i + volume->DefaultPartitionCount;
            part->AlterVersion = volume->AlterData->AlterVersion;
            volume->Shards[shardIdx] = std::move(part);
        }

        // update the volume shard if needed
        auto volumeOp = TTxState::ConfigureParts;
        auto shardIdx = volume->VolumeShardIdx;
        auto& shardInfo = context.SS->ShardInfos[shardIdx];
        for (ui32 i = 0; i < volumeChannels.size(); ++i) {
            if (i >= shardInfo.BindedChannels.size()) {
                shardInfo.BindedChannels.push_back(volumeChannels[i]);
                volumeOp = TTxState::CreateParts;
            } else if (!Compare(volumeChannels[i], shardInfo.BindedChannels[i])) {
                shardInfo.BindedChannels[i] = volumeChannels[i];
                volumeOp = TTxState::CreateParts;
            }
        }
        txState.Shards.emplace_back(shardIdx, ETabletType::BlockStoreVolume, volumeOp);

        return shardsToCreate > 0;
    }

    const NKikimrBlockStore::TVolumeConfig* ParseParams(
            const NKikimrBlockStore::TVolumeConfig& baseline,
            const NKikimrSchemeOp::TBlockStoreVolumeDescription& alter,
            TString& errStr)
    {
        if (alter.PartitionsSize() > 0 || alter.HasVolumeTabletId() || alter.HasAlterVersion()) {
            errStr = "Setting schemeshard owned properties is not allowed";
            return nullptr;
        }
        if (alter.HasMountToken()) {
            errStr = "Changing mount token must be done with an assign operation";
            return nullptr;
        }

        if (!alter.HasVolumeConfig()) {
            errStr = "Missing changes to volume config";
            return nullptr;
        }

        const auto& volumeConfig = alter.GetVolumeConfig();
        if (volumeConfig.HasBlockSize()) {
            errStr = "Cannot change block size after creation";
            return nullptr;
        }
        if (volumeConfig.HasStripeSize()) {
            errStr = "Cannot change stripe size after creation";
            return nullptr;
        }

        // treating missing media kind and 0 (default media kind) in the same way
        if (baseline.GetStorageMediaKind() && volumeConfig.HasStorageMediaKind()) {
            errStr = TStringBuilder() << "Cannot change media kind after creation"
                << ", current media kind: " << baseline.GetStorageMediaKind()
                << ", attempted to change to: " << volumeConfig.GetStorageMediaKind()
                ;
            return nullptr;
        }

        if (volumeConfig.HasTabletVersion()) {
            errStr = "Cannot change tablet version after creation";
            return nullptr;
        }

        return &volumeConfig;
    }

    bool ProcessVolumeChannelProfiles(
        const TPath& path,
        const NKikimrBlockStore::TVolumeConfig& volumeConfig,
        const NKikimrBlockStore::TVolumeConfig& alterVolumeConfig,
        TOperationContext& context,
        TProposeResponse& result,
        TChannelsBindings* volumeChannelsBinding)
    {
        const auto& alterVolumeEcps = alterVolumeConfig.GetVolumeExplicitChannelProfiles();

        if (alterVolumeEcps.size() &&
            (ui32)alterVolumeEcps.size() != TBlockStoreVolumeInfo::NumVolumeTabletChannels)
        {
            auto errStr = Sprintf("Wrong number of channels %u , should be %lu",
                                alterVolumeEcps.size(),
                                TBlockStoreVolumeInfo::NumVolumeTabletChannels);
            result.SetError(NKikimrScheme::StatusInvalidParameter, errStr);
            return false;
        }

        if (alterVolumeEcps.size() || volumeConfig.VolumeExplicitChannelProfilesSize()) {
            const auto& ecps = alterVolumeEcps.empty()
                ? volumeConfig.GetVolumeExplicitChannelProfiles()
                : alterVolumeEcps;

            TVector<TStringBuf> poolKinds;
            poolKinds.reserve(ecps.size());
            for (const auto& ecp : ecps) {
                poolKinds.push_back(ecp.GetPoolKind());
            }

            const auto volumeChannelsResolved = context.SS->ResolveChannelsByPoolKinds(
                poolKinds,
                path.GetPathIdForDomain(),
                *volumeChannelsBinding);

            if (!volumeChannelsResolved) {
                result.SetError(NKikimrScheme::StatusInvalidParameter,
                                "Unable to construct channel binding for volume with the storage pool");
                return false;
            }
            context.SS->SetNbsChannelsParams(ecps, *volumeChannelsBinding);
        } else {
            const ui32 volumeProfileId = 0;
            if (!context.SS->ResolveTabletChannels(volumeProfileId, path.GetPathIdForDomain(), *volumeChannelsBinding)) {
                result.SetError(NKikimrScheme::StatusInvalidParameter,
                                "Unable to construct channel binding for volume with the profile");
                return false;
            }
        }

        return true;
    }

    bool ProcessChannelProfiles(
        const TPath& path,
        const NKikimrBlockStore::TVolumeConfig& volumeConfig,
        const NKikimrBlockStore::TVolumeConfig& alterVolumeConfig,
        TOperationContext& context,
        TProposeResponse& result,
        TChannelsBindings* partitionChannelsBinding)
    {
        const auto& alterEcps = alterVolumeConfig.GetExplicitChannelProfiles();

        if (alterEcps.size()) {
            if (ui32(alterEcps.size()) > NHive::MAX_TABLET_CHANNELS) {
                auto errStr = Sprintf("Wrong number of channels %u , should be [1 .. %lu]",
                                    alterEcps.size(),
                                    NHive::MAX_TABLET_CHANNELS);
                result.SetError(NKikimrScheme::StatusInvalidParameter, errStr);
                return false;
            }

            // Cannot delete explicit profiles for existing channels
            if (alterVolumeConfig.ExplicitChannelProfilesSize()
                    < volumeConfig.ExplicitChannelProfilesSize())
            {
                result.SetError(
                    NKikimrScheme::StatusInvalidParameter,
                    "Cannot reduce the number of channel profiles");
                return false;
            }

            if (!alterVolumeConfig.GetPoolKindChangeAllowed()) {
                // Cannot change pool kinds for existing channels
                // But it's ok to change other params, e.g. DataKind
                for (ui32 i = 0; i < volumeConfig.ExplicitChannelProfilesSize(); ++i) {
                    const auto& prevProfile =
                        volumeConfig.GetExplicitChannelProfiles(i);
                    const auto& newProfile =
                        alterVolumeConfig.GetExplicitChannelProfiles(i);
                    if (prevProfile.GetPoolKind() != newProfile.GetPoolKind()) {
                        result.SetError(
                            NKikimrScheme::StatusInvalidParameter,
                            TStringBuilder() << "Cannot change PoolKind for channel " << i
                                << ", " << prevProfile.GetPoolKind()
                                << " -> " << newProfile.GetPoolKind());
                        return false;
                    }
                }
            }
        }

        const auto& ecps = alterEcps.empty()
            ? volumeConfig.GetExplicitChannelProfiles()
            : alterEcps;

        TVector<TStringBuf> partitionPoolKinds;
        partitionPoolKinds.reserve(ecps.size());
        for (const auto& ecp : ecps) {
            partitionPoolKinds.push_back(ecp.GetPoolKind());
        }

        const auto partitionChannelsResolved = context.SS->ResolveChannelsByPoolKinds(
            partitionPoolKinds,
            path.GetPathIdForDomain(),
            *partitionChannelsBinding
        );
        if (!partitionChannelsResolved) {
            result.SetError(NKikimrScheme::StatusInvalidParameter,
                            "Unable to construct channel binding for partition with the storage pool");
            return false;
        }

        context.SS->SetNbsChannelsParams(ecps, *partitionChannelsBinding);

        return true;
    }

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        const TTabletId ssId = context.SS->SelfTabletId();

        const auto& alter = Transaction.GetAlterBlockStoreVolume();

        const TString& parentPathStr = Transaction.GetWorkingDir();
        const TString& name = alter.GetName();
        const TPathId pathId = alter.HasPathId()
            ? context.SS->MakeLocalId(alter.GetPathId()) : InvalidPathId;

        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TAlterBlockStoreVolume Propose"
                         << ", path: " << parentPathStr << "/" << name
                         << ", pathId: " << pathId
                         << ", opId: " << OperationId
                         << ", at schemeshard: " << ssId);

        auto result = MakeHolder<TProposeResponse>(
            NKikimrScheme::StatusAccepted,
            ui64(OperationId.GetTxId()),
            ui64(ssId)
        );

        TString errStr;

        if (!alter.HasName() && !alter.HasPathId()) {
            errStr = "Neither volume name nor pathId are present in BlockStoreVolume";
            result->SetError(NKikimrScheme::StatusInvalidParameter, errStr);
            return result;
        }

        TPath path = alter.HasPathId()
            ? TPath::Init(pathId, context.SS)
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
                .NotUnderOperation()
                .IsCommonSensePath();

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        TBlockStoreVolumeInfo::TPtr volume = context.SS->BlockStoreVolumes.at(path.Base()->PathId);
        Y_ABORT_UNLESS(volume);

        const auto* alterVolumeConfig = ParseParams(volume->VolumeConfig, alter, errStr);
        if (!alterVolumeConfig) {
            result->SetError(NKikimrScheme::StatusInvalidParameter, errStr);
            return result;
        }

        if (volume->AlterVersion == 0) {
            result->SetError(
                NKikimrScheme::StatusMultipleModifications,
                "Block store volume is not created yet");
            return result;
        }
        if (volume->AlterData) {
            result->SetError(
                NKikimrScheme::StatusMultipleModifications,
                "There's another alter in flight");
            return result;
        }

        const auto defaultPartitionCount =
            TBlockStoreVolumeInfo::CalculateDefaultPartitionCount(volume->VolumeConfig);
        const auto newDefaultPartitionCount =
            TBlockStoreVolumeInfo::CalculateDefaultPartitionCount(*alterVolumeConfig);

        TChannelsBindings partitionChannelsBinding;
        if (defaultPartitionCount || newDefaultPartitionCount) {
            const auto channelProfilesProcessed = ProcessChannelProfiles(
                path,
                volume->VolumeConfig,
                *alterVolumeConfig,
                context,
                *result,
                &partitionChannelsBinding
            );

            if (!channelProfilesProcessed) {
                return result;
            }
        }

        TChannelsBindings volumeChannelsBinding;
        const auto channelProfilesProcessed = ProcessVolumeChannelProfiles(
                path,
                volume->VolumeConfig,
                *alterVolumeConfig,
                context,
                *result,
                &volumeChannelsBinding);

        if (!channelProfilesProcessed) {
            return result;
        }

        if (alterVolumeConfig->PartitionsSize()) {
            // Cannot delete individual partitions
            // Do we need to verify whether geometry changes make sense?
            if (alterVolumeConfig->PartitionsSize()
                    < volume->VolumeConfig.PartitionsSize())
            {
                result->SetError(
                    NKikimrScheme::StatusInvalidParameter,
                    "Cannot reduce the number of partitions");
                return result;
            }

            for (ui32 i = 0; i < volume->VolumeConfig.PartitionsSize(); ++i) {
                if (!volume->VolumeConfig.GetSizeDecreaseAllowed()) {
                    const auto newBlockCount =
                        alterVolumeConfig->GetPartitions(i).GetBlockCount();
                    const auto prevBlockCount =
                        volume->VolumeConfig.GetPartitions(i).GetBlockCount();
                    if (newBlockCount < prevBlockCount) {
                        result->SetError(
                            NKikimrScheme::StatusInvalidParameter,
                            "Decreasing block count is not allowed");
                        return result;
                    }
                }

                if (alterVolumeConfig->GetPartitions(i).GetType()
                        != volume->VolumeConfig.GetPartitions(i).GetType())
                {
                    result->SetError(
                        NKikimrScheme::StatusInvalidParameter,
                        "partition type can't be changed");
                    return result;
                }
            }
        }

        if (alterVolumeConfig->HasVersion() &&
            alterVolumeConfig->GetVersion() != volume->AlterVersion)
        {
            result->SetError(
                NKikimrScheme::StatusPreconditionFailed,
                "Wrong version in VolumeConfig");
            return result;
        }

        if (alterVolumeConfig->HasFillGeneration() &&
            alterVolumeConfig->GetFillGeneration() != volume->VolumeConfig.GetFillGeneration())
        {
            result->SetError(
                NKikimrScheme::StatusPreconditionFailed,
                "Wrong FillGeneration in VolumeConfig");
            return result;
        }

        if (!context.SS->CheckApplyIf(Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, errStr);
            return result;
        }

        ui64 shardsToCreate = newDefaultPartitionCount
            ? newDefaultPartitionCount - defaultPartitionCount
            : 0;

        {
            TPath::TChecker checks = path.Check();
            checks
                .ShardsLimit(shardsToCreate)
                .PathShardsLimit(shardsToCreate);

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        auto oldVolumeSpace = volume->GetVolumeSpace();

        TBlockStoreVolumeInfo::TPtr alterData = new TBlockStoreVolumeInfo();
        alterData->VolumeConfig.CopyFrom(volume->VolumeConfig);
        if (alterVolumeConfig->PartitionsSize() > 0) {
            // MergeFrom will append partitions, we want a replace operation instead
            alterData->VolumeConfig.ClearPartitions();
        }
        if (alterVolumeConfig->ExplicitChannelProfilesSize() > 0) {
            // MergeFrom will append explicit channel profiles, we want a replace operation instead
            alterData->VolumeConfig.ClearExplicitChannelProfiles();
        }
        if (alterVolumeConfig->VolumeExplicitChannelProfilesSize() > 0) {
            // MergeFrom will append explicit channel profiles, we want a replace operation instead
            alterData->VolumeConfig.ClearVolumeExplicitChannelProfiles();
        }
        if (alterVolumeConfig->TagsSize() > 0) {
            // MergeFrom will append tags, we want a replace operation instead
            alterData->VolumeConfig.ClearTags();
        }
        alterData->VolumeConfig.MergeFrom(*alterVolumeConfig);
        alterData->VolumeConfig.ClearSizeDecreaseAllowed();
        alterData->VolumeConfig.ClearPoolKindChangeAllowed();
        alterData->DefaultPartitionCount =
            TBlockStoreVolumeInfo::CalculateDefaultPartitionCount(alterData->VolumeConfig);
        alterData->ExplicitChannelProfileCount = alterData->VolumeConfig.ExplicitChannelProfilesSize();
        volume->PrepareAlter(alterData);

        auto newVolumeSpace = volume->GetVolumeSpace();

        auto domainDir = context.SS->PathsById.at(path.GetPathIdForDomain());
        Y_ABORT_UNLESS(domainDir);

        auto checkedSpaceChange = domainDir->CheckVolumeSpaceChange(newVolumeSpace, oldVolumeSpace, errStr);
        if (!checkedSpaceChange) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, errStr);
            volume->ForgetAlter();
            return result;
        }

        // Increase in occupied space is applied immediately
        domainDir->ChangeVolumeSpaceBegin(newVolumeSpace, oldVolumeSpace);

        const TTxState& txState = PrepareChanges(
            OperationId,
            path.Base(),
            volume,
            partitionChannelsBinding,
            volumeChannelsBinding,
            shardsToCreate,
            context);

        context.SS->ClearDescribePathCaches(path.Base());
        context.OnComplete.PublishToSchemeBoard(OperationId, path.Base()->PathId);

        TSubDomainInfo::TPtr domainInfo = path.DomainInfo();
        domainInfo->AddInternalShards(txState, context.SS);

        path.Base()->IncShardsInside(shardsToCreate);

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext&) override {
        Y_ABORT("no AbortPropose for TAlterBlockStoreVolume");
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TAlterBlockStoreVolume AbortUnsafe"
                         << ", opId: " << OperationId
                         << ", forceDropId: " << forceDropTxId
                         << ", at schemeshard: " << context.SS->TabletID());

        context.OnComplete.DoneOperation(OperationId);
    }
};

}

namespace NKikimr::NSchemeShard {

ISubOperation::TPtr CreateAlterBSV(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TAlterBlockStoreVolume>(id, tx);
}

ISubOperation::TPtr CreateAlterBSV(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    return MakeSubOperation<TAlterBlockStoreVolume>(id, state);
}

}

#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"

#include <ydb/core/base/subdomain.h>
#include <ydb/core/mind/hive/hive.h>
#include <ydb/core/persqueue/config/config.h>
#include <ydb/core/persqueue/partition_key_range/partition_key_range.h>
#include <ydb/core/persqueue/utils.h>
#include <ydb/services/lib/sharding/sharding.h>


namespace {

using namespace NKikimr;
using namespace NSchemeShard;

class TAlterPQ: public TSubOperation {
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
            return MakeHolder<NPQState::TConfigureParts>(OperationId);
        case TTxState::Propose:
            return MakeHolder<NPQState::TPropose>(OperationId);
        case TTxState::Done:
            return MakeHolder<TDone>(OperationId);
        default:
            return nullptr;
        }
    }

public:
    using TSubOperation::TSubOperation;

    TTopicInfo::TPtr ParseParams(
            TOperationContext& context,
            NKikimrPQ::TPQTabletConfig* tabletConfig,
            const NKikimrSchemeOp::TPersQueueGroupDescription& alter,
            TString& errStr)
    {
        bool splitMergeEnabled = AppData()->FeatureFlags.GetEnableTopicSplitMerge()
            && NPQ::SplitMergeEnabled(*tabletConfig)
            && (!alter.HasPQTabletConfig() || !alter.GetPQTabletConfig().HasPartitionStrategy() || NPQ::SplitMergeEnabled(alter.GetPQTabletConfig()));

        TTopicInfo::TPtr params = new TTopicInfo();
        const bool hasKeySchema = tabletConfig->PartitionKeySchemaSize();

        if (!splitMergeEnabled && (alter.MergeSize() || alter.SplitSize())) {
            errStr = "Split and merge operations disabled";
            return nullptr;
        }

        if (alter.SplitSize()) {
            for (const auto& split : alter.GetSplit()) {
                if (!split.HasPartition()) {
                    errStr = "Partition for split must be specified for split operation";
                    return nullptr;
                }
                if (!split.HasSplitBoundary()) {
                    errStr = "SplitBoundary must be specified for split operation";
                    return nullptr;
                }
            }
        }
        if (alter.MergeSize()) {
            for (const auto& merge : alter.GetMerge()) {
                if (!merge.HasPartition()) {
                    errStr = "Partition for merge must be specified for merge operation";
                    return nullptr;
                }
                if (!merge.HasAdjacentPartition()) {
                    errStr = "AdjacentPartition for merge must be specified for merge operation";
                    return nullptr;
                }
            }
        }

        if (alter.HasTotalGroupCount()) {
            if (hasKeySchema) {
                errStr = "Cannot change partition count. Use split/merge instead";
                return nullptr;
            }

            ui32 totalGroupCount = alter.GetTotalGroupCount();
            if (!totalGroupCount || totalGroupCount > TSchemeShard::MaxPQGroupPartitionsCount) {
                errStr = Sprintf("Invalid total groups count specified: %u", totalGroupCount);
                return nullptr;
            }

            if (!splitMergeEnabled) {
                params->TotalGroupCount = totalGroupCount;
            }
        }
        if (alter.HasPQTabletConfig() && alter.GetPQTabletConfig().HasPartitionStrategy() && NPQ::SplitMergeEnabled(alter.GetPQTabletConfig())) {
            const auto strategy = alter.GetPQTabletConfig().GetPartitionStrategy();
            if (strategy.GetMaxPartitionCount() < strategy.GetMinPartitionCount()) {
                errStr = Sprintf("Invalid min and max partition count specified: %u > %u", strategy.GetMinPartitionCount(), strategy.GetMaxPartitionCount());
                return nullptr;
            }
        }
        if (alter.HasPartitionPerTablet()) {
            ui32 maxPartsPerTablet = alter.GetPartitionPerTablet();
            if (!maxPartsPerTablet) {
                errStr = Sprintf("Invalid partition per tablet count specified: %u", maxPartsPerTablet);
                return nullptr;
            }
            params->MaxPartsPerTablet = maxPartsPerTablet;
        }
        if (alter.HasPQTabletConfig()) {
            NKikimrPQ::TPQTabletConfig alterConfig = alter.GetPQTabletConfig();
            alterConfig.ClearPartitionIds();
            alterConfig.ClearPartitions();

            if (!CheckPersQueueConfig(alterConfig, false, &errStr)) {
                return nullptr;
            }

            if (!alterConfig.HasPartitionStrategy() && tabletConfig->HasPartitionStrategy()) {
                alterConfig.MutablePartitionStrategy()->CopyFrom(tabletConfig->GetPartitionStrategy());
            }

            if (alterConfig.GetPartitionConfig().HasLifetimeSeconds()) {
                const auto lifetimeSeconds = alterConfig.GetPartitionConfig().GetLifetimeSeconds();
                if (lifetimeSeconds <= 0 || (ui32)lifetimeSeconds > TSchemeShard::MaxPQLifetimeSeconds) {
                    errStr = TStringBuilder() << "Invalid retention period"
                        << ": specified: " << lifetimeSeconds << "s"
                        << ", min: " << 1 << "s"
                        << ", max: " << TSchemeShard::MaxPQLifetimeSeconds << "s";
                    return nullptr;
                }
            } else {
                alterConfig.MutablePartitionConfig()->SetLifetimeSeconds(tabletConfig->GetPartitionConfig().GetLifetimeSeconds());
            }

            if (alterConfig.GetPartitionConfig().ExplicitChannelProfilesSize() > 0) {
                // Validate explicit channel profiles alter attempt
                const auto& ecps = alterConfig.GetPartitionConfig().GetExplicitChannelProfiles();
                if (ecps.size() < 3 || ui32(ecps.size()) > NHive::MAX_TABLET_CHANNELS) {
                    auto errStr = Sprintf("ExplicitChannelProfiles has %u channels, should be [3 .. %lu]",
                                        ecps.size(),
                                        NHive::MAX_TABLET_CHANNELS);
                    return nullptr;
                }
                if (ecps.size() < tabletConfig->GetPartitionConfig().GetExplicitChannelProfiles().size()) {
                    auto errStr = Sprintf("ExplicitChannelProfiles has %u channels, should be at least %lu (current config)",
                                        ecps.size(),
                                        tabletConfig->GetPartitionConfig().ExplicitChannelProfilesSize());
                    return nullptr;
                }
            } else if (tabletConfig->GetPartitionConfig().ExplicitChannelProfilesSize() > 0) {
                // Make sure alter config has correct explicit channel profiles
                alterConfig.MutablePartitionConfig()->MutableExplicitChannelProfiles()->Swap(
                    tabletConfig->MutablePartitionConfig()->MutableExplicitChannelProfiles());
            }

            if (alterConfig.PartitionKeySchemaSize()) {
                errStr = "Cannot change key schema";
                return nullptr;
            }

            if (alterConfig.HasPartitionStrategy() && !NPQ::SplitMergeEnabled(alterConfig)
                && tabletConfig->HasPartitionStrategy() && NPQ::SplitMergeEnabled(*tabletConfig)) {
                if (!alterConfig.GetPartitionStrategy().HasMaxPartitionCount() || 0 != alterConfig.GetPartitionStrategy().GetMaxPartitionCount()) {
                    errStr = TStringBuilder() << "Can`t disable auto partitioning. Disabling auto partitioning is a destructive operation, "
                            << "after which all partitions will become active and the message order guarantee will be violated. "
                            << "If you are sure of this, then set max_active_partitions to 0.";
                    return nullptr;
                }
            }

            if (!alterConfig.HasPartitionStrategy() && tabletConfig->HasPartitionStrategy()) {
                alterConfig.MutablePartitionStrategy()->CopyFrom(tabletConfig->GetPartitionStrategy());
            }

            const TPathElement::TPtr dbRootEl = context.SS->PathsById.at(context.SS->RootPathId());
            if (dbRootEl->UserAttrs->Attrs.contains("cloud_id")) {
                auto cloudId = dbRootEl->UserAttrs->Attrs.at("cloud_id");
                alterConfig.SetYcCloudId(cloudId);
            }
            if (dbRootEl->UserAttrs->Attrs.contains("folder_id")) {
                auto folderId = dbRootEl->UserAttrs->Attrs.at("folder_id");
                alterConfig.SetYcFolderId(folderId);
            }
            if (dbRootEl->UserAttrs->Attrs.contains("database_id")) {
                auto databaseId = dbRootEl->UserAttrs->Attrs.at("database_id");
                alterConfig.SetYdbDatabaseId(databaseId);
            }
            const TString databasePath = TPath::Init(context.SS->RootPathId(), context.SS).PathString();
            alterConfig.SetYdbDatabasePath(databasePath);


            if (alterConfig.HasOffloadConfig()) {
                // TODO: check validity
                auto* pathId = alterConfig.MutableOffloadConfig()->MutableIncrementalBackup()->MutableDstPathId();
                PathIdFromPathId(TPath::Resolve(alterConfig.GetOffloadConfig().GetIncrementalBackup().GetDstPath(), context.SS).Base()->PathId, pathId);
            }

            alterConfig.MutablePartitionKeySchema()->Swap(tabletConfig->MutablePartitionKeySchema());
            Y_PROTOBUF_SUPPRESS_NODISCARD alterConfig.SerializeToString(&params->TabletConfig);
            alterConfig.Swap(tabletConfig);
        }
        if (alter.PartitionsToDeleteSize()) {
            errStr = Sprintf("deletion of partitions is not supported yet");
            return nullptr;
        }
        if (alter.PartitionsToAddSize()) {
            if (hasKeySchema || splitMergeEnabled) {
                errStr = "Cannot change partition count. Use split/merge instead";
                return nullptr;
            }

            if (params->TotalGroupCount) {
                errStr = Sprintf("providing TotalGroupCount and PartitionsToAdd at the same time is forbidden");
                return nullptr;
            }

            THashSet<ui32> parts;
            for (const auto& p : alter.GetPartitionsToAdd()) {
                if (!parts.insert(p.GetPartitionId()).second) {
                    errStr = TStringBuilder()
                            << "providing partition " <<  p.GetPartitionId() << " several times in PartitionsToAdd is forbidden";
                    return nullptr;
                }
                params->PartitionsToAdd.emplace(p.GetPartitionId(), p.GetGroupId());
            }
        }
        if (alter.HasBootstrapConfig()) {
            errStr = "Bootstrap config can be passed only upon creation";
            return nullptr;
        }
        return params;
    }

    TTxState& PrepareChanges(
            TOperationId operationId,
            const TPath& path,
            TTopicInfo::TPtr pqGroup,
            ui64 shardsToCreate,
            const TChannelsBindings& rbChannelsBinding,
            const TChannelsBindings& pqChannelsBinding,
            TOperationContext& context,
            const NKikimrPQ::TPQTabletConfig& tabletConfig,
            const NKikimrPQ::TPQTabletConfig& newTabletConfig)
    {
        TPathElement::TPtr item = path.Base();
        NIceDb::TNiceDb db(context.GetDB());

        item->LastTxId = operationId.GetTxId();
        item->PathState = TPathElement::EPathState::EPathStateAlter;

        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxAlterPQGroup, item->PathId);
        txState.State = TTxState::CreateParts;

        bool needMoreShards = ApplySharding(operationId.GetTxId(), item->PathId, pqGroup, txState, rbChannelsBinding, pqChannelsBinding, context);
        if (needMoreShards) {
            context.SS->PersistUpdateNextShardIdx(db);
        }

        bool splitMergeWasDisabled = NKikimr::NPQ::SplitMergeEnabled(tabletConfig)
                && !NKikimr::NPQ::SplitMergeEnabled(newTabletConfig);
        bool splitMergeWasEnabled = !NKikimr::NPQ::SplitMergeEnabled(tabletConfig)
                && NKikimr::NPQ::SplitMergeEnabled(newTabletConfig);

        if (splitMergeWasEnabled) {
            auto partitions = pqGroup->GetPartitions();

            TString prevBound;
            for (size_t i = 0; i < partitions.size(); ++i) {
                auto* partitionInfo = partitions[i].second;
                if (i) {
                    partitionInfo->KeyRange.ConstructInPlace();
                    partitionInfo->KeyRange->FromBound = prevBound;
                }
                if (i != (partitions.size() - 1)) {
                    if (!partitionInfo->KeyRange) {
                        partitionInfo->KeyRange.ConstructInPlace();
                    }
                    auto range = NDataStreams::V1::RangeFromShardNumber(i, partitions.size());
                    prevBound = NPQ::AsKeyBound(range.End);
                    partitionInfo->KeyRange->ToBound = prevBound;
                }

                context.SS->PersistPersQueue(db, item->PathId, partitions[i].first, *partitionInfo);
            }
        } else {
            for (auto& [shardIdx, tabletInfo] : pqGroup->Shards) {
                for (const auto& partitionInfo : tabletInfo->Partitions) {
                    if (splitMergeWasDisabled) {
                        // clear all splitmerge fields
                        partitionInfo->Status = NKikimrPQ::ETopicPartitionStatus::Active;
                        partitionInfo->KeyRange.Clear();
                        partitionInfo->ParentPartitionIds.clear();
                        partitionInfo->ChildPartitionIds.clear();
                    }
                    context.SS->PersistPersQueue(db, item->PathId, shardIdx, *partitionInfo.Get());
                }
            }
        }

        context.SS->PersistAddPersQueueGroupAlter(db, item->PathId, pqGroup->AlterData);

        context.SS->PersistTxState(db, operationId);
        ui64 checkShardsToCreate = 0;
        for (auto shard : txState.Shards) {
            if (shard.Operation == TTxState::CreateParts) {
                TShardInfo& shardInfo = context.SS->ShardInfos[shard.Idx];
                context.SS->PersistShardMapping(db, shard.Idx, shardInfo.TabletID, item->PathId, operationId.GetTxId(), shard.TabletType);
                switch (shard.TabletType) {
                    case ETabletType::PersQueueReadBalancer:
                        context.SS->PersistChannelsBinding(db, shard.Idx, rbChannelsBinding);
                        context.SS->TabletCounters->Simple()[COUNTER_PQ_RB_SHARD_COUNT].Add(1);
                        break;
                    default:
                        context.SS->PersistChannelsBinding(db, shard.Idx, pqChannelsBinding);
                        context.SS->TabletCounters->Simple()[COUNTER_PQ_SHARD_COUNT].Add(1);
                        break;
                }
                if (!shardInfo.TabletID) {
                    ++checkShardsToCreate;
                }
            }
        }
        Y_ABORT_UNLESS(shardsToCreate == checkShardsToCreate);

        return txState;
    }

    static bool IsChannelsEqual(const TChannelsBindings& a, const TChannelsBindings& b) {
        // for some reason, the default equality operator doesn't work with this proto message
        return std::equal(a.begin(), a.end(), b.begin(), b.end(),
                          [](const NKikimrStoragePool::TChannelBind& a, const NKikimrStoragePool::TChannelBind& b) -> bool {
                            return a.storagepoolname() == b.storagepoolname()
                                    && a.iops() == b.iops()
                                    && a.throughput() == b.throughput()
                                    && a.size() == b.size();
                          });
    }

    static bool IsShardRequiresRecreation(const TShardInfo& actual, const TShardInfo& requested) {
        if (actual.BindedChannels.size() < requested.BindedChannels.size()) {
            return true;
        }
        if (actual.BindedChannels.size() == requested.BindedChannels.size()
            && !IsChannelsEqual(actual.BindedChannels, requested.BindedChannels)) {
            return true;
        }
        return false;
    }

    bool ApplySharding(
            TTxId txId,
            const TPathId& pathId,
            TTopicInfo::TPtr pqGroup,
            TTxState& txState,
            const TChannelsBindings& rbBindedChannels,
            const TChannelsBindings& pqBindedChannels,
            TOperationContext& context)
    {
        TShardInfo defaultShardInfo = TShardInfo::PersQShardInfo(txId, pathId);
        defaultShardInfo.BindedChannels = pqBindedChannels;

        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "AlterPQGroup txid# " << txId
                    << " AlterVersion# " << pqGroup->AlterData->AlterVersion
                    << " Parts count# " << pqGroup->TotalPartitionCount << "->" << pqGroup->AlterData->TotalPartitionCount
                    << " Groups count# " << pqGroup->TotalGroupCount << "->" << pqGroup->AlterData->TotalGroupCount
                    << " MaxPerPQ# " << pqGroup->MaxPartsPerTablet << "->" << pqGroup->AlterData->MaxPartsPerTablet
                    << " adding partitions# " << pqGroup->AlterData->PartitionsToAdd.size()
                    << " deleting partitions#  " << pqGroup->AlterData->PartitionsToDelete.size());

        // Leave pqGroup->AlterVersion unchanged. It will be updated in TxPlanStep.
        ui32 shardsNeeded = pqGroup->AlterData->ExpectedShardCount();
        ui32 shardsCurrent = pqGroup->ShardCount();
        if (shardsNeeded < shardsCurrent) // can't reduce counts
            shardsNeeded = shardsCurrent;

        ui32 pqShardsToCreate = shardsNeeded - shardsCurrent;

        txState.Shards.reserve(shardsNeeded);

        ui32 shardsToCreate = pqShardsToCreate;

        bool hasBalancer = pqGroup->HasBalancer();
        if (!hasBalancer) {
            shardsToCreate += 1;
        }

        // reconfig old shards
        for (auto& shard : pqGroup->Shards) {
            auto shardIdx = shard.first;
            auto& shardInfo = context.SS->ShardInfos[shardIdx];

            if (IsShardRequiresRecreation(shardInfo, defaultShardInfo)) {
                txState.Shards.emplace_back(shardIdx, ETabletType::PersQueue, TTxState::CreateParts);
                shardInfo.CurrentTxId = defaultShardInfo.CurrentTxId;
                shardInfo.BindedChannels = defaultShardInfo.BindedChannels;
            } else {
                txState.Shards.emplace_back(shardIdx, ETabletType::PersQueue, TTxState::ConfigureParts);
            }
        }

        // create new shards
        const auto startShardIdx = context.SS->ReserveShardIdxs(shardsToCreate);
        for (ui64 i = 0; i < pqShardsToCreate; ++i) {
            const auto idx = context.SS->NextShardIdx(startShardIdx, i);
            txState.Shards.emplace_back(idx, ETabletType::PersQueue, TTxState::CreateParts);

            context.SS->RegisterShardInfo(idx, defaultShardInfo);
            pqGroup->Shards[idx] = new TTopicTabletInfo();
        }

        if (!hasBalancer) {
            const auto idx = context.SS->NextShardIdx(startShardIdx, pqShardsToCreate);
            pqGroup->BalancerShardIdx = idx;
            txState.Shards.emplace_back(idx, ETabletType::PersQueueReadBalancer, TTxState::CreateParts);
            context.SS->RegisterShardInfo(idx,
                defaultShardInfo
                    .WithTabletType(ETabletType::PersQueueReadBalancer)
                    .WithBindedChannels(rbBindedChannels));
        } else {
            auto shardIdx = pqGroup->BalancerShardIdx;
            txState.Shards.emplace_back(shardIdx, ETabletType::PersQueueReadBalancer, TTxState::ConfigureParts);
        }

        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "AlterPQGroup txid# " << txId
                    << ". Shard count " << shardsCurrent << "->" << shardsNeeded
                    << ", first new shardIdx " << startShardIdx
                    << " hasBalancer " << hasBalancer);

        ReassignIds(pqGroup);
        return shardsToCreate > 0;
    }

    void ReassignIds(TTopicInfo::TPtr pqGroup) {
        Y_ABORT_UNLESS(pqGroup->TotalPartitionCount >= pqGroup->TotalGroupCount);
        ui32 numOld = pqGroup->TotalPartitionCount;
        ui32 numNew = pqGroup->AlterData->PartitionsToAdd.size() + numOld;
        //ui32 maxPerPart = pqGroup->AlterData->MaxPartsPerTablet;
        ui32 average = numNew / pqGroup->Shards.size(); // TODO: not optimal
        if (numNew % pqGroup->Shards.size())
            ++average;
        ui64 alterVersion = pqGroup->AlterData->AlterVersion;

        auto it = pqGroup->Shards.begin();

        for (const auto& p : pqGroup->AlterData->PartitionsToAdd) {
            auto partition = MakeHolder<TTopicTabletInfo::TTopicPartitionInfo>();
            partition->PqId = p.PartitionId;
            partition->GroupId = p.GroupId;
            partition->KeyRange = p.KeyRange;
            partition->AlterVersion = alterVersion;
            partition->CreateVersion = alterVersion;
            partition->Status = NKikimrPQ::ETopicPartitionStatus::Active;
            for (const auto parent : p.ParentPartitionIds) {
                partition->ParentPartitionIds.emplace(parent);
            }
            while (it->second->Partitions.size() >= average) {
                ++it;
            }

            for (const auto parentId : partition->ParentPartitionIds) {
                auto* parent = pqGroup->Partitions[parentId];
                parent->Status = NKikimrPQ::ETopicPartitionStatus::Inactive;
                parent->AlterVersion = alterVersion;
            }

            pqGroup->AddPartition(it->first, partition.Release());
        }
        pqGroup->InitSplitMergeGraph();
    }

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        const TTabletId ssId = context.SS->SelfTabletId();

        const auto& alter = Transaction.GetAlterPersQueueGroup();

        const TString& parentPathStr = Transaction.GetWorkingDir();
        const TString& name = alter.GetName();
        const TPathId pathId = alter.HasPathId() ? context.SS->MakeLocalId(alter.GetPathId()) : InvalidPathId;

        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TAlterPQ Propose"
                         << ", path: " << parentPathStr << "/" << name
                         << ", pathId: " << pathId
                         << ", opId: " << OperationId
                         << ", at schemeshard: " << ssId);


        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), ui64(ssId));

        TString errStr;

        if (!alter.HasName() && !alter.HasPathId()) {
            errStr = "Neither topic name nor pathId in Alter";
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
                .IsPQGroup()
                .NotUnderOperation();

            if (!Transaction.GetAllowAccessToPrivatePaths()) {
                checks.IsCommonSensePath();
            }

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        TTopicInfo::TPtr topic = context.SS->Topics.at(path.Base()->PathId);
        Y_ABORT_UNLESS(topic);

        if (topic->AlterVersion == 0) {
            result->SetError(NKikimrScheme::StatusMultipleModifications, "PQGroup is not created yet");
            return result;
        }
        if (topic->AlterData) {
            result->SetError(NKikimrScheme::StatusMultipleModifications, "There's another Alter in flight");
            return result;
        }

        NKikimrPQ::TPQTabletConfig tabletConfig = topic->GetTabletConfig();
        NKikimrPQ::TPQTabletConfig newTabletConfig = tabletConfig;

        TTopicInfo::TPtr alterData = ParseParams(context, &newTabletConfig, alter, errStr);

        if (!alterData) {
            result->SetError(NKikimrScheme::StatusInvalidParameter, errStr);
            return result;
        }

        // do not change if not set
        if (!alterData->TotalGroupCount) {
            alterData->TotalGroupCount = topic->TotalGroupCount;
        }

        if (!alterData->MaxPartsPerTablet) {
            alterData->MaxPartsPerTablet = topic->MaxPartsPerTablet;
        }

        if (alterData->TotalGroupCount < topic->TotalGroupCount) {
            errStr = TStringBuilder() << "Invalid total groups count specified: " << alterData->TotalGroupCount
                                      << " vs " << topic->TotalGroupCount << " (current)";
            result->SetError(NKikimrScheme::StatusInvalidParameter, errStr);
            return result;
        }

        alterData->ActivePartitionCount = topic->ActivePartitionCount;

        bool splitMergeEnabled = AppData()->FeatureFlags.GetEnableTopicSplitMerge()
                && NKikimr::NPQ::SplitMergeEnabled(tabletConfig)
                && NKikimr::NPQ::SplitMergeEnabled(newTabletConfig);

        THashSet<ui32> involvedPartitions;
        if (splitMergeEnabled) {
            auto Hex = [](const auto& value) {
                return HexText(TBasicStringBuf(value));
            };

            ui32 nextId = topic->NextPartitionId;
            ui32 nextGroupId = topic->TotalGroupCount;

            for (const auto& split : alter.GetSplit()) {
                alterData->TotalGroupCount += 2;
                ++alterData->ActivePartitionCount;

                const auto splittedPartitionId = split.GetPartition();
                if (!topic->Partitions.contains(splittedPartitionId)) {
                    errStr = TStringBuilder() << "Splitting partition does not exists: " << splittedPartitionId;
                    result->SetError(NKikimrScheme::StatusInvalidParameter, errStr);
                    return result;
                }
                if (!involvedPartitions.emplace(splittedPartitionId).second) {
                    errStr = TStringBuilder()
                             << "Partition can be involved only in one split/merge operation: " << splittedPartitionId;
                    result->SetError(NKikimrScheme::StatusInvalidParameter, errStr);
                    return result;
                }

                auto splittedPartition = topic->Partitions[splittedPartitionId];
                if (splittedPartition->Status != NKikimrPQ::ETopicPartitionStatus::Active) {
                    errStr = TStringBuilder() << "Invalid partition status: " << (ui32)splittedPartition->Status;
                    result->SetError(NKikimrScheme::StatusInvalidParameter, errStr);
                    return result;
                }

                auto splitBoundary = split.GetSplitBoundary();
                if (splitBoundary.empty()) {
                    errStr = TStringBuilder() << "Split boundary is empty";
                    result->SetError(NKikimrScheme::StatusInvalidParameter, errStr);
                    return result;
                }
                const auto keyRange = splittedPartition->KeyRange;
                if (keyRange) {
                    if (keyRange->FromBound && splitBoundary <= *keyRange->FromBound) {
                        errStr = TStringBuilder()
                                 << "Split boundary less or equals FromBound of partition: '" << Hex(splitBoundary)
                                 << "' <= '" << Hex(*keyRange->FromBound) << "'";
                        result->SetError(NKikimrScheme::StatusInvalidParameter, errStr);
                        return result;
                    }
                    if (keyRange->ToBound && splitBoundary >= *keyRange->ToBound) {
                        errStr = TStringBuilder()
                                 << "Split boundary greate or equals ToBound of partition: '" << Hex(splitBoundary)
                                 << "' >= '" << Hex(*keyRange->ToBound)
                                 << "' (FromBound is '" << Hex(keyRange->FromBound ? *keyRange->FromBound : TString{}) << "')";
                        result->SetError(NKikimrScheme::StatusInvalidParameter, errStr);
                        return result;
                    }
                }

                THashSet<ui32> parents{splittedPartitionId};

                TTopicTabletInfo::TKeyRange range;
                range.FromBound = keyRange ? keyRange->FromBound : Nothing();
                range.ToBound = splitBoundary;

                alterData->PartitionsToAdd.emplace(nextId++, ++nextGroupId, range, parents);

                range.FromBound = splitBoundary;
                range.ToBound = keyRange ? keyRange->ToBound : Nothing();

                alterData->PartitionsToAdd.emplace(nextId++, ++nextGroupId, range, parents);
            }
            for (const auto& merge : alter.GetMerge()) {
                alterData->TotalGroupCount += 1;
                --alterData->ActivePartitionCount;

                const auto partitionId = merge.GetPartition();
                if (!topic->Partitions.contains(partitionId)) {
                    errStr = TStringBuilder() << "Merging partition does not exists: " << partitionId;
                    result->SetError(NKikimrScheme::StatusInvalidParameter, errStr);
                    return result;
                }
                if (!involvedPartitions.emplace(partitionId).second) {
                    errStr = TStringBuilder()
                             << "Partition can be involved only in one split/merge operation: " << partitionId;
                    result->SetError(NKikimrScheme::StatusInvalidParameter, errStr);
                    return result;
                }
                auto partition = topic->Partitions[partitionId];
                if (partition->Status != NKikimrPQ::ETopicPartitionStatus::Active) {
                    errStr = TStringBuilder() << "Invalid partition status: " << (ui32)partition->Status;
                    result->SetError(NKikimrScheme::StatusInvalidParameter, errStr);
                    return result;
                }

                const auto adjacentPartitionId = merge.GetAdjacentPartition();
                if (!topic->Partitions.contains(adjacentPartitionId)) {
                    errStr = TStringBuilder() << "Invalid adjacent partition for merge: " << adjacentPartitionId;
                    result->SetError(NKikimrScheme::StatusInvalidParameter, errStr);
                    return result;
                }
                if (!involvedPartitions.emplace(adjacentPartitionId).second) {
                    errStr = TStringBuilder()
                             << "Partition can be involved only in one split/merge operation: " << adjacentPartitionId;
                    result->SetError(NKikimrScheme::StatusInvalidParameter, errStr);
                    return result;
                }
                auto adjacentPartition = topic->Partitions[adjacentPartitionId];
                if (adjacentPartition->Status != NKikimrPQ::ETopicPartitionStatus::Active) {
                    errStr = TStringBuilder()
                             << "Invalid adjacent partition status: " << (ui32)adjacentPartition->Status;
                    result->SetError(NKikimrScheme::StatusInvalidParameter, errStr);
                    return result;
                }

                NKikimr::NSchemeShard::TTopicTabletInfo::TTopicPartitionInfo *left, *right;
                if (partition->KeyRange->FromBound && adjacentPartition->KeyRange->ToBound &&
                    *partition->KeyRange->FromBound == *adjacentPartition->KeyRange->ToBound) {
                    left = adjacentPartition;
                    right = partition;
                } else if (partition->KeyRange->ToBound && adjacentPartition->KeyRange->FromBound &&
                           *partition->KeyRange->ToBound == *adjacentPartition->KeyRange->FromBound) {
                    left = partition;
                    right = adjacentPartition;
                } else {
                    errStr = TStringBuilder() << "You cannot merge non-contiguous partitions";
                    result->SetError(NKikimrScheme::StatusInvalidParameter, errStr);
                    return result;
                }

                THashSet<ui32> parents{partitionId, adjacentPartitionId};

                TTopicTabletInfo::TKeyRange range;
                range.FromBound = left->KeyRange->FromBound;
                range.ToBound = right->KeyRange->ToBound;

                TMaybe<TTopicTabletInfo::TKeyRange> rangem;
                if (range.FromBound || range.ToBound) {
                    rangem = range;
                }

                alterData->PartitionsToAdd.emplace(nextId++, ++nextGroupId, rangem, parents);
            }
        }

        if (!splitMergeEnabled) {
            ui32 diff = alterData->TotalGroupCount - topic->TotalGroupCount;

            for (ui32 i = 0; i < diff; ++i) {
                alterData->PartitionsToAdd.emplace(topic->NextPartitionId + i, topic->TotalGroupCount + 1 + i);
            }

            if (diff > 0) {
                alterData->TotalGroupCount = topic->TotalGroupCount + diff;
            }
        }

        alterData->TotalPartitionCount = topic->TotalPartitionCount + alterData->PartitionsToAdd.size();
        if (!splitMergeEnabled) {
            alterData->ActivePartitionCount = alterData->TotalPartitionCount;
        }

        alterData->NextPartitionId = topic->NextPartitionId;
        for (const auto& p : alterData->PartitionsToAdd) {
            if (p.GroupId == 0 || p.GroupId > alterData->TotalGroupCount) {
                errStr = TStringBuilder()
                         << "Invalid partition group id " << p.GroupId << " vs " << topic->TotalGroupCount;
                result->SetError(NKikimrScheme::StatusInvalidParameter, errStr);
                return result;
            }

            alterData->NextPartitionId = Max<ui32>(alterData->NextPartitionId, p.PartitionId + 1);
        }

        if (alterData->MaxPartsPerTablet < topic->MaxPartsPerTablet) {
            errStr =
                TStringBuilder() << "Invalid partition per tablet count specified: " << alterData->MaxPartsPerTablet
                                 << " vs " << topic->MaxPartsPerTablet << " (current)";
            result->SetError(NKikimrScheme::StatusInvalidParameter, errStr);
            return result;
        }

        ui64 shardsToCreate = ui64(!topic->HasBalancer());
        if (alterData->ExpectedShardCount() > topic->ShardCount()) {
            shardsToCreate += alterData->ExpectedShardCount() - topic->ShardCount();
        }
        ui64 partitionsToCreate = alterData->PartitionsToAdd.size();

        if (alterData->TotalGroupCount > TSchemeShard::MaxPQGroupPartitionsCount) {
            errStr = TStringBuilder()
                    << "Invalid partition count specified: " << alterData->TotalGroupCount
                    << " vs " << TSchemeShard::MaxPQGroupPartitionsCount;
            result->SetError(NKikimrScheme::StatusInvalidParameter, errStr);
            return result;
        }

        const auto& partConfig = newTabletConfig.GetPartitionConfig();

        if ((ui32)partConfig.GetWriteSpeedInBytesPerSecond() > TSchemeShard::MaxPQWriteSpeedPerPartition) {
            result->SetError(NKikimrScheme::StatusInvalidParameter, TStringBuilder() << "Invalid write speed"
                << ": specified: " << partConfig.GetWriteSpeedInBytesPerSecond() << "bps"
                << ", max: " << TSchemeShard::MaxPQWriteSpeedPerPartition << "bps");
            return result;
        }

        const PQGroupReserve reserve(newTabletConfig, alterData->ActivePartitionCount + involvedPartitions.size());
        const PQGroupReserve oldReserve(tabletConfig, topic->ActivePartitionCount);

        const ui64 storageToReserve = reserve.Storage > oldReserve.Storage ? reserve.Storage - oldReserve.Storage : 0;

        {
            TPath::TChecker checks = path.Check();
            checks
                .ShardsLimit(shardsToCreate)
                .PathShardsLimit(shardsToCreate)
                .PQPartitionsLimit(partitionsToCreate)
                .PQReservedStorageLimit(storageToReserve);

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        if (!context.SS->CheckApplyIf(Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, errStr);
            return result;
        }

        // This profile id is only used for pq read balancer tablet when
        // explicit channel profiles are specified. Read balancer tablet is
        // a tablet with local db which doesn't use extra channels in any way.
        const ui32 tabletProfileId = 0;
        TChannelsBindings tabletChannelsBinding;
        if (!context.SS->ResolvePqChannels(tabletProfileId, path.GetPathIdForDomain(), tabletChannelsBinding)) {
            result->SetError(NKikimrScheme::StatusInvalidParameter,
                             "Unable to construct channel binding for PQ with the storage pool");
            return result;
        }

        // This channel bindings are for PersQueue shards. They either use
        // explicit channel profiles, or reuse channel profile above.
        TChannelsBindings pqChannelsBinding;
        if (partConfig.ExplicitChannelProfilesSize() > 0) {
            // N.B. no validation necessary at this step
            const auto& ecps = partConfig.GetExplicitChannelProfiles();

            TVector<TStringBuf> partitionPoolKinds;
            partitionPoolKinds.reserve(ecps.size());
            for (const auto& ecp : ecps) {
                partitionPoolKinds.push_back(ecp.GetPoolKind());
            }

            const auto resolved = context.SS->ResolveChannelsByPoolKinds(
                partitionPoolKinds,
                path.GetPathIdForDomain(),
                pqChannelsBinding);
            if (!resolved) {
                result->SetError(NKikimrScheme::StatusInvalidParameter,
                                "Unable to construct channel binding for PersQueue with the storage pool");
                return result;
            }

            context.SS->SetPqChannelsParams(ecps, pqChannelsBinding);
        } else {
            pqChannelsBinding = tabletChannelsBinding;
        }

        topic->PrepareAlter(alterData);
        const TTxState& txState = PrepareChanges(OperationId, path, topic, shardsToCreate, tabletChannelsBinding,
                pqChannelsBinding, context, tabletConfig, newTabletConfig);

        context.OnComplete.ActivateTx(OperationId);
        context.SS->ClearDescribePathCaches(path.Base());
        context.OnComplete.PublishToSchemeBoard(OperationId, path.Base()->PathId);

        path.DomainInfo()->AddInternalShards(txState);
        path.DomainInfo()->IncPQPartitionsInside(partitionsToCreate);
        path.DomainInfo()->UpdatePQReservedStorage(oldReserve.Storage, reserve.Storage);
        path.Base()->IncShardsInside(shardsToCreate);

        context.SS->TabletCounters->Simple()[COUNTER_STREAM_RESERVED_THROUGHPUT].Add(reserve.Throughput);
        context.SS->TabletCounters->Simple()[COUNTER_STREAM_RESERVED_THROUGHPUT].Sub(oldReserve.Throughput);

        context.SS->TabletCounters->Simple()[COUNTER_STREAM_RESERVED_STORAGE].Add(reserve.Storage);
        context.SS->TabletCounters->Simple()[COUNTER_STREAM_RESERVED_STORAGE].Sub(oldReserve.Storage);

        context.SS->TabletCounters->Simple()[COUNTER_STREAM_SHARDS_COUNT].Add(partitionsToCreate);

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext&) override {
        Y_ABORT("no AbortPropose for TAlterPQ");
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TAlterPQ AbortUnsafe"
                         << ", opId: " << OperationId
                         << ", forceDropId: " << forceDropTxId
                         << ", at schemeshard: " << context.SS->TabletID());

        context.OnComplete.DoneOperation(OperationId);
    }
};

}

namespace NKikimr::NSchemeShard {

ISubOperation::TPtr CreateAlterPQ(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TAlterPQ>(id, tx);
}

ISubOperation::TPtr CreateAlterPQ(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    return MakeSubOperation<TAlterPQ>(id, state);
}

}

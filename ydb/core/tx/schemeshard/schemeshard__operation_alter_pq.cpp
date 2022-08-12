#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"

#include <ydb/core/base/subdomain.h>
#include <ydb/core/persqueue/config/config.h>
#include <ydb/core/mind/hive/hive.h>

namespace {

using namespace NKikimr;
using namespace NSchemeShard;

class TAlterPQ: public TSubOperation {
    const TOperationId OperationId;
    const TTxTransaction Transaction;
    TTxState::ETxState State = TTxState::Invalid;

    TTxState::ETxState NextState() {
        return TTxState::CreateParts;
    }

    TTxState::ETxState NextState(TTxState::ETxState state) {
        switch(state) {
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

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) {
        switch(state) {
        case TTxState::Waiting:
        case TTxState::CreateParts:
            return THolder(new TCreateParts(OperationId));
        case TTxState::ConfigureParts:
            return THolder(new NPQState::TConfigureParts(OperationId));
        case TTxState::Propose:
            return THolder(new NPQState::TPropose(OperationId));
        case TTxState::Done:
            return THolder(new TDone(OperationId));
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
    TAlterPQ(TOperationId id, const TTxTransaction& tx)
        : OperationId(id)
        , Transaction(tx)
    {
    }

    TAlterPQ(TOperationId id, TTxState::ETxState state)
        : OperationId(id)
        , State(state)
    {
        SetState(SelectStateFunc(state));
    }

    TPersQueueGroupInfo::TPtr ParseParams(
            TOperationContext& context,
            NKikimrPQ::TPQTabletConfig* tabletConfig,
            const NKikimrSchemeOp::TPersQueueGroupDescription& alter,
            TString& errStr)
    {
        TPersQueueGroupInfo::TPtr params = new TPersQueueGroupInfo();
        const bool hasKeySchema = tabletConfig->PartitionKeySchemaSize();

        if (alter.HasTotalGroupCount()) {
            if (hasKeySchema) {
                errStr = "Cannot change partition count. Use split/merge instead";
                return nullptr;
            }

            ui32 totalGroupCount = alter.GetTotalGroupCount();
            if (!totalGroupCount) {
                errStr = Sprintf("Invalid total groups count specified: %u", totalGroupCount);
                return nullptr;
            }
            params->TotalGroupCount = totalGroupCount;
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

            alterConfig.MutablePartitionKeySchema()->Swap(tabletConfig->MutablePartitionKeySchema());
            Y_PROTOBUF_SUPPRESS_NODISCARD alterConfig.SerializeToString(&params->TabletConfig);
            alterConfig.Swap(tabletConfig);
        }
        if (alter.PartitionsToDeleteSize()) {
            errStr = Sprintf("deletion of partitions is not supported yet");
            return nullptr;
        }
        if (alter.PartitionsToAddSize()) {
            if (hasKeySchema) {
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
                            << "providing partition " <<  p.GetPartitionId() << " serveral times in PartitionsToAdd is forbidden";
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
            TPersQueueGroupInfo::TPtr pqGroup,
            ui64 shardsToCreate,
            const TChannelsBindings& rbChannelsBinding,
            const TChannelsBindings& pqChannelsBinding,
            TOperationContext& context)
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

        for (auto& shard : pqGroup->Shards) {
            auto shardIdx = shard.first;
            for (const auto& pqInfo : shard.second->PQInfos) {
                context.SS->PersistPersQueue(db, item->PathId, shardIdx, pqInfo);
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
        Y_VERIFY(shardsToCreate == checkShardsToCreate);

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
            TPersQueueGroupInfo::TPtr pqGroup,
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
            pqGroup->Shards[idx] = new TPQShardInfo();
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

    void ReassignIds(TPersQueueGroupInfo::TPtr pqGroup) {
        Y_VERIFY(pqGroup->TotalPartitionCount >= pqGroup->TotalGroupCount);
        ui32 numOld = pqGroup->TotalPartitionCount;
        ui32 numNew = pqGroup->AlterData->PartitionsToAdd.size() + numOld;
        //ui32 maxPerPart = pqGroup->AlterData->MaxPartsPerTablet;
        ui32 average = numNew / pqGroup->Shards.size(); // TODO: not optimal
        if (numNew % pqGroup->Shards.size())
            ++average;
        ui64 alterVersion = pqGroup->AlterData->AlterVersion;

        auto it = pqGroup->Shards.begin();

        for (const auto& p : pqGroup->AlterData->PartitionsToAdd) {
            TPQShardInfo::TPersQueueInfo pqInfo;
            pqInfo.PqId = p.PartitionId;
            pqInfo.GroupId = p.GroupId;
            pqInfo.KeyRange = p.KeyRange;
            pqInfo.AlterVersion = alterVersion;
            while (it->second->PQInfos.size() >= average) {
                ++it;
            }
            it->second->PQInfos.push_back(pqInfo);
        }
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
            errStr = "Niether topic name nor pathId in Alter";
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
                TString explain = TStringBuilder() << "path fail checks"
                                                   << ", path: " << path.PathString();
                auto status = checks.GetStatus(&explain);
                result->SetError(status, explain);
                return result;
            }
        }

        TPersQueueGroupInfo::TPtr pqGroup = context.SS->PersQueueGroups.at(path.Base()->PathId);
        Y_VERIFY(pqGroup);

        if (pqGroup->AlterVersion == 0) {
            result->SetError(NKikimrScheme::StatusMultipleModifications, "PQGroup is not created yet");
            return result;
        }
        if (pqGroup->AlterData) {
            result->SetError(NKikimrScheme::StatusMultipleModifications, "There's another Alter in flight");
            return result;
        }

        NKikimrPQ::TPQTabletConfig tabletConfig, newTabletConfig;
        if (!pqGroup->TabletConfig.empty()) {
            bool parseOk = ParseFromStringNoSizeLimit(tabletConfig, pqGroup->TabletConfig);
            Y_VERIFY(parseOk, "Previously serialized pq tablet config cannot be parsed");
        }
        newTabletConfig = tabletConfig;

        TPersQueueGroupInfo::TPtr alterData = ParseParams(context, &newTabletConfig, alter, errStr);
        if (!alterData) {
            result->SetError(NKikimrScheme::StatusInvalidParameter, errStr);
            return result;
        }

        // do not change if not set
        if (!alterData->TotalGroupCount) {
            alterData->TotalGroupCount = pqGroup->TotalGroupCount;
        }

        if (!alterData->MaxPartsPerTablet) {
            alterData->MaxPartsPerTablet = pqGroup->MaxPartsPerTablet;
        }

        if (alterData->TotalGroupCount < pqGroup->TotalGroupCount) {
            errStr = TStringBuilder()
                    << "Invalid total groups count specified: " << alterData->TotalGroupCount
                    << " vs " << pqGroup->TotalGroupCount << " (current)";
            result->SetError(NKikimrScheme::StatusInvalidParameter, errStr);
            return result;
        }

        ui32 diff = alterData->TotalGroupCount - pqGroup->TotalGroupCount;

        for (ui32 i = 0; i < diff; ++i) {
            alterData->PartitionsToAdd.emplace(pqGroup->NextPartitionId + i, pqGroup->TotalGroupCount + 1 + i);
        }

        if (diff > 0) {
            alterData->TotalGroupCount = pqGroup->TotalGroupCount + diff;
        }

        alterData->TotalPartitionCount = pqGroup->TotalPartitionCount + alterData->PartitionsToAdd.size();
        alterData->NextPartitionId = pqGroup->NextPartitionId;
        for (const auto& p : alterData->PartitionsToAdd) {
            if (p.GroupId == 0 || p.GroupId > alterData->TotalGroupCount) {
                errStr = TStringBuilder()
                        << "Invalid partition group id " << p.GroupId
                        << " vs " << pqGroup->TotalGroupCount;
                result->SetError(NKikimrScheme::StatusInvalidParameter, errStr);
                return result;
            }

            alterData->NextPartitionId = Max<ui32>(alterData->NextPartitionId, p.PartitionId + 1);
        }

        if (alterData->MaxPartsPerTablet < pqGroup->MaxPartsPerTablet) {
            errStr = TStringBuilder()
                    << "Invalid partition per tablet count specified: " << alterData->MaxPartsPerTablet
                    << " vs " << pqGroup->MaxPartsPerTablet << " (current)";
            result->SetError(NKikimrScheme::StatusInvalidParameter, errStr);
            return result;
        }

        ui64 shardsToCreate = ui64(!pqGroup->HasBalancer());
        if (alterData->ExpectedShardCount() > pqGroup->ShardCount()) {
            shardsToCreate += alterData->ExpectedShardCount() - pqGroup->ShardCount();
        }
        ui64 partitionsToCreate = alterData->PartitionsToAdd.size();

        if (alterData->TotalGroupCount > TSchemeShard::MaxPQGroupPartitionsCount) {
            errStr = TStringBuilder()
                    << "Invalid partition count specified: " << alterData->TotalGroupCount
                    << " vs " << TSchemeShard::MaxPQGroupPartitionsCount;
            result->SetError(NKikimrScheme::StatusInvalidParameter, errStr);
            return result;
        }
        if ((ui32)newTabletConfig.GetPartitionConfig().GetWriteSpeedInBytesPerSecond() > TSchemeShard::MaxPQWriteSpeedPerPartition) {
            errStr = TStringBuilder()
                    << "Invalid write speed per second in partition specified: " << newTabletConfig.GetPartitionConfig().GetWriteSpeedInBytesPerSecond()
                    << " vs " << TSchemeShard::MaxPQWriteSpeedPerPartition;
            result->SetError(NKikimrScheme::StatusInvalidParameter, errStr);
            return result;
        }

        if ((ui32)newTabletConfig.GetPartitionConfig().GetLifetimeSeconds() > TSchemeShard::MaxPQLifetimeSeconds) {
            errStr = TStringBuilder()
                    << "Invalid retention period specified: " << newTabletConfig.GetPartitionConfig().GetLifetimeSeconds()
                    << " vs " << TSchemeShard::MaxPQLifetimeSeconds;
            result->SetError(NKikimrScheme::StatusInvalidParameter, errStr);
            return result;
        }

        auto getStorageLimit = [](auto &config, ui64 throughput) {
            if (config.GetPartitionConfig().HasStorageLimitBytes()) {
                return config.GetPartitionConfig().GetStorageLimitBytes();
            } else {
                return throughput * config.GetPartitionConfig().GetLifetimeSeconds();
            }
        };

        const ui64 throughput = ((ui64)(newTabletConfig.GetPartitionConfig().GetWriteSpeedInBytesPerSecond())) *
                             (alterData->TotalGroupCount);
        const ui64 oldThroughput = ((ui64)(tabletConfig.GetPartitionConfig().GetWriteSpeedInBytesPerSecond())) *
                             (pqGroup->TotalGroupCount);

        const ui64 storage = getStorageLimit(newTabletConfig, throughput);
        const ui64 oldStorage = getStorageLimit(tabletConfig, oldThroughput);

        const ui64 storageToReserve = storage > oldStorage ? storage - oldStorage : 0;

        {
            TPath::TChecker checks = path.Check();
            checks
                .ShardsLimit(shardsToCreate)
                .PathShardsLimit(shardsToCreate)
                .PQPartitionsLimit(partitionsToCreate)
                .PQReservedStorageLimit(storageToReserve);

            if (!checks) {
                TString explain = TStringBuilder() << "path fail checks"
                                                   << ", path: " << path.PathString();
                auto status = checks.GetStatus(&explain);
                result->SetError(status, explain);
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
        if (!context.SS->CheckInFlightLimit(TTxState::TxAlterPQGroup, errStr)) {
            result->SetError(NKikimrScheme::StatusResourceExhausted, errStr);
            return result;
        }

        // This channel bindings are for PersQueue shards. They either use
        // explicit channel profiles, or reuse channel profile above.
        const auto& partConfig = newTabletConfig.GetPartitionConfig();
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

        pqGroup->PrepareAlter(alterData);
        const TTxState& txState = PrepareChanges(OperationId, path, pqGroup, shardsToCreate, tabletChannelsBinding, pqChannelsBinding, context);

        context.OnComplete.ActivateTx(OperationId);
        context.SS->ClearDescribePathCaches(path.Base());
        context.OnComplete.PublishToSchemeBoard(OperationId, path.Base()->PathId);

        path.DomainInfo()->AddInternalShards(txState);
        path.DomainInfo()->IncPQPartitionsInside(partitionsToCreate);
        path.DomainInfo()->UpdatePQReservedStorage(oldStorage, storage);


        context.SS->TabletCounters->Simple()[COUNTER_STREAM_RESERVED_THROUGHPUT].Add(throughput);
        context.SS->TabletCounters->Simple()[COUNTER_STREAM_RESERVED_THROUGHPUT].Sub(oldThroughput);

        context.SS->TabletCounters->Simple()[COUNTER_STREAM_RESERVED_STORAGE].Add(storage);
        context.SS->TabletCounters->Simple()[COUNTER_STREAM_RESERVED_STORAGE].Sub(oldStorage);

        context.SS->TabletCounters->Simple()[COUNTER_STREAM_SHARDS_COUNT].Add(partitionsToCreate);

        path.Base()->IncShardsInside(shardsToCreate);
        State = NextState();
        SetState(SelectStateFunc(State));
        return result;
    }

    void AbortPropose(TOperationContext&) override {
        Y_FAIL("no AbortPropose for TAlterPQ");
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

namespace NKikimr {
namespace NSchemeShard {

ISubOperationBase::TPtr CreateAlterPQ(TOperationId id, const TTxTransaction& tx) {
    return new TAlterPQ(id, tx);
}

ISubOperationBase::TPtr CreateAlterPQ(TOperationId id, TTxState::ETxState state) {
    Y_VERIFY(state != TTxState::Invalid);
    return new TAlterPQ(id, state);
}

}
}

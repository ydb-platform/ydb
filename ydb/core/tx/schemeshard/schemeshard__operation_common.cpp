#include "schemeshard__operation_common.h"

namespace NKikimr {
namespace NSchemeShard {

THolder<TEvHive::TEvCreateTablet> CreateEvCreateTablet(TPathElement::TPtr targetPath, TShardIdx shardIdx, TOperationContext& context)
{
    auto tablePartitionConfig = context.SS->GetTablePartitionConfigWithAlterData(targetPath->PathId);
    const auto& shard = context.SS->ShardInfos[shardIdx];

    if (shard.TabletType == ETabletType::BlockStorePartition ||
        shard.TabletType == ETabletType::BlockStorePartition2)
    {
        auto it = context.SS->BlockStoreVolumes.FindPtr(targetPath->PathId);
        Y_ABORT_UNLESS(it, "Missing BlockStoreVolume while creating BlockStorePartition tablet");
        auto volume = *it;
        /*const auto* volumeConfig = &volume->VolumeConfig;
        if (volume->AlterData) {
            volumeConfig = &volume->AlterData->VolumeConfig;
        }*/
    }

    THolder<TEvHive::TEvCreateTablet> ev = MakeHolder<TEvHive::TEvCreateTablet>(ui64(shardIdx.GetOwnerId()), ui64(shardIdx.GetLocalId()), shard.TabletType, shard.BindedChannels);

    TPathId domainId = context.SS->ResolvePathIdForDomain(targetPath);

    TPathElement::TPtr domainEl = context.SS->PathsById.at(domainId);
    auto objectDomain = ev->Record.MutableObjectDomain();
    if (domainEl->IsRoot()) {
        objectDomain->SetSchemeShard(context.SS->ParentDomainId.OwnerId);
        objectDomain->SetPathId(context.SS->ParentDomainId.LocalPathId);
    } else {
        objectDomain->SetSchemeShard(domainId.OwnerId);
        objectDomain->SetPathId(domainId.LocalPathId);
    }

    Y_ABORT_UNLESS(context.SS->SubDomains.contains(domainId));
    TSubDomainInfo::TPtr subDomain = context.SS->SubDomains.at(domainId);

    TPathId resourcesDomainId;
    if (subDomain->GetResourcesDomainId()) {
        resourcesDomainId = subDomain->GetResourcesDomainId();
    } else if (subDomain->GetAlter() && subDomain->GetAlter()->GetResourcesDomainId()) {
        resourcesDomainId = subDomain->GetAlter()->GetResourcesDomainId();
    } else {
        Y_ABORT("Cannot retrieve resources domain id");
    }

    auto allowedDomain = ev->Record.AddAllowedDomains();
    allowedDomain->SetSchemeShard(resourcesDomainId.OwnerId);
    allowedDomain->SetPathId(resourcesDomainId.LocalPathId);

    if (tablePartitionConfig) {
        if (tablePartitionConfig->FollowerGroupsSize()) {
            ev->Record.MutableFollowerGroups()->CopyFrom(tablePartitionConfig->GetFollowerGroups());
        } else {
            if (tablePartitionConfig->HasAllowFollowerPromotion()) {
                ev->Record.SetAllowFollowerPromotion(tablePartitionConfig->GetAllowFollowerPromotion());
            }

            if (tablePartitionConfig->HasCrossDataCenterFollowerCount()) {
                ev->Record.SetCrossDataCenterFollowerCount(tablePartitionConfig->GetCrossDataCenterFollowerCount());
            } else if (tablePartitionConfig->HasFollowerCount()) {
                ev->Record.SetFollowerCount(tablePartitionConfig->GetFollowerCount());
            }
        }
    }

    if (shard.TabletType == ETabletType::BlockStorePartition   ||
        shard.TabletType == ETabletType::BlockStorePartition2 ||
        shard.TabletType == ETabletType::RTMRPartition) {
        // Partitions should never be booted by local
        ev->Record.SetTabletBootMode(NKikimrHive::TABLET_BOOT_MODE_EXTERNAL);
    }

    ev->Record.SetObjectId(targetPath->PathId.LocalPathId);

    if (shard.TabletID) {
        ev->Record.SetTabletID(ui64(shard.TabletID));
    }

    return ev;
}


namespace
{

template <typename T, typename TFuncCheck, typename TFuncToString>
bool CollectProposeTxResults(
        const T& ev,
        const NKikimr::NSchemeShard::TOperationId& operationId,
        NKikimr::NSchemeShard::TOperationContext& context,
        TFuncCheck checkPrepared,
        TFuncToString toString)
{
    auto ssId = context.SS->SelfTabletId();

    LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "TEvProposeTransactionResult at tablet: " << ssId);

    auto tabletId = TTabletId(ev->Get()->Record.GetOrigin());
    auto shardMinStep = TStepId(ev->Get()->Record.GetMinStep());
    auto status = ev->Get()->Record.GetStatus();

    // Ignore COMPLETE
    if (!checkPrepared(status)) {
        LOG_ERROR_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "Ignore TEvProposeTransactionResult as not prepared"
                        << ", shard: " << tabletId
                        << ", operationId: " << operationId
                        << ", result status: " << toString(status)
                        << ", at schemeshard: " << ssId);
        return false;
    }

    NIceDb::TNiceDb db(context.GetDB());

    TTxState& txState = *context.SS->FindTx(operationId);

    if (txState.MinStep < shardMinStep) {
        txState.MinStep = shardMinStep;
        context.SS->PersistTxMinStep(db, operationId, txState.MinStep);
    }

    auto shardIdx = context.SS->MustGetShardIdx(tabletId);

    // Ignore if this is a repeated message
    if (!txState.ShardsInProgress.contains(shardIdx)) {
        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "Ignore TEvProposeTransactionResult as duplicate"
                        << ", shard: " << tabletId
                        << ", shardIdx: " << shardIdx
                        << ", operationId: " << operationId
                        << ", at schemeshard: " << ssId);
        return false;
    }

    txState.ShardsInProgress.erase(shardIdx);
    context.OnComplete.UnbindMsgFromPipe(operationId, tabletId, shardIdx);

    LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "CollectProposeTransactionResults accept TEvProposeTransactionResult"
                    << ", shard: " << tabletId
                    << ", shardIdx: " << shardIdx
                    << ", operationId: " << operationId
                    << ", left await: " << txState.ShardsInProgress.size()
                    << ", at schemeshard: " << ssId);

    if (txState.ShardsInProgress.empty()) {
        // All datashards have replied so we can proceed with this transaction
        context.SS->ChangeTxState(db, operationId, TTxState::Propose);
        return true;
    }

    return false;
}

} // anonymous namespace

bool NTableState::CollectProposeTransactionResults(
        const NKikimr::NSchemeShard::TOperationId &operationId,
        const TEvDataShard::TEvProposeTransactionResult::TPtr &ev,
        NKikimr::NSchemeShard::TOperationContext &context)
{
    auto prepared = [](NKikimrTxDataShard::TEvProposeTransactionResult::EStatus status) -> bool {
        return status == NKikimrTxDataShard::TEvProposeTransactionResult::PREPARED;
    };

    auto toString = [](NKikimrTxDataShard::TEvProposeTransactionResult::EStatus status) -> TString {
        return NKikimrTxDataShard::TEvProposeTransactionResult_EStatus_Name(status);
    };

    return CollectProposeTxResults(ev, operationId, context, prepared, toString);
}

bool NTableState::CollectProposeTransactionResults(
        const NKikimr::NSchemeShard::TOperationId& operationId,
        const TEvColumnShard::TEvProposeTransactionResult::TPtr& ev,
        NKikimr::NSchemeShard::TOperationContext& context)
{
    auto prepared = [](NKikimrTxColumnShard::EResultStatus status) -> bool {
        return status == NKikimrTxColumnShard::EResultStatus::PREPARED;
    };

    auto toString = [](NKikimrTxColumnShard::EResultStatus status) -> TString {
        return NKikimrTxColumnShard::EResultStatus_Name(status);
    };

    return CollectProposeTxResults(ev, operationId, context, prepared, toString);
}

bool NTableState::CollectSchemaChanged(
        const TOperationId& operationId,
        const TEvDataShard::TEvSchemaChanged::TPtr& ev,
        TOperationContext& context)
{
    auto ssId = context.SS->SelfTabletId();

    const auto& evRecord = ev->Get()->Record;
    const TActorId ackTo = ev->Get()->GetSource();

    auto datashardId = TTabletId(evRecord.GetOrigin());

    Y_ABORT_UNLESS(context.SS->FindTx(operationId));
    TTxState& txState = *context.SS->FindTx(operationId);

    auto shardIdx = context.SS->MustGetShardIdx(datashardId);
    Y_ABORT_UNLESS(context.SS->ShardInfos.contains(shardIdx));

    // Save this notification if was received earlier than the Tx switched to ProposedWaitParts state
    ui32 generation = evRecord.GetGeneration();
    auto pTablet = txState.SchemeChangeNotificationReceived.FindPtr(shardIdx);
    if (pTablet && pTablet->second >= generation) {
        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "CollectSchemaChanged Ignore TEvDataShard::TEvSchemaChanged as outdated"
                        << ", operationId: " << operationId
                        << ", shardIdx: " << shardIdx
                        << ", datashard " << datashardId
                        << ", event generation: " << generation
                        << ", known generation: " << pTablet->second
                        << ", at schemeshard: " << ssId);
        return false;
    }

    txState.SchemeChangeNotificationReceived[shardIdx] = std::make_pair(ackTo, generation);


    if (evRecord.HasOpResult()) {
        // TODO: remove TxBackup handling
        Y_DEBUG_ABORT_UNLESS(txState.TxType == TTxState::TxBackup || txState.TxType == TTxState::TxRestore);
    }

    if (!txState.ReadyForNotifications) {
        return false;
    }
    if (txState.TxType == TTxState::TxBackup || txState.TxType == TTxState::TxRestore) {
        Y_ABORT_UNLESS(txState.State == TTxState::ProposedWaitParts || txState.State == TTxState::Aborting);
    } else {
        Y_ABORT_UNLESS(txState.State == TTxState::ProposedWaitParts);
    }

    txState.ShardsInProgress.erase(shardIdx);

    LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "CollectSchemaChanged accept TEvDataShard::TEvSchemaChanged"
                    << ", operationId: " << operationId
                    << ", shardIdx: " << shardIdx
                    << ", datashard: " << datashardId
                    << ", left await: " << txState.ShardsInProgress.size()
                    << ", txState.State: " << TTxState::StateName(txState.State)
                    << ", txState.ReadyForNotifications: " << txState.ReadyForNotifications
                    << ", at schemeshard: " << ssId);

    if (txState.ShardsInProgress.empty()) {
        AckAllSchemaChanges(operationId, txState, context);

        NIceDb::TNiceDb db(context.GetDB());
        context.SS->ChangeTxState(db, operationId, TTxState::Done);
        return true;
    }

    return false;
}

void NTableState::AckAllSchemaChanges(const TOperationId &operationId, TTxState &txState, TOperationContext &context) {
    TTabletId ssId = context.SS->SelfTabletId();

    LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "all shard schema changes has been received"
                    << ", operationId: " << operationId
                    << ", at schemeshard: " << ssId);

    // Ack to all participating datashards
    for (const auto& items : txState.SchemeChangeNotificationReceived) {
        const TActorId ackTo = items.second.first;
        const auto shardIdx = items.first;
        const auto tabletId = context.SS->ShardInfos[shardIdx].TabletID;

        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "send schema changes ack message"
                        << ", operation: " << operationId
                        << ", datashard: " << tabletId
                        << ", at schemeshard: " << ssId);

        auto event = MakeHolder<TEvDataShard::TEvSchemaChangedResult>();
        event->Record.SetTxId(ui64(operationId.GetTxId()));

        context.OnComplete.Send(ackTo, std::move(event), ui64(shardIdx.GetLocalId()));
    }
}

bool NTableState::CheckPartitioningChangedForTableModification(TTxState &txState, TOperationContext &context) {
    Y_ABORT_UNLESS(context.SS->Tables.contains(txState.TargetPathId));
    TTableInfo::TPtr table = context.SS->Tables.at(txState.TargetPathId);

    THashSet<TShardIdx> shardIdxsLeft;
    for (auto& shard : table->GetPartitions()) {
        shardIdxsLeft.insert(shard.ShardIdx);
    }

    for (auto& shardOp : txState.Shards) {
        // Is this shard still on the list of partitions?
        if (shardIdxsLeft.erase(shardOp.Idx) == 0)
            return true;
    }

    // Any new partitions?
    return !shardIdxsLeft.empty();
}

void NTableState::UpdatePartitioningForTableModification(TOperationId operationId, TTxState &txState, TOperationContext &context) {
    Y_ABORT_UNLESS(!txState.TxShardsListFinalized, "Rebuilding the list of shards must not happen twice");

    NIceDb::TNiceDb db(context.GetDB());

    THashSet<TShardIdx> prevAlterCreateParts;

    // Delete old tx shards from db
    for (const auto& shard : txState.Shards) {
        if (txState.TxType == TTxState::TxAlterTable && shard.Operation == TTxState::CreateParts) {
            // Remember alter table parts that had CreateParts set (possible channel bindings change)
            prevAlterCreateParts.insert(shard.Idx);
        }
        context.SS->PersistRemoveTxShard(db, operationId, shard.Idx);
    }
    txState.Shards.clear();
    Y_ABORT_UNLESS(txState.ShardsInProgress.empty());

    Y_ABORT_UNLESS(context.SS->Tables.contains(txState.TargetPathId));
    TTableInfo::TPtr table = context.SS->Tables.at(txState.TargetPathId);
    TTxState::ETxState commonShardOp = TTxState::CreateParts;

    if (txState.TxType == TTxState::TxAlterTable) {
        commonShardOp = table->NeedRecreateParts()
                    ? TTxState::CreateParts
                    : TTxState::ConfigureParts;
    } else if (txState.TxType == TTxState::TxDropTable) {
        commonShardOp = TTxState::DropParts;
    } else if (txState.TxType == TTxState::TxBackup) {
        commonShardOp = TTxState::ConfigureParts;
    } else if (txState.TxType == TTxState::TxRestore) {
        commonShardOp = TTxState::ConfigureParts;
    } else if (txState.TxType == TTxState::TxInitializeBuildIndex) {
        commonShardOp = TTxState::ConfigureParts;
    } else if (txState.TxType == TTxState::TxFinalizeBuildIndex) {
        commonShardOp = TTxState::ConfigureParts;
    } else if (txState.TxType == TTxState::TxDropTableIndexAtMainTable) {
        commonShardOp = TTxState::ConfigureParts;
    } else if (txState.TxType == TTxState::TxUpdateMainTableOnIndexMove) {
        commonShardOp = TTxState::ConfigureParts;
    } else if (txState.TxType == TTxState::TxCreateCdcStreamAtTable) {
        commonShardOp = TTxState::ConfigureParts;
    } else if (txState.TxType == TTxState::TxCreateCdcStreamAtTableWithInitialScan) {
        commonShardOp = TTxState::ConfigureParts;
    } else if (txState.TxType == TTxState::TxAlterCdcStreamAtTable) {
        commonShardOp = TTxState::ConfigureParts;
    } else if (txState.TxType == TTxState::TxAlterCdcStreamAtTableDropSnapshot) {
        commonShardOp = TTxState::ConfigureParts;
    } else if (txState.TxType == TTxState::TxDropCdcStreamAtTable) {
        commonShardOp = TTxState::ConfigureParts;
    } else if (txState.TxType == TTxState::TxDropCdcStreamAtTableDropSnapshot) {
        commonShardOp = TTxState::ConfigureParts;
    } else {
        Y_ABORT("UNREACHABLE");
    }

    TBindingsRoomsChanges bindingChanges;

    bool tryApplyBindingChanges = (
        txState.TxType == TTxState::TxAlterTable &&
        table->AlterData->IsFullPartitionConfig() &&
        context.SS->IsStorageConfigLogic(table));

    if (tryApplyBindingChanges) {
        TString errStr;
        auto dstPath = context.SS->PathsById.at(txState.TargetPathId);
        bool isOk = context.SS->GetBindingsRoomsChanges(
                dstPath->DomainPathId,
                table->GetPartitions(),
                table->AlterData->PartitionConfigFull(),
                bindingChanges,
                errStr);
        if (!isOk) {
            Y_ABORT("Unexpected failure to rebind column families to storage pools: %s", errStr.c_str());
        }
    }

    // Fill new list of tx shards
    for (auto& shard : table->GetPartitions()) {
        auto shardIdx = shard.ShardIdx;
        Y_ABORT_UNLESS(context.SS->ShardInfos.contains(shardIdx));
        auto& shardInfo = context.SS->ShardInfos.at(shardIdx);

        auto shardOp = commonShardOp;
        if (txState.TxType == TTxState::TxAlterTable) {
            if (tryApplyBindingChanges && shardInfo.BindedChannels) {
                auto it = bindingChanges.find(GetPoolsMapping(shardInfo.BindedChannels));
                if (it != bindingChanges.end()) {
                    if (it->second.ChannelsBindingsUpdated) {
                        // We must recreate this shard to apply new channel bindings
                        shardOp = TTxState::CreateParts;
                        shardInfo.BindedChannels = it->second.ChannelsBindings;
                        context.SS->PersistChannelsBinding(db, shardIdx, shardInfo.BindedChannels);
                    }

                    table->PerShardPartitionConfig[shardIdx].CopyFrom(it->second.PerShardConfig);
                    context.SS->PersistAddTableShardPartitionConfig(db, shardIdx, it->second.PerShardConfig);
                }
            }

            if (prevAlterCreateParts.contains(shardIdx)) {
                // Make sure shards that don't have channel changes this time
                // still go through their CreateParts round to apply any
                // previously changed ChannelBindings
                shardOp = TTxState::CreateParts;
            }
        }

        txState.Shards.emplace_back(shardIdx, ETabletType::DataShard, shardOp);

        shardInfo.CurrentTxId = operationId.GetTxId();
        context.SS->PersistShardTx(db, shardIdx, operationId.GetTxId());
        context.SS->PersistUpdateTxShard(db, operationId, shardIdx, shardOp);
    }
    txState.TxShardsListFinalized = true;
}

bool NTableState::SourceTablePartitioningChangedForCopyTable(const TTxState &txState, TOperationContext &context) {
    Y_ABORT_UNLESS(txState.SourcePathId != InvalidPathId);
    Y_ABORT_UNLESS(txState.TargetPathId != InvalidPathId);
    const TTableInfo::TPtr srcTableInfo = *context.SS->Tables.FindPtr(txState.SourcePathId);

    THashSet<TShardIdx> srcShardIdxsLeft;
    for (const auto& p : srcTableInfo->GetPartitions()) {
        srcShardIdxsLeft.insert(p.ShardIdx);
    }

    for (const auto& shard : txState.Shards) {
        // Skip shards of the new table
        if (shard.Operation == TTxState::CreateParts)
            continue;

        Y_ABORT_UNLESS(shard.Operation == TTxState::ConfigureParts);
        // Is this shard still present in src table partitioning?
        if (srcShardIdxsLeft.erase(shard.Idx) == 0)
            return true;
    }

    // Any new shards were added to src table?
    return !srcShardIdxsLeft.empty();
}

void NTableState::UpdatePartitioningForCopyTable(TOperationId operationId, TTxState &txState, TOperationContext &context) {
    Y_ABORT_UNLESS(!txState.TxShardsListFinalized, "CopyTable can adjust partitioning only once");

    // Source table must not be altered or drop while we are performing copying. So we put it into a special state.
    Y_ABORT_UNLESS(context.SS->PathsById.contains(txState.SourcePathId));
    Y_ABORT_UNLESS(context.SS->PathsById.at(txState.SourcePathId)->PathState == TPathElement::EPathState::EPathStateCopying);
    Y_ABORT_UNLESS(context.SS->PathsById.contains(txState.TargetPathId));
    auto dstPath = context.SS->PathsById.at(txState.TargetPathId);
    auto domainInfo = context.SS->SubDomains.at(dstPath->DomainPathId);

    auto srcTableInfo = context.SS->Tables.at(txState.SourcePathId);
    auto dstTableInfo = context.SS->Tables.at(txState.TargetPathId);

    NIceDb::TNiceDb db(context.GetDB());

    // Erase previous partitioning as we are going to generate new one
    context.SS->PersistTablePartitioningDeletion(db, txState.TargetPathId, dstTableInfo);

    // Remove old shardIdx info and old txShards
    for (const auto& shard : txState.Shards) {
        context.SS->PersistRemoveTxShard(db, operationId, shard.Idx);
        if (shard.Operation == TTxState::CreateParts) {
            Y_ABORT_UNLESS(context.SS->ShardInfos.contains(shard.Idx));
            Y_ABORT_UNLESS(context.SS->ShardInfos[shard.Idx].TabletID == InvalidTabletId, "Dst shard must not exist yet");
            auto pathId = context.SS->ShardInfos[shard.Idx].PathId;
            dstTableInfo->PerShardPartitionConfig.erase(shard.Idx);
            context.SS->PersistShardDeleted(db, shard.Idx, context.SS->ShardInfos[shard.Idx].BindedChannels);
            context.SS->ShardInfos.erase(shard.Idx);
            domainInfo->RemoveInternalShard(shard.Idx);
            context.SS->DecrementPathDbRefCount(pathId, "remove shard from txState");
            context.SS->ShardRemoved(shard.Idx);
        }
    }
    txState.Shards.clear();

    TChannelsBindings channelsBinding;

    bool storePerShardConfig = false;
    NKikimrSchemeOp::TPartitionConfig perShardConfig;

    if (context.SS->IsStorageConfigLogic(dstTableInfo)) {
        TVector<TStorageRoom> storageRooms;
        storageRooms.emplace_back(0);
        THashMap<ui32, ui32> familyRooms;

        TString errStr;
        bool isOk = context.SS->GetBindingsRooms(dstPath->DomainPathId, dstTableInfo->PartitionConfig(), storageRooms, familyRooms, channelsBinding, errStr);
        if (!isOk) {
            errStr = TString("database must have required storage pools to create tablet with storage config, details: ") + errStr;
            Y_ABORT("%s", errStr.c_str());
        }

        storePerShardConfig = true;
        for (const auto& room : storageRooms) {
            perShardConfig.AddStorageRooms()->CopyFrom(room);
        }
        for (const auto& familyRoom : familyRooms) {
            auto* protoFamily = perShardConfig.AddColumnFamilies();
            protoFamily->SetId(familyRoom.first);
            protoFamily->SetRoom(familyRoom.second);
        }
    } else if (context.SS->IsCompatibleChannelProfileLogic(dstPath->DomainPathId, dstTableInfo)) {
        TString errStr;
        bool isOk = context.SS->GetChannelsBindings(dstPath->DomainPathId, dstTableInfo, channelsBinding, errStr);
        if (!isOk) {
            errStr = TString("database must have required storage pools to create tablet with channel profile, details: ") + errStr;
            Y_ABORT("%s", errStr.c_str());
        }
    }

    TShardInfo datashardInfo = TShardInfo::DataShardInfo(operationId.GetTxId(), txState.TargetPathId);
    datashardInfo.BindedChannels = channelsBinding;

    context.SS->SetPartitioning(txState.TargetPathId, dstTableInfo,
        ApplyPartitioningCopyTable(datashardInfo, srcTableInfo, txState, context.SS));

    ui32 newShardCout = dstTableInfo->GetPartitions().size();

    dstPath->SetShardsInside(newShardCout);
    domainInfo->AddInternalShards(txState);

    context.SS->PersistTable(db, txState.TargetPathId);
    context.SS->PersistTxState(db, operationId);

    context.SS->PersistUpdateNextPathId(db);
    context.SS->PersistUpdateNextShardIdx(db);
    // Persist new shards info
    for (const auto& shard : dstTableInfo->GetPartitions()) {
        Y_ABORT_UNLESS(context.SS->ShardInfos.contains(shard.ShardIdx), "shard info is set before");
        const auto tabletType = context.SS->ShardInfos[shard.ShardIdx].TabletType;
        context.SS->PersistShardMapping(db, shard.ShardIdx, InvalidTabletId, txState.TargetPathId, operationId.GetTxId(), tabletType);
        context.SS->PersistChannelsBinding(db, shard.ShardIdx, channelsBinding);

        if (storePerShardConfig) {
            dstTableInfo->PerShardPartitionConfig[shard.ShardIdx].CopyFrom(perShardConfig);
            context.SS->PersistAddTableShardPartitionConfig(db, shard.ShardIdx, perShardConfig);
        }
    }

    txState.TxShardsListFinalized = true;
}

TVector<TTableShardInfo> NTableState::ApplyPartitioningCopyTable(const TShardInfo &templateDatashardInfo, TTableInfo::TPtr srcTableInfo, TTxState &txState, TSchemeShard *ss) {
    TVector<TTableShardInfo> dstPartitions = srcTableInfo->GetPartitions();

    // Source table must not be altered or drop while we are performing copying. So we put it into a special state.
    ui64 count = dstPartitions.size();
    txState.Shards.reserve(count*2);
    for (ui64 i = 0; i < count; ++i) {
        // Source shards need to get "Send parts" transaction
        auto srcShardIdx = srcTableInfo->GetPartitions()[i].ShardIdx;
        Y_ABORT_UNLESS(ss->ShardInfos.contains(srcShardIdx), "Source table shard not found");
        auto srcTabletId = ss->ShardInfos[srcShardIdx].TabletID;
        Y_ABORT_UNLESS(srcTabletId != InvalidTabletId);
        txState.Shards.emplace_back(srcShardIdx, ETabletType::DataShard, TTxState::ConfigureParts);
        // Destination shards need to be created, configured and then they will receive parts
        auto idx = ss->RegisterShardInfo(templateDatashardInfo);
        txState.Shards.emplace_back(idx, ETabletType::DataShard, TTxState::CreateParts);
        // Properly set new shard idx
        dstPartitions[i].ShardIdx = idx;
        // clear lag to avoid counter underflow
        dstPartitions[i].LastCondEraseLag.Clear();
    }

    return dstPartitions;
}

namespace NPQState {

bool CollectProposeTransactionResults(const TOperationId& operationId,
                                      const TEvPersQueue::TEvProposeTransactionResult::TPtr& ev,
                                      TOperationContext& context)
{
    auto prepared = [](NKikimrPQ::TEvProposeTransactionResult::EStatus status) -> bool {
        return status == NKikimrPQ::TEvProposeTransactionResult::PREPARED;
    };

    auto toString = [](NKikimrPQ::TEvProposeTransactionResult::EStatus status) -> TString {
        return NKikimrPQ::TEvProposeTransactionResult_EStatus_Name(status);
    };

    return CollectProposeTxResults(ev, operationId, context, prepared, toString);
}

bool CollectPQConfigChanged(const TOperationId& operationId,
                            const TEvPersQueue::TEvProposeTransactionResult::TPtr& ev,
                            TOperationContext& context)
{
    Y_ABORT_UNLESS(context.SS->FindTx(operationId));
    TTxState& txState = *context.SS->FindTx(operationId);

    const auto& evRecord = ev->Get()->Record;
    if (evRecord.GetStatus() == NKikimrPQ::TEvProposeTransactionResult::COMPLETE) {
        const auto ssId = context.SS->SelfTabletId();
        const TTabletId shardId(evRecord.GetOrigin());

        const auto shardIdx = context.SS->MustGetShardIdx(shardId);
        Y_ABORT_UNLESS(context.SS->ShardInfos.contains(shardIdx));

        Y_ABORT_UNLESS(txState.State == TTxState::Propose);

        txState.ShardsInProgress.erase(shardIdx);

        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "CollectPQConfigChanged accept TEvPersQueue::TEvProposeTransactionResult"
                    << ", operationId: " << operationId
                    << ", shardIdx: " << shardIdx
                    << ", shard: " << shardId
                    << ", left await: " << txState.ShardsInProgress.size()
                    << ", txState.State: " << TTxState::StateName(txState.State)
                    << ", txState.ReadyForNotifications: " << txState.ReadyForNotifications
                    << ", at schemeshard: " << ssId);
    }

    return txState.ShardsInProgress.empty();
}

bool CollectPQConfigChanged(const TOperationId& operationId,
                            const TEvPersQueue::TEvProposeTransactionAttachResult::TPtr& ev,
                            TOperationContext& context)
{
    Y_ABORT_UNLESS(context.SS->FindTx(operationId));
    TTxState& txState = *context.SS->FindTx(operationId);

    //
    // The PQ tablet can perform a transaction and send a TEvProposeTransactionResult(COMPLETE) response.
    // The SchemeShard tablet can restart at this point. After restarting at the TPropose step, it will
    // send the TEvProposeTransactionAttach message to the PQ tablets. If the NODATA status is specified in
    // the response TEvProposeTransactionAttachResult, then the PQ tablet has already completed the transaction.
    // Otherwise, she continues to execute the transaction
    //

    const auto& evRecord = ev->Get()->Record;
    if (evRecord.GetStatus() != NKikimrProto::NODATA) {
        //
        // If the PQ tablet returned something other than NODATA, then it continues to execute the transaction
        //
        return txState.ShardsInProgress.empty();
    }

    //
    // Otherwise, she has already completed the transaction and has forgotten about it. Then we can
    // remove PQ tablet from the list of shards
    //

    const auto ssId = context.SS->SelfTabletId();
    const TTabletId shardId(evRecord.GetTabletId());

    const auto shardIdx = context.SS->MustGetShardIdx(shardId);
    Y_ABORT_UNLESS(context.SS->ShardInfos.contains(shardIdx));

    Y_ABORT_UNLESS(txState.State == TTxState::Propose);

    txState.ShardsInProgress.erase(shardIdx);

    LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "CollectPQConfigChanged accept TEvPersQueue::TEvProposeTransactionAttachResult"
                << ", operationId: " << operationId
                << ", shardIdx: " << shardIdx
                << ", shard: " << shardId
                << ", left await: " << txState.ShardsInProgress.size()
                << ", txState.State: " << TTxState::StateName(txState.State)
                << ", txState.ReadyForNotifications: " << txState.ReadyForNotifications
                << ", at schemeshard: " << ssId);

    return txState.ShardsInProgress.empty();
}

bool TConfigureParts::HandleReply(TEvPersQueue::TEvProposeTransactionResult::TPtr& ev, TOperationContext& context)
{
    const TTabletId ssId = context.SS->SelfTabletId();

    LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
               DebugHint() << " HandleReply TEvProposeTransactionResult"
               << ", at schemeshard: " << ssId);

    return CollectProposeTransactionResults(OperationId, ev, context);
}

THolder<TEvPersQueue::TEvProposeTransaction> TConfigureParts::MakeEvProposeTransaction(TTxId txId,
                                                                                       const TTopicInfo& pqGroup,
                                                                                       const TTopicTabletInfo& pqShard,
                                                                                       const TString& topicName,
                                                                                       const TString& topicPath,
                                                                                       const std::optional<NKikimrPQ::TBootstrapConfig>& bootstrapConfig,
                                                                                       const TString& cloudId,
                                                                                       const TString& folderId,
                                                                                       const TString& databaseId,
                                                                                       const TString& databasePath,
                                                                                       TTxState::ETxType txType,
                                                                                       const TOperationContext& context)
{
    auto event = MakeHolder<TEvPersQueue::TEvProposeTransaction>();
    event->Record.SetTxId(ui64(txId));
    ActorIdToProto(context.SS->SelfId(), event->Record.MutableSourceActor());

    MakePQTabletConfig(context,
                      *event->Record.MutableConfig()->MutableTabletConfig(),
                       pqGroup,
                       pqShard,
                       topicName,
                       topicPath,
                       cloudId,
                       folderId,
                       databaseId,
                       databasePath);
    if (bootstrapConfig) {
        Y_ABORT_UNLESS(txType == TTxState::TxCreatePQGroup);
        event->Record.MutableConfig()->MutableBootstrapConfig()->CopyFrom(*bootstrapConfig);
    }

    LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Propose configure PersQueue" <<
                ", message: " << event->Record.ShortUtf8DebugString());

    return event;
}

THolder<TEvPersQueue::TEvUpdateConfig> TConfigureParts::MakeEvUpdateConfig(TTxId txId,
                                                                           const TTopicInfo& pqGroup,
                                                                           const TTopicTabletInfo& pqShard,
                                                                           const TString& topicName,
                                                                           const TString& topicPath,
                                                                           const std::optional<NKikimrPQ::TBootstrapConfig>& bootstrapConfig,
                                                                           const TString& cloudId,
                                                                           const TString& folderId,
                                                                           const TString& databaseId,
                                                                           const TString& databasePath,
                                                                           TTxState::ETxType txType,
                                                                           const TOperationContext& context)
{
    auto event = MakeHolder<TEvPersQueue::TEvUpdateConfig>();
    event->Record.SetTxId(ui64(txId));

    MakePQTabletConfig(context,
                       *event->Record.MutableTabletConfig(),
                       pqGroup,
                       pqShard,
                       topicName,
                       topicPath,
                       cloudId,
                       folderId,
                       databaseId,
                       databasePath);
    if (bootstrapConfig) {
        Y_ABORT_UNLESS(txType == TTxState::TxCreatePQGroup);
        event->Record.MutableBootstrapConfig()->CopyFrom(*bootstrapConfig);
    }

    LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Propose configure PersQueue" <<
                ", message: " << event->Record.ShortUtf8DebugString());

    return event;
}

bool TPropose::HandleReply(TEvPersQueue::TEvProposeTransactionResult::TPtr& ev, TOperationContext& context)
{
    const TTabletId ssId = context.SS->SelfTabletId();
    const auto& evRecord = ev->Get()->Record;

    LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
               DebugHint() << " HandleReply TEvProposeTransactionResult"
               << " triggers early"
               << ", at schemeshard: " << ssId
               << " message# " << evRecord.ShortDebugString());

    const bool collected = CollectPQConfigChanged(OperationId, ev, context);
    LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                DebugHint() << " HandleReply TEvProposeTransactionResult"
                << " CollectPQConfigChanged: " << (collected ? "true" : "false"));

    return TryPersistState(context);
}

bool TPropose::HandleReply(TEvPersQueue::TEvProposeTransactionAttachResult::TPtr& ev, TOperationContext& context)
{
    const auto ssId = context.SS->SelfTabletId();
    const auto& evRecord = ev->Get()->Record;

    LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
               DebugHint() << " HandleReply TEvProposeTransactionAttachResult"
               << " triggers early"
               << ", at schemeshard: " << ssId
               << " message# " << evRecord.ShortDebugString());

    const bool collected = CollectPQConfigChanged(OperationId, ev, context);
    LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                DebugHint() << " HandleReply TEvProposeTransactionAttachResult"
                << " CollectPQConfigChanged: " << (collected ? "true" : "false"));

    return TryPersistState(context);
}

void TPropose::PrepareShards(TTxState& txState, TSet<TTabletId>& shardSet, TOperationContext& context)
{
    txState.UpdateShardsInProgress();

    for (const auto& shard : txState.Shards) {
        const TShardIdx idx = shard.Idx;
        //
        // The operation involves the ETabletType::PersQueue and Tabletype::PersQueueReadBalancer shards.
        // The program receives responses from PersQueueReadBalancer at the previous stage. At this stage,
        // it only expects TEvProposeTransactionResult from PersQueue
        //
        if (shard.TabletType == ETabletType::PersQueue) {
            const TTabletId tablet = context.SS->ShardInfos.at(idx).TabletID;

            shardSet.insert(tablet);

            //
            // By this point, the SchemeShard tablet could restart and the actor ID changed. Therefore, we send
            // the TEvProposeTransactionAttach message to the PQ tablets so that they recognize the new recipient
            //
            SendEvProposeTransactionAttach(idx, tablet, context);
        } else {
            txState.ShardsInProgress.erase(idx);
        }
    }
}

void TPropose::SendEvProposeTransactionAttach(TShardIdx shard, TTabletId tablet,
                                              TOperationContext& context)
{
    auto event =
        MakeHolder<TEvPersQueue::TEvProposeTransactionAttach>(ui64(tablet),
                                                              ui64(OperationId.GetTxId()));
    context.OnComplete.BindMsgToPipe(OperationId, tablet, shard, event.Release());
}

bool TPropose::CanPersistState(const TTxState& txState,
                               TOperationContext& context)
{
    if (!txState.ShardsInProgress.empty()) {
        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    DebugHint() << " can't persist state: " <<
                    "ShardsInProgress is not empty, remain: " << txState.ShardsInProgress.size());
        return false;
    }

    PathId = txState.TargetPathId;
    Path = context.SS->PathsById.at(PathId);

    if (Path->StepCreated == InvalidStepId) {
        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    DebugHint() << " can't persist state: " <<
                    "StepCreated is invalid");
        return false;
    }

    return true;
}

void TPropose::PersistState(const TTxState& txState,
                            TOperationContext& context) const
{
    NIceDb::TNiceDb db(context.GetDB());

    if (txState.TxType == TTxState::TxCreatePQGroup) {
        auto parentDir = context.SS->PathsById.at(Path->ParentPathId);
        ++parentDir->DirAlterVersion;
        context.SS->PersistPathDirAlterVersion(db, parentDir);
        context.SS->ClearDescribePathCaches(parentDir);
        context.OnComplete.PublishToSchemeBoard(OperationId, parentDir->PathId);
    }

    context.SS->ClearDescribePathCaches(Path);
    context.OnComplete.PublishToSchemeBoard(OperationId, PathId);

    TTopicInfo::TPtr pqGroup = context.SS->Topics[PathId];

    NKikimrPQ::TPQTabletConfig tabletConfig = pqGroup->GetTabletConfig();
    NKikimrPQ::TPQTabletConfig newTabletConfig = pqGroup->AlterData->GetTabletConfig();

    pqGroup->FinishAlter();

    context.SS->PersistPersQueueGroup(db, PathId, pqGroup);
    context.SS->PersistRemovePersQueueGroupAlter(db, PathId);

    context.SS->ChangeTxState(db, OperationId, TTxState::Done);
}

bool TPropose::TryPersistState(TOperationContext& context)
{
    TTxState* txState = context.SS->FindTx(OperationId);
    Y_ABORT_UNLESS(txState);

    if (!CanPersistState(*txState, context)) {
        return false;
    }

    PersistState(*txState, context);

    return true;
}

}

TSet<ui32> AllIncomingEvents() {
    TSet<ui32> result;

#define AddToList(TEvType, TxType)          \
    result.insert(TEvType::EventType);

    SCHEMESHARD_INCOMING_EVENTS(AddToList)
#undef AddToList

    return result;
}

void NForceDrop::CollectShards(const THashSet<TPathId>& paths, TOperationId operationId, TTxState *txState, TOperationContext &context) {
    NIceDb::TNiceDb db(context.GetDB());

    auto shards = context.SS->CollectAllShards(paths);
    for (auto shardIdx: shards) {
        Y_VERIFY_S(context.SS->ShardInfos.contains(shardIdx), "Unknown shardIdx " << shardIdx);
        auto& shardInfo = context.SS->ShardInfos.at(shardIdx);
        LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "Collect shard"
                    << ", shard idx: " << shardIdx
                    << ", tabletID: " << shardInfo.TabletID
                    << ", path id: " << shardInfo.PathId);

        txState->Shards.emplace_back(shardIdx, shardInfo.TabletType, txState->State);

        shardInfo.CurrentTxId = operationId.GetTxId();
        context.SS->PersistShardTx(db, shardIdx, operationId.GetTxId());

        if (TTabletTypes::DataShard == shardInfo.TabletType) {
            context.SS->TabletCounters->Simple()[COUNTER_TABLE_SHARD_ACTIVE_COUNT].Sub(1);
            context.SS->TabletCounters->Simple()[COUNTER_TABLE_SHARD_INACTIVE_COUNT].Add(1);
        }
    }

    context.SS->PersistTxState(db, operationId);
}

void NForceDrop::ValidateNoTransactionOnPaths(TOperationId operationId, const THashSet<TPathId>& paths, TOperationContext &context) {
    // No transaction should materialize in a subdomain that is being deleted --
    // -- all operations should be checking parent dir status at Propose stage.
    // However, it is better to verify that, just in case.
    auto transactions = context.SS->GetRelatedTransactions(paths, context.Ctx);
    for (auto otherTxId: transactions) {
        if (otherTxId == operationId.GetTxId()) {
            continue;
        }
        Y_VERIFY_S(false, "unexpected transaction: " << otherTxId << " found on the subdomain being deleted by transaction " << operationId.GetTxId());
    }
}

void IncParentDirAlterVersionWithRepublishSafeWithUndo(const TOperationId& opId, const TPath& path, TSchemeShard* ss, TSideEffects& onComplete) {
    auto parent = path.Parent();
    if (parent.Base()->IsDirectory() || parent.Base()->IsDomainRoot()) {
        ++parent.Base()->DirAlterVersion;
    }

    if (parent.IsActive()) {
        ss->ClearDescribePathCaches(parent.Base());
        onComplete.PublishToSchemeBoard(opId, parent->PathId);
    }

    if (path.IsActive()) {
        ss->ClearDescribePathCaches(path.Base());
        onComplete.PublishToSchemeBoard(opId, path->PathId);
    }
}

void IncParentDirAlterVersionWithRepublish(const TOperationId& opId, const TPath& path, TOperationContext &context) {
    IncParentDirAlterVersionWithRepublishSafeWithUndo(opId, path, context.SS, context.OnComplete);

    auto parent = path.Parent();
    if (parent.Base()->IsDirectory() || parent.Base()->IsDomainRoot()) {
        NIceDb::TNiceDb db(context.GetDB());
        context.SS->PersistPathDirAlterVersion(db, parent.Base());
    }
}

NKikimrSchemeOp::TModifyScheme MoveTableTask(NKikimr::NSchemeShard::TPath& src, NKikimr::NSchemeShard::TPath& dst) {
    NKikimrSchemeOp::TModifyScheme scheme;

    scheme.SetWorkingDir(dst.Parent().PathString());
    scheme.SetFailOnExist(true);
    scheme.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpMoveTable);
    auto operation = scheme.MutableMoveTable();
    operation->SetSrcPath(src.PathString());
    operation->SetDstPath(dst.PathString());

    return scheme;
}

NKikimrSchemeOp::TModifyScheme MoveTableIndexTask(NKikimr::NSchemeShard::TPath& src, NKikimr::NSchemeShard::TPath& dst) {
    NKikimrSchemeOp::TModifyScheme scheme;

    scheme.SetWorkingDir(dst.Parent().PathString());
    scheme.SetFailOnExist(true);
    scheme.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpMoveTableIndex);
    auto operation = scheme.MutableMoveTableIndex();
    operation->SetSrcPath(src.PathString());
    operation->SetDstPath(dst.PathString());

    return scheme;
}

void AbortUnsafeDropOperation(const TOperationId& opId, const TTxId& txId, TOperationContext& context) {
    TTxState* txState = context.SS->FindTx(opId);
    Y_ABORT_UNLESS(txState);

    LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, ""
        << TTxState::TypeName(txState->TxType) << " AbortUnsafe"
        << ": opId# " << opId
        << ", txId# " << txId
        << ", ssId# " << context.SS->TabletID());

    const auto& pathId = txState->TargetPathId;
    Y_ABORT_UNLESS(context.SS->PathsById.contains(pathId));
    auto path = context.SS->PathsById.at(pathId);
    Y_ABORT_UNLESS(path);

    if (path->Dropped()) {
        for (const auto& shard : txState->Shards) {
            context.OnComplete.DeleteShard(shard.Idx);
        }
    }

    context.OnComplete.DoneOperation(opId);
}

}
}

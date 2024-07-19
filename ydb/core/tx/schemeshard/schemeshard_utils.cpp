#include "schemeshard_utils.h"

#include <ydb/core/mind/hive/hive.h>
#include <ydb/core/persqueue/utils.h>
#include <ydb/core/protos/counters_schemeshard.pb.h>

namespace NKikimr {
namespace NSchemeShard {

void TShardDeleter::Shutdown(const NActors::TActorContext &ctx) {
    for (auto& info : PerHiveDeletions) {
        NTabletPipe::CloseClient(ctx, info.second.PipeToHive);
    }
    PerHiveDeletions.clear();
}

void TShardDeleter::SendDeleteRequests(TTabletId hiveTabletId,
                                       const THashSet<TShardIdx> &shardsToDelete,
                                       const THashMap<NKikimr::NSchemeShard::TShardIdx, NKikimr::NSchemeShard::TShardInfo>& shardsInfos,
                                       const NActors::TActorContext &ctx) {
    if (shardsToDelete.empty())
        return;

    TPerHiveDeletions& info = PerHiveDeletions[hiveTabletId];
    if (!info.PipeToHive) {
        NTabletPipe::TClientConfig clientConfig;
        clientConfig.RetryPolicy = HivePipeRetryPolicy;
        info.PipeToHive = ctx.Register(NTabletPipe::CreateClient(ctx.SelfID, ui64(hiveTabletId), clientConfig));
    }
    info.ShardsToDelete.insert(shardsToDelete.begin(), shardsToDelete.end());

    for (auto shardIdx : shardsToDelete) {
        ShardHive[shardIdx] = hiveTabletId;
        // !HACK: use shardIdx as  TxId because Hive only replies with TxId
        // TODO: change hive events to get rid of this hack
        // svc@ in progress fixing it
        TAutoPtr<TEvHive::TEvDeleteTablet> event = new TEvHive::TEvDeleteTablet(shardIdx.GetOwnerId(), ui64(shardIdx.GetLocalId()), ui64(shardIdx.GetLocalId()));
        auto itShard = shardsInfos.find(shardIdx);
        if (itShard != shardsInfos.end()) {
            TTabletId shardTabletId = itShard->second.TabletID;
            if (shardTabletId) {
                event->Record.AddTabletID(ui64(shardTabletId));
            }
        }

        Y_ABORT_UNLESS(shardIdx);

        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "Free shard " << shardIdx << " hive " << hiveTabletId << " at ss " << MyTabletID);

        NTabletPipe::SendData(ctx, info.PipeToHive, event.Release());
    }
}

void TShardDeleter::ResendDeleteRequests(TTabletId hiveTabletId, const THashMap<TShardIdx, TShardInfo>& shardsInfos, const NActors::TActorContext &ctx) {
    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                 "Resending tablet deletion requests from " << MyTabletID << " to " << hiveTabletId);

    auto itPerHive = PerHiveDeletions.find(hiveTabletId);
    if (itPerHive == PerHiveDeletions.end()) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Hive " << hiveTabletId << " not found for delete requests");
        return;
    }

    THashSet<TShardIdx> toResend(std::move(itPerHive->second.ShardsToDelete));
    PerHiveDeletions.erase(itPerHive);

    SendDeleteRequests(hiveTabletId, toResend, shardsInfos, ctx);
}

void TShardDeleter::ResendDeleteRequest(TTabletId hiveTabletId,
                                        const THashMap<TShardIdx, TShardInfo>& shardsInfos,
                                        TShardIdx shardIdx,
                                        const NActors::TActorContext &ctx) {
    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                 "Resending tablet deletion request from " << MyTabletID << " to " << hiveTabletId);

    auto itPerHive = PerHiveDeletions.find(hiveTabletId);
    if (itPerHive == PerHiveDeletions.end())
        return;

    auto itShardIdx = itPerHive->second.ShardsToDelete.find(shardIdx);
    if (itShardIdx != itPerHive->second.ShardsToDelete.end()) {
        THashSet<TShardIdx> toResend({shardIdx});
        itPerHive->second.ShardsToDelete.erase(itShardIdx);
        if (itPerHive->second.ShardsToDelete.empty()) {
            PerHiveDeletions.erase(itPerHive);
        }
        SendDeleteRequests(hiveTabletId, toResend, shardsInfos, ctx);
    } else {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Shard " << shardIdx << " not found for delete request for Hive " << hiveTabletId);
    }
}

void TShardDeleter::RedirectDeleteRequest(TTabletId hiveFromTabletId,
                                          TTabletId hiveToTabletId,
                                          TShardIdx shardIdx,
                                          const THashMap<TShardIdx, TShardInfo>& shardsInfos,
                                          const NActors::TActorContext &ctx) {
    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                 "Redirecting tablet deletion requests from " << hiveFromTabletId << " to " << hiveToTabletId);
    auto itFromHive = PerHiveDeletions.find(hiveFromTabletId);
    if (itFromHive != PerHiveDeletions.end()) {
        auto& toHive(PerHiveDeletions[hiveToTabletId]);
        auto itShardIdx = itFromHive->second.ShardsToDelete.find(shardIdx);
        if (itShardIdx != itFromHive->second.ShardsToDelete.end()) {
            toHive.ShardsToDelete.emplace(*itShardIdx);
            itFromHive->second.ShardsToDelete.erase(itShardIdx);
        } else {
            LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                       "Shard " << shardIdx << " not found for delete request for Hive " << hiveFromTabletId);
        }
        if (itFromHive->second.ShardsToDelete.empty()) {
            PerHiveDeletions.erase(itFromHive);
        }
    }

    ResendDeleteRequest(hiveToTabletId, shardsInfos, shardIdx, ctx);
}

void TShardDeleter::ShardDeleted(TShardIdx shardIdx, const NActors::TActorContext &ctx) {
    if (!ShardHive.contains(shardIdx))
        return;

    TTabletId hiveTabletId = ShardHive[shardIdx];
    ShardHive.erase(shardIdx);
    PerHiveDeletions[hiveTabletId].ShardsToDelete.erase(shardIdx);

    if (PerHiveDeletions[hiveTabletId].ShardsToDelete.empty()) {
        NTabletPipe::CloseClient(ctx, PerHiveDeletions[hiveTabletId].PipeToHive);
        PerHiveDeletions.erase(hiveTabletId);
    }
}

bool TShardDeleter::Has(TTabletId hiveTabletId, TActorId pipeClientActorId) const {
    return PerHiveDeletions.contains(hiveTabletId) && PerHiveDeletions.at(hiveTabletId).PipeToHive == pipeClientActorId;
}

bool TShardDeleter::Has(TShardIdx shardIdx) const {
    return ShardHive.contains(shardIdx);
}

bool TShardDeleter::Empty() const {
    return PerHiveDeletions.empty();
}

void TSelfPinger::Handle(TEvSchemeShard::TEvMeasureSelfResponseTime::TPtr &ev, const NActors::TActorContext &ctx) {
    Y_UNUSED(ev);
    TInstant now = AppData(ctx)->TimeProvider->Now();
    TDuration responseTime = now - SelfPingSentTime;
    LastResponseTime = responseTime;
    TabletCounters->Simple()[COUNTER_RESPONSE_TIME_USEC].Set(LastResponseTime.MicroSeconds());
    if (responseTime.MilliSeconds() > 1000) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Schemeshard " << TabletId << " response time is " << responseTime.MilliSeconds() << " msec");
    }
    SelfPingInFlight = false;
    if (responseTime > SELF_PING_INTERVAL) {
        DoSelfPing(ctx);
    } else {
        ScheduleSelfPingWakeup(ctx);
    }
}

void TSelfPinger::Handle(TEvSchemeShard::TEvWakeupToMeasureSelfResponseTime::TPtr &ev, const NActors::TActorContext &ctx) {
    Y_UNUSED(ev);
    SelfPingWakeupScheduled = false;
    DoSelfPing(ctx);
}

void TSelfPinger::OnAnyEvent(const NActors::TActorContext &ctx) {
    TInstant now = AppData(ctx)->TimeProvider->Now();
    if (SelfPingInFlight) {
        TDuration responseTime = now - SelfPingSentTime;
        // Increase measured response time is ping is taking longer than then the previous one
        LastResponseTime = Max(LastResponseTime, responseTime);
        TabletCounters->Simple()[COUNTER_RESPONSE_TIME_USEC].Set(LastResponseTime.MicroSeconds());
    } else if ((now - SelfPingWakeupScheduledTime) > SELF_PING_INTERVAL) {
        DoSelfPing(ctx);
    }
}

void TSelfPinger::DoSelfPing(const NActors::TActorContext &ctx) {
    if (SelfPingInFlight)
        return;

    ctx.Send(ctx.SelfID, new TEvSchemeShard::TEvMeasureSelfResponseTime);
    SelfPingSentTime = AppData(ctx)->TimeProvider->Now();
    SelfPingInFlight = true;
}

void TSelfPinger::ScheduleSelfPingWakeup(const NActors::TActorContext &ctx) {
    if (SelfPingWakeupScheduled)
        return;

    ctx.Schedule(SELF_PING_INTERVAL, new TEvSchemeShard::TEvWakeupToMeasureSelfResponseTime);
    SelfPingWakeupScheduled = true;
    SelfPingWakeupScheduledTime = AppData(ctx)->TimeProvider->Now();
}

PQGroupReserve::PQGroupReserve(const ::NKikimrPQ::TPQTabletConfig& tabletConfig, ui64 partitions) {
    Storage = partitions * NPQ::TopicPartitionReserveSize(tabletConfig);
    Throughput = partitions * NPQ::TopicPartitionReserveThroughput(tabletConfig);
}

}

namespace NTableIndex {

TTableColumns ExtractInfo(const NKikimrSchemeOp::TTableDescription &tableDescr) {
    NTableIndex::TTableColumns result;
    for (auto& column: tableDescr.GetColumns()) {
        result.Columns.insert(column.GetName());
    }
    for (auto& keyName: tableDescr.GetKeyColumnNames()) {
        result.Keys.push_back(keyName);
    }
    return result;
}

TIndexColumns ExtractInfo(const NKikimrSchemeOp::TIndexCreationConfig &indexDesc) {
    NTableIndex::TIndexColumns result;
    for (auto& keyName: indexDesc.GetKeyColumnNames()) {
        result.KeyColumns.push_back(keyName);
    }
    for (auto& keyName: indexDesc.GetDataColumnNames()) {
        result.DataColumns.push_back(keyName);
    }
    return result;
}

TTableColumns ExtractInfo(const NSchemeShard::TTableInfo::TPtr &tableInfo) {
    NTableIndex::TTableColumns result;
    for (auto& item: tableInfo->Columns) {
        const auto& column = item.second;
        if (column.IsDropped()) {
            continue;
        }

        result.Columns.insert(item.second.Name);
    }

    for (auto& keyId: tableInfo->KeyColumnIds) {
        const auto& keyColumn = tableInfo->Columns.at(keyId);
        if (keyColumn.IsDropped()) {
            continue;
        }

        Y_ABORT_UNLESS(result.Columns.contains(keyColumn.Name));
        result.Keys.push_back(keyColumn.Name);
    }

    return result;
}

NKikimrSchemeOp::TPartitionConfig PartitionConfigForIndexes(
        const NKikimrSchemeOp::TPartitionConfig& baseTablePartitionConfig,
        const NKikimrSchemeOp::TTableDescription& indexTableDesc)
{
    // KIKIMR-6687
    NKikimrSchemeOp::TPartitionConfig result;

    if (baseTablePartitionConfig.HasNamedCompactionPolicy()) {
        result.SetNamedCompactionPolicy(baseTablePartitionConfig.GetNamedCompactionPolicy());
    }
    if (baseTablePartitionConfig.HasCompactionPolicy()) {
        result.MutableCompactionPolicy()->CopyFrom(baseTablePartitionConfig.GetCompactionPolicy());
    }
    // skip optional uint64 FollowerCount = 3;
    if (baseTablePartitionConfig.HasExecutorCacheSize()) {
        result.SetExecutorCacheSize(baseTablePartitionConfig.GetExecutorCacheSize());
    }
    // skip     optional bool AllowFollowerPromotion = 5 [default = true];
    if (baseTablePartitionConfig.HasTxReadSizeLimit()) {
        result.SetTxReadSizeLimit(baseTablePartitionConfig.GetTxReadSizeLimit());
    }
    // skip optional uint32 CrossDataCenterFollowerCount = 8;
    if (baseTablePartitionConfig.HasChannelProfileId()) {
        result.SetChannelProfileId(baseTablePartitionConfig.GetChannelProfileId());
    }

    if (indexTableDesc.GetPartitionConfig().HasPartitioningPolicy()) {
        result.MutablePartitioningPolicy()->CopyFrom(indexTableDesc.GetPartitionConfig().GetPartitioningPolicy());
    } else {
        result.MutablePartitioningPolicy()->SetSizeToSplit(2_GB);
        result.MutablePartitioningPolicy()->SetMinPartitionsCount(1);
    }
    if (baseTablePartitionConfig.HasPipelineConfig()) {
        result.MutablePipelineConfig()->CopyFrom(baseTablePartitionConfig.GetPipelineConfig());
    }
    if (baseTablePartitionConfig.ColumnFamiliesSize()) {
        // Indexes don't need column families unless it's the default column family
        for (const auto& family : baseTablePartitionConfig.GetColumnFamilies()) {
            const bool isDefaultFamily = (
                (!family.HasId() && !family.HasName()) ||
                (family.HasId() && family.GetId() == 0) ||
                (family.HasName() && family.GetName() == "default"));
            if (isDefaultFamily) {
                result.AddColumnFamilies()->CopyFrom(family);
            }
        }
    }
    if (baseTablePartitionConfig.HasResourceProfile()) {
        result.SetResourceProfile(baseTablePartitionConfig.GetResourceProfile());
    }
    if (baseTablePartitionConfig.HasDisableStatisticsCalculation()) {
        result.SetDisableStatisticsCalculation(baseTablePartitionConfig.GetDisableStatisticsCalculation());
    }
    if (baseTablePartitionConfig.HasEnableFilterByKey()) {
        result.SetEnableFilterByKey(baseTablePartitionConfig.GetEnableFilterByKey());
    }
    if (baseTablePartitionConfig.HasExecutorFastLogPolicy()) {
        result.SetExecutorFastLogPolicy(baseTablePartitionConfig.GetExecutorFastLogPolicy());
    }
    if (baseTablePartitionConfig.HasEnableEraseCache()) {
        result.SetEnableEraseCache(baseTablePartitionConfig.GetEnableEraseCache());
    }
    if (baseTablePartitionConfig.HasEraseCacheMinRows()) {
        result.SetEraseCacheMinRows(baseTablePartitionConfig.GetEraseCacheMinRows());
    }
    if (baseTablePartitionConfig.HasEraseCacheMaxBytes()) {
        result.SetEraseCacheMaxBytes(baseTablePartitionConfig.GetEraseCacheMaxBytes());
    }
    if (baseTablePartitionConfig.HasKeepSnapshotTimeout()) {
        result.SetKeepSnapshotTimeout(baseTablePartitionConfig.GetKeepSnapshotTimeout());
    }
    // skip repeated NKikimrStorageSettings.TStorageRoom StorageRooms = 17;
    // skip optional NKikimrHive.TFollowerGroup FollowerGroup = 23;

    return result;
}

void SetImplTablePartitionConfig(
    const NKikimrSchemeOp::TPartitionConfig& baseTablePartitionConfig,
    const NKikimrSchemeOp::TTableDescription& indexTableDesc,
    NKikimrSchemeOp::TTableDescription& tableDescription)
{
    if (indexTableDesc.HasUniformPartitionsCount()) {
        tableDescription.SetUniformPartitionsCount(indexTableDesc.GetUniformPartitionsCount());
    }

    if (indexTableDesc.SplitBoundarySize()) {
        tableDescription.MutableSplitBoundary()->CopyFrom(indexTableDesc.GetSplitBoundary());
    }

    *tableDescription.MutablePartitionConfig() = PartitionConfigForIndexes(baseTablePartitionConfig, indexTableDesc);
}

 void FillIndexImplTableColumns(
    const THashMap<ui32, struct NSchemeShard::TTableInfo::TColumn>& baseTableColumns,
    const NTableIndex::TTableColumns& implTableColumns,
    NKikimrSchemeOp::TTableDescription& implTableDesc)
{
    //Columns and KeyColumnNames order is really important
    //the order of implTableColumns.Keys is the right one

    THashMap<TString, ui32> implKeyToImplColumn;
    for (ui32 keyId = 0; keyId < implTableColumns.Keys.size(); ++keyId) {
        implKeyToImplColumn[implTableColumns.Keys[keyId]] = keyId;
    }

    for (auto& iter: baseTableColumns) {
        const NSchemeShard::TTableInfo::TColumn& column = iter.second;
        if (column.IsDropped()) {
            continue;
        }

        if (implTableColumns.Columns.contains(column.Name)) {
            auto item = implTableDesc.AddColumns();
            item->SetName(column.Name);
            item->SetType(NScheme::TypeName(column.PType, column.PTypeMod));
            item->SetNotNull(column.NotNull);
            ui32 order = Max<ui32>();
            if (implKeyToImplColumn.contains(column.Name)) {
                order = implKeyToImplColumn.at(column.Name);
            }
            item->SetId(order);
        }
    }

    std::sort(implTableDesc.MutableColumns()->begin(),
              implTableDesc.MutableColumns()->end(),
              [] (auto& left, auto& right) {
                  return left.GetId() < right.GetId();
              });

    for (auto& column: *implTableDesc.MutableColumns()) {
        column.ClearId();
    }

    for (auto& keyName: implTableColumns.Keys) {
        implTableDesc.AddKeyColumnNames(keyName);
    }
}

void FillIndexImplTableColumns(
    const ::google::protobuf::RepeatedPtrField<NKikimrSchemeOp::TColumnDescription>& baseTableColumns,
    const NTableIndex::TTableColumns& implTableColumns,
    NKikimrSchemeOp::TTableDescription& implTableDesc)
{
    //Columns and KeyColumnNames order is really important
    //the order of implTableColumns.Keys is the right one

    THashMap<TString, ui32> implKeyToImplColumn;
    for (ui32 keyId = 0; keyId < implTableColumns.Keys.size(); ++keyId) {
        implKeyToImplColumn[implTableColumns.Keys[keyId]] = keyId;
    }

    for (auto& column: baseTableColumns) {
        auto& columnName = column.GetName();
        if (implTableColumns.Columns.contains(columnName)) {
            auto item = implTableDesc.AddColumns();
            *item = column;

            // Indexes don't use column families
            item->ClearFamily();
            item->ClearFamilyName();

            // Indexes can't have a default value
            item->ClearDefaultValue();

            ui32 order = Max<ui32>();
            if (implKeyToImplColumn.contains(columnName)) {
                order = implKeyToImplColumn.at(columnName);
            }
            item->SetId(order);
        }
    }

    std::sort(implTableDesc.MutableColumns()->begin(),
              implTableDesc.MutableColumns()->end(),
              [] (auto& left, auto& right) {
                  return left.GetId() < right.GetId();
              });

    for (auto& column: *implTableDesc.MutableColumns()) {
        column.ClearId();
    }

    for (auto& keyName: implTableColumns.Keys) {
        implTableDesc.AddKeyColumnNames(keyName);
    }
}

NKikimrSchemeOp::TTableDescription CalcImplTableDesc(
    const NSchemeShard::TTableInfo::TPtr& baseTableInfo,
    const NTableIndex::TTableColumns& implTableColumns,
    const NKikimrSchemeOp::TTableDescription& indexTableDesc)
{
    NKikimrSchemeOp::TTableDescription implTableDesc;

    implTableDesc.SetName(NTableIndex::ImplTable);

    SetImplTablePartitionConfig(baseTableInfo->PartitionConfig(), indexTableDesc, implTableDesc);

    FillIndexImplTableColumns(baseTableInfo->Columns, implTableColumns, implTableDesc);

    return implTableDesc;
}

NKikimrSchemeOp::TTableDescription CalcImplTableDesc(
    const NKikimrSchemeOp::TTableDescription& baseTableDescr,
    const TTableColumns& implTableColumns,
    const NKikimrSchemeOp::TTableDescription& indexTableDesc)
{
    NKikimrSchemeOp::TTableDescription implTableDesc;

    implTableDesc.SetName(NTableIndex::ImplTable);

    SetImplTablePartitionConfig(baseTableDescr.GetPartitionConfig(), indexTableDesc, implTableDesc);

    FillIndexImplTableColumns(baseTableDescr.GetColumns(), implTableColumns, implTableDesc);

    return implTableDesc;
}

NKikimrSchemeOp::TTableDescription CalcVectorKmeansTreeLevelImplTableDesc(
    const NKikimrSchemeOp::TPartitionConfig& baseTablePartitionConfig,
    const NKikimrSchemeOp::TTableDescription& indexTableDesc)
{
    NKikimrSchemeOp::TTableDescription implTableDesc;

    implTableDesc.SetName(NTableVectorKmeansTreeIndex::LevelTable);

    SetImplTablePartitionConfig(baseTablePartitionConfig, indexTableDesc, implTableDesc);

    {
        auto parentIdColumn = implTableDesc.AddColumns();
        parentIdColumn->SetName(NTableVectorKmeansTreeIndex::LevelTable_ParentIdColumn);
        parentIdColumn->SetType("Uint32");
        parentIdColumn->SetTypeId(NScheme::NTypeIds::Uint32);
        parentIdColumn->SetId(0);
    }
    {
        auto idColumn = implTableDesc.AddColumns();
        idColumn->SetName(NTableVectorKmeansTreeIndex::LevelTable_IdColumn);
        idColumn->SetType("Uint32");
        idColumn->SetTypeId(NScheme::NTypeIds::Uint32);
        idColumn->SetId(1);
    }
    {
        auto centroidColumn = implTableDesc.AddColumns();
        centroidColumn->SetName(NTableVectorKmeansTreeIndex::LevelTable_EmbeddingColumn);
        centroidColumn->SetType("String");
        centroidColumn->SetTypeId(NScheme::NTypeIds::String);
        centroidColumn->SetId(2);
    }

    implTableDesc.AddKeyColumnNames(NTableVectorKmeansTreeIndex::LevelTable_ParentIdColumn);
    implTableDesc.AddKeyColumnNames(NTableVectorKmeansTreeIndex::LevelTable_IdColumn);

    return implTableDesc;
}

NKikimrSchemeOp::TTableDescription CalcVectorKmeansTreePostingImplTableDesc(
    const NSchemeShard::TTableInfo::TPtr& baseTableInfo,
    const NKikimrSchemeOp::TPartitionConfig& baseTablePartitionConfig,
    const NTableIndex::TTableColumns& implTableColumns,
    const NKikimrSchemeOp::TTableDescription& indexTableDesc)
{
    NKikimrSchemeOp::TTableDescription implTableDesc;

    implTableDesc.SetName(NTableVectorKmeansTreeIndex::PostingTable);

    SetImplTablePartitionConfig(baseTablePartitionConfig, indexTableDesc, implTableDesc);

    {
        auto parentIdColumn = implTableDesc.AddColumns();
        parentIdColumn->SetName(NTableVectorKmeansTreeIndex::PostingTable_ParentIdColumn);
        parentIdColumn->SetType("Uint32");
        parentIdColumn->SetTypeId(NScheme::NTypeIds::Uint32);
        parentIdColumn->SetId(0);
    }

    FillIndexImplTableColumns(baseTableInfo->Columns, implTableColumns, implTableDesc);

    return implTableDesc;
}

NKikimrSchemeOp::TTableDescription CalcVectorKmeansTreePostingImplTableDesc(
    const NKikimrSchemeOp::TTableDescription &baseTableDescr,
    const NKikimrSchemeOp::TPartitionConfig& baseTablePartitionConfig,
    const NTableIndex::TTableColumns& implTableColumns,
    const NKikimrSchemeOp::TTableDescription& indexTableDesc)
{
    NKikimrSchemeOp::TTableDescription implTableDesc;

    implTableDesc.SetName(NTableVectorKmeansTreeIndex::PostingTable);

    SetImplTablePartitionConfig(baseTablePartitionConfig, indexTableDesc, implTableDesc);

    {
        auto parentIdColumn = implTableDesc.AddColumns();
        parentIdColumn->SetName(NTableVectorKmeansTreeIndex::PostingTable_ParentIdColumn);
        parentIdColumn->SetType("Uint32");
        parentIdColumn->SetTypeId(NScheme::NTypeIds::Uint32);
        parentIdColumn->SetId(0);
    }

    FillIndexImplTableColumns(baseTableDescr.GetColumns(), implTableColumns, implTableDesc);

    return implTableDesc;
}



bool ExtractTypes(const NKikimrSchemeOp::TTableDescription& baseTableDescr, TColumnTypes& columnTypes, TString& explain) {
    const NScheme::TTypeRegistry* typeRegistry = AppData()->TypeRegistry;
    Y_ABORT_UNLESS(typeRegistry);

    for (auto& column: baseTableDescr.GetColumns()) {
        auto& columnName = column.GetName();
        auto typeName = NMiniKQL::AdaptLegacyYqlType(column.GetType());
        const NScheme::IType* type = typeRegistry->GetType(typeName);
        if (!type) {
            auto* typeDesc = NPg::TypeDescFromPgTypeName(typeName);
            if (!typeDesc) {
                explain += TStringBuilder() << "Type '" << column.GetType() << "' specified for column '" << columnName << "' is not supported by storage";
                return false;
            }
            columnTypes[columnName] = NScheme::TTypeInfo(NScheme::NTypeIds::Pg, typeDesc);
        } else {
            columnTypes[columnName] = NScheme::TTypeInfo(type->GetTypeId());
        }
    }

    return true;
}

bool ExtractTypes(const NSchemeShard::TTableInfo::TPtr& baseTableInfo, TColumnTypes& columnsTypes, TString& explain) {
    Y_UNUSED(explain);

    for (const auto& [_, column] : baseTableInfo->Columns) {
        columnsTypes[column.Name] = column.PType;
    }

    return true;
}

bool IsCompatibleKeyTypes(
    const TColumnTypes& baseTableColumnTypes,
    const TTableColumns& implTableColumns,
    bool uniformTable,
    TString& explain)
{
    const NScheme::TTypeRegistry* typeRegistry = AppData()->TypeRegistry;
    Y_ABORT_UNLESS(typeRegistry);

    for (const auto& item: baseTableColumnTypes) {
        auto& columnName = item.first;
        auto typeId = item.second.GetTypeId();

        if (typeId == NScheme::NTypeIds::Pg) {
            if (!item.second.GetTypeDesc()) {
                explain += TStringBuilder() << "unknown pg type for column '" << columnName << "'";
                return false;
            }

        } else {
            auto typeSP = typeRegistry->GetType(typeId);
            if (!typeSP) {
                explain += TStringBuilder() << "unknown typeId '" << typeId << "' for column '" << columnName << "'";
                return false;
            }

            if (!NScheme::NTypeIds::IsYqlType(typeId)) {
                explain += TStringBuilder() << "Type '" << typeId << "' specified for column '" << columnName << "' is no longer supported";
                return false;
            }
        }
    }


    for (auto& keyName: implTableColumns.Keys) {
        Y_ABORT_UNLESS(baseTableColumnTypes.contains(keyName));
        auto typeInfo = baseTableColumnTypes.at(keyName);

        if (typeInfo.GetTypeId() == NScheme::NTypeIds::Uuid) {
            if (!AppData()->FeatureFlags.GetEnableUuidAsPrimaryKey()) {
                explain += TStringBuilder() << "Uuid as primary key is forbiden by configuration: " << keyName;
                return false;
            }
        }

        if (uniformTable) {
            switch (typeInfo.GetTypeId()) {
            case NScheme::NTypeIds::Uint32:
            case NScheme::NTypeIds::Uint64:
                break;
            default:
                explain += TStringBuilder() << "Column '" << keyName << "' has wrong key type "
                                            << NScheme::TypeName(typeInfo) << " for being key of table with uniform partitioning";
                return false;
            }
        }

        if (!NSchemeShard::IsAllowedKeyType(typeInfo)) {
            explain += TStringBuilder() << "Column '" << keyName << "' has wrong key type " << NScheme::TypeName(typeInfo) << " for being key";
            return false;
        }
    }

    return true;
}

}

}

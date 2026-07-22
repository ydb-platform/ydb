#include "schemeshard_impl.h"
#include "schemeshard__local_index_migration.h"
#include "schemeshard_svp_migration.h"

#include "olap/bg_tasks/adapter/adapter.h"
#include "olap/bg_tasks/events/global.h"
#include "olap/operations/local_index_helpers.h"
#include "schemeshard.h"
#include "schemeshard__root_shred_manager.h"
#include "schemeshard__tenant_shred_manager.h"
#include "schemeshard_svp_migration.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/tx_processing.h>
#include <ydb/core/engine/minikql/flat_local_tx_factory.h>
#include <ydb/core/engine/mkql_proto.h>
#include <ydb/core/keyvalue/keyvalue_events.h>
#include <ydb/core/protos/auth.pb.h>
#include <ydb/core/protos/feature_flags.pb.h>
#include <ydb/core/protos/fs_settings.pb.h>
#include <ydb/core/protos/s3_settings.pb.h>
#include <ydb/core/protos/schemeshard_config.pb.h>
#include <ydb/core/protos/table_stats.pb.h>  // for TStoragePoolsStats
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/statistics/events.h>
#include <ydb/core/statistics/service/service.h>
#include <ydb/core/sys_view/common/path.h>
#include <ydb/core/sys_view/common/resolver.h>
#include <ydb/core/sys_view/partition_stats/partition_stats.h>
#include <ydb/core/tablet/tablet_counters_aggregator.h>
#include <ydb/core/tablet/tablet_counters_protobuf.h>
#include <ydb/core/tablet_flat/bloom_filter_defaults.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/test_tablet/events.h>
#include <ydb/core/tx/columnshard/bg_tasks/events/events.h>
#include <ydb/core/tx/scheme_board/events_schemeshard.h>
#include <ydb/core/tx/schemeshard/schemeshard_path.h>
#include <ydb/core/tx/schemeshard/schemeshard_sysviews_update.h>

#include <ydb/library/login/account_lockout/account_lockout.h>
#include <ydb/library/login/password_checker/password_checker.h>

#include <yql/essentials/minikql/mkql_type_ops.h>
#include <yql/essentials/providers/common/proto/gateways_config.pb.h>

#include <util/random/random.h>
#include <util/system/byteorder.h>
#include <util/system/unaligned_mem.h>

namespace NKikimr {
namespace NSchemeShard {
void TSchemeShard::Handle(TEvIndexBuilder::TEvCreateResponse::TPtr& ev, const TActorContext& ctx) {
    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Handle: TEvIndexBuilder::TEvCreateResponse"
                   << ": txId# " << ev->Get()->Record.GetTxId()
                   << ", status# " << ev->Get()->Record.GetStatus());
    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Message:\n" << ev->Get()->Record.ShortDebugString());

    const auto txId = TTxId(ev->Get()->Record.GetTxId());

    if (TxIdToImport.contains(txId)) {
        return Execute(CreateTxProgressImport(ev), ctx);
    }

    LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
               "no able to determine destination for message TEvIndexBuilder::TEvCreateResponse: "
                   << " txId: " << txId
                   << ", at schemeshard: " << TabletID());
}

void TSchemeShard::Handle(TEvSchemeShard::TEvNotifyTxCompletionRegistered::TPtr&, const TActorContext&) {
    // just ignore
}

void TSchemeShard::Handle(TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr& ev, const TActorContext& ctx) {
    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
               "Handle: TEvNotifyTxCompletionResult"
                   << ": txId# " << ev->Get()->Record.GetTxId());
    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Message:\n" << ev->Get()->Record.ShortDebugString());

    const auto txId = TTxId(ev->Get()->Record.GetTxId());
    bool executed = false;

    if (TxIdToExport.contains(txId) || TxIdToDependentExport.contains(txId)) {
        Execute(CreateTxProgressExport(txId), ctx);
        executed = true;
    }
    if (TxIdToImport.contains(txId)) {
        Execute(CreateTxProgressImport(txId), ctx);
        executed = true;
    }
    if (TxIdToIncrementalRestore.contains(txId)) {
        Execute(CreateTxProgressIncrementalRestore(txId, ctx), ctx);
        executed = true;
    }
    if (TxIdToIndexBuilds.contains(txId) || TxIdToDependentIndexBuild.contains(txId)) {
        Execute(CreateTxReply(txId), ctx);
        executed = true;
    }
    if (FullBackups.contains(ui64(txId))) {
        // Control op completed; finalize the tracked record.
        Execute(CreateTxFullBackupProgress(ui64(txId)), ctx);
    }
    if (TxIdToSetColumnConstraintOperations.contains(txId) || TxIdToDependentSetColumnConstraint.contains(txId)) {
        Execute(CreateTxReplyCompletedSetColumnConstraint(txId), ctx);
        executed = true;
    }
    if (BackgroundCleaningTxToDirPathId.contains(txId)) {
        HandleBackgroundCleaningCompletionResult(txId);
        executed = true;
    }

    if (executed) {
        return;
    }

    LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
               "no able to determine destination for message TEvNotifyTxCompletionResult: "
                   << " txId: " << txId
                   << ", at schemeshard: " << TabletID());
}

void TSchemeShard::Handle(TEvSchemeShard::TEvCancelTxResult::TPtr& ev, const TActorContext& ctx) {
    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
               "Handle: TEvCancelTxResult"
                   << ": Cookie: " << ev->Cookie
                   << ", at schemeshard: " << TabletID());
    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Message:\n" << ev->Get()->Record.ShortDebugString());

    const ui64 id = ev->Cookie;
    if (Exports.contains(id)) {
        return Execute(CreateTxCancelExportAck(ev), ctx);
    } else if (Imports.contains(id)) {
        return Execute(CreateTxCancelImportAck(ev), ctx);
    }

    LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
               "no able to determine destination for message TEvCancelTxResult"
                   << ": Cookie: " << id
                   << ", at schemeshard: " << TabletID());
}

void TSchemeShard::Handle(TEvIndexBuilder::TEvCancelResponse::TPtr& ev, const TActorContext& ctx) {
    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
               "Handle: TEvIndexBuilder::TEvCancelResponse"
                   << ": Cookie: " << ev->Cookie
                   << ", at schemeshard: " << TabletID());
    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Message:\n" << ev->Get()->Record.ShortDebugString());

    const ui64 id = ev->Cookie;
    if (Imports.contains(id)) {
        return Execute(CreateTxCancelImportAck(ev), ctx);
    }

    LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
               "no able to determine destination for message TEvIndexBuilder::TEvCancelResponse"
                   << ": Cookie: " << id
                   << ", at schemeshard: " << TabletID());
}

void TSchemeShard::FillSeqNo(NKikimrTxDataShard::TFlatSchemeTransaction& tx, TMessageSeqNo seqNo) {
    tx.MutableSeqNo()->SetGeneration(seqNo.Generation);
    tx.MutableSeqNo()->SetRound(seqNo.Round);
}

void TSchemeShard::FillSeqNo(NKikimrTxColumnShard::TSchemaTxBody& tx, TMessageSeqNo seqNo) {
    tx.MutableSeqNo()->SetGeneration(seqNo.Generation);
    tx.MutableSeqNo()->SetRound(seqNo.Round);
}

TString TSchemeShard::FillAlterTableTxBody(TPathId pathId, TShardIdx shardIdx, TMessageSeqNo seqNo) const {
    Y_VERIFY_S(Tables.contains(pathId), "Unknown table " << pathId);
    Y_VERIFY_S(PathsById.contains(pathId), "Unknown path " << pathId);

    TPathElement::TPtr path = PathsById.at(pathId);
    TTableInfo::TPtr tableInfo = Tables.at(pathId);
    TTableInfo::TAlterDataPtr alterData = tableInfo->AlterData;

    Y_VERIFY_S(alterData, "No alter data for table " << pathId);

    NKikimrTxDataShard::TFlatSchemeTransaction tx;
    FillSeqNo(tx, seqNo);
    auto proto = tx.MutableAlterTable();
    FillTableSchemaVersion(alterData->AlterVersion, proto);
    proto->SetName(path->Name);

    proto->SetId_Deprecated(pathId.LocalPathId);
    pathId.ToProto(proto->MutablePathId());

    for (const auto& col : alterData->Columns) {
        const TTableInfo::TColumn& colInfo = col.second;
        if (colInfo.IsDropped()) {
            auto descr = proto->AddDropColumns();
            descr->SetName(colInfo.Name);
            descr->SetId(colInfo.Id);
            auto columnType = NScheme::ProtoColumnTypeFromTypeInfoMod(colInfo.PType, colInfo.PTypeMod);
            descr->SetTypeId(columnType.TypeId);
            if (columnType.TypeInfo) {
                *descr->MutableTypeInfo() = *columnType.TypeInfo;
            }
        } else {
            auto descr = proto->AddColumns();
            descr->SetName(colInfo.Name);
            descr->SetId(colInfo.Id);
            auto columnType = NScheme::ProtoColumnTypeFromTypeInfoMod(colInfo.PType, colInfo.PTypeMod);
            descr->SetTypeId(columnType.TypeId);
            if (columnType.TypeInfo) {
                *descr->MutableTypeInfo() = *columnType.TypeInfo;
            }
            descr->SetFamily(colInfo.Family);
            descr->SetNotNull(colInfo.NotNull);
            descr->SetSetNotNullInProgress(colInfo.SetNotNullInProgress);
        }
    }

    for (ui32 keyId : alterData->KeyColumnIds) {
        proto->AddKeyColumnIds(keyId);
    }

    proto->MutablePartitionConfig()->CopyFrom(alterData->PartitionConfigCompatible());

    if (auto* patch = tableInfo->PerShardPartitionConfig.FindPtr(shardIdx)) {
        ApplyPartitionConfigStoragePatch(
            *proto->MutablePartitionConfig(),
            *patch);
    }

    if (alterData->TableDescriptionFull.Defined() && alterData->TableDescriptionFull->HasReplicationConfig()) {
        proto->MutableReplicationConfig()->CopyFrom(alterData->TableDescriptionFull->GetReplicationConfig());
    } else if (tableInfo->HasReplicationConfig()) {
        proto->MutableReplicationConfig()->CopyFrom(tableInfo->ReplicationConfig());
    }

    if (alterData->TableDescriptionFull.Defined() && alterData->TableDescriptionFull->HasIncrementalBackupConfig()) {
        proto->MutableIncrementalBackupConfig()->CopyFrom(alterData->TableDescriptionFull->GetIncrementalBackupConfig());
    } else if (tableInfo->HasIncrementalBackupConfig()) {
        proto->MutableIncrementalBackupConfig()->CopyFrom(tableInfo->IncrementalBackupConfig());
    }

    TString txBody;
    Y_PROTOBUF_SUPPRESS_NODISCARD tx.SerializeToString(&txBody);
    return txBody;
}

bool TSchemeShard::FillSplitPartitioning(TVector<TString>& rangeEnds, const TConstArrayRef<NScheme::TTypeInfo>& keyColTypes,
                                             const::google::protobuf::RepeatedPtrField<NKikimrSchemeOp::TSplitBoundary> &boundaries,
                                             TString& errStr) {
    for (int i = 0; i < boundaries.size(); ++i) {
        // Convert split boundary to serialized range end
        auto& boundary = boundaries.Get(i);
        TVector<TCell> rangeEnd;
        TSerializedCellVec prefix;
        TVector<TString> memoryOwner;
        if (boundary.HasSerializedKeyPrefix()) {
            prefix.Parse(boundary.GetSerializedKeyPrefix());
            rangeEnd = TVector<TCell>(prefix.GetCells().begin(), prefix.GetCells().end());
        } else if (!NMiniKQL::CellsFromTuple(nullptr, boundary.GetKeyPrefix(), keyColTypes, {}, false, rangeEnd, errStr, memoryOwner)) {
            errStr = Sprintf("Error at split boundary %d: %s", i, errStr.data());
            return false;
        }
        rangeEnd.resize(keyColTypes.size());     // Extend with NULLs
        rangeEnds.push_back(TSerializedCellVec::Serialize(rangeEnd));
    }
    return true;
}

void TSchemeShard::ApplyPartitionConfigStoragePatch(
        NKikimrSchemeOp::TPartitionConfig& config,
        const NKikimrSchemeOp::TPartitionConfig& patch) const
{
    THashMap<ui32, ui32> familyRooms;
    for (const auto& family : patch.GetColumnFamilies()) {
        familyRooms[family.GetId()] = family.GetRoom();
    }

    // Patch column families
    for (size_t i = 0; i < config.ColumnFamiliesSize(); ++i) {
        auto& family = *config.MutableColumnFamilies(i);
        auto it = familyRooms.find(family.GetId());
        if (it != familyRooms.end()) {
            family.SetRoom(it->second);
        } else {
            family.ClearRoom();
        }
    }

    // Copy storage rooms as is
    config.ClearStorageRooms();
    if (patch.StorageRoomsSize()) {
        config.MutableStorageRooms()->CopyFrom(patch.GetStorageRooms());
    }
}

std::optional<TTempDirInfo> TSchemeShard::ResolveTempDirInfo(const TPathId& pathId) {
    auto path = TPath::Init(pathId, this);
    if (!path) {
        return std::nullopt;
    }
    TTempDirInfo info;
    info.Name = path.LeafName();
    info.WorkingDir = path.Parent().PathString();

    auto pathInfo = PathsById.at(path.Base()->PathId);
    if (!pathInfo) {
        return std::nullopt;
    }
    if (!pathInfo->TempDirOwnerActorId) {
        return std::nullopt;
    }

    info.TempDirOwnerActorId = pathInfo->TempDirOwnerActorId;
    return info;
}

// Fills CreateTable transaction for datashard with the specified range
void TSchemeShard::FillTableDescriptionForShardIdx(
        TPathId tableId, TShardIdx shardIdx, NKikimrSchemeOp::TTableDescription* tableDescr,
        TString rangeBegin, TString rangeEnd,
        bool rangeBeginInclusive, bool rangeEndInclusive, bool newTable)
{
    Y_VERIFY_S(Tables.contains(tableId), "Unknown table id " << tableId);
    const TTableInfo::TPtr tinfo = Tables.at(tableId);
    TPathElement::TPtr pinfo = *PathsById.FindPtr(tableId);

    TVector<ui32> keyColumnIds = tinfo->FillDescriptionCache(pinfo);
    if (!tinfo->TableDescription.HasPath()) {
        tinfo->TableDescription.SetPath(PathToString(pinfo));
    }
    tableDescr->CopyFrom(tinfo->TableDescription);

    if (rangeBegin.empty()) {
        // First partition starts with <NULL, NULL, ..., NULL> key
        TVector<TCell> nullKey(keyColumnIds.size());
        rangeBegin = TSerializedCellVec::Serialize(nullKey);
    }

    tableDescr->SetPartitionRangeBegin(std::move(rangeBegin));
    tableDescr->SetPartitionRangeEnd(std::move(rangeEnd));
    tableDescr->SetPartitionRangeBeginIsInclusive(rangeBeginInclusive);
    tableDescr->SetPartitionRangeEndIsInclusive(rangeEndInclusive);

    // Patch partition config for new-style shards
    if (const auto* patch = tinfo->PerShardPartitionConfig.FindPtr(shardIdx)) {
        ApplyPartitionConfigStoragePatch(
            *tableDescr->MutablePartitionConfig(),
            *patch);
    }

    if (tinfo->IsBackup) {
        tableDescr->SetIsBackup(true);
    }

    if (tinfo->IsRestore) {
        tableDescr->SetIsRestore(true);
    }

    if (tinfo->HasReplicationConfig()) {
        tableDescr->MutableReplicationConfig()->CopyFrom(tinfo->ReplicationConfig());
    }

    if (tinfo->HasIncrementalBackupConfig()) {
        tableDescr->MutableIncrementalBackupConfig()->CopyFrom(tinfo->IncrementalBackupConfig());
    }

    if (AppData()->DisableRichTableDescriptionForTest) {
        return;
    }

    // Fill indexes & cdc streams (if any)
    for (const auto& child : pinfo->GetChildren()) {
        const auto& childName = child.first;
        const auto& childPathId = child.second;

        Y_ABORT_UNLESS(PathsById.contains(childPathId));
        auto childPath = PathsById.at(childPathId);

        if (childPath->Dropped() || childPath->PlannedToDrop()) {
            continue;
        }

        switch (childPath->PathType) {
            case NKikimrSchemeOp::EPathTypeTableIndex: {
                Y_ABORT_UNLESS(Indexes.contains(childPathId));
                auto info = Indexes.at(childPathId);
                DescribeTableIndex(childPathId, childName, newTable ? info->AlterData : info, false, false,
                    *tableDescr->MutableTableIndexes()->Add()
                );
                break;
            }

            case NKikimrSchemeOp::EPathTypeCdcStream: {
                Y_VERIFY_S(CdcStreams.contains(childPathId), "Cdc stream not found"
                    << ": pathId# " << childPathId
                    << ", name# " << childName);
                auto info = CdcStreams.at(childPathId);
                DescribeCdcStream(childPathId, childName, info, *tableDescr->MutableCdcStreams()->Add());
                break;
            }

            case NKikimrSchemeOp::EPathTypeSequence: {
                Y_VERIFY_S(Sequences.contains(childPathId), "Sequence not found"
                    << ": path#d# " << childPathId
                    << ", name# " << childName);
                auto info = Sequences.at(childPathId);
                DescribeSequence(childPathId, childName, info, *tableDescr->MutableSequences()->Add());
                break;
            }

            case NKikimrSchemeOp::EPathTypeTable: {
                // TODO: move BackupImplTable under special scheme element
                break;
            }

            default:
                Y_FAIL_S("Unexpected table's child"
                    << ": tableId# " << tableId
                    << ", childId# " << childPathId
                    << ", childName# " << childName
                    << ", childType# " << static_cast<ui32>(childPath->PathType));
        }
    }
}

// Fills CreateTable transaction that is sent to datashards
void TSchemeShard::FillTableDescription(TPathId tableId, ui32 partitionIdx, ui64 schemaVersion,
    NKikimrSchemeOp::TTableDescription* tableDescr)
{
    Y_VERIFY_S(Tables.contains(tableId), "Unknown table id " << tableId);
    const TTableInfo::TPtr tinfo = Tables.at(tableId);

    TString rangeBegin = (partitionIdx != 0)
        ? tinfo->GetPartitions()[partitionIdx-1]->EndOfRange
        : TString();
    TString rangeEnd = tinfo->GetPartitions()[partitionIdx]->EndOfRange;

    // For uniform partitioning we include range start and exclude range end
    FillTableDescriptionForShardIdx(
        tableId,
        tinfo->GetPartitions()[partitionIdx]->ShardIdx,
        tableDescr,
        std::move(rangeBegin),
        std::move(rangeEnd),
        true /* rangeBeginInclusive */, false /* rangeEndInclusive */, true /* newTable */);
    FillTableSchemaVersion(schemaVersion, tableDescr);
}

bool TSchemeShard::FillUniformPartitioning(TVector<TString>& rangeEnds, ui32 keySize, NScheme::TTypeInfo firstKeyColType, ui32 partitionCount, const NScheme::TTypeRegistry* typeRegistry, TString& errStr) {
    Y_UNUSED(typeRegistry);
    if (partitionCount > 1) {
        // RangeEnd key will have first cell with non-NULL value and rest of the cells with NULLs
        TVector<TCell> rangeEnd(keySize);
        ui64 maxVal = 0;
        ui32 valSz = 0;

        // Check that first key column has integer type
        auto typeId = firstKeyColType.GetTypeId();
        switch(typeId) {
        case NScheme::NTypeIds::Uint32:
            maxVal = Max<ui32>();
            valSz = 4;
            break;
        case NScheme::NTypeIds::Uint64:
            maxVal = Max<ui64>();
            valSz = 8;
            break;
        case NScheme::NTypeIds::Uuid: {
            maxVal = Max<ui64>();
            valSz = 16;
            char buffer[16] = {};

            for (ui32 i = 1; i < partitionCount; ++i) {
                ui64 val = maxVal * (double(i) / partitionCount);
                // Make sure most significant byte is at the start of the byte buffer for UUID comparison.
                val = HostToInet(val);
                WriteUnaligned<ui64>(buffer, val);
                rangeEnd[0] = TCell(buffer, valSz);
                rangeEnds.push_back(TSerializedCellVec::Serialize(rangeEnd));
            }

            return true;
        }
        default:
            errStr = TStringBuilder() << "Unsupported first key column type " << NScheme::TypeName(firstKeyColType) << ", only Uint32 and Uint64 are supported";
            return false;
        }

        // Generate range boundaries
        for (ui32 i = 1; i < partitionCount; ++i) {
            ui64 val = maxVal * (double(i)/partitionCount);
            rangeEnd[0] = TCell((const char*)&val, valSz);
            rangeEnds.push_back(TSerializedCellVec::Serialize(rangeEnd));
        }
    }
    return true;
}

void TSchemeShard::SetPartitioning(TPathId pathId, const std::vector<TShardIdx>& partitioning) {
    TVector<std::pair<ui64, ui64>> shardIndices;
    shardIndices.reserve(partitioning.size());
    for (auto& shardIdx : partitioning) {
        shardIndices.emplace_back(ui64(shardIdx.GetOwnerId()), ui64(shardIdx.GetLocalId()));
    }

    auto path = TPath::Init(pathId, this);
    auto ev = MakeHolder<NSysView::TEvSysView::TEvSetPartitioning>(GetDomainKey(pathId), pathId, path.PathString());
    ev->ShardIndices.swap(shardIndices);
    Send(SysPartitionStatsCollector, ev.Release());
}

void TSchemeShard::SetPartitioning(TPathId pathId, TOlapStoreInfo::TPtr storeInfo) {
    SetPartitioning(pathId, storeInfo->GetColumnShards());
}

void TSchemeShard::SetPartitioning(TPathId pathId, TColumnTableInfo::TPtr tableInfo) {
    SetPartitioning(pathId, tableInfo->BuildOwnedColumnShardsVerified());
}

void TSchemeShard::SetPartitioning(TPathId pathId, TTableInfo::TPtr tableInfo, TVector<TTableShardInfo>&& newPartitioning) {
    TVector<std::pair<ui64, ui64>> shardIndices;
    shardIndices.reserve(newPartitioning.size());
    for (auto& info : newPartitioning) {
        shardIndices.push_back(
            std::make_pair(ui64(info.ShardIdx.GetOwnerId()), ui64(info.ShardIdx.GetLocalId()))
        );
    }

    auto path = TPath::Init(pathId, this);
    auto ev = MakeHolder<NSysView::TEvSysView::TEvSetPartitioning>(GetDomainKey(pathId), pathId, path.PathString());
    ev->ShardIndices.swap(shardIndices);
    Send(SysPartitionStatsCollector, ev.Release());

    tableInfo->SetPartitioning(std::move(newPartitioning));

    // report TTableInfo::VerifyConsistency() time
    TabletCounters->Cumulative()[COUNTER_TABLE_PARTITIONS_CONSISTENCY_CHECK_TIME_NS].Increment(tableInfo->LastVerifyConsistencyTime);
}

void TSchemeShard::MovePartitioning(TPathId pathId, TTableInfo::TPtr tableInfo, TVector<TTableShardInfo>&& newPartitioning) {
    TVector<std::pair<ui64, ui64>> shardIndices;
    shardIndices.reserve(newPartitioning.size());
    for (auto& info : newPartitioning) {
        shardIndices.push_back(
            std::make_pair(ui64(info.ShardIdx.GetOwnerId()), ui64(info.ShardIdx.GetLocalId()))
        );
    }
    auto path = TPath::Init(pathId, this);
    auto ev = MakeHolder<NSysView::TEvSysView::TEvSetPartitioning>(GetDomainKey(pathId), pathId, path.PathString());
    ev->ShardIndices.swap(shardIndices);
    Send(SysPartitionStatsCollector, ev.Release());

    if (!tableInfo->IsBackup) {
        const auto& partitionStats = tableInfo->GetStats().PartitionStats;
        const TInstant now = AppData()->TimeProvider->Now();
        for (const auto& p : newPartitioning) {
            auto it = partitionStats.find(p.ShardIdx);
            if (it != partitionStats.end()) {
                EnqueueBackgroundCompaction(p.ShardIdx, it->second);
                UpdateShardMetrics(p.ShardIdx, it->second, now);
            }
        }
        // No OnShardRemoved — same physical shards, just a new path.
    }

    tableInfo->MovePartitioning(std::move(newPartitioning));

    // report TTableInfo::VerifyConsistency() time
    TabletCounters->Cumulative()[COUNTER_TABLE_PARTITIONS_CONSISTENCY_CHECK_TIME_NS].Increment(tableInfo->LastVerifyConsistencyTime);
}

void TSchemeShard::CopyPartitioning(TPathId pathId, TTableInfo::TPtr tableInfo, TVector<TTableShardInfo>&& newPartitioning) {
    TVector<std::pair<ui64, ui64>> shardIndices;
    shardIndices.reserve(newPartitioning.size());
    for (auto& info : newPartitioning) {
        shardIndices.push_back(
            std::make_pair(ui64(info.ShardIdx.GetOwnerId()), ui64(info.ShardIdx.GetLocalId()))
        );
    }
    auto path = TPath::Init(pathId, this);
    auto ev = MakeHolder<NSysView::TEvSysView::TEvSetPartitioning>(GetDomainKey(pathId), pathId, path.PathString());
    ev->ShardIndices.swap(shardIndices);
    Send(SysPartitionStatsCollector, ev.Release());

    if (!tableInfo->IsBackup) {
        for (const auto& shardIdx : tableInfo->GetPartitionStore() | std::views::keys) {
            OnShardRemoved(shardIdx); // note that queues might not contain the shard
        }
        // No EnqueueBackgroundCompaction — new shards have no stats yet.
    }

    tableInfo->CopyPartitioning(std::move(newPartitioning));

    // report TTableInfo::VerifyConsistency() time
    TabletCounters->Cumulative()[COUNTER_TABLE_PARTITIONS_CONSISTENCY_CHECK_TIME_NS].Increment(tableInfo->LastVerifyConsistencyTime);
}

void TSchemeShard::ApplySplitMerge(
    TPathId pathId,
    TTableInfo::TPtr tableInfo,
    TVector<TTableShardInfo>&& dstPartitions,
    const TVector<TShardIdx>& removedShards,
    ui64 splitStartIdx
) {
    const TInstant now = AppData()->TimeProvider->Now();
    if (!tableInfo->IsBackup) {
        for (const TShardIdx& shardIdx : removedShards) {
            OnShardRemoved(shardIdx);
        }
        // New dst shards have no stats yet; they are enqueued on their first EvPeriodicTableStats.
        const auto& partitionStats = tableInfo->GetStats().PartitionStats;
        for (const auto& dst : dstPartitions) {
            auto it = partitionStats.find(dst.ShardIdx);
            if (it != partitionStats.end()) {
                EnqueueBackgroundCompaction(dst.ShardIdx, it->second);
                UpdateShardMetrics(dst.ShardIdx, it->second, now);
            }
        }
    }

    tableInfo->ApplySplitMerge(std::move(dstPartitions), removedShards, splitStartIdx, now);

    // report TTableInfo::VerifyConsistency() time
    TabletCounters->Cumulative()[COUNTER_TABLE_PARTITIONS_CONSISTENCY_CHECK_TIME_NS].Increment(tableInfo->LastVerifyConsistencyTime);

    TVector<std::pair<ui64, ui64>> shardIndices;
    shardIndices.reserve(tableInfo->GetPartitions().size());
    for (const auto* info : tableInfo->GetPartitions()) {
        shardIndices.push_back(
            std::make_pair(ui64(info->ShardIdx.GetOwnerId()), ui64(info->ShardIdx.GetLocalId()))
        );
    }
    auto path = TPath::Init(pathId, this);
    auto ev = MakeHolder<NSysView::TEvSysView::TEvSetPartitioning>(GetDomainKey(pathId), pathId, path.PathString());
    ev->ShardIndices.swap(shardIndices);
    Send(SysPartitionStatsCollector, ev.Release());
}

void TSchemeShard::OnShardRemoved(const TShardIdx& shardIdx) {
    RemoveBackgroundCompaction(shardIdx);
    RemoveBorrowedCompaction(shardIdx);
    RemoveShardMetrics(shardIdx);
}

void TSchemeShard::FillAsyncIndexInfo(const TPathId& tableId, NKikimrTxDataShard::TFlatSchemeTransaction& tx) {
    Y_ABORT_UNLESS(PathsById.contains(tableId));

    auto parent = TPath::Init(tableId, this).Parent();
    Y_ABORT_UNLESS(parent.IsResolved());

    if (!parent.Base()->IsTableIndex()) {
        return;
    }

    Y_ABORT_UNLESS(Indexes.contains(parent.Base()->PathId));
    auto index = Indexes.at(parent.Base()->PathId);

    if (index->Type == TTableIndexInfo::EType::EIndexTypeGlobalAsync) {
        tx.MutableAsyncIndexInfo();
    }
}

bool TSchemeShard::ReadSysValue(NIceDb::TNiceDb &db, ui64 sysTag, TString &value, TString defValue) {
    auto sysParamsRowset = db.Table<Schema::SysParams>().Key(sysTag).Select<Schema::SysParams::Value>();
    if (!sysParamsRowset.IsReady()) {
        return false;
    }

    if (!sysParamsRowset.IsValid()) {
        value = defValue;
        return true;
    }

    value = sysParamsRowset.GetValue<Schema::SysParams::Value>();
    return true;
}

bool TSchemeShard::ReadSysValue(NIceDb::TNiceDb &db, ui64 sysTag, ui64 &value, ui64 defVal) {
    auto sysParamsRowset = db.Table<Schema::SysParams>().Key(sysTag).Select<Schema::SysParams::Value>();
    if (!sysParamsRowset.IsReady()) {
        return false;
    }

    if (!sysParamsRowset.IsValid()) {
        value = defVal;
        return true;
    }

    TString rawValue = sysParamsRowset.GetValue<Schema::SysParams::Value>(); \
    value = FromString<ui64>(rawValue);

    return true;
}

void TSchemeShard::SubscribeConsoleConfigs(const TActorContext &ctx) {
    ctx.Send(
        NConsole::MakeConfigsDispatcherID(ctx.SelfID.NodeId()),
        new NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest({
            (ui32)NKikimrConsole::TConfigItem::FeatureFlagsItem,
            (ui32)NKikimrConsole::TConfigItem::CompactionConfigItem,
            (ui32)NKikimrConsole::TConfigItem::SchemeShardConfigItem,
            (ui32)NKikimrConsole::TConfigItem::TableProfilesConfigItem,
            (ui32)NKikimrConsole::TConfigItem::QueryServiceConfigItem,
        }),
        IEventHandle::FlagTrackDelivery
    );
    ctx.Schedule(TDuration::Seconds(15), new TEvPrivate::TEvConsoleConfigsTimeout);
}

void TSchemeShard::Handle(TEvPrivate::TEvConsoleConfigsTimeout::TPtr&, const TActorContext& ctx) {
    LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Cannot get console configs");
    LoadTableProfiles(nullptr, ctx);
}

void TSchemeShard::Handle(TEvents::TEvUndelivered::TPtr& ev, const TActorContext& ctx) {
    if (CheckOwnerUndelivered(ev)) {
        return;
    }
    LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Cannot subscribe to console configs");
    LoadTableProfiles(nullptr, ctx);
}

void TSchemeShard::ApplyConsoleConfigs(const NKikimrConfig::TAppConfig& appConfig, const TActorContext& ctx) {
    if (appConfig.HasFeatureFlags()) {
        ApplyConsoleConfigs(appConfig.GetFeatureFlags(), ctx);
    }

    if (appConfig.HasCompactionConfig()) {
        const auto& compactionConfig = appConfig.GetCompactionConfig();
        ConfigureCompactionQueues(compactionConfig, ctx);
    }

    if (appConfig.HasBackgroundCleaningConfig()) {
        const auto& backgroundCleaningConfig = appConfig.GetBackgroundCleaningConfig();
        ConfigureBackgroundCleaningQueue(backgroundCleaningConfig, ctx);
    }

    if (appConfig.HasDataErasureConfig()) {
        const auto& shredConfig = appConfig.GetDataErasureConfig();
        ConfigureShredManager(shredConfig);
    }

    if (appConfig.HasSchemeShardConfig()) {
        const auto& schemeShardConfig = appConfig.GetSchemeShardConfig();
        ConfigureStatsBatching(schemeShardConfig, ctx);
        ConfigureStatsOperations(schemeShardConfig, ctx);
        MaxCdcInitialScanShardsInFlight = schemeShardConfig.GetMaxCdcInitialScanShardsInFlight();
        MaxRestoreBuildIndexShardsInFlight = schemeShardConfig.GetMaxRestoreBuildIndexShardsInFlight();
        MaxBuildIndexShardsInFlight = schemeShardConfig.GetMaxBuildIndexShardsInFlight();
        MaxStoredIndexBuilds = schemeShardConfig.GetMaxStoredIndexBuilds();
        ConfigureCondErase(schemeShardConfig, ctx);
    }

    if (appConfig.HasTableProfilesConfig()) {
        LoadTableProfiles(&appConfig.GetTableProfilesConfig(), ctx);
    } else {
        LoadTableProfiles(nullptr, ctx);
    }

    if (appConfig.HasQueryServiceConfig()) {
        const auto& queryServiceConfig = appConfig.GetQueryServiceConfig();
        ConfigureExternalSources(queryServiceConfig, ctx);
    }

    if (appConfig.HasAuthConfig()) {
        ConfigureLoginProvider(appConfig.GetAuthConfig(), ctx);
        ConfigureAccountLockout(appConfig.GetAuthConfig(), ctx);
    }

    if (IsSchemeShardConfigured()) {
        StartStopCompactionQueues();
        if (BackgroundCleaningQueue) {
            BackgroundCleaningQueue->Start();
        }

        StartStopShred();
    }
}

void TSchemeShard::ApplyConsoleConfigs(const NKikimrConfig::TFeatureFlags& featureFlags, const TActorContext& ctx) {
    if (featureFlags.GetAllowServerlessStorageBillingForSchemeShard() != (bool)AllowServerlessStorageBilling) {
        LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "ApplyConsoleConfigs: AllowServerlessStorageBillingForSchemeShard has been changed"
                     << ", schemeshardId: " << SelfTabletId()
                     << ", old: " << (bool)AllowServerlessStorageBilling
                     << ", new: " << (bool)featureFlags.GetAllowServerlessStorageBillingForSchemeShard());
        AllowServerlessStorageBilling = (i64)featureFlags.GetAllowServerlessStorageBillingForSchemeShard();
    }

    EnableBackgroundCompaction = featureFlags.GetEnableBackgroundCompaction();
    EnableBackgroundCompactionServerless = featureFlags.GetEnableBackgroundCompactionServerless();
    EnableBorrowedSplitCompaction = featureFlags.GetEnableBorrowedSplitCompaction();
    EnableMoveIndex = featureFlags.GetEnableMoveIndex();
    EnableAlterDatabaseCreateHiveFirst = featureFlags.GetEnableAlterDatabaseCreateHiveFirst();
    EnableStatistics = featureFlags.GetEnableStatistics();
    EnableServerlessExclusiveDynamicNodes = featureFlags.GetEnableServerlessExclusiveDynamicNodes();
    EnableAddColumsWithDefaults = featureFlags.GetEnableAddColumsWithDefaults();
    EnableTempTables = featureFlags.GetEnableTempTables();
    EnableReplaceIfExistsForExternalEntities = featureFlags.GetEnableReplaceIfExistsForExternalEntities();
    EnableResourcePoolsOnServerless = featureFlags.GetEnableResourcePoolsOnServerless();
    EnableInitialUniqueIndex = featureFlags.GetEnableUniqConstraint();
    EnableAddUniqueIndex = featureFlags.GetEnableAddUniqueIndex();
    EnableFulltextIndex = featureFlags.GetEnableFulltextIndex();
    EnableCompactFulltextIndex = featureFlags.GetEnableCompactFulltextIndex();
    EnableJsonIndex = featureFlags.GetEnableJsonIndex();
    EnableExternalDataSourcesOnServerless = featureFlags.GetEnableExternalDataSourcesOnServerless();
    EnableShred = featureFlags.GetEnableDataErasure();
    EnableExternalSourceSchemaInference = featureFlags.GetEnableExternalSourceSchemaInference();
}

void TSchemeShard::ConfigureStatsBatching(const NKikimrConfig::TSchemeShardConfig& config, const TActorContext& ctx) {
    StatsBatchTimeout = TDuration::MilliSeconds(config.GetStatsBatchTimeoutMs());
    StatsMaxBatchSize = config.GetStatsMaxBatchSize();
    StatsMaxExecuteTime = TDuration::MilliSeconds(config.GetStatsMaxExecuteMs());
    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                 "StatsBatching config: StatsBatchTimeout# " << StatsBatchTimeout
                 << ", StatsMaxBatchSize# " << StatsMaxBatchSize
                 << ", StatsMaxExecuteTime# " << StatsMaxExecuteTime);
}

void TSchemeShard::ConfigureStatsOperations(const NKikimrConfig::TSchemeShardConfig& config, const TActorContext& ctx) {
    for (const auto& operationConfig: config.GetInFlightCounterConfig()) {
        ui32 limit = operationConfig.GetInFlightLimit();
        auto txState = ConvertToTxType(operationConfig.GetType());
        InFlightLimits[txState] = limit;
    }

    if (InFlightLimits.empty()) {
        NKikimrConfig::TSchemeShardConfig_TInFlightCounterConfig inFlightCounterConfig;
        auto defaultInFlightLimit = inFlightCounterConfig.GetInFlightLimit();
        InFlightLimits[TTxState::ETxType::TxSplitTablePartition] = defaultInFlightLimit;
        InFlightLimits[TTxState::ETxType::TxMergeTablePartition] = defaultInFlightLimit;
        LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "OperationsProcessing config: using default configuration");
    }

    for (auto it = InFlightLimits.begin(); it != InFlightLimits.end(); ++it) {
        LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "OperationsProcessing config: type " << TTxState::TypeName(it->first)
                    << ", limit " << it->second);
    }
}

void TSchemeShard::ConfigureCompactionQueues(
    const NKikimrConfig::TCompactionConfig& compactionConfig,
    const TActorContext &ctx)
{
    if (compactionConfig.HasBackgroundCompactionConfig()) {
        ConfigureBackgroundCompactionQueue(compactionConfig.GetBackgroundCompactionConfig(), ctx);
    } else {
        ConfigureBackgroundCompactionQueue(NKikimrConfig::TCompactionConfig::TBackgroundCompactionConfig(), ctx);
    }

    if (compactionConfig.HasBorrowedCompactionConfig()) {
        ConfigureBorrowedCompactionQueue(compactionConfig.GetBorrowedCompactionConfig(), ctx);
    } else {
        ConfigureBorrowedCompactionQueue(NKikimrConfig::TCompactionConfig::TBorrowedCompactionConfig(), ctx);
    }

    if (compactionConfig.HasForcedCompactionConfig()) {
        ConfigureForcedCompactionQueue(compactionConfig.GetForcedCompactionConfig(), ctx);
    } else {
        ConfigureForcedCompactionQueue(NKikimrConfig::TCompactionConfig::TForcedCompactionConfig(), ctx);
    }
}

void TSchemeShard::ConfigureBackgroundCompactionQueue(
    const NKikimrConfig::TCompactionConfig::TBackgroundCompactionConfig& config,
    const TActorContext &ctx)
{
    // note that we use TCompactionQueueImpl::TConfig
    // instead of its base NOperationQueue::TConfig
    TCompactionQueueImpl::TConfig queueConfig;
    queueConfig.SearchHeightThreshold = config.GetSearchHeightThreshold();
    queueConfig.RowDeletesThreshold = config.GetRowDeletesThreshold();
    queueConfig.RowCountThreshold = config.GetRowCountThreshold();
    queueConfig.CompactSinglePartedShards = config.GetCompactSinglePartedShards();

    TBackgroundCompactionQueue::TConfig compactionConfig;

    // schemeshard specific
    compactionConfig.IsCircular = true;

    compactionConfig.Timeout = TDuration::Seconds(config.GetTimeoutSeconds());
    compactionConfig.WakeupInterval = TDuration::Seconds(config.GetWakeupIntervalSeconds());
    compactionConfig.MinWakeupInterval = TDuration::MilliSeconds(config.GetMinWakeupIntervalMs());
    compactionConfig.InflightLimit = config.GetInflightLimit();
    compactionConfig.RoundInterval = TDuration::Seconds(config.GetRoundSeconds());
    compactionConfig.MaxRate = config.GetMaxRate();
    compactionConfig.MinOperationRepeatDelay = TDuration::Seconds(config.GetMinCompactionRepeatDelaySeconds());

    if (BackgroundCompactionQueue) {
        BackgroundCompactionQueue->UpdateConfig(compactionConfig, queueConfig);
    } else {
        BackgroundCompactionQueue = new TBackgroundCompactionQueue(
            compactionConfig,
            queueConfig,
            BackgroundCompactionStarter);
        ctx.RegisterWithSameMailbox(BackgroundCompactionQueue);
    }

    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                 "BackgroundCompactionQueue configured: Timeout# " << compactionConfig.Timeout
                 << ", compact single parted# " << (queueConfig.CompactSinglePartedShards ? "yes" : "no")
                 << ", Rate# " << BackgroundCompactionQueue->GetRate()
                 << ", WakeupInterval# " << compactionConfig.WakeupInterval
                 << ", RoundInterval# " << compactionConfig.RoundInterval
                 << ", InflightLimit# " << compactionConfig.InflightLimit
                 << ", MinCompactionRepeatDelaySeconds# " << compactionConfig.MinOperationRepeatDelay
                 << ", MaxRate# " << compactionConfig.MaxRate);
}

void TSchemeShard::ConfigureBorrowedCompactionQueue(
    const NKikimrConfig::TCompactionConfig::TBorrowedCompactionConfig& config,
    const TActorContext &ctx)
{
    TBorrowedCompactionQueue::TConfig compactionConfig;

    compactionConfig.IsCircular = false;
    compactionConfig.Timeout = TDuration::Seconds(config.GetTimeoutSeconds());
    compactionConfig.MinWakeupInterval = TDuration::MilliSeconds(config.GetMinWakeupIntervalMs());
    compactionConfig.InflightLimit = config.GetInflightLimit();
    compactionConfig.MaxRate = config.GetMaxRate();

    if (BorrowedCompactionQueue) {
        BorrowedCompactionQueue->UpdateConfig(compactionConfig);
    } else {
        BorrowedCompactionQueue = new TBorrowedCompactionQueue(
            compactionConfig,
            BorrowedCompactionStarter);
        ctx.RegisterWithSameMailbox(BorrowedCompactionQueue);
    }

    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                 "BorrowedCompactionQueue configured: Timeout# " << compactionConfig.Timeout
                 << ", Rate# " << BorrowedCompactionQueue->GetRate()
                 << ", WakeupInterval# " << compactionConfig.WakeupInterval
                 << ", InflightLimit# " << compactionConfig.InflightLimit);
}

void TSchemeShard::ConfigureForcedCompactionQueue(
    const NKikimrConfig::TCompactionConfig::TForcedCompactionConfig& config,
    const TActorContext &ctx)
{
    TForcedCompactionQueue::TConfig compactionConfig;

    compactionConfig.IsCircular = false;
    compactionConfig.Timeout = TDuration::Seconds(config.GetTimeoutSeconds());
    compactionConfig.MinWakeupInterval = TDuration::MilliSeconds(config.GetMinWakeupIntervalMs());
    compactionConfig.InflightLimit = config.GetInflightLimit();
    compactionConfig.MaxRate = config.GetMaxRate();

    if (ForcedCompactionQueue) {
        ForcedCompactionQueue->UpdateConfig(compactionConfig);
    } else {
        ForcedCompactionQueue = new TForcedCompactionQueue(
            compactionConfig,
            ForcedCompactionStarter);
        ctx.RegisterWithSameMailbox(ForcedCompactionQueue);
    }

    ForcedCompactionPersistBatchSize = config.GetPersistBatchSize();
    ForcedCompactionPersistBatchMaxTime = TDuration::MilliSeconds(config.GetPersistBatchMaxTimeMs());

    ForcedCompactionStoredOperationsLimit = config.GetStoredOperationsLimit();
    ForcedCompactionAutoForgetOperations = config.GetAutoForgetOperations();

    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                 "ForcedCompactionQueue configured: Timeout# " << compactionConfig.Timeout
                 << ", Rate# " << ForcedCompactionQueue->GetRate()
                 << ", WakeupInterval# " << compactionConfig.WakeupInterval
                 << ", InflightLimit# " << compactionConfig.InflightLimit
                 << ", ForcedCompactionPersistBatchSize# " << ForcedCompactionPersistBatchSize
                 << ", ForcedCompactionPersistBatchMaxTime# " << ForcedCompactionPersistBatchMaxTime
                 << ", ForcedCompactionStoredOperationsLimit# " << ForcedCompactionStoredOperationsLimit
                 << ", ForcedCompactionAutoForgetOperations# " << ForcedCompactionAutoForgetOperations);
}

void TSchemeShard::ConfigureBackgroundCleaningQueue(
    const NKikimrConfig::TBackgroundCleaningConfig& config,
    const TActorContext &ctx)
{
    TBackgroundCleaningQueue::TConfig cleaningConfig;

    cleaningConfig.IsCircular = false;
    cleaningConfig.Timeout = TDuration::Seconds(config.GetTimeoutSeconds());
    cleaningConfig.MinWakeupInterval = TDuration::MilliSeconds(config.GetMinWakeupIntervalMs());
    cleaningConfig.InflightLimit = config.GetInflightLimit();
    cleaningConfig.MaxRate = config.GetMaxRate();

    if (config.HasRetrySettings()) {
        BackgroundCleaningRetrySettings = config.GetRetrySettings();
    }

    if (BackgroundCleaningQueue) {
        BackgroundCleaningQueue->UpdateConfig(cleaningConfig);
    } else {
        BackgroundCleaningQueue = new TBackgroundCleaningQueue(
            cleaningConfig,
            BackgroundCleaningStarter);
        ctx.RegisterWithSameMailbox(BackgroundCleaningQueue);
    }

    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                 "BackgroundCleaningQueue configured: Timeout# " << cleaningConfig.Timeout
                 << ", Rate# " << BackgroundCleaningQueue->GetRate()
                 << ", WakeupInterval# " << cleaningConfig.WakeupInterval
                 << ", InflightLimit# " << cleaningConfig.InflightLimit);
}

void TSchemeShard::ConfigureLoginProvider(
        const ::NKikimrProto::TAuthConfig& config,
        const TActorContext &ctx)
{
    const auto& passwordComplexityConfig = config.GetPasswordComplexity();
    NLogin::TPasswordComplexity passwordComplexity({
        .MinLength = passwordComplexityConfig.GetMinLength(),
        .MinLowerCaseCount = passwordComplexityConfig.GetMinLowerCaseCount(),
        .MinUpperCaseCount = passwordComplexityConfig.GetMinUpperCaseCount(),
        .MinNumbersCount = passwordComplexityConfig.GetMinNumbersCount(),
        .MinSpecialCharsCount = passwordComplexityConfig.GetMinSpecialCharsCount(),
        .SpecialChars = passwordComplexityConfig.GetSpecialChars(),
        .CanContainUsername = passwordComplexityConfig.GetCanContainUsername()
    });
    LoginProvider.UpdatePasswordCheckParameters(passwordComplexity);

    auto getSpecialChars = [&passwordComplexity] () {
        TStringBuilder result;
        for (const auto& ch : passwordComplexity.SpecialChars) {
            result << ch;
        }
        return result;
    };

    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                 "PasswordComplexity for LoginProvider configured: MinLength# " << passwordComplexity.MinLength
                 << ", MinLowerCaseCount# " << passwordComplexity.MinLowerCaseCount
                 << ", MinUpperCaseCount# " << passwordComplexity.MinUpperCaseCount
                 << ", MinNumbersCount# " << passwordComplexity.MinNumbersCount
                 << ", MinSpecialCharsCount# " << passwordComplexity.MinSpecialCharsCount
                 << ", SpecialChars# " << getSpecialChars()
                 << ", CanContainUsername# " << (passwordComplexity.CanContainUsername ? "true" : "false"));
}

void TSchemeShard::ConfigureAccountLockout(
        const ::NKikimrProto::TAuthConfig& config,
        const TActorContext &ctx)
{
    NLogin::TAccountLockout::TInitializer accountLockoutInitializer {
        .AttemptThreshold = config.GetAccountLockout().GetAttemptThreshold(),
        .AttemptResetDuration = config.GetAccountLockout().GetAttemptResetDuration()
    };

    LoginProvider.UpdateAccountLockout(accountLockoutInitializer);

    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                 "AccountLockout configured: AttemptThreshold# " << accountLockoutInitializer.AttemptThreshold
                 << ", AttemptResetDuration# " << accountLockoutInitializer.AttemptResetDuration);
}

void TSchemeShard::ConfigureExternalSources(
    const NKikimrConfig::TQueryServiceConfig& config,
    const TActorContext& ctx) {
    const auto& hostnamePatterns = config.GetHostnamePatterns();
    const auto& availableExternalDataSources = config.GetAvailableExternalDataSources();
    ExternalSourceFactory = NExternalSource::CreateExternalSourceFactory(
        std::vector<TString>(hostnamePatterns.begin(), hostnamePatterns.end()),
        nullptr,
        config.GetS3().GetGeneratorPathsLimit(),
        nullptr,
        EnableExternalSourceSchemaInference,
        config.GetS3().GetAllowLocalFiles(),
        config.GetAllExternalDataSourcesAreAvailable(),
        std::set<TString>(availableExternalDataSources.cbegin(), availableExternalDataSources.cend())
    );

    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "ExternalSources configured: HostnamePatterns# " << Join(", ", hostnamePatterns)
        << ", AvailableExternalDataSources# " << Join(", ", availableExternalDataSources));
}

void TSchemeShard::ConfigureCondErase(const NKikimrConfig::TSchemeShardConfig& config, const TActorContext &ctx) {
    MaxTTLShardsInFlight = config.GetMaxTTLShardsInFlight();
    CondEraseResponseBatchSize = config.GetCondEraseResponseBatchSize();
    CondEraseResponseBatchMaxTime = TDuration::MilliSeconds(
        std::max(ui32(1), std::min(ui32(1000), config.GetCondEraseResponseBatchMaxTimeMs()))
    );

    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "ConditionalErase configured"
        << ": ShardsInFlight (for table) " << MaxTTLShardsInFlight
        << ", BatchSize " << CondEraseResponseBatchSize
        << ", BatchMaxTime " << CondEraseResponseBatchMaxTime
    );
}

void TSchemeShard::StartStopCompactionQueues() {
    // note, that we don't need to check current state of compaction queue
    if (IsServerlessDomain(TPath::Init(RootPathId(), this))) {
        if (EnableBackgroundCompactionServerless) {
            BackgroundCompactionQueue->Start();
        } else {
            BackgroundCompactionQueue->Stop();
        }
    } else {
        if (EnableBackgroundCompaction) {
            BackgroundCompactionQueue->Start();
        } else {
            BackgroundCompactionQueue->Stop();
        }
    }

    BorrowedCompactionQueue->Start();

    if (ForcedCompactionQueue) {
        ForcedCompactionQueue->Start();
    }
}

void TSchemeShard::Handle(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse::TPtr &, const TActorContext &ctx) {
     LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                  "Subscription to Console has been set up"
                  << ", schemeshardId: " << SelfTabletId());
}

void TSchemeShard::Handle(NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr &ev, const TActorContext &ctx) {
    auto &rec = ev->Get()->Record;

    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
               "Got new config: " << rec.GetConfig().ShortDebugString());

    ApplyConsoleConfigs(rec.GetConfig(), ctx);

    auto resp = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationResponse>(rec);

    LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Send TEvConfigNotificationResponse: " << resp->Record.ShortDebugString());

    ctx.Send(ev->Sender, resp.Release(), 0, ev->Cookie);
}

void TSchemeShard::ChangeStreamShardsCount(i64 delta) {
    TabletCounters->Simple()[COUNTER_STREAM_SHARDS_COUNT].Add(delta);
}

void TSchemeShard::ChangeStreamShardsQuota(i64 delta) {
    TabletCounters->Simple()[COUNTER_STREAM_SHARDS_QUOTA].Add(delta);
}

void TSchemeShard::ChangeStreamReservedStorageCount(i64 delta) {
    TabletCounters->Simple()[COUNTER_STREAM_RESERVED_STORAGE].Add(delta);
}

void TSchemeShard::ChangeStreamReservedStorageQuota(i64 delta) {
    TabletCounters->Simple()[COUNTER_STREAM_RESERVED_STORAGE_QUOTA].Add(delta);
}

void TSchemeShard::ChangeDiskSpaceTablesDataBytes(i64 delta) {
    TabletCounters->Simple()[COUNTER_DISK_SPACE_TABLES_DATA_BYTES].Add(delta);
}

void TSchemeShard::ChangeDiskSpaceTablesIndexBytes(i64 delta) {
    TabletCounters->Simple()[COUNTER_DISK_SPACE_TABLES_INDEX_BYTES].Add(delta);
}

void TSchemeShard::ChangeDiskSpaceTablesTotalBytes(i64 delta) {
    TabletCounters->Simple()[COUNTER_DISK_SPACE_TABLES_TOTAL_BYTES].Add(delta);
}

void TSchemeShard::AddDiskSpaceTables(EUserFacingStorageType storageType, ui64 data, ui64 index) {
    if (storageType == EUserFacingStorageType::Ssd) {
        TabletCounters->Simple()[COUNTER_DISK_SPACE_TABLES_DATA_BYTES_ON_SSD].Add(data);
        TabletCounters->Simple()[COUNTER_DISK_SPACE_TABLES_INDEX_BYTES_ON_SSD].Add(index);
        TabletCounters->Simple()[COUNTER_DISK_SPACE_TABLES_TOTAL_BYTES_ON_SSD].Add(data + index);
    } else if (storageType == EUserFacingStorageType::Hdd) {
        TabletCounters->Simple()[COUNTER_DISK_SPACE_TABLES_DATA_BYTES_ON_HDD].Add(data);
        TabletCounters->Simple()[COUNTER_DISK_SPACE_TABLES_INDEX_BYTES_ON_HDD].Add(index);
        TabletCounters->Simple()[COUNTER_DISK_SPACE_TABLES_TOTAL_BYTES_ON_HDD].Add(data + index);
    }
}

void TSchemeShard::ChangeDiskSpaceTopicsTotalBytes(ui64 value) {
    TabletCounters->Simple()[COUNTER_DISK_SPACE_TOPICS_TOTAL_BYTES].Set(value);
}

void TSchemeShard::ChangeSimpleCounter(ESimpleCounters counter, i64 delta) {
    TabletCounters->Simple()[counter].Add(delta);
}

void TSchemeShard::ChangeDiskSpaceHardQuotaBytes(i64 delta) {
    TabletCounters->Simple()[COUNTER_DISK_SPACE_HARD_QUOTA_BYTES].Add(delta);
}

void TSchemeShard::ChangeDiskSpaceSoftQuotaBytes(i64 delta) {
    TabletCounters->Simple()[COUNTER_DISK_SPACE_SOFT_QUOTA_BYTES].Add(delta);
}

void TSchemeShard::AddDiskSpaceSoftQuotaBytes(EUserFacingStorageType storageType, ui64 addend) {
    if (storageType == EUserFacingStorageType::Ssd) {
        TabletCounters->Simple()[COUNTER_DISK_SPACE_SOFT_QUOTA_BYTES_ON_SSD].Add(addend);
    } else if (storageType == EUserFacingStorageType::Hdd) {
        TabletCounters->Simple()[COUNTER_DISK_SPACE_SOFT_QUOTA_BYTES_ON_HDD].Add(addend);
    }
}

void TSchemeShard::ChangeSmallBlobsVolumeBytes(i64 delta) {
    TabletCounters->Simple()[COUNTER_SMALL_BLOBS_VOLUME_BYTES].Add(delta);
}

void TSchemeShard::ChangeSmallBlobsCount(i64 delta) {
    TabletCounters->Simple()[COUNTER_SMALL_BLOBS_COUNT].Add(delta);
}

void TSchemeShard::ChangeSmallBlobsVolumeHardQuotaBytes(i64 delta) {
    TabletCounters->Simple()[COUNTER_SMALL_BLOBS_VOLUME_HARD_QUOTA_BYTES].Add(delta);
}

void TSchemeShard::ChangeSmallBlobsVolumeSoftQuotaBytes(i64 delta) {
    TabletCounters->Simple()[COUNTER_SMALL_BLOBS_VOLUME_SOFT_QUOTA_BYTES].Add(delta);
}

void TSchemeShard::ChangeSmallBlobsCountHardQuota(i64 delta) {
    TabletCounters->Simple()[COUNTER_SMALL_BLOBS_COUNT_HARD_QUOTA].Add(delta);
}

void TSchemeShard::ChangeSmallBlobsCountSoftQuota(i64 delta) {
    TabletCounters->Simple()[COUNTER_SMALL_BLOBS_COUNT_SOFT_QUOTA].Add(delta);
}

void TSchemeShard::ChangePathCount(i64 delta) {
    TabletCounters->Simple()[COUNTER_PATHS].Add(delta);
}

void TSchemeShard::SetPathCount(ui64 value) {
    TabletCounters->Simple()[COUNTER_PATHS].Set(value);
}

void TSchemeShard::SetPathsQuota(ui64 value) {
    TabletCounters->Simple()[COUNTER_PATHS_QUOTA].Set(value);
}

void TSchemeShard::ChangeShardCount(i64 delta) {
    TabletCounters->Simple()[COUNTER_SHARDS].Add(delta);
}

void TSchemeShard::SetShardCount(ui64 value) {
    TabletCounters->Simple()[COUNTER_SHARDS].Set(value);
}

void TSchemeShard::SetShardsQuota(ui64 value) {
    TabletCounters->Simple()[COUNTER_SHARDS_QUOTA].Set(value);
}

void TSchemeShard::Handle(TEvSchemeShard::TEvLogin::TPtr &ev, const TActorContext &ctx) {
    Execute(CreateTxLogin(ev), ctx);
}

void TSchemeShard::Handle(TEvSchemeShard::TEvListUsers::TPtr &ev, const TActorContext &ctx) {
    Execute(CreateTxListUsers(ev), ctx);
}

void TSchemeShard::Handle(TEvSchemeShard::TEvWakeupToRunShred::TPtr &ev, const TActorContext &ctx) {
    if (!IsDomainSchemeShard) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Cannot handle EvWakeupToRunShred in tenant schemeshard: " << TabletID());
        return;
    }
    RootShredManager->WakeupToRunShred(ev, ctx);
}

void TSchemeShard::Handle(TEvSchemeShard::TEvShredInfoRequest::TPtr& ev, const TActorContext& ctx) {
    LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::FLAT_TX_SCHEMESHARD,
        "Handle TEvShredInfoRequest, at schemeshard: " << TabletID());
    if (!IsDomainSchemeShard) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Cannot handle EvShredInfoRequest in tenant schemeshard: " << TabletID());
        return;
    }
    NKikimrScheme::TEvShredInfoResponse::EStatus status = NKikimrScheme::TEvShredInfoResponse::UNSPECIFIED;

    switch (RootShredManager->GetStatus()) {
    case EShredStatus::UNSPECIFIED:
        status = NKikimrScheme::TEvShredInfoResponse::UNSPECIFIED;
        break;
    case EShredStatus::COMPLETED:
        status = NKikimrScheme::TEvShredInfoResponse::COMPLETED;
        break;
    case EShredStatus::IN_PROGRESS:
        status = NKikimrScheme::TEvShredInfoResponse::IN_PROGRESS_TENANT;
        break;
    case EShredStatus::IN_PROGRESS_BSC:
        status = NKikimrScheme::TEvShredInfoResponse::IN_PROGRESS_BSC;
        break;
    }
    ctx.Send(ev->Sender, new TEvSchemeShard::TEvShredInfoResponse(RootShredManager->GetGeneration(), status));
}

void TSchemeShard::Handle(TEvSchemeShard::TEvShredManualStartupRequest::TPtr&, const TActorContext& ctx) {
    if (!IsDomainSchemeShard) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Cannot handle EvShredManualStartupRequest in tenant schemeshard: " << TabletID());
        return;
    }
    RunRootShred();
}

void TSchemeShard::Handle(TEvSchemeShard::TEvTenantShredRequest::TPtr& ev, const TActorContext& ctx) {
    LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::FLAT_TX_SCHEMESHARD,
        "Handle TEvTenantShredRequest, at schemeshard: " << TabletID());
    Execute(CreateTxRunTenantShred(ev), ctx);
}

void TSchemeShard::Handle(TEvDataShard::TEvVacuumResult::TPtr& ev, const TActorContext& ctx) {
    Execute(CreateTxCompleteShredShard<TEvDataShard::TEvVacuumResult::TPtr>(ev), ctx);
}

void TSchemeShard::Handle(TEvKeyValue::TEvVacuumResponse::TPtr& ev, const TActorContext& ctx) {
    Execute(this->CreateTxCompleteShredShard(ev), ctx);
}

void TSchemeShard::Handle(TEvPrivate::TEvAddNewShardToShred::TPtr& ev, const TActorContext& ctx) {
    Execute(CreateTxAddNewShardToShred(ev), ctx);
}

void TSchemeShard::Handle(TEvSchemeShard::TEvTenantShredResponse::TPtr& ev, const TActorContext& ctx) {
    if (!IsDomainSchemeShard) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Cannot handle EvTenantShredResponse in tenant schemeshard: " << TabletID());
        return;
    }
    Execute(CreateTxCompleteShredTenant(ev), ctx);
}

void TSchemeShard::Handle(TEvBlobStorage::TEvControllerShredResponse::TPtr& ev, const TActorContext& ctx) {
    LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::FLAT_TX_SCHEMESHARD,
        "Handle TEvControllerShredResponse, at schemeshard: " << TabletID());
    if (!IsDomainSchemeShard) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Cannot handle EvControllerShredResponse in tenant schemeshard: " << TabletID());
        return;
    }
    Execute(CreateTxCompleteShredBSC(ev), ctx);
}

void TSchemeShard::Handle(TEvSchemeShard::TEvWakeupToRunShredBSC::TPtr&, const TActorContext& ctx) {
    if (!IsDomainSchemeShard) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Cannot handle EvWakeupToRunShredBSC in tenant schemeshard: " << TabletID());
        return;
    }
    RootShredManager->WakeupSendRequestToBSC();
}

void TSchemeShard::Handle(NKikimr::NTestShard::TEvControlResponse::TPtr& ev, const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Handle TEvControlResponse"
                << ", at schemeshard: " << TabletID()
                << ", message: " << ev->Get()->Record.ShortDebugString());

    auto tabletId = TTabletId(ev->Get()->Record.GetTabletId());
    auto shardIdx = GetShardIdx(tabletId);
    if (shardIdx == InvalidShardIdx) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvControlResponse for unknown tabletId, ignore it"
                       << ", tabletId: " << tabletId
                       << ", message: " << ev->Get()->Record.ShortDebugString()
                       << ", at schemeshard: " << TabletID());
        return;
    }

    const auto txId = ShardInfos.at(shardIdx).CurrentTxId;
    if (!Operations.contains(txId)) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvControlResponse for unknown txId, ignore it"
                       << ", txId: " << txId
                       << ", message: " << ev->Get()->Record.ShortDebugString()
                       << ", at schemeshard: " << TabletID());
        return;
    }

    TSubTxId partId = Operations.at(txId)->FindRelatedPartByTabletId(tabletId, ctx);
    if (partId == InvalidSubTxId) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvControlResponse but partId in unknown"
                       << ", for txId: " << txId
                       << ", tabletId: " << tabletId
                       << ", at schemeshard: " << TabletID());
        return;
    }
    Execute(CreateTxOperationReply(TOperationId(txId, partId), ev), ctx);
}

void TSchemeShard::Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext&) {
    LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::STATISTICS,
        "Handle TEvTxProxySchemeCache::TEvNavigateKeySetResult"
        << ", at schemeshard: " << TabletID());

    using TNavigate = NSchemeCache::TSchemeCacheNavigate;
    std::unique_ptr<TNavigate> request(ev->Get()->Request.Release());
    if (request->ResultSet.size() != 1) {
        return;
    }
    auto& entry = request->ResultSet.back();
    if (entry.Status != TNavigate::EStatus::Ok) {
        return;
    }

    if (entry.DomainInfo->Params.HasStatisticsAggregator()) {
        StatisticsAggregatorId = TTabletId(entry.DomainInfo->Params.GetStatisticsAggregator());
        LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::STATISTICS,
            "Handle TEvTxProxySchemeCache::TEvNavigateKeySetResult, StatisticsAggregatorId=" << StatisticsAggregatorId
            << ", at schemeshard: " << TabletID());
        ConnectToSA();
    }
}

void TSchemeShard::Handle(TEvPrivate::TEvSendBaseStatsToSA::TPtr&, const TActorContext& ctx) {
    TDuration delta = SendBaseStatsToSA();
    LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::STATISTICS,
        "Schedule next SendBaseStatsToSA in " << delta
        << ", at schemeshard: " << TabletID());
    ctx.Schedule(delta, new TEvPrivate::TEvSendBaseStatsToSA());
}

void TSchemeShard::InitializeStatistics(const TActorContext& ctx) {
    ResolveSA();
    // Give table shards some time to report statistics. This is not required for correctness,
    // but if we tried to send the statistics right away, info for all paths would probably
    // be incomplete.
    ctx.Schedule(TDuration::Seconds(30), new TEvPrivate::TEvSendBaseStatsToSA());
}

void TSchemeShard::ResolveSA() {
    auto subDomainInfo = SubDomains.at(RootPathId());
    if (IsServerlessDomain(subDomainInfo)) {
        auto resourcesDomainId = subDomainInfo->GetResourcesDomainId();

        using TNavigate = NSchemeCache::TSchemeCacheNavigate;
        auto navigate = std::make_unique<TNavigate>();
        navigate->DatabaseName = AppData()->DomainsInfo->GetDomain()->Name;
        auto& entry = navigate->ResultSet.emplace_back();
        entry.TableId = TTableId(resourcesDomainId.OwnerId, resourcesDomainId.LocalPathId);
        entry.Operation = TNavigate::EOp::OpPath;
        entry.RequestType = TNavigate::TEntry::ERequestType::ByTableId;
        entry.RedirectRequired = false;

        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(navigate.release()));
    } else {
        StatisticsAggregatorId = subDomainInfo->GetTenantStatisticsAggregatorID();
        LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::STATISTICS,
            "ResolveSA(), StatisticsAggregatorId=" << StatisticsAggregatorId
            << ", at schemeshard: " << TabletID());
        ConnectToSA();
    }
}

void TSchemeShard::ConnectToSA() {
    if (!EnableStatistics)
        return;

    if (!StatisticsAggregatorId) {
        LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::STATISTICS,
            "ConnectToSA(), no StatisticsAggregatorId"
            << ", at schemeshard: " << TabletID());
        return;
    }
    auto policy = NTabletPipe::TClientRetryPolicy::WithRetries();
    NTabletPipe::TClientConfig pipeConfig{.RetryPolicy = policy};
    SAPipeClientId = Register(NTabletPipe::CreateClient(SelfId(), (ui64)StatisticsAggregatorId, pipeConfig));

    auto connect = std::make_unique<NStat::TEvStatistics::TEvConnectSchemeShard>();
    connect->Record.SetSchemeShardId(TabletID());

    NTabletPipe::SendData(SelfId(), SAPipeClientId, connect.release());

    LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::STATISTICS,
        "ConnectToSA()"
        << ", pipe client id: " << SAPipeClientId
        << ", at schemeshard: " << TabletID()
        << ", StatisticsAggregatorId: " << StatisticsAggregatorId
        << ", at schemeshard: " << TabletID()
    );
}

TDuration TSchemeShard::SendBaseStatsToSA() {
    if (!EnableStatistics) {
        return TDuration::Seconds(30);
    }

    if (!SAPipeClientId) {
        ResolveSA();
        if (!StatisticsAggregatorId) {
            LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::STATISTICS,
                "SendBaseStatsToSA(), no StatisticsAggregatorId"
                << ", at schemeshard: " << TabletID());
            return TDuration::Seconds(30);
        } else {
            LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::STATISTICS,
                "SendBaseStatsToSA(), StatisticsAggregatorId=" << StatisticsAggregatorId
                << ", at schemeshard: " << TabletID());
        }
    }

    int count = 0;
    int incompleteCount = 0;

    NKikimrStat::TSchemeShardStats record;
    for (const auto& [pathId, tableInfo] : Tables) {
        const auto& stats = tableInfo->GetStats();
        const auto& aggregated = stats.Aggregated;
        bool areStatsFull = stats.AreStatsFull();

        auto* entry = record.AddEntries();
        auto* entryPathId = entry->MutablePathId();
        entryPathId->SetOwnerId(pathId.OwnerId);
        entryPathId->SetLocalId(pathId.LocalPathId);
        entry->SetRowCount(areStatsFull ? aggregated.RowCount : 0);
        entry->SetBytesSize(areStatsFull ? aggregated.DataSize : 0);
        entry->SetIsColumnTable(false);
        entry->SetAreStatsFull(areStatsFull);

        ++count;
        if (!areStatsFull) {
            ++incompleteCount;
        }
    }

    auto columnTablesPathIds = ColumnTables.GetAllPathIds();
    for (const auto& pathId : columnTablesPathIds) {
        const auto& tableInfo = ColumnTables.GetVerified(pathId);
        const auto& stats = tableInfo->GetStats();
        const TTableAggregatedStats* aggregatedStats = nullptr;

        // stats are stored differently for standalone and non-standalone column tables
        if (tableInfo->IsStandalone()) {
            aggregatedStats = &stats;
        } else {
            auto it = stats.TableStats.find(pathId);
            if (it == stats.TableStats.end()) {
                continue;
            }
            aggregatedStats = &it->second;
        }
        const auto& aggregated = aggregatedStats->Aggregated;
        bool areStatsFull = aggregatedStats->AreStatsFull();

        auto* entry = record.AddEntries();
        auto* entryPathId = entry->MutablePathId();
        entryPathId->SetOwnerId(pathId.OwnerId);
        entryPathId->SetLocalId(pathId.LocalPathId);
        entry->SetRowCount(areStatsFull ? aggregated.RowCount : 0);
        entry->SetBytesSize(areStatsFull ? aggregated.DataSize : 0);
        entry->SetIsColumnTable(true);
        entry->SetAreStatsFull(areStatsFull);

        ++count;
        if (!areStatsFull) {
            ++incompleteCount;
        }
    }

    if (!count) {
        LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::STATISTICS,
            "SendBaseStatsToSA() No tables to send"
            << ", at schemeshard: " << TabletID());
        return TDuration::Seconds(30);
    }

    record.SetAreAllStatsFull(incompleteCount == 0);

    TString stats;
    Y_PROTOBUF_SUPPRESS_NODISCARD record.SerializeToString(&stats);

    auto event = std::make_unique<NStat::TEvStatistics::TEvSchemeShardStats>();
    event->Record.SetSchemeShardId(TabletID());
    event->Record.SetStats(stats);

    NTabletPipe::SendData(SelfId(), SAPipeClientId, event.release());

    LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::STATISTICS,
        "SendBaseStatsToSA()"
        << ", path count: " << count
        << ", paths with incomplete stats: " << incompleteCount
        << ", at schemeshard: " << TabletID());

    if (IsServerlessDomain(SubDomains.at(RootPathId()))) {
        // In serverless subdomains several schemeshards send stats to a single SA
        // so we use a bigger interval with jitter.
        const auto max = TDuration::Seconds(SendStatsIntervalSecondsServerless);
        const auto min = max * 3 / 4;
        return min + TDuration::MilliSeconds(
            RandomNumber<ui64>(max.MilliSeconds() - min.MilliSeconds()));
    } else {
        // Dedicated subdomains can use a smaller interval.
        return TDuration::Seconds(SendStatsIntervalSecondsDedicated);
    }
}

void TSchemeShard::ConfigureShredManager(const NKikimrConfig::TDataErasureConfig& config) {
    if (IsDomainSchemeShard) {
        if (RootShredManager) {
            RootShredManager->UpdateConfig(config);
        } else {
            RootShredManager = MakeHolder<TRootShredManager>(this, config);
        }
    }
    if (TenantShredManager) {
        TenantShredManager->UpdateConfig(config);
    } else {
        TenantShredManager = MakeHolder<TTenantShredManager>(this, config);
    }
}

void TSchemeShard::StartStopShred() {
    if (EnableShred) {
        if (IsDomainSchemeShard) {
            RootShredManager->Start();
        }
        TenantShredManager->Start();
    } else {
        if (IsDomainSchemeShard) {
            RootShredManager->Stop();
        }
        TenantShredManager->Stop();
    }
}

void TSchemeShard::InitRootShred() {
    Execute(CreateTxShredManagerInit(), this->ActorContext());
}

void TSchemeShard::RunRootShred() {
    Execute(CreateTxRunShred(), this->ActorContext());
}

} // namespace NSchemeShard
} // namespace NKikimr

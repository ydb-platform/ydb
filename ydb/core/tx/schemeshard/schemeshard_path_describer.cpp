#include "schemeshard_path_describer.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/engine/mkql_proto.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/public/api/protos/annotations/sensitive.pb.h>

#include <util/stream/format.h>

namespace {

void FillPartitionConfig(const NKikimrSchemeOp::TPartitionConfig& in, NKikimrSchemeOp::TPartitionConfig& out) {
    out.CopyFrom(in);
    NKikimr::NSchemeShard::TPartitionConfigMerger::DeduplicateColumnFamiliesById(out);
    out.MutableStorageRooms()->Clear();
}

}

namespace NKikimr {
namespace NSchemeShard {

static void FillTableStats(NKikimrTableStats::TTableStats* stats, const TPartitionStats& tableStats) {
    stats->SetRowCount(tableStats.RowCount);
    stats->SetDataSize(tableStats.DataSize);
    stats->SetIndexSize(tableStats.IndexSize);
    stats->SetByKeyFilterSize(tableStats.ByKeyFilterSize);
    stats->SetLastAccessTime(tableStats.LastAccessTime.MilliSeconds());
    stats->SetLastUpdateTime(tableStats.LastUpdateTime.MilliSeconds());
    stats->SetImmediateTxCompleted(tableStats.ImmediateTxCompleted);
    stats->SetPlannedTxCompleted(tableStats.PlannedTxCompleted);
    stats->SetTxRejectedByOverload(tableStats.TxRejectedByOverload);
    stats->SetTxRejectedBySpace(tableStats.TxRejectedBySpace);
    stats->SetTxCompleteLagMsec(tableStats.TxCompleteLag.MilliSeconds());
    stats->SetInFlightTxCount(tableStats.InFlightTxCount);

    stats->SetRowUpdates(tableStats.RowUpdates);
    stats->SetRowDeletes(tableStats.RowDeletes);
    stats->SetRowReads(tableStats.RowReads);
    stats->SetRangeReads(tableStats.RangeReads);
    stats->SetRangeReadRows(tableStats.RangeReadRows);

    stats->SetPartCount(tableStats.PartCount);

    auto* storagePoolsStats = stats->MutableStoragePools()->MutablePoolsUsage();
    for (const auto& [poolKind, stats] : tableStats.StoragePoolsStats) {
        auto* storagePoolStats = storagePoolsStats->Add();
        storagePoolStats->SetPoolKind(poolKind);
        storagePoolStats->SetDataSize(stats.DataSize);
        storagePoolStats->SetIndexSize(stats.IndexSize);
    }
}

static void FillTableMetrics(NKikimrTabletBase::TMetrics* metrics, const TPartitionStats& tableStats) {
    metrics->SetCPU(tableStats.GetCurrentRawCpuUsage());
    metrics->SetMemory(tableStats.Memory);
    metrics->SetNetwork(tableStats.Network);
    metrics->SetStorage(tableStats.Storage);
    metrics->SetReadThroughput(tableStats.ReadThroughput);
    metrics->SetWriteThroughput(tableStats.WriteThroughput);
    metrics->SetReadIops(tableStats.ReadIops);
    metrics->SetWriteIops(tableStats.WriteIops);
}

static void FillAggregatedStats(NKikimrSchemeOp::TPathDescription& pathDescription, const TAggregatedStats& stats) {
    FillTableStats(pathDescription.MutableTableStats(), stats.Aggregated);
    FillTableMetrics(pathDescription.MutableTabletMetrics(), stats.Aggregated);
}

static void FillTableStats(NKikimrSchemeOp::TPathDescription& pathDescription, const TPartitionStats& stats) {
    FillTableStats(pathDescription.MutableTableStats(), stats);
    FillTableMetrics(pathDescription.MutableTabletMetrics(), stats);
}

static void FillColumns(
    const TTableInfo& tableInfo,
    google::protobuf::RepeatedPtrField<NKikimrSchemeOp::TColumnDescription>& out
) {
    bool familyNamesBuilt = false;
    THashMap<ui32, TString> familyNames;

    out.Reserve(tableInfo.Columns.size());
    for (const auto& col : tableInfo.Columns) {
        const auto& cinfo = col.second;
        if (cinfo.IsDropped())
            continue;

        auto* colDescr = out.Add();
        colDescr->SetName(cinfo.Name);
        colDescr->SetType(NScheme::TypeName(cinfo.PType, cinfo.PTypeMod));
        auto columnType = NScheme::ProtoColumnTypeFromTypeInfoMod(cinfo.PType, cinfo.PTypeMod);
        colDescr->SetTypeId(columnType.TypeId);
        if (columnType.TypeInfo) {
            *colDescr->MutableTypeInfo() = *columnType.TypeInfo;
        }
        colDescr->SetId(cinfo.Id);
        colDescr->SetNotNull(cinfo.NotNull);

        if (cinfo.Family != 0) {
            colDescr->SetFamily(cinfo.Family);

            if (!familyNamesBuilt) {
                for (const auto& family : tableInfo.PartitionConfig().GetColumnFamilies()) {
                    if (family.HasName() && family.HasId()) {
                        familyNames[family.GetId()] = family.GetName();
                    }
                }
                familyNamesBuilt = true;
            }

            auto it = familyNames.find(cinfo.Family);
            if (it != familyNames.end() && !it->second.empty()) {
                colDescr->SetFamilyName(it->second);
            }
        }

        colDescr->SetIsBuildInProgress(cinfo.IsBuildInProgress);

        switch (cinfo.DefaultKind) {
            case ETableColumnDefaultKind::None:
                break;
            case ETableColumnDefaultKind::FromSequence:
                colDescr->SetDefaultFromSequence(cinfo.DefaultValue);
                break;
            case ETableColumnDefaultKind::FromLiteral:
                Y_ABORT_UNLESS(colDescr->MutableDefaultFromLiteral()->ParseFromString(
                    cinfo.DefaultValue));
                break;
        }
    }
}

static void FillKeyColumns(
    const TTableInfo& tableInfo,
    google::protobuf::RepeatedPtrField<TProtoStringType>& names,
    google::protobuf::RepeatedField<ui32>& ids
) {
    Y_ABORT_UNLESS(!tableInfo.KeyColumnIds.empty());
    names.Reserve(tableInfo.KeyColumnIds.size());
    ids.Reserve(tableInfo.KeyColumnIds.size());
    for (ui32 keyColId : tableInfo.KeyColumnIds) {
        *names.Add() = tableInfo.Columns.at(keyColId).Name;
        *ids.Add() = keyColId;
    }

}

void TPathDescriber::FillPathDescr(NKikimrSchemeOp::TDirEntry* descr, TPathElement::TPtr pathEl, TPathElement::EPathSubType subType) {
    FillChildDescr(descr, pathEl);

    descr->SetACL(pathEl->ACL);
    descr->SetPathSubType(subType);
}

void TPathDescriber::FillChildDescr(NKikimrSchemeOp::TDirEntry* descr, TPathElement::TPtr pathEl) {
    bool createFinished = pathEl->IsCreateFinished();

    descr->SetName(pathEl->Name);

    descr->SetSchemeshardId(pathEl->PathId.OwnerId);
    descr->SetPathId(pathEl->PathId.LocalPathId);

    descr->SetParentPathId(pathEl->ParentPathId.LocalPathId); //???? ParentPathOwnerId

    descr->SetPathType(pathEl->PathType);
    descr->SetOwner(pathEl->Owner);

    descr->SetPathState(pathEl->PathState); // ???? can't be consistent KIKIMR-8861
    descr->SetCreateFinished(createFinished); // use this instead of PathState

    descr->SetCreateTxId(ui64(pathEl->CreateTxId));
    if (createFinished) {
        descr->SetCreateStep(ui64(pathEl->StepCreated));
    }

    if (pathEl->PathType == NKikimrSchemeOp::EPathTypePersQueueGroup) {
        auto it = Self->Topics.FindPtr(pathEl->PathId);
        Y_ABORT_UNLESS(it, "PersQueueGroup is not found");

        TTopicInfo::TPtr pqGroupInfo = *it;
        if (pqGroupInfo->HasBalancer()) {
            descr->SetBalancerTabletID(ui64(pqGroupInfo->BalancerTabletID));
        }
    } else {
        descr->SetACL(pathEl->ACL); // YDBOPS-1328
    }
}

TPathElement::EPathSubType TPathDescriber::CalcPathSubType(const TPath& path) {
    if (!path.IsResolved()) {
        return TPathElement::EPathSubType::EPathSubTypeEmpty;
    }

    if (path.IsCommonSensePath()) {
        return TPathElement::EPathSubType::EPathSubTypeEmpty;
    }

    const auto parentPath = path.Parent();
    Y_ABORT_UNLESS(parentPath.IsResolved());

    if (parentPath.IsTableIndex()) {
        const auto& pathId = parentPath.Base()->PathId;
        Y_ABORT_UNLESS(Self->Indexes.contains(pathId));
        auto indexInfo = Self->Indexes.at(pathId);

        switch (indexInfo->Type) {
            case NKikimrSchemeOp::EIndexTypeGlobalAsync:
                return TPathElement::EPathSubType::EPathSubTypeAsyncIndexImplTable;
            case NKikimrSchemeOp::EIndexTypeGlobal:
            case NKikimrSchemeOp::EIndexTypeGlobalUnique:
                return TPathElement::EPathSubType::EPathSubTypeSyncIndexImplTable;
            case NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree:
                return TPathElement::EPathSubType::EPathSubTypeVectorKmeansTreeIndexImplTable;
            default:
                Y_DEBUG_ABORT("%s", (TStringBuilder() << "unexpected indexInfo->Type# " << indexInfo->Type).data());
                return TPathElement::EPathSubType::EPathSubTypeEmpty;
        }
    } else if (parentPath.IsCdcStream()) {
        return TPathElement::EPathSubType::EPathSubTypeStreamImpl;
    }

    return TPathElement::EPathSubType::EPathSubTypeEmpty;
}

void TPathDescriber::FillPathDescr(NKikimrSchemeOp::TDirEntry* descr, const TPath& path) {
    FillPathDescr(descr, path.Base(), CalcPathSubType(path));
}

void TPathDescriber::BuildEffectiveACL(NKikimrSchemeOp::TDirEntry* descr, const TPath& path) {
    descr->SetEffectiveACL(path.GetEffectiveACL());
}

void TPathDescriber::FillLastExistedPrefixDescr(const TPath& path) {
    if (path.IsEmpty()) {
        return;
    }
    Y_ABORT_UNLESS(path.IsResolved());
    Y_ABORT_UNLESS(!path.IsDeleted());

    Result->Record.SetLastExistedPrefixPathId(path.Base()->PathId.LocalPathId);
    Result->Record.SetLastExistedPrefixPath(path.PathString());

    Y_VERIFY_S(Self->PathsById.contains(path.Base()->PathId), "Unknown pathId " <<  path.Base()->PathId);
    auto descr = Result->Record.MutableLastExistedPrefixDescription()->MutableSelf();
    FillPathDescr(descr, path);
    BuildEffectiveACL(descr, path);
}

void TPathDescriber::DescribeChildren(const TPath& path) {
    auto pathEl = path.Base();

    if (!Params.GetOptions().GetReturnChildren()) {
        return;
    }

    if (pathEl->IsTable()) {
        return;
    }

    if (pathEl->PreSerializedChildrenListing.empty()) {
        NKikimrScheme::TEvDescribeSchemeResult preSerializedResult;
        auto pathDescription = preSerializedResult.MutablePathDescription();
        pathDescription->MutableChildren()->Reserve(pathEl->GetAliveChildren());
        for (const auto& child : pathEl->GetChildren()) {
            TPathId childId = child.second;
            const auto* childElPtr = Self->PathsById.FindPtr(childId);
            Y_ASSERT(childElPtr);
            TPathElement::TPtr childEl = *childElPtr;
            if (childEl->Dropped() || childEl->IsMigrated()) {
                continue;
            }
            auto entry = pathDescription->AddChildren();

            if (pathEl->IsTableIndex()) {
                // we put version of child here in addition
                auto childPath = path.Child(child.first);
                FillPathDescr(entry, childEl, CalcPathSubType(childPath));
                const auto version = Self->GetPathVersion(childPath);
                entry->MutableVersion()->CopyFrom(version);
            } else if (pathEl->IsCdcStream()) {
                auto childPath = path.Child(child.first);
                FillPathDescr(entry, childEl, CalcPathSubType(childPath));
            } else {
                FillChildDescr(entry, childEl);
            }
        }
        Y_PROTOBUF_SUPPRESS_NODISCARD preSerializedResult.SerializeToString(&pathEl->PreSerializedChildrenListing);
    }
    Result->PreSerializedData += pathEl->PreSerializedChildrenListing;

    if (!pathEl->IsCreateFinished()) {
        pathEl->PreSerializedChildrenListing.clear();
    }
}


void TPathDescriber::DescribeDir(const TPath& path) {
   DescribeChildren(path);
}

void FillTableBoundaries(
    google::protobuf::RepeatedPtrField<NKikimrSchemeOp::TSplitBoundary>* result,
    const TTableInfo& tableInfo
) {
    TString errStr;
    // Number of split boundaries equals to number of partitions - 1
    result->Reserve(tableInfo.GetPartitions().size() - 1);
    for (ui32 pi = 0; pi < tableInfo.GetPartitions().size() - 1; ++pi) {
        const auto& p = tableInfo.GetPartitions()[pi];
        TSerializedCellVec endKey(p.EndOfRange);
        auto boundary = result->Add()->MutableKeyPrefix();
        for (ui32 ki = 0;  ki < endKey.GetCells().size(); ++ki){
            const auto& c = endKey.GetCells()[ki];
            auto type = tableInfo.Columns.at(tableInfo.KeyColumnIds[ki]).PType;
            bool ok = NMiniKQL::CellToValue(type, c, *boundary->AddTuple(), errStr);
            Y_ABORT_UNLESS(ok, "Failed to build key tuple at position %" PRIu32 " error: %s", ki, errStr.data());
        }
    }
}

void FillTablePartitions(
    google::protobuf::RepeatedPtrField<NKikimrSchemeOp::TTablePartition>* result,
    const TTableInfo& tableInfo,
    const THashMap<TShardIdx, TShardInfo>& shardInfos,
    bool includeKeys
) {
    result->Reserve(tableInfo.GetPartitions().size());
    for (auto& p : tableInfo.GetPartitions()) {
        const auto& tabletId = ui64(shardInfos.at(p.ShardIdx).TabletID);
        const auto& key = p.EndOfRange;

        auto part = result->Add();
        part->SetDatashardId(tabletId);
        if (includeKeys) {
            // Currently we only support uniform partitioning where each range is [start, end)
            // +inf as the end of the last range is represented by empty TCell vector
            part->SetIsPoint(false);
            part->SetIsInclusive(false);
            part->SetEndOfRangeKeyPrefix(key);
        }
    }
}

const TString& GetSerializedTablePartitions(
    TTableInfo& tableInfo,
    const THashMap<TShardIdx, TShardInfo>& shardInfos,
    bool returnRangeKey
) {
    TString& cache = (returnRangeKey
        ? tableInfo.PreserializedTablePartitions
        : tableInfo.PreserializedTablePartitionsNoKeys
    );

    if (cache.empty()) {
        NKikimrScheme::TEvDescribeSchemeResult result;
        FillTablePartitions(result.MutablePathDescription()->MutableTablePartitions(), tableInfo, shardInfos, returnRangeKey);
        Y_PROTOBUF_SUPPRESS_NODISCARD result.SerializeToString(&cache);
    }

    return cache;
}

void TPathDescriber::DescribeTable(const TActorContext& ctx, TPathId pathId, TPathElement::TPtr pathEl) {
    const NScheme::TTypeRegistry* typeRegistry = AppData(ctx)->TypeRegistry;
    const auto* tableInfoPtr = Self->Tables.FindPtr(pathId);
    Y_ASSERT(tableInfoPtr);
    auto& tableInfo = *tableInfoPtr->Get();
    auto pathDescription = Result->Record.MutablePathDescription();
    auto entry = pathDescription->MutableTable();

    bool returnConfig = Params.GetReturnPartitionConfig();
    bool returnPartitioning = Params.GetReturnPartitioningInfo();
    bool returnPartitionStats = Params.GetOptions().GetReturnPartitionStats();
    bool returnBackupInfo = Params.GetBackupInfo();
    bool returnBoundaries = false;
    bool returnRangeKey = true;
    bool returnSetVal = Params.GetOptions().GetReturnSetVal();
    bool returnIndexTableBoundaries = Params.GetOptions().GetReturnIndexTableBoundaries();
    if (Params.HasOptions()) {
        returnConfig = Params.GetOptions().GetReturnPartitionConfig();
        returnPartitioning = Params.GetOptions().GetReturnPartitioningInfo();
        returnBackupInfo = Params.GetOptions().GetBackupInfo();
        returnBoundaries = Params.GetOptions().GetReturnBoundaries();
        returnRangeKey = Params.GetOptions().GetReturnRangeKey();
    }

    Self->DescribeTable(tableInfo, typeRegistry, returnConfig, entry);
    entry->SetName(pathEl->Name);

    if (returnBoundaries) {
        // split boundaries (split keys without shard's tablet-ids)
        if (tableInfo.PreserializedTableSplitBoundaries.empty()) {
            NKikimrScheme::TEvDescribeSchemeResult preSerializedResult;
            auto& tableDesc = *preSerializedResult.MutablePathDescription()->MutableTable();
            FillTableBoundaries(tableDesc.MutableSplitBoundary(), tableInfo);
            Y_PROTOBUF_SUPPRESS_NODISCARD preSerializedResult.SerializeToString(&tableInfo.PreserializedTableSplitBoundaries);
        }
        Result->PreSerializedData += tableInfo.PreserializedTableSplitBoundaries;
    }

    if (returnPartitioning) {
        // partitions (shard tablet-ids with range keys)
        Result->PreSerializedData += GetSerializedTablePartitions(tableInfo, Self->ShardInfos, returnRangeKey);
    }

    // KIKIMR-4337: table info is in flux until table is finally created
    if (!pathEl->IsCreateFinished()) {
        tableInfo.PreserializedTablePartitions.clear();
        tableInfo.PreserializedTablePartitionsNoKeys.clear();
        tableInfo.PreserializedTableSplitBoundaries.clear();
    }

    FillAggregatedStats(*Result->Record.MutablePathDescription(), tableInfo.GetStats());

    if (returnPartitionStats) {
        NKikimrSchemeOp::TPathDescription& pathDescription = *Result->Record.MutablePathDescription();
        pathDescription.MutableTablePartitionStats()->Reserve(tableInfo.GetPartitions().size());
        for (auto& p : tableInfo.GetPartitions()) {
            const auto* stats = tableInfo.GetStats().PartitionStats.FindPtr(p.ShardIdx);
            Y_ABORT_UNLESS(stats);
            auto pbStats = pathDescription.AddTablePartitionStats();
            FillTableStats(pbStats, *stats);
            auto pbMetrics = pathDescription.AddTablePartitionMetrics();
            FillTableMetrics(pbMetrics, *stats);
        }
    }

    if (returnBackupInfo) {
        /* Find active backup */
        for (const auto& pair: Self->TxInFlight) {
            const auto& txId = pair.first.GetTxId();
            const auto& txState = pair.second;
            if (txState.TargetPathId != pathId)
                continue;
            if (txState.TxType != TTxState::TxBackup)
                continue;

            ui32 notCompleteYet = txState.ShardsInProgress.size();
            ui32 total = txState.Shards.size();

            auto progress = pathDescription->MutableBackupProgress();
            progress->SetNotCompleteYet(notCompleteYet);
            progress->SetTotal(total);
            progress->SetErrorCount(CountIf(txState.ShardStatuses, [](const auto& kv) {
                return !kv.second.Success;
            }));
            progress->SetStartTime(txState.StartTime.Seconds());
            progress->MutableYTSettings()->CopyFrom(
                tableInfo.BackupSettings.GetYTSettings());
            progress->SetDataTotalSize(txState.DataTotalSize);
            progress->SetTxId(ui64(txId));

            LOG_TRACE(ctx, NKikimrServices::SCHEMESHARD_DESCRIBE,
                      "Add backup process info");
            break;
        }

        /* Get information about last completed backup */
        for (const auto& iter: tableInfo.BackupHistory) {
            LOG_TRACE(ctx, NKikimrServices::SCHEMESHARD_DESCRIBE,
                      "Add last backup info item to history");
            auto protoResult = pathDescription->AddLastBackupResult();
            const auto& txId = iter.first;
            const auto& tInfoResult = iter.second;
            protoResult->SetErrorCount(
                tInfoResult.TotalShardCount - tInfoResult.SuccessShardCount);
            protoResult->SetCompleteTimeStamp(
                tInfoResult.CompletionDateTime);
            protoResult->SetStartTimeStamp(tInfoResult.StartDateTime);
            protoResult->SetDataTotalSize(
                tInfoResult.DataTotalSize);
            protoResult->SetTxId(ui64(txId));

            for (const auto& [shardId, status]: tInfoResult.ShardStatuses) {
                if (status.Success) {
                    continue;
                }

                auto shardError = protoResult->AddErrors();
                shardError->SetShardId(ui64(shardId.GetLocalId()));
                shardError->SetExplain(status.Error);
            }
        }
    }

    for (const auto& child : pathEl->GetChildren()) {
        const auto& childName = child.first;
        const auto& childPathId = child.second;

        Y_ABORT_UNLESS(Self->PathsById.contains(childPathId));
        auto childPath = Self->PathsById.at(childPathId);

        if (childPath->Dropped()) {
            continue;
        }

        if (!childPath->IsCreateFinished()) {
            continue;
        }

        switch (childPath->PathType) {
        case NKikimrSchemeOp::EPathTypeTableIndex:
            Self->DescribeTableIndex(
                childPathId, childName, returnConfig, returnIndexTableBoundaries, *entry->AddTableIndexes()
            );
            break;
        case NKikimrSchemeOp::EPathTypeCdcStream:
            Self->DescribeCdcStream(childPathId, childName, *entry->AddCdcStreams());
            break;
        case NKikimrSchemeOp::EPathTypeSequence:
            Self->DescribeSequence(childPathId, childName, *entry->AddSequences(), returnSetVal);
            break;
        case NKikimrSchemeOp::EPathTypeTable:
            // TODO: move BackupImplTable under special scheme element
            break;
        default:
            Y_FAIL_S("Unexpected table's child"
                << ": tableId# " << pathId
                << ", childId# " << childPathId
                << ", childName# " << childName
                << ", childType# " << static_cast<ui32>(childPath->PathType));
        }
    }
}

void TPathDescriber::DescribeOlapStore(TPathId pathId, TPathElement::TPtr pathEl) {
    const auto* storeInfoPtr = Self->OlapStores.FindPtr(pathId);
    Y_ASSERT(storeInfoPtr);
    const TOlapStoreInfo::TPtr storeInfo = *Self->OlapStores.FindPtr(pathId);

    Y_ABORT_UNLESS(storeInfo, "OlapStore not found");
    Y_UNUSED(pathEl);

    auto description = Result->Record.MutablePathDescription()->MutableColumnStoreDescription();
    description->CopyFrom(storeInfo->GetDescription());

    description->ClearColumnShards();
    description->MutableColumnShards()->Reserve(storeInfo->ColumnShards.size());
    for (auto& shard : storeInfo->ColumnShards) {
        auto shardInfo = Self->ShardInfos.FindPtr(shard);
        Y_ABORT_UNLESS(shardInfo, "ColumnShard not found");
        description->AddColumnShards(shardInfo->TabletID.GetValue());
    }

    FillAggregatedStats(*Result->Record.MutablePathDescription(), storeInfo->GetStats());
}

void TPathDescriber::DescribeColumnTable(TPathId pathId, TPathElement::TPtr pathEl) {
    const auto tableInfo = Self->ColumnTables.GetVerified(pathId);
    Y_UNUSED(pathEl);

    auto* pathDescription = Result->Record.MutablePathDescription();
    auto description = pathDescription->MutableColumnTableDescription();
    description->CopyFrom(tableInfo->Description);
    description->MutableSharding()->CopyFrom(tableInfo->Description.GetSharding());

    if (tableInfo->IsStandalone()) {
        FillAggregatedStats(*pathDescription, tableInfo->GetStats());
    } else {
        const auto* storeInfoPtr = Self->OlapStores.FindPtr(tableInfo->GetOlapStorePathIdVerified());
        Y_ASSERT(storeInfoPtr);
        const TOlapStoreInfo::TPtr storeInfo = *storeInfoPtr;
        Y_ABORT_UNLESS(storeInfo, "OlapStore not found");

        auto& preset = storeInfo->SchemaPresets.at(description->GetSchemaPresetId());
        auto& presetProto = storeInfo->GetDescription().GetSchemaPresets(preset.GetProtoIndex());
        *description->MutableSchema() = presetProto.GetSchema();
        if (description->HasSchemaPresetVersionAdj()) {
            description->MutableSchema()->SetVersion(description->GetSchema().GetVersion() + description->GetSchemaPresetVersionAdj());
        }
        if (tableInfo->GetStats().TableStats.contains(pathId)) {
            FillTableStats(*pathDescription, tableInfo->GetStats().TableStats.at(pathId));
        } else {
            FillTableStats(*pathDescription, TPartitionStats());
        }
    }
}

void TPathDescriber::DescribePersQueueGroup(TPathId pathId, TPathElement::TPtr pathEl) {
    auto it = Self->Topics.FindPtr(pathId);
    Y_ABORT_UNLESS(it, "PersQueueGroup is not found");
    TTopicInfo::TPtr pqGroupInfo = *it;

    if (pqGroupInfo->PreSerializedPathDescription.empty()) {
        NKikimrScheme::TEvDescribeSchemeResult preSerializedResult;
        auto entry = preSerializedResult.MutablePathDescription()->MutablePersQueueGroup();
        entry->SetName(pathEl->Name);
        entry->SetPathId(pathId.LocalPathId);
        entry->SetNextPartitionId(pqGroupInfo->NextPartitionId);
        entry->SetTotalGroupCount(pqGroupInfo->TotalGroupCount);
        entry->SetPartitionPerTablet(pqGroupInfo->MaxPartsPerTablet);
        entry->SetAlterVersion(pqGroupInfo->AlterVersion);
        Y_PROTOBUF_SUPPRESS_NODISCARD entry->MutablePQTabletConfig()->ParseFromString(pqGroupInfo->TabletConfig);

        if (pqGroupInfo->HasBalancer()) {
            entry->SetBalancerTabletID(ui64(pqGroupInfo->BalancerTabletID));
        }

        Y_PROTOBUF_SUPPRESS_NODISCARD preSerializedResult.SerializeToString(&pqGroupInfo->PreSerializedPathDescription);
    }

    Y_DEBUG_ABORT_UNLESS(!pqGroupInfo->PreSerializedPathDescription.empty());
    Result->PreSerializedData += pqGroupInfo->PreSerializedPathDescription;

    bool returnPartitioning = Params.GetReturnPartitioningInfo();
    if (returnPartitioning) {
        if (pqGroupInfo->PreSerializedPartitionsDescription.empty()) {
            NKikimrScheme::TEvDescribeSchemeResult preSerializedResult;
            auto entry = preSerializedResult.MutablePathDescription()->MutablePersQueueGroup();

            struct TPartitionDesc {
            TTabletId TabletId;
            const TTopicTabletInfo::TTopicPartitionInfo* Info = nullptr;
            };

            // it is sorted list of partitions by partition id
            TVector<TPartitionDesc> descriptions; // index is pqId
            descriptions.resize(pqGroupInfo->Partitions.size());

            for (const auto& [shardIdx, pqShard] : pqGroupInfo->Shards) {
                auto it = Self->ShardInfos.find(shardIdx);
                Y_VERIFY_S(it != Self->ShardInfos.end(), "No shard with shardIdx: " << shardIdx);

                for (const auto& partition : pqShard->Partitions) {
                    if (partition->AlterVersion <= pqGroupInfo->AlterVersion) {
                        Y_VERIFY_S(partition->PqId < pqGroupInfo->NextPartitionId,
                                   "Wrong pqId: " << partition->PqId << ", nextPqId: " << pqGroupInfo->NextPartitionId);
                        descriptions[partition->PqId] = {it->second.TabletID, partition.Get()};
                    }
                }
            }

            for (const auto& desc : descriptions) {
                if (desc.Info == nullptr || desc.Info->Status == NKikimrPQ::ETopicPartitionStatus::Deleted) {
                    continue;
                }
                const auto pqId = desc.Info->PqId;
                auto& partition = *entry->AddPartitions();

                Y_VERIFY_S(desc.TabletId, "Unassigned tabletId for partition: " << pqId);
                Y_VERIFY_S(desc.Info, "Empty info for partition: " << pqId);

                partition.SetPartitionId(pqId);
                partition.SetTabletId(ui64(desc.TabletId));
                if (desc.Info->KeyRange) {
                    desc.Info->KeyRange->SerializeToProto(*partition.MutableKeyRange());
                }

                partition.SetStatus(desc.Info->Status);
                for (const auto parent : desc.Info->ParentPartitionIds) {
                    partition.AddParentPartitionIds(parent);
                }
                for (const auto child : desc.Info->ChildPartitionIds) {
                    partition.AddChildPartitionIds(child);
                }
            }

            Y_PROTOBUF_SUPPRESS_NODISCARD preSerializedResult.SerializeToString(&pqGroupInfo->PreSerializedPartitionsDescription);
        }

        Y_DEBUG_ABORT_UNLESS(!pqGroupInfo->PreSerializedPartitionsDescription.empty());
        Result->PreSerializedData += pqGroupInfo->PreSerializedPartitionsDescription;
    }

    if (Self->FillAllocatePQ && pqGroupInfo->TotalGroupCount == pqGroupInfo->TotalPartitionCount) {
        auto allocate = Result->Record.MutablePathDescription()->MutablePersQueueGroup()->MutableAllocate();

        allocate->SetName(pathEl->Name);
        allocate->SetTotalGroupCount(pqGroupInfo->TotalGroupCount);
        allocate->SetNextPartitionId(pqGroupInfo->NextPartitionId);
        allocate->SetPartitionPerTablet(pqGroupInfo->MaxPartsPerTablet);
        Y_PROTOBUF_SUPPRESS_NODISCARD allocate->MutablePQTabletConfig()->ParseFromString(pqGroupInfo->TabletConfig);
        allocate->SetBalancerTabletID(ui64(pqGroupInfo->BalancerTabletID));
        allocate->SetBalancerOwnerId(pqGroupInfo->BalancerShardIdx.GetOwnerId());
        allocate->SetBalancerShardId(ui64(pqGroupInfo->BalancerShardIdx.GetLocalId()));

        for (const auto& [shardIdx, pqShard] : pqGroupInfo->Shards) {
            const auto& shardInfo = Self->ShardInfos.at(shardIdx);
            for (const auto& pq : pqShard->Partitions) {
                if (pq->AlterVersion <= pqGroupInfo->AlterVersion) {
                    auto partition = allocate->MutablePartitions()->Add();
                    partition->SetPartitionId(pq->PqId);
                    partition->SetGroupId(pq->GroupId);
                    partition->SetTabletId(ui64(shardInfo.TabletID));
                    partition->SetOwnerId(shardIdx.GetOwnerId());
                    partition->SetShardId(ui64(shardIdx.GetLocalId()));
                    partition->SetStatus(pq->Status);
                    for (const auto parent : pq->ParentPartitionIds) {
                        partition->AddParentPartitionIds(parent);
                    }
                    if (pq->KeyRange) {
                        pq->KeyRange->SerializeToProto(*partition->MutableKeyRange());
                    }
                }
            }
        }
        allocate->SetAlterVersion(pqGroupInfo->AlterVersion);
    }

    Y_DEBUG_ABORT_UNLESS(!Result->PreSerializedData.empty());
    if (!pathEl->IsCreateFinished()) {
        // Don't cache until create finishes (KIKIMR-4337)
        pqGroupInfo->PreSerializedPathDescription.clear();
        pqGroupInfo->PreSerializedPartitionsDescription.clear();
    }
}

void TPathDescriber::DescribeRtmrVolume(TPathId pathId, TPathElement::TPtr pathEl) {
    auto it = Self->RtmrVolumes.FindPtr(pathId);
    Y_ABORT_UNLESS(it, "RtmrVolume is not found");
    TRtmrVolumeInfo::TPtr rtmrVolumeInfo = *it;

    auto entry = Result->Record.MutablePathDescription()->MutableRtmrVolumeDescription();
    entry->SetName(pathEl->Name);
    entry->SetPathId(pathId.LocalPathId);
    entry->SetPartitionsCount(rtmrVolumeInfo->Partitions.size());

    for (const auto& partition: rtmrVolumeInfo->Partitions) {
        auto part = entry->AddPartitions();
        part->SetPartitionId((const char*)partition.second->Id.dw, sizeof(TGUID));
        part->SetBusKey(partition.second->BusKey);
        part->SetTabletId(ui64(partition.second->TabletId));
    }

    Sort(entry->MutablePartitions()->begin(),
         entry->MutablePartitions()->end(),
         [](const auto& part1, const auto& part2){
             return part1.GetBusKey() < part2.GetBusKey();
         });
}

void TPathDescriber::DescribeTableIndex(const TPath& path) {
    bool returnConfig = Params.GetReturnPartitionConfig();
    bool returnBoundaries = Params.HasOptions() && Params.GetOptions().GetReturnBoundaries();

    Self->DescribeTableIndex(path.Base()->PathId, path.Base()->Name, returnConfig, returnBoundaries,
        *Result->Record.MutablePathDescription()->MutableTableIndex()
    );
    DescribeChildren(path);
}

void TPathDescriber::DescribeCdcStream(const TPath& path) {
    Self->DescribeCdcStream(path.Base()->PathId, path.Base()->Name,
        *Result->Record.MutablePathDescription()->MutableCdcStreamDescription());
    DescribeChildren(path);
}

void TPathDescriber::DescribeSolomonVolume(TPathId pathId, TPathElement::TPtr pathEl, bool returnChannelsBinding) {
    auto it = Self->SolomonVolumes.FindPtr(pathId);
    Y_ABORT_UNLESS(it, "SolomonVolume is not found");
    TSolomonVolumeInfo::TPtr solomonVolumeInfo = *it;

    auto entry = Result->Record.MutablePathDescription()->MutableSolomonDescription();
    entry->SetName(pathEl->Name);
    entry->SetPathId(pathId.LocalPathId);
    entry->SetPartitionCount(solomonVolumeInfo->Partitions.size());

    entry->MutablePartitions()->Reserve(entry->GetPartitionCount());
    for (ui32 idx = 0; idx < entry->GetPartitionCount(); ++idx) {
        entry->AddPartitions();
    }

    for (const auto& partition: solomonVolumeInfo->Partitions) {
        auto shardId = partition.first;
        auto shardInfo = Self->ShardInfos.FindPtr(shardId);
        Y_ABORT_UNLESS(shardInfo);

        auto part = entry->MutablePartitions()->Mutable(partition.second->PartitionId);
        part->SetPartitionId(partition.second->PartitionId);
        part->SetShardIdx(ui64(shardId.GetLocalId()));
        auto tabletId = partition.second->TabletId;
        if (tabletId != InvalidTabletId) {
            part->SetTabletId(ui64(partition.second->TabletId));
        }

        if (returnChannelsBinding) {
            for (const auto& channel : shardInfo->BindedChannels) {
                part->AddBoundChannels()->CopyFrom(channel);
            }
        }
    }

    Sort(entry->MutablePartitions()->begin(),
         entry->MutablePartitions()->end(),
         [] (auto& part1, auto& part2) {
             return part1.GetPartitionId() < part2.GetPartitionId();
         });
}

void TPathDescriber::DescribeUserAttributes(TPathElement::TPtr pathEl) {
    if (!pathEl->UserAttrs->Size() && !pathEl->HasRuntimeAttrs()) {
        return;
    }

    auto* userAttrs = Result->Record.MutablePathDescription()->MutableUserAttributes();
    for (const auto& item: pathEl->UserAttrs->Attrs) {
        auto* attr = userAttrs->Add();
        attr->SetKey(item.first);
        attr->SetValue(item.second);
    }
    pathEl->SerializeRuntimeAttrs(userAttrs);
}

void TPathDescriber::DescribePathVersion(const TPath& path) {
    const auto& version = Self->GetPathVersion(path);
    Result->Record.MutablePathDescription()->MutableSelf()->SetPathVersion(version.GetGeneralVersion());
    Result->Record.MutablePathDescription()->MutableSelf()->MutableVersion()->CopyFrom(version);
}

void TPathDescriber::DescribeDomain(TPathElement::TPtr pathEl) {
    TPathId domainId = Self->ResolvePathIdForDomain(pathEl);

    TPathElement::TPtr domainEl = Self->PathsById.at(domainId);
    Y_ABORT_UNLESS(domainEl);

    DescribeDomainRoot(domainEl);
}

void TPathDescriber::DescribeRevertedMigrations(TPathElement::TPtr pathEl) {
    Y_ABORT_UNLESS(pathEl->IsDomainRoot());

    if (!Self->RevertedMigrations.contains(pathEl->PathId)) {
        return;
    }

    auto list = Result->Record.MutablePathDescription()->MutableAbandonedTenantsSchemeShards();
    for (const TTabletId& abandonedSchemeShardId: Self->RevertedMigrations.at(pathEl->PathId)) {
        list->Add(ui64(abandonedSchemeShardId));
    }
}

void TPathDescriber::DescribeDomainRoot(TPathElement::TPtr pathEl) {
    Y_ABORT_UNLESS(pathEl->IsDomainRoot());
    auto it = Self->SubDomains.FindPtr(pathEl->PathId);
    Y_ABORT_UNLESS(it, "SubDomain not found");
    auto subDomainInfo = *it;

    NKikimrSubDomains::TDomainDescription * entry = Result->Record.MutablePathDescription()->MutableDomainDescription();

    entry->SetSchemeShardId_Depricated(Self->ParentDomainId.OwnerId);
    entry->SetPathId_Depricated(Self->ParentDomainId.LocalPathId);

    auto domainKey = Self->GetDomainKey(pathEl->PathId);
    NKikimrSubDomains::TDomainKey *key = entry->MutableDomainKey();
    key->SetSchemeShard(domainKey.OwnerId);
    key->SetPathId(domainKey.LocalPathId);

    entry->MutableProcessingParams()->CopyFrom(subDomainInfo->GetProcessingParams());

    entry->SetPathsInside(subDomainInfo->GetPathsInside());
    entry->SetPathsLimit(subDomainInfo->GetSchemeLimits().MaxPaths);
    entry->SetShardsInside(subDomainInfo->GetShardsInside());
    entry->SetShardsLimit(subDomainInfo->GetSchemeLimits().MaxShards);
    entry->SetPQPartitionsInside(subDomainInfo->GetPQPartitionsInside());
    entry->SetPQPartitionsLimit(subDomainInfo->GetSchemeLimits().MaxPQPartitions);

    NKikimrSubDomains::TDomainKey *resourcesKey = entry->MutableResourcesDomainKey();
    resourcesKey->SetSchemeShard(subDomainInfo->GetResourcesDomainId().OwnerId);
    resourcesKey->SetPathId(subDomainInfo->GetResourcesDomainId().LocalPathId);

    NKikimrSubDomains::TDiskSpaceUsage *diskSpaceUsage = entry->MutableDiskSpaceUsage();
    diskSpaceUsage->MutableTables()->SetTotalSize(subDomainInfo->GetDiskSpaceUsage().Tables.TotalSize);
    diskSpaceUsage->MutableTables()->SetDataSize(subDomainInfo->GetDiskSpaceUsage().Tables.DataSize);
    diskSpaceUsage->MutableTables()->SetIndexSize(subDomainInfo->GetDiskSpaceUsage().Tables.IndexSize);
    diskSpaceUsage->MutableTopics()->SetReserveSize(subDomainInfo->GetPQReservedStorage());
    diskSpaceUsage->MutableTopics()->SetAccountSize(subDomainInfo->GetPQAccountStorage());
    diskSpaceUsage->MutableTopics()->SetDataSize(subDomainInfo->GetDiskSpaceUsage().Topics.DataSize);
    diskSpaceUsage->MutableTopics()->SetUsedReserveSize(subDomainInfo->GetDiskSpaceUsage().Topics.UsedReserveSize);
    auto* storagePoolsUsage = diskSpaceUsage->MutableStoragePoolsUsage();
    for (const auto& [poolKind, usage] : subDomainInfo->GetDiskSpaceUsage().StoragePoolsUsage) {
        auto* storagePoolUsage = storagePoolsUsage->Add();
        storagePoolUsage->SetPoolKind(poolKind);
        storagePoolUsage->SetDataSize(usage.DataSize);
        storagePoolUsage->SetIndexSize(usage.IndexSize);
        storagePoolUsage->SetTotalSize(usage.DataSize + usage.IndexSize);
    }

    if (subDomainInfo->GetDeclaredSchemeQuotas()) {
        entry->MutableDeclaredSchemeQuotas()->CopyFrom(*subDomainInfo->GetDeclaredSchemeQuotas());
    }

    if (const auto& databaseQuotas = subDomainInfo->GetDatabaseQuotas()) {
        entry->MutableDatabaseQuotas()->CopyFrom(*databaseQuotas);
    }

    if (subDomainInfo->GetDiskQuotaExceeded()) {
        entry->MutableDomainState()->SetDiskQuotaExceeded(true);
    }

    if (const auto& auditSettings = subDomainInfo->GetAuditSettings()) {
        entry->MutableAuditSettings()->CopyFrom(*auditSettings);
    }

    if (const auto& serverlessComputeResourcesMode = subDomainInfo->GetServerlessComputeResourcesMode()) {
        entry->SetServerlessComputeResourcesMode(*serverlessComputeResourcesMode);
    }

    if (TTabletId sharedHive = subDomainInfo->GetSharedHive()) {
        entry->SetSharedHive(sharedHive.GetValue());
    }
}

void TPathDescriber::DescribeDomainExtra(TPathElement::TPtr pathEl) {
    Y_ABORT_UNLESS(pathEl->IsDomainRoot());
    auto it = Self->SubDomains.FindPtr(pathEl->PathId);
    Y_ABORT_UNLESS(it, "SubDomain not found");
    auto subDomainInfo = *it;

    NKikimrSubDomains::TDomainDescription * entry = Result->Record.MutablePathDescription()->MutableDomainDescription();

    for (auto& pool: subDomainInfo->GetStoragePools()) {
        *entry->AddStoragePools() = pool;
    }
    if (subDomainInfo->HasSecurityState()) {
        entry->MutableSecurityState()->CopyFrom(subDomainInfo->GetSecurityState());
    }
}

void TPathDescriber::DescribeBlockStoreVolume(TPathId pathId, TPathElement::TPtr pathEl) {
    Y_ABORT_UNLESS(pathEl->IsBlockStoreVolume());
    auto it = Self->BlockStoreVolumes.FindPtr(pathId);
    Y_ABORT_UNLESS(it, "BlockStore volume is not found");
    TBlockStoreVolumeInfo::TPtr volume = *it;

    auto* entry = Result->Record.MutablePathDescription()->MutableBlockStoreVolumeDescription();
    entry->SetName(pathEl->Name);
    entry->SetPathId(pathId.LocalPathId);
    entry->MutableVolumeConfig()->CopyFrom(volume->VolumeConfig);
    entry->MutableVolumeConfig()->SetVersion(volume->AlterVersion);
    entry->SetVolumeTabletId(ui64(volume->VolumeTabletId));
    entry->SetAlterVersion(volume->AlterVersion);
    entry->SetMountToken(volume->MountToken);
    entry->SetTokenVersion(volume->TokenVersion);

    const auto& tablets = volume->GetTablets(Self->ShardInfos);
    for (ui32 idx = 0; idx < tablets.size(); ++idx) {
        auto* part = entry->AddPartitions();
        part->SetPartitionId(idx);
        part->SetTabletId(ui64(tablets[idx]));
    }
}

void TPathDescriber::DescribeFileStore(TPathId pathId, TPathElement::TPtr pathEl) {
    Y_ABORT_UNLESS(pathEl->IsFileStore());
    auto it = Self->FileStoreInfos.FindPtr(pathId);
    Y_ABORT_UNLESS(it, "FileStore info is not found");
    TFileStoreInfo::TPtr fs = *it;

    auto* entry = Result->Record.MutablePathDescription()->MutableFileStoreDescription();
    entry->SetName(pathEl->Name);
    entry->SetPathId(pathId.LocalPathId);
    if (fs->IndexTabletId) {
        entry->SetIndexTabletId(ui64(fs->IndexTabletId));
    }
    entry->SetVersion(fs->Version);

    auto* config = entry->MutableConfig();
    config->CopyFrom(fs->Config);
    config->SetVersion(fs->Version);
}

void TPathDescriber::DescribeKesus(TPathId pathId, TPathElement::TPtr pathEl) {
    Y_ABORT_UNLESS(pathEl->IsKesus());
    auto it = Self->KesusInfos.FindPtr(pathId);
    Y_ABORT_UNLESS(it, "Kesus info not found");
    TKesusInfo::TPtr kesus = *it;

    auto* entry = Result->Record.MutablePathDescription()->MutableKesus();
    entry->SetName(pathEl->Name);
    entry->SetPathId(pathId.LocalPathId);
    if (kesus->KesusTabletId) {
        entry->SetKesusTabletId(ui64(kesus->KesusTabletId));
    }
    entry->MutableConfig()->CopyFrom(kesus->Config);
    entry->SetVersion(kesus->Version);
}

void TPathDescriber::DescribeSequence(TPathId pathId, TPathElement::TPtr pathEl) {
    Y_ABORT_UNLESS(pathEl->IsSequence());
    Self->DescribeSequence(pathId, pathEl->Name, *Result->Record.MutablePathDescription()->MutableSequenceDescription());
}

void TPathDescriber::DescribeReplication(TPathId pathId, TPathElement::TPtr pathEl) {
    Y_ABORT_UNLESS(pathEl->IsReplication());
    Self->DescribeReplication(pathId, pathEl->Name, *Result->Record.MutablePathDescription()->MutableReplicationDescription());
}

void TPathDescriber::DescribeBlobDepot(const TPath& path) {
    Y_ABORT_UNLESS(path->IsBlobDepot());
    Self->DescribeBlobDepot(path->PathId, path->Name, *Result->Record.MutablePathDescription()->MutableBlobDepotDescription());
}

void TPathDescriber::DescribeExternalTable(const TActorContext& ctx, TPathId pathId, TPathElement::TPtr pathEl) {
    Y_UNUSED(ctx);

    auto it = Self->ExternalTables.FindPtr(pathId);
    Y_ABORT_UNLESS(it, "ExternalTable is not found");
    TExternalTableInfo::TPtr externalTableInfo = *it;

    auto entry = Result->Record.MutablePathDescription()->MutableExternalTableDescription();
    entry->SetName(pathEl->Name);
    PathIdFromPathId(pathId, entry->MutablePathId());
    entry->SetSourceType(externalTableInfo->SourceType);
    entry->SetDataSourcePath(externalTableInfo->DataSourcePath);
    entry->SetLocation(externalTableInfo->Location);
    entry->SetVersion(externalTableInfo->AlterVersion);

    entry->MutableColumns()->Reserve(externalTableInfo->Columns.size());
    for (auto col : externalTableInfo->Columns) {
        const auto& cinfo = col.second;
        if (cinfo.IsDropped())
            continue;

        auto colDescr = entry->AddColumns();
        colDescr->SetName(cinfo.Name);
        colDescr->SetType(NScheme::TypeName(cinfo.PType, cinfo.PTypeMod));
        auto columnType = NScheme::ProtoColumnTypeFromTypeInfoMod(cinfo.PType, cinfo.PTypeMod);
        colDescr->SetTypeId(columnType.TypeId);
        if (columnType.TypeInfo) {
            *colDescr->MutableTypeInfo() = *columnType.TypeInfo;
        }
        colDescr->SetId(cinfo.Id);
        colDescr->SetNotNull(cinfo.NotNull);
    }
    entry->SetContent(externalTableInfo->Content);
}

void TPathDescriber::DescribeExternalDataSource(const TActorContext&, TPathId pathId, TPathElement::TPtr pathEl) {
    auto it = Self->ExternalDataSources.FindPtr(pathId);
    Y_ABORT_UNLESS(it, "ExternalDataSource is not found");
    TExternalDataSourceInfo::TPtr externalDataSourceInfo = *it;

    auto entry = Result->Record.MutablePathDescription()->MutableExternalDataSourceDescription();
    entry->SetName(pathEl->Name);
    PathIdFromPathId(pathId, entry->MutablePathId());
    entry->SetVersion(externalDataSourceInfo->AlterVersion);
    entry->SetSourceType(externalDataSourceInfo->SourceType);
    entry->SetLocation(externalDataSourceInfo->Location);
    entry->SetInstallation(externalDataSourceInfo->Installation);
    entry->MutableAuth()->CopyFrom(externalDataSourceInfo->Auth);
    entry->MutableProperties()->CopyFrom(externalDataSourceInfo->Properties);
}

void TPathDescriber::DescribeView(const TActorContext&, TPathId pathId, TPathElement::TPtr pathEl) {
    auto it = Self->Views.FindPtr(pathId);
    Y_ABORT_UNLESS(it, "View is not found");
    TViewInfo::TPtr viewInfo = *it;

    auto entry = Result->Record.MutablePathDescription()->MutableViewDescription();
    entry->SetName(pathEl->Name);
    PathIdFromPathId(pathId, entry->MutablePathId());
    entry->SetVersion(viewInfo->AlterVersion);
    entry->SetQueryText(viewInfo->QueryText);
}

void TPathDescriber::DescribeResourcePool(TPathId pathId, TPathElement::TPtr pathEl) {
    auto it = Self->ResourcePools.FindPtr(pathId);
    Y_ABORT_UNLESS(it, "ResourcePools is not found");
    TResourcePoolInfo::TPtr resourcePoolInfo = *it;

    auto entry = Result->Record.MutablePathDescription()->MutableResourcePoolDescription();
    entry->SetName(pathEl->Name);
    PathIdFromPathId(pathId, entry->MutablePathId());
    entry->SetVersion(resourcePoolInfo->AlterVersion);
    entry->MutableProperties()->CopyFrom(resourcePoolInfo->Properties);
}

static bool ConsiderAsDropped(const TPath& path) {
    Y_ABORT_UNLESS(path.IsResolved());

    if (path.Base()->IsTable() || path.Base()->IsTableIndex()) {
        return false;
    }
    if (path.Base()->IsDirectory() || path.Base()->IsDomainRoot()) {
        return false;
    }
    if (path.IsCdcStream()) {
        return false;
    }
    if (path.IsReplication()) {
        return false;
    }

    return true;
}

THolder<TEvSchemeShard::TEvDescribeSchemeResultBuilder> TPathDescriber::Describe(const TActorContext& ctx) {
    TPathId pathId = Params.HasPathId() ? TPathId(Params.GetSchemeshardId(), Params.GetPathId()) : InvalidPathId;
    TString pathStr = Params.GetPath();

    TPath path = Params.HasPathId()
        ? TPath::Init(pathId, Self)
        : TPath::Resolve(pathStr, Self);
    {
        TPath::TChecker checks = path.Check();
        checks
            .NotEmpty(NKikimrScheme::StatusPathDoesNotExist)
            .IsAtLocalSchemeShard()
            .IsResolved();

        if (checks) {
            if (Params.HasPathId()) {
                pathStr = path.PathString();
            } else {
                pathId = path.Base()->PathId;
            }
        }

        checks
            .NotDeleted();

        if (checks && ConsiderAsDropped(path)) {
            // KIKIMR-13173
            // PQ BSV drop their shard before PlanStep
            // If they are being deleted consider them as deleted
            checks.NotUnderDeleting(NKikimrScheme::StatusPathDoesNotExist);
        }

        if (!Params.GetOptions().GetShowPrivateTable()) {
            checks.IsCommonSensePath();
        }

        if (!checks) {
            if (Params.HasPathId()) {
                pathStr = path.PathString();
            }

            Result.Reset(new TEvSchemeShard::TEvDescribeSchemeResultBuilder(pathStr, pathId));
            Result->Record.SetStatus(checks.GetStatus());
            Result->Record.SetReason(checks.GetError());

            TPath firstExisted = path.FirstExistedParent();
            FillLastExistedPrefixDescr(firstExisted);

            if (checks.GetStatus() == NKikimrScheme::StatusRedirectDomain) {
                DescribeDomain(firstExisted.Base());
            }

            return std::move(Result);
        }
    }

    Result = MakeHolder<TEvSchemeShard::TEvDescribeSchemeResultBuilder>(pathStr, pathId);

    auto descr = Result->Record.MutablePathDescription()->MutableSelf();
    FillPathDescr(descr, path);
    BuildEffectiveACL(descr, path);
    auto base = path.Base();
    DescribeDomain(base);
    DescribeUserAttributes(base);
    DescribePathVersion(path);

    if (base->IsCreateFinished()) {
        switch (base->PathType) {
        case NKikimrSchemeOp::EPathTypeDir:
            DescribeDir(path);
            if (base->IsRoot()) {
                DescribeDomainExtra(base);
            }
            break;
        case NKikimrSchemeOp::EPathTypeSubDomain:
        case NKikimrSchemeOp::EPathTypeExtSubDomain:
            DescribeDir(path);
            DescribeDomainExtra(base);
            DescribeRevertedMigrations(base);
            break;
        case NKikimrSchemeOp::EPathTypeTable:
            DescribeTable(ctx, base->PathId, base);
            break;
        case NKikimrSchemeOp::EPathTypeColumnStore:
            DescribeDir(path);
            DescribeOlapStore(base->PathId, base);
            break;
        case NKikimrSchemeOp::EPathTypeColumnTable:
            DescribeColumnTable(base->PathId, base);
            break;
        case NKikimrSchemeOp::EPathTypePersQueueGroup:
            DescribePersQueueGroup(base->PathId, base);
            break;
        case NKikimrSchemeOp::EPathTypeRtmrVolume:
            DescribeRtmrVolume(base->PathId, base);
            break;
        case NKikimrSchemeOp::EPathTypeBlockStoreVolume:
            DescribeBlockStoreVolume(base->PathId, base);
            break;
        case NKikimrSchemeOp::EPathTypeFileStore:
            DescribeFileStore(base->PathId, base);
            break;
        case NKikimrSchemeOp::EPathTypeKesus:
            DescribeKesus(base->PathId, base);
            break;
        case NKikimrSchemeOp::EPathTypeSolomonVolume:
            DescribeSolomonVolume(base->PathId, base, Params.GetOptions().GetReturnChannelsBinding());
            break;
        case NKikimrSchemeOp::EPathTypeTableIndex:
            DescribeTableIndex(path);
            break;
        case NKikimrSchemeOp::EPathTypeCdcStream:
            DescribeCdcStream(path);
            break;
        case NKikimrSchemeOp::EPathTypeSequence:
            DescribeSequence(path.Base()->PathId, path.Base());
            break;
        case NKikimrSchemeOp::EPathTypeReplication:
            DescribeReplication(path.Base()->PathId, path.Base());
            break;
        case NKikimrSchemeOp::EPathTypeBlobDepot:
            DescribeBlobDepot(path);
            break;
        case NKikimrSchemeOp::EPathTypeExternalTable:
            DescribeExternalTable(ctx, base->PathId, base);
            break;
        case NKikimrSchemeOp::EPathTypeExternalDataSource:
            DescribeExternalDataSource(ctx, base->PathId, base);
            break;
        case NKikimrSchemeOp::EPathTypeView:
            DescribeView(ctx, base->PathId, base);
            break;
        case NKikimrSchemeOp::EPathTypeResourcePool:
            DescribeResourcePool(base->PathId, base);
            break;
        case NKikimrSchemeOp::EPathTypeInvalid:
            Y_UNREACHABLE();
        }
    } else {
        // here we do not full any object specific information, like table description
        // nevertheless, children list should be set even when dir or children is being created right now
        DescribeChildren(path);
    }

    Result->Record.SetStatus(NKikimrScheme::StatusSuccess);
    return std::move(Result);
}

THolder<TEvSchemeShard::TEvDescribeSchemeResultBuilder> DescribePath(
    TSchemeShard* self,
    const TActorContext& ctx,
    TPathId pathId,
    const NKikimrSchemeOp::TDescribeOptions& opts
) {
    NKikimrSchemeOp::TDescribePath params;
    params.SetSchemeshardId(pathId.OwnerId); // ????
    params.SetPathId(pathId.LocalPathId);
    params.MutableOptions()->CopyFrom(opts);

    return TPathDescriber(self, std::move(params)).Describe(ctx);
}

THolder<TEvSchemeShard::TEvDescribeSchemeResultBuilder> DescribePath(
    TSchemeShard* self,
    const TActorContext& ctx,
    TPathId pathId
) {
    NKikimrSchemeOp::TDescribeOptions options;
    options.SetShowPrivateTable(true);
    return DescribePath(self, ctx, pathId, options);
}

void TSchemeShard::DescribeTable(
        const TTableInfo& tableInfo,
        const NScheme::TTypeRegistry* typeRegistry,
        bool fillConfig,
        NKikimrSchemeOp::TTableDescription* entry
    ) const
{
    Y_UNUSED(typeRegistry);

    entry->SetTableSchemaVersion(tableInfo.AlterVersion);
    FillColumns(tableInfo, *entry->MutableColumns());
    FillKeyColumns(tableInfo, *entry->MutableKeyColumnNames(), *entry->MutableKeyColumnIds());

    if (fillConfig) {
        FillPartitionConfig(tableInfo.PartitionConfig(), *entry->MutablePartitionConfig());
    }

    if (tableInfo.HasTTLSettings()) {
        entry->MutableTTLSettings()->CopyFrom(tableInfo.TTLSettings());
    }

    if (tableInfo.HasReplicationConfig()) {
        entry->MutableReplicationConfig()->CopyFrom(tableInfo.ReplicationConfig());
    }

    if (tableInfo.HasIncrementalBackupConfig()) {
        entry->MutableIncrementalBackupConfig()->CopyFrom(tableInfo.IncrementalBackupConfig());
    }

    entry->SetIsBackup(tableInfo.IsBackup);
}

void TSchemeShard::DescribeTableIndex(const TPathId& pathId, const TString& name,
    bool fillConfig, bool fillBoundaries, NKikimrSchemeOp::TIndexDescription& entry) const
{
    auto it = Indexes.FindPtr(pathId);
    Y_ABORT_UNLESS(it, "TableIndex is not found");
    TTableIndexInfo::TPtr indexInfo = *it;

    DescribeTableIndex(pathId, name, indexInfo, fillConfig, fillBoundaries, entry);
}

void TSchemeShard::DescribeTableIndex(const TPathId& pathId, const TString& name, TTableIndexInfo::TPtr indexInfo,
    bool fillConfig, bool fillBoundaries, NKikimrSchemeOp::TIndexDescription& entry) const
{
    Y_ABORT_UNLESS(indexInfo, "Empty index info");

    entry.SetName(name);
    entry.SetLocalPathId(pathId.LocalPathId);
    entry.SetPathOwnerId(pathId.OwnerId);

    entry.SetType(indexInfo->Type);
    entry.SetState(indexInfo->State);
    entry.SetSchemaVersion(indexInfo->AlterVersion);

    for (const auto& keyName: indexInfo->IndexKeys) {
        *entry.MutableKeyColumnNames()->Add() = keyName;
    }

    for (const auto& dataColumns: indexInfo->IndexDataColumns) {
        *entry.MutableDataColumnNames()->Add() = dataColumns;
    }

    const auto* indexPathPtr = PathsById.FindPtr(pathId);
    Y_ABORT_UNLESS(indexPathPtr);
    const auto& indexPath = *indexPathPtr->Get();
    if (const auto size = indexPath.GetChildren().size(); indexInfo->Type == NKikimrSchemeOp::EIndexType::EIndexTypeGlobalVectorKmeansTree) {
        // For vector index we have 2 impl tables and 2 tmp impl tables
        Y_VERIFY_S(2 <= size && size <= 4, size);
    } else {
        Y_VERIFY_S(size == 1, size);
    }

    ui64 dataSize = 0;
    for (const auto& indexImplTablePathId : indexPath.GetChildren()) {
        const auto* tableInfoPtr = Tables.FindPtr(indexImplTablePathId.second);
        if (!tableInfoPtr && NTableIndex::IsTmpImplTable(indexImplTablePathId.first)) {
            continue; // it's possible because of dropping tmp index impl tables without dropping index
        }
        Y_ABORT_UNLESS(tableInfoPtr);
        const auto& tableInfo = *tableInfoPtr->Get();

        const auto& tableStats = tableInfo.GetStats().Aggregated;
        dataSize += tableStats.DataSize + tableStats.IndexSize;

        auto* tableDescription = entry.AddIndexImplTableDescriptions();
        if (fillConfig) {
            FillPartitionConfig(tableInfo.PartitionConfig(), *tableDescription->MutablePartitionConfig());
        }
        if (fillBoundaries) {
            // column info is necessary for split boundary type conversion
            FillColumns(tableInfo, *tableDescription->MutableColumns());
            FillKeyColumns(tableInfo, *tableDescription->MutableKeyColumnNames(), *tableDescription->MutableKeyColumnIds());
            FillTableBoundaries(tableDescription->MutableSplitBoundary(), tableInfo);
        }
    }
    entry.SetDataSize(dataSize);

    if (indexInfo->Type == NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree) {
        if (const auto* vectorIndexKmeansTreeDescription = std::get_if<NKikimrSchemeOp::TVectorIndexKmeansTreeDescription>(&indexInfo->SpecializedIndexDescription)) {
            const auto& indexInfoSettings = vectorIndexKmeansTreeDescription->GetSettings();
            auto entrySettings = entry.MutableVectorIndexKmeansTreeDescription()->MutableSettings();
            if (indexInfoSettings.has_distance())
                entrySettings->set_distance(indexInfoSettings.distance());
            else if (indexInfoSettings.has_similarity())
                entrySettings->set_similarity(indexInfoSettings.similarity());
            else
                Y_FAIL_S("Either distance or similarity should be set in index settings: " << indexInfoSettings);
            entrySettings->set_vector_type(indexInfoSettings.vector_type());
            entrySettings->set_vector_dimension(indexInfoSettings.vector_dimension());
        } else {
            Y_FAIL_S("SpecializedIndexDescription should be set");
        }
    }

}

void TSchemeShard::DescribeCdcStream(const TPathId& pathId, const TString& name,
        NKikimrSchemeOp::TCdcStreamDescription& desc)
{
    Y_VERIFY_S(CdcStreams.contains(pathId), "Cdc stream not found"
        << ": pathId# " << pathId
        << ", name# " << name);
    DescribeCdcStream(pathId, name, CdcStreams.at(pathId), desc);
}

void TSchemeShard::DescribeCdcStream(const TPathId& pathId, const TString& name, TCdcStreamInfo::TPtr info,
        NKikimrSchemeOp::TCdcStreamDescription& desc)
{
    Y_VERIFY_S(info, "Empty cdc stream info"
        << ": pathId# " << pathId
        << ", name# " << name);

    desc.SetName(name);
    desc.SetMode(info->Mode);
    desc.SetFormat(info->Format);
    desc.SetVirtualTimestamps(info->VirtualTimestamps);
    desc.SetResolvedTimestampsIntervalMs(info->ResolvedTimestamps.MilliSeconds());
    desc.SetAwsRegion(info->AwsRegion);
    PathIdFromPathId(pathId, desc.MutablePathId());
    desc.SetState(info->State);
    desc.SetSchemaVersion(info->AlterVersion);

    if (info->ScanShards) {
        auto& scanProgress = *desc.MutableScanProgress();
        scanProgress.SetShardsTotal(info->ScanShards.size());
        scanProgress.SetShardsCompleted(info->DoneShards.size());
    }

    Y_ABORT_UNLESS(PathsById.contains(pathId));
    auto path = PathsById.at(pathId);

    for (const auto& [key, value] : path->UserAttrs->Attrs) {
        auto& attr = *desc.AddUserAttributes();
        attr.SetKey(key);
        attr.SetValue(value);
    }
}

void TSchemeShard::DescribeSequence(const TPathId& pathId, const TString& name,
        NKikimrSchemeOp::TSequenceDescription& desc, bool fillSetVal)
{
    auto it = Sequences.find(pathId);
    Y_VERIFY_S(it != Sequences.end(), "Sequence not found"
        << " pathId# " << pathId
        << " name# " << name);
    DescribeSequence(pathId, name, it->second, desc, fillSetVal);
}

void TSchemeShard::DescribeSequence(const TPathId& pathId, const TString& name, TSequenceInfo::TPtr info,
        NKikimrSchemeOp::TSequenceDescription& desc, bool fillSetVal)
{
    Y_VERIFY_S(info, "Empty sequence info"
        << " pathId# " << pathId
        << " name# " << name);

    desc = info->Description;

    if (!fillSetVal) {
        desc.ClearSetVal();
    }

    desc.SetName(name);
    PathIdFromPathId(pathId, desc.MutablePathId());
    desc.SetVersion(info->AlterVersion);

    if (info->Sharding.SequenceShardsSize() > 0) {
        auto lastIdx = info->Sharding.SequenceShardsSize() - 1;
        TShardIdx shardIdx = FromProto(info->Sharding.GetSequenceShards(lastIdx));
        const auto& shardInfo = ShardInfos.at(shardIdx);
        if (shardInfo.TabletID != InvalidTabletId) {
            desc.SetSequenceShard(ui64(shardInfo.TabletID));
        }
    }
}

void TSchemeShard::DescribeReplication(const TPathId& pathId, const TString& name,
        NKikimrSchemeOp::TReplicationDescription& desc)
{
    auto it = Replications.find(pathId);
    Y_VERIFY_S(it != Replications.end(), "Replication not found"
        << " pathId# " << pathId
        << " name# " << name);
    DescribeReplication(pathId, name, it->second, desc);
}

static void ClearSensitiveFields(google::protobuf::Message* message) {
    const auto* desc = message->GetDescriptor();
    const auto* self = message->GetReflection();

    for (int i = 0; i < desc->field_count(); ++i) {
        const auto* field = desc->field(i);
        if (field->options().GetExtension(Ydb::sensitive)) {
            self->ClearField(message, field);
        } else if (field->message_type()) {
            if (!field->is_repeated() && self->HasField(*message, field)) {
                ClearSensitiveFields(self->MutableMessage(message, field));
            } else if (field->is_repeated()) {
                for (int j = 0, size = self->FieldSize(*message, field); j < size; ++j) {
                    ClearSensitiveFields(self->MutableRepeatedMessage(message, field, j));
                }
            }
        }
    }
}

void TSchemeShard::DescribeReplication(const TPathId& pathId, const TString& name, TReplicationInfo::TPtr info,
        NKikimrSchemeOp::TReplicationDescription& desc)
{
    Y_VERIFY_S(info, "Empty replication info"
        << " pathId# " << pathId
        << " name# " << name);

    desc = info->Description;
    ClearSensitiveFields(&desc);

    desc.SetName(name);
    PathIdFromPathId(pathId, desc.MutablePathId());
    desc.SetVersion(info->AlterVersion);

    if (const auto& shardIdx = info->ControllerShardIdx; shardIdx != InvalidShardIdx) {
        Y_ABORT_UNLESS(ShardInfos.contains(shardIdx));
        const auto& shardInfo = ShardInfos.at(shardIdx);

        if (shardInfo.TabletID != InvalidTabletId) {
            desc.SetControllerId(ui64(shardInfo.TabletID));
        }
    }
}

void TSchemeShard::DescribeBlobDepot(const TPathId& pathId, const TString& name, NKikimrSchemeOp::TBlobDepotDescription& desc) {
    auto it = BlobDepots.find(pathId);
    Y_ABORT_UNLESS(it != BlobDepots.end());
    desc = it->second->Description;
    desc.SetName(name);
    PathIdFromPathId(pathId, desc.MutablePathId());
    desc.SetVersion(it->second->AlterVersion);
    desc.SetTabletId(static_cast<ui64>(it->second->BlobDepotTabletId));
}

} // NSchemeShard
} // NKikimr

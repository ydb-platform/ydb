#include "schemeshard_path_describer.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/engine/mkql_proto.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>

#include <util/stream/format.h>

namespace NKikimr {
namespace NSchemeShard {

static void FillTableStats(NKikimrTableStats::TTableStats* stats, const TTableInfo::TPartitionStats& tableStats) {
    stats->SetRowCount(tableStats.RowCount);
    stats->SetDataSize(tableStats.DataSize);
    stats->SetIndexSize(tableStats.IndexSize);
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
}

static void FillTableMetrics(NKikimrTabletBase::TMetrics* metrics, const TTableInfo::TPartitionStats& tableStats) {
    metrics->SetCPU(tableStats.GetCurrentRawCpuUsage());
    metrics->SetMemory(tableStats.Memory);
    metrics->SetNetwork(tableStats.Network);
    metrics->SetStorage(tableStats.Storage);
    metrics->SetReadThroughput(tableStats.ReadThroughput);
    metrics->SetWriteThroughput(tableStats.WriteThroughput);
    metrics->SetReadIops(tableStats.ReadIops);
    metrics->SetWriteIops(tableStats.WriteIops);
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

    descr->SetParentPathId(pathEl->ParentPathId.LocalPathId); //???? ParnetPathOwnerId

    descr->SetPathType(pathEl->PathType);
    descr->SetOwner(pathEl->Owner);

    descr->SetPathState(pathEl->PathState); // ???? can't be consistent KIKIMR-8861
    descr->SetCreateFinished(createFinished); // use this insted PathState

    descr->SetCreateTxId(ui64(pathEl->CreateTxId));
    if (createFinished) {
        descr->SetCreateStep(ui64(pathEl->StepCreated));
    }

    if (pathEl->PathType == NKikimrSchemeOp::EPathTypePersQueueGroup) {
        auto it = Self->PersQueueGroups.FindPtr(pathEl->PathId);
        Y_VERIFY(it, "PersQueueGroup is not found");

        TPersQueueGroupInfo::TPtr pqGroupInfo = *it;
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
    Y_VERIFY(parentPath.IsResolved());

    if (parentPath.IsTableIndex()) {
        const auto& pathId = parentPath.Base()->PathId;
        Y_VERIFY(Self->Indexes.contains(pathId));
        auto indexInfo = Self->Indexes.at(pathId);

        switch (indexInfo->Type) {
        case NKikimrSchemeOp::EIndexTypeGlobalAsync:
            return TPathElement::EPathSubType::EPathSubTypeAsyncIndexImplTable;
        default:
            return TPathElement::EPathSubType::EPathSubTypeSyncIndexImplTable;
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
    Y_VERIFY(path.IsResolved());
    Y_VERIFY(!path.IsDeleted());

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
            TPathElement::TPtr childEl = *Self->PathsById.FindPtr(childId);
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

void TPathDescriber::DescribeTable(const TActorContext& ctx, TPathId pathId, TPathElement::TPtr pathEl) {
    const NScheme::TTypeRegistry* typeRegistry = AppData(ctx)->TypeRegistry;
    const TTableInfo::TPtr tableInfo = *Self->Tables.FindPtr(pathId);
    auto pathDescription = Result->Record.MutablePathDescription();
    auto entry = pathDescription->MutableTable();

    bool returnConfig = Params.GetReturnPartitionConfig();
    bool returnPartitioning = Params.GetReturnPartitioningInfo();
    bool returnPartitionStats = Params.GetOptions().GetReturnPartitionStats();
    bool returnBackupInfo = Params.GetBackupInfo();
    bool returnBoundaries = false;
    if (Params.HasOptions()) {
        returnConfig = Params.GetOptions().GetReturnPartitionConfig();
        returnPartitioning = Params.GetOptions().GetReturnPartitioningInfo();
        returnBackupInfo = Params.GetOptions().GetBackupInfo();
        returnBoundaries = Params.GetOptions().GetReturnBoundaries();
    }

    Self->DescribeTable(tableInfo, typeRegistry, returnConfig, returnBoundaries, entry);
    entry->SetName(pathEl->Name);

    if (returnPartitioning) {
        // partitions
        if (tableInfo->PreSerializedPathDescription.empty()) {
            NKikimrScheme::TEvDescribeSchemeResult preSerializedResult;
            NKikimrSchemeOp::TPathDescription& pathDescription = *preSerializedResult.MutablePathDescription();
            pathDescription.MutableTablePartitions()->Reserve(tableInfo->GetPartitions().size());
            for (auto& p : tableInfo->GetPartitions()) {
                auto part = pathDescription.AddTablePartitions();
                auto datashardIdx = p.ShardIdx;
                auto datashardTabletId = Self->ShardInfos[datashardIdx].TabletID;
                // Currently we only support uniform partitioning where each range is [start, end)
                // +inf as the end of the last range is represented by empty TCell vector
                part->SetDatashardId(ui64(datashardTabletId));
                part->SetIsPoint(false);
                part->SetIsInclusive(false);
                part->SetEndOfRangeKeyPrefix(p.EndOfRange);
            }
            Y_PROTOBUF_SUPPRESS_NODISCARD preSerializedResult.SerializeToString(&tableInfo->PreSerializedPathDescription);
        }
        Result->PreSerializedData += tableInfo->PreSerializedPathDescription;
        if (!pathEl->IsCreateFinished()) {
            tableInfo->PreSerializedPathDescription.clear(); // KIKIMR-4337
        }
    }

    const auto& tableStats(tableInfo->GetStats().Aggregated);

    {
        auto* stats = Result->Record.MutablePathDescription()->MutableTableStats();
        FillTableStats(stats, tableStats);
    }

    {
        auto* metrics = Result->Record.MutablePathDescription()->MutableTabletMetrics();
        FillTableMetrics(metrics, tableStats);
    }

    if (returnPartitionStats) {
        NKikimrSchemeOp::TPathDescription& pathDescription = *Result->Record.MutablePathDescription();
        pathDescription.MutableTablePartitionStats()->Reserve(tableInfo->GetPartitions().size());
        for (auto& p : tableInfo->GetPartitions()) {
            const auto* stats = tableInfo->GetStats().PartitionStats.FindPtr(p.ShardIdx);
            Y_VERIFY(stats);
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
                tableInfo->BackupSettings.GetYTSettings());
            progress->SetDataTotalSize(txState.DataTotalSize);
            progress->SetTxId(ui64(txId));

            LOG_TRACE(ctx, NKikimrServices::SCHEMESHARD_DESCRIBE,
                      "Add backup process info");
            break;
        }

        /* Get information about last completed backup */
        for (const auto& iter: tableInfo->BackupHistory) {
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

        Y_VERIFY(Self->PathsById.contains(childPathId));
        auto childPath = Self->PathsById.at(childPathId);

        if (childPath->Dropped()) {
            continue;
        }

        if (!childPath->IsCreateFinished()) {
            continue;
        }

        switch (childPath->PathType) {
        case NKikimrSchemeOp::EPathTypeTableIndex:
            Self->DescribeTableIndex(childPathId, childName, *entry->AddTableIndexes());
            break;
        case NKikimrSchemeOp::EPathTypeCdcStream:
            Self->DescribeCdcStream(childPathId, childName, *entry->AddCdcStreams());
            break;
        case NKikimrSchemeOp::EPathTypeSequence:
            Self->DescribeSequence(childPathId, childName, *entry->AddSequences());
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
    const TOlapStoreInfo::TPtr storeInfo = *Self->OlapStores.FindPtr(pathId);
    Y_VERIFY(storeInfo, "OlapStore not found");
    Y_UNUSED(pathEl);

    auto description = Result->Record.MutablePathDescription()->MutableColumnStoreDescription();
    description->CopyFrom(storeInfo->Description);

    description->ClearColumnShards();
    description->MutableColumnShards()->Reserve(storeInfo->ColumnShards.size());
    for (auto& shard : storeInfo->ColumnShards) {
        auto shardInfo = Self->ShardInfos.FindPtr(shard);
        Y_VERIFY(shardInfo, "ColumnShard not found");
        description->AddColumnShards(shardInfo->TabletID.GetValue());
    }
}

void TPathDescriber::DescribeColumnTable(TPathId pathId, TPathElement::TPtr pathEl) {
    const TColumnTableInfo::TPtr tableInfo = *Self->ColumnTables.FindPtr(pathId);
    Y_VERIFY(tableInfo, "ColumnTable not found");
    const TOlapStoreInfo::TPtr storeInfo = *Self->OlapStores.FindPtr(tableInfo->OlapStorePathId);
    Y_VERIFY(storeInfo, "OlapStore not found");
    Y_UNUSED(pathEl);

    auto description = Result->Record.MutablePathDescription()->MutableColumnTableDescription();
    description->CopyFrom(tableInfo->Description);
    description->MutableSharding()->CopyFrom(tableInfo->Sharding);

    if (!description->HasSchema() && description->HasSchemaPresetId()) {
        auto& preset = storeInfo->SchemaPresets.at(description->GetSchemaPresetId());
        auto& presetProto = storeInfo->Description.GetSchemaPresets(preset.ProtoIndex);
        *description->MutableSchema() = presetProto.GetSchema();
        if (description->HasSchemaPresetVersionAdj()) {
            description->MutableSchema()->SetVersion(description->GetSchema().GetVersion() + description->GetSchemaPresetVersionAdj());
        }
    }
}

void TPathDescriber::DescribePersQueueGroup(TPathId pathId, TPathElement::TPtr pathEl) {
    auto it = Self->PersQueueGroups.FindPtr(pathId);
    Y_VERIFY(it, "PersQueueGroup is not found");
    TPersQueueGroupInfo::TPtr pqGroupInfo = *it;

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

    Y_VERIFY_DEBUG(!pqGroupInfo->PreSerializedPathDescription.empty());
    Result->PreSerializedData += pqGroupInfo->PreSerializedPathDescription;

    bool returnPartitioning = Params.GetReturnPartitioningInfo();
    if (returnPartitioning) {
        if (pqGroupInfo->PreSerializedPartitionsDescription.empty()) {
            NKikimrScheme::TEvDescribeSchemeResult preSerializedResult;
            auto entry = preSerializedResult.MutablePathDescription()->MutablePersQueueGroup();

            struct TPartitionDesc {
                TTabletId TabletId = InvalidTabletId;
                const TPQShardInfo::TPersQueueInfo* Info = nullptr;
            };

            TVector<TPartitionDesc> descriptions; // index is pqId
            descriptions.resize(pqGroupInfo->TotalPartitionCount);

            for (const auto& [shardIdx, pqShard] : pqGroupInfo->Shards) {
                auto it = Self->ShardInfos.find(shardIdx);
                Y_VERIFY_S(it != Self->ShardInfos.end(), "No shard with shardIdx: " << shardIdx);

                for (const auto& pq : pqShard->PQInfos) {
                    if (pq.AlterVersion <= pqGroupInfo->AlterVersion) {
                        Y_VERIFY_S(pq.PqId < pqGroupInfo->NextPartitionId,
                            "Wrong pqId: " << pq.PqId << ", nextPqId: " << pqGroupInfo->NextPartitionId);
                        descriptions[pq.PqId] = {it->second.TabletID, &pq};
                    }
                }
            }

            for (ui32 pqId = 0; pqId < descriptions.size(); ++pqId) {
                const auto& desc = descriptions.at(pqId);
                auto& partition = *entry->AddPartitions();

                Y_VERIFY_S(desc.TabletId, "Unassigned tabetId for partition: " << pqId);
                Y_VERIFY_S(desc.Info, "Empty info for partition: " << pqId);

                partition.SetPartitionId(pqId);
                partition.SetTabletId(ui64(desc.TabletId));
                if (desc.Info->KeyRange) {
                    desc.Info->KeyRange->SerializeToProto(*partition.MutableKeyRange());
                }
            }

            Y_PROTOBUF_SUPPRESS_NODISCARD preSerializedResult.SerializeToString(&pqGroupInfo->PreSerializedPartitionsDescription);
        }

        Y_VERIFY_DEBUG(!pqGroupInfo->PreSerializedPartitionsDescription.empty());
        Result->PreSerializedData += pqGroupInfo->PreSerializedPartitionsDescription;
    }

    Y_VERIFY_DEBUG(!Result->PreSerializedData.empty());
    if (!pathEl->IsCreateFinished()) {
        // Don't cache until create finishes (KIKIMR-4337)
        pqGroupInfo->PreSerializedPathDescription.clear();
        pqGroupInfo->PreSerializedPartitionsDescription.clear();
    }
}

void TPathDescriber::DescribeRtmrVolume(TPathId pathId, TPathElement::TPtr pathEl) {
    auto it = Self->RtmrVolumes.FindPtr(pathId);
    Y_VERIFY(it, "RtmrVolume is not found");
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
    Self->DescribeTableIndex(path.Base()->PathId, path.Base()->Name,
        *Result->Record.MutablePathDescription()->MutableTableIndex());
    DescribeChildren(path);
}

void TPathDescriber::DescribeCdcStream(const TPath& path) {
    Self->DescribeCdcStream(path.Base()->PathId, path.Base()->Name,
        *Result->Record.MutablePathDescription()->MutableCdcStreamDescription());
    DescribeChildren(path);
}

void TPathDescriber::DescribeSolomonVolume(TPathId pathId, TPathElement::TPtr pathEl, bool returnChannelsBinding) {
    auto it = Self->SolomonVolumes.FindPtr(pathId);
    Y_VERIFY(it, "SolomonVolume is not found");
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
        Y_VERIFY(shardInfo);

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
    TPathId domainId = Self->ResolveDomainId(pathEl);

    TPathElement::TPtr domainEl = Self->PathsById.at(domainId);
    Y_VERIFY(domainEl);

    DescribeDomainRoot(domainEl);
}

void TPathDescriber::DescribeRevertedMigrations(TPathElement::TPtr pathEl) {
    Y_VERIFY(pathEl->IsDomainRoot());

    if (!Self->RevertedMigrations.contains(pathEl->PathId)) {
        return;
    }

    auto list = Result->Record.MutablePathDescription()->MutableAbandonedTenantsSchemeShards();
    for (const TTabletId& abandonedSchemeShardId: Self->RevertedMigrations.at(pathEl->PathId)) {
        list->Add(ui64(abandonedSchemeShardId));
    }
}

void TPathDescriber::DescribeDomainRoot(TPathElement::TPtr pathEl) {
    Y_VERIFY(pathEl->IsDomainRoot());
    auto it = Self->SubDomains.FindPtr(pathEl->PathId);
    Y_VERIFY(it, "SubDomain not found");
    auto subDomainInfo = *it;

    NKikimrSubDomains::TDomainDescription * entry = Result->Record.MutablePathDescription()->MutableDomainDescription();

    NKikimrSubDomains::TDomainKey *key = entry->MutableDomainKey();
    entry->SetSchemeShardId_Depricated(Self->ParentDomainId.OwnerId);
    entry->SetPathId_Depricated(Self->ParentDomainId.LocalPathId);

    if (pathEl->IsRoot()) {
        key->SetSchemeShard(Self->ParentDomainId.OwnerId);
        key->SetPathId(Self->ParentDomainId.LocalPathId);
    } else {
        key->SetSchemeShard(pathEl->PathId.OwnerId);
        key->SetPathId(pathEl->PathId.LocalPathId);
    }

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

    if (subDomainInfo->GetDeclaredSchemeQuotas()) {
        entry->MutableDeclaredSchemeQuotas()->CopyFrom(*subDomainInfo->GetDeclaredSchemeQuotas());
    }

    if (const auto& databaseQuotas = subDomainInfo->GetDatabaseQuotas()) {
        entry->MutableDatabaseQuotas()->CopyFrom(*databaseQuotas);
    }

    if (subDomainInfo->GetDiskQuotaExceeded()) {
        entry->MutableDomainState()->SetDiskQuotaExceeded(true);
    }
}

void TPathDescriber::DescribeDomainExtra(TPathElement::TPtr pathEl) {
    Y_VERIFY(pathEl->IsDomainRoot());
    auto it = Self->SubDomains.FindPtr(pathEl->PathId);
    Y_VERIFY(it, "SubDomain not found");
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
    Y_VERIFY(pathEl->IsBlockStoreVolume());
    auto it = Self->BlockStoreVolumes.FindPtr(pathId);
    Y_VERIFY(it, "BlockStore volume is not found");
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
    Y_VERIFY(pathEl->IsFileStore());
    auto it = Self->FileStoreInfos.FindPtr(pathId);
    Y_VERIFY(it, "FileStore info is not found");
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
    Y_VERIFY(pathEl->IsKesus());
    auto it = Self->KesusInfos.FindPtr(pathId);
    Y_VERIFY(it, "Kesus info not found");
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
    Y_VERIFY(pathEl->IsSequence());
    Self->DescribeSequence(pathId, pathEl->Name, *Result->Record.MutablePathDescription()->MutableSequenceDescription());
}

void TPathDescriber::DescribeReplication(TPathId pathId, TPathElement::TPtr pathEl) {
    Y_VERIFY(pathEl->IsReplication());
    Self->DescribeReplication(pathId, pathEl->Name, *Result->Record.MutablePathDescription()->MutableReplicationDescription());
}

void TPathDescriber::DescribeBlobDepot(const TPath& path) {
    Y_VERIFY(path->IsBlobDepot());
    Self->DescribeBlobDepot(path->PathId, path->Name, *Result->Record.MutablePathDescription()->MutableBlobDepotDescription());
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

        if (checks && !path.Base()->IsTable() && !path.Base()->IsTableIndex() && !path.Base()->IsDirectory()) {
            // KIKIMR-13173
            // PQ BSV drop their shard before PlatStep
            // If they are being deleted consider them as deleted
            checks.NotUnderDeleting(NKikimrScheme::StatusPathDoesNotExist);
        }

        if (!Params.GetOptions().GetShowPrivateTable()) {
            checks.IsCommonSensePath();
        }

        if (!checks) {
            TString explain;
            auto status = checks.GetStatus(&explain);

            if (Params.HasPathId()) {
                pathStr = path.PathString();
            }

            Result.Reset(new TEvSchemeShard::TEvDescribeSchemeResultBuilder(
                pathStr,
                Self->TabletID(),
                pathId
                ));
            Result->Record.SetStatus(status);
            Result->Record.SetReason(explain);

            TPath firstExisted = path.FirstExistedParent();
            FillLastExistedPrefixDescr(firstExisted);

            if (status ==  NKikimrScheme::StatusRedirectDomain) {
                DescribeDomain(firstExisted.Base());
            }

            return std::move(Result);
        }
    }

    Result = MakeHolder<TEvSchemeShard::TEvDescribeSchemeResultBuilder>(pathStr, Self->TabletID(), pathId);

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
        case NKikimrSchemeOp::EPathTypeInvalid:
            Y_UNREACHABLE();
        }
    } else {
        // here we do not full any object specific information, like table description
        // nevertheless, chindren list should be set even when dir or children is being created right now
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

void TSchemeShard::DescribeTable(const TTableInfo::TPtr tableInfo, const NScheme::TTypeRegistry* typeRegistry,
                                     bool fillConfig, bool fillBoundaries, NKikimrSchemeOp::TTableDescription* entry) const
{
    THashMap<ui32, TString> familyNames;
    bool familyNamesBuilt = false;

    entry->SetTableSchemaVersion(tableInfo->AlterVersion);
    entry->MutableColumns()->Reserve(tableInfo->Columns.size());
    for (auto col : tableInfo->Columns) {
        const auto& cinfo = col.second;
        if (cinfo.IsDropped())
            continue;

        auto colDescr = entry->AddColumns();
        colDescr->SetName(cinfo.Name);
        colDescr->SetType(typeRegistry->GetTypeName(cinfo.PType));
        colDescr->SetTypeId(cinfo.PType);
        colDescr->SetId(cinfo.Id);
        colDescr->SetNotNull(cinfo.NotNull);

        if (cinfo.Family != 0) {
            colDescr->SetFamily(cinfo.Family);

            if (!familyNamesBuilt) {
                for (const auto& family : tableInfo->PartitionConfig().GetColumnFamilies()) {
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

        switch (cinfo.DefaultKind) {
            case ETableColumnDefaultKind::None:
                break;
            case ETableColumnDefaultKind::FromSequence:
                colDescr->SetDefaultFromSequence(cinfo.DefaultValue);
                break;
        }
    }
    Y_VERIFY(!tableInfo->KeyColumnIds.empty());

    entry->MutableKeyColumnNames()->Reserve(tableInfo->KeyColumnIds.size());
    entry->MutableKeyColumnIds()->Reserve(tableInfo->KeyColumnIds.size());
    for (ui32 keyColId : tableInfo->KeyColumnIds) {
        entry->AddKeyColumnNames(tableInfo->Columns[keyColId].Name);
        entry->AddKeyColumnIds(keyColId);
    }

    if (fillConfig) {
        entry->MutablePartitionConfig()->CopyFrom(tableInfo->PartitionConfig());
        TPartitionConfigMerger::DeduplicateColumnFamiliesById(*entry->MutablePartitionConfig());
        entry->MutablePartitionConfig()->MutableStorageRooms()->Clear();
    }

    if (fillBoundaries) {
        FillTableBoundaries(tableInfo, *entry->MutableSplitBoundary());
    }

    if (tableInfo->HasTTLSettings()) {
        entry->MutableTTLSettings()->CopyFrom(tableInfo->TTLSettings());
    }

    entry->SetIsBackup(tableInfo->IsBackup);
}

void TSchemeShard::DescribeTableIndex(const TPathId& pathId, const TString& name,
        NKikimrSchemeOp::TIndexDescription& entry)
{
    auto it = Indexes.FindPtr(pathId);
    Y_VERIFY(it, "TableIndex is not found");
    TTableIndexInfo::TPtr indexInfo = *it;

    DescribeTableIndex(pathId, name, indexInfo, entry);
}

void TSchemeShard::DescribeTableIndex(const TPathId& pathId, const TString& name, TTableIndexInfo::TPtr indexInfo,
        NKikimrSchemeOp::TIndexDescription& entry)
{
    Y_VERIFY(indexInfo, "Empty index info");

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

    Y_VERIFY(PathsById.contains(pathId));
    auto indexPath = PathsById.at(pathId);

    Y_VERIFY(indexPath->GetChildren().size() == 1);
    const auto& indexImplPathId = indexPath->GetChildren().begin()->second;

    Y_VERIFY(Tables.contains(indexImplPathId));
    auto indexImplTable = Tables.at(indexImplPathId);

    const auto& tableStats = indexImplTable->GetStats().Aggregated;
    entry.SetDataSize(tableStats.DataSize + tableStats.IndexSize);
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
    PathIdFromPathId(pathId, desc.MutablePathId());
    desc.SetState(info->State);
    desc.SetSchemaVersion(info->AlterVersion);
}

void TSchemeShard::DescribeSequence(const TPathId& pathId, const TString& name,
        NKikimrSchemeOp::TSequenceDescription& desc)
{
    auto it = Sequences.find(pathId);
    Y_VERIFY_S(it != Sequences.end(), "Sequence not found"
        << " pathId# " << pathId
        << " name# " << name);
    DescribeSequence(pathId, name, it->second, desc);
}

void TSchemeShard::DescribeSequence(const TPathId& pathId, const TString& name, TSequenceInfo::TPtr info,
        NKikimrSchemeOp::TSequenceDescription& desc)
{
    Y_VERIFY_S(info, "Empty sequence info"
        << " pathId# " << pathId
        << " name# " << name);

    desc = info->Description;

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

void TSchemeShard::DescribeReplication(const TPathId& pathId, const TString& name, TReplicationInfo::TPtr info,
        NKikimrSchemeOp::TReplicationDescription& desc)
{
    Y_VERIFY_S(info, "Empty sequence info"
        << " pathId# " << pathId
        << " name# " << name);

    desc = info->Description;

    desc.SetName(name);
    PathIdFromPathId(pathId, desc.MutablePathId());
    desc.SetVersion(info->AlterVersion);

    const auto& controllers = ResolveDomainInfo(pathId)->GetReplicationControllers();
    if (!controllers.empty()) {
        const auto shardIdx = *controllers.begin();

        Y_VERIFY(ShardInfos.contains(shardIdx));
        const auto& shardInfo = ShardInfos.at(shardIdx);

        if (shardInfo.TabletID != InvalidTabletId) {
            desc.SetControllerId(ui64(shardInfo.TabletID));
        }
    }
}

void TSchemeShard::DescribeBlobDepot(const TPathId& pathId, const TString& name, NKikimrSchemeOp::TBlobDepotDescription& desc) {
    auto it = BlobDepots.find(pathId);
    Y_VERIFY(it != BlobDepots.end());
    desc = it->second->Description;
    desc.SetName(name);
    PathIdFromPathId(pathId, desc.MutablePathId());
    desc.SetVersion(it->second->AlterVersion);
    desc.SetTabletId(static_cast<ui64>(it->second->BlobDepotTabletId));
}

void TSchemeShard::FillTableBoundaries(const TTableInfo::TPtr tableInfo, google::protobuf::RepeatedPtrField<NKikimrSchemeOp::TSplitBoundary>& boundaries) {
    TString errStr;
    // Number of split boundaries equals to number of partitions - 1
    boundaries.Reserve(tableInfo->GetPartitions().size() - 1);
    for (ui32 pi = 0; pi < tableInfo->GetPartitions().size() - 1; ++pi) {
        const auto& p = tableInfo->GetPartitions()[pi];
        TSerializedCellVec endKey(p.EndOfRange);
        auto boundary = boundaries.Add()->MutableKeyPrefix();
        for (ui32 ki = 0;  ki < endKey.GetCells().size(); ++ki){
            const auto& c = endKey.GetCells()[ki];
            ui32 typeId = tableInfo->Columns[tableInfo->KeyColumnIds[ki]].PType;
            bool ok = NMiniKQL::CellToValue(typeId, c, *boundary->AddTuple(), errStr);
            Y_VERIFY(ok, "Failed to build key tuple at postition %" PRIu32 " error: %s", ki, errStr.data());
        }
    }
}

} // NSchemeShard
} // NKikimr

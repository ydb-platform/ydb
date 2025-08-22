#include "schemeshard_build_index.h"

#include "schemeshard_impl.h"

namespace NKikimr {
namespace NSchemeShard {

void TSchemeShard::Handle(TEvIndexBuilder::TEvCreateRequest::TPtr& ev, const TActorContext& ctx) {
    Execute(CreateTxCreate(ev), ctx);
}

void TSchemeShard::Handle(TEvIndexBuilder::TEvGetRequest::TPtr& ev, const TActorContext& ctx) {
    Execute(CreateTxGet(ev), ctx);
}

void TSchemeShard::Handle(TEvIndexBuilder::TEvCancelRequest::TPtr& ev, const TActorContext& ctx) {
    Execute(CreateTxCancel(ev), ctx);
}

void TSchemeShard::Handle(TEvIndexBuilder::TEvForgetRequest::TPtr& ev, const TActorContext& ctx) {
    Execute(CreateTxForget(ev), ctx);
}

void TSchemeShard::Handle(TEvIndexBuilder::TEvListRequest::TPtr& ev, const TActorContext& ctx) {
    Execute(CreateTxList(ev), ctx);
}

void TSchemeShard::Handle(TEvDataShard::TEvBuildIndexProgressResponse::TPtr& ev, const TActorContext& ctx) {
    Execute(CreateTxReply(ev), ctx);
}

void TSchemeShard::Handle(TEvDataShard::TEvSampleKResponse::TPtr& ev, const TActorContext& ctx) {
    Execute(CreateTxReply(ev), ctx);
}

void TSchemeShard::Handle(TEvDataShard::TEvReshuffleKMeansResponse::TPtr& ev, const TActorContext& ctx) {
    Execute(CreateTxReply(ev), ctx);
}

void TSchemeShard::Handle(TEvDataShard::TEvRecomputeKMeansResponse::TPtr& ev, const TActorContext& ctx) {
    Execute(CreateTxReply(ev), ctx);
}

void TSchemeShard::Handle(TEvDataShard::TEvLocalKMeansResponse::TPtr& ev, const TActorContext& ctx) {
    Execute(CreateTxReply(ev), ctx);
}

void TSchemeShard::Handle(TEvDataShard::TEvPrefixKMeansResponse::TPtr& ev, const TActorContext& ctx) {
    Execute(CreateTxReply(ev), ctx);
}

void TSchemeShard::Handle(TEvIndexBuilder::TEvUploadSampleKResponse::TPtr& ev, const TActorContext& ctx) {
    Execute(CreateTxReply(ev), ctx);
}

void TSchemeShard::Handle(TEvDataShard::TEvValidateUniqueIndexResponse::TPtr& ev, const TActorContext& ctx) {
    Execute(CreateTxReply(ev), ctx);
}

void TSchemeShard::Handle(TEvPrivate::TEvIndexBuildingMakeABill::TPtr& ev, const TActorContext& ctx) {
    Execute(CreateTxBilling(ev), ctx);
}

void TSchemeShard::PersistCreateBuildIndex(NIceDb::TNiceDb& db, const TIndexBuildInfo& info) {
    Y_ENSURE(info.BuildKind != TIndexBuildInfo::EBuildKind::BuildKindUnspecified);
    auto persistedBuildIndex = db.Table<Schema::IndexBuild>().Key(info.Id);
    persistedBuildIndex.Update(
        NIceDb::TUpdate<Schema::IndexBuild::Uid>(info.Uid),
        NIceDb::TUpdate<Schema::IndexBuild::DomainOwnerId>(info.DomainPathId.OwnerId),
        NIceDb::TUpdate<Schema::IndexBuild::DomainLocalId>(info.DomainPathId.LocalPathId),
        NIceDb::TUpdate<Schema::IndexBuild::TableOwnerId>(info.TablePathId.OwnerId),
        NIceDb::TUpdate<Schema::IndexBuild::TableLocalId>(info.TablePathId.LocalPathId),
        NIceDb::TUpdate<Schema::IndexBuild::IndexName>(info.IndexName),
        NIceDb::TUpdate<Schema::IndexBuild::IndexType>(info.IndexType),
        NIceDb::TUpdate<Schema::IndexBuild::MaxBatchRows>(info.ScanSettings.GetMaxBatchRows()),
        NIceDb::TUpdate<Schema::IndexBuild::MaxBatchBytes>(info.ScanSettings.GetMaxBatchBytes()),
        NIceDb::TUpdate<Schema::IndexBuild::MaxShards>(info.MaxInProgressShards),
        NIceDb::TUpdate<Schema::IndexBuild::MaxRetries>(info.ScanSettings.GetMaxBatchRetries()),
        NIceDb::TUpdate<Schema::IndexBuild::BuildKind>(ui32(info.BuildKind)),
        NIceDb::TUpdate<Schema::IndexBuild::StartTime>(info.StartTime.Seconds())
    );
    if (info.UserSID) {
        persistedBuildIndex.Update(
            NIceDb::TUpdate<Schema::IndexBuild::UserSID>(*info.UserSID)
        );
    }
    // Persist details of the index build operation: ImplTableDescriptions and SpecializedIndexDescription.
    // We have chosen TIndexCreationConfig's string representation as the serialization format.
    if (bool hasSpecializedDescription = !std::holds_alternative<std::monostate>(info.SpecializedIndexDescription);
        info.ImplTableDescriptions || hasSpecializedDescription
    ) {
        NKikimrSchemeOp::TIndexCreationConfig serializableRepresentation;

        for (const auto& description : info.ImplTableDescriptions) {
            *serializableRepresentation.AddIndexImplTableDescriptions() = description;
        }

        std::visit([&]<typename T>(const T& specializedDescription) {
            if constexpr (std::is_same_v<T, NKikimrSchemeOp::TVectorIndexKmeansTreeDescription>) {
                *serializableRepresentation.MutableVectorIndexKmeansTreeDescription() = specializedDescription;
            }
        }, info.SpecializedIndexDescription);

        persistedBuildIndex.Update(
            NIceDb::TUpdate<Schema::IndexBuild::CreationConfig>(serializableRepresentation.SerializeAsString())
        );
    }

    ui32 columnNo = 0;
    for (ui32 i = 0; i < info.IndexColumns.size(); ++i, ++columnNo) {
        db.Table<Schema::IndexBuildColumns>().Key(info.Id, columnNo).Update(
            NIceDb::TUpdate<Schema::IndexBuildColumns::ColumnName>(info.IndexColumns[i]),
            NIceDb::TUpdate<Schema::IndexBuildColumns::ColumnKind>(EIndexColumnKind::KeyColumn)
        );
    }

    for (ui32 i = 0; i < info.DataColumns.size(); ++i, ++columnNo) {
        db.Table<Schema::IndexBuildColumns>().Key(info.Id, columnNo).Update(
            NIceDb::TUpdate<Schema::IndexBuildColumns::ColumnName>(info.DataColumns[i]),
            NIceDb::TUpdate<Schema::IndexBuildColumns::ColumnKind>(EIndexColumnKind::DataColumn)
        );
    }

    for(ui32 i = 0; i < info.BuildColumns.size(); i++) {
        db.Table<Schema::BuildColumnOperationSettings>().Key(info.Id, i).Update(
            NIceDb::TUpdate<Schema::BuildColumnOperationSettings::ColumnName>(info.BuildColumns[i].ColumnName),
            NIceDb::TUpdate<Schema::BuildColumnOperationSettings::DefaultFromLiteral>(
                TString(info.BuildColumns[i].DefaultFromLiteral.SerializeAsString())),
            NIceDb::TUpdate<Schema::BuildColumnOperationSettings::NotNull>(info.BuildColumns[i].NotNull),
            NIceDb::TUpdate<Schema::BuildColumnOperationSettings::FamilyName>(info.BuildColumns[i].FamilyName)
        );
    }
}

void TSchemeShard::PersistBuildIndexState(NIceDb::TNiceDb& db, const TIndexBuildInfo& indexInfo) {
    db.Table<Schema::IndexBuild>().Key(indexInfo.Id).Update(
        NIceDb::TUpdate<Schema::IndexBuild::State>(ui32(indexInfo.State)),
        NIceDb::TUpdate<Schema::IndexBuild::SubState>(ui32(indexInfo.SubState)),
        NIceDb::TUpdate<Schema::IndexBuild::Issue>(indexInfo.GetIssue()),
        NIceDb::TUpdate<Schema::IndexBuild::StartTime>(indexInfo.StartTime.Seconds()),
        NIceDb::TUpdate<Schema::IndexBuild::EndTime>(indexInfo.EndTime.Seconds())
    );
}

void TSchemeShard::PersistBuildIndexCancelRequest(NIceDb::TNiceDb& db, const TIndexBuildInfo& indexInfo) {
    db.Table<Schema::IndexBuild>().Key(indexInfo.Id).Update(
        NIceDb::TUpdate<Schema::IndexBuild::CancelRequest>(indexInfo.CancelRequested));
}

void TSchemeShard::PersistBuildIndexAddIssue(NIceDb::TNiceDb& db, TIndexBuildInfo& indexInfo, const TString& issue) {
    if (indexInfo.AddIssue(issue)) {
        db.Table<Schema::IndexBuild>().Key(indexInfo.Id).Update(
            NIceDb::TUpdate<Schema::IndexBuild::Issue>(indexInfo.GetIssue()));
    }
}

void TSchemeShard::PersistBuildIndexAlterMainTableTxId(NIceDb::TNiceDb& db, const TIndexBuildInfo& indexInfo) {
    db.Table<Schema::IndexBuild>().Key(indexInfo.Id).Update(
        NIceDb::TUpdate<Schema::IndexBuild::AlterMainTableTxId>(indexInfo.AlterMainTableTxId));
}

void TSchemeShard::PersistBuildIndexAlterMainTableTxStatus(NIceDb::TNiceDb& db, const TIndexBuildInfo& indexInfo) {
    db.Table<Schema::IndexBuild>().Key(indexInfo.Id).Update(
        NIceDb::TUpdate<Schema::IndexBuild::AlterMainTableTxStatus>(indexInfo.AlterMainTableTxStatus));
}

void TSchemeShard::PersistBuildIndexAlterMainTableTxDone(NIceDb::TNiceDb& db, const TIndexBuildInfo& indexInfo) {
    db.Table<Schema::IndexBuild>().Key(indexInfo.Id).Update(
        NIceDb::TUpdate<Schema::IndexBuild::AlterMainTableTxDone>(indexInfo.AlterMainTableTxDone));
}

void TSchemeShard::PersistBuildIndexInitiateTxId(NIceDb::TNiceDb& db, const TIndexBuildInfo& indexInfo) {
    db.Table<Schema::IndexBuild>().Key(indexInfo.Id).Update(
        NIceDb::TUpdate<Schema::IndexBuild::InitiateTxId>(indexInfo.InitiateTxId));
}

void TSchemeShard::PersistBuildIndexInitiateTxStatus(NIceDb::TNiceDb& db, const TIndexBuildInfo& indexInfo) {
    db.Table<Schema::IndexBuild>().Key(indexInfo.Id).Update(
        NIceDb::TUpdate<Schema::IndexBuild::InitiateTxStatus>(indexInfo.InitiateTxStatus));
}

void TSchemeShard::PersistBuildIndexInitiateTxDone(NIceDb::TNiceDb& db, const TIndexBuildInfo& indexInfo) {
    db.Table<Schema::IndexBuild>().Key(indexInfo.Id).Update(
        NIceDb::TUpdate<Schema::IndexBuild::InitiateTxDone>(indexInfo.InitiateTxDone));
}

void TSchemeShard::PersistBuildIndexApplyTxId(NIceDb::TNiceDb& db, const TIndexBuildInfo& indexInfo) {
    db.Table<Schema::IndexBuild>().Key(indexInfo.Id).Update(
        NIceDb::TUpdate<Schema::IndexBuild::ApplyTxId>(indexInfo.ApplyTxId));
}

void TSchemeShard::PersistBuildIndexApplyTxStatus(NIceDb::TNiceDb& db, const TIndexBuildInfo& indexInfo) {
    db.Table<Schema::IndexBuild>().Key(indexInfo.Id).Update(
        NIceDb::TUpdate<Schema::IndexBuild::ApplyTxStatus>(indexInfo.ApplyTxStatus));
}

void TSchemeShard::PersistBuildIndexApplyTxDone(NIceDb::TNiceDb& db, const TIndexBuildInfo& indexInfo) {
    db.Table<Schema::IndexBuild>().Key(indexInfo.Id).Update(
        NIceDb::TUpdate<Schema::IndexBuild::ApplyTxDone>(indexInfo.ApplyTxDone));
}

void TSchemeShard::PersistBuildIndexLockTxId(NIceDb::TNiceDb& db, const TIndexBuildInfo& indexInfo) {
    db.Table<Schema::IndexBuild>().Key(indexInfo.Id).Update(
        NIceDb::TUpdate<Schema::IndexBuild::LockTxId>(indexInfo.LockTxId));
}

void TSchemeShard::PersistBuildIndexLockTxStatus(NIceDb::TNiceDb& db, const TIndexBuildInfo& indexInfo) {
    db.Table<Schema::IndexBuild>().Key(indexInfo.Id).Update(
        NIceDb::TUpdate<Schema::IndexBuild::LockTxStatus>(indexInfo.LockTxStatus));
}

void TSchemeShard::PersistBuildIndexLockTxDone(NIceDb::TNiceDb& db, const TIndexBuildInfo& indexInfo) {
    db.Table<Schema::IndexBuild>().Key(indexInfo.Id).Update(
        NIceDb::TUpdate<Schema::IndexBuild::LockTxDone>(indexInfo.LockTxDone));
}

void TSchemeShard::PersistBuildIndexUnlockTxDone(NIceDb::TNiceDb& db, const TIndexBuildInfo& indexInfo) {
    db.Table<Schema::IndexBuild>().Key(indexInfo.Id).Update(
        NIceDb::TUpdate<Schema::IndexBuild::UnlockTxDone>(indexInfo.UnlockTxDone));
}

void TSchemeShard::PersistBuildIndexUnlockTxStatus(NIceDb::TNiceDb& db, const TIndexBuildInfo& indexInfo) {
    db.Table<Schema::IndexBuild>().Key(indexInfo.Id).Update(
        NIceDb::TUpdate<Schema::IndexBuild::UnlockTxStatus>(indexInfo.UnlockTxStatus));
}

void TSchemeShard::PersistBuildIndexUnlockTxId(NIceDb::TNiceDb& db, const TIndexBuildInfo& indexInfo) {
    db.Table<Schema::IndexBuild>().Key(indexInfo.Id).Update(
        NIceDb::TUpdate<Schema::IndexBuild::UnlockTxId>(indexInfo.UnlockTxId));
}

void TSchemeShard::PersistBuildIndexDropColumnsTxId(NIceDb::TNiceDb& db, const TIndexBuildInfo& indexInfo) {
    db.Table<Schema::IndexBuild>().Key(indexInfo.Id).Update(
        NIceDb::TUpdate<Schema::IndexBuild::DropColumnsTxId>(indexInfo.DropColumnsTxId));
}

void TSchemeShard::PersistBuildIndexDropColumnsTxStatus(NIceDb::TNiceDb& db, const TIndexBuildInfo& indexInfo) {
    db.Table<Schema::IndexBuild>().Key(indexInfo.Id).Update(
        NIceDb::TUpdate<Schema::IndexBuild::DropColumnsTxStatus>(indexInfo.DropColumnsTxStatus));
}

void TSchemeShard::PersistBuildIndexDropColumnsTxDone(NIceDb::TNiceDb& db, const TIndexBuildInfo& indexInfo) {
    db.Table<Schema::IndexBuild>().Key(indexInfo.Id).Update(
        NIceDb::TUpdate<Schema::IndexBuild::DropColumnsTxDone>(indexInfo.DropColumnsTxDone));
}

void TSchemeShard::PersistBuildIndexProcessed(NIceDb::TNiceDb& db, const TIndexBuildInfo& indexInfo) {
    db.Table<Schema::IndexBuild>().Key(indexInfo.Id).Update(
        NIceDb::TUpdate<Schema::IndexBuild::UploadRowsProcessed>(indexInfo.Processed.GetUploadRows()),
        NIceDb::TUpdate<Schema::IndexBuild::UploadBytesProcessed>(indexInfo.Processed.GetUploadBytes()),
        NIceDb::TUpdate<Schema::IndexBuild::ReadRowsProcessed>(indexInfo.Processed.GetReadRows()),
        NIceDb::TUpdate<Schema::IndexBuild::ReadBytesProcessed>(indexInfo.Processed.GetReadBytes()),
        NIceDb::TUpdate<Schema::IndexBuild::CpuTimeUsProcessed>(indexInfo.Processed.GetCpuTimeUs())
    );
}

void TSchemeShard::PersistBuildIndexBilled(NIceDb::TNiceDb& db, const TIndexBuildInfo& indexInfo) {
    db.Table<Schema::IndexBuild>().Key(indexInfo.Id).Update(
        NIceDb::TUpdate<Schema::IndexBuild::UploadRowsBilled>(indexInfo.Billed.GetUploadRows()),
        NIceDb::TUpdate<Schema::IndexBuild::UploadBytesBilled>(indexInfo.Billed.GetUploadBytes()),
        NIceDb::TUpdate<Schema::IndexBuild::ReadRowsBilled>(indexInfo.Billed.GetReadRows()),
        NIceDb::TUpdate<Schema::IndexBuild::ReadBytesBilled>(indexInfo.Billed.GetReadBytes()),
        NIceDb::TUpdate<Schema::IndexBuild::CpuTimeUsBilled>(indexInfo.Processed.GetCpuTimeUs())
    );
}

void TSchemeShard::PersistBuildIndexShardStatus(NIceDb::TNiceDb& db, TIndexBuildId buildId, const TShardIdx& shardIdx, const TIndexBuildInfo::TShardStatus& shardStatus) {
    db.Table<Schema::IndexBuildShardStatus>().Key(buildId, shardIdx.GetOwnerId(), shardIdx.GetLocalId()).Update(
        NIceDb::TUpdate<Schema::IndexBuildShardStatus::LastKeyAck>(shardStatus.LastKeyAck),
        NIceDb::TUpdate<Schema::IndexBuildShardStatus::Status>(shardStatus.Status),
        NIceDb::TUpdate<Schema::IndexBuildShardStatus::Message>(shardStatus.DebugMessage),
        NIceDb::TUpdate<Schema::IndexBuildShardStatus::UploadStatus>(shardStatus.UploadStatus),
        NIceDb::TUpdate<Schema::IndexBuildShardStatus::UploadRowsProcessed>(shardStatus.Processed.GetUploadRows()),
        NIceDb::TUpdate<Schema::IndexBuildShardStatus::UploadBytesProcessed>(shardStatus.Processed.GetUploadBytes()),
        NIceDb::TUpdate<Schema::IndexBuildShardStatus::ReadRowsProcessed>(shardStatus.Processed.GetReadRows()),
        NIceDb::TUpdate<Schema::IndexBuildShardStatus::ReadBytesProcessed>(shardStatus.Processed.GetReadBytes()),
        NIceDb::TUpdate<Schema::IndexBuildShardStatus::CpuTimeUsProcessed>(shardStatus.Processed.GetCpuTimeUs())
    );
}

void TSchemeShard::PersistBuildIndexShardRange(NIceDb::TNiceDb& db, TIndexBuildId buildId, const TShardIdx& shardIdx, const TIndexBuildInfo::TShardStatus& shardStatus) {
    NKikimrTx::TKeyRange range;
    shardStatus.Range.Serialize(range);
    db.Table<Schema::IndexBuildShardStatus>().Key(buildId, shardIdx.GetOwnerId(), shardIdx.GetLocalId()).Update(
        NIceDb::TUpdate<Schema::IndexBuildShardStatus::Status>(shardStatus.Status),
        NIceDb::TUpdate<Schema::IndexBuildShardStatus::Message>(shardStatus.DebugMessage),
        NIceDb::TUpdate<Schema::IndexBuildShardStatus::Range>(range),
        NIceDb::TUpdate<Schema::IndexBuildShardStatus::ReadRowsProcessed>(shardStatus.Processed.GetReadRows()),
        NIceDb::TUpdate<Schema::IndexBuildShardStatus::ReadBytesProcessed>(shardStatus.Processed.GetReadBytes())
    );
}

void TSchemeShard::PersistBuildIndexShardStatusInitiate(NIceDb::TNiceDb& db, TIndexBuildId buildId, const TShardIdx& shardIdx, const TIndexBuildInfo::TShardStatus& shardStatus) {
    NKikimrTx::TKeyRange range;
    shardStatus.Range.Serialize(range);
    db.Table<Schema::IndexBuildShardStatus>().Key(buildId, shardIdx.GetOwnerId(), shardIdx.GetLocalId()).Update(
        NIceDb::TUpdate<Schema::IndexBuildShardStatus::Range>(range),
        NIceDb::TUpdate<Schema::IndexBuildShardStatus::LastKeyAck>(shardStatus.LastKeyAck),
        NIceDb::TUpdate<Schema::IndexBuildShardStatus::Status>(shardStatus.Status),
        NIceDb::TUpdate<Schema::IndexBuildShardStatus::UploadStatus>(shardStatus.UploadStatus)
    );
}

void TSchemeShard::PersistBuildIndexShardStatusReset(NIceDb::TNiceDb& db, TIndexBuildId buildId, const TShardIdx& shardIdx, TIndexBuildInfo::TShardStatus& shardStatus) {
    shardStatus.Status = NKikimrIndexBuilder::EBuildStatus::INVALID;
    shardStatus.Processed = {};
    db.Table<Schema::IndexBuildShardStatus>().Key(buildId, shardIdx.GetOwnerId(), shardIdx.GetLocalId()).Update(
        NIceDb::TUpdate<Schema::IndexBuildShardStatus::Status>(shardStatus.Status),
        NIceDb::TUpdate<Schema::IndexBuildShardStatus::UploadRowsProcessed>(shardStatus.Processed.GetUploadRows()),
        NIceDb::TUpdate<Schema::IndexBuildShardStatus::UploadBytesProcessed>(shardStatus.Processed.GetUploadBytes()),
        NIceDb::TUpdate<Schema::IndexBuildShardStatus::ReadRowsProcessed>(shardStatus.Processed.GetReadRows()),
        NIceDb::TUpdate<Schema::IndexBuildShardStatus::ReadBytesProcessed>(shardStatus.Processed.GetReadBytes()),
        NIceDb::TUpdate<Schema::IndexBuildShardStatus::CpuTimeUsProcessed>(shardStatus.Processed.GetCpuTimeUs())
    );
}

void TSchemeShard::PersistBuildIndexShardStatusReset(NIceDb::TNiceDb& db, TIndexBuildInfo& info) {
    for (const auto& [shardIdx, _]: info.Shards) {
        db.Table<Schema::IndexBuildShardStatus>().Key(info.Id, shardIdx.GetOwnerId(), shardIdx.GetLocalId()).Delete();
    }
    info.Shards.clear();
}


void TSchemeShard::PersistBuildIndexSampleForget(NIceDb::TNiceDb& db, const TIndexBuildInfo& info) {
    Y_ASSERT(info.IsBuildVectorIndex());
    for (ui32 row = 0; row < info.KMeans.K * 2; ++row) {
        db.Table<Schema::KMeansTreeSample>().Key(info.Id, row).Delete();
    }
}

void TSchemeShard::PersistBuildIndexSampleToClusters(NIceDb::TNiceDb& db, TIndexBuildInfo& info) {
    TVector<TString> clusters;
    for (const auto& [_, row] : info.Sample.Rows) {
        clusters.push_back(TString(TSerializedCellVec::ExtractCell(row, 0).AsBuf()));
    }
    for (ui32 i = info.KMeans.K; i <= 2*info.KMeans.K; i++) {
        db.Table<Schema::KMeansTreeSample>().Key(info.Id, i).Delete();
    }
    for (ui32 i = 0; i < info.Sample.Rows.size(); i++) {
        db.Table<Schema::KMeansTreeClusters>().Key(info.Id, i).Update(
            NIceDb::TUpdate<Schema::KMeansTreeClusters::OldSize>(0),
            NIceDb::TUpdate<Schema::KMeansTreeClusters::Size>(0),
            NIceDb::TUpdate<Schema::KMeansTreeClusters::Data>(clusters[i])
        );
    }
    for (ui32 i = info.Sample.Rows.size(); i < info.KMeans.K; i++) {
        db.Table<Schema::KMeansTreeClusters>().Key(info.Id, i).Delete();
    }
    bool ok = info.Clusters->SetClusters(std::move(clusters));
    Y_ENSURE(ok);
}

void TSchemeShard::PersistBuildIndexClustersToSample(NIceDb::TNiceDb& db, TIndexBuildInfo& info) {
    info.Sample.Clear();
    const auto & clusters = info.Clusters->GetClusters();
    const auto & sizes = info.Clusters->GetClusterSizes();
    for (ui32 i = 0; i < clusters.size(); i++) {
        auto sampleRow = TSerializedCellVec::Serialize(TVector<TCell>{TCell(clusters[i])});
        info.Sample.Add(i+1, sampleRow);
        db.Table<Schema::KMeansTreeSample>().Key(info.Id, i).Update(
            NIceDb::TUpdate<Schema::KMeansTreeSample::Probability>(i+1),
            NIceDb::TUpdate<Schema::KMeansTreeSample::Data>(sampleRow)
        );
        db.Table<Schema::KMeansTreeClusters>().Key(info.Id, i).Update(
            NIceDb::TUpdate<Schema::KMeansTreeClusters::OldSize>(sizes[i]),
            NIceDb::TUpdate<Schema::KMeansTreeClusters::Size>(0),
            NIceDb::TUpdate<Schema::KMeansTreeClusters::Data>(clusters[i])
        );
    }
    for (ui32 i = clusters.size(); i < 2*info.KMeans.K; ++i) {
        db.Table<Schema::KMeansTreeSample>().Key(info.Id, i).Delete();
    }
    for (ui32 i = clusters.size(); i < info.KMeans.K; i++) {
        db.Table<Schema::KMeansTreeClusters>().Key(info.Id, i).Delete();
    }
}

void TSchemeShard::PersistBuildIndexClustersUpdate(NIceDb::TNiceDb& db, const TIndexBuildInfo& info) {
    auto& newClusters = info.Clusters->GetClusters();
    auto& newSizes = info.Clusters->GetNextClusterSizes();
    for (ui32 i = 0; i < newClusters.size(); i++) {
        if (newSizes[i] > 0) {
            db.Table<Schema::KMeansTreeClusters>().Key(info.Id, i).Update(
                NIceDb::TUpdate<Schema::KMeansTreeClusters::Size>(newSizes[i]),
                NIceDb::TUpdate<Schema::KMeansTreeClusters::Data>(newClusters[i])
            );
        }
    }
}

void TSchemeShard::PersistBuildIndexClustersForget(NIceDb::TNiceDb& db, const TIndexBuildInfo& info) {
    Y_ASSERT(info.IsBuildVectorIndex());
    for (ui32 row = 0; row < info.KMeans.K; ++row) {
        db.Table<Schema::KMeansTreeClusters>().Key(info.Id, row).Delete();
    }
}

void TSchemeShard::PersistBuildIndexForget(NIceDb::TNiceDb& db, const TIndexBuildInfo& info) {
    db.Table<Schema::IndexBuild>().Key(info.Id).Delete();

    ui32 columnNo = 0;
    for (ui32 i = 0; i < info.IndexColumns.size(); ++i, ++columnNo) {
        db.Table<Schema::IndexBuildColumns>().Key(info.Id, columnNo).Delete();
    }

    for (ui32 i = 0; i < info.DataColumns.size(); ++i, ++columnNo) {
        db.Table<Schema::IndexBuildColumns>().Key(info.Id, columnNo).Delete();
    }

    for (const auto& item: info.Shards) {
        auto shardIdx = item.first;
        db.Table<Schema::IndexBuildShardStatus>().Key(info.Id, shardIdx.GetOwnerId(), shardIdx.GetLocalId()).Delete();
    }

    for(ui32 idx = 0; idx < info.BuildColumns.size(); ++idx) {
        db.Table<Schema::BuildColumnOperationSettings>().Key(info.Id, idx).Delete();
    }

    if (info.IsBuildVectorIndex()) {
        db.Table<Schema::KMeansTreeProgress>().Key(info.Id).Delete();
        PersistBuildIndexSampleForget(db, info);
        PersistBuildIndexClustersForget(db, info);
    }
}

void TSchemeShard::Resume(const TDeque<TIndexBuildId>& indexIds, const TActorContext& ctx) {
    for (const auto& id : indexIds) {
        const auto* buildInfoPtr = IndexBuilds.FindPtr(id);
        if (!buildInfoPtr || buildInfoPtr->Get()->IsBroken) {
            continue;
        }

        Execute(CreateTxProgress(id), ctx);
    }
}

void TSchemeShard::SetupRouting(const TDeque<TIndexBuildId>& indexIds, const TActorContext &) {
    for (const auto& id : indexIds) {
        const auto* buildInfoPtr = IndexBuilds.FindPtr(id);
        if (!buildInfoPtr) {
            continue;
        }
        const auto& buildInfo = *buildInfoPtr->Get();

        auto handle = [&] (auto txId) {
            if (txId) {
                auto [it, emplaced] = TxIdToIndexBuilds.try_emplace(txId, buildInfo.Id);
                Y_ENSURE(it->second == buildInfo.Id);
            }
        };

        // TODO(mbkkt) order here is unexpected, is it intentional or accidental?
        handle(buildInfo.LockTxId);
        handle(buildInfo.AlterMainTableTxId);
        handle(buildInfo.InitiateTxId);
        handle(buildInfo.ApplyTxId);
        handle(buildInfo.UnlockTxId);
        handle(buildInfo.DropColumnsTxId);
    }
}

}
}

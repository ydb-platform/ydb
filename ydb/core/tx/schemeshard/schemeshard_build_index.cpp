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

void TSchemeShard::Handle(TEvPrivate::TEvIndexBuildingMakeABill::TPtr& ev, const TActorContext& ctx) {
    Execute(CreateTxBilling(ev), ctx);
}

void TSchemeShard::PersistCreateBuildIndex(NIceDb::TNiceDb& db, const TIndexBuildInfo::TPtr info) {
    Y_ABORT_UNLESS(info->BuildKind != TIndexBuildInfo::EBuildKind::BuildKindUnspecified);
    db.Table<Schema::IndexBuild>().Key(info->Id).Update(
        NIceDb::TUpdate<Schema::IndexBuild::Uid>(info->Uid),
        NIceDb::TUpdate<Schema::IndexBuild::DomainOwnerId>(info->DomainPathId.OwnerId),
        NIceDb::TUpdate<Schema::IndexBuild::DomainLocalId>(info->DomainPathId.LocalPathId),
        NIceDb::TUpdate<Schema::IndexBuild::TableOwnerId>(info->TablePathId.OwnerId),
        NIceDb::TUpdate<Schema::IndexBuild::TableLocalId>(info->TablePathId.LocalPathId),
        NIceDb::TUpdate<Schema::IndexBuild::IndexName>(info->IndexName),
        NIceDb::TUpdate<Schema::IndexBuild::IndexType>(info->IndexType),
        NIceDb::TUpdate<Schema::IndexBuild::MaxBatchRows>(info->Limits.MaxBatchRows),
        NIceDb::TUpdate<Schema::IndexBuild::MaxBatchBytes>(info->Limits.MaxBatchBytes),
        NIceDb::TUpdate<Schema::IndexBuild::MaxShards>(info->Limits.MaxShards),
        NIceDb::TUpdate<Schema::IndexBuild::MaxRetries>(info->Limits.MaxRetries),
        NIceDb::TUpdate<Schema::IndexBuild::BuildKind>(ui32(info->BuildKind))

        // TODO save info->ImplTableDescriptions
    );

    ui32 columnNo = 0;
    for (ui32 i = 0; i < info->IndexColumns.size(); ++i, ++columnNo) {
        db.Table<Schema::IndexBuildColumns>().Key(info->Id, columnNo).Update(
            NIceDb::TUpdate<Schema::IndexBuildColumns::ColumnName>(info->IndexColumns[i]),
            NIceDb::TUpdate<Schema::IndexBuildColumns::ColumnKind>(EIndexColumnKind::KeyColumn)
        );
    }

    for (ui32 i = 0; i < info->DataColumns.size(); ++i, ++columnNo) {
        db.Table<Schema::IndexBuildColumns>().Key(info->Id, columnNo).Update(
            NIceDb::TUpdate<Schema::IndexBuildColumns::ColumnName>(info->DataColumns[i]),
            NIceDb::TUpdate<Schema::IndexBuildColumns::ColumnKind>(EIndexColumnKind::DataColumn)
        );
    }

    for(ui32 i = 0; i < info->BuildColumns.size(); i++) {
        db.Table<Schema::BuildColumnOperationSettings>().Key(info->Id, i).Update(
            NIceDb::TUpdate<Schema::BuildColumnOperationSettings::ColumnName>(info->BuildColumns[i].ColumnName),
            NIceDb::TUpdate<Schema::BuildColumnOperationSettings::DefaultFromLiteral>(
                TString(info->BuildColumns[i].DefaultFromLiteral.SerializeAsString())),
            NIceDb::TUpdate<Schema::BuildColumnOperationSettings::NotNull>(info->BuildColumns[i].NotNull),
            NIceDb::TUpdate<Schema::BuildColumnOperationSettings::FamilyName>(info->BuildColumns[i].FamilyName)
        );
    }
}

void TSchemeShard::PersistBuildIndexState(NIceDb::TNiceDb& db, const TIndexBuildInfo::TPtr indexInfo) {
    db.Table<Schema::IndexBuild>().Key(indexInfo->Id).Update(
        NIceDb::TUpdate<Schema::IndexBuild::State>(ui32(indexInfo->State)));
}

void TSchemeShard::PersistBuildIndexCancelRequest(NIceDb::TNiceDb& db, const TIndexBuildInfo::TPtr indexInfo) {
    db.Table<Schema::IndexBuild>().Key(indexInfo->Id).Update(
        NIceDb::TUpdate<Schema::IndexBuild::CancelRequest>(indexInfo->CancelRequested));
}

void TSchemeShard::PersistBuildIndexIssue(NIceDb::TNiceDb& db, const TIndexBuildInfo::TPtr indexInfo) {
    db.Table<Schema::IndexBuild>().Key(indexInfo->Id).Update(
        NIceDb::TUpdate<Schema::IndexBuild::Issue>(indexInfo->Issue));
}

void TSchemeShard::PersistBuildIndexAlterMainTableTxId(NIceDb::TNiceDb& db, const TIndexBuildInfo::TPtr indexInfo) {
    db.Table<Schema::IndexBuild>().Key(indexInfo->Id).Update(
        NIceDb::TUpdate<Schema::IndexBuild::AlterMainTableTxId>(indexInfo->AlterMainTableTxId));
}

void TSchemeShard::PersistBuildIndexAlterMainTableTxStatus(NIceDb::TNiceDb& db, const TIndexBuildInfo::TPtr indexInfo) {
    db.Table<Schema::IndexBuild>().Key(indexInfo->Id).Update(
        NIceDb::TUpdate<Schema::IndexBuild::AlterMainTableTxStatus>(indexInfo->AlterMainTableTxStatus));
}

void TSchemeShard::PersistBuildIndexAlterMainTableTxDone(NIceDb::TNiceDb& db, const TIndexBuildInfo::TPtr indexInfo) {
    db.Table<Schema::IndexBuild>().Key(indexInfo->Id).Update(
        NIceDb::TUpdate<Schema::IndexBuild::AlterMainTableTxDone>(indexInfo->AlterMainTableTxDone));
}

void TSchemeShard::PersistBuildIndexInitiateTxId(NIceDb::TNiceDb& db, const TIndexBuildInfo::TPtr indexInfo) {
    db.Table<Schema::IndexBuild>().Key(indexInfo->Id).Update(
        NIceDb::TUpdate<Schema::IndexBuild::InitiateTxId>(indexInfo->InitiateTxId));
}

void TSchemeShard::PersistBuildIndexInitiateTxStatus(NIceDb::TNiceDb& db, const TIndexBuildInfo::TPtr indexInfo) {
    db.Table<Schema::IndexBuild>().Key(indexInfo->Id).Update(
        NIceDb::TUpdate<Schema::IndexBuild::InitiateTxStatus>(indexInfo->InitiateTxStatus));
}

void TSchemeShard::PersistBuildIndexInitiateTxDone(NIceDb::TNiceDb& db, const TIndexBuildInfo::TPtr indexInfo) {
    db.Table<Schema::IndexBuild>().Key(indexInfo->Id).Update(
        NIceDb::TUpdate<Schema::IndexBuild::InitiateTxDone>(indexInfo->InitiateTxDone));
}

void TSchemeShard::PersistBuildIndexApplyTxId(NIceDb::TNiceDb& db, const TIndexBuildInfo::TPtr indexInfo) {
    db.Table<Schema::IndexBuild>().Key(indexInfo->Id).Update(
        NIceDb::TUpdate<Schema::IndexBuild::ApplyTxId>(indexInfo->ApplyTxId));
}

void TSchemeShard::PersistBuildIndexApplyTxStatus(NIceDb::TNiceDb& db, const TIndexBuildInfo::TPtr indexInfo) {
    db.Table<Schema::IndexBuild>().Key(indexInfo->Id).Update(
        NIceDb::TUpdate<Schema::IndexBuild::ApplyTxStatus>(indexInfo->ApplyTxStatus));
}

void TSchemeShard::PersistBuildIndexApplyTxDone(NIceDb::TNiceDb& db, const TIndexBuildInfo::TPtr indexInfo) {
    db.Table<Schema::IndexBuild>().Key(indexInfo->Id).Update(
        NIceDb::TUpdate<Schema::IndexBuild::ApplyTxDone>(indexInfo->ApplyTxDone));
}

void TSchemeShard::PersistBuildIndexLockTxId(NIceDb::TNiceDb& db, const TIndexBuildInfo::TPtr indexInfo) {
    db.Table<Schema::IndexBuild>().Key(indexInfo->Id).Update(
        NIceDb::TUpdate<Schema::IndexBuild::LockTxId>(indexInfo->LockTxId));
}

void TSchemeShard::PersistBuildIndexLockTxStatus(NIceDb::TNiceDb& db, const TIndexBuildInfo::TPtr indexInfo) {
    db.Table<Schema::IndexBuild>().Key(indexInfo->Id).Update(
        NIceDb::TUpdate<Schema::IndexBuild::LockTxStatus>(indexInfo->LockTxStatus));
}

void TSchemeShard::PersistBuildIndexLockTxDone(NIceDb::TNiceDb& db, const TIndexBuildInfo::TPtr indexInfo) {
    db.Table<Schema::IndexBuild>().Key(indexInfo->Id).Update(
        NIceDb::TUpdate<Schema::IndexBuild::LockTxDone>(indexInfo->LockTxDone));
}

void TSchemeShard::PersistBuildIndexUnlockTxDone(NIceDb::TNiceDb& db, const TIndexBuildInfo::TPtr indexInfo) {
    db.Table<Schema::IndexBuild>().Key(indexInfo->Id).Update(
        NIceDb::TUpdate<Schema::IndexBuild::UnlockTxDone>(indexInfo->UnlockTxDone));
}

void TSchemeShard::PersistBuildIndexUnlockTxStatus(NIceDb::TNiceDb& db, const TIndexBuildInfo::TPtr indexInfo) {
    db.Table<Schema::IndexBuild>().Key(indexInfo->Id).Update(
        NIceDb::TUpdate<Schema::IndexBuild::UnlockTxStatus>(indexInfo->UnlockTxStatus));
}

void TSchemeShard::PersistBuildIndexUnlockTxId(NIceDb::TNiceDb& db, const TIndexBuildInfo::TPtr indexInfo) {
    db.Table<Schema::IndexBuild>().Key(indexInfo->Id).Update(
        NIceDb::TUpdate<Schema::IndexBuild::UnlockTxId>(indexInfo->UnlockTxId));
}

void TSchemeShard::PersistBuildIndexBilling(NIceDb::TNiceDb& db, const TIndexBuildInfo::TPtr indexInfo) {
    db.Table<Schema::IndexBuild>().Key(indexInfo->Id).Update(
        NIceDb::TUpdate<Schema::IndexBuild::RowsBilled>(indexInfo->Billed.GetRows()),
        NIceDb::TUpdate<Schema::IndexBuild::BytesBilled>(indexInfo->Billed.GetBytes())
        );
}

void TSchemeShard::PersistBuildIndexUploadProgress(NIceDb::TNiceDb& db, const TIndexBuildInfo::TPtr indexInfo, const TShardIdx& shardIdx) {
    const TIndexBuildInfo::TShardStatus& shardStatus = indexInfo->Shards.at(shardIdx);
    db.Table<Schema::IndexBuildShardStatus>().Key(indexInfo->Id, shardIdx.GetOwnerId(), shardIdx.GetLocalId()).Update(
        NIceDb::TUpdate<Schema::IndexBuildShardStatus::LastKeyAck>(shardStatus.LastKeyAck),
        NIceDb::TUpdate<Schema::IndexBuildShardStatus::Status>(shardStatus.Status),
        NIceDb::TUpdate<Schema::IndexBuildShardStatus::Message>(shardStatus.DebugMessage),
        NIceDb::TUpdate<Schema::IndexBuildShardStatus::UploadStatus>(shardStatus.UploadStatus),
        NIceDb::TUpdate<Schema::IndexBuildShardStatus::RowsProcessed>(shardStatus.Processed.GetRows()),
        NIceDb::TUpdate<Schema::IndexBuildShardStatus::BytesProcessed>(shardStatus.Processed.GetBytes())
        );
}

void TSchemeShard::PersistBuildIndexUploadInitiate(NIceDb::TNiceDb& db, const TIndexBuildInfo::TPtr indexInfo, const TShardIdx& shardIdx) {
    const TIndexBuildInfo::TShardStatus& shardStatus = indexInfo->Shards.at(shardIdx);
    NKikimrTx::TKeyRange range;
    shardStatus.Range.Serialize(range);
    db.Table<Schema::IndexBuildShardStatus>().Key(indexInfo->Id, shardIdx.GetOwnerId(), shardIdx.GetLocalId()).Update(
        NIceDb::TUpdate<Schema::IndexBuildShardStatus::Range>(range),
        NIceDb::TUpdate<Schema::IndexBuildShardStatus::LastKeyAck>(shardStatus.LastKeyAck),
        NIceDb::TUpdate<Schema::IndexBuildShardStatus::Status>(shardStatus.Status),
        NIceDb::TUpdate<Schema::IndexBuildShardStatus::UploadStatus>(shardStatus.UploadStatus));
}

void TSchemeShard::PersistBuildIndexForget(NIceDb::TNiceDb& db, const TIndexBuildInfo::TPtr info) {
    db.Table<Schema::IndexBuild>().Key(info->Id).Delete();

    ui32 columnNo = 0;
    for (ui32 i = 0; i < info->IndexColumns.size(); ++i, ++columnNo) {
        db.Table<Schema::IndexBuildColumns>().Key(info->Id, columnNo).Delete();
    }

    for (ui32 i = 0; i < info->DataColumns.size(); ++i, ++columnNo) {
        db.Table<Schema::IndexBuildColumns>().Key(info->Id, columnNo).Delete();
    }

    for (const auto& item: info->Shards) {
        auto shardIdx = item.first;
        db.Table<Schema::IndexBuildShardStatus>().Key(info->Id, shardIdx.GetOwnerId(), shardIdx.GetLocalId()).Delete();
    }

    for(ui32 idx = 0; idx < info->BuildColumns.size(); ++idx) {
        db.Table<Schema::BuildColumnOperationSettings>().Key(info->Id, idx).Delete();
    }
}

void TSchemeShard::Resume(const TDeque<TIndexBuildId>& indexIds, const TActorContext& ctx) {
    for (const auto& id : indexIds) {
        if (IndexBuilds.contains(id)) {
            Execute(CreateTxProgress(id), ctx);
        }
    }
}

void TSchemeShard::SetupRouting(const TDeque<TIndexBuildId>& indexIds, const TActorContext &) {
    for (const auto& id : indexIds) {
        auto* buildInfoPtr = IndexBuilds.FindPtr(id);
        if (!buildInfoPtr) {
            continue;
        }
        Y_ASSERT(*buildInfoPtr);
        auto& buildInfo = **buildInfoPtr;

        auto handle = [&] (auto txId) {
            if (txId) {
                auto [it, emplaced] = TxIdToIndexBuilds.try_emplace(txId, buildInfo.Id);
                Y_ABORT_UNLESS(it->second == buildInfo.Id);
            }
        };

        handle(buildInfo.AlterMainTableTxId);
        handle(buildInfo.LockTxId);
        handle(buildInfo.InitiateTxId);
        handle(buildInfo.ApplyTxId);
        handle(buildInfo.UnlockTxId);
    }
}

}
}

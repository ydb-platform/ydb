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

void TSchemeShard::PersistCreateBuildIndex(NIceDb::TNiceDb& db, const TIndexBuildInfo::TPtr indexInfo) {
    db.Table<Schema::IndexBuild>().Key(indexInfo->Id).Update(
        NIceDb::TUpdate<Schema::IndexBuild::Uid>(indexInfo->Uid),
        NIceDb::TUpdate<Schema::IndexBuild::DomainOwnerId>(indexInfo->DomainPathId.OwnerId),
        NIceDb::TUpdate<Schema::IndexBuild::DomainLocalId>(indexInfo->DomainPathId.LocalPathId),
        NIceDb::TUpdate<Schema::IndexBuild::TableOwnerId>(indexInfo->TablePathId.OwnerId),
        NIceDb::TUpdate<Schema::IndexBuild::TableLocalId>(indexInfo->TablePathId.LocalPathId),
        NIceDb::TUpdate<Schema::IndexBuild::IndexName>(indexInfo->IndexName),
        NIceDb::TUpdate<Schema::IndexBuild::IndexType>(indexInfo->IndexType),
        NIceDb::TUpdate<Schema::IndexBuild::MaxBatchRows>(indexInfo->Limits.MaxBatchRows),
        NIceDb::TUpdate<Schema::IndexBuild::MaxBatchBytes>(indexInfo->Limits.MaxBatchBytes),
        NIceDb::TUpdate<Schema::IndexBuild::MaxShards>(indexInfo->Limits.MaxShards),
        NIceDb::TUpdate<Schema::IndexBuild::MaxRetries>(indexInfo->Limits.MaxRetries)
    );

    ui32 columnNo = 0;
    for (ui32 i = 0; i < indexInfo->IndexColumns.size(); ++i, ++columnNo) {
        db.Table<Schema::IndexBuildColumns>().Key(indexInfo->Id, columnNo).Update(
            NIceDb::TUpdate<Schema::IndexBuildColumns::ColumnName>(indexInfo->IndexColumns[i]),
            NIceDb::TUpdate<Schema::IndexBuildColumns::ColumnKind>(EIndexColumnKind::KeyColumn)
        );
    }

    for (ui32 i = 0; i < indexInfo->DataColumns.size(); ++i, ++columnNo) {
        db.Table<Schema::IndexBuildColumns>().Key(indexInfo->Id, columnNo).Update(
            NIceDb::TUpdate<Schema::IndexBuildColumns::ColumnName>(indexInfo->DataColumns[i]),
            NIceDb::TUpdate<Schema::IndexBuildColumns::ColumnKind>(EIndexColumnKind::DataColumn)
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

void TSchemeShard::PersistBuildIndexForget(NIceDb::TNiceDb& db, const TIndexBuildInfo::TPtr indexInfo) {
    db.Table<Schema::IndexBuild>().Key(indexInfo->Id).Delete();

    ui32 columnNo = 0;
    for (ui32 i = 0; i < indexInfo->IndexColumns.size(); ++i, ++columnNo) {
        db.Table<Schema::IndexBuildColumns>().Key(indexInfo->Id, columnNo).Delete();
    }

    for (ui32 i = 0; i < indexInfo->DataColumns.size(); ++i, ++columnNo) {
        db.Table<Schema::IndexBuildColumns>().Key(indexInfo->Id, columnNo).Delete();
    }

    for (const auto& item: indexInfo->Shards) {
        auto shardIdx = item.first;
        db.Table<Schema::IndexBuildShardStatus>().Key(indexInfo->Id, shardIdx.GetOwnerId(), shardIdx.GetLocalId()).Delete();
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
        if (!IndexBuilds.contains(id)) {
            continue;
        }

        auto buildInfo = IndexBuilds.at(id);

        if (buildInfo->LockTxId) {
            Y_VERIFY(!TxIdToIndexBuilds.contains(buildInfo->LockTxId)
                     || TxIdToIndexBuilds.at(buildInfo->LockTxId) == buildInfo->Id);
            TxIdToIndexBuilds[buildInfo->LockTxId] = buildInfo->Id;
        }

        if (buildInfo->InitiateTxId) {
            Y_VERIFY(!TxIdToIndexBuilds.contains(buildInfo->InitiateTxId)
                     || TxIdToIndexBuilds.at(buildInfo->InitiateTxId) == buildInfo->Id);
            TxIdToIndexBuilds[buildInfo->InitiateTxId] = buildInfo->Id;
        }

        if (buildInfo->ApplyTxId) {
            Y_VERIFY(!TxIdToIndexBuilds.contains(buildInfo->ApplyTxId)
                     || TxIdToIndexBuilds.at(buildInfo->ApplyTxId) == buildInfo->Id);
            TxIdToIndexBuilds[buildInfo->ApplyTxId] = buildInfo->Id;
        }

        if (buildInfo->UnlockTxId) {
            Y_VERIFY(!TxIdToIndexBuilds.contains(buildInfo->UnlockTxId)
                     || TxIdToIndexBuilds.at(buildInfo->UnlockTxId) == buildInfo->Id);
            TxIdToIndexBuilds[buildInfo->UnlockTxId] = buildInfo->Id;
        }
    }
}

}
}

#include "schemeshard_continuous_backup_cleaner.h"
#include "schemeshard_impl.h"

#include <ydb/core/backup/impl/logging.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::CONTINUOUS_BACKUP

namespace NKikimr::NSchemeShard {

using namespace NTabletFlatExecutor;

struct TSchemeShard::TIncrementalBackup::TTxProgress: public NTabletFlatExecutor::TTransactionBase<TSchemeShard>{
public:

    explicit TTxProgress(TSelf* self, ui64 id)
        : TBase(self)
        , Id(id)
    {}

    explicit TTxProgress(TSelf* self, TEvPersQueue::TEvOffloadStatus::TPtr& ev)
        : TBase(self)
        , OffloadStatus(ev)
    {}

    explicit TTxProgress(TSelf* self, TEvPrivate::TEvContinuousBackupCleanerResult::TPtr& ev)
        : TBase(self)
        , CleanerResult(ev)
    {}


    const char* GetLogPrefix() const {
        return "TIncrementalBackup::TTxProgress: ";
    }

    TTxType GetTxType() const override {
        return TXTYPE_PROGRESS_INCREMENTAL_BACKUP;
    }

    void TryStartCleaner(TTransactionContext& txc, TIncrementalBackupInfo& backupInfo, TIncrementalBackupInfo::TItem& item) {
        if (!Self->PathsById.contains(item.PathId)) {
            MarkItemAsDone(txc, backupInfo, item);
            return;
        }

        const auto& streamPath = Self->PathsById.at(item.PathId);
        const auto& tablePath = Self->PathsById.at(streamPath->ParentPathId);
        const auto& workingDir = Self->PathToString(Self->PathsById.at(tablePath->ParentPathId));

        NewCleaners.emplace_back(
            CreateContinuousBackupCleaner(
                Self->TxAllocatorClient,
                Self->SelfId(),
                backupInfo.Id,
                item.PathId,
                workingDir,
                tablePath->Name,
                streamPath->Name
        ));
    }

    // Cleanup for cases when there are no operation created (e.g. in some tests)
    void TryStartOrphanCleaner(TPathId pathId) {
        if (!Self->PathsById.contains(pathId)) {
            return;
        }

        auto streamPathId = Self->PathsById.at(pathId)->ParentPathId;
        if (!Self->PathsById.at(streamPathId)) {
            return;
        }

        const auto& streamPath = Self->PathsById.at(streamPathId);
        if (!Self->PathsById.contains(streamPath->ParentPathId)) {
            return;
        }

        const auto& tablePath = Self->PathsById.at(streamPath->ParentPathId);
        if (!Self->PathsById.contains(tablePath->ParentPathId)) {
            return;
        }

        const auto& workingDir = Self->PathToString(Self->PathsById.at(tablePath->ParentPathId));

        NewCleaners.emplace_back(
            CreateContinuousBackupCleaner(
                Self->TxAllocatorClient,
                Self->SelfId(),
                0,
                streamPathId,
                workingDir,
                tablePath->Name,
                streamPath->Name
        ));
    }

    void MarkItemAsDone(TTransactionContext& txc, TIncrementalBackupInfo& backupInfo, TIncrementalBackupInfo::TItem& item) {
        NIceDb::TNiceDb db(txc.DB);

        item.State = TIncrementalBackupInfo::TItem::EState::Done;
        PersistIncrementalBackupItem(db, backupInfo.Id, item);

        if (backupInfo.IsAllItemsDone()) {
            backupInfo.State = TIncrementalBackupInfo::EState::Done;
            backupInfo.EndTime = TAppData::TimeProvider->Now();
            PersistIncrementalBackupState(db, backupInfo);
        }
    }

    void OnOffloadStatus(TTransactionContext& txc) {
        const auto& record = OffloadStatus->Get()->Record;
        ui64 id = record.GetTxId();
        auto tabletId = TTabletId(record.GetTabletId());
        ui32 partitionId = record.GetPartitionId();

        YDB_LOG_DEBUG("OnOffloadStatus",
            {"logPrefix", GetLogPrefix()},
            {"id", id},
            {"tabletId", tabletId},
            {"partitionId", partitionId});

        auto shardIdx = Self->GetShardIdx(tabletId);
        if (shardIdx == InvalidShardIdx) {
            YDB_LOG_ERROR("Shard index for not found",
                {"logPrefix", GetLogPrefix()},
                {"tabletId", tabletId});
            return;
        }

        Y_ABORT_UNLESS(Self->ShardInfos.contains(shardIdx));
        const auto& shardInfo = Self->ShardInfos.at(shardIdx);
        Y_ABORT_UNLESS(shardInfo.TabletType == ETabletType::PersQueue);

        Y_ABORT_UNLESS(Self->Topics.contains(shardInfo.PathId));
        const auto& topic = Self->Topics.at(shardInfo.PathId);

        if (!topic->Partitions.contains(partitionId)) {
            YDB_LOG_ERROR("Partition with not found in topic with",
                {"logPrefix", GetLogPrefix()},
                {"id", partitionId},
                {"pathId", shardInfo.PathId});
            return;
        }

        topic->OffloadDonePartitions.insert(partitionId);
        if (topic->OffloadDonePartitions.size() != topic->TotalPartitionCount) {
            return;
        }

        if (!Self->IncrementalBackups.contains(id)) {
            YDB_LOG_ERROR("Incremental backup with not found",
                {"logPrefix", GetLogPrefix()},
                {"id", id});
            TryStartOrphanCleaner(shardInfo.PathId);
            return;
        }

        auto& backupInfo = *Self->IncrementalBackups.at(id);
        if (backupInfo.IsFinished()) {
            YDB_LOG_ERROR("Incremental backup with is already finished",
                {"logPrefix", GetLogPrefix()},
                {"id", id});
            return;
        }

        Y_ABORT_UNLESS(Self->PathsById.contains(shardInfo.PathId));
        auto itemPathId = Self->PathsById.at(shardInfo.PathId)->ParentPathId;
        if (!backupInfo.Items.contains(itemPathId)) {
            YDB_LOG_ERROR("Incremental backup item with not found in backup with",
                {"logPrefix", GetLogPrefix()},
                {"pathId", itemPathId},
                {"id", id});
            return;
        }

        auto& item = backupInfo.Items.at(itemPathId);
        if (item.State != TIncrementalBackupInfo::TItem::EState::Transferring) {
            return;
        }

        NIceDb::TNiceDb db(txc.DB);
        item.State = TIncrementalBackupInfo::TItem::EState::Dropping;
        PersistIncrementalBackupItem(db, id, item);
        TryStartCleaner(txc, backupInfo, item);
    }

    void OnCleanerResult(TTransactionContext& txc) {
        auto id = CleanerResult->Get()->BackupId;
        auto itemPathId = CleanerResult->Get()->Item;
        auto success = CleanerResult->Get()->Success;
        auto error = CleanerResult->Get()->Error;

        YDB_LOG_DEBUG("OnCleanerResult",
            {"logPrefix", GetLogPrefix()},
            {"id", id},
            {"itemPathId", itemPathId},
            {"success", success},
            {"error", error});

        Self->RunningContinuousBackupCleaners.erase(CleanerResult->Sender);

        if (!success) {
            YDB_LOG_ERROR("Continuous backup cleaner has",
                {"logPrefix", GetLogPrefix()},
                {"failed", error});
            return;
        }

        if (!Self->IncrementalBackups.contains(id)) {
            YDB_LOG_ERROR("Incremental backup with not found",
                {"logPrefix", GetLogPrefix()},
                {"id", id});
            return;
        }

        auto& backupInfo = *Self->IncrementalBackups.at(id);
        if (backupInfo.IsFinished()) {
            YDB_LOG_ERROR("Incremental backup with is already finished",
                {"logPrefix", GetLogPrefix()},
                {"id", id});
            return;
        }

        if (!backupInfo.Items.contains(itemPathId)) {
            YDB_LOG_ERROR("Incremental backup item with not found in backup with",
                {"logPrefix", GetLogPrefix()},
                {"pathId", itemPathId},
                {"id", id});
            return;
        }

        auto& item = backupInfo.Items.at(itemPathId);
        if (item.State != TIncrementalBackupInfo::TItem::EState::Dropping) {
            YDB_LOG_ERROR("Incremental backup item with in backup with is not in Dropping state, but",
                {"logPrefix", GetLogPrefix()},
                {"pathId", itemPathId},
                {"id", id},
                {"#_item.State", item.State});
            return;
        }

        MarkItemAsDone(txc, backupInfo, item);
    }

    void Resume(TTransactionContext& txc) {
        YDB_LOG_DEBUG("Resume",
            {"logPrefix", GetLogPrefix()},
            {"id", Id});

        if (!Self->IncrementalBackups.contains(Id)) {
            YDB_LOG_ERROR("Incremental backup with not found",
                {"logPrefix", GetLogPrefix()},
                {"id", Id});
            return;
        }

        auto& backupInfo = *Self->IncrementalBackups.at(Id);
        if (backupInfo.IsFinished()) {
            YDB_LOG_ERROR("Incremental backup with is already finished",
                {"logPrefix", GetLogPrefix()},
                {"id", Id});
            return;
        }

        for (auto& [_, item] : backupInfo.Items) {
            if (item.State == TIncrementalBackupInfo::TItem::EState::Dropping) {
                TryStartCleaner(txc, backupInfo, item);
            }
        }
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        if (OffloadStatus) {
            OnOffloadStatus(txc);
        } else if (CleanerResult) {
            OnCleanerResult(txc);
        } else {
            Resume(txc);
        }

        SideEffects.ApplyOnExecute(Self, txc, ctx);
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        for (auto& newCleaner : NewCleaners) {
            Self->RunningContinuousBackupCleaners.insert(ctx.Register(newCleaner.Release()));
        }
        SideEffects.ApplyOnComplete(Self, ctx);
    }

private:
    TSideEffects SideEffects;
    ui64 Id;
    TEvPrivate::TEvContinuousBackupCleanerResult::TPtr CleanerResult;
    TEvPersQueue::TEvOffloadStatus::TPtr OffloadStatus;

    TVector<THolder<IActor>> NewCleaners;
};

ITransaction* TSchemeShard::CreateTxProgress(ui64 id) {
    return new TIncrementalBackup::TTxProgress(this, id);
}

ITransaction* TSchemeShard::CreateTxProgress(TEvPersQueue::TEvOffloadStatus::TPtr& ev) {
    return new TIncrementalBackup::TTxProgress(this, ev);
}

ITransaction* TSchemeShard::CreateTxProgress(TEvPrivate::TEvContinuousBackupCleanerResult::TPtr& ev) {
    return new TIncrementalBackup::TTxProgress(this, ev);
}

} // NKikimr::NSchemeShard

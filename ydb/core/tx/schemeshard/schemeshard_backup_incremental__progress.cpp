#include "schemeshard_continuous_backup_cleaner.h"
#include "schemeshard_impl.h"

#include <ydb/core/backup/impl/logging.h>

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

        LOG_D("OnOffloadStatus id# " << id
            << ", tabletId# " << tabletId
            << ", partitionId# " << partitionId);

        auto shardIdx = Self->GetShardIdx(tabletId);
        if (shardIdx == InvalidShardIdx) {
            LOG_E("Shard index for tabletId# " << tabletId << " not found");
            return;
        }

        Y_ABORT_UNLESS(Self->ShardInfos.contains(shardIdx));
        const auto& shardInfo = Self->ShardInfos.at(shardIdx);
        Y_ABORT_UNLESS(shardInfo.TabletType == ETabletType::PersQueue);

        Y_ABORT_UNLESS(Self->Topics.contains(shardInfo.PathId));
        const auto& topic = Self->Topics.at(shardInfo.PathId);

        if (!topic->Partitions.contains(partitionId)) {
            LOG_E("Partition with id# " << partitionId << " not found in topic with pathId# " << shardInfo.PathId);
            return;
        }

        topic->OffloadDonePartitions.insert(partitionId);
        if (topic->OffloadDonePartitions.size() != topic->TotalPartitionCount) {
            return;
        }

        if (!Self->IncrementalBackups.contains(id)) {
            LOG_E("Incremental backup with id# " << id << " not found");
            TryStartOrphanCleaner(shardInfo.PathId);
            return;
        }

        auto& backupInfo = *Self->IncrementalBackups.at(id);
        if (backupInfo.IsFinished()) {
            LOG_E("Incremental backup with id# " << id << " is already finished");
            return;
        }

        Y_ABORT_UNLESS(Self->PathsById.contains(shardInfo.PathId));
        auto itemPathId = Self->PathsById.at(shardInfo.PathId)->ParentPathId;
        if (!backupInfo.Items.contains(itemPathId)) {
            LOG_E("Incremental backup item with pathId# " << itemPathId << " not found in backup with id# " << id);
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

        LOG_D("OnCleanerResult id# " << id
            << ", itemPathId# " << itemPathId
            << ", success# " << success
            << ", error# " << error);

        Self->RunningContinuousBackupCleaners.erase(CleanerResult->Sender);

        if (!success) {
            LOG_E("Continuous backup cleaner has failed: " << error);
            return;
        }

        if (!Self->IncrementalBackups.contains(id)) {
            LOG_E("Incremental backup with id# " << id << " not found");
            return;
        }

        auto& backupInfo = *Self->IncrementalBackups.at(id);
        if (backupInfo.IsFinished()) {
            LOG_E("Incremental backup with id# " << id << " is already finished");
            return;
        }

        if (!backupInfo.Items.contains(itemPathId)) {
            LOG_E("Incremental backup item with pathId# " << itemPathId << " not found in backup with id# " << id);
            return;
        }

        auto& item = backupInfo.Items.at(itemPathId);
        if (item.State != TIncrementalBackupInfo::TItem::EState::Dropping) {
            LOG_E("Incremental backup item with pathId# " << itemPathId
                << " in backup with id# " << id
                << " is not in Dropping state, but in " << item.State);
            return;
        }

        MarkItemAsDone(txc, backupInfo, item);
    }

    void Resume(TTransactionContext& txc) {
        LOG_D("Resume id# " << Id);

        if (!Self->IncrementalBackups.contains(Id)) {
            LOG_E("Incremental backup with id# " << Id << " not found");
            return;
        }

        auto& backupInfo = *Self->IncrementalBackups.at(Id);
        if (backupInfo.IsFinished()) {
            LOG_E("Incremental backup with id# " << Id << " is already finished");
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

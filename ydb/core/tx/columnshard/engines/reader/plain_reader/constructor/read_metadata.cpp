#include "read_metadata.h"

#include <ydb/core/kqp/compute_actor/kqp_compute_events.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/engines/reader/plain_reader/iterator/iterator.h>
#include <ydb/core/tx/columnshard/engines/reader/plain_reader/iterator/plain_read_data.h>
#include <ydb/core/tx/columnshard/transactions/locks/read_finished.h>
#include <ydb/core/tx/columnshard/transactions/locks/read_start.h>

namespace NKikimr::NOlap::NReader::NPlain {

std::unique_ptr<TScanIteratorBase> TReadMetadata::StartScan(const std::shared_ptr<TReadContext>& readContext) const {
    return std::make_unique<TColumnShardScanIterator>(readContext, readContext->GetReadMetadataPtrVerifiedAs<TReadMetadata>());
}

TConclusionStatus TReadMetadata::Init(
    const NColumnShard::TColumnShard* owner, const TReadDescription& readDescription, const TDataStorageAccessor& dataAccessor) {
    SetPKRangesFilter(readDescription.PKRangesFilter);
    InitShardingInfo(readDescription.PathId);
    TxId = readDescription.TxId;
    LockId = readDescription.LockId;
    if (LockId) {
        owner->GetOperationsManager().RegisterLock(*LockId, owner->Generation());
        LockSharingInfo = owner->GetOperationsManager().GetLockVerified(*LockId).GetSharingInfo();
    }

    /// @note We could have column name changes between schema versions:
    /// Add '1:foo', Drop '1:foo', Add '2:foo'. Drop should hide '1:foo' from reads.
    /// It's expected that we have only one version on 'foo' in blob and could split them by schema {planStep:txId}.
    /// So '1:foo' would be omitted in blob records for the column in new snapshots. And '2:foo' - in old ones.
    /// It's not possible for blobs with several columns. There should be a special logic for them.
    CommittedBlobs =
        dataAccessor.GetCommitedBlobs(readDescription, ResultIndexSchema->GetIndexInfo().GetReplaceKey(), LockId, GetRequestSnapshot());

    if (LockId) {
        for (auto&& i : CommittedBlobs) {
            if (!i.IsCommitted()) {
                if (owner->HasLongTxWrites(i.GetInsertWriteId())) {
                } else {
                    auto op = owner->GetOperationsManager().GetOperationByInsertWriteIdVerified(i.GetInsertWriteId());
                    AddWriteIdToCheck(i.GetInsertWriteId(), op->GetLockId());
                }
            }
        }
    }

    SelectInfo = dataAccessor.Select(readDescription);
    StatsMode = readDescription.StatsMode;
    return TConclusionStatus::Success();
}

std::set<ui32> TReadMetadata::GetEarlyFilterColumnIds() const {
    auto& indexInfo = ResultIndexSchema->GetIndexInfo();
    std::set<ui32> result;
    for (auto&& i : GetProgram().GetEarlyFilterColumns()) {
        auto id = indexInfo.GetColumnIdOptional(i);
        if (id) {
            result.emplace(*id);
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("early_filter_column", i);
        }
    }
    return result;
}

std::set<ui32> TReadMetadata::GetPKColumnIds() const {
    std::set<ui32> result;
    auto& indexInfo = ResultIndexSchema->GetIndexInfo();
    for (auto&& i : indexInfo.GetPrimaryKeyColumns()) {
        Y_ABORT_UNLESS(result.emplace(indexInfo.GetColumnIdVerified(i.first)).second);
    }
    return result;
}

std::shared_ptr<IDataReader> TReadMetadata::BuildReader(const std::shared_ptr<TReadContext>& context) const {
    return std::make_shared<TPlainReadData>(context);
}

NArrow::NMerger::TSortableBatchPosition TReadMetadata::BuildSortedPosition(const NArrow::TReplaceKey& key) const {
    return NArrow::NMerger::TSortableBatchPosition(key.ToBatch(GetReplaceKey()), 0, GetReplaceKey()->field_names(), {}, IsDescSorted());
}

void TReadMetadata::DoOnReadFinished(NColumnShard::TColumnShard& owner) const {
    if (!GetLockId()) {
        return;
    }
    const ui64 lock = *GetLockId();
    if (GetBrokenWithCommitted()) {
        owner.GetOperationsManager().GetLockVerified(lock).SetBroken();
    } else {
        NOlap::NTxInteractions::TTxConflicts conflicts;
        for (auto&& i : GetConflictableLockIds()) {
            conflicts.Add(i, lock);
        }
        auto writer = std::make_shared<NOlap::NTxInteractions::TEvReadFinishedWriter>(PathId, conflicts);
        owner.GetOperationsManager().AddEventForLock(owner, lock, writer);
    }
}

void TReadMetadata::DoOnBeforeStartReading(NColumnShard::TColumnShard& owner) const {
    if (!LockId) {
        return;
    }
    auto evWriter = std::make_shared<NOlap::NTxInteractions::TEvReadStartWriter>(
        PathId, GetResultSchema()->GetIndexInfo().GetPrimaryKey(), GetPKRangesFilterPtr(), GetConflictableLockIds());
    owner.GetOperationsManager().AddEventForLock(owner, *LockId, evWriter);
}

void TReadMetadata::DoOnReplyConstruction(const ui64 tabletId, NKqp::NInternalImplementation::TEvScanData& scanData) const {
    if (LockSharingInfo) {
        NKikimrDataEvents::TLock lockInfo;
        lockInfo.SetLockId(LockSharingInfo->GetLockId());
        lockInfo.SetGeneration(LockSharingInfo->GetGeneration());
        lockInfo.SetDataShard(tabletId);
        lockInfo.SetCounter(LockSharingInfo->GetCounter());
        lockInfo.SetPathId(PathId);
        lockInfo.SetHasWrites(LockSharingInfo->HasWrites());
        if (LockSharingInfo->IsBroken()) {
            scanData.LocksInfo.BrokenLocks.emplace_back(std::move(lockInfo));
        } else {
            scanData.LocksInfo.Locks.emplace_back(std::move(lockInfo));
        }
    }
}

bool TReadMetadata::IsMyUncommitted(const TInsertWriteId writeId) const {
    AFL_VERIFY(LockSharingInfo);
    auto it = ConflictedWriteIds.find(writeId);
    AFL_VERIFY(it != ConflictedWriteIds.end())("write_id", writeId)("write_ids_count", ConflictedWriteIds.size());
    return it->second.GetLockId() == LockSharingInfo->GetLockId();
}

}   // namespace NKikimr::NOlap::NReader::NPlain

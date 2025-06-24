#include "read_metadata.h"

#include <ydb/core/kqp/compute_actor/kqp_compute_events.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/transactions/locks/read_finished.h>
#include <ydb/core/tx/columnshard/transactions/locks/read_start.h>

namespace NKikimr::NOlap::NReader::NCommon {

TConclusionStatus TReadMetadata::Init(
    const NColumnShard::TColumnShard* owner, const TReadDescription& readDescription, const TDataStorageAccessor& dataAccessor) {
    SetPKRangesFilter(readDescription.PKRangesFilter);
    InitShardingInfo(readDescription.PathId.InternalPathId);
    TxId = readDescription.TxId;
    LockId = readDescription.LockId;
    if (LockId) {
        owner->GetOperationsManager().RegisterLock(*LockId, owner->Generation());
        LockSharingInfo = owner->GetOperationsManager().GetLockVerified(*LockId).GetSharingInfo();
    }

    SelectInfo = dataAccessor.Select(readDescription, !!LockId);
    if (LockId) {
        for (auto&& i : SelectInfo->Portions) {
            if (!i->IsCommitted()) {
                AFL_VERIFY(i->GetPortionType() == EPortionType::Written);
                auto* written = static_cast<const TWrittenPortionInfo*>(i.get());
                auto op = owner->GetOperationsManager().GetOperationByInsertWriteIdVerified(written->GetInsertWriteId());
                AddWriteIdToCheck(written->GetInsertWriteId(), op->GetLockId());
            }
        }
    }

    {
        auto customConclusion = DoInitCustom(owner, readDescription, dataAccessor);
        if (customConclusion.IsFail()) {
            return customConclusion;
        }
    }

    StatsMode = readDescription.StatsMode;
    DeduplicationPolicy = readDescription.DeduplicationPolicy;
    return TConclusionStatus::Success();
}

TReadMetadata::TReadMetadata(const std::shared_ptr<TVersionedIndex>& schemaIndex, const TReadDescription& read)
    : TBase(schemaIndex, read.GetSorting(), read.GetProgram(), schemaIndex->GetSchemaVerified(read.GetSnapshot()), read.GetSnapshot(),
          read.GetScanCursorOptional(), read.GetTabletId())
    , PathId(read.PathId)
    , ReadStats(std::make_shared<TReadStats>()) {
}

std::set<ui32> TReadMetadata::GetEarlyFilterColumnIds() const {
    auto& indexInfo = ResultIndexSchema->GetIndexInfo();
    const auto& ids = GetProgram().GetEarlyFilterColumns();
    std::set<ui32> result(ids.begin(), ids.end());
    AFL_VERIFY(result.size() == ids.size());
    for (auto&& i : GetProgram().GetEarlyFilterColumns()) {
        AFL_VERIFY(indexInfo.HasColumnId(i));
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

NArrow::NMerger::TSortableBatchPosition TReadMetadata::BuildSortedPosition(const NArrow::TSimpleRow& key) const {
    return NArrow::NMerger::TSortableBatchPosition(key.ToBatch(), 0, GetReplaceKey()->field_names(), {}, IsDescSorted());
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
        auto writer = std::make_shared<NOlap::NTxInteractions::TEvReadFinishedWriter>(PathId.InternalPathId, conflicts);
        owner.GetOperationsManager().AddEventForLock(owner, lock, writer);
    }
}

void TReadMetadata::DoOnBeforeStartReading(NColumnShard::TColumnShard& owner) const {
    if (!LockId) {
        return;
    }
    auto evWriter = std::make_shared<NOlap::NTxInteractions::TEvReadStartWriter>(
        PathId.InternalPathId, GetResultSchema()->GetIndexInfo().GetPrimaryKey(), GetPKRangesFilterPtr(), GetConflictableLockIds());
    owner.GetOperationsManager().AddEventForLock(owner, *LockId, evWriter);
}

void TReadMetadata::DoOnReplyConstruction(const ui64 tabletId, NKqp::NInternalImplementation::TEvScanData& scanData) const {
    if (LockSharingInfo) {
        NKikimrDataEvents::TLock lockInfo;
        lockInfo.SetLockId(LockSharingInfo->GetLockId());
        lockInfo.SetGeneration(LockSharingInfo->GetGeneration());
        lockInfo.SetDataShard(tabletId);
        lockInfo.SetCounter(LockSharingInfo->GetInternalGenerationCounter());
        PathId.SchemeShardLocalPathId.ToProto(lockInfo);
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

}   // namespace NKikimr::NOlap::NReader::NCommon

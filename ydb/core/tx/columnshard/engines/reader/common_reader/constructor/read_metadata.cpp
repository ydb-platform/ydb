#include "read_metadata.h"

#include <ydb/core/kqp/compute_actor/kqp_compute_events.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/transactions/locks/read_finished.h>
#include <ydb/core/tx/columnshard/transactions/locks/read_start.h>

namespace NKikimr::NOlap::NReader::NCommon {

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

    SelectInfo = dataAccessor.Select(readDescription, !!LockId);
    if (LockId) {
        for (auto&& i : SelectInfo->Portions) {
            if (!readDescription.ReadExpiredRows && GetTtlBound() &&
                GetTtlBound()->GetColumnId() == ResultIndexSchema->GetIndexInfo().GetPKFirstColumnId()) {
                const std::shared_ptr<arrow::Scalar> bound = GetTtlBound()->GetLargestExpiredScalar();
                const std::shared_ptr<arrow::Scalar> maxValue =
                    NArrow::TReplaceKey::ToScalar(i->GetMeta().GetFirstLastPK().GetFirst(ResultIndexSchema->GetIndexInfo().GetPrimaryKey()), 0);
                if (!NArrow::ScalarLess(bound, maxValue)) {
                    continue;
                }
            }
            if (i->HasInsertWriteId() && !i->HasCommitSnapshot()) {
                if (owner->HasLongTxWrites(i->GetInsertWriteIdVerified())) {
                } else {
                    auto op = owner->GetOperationsManager().GetOperationByInsertWriteIdVerified(i->GetInsertWriteIdVerified());
                    AddWriteIdToCheck(i->GetInsertWriteIdVerified(), op->GetLockId());
                }
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
    return TConclusionStatus::Success();
}

TReadMetadata::TReadMetadata(
    const std::shared_ptr<TVersionedIndex>& schemaIndex, const NColumnShard::TTtlVersions& ttlVersions, const TReadDescription& read)
    : TBase(schemaIndex, read.PKRangesFilter->IsReverse() ? TReadMetadataBase::ESorting::DESC : TReadMetadataBase::ESorting::ASC,
          read.GetProgram(), schemaIndex->GetSchemaVerified(read.GetSnapshot()), read.GetSnapshot(), read.GetScanCursor(),
          MakeTtlBound(schemaIndex, ttlVersions, read))
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

std::optional<TReadMetadataBase::TTtlBound> TReadMetadata::MakeTtlBound(
    const std::shared_ptr<TVersionedIndex> schemaIndex, const NColumnShard::TTtlVersions& ttlVersions, const TReadDescription& read) {
    std::optional<TTiering> ttl = ttlVersions.GetTableTtl(read.PathId, read.GetSnapshot());
    if (!ttl) {
        return std::nullopt;
    }
    const auto& lastTier = std::prev(ttl->GetOrderedTiers().end())->Get();
    if (lastTier.GetExternalStorageId()) {
        return std::nullopt;
    }
    const auto& indexInfo = schemaIndex->GetSchemaVerified(read.GetSnapshot())->GetIndexInfo();
    const ui64 columnId = indexInfo.GetColumnIdVerified(lastTier.GetEvictColumnName());
    const auto scalar = TValidator::CheckNotNull(lastTier.GetLargestExpiredScalar(
        read.GetSnapshot().GetPlanInstant(), indexInfo.GetColumnFeaturesVerified(columnId).GetArrowField()->type()->id()));
    return TReadMetadataBase::TTtlBound(columnId, scalar);
}

}   // namespace NKikimr::NOlap::NReader::NCommon

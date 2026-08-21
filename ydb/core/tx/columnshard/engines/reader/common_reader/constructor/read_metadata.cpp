#include "read_metadata.h"

#include <ydb/core/kqp/compute_actor/kqp_compute_events.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/data_locks/locks/list.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/engines/portions/written.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/source.h>
#include <ydb/core/tx/columnshard/engines/reader/plain_reader/iterator/constructors.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/collections/constructors.h>
#include <ydb/core/tx/columnshard/engines/reader/trivial_reader/iterator/collections/constructors.h>
#include <ydb/core/tx/columnshard/transactions/locks/read_finished.h>
#include <ydb/core/tx/columnshard/transactions/locks/read_start.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/struct_log/log_stack.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD_SCAN

namespace NKikimr::NOlap::NReader::NCommon {

namespace {
TString SortingName(const ERequestSorting sorting) {
    switch (sorting) {
        case ERequestSorting::NONE:
            return "NONE";
        case ERequestSorting::ASC:
            return "ASC";
        case ERequestSorting::DESC:
            return "DESC";
    }
    return "UNKNOWN";
}

TString CursorDebugString(const TReadDescription& read) {
    const auto& cursor = read.GetScanCursorVerified();
    if (!cursor || !cursor->IsInitialized()) {
        return "none";
    }
    return cursor->SerializeToProto().ShortDebugString();
}

bool GranuleHasIntervalTree(const NColumnShard::TColumnShard* owner, const TReadDescription& read) {
    if (!owner->HasIndex()) {
        return false;
    }
    const auto pathId = read.TableMetadataAccessor->GetPathId();
    if (!pathId) {
        return false;
    }
    const auto internal = pathId->GetInternalPathIdOptional();
    if (!internal) {
        return false;
    }
    const auto granule = owner->GetIndexAs<TColumnEngineForLogs>().GetGranuleOptional(*internal);
    return granule && granule->HasPortionIntervalTree();
}

// TEMPORARY DEBUG: dump selected constructors in heap pop order. Remove after SCAN_SET_DUMP investigation.
// portions format: "<idx>:<portionId>[C|V][c|u]n<records>cs=<commit|->rs=<remove|->as=<appearance|-> ..."
// C=conflicting V=visible, c=committed u=uncommitted; snapshots as planStep:txId (or -)
// Split like leaked-blobs normalizer (chunkIdx/chunksTotal) so a truncated line does not drop the rest.
void DumpScanSet(const TReadDescription& read, ISourcesConstructor& sources, const NColumnShard::TColumnShard* owner) {
    const TString lockMode = read.LockMode ? NKikimrDataEvents::ELockMode_Name(*read.LockMode) : "none";
    const auto entries = sources.DebugPopOrderEntries();
    static constexpr ui32 ChunkSize = 50;
    const ui32 count = entries.size();
    const ui32 chunksTotal = count ? (count + ChunkSize - 1) / ChunkSize : 1;
    const TString cursor = CursorDebugString(read);
    const TString snapshot = read.GetSnapshot().DebugString();
    const TString table = read.TableMetadataAccessor->GetTablePath();
    for (ui32 chunkIdx = 0; chunkIdx < chunksTotal; ++chunkIdx) {
        const ui32 begin = chunkIdx * ChunkSize;
        const ui32 end = Min<ui32>(begin + ChunkSize, count);
        TStringBuilder portions;
        for (ui32 i = begin; i < end; ++i) {
            if (i > begin) {
                portions << " ";
            }
            portions << entries[i];
        }
        YDB_LOG_WARN("",
            {"event", "SCAN_SET_DUMP"},
            {"tablet", read.GetTabletId()},
            {"txId", read.TxId},
            {"scanId", read.ScanId},
            {"gen", read.ScanGeneration},
            {"table", table},
            {"snapshot", snapshot},
            {"lockTxId", read.LockId.value_or(0)},
            {"lockMode", lockMode},
            {"sorting", SortingName(read.GetSorting())},
            {"fakeSort", read.GetFakeSort()},
            {"dedup", read.DeduplicationPolicy == EDeduplicationPolicy::PREVENT_DUPLICATES},
            {"readConflicting", read.readConflictingPortions},
            {"hasTree", GranuleHasIntervalTree(owner, read)},
            {"rangeEmpty", !read.PKRangesFilter || read.PKRangesFilter->IsEmpty()},
            {"cursor", cursor},
            {"count", count},
            {"chunkIdx", chunkIdx},
            {"chunksTotal", chunksTotal},
            {"chunkSize", end - begin},
            {"portions", portions});
    }
}

// TEMPORARY DEBUG: put scan identity into TlsLogContext so AFL_VERIFY/Y_ABORT carries join keys.
template <typename TFunc>
void WithScanCursorLogContext(const TReadDescription& read, TFunc&& func) {
    const TString lockMode = read.LockMode ? NKikimrDataEvents::ELockMode_Name(*read.LockMode) : "none";
    NActors::TLogContextGuard g(NActors::TLogContextBuilder::Build()("event", "SCAN_CURSOR_INIT")("tablet", read.GetTabletId())(
        "txId", read.TxId)("scanId", read.ScanId)("gen", read.ScanGeneration)("table", read.TableMetadataAccessor->GetTablePath())(
        "snapshot", read.GetSnapshot().DebugString())("lockTxId", read.LockId.value_or(0))("lockMode", lockMode)(
        "readConflicting", read.readConflictingPortions)("sorting", SortingName(read.GetSorting()))("cursor", CursorDebugString(read)));
    func();
}
}   // namespace

TConclusionStatus TReadMetadata::Init(const NColumnShard::TColumnShard* owner, const TReadDescription& read, const EReaderClass readerClass) {
    SetPKRangesFilter(read.PKRangesFilter);
    InitShardingInfo(read.TableMetadataAccessor);
    TxId = read.TxId;
    LockId = read.LockId;
    auto lockNodeId = read.LockNodeId;
    LockMode = read.LockMode;
    if (LockId) {
        owner->GetOperationsManager().RegisterLock(*LockId, owner->Generation());
        if (lockNodeId.has_value()) {
            owner->SubscribeLockIfNotAlready(LockId.value(), lockNodeId.value());
        }
        LockSharingInfo = owner->GetOperationsManager().GetLockVerified(*LockId).GetSharingInfo();
    }
    if (!owner->GetIndexOptional()) {
        switch (readerClass) {
            case EReaderClass::Plain:
                SourcesConstructor = NReader::NPlain::TPortionSources::BuildEmpty();
                break;
            case EReaderClass::Simple:
                SourcesConstructor = NReader::NSimple::TPortionsSources::BuildEmpty();
                break;
            case EReaderClass::Trivial:
                SourcesConstructor = NReader::NTrivial::TPortionsSources::BuildEmpty();
                break;
        }
        WithScanCursorLogContext(read, [&] {
            DumpScanSet(read, *SourcesConstructor, owner);
            SourcesConstructor->InitCursor(nullptr);
        });
        return TConclusionStatus::Success();
    }

    ITableMetadataAccessor::TSelectMetadataContext context(
        owner->GetTablesManager(), owner->GetIndexVerified(), read.Orbit, owner->GetDataLocksManager());
    SourcesConstructor = read.TableMetadataAccessor->SelectMetadata(context, read, readerClass);

    if (!SourcesConstructor) {
        return TConclusionStatus::Fail("cannot build sources constructor for " + read.TableMetadataAccessor->GetTablePath());
    }

    WithScanCursorLogContext(read, [&] {
        DumpScanSet(read, *SourcesConstructor, owner);
        SourcesConstructor->InitCursor(read.GetScanCursorVerified());
    });

    {
        auto customConclusion = DoInitCustom(owner, read);
        if (customConclusion.IsFail()) {
            return customConclusion;
        }
    }

    StatsMode = read.StatsMode;
    GroupedMemoryLimiterOperator = read.GroupedMemoryLimiterOperator;

    if (read.readConflictingPortions) {
        auto& opManager = owner->GetOperationsManager();
        std::vector<TPortionInfo::TConstPtr> conflictingPortions = SourcesConstructor->GetConflictingPortions();
        if (!conflictingPortions.empty()) {
            for (const TPortionInfo::TConstPtr& p : conflictingPortions) {
                // add maybe conflicting writes
                if (!p->IsCommitted()) {
                    AFL_VERIFY(p->GetPortionType() == EPortionType::Written);
                    auto* written = static_cast<const TWrittenPortionInfo*>(p.get());
                    auto writeId = written->GetInsertWriteId();
                    auto op = opManager.GetOperationByInsertWriteIdVerified(writeId);
                    // we do not need to check our own uncommitted writes
                    if (op->GetLockId() != *LockId) {
                        AddMaybeConflictingWrite(writeId, op->GetLockId());
                    }
                }
            }

            // register the lock in the end, when Init() is successful for sure
            DataLockGuard = owner->GetDataLocksManager()->RegisterLock<NDataLocks::TListPortionsLock>(
                read.GetLockName(), conflictingPortions, NDataLocks::ELockCategory::Scan, true);
        }
    }
    return TConclusionStatus::Success();
}

TReadMetadata::TReadMetadata(const std::shared_ptr<const TVersionedIndex>& schemaIndex, const TReadDescription& read)
    : TBase(schemaIndex, read.GetSorting(), read.GetProgram(), schemaIndex->GetSchemaVerified(read.GetSnapshot()), read.GetSnapshot(),
          read.GetScanCursorVerified(), read.GetTabletId())
    , DuplicateFilteringNeeded(read.NeedDuplicateFiltering())
    , TableMetadataAccessor(read.TableMetadataAccessor)
    , ReadStats(std::make_shared<TReadStats>())
{
}

std::set<ui32> TReadMetadata::GetEarlyFilterColumnIds() const {
    auto& indexInfo = ResultIndexSchema->GetIndexInfo();
    const auto& ids = GetProgram().GetEarlyFilterColumns();
    std::set<ui32> result(ids.begin(), ids.end());
    AFL_VERIFY(result.size() == ids.size());
    for (auto&& i : GetProgram().GetEarlyFilterColumns()) {
        AFL_VERIFY(indexInfo.HasColumnId(i))("column_id", i);
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
    if (DataLockGuard) {
        DataLockGuard->Release(*owner.GetDataLocksManager());
    }

    auto alreadyAborted = LockId.has_value() && owner.GetOperationsManager().GetLockOptional(*GetLockId()) == nullptr;
    if (!NeedToDetectConflicts() || alreadyAborted) {
        return;
    }

    const ui64 lock = *GetLockId();
    if (GetBreakLockOnReadFinished()) {
        owner.GetOperationsManager().GetLockVerified(lock).SetBroken();
    } else {
        NOlap::NTxInteractions::TTxConflicts conflicts;
        for (auto&& lockIdToCommit : GetConflictingLockIds()) {
            // if lockIdToCommit commits, lock must be broken
            conflicts.Add(lockIdToCommit, lock);
        }
        if (!conflicts.IsEmpty()) {
            auto writer = std::make_shared<NOlap::NTxInteractions::TEvReadFinishedWriter>(
                TableMetadataAccessor->GetPathIdVerified().InternalPathId, conflicts);
            owner.GetOperationsManager().AddEventForLock(owner, lock, writer);
        }
    }
}

void TReadMetadata::DoOnBeforeStartReading(NColumnShard::TColumnShard& owner) const {
    if (!NeedToDetectConflicts()) {
        return;
    }

    auto evWriter = std::make_shared<NOlap::NTxInteractions::TEvReadStartWriter>(TableMetadataAccessor->GetPathIdVerified(),
        GetResultSchema()->GetIndexInfo().GetPrimaryKey(), GetPKRangesFilterPtr(), GetMaybeConflictingLockIds());
    owner.GetOperationsManager().AddEventForLock(owner, *LockId, evWriter);
}

void TReadMetadata::DoOnReplyConstruction(const ui64 tabletId, NKqp::NInternalImplementation::TEvScanData& scanData) const {
    if (LockSharingInfo) {
        NKikimrDataEvents::TLock lockInfo;
        lockInfo.SetLockId(LockSharingInfo->GetLockId());
        lockInfo.SetGeneration(LockSharingInfo->GetGeneration());
        lockInfo.SetDataShard(tabletId);
        lockInfo.SetCounter(LockSharingInfo->GetInternalGenerationCounter());
        TableMetadataAccessor->GetPathIdVerified().SchemeShardLocalPathId.ToProto(lockInfo);
        lockInfo.SetHasWrites(LockSharingInfo->HasWrites());
        if (LockSharingInfo->IsBroken()) {
            scanData.LocksInfo.BrokenLocks.emplace_back(std::move(lockInfo));
        } else {
            scanData.LocksInfo.Locks.emplace_back(std::move(lockInfo));
        }
    }
}

}   // namespace NKikimr::NOlap::NReader::NCommon

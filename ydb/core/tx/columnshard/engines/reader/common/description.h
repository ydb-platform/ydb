#pragma once
#include <ydb/core/tx/columnshard/common/path_id.h>
#include <ydb/core/tx/columnshard/common/snapshot.h>
#include <ydb/core/tx/columnshard/engines/metadata_accessor.h>
#include <ydb/core/tx/columnshard/engines/predicate/filter.h>
#include <ydb/core/tx/columnshard/operations/manager.h>
#include <ydb/core/tx/program/program.h>

#include <ydb/library/yql/dq/actors/protos/dq_stats.pb.h>
namespace NKikimr::NOlap::NReader {

enum class ERequestSorting {
    NONE = 0 /* "not_sorted" */,
    ASC /* "ascending" */,
    DESC /* "descending" */,
};

enum class EDeduplicationPolicy {
    ALLOW_DUPLICATES = 0,
    PREVENT_DUPLICATES,
};

// Describes read/scan request
class TReadDescription {
private:
    TSnapshot Snapshot;
    TProgramContainer Program;
    std::optional<std::shared_ptr<IScanCursor>> ScanCursor;
    YDB_ACCESSOR_DEF(TString, ScanIdentifier);
    YDB_ACCESSOR(ERequestSorting, Sorting, ERequestSorting::NONE);
    YDB_READONLY(ui64, TabletId, 0);

public:
    // Table
    ui64 TxId = 0;
    std::optional<ui64> LockId;
    std::optional<NKikimrDataEvents::ELockMode> LockMode;
    std::shared_ptr<ITableMetadataAccessor> TableMetadataAccessor;
    std::shared_ptr<NOlap::TPKRangesFilter> PKRangesFilter;
    NYql::NDqProto::EDqStatsMode StatsMode = NYql::NDqProto::EDqStatsMode::DQ_STATS_MODE_NONE;
    EDeduplicationPolicy DeduplicationPolicy = EDeduplicationPolicy::ALLOW_DUPLICATES;
    bool readNonconflictingPortions;
    bool readConflictingPortions;
    // portions that the current tx has written
    std::optional<THashSet<TInsertWriteId>> ownPortions;

    bool IsReverseSort() const {
        return Sorting == ERequestSorting::DESC;
    }

    // List of columns
    std::vector<ui32> ColumnIds;

    const std::shared_ptr<IScanCursor>& GetScanCursorVerified() const {
        AFL_VERIFY(ScanCursor);
        return *ScanCursor;
    }

    void SetScanCursor(const std::shared_ptr<IScanCursor>& cursor) {
        AFL_VERIFY(!ScanCursor);
        ScanCursor = cursor;
    }

    void SetLock(std::optional<ui64> lockId, std::optional<NKikimrDataEvents::ELockMode> lockMode, const NColumnShard::TLockFeatures* lock) {
        LockId = lockId;
        LockMode = lockMode;

        auto snapshotIsolation = lockMode.value_or(NKikimrDataEvents::OPTIMISTIC) == NKikimrDataEvents::OPTIMISTIC_SNAPSHOT_ISOLATION;

        // always true for now, will be false for reads that check only conflicts (comming soon with Snapshot Isolation)
        readNonconflictingPortions = true;

        // do not check conflicts for Snapshot isolated txs or txs with no lock
        readConflictingPortions = LockId.has_value() && !snapshotIsolation;

        if (readNonconflictingPortions && !readConflictingPortions && lock != nullptr && lock->GetWriteOperations().size() > 0) {
            ownPortions = THashSet<TInsertWriteId>();
            for (auto& writeOperation : lock->GetWriteOperations()) {
                for (auto insertWriteId : writeOperation->GetInsertWriteIds()) {
                    ownPortions->emplace(insertWriteId);
                }
            }
        }

        // we want to read something, don't we?
        AFL_VERIFY(readNonconflictingPortions || readConflictingPortions);
        if (ownPortions.has_value() && !ownPortions->empty()) {
            AFL_VERIFY(readNonconflictingPortions);
        }
    }

    TReadDescription(const ui64 tabletId, const TSnapshot& snapshot, const ERequestSorting sorting)
        : Snapshot(snapshot)
        , Sorting(sorting)
        , TabletId(tabletId)
        , PKRangesFilter(std::make_shared<TPKRangesFilter>(TPKRangesFilter::BuildEmpty()))
    {
    }

    void SetProgram(TProgramContainer&& value) {
        Program = std::move(value);
    }

    const TSnapshot& GetSnapshot() const {
        return Snapshot;
    }

    const TProgramContainer& GetProgram() const {
        return Program;
    }
};

}   // namespace NKikimr::NOlap::NReader

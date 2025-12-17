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
    std::optional<ui32> LockNodeId;
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

    void SetLock(
        std::optional<ui64> lockId, 
        std::optional<ui32> lockNodeId,
        std::optional<NKikimrDataEvents::ELockMode> lockMode, 
        const NColumnShard::TLockFeatures* lock,
        const bool readOnlyConflicts
    ) {
        LockId = lockId;
        LockNodeId = lockNodeId;
        LockMode = lockMode;
        auto snapshotIsolation = lockId.has_value() && lockMode.value_or(NKikimrDataEvents::OPTIMISTIC) == NKikimrDataEvents::OPTIMISTIC_SNAPSHOT_ISOLATION;

        readNonconflictingPortions = !readOnlyConflicts;

        // do not check conflicts for Snapshot isolated txs or txs with no lock
        readConflictingPortions = (LockId.has_value() && !snapshotIsolation) || readOnlyConflicts;

        // if we need conflicting portions, we just take all uncommitted portions (from other txs and own)
        // but if we do not need conflicting portions, we need to remember own portions ids,
        // so that we can pick only own uncommitted portions for the read
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
        // we do not have cases (at the moment) when we need to read only conflicts for a scan with no transaction
        if (!LockId.has_value()) {
            AFL_VERIFY(!readOnlyConflicts);
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

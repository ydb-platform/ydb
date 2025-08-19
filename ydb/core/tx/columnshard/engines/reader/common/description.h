#pragma once
#include <ydb/core/tx/columnshard/common/path_id.h>
#include <ydb/core/tx/columnshard/common/snapshot.h>
#include <ydb/core/tx/columnshard/engines/metadata_accessor.h>
#include <ydb/core/tx/columnshard/engines/predicate/filter.h>
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
    std::shared_ptr<ITableMetadataAccessor> TableMetadataAccessor;
    std::shared_ptr<NOlap::TPKRangesFilter> PKRangesFilter;
    NYql::NDqProto::EDqStatsMode StatsMode = NYql::NDqProto::EDqStatsMode::DQ_STATS_MODE_NONE;
    EDeduplicationPolicy DeduplicationPolicy = EDeduplicationPolicy::ALLOW_DUPLICATES;

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

    TReadDescription(const ui64 tabletId, const TSnapshot& snapshot, const ERequestSorting sorting)
        : Snapshot(snapshot)
        , Sorting(sorting)
        , TabletId(tabletId)
        , PKRangesFilter(std::make_shared<NOlap::TPKRangesFilter>()) {
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

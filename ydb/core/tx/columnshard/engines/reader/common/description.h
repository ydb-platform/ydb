#pragma once
#include <ydb/core/tx/columnshard/common/path_id.h>
#include <ydb/core/tx/columnshard/common/snapshot.h>
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

class ITableMetadataAccessor {
private:
    YDB_READONLY_DEF(TString, TablePath);

public:
    ITableMetadataAccessor(const TString& tablePath)
        : TablePath(tablePath) {
        AFL_VERIFY(!!TablePath);
    }

    TString GetTableName() const {
        return TFsPath(TablePath).Fix().GetName();
    }
};

class TSysViewTableAccessor: public ITableMetadataAccessor {
private:
    using TBase = ITableMetadataAccessor;

public:
    TSysViewTableAccessor(const TString& tableName)
        : TBase(tableName) {
        AFL_VERIFY(GetTablePath().find(".sys") != TString::npos);
    }
};

class TUserTableAccessor: public ITableMetadataAccessor {
private:
    using TBase = ITableMetadataAccessor;
    YDB_READONLY_DEF(NColumnShard::TUnifiedPathId, PathId);

public:
    TSysViewTableAccessor(const TString& tableName, const TUnifiedPathId& pathId)
        : TBase(tableName)
        , PathId(pathId) {
        AFL_VERIFY(GetTablePath().find(".sys") == TString::npos);
    }
};

class TAbsentTableAccessor: public ITableMetadataAccessor {
private:
    YDB_READONLY_DEF(NColumnShard::TUnifiedPathId, PathId);

public:
    TAbsentTableAccessor(const TString& tableName, const TUnifiedPathId& pathId)
        : TBase(tableName)
        , PathId(pathId) {
    }
};

// Describes read/scan request
struct TReadDescription {
private:
    TSnapshot Snapshot;
    TProgramContainer Program;
    std::shared_ptr<IScanCursor> ScanCursor;
    YDB_ACCESSOR_DEF(TString, ScanIdentifier);
    YDB_ACCESSOR(ERequestSorting, Sorting, ERequestSorting::NONE);
    YDB_READONLY(ui64, TabletId, 0);

public:
    // Table
    ui64 TxId = 0;
    std::optional<ui64> LockId;
    std::shared_ptr<ITableMetadataAccessor> TableMetadataAccessor;
    bool ReadNothing = false;
    // Less[OrEqual], Greater[OrEqual] or both
    // There's complex logic in NKikimr::TTableRange comparison that could be emulated only with separated compare
    // operations with potentially different columns. We have to remove columns to support -Inf (Null) and +Inf.
    std::shared_ptr<NOlap::TPKRangesFilter> PKRangesFilter;
    NYql::NDqProto::EDqStatsMode StatsMode = NYql::NDqProto::EDqStatsMode::DQ_STATS_MODE_NONE;
    EDeduplicationPolicy DeduplicationPolicy = EDeduplicationPolicy::ALLOW_DUPLICATES;

    // List of columns
    std::vector<ui32> ColumnIds;

    const std::shared_ptr<IScanCursor>& GetScanCursorOptional() const {
        return ScanCursor;
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

#pragma once
#include <ydb/core/tx/columnshard/common/snapshot.h>
#include <ydb/core/tx/columnshard/engines/predicate/filter.h>
#include <ydb/core/tx/program/program.h>

#include <ydb/library/yql/dq/actors/protos/dq_stats.pb.h>
namespace NKikimr::NOlap::NReader {

class IScanCursor {
public:
};

class TSimpleScanCursor: public IScanCursor {
private:
    YDB_READONLY_DEF(std::shared_ptr<arrow::RecordBatch>, PrimaryKey);
    YDB_READONLY(ui64, PortionId, 0);
    YDB_READONLY(ui32, RecordIndex, 0);

public:
    TSimpleScanCursor(const std::shared_ptr<arrow::RecordBatch>& pk, const ui64 portionId, const ui32 recordIndex)
        : PrimaryKey(pk)
        , PortionId(portionId)
        , RecordIndex(recordIndex) {
    }
};

// Describes read/scan request
struct TReadDescription {
private:
    TSnapshot Snapshot;
    TProgramContainer Program;
    std::shared_ptr<IScanCursor> ScanCursor;

public:
    // Table
    ui64 TxId = 0;
    std::optional<ui64> LockId;
    ui64 PathId = 0;
    TString TableName;
    bool ReadNothing = false;
    // Less[OrEqual], Greater[OrEqual] or both
    // There's complex logic in NKikimr::TTableRange comparison that could be emulated only with separated compare
    // operations with potentially different columns. We have to remove columns to support -Inf (Null) and +Inf.
    std::shared_ptr<NOlap::TPKRangesFilter> PKRangesFilter;
    NYql::NDqProto::EDqStatsMode StatsMode = NYql::NDqProto::EDqStatsMode::DQ_STATS_MODE_NONE;

    // List of columns
    std::vector<ui32> ColumnIds;
    std::vector<TString> ColumnNames;

    const std::shared_ptr<IScanCursor>& GetScanCursor() const {
        AFL_VERIFY(ScanCursor);
        return ScanCursor;
    }

    void SetScanCursor(const std::shared_ptr<IScanCursor>& cursor) {
        AFL_VERIFY(!ScanCursor);
        ScanCursor = cursor;
    }

    TReadDescription(const TSnapshot& snapshot, const bool isReverse)
        : Snapshot(snapshot)
        , PKRangesFilter(std::make_shared<NOlap::TPKRangesFilter>(isReverse)) {
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

}

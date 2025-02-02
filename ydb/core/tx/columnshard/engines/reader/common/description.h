#pragma once
#include <ydb/core/tx/columnshard/common/snapshot.h>
#include <ydb/core/tx/columnshard/engines/predicate/filter.h>
#include <ydb/core/tx/program/program.h>

#include <ydb/library/yql/dq/actors/protos/dq_stats.pb.h>
namespace NKikimr::NOlap::NReader {

// Describes read/scan request
struct TReadDescription {
private:
    TSnapshot Snapshot;
    std::optional<NOlap::TLock> Lock;
    TProgramContainer Program;
    std::shared_ptr<IScanCursor> ScanCursor;
    YDB_ACCESSOR_DEF(TString, ScanIdentifier);

public:
    // Table
    ui64 TxId = 0;
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

    TReadDescription(const TSnapshot& snapshot, const std::optional<NOlap::TLock>& lock, const bool isReverse)
        : Snapshot(snapshot)
        , Lock(lock)
        , PKRangesFilter(std::make_shared<NOlap::TPKRangesFilter>(isReverse)) {
    }

    void SetProgram(TProgramContainer&& value) {
        Program = std::move(value);
    }

    const TSnapshot& GetSnapshot() const {
        return Snapshot;
    }

    const std::optional<NOlap::TLock>& GetLock() const {
        return Lock;
    }

    const TProgramContainer& GetProgram() const {
        return Program;
    }
};

}

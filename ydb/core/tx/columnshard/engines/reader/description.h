#pragma once
#include <ydb/core/tx/columnshard/engines/predicate/filter.h>
#include <ydb/core/tablet_flat/flat_dbase_scheme.h>
#include <ydb/core/formats/arrow/program.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>

namespace NKikimr::NOlap {

class IColumnResolver {
public:
    virtual ~IColumnResolver() = default;
    virtual TString GetColumnName(ui32 id, bool required = true) const = 0;
    virtual const NTable::TScheme::TTableSchema& GetSchema() const = 0;
};

// Describes read/scan request
struct TReadDescription {
private:
    TSnapshot Snapshot;
public:
    // Table
    ui64 PathId = 0;
    TString TableName;
    bool ReadNothing = false;
    // Less[OrEqual], Greater[OrEqual] or both
    // There's complex logic in NKikimr::TTableRange comparison that could be emulated only with separated compare
    // operations with potentially different columns. We have to remove columns to support -Inf (Null) and +Inf.
    NOlap::TPKRangesFilter PKRangesFilter;

    // SSA Program
    std::shared_ptr<NSsa::TProgram> Program;
    std::shared_ptr<arrow::RecordBatch> ProgramParameters; // TODO

    // List of columns
    std::vector<ui32> ColumnIds;
    std::vector<TString> ColumnNames;

    std::shared_ptr<NSsa::TProgram> AddProgram(const IColumnResolver& columnResolver, const NKikimrSSA::TProgram& program);
    TReadDescription(const TSnapshot& snapshot, const bool isReverse)
        : Snapshot(snapshot)
        , PKRangesFilter(isReverse) {

    }

    const TSnapshot& GetSnapshot() const {
        return Snapshot;
    }
};

}

#pragma once
#include "program.h"
#include <ydb/core/tx/columnshard/engines/predicate/filter.h>

namespace NKikimr::NOlap {

// Describes read/scan request
struct TReadDescription {
private:
    TSnapshot Snapshot;
    TProgramContainer Program;
public:
    // Table
    ui64 PathId = 0;
    TString TableName;
    bool ReadNothing = false;
    // Less[OrEqual], Greater[OrEqual] or both
    // There's complex logic in NKikimr::TTableRange comparison that could be emulated only with separated compare
    // operations with potentially different columns. We have to remove columns to support -Inf (Null) and +Inf.
    NOlap::TPKRangesFilter PKRangesFilter;

    // List of columns
    std::vector<ui32> ColumnIds;
    std::vector<TString> ColumnNames;
    
    TReadDescription(const TSnapshot& snapshot, const bool isReverse)
        : Snapshot(snapshot)
        , PKRangesFilter(isReverse) {
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

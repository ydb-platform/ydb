#pragma once
#include <ydb/core/formats/arrow_helpers.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/protos/ssa.pb.h>
#include <ydb/core/tx/columnshard/engines/predicate.h>

namespace NKikimr::NOlap {
    struct TIndexInfo;
}

namespace NKikimr::NColumnShard {

using NOlap::TWriteId;

std::pair<NOlap::TPredicate, NOlap::TPredicate>
RangePredicates(const TSerializedTableRange& range, const TVector<std::pair<TString, NScheme::TTypeId>>& columns);

class IColumnResolver {
public:
    virtual ~IColumnResolver() = default;
    virtual TString GetColumnName(ui32 id, bool required = true) const = 0;
};

// Describes read/scan request
struct TReadDescription {
    // Table
    ui64 PathId = 0;
    TString TableName;
    bool ReadNothing = false;
    // Less[OrEqual], Greater[OrEqual] or both
    // There's complex logic in NKikimr::TTableRange comparison that could be emulated only with separated compare
    // operations with potentially different columns. We have to remove columns to support -Inf (Null) and +Inf.
    std::shared_ptr<NOlap::TPredicate> GreaterPredicate;
    std::shared_ptr<NOlap::TPredicate> LessPredicate;

    // SSA Program
    std::vector<std::shared_ptr<NArrow::TProgramStep>> Program;
    THashMap<ui32, TString> ProgramSourceColumns;
    std::shared_ptr<arrow::RecordBatch> ProgramParameters;

    // List of columns
    TVector<ui32> ColumnIds;
    TVector<TString> ColumnNames;
    // Order
    bool Ascending = false;
    bool Descending = false;
    // Snapshot
    ui64 PlanStep = 0;
    ui64 TxId = 0;

    std::shared_ptr<NArrow::TSsaProgramSteps> AddProgram(const IColumnResolver& columnResolver, const NKikimrSSA::TProgram& program);
};

}

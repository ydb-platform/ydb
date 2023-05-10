#include "columnshard_common.h"
#include <ydb/core/formats/arrow/arrow_batch_builder.h>

namespace NKikimr::NColumnShard {

namespace {

using EOperation = NArrow::EOperation;
using EAggregate = NArrow::EAggregate;
using TAssign = NSsa::TAssign;
using TAggregateAssign = NSsa::TAggregateAssign;

std::vector<NScheme::TTypeInfo> ExtractTypes(const std::vector<std::pair<TString, NScheme::TTypeInfo>>& columns) {
    std::vector<NScheme::TTypeInfo> types;
    types.reserve(columns.size());
    for (auto& [name, type] : columns) {
        types.push_back(type);
    }
    return types;
}

TString FromCells(const TConstArrayRef<TCell>& cells, const std::vector<std::pair<TString, NScheme::TTypeInfo>>& columns) {
    Y_VERIFY(cells.size() == columns.size());
    if (cells.empty()) {
        return {};
    }

    std::vector<NScheme::TTypeInfo> types = ExtractTypes(columns);

    NArrow::TArrowBatchBuilder batchBuilder;
    batchBuilder.Reserve(1);
    bool ok = batchBuilder.Start(columns);
    Y_VERIFY(ok);

    batchBuilder.AddRow(NKikimr::TDbTupleRef(), NKikimr::TDbTupleRef(types.data(), cells.data(), cells.size()));

    auto batch = batchBuilder.FlushBatch(false);
    Y_VERIFY(batch);
    Y_VERIFY(batch->num_columns() == (int)cells.size());
    Y_VERIFY(batch->num_rows() == 1);
    return NArrow::SerializeBatchNoCompression(batch);
}
}

using EOperation = NArrow::EOperation;
using TPredicate = NOlap::TPredicate;

std::pair<TPredicate, TPredicate> RangePredicates(const TSerializedTableRange& range,
                                                  const std::vector<std::pair<TString, NScheme::TTypeInfo>>& columns) {
    std::vector<TCell> leftCells;
    std::vector<std::pair<TString, NScheme::TTypeInfo>> leftColumns;
    bool leftTrailingNull = false;
    {
        TConstArrayRef<TCell> cells = range.From.GetCells();
        const size_t size = cells.size();
        Y_ASSERT(size <= columns.size());
        leftCells.reserve(size);
        leftColumns.reserve(size);
        for (size_t i = 0; i < size; ++i) {
            if (!cells[i].IsNull()) {
                leftCells.push_back(cells[i]);
                leftColumns.push_back(columns[i]);
                leftTrailingNull = false;
            } else {
                leftTrailingNull = true;
            }
        }
    }

    std::vector<TCell> rightCells;
    std::vector<std::pair<TString, NScheme::TTypeInfo>> rightColumns;
    bool rightTrailingNull = false;
    {
        TConstArrayRef<TCell> cells = range.To.GetCells();
        const size_t size = cells.size();
        Y_ASSERT(size <= columns.size());
        rightCells.reserve(size);
        rightColumns.reserve(size);
        for (size_t i = 0; i < size; ++i) {
            if (!cells[i].IsNull()) {
                rightCells.push_back(cells[i]);
                rightColumns.push_back(columns[i]);
                rightTrailingNull = false;
            } else {
                rightTrailingNull = true;
            }
        }
    }

    const bool fromInclusive = range.FromInclusive || leftTrailingNull;
    const bool toInclusive = range.ToInclusive && !rightTrailingNull;

    TString leftBorder = FromCells(leftCells, leftColumns);
    TString rightBorder = FromCells(rightCells, rightColumns);
    return std::make_pair(
        TPredicate(fromInclusive ? EOperation::GreaterEqual : EOperation::Greater, leftBorder, NArrow::MakeArrowSchema(leftColumns)),
        TPredicate(toInclusive ? EOperation::LessEqual : EOperation::Less, rightBorder, NArrow::MakeArrowSchema(rightColumns)));
}

}

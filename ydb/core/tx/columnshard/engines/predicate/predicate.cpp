#include "predicate.h"

#include <ydb/core/formats/arrow/arrow_batch_builder.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/switch_type.h>

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NOlap {

TPredicate::TPredicate(EOperation op, std::shared_ptr<arrow::RecordBatch> batch) noexcept
    : Operation(op)
    , Batch(std::move(batch)) {
    Y_ABORT_UNLESS(IsFrom() || IsTo());
}

TPredicate::TPredicate(EOperation op, const TString& serializedBatch, const std::shared_ptr<arrow::Schema>& schema)
    : Operation(op) {
    Y_ABORT_UNLESS(IsFrom() || IsTo());
    if (!serializedBatch.empty()) {
        Batch = NArrow::DeserializeBatch(serializedBatch, schema);
        Y_ABORT_UNLESS(Batch);
    }
}

std::vector<TString> TPredicate::ColumnNames() const {
    std::vector<TString> out;
    out.reserve(Batch->num_columns());
    for (const auto& field : Batch->schema()->fields()) {
        out.emplace_back(field->name());
    }
    return out;
}

std::vector<NScheme::TTypeInfo> ExtractTypes(const std::vector<std::pair<TString, NScheme::TTypeInfo>>& columns) {
    std::vector<NScheme::TTypeInfo> types;
    types.reserve(columns.size());
    for (auto& [name, type] : columns) {
        types.push_back(type);
    }
    return types;
}

TString FromCells(const TConstArrayRef<TCell>& cells, const std::vector<std::pair<TString, NScheme::TTypeInfo>>& columns) {
    Y_ABORT_UNLESS(cells.size() == columns.size());
    if (cells.empty()) {
        return {};
    }

    std::vector<NScheme::TTypeInfo> types = ExtractTypes(columns);

    NArrow::TArrowBatchBuilder batchBuilder;
    batchBuilder.Reserve(1);
    auto startStatus = batchBuilder.Start(columns);
    Y_ABORT_UNLESS(startStatus.ok(), "%s", startStatus.ToString().c_str());

    batchBuilder.AddRow(NKikimr::TDbTupleRef(), NKikimr::TDbTupleRef(types.data(), cells.data(), cells.size()));

    auto batch = batchBuilder.FlushBatch(false);
    Y_ABORT_UNLESS(batch);
    Y_ABORT_UNLESS(batch->num_columns() == (int)cells.size());
    Y_ABORT_UNLESS(batch->num_rows() == 1);
    return NArrow::SerializeBatchNoCompression(batch);
}

std::pair<NKikimr::NOlap::TPredicate, NKikimr::NOlap::TPredicate> TPredicate::DeserializePredicatesRange(
    const TSerializedTableRange& range, const std::vector<std::pair<TString, NScheme::TTypeInfo>>& columns) {
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
    auto leftSchema = NArrow::MakeArrowSchema(leftColumns);
    Y_ASSERT(leftSchema.ok());
    auto rightSchema = NArrow::MakeArrowSchema(rightColumns);
    Y_ASSERT(rightSchema.ok());
    return std::make_pair(
        TPredicate(fromInclusive ? NKernels::EOperation::GreaterEqual : NKernels::EOperation::Greater, leftBorder, leftSchema.ValueUnsafe()),
        TPredicate(toInclusive ? NKernels::EOperation::LessEqual : NKernels::EOperation::Less, rightBorder, rightSchema.ValueUnsafe()));
}

std::shared_ptr<arrow::RecordBatch> TPredicate::CutNulls(const std::shared_ptr<arrow::RecordBatch>& batch) {
    AFL_VERIFY(batch->num_rows() == 1)("count", batch->num_rows());
    AFL_VERIFY(batch->num_columns());
    std::vector<std::shared_ptr<arrow::Array>> colsNotNull;
    std::vector<std::shared_ptr<arrow::Field>> fieldsNotNull;
    ui32 idx = 0;
    for (auto&& i : batch->columns()) {
        if (i->IsNull(0)) {
            break;
        }
        colsNotNull.emplace_back(i);
        fieldsNotNull.emplace_back(batch->schema()->field(idx));
        ++idx;
    }
    AFL_VERIFY(colsNotNull.size());
    return arrow::RecordBatch::Make(std::make_shared<arrow::Schema>(fieldsNotNull), 1, colsNotNull);
}

IOutputStream& operator<<(IOutputStream& out, const TPredicate& pred) {
    out << NSsa::GetFunctionName(pred.Operation);

    for (i32 i = 0; i < pred.Batch->num_columns(); ++i) {
        auto array = pred.Batch->column(i);
        out << pred.Batch->schema()->field(i)->name() << ": ";
        NArrow::SwitchType(array->type_id(), [&](const auto& type) {
            using TWrap = std::decay_t<decltype(type)>;
            using TArray = typename arrow::TypeTraits<typename TWrap::T>::ArrayType;

            auto& typedArray = static_cast<const TArray&>(*array);
            if (typedArray.IsNull(0)) {
                out << "NULL";
            } else {
                auto value = typedArray.GetView(0);
                using T = std::decay_t<decltype(value)>;
                if constexpr (std::is_same_v<T, arrow::util::string_view>) {
                    out << "'" << std::string_view(value.data(), value.size()) << "'";
                } else {
                    out << "'" << value << "'";
                }
            }
            return true;
        });
        out << " ";
    }

    return out;
}

}   // namespace NKikimr::NOlap

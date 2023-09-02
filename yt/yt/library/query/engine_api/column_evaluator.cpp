#include "column_evaluator.h"

#include <yt/yt/client/table_client/row_buffer.h>

namespace NYT::NQueryClient {

using NTableClient::TMutableVersionedRow;
using NTableClient::TMutableUnversionedRow;
using NTableClient::TUnversionedValue;

////////////////////////////////////////////////////////////////////////////////

Y_WEAK TColumnEvaluatorPtr TColumnEvaluator::Create(
    const TTableSchemaPtr& /*schema*/,
    const TConstTypeInferrerMapPtr& /*typeInferrers*/,
    const TConstFunctionProfilerMapPtr& /*profilers*/)
{
    // Proper implementation resides in yt/yt/library/query/engine/column_evaluator.cpp.
    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

TColumnEvaluator::TColumnEvaluator(
    std::vector<TColumn> columns,
    std::vector<bool> isAggregate)
    : Columns_(std::move(columns))
    , IsAggregate_(std::move(isAggregate))
{ }

void TColumnEvaluator::EvaluateKey(TMutableRow fullRow, const TRowBufferPtr& buffer, int index) const
{
    YT_VERIFY(index < static_cast<int>(fullRow.GetCount()));
    YT_VERIFY(index < std::ssize(Columns_));

    const auto& column = Columns_[index];
    const auto& evaluator = column.Evaluator;
    YT_VERIFY(evaluator);

    // Zero row to avoid garbage after evaluator.
    fullRow[index] = MakeUnversionedSentinelValue(EValueType::Null);

    evaluator(
        column.Variables.GetLiteralValues(),
        column.Variables.GetOpaqueData(),
        &fullRow[index],
        fullRow.Elements(),
        buffer.Get());

    fullRow[index].Id = index;
}

void TColumnEvaluator::EvaluateKeys(TMutableRow fullRow, const TRowBufferPtr& buffer) const
{
    for (int index = 0; index < std::ssize(Columns_); ++index) {
        if (Columns_[index].Evaluator) {
            EvaluateKey(fullRow, buffer, index);
        }
    }
}

void TColumnEvaluator::EvaluateKeys(
    TMutableVersionedRow fullRow,
    const TRowBufferPtr& buffer) const
{
    auto row = buffer->CaptureRow(fullRow.Keys(), /*captureValues*/ false);
    EvaluateKeys(row, buffer);

    for (int index = 0; index < fullRow.GetKeyCount(); ++index) {
        if (Columns_[index].Evaluator) {
            fullRow.Keys()[index] = row[index];
        }
    }
}

const std::vector<int>& TColumnEvaluator::GetReferenceIds(int index) const
{
    return Columns_[index].ReferenceIds;
}

TConstExpressionPtr TColumnEvaluator::GetExpression(int index) const
{
    return Columns_[index].Expression;
}

void TColumnEvaluator::InitAggregate(
    int index,
    TUnversionedValue* state,
    const TRowBufferPtr& buffer) const
{
    Columns_[index].Aggregate.Init(buffer.Get(), state);
    state->Id = index;
}

void TColumnEvaluator::UpdateAggregate(
    int index,
    TUnversionedValue* state,
    const TRange<TUnversionedValue> update,
    const TRowBufferPtr& buffer) const
{
    Columns_[index].Aggregate.Update(buffer.Get(), state, update);
    state->Id = index;
}

void TColumnEvaluator::MergeAggregate(
    int index,
    TUnversionedValue* state,
    const TUnversionedValue& mergeeState,
    const TRowBufferPtr& buffer) const
{
    Columns_[index].Aggregate.Merge(buffer.Get(), state, &mergeeState);
    state->Id = index;
}

void TColumnEvaluator::FinalizeAggregate(
    int index,
    TUnversionedValue* result,
    const TUnversionedValue& state,
    const TRowBufferPtr& buffer) const
{
    Columns_[index].Aggregate.Finalize(buffer.Get(), result, &state);
    result->Id = index;
}

////////////////////////////////////////////////////////////////////////////////

Y_WEAK IColumnEvaluatorCachePtr CreateColumnEvaluatorCache(
    TColumnEvaluatorCacheConfigPtr /*config*/,
    TConstTypeInferrerMapPtr /*typeInferrers*/,
    TConstFunctionProfilerMapPtr /*profilers*/)
{
    // Proper implementation resides in yt/yt/library/query/engine/column_evaluator.cpp.
    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

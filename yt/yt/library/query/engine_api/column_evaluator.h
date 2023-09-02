#pragma once

#include "evaluation_helpers.h"
#include "public.h"

#include <yt/yt/client/table_client/unversioned_row.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

class TColumnEvaluator
    : public TRefCounted
{
public:
    static TColumnEvaluatorPtr Create(
        const TTableSchemaPtr& schema,
        const TConstTypeInferrerMapPtr& typeInferrers,
        const TConstFunctionProfilerMapPtr& profilers);

    void EvaluateKey(
        TMutableRow fullRow,
        const TRowBufferPtr& buffer,
        int index) const;

    void EvaluateKeys(
        TMutableRow fullRow,
        const TRowBufferPtr& buffer) const;

    void EvaluateKeys(
        NTableClient::TMutableVersionedRow fullRow,
        const TRowBufferPtr& buffer) const;

    const std::vector<int>& GetReferenceIds(int index) const;
    TConstExpressionPtr GetExpression(int index) const;

    void InitAggregate(
        int schemaId,
        NTableClient::TUnversionedValue* state,
        const TRowBufferPtr& buffer) const;

    void UpdateAggregate(
        int index,
        NTableClient::TUnversionedValue* state,
        const TRange<NTableClient::TUnversionedValue> update,
        const TRowBufferPtr& buffer) const;

    void MergeAggregate(
        int index,
        NTableClient::TUnversionedValue* state,
        const NTableClient::TUnversionedValue& mergeeState,
        const TRowBufferPtr& buffer) const;

    void FinalizeAggregate(
        int index,
        NTableClient::TUnversionedValue* result,
        const NTableClient::TUnversionedValue& state,
        const TRowBufferPtr& buffer) const;

    bool IsAggregate(int index) const;

private:
    struct TColumn
    {
        TCGExpressionCallback Evaluator;
        TCGVariables Variables;
        std::vector<int> ReferenceIds;
        TConstExpressionPtr Expression;
        TCGAggregateCallbacks Aggregate;
    };

    std::vector<TColumn> Columns_;
    std::vector<bool> IsAggregate_;

    TColumnEvaluator(
        std::vector<TColumn> columns,
        std::vector<bool> isAggregate);

    DECLARE_NEW_FRIEND()
};

DEFINE_REFCOUNTED_TYPE(TColumnEvaluator)

////////////////////////////////////////////////////////////////////////////////

struct IColumnEvaluatorCache
    : public virtual TRefCounted
{
    virtual TColumnEvaluatorPtr Find(const TTableSchemaPtr& schema) = 0;

    virtual void Configure(const TColumnEvaluatorCacheDynamicConfigPtr& config) = 0;
};

DEFINE_REFCOUNTED_TYPE(IColumnEvaluatorCache)

IColumnEvaluatorCachePtr CreateColumnEvaluatorCache(
    TColumnEvaluatorCacheConfigPtr config,
    TConstTypeInferrerMapPtr typeInferrers = GetBuiltinTypeInferrers(),
    TConstFunctionProfilerMapPtr profilers = GetBuiltinFunctionProfilers());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

#define COLUMN_EVALUATOR_INL_H_
#include "column_evaluator-inl.h"
#undef COLUMN_EVALUATOR_INL_H_

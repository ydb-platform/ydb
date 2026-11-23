#pragma once

#include "ast.h"
#include "query.h"
#include "public.h"

#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/unversioned_row.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

struct TQueryPreparerBufferTag
{ };

////////////////////////////////////////////////////////////////////////////////

EValueType GetType(const NAst::TLiteralValue& literalValue);

TValue GetValue(const NAst::TLiteralValue& literalValue);

void BuildRow(
    TUnversionedRowBuilder* rowBuilder,
    const NAst::TLiteralValueTuple& tuple,
    const std::vector<EValueType>& argTypes,
    TStringBuf source);

TSharedRange<TRow> LiteralTupleListToRows(
    const NAst::TLiteralValueTupleList& literalTuples,
    const std::vector<EValueType>& argTypes,
    TStringBuf source);

TSharedRange<TRowRange> LiteralRangesListToRows(
    const NAst::TLiteralValueRangeList& literalRanges,
    const std::vector<EValueType>& argTypes,
    TStringBuf source);

////////////////////////////////////////////////////////////////////////////////

TConstExpressionPtr ApplyRewriters(TConstExpressionPtr expr);

////////////////////////////////////////////////////////////////////////////////

//! Reinterpret a "table.column[accessor]" reference as "column.member[accessor]".
//! Return nullopt if the reference has no table part.
std::optional<NAst::TReference> TryReinterpretAsMemberAccess(
    const NAst::TReference& reference);

////////////////////////////////////////////////////////////////////////////////

std::optional<TValue> FoldConstants(
    EUnaryOp opcode,
    const TConstExpressionPtr& operand);

std::optional<TValue> FoldConstants(
    EBinaryOp opcode,
    const TConstExpressionPtr& lhsExpr,
    const TConstExpressionPtr& rhsExpr);

////////////////////////////////////////////////////////////////////////////////

struct TNameSource
{
    const TTableSchema& Schema;
    std::optional<std::string> Alias;
    std::vector<TColumnDescriptor>* Mapping = nullptr;

    // Columns inherited from previous sources.
    THashSet<std::string>* SelfJoinedColumns = nullptr;
    // Columns from current source.
    THashSet<std::string>* ForeignJoinedColumns = nullptr;
    THashSet<std::string> SharedColumns = {};
    TTableSchemaPtr ModifiedSchemaHolder = {};
};

class TReferenceResolver
{
public:
    void AddTable(TNameSource nameSource);

    TLogicalTypePtr Resolve(const NAst::TColumnReference& reference);

    void PopulateAllColumns();

    void Finish();

private:
    struct TResolvedInfo
    {
        TLogicalTypePtr Type;
        int SourceIndex;
    };

    THashMap<
        NAst::TColumnReference,
        TResolvedInfo,
        NAst::TColumnReferenceHasher,
        NAst::TColumnReferenceEqComparer> Lookup_;

    std::vector<TNameSource> NameSources_;
};

class TExpressionBuilder
{
public:
    DEFINE_BYVAL_RO_PROPERTY(TStringBuf, Source);
    DEFINE_BYREF_RO_PROPERTY(TConstTypeInferrerMapPtr, Functions);

    TExpressionBuilder(
        TStringBuf source,
        const TConstTypeInferrerMapPtr& functions,
        const TPreparePlanFragmentContext& context);

    virtual void AddTable(TNameSource nameSource) = 0;
    virtual TLogicalTypePtr ResolveColumn(const NAst::TColumnReference& reference) = 0;
    virtual void PopulateAllColumns() = 0;
    virtual void Finish() = 0;

    virtual void SetGroupData(const TNamedItemList* groupItems, TAggregateItemList* aggregateItems, ETotalsMode totalsMode) = 0;

    virtual std::string InferGroupItemName(
        const TConstExpressionPtr& typedExpression,
        const NAst::TExpression& expressionsAst) = 0;

    virtual TConstExpressionPtr BuildTypedExpression(
        const NAst::TExpression* expr,
        TRange<EValueType> resultTypes = {
            EValueType::Null,
            EValueType::Int64,
            EValueType::Uint64,
            EValueType::Double,
            EValueType::Boolean,
            EValueType::String,
            EValueType::Any,
            EValueType::Composite
        }) = 0;

    virtual ~TExpressionBuilder() = default;

    const NLogging::TLogger& GetLogger() const;
    const TPreparePlanFragmentContext& GetContext() const;

protected:
    const TPreparePlanFragmentContext& Context_;
};

std::unique_ptr<TExpressionBuilder> CreateExpressionBuilderV1(
    TStringBuf source,
    const TConstTypeInferrerMapPtr& functions,
    const NAst::TAliasMap& aliasMap,
    const TPreparePlanFragmentContext& context);

std::unique_ptr<TExpressionBuilder> CreateExpressionBuilderV2(
    TStringBuf source,
    const TConstTypeInferrerMapPtr& functions,
    const NAst::TAliasMap& aliasMap,
    const TPreparePlanFragmentContext& context);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

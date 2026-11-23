#pragma once

#include "ast.h"
#include "callbacks.h"

#include <yt/yt/core/misc/memory_usage_tracker.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

using TFunctionsFetcher = std::function<void(
    TRange<std::string> names,
    const TTypeInferrerMapPtr& typeInferrers,
    NCodegen::EExecutionBackend executionBackend)>;

void DefaultFetchFunctions(
    TRange<std::string> names,
    const TTypeInferrerMapPtr& typeInferrers,
    NCodegen::EExecutionBackend executionBackend);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EParseMode,
    (Query)
    (JobQuery)
    (Expression)
);

struct TParsedSource
{
    TParsedSource(
        TStringBuf source,
        NAst::TAstHead astHead);

    std::string Source;
    NAst::TAstHead AstHead;
};

std::unique_ptr<TParsedSource> ParseSource(
    TStringBuf source,
    EParseMode mode,
    NYson::TYsonStringBuf placeholderValues = {},
    int syntaxVersion = 1);

////////////////////////////////////////////////////////////////////////////////

void ValidateOrderByOffsetAndLimit(const NAst::TQuery& queryAst);
void ValidateHaving(const NAst::TQuery& queryAst, bool hasOrderBy);

TPlanFragmentPtr PreparePlanFragment(
    IPrepareCallbacks* callbacks,
    TStringBuf source,
    const TPreparePlanFragmentOptions& options = {},
    NYson::TYsonStringBuf placeholderValues = {},
    IMemoryUsageTrackerPtr memoryTracker = nullptr);

TPlanFragmentPtr PreparePlanFragment(
    IPrepareCallbacks* callbacks,
    TStringBuf source,
    NAst::TQuery& query,
    NAst::TAstHead& astHead,
    const TPreparePlanFragmentOptions& options = {},
    IMemoryUsageTrackerPtr memoryTracker = nullptr);

////////////////////////////////////////////////////////////////////////////////

TQueryPtr PrepareJobQuery(
    TStringBuf source,
    const TTableSchemaPtr& tableSchema,
    const TFunctionsFetcher& functionsFetcher);

TConstExpressionPtr PrepareExpression(
    TStringBuf source,
    const TTableSchema& tableSchema,
    const TConstTypeInferrerMapPtr& functions = GetBuiltinTypeInferrers(),
    THashSet<std::string>* references = nullptr);

TConstExpressionPtr PrepareExpression(
    const TParsedSource& parsedSource,
    const TTableSchema& tableSchema,
    const TConstTypeInferrerMapPtr& functions);

TConstExpressionPtr PrepareExpression(
    const TParsedSource& parsedSource,
    const TTableSchema& tableSchema,
    int builderVersion = 1,
    const TConstTypeInferrerMapPtr& functions = GetBuiltinTypeInferrers(),
    THashSet<std::string>* references = nullptr);

////////////////////////////////////////////////////////////////////////////////

class TExpressionBuilder;

TJoinClausePtr BuildJoinClause(
    const TDataSplit& foreignDataSplit,
    const NAst::TJoin& tableJoin,
    TStringBuf source,
    const NAst::TAliasMap& aliasMap,
    const TConstTypeInferrerMapPtr& functions,
    size_t* globalCommonKeyPrefix,
    const TTableSchemaPtr& tableSchema,
    const std::optional<std::string>& tableAlias,
    TExpressionBuilder* builder,
    const TPreparePlanFragmentContext& context);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

#pragma once

#include "query.h"
#include "ast.h"
#include "callbacks.h"

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

using TFunctionsFetcher = std::function<void(
    const std::vector<TString>& names,
    const TTypeInferrerMapPtr& typeInferrers)>;

void DefaultFetchFunctions(
    const std::vector<TString>& names,
    const TTypeInferrerMapPtr& typeInferrers);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EParseMode,
    (Query)
    (JobQuery)
    (Expression)
);

struct TParsedSource
{
    TParsedSource(
        const TString& source,
        NAst::TAstHead astHead);

    TString Source;
    NAst::TAstHead AstHead;
};

std::unique_ptr<TParsedSource> ParseSource(
    const TString& source,
    EParseMode mode,
    NYson::TYsonStringBuf placeholderValues = {});

////////////////////////////////////////////////////////////////////////////////

struct TPlanFragment
{
    TQueryPtr Query;
    TDataSource DataSource;
};

std::unique_ptr<TPlanFragment> PreparePlanFragment(
    IPrepareCallbacks* callbacks,
    const TString& source,
    const TFunctionsFetcher& functionsFetcher = DefaultFetchFunctions,
    NYson::TYsonStringBuf placeholderValues = {});

std::unique_ptr<TPlanFragment> PreparePlanFragment(
    IPrepareCallbacks* callbacks,
    const TParsedSource& parsedSource,
    const TFunctionsFetcher& functionsFetcher = DefaultFetchFunctions);

////////////////////////////////////////////////////////////////////////////////

TQueryPtr PrepareJobQuery(
    const TString& source,
    const TTableSchemaPtr& tableSchema,
    const TFunctionsFetcher& functionsFetcher);

TConstExpressionPtr PrepareExpression(
    const TString& source,
    const TTableSchema& tableSchema,
    const TConstTypeInferrerMapPtr& functions = GetBuiltinTypeInferrers(),
    THashSet<TString>* references = nullptr);

TConstExpressionPtr PrepareExpression(
    const TParsedSource& parsedSource,
    const TTableSchema& tableSchema,
    const TConstTypeInferrerMapPtr& functions = GetBuiltinTypeInferrers(),
    THashSet<TString>* references = nullptr);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

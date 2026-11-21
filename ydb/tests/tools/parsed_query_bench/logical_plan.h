#pragma once

#include "engine.h"

#include <ydb/core/kqp/opt/rbo/kqp_operator.h>
#include <ydb/core/kqp/provider/yql_kikimr_gateway.h>
#include <ydb/tests/tools/parsed_query_bench/proto/logical_plan.pb.h>

namespace NKikimr::NKqp::NParsedQueryBench {

using TCatalogLookup = std::function<NYql::TKikimrTableMetadataPtr(const TString&)>;
struct TImportedPlan {
    TVector<TIntrusivePtr<TOpRoot>> Roots;
    TVector<TVector<const NYql::TTypeAnnotationNode*>> ColumnTypes;
};

TImportedPlan ImportLogicalPlan(const Ydb::Query::LogicalPlan& plan, NYql::TExprContext& ctx,
    const TCatalogLookup& lookup, const TParameterMap& parameters);
TString LogicalSignature(TOpRoot& root);

} // namespace NKikimr::NKqp::NParsedQueryBench

#pragma once

#include <ydb/tests/tools/parsed_query_bench/proto/main_stages.pb.h>
#include <yql/essentials/ast/yql_expr.h>

namespace NYql {
struct TTypeAnnotationContext;
}

namespace NKikimr::NKqp::NParsedQueryBench {

void ExportKqlQueryList(const NYql::TExprNode& query, NYql::TExprContext& ctx,
    const NYql::TTypeAnnotationContext& types, Ydb::Query::KqlQueryList& output);
NYql::TExprNode::TPtr ImportKqlQueryList(const Ydb::Query::KqlQueryList& input, NYql::TExprContext& ctx,
    NYql::TTypeAnnotationContext& types);

} // namespace NKikimr::NKqp::NParsedQueryBench

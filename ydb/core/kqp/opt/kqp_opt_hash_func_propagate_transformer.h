#pragma once

#include <ydb/core/kqp/common/kqp_yql.h>

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/yql_graph_transformer.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/core/yql_opt_utils.h>

namespace NYql {

struct TKikimrConfiguration;

} // namespace NYql

namespace NKikimr::NKqp {

TAutoPtr<NYql::IGraphTransformer> CreateKqpTxsHashFuncPropagateTransformer(
    NYql::TTypeAnnotationContext& typesCtx,
    const TIntrusivePtr<NYql::TKikimrConfiguration>& config
);

} // namespace NKikimr::NKqp

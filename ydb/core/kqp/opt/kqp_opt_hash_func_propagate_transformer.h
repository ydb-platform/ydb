#pragma once

#include "kqp_opt.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/yql_graph_transformer.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/core/yql_opt_utils.h>


namespace NKikimr {
namespace NKqp {

using namespace NYql;
using namespace NYql::NNodes;
using namespace NOpt;

TAutoPtr<IGraphTransformer> CreateKqpTxsHashFuncPropagateTransformer(
    TAutoPtr<NYql::IGraphTransformer> typeAnnTransformer,
    TTypeAnnotationContext& typesCtx,
    const TKikimrConfiguration::TPtr& config
);

}
}
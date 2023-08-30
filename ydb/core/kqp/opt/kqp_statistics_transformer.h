#pragma once

#include "kqp_opt.h"

#include <ydb/library/yql/core/yql_statistics.h>

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/library/yql/core/yql_graph_transformer.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider_impl.h>
#include <ydb/library/yql/core/yql_opt_utils.h>

namespace NKikimr {
namespace NKqp {

using namespace NYql;
using namespace NYql::NNodes;
using namespace NOpt;

/***
 * Statistics transformer is a transformer that propagates statistics and costs from
 * the leaves of the plan DAG up to the root of the DAG. It handles a number of operators,
 * but will simply stop propagation if in encounters an operator that it has no rules for.
 * One of such operators is EquiJoin, but there is a special rule to handle EquiJoin.
*/
class TKqpStatisticsTransformer : public TSyncTransformerBase {

    TTypeAnnotationContext* TypeCtx;
    const TKikimrConfiguration::TPtr& Config;
    const TKqpOptimizeContext& KqpCtx;

    public:
        TKqpStatisticsTransformer(const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx, TTypeAnnotationContext& typeCtx, 
            const TKikimrConfiguration::TPtr& config) : 
            TypeCtx(&typeCtx), 
            Config(config),
            KqpCtx(*kqpCtx) {}

        // Main method of the transformer
        IGraphTransformer::TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final;

        // Rewind currently does nothing
        void Rewind() {

    }
};

TAutoPtr<IGraphTransformer> CreateKqpStatisticsTransformer(const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx, 
    TTypeAnnotationContext& typeCtx, const TKikimrConfiguration::TPtr& config);
}
}

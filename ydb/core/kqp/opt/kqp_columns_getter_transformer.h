#pragma once

#include "kqp_opt.h"

#include <ydb/library/yql/core/yql_statistics.h>

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/opt/logical/kqp_opt_cbo.h>
#include <ydb/library/yql/core/yql_graph_transformer.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider_impl.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/dq/opt/dq_opt_stat_transformer_base.h>

namespace NKikimr::NKqp {

using namespace NYql;
using namespace NYql::NNodes;
using namespace NOpt;

class TKqpColumnsGetterTransformer : public TSyncTransformerBase {
public:
    TKqpColumnsGetterTransformer(
        const TKikimrConfiguration::TPtr& 
    )
        // : Config(config)
    {}

    // Main method of the transformer
    IGraphTransformer::TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final; 

    void Rewind() override {}

    ~TKqpColumnsGetterTransformer() override = default;

private:
    bool BeforeLambdas(const TExprNode::TPtr& input);

    bool BeforeLambdasUnmatched(const TExprNode::TPtr& input);

    void PropagateTableToLambdaArgument(const TExprNode::TPtr& input);

    bool AfterLambdas(const TExprNode::TPtr& input);

    bool AfterLambdasUnmatched(const TExprNode::TPtr& input);

private:
    THashMap<TExprNode::TPtr, TExprNode::TPtr> TableByExprNode;
    THashMap<TString, TVector<TString>> ColumnsByTableName;
    // const TKikimrConfiguration::TPtr& Config;
};

TAutoPtr<IGraphTransformer> CreateKqpColumnsGetterTransformer(
    const TKikimrConfiguration::TPtr& config
);

} // end of NKikimr::NKqp namespace

#pragma once

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/library/yql/core/yql_graph_transformer.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider.h>

namespace NKikimr::NKqp {

using namespace NYql;
using namespace NYql::NNodes;

class TKqpColumnsGetterTransformer : public TSyncTransformerBase {
public:
    TKqpColumnsGetterTransformer(
        const TKikimrConfiguration::TPtr& config,
        TTypeAnnotationContext& typesCtx,
        TKikimrTablesData& tables,
        TString cluster,
        TActorSystem* actorSystem
    )
        : Config(config)
        , TypesCtx(typesCtx)
        , Tables(tables)
        , Cluster(cluster)
        , ActorSystem(actorSystem)
    {}

    // Main method of the transformer
    IGraphTransformer::TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final; 

    void Rewind() override {}

    ~TKqpColumnsGetterTransformer() override  = default;

private:
    bool BeforeLambdas(const TExprNode::TPtr& input);

    bool BeforeLambdasUnmatched(const TExprNode::TPtr& input);

    void PropagateTableToLambdaArgument(const TExprNode::TPtr& input);

    bool AfterLambdas(const TExprNode::TPtr& input);

    bool AfterLambdasUnmatched(const TExprNode::TPtr& input);
    
private:
    THashMap<TExprNode::TPtr, TExprNode::TPtr> TableByExprNode;
    THashMap<TString, THashSet<TString>> ColumnsByTableName;

    const TKikimrConfiguration::TPtr& Config;
    TTypeAnnotationContext& TypesCtx;
    TKikimrTablesData& Tables;
    TString Cluster;
    TActorSystem* ActorSystem;
};

TAutoPtr<IGraphTransformer> CreateKqpColumnsGetterTransformer(
    const TKikimrConfiguration::TPtr& config,
    TTypeAnnotationContext& typesCtx,
    TKikimrTablesData& tables,
    TString cluster,
    TActorSystem* actorSystem
);

} // end of NKikimr::NKqp namespace

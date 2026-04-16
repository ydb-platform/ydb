#pragma once
#include "kqp_column_statistics_utils.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <yql/essentials/core/yql_graph_transformer.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/core/yql_opt_utils.h>
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider.h>

namespace NKikimr::NKqp {

using namespace NYql;
using namespace NYql::NNodes;

/*
 * This tranformer collects column's and table's names from an AST. It propogates
 * KqpTable node from the leaves to the root of the tree and searches members in filters.
 * Then it requests column statistics for these attributes from the column statistics service
 * and stores it into a TTypeAnnotationContext.
 */
class TKqpColumnStatisticsRequester : public TGraphTransformerBase {
public:
    TKqpColumnStatisticsRequester(
        const TKikimrConfiguration::TPtr& config,
        TTypeAnnotationContext& typesCtx,
        TKikimrTablesData& tables,
        const TString& cluster,
        const TString& database,
        TActorSystem* actorSystem
    )
        : Config(config)
        , TypesCtx(typesCtx)
        , Tables(tables)
        , Cluster(cluster)
        , Database(database)
        , ActorSystem(actorSystem)
    {}

    // Main method of the transformer
    IGraphTransformer::TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final;

    NThreading::TFuture<void> DoGetAsyncFuture(const TExprNode& input) final;

    IGraphTransformer::TStatus DoApplyAsyncChanges(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final;

    void Rewind() override {}

    ~TKqpColumnStatisticsRequester() override  = default;

private:
    bool BeforeLambdas(const TExprNode::TPtr& input);

    bool BeforeLambdasUnmatched(const TExprNode::TPtr& input);

    void PropagateTableToLambdaArgument(const TExprNode::TPtr& input);

    bool AfterLambdas(const TExprNode::TPtr& input);

    bool AfterLambdasUnmatched(const TExprNode::TPtr& input);

private:
    TMaybe<std::pair<TString, TString>> GetTableAndColumnNames(const TCoMember& member);

private:
    THashMap<TExprNode::TPtr, TExprNode::TPtr> KqpTableByExprNode;
    THashMap<TString, THashSet<TString>> CMColumnsByTableName;
    THashMap<TString, THashSet<TString>> HistColumnsByTableName;

    //////////////////////////////////////////////////////////////
    /* for waiting response with column statistics */
    std::optional<TColumnStatisticsResponse> ColumnStatisticsResponse;
    NThreading::TFuture<void> AsyncReadiness;

    //////////////////////////////////////////////////////////////

    const TKikimrConfiguration::TPtr& Config;
    TTypeAnnotationContext& TypesCtx;
    TKikimrTablesData& Tables;
    const TString Cluster;
    const TString Database;
    TActorSystem* ActorSystem;
};

TAutoPtr<IGraphTransformer> CreateKqpColumnStatisticsRequester(
    const TKikimrConfiguration::TPtr& config,
    TTypeAnnotationContext& typesCtx,
    TKikimrTablesData& tables,
    const TString& cluster,
    const TString& database,
    TActorSystem* actorSystem
);

} // end of NKikimr::NKqp namespace

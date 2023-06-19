#pragma once

#include <ydb/core/kqp/common/kqp_tx_info.h>

#include <ydb/core/kqp/provider/yql_kikimr_expr_nodes.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider.h>

#include <ydb/library/yql/utils/log/log.h>

namespace NKikimr {
namespace NKqp {

struct TKqlTransformContext : TThrRefBase {
    TKqlTransformContext(NYql::TKikimrConfiguration::TPtr& config, TIntrusivePtr<NYql::TKikimrQueryContext> queryCtx,
        TIntrusivePtr<NYql::TKikimrTablesData> tables)
        : Config(config)
        , QueryCtx(queryCtx)
        , Tables(tables) {}

    NYql::TKikimrConfiguration::TPtr Config;
    TIntrusivePtr<NYql::TKikimrQueryContext> QueryCtx;
    TIntrusivePtr<NYql::TKikimrTablesData> Tables;
    NActors::TActorId ReplyTarget;

    NKqpProto::TKqpStatsQuery QueryStats;
    std::shared_ptr<const NKqpProto::TKqpPhyQuery> PhysicalQuery;

    TVector<TSimpleSharedPtr<NKikimrMiniKQL::TResult>> MkqlResults;
    TVector<NKikimrMiniKQL::TResult> PhysicalQueryResults;

    void Reset() {
        ReplyTarget = {};
        MkqlResults.clear();
        QueryStats = {};
        PhysicalQuery = nullptr;
        PhysicalQueryResults.clear();
    }
};

class TLogExprTransformer {
public:
    TLogExprTransformer(const TString& description, NYql::NLog::EComponent component, NYql::NLog::ELevel level)
        : Description(description)
        , Component(component)
        , Level(level) {}

    NYql::IGraphTransformer::TStatus operator()(const NYql::TExprNode::TPtr& input, NYql::TExprNode::TPtr& output,
        NYql::TExprContext& ctx);

    static TAutoPtr<NYql::IGraphTransformer> Sync(const TString& description,
        NYql::NLog::EComponent component = NYql::NLog::EComponent::ProviderKqp,
        NYql::NLog::ELevel level = NYql::NLog::ELevel::INFO);

    static void LogExpr(const NYql::TExprNode& input, NYql::TExprContext& ctx, const TString& description,
        NYql::NLog::EComponent component = NYql::NLog::EComponent::ProviderKqp,
        NYql::NLog::ELevel level = NYql::NLog::ELevel::INFO);

private:
    TString Description;
    NYql::NLog::EComponent Component;
    NYql::NLog::ELevel Level;
};

} // namespace NKqp
} // namespace NKikimr

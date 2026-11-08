#pragma once

#include <ydb/core/kqp/provider/yql_kikimr_expr_nodes.h>
#include <ydb/core/protos/kqp_stats.pb.h>
#include <ydb/library/actors/core/actorid.h>

#include <yql/essentials/utils/log/log.h>

#include <library/cpp/json/writer/json_value.h>

#include <util/generic/maybe.h>
#include <util/generic/ptr.h>

#include <optional>
#include <functional>

namespace NYql {

struct TKikimrConfiguration;
struct TKikimrQueryContext;
class TKikimrTablesData;
class TExprNode;

} // namespace NYql

namespace NKqpProto {

class TKqpPhyQuery;

} // namespace NKqpProto

namespace NKikimr::NKqp {

class TOpRoot;

struct TKqlTransformContext : TThrRefBase {
    TKqlTransformContext(const TIntrusivePtr<NYql::TKikimrConfiguration>& config, TIntrusivePtr<NYql::TKikimrQueryContext> queryCtx,
        TIntrusivePtr<NYql::TKikimrTablesData> tables);

    TIntrusivePtr<NYql::TKikimrConfiguration> Config;
    TIntrusivePtr<NYql::TKikimrQueryContext> QueryCtx;
    TIntrusivePtr<NYql::TKikimrTablesData> Tables;
    NActors::TActorId ReplyTarget;

    NKqpProto::TKqpStatsQuery QueryStats;
    std::shared_ptr<const NKqpProto::TKqpPhyQuery> PhysicalQuery;

    TIntrusivePtr<NYql::TExprNode> ExplainTransformerInput; // Explain transformer must work after other transformers, but use input before peephole
    std::optional<NJson::TJsonValue> PlanJson; // JSON plan for the new optimizer
    TMaybe<NYql::NNodes::TKiDataQueryBlocks> DataQueryBlocks;

    // Frontend input, retained across Reset() of runner outputs. SetupSession
    // replaces it before destroying the AST and its annotation context.
    // The callback receives a top-level query index, the legacy result root and
    // column hints. A null result requests legacy conversion for that root; user
    // diagnostics must already have failed the earlier frontend passes. Count
    // checks in the RBO transformer protect the positional query/result mapping.
    size_t NewParserRboQueryCount = 0;
    std::function<TIntrusivePtr<TOpRoot>(size_t, const NYql::TExprNode::TPtr&,
        const NYql::TExprNode::TPtr&, NYql::TExprContext&)> NewParserRboLowering;

    void Reset();
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

// Saves current input into TKqlTransformContext::ExplainTransformerInput
TAutoPtr<NYql::IGraphTransformer> CreateSaveExplainTransformerInput(TKqlTransformContext& transformCtx);

} // namespace NKikimr::NKqp

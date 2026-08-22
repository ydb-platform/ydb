#pragma once

#include <ydb/core/kqp/provider/yql_kikimr_expr_nodes.h>
#include <ydb/core/protos/kqp_stats.pb.h>
#include <ydb/library/actors/core/actorid.h>

#include <yql/essentials/utils/log/log.h>

#include <library/cpp/json/writer/json_value.h>

#include <util/generic/maybe.h>
#include <util/generic/ptr.h>

#include <optional>

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

#include "kqp_transform.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>

#include <yql/essentials/minikql/mkql_string_util.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/providers/common/provider/yql_provider.h>

namespace NKikimr::NKqp {

using namespace NYql;
using namespace NKikimr::NMiniKQL;
using namespace NUdf;

TKqlTransformContext::TKqlTransformContext(const TIntrusivePtr<TKikimrConfiguration>& config, TIntrusivePtr<TKikimrQueryContext> queryCtx, TIntrusivePtr<TKikimrTablesData> tables)
    : Config(config)
    , QueryCtx(queryCtx)
    , Tables(tables)
{}

void TKqlTransformContext::Reset() {
    ReplyTarget = {};
    QueryStats = {};
    PhysicalQuery = nullptr;
    ExplainTransformerInput = nullptr;
    DataQueryBlocks = Nothing();
    // QUCA2: [Reset] При повторном запуске и fallback очищать выбранный объект для кэша и пакет новых
    //   PeepholeResult: артефакты неуспешного прохода не должны попасть в ответ следующей попытки.
    //   Запрос: добавить сюда PlanJson.reset(). Сейчас PlanJson в Reset() не очищается, а AddPlans
    //   (kqp_rbo_transformer.cpp) требует пустого слота; это работает только потому, что контекст создаётся
    //   заново на каждую компиляцию. Нам нужно чистить все выходные поля в одном месте.
}

IGraphTransformer::TStatus TLogExprTransformer::operator()(const TExprNode::TPtr& input, TExprNode::TPtr& output,
    TExprContext& ctx)
{
    Y_UNUSED(ctx);

    output = input;
    LogExpr(*input, ctx, Description, Component, Level);
    return IGraphTransformer::TStatus::Ok;
}

TAutoPtr<IGraphTransformer> TLogExprTransformer::Sync(const TString& description, NYql::NLog::EComponent component,
    NYql::NLog::ELevel level)
{
    return CreateFunctorTransformer(TLogExprTransformer(description, component, level));
}

void TLogExprTransformer::LogExpr(const TExprNode& input, TExprContext& ctx, const TString& description, NYql::NLog::EComponent component,
    NYql::NLog::ELevel level)
{
    YQL_CVLOG(level, component) << description << ":\n" << KqpExprToPrettyString(input, ctx);
}

namespace {

class TSaveExplainTransformerInputTransformer : public TSyncTransformerBase {
public:
    TSaveExplainTransformerInputTransformer(TKqlTransformContext& transformCtx)
        : TransformCtx(transformCtx)
    {
    }

    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) override {
        Y_UNUSED(ctx);
        output = input;
        if (!TransformCtx.ExplainTransformerInput) {
            YQL_CVLOG(NYql::NLog::ELevel::TRACE, NYql::NLog::EComponent::Core) << "Saving explain plan";
            TransformCtx.ExplainTransformerInput = input;
        }

        return TStatus::Ok;
    }

    void Rewind() override {
        TransformCtx.ExplainTransformerInput = nullptr;
    }

private:
    TKqlTransformContext& TransformCtx;
};

} // anonymous namespace

TAutoPtr<IGraphTransformer> CreateSaveExplainTransformerInput(TKqlTransformContext& transformCtx) {
    return new TSaveExplainTransformerInputTransformer(transformCtx);
}

} // namespace NKikimr::NKqp

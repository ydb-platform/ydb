#include "yql_co_transformer.h"
#include "yql_co.h"

#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/core/yql_join.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/core/yql_opt_window.h>

#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/utils/yql_panic.h>
#include <ydb/library/yql/ast/yql_gc_nodes.h>

#include <util/string/cast.h>
#include <unordered_set>

namespace NYql {
namespace {

class TCommonOptTransformer final : public TSyncTransformerBase {
public:
    TCommonOptTransformer(TTypeAnnotationContext* typeCtx, bool final)
        : TypeCtx(typeCtx)
        , Final(final)
    {}

    IGraphTransformer::TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final;
    void Rewind() final;
private:
    IGraphTransformer::TStatus DoTransform(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx,
        const TCallableOptimizerMap& callables, TProcessedNodesSet& processedNodes,
        bool withParents);

    IGraphTransformer::TStatus DoTransform(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx,
        const TFinalizingOptimizerMap& callables);

private:
    TProcessedNodesSet SimpleProcessedNodes[TCoCallableRules::SIMPLE_STEPS];
    TProcessedNodesSet FlowProcessedNodes[TCoCallableRules::FLOW_STEPS];
    TProcessedNodesSet FinalProcessedNodes;
    TTypeAnnotationContext* TypeCtx;
    const bool Final;
};

}

TAutoPtr<IGraphTransformer> CreateCommonOptTransformer(TTypeAnnotationContext* typeCtx) {
    return new TCommonOptTransformer(typeCtx, false);
}

TAutoPtr<IGraphTransformer> CreateCommonOptFinalTransformer(TTypeAnnotationContext* typeCtx) {
    return new TCommonOptTransformer(typeCtx, true);
}

IGraphTransformer::TStatus TCommonOptTransformer::DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) {
    IGraphTransformer::TStatus status = IGraphTransformer::TStatus::Ok;
    output = std::move(input);

    if (Final) {
        return DoTransform(input = std::move(output), output, ctx, TCoCallableRules::Instance().FinalCallables, FinalProcessedNodes, true);
    }

    for (ui32 i = 0; i < TCoCallableRules::SIMPLE_STEPS; ++i) {
        status = DoTransform(input = std::move(output), output, ctx, TCoCallableRules::Instance().SimpleCallables[i], SimpleProcessedNodes[i], true);
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }
    }

    for (ui32 i = 0; i < TCoCallableRules::FLOW_STEPS; ++i) {
        status = DoTransform(input = std::move(output), output, ctx, TCoCallableRules::Instance().FlowCallables[i], FlowProcessedNodes[i], true);
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }
    }

    status = DoTransform(input = std::move(output), output, ctx, TCoCallableRules::Instance().Finalizers);
    if (status.Level != IGraphTransformer::TStatus::Ok) {
        return status;
    }

    return status;
}

void TCommonOptTransformer::Rewind() {
    FinalProcessedNodes.clear();

    for (auto& set : FlowProcessedNodes) {
        set.clear();
    }

    for (auto& set : SimpleProcessedNodes) {
        set.clear();
    }
}

IGraphTransformer::TStatus TCommonOptTransformer::DoTransform(
    const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx,
    const TCallableOptimizerMap& callables,
    TProcessedNodesSet& processedNodes, bool withParents)
{
    TOptimizeExprSettings settings(TypeCtx);
    settings.ProcessedNodes = &processedNodes;
    settings.CustomInstantTypeTransformer = TypeCtx->CustomInstantTypeTransformer.Get();
    TParentsMap parentsMap;
    TOptimizeContext optCtx;
    optCtx.Types = TypeCtx;
    if (withParents) {
        GatherParents(*input, parentsMap);
        optCtx.ParentsMap = &parentsMap;
    }

    TCallableOptimizerExt defaultOpt;
    auto defaultIt = callables.find("");
    if (defaultIt != callables.end()) {
        defaultOpt = defaultIt->second;
    }

    return OptimizeExpr(input, output, [&callables, &optCtx, defaultOpt](const TExprNode::TPtr& node, TExprContext& ctx) -> TExprNode::TPtr {
        const auto rule = callables.find(node->Content());
        TExprNode::TPtr result = node;
        if (rule != callables.cend()) {
            result = (rule->second)(node, ctx, optCtx);
        }

        if (defaultOpt && result == node) {
            result = defaultOpt(node, ctx, optCtx);
        }

        return result;
    }, ctx, settings);
}

IGraphTransformer::TStatus TCommonOptTransformer::DoTransform(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx,
    const TFinalizingOptimizerMap& callables)
{
    TParentsMap parentsMap;
    TOptimizeContext optCtx;
    optCtx.Types = TypeCtx;
    GatherParents(*input, parentsMap);
    optCtx.ParentsMap = &parentsMap;

    TNodeOnNodeOwnedMap toOptimize;
    bool isError = false;
    TFinalizingOptimizerExt defaultOpt;
    auto defaultIt = callables.find("");
    if (defaultIt != callables.end()) {
        defaultOpt = defaultIt->second;
    }
    VisitExpr(input,
        [&toOptimize, &isError](const TExprNode::TPtr&) {
            return toOptimize.empty() && !isError;
        },
        [&callables, &defaultOpt, &toOptimize, &ctx, &optCtx, &isError](const TExprNode::TPtr& node) {
            if (isError) {
                return false;
            }

            if (toOptimize.empty()) {
                const auto rule = callables.find(node->Content());
                if (callables.cend() != rule) {
                    isError = isError || !(rule->second)(node, toOptimize, ctx, optCtx);
                }
                if (defaultOpt && toOptimize.empty() && !isError) {
                    isError = isError || !defaultOpt(node, toOptimize, ctx, optCtx);
                }
            }
            return toOptimize.empty() && !isError;
        }
    );

    if (isError) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!toOptimize.empty()) {
        TOptimizeExprSettings settings(TypeCtx);
        settings.VisitTuples = true;
        settings.CustomInstantTypeTransformer = TypeCtx->CustomInstantTypeTransformer.Get();
        return RemapExpr(input, output, toOptimize, ctx, settings);
    }

    output = input;
    return IGraphTransformer::TStatus::Ok;
}

const TCoCallableRules& TCoCallableRules::Instance() {
    return *Singleton<TCoCallableRules>();
}

TCoCallableRules::TCoCallableRules() {
    RegisterCoSimpleCallables1(SimpleCallables[SIMPLE_STEP_1]);
    RegisterCoSimpleCallables2(SimpleCallables[SIMPLE_STEP_2]);
    RegisterCoSimpleCallables3(SimpleCallables[SIMPLE_STEP_3]);
    RegisterCoFlowCallables1(FlowCallables[FLOW_STEP_1]);
    RegisterCoFlowCallables2(FlowCallables[FLOW_STEP_2]);
    RegisterCoFinalizers(Finalizers);
    RegisterCoFinalCallables(FinalCallables);
}

}

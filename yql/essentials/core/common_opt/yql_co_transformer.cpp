#include "yql_co_transformer.h"
#include "yql_co.h"
#include "yql_co_blocks.h"

#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/core/yql_join.h>
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/core/yql_opt_window.h>

#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/yql_panic.h>
#include <yql/essentials/ast/yql_gc_nodes.h>

#include <util/string/cast.h>
#include <unordered_set>

namespace NYql {

static const char CheckMissingWorldOptName[] = "CheckMissingWorld";

namespace {

class TCommonOptTransformer final : public TSyncTransformerBase {
public:
    TCommonOptTransformer(TTypeAnnotationContext* typeCtx, bool final, bool forPeephole)
        : TypeCtx_(typeCtx)
        , Final_(final)
        , ForPeephole_(forPeephole)
    {}

    IGraphTransformer::TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final;
    void Rewind() final;
private:
    IGraphTransformer::TStatus DoTransform(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx,
        const TCallableOptimizerMap& callables, TProcessedNodesSet& processedNodes,
        bool withParents);

    IGraphTransformer::TStatus DoTransform(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx,
        const TFinalizingOptimizerMap& callables);

    bool ScanErrors(const TExprNode& node, TExprContext& ctx);

private:
    std::array<TProcessedNodesSet, TCoCallableRules::SIMPLE_STEPS> SimpleProcessedNodes_;
    std::array<TProcessedNodesSet, TCoCallableRules::FLOW_STEPS> FlowProcessedNodes_;
    TProcessedNodesSet FinalProcessedNodes_;
    TProcessedNodesSet ErrorProcessedNodes_;
    THashSet<TIssue> AddedErrors_;
    TTypeAnnotationContext* TypeCtx_;
    const bool Final_;
    const bool ForPeephole_;
    bool CheckMissingWorld_ = false;
};

}

TAutoPtr<IGraphTransformer> CreateCommonOptTransformer(bool forPeephole, TTypeAnnotationContext* typeCtx) {
    return new TCommonOptTransformer(typeCtx, false, forPeephole);
}

TAutoPtr<IGraphTransformer> CreateCommonOptFinalTransformer(TTypeAnnotationContext* typeCtx) {
    return new TCommonOptTransformer(typeCtx, true, false);
}

IGraphTransformer::TStatus TCommonOptTransformer::DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) {
    IGraphTransformer::TStatus status = IGraphTransformer::TStatus::Ok;
    output = std::move(input);

    if (IsOptimizerEnabled<CheckMissingWorldOptName>(*TypeCtx_) && !IsOptimizerDisabled<CheckMissingWorldOptName>(*TypeCtx_)) {
        CheckMissingWorld_ = true;
    }

    if (Final_) {
        return DoTransform(input = std::move(output), output, ctx, TCoCallableRules::Instance().FinalCallables, FinalProcessedNodes_, true);
    }

    for (ui32 i = 0; i < TCoCallableRules::SIMPLE_STEPS; ++i) {
        status = DoTransform(input = std::move(output), output, ctx, TCoCallableRules::Instance().SimpleCallables[i], SimpleProcessedNodes_[i], true);
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }
    }

    for (ui32 i = 0; i < TCoCallableRules::FLOW_STEPS; ++i) {
        status = DoTransform(input = std::move(output), output, ctx, TCoCallableRules::Instance().FlowCallables[i], FlowProcessedNodes_[i], true);
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }
    }

    status = DoTransform(input = std::move(output), output, ctx, TCoCallableRules::Instance().Finalizers);
    if (status.Level != IGraphTransformer::TStatus::Ok) {
        return status;
    }

    if (TypeCtx_->LangVer >= MakeLangVersion(2025, 4)) {
        status = OptimizeBlocks(input = std::move(output), output, ctx, *TypeCtx_);
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }
    }

    if (!ScanErrors(*output, ctx)) {
        return IGraphTransformer::TStatus::Error;
    }

    return IGraphTransformer::TStatus::Ok;
}

void TCommonOptTransformer::Rewind() {
    CheckMissingWorld_ = false;
    AddedErrors_.clear();
    ErrorProcessedNodes_.clear();
    FinalProcessedNodes_.clear();

    for (auto& set : FlowProcessedNodes_) {
        set.clear();
    }

    for (auto& set : SimpleProcessedNodes_) {
        set.clear();
    }
}

bool TCommonOptTransformer::ScanErrors(const TExprNode& node, TExprContext& ctx) {
    auto [it, inserted] = ErrorProcessedNodes_.emplace(node.UniqueId());
    if (!inserted) {
        return true;
    }

    for (const auto& child : node.Children()) {
        if (!ScanErrors(*child, ctx)) {
            return false;
        }
    }

    if (!node.IsCallable("ErrorType")) {
        return true;
    }

    auto issue = node.GetTypeAnn()->Cast<TTypeExprType>()->GetType()->Cast<TErrorExprType>()->GetError();
    if (AddedErrors_.insert(issue).second) {
        ctx.AddError(issue);
    }

    return false;
}

IGraphTransformer::TStatus TCommonOptTransformer::DoTransform(
    const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx,
    const TCallableOptimizerMap& callables,
    TProcessedNodesSet& processedNodes, bool withParents)
{
    TOptimizeExprSettings settings(TypeCtx_);
    settings.ProcessedNodes = &processedNodes;
    settings.CustomInstantTypeTransformer = TypeCtx_->CustomInstantTypeTransformer.Get();
    TParentsMap parentsMap;
    TOptimizeContext optCtx;
    optCtx.Types = TypeCtx_;
    optCtx.ForPeephole = ForPeephole_;
    if (withParents) {
        GatherParents(*input, parentsMap);
        optCtx.ParentsMap = &parentsMap;
    }

    TCallableOptimizerExt defaultOpt;
    auto defaultIt = callables.find("");
    if (defaultIt != callables.end()) {
        defaultOpt = defaultIt->second;
    }

    return OptimizeExpr(input, output, [&callables, &optCtx, defaultOpt, checkMissingWorld = CheckMissingWorld_](const TExprNode::TPtr& node, TExprContext& ctx) -> TExprNode::TPtr {
        const auto rule = callables.find(node->Content());
        TExprNode::TPtr result = node;
        if (rule != callables.cend()) {
            result = (rule->second)(node, ctx, optCtx);
        }

        if (defaultOpt && result == node) {
            result = defaultOpt(node, ctx, optCtx);
        }

        if (checkMissingWorld && result && result != node && !node->GetTypeAnn()->ReturnsWorld() && !node->IsCallable(RightName)) {
            if (HasMissingWorlds(result, *node, *optCtx.Types)) {
                throw yexception() << "Missing world over " << result->Dump();
            }
        }

        return result;
    }, ctx, settings);
}

IGraphTransformer::TStatus TCommonOptTransformer::DoTransform(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx,
    const TFinalizingOptimizerMap& callables)
{
    TParentsMap parentsMap;
    TOptimizeContext optCtx;
    optCtx.Types = TypeCtx_;
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
        TOptimizeExprSettings settings(TypeCtx_);
        settings.VisitTuples = true;
        settings.CustomInstantTypeTransformer = TypeCtx_->CustomInstantTypeTransformer.Get();
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

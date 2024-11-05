#pragma once
#include <ydb/library/yql/ast/yql_expr.h>
#include "yql_graph_transformer.h"
#include "yql_type_annotation.h"
#include <util/generic/hash_set.h>
#include <functional>

namespace NYql {

typedef std::function<TExprNode::TPtr (const TExprNode::TPtr&, TExprContext&)> TCallableOptimizer;
typedef std::function<TExprNode::TPtr (const TExprNode::TPtr&, bool&, TExprContext&)> TCallableOptimizerFast;

typedef std::unordered_set<ui64> TProcessedNodesSet;

struct TOptimizeExprSettings {
    explicit TOptimizeExprSettings(TTypeAnnotationContext* types)
        : Types(types)
    {}

    bool VisitChanges = false;
    TProcessedNodesSet* ProcessedNodes = nullptr;
    bool VisitStarted = false;
    IGraphTransformer* CustomInstantTypeTransformer = nullptr;
    bool VisitLambdas = true;
    TTypeAnnotationContext* Types;
    bool VisitTuples = false;
    std::function<bool(const TExprNode&)> VisitChecker;
};

IGraphTransformer::TStatus OptimizeExpr(const TExprNode::TPtr& input, TExprNode::TPtr& output, TCallableOptimizer optimizer,
    TExprContext& ctx, const TOptimizeExprSettings& settings);

IGraphTransformer::TStatus OptimizeExpr(const TExprNode::TPtr& input, TExprNode::TPtr& output, const TCallableOptimizerFast& optimizer,
    TExprContext& ctx, const TOptimizeExprSettings& settings);

IGraphTransformer::TStatus RemapExpr(const TExprNode::TPtr& input, TExprNode::TPtr& output, const TNodeOnNodeOwnedMap& remaps,
    TExprContext& ctx, const TOptimizeExprSettings& settings);


class IOptimizationContext {
public:
    virtual ~IOptimizationContext() = default;
    virtual void RemapNode(const TExprNode& fromNode, const TExprNode::TPtr& toNode) = 0;
};

typedef std::function<TExprNode::TPtr (const TExprNode::TPtr&, TExprContext&, IOptimizationContext&)> TCallableOptimizerEx;

IGraphTransformer::TStatus OptimizeExprEx(const TExprNode::TPtr& input, TExprNode::TPtr& output, TCallableOptimizerEx optimizer,
    TExprContext& ctx, const TOptimizeExprSettings& settings);

IGraphTransformer::TStatus ExpandApply(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx);
TExprNode::TPtr ApplySyncListToWorld(const TExprNode::TPtr& main, const TSyncMap& syncList, TExprContext& ctx);

typedef std::function<bool (const TExprNode::TPtr&)> TExprVisitPtrFunc;
typedef std::function<bool (const TExprNode&)> TExprVisitRefFunc;

void VisitExpr(const TExprNode::TPtr& root, const TExprVisitPtrFunc& func);
void VisitExpr(const TExprNode::TPtr& root, const TExprVisitPtrFunc& preFunc, const TExprVisitPtrFunc& postFunc);
void VisitExpr(const TExprNode::TPtr& root, const TExprVisitPtrFunc& func, TNodeSet& visitedNodes);
void VisitExpr(const TExprNode& root, const TExprVisitRefFunc& func);
void VisitExpr(const TExprNode& root, const TExprVisitRefFunc& preFunc, const TExprVisitRefFunc& postFunc);
void VisitExprLambdasLast(const TExprNode::TPtr& root, const TExprVisitPtrFunc& preLambdaFunc, const TExprVisitPtrFunc& postLambdaFunc);


void VisitExprByFirst(const TExprNode::TPtr& root, const TExprVisitPtrFunc& func);
void VisitExprByFirst(const TExprNode::TPtr& root, const TExprVisitPtrFunc& preFunc, const TExprVisitPtrFunc& postFunc);
void VisitExprByFirst(const TExprNode::TPtr& root, const TExprVisitPtrFunc& func, TNodeSet& visitedNodes);
void VisitExprByFirst(const TExprNode& root, const TExprVisitRefFunc& func);

TExprNode::TPtr FindNode(const TExprNode::TPtr& root, const TExprVisitPtrFunc& predicate);
TExprNode::TPtr FindNode(const TExprNode::TPtr& root, const TExprVisitPtrFunc& filter, const TExprVisitPtrFunc& predicate);

TExprNode::TListType FindNodes(const TExprNode::TPtr& root, const TExprVisitPtrFunc& predicate);
TExprNode::TListType FindNodes(const TExprNode::TPtr& root, const TExprVisitPtrFunc& filter, const TExprVisitPtrFunc& predicate);

std::pair<TExprNode::TPtr, bool> FindSharedNode(const TExprNode::TPtr& firstRoot, const TExprNode::TPtr& secondRoot, const TExprVisitPtrFunc& predicate);
bool HaveSharedNodes(const TExprNode::TPtr& firstRoot, const TExprNode::TPtr& secondRoot, const TExprVisitPtrFunc& predicate);

TExprNode::TPtr CloneCompleteFlow(TExprNode::TPtr&& node, TExprContext& ctx);

}

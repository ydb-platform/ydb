#include "yql_expr_optimize.h"

#include <util/generic/hash.h>
#include "yql_expr_type_annotation.h"

namespace NYql {

namespace {
    template<typename TOptimizer>
    struct TOptimizationContext : IOptimizationContext {
        TOptimizer Optimizer;
        TExprContext& Expr;
        const TOptimizeExprSettings& Settings;
        TNodeOnNodeOwnedMap Memoization;
        const TNodeOnNodeOwnedMap* Replaces;
        ui64 LastNodeId;
        bool HasRemaps;

        TOptimizationContext(TOptimizer optimizer, const TNodeOnNodeOwnedMap* replaces, TExprContext& expr, const TOptimizeExprSettings& settings)
            : Optimizer(optimizer)
            , Expr(expr)
            , Settings(settings)
            , Replaces(replaces)
            , LastNodeId(expr.NextUniqueId)
            , HasRemaps(false)
        {
        }

        void RemapNode(const TExprNode& fromNode, const TExprNode::TPtr& toNode) final {
            YQL_ENSURE(fromNode.UniqueId() <= LastNodeId);
            YQL_ENSURE(toNode->UniqueId() > LastNodeId);
            Memoization[&fromNode] = toNode;
            HasRemaps = true;
            if (Settings.ProcessedNodes) {
                Settings.ProcessedNodes->erase(fromNode.UniqueId());
            }
        }
    };

    TExprNode::TPtr RunOptimizer(TOptimizationContext<TCallableOptimizer>& ctx, const TExprNode::TPtr& node) {
        return (!ctx.Settings.VisitChanges && ctx.HasRemaps) ? node : ctx.Optimizer(node, ctx.Expr);
    }

    TExprNode::TPtr RunOptimizer(TOptimizationContext<TCallableOptimizerEx>& ctx, const TExprNode::TPtr& node) {
        return (!ctx.Settings.VisitChanges && ctx.HasRemaps) ? node : ctx.Optimizer(node, ctx.Expr, ctx);
    }

    TExprNode::TPtr RunOptimizer(TOptimizationContext<TCallableOptimizerFast>& ctx, bool& changed, const TExprNode::TPtr& node) {
        return (!ctx.Settings.VisitChanges && ctx.HasRemaps) ? node : ctx.Optimizer(node, changed, ctx.Expr);
    }

    template<typename TContext>
    TExprNode::TPtr ApplyRemaps(const TExprNode::TPtr& node, TContext& ctx) {
        const auto memoization = ctx.Memoization.find(node.Get());
        if (ctx.Memoization.cend() != memoization && memoization->second && memoization->second != node) {
            return memoization->second;
        }

        TExprNode::TListType newChildren;
        bool hasRemaps = false;
        for (const auto& child : node->Children()) {
            auto newChild = ApplyRemaps(child, ctx);
            YQL_ENSURE(newChild);

            if (newChild != child) {
                hasRemaps = true;
            }

            newChildren.emplace_back(std::move(newChild));
        }

        return hasRemaps ? ctx.Expr.ChangeChildren(*node, std::move(newChildren)) : node;
    }

    void AddExpected(const TExprNode& src, const TExprNode& dst, TExprContext& ctx, const TOptimizeExprSettings& settings) {
        if (!src.GetTypeAnn() || !settings.Types) {
            return;
        }

        if (src.Type() == TExprNode::Argument || src.Type() == TExprNode::Arguments) {
            return;
        }

        settings.Types->ExpectedTypes[dst.UniqueId()] = src.GetTypeAnn();
        if (src.GetState() >= TExprNode::EState::ConstrComplete) {
            settings.Types->ExpectedConstraints[dst.UniqueId()] = src.GetAllConstraints();
        } else {
            settings.Types->ExpectedConstraints.erase(dst.UniqueId());
        }
        auto columnOrder = settings.Types->LookupColumnOrder(src);
        if (columnOrder) {
            settings.Types->ExpectedColumnOrders[dst.UniqueId()] = *columnOrder;
        } else {
            settings.Types->ExpectedColumnOrders.erase(dst.UniqueId());
        }

        if (dst.GetTypeAnn()) {
            // we should check expected types / constraints / column order immediately
            // TODO: check constraints
            CheckExpectedTypeAndColumnOrder(dst, ctx, *settings.Types);
        }
    }

    template<typename TContext>
    TExprNode::TPtr OptimizeNode(const TExprNode::TPtr& node, TContext& ctx, size_t level) {
        if ((!ctx.Replaces && node->Type() == TExprNode::Argument) || node->Type() == TExprNode::Atom ||
            node->Type() == TExprNode::Arguments || node->Type() == TExprNode::World) {
            return node;
        }

        if (!ctx.Settings.VisitStarted && node->StartsExecution()) {
            return node;
        }

        if (ctx.Settings.VisitChecker && !ctx.Settings.VisitChecker(*node)) {
            return node;
        }

        YQL_ENSURE(level < 3000U, "Too deep graph!");

        if (ctx.Settings.ProcessedNodes) {
            if (ctx.Settings.ProcessedNodes->find(node->UniqueId()) != ctx.Settings.ProcessedNodes->cend()) {
                return node;
            }
        }

        const auto it = ctx.Memoization.find(node.Get());
        if (it != ctx.Memoization.cend()) {
            return it->second ? it->second : node;
        }

        TExprNode::TPtr current = node;
        if (ctx.Replaces) {
            const auto it = ctx.Replaces->find(node.Get());
            if (it != ctx.Replaces->cend()) {
                current = it->second;
            }
        }

        TExprNode::TPtr ret;
        if (current->Type() == TExprNode::Lambda) {
            ret = current;

            if (ctx.Settings.VisitLambdas) {
                TExprNode::TListType newBody;
                newBody.reserve(node->ChildrenSize() - 1U);
                bool bodyChanged = false;
                for (ui32 i = 1U; i < node->ChildrenSize(); ++i) {
                    const auto& oldNode = node->ChildPtr(i);
                    auto newNode = OptimizeNode(oldNode, ctx, level + 1);
                    if (!newNode)
                        return nullptr;
                    bodyChanged = bodyChanged || newNode != oldNode;
                    if (newNode->ForDisclosing()) {
                        auto list = newNode->ChildrenList();
                        std::move(list.begin(), list.end(), std::back_inserter(newBody));
                    } else {
                        newBody.emplace_back(std::move(newNode));
                    }
                }
                if (bodyChanged) {
                    ret = ctx.Expr.DeepCopyLambda(*current, std::move(newBody));
                    AddExpected(*node, *ret, ctx.Expr, ctx.Settings);
                }
            }
        } else {
            TExprNode::TListType newChildren;
            newChildren.reserve(current->ChildrenSize());
            bool hasRenames = false;
            for (auto& child : current->Children()) {
                auto newChild = OptimizeNode(child, ctx, level + 1);
                if (!newChild)
                    return nullptr;

                if (newChild != child) {
                    hasRenames = true;
                }

                if (newChild->ForDisclosing()) {
                    auto list = newChild->ChildrenList();
                    std::move(list.begin(), list.end(), std::back_inserter(newChildren));
                } else {
                    newChildren.emplace_back(std::move(newChild));
                }
            }

            auto renamedNode = hasRenames ? ctx.Expr.ChangeChildren(*current, std::move(newChildren)) : TExprNode::TPtr();
            newChildren.clear();

            if (!ctx.Settings.VisitChanges && hasRenames && ctx.Settings.CustomInstantTypeTransformer) {
                auto root = renamedNode ? renamedNode : current;
                ctx.Settings.CustomInstantTypeTransformer->Rewind();
                auto status = InstantTransform(*ctx.Settings.CustomInstantTypeTransformer, root, ctx.Expr);
                if (status.Level == IGraphTransformer::TStatus::Error) {
                    return nullptr;
                }

                YQL_ENSURE(root->GetTypeAnn());
                if (status.HasRestart) {
                    ret = std::move(root);
                } else {
                    renamedNode = std::move(root);
                    hasRenames = false;
                }
            }

            if (!ret) {
                const auto& nextNode = renamedNode ? renamedNode : current;
                const bool visitTuples = ctx.Settings.VisitTuples && nextNode->Type() == TExprNode::List;
                if ((nextNode->Type() != TExprNode::Callable && !visitTuples) || (hasRenames && !ctx.Settings.VisitChanges) || !ctx.Optimizer) {
                    ret = nextNode;
                } else {
                    ret = RunOptimizer(ctx, nextNode);
                    if (!ret)
                        return nullptr;
                }
            }

            AddExpected(*node, *ret, ctx.Expr, ctx.Settings);
        }

        if (node == ret && ctx.Settings.ProcessedNodes) {
            ctx.Settings.ProcessedNodes->insert(node->UniqueId());
        }

        if (node == ret) {
            YQL_ENSURE(ctx.Memoization.emplace(node.Get(), TExprNode::TPtr()).second);
        } else {
            if (!node->Unique()) {
                YQL_ENSURE(ctx.Memoization.emplace(node.Get(), ret).second);
            }
            if (current != node && current != ret) {
                ctx.Memoization.emplace(current.Get(), ret);
            }
        }
        return ret;
    }

    TExprNode::TPtr OptimizeNode(const TExprNode::TPtr& node, bool& changed, TOptimizationContext<TCallableOptimizerFast>& ctx, size_t level) {
        if (node->Type() == TExprNode::Atom || node->Type() == TExprNode::Argument ||
            node->Type() == TExprNode::Arguments || node->Type() == TExprNode::World) {
            return node;
        }

        if (ctx.Settings.VisitChecker && !ctx.Settings.VisitChecker(*node)) {
            return node;
        }

        const auto it = ctx.Memoization.find(node.Get());
        if (it != ctx.Memoization.cend()) {
            changed = changed || bool(it->second);
            return it->second ? it->second : node;
        }

        YQL_ENSURE(level < 3000U, "Too deep graph!");

        TExprNode::TPtr ret;
        if (node->Type() == TExprNode::Lambda) {
            ret = node;

            if (ctx.Settings.VisitLambdas) {
                TExprNode::TListType newBody;
                newBody.reserve(node->ChildrenSize() - 1U);
                bool bodyChanged = false;
                for (ui32 i = 1U; i < node->ChildrenSize(); ++i) {
                    auto newNode = OptimizeNode(node->ChildPtr(i), bodyChanged, ctx, level + 1);
                    if (!newNode)
                        return nullptr;
                    if (newNode->ForDisclosing()) {
                        auto list = newNode->ChildrenList();
                        std::move(list.begin(), list.end(), std::back_inserter(newBody));
                    } else {
                        newBody.emplace_back(std::move(newNode));
                    }
                }

                if (bodyChanged) {
                    ret = ctx.Expr.DeepCopyLambda(*node, std::move(newBody));
                    changed = true;
                }
            }
        } else {
            TExprNode::TListType newChildren;
            newChildren.reserve(node->ChildrenSize());
            bool hasRenames = false;

            for (auto& child : node->Children()) {
                bool childChanged = false;
                auto newChild = OptimizeNode(child, childChanged, ctx, level + 1);

                if (!newChild)
                    return nullptr;

                hasRenames = hasRenames || childChanged;
                if (newChild->ForDisclosing()) {
                    auto list = newChild->ChildrenList();
                    std::move(list.begin(), list.end(), std::back_inserter(newChildren));
                } else {
                    newChildren.emplace_back(std::move(newChild));
                }
            }

            auto renamedNode = hasRenames ? ctx.Expr.ChangeChildren(*node, std::move(newChildren)) : TExprNode::TPtr();
            newChildren.clear();

            changed = changed || hasRenames;

            const auto& nextNode = renamedNode ? renamedNode : node;
            if (nextNode->Type() != TExprNode::Callable && (nextNode->Type() != TExprNode::List || !ctx.Settings.VisitTuples)) {
                ret = nextNode;
            } else {
                bool optimized = false;
                ret = RunOptimizer(ctx, optimized, nextNode);
                if (!ret)
                    return nullptr;
                changed = changed || optimized;
            }
        }

        if (!node->Unique()) {
            YQL_ENSURE(ctx.Memoization.emplace(node.Get(), ret == node ? TExprNode::TPtr() : ret).second);
        }
        return ret;
    }

    void VisitExprInternal(const TExprNode::TPtr& node, const TExprVisitPtrFunc& preFunc,
        const TExprVisitPtrFunc& postFunc, TNodeSet& visitedNodes)
    {
        if (!visitedNodes.emplace(node.Get()).second) {
            return;
        }

        if (!preFunc || preFunc(node)) {
            for (const auto& child : node->Children()) {
                VisitExprInternal(child, preFunc, postFunc, visitedNodes);
            }
        }

        if (postFunc) {
            postFunc(node);
        }
    }

    void VisitExprLambdasLastInternal(const TExprNode::TPtr& node, 
        const TExprVisitPtrFunc& preLambdaFunc,
        const TExprVisitPtrFunc& postLambdaFunc,
        TNodeSet& visitedNodes)
    {
        if (!visitedNodes.emplace(node.Get()).second) {
            return;
        }

        for (auto child : node->Children()) {
            if (!child->IsLambda()) {
                VisitExprLambdasLastInternal(child, preLambdaFunc, postLambdaFunc, visitedNodes);
            }
        }
        
        preLambdaFunc(node);
        
        for (auto child : node->Children()) {
            if (child->IsLambda()) {
                VisitExprLambdasLastInternal(child, preLambdaFunc, postLambdaFunc, visitedNodes);
            }
        }

        postLambdaFunc(node);
    }

    void VisitExprInternal(const TExprNode& node, const TExprVisitRefFunc& preFunc,
        const TExprVisitRefFunc& postFunc, TNodeSet& visitedNodes)
    {
        if (!visitedNodes.emplace(&node).second) {
            return;
        }

        if (!preFunc || preFunc(node)) {
            node.ForEachChild([&](const TExprNode& child) {
                VisitExprInternal(child, preFunc, postFunc, visitedNodes);
            });
        }

        if (postFunc) {
            postFunc(node);
        }
    }

    void VisitExprByFirstInternal(const TExprNode::TPtr& node, const TExprVisitPtrFunc& preFunc,
        const TExprVisitPtrFunc& postFunc, TNodeSet& visitedNodes)
    {
        if (!visitedNodes.emplace(node.Get()).second) {
            return;
        }

        if (!preFunc || preFunc(node)) {
            if (node->ChildrenSize() > 0) {
                if (node->Content() == SyncName) {
                    for (const auto& child : node->Children()) {
                        VisitExprByFirstInternal(child, preFunc, postFunc, visitedNodes);
                    }
                }
                else {
                    VisitExprByFirstInternal(node->HeadPtr(), preFunc, postFunc, visitedNodes);
                }
            }
        }

        if (postFunc) {
            postFunc(node);
        }
    }

    void VisitExprByFirstInternal(const TExprNode& node, const TExprVisitRefFunc& preFunc,
        const TExprVisitRefFunc& postFunc, TNodeSet& visitedNodes)
    {
        if (!visitedNodes.emplace(&node).second) {
            return;
        }

        if (!preFunc || preFunc(node)) {
            if (node.ChildrenSize() > 0) {
                if (node.Content() == SyncName) {
                    for (const auto& child : node.Children()) {
                        VisitExprByFirstInternal(*child, preFunc, postFunc, visitedNodes);
                    }
                }
                else {
                    VisitExprByFirstInternal(node.Head(), preFunc, postFunc, visitedNodes);
                }
            }
        }

        if (postFunc) {
            postFunc(node);
        }
    }

    void VisitExprByPrimaryBranch(const TExprNode::TPtr& node, const TExprVisitPtrFunc& predicate, bool& primary, TNodeSet& visitedNodes)
    {
        if (!visitedNodes.emplace(node.Get()).second) {
            return;
        }

        if (!predicate(node) || !node->ChildrenSize())
            return;

        if (node->IsCallable({"If", "IfPresent", "And", "Or", "Xor", "Coalesce"})) {
            VisitExprByPrimaryBranch(node->HeadPtr(), predicate, primary, visitedNodes);
        } else {
            for (ui32 i = 0U; i < node->ChildrenSize(); ++i)
                VisitExprByPrimaryBranch(node->ChildPtr(i), predicate, primary, visitedNodes);
            return;
        }

        primary = false;
        for (ui32 i = 1U; i < node->ChildrenSize(); ++i)
            VisitExprByPrimaryBranch(node->ChildPtr(i), predicate, primary, visitedNodes);
    }

    template<typename TOptimizer>
    IGraphTransformer::TStatus OptimizeExprInternal(TExprNode::TPtr input, TExprNode::TPtr& output, TOptimizer optimizer,
        const TNodeOnNodeOwnedMap* replaces, TExprContext& ctx, const TOptimizeExprSettings& settings) try
    {
        YQL_ENSURE(&input != &output);
        TOptimizationContext<TOptimizer> optCtx(optimizer, replaces, ctx, settings);
        output = OptimizeNode(input, optCtx, 0U);

        if (!output)
            return IGraphTransformer::TStatus::Error;

        if (optCtx.HasRemaps) {
            output = ApplyRemaps(output, optCtx);
            if (settings.ProcessedNodes) {
                settings.ProcessedNodes->clear();
            }
        }

        if (!settings.VisitChanges && (output != input)) {
            return IGraphTransformer::TStatus(IGraphTransformer::TStatus::Repeat, true);
        }

        return IGraphTransformer::TStatus::Ok;
    } catch (const std::exception& e) {
        ctx.AddError(ExceptionToIssue(e, ctx.GetPosition(input->Pos())));
        return IGraphTransformer::TStatus::Error;
    }

    IGraphTransformer::TStatus OptimizeExprInternal(TExprNode::TPtr input, TExprNode::TPtr& output, const TCallableOptimizerFast& optimizer,
        TExprContext& ctx, const TOptimizeExprSettings& settings) try
    {
        YQL_ENSURE(optimizer);
        TOptimizationContext<TCallableOptimizerFast> optCtx(optimizer, nullptr, ctx, settings);
        bool changed = false;
        output = OptimizeNode(input, changed, optCtx, 0U);

        if (!output)
            return IGraphTransformer::TStatus::Error;

        if (changed)
            return IGraphTransformer::TStatus(IGraphTransformer::TStatus::Repeat, true);

        return IGraphTransformer::TStatus::Ok;
    } catch (const std::exception& e) {
        ctx.AddError(ExceptionToIssue(e, ctx.GetPosition(input->Pos())));
        return IGraphTransformer::TStatus::Error;
    }
}

IGraphTransformer::TStatus OptimizeExpr(const TExprNode::TPtr& input, TExprNode::TPtr& output, TCallableOptimizer optimizer,
    TExprContext& ctx, const TOptimizeExprSettings& settings)
{
    return OptimizeExprInternal(input, output, optimizer, nullptr, ctx, settings);
}

IGraphTransformer::TStatus OptimizeExprEx(const TExprNode::TPtr& input, TExprNode::TPtr& output, TCallableOptimizerEx optimizer,
    TExprContext& ctx, const TOptimizeExprSettings& settings)
{
    return OptimizeExprInternal(input, output, optimizer, nullptr, ctx, settings);
}

IGraphTransformer::TStatus OptimizeExpr(const TExprNode::TPtr& input, TExprNode::TPtr& output, const TCallableOptimizerFast& optimizer,
    TExprContext& ctx, const TOptimizeExprSettings& settings)
{
    return OptimizeExprInternal(input, output, optimizer, ctx, settings);
}

IGraphTransformer::TStatus RemapExpr(const TExprNode::TPtr& input, TExprNode::TPtr& output, const TNodeOnNodeOwnedMap& remaps,
    TExprContext& ctx, const TOptimizeExprSettings& settings)
{
    return OptimizeExprInternal<TCallableOptimizer>(input, output, {}, &remaps, ctx, settings);
}

IGraphTransformer::TStatus ExpandApply(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
    if (ctx.Step.IsDone(TExprStep::ExpandApplyForLambdas))
        return IGraphTransformer::TStatus::Ok;

    TOptimizeExprSettings settings(nullptr);
    auto ret = OptimizeExpr(input, output, [&](const TExprNode::TPtr& node, bool& changed, TExprContext& ctx) -> TExprNode::TPtr {
        if (node->Content() == "WithOptionalArgs") {
            if (!EnsureArgsCount(*node, 2, ctx)) {
                return nullptr;
            }

            if (!EnsureLambda(*node->Child(0), ctx)) {
                return nullptr;
            }

            if (!EnsureAtom(*node->Child(1), ctx)) {
                return nullptr;
            }

            ui32 count = 0;
            if (!TryFromString(node->Child(1)->Content(), count)) {
                ctx.AddError(TIssue(ctx.GetPosition(node->Child(1)->Pos()),
                    TStringBuilder() << "Failed to convert to integer: " << node->Child(1)->Content()));
                return nullptr;
            }

            if (count > node->Child(0)->Child(0)->ChildrenSize()) {
                ctx.AddError(TIssue(ctx.GetPosition(node->Child(1)->Pos()),
                    TStringBuilder() << "Too many optional arguments: " << count
                    << ", lambda has only: " << node->Child(0)->Child(0)->ChildrenSize()));
                return nullptr;
            }

            return node;
        }

        if (node->Content() != "Apply" && node->Content() != "NamedApply")
            return node;

        ui32 optionalArgsCount = 0;
        auto lambdaNode = node;
        if (lambdaNode->ChildrenSize() > 0 && lambdaNode->Head().IsCallable("WithOptionalArgs")) {
            lambdaNode = lambdaNode->Child(0);
            optionalArgsCount = FromString<ui32>(lambdaNode->Child(1)->Content());
        }

        if (lambdaNode->ChildrenSize() == 0) {
            ctx.AddError(TIssue(ctx.GetPosition(lambdaNode->Pos()), "Expected at least one argument - lambda or callable"));
            return nullptr;
        }

        if (lambdaNode->Head().Type() != TExprNode::Lambda) {
            return node;
        }

        auto nullNode = ctx.NewCallable(input->Pos(), "Null", {});
        const auto& lambda = lambdaNode->Head();
        const auto& args = lambda.Head();
        TExprNode::TPtr ret;
        if (node->Content() == "Apply") {
            const ui32 maxArgs = args.ChildrenSize();
            const ui32 minArgs = args.ChildrenSize() - optionalArgsCount;
            const ui32 providedArgs = node->ChildrenSize() - 1;

            if (providedArgs < minArgs || providedArgs > maxArgs) {
                ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), TStringBuilder() << "Minimum arguments count: "
                    << minArgs << ", maximum arguments count: " << maxArgs << ", but provided " << providedArgs));
                return nullptr;
            }

            TNodeOnNodeOwnedMap replaces;
            replaces.reserve(args.ChildrenSize());
            ui32 i = 0U;
            args.ForEachChild([&](const TExprNode& arg) {
                auto value = nullNode;
                if (i < providedArgs) {
                    value = node->ChildPtr(++i);
                }

                replaces.emplace(&arg, value);
            });
            if (lambda.ChildrenSize() != 2U) {
                ret = ctx.NewList(node->Pos(), ctx.ReplaceNodes(GetLambdaBody(lambda), replaces));
                ret->SetDisclosing();
            } else {
                ret = ctx.ReplaceNodes(lambda.TailPtr(), replaces);
            }
            changed = true;
        }  else if (node->Content() == "NamedApply") {
            if (!EnsureMinArgsCount(*node, 3, ctx)) {
                return nullptr;
            }

            if (!EnsureTuple(*node->Child(1), ctx)) {
                return nullptr;
            }

            if (!EnsureCallable(*node->Child(2), ctx)) {
                return nullptr;
            }

            if (!node->Child(2)->IsCallable("AsStruct") || node->Child(2)->ChildrenSize() != 0) {
                ctx.AddError(TIssue(ctx.GetPosition(node->Child(2)->Pos()), "Expected empty AsStruct in NamedApply"));
                return nullptr;
            }

            for (ui32 i = 3; i < node->ChildrenSize(); ++i) {
                if (!EnsureCallable(*node->Child(i), ctx)) {
                    return nullptr;
                }

                if (!node->Child(i)->IsCallable("DependsOn")) {
                    ctx.AddError(TIssue(ctx.GetPosition(node->Child(i)->Pos()), "Expected DependsOn"));
                    return nullptr;
                }
            }

            const auto depArgs = node->ChildrenSize() - 3U;
            const auto totalArgs = depArgs + node->Child(1)->ChildrenSize();
            if (totalArgs < args.ChildrenSize() - optionalArgsCount) {
                ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), TStringBuilder() << "Too few arguments, lambda has "
                    << args.ChildrenSize() << " arguments with optional "<< optionalArgsCount << " arguments, but got: " << totalArgs));
                return nullptr;
            }

            ui32 posArgs = Min(args.ChildrenSize(), node->Child(1)->ChildrenSize());
            TNodeOnNodeOwnedMap replaces;
            replaces.reserve(args.ChildrenSize());
            for (ui32 i = 0; i < posArgs; ++i) {
                replaces.emplace(args.Child(i), node->Child(1)->ChildPtr(i));
            }

            if (posArgs == node->Child(1)->ChildrenSize()) {
                for (ui32 i = 3; i < node->ChildrenSize(); ++i) {
                    auto index = i - 3 + node->Child(1)->ChildrenSize();
                    if (index >= args.ChildrenSize()) {
                        break;
                    }

                    replaces.emplace(args.Child(index), node->ChildPtr(i));
                }
            }

            for (ui32 i = replaces.size(); i < args.ChildrenSize(); ++i) {
                replaces.emplace(args.Child(i), nullNode);
            }

            if (lambda.ChildrenSize() > 2U) {
                ret = ctx.NewList(node->Pos(), ctx.ReplaceNodes(GetLambdaBody(lambda), replaces));
                ret->SetDisclosing();
            } else {
                ret = ctx.ReplaceNodes(lambda.TailPtr(), replaces);
            }
            changed = true;
        }

        return ret;
    }, ctx, settings);

    if (ret.Level == IGraphTransformer::TStatus::Ok) {
        ret = ret.Combine(OptimizeExpr(output, output, [&](const TExprNode::TPtr& node, bool& changed, TExprContext& ctx) -> TExprNode::TPtr {
            if (node->Content() == "Combine") {
                if (!EnsureMinArgsCount(*node, 1, ctx)) {
                    return nullptr;
                }

                ui32 flags = 0U;
                TString content;
                for (const auto& child : node->Children()) {
                    if (!EnsureAtom(*child, ctx)) {
                        return nullptr;
                    }

                    const auto f = child->Flags();

                    if ((TNodeFlags::MultilineContent | TNodeFlags::BinaryContent) & f) {
                        ctx.AddError(TIssue(ctx.GetPosition(child->Pos()), "Can't combine binary or multiline atoms."));
                        return nullptr;
                    }

                    content += child->Content();
                    flags |= f;
                }

                changed = true;
                return ctx.NewAtom(node->Pos(), content, flags);
            } else if (node->Content() == "Nth") {
                if (!EnsureArgsCount(*node, 2, ctx)) {
                    return nullptr;
                }

                if (!node->Tail().IsAtom()) {
                    return node;
                }

                if (node->Head().Type() != TExprNode::List) {
                    return node;
                }

                ui32 index = 0U;
                if (const auto& indexValue = node->Tail().Content(); !TryFromString(indexValue, index)) {
                    ctx.AddError(TIssue(ctx.GetPosition(node->Child(1)->Pos()), TStringBuilder() << "Index '" << indexValue << "' isn't UI32."));
                    return nullptr;
                }

                if (const auto size = node->Head().ChildrenSize(); size <= index) {
                    ctx.AddError(TIssue(ctx.GetPosition(node->Tail().Pos()), TStringBuilder() << "Index too large: (" << index << " >= " << size << ")."));
                    return nullptr;
                }

                changed = true;
                return node->Head().ChildPtr(index);
            } else if (node->Content() == "NthArg") {
                if (!EnsureArgsCount(*node, 2, ctx)) {
                    return nullptr;
                }

                if (!EnsureAtom(node->Head(), ctx)) {
                    return nullptr;
                }

                if (!EnsureCallable(node->Tail(), ctx)) {
                    return nullptr;
                }

                if (node->Tail().IsCallable("Apply")) {
                    return node;
                }

                ui32 index = 0U;
                if (const auto& indexValue = node->Head().Content(); !TryFromString(indexValue, index)) {
                    ctx.AddError(TIssue(ctx.GetPosition(node->Head().Pos()), TStringBuilder() << "Index '" << indexValue << "' isn't UI32."));
                    return nullptr;
                }

                if (const auto size = node->Tail().ChildrenSize(); size <= index) {
                    ctx.AddError(TIssue(ctx.GetPosition(node->Head().Pos()), TStringBuilder() << "Index too large: (" << index << " >= " << size << ")."));
                    return nullptr;
                }

                changed = true;
                return node->Tail().ChildPtr(index);
            }
            else if (node->Content() == "Seq!") {
                if (!EnsureMinArgsCount(*node, 1, ctx)) {
                    return nullptr;
                }

                auto world = node->HeadPtr();
                for (ui32 i = 1; i < node->ChildrenSize(); ++i) {
                    const auto lambda = node->Child(i);
                    if (!lambda->IsLambda()) {
                        return node;
                    }

                    const auto& lambdaArgs = lambda->Head();
                    if (!EnsureArgsCount(lambdaArgs, 1, ctx)) {
                        return nullptr;
                    }

                    world = ctx.ReplaceNode(lambda->TailPtr(), lambdaArgs.Head(), std::move(world));
                }

                changed = true;
                return world;
            } else if (node->Content() == "SubqueryExtend" || node->Content() == "SubqueryUnionAll" ||
                node->Content() == "SubqueryMerge" || node->Content() == "SubqueryUnionMerge") {
                if (!EnsureMinArgsCount(*node, 1, ctx)) {
                    return nullptr;
                }

                TMaybe<ui32> commonArgs;
                for (ui32 i = 0; i < node->ChildrenSize(); ++i) {
                    const auto lambda = node->Child(i);
                    if (!lambda->IsLambda()) {
                        return node;
                    }

                    const auto& lambdaArgs = lambda->Head();
                    if (!EnsureMinArgsCount(lambdaArgs, 1, ctx)) {
                        return nullptr;
                    }

                    if (!commonArgs) {
                        commonArgs = lambdaArgs.ChildrenSize();
                    } else if (*commonArgs != lambdaArgs.ChildrenSize()) {
                        ctx.AddError(TIssue(ctx.GetPosition(lambda->Pos()),
                            TStringBuilder() << "Mismatch of arguments count in subquery, got: "
                                << (lambdaArgs.ChildrenSize() - 1) << ", but expected: " << (*commonArgs - 1)));
                        return nullptr;
                    }
                }

                TExprNodeList argItems;
                for (ui32 i = 0; i < *commonArgs; ++i) {
                    argItems.push_back(ctx.NewArgument(node->Pos(), "arg" + ToString(i)));
                }

                TExprNodeList inputs;
                for (ui32 i = 0; i < node->ChildrenSize(); ++i) {
                    const auto lambda = node->Child(i);
                    const auto& lambdaArgs = lambda->Head();
                    TNodeOnNodeOwnedMap replaces;
                    for (ui32 j = 0; j < argItems.size(); ++j) {
                        replaces[lambdaArgs.Child(j)] = argItems[j];
                    }

                    inputs.push_back(ctx.ReplaceNodes(lambda->TailPtr(), replaces));
                }

                auto body = ctx.NewCallable(node->Pos(), node->Content().substr(8), std::move(inputs) );
                auto args = ctx.NewArguments(node->Pos(), std::move(argItems));
                auto merged = ctx.NewLambda(node->Pos(), std::move(args), std::move(body));
                changed = true;
                return merged;
            } else {
                return node;
            }
        }, ctx, settings));
    }

    if (ret.Level == IGraphTransformer::TStatus::Ok) {
        ctx.Step.Done(TExprStep::ExpandApplyForLambdas);
    }

    return ret;
}

TExprNode::TPtr ApplySyncListToWorld(const TExprNode::TPtr& main, const TSyncMap& syncList, TExprContext& ctx) {
    if (syncList.empty()) {
        return main;
    }

    using TPair = std::pair<TExprNode::TPtr, ui64>;
    TVector<TPair> sortedList(syncList.cbegin(), syncList.cend());
    TExprNode::TListType syncChildren;
    syncChildren.push_back(main);
    Sort(sortedList, [](const TPair& x, const TPair& y) { return x.second < y.second; });
    for (auto x : sortedList) {
        if (x.first->IsCallable(RightName)) {
            auto world = ctx.NewCallable(main->Pos(), LeftName, { x.first->HeadPtr() });
            syncChildren.push_back(world);
        } else if (x.first->GetTypeAnn()->GetKind() == ETypeAnnotationKind::World) {
            syncChildren.push_back(x.first);
        } else {
            auto world = ctx.NewCallable(main->Pos(), LeftName, { x.first });
            syncChildren.push_back(world);
        }
    }

    return ctx.NewCallable(main->Pos(), SyncName, std::move(syncChildren));
}

void VisitExpr(const TExprNode::TPtr& root, const TExprVisitPtrFunc& func) {
    TNodeSet visitedNodes;
    VisitExprInternal(root, func, {}, visitedNodes);
}

void VisitExpr(const TExprNode::TPtr& root, const TExprVisitPtrFunc& preFunc, const TExprVisitPtrFunc& postFunc) {
    TNodeSet visitedNodes;
    VisitExprInternal(root, preFunc, postFunc, visitedNodes);
}

void VisitExpr(const TExprNode& root, const TExprVisitRefFunc& func) {
    TNodeSet visitedNodes;
    VisitExprInternal(root, func, {}, visitedNodes);
}

void VisitExpr(const TExprNode& root, const TExprVisitRefFunc& preFunc, const TExprVisitRefFunc& postFunc) {
    TNodeSet visitedNodes;
    VisitExprInternal(root, preFunc, postFunc, visitedNodes);
}

void VisitExpr(const TExprNode::TPtr& root, const TExprVisitPtrFunc& func, TNodeSet& visitedNodes) {
    VisitExprInternal(root, func, {}, visitedNodes);
}
    
void VisitExprLambdasLast(const TExprNode::TPtr& root, const TExprVisitPtrFunc& preLambdaFunc, const TExprVisitPtrFunc& postLambdaFunc)
{
    TNodeSet visitedNodes;
    VisitExprLambdasLastInternal(root, preLambdaFunc, postLambdaFunc, visitedNodes);
}

void VisitExprByFirst(const TExprNode::TPtr& root, const TExprVisitPtrFunc& func) {
    TNodeSet visitedNodes;
    VisitExprByFirstInternal(root, func, {}, visitedNodes);
}

void VisitExprByFirst(const TExprNode::TPtr& root, const TExprVisitPtrFunc& preFunc, const TExprVisitPtrFunc& postFunc) {
    TNodeSet visitedNodes;
    VisitExprByFirstInternal(root, preFunc, postFunc, visitedNodes);
}

void VisitExprByFirst(const TExprNode& root, const TExprVisitRefFunc& func) {
    TNodeSet visitedNodes;
    VisitExprByFirstInternal(root, func, {}, visitedNodes);
}

void VisitExprByFirst(const TExprNode::TPtr& root, const TExprVisitPtrFunc& func, TNodeSet& visitedNodes) {
    VisitExprByFirstInternal(root, func, {}, visitedNodes);
}

TExprNode::TPtr FindNode(const TExprNode::TPtr& root, const TExprVisitPtrFunc& predicate) {
    TExprNode::TPtr result;
    VisitExpr(root, [&result, &predicate] (const TExprNode::TPtr& node) {
        if (result)
            return false;

        if (predicate(node)) {
            result = node;
            return false;
        }

        return true;
    });

    return result;
}

TExprNode::TPtr FindNode(const TExprNode::TPtr& root, const TExprVisitPtrFunc& filter, const TExprVisitPtrFunc& predicate) {
    TExprNode::TPtr result;
    VisitExpr(root, filter, [&result, &predicate] (const TExprNode::TPtr& node) {
        if (result)
            return false;

        if (predicate(node)) {
            result = node;
            return false;
        }

        return true;
    });

    return result;
}

TExprNode::TListType FindNodes(const TExprNode::TPtr& root, const TExprVisitPtrFunc& predicate) {
    TExprNode::TListType result;
    VisitExpr(root, [&result, &predicate] (const TExprNode::TPtr& node) {
        if (predicate(node)) {
            result.emplace_back(node);
        }

        return true;
    });

    return result;
}

TExprNode::TListType FindNodes(const TExprNode::TPtr& root, const TExprVisitPtrFunc& filter, const TExprVisitPtrFunc& predicate) {
    TExprNode::TListType result;
    VisitExpr(root, filter, [&result, &predicate] (const TExprNode::TPtr& node) {
        if (predicate(node)) {
            result.emplace_back(node);
        }

        return true;
    });

    return result;
}

std::pair<TExprNode::TPtr, bool> FindSharedNode(const TExprNode::TPtr& firstRoot, const TExprNode::TPtr& secondRoot, const TExprVisitPtrFunc& predicate)
{
    TNodeSet nodes, visited;
    VisitExpr(firstRoot, [&nodes, &predicate] (const TExprNode::TPtr& node) {
        if (predicate(node)) {
            nodes.insert(node.Get());
        }

        return true;
    });

    TExprNode::TPtr result;
    bool primary = true;
    VisitExprByPrimaryBranch(secondRoot, [&nodes, &result] (const TExprNode::TPtr& node) {
        if (result)
            return false;

        if (nodes.find(node.Get()) != nodes.end()) {
            result = node;
            return false;
        }

        return true;
    }, primary, visited);

    return std::make_pair(std::move(result), primary);
}

bool HaveSharedNodes(const TExprNode::TPtr& firstRoot, const TExprNode::TPtr& secondRoot, const TExprVisitPtrFunc& predicate)
{
    return bool(FindSharedNode(firstRoot, secondRoot, predicate).first);
}

TExprNode::TPtr CloneCompleteFlow(TExprNode::TPtr&& node, TExprContext& ctx) {
    const TExprNode* original = nullptr;
    TExprNode::TPtr copy;
    VisitExpr(*node, [&](const TExprNode& child) {
        if (const auto kind = child.GetTypeAnn()->GetKind(); (ETypeAnnotationKind::Stream == kind || ETypeAnnotationKind::Flow == kind) && child.IsComplete() && child.IsCallable() && original != &child) {
            original = &child;
            copy = ctx.ShallowCopy(child);
            return true;
        }
        return false;
    });
    return original ? ctx.ReplaceNode(std::move(node), *original, std::move(copy)) : std::move(node);
}

}

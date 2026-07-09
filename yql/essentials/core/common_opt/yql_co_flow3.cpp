#include "yql_co.h"

#include <yql/essentials/core/yql_join.h>
#include <yql/essentials/core/yql_opt_utils.h>

#include <yql/essentials/utils/log/log.h>

namespace NYql {
namespace {

using namespace NNodes;

TExprNode::TPtr EquiJoinPushAny(const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx) {
    // Add any to EquiJoin tree
    if (!IsEmitPruneKeysEnabled(optCtx.Types)) {
        return node;
    }

    auto equiJoin = TCoEquiJoin(node);
    auto equiJoinTree = equiJoin.Arg(equiJoin.ArgCount() - 2).Cast<TCoEquiJoinTuple>();

    if (auto result = PushAnyInEquiJoin(ctx, equiJoinTree); result != equiJoinTree.Ptr()) {
        return ctx.ChangeChild(*node, equiJoin.ArgCount() - 2, std::move(result));
    }

    return node;
}

TExprNode::TPtr EquiJoinEmitPruneKeys(const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx) {
    // Add PruneKeys to EquiJoin inputs
    if (!IsEmitPruneKeysEnabled(optCtx.Types)) {
        return node;
    }
    auto equiJoin = TCoEquiJoin(node);
    if (HasSetting(equiJoin.Arg(equiJoin.ArgCount() - 1).Ref(), "prune_keys_added")) {
        return node;
    }

    THashMap<TStringBuf, THashSet<TStringBuf>> columnsForPruneKeysExtractor;
    GetPruneKeysColumnsForJoinLeaves(equiJoin.Arg(equiJoin.ArgCount() - 2).Cast<TCoEquiJoinTuple>(), columnsForPruneKeysExtractor);

    TExprNode::TListType children;
    bool hasChanges = false;
    for (size_t i = 0; i + 2 < equiJoin.ArgCount(); ++i) {
        auto child = equiJoin.Arg(i).Cast<TCoEquiJoinInput>();
        auto list = child.List();
        auto scope = child.Scope();

        if (!scope.Ref().IsAtom()) {
            children.push_back(equiJoin.Arg(i).Ptr());
            continue;
        }

        THashSet<TString> columns;
        auto itemNames = columnsForPruneKeysExtractor.find(scope.Ref().Content());
        if (itemNames == columnsForPruneKeysExtractor.end() || itemNames->second.empty()) {
            children.push_back(equiJoin.Arg(i).Ptr());
            continue;
        }
        for (const auto& elem : itemNames->second) {
            columns.insert(TString(elem));
        }

        if (IsAlreadyDistinct(list.Ref(), columns)) {
            children.push_back(equiJoin.Arg(i).Ptr());
            continue;
        }
        auto pruneKeysCallable = IsOrdered(list.Ref(), columns) ? "PruneAdjacentKeys" : "PruneKeys";
        YQL_CLOG(DEBUG, Core) << "Add " << pruneKeysCallable << " to EquiJoin input #" << i << ", label " << scope.Ref().Content();
        children.push_back(ctx.Builder(child.Pos())
            .List()
                .Callable(0, pruneKeysCallable)
                    .Add(0, list.Ptr())
                    .Add(1, MakePruneKeysExtractorLambda(child.Ref(), columns, ctx))
                .Seal()
                .Add(1, scope.Ptr())
            .Seal()
            .Build());
        hasChanges = true;
    }

    if (!hasChanges) {
        return node;
    }

    children.push_back(equiJoin.Arg(equiJoin.ArgCount() - 2).Ptr());
    children.push_back(AddSetting(
        equiJoin.Arg(equiJoin.ArgCount() - 1).Ref(),
        equiJoin.Arg(equiJoin.ArgCount() - 1).Pos(),
        "prune_keys_added",
        nullptr,
        ctx));
    return ctx.ChangeChildren(*node, std::move(children));
}

} // namespace

void RegisterCoFlowCallables3(TCallableOptimizerMap& map) {
    using namespace std::placeholders;
    map["EquiJoin"] = [](const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx) {
        if (auto ret = EquiJoinPushAny(node, ctx, optCtx); ret != node) {
            YQL_CLOG(DEBUG, Core) << "EquiJoinPushAny";
            return ret;
        }

        if (auto ret = EquiJoinEmitPruneKeys(node, ctx, optCtx); ret != node) {
            YQL_CLOG(DEBUG, Core) << "EquiJoinEmitPruneKeys";
            return ret;
        }

        return node;
    };

}

}

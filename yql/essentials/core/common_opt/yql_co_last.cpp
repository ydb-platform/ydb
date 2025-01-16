#include "yql_co.h"
#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/utils/log/log.h>

namespace NYql {

namespace {

using namespace NNodes;

std::unordered_set<ui32> GetUselessSortedJoinInputs(const TCoEquiJoin& equiJoin) {
    std::unordered_map<std::string_view, std::tuple<ui32, const TSortedConstraintNode*, const TChoppedConstraintNode*>> sorteds(equiJoin.ArgCount() - 2U);
    for (ui32 i = 0U; i + 2U < equiJoin.ArgCount(); ++i) {
        if (const auto joinInput = equiJoin.Arg(i).Cast<TCoEquiJoinInput>(); joinInput.Scope().Ref().IsAtom()) {
            const auto sorted = joinInput.List().Ref().GetConstraint<TSortedConstraintNode>();
            const auto chopped = joinInput.List().Ref().GetConstraint<TChoppedConstraintNode>();
            if (sorted || chopped)
                sorteds.emplace(joinInput.Scope().Ref().Content(), std::make_tuple(i, sorted, chopped));
        }
    }

    for (std::vector<const TExprNode*> joinTreeNodes(1U, equiJoin.Arg(equiJoin.ArgCount() - 2).Raw()); !joinTreeNodes.empty();) {
        const auto joinTree = joinTreeNodes.back();
        joinTreeNodes.pop_back();

        if (!joinTree->Child(1)->IsAtom())
            joinTreeNodes.emplace_back(joinTree->Child(1));

        if (!joinTree->Child(2)->IsAtom())
            joinTreeNodes.emplace_back(joinTree->Child(2));

        if (!joinTree->Head().IsAtom("Cross")) {
            std::unordered_map<std::string_view, TPartOfConstraintBase::TSetType> tableJoinKeys;
            for (const auto keys : {joinTree->Child(3), joinTree->Child(4)})
                for (ui32 i = 0U; i < keys->ChildrenSize(); i += 2)
                    tableJoinKeys[keys->Child(i)->Content()].insert_unique(TPartOfConstraintBase::TPathType(1U, keys->Child(i + 1)->Content()));

            for (const auto& [label, joinKeys]: tableJoinKeys) {
                if (const auto it = sorteds.find(label); sorteds.cend() != it) {
                    const auto sorted = std::get<const TSortedConstraintNode*>(it->second);
                    const auto chopped = std::get<const TChoppedConstraintNode*>(it->second);
                    if (sorted && sorted->StartsWith(joinKeys) || chopped && chopped->Equals(joinKeys))
                        sorteds.erase(it);
                }
            }
        }
    }

    std::unordered_set<ui32> result(sorteds.size());
    for (const auto& sort : sorteds)
        result.emplace(std::get<ui32>(sort.second));
    return result;
}

} // namespace

void RegisterCoFinalCallables(TCallableOptimizerMap& map) {
    map["UnorderedSubquery"] = [](const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx) {
        Y_UNUSED(optCtx);
        if (node->Head().IsCallable("Sort")) {
            if (!WarnUnroderedSubquery(*node, ctx)) {
                return TExprNode::TPtr();
            }
        }
        YQL_CLOG(DEBUG, Core) << "Replace " << node->Content() << " with Unordered";
        return ctx.RenameNode(*node, "Unordered");
    };

    map["EquiJoin"] = [](const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& /*optCtx*/) {
        if (const auto indexes = GetUselessSortedJoinInputs(TCoEquiJoin(node)); !indexes.empty()) {
            YQL_CLOG(DEBUG, Core) << "Suppress order on " << indexes.size() << ' ' << node->Content() << " inputs";
            auto children = node->ChildrenList();
            for (const auto idx : indexes)
                children[idx] = ctx.Builder(children[idx]->Pos())
                    .List()
                        .Callable(0, "Unordered")
                            .Add(0, children[idx]->HeadPtr())
                        .Seal()
                        .Add(1, children[idx]->TailPtr())
                    .Seal().Build();
            return ctx.ChangeChildren(*node, std::move(children));
        }
        return node;
    };
}

}

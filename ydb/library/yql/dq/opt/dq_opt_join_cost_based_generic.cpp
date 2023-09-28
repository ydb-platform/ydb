#include "dq_opt_join.h"
#include "dq_opt_phy.h"

#include <ydb/library/yql/core/cbo/cbo_optimizer.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/core/yql_opt_utils.h>

namespace NYql::NDq {
    
using namespace NYql::NNodes;

namespace {

struct TState {
	IOptimizer::TInput Input;
    IOptimizer::TOutput Result;
    std::vector<TStringBuf> Tables; // relId -> table
    std::vector<THashMap<TStringBuf, int>> VarIds; // relId -> varsIds
    THashMap<TStringBuf, std::vector<int>> Table2RelIds;
    std::vector<std::vector<std::tuple<TStringBuf, TStringBuf>>> Var2TableCol; // relId, varId -> table, col
    TPositionHandle Pos;

    TState(const TCoEquiJoin& join)
        : Pos(join.Pos())
    { }

    int GetVarId(int relId, TStringBuf column) {
        int varId = 0;
        auto maybeVarId = VarIds[relId-1].find(column);
        if (maybeVarId != VarIds[relId-1].end()) {
            varId = maybeVarId->second;
        } else {
            varId = Input.Rels[relId - 1].TargetVars.size() + 1;
            VarIds[relId - 1][column] = varId;
            Input.Rels[relId - 1].TargetVars.emplace_back();
            Var2TableCol[relId - 1].emplace_back();
        }
        return varId;
    }

    void CollectRel(TStringBuf label, auto stat) {
        Input.Rels.emplace_back();
        Var2TableCol.emplace_back();
        VarIds.emplace_back();
        int relId = Input.Rels.size();
        Input.Rels.back().Rows = stat->Nrows;
        Input.Rels.back().TotalCost = *stat->Cost;
        Tables.emplace_back(label);
        Table2RelIds[label].emplace_back(relId);
    }

    void CollectJoins(TExprNode::TPtr joinTuple) {
        // joinTuple->Child(0)->Content(); // type
        auto leftScope = joinTuple->Child(1);
        auto rightScope = joinTuple->Child(2);
        auto leftKeys = joinTuple->Child(3);
        auto rightKeys = joinTuple->Child(4);

        YQL_ENSURE(leftKeys->ChildrenSize() == rightKeys->ChildrenSize());
        for (ui32 i = 0; i < leftKeys->ChildrenSize(); i += 2) {
            auto ltable = leftKeys->Child(i)->Content();
            auto lcolumn = leftKeys->Child(i + 1)->Content();
            auto rtable = rightKeys->Child(i)->Content();
            auto rcolumn = rightKeys->Child(i + 1)->Content();

            size_t nclasses = Input.EqClasses.size();
            for (auto lrelId : Table2RelIds[ltable]) {
                for (auto rrelId : Table2RelIds[rtable]) {
                    auto lvarId = GetVarId(lrelId, lcolumn);
                    auto rvarId = GetVarId(rrelId, rcolumn);

                    Var2TableCol[lrelId - 1][lvarId - 1] = std::make_tuple(ltable, lcolumn);
                    Var2TableCol[rrelId - 1][rvarId - 1] = std::make_tuple(rtable, rcolumn);

                    IOptimizer::TEq eqClass; eqClass.Vars.reserve(2);
                    eqClass.Vars.emplace_back(std::make_tuple(lrelId, lvarId));
                    eqClass.Vars.emplace_back(std::make_tuple(rrelId, rvarId));

                    Input.EqClasses.emplace_back(std::move(eqClass));
                }
            }

            YQL_ENSURE(nclasses != Input.EqClasses.size());
        }

        if (TMaybeNode<TCoEquiJoinTuple>(leftScope)) {
            CollectJoins(leftScope);
        }
        if (TMaybeNode<TCoEquiJoinTuple>(rightScope)) {
            CollectJoins(rightScope);
        }
    }

    auto MakeLabel(TExprContext& ctx, const std::vector<IOptimizer::TVarId>& vars) const {
        TVector<TExprNodePtr> label; label.reserve(vars.size() * 2);

        for (auto [relId, varId] : vars) {
            auto [table, column] = Var2TableCol[relId - 1][varId - 1];
    
            label.emplace_back(ctx.NewAtom(Pos, table));
            label.emplace_back(ctx.NewAtom(Pos, column));
        }
    
        return label;
    }

    TExprBase BuildTree(TExprContext& ctx, int nodeId) {
        const IOptimizer::TJoinNode* node = &Result.Nodes[nodeId];
        if (node->Outer == -1 && node->Inner == -1) {
            // leaf
            YQL_ENSURE(node->Rels.size() == 1);
            auto scope = Tables[node->Rels[0]-1];
            return BuildAtom(scope, Pos, ctx);
        } else if (node->Outer != -1 && node->Inner != -1) {
			TString joinKind;
            switch (node->Mode) {
            case IOptimizer::EJoinType::Inner:
                joinKind = "Inner";
                break;
            case IOptimizer::EJoinType::Left:
                joinKind = "Left";
                break;
            case IOptimizer::EJoinType::Right:
                joinKind = "Right";
                break;
            default:
                YQL_ENSURE(false, "Unsupported join type");
                break;
            }

            TVector<TExprBase> options;
            return Build<TCoEquiJoinTuple>(ctx, Pos)
                .Type(BuildAtom(joinKind, Pos, ctx))
                .LeftScope(BuildTree(ctx, node->Outer))
                .RightScope(BuildTree(ctx, node->Inner))
                .LeftKeys()
                    .Add(MakeLabel(ctx, node->LeftVars))
                    .Build()
                .RightKeys()
                    .Add(MakeLabel(ctx, node->RightVars))
                    .Build()
                .Options()
                    .Add(options)
                    .Build()
                .Done();
        } else {
            YQL_ENSURE(false, "Wrong CBO node");
        }
    }
};

} // namespace

TExprBase DqOptimizeEquiJoinWithCosts(
	const TExprBase& node, 
	TExprContext& ctx, 
	TTypeAnnotationContext& typesCtx,
	const std::function<IOptimizer*(IOptimizer::TInput&&)>& optFactory,
    bool ruleEnabled)
{
    Y_UNUSED(ctx);

    if (!ruleEnabled) {
        return node;
    }

    if (!node.Maybe<TCoEquiJoin>()) {
        return node;
    }
    auto equiJoin = node.Cast<TCoEquiJoin>();
    YQL_ENSURE(equiJoin.ArgCount() >= 4);

	auto maybeStat = typesCtx.StatisticsMap.find(equiJoin.Raw());
    if (maybeStat != typesCtx.StatisticsMap.end() &&
        maybeStat->second->Cost.has_value()) {
        return node;
    }

    if (! HasOnlyOneJoinType(*equiJoin.Arg(equiJoin.ArgCount() - 2).Ptr(), "Inner")) {
        return node;
    }

    YQL_CLOG(TRACE, CoreDq) << "Optimizing join with costs";

    TState state(equiJoin);
    // collect Rels
    if (!DqCollectJoinRelationsWithStats(typesCtx, equiJoin, [&](auto label, auto stat) {
        state.CollectRel(label, stat);
    })) {
        return node;
    }

    int cols = 0;
    state.CollectJoins(equiJoin.Arg(equiJoin.ArgCount() - 2).Ptr());
    for (auto& rel : state.Input.Rels) {
        cols += rel.TargetVars.size();
    }
    state.Input.Normalize();
    std::unique_ptr<IOptimizer> opt = std::unique_ptr<IOptimizer>(optFactory(std::move(state.Input)));
    state.Result = opt->JoinSearch();

    TVector<TExprBase> joinArgs;
    for (size_t i = 0; i < equiJoin.ArgCount() - 2; i++) {
        joinArgs.push_back(equiJoin.Arg(i));
    }

    joinArgs.push_back(state.BuildTree(ctx, 0));
    joinArgs.push_back(equiJoin.Arg(equiJoin.ArgCount() - 1));

    auto res = Build<TCoEquiJoin>(ctx, equiJoin.Pos())
        .Add(joinArgs)
        .Done();


    typesCtx.StatisticsMap[res.Raw()] = std::make_shared<TOptimizerStatistics>(state.Result.Rows, cols, state.Result.TotalCost);

    return res;
}

} // namespace NYql::NDq


#include "dphyp_opt_join_cost_based.h"
#include "dphyp_dphyp_solver.h"
#include "dphyp_make_join_hypergraph.h"

#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/dq/opt/dq_opt.h>
#include <ydb/library/yql/utils/log/log.h>

namespace NYql::NDq::NDphyp {

using namespace NYql::NNodes;

/**
 * Collects EquiJoin inputs with statistics for cost based optimization
*/
bool DqCollectJoinRelationsWithStats(
    TVector<std::shared_ptr<TRelOptimizerNode>>& rels,
    TTypeAnnotationContext& typesCtx,
    const TCoEquiJoin& equiJoin,
    const TProviderCollectFunction& collector
) {
    if (equiJoin.ArgCount() < 3) {
        return false;
    }

    for (size_t i = 0; i < equiJoin.ArgCount() - 2; ++i) {
        auto input = equiJoin.Arg(i).Cast<TCoEquiJoinInput>();
        auto joinArg = input.List();

        auto maybeStat = typesCtx.StatisticsMap.find(joinArg.Raw());

        if (maybeStat == typesCtx.StatisticsMap.end()) {
            YQL_CLOG(TRACE, CoreDq) << "Didn't find statistics for scope " << input.Scope().Cast<TCoAtom>().StringValue() << "\n";
            return false;
        }

        auto scope = input.Scope();
        if (!scope.Maybe<TCoAtom>()){
            return false;
        }

        TStringBuf label = scope.Cast<TCoAtom>();
        auto stats = maybeStat->second;
        collector(rels, label, joinArg.Ptr(), stats);
    }
    return true;
}

/**
 * Convert JoinTuple from AST into an internal representation of a optimizer plan
 * This procedure also hooks up rels with statistics to the leaf nodes of the plan
 * Statistics for join nodes are not computed
*/
std::shared_ptr<TJoinOptimizerNode> ConvertToJoinTree(
    const TCoEquiJoinTuple& joinTuple,
    const TVector<std::shared_ptr<TRelOptimizerNode>>& rels
) {

    std::shared_ptr<IBaseOptimizerNode> left;
    std::shared_ptr<IBaseOptimizerNode> right;


    if (joinTuple.LeftScope().Maybe<TCoEquiJoinTuple>()) {
        left = ConvertToJoinTree(joinTuple.LeftScope().Cast<TCoEquiJoinTuple>(), rels);
    }
    else {
        auto scope = joinTuple.LeftScope().Cast<TCoAtom>().StringValue();
        auto it = find_if(rels.begin(), rels.end(), [scope] (const std::shared_ptr<TRelOptimizerNode>& n) {
            return scope == n->Label;
        } );
        left = *it;
    }

    if (joinTuple.RightScope().Maybe<TCoEquiJoinTuple>()) {
        right = ConvertToJoinTree(joinTuple.RightScope().Cast<TCoEquiJoinTuple>(), rels);
    }
    else {
        auto scope = joinTuple.RightScope().Cast<TCoAtom>().StringValue();
        auto it = find_if(rels.begin(), rels.end(), [scope] (const std::shared_ptr<TRelOptimizerNode>& n) {
            return scope == n->Label;
        } );
        right =  *it;
    }

    std::set<std::pair<TJoinColumn, TJoinColumn>> joinConds;

    size_t joinKeysCount = joinTuple.LeftKeys().Size() / 2;
    for (size_t i = 0; i < joinKeysCount; ++i) {
        size_t keyIndex = i * 2;

        auto leftScope = joinTuple.LeftKeys().Item(keyIndex).StringValue();
        auto leftColumn = joinTuple.LeftKeys().Item(keyIndex + 1).StringValue();
        auto rightScope = joinTuple.RightKeys().Item(keyIndex).StringValue();
        auto rightColumn = joinTuple.RightKeys().Item(keyIndex + 1).StringValue();

        joinConds.insert( std::make_pair( TJoinColumn(leftScope, leftColumn),
            TJoinColumn(rightScope, rightColumn)));
    }

    return std::make_shared<TJoinOptimizerNode>(left, right, joinConds, ConvertToJoinKind(joinTuple.Type().StringValue()), EJoinAlgoType::Undefined);
}

/**
 * Build a join tree that will replace the original join tree in equiJoin
 * TODO: Add join implementations here
*/
TExprBase BuildTree(TExprContext& ctx, const TCoEquiJoin& equiJoin,
    std::shared_ptr<TJoinOptimizerNode>& reorderResult) {

    // Create dummy left and right arg that will be overwritten
    TExprBase leftArg(equiJoin);
    TExprBase rightArg(equiJoin);

    // Build left argument of the join
    if (reorderResult->LeftArg->Kind == RelNodeType) {
        std::shared_ptr<TRelOptimizerNode> rel =
            std::static_pointer_cast<TRelOptimizerNode>(reorderResult->LeftArg);
        leftArg = BuildAtom(rel->Label, equiJoin.Pos(), ctx);
    } else {
        std::shared_ptr<TJoinOptimizerNode> join =
            std::static_pointer_cast<TJoinOptimizerNode>(reorderResult->LeftArg);
        leftArg = BuildTree(ctx,equiJoin,join);
    }
    // Build right argument of the join
    if (reorderResult->RightArg->Kind == RelNodeType) {
        std::shared_ptr<TRelOptimizerNode> rel =
            std::static_pointer_cast<TRelOptimizerNode>(reorderResult->RightArg);
        rightArg = BuildAtom(rel->Label, equiJoin.Pos(), ctx);
    } else {
        std::shared_ptr<TJoinOptimizerNode> join =
            std::static_pointer_cast<TJoinOptimizerNode>(reorderResult->RightArg);
        rightArg = BuildTree(ctx,equiJoin,join);
    }

    TVector<TExprBase> leftJoinColumns;
    TVector<TExprBase> rightJoinColumns;

    // Build join conditions
    for( auto pair : reorderResult->JoinConditions) {
        leftJoinColumns.push_back(BuildAtom(pair.first.RelName, equiJoin.Pos(), ctx));
        leftJoinColumns.push_back(BuildAtom(pair.first.AttributeName, equiJoin.Pos(), ctx));
        rightJoinColumns.push_back(BuildAtom(pair.second.RelName, equiJoin.Pos(), ctx));
        rightJoinColumns.push_back(BuildAtom(pair.second.AttributeName, equiJoin.Pos(), ctx));
    }

    auto optionsList = ctx.Builder(equiJoin.Pos())
        .List()
            .List(0)
                .Atom(0, "join_algo")
                .Atom(1, ToString(reorderResult->JoinAlgo))
            .Seal()
        .Seal()
        .Build();

    // Build the final output
    return Build<TCoEquiJoinTuple>(ctx,equiJoin.Pos())
        .Type(BuildAtom(ConvertToJoinString(reorderResult->JoinType),equiJoin.Pos(),ctx))
        .LeftScope(leftArg)
        .RightScope(rightArg)
        .LeftKeys()
            .Add(leftJoinColumns)
            .Build()
        .RightKeys()
            .Add(rightJoinColumns)
            .Build()
        .Options(optionsList)
        .Done();
}

/**
 * Rebuild the equiJoinOperator with a new tree, that was obtained by optimizing join order
*/
TExprBase RearrangeEquiJoinTree(TExprContext& ctx, const TCoEquiJoin& equiJoin,
    std::shared_ptr<TJoinOptimizerNode> reorderResult) {
    TVector<TExprBase> joinArgs;
    for (size_t i = 0; i < equiJoin.ArgCount() - 2; i++){
        joinArgs.push_back(equiJoin.Arg(i));
    }

    joinArgs.push_back(BuildTree(ctx,equiJoin,reorderResult));

    joinArgs.push_back(equiJoin.Arg(equiJoin.ArgCount() - 1));

    return Build<TCoEquiJoin>(ctx, equiJoin.Pos())
        .Add(joinArgs)
        .Done();
}

class TOptimizerNativeNewDpHyp: public IOptimizerNew {
public:
    using TNodeSet = std::bitset<128>;

    TOptimizerNativeNewDpHyp(IProviderContext& ctx)
        : IOptimizerNew(ctx) 
    {}

    std::shared_ptr<TJoinOptimizerNode> JoinSearch(const std::shared_ptr<TJoinOptimizerNode>& joinTree) override {
        TJoinHypergraph<TNodeSet> hypergraph = MakeJoinHypergraph<TNodeSet>(joinTree);
        TDPHypSolver solver(hypergraph, this->Pctx);

        Y_ASSERT(hypergraph.GetNodeCount() > 0 && hypergraph.GetEdges().size() > 0);

        auto bestJoinOrder = solver.Solve();
        return ConvertFromInternal(bestJoinOrder);
    }
};

TExprBase DqOptimizeEquiJoinWithCosts(
    const TExprBase& node,
    TExprContext& ctx,
    TTypeAnnotationContext& typesCtx,
    ui32 optLevel,
    IOptimizerNew& opt,
    const TProviderCollectFunction& providerCollect
) {
    if (optLevel == 0) {
        return node;
    }

    if (!node.Maybe<TCoEquiJoin>()) {
        return node;
    }
    auto equiJoin = node.Cast<TCoEquiJoin>();
    YQL_ENSURE(equiJoin.ArgCount() >= 4);

    if (typesCtx.StatisticsMap.contains(equiJoin.Raw())) {
        return node;
    }

    YQL_CLOG(TRACE, CoreDq) << "Optimizing join with costs";

    TVector<std::shared_ptr<TRelOptimizerNode>> rels;

    // Check that statistics for all inputs of equiJoin were computed
    // The arguments of the EquiJoin are 1..n-2, n-2 is the actual join tree
    // of the EquiJoin and n-1 argument are the parameters to EquiJoin

    if (!DqCollectJoinRelationsWithStats(rels, typesCtx, equiJoin, providerCollect)){
        return node;
    }

    YQL_CLOG(TRACE, CoreDq) << "All statistics for join in place";

    auto joinTuple = equiJoin.Arg(equiJoin.ArgCount() - 2).Cast<TCoEquiJoinTuple>();

    // Generate an initial tree
    auto joinTree = ConvertToJoinTree(joinTuple, rels);

    if (NYql::NLog::YqlLogger().NeedToLog(NYql::NLog::EComponent::ProviderKqp, NYql::NLog::ELevel::TRACE)) {
        std::stringstream str;
        str << "Converted join tree:\n";
        joinTree->Print(str);
        YQL_CLOG(TRACE, CoreDq) << str.str();
    }

    auto optik = TOptimizerNativeNewDpHyp(opt.Pctx);
    joinTree = optik.JoinSearch(joinTree);

    // rewrite the join tree and record the output statistics
    TExprBase res = RearrangeEquiJoinTree(ctx, equiJoin, joinTree);
    typesCtx.StatisticsMap[res.Raw()] = joinTree->Stats;
    return res;

}

} // namespace NYql::NDq

#include "kqp_rbo_rules.h"
#include <ydb/core/kqp/common/kqp_yql.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/utils/log/log.h>
#include <typeinfo>

using namespace NYql::NNodes;

namespace {
using namespace NKikimr;
using namespace NKikimr::NKqp;

/**
 * Run dynamic programming CBO and convert the resulting tree into operator tree
 */
std::shared_ptr<IOperator> TOptimizeCBOTreeRule::SimpleTestAndApply(const std::shared_ptr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) {
    if (input->Kind != EOperator::CBOTree) {
        return input;
    }

    auto & Config = ctx.KqpCtx.Config;
    auto optLevel = Config->CostBasedOptimizationLevel.Get().GetOrElse(Config->DefaultCostBasedOptimizationLevel);

    if (optLevel <= 1) {
        return input;
    }

    auto cboTree = CastOperator<TCBOTree>(input);
    
    // Check that all inputs have statistics
    for (auto c : cboTree.Children()) {
        if (!c->Props.Statistics.has_value()) {
            ctx.ExprCtx.AddWarning(
                YqlIssue(ctx.GetPosition(equiJoin.Pos()), TIssuesIds::CBO_MISSING_TABLE_STATS,
                "Cost Based Optimizer could not be applied to this query: couldn't load statistics"
            )
        );
            return input;
        }
    }

    TVector<std::shared_ptr<TRelOptimizerNode>> rels;

    bool allRowStorage = std::any_of(
        rels.begin(),
        rels.end(),
        [](std::shared_ptr<TRelOptimizerNode>& r) {return r->Stats.StorageType==EStorageType::RowStorage; });

    if (optLevel == 2 && allRowStorage) {
        return input;
    }

    TCBOSettings settings{
        .MaxDPhypDPTableSize = Config->MaxDPHypDPTableSize.Get().GetOrElse(TDqSettings::TDefault::MaxDPHypDPTableSize),
        .ShuffleEliminationJoinNumCutoff = Config->ShuffleEliminationJoinNumCutoff.Get().GetOrElse(TDqSettings::TDefault::ShuffleEliminationJoinNumCutoff)
    };

    bool enableShuffleElimination = KqpCtx.Config->OptShuffleElimination.Get().GetOrElse(KqpCtx.Config->DefaultEnableShuffleElimination);

    // Shuffle elimination is currently disabled
    auto opt = std::unique_ptr<IOptimizerNew>(MakeNativeOptimizerNew(providerCtx, settings, ctx, false, nullptr, nullptr));

    // Generate an initial tree
    auto joinTree = ConvertToJoinTree(joinTuple, rels);

    if (NYql::NLog::YqlLogger().NeedToLog(NYql::NLog::EComponent::CoreDq, NYql::NLog::ELevel::TRACE)) {
        std::stringstream str;
        str << "Converted join tree:\n";
        joinTree->Print(str);
        YQL_CLOG(TRACE, CoreDq) << str.str();
    }

    {
        YQL_PROFILE_SCOPE(TRACE, "CBO");
        joinTree = opt.JoinSearch(joinTree, ctx.KqpCtx.GetOptimizerHints());
    }

    if (NYql::NLog::YqlLogger().NeedToLog(NYql::NLog::EComponent::CoreDq, NYql::NLog::ELevel::TRACE)) {
        std::stringstream str;
        str << "Optimizied join tree:\n";
        joinTree->Print(str);
        YQL_CLOG(TRACE, CoreDq) << str.str();
    }


    // rewrite the join tree and record the output statistics
    TExprBase res = RearrangeEquiJoinTree(typesCtx, ctx, equiJoin, joinTree, shufflingOrderingsByJoinLabels);


}

}
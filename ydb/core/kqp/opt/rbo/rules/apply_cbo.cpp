#include "kqp_cbo_trees.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/opt/cbo/solver/kqp_opt_join_cost_based.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_cbo.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_rules.h>
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>
#include <ydb/library/yql/providers/dq/common/yql_dq_settings.h>

#include <yql/essentials/utils/log/log.h>

#include <algorithm>
#include <memory>
#include <optional>

namespace NKikimr::NKqp {

namespace {

bool NeedCoreDqTrace() {
    return NYql::NLog::YqlLogger().NeedToLog(
        NYql::NLog::EComponent::CoreDq,
        NYql::NLog::ELevel::TRACE);
}

} // anonymous namespace

/**
 * Run dynamic programming CBO and convert the resulting tree into operator tree
 *
 * In order to support good CBO with pg syntax, where all the variables in the joins
 * are transformed into Pg types, we remap the synthenic variables back into original ones
 * to run the CBO, and then map them back
 */
TIntrusivePtr<IOperator> TOptimizeCBOTreeRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) {
    Y_UNUSED(props);

    if (input->Kind != EOperator::CBOTree) {
        return input;
    }

    auto cboTree = CastOperator<TOpCBOTree>(input);
    auto& cboStats = ctx.KqpCtx.CBOStats;

    auto& Config = ctx.KqpCtx.Config;
    auto optLevel = Config->CostBasedOptimizationLevel.Get().GetOrElse(Config->GetDefaultCostBasedOptimizationLevel());
    auto useBlockHashJoin = Config->UseBlockHashJoin.Get().GetOrElse(false);

    if (optLevel <= 1) {
        return input;
    }

    ++cboStats.TreesTotal;
    const auto leaves = BuildCBOLeaves(*cboTree);

    // Check that all inputs have statistics
    for (auto c : cboTree->Children) {
        if (!c->Props.Statistics.has_value()) {
            ctx.ExprCtx.AddWarning(
                YqlIssue(ctx.ExprCtx.GetPosition(cboTree->Pos), TIssuesIds::CBO_MISSING_TABLE_STATS,
                "Cost Based Optimizer could not be applied to this query: couldn't load statistics"
            )
        );
            return input;
        }
    }

    TVector<std::shared_ptr<TRelOptimizerNode>> rels;
    auto joinTree = ConvertJoinTree(cboTree, ctx.TypeCtx, rels, leaves);

    bool allRowStorage = std::any_of(
        rels.begin(),
        rels.end(),
        [](std::shared_ptr<TRelOptimizerNode>& r) {return r->Stats.StorageType==EStorageType::RowStorage; });

    if (optLevel == 2 && allRowStorage) {
        return input;
    }

    TCBOSettings settings{
        .CBOTimeout = Config->CBOTimeout.Get().GetOrElse(NKikimr::NKqp::TCBOSettings{}.CBOTimeout),
        .CBOHardTimeout = Config->CBOHardTimeout.Get().GetOrElse(NKikimr::NKqp::TCBOSettings{}.CBOHardTimeout),
        .ShuffleEliminationJoinNumCutoff = Config->ShuffleEliminationJoinNumCutoff.Get().GetOrElse(TDqSettings::TDefault::ShuffleEliminationJoinNumCutoff)
    };

    bool enableShuffleElimination = ctx.KqpCtx.Config->OptShuffleElimination.Get().GetOrElse(ctx.KqpCtx.Config->GetDefaultEnableShuffleElimination());

    const bool canBuildShuffleCtx = rels.size() <= MaxShuffleEliminationRelationCount;
    std::optional<TShuffleEliminationContext> shuffleCtx;
    if (enableShuffleElimination && canBuildShuffleCtx) {
        shuffleCtx.emplace(BuildShuffleEliminationContext(joinTree, rels, leaves));
    } else if (enableShuffleElimination) {
        YQL_CLOG(TRACE, CoreDq)
            << "Shuffle elimination disabled for CBO tree with " << rels.size()
            << " relations; maximum supported relation count is "
            << MaxShuffleEliminationRelationCount;
    }

    auto providerCtx = NOpt::TRBOProviderContext(ctx.KqpCtx, optLevel, useBlockHashJoin);
    auto opt = std::unique_ptr<IOptimizerNew>(MakeNativeOptimizerNew(
        providerCtx, settings, ctx.ExprCtx,
        enableShuffleElimination && canBuildShuffleCtx,
        shuffleCtx ? shuffleCtx->FSM : nullptr,
        shuffleCtx ? &shuffleCtx->TableAliasMap : nullptr)
    );

    if (NeedCoreDqTrace()) {
        YQL_CLOG(TRACE, CoreDq) << FormatJoinTree("Converted join tree", joinTree);
    }

    {
        YQL_PROFILE_SCOPE(TRACE, "CBO");
        joinTree = opt->JoinSearch(joinTree, ctx.KqpCtx.GetOptimizerHints(), &cboStats);
    }

    if (NeedCoreDqTrace()) {
        YQL_CLOG(TRACE, CoreDq) << FormatJoinTree("Optimizied join tree", joinTree);
    }

    return ConvertOptimizedTree(joinTree, leaves, cboTree->Pos);
}

} // namespace NKikimr::NKqp

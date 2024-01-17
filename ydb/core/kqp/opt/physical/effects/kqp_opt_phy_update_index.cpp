#include "kqp_opt_phy_effects_rules.h"
#include "kqp_opt_phy_effects_impl.h"

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NDq;
using namespace NYql::NNodes;

TExprBase KqpBuildUpdateIndexStages(TExprBase node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx) {
    if (!node.Maybe<TKqlUpdateRowsIndex>()) {
        return node;
    }

    auto update = node.Cast<TKqlUpdateRowsIndex>();
    const auto& table = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, update.Table().Path());

    TCoAtomList empty = Build<TCoAtomList>(ctx, node.Pos()).Done();

    auto effects = KqpPhyUpsertIndexEffectsImpl(TKqpPhyUpsertIndexMode::UpdateOn, update.Input(),
        update.Columns(), update.ReturningColumns(), empty, table, update.Settings(), update.Pos(), ctx);

    if (!effects) {
        return node;
    }

    return effects.Cast();
}

} // namespace NKikimr::NKqp::NOpt

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


}

}
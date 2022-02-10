#include "yql_co.h"
#include <ydb/library/yql/core/yql_opt_utils.h>

namespace NYql {

void RegisterCoFinalCallables(TCallableOptimizerMap& map) {
    map["UnorderedSubquery"] = [](const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx) {
        Y_UNUSED(optCtx);
        if (node->Head().IsCallable("Sort")) {
            if (!WarnUnroderedSubquery(*node, ctx)) {
                return TExprNode::TPtr();
            }
        }
        return ctx.RenameNode(*node, "Unordered");
    };
}

}

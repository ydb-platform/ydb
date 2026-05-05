#pragma once

#include <yql/essentials/ast/yql_expr.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/map.h>
#include <util/generic/maybe.h>

namespace NYql {

class IOptimizationContext;

class IDqHelper {
public:
    using TPtr = std::shared_ptr<IDqHelper>;

    virtual ~IDqHelper() {
    }

    virtual bool IsSingleConsumerConnection(const TExprNode::TPtr& node, const TParentsMap& parentsMap) = 0;
    virtual TExprNode::TPtr PushLambdaAndCreateCnResult(const TExprNode::TPtr& dcUnionAll, const TExprNode::TPtr& lambda, TPositionHandle pos,
                                                        TExprContext& ctx, IOptimizationContext& optCtx) = 0;
    virtual TExprNode::TPtr CreateDqStageSettings(bool singleTask, TExprContext& ctx, TPositionHandle pos) = 0;
    // TODO(udovichenko-r): remove default impl from interface after syncing arc <-> github
    virtual bool IsSinglePartitionMode(const TExprNode& stage) {
        Y_UNUSED(stage);
        return false;
    }
    // TODO(udovichenko-r): remove default impl from interface after syncing arc <-> github
    virtual TExprNode::TPtr SetSinglePartitionMode(const TExprNode::TPtr& stage, TExprContext& ctx) {
        Y_UNUSED(ctx);
        return stage;
    }
    virtual TExprNode::TListType RemoveVariadicDqStageSettings(const TExprNode& settings) = 0;
};

} // namespace NYql

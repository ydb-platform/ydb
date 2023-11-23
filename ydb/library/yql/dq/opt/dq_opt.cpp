#include "dq_opt.h"

#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>

#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/providers/dq/common/yql_dq_settings.h>

using namespace NYql::NNodes;

namespace NYql::NDq {


TCoAtom BuildAtom(TStringBuf value, TPositionHandle pos, TExprContext& ctx) {
    return Build<TCoAtom>(ctx, pos)
        .Value(value)
        .Done();
}

TCoAtomList BuildAtomList(TStringBuf value, TPositionHandle pos, TExprContext& ctx) {
    return Build<TCoAtomList>(ctx, pos)
        .Add<TCoAtom>()
            .Value(value)
            .Build()
        .Done();
}

TCoLambda BuildIdentityLambda(TPositionHandle pos, TExprContext& ctx) {
    return Build<TCoLambda>(ctx, pos)
        .Args({"item"})
        .Body("item")
        .Done();
}

bool EnsureDqUnion(const TExprBase& node, TExprContext& ctx) {
    if (!node.Maybe<TDqCnUnionAll>()) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()),
            TStringBuilder() << "Expected physical input, got " << node.Ref().Content()));
        return false;
    }

    return true;
}

const TNodeSet& GetConsumers(const TExprBase& node, const TParentsMap& parentsMap) {
    auto consumersIt = parentsMap.find(node.Raw());
    YQL_ENSURE(consumersIt != parentsMap.end());

    return consumersIt->second;
}

const TNodeMultiSet& GetConsumers(const TExprBase& node, const TParentsMultiMap& parentsMap) {
    auto consumersIt = parentsMap.find(node.Raw());
    YQL_ENSURE(consumersIt != parentsMap.end());

    return consumersIt->second;
}

ui32 GetConsumersCount(const TExprBase& node, const TParentsMap& parentsMap) {
    return GetConsumers(node, parentsMap).size();
}

bool IsSingleConsumer(const TExprBase& node, const TParentsMap& parentsMap) {
    return GetConsumersCount(node, parentsMap) == 1;
}

bool IsSingleConsumerConnection(const TDqConnection& node, const TParentsMap& parentsMap, bool allowStageMultiUsage) {
    return IsSingleConsumer(node, parentsMap)
        && IsSingleConsumer(node.Output(), parentsMap)
        && (allowStageMultiUsage || IsSingleConsumer(node.Output().Stage(), parentsMap));
}

ui32 GetStageOutputsCount(const TDqStageBase& stage) {
    auto stageType = stage.Ref().GetTypeAnn();
    YQL_ENSURE(stageType);
    auto resultsTypeTuple = stageType->Cast<TTupleExprType>();
    return resultsTypeTuple->GetSize();
}

bool IsDqPureNode(const TExprBase& node) {
    return !node.Maybe<TDqSource>() &&
           !node.Maybe<TDqConnection>() &&
           !node.Maybe<TDqPrecompute>();
}

void FindDqConnections(const TExprBase& node, TVector<TDqConnection>& connections, bool& isPure) {
    isPure = true;
    VisitExpr(node.Ptr(), [&](const TExprNode::TPtr& exprNode) {
        TExprBase node(exprNode);

        if (node.Maybe<TDqPhyPrecompute>()) {
            return false;
        }

        if (auto maybeConnection = node.Maybe<TDqConnection>()) {
            YQL_ENSURE(!maybeConnection.Maybe<TDqCnValue>());
            connections.emplace_back(maybeConnection.Cast());
            return false;
        }

        if (!IsDqPureNode(node)) {
            isPure = false;
        }

        return true;
    });
}

bool IsDqPureExpr(const TExprBase& node, bool isPrecomputePure) {
    auto filter = [](const TExprNode::TPtr& node) {
        return !TMaybeNode<TDqPhyPrecompute>(node).IsValid();
    };

    auto predicate = [](const TExprNode::TPtr& node) {
        return !IsDqPureNode(TExprBase(node));
    };

    if (isPrecomputePure) {
        return !FindNode(node.Ptr(), filter, predicate);
    }

    return !FindNode(node.Ptr(), predicate);
}

bool IsDqSelfContainedExpr(const TExprBase& node) {
    bool selfContained = true;
    TNodeSet knownArguments;

    VisitExpr(node.Ptr(),
        [&selfContained, &knownArguments] (const TExprNode::TPtr& node) {
            if (!selfContained) {
                return false;
            }

            if (auto maybeLambda = TMaybeNode<TCoLambda>(node)) {
                for (const auto& arg : maybeLambda.Cast().Args()) {
                    YQL_ENSURE(knownArguments.emplace(arg.Raw()).second);
                }
            }

            if (node->IsArgument()) {
                if (!knownArguments.contains(node.Get())) {
                    selfContained = false;
                    return false;
                }
            }

            return true;
        },
        [&selfContained, &knownArguments] (const TExprNode::TPtr& node) {
            if (!selfContained) {
                return false;
            }

            if (auto maybeLambda = TMaybeNode<TCoLambda>(node)) {
                for (const auto& arg : maybeLambda.Cast().Args()) {
                    auto it = knownArguments.find(arg.Raw());
                    YQL_ENSURE(it != knownArguments.end());
                    knownArguments.erase(it);
                }
            }

            return true;
        });

    return selfContained;
}

bool IsDqDependsOnStage(const TExprBase& node, const TDqStageBase& stage) {
    return !!FindNode(node.Ptr(), [ptr = stage.Raw()](const TExprNode::TPtr& exprNode) {
        return exprNode.Get() == ptr;
    });
}

bool CanPushDqExpr(const TExprBase& expr, const TDqStageBase& stage) {
    return IsDqPureExpr(expr, true) && !IsDqDependsOnStage(expr, stage);
}

bool CanPushDqExpr(const TExprBase& expr, const TDqConnection& connection) {
    return CanPushDqExpr(expr, connection.Output().Stage());
}

} // namespace NYql::NDq

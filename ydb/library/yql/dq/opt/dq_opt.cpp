#include "dq_opt.h"

#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>

#include <yql/essentials/core/yql_expr_optimize.h>
#include <ydb/library/yql/providers/dq/common/yql_dq_settings.h>
#include <yql/essentials/core/dq_integration/yql_dq_optimization.h>

#include <util/string/cast.h>

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

bool DqStageFirstInputIsBroadcast(const TDqStageBase& stage) {
    if (stage.Inputs().Empty()) {
        return false;
    }

    return TDqCnBroadcast::Match(stage.Inputs().Item(0).Raw());
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

bool IsDqRepeatableExpr(const TExprBase& node) {
    return !FindNode(node.Ptr(), [](const TExprNode::TPtr& expr) {
        return !TDqPhyPrecompute::Match(expr.Get());
    }, [](const TExprNode::TPtr& expr) {
        // Opaque functions may keep state even when their arguments are pure.
        return TCoNonDeterministicBase::Match(expr.Get()) || expr->IsCallable({"Udf", "ScriptUdf", "SqlCall"});
    });
}

TExprBase BuildDqTakeSkipCount(const TExprBase& take, const TExprBase& skip, TExprContext& ctx) {
    if (take.Maybe<TCoUint64>() && skip.Maybe<TCoUint64>()) {
        const ui64 takeCount = FromString<ui64>(take.Cast<TCoUint64>().Literal().Value());
        const ui64 skipCount = FromString<ui64>(skip.Cast<TCoUint64>().Literal().Value());
        const ui64 total = takeCount > Max<ui64>() - skipCount ? Max<ui64>() : takeCount + skipCount;
        return Build<TCoUint64>(ctx, take.Pos()).Literal().Build(ToString(total)).Done();
    }

    const auto max = Build<TCoUint64>(ctx, take.Pos()).Literal().Build(ToString(Max<ui64>())).Done();
    return Build<TCoIf>(ctx, take.Pos())
        .Predicate<TCoCmpGreater>()
            .Left(take)
            .Right<TCoSub>()
                .Left(max)
                .Right(skip)
                .Build()
            .Build()
        .ThenValue(max)
        .ElseValue<TCoAggrAdd>()
            .Left(take)
            .Right(skip)
            .Build()
        .Done();
}

bool IsDqSelfContainedExpr(const TExprBase& node) {
    return node.Ref().IsComplete();
}

bool IsDqDependsOnStage(const TExprBase& node, const TDqStageBase& stage) {
    return !!FindNode(node.Ptr(), [ptr = stage.Raw()](const TExprNode::TPtr& exprNode) {
        return exprNode.Get() == ptr;
    });
}

bool IsDqDependsOnOtherStage(const TExprBase& node, const TDqStageBase& stage) {
    return !!FindNode(node.Ptr(), [ptr = stage.Raw()](const TExprNode::TPtr& node) {
        if (TMaybeNode<TDqStage>(node)) {
            return node.Get() != ptr;
        }
        return false;
    });
}

bool IsDqDependsOnStageOutput(const TExprBase& node, const TDqStageBase& stage, ui32 outputIndex) {
    return !!FindNode(node.Ptr(), [ptr = stage.Raw(), outputIndex](const TExprNode::TPtr& exprNode) {
        if (TDqOutput::Match(exprNode.Get())) {
            TDqOutput output(exprNode);
            if (output.Stage().Ptr().Get() == ptr) {
                return FromString<ui32>(output.Index().Value()) == outputIndex;
            }
        }

        return false;
    });
}

bool CanPushDqExpr(const TExprBase& expr, const TDqStageBase& stage) {
    return IsDqCompletePureExpr(expr, true) && !IsDqDependsOnStage(expr, stage);
}

bool CanPushDqExpr(const TExprBase& expr, const TDqConnection& connection) {
    return CanPushDqExpr(expr, connection.Output().Stage());
}

IDqOptimization* GetDqOptCallback(const TExprBase& providerCall, const TTypeAnnotationContext& typeAnnCtx) {
    if (providerCall.Ref().ChildrenSize() > 1 && TCoDataSource::Match(providerCall.Ref().Child(1))) {
        auto dataSourceName = providerCall.Ref().Child(1)->Child(0)->Content();
        auto datasource = typeAnnCtx.DataSourceMap.FindPtr(dataSourceName);
        YQL_ENSURE(datasource);
        return (*datasource)->GetDqOptimization();
    }
    return nullptr;
}

// Returns all parents for specific node.
template <typename Type> TVector<const TExprNode*> GetParentsWithType(const TExprNode* node, const TParentsMap& parentsMap) {
    auto it = parentsMap.find(node);
    if (it == parentsMap.end() || it->second.empty()) {
        return {};
    }

    TVector<const TExprNode*> parents;
    for (const auto* node : it->second) {
        if (!Type::Match(node)) {
            return {};
        }
        parents.push_back(node);
    }

    return parents;
}

// Do not create a scalar precompute for specific pattern if `AggregationResultStage` option is enabled.
// The specific pattern is `(AsStruct((Memer(ToOptional)...)))`.
bool IsSuitableToBuildScalarPrecompute(const TExprBase& node, const TParentsMap& parentsMap, bool allowStageMultiUsage,
                                       const TBuildAggregationResultStageOptions& options) {
    // Do not apply for global opt (when `allowStageMultiUsage` is enabled)
    if (options.ApplyForDqPhyPrecompute || !options.IsEnabled || allowStageMultiUsage) {
        return true;
    }

    // Get all `TCoMembers`
    auto members = GetParentsWithType<TCoMember>(node.Raw(), parentsMap);
    if (!members.size()) {
        return true;
    }

    for (const auto* member : members) {
        auto exprLists = GetParentsWithType<TExprList>(member, parentsMap);
        // Only one parent is allowed.
        if (!exprLists.size() || exprLists.size() > 1) {
            return true;
        }

        auto exprStructs = GetParentsWithType<TCoAsStruct>(exprLists.front(), parentsMap);
        if (!exprStructs.size()) {
            return true;
        }
    }

    return false;
}

} // namespace NYql::NDq

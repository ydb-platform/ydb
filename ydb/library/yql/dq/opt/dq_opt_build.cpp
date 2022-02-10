#include "dq_opt_build.h"
#include "dq_opt.h"
#include "dq_opt_phy_finalizing.h"

#include <ydb/library/yql/ast/yql_expr.h> 
#include <ydb/library/yql/core/yql_expr_optimize.h> 
#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h> 
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h> 

namespace NYql::NDq {

using namespace NNodes;

namespace {

TExprBase RewriteProgramResultToStream(const TExprBase& result, TExprContext& ctx) {
    if (result.Ref().GetTypeAnn()->GetKind() != ETypeAnnotationKind::Flow) {
        return result;
    }

    if (auto maybeToFlow = result.Maybe<TCoToFlow>()) {
        auto toFlow = maybeToFlow.Cast();
        if (toFlow.Input().Ref().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Flow) {
            return toFlow.Input();
        }

        return Build<TCoToStream>(ctx, result.Pos())
            .Input(toFlow.Input())
            .Done();
    }

    if (const auto itemType = result.Ref().GetTypeAnn()->Cast<TFlowExprType>()->GetItemType();
        ETypeAnnotationKind::Struct == itemType->GetKind() && result.Ref().IsCallable({"PartitionsByKeys", "CombineByKey"})) {
        if (const auto structType = itemType->Cast<TStructExprType>(); structType->GetSize() > 0U) {
            return TCoFromFlow(ctx.Builder(result.Pos())
                .Callable("FromFlow")
                    .Callable(0, "NarrowMap")
                        .Callable(0, "ExpandMap")
                            .Add(0, result.Ptr())
                            .Lambda(1)
                                .Param("item")
                                .Do([&](TExprNodeBuilder& lambda) -> TExprNodeBuilder& {
                                    ui32 i = 0U;
                                    for (const auto& item : structType->GetItems()) {
                                        lambda.Callable(i++, "Member")
                                            .Arg(0, "item")
                                            .Atom(1, item->GetName())
                                        .Seal();
                                    }
                                    return lambda;
                                })
                            .Seal()
                        .Seal()
                        .Lambda(1)
                            .Params("fields", structType->GetSize())
                            .Callable("AsStruct")
                                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                    ui32 i = 0U;
                                    for (const auto& item : structType->GetItems()) {
                                        parent.List(i)
                                            .Atom(0, item->GetName())
                                            .Arg(1, "fields", i)
                                        .Seal();
                                        ++i;
                                    }
                                    return parent;
                                })
                            .Seal()
                        .Seal()
                    .Seal()
                .Seal().Build());
        }
    }

    return Build<TCoFromFlow>(ctx, result.Pos()) // TODO: TDqOutputReader?
        .Input(result)
        .Done();
}

void CollectArgsReplaces(const TDqStage& dqStage, TVector<TCoArgument>& newArgs, TNodeOnNodeOwnedMap& argsMap,
    TExprContext& ctx)
{
    newArgs.reserve(dqStage.Program().Args().Size());

    for (const auto& arg : dqStage.Program().Args()) {
        auto newArg = TCoArgument(ctx.NewArgument(arg.Pos(), arg.Name()));
        newArgs.emplace_back(newArg);
        if (arg.Ref().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Flow) {
            auto argReplace = Build<TCoToFlow>(ctx, arg.Pos()) // TODO: TDqInputReader
                .Input(newArg)
                .Done();
            argsMap.emplace(arg.Raw(), argReplace.Ptr());
        } else {
            argsMap.emplace(arg.Raw(), newArg.Ptr());
        }
    }
}

struct TStageConsumersInfo {
    ui32 ConsumersCount = 0;
    std::vector<TMaybeNode<TDqOutput>> Consumers;
    bool HasDependantConsumers = false;
};

void MakeConsumerReplaces(
    const TDqStage& dqStage,
    const std::vector<TDqOutput>& consumers,
    TNodeOnNodeOwnedMap& replaces,
    TExprContext& ctx)
{
    auto replicate = dqStage.Program().Body().Cast<TDqReplicate>();

    TVector<TExprBase> stageResults;
    for (const auto& output : consumers) {
        auto index = FromString<ui32>(output.Index().Value());
        stageResults.push_back(replicate.Args().Get(index + 1));
    }

    YQL_ENSURE(!stageResults.empty());
    TMaybeNode<TExprBase> stageResult;
    if (stageResults.size() == 1) {
        stageResult = Build<TExprApplier>(ctx, dqStage.Pos())
            .Apply(stageResults[0].Cast<TCoLambda>())
            .With(0, replicate.Input())
            .Done();
    } else {
        stageResult = Build<TDqReplicate>(ctx, dqStage.Pos())
            .Input(replicate.Input())
            .FreeArgs()
                .Add(stageResults)
                .Build()
            .Done();
    }

    TVector<TCoArgument> newArgs;
    newArgs.reserve(dqStage.Inputs().Size());
    TNodeOnNodeOwnedMap argsMap;
    CollectArgsReplaces(dqStage, newArgs, argsMap, ctx);

    auto newStage = Build<TDqStage>(ctx, dqStage.Pos())
        .Inputs(dqStage.Inputs())
        .Program<TCoLambda>()
            .Args(newArgs)
            .Body(ctx.ReplaceNodes(stageResult.Cast().Ptr(), argsMap))
            .Build()
        .Settings(TDqStageSettings::New(dqStage).BuildNode(ctx, dqStage.Pos()))
        .Sinks(dqStage.Sinks())
        .Done();

    for (ui32 i = 0; i < consumers.size(); ++i) {
        auto newOutput = Build<TDqOutput>(ctx, dqStage.Pos())
            .Stage(newStage)
            .Index().Build(ToString(i))
            .Done();

        replaces.emplace(consumers[i].Raw(), newOutput.Ptr());
    }
}

void MakeConsumerReplaces(
    const TDqStage& dqStage,
    const TStageConsumersInfo& info,
    bool allowDependantConsumers,
    TNodeOnNodeOwnedMap& replaces,
    TExprContext& ctx)
{
    if (info.Consumers.size() <= 1) {
        YQL_ENSURE(info.ConsumersCount == info.Consumers.size());
        return;
    }

    if (info.HasDependantConsumers && !allowDependantConsumers) {
        for (ui32 i = 0; i < info.Consumers.size(); ++i) {
            if (info.Consumers[i]) {
                MakeConsumerReplaces(dqStage, {info.Consumers[i].Cast()}, replaces, ctx);
            }
        }

        return;
    }

    if (info.ConsumersCount == info.Consumers.size()) {
        return;
    }

    std::vector<TDqOutput> consumers;
    for (ui32 i = 0; i < info.Consumers.size(); ++i) {
        if (info.Consumers[i]) {
            consumers.push_back(info.Consumers[i].Cast());
        }
    }
    MakeConsumerReplaces(dqStage, consumers, replaces, ctx);
}

class TDqReplaceStageConsumersTransformer : public TSyncTransformerBase {
public:
    explicit TDqReplaceStageConsumersTransformer(bool allowDependantConsumers)
        : AllowDependantConsumers(allowDependantConsumers) {}

    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        YQL_CLOG(TRACE, CoreDq) << "[DQ/Build/TransformConsumers] " << NCommon::ExprToPrettyString(ctx, *input);
        output = input;

        // All physical query roots are considered independent, so we only
        // clone stages with multiple consumers from single query result.
        TVector<TExprBase> queryRoots;
        TDeque<TExprBase> queryNodes;
        queryNodes.emplace_back(input);
        while (!queryNodes.empty()) {
            auto head = queryNodes.front();
            queryNodes.pop_front();

            if (auto maybeList = head.Maybe<TExprList>()) {
                for (const auto& item : maybeList.Cast()) {
                    queryNodes.push_back(item);
                }
            } else {
                queryRoots.push_back(head);
            }
        }

        auto filter = [](const TExprNode::TPtr& exprNode) {
            return !exprNode->IsLambda();
        };

        TNodeMap<TStageConsumersInfo> consumersMap;
        for (const auto& root : queryRoots) {
            TNodeMap<ui32> stageUsages;

            VisitExpr(root.Ptr(), filter, [&consumersMap, &stageUsages](const TExprNode::TPtr& node) {
                if (auto maybeOutput = TMaybeNode<TDqOutput>(node)) {
                    auto output = maybeOutput.Cast();
                    auto index = FromString<ui32>(output.Index().Value());

                    if (output.Stage().Maybe<TDqStage>()) {
                        auto& info = consumersMap[output.Stage().Raw()];
                        if (info.Consumers.empty()) {
                            info.Consumers.resize(GetStageOutputsCount(output.Stage(), false));
                        }

                        YQL_ENSURE(index <= info.Consumers.size());

                        if (info.Consumers[index]) {
                            YQL_ENSURE(info.Consumers[index].Cast().Raw() == output.Raw());
                        } else {
                            info.Consumers[index] = output;
                            info.ConsumersCount++;
                        }

                        stageUsages[output.Stage().Raw()]++;
                    }
                }

                return true;
            });

            for (const auto& [stage, usages] : stageUsages) {
                if (usages > 1) {
                    consumersMap[stage].HasDependantConsumers = true;
                }
            }
        }

        TNodeOnNodeOwnedMap replaces;
        for (const auto& [stage, info] : consumersMap) {
            MakeConsumerReplaces(TDqStage(stage), info, AllowDependantConsumers, replaces, ctx);
        }

        if (replaces.empty()) {
            return TStatus::Ok;
        }

        TOptimizeExprSettings settings{nullptr};
        settings.VisitLambdas = false;
        return RemapExpr(input, output, replaces, ctx, settings);
    }

private:
    const bool AllowDependantConsumers;
};

class TDqBuildPhysicalStagesTransformer : public TSyncTransformerBase {
public:
    explicit TDqBuildPhysicalStagesTransformer() {}

    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        YQL_CLOG(TRACE, CoreDq) << "[DQ/Build/TransformPhysical] " << NCommon::ExprToPrettyString(ctx, *input);
        output = input;

        TNodeOnNodeOwnedMap replaces;
        VisitExpr(input, [&ctx, &replaces](const TExprNode::TPtr& node) {
            if (node->IsLambda()) {
                return false;
            }

            if (replaces.contains(node.Get())) {
                return false;
            }

            TExprBase expr{node};

            if (expr.Maybe<TDqStage>()) {
                auto stage = expr.Cast<TDqStage>();

                TVector<TCoArgument> newArgs;
                newArgs.reserve(stage.Inputs().Size());
                TNodeOnNodeOwnedMap argsMap;
                CollectArgsReplaces(stage, newArgs, argsMap, ctx);

                auto result = RewriteProgramResultToStream(stage.Program().Body(), ctx).Ptr();
                auto newBody = ctx.ReplaceNodes(std::move(result), argsMap);

                auto newStage = Build<TDqPhyStage>(ctx, stage.Pos())
                    .Inputs(stage.Inputs())
                    .Program()
                        .Args(newArgs)
                        .Body(newBody)
                        .Build()
                    .Settings(TDqStageSettings::New(stage).BuildNode(ctx, stage.Pos()))
                    .Sinks(stage.Sinks())
                    .Done();

                replaces.emplace(stage.Raw(), newStage.Ptr());

                YQL_CLOG(TRACE, CoreDq) << " [DQ/Build/TransformPhysical] replace stage #"
                    << stage.Ref().UniqueId() << " -> #" << newStage.Ref().UniqueId();
            }

            return true;
        });

        if (replaces.empty()) {
            return TStatus::Ok;
        }

        TOptimizeExprSettings settings{nullptr};
        settings.VisitLambdas = false;
        auto status = RemapExpr(input, output, replaces, ctx, settings);
#if 0
        VisitExpr(output, [](const TExprNode::TPtr& node) {
            YQL_ENSURE(!TDqStage::Match(node.Get()), "DqStage #" << node->UniqueId());
            return true;
        });
#endif
        return status;
    }
};

} // namespace

TAutoPtr<IGraphTransformer> CreateDqBuildPhyStagesTransformer(bool allowDependantConsumers) {
    TVector<TTransformStage> transformers;

    transformers.push_back(TTransformStage(CreateFunctorTransformer(
        [](const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
            return DqReplicateStageMultiOutput(input, output, ctx);
        }),
        "ReplicateStageMultiOutput",
        TIssuesIds::DEFAULT_ERROR));

    transformers.push_back(TTransformStage(
        new TDqReplaceStageConsumersTransformer(allowDependantConsumers),
        "ReplaceStageConsumers",
        TIssuesIds::DEFAULT_ERROR));

    transformers.push_back(TTransformStage(
        new TDqBuildPhysicalStagesTransformer(),
        "BuildPhysicalStages",
        TIssuesIds::DEFAULT_ERROR));

    return CreateCompositeGraphTransformer(transformers, false);
}

} // namespace NYql::NDq

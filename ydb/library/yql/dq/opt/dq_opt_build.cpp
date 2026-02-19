#include "dq_opt_build.h"
#include "dq_opt.h"
#include "dq_opt_phy_finalizing.h"

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/common_opt/yql_co.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <ydb/library/yql/dq/type_ann/dq_type_ann.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/providers/common/provider/yql_provider.h>

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
    struct TAsyncOutput {
        TDqOutputAnnotationBase Output;
        size_t OutputIndex; // Index in stage outputs list

        bool operator ==(const TAsyncOutput& rhs) const {
            return Output.Raw() == rhs.Output.Raw() && OutputIndex == rhs.OutputIndex;
        }
    };

    using TInfo = std::variant<TAsyncOutput, TDqOutput>;

    ui32 ConsumersCount = 0;
    std::vector<std::optional<TInfo>> Consumers;
    bool HasDependantConsumers = false;
};

void MakeConsumerReplaces(
    const TDqStage& dqStage,
    const std::vector<TStageConsumersInfo::TInfo>& consumers,
    TNodeOnNodeOwnedMap& replaces,
    TExprContext& ctx)
{
    const auto getConsumerIndex = [](const TStageConsumersInfo::TInfo& consumer) {
        if (std::holds_alternative<TDqOutput>(consumer)) {
            return std::get<TDqOutput>(consumer).Index();
        }
        return std::get<TStageConsumersInfo::TAsyncOutput>(consumer).Output.Index();
    };

    if (!dqStage.Program().Body().Maybe<TDqReplicate>()) {
        for (const auto& consumer : consumers) {
            TVector<TCoArgument> newArgs;
            newArgs.reserve(dqStage.Inputs().Size());
            TNodeOnNodeOwnedMap argsMap;
            CollectArgsReplaces(dqStage, newArgs, argsMap, ctx);

            auto newStageBuilder = Build<TDqStage>(ctx, dqStage.Pos())
                .InitFrom(dqStage)
                .Program()
                    .Args(newArgs)
                    .Body<TCoFlatMap>()
                        .Input(ctx.ReplaceNodes(dqStage.Program().Body().Ptr(), argsMap))
                        .Lambda()
                            .Args({"arg"})
                            .template Body<TCoGuess>()
                                .Variant("arg")
                                .Index(getConsumerIndex(consumer))
                            .Build()
                        .Build()
                    .Build()
                    .Build()
                .Settings(TDqStageSettings().BuildNode(ctx, dqStage.Pos()));

            if (std::holds_alternative<TDqOutput>(consumer)) {
                const auto newOutput = Build<TDqOutput>(ctx, dqStage.Pos())
                    .Stage(newStageBuilder.Outputs(TMaybeNode<TDqStageOutputsList>{}).Done())
                    .Index().Build(0)
                    .Done().Ptr();
                YQL_ENSURE(replaces.emplace(std::get<TDqOutput>(consumer).Raw(), newOutput).second);
            } else {
                const auto& info = std::get<TStageConsumersInfo::TAsyncOutput>(consumer);
                const auto newStage = newStageBuilder
                    .Outputs()
                        .Add(info.Output)
                        .Build()
                    .Done().Ptr();

                // DQ stage result replication in multiple sinks without TDqReplicate is not allowed
                YQL_ENSURE(replaces.emplace(dqStage.Raw(), newStage).second, "Can not replicate multiple async outputs into different stages");
            }
        }
        return;
    }

    auto replicate = dqStage.Program().Body().Cast<TDqReplicate>();

    std::map<ui32, std::pair<TDqOutputAnnotationBase, size_t>> asyncOutputs; // Preserve same async outputs order in DQ stage after rebuild
    TVector<TExprBase> stageResults;
    for (size_t i = 0; i < consumers.size(); ++i) {
        const auto& output = consumers[i];
        ui32 index = FromString(getConsumerIndex(output).Value());
        stageResults.push_back(replicate.Args().Get(index + 1));

        if (std::holds_alternative<TStageConsumersInfo::TAsyncOutput>(output)) {
            const auto& info = std::get<TStageConsumersInfo::TAsyncOutput>(output);
            YQL_ENSURE(asyncOutputs.emplace(info.OutputIndex, std::pair(info.Output, i)).second);
        }
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

    auto newStageBuilder = Build<TDqStage>(ctx, dqStage.Pos())
        .Inputs(dqStage.Inputs())
        .Program<TCoLambda>()
            .Args(newArgs)
            .Body(ctx.ReplaceNodes(stageResult.Cast().Ptr(), argsMap))
            .Build()
        .Settings(TDqStageSettings::New(dqStage).BuildNode(ctx, dqStage.Pos()));

    TMaybeNode<TDqStage> newStage;
    if (asyncOutputs.empty()) {
        newStage = newStageBuilder.Done();
    } else {
        YQL_ENSURE(dqStage.Outputs());
        YQL_ENSURE(dqStage.Outputs().Cast().Size() == asyncOutputs.size(), "Number of async outputs either should not change or become zero");

        auto outputsBuilder = Build<TDqStageOutputsList>(ctx, dqStage.Pos());
        for (const auto& [_, output] : asyncOutputs) {
            if (const auto& transform = output.first.Maybe<TDqTransform>()) {
                outputsBuilder.Add(Build<TDqTransform>(ctx, dqStage.Pos())
                    .InitFrom(transform.Cast())
                    .Index().Build(output.second)
                    .Done()
                );
            } else if (const auto& sink = output.first.Maybe<TDqSink>()) {
                outputsBuilder.Add(Build<TDqSink>(ctx, dqStage.Pos())
                    .InitFrom(sink.Cast())
                    .Index().Build(output.second)
                    .Done()
                );
            } else {
                YQL_ENSURE(false, "Unknown output type");
            }
        }

        newStage = newStageBuilder.Outputs(outputsBuilder.Done()).Done();

        // MakeConsumerReplaces should be called at most once with async outputs consumers
        YQL_ENSURE(replaces.emplace(dqStage.Raw(), newStage.Cast().Ptr()).second, "Can not replicate multiple async outputs into different stages");
    }

    for (ui32 i = 0; i < consumers.size(); ++i) {
        const auto& consumer = consumers[i];
        if (!std::holds_alternative<TDqOutput>(consumer)) {
            continue;
        }

        auto newOutput = Build<TDqOutput>(ctx, dqStage.Pos())
            .Stage(newStage.Cast())
            .Index().Build(i)
            .Done();

        replaces.emplace(std::get<TDqOutput>(consumer).Raw(), newOutput.Ptr());
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

    const bool replicateOutputChannels = info.HasDependantConsumers && !allowDependantConsumers;
    if (info.ConsumersCount == info.Consumers.size() && !replicateOutputChannels) {
        return;
    }

    ui64 outputChannelsCount = 0;
    std::vector<TStageConsumersInfo::TInfo> consumers;
    consumers.reserve(info.ConsumersCount);
    YQL_ENSURE(info.ConsumersCount > 0);

    for (const auto& consumer : info.Consumers) {
        if (!consumer) {
            continue;
        }

        const bool isOutputChannel = std::holds_alternative<TDqOutput>(*consumer);
        outputChannelsCount += isOutputChannel;

        if (!replicateOutputChannels || !isOutputChannel || outputChannelsCount == 1) {
            // If replicateOutputChannels=1, join all async outputs with first output channel
            consumers.emplace_back(*consumer);
        } else {
            // Copy only DQ output channels after second one
            MakeConsumerReplaces(dqStage, {*consumer}, replaces, ctx);
        }
    }

    if (consumers.size() == info.Consumers.size()) {
        // There is no consumers which should be replicated due to replicateOutputChannels=1
        return;
    }

    YQL_ENSURE(!consumers.empty());
    MakeConsumerReplaces(dqStage, consumers, replaces, ctx);
}

class TDqReplaceStageConsumersTransformer : public TSyncTransformerBase {
public:
    explicit TDqReplaceStageConsumersTransformer(bool allowDependantConsumers)
        : AllowDependantConsumers_(allowDependantConsumers) {}

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
        const auto addStageConsumer = [&consumersMap](const TDqStageBase& stage, ui32 index, TStageConsumersInfo::TInfo&& output) {
            auto& info = consumersMap[stage.Raw()];

            if (info.Consumers.empty()) {
                info.Consumers.resize(GetStageOutputsCount(stage));
            }

            YQL_ENSURE(index <= info.Consumers.size());

            if (const auto& consumer = info.Consumers[index]) {
                YQL_ENSURE(consumer->index() == output.index());
                if (std::holds_alternative<TDqOutput>(output)) {
                    YQL_ENSURE(std::get<TDqOutput>(*consumer).Raw() == std::get<TDqOutput>(output).Raw());
                } else {
                    YQL_ENSURE(std::get<TStageConsumersInfo::TAsyncOutput>(*consumer) == std::get<TStageConsumersInfo::TAsyncOutput>(output));
                }
            } else {
                info.Consumers[index] = std::move(output);
                info.ConsumersCount++;
            }
        };

        for (const auto& root : queryRoots) {
            TNodeMap<ui32> stageUsages;

            VisitExpr(root.Ptr(), filter, [addStageConsumer, &stageUsages, &consumersMap](const TExprNode::TPtr& node) {
                if (auto maybeOutput = TMaybeNode<TDqOutput>(node)) {
                    auto output = maybeOutput.Cast();

                    if (output.Stage().Maybe<TDqStage>()) {
                        addStageConsumer(output.Stage(), FromString<ui32>(output.Index().Value()), output);
                        stageUsages[output.Stage().Raw()]++;

                        if (output.Stage().Outputs()
                                && output.Stage().Outputs().Cast().Size() == 1
                                && TDqTransform::Match(output.Stage().Outputs().Cast().Item(0).Raw())) {
                            YQL_ENSURE(stageUsages[output.Stage().Raw()] == 1);
                            YQL_ENSURE(consumersMap[output.Stage().Raw()].Consumers.size() == 1);
                            YQL_ENSURE(consumersMap[output.Stage().Raw()].ConsumersCount == 1);
                        }
                    }
                }

                if (const auto maybeStage = TMaybeNode<TDqStage>(node)) {
                    if (const auto stage = maybeStage.Cast(); const auto maybeOutputs = stage.Outputs()) {
                        const auto outputs = maybeOutputs.Cast();
                        for (size_t i = 0; i < outputs.Size(); ++i) {
                            auto output = outputs.Item(i);
                            if (TDqTransform::Match(output.Raw())) {
                                // Don't count output transform as consumer
                                YQL_ENSURE(outputs.Size() == 1);
                                YQL_ENSURE(stageUsages[stage.Raw()] <= 1);
                            } else {
                                addStageConsumer(stage, FromString<ui32>(output.Index().Value()), TStageConsumersInfo::TAsyncOutput{
                                    .Output = output,
                                    .OutputIndex = i,
                                });
                            }
                        }
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
            MakeConsumerReplaces(TDqStage(stage), info, AllowDependantConsumers_, replaces, ctx);
        }

        if (replaces.empty()) {
            return TStatus::Ok;
        }

        TOptimizeExprSettings settings{nullptr};
        settings.VisitLambdas = false;
        return RemapExpr(input, output, replaces, ctx, settings);
    }

    void Rewind() final {
    }

private:
    const bool AllowDependantConsumers_;
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
                    .Outputs(stage.Outputs())
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
    void Rewind() final {
    }
};

using TStageOptimizer = std::function<TDqPhyStage (const TDqPhyStage&, TExprContext&)>;

THolder<IGraphTransformer> CreateDqPhyStageTransformer(const TStageOptimizer& func, TTypeAnnotationContext* typesCtx) {
    return CreateFunctorTransformer([=](const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) -> IGraphTransformer::TStatus {
        TOptimizeExprSettings settings(typesCtx);
        settings.CustomInstantTypeTransformer = typesCtx->CustomInstantTypeTransformer.Get();
        settings.VisitLambdas = false;

        return OptimizeExpr(input, output, [&func](const TExprNode::TPtr& node, TExprContext& ctx) -> TExprNode::TPtr {
            TMaybeNode<TDqPhyStage> maybeStage(node);
            if (!maybeStage) {
                return node;
            }
            return func(maybeStage.Cast(), ctx).Ptr();
        }, ctx, settings);
    });
}

bool CanRebuildForWideChannelOutput(const TDqPhyStage& stage) {
    auto stageSettings = TDqStageSettings::Parse(stage);
    if (stageSettings.WideChannels) {
        return false;
    }

    auto stageOutputType = stage.Ref().GetTypeAnn()->Cast<TTupleExprType>();
    if (stageOutputType->GetSize() != 1 || stage.Outputs().IsValid()) {
        return false;
    }

    if (stageOutputType->GetItems()[0]->Cast<TListExprType>()->GetItemType()->GetKind() != ETypeAnnotationKind::Struct) {
        return false;
    }

    return true;
}

bool CanRebuildForWideChannelOutput(const TDqOutput& output) {
    ui32 index = FromString<ui32>(output.Index().Value());
    if (index != 0) {
        // stage has multiple outputs
        return false;
    }

    return CanRebuildForWideChannelOutput(output.Stage().Cast<TDqPhyStage>());
}

bool IsSupportedForWide(const TDqConnection& conn) {
    if (!(conn.Maybe<TDqCnHashShuffle>() ||
          conn.Maybe<TDqCnMerge>() ||
          conn.Maybe<TDqCnUnionAll>() ||
          conn.Maybe<TDqCnParallelUnionAll>() ||
          conn.Maybe<TDqCnBroadcast>() ||
          conn.Maybe<TDqCnMap>()))
    {
        return false;
    }

    ui32 index = FromString<ui32>(conn.Output().Index().Value());
    if (index != 0) {
        // stage has multiple outputs
        return false;
    }

    return true;
}

bool IsSupportedForWideBlocks(const TDqConnection& conn) {
    // currently all connections supporting wide channels also support wide block channels
    return IsSupportedForWide(conn);
}

const TStructExprType* GetStageOutputItemType(const TDqPhyStage& stage) {
    const TTupleExprType* stageType = stage.Ref().GetTypeAnn()->Cast<TTupleExprType>();
    YQL_ENSURE(stageType->GetSize() == 1);
    return stageType->GetItems()[0]->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
}

bool IsCompatibleWithBlocks(TPositionHandle pos, const TStructExprType& type, TExprContext& ctx, TTypeAnnotationContext& typesCtx) {
    TVector<const TTypeAnnotationNode*> types;
    for (auto& item : type.GetItems()) {
        types.emplace_back(item->GetItemType());
    }

    auto resolveStatus = typesCtx.ArrowResolver->AreTypesSupported(ctx.GetPosition(pos), types, ctx);
    YQL_ENSURE(resolveStatus != IArrowResolver::ERROR);
    return resolveStatus == IArrowResolver::OK;
}

TDqPhyStage RebuildStageOutputAsWide(const TDqPhyStage& stage, const TStructExprType& outputItemType, TExprContext& ctx)
{
    TCoLambda program(ctx.DeepCopyLambda(stage.Program().Ref()));

    auto stageSettings = TDqStageSettings::Parse(stage);
    YQL_CLOG(INFO, CoreDq) << "Enabled wide channels for stage with logical id = " << stageSettings.LogicalId;

    // convert stream to wide stream
    auto resultStream = ctx.Builder(program.Body().Pos())
        .Callable("FromFlow")
            .Callable(0, "ExpandMap")
                .Callable(0, "ToFlow")
                    .Add(0, program.Body().Ptr())
                .Seal()
                .Lambda(1)
                    .Param("item")
                    .Do([&](TExprNodeBuilder& lambda) -> TExprNodeBuilder& {
                        ui32 i = 0U;
                        for (const auto& item : outputItemType.GetItems()) {
                            lambda.Callable(i++, "Member")
                                .Arg(0, "item")
                                .Atom(1, item->GetName())
                            .Seal();
                        }
                        return lambda;
                    })
                .Seal()
            .Seal()
        .Seal()
        .Build();

    return Build<TDqPhyStage>(ctx, stage.Pos())
        .InitFrom(stage)
        .Program()
            .Args(program.Args())
            .Body(resultStream)
        .Build()
        .Settings(TDqStageSettings::New(stage).SetWideChannels(outputItemType).BuildNode(ctx, stage.Pos()))
        .Outputs(stage.Outputs())
        .Done();
}
}

TDqPhyStage RebuildStageInputsAsWide(const TDqPhyStage& stage, TExprContext& ctx) {
    TVector<TCoArgument> newArgs;
    TExprNodeList newInputs;
    newArgs.reserve(stage.Inputs().Size());
    newInputs.reserve(stage.Inputs().Size());
    TNodeOnNodeOwnedMap argsMap;

    YQL_ENSURE(stage.Inputs().Size() == stage.Program().Args().Size());

    bool needRebuild = false;
    for (size_t i = 0; i < stage.Inputs().Size(); ++i) {
        TCoArgument arg = stage.Program().Args().Arg(i);

        auto newArg = TCoArgument(ctx.NewArgument(arg.Pos(), arg.Name()));
        newArgs.emplace_back(newArg);

        auto maybeConn = stage.Inputs().Item(i).Maybe<TDqConnection>();

        if (maybeConn && IsSupportedForWide(maybeConn.Cast()) && CanRebuildForWideChannelOutput(maybeConn.Cast().Output())) {
            needRebuild = true;
            auto itemType = arg.Ref().GetTypeAnn()->Cast<TStreamExprType>()->GetItemType()->Cast<TStructExprType>();
            TExprNode::TPtr newArgNode = newArg.Ptr();
            // input will actually be wide stream - need to convert it back to stream
            auto argReplace = ctx.Builder(arg.Pos())
                .Callable("FromFlow")
                    .Callable(0, "NarrowMap")
                        .Callable(0, "ToFlow")
                            .Add(0, newArgNode)
                        .Seal()
                        .Lambda(1)
                            .Params("fields", itemType->GetSize())
                            .Callable("AsStruct")
                                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                    ui32 i = 0U;
                                    for (const auto& item : itemType->GetItems()) {
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
                .Seal()
                .Build();

            argsMap.emplace(arg.Raw(), argReplace);
            const TDqConnection& conn = maybeConn.Cast();

            auto outputItemType = GetStageOutputItemType(conn.Output().Stage().Cast<TDqPhyStage>());
            auto newStage = RebuildStageOutputAsWide(conn.Output().Stage().Cast<TDqPhyStage>(), *outputItemType, ctx);

            if (conn.Maybe<TDqCnHashShuffle>()) {
                auto shuffle = conn.Maybe<TDqCnHashShuffle>().Cast();
                auto builder = Build<TCoAtomList>(ctx, shuffle.KeyColumns().Pos());
                for (auto key : shuffle.KeyColumns()) {
                    ui32 idx = *outputItemType->FindItem(key.Value());
                    builder.Add<TCoAtom>().Build(ToString(idx));
                }

                newInputs.push_back(Build<TDqCnHashShuffle>(ctx, conn.Pos())
                    .Output<TDqOutput>()
                        .InitFrom(conn.Output())
                        .Stage(newStage)
                    .Build()
                    .KeyColumns(builder.Build().Value())
                    .UseSpilling(shuffle.UseSpilling())
                    .Done().Ptr());
            } else if (conn.Maybe<TDqCnMerge>()) {
                auto merge = conn.Maybe<TDqCnMerge>().Cast();
                auto builder = Build<TDqSortColumnList>(ctx, merge.SortColumns().Pos());
                for (auto sortColumn : merge.SortColumns()) {
                    ui32 idx = *outputItemType->FindItem(sortColumn.Column().Value());
                    builder.Add<TDqSortColumn>()
                        .Column<TCoAtom>().Build(ToString(idx))
                        .SortDirection(sortColumn.SortDirection())
                        .Build();
                }

                newInputs.push_back(Build<TDqCnMerge>(ctx, conn.Pos())
                    .Output<TDqOutput>()
                        .InitFrom(conn.Output())
                        .Stage(newStage)
                    .Build()
                    .SortColumns(builder.Build().Value())
                    .Done().Ptr());
            } else {
                auto newOutput = Build<TDqOutput>(ctx, conn.Output().Pos())
                    .InitFrom(conn.Output())
                    .Stage(newStage)
                    .Done();
                newInputs.push_back(ctx.ChangeChild(conn.Ref(), TDqConnection::idx_Output, newOutput.Ptr()));
            }
        } else {
            argsMap.emplace(arg.Raw(), newArg.Ptr());
            newInputs.push_back(stage.Inputs().Item(i).Ptr());
        }
    }

    if (!needRebuild) {
        return stage;
    }

    YQL_ENSURE(argsMap.size() == stage.Inputs().Size());
    YQL_ENSURE(newInputs.size() == stage.Inputs().Size());

    return Build<TDqPhyStage>(ctx, stage.Pos())
        .InitFrom(stage)
        .Inputs<TExprList>()
            .Add(newInputs)
        .Build()
        .Program()
            .Args(newArgs)
            .Body(ctx.ReplaceNodes(stage.Program().Body().Ptr(), argsMap))
        .Build()
        .Done();
}

namespace {
TDqPhyStage DqEnableWideChannelsInputForStage(const TDqPhyStage& stage, TExprContext& ctx) {
    auto stageSettings = TDqStageSettings::Parse(stage);
    if (stageSettings.WideChannels) {
        // Optimization: If current stage is already wide, assume all its inputs already rebuilt for wide channels
        return stage;
    }
    return RebuildStageInputsAsWide(stage, ctx);
}

bool CanPullReplicateScalars(const TDqPhyStage& stage) {
    return bool(stage.Program().Body().Maybe<TCoReplicateScalars>());
}

bool CanPullReplicateScalars(const TDqOutput& output) {
    ui32 index = FromString<ui32>(output.Index().Value());
    if (index != 0) {
        // stage has multiple outputs
        return false;
    }

    return CanPullReplicateScalars(output.Stage().Cast<TDqPhyStage>());
}

bool CanPullReplicateScalars(const TDqConnection& conn) {
    if (!IsSupportedForWideBlocks(conn)) {
        return false;
    }

    return CanPullReplicateScalars(conn.Output());
}

TDqPhyStage DqPullReplicateScalarsFromInputs(const TDqPhyStage& stage, TExprContext& ctx) {
    TVector<TCoArgument> newArgs;
    TExprNodeList newInputs;
    newArgs.reserve(stage.Inputs().Size());
    newInputs.reserve(stage.Inputs().Size());
    TNodeOnNodeOwnedMap argsMap;

    YQL_ENSURE(stage.Inputs().Size() == stage.Program().Args().Size());

    size_t pulled = 0;
    for (size_t i = 0; i < stage.Inputs().Size(); ++i) {
        TCoArgument arg = stage.Program().Args().Arg(i);

        auto newArg = TCoArgument(ctx.NewArgument(arg.Pos(), arg.Name()));
        newArgs.emplace_back(newArg);

        auto maybeConn = stage.Inputs().Item(i).Maybe<TDqConnection>();
        if (maybeConn && CanPullReplicateScalars(maybeConn.Cast())) {
            ++pulled;
            TDqConnection conn = maybeConn.Cast();
            TDqPhyStage childStage = conn.Output().Stage().Cast<TDqPhyStage>();
            TCoLambda childProgram(ctx.DeepCopyLambda(childStage.Program().Ref()));

            TCoReplicateScalars childReplicateScalars = childProgram.Body().Cast<TCoReplicateScalars>();

            // replace (ReplicateScalars(x, ...)) with (x)
            auto newChildStage = Build<TDqPhyStage>(ctx, childStage.Pos())
                .InitFrom(childStage)
                .Program()
                    .Args(childProgram.Args())
                    .Body(childReplicateScalars.Input())
                .Build()
                .Done();
            auto newOutput = Build<TDqOutput>(ctx, conn.Output().Pos())
                .InitFrom(conn.Output())
                .Stage(newChildStage)
                .Done();
            newInputs.push_back(ctx.ChangeChild(conn.Ref(), TDqConnection::idx_Output, newOutput.Ptr()));

            TExprNode::TPtr newArgNode = newArg.Ptr();
            TExprNode::TPtr argReplace = Build<TCoReplicateScalars>(ctx, arg.Pos())
                .Input(newArgNode)
                .Indexes(childReplicateScalars.Indexes())
                .Done()
                .Ptr();
            argsMap.emplace(arg.Raw(), argReplace);
        } else {
            argsMap.emplace(arg.Raw(), newArg.Ptr());
            newInputs.push_back(stage.Inputs().Item(i).Ptr());
        }
    }

    YQL_ENSURE(argsMap.size() == stage.Inputs().Size());
    YQL_ENSURE(newInputs.size() == stage.Inputs().Size());

    if (!pulled) {
        return stage;
    }

    auto stageSettings = TDqStageSettings::Parse(stage);
    YQL_CLOG(INFO, CoreDq) << "Pulled ReplicateScalars from " << pulled << " inputs of stage with logical id = " << stageSettings.LogicalId;
    return Build<TDqPhyStage>(ctx, stage.Pos())
        .InitFrom(stage)
        .Inputs<TExprList>()
            .Add(newInputs)
        .Build()
        .Program()
            .Args(newArgs)
            .Body(ctx.ReplaceNodes(stage.Program().Body().Ptr(), argsMap))
        .Build()
        .Done();
}

bool CanRebuildForWideBlockChannelOutput(bool forceBlocks, const TDqPhyStage& stage, TExprContext& ctx, TTypeAnnotationContext& typesCtx) {
    auto outputItemType = stage.Program().Ref().GetTypeAnn()->Cast<TStreamExprType>()->GetItemType();
    if (IsWideBlockType(*outputItemType)) {
        // output is already wide block
        return false;
    }

    auto stageSettings = TDqStageSettings::Parse(stage);
    if (!stageSettings.WideChannels) {
        return false;
    }

    YQL_ENSURE(stageSettings.OutputNarrowType);

    if (!IsCompatibleWithBlocks(stage.Pos(), *stageSettings.OutputNarrowType, ctx, typesCtx)) {
        return false;
    }

    if (!forceBlocks) {
        // Ensure that stage has blocks on top level (i.e.
        // (WideFromBlocks(...))).
        if (!stage.Program().Body().Maybe<TCoWideFromBlocks>()) {
            return false;
        }
    }

    return true;
}

bool CanRebuildForWideBlockChannelOutput(bool forceBlocks, const TDqOutput& output, TExprContext& ctx, TTypeAnnotationContext& typesCtx) {
    ui32 index = FromString<ui32>(output.Index().Value());
    if (index != 0) {
        // stage has multiple outputs
        return false;
    }

    return CanRebuildForWideBlockChannelOutput(forceBlocks, output.Stage().Cast<TDqPhyStage>(), ctx, typesCtx);
}

TDqPhyStage RebuildStageOutputAsWideBlock(const TDqPhyStage& stage, TExprContext& ctx)
{
    return Build<TDqPhyStage>(ctx, stage.Pos())
        .InitFrom(stage)
        .Program()
            .Args(stage.Program().Args())
            .Body<TCoWideToBlocks>()
                .Input(stage.Program().Body())
            .Build()
        .Build()
        .Done();
}

TDqPhyStage RebuildStageInputsAsWideBlock(bool forceBlocks, const TDqPhyStage& stage, TExprContext& ctx, TTypeAnnotationContext& typesCtx) {
    TVector<TCoArgument> newArgs;
    TExprNodeList newInputs;
    newArgs.reserve(stage.Inputs().Size());
    newInputs.reserve(stage.Inputs().Size());
    TNodeOnNodeOwnedMap argsMap;

    YQL_ENSURE(stage.Inputs().Size() == stage.Program().Args().Size());

    size_t blockInputs = 0;
    for (size_t i = 0; i < stage.Inputs().Size(); ++i) {
        TCoArgument arg = stage.Program().Args().Arg(i);

        auto newArg = TCoArgument(ctx.NewArgument(arg.Pos(), arg.Name()));
        newArgs.emplace_back(newArg);

        auto maybeConn = stage.Inputs().Item(i).Maybe<TDqConnection>();

        if (maybeConn && IsSupportedForWideBlocks(maybeConn.Cast()) && CanRebuildForWideBlockChannelOutput(forceBlocks, maybeConn.Cast().Output(), ctx, typesCtx)) {
            ++blockInputs;
            // input will actually be wide block stream - convert it to wide stream first
            TExprNode::TPtr newArgNode = ctx.Builder(arg.Pos())
                .Callable("WideFromBlocks")
                    .Add(0, newArg.Ptr())
                .Seal()
                .Build();
            argsMap.emplace(arg.Raw(), newArgNode);

            const TDqConnection& conn = maybeConn.Cast();
            auto newOutput = Build<TDqOutput>(ctx, conn.Output().Pos())
                .InitFrom(conn.Output())
                .Stage(RebuildStageOutputAsWideBlock(conn.Output().Stage().Cast<TDqPhyStage>(), ctx))
                .Done();
            newInputs.push_back(ctx.ChangeChild(conn.Ref(), TDqConnection::idx_Output, newOutput.Ptr()));
        } else {
            argsMap.emplace(arg.Raw(), newArg.Ptr());
            newInputs.push_back(stage.Inputs().Item(i).Ptr());
        }
    }

    YQL_ENSURE(argsMap.size() == stage.Inputs().Size());
    YQL_ENSURE(newInputs.size() == stage.Inputs().Size());

    if (!blockInputs) {
        return stage;
    }

    auto stageSettings = TDqStageSettings::Parse(stage);
    YQL_CLOG(INFO, CoreDq) << "Enabled " << blockInputs << " block inputs for stage with logical id = " << stageSettings.LogicalId;

    return Build<TDqPhyStage>(ctx, stage.Pos())
        .InitFrom(stage)
        .Inputs<TExprList>()
            .Add(newInputs)
        .Build()
        .Program()
            .Args(newArgs)
            .Body(ctx.ReplaceNodes(stage.Program().Body().Ptr(), argsMap))
        .Build()
        .Done();
}

} // namespace

TAutoPtr<IGraphTransformer> CreateDqBuildPhyStagesTransformer(bool allowDependantConsumers, TTypeAnnotationContext& typesCtx, EChannelMode mode) {
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

    if (mode != CHANNEL_SCALAR) {
        transformers.push_back(TTransformStage(CreateDqPhyStageTransformer(&DqEnableWideChannelsInputForStage, &typesCtx),
            "EnableWideChannels",
            TIssuesIds::DEFAULT_ERROR));
    }

    return CreateCompositeGraphTransformer(transformers, false);
}

TAutoPtr<IGraphTransformer> CreateDqBuildWideBlockChannelsTransformer(TTypeAnnotationContext& typesCtx, EChannelMode mode) {
    TVector<TTransformStage> transformers;

    if (mode == CHANNEL_WIDE_AUTO_BLOCK || mode == CHANNEL_WIDE_FORCE_BLOCK) {
        transformers.push_back(TTransformStage(CreateDqPhyStageTransformer(
            [mode, &typesCtx](const TDqPhyStage& stage, TExprContext& ctx) {
                const bool forceBlocks = mode == CHANNEL_WIDE_FORCE_BLOCK;
                return RebuildStageInputsAsWideBlock(forceBlocks, stage, ctx, typesCtx);
            }, &typesCtx),
            "EnableBlockChannels",
            TIssuesIds::DEFAULT_ERROR));
        transformers.push_back(TTransformStage(CreateDqPhyStageTransformer(&DqPullReplicateScalarsFromInputs, &typesCtx),
            "PullReplicateScalars",
            TIssuesIds::DEFAULT_ERROR));
    }

    return CreateCompositeGraphTransformer(transformers, false);
}

} // namespace NYql::NDq

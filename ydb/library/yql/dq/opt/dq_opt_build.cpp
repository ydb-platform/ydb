#include "dq_opt_build.h"
#include "dq_opt.h"
#include "dq_opt_phy_finalizing.h"

#include <ydb/core/kqp/expr_nodes/kqp_expr_nodes.h>

#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <ydb/library/yql/dq/type_ann/dq_type_ann.h>
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
    if (!dqStage.Program().Body().Maybe<TDqReplicate>()) {
        for (ui32 i = 0; i < consumers.size(); ++i) {
            TVector<TCoArgument> newArgs;
            newArgs.reserve(dqStage.Inputs().Size());
            TNodeOnNodeOwnedMap argsMap;
            CollectArgsReplaces(dqStage, newArgs, argsMap, ctx);
            auto newStage = Build<TDqStage>(ctx, dqStage.Pos())
                .InitFrom(dqStage)
                .Program()
                    .Args(newArgs)
                    .Body<TCoFlatMap>()
                        .Input(ctx.ReplaceNodes(dqStage.Program().Body().Ptr(), argsMap))
                        .Lambda()
                            .Args({"arg"})
                            .template Body<TCoGuess>()
                                .Variant("arg")
                                .Index(consumers[i].Index())
                            .Build()
                        .Build()
                    .Build()
                    .Build()
                .Settings(TDqStageSettings().BuildNode(ctx, dqStage.Pos()))
                .Done().Ptr();
            auto newOutput = Build<TDqOutput>(ctx, dqStage.Pos())
                .Stage(newStage)
                .Index().Build(0)
                .Done().Ptr();
            replaces.emplace(consumers[i].Raw(), newOutput);
        }
        return;
    }
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
        .Outputs(dqStage.Outputs())
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
        for (const auto& root : queryRoots) {
            TNodeMap<ui32> stageUsages;

            VisitExpr(root.Ptr(), filter, [&consumersMap, &stageUsages](const TExprNode::TPtr& node) {
                if (auto maybeOutput = TMaybeNode<TDqOutput>(node)) {
                    auto output = maybeOutput.Cast();
                    auto index = FromString<ui32>(output.Index().Value());

                    if (output.Stage().Maybe<TDqStage>()) {
                        auto& info = consumersMap[output.Stage().Raw()];

                        if (info.Consumers.empty()) {
                            info.Consumers.resize(GetStageOutputsCount(output.Stage()));
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

bool IsSupportedForWide(const TDqConnection& conn) {
    if (conn.Maybe<TDqCnResult>() || conn.Maybe<TDqCnValue>() || conn.Maybe<TDqCnUnionAll>() || conn.Maybe<TKqpCnStreamLookup>()) {
        return false;
    }

    ui32 index = FromString<ui32>(conn.Output().Index().Value());
    if (index != 0) {
        // stage has multiple outputs
        return false;
    }

    return true;
}

bool CanRebuildForWideChannelOutput(const TDqConnection& conn) {
    if (!IsSupportedForWide(conn)) {
        return false;
    }

    return CanRebuildForWideChannelOutput(conn.Output().Stage().Cast<TDqPhyStage>());
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

TDqPhyStage RebuildStageInputsAsWide(const TDqPhyStage& stage, TExprContext& ctx) {
    TVector<TCoArgument> newArgs;
    newArgs.reserve(stage.Inputs().Size());
    TNodeOnNodeOwnedMap argsMap;

    YQL_ENSURE(stage.Inputs().Size() == stage.Program().Args().Size());

    bool needRebuild = false;
    for (size_t i = 0; i < stage.Inputs().Size(); ++i) {
        TCoArgument arg = stage.Program().Args().Arg(i);

        auto newArg = TCoArgument(ctx.NewArgument(arg.Pos(), arg.Name()));
        newArgs.emplace_back(newArg);

        auto maybeConn = stage.Inputs().Item(i).Maybe<TDqConnection>();

        if (maybeConn && CanRebuildForWideChannelOutput(maybeConn.Cast())) {
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
        } else {
            argsMap.emplace(arg.Raw(), newArg.Ptr());
        }
    }

    if (!needRebuild) {
        return stage;
    }

    return Build<TDqPhyStage>(ctx, stage.Pos())
        .InitFrom(stage)
        .Program()
            .Args(newArgs)
            .Body(ctx.ReplaceNodes(stage.Program().Body().Ptr(), argsMap))
        .Build()
        .Done();
}

TDqPhyStage RebuildStageOutputAsWide(const TDqPhyStage& stage, const TStructExprType& outputItemType, TExprContext& ctx)
{
    TCoLambda program(ctx.DeepCopyLambda(stage.Program().Ref()));

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

TDqPhyStage RebuildStageAsWide(const TDqPhyStage& stage, TExprContext& ctx) {
    const TStructExprType* outputItemType = GetStageOutputItemType(stage);
    return RebuildStageOutputAsWide(RebuildStageInputsAsWide(stage, ctx), *outputItemType, ctx);
}

IGraphTransformer::TStatus DqEnableWideChannels(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx)
{
    output = input;
    TNodeOnNodeOwnedMap replaces;
    TNodeSet processedStages;
    VisitExpr(input, [&](const TExprNode::TPtr& node) {
        if (node->IsLambda()) {
            return false;
        }

        TExprBase expr{node};
        auto maybeConn = expr.Maybe<TDqConnection>();
        if (maybeConn && CanRebuildForWideChannelOutput(maybeConn.Cast())) {
            auto conn = maybeConn.Cast();
            processedStages.insert(conn.Output().Stage().Raw());
            auto newStage = RebuildStageAsWide(conn.Output().Stage().Cast<TDqPhyStage>(), ctx);
            auto outputItemType = GetStageOutputItemType(conn.Output().Stage().Cast<TDqPhyStage>());

            if (conn.Maybe<TDqCnHashShuffle>()) {
                auto shuffle = conn.Maybe<TDqCnHashShuffle>().Cast();
                auto builder = Build<TCoAtomList>(ctx, shuffle.KeyColumns().Pos());
                for (auto key : shuffle.KeyColumns()) {
                    ui32 idx = *outputItemType->FindItem(key.Value());
                    builder.Add<TCoAtom>().Build(ToString(idx));
                }

                replaces[conn.Raw()] = Build<TDqCnHashShuffle>(ctx, conn.Pos())
                    .Output<TDqOutput>()
                        .InitFrom(conn.Output())
                        .Stage(newStage)
                    .Build()
                    .KeyColumns(builder.Build().Value())
                    .Done().Ptr();
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

                replaces[conn.Raw()] = Build<TDqCnMerge>(ctx, conn.Pos())
                    .Output<TDqOutput>()
                        .InitFrom(conn.Output())
                        .Stage(newStage)
                    .Build()
                    .SortColumns(builder.Build().Value())
                    .Done().Ptr();
            } else {
                auto newOutput = Build<TDqOutput>(ctx, conn.Output().Pos())
                    .InitFrom(conn.Output())
                    .Stage(newStage)
                    .Done();
                replaces[conn.Raw()] = ctx.ChangeChild(conn.Ref(), TDqConnection::idx_Output, newOutput.Ptr());
            }
        } else if (expr.Maybe<TDqPhyStage>()) {
            auto stage = expr.Maybe<TDqPhyStage>().Cast();
            if (!processedStages.contains(stage.Raw())) {
                processedStages.insert(stage.Raw());
                auto newStage = RebuildStageInputsAsWide(stage, ctx);
                if (newStage.Raw() != stage.Raw()) {
                    replaces[stage.Raw()] = newStage.Ptr();
                }
            }
        }

        return true;
    });

    if (replaces.empty()) {
        return IGraphTransformer::TStatus::Ok;
    }

    YQL_CLOG(INFO, CoreDq) << "[DQ/Build/EnableWideChannels] " << "Enabled wide channels for " << replaces.size() << " stages";
    TOptimizeExprSettings settings{nullptr};
    settings.VisitLambdas = false;
    auto status = RemapExpr(input, output, replaces, ctx, settings);
    YQL_CLOG(TRACE, CoreDq) << "[DQ/Build/EnableWideChannels] " << "Dump: " << NCommon::ExprToPrettyString(ctx, *output);
    return status;
}

bool CanPullReplicateScalars(const TDqPhyStage& stage) {
    auto maybeFromFlow = stage.Program().Body().Maybe<TCoFromFlow>();
    if (!maybeFromFlow) {
        return false;
    }

    return bool(maybeFromFlow.Cast().Input().Maybe<TCoReplicateScalars>());
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
    if (!IsSupportedForWide(conn)) {
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

            TCoReplicateScalars childReplicateScalars = childProgram.Body().Cast<TCoFromFlow>().Input().Cast<TCoReplicateScalars>();

            // replace FromFlow(ReplicateScalars(x, ...)) with FromFlow(x)
            auto newChildStage = Build<TDqPhyStage>(ctx, childStage.Pos())
                .InitFrom(childStage)
                .Program()
                    .Args(childProgram.Args())
                    .Body(ctx.ChangeChild(childProgram.Body().Ref(), TCoFromFlow::idx_Input, childReplicateScalars.Input().Ptr()))
                .Build()
                .Done();
            auto newOutput = Build<TDqOutput>(ctx, conn.Output().Pos())
                .InitFrom(conn.Output())
                .Stage(newChildStage)
                .Done();
            newInputs.push_back(ctx.ChangeChild(conn.Ref(), TDqConnection::idx_Output, newOutput.Ptr()));

            TExprNode::TPtr newArgNode = newArg.Ptr();
            TExprNode::TPtr argReplace = Build<TCoFromFlow>(ctx, arg.Pos())
                .Input<TCoReplicateScalars>()
                    .Input<TCoToFlow>()
                        .Input(newArgNode)
                    .Build()
                    .Indexes(childReplicateScalars.Indexes())
                .Build()
                .Done()
                .Ptr();
            argsMap.emplace(arg.Raw(), argReplace);
        } else {
            argsMap.emplace(arg.Raw(), newArg.Ptr());
            newInputs.push_back(stage.Inputs().Item(i).Ptr());
        }
    }

    if (!pulled) {
        return stage;
    }

    YQL_CLOG(INFO, CoreDq) << "Pulled " << pulled << " ReplicateScalars from stage inputs";
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
        // ensure that stage has blocks on top level (i.e. FromFlow(WideFromBlocks(...)))
        if (!stage.Program().Body().Maybe<TCoFromFlow>() ||
            !stage.Program().Body().Cast<TCoFromFlow>().Input().Maybe<TCoWideFromBlocks>())
        {
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

bool CanRebuildForWideBlockChannelOutput(bool forceBlocks, const TDqConnection& conn, TExprContext& ctx, TTypeAnnotationContext& typesCtx) {
    if (conn.Maybe<TDqCnResult>() || conn.Maybe<TDqCnValue>()) {
        return false;
    }

    ui32 index = FromString<ui32>(conn.Output().Index().Value());
    if (index != 0) {
        // stage has multiple outputs
        return false;
    }

    return CanRebuildForWideBlockChannelOutput(forceBlocks, conn.Output().Stage().Cast<TDqPhyStage>(), ctx, typesCtx);
}

TDqPhyStage RebuildStageInputsAsWideBlock(bool forceBlocks, const TDqPhyStage& stage, TExprContext& ctx, TTypeAnnotationContext& typesCtx) {
    TVector<TCoArgument> newArgs;
    newArgs.reserve(stage.Inputs().Size());
    TNodeOnNodeOwnedMap argsMap;

    YQL_ENSURE(stage.Inputs().Size() == stage.Program().Args().Size());

    bool needRebuild = false;
    for (size_t i = 0; i < stage.Inputs().Size(); ++i) {
        TCoArgument arg = stage.Program().Args().Arg(i);

        auto newArg = TCoArgument(ctx.NewArgument(arg.Pos(), arg.Name()));
        newArgs.emplace_back(newArg);

        auto maybeConn = stage.Inputs().Item(i).Maybe<TDqConnection>();

        if (maybeConn && CanRebuildForWideBlockChannelOutput(forceBlocks, maybeConn.Cast().Output(), ctx, typesCtx)) {
            needRebuild = true;
            // input will actually be wide block stream - convert it to wide stream first
            TExprNode::TPtr newArgNode = ctx.Builder(arg.Pos())
                .Callable("FromFlow")
                    .Callable(0, "WideFromBlocks")
                        .Callable(0, "ToFlow")
                            .Add(0, newArg.Ptr())
                        .Seal()
                    .Seal()
                .Seal()
                .Build();
            argsMap.emplace(arg.Raw(), newArgNode);
        } else {
            argsMap.emplace(arg.Raw(), newArg.Ptr());
        }
    }

    if (!needRebuild) {
        return stage;
    }

    return Build<TDqPhyStage>(ctx, stage.Pos())
        .InitFrom(stage)
        .Program()
            .Args(newArgs)
            .Body(ctx.ReplaceNodes(stage.Program().Body().Ptr(), argsMap))
        .Build()
        .Done();
}

TDqPhyStage RebuildStageOutputAsWideBlock(const TDqPhyStage& stage, TExprContext& ctx)
{
    return Build<TDqPhyStage>(ctx, stage.Pos())
        .InitFrom(stage)
        .Program()
            .Args(stage.Program().Args())
            .Body<TCoFromFlow>()
                .Input<TCoWideToBlocks>()
                    .Input<TCoToFlow>()
                        .Input(stage.Program().Body())
                    .Build()
                .Build()
            .Build()
        .Build()
        .Done();
}

TDqPhyStage RebuildStageAsWideBlock(bool forceBlocks, const TDqPhyStage& stage, TExprContext& ctx, TTypeAnnotationContext& typesCtx) {
    return RebuildStageOutputAsWideBlock(RebuildStageInputsAsWideBlock(forceBlocks, stage, ctx, typesCtx), ctx);
}

IGraphTransformer::TStatus DqEnableWideBlockChannels(bool forceBlocks, TExprNode::TPtr input, TExprNode::TPtr& output,
    TExprContext& ctx, TTypeAnnotationContext& typesCtx)
{
    output = input;
    TNodeOnNodeOwnedMap replaces;
    TNodeSet processedStages;
    VisitExpr(input, [&](const TExprNode::TPtr& node) {
        if (node->IsLambda()) {
            return false;
        }

        TExprBase expr{node};
        auto maybeConn = expr.Maybe<TDqConnection>();
        if (maybeConn && CanRebuildForWideBlockChannelOutput(forceBlocks, maybeConn.Cast(), ctx, typesCtx)) {
            auto conn = maybeConn.Cast();
            processedStages.insert(conn.Output().Stage().Raw());
            auto newStage = RebuildStageAsWideBlock(forceBlocks, conn.Output().Stage().Cast<TDqPhyStage>(), ctx, typesCtx);
            auto newOutput = Build<TDqOutput>(ctx, conn.Output().Pos())
                .InitFrom(conn.Output())
                .Stage(newStage)
                .Done();
            replaces[conn.Raw()] = ctx.ChangeChild(conn.Ref(), TDqConnection::idx_Output, newOutput.Ptr());
        } else if (expr.Maybe<TDqPhyStage>()) {
            auto stage = expr.Maybe<TDqPhyStage>().Cast();
            if (!processedStages.contains(stage.Raw())) {
                processedStages.insert(stage.Raw());
                auto newStage = RebuildStageInputsAsWideBlock(forceBlocks, stage, ctx, typesCtx);
                if (newStage.Raw() != stage.Raw()) {
                    replaces[stage.Raw()] = newStage.Ptr();
                }
            }
        }

        return true;
    });

    if (replaces.empty()) {
        return IGraphTransformer::TStatus::Ok;
    }

    YQL_CLOG(INFO, CoreDq) << "[DQ/Build/EnableWideBlockChannels] " << "Enabled block channels for " << replaces.size() << " stages";
    TOptimizeExprSettings settings{nullptr};
    settings.VisitLambdas = false;
    auto status = RemapExpr(input, output, replaces, ctx, settings);
    YQL_CLOG(TRACE, CoreDq) << "[DQ/Build/EnableWideBlockChannels] " << "Dump: " << NCommon::ExprToPrettyString(ctx, *output);
    return status;
}

} // namespace

TAutoPtr<IGraphTransformer> CreateDqBuildPhyStagesTransformer(bool allowDependantConsumers, TTypeAnnotationContext& typesCtx, EChannelMode mode) {
    Y_UNUSED(typesCtx);
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
        transformers.push_back(TTransformStage(CreateFunctorTransformer(&DqEnableWideChannels),
            "EnableWideChannels",
            TIssuesIds::DEFAULT_ERROR));
    }

    return CreateCompositeGraphTransformer(transformers, false);
}

TAutoPtr<IGraphTransformer> CreateDqBuildWideBlockChannelsTransformer(TTypeAnnotationContext& typesCtx, EChannelMode mode) {
    TVector<TTransformStage> transformers;

    if (mode == CHANNEL_WIDE_AUTO_BLOCK || mode == CHANNEL_WIDE_FORCE_BLOCK) {
        transformers.push_back(TTransformStage(CreateFunctorTransformer(
            [mode, &typesCtx](const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
                const bool forceBlocks = mode == CHANNEL_WIDE_FORCE_BLOCK;
                return DqEnableWideBlockChannels(forceBlocks, input, output, ctx, typesCtx);
            }),
            "EnableBlockChannels",
            TIssuesIds::DEFAULT_ERROR));
        transformers.push_back(TTransformStage(CreateFunctorTransformer(
            [&typesCtx](const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
                TOptimizeExprSettings optSettings{&typesCtx};
                optSettings.VisitLambdas = false;
                return OptimizeExpr(input, output, [](const TExprNode::TPtr& node, TExprContext& ctx) -> TExprNode::TPtr {
                    TExprBase expr{node};
                    if (auto stage = expr.Maybe<TDqPhyStage>()) {
                        return DqPullReplicateScalarsFromInputs(stage.Cast(), ctx).Ptr();
                    }
                    return node;
                }, ctx, optSettings);
            }),
            "PullReplicateScalars",
            TIssuesIds::DEFAULT_ERROR));
    }

    return CreateCompositeGraphTransformer(transformers, false);
}

} // namespace NYql::NDq

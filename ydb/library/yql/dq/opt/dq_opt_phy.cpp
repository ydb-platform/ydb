#include "dq_opt_phy.h"
#include "dq_opt_join.h"

#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/core/yql_type_helpers.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/dq/type_ann/dq_type_ann.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/core/yql_cost_function.h>

namespace NYql::NDq {

using namespace NYql::NNodes;

TVector<TCoArgument> PrepareArgumentsReplacement(const TExprBase& node, const TVector<TDqConnection>& newInputs,
    TExprContext& ctx, TNodeOnNodeOwnedMap& replaceMap)
{
    TVector<TCoArgument> newArgs;
    replaceMap.clear();

    if (auto maybeArgs = node.Maybe<TCoArguments>()) {
        auto args = maybeArgs.Cast();

        newArgs.reserve(args.Size() + newInputs.size());
        for (size_t i = 0; i < args.Size(); ++i) {
            TCoArgument newArg{ctx.NewArgument(node.Pos(), TStringBuilder()
                << "_dq_replace_arg_" << i)};
            replaceMap[args.Arg(i).Raw()] = newArg.Ptr();
            newArgs.emplace_back(newArg);
        }
    } else {
        newArgs.reserve(newInputs.size());
    }

    for (size_t i = 0; i < newInputs.size(); ++i) {
        TCoArgument newArg{ctx.NewArgument(node.Pos(), TStringBuilder()
            << "_dq_replace_input_arg_" << newArgs.size())};
        replaceMap[newInputs[i].Raw()] = newArg.Ptr();
        newArgs.emplace_back(newArg);
    }

    return newArgs;
}

namespace {
TMaybeNode<TCoMux> ConvertMuxArgumentsToFlows(TCoMux node, TExprContext& ctx) {
    auto mux = node.Cast<TCoMux>();
    bool hasConnAsArg = false;
    for (auto child: mux.Input().template Cast<TExprList>()) {
        if (child.Maybe<TDqConnection>().IsValid()) {
            hasConnAsArg = true;
            break;
        }
    }
    if (!hasConnAsArg) {
        return {};
    }
    TVector<TExprBase> muxArgs;
    for (auto child: mux.Input().template Cast<TExprList>()) {
        if (child.Maybe<TDqConnection>().IsValid()) {
            muxArgs.push_back(child);
        }
        else if (IsDqCompletePureExpr(child)) {
            muxArgs.push_back(Build<TCoToFlow>(ctx, node.Pos())
                .Input(child)
                .Done());
        } else {
            return {};
        }
    }
    return Build<TCoMux>(ctx, node.Pos())
        .Input<TExprList>()
            .Add(muxArgs)
            .Build()
        .Done();
}

bool PrepareKeySelectorToStage(TCoLambda& keySelector, TCoLambda& stageLambda, TCoLambda& handlerLambda, TExprContext& ctx) {
    if (keySelector.Body().Ref().IsComplete()) {
        // constant key
        return false;
    }
    const TPositionHandle pos = keySelector.Pos();
    TVector<TExprBase> keyElements;
    if (auto maybeTuple = keySelector.Body().Maybe<TExprList>()) {
        auto tuple = maybeTuple.Cast();
        for (const auto& element : tuple) {
            keyElements.push_back(element);
        }
    } else {
        keyElements.push_back(keySelector.Body());
    }

    size_t genCount = 0;
    TCoLambda removeColumns = BuildIdentityLambda(pos, ctx);
    TCoLambda addColumns = BuildIdentityLambda(pos, ctx);
    for (auto& element : keyElements) {
        if (element.Maybe<TCoMember>()) {
            auto member = element.Cast<TCoMember>();
            if (member.Struct().Raw() == keySelector.Args().Arg(0).Raw()) {
                continue;
            }
        }

        TString newMemberName = TString("_yql_key_selector_") + ToString(genCount++);

        TCoLambda computeElement = Build<TCoLambda>(ctx, pos)
            .InitFrom(keySelector)
            .Body(element)
            .Done();

        addColumns = Build<TCoLambda>(ctx, pos)
            .Args({"row"})
            .Body<TCoAddMember>()
                .Struct<TExprApplier>()
                    .Apply(addColumns)
                    .With(0, "row")
                .Build()
                .Name()
                    .Value(newMemberName)
                .Build()
                .Item<TExprApplier>()
                    .Apply(computeElement)
                    .With(0, "row")
                .Build()
            .Build()
            .Done();

        removeColumns = Build<TCoLambda>(ctx, pos)
            .Args({"row"})
            .Body<TCoRemoveMember>()
                .Struct<TExprApplier>()
                    .Apply(removeColumns)
                    .With(0, "row")
                .Build()
                .Name()
                    .Value(newMemberName)
                .Build()
            .Build()
            .Done();

        element = Build<TCoMember>(ctx, pos)
            .Struct(keySelector.Args().Arg(0))
            .Name()
                .Value(newMemberName)
            .Build()
            .Done();

    }

    if (!genCount) {
        return false;
    }

    handlerLambda = Build<TCoLambda>(ctx, pos)
        .Args({"flow"})
        .Body<TExprApplier>()
            .Apply(handlerLambda)
            .With<TCoOrderedMap>(0)
                .Input("flow")
                .Lambda(removeColumns)
            .Build()
        .Build()
        .Done();

    stageLambda = Build<TCoLambda>(ctx, pos)
        .Args({"flow"})
        .Body<TCoOrderedMap>()
            .Input("flow")
            .Lambda(addColumns)
        .Build()
        .Done();

    if (keyElements.size() == 1) {
        keySelector = Build<TCoLambda>(ctx, pos)
            .InitFrom(keySelector)
            .Body(keyElements.front())
            .Done();
    } else {
        keySelector = Build<TCoLambda>(ctx, pos)
            .InitFrom(keySelector)
            .Body<TExprList>()
                .Add(keyElements)
            .Build()
            .Done();
    }

    keySelector = TCoLambda(ctx.DeepCopyLambda(keySelector.Ref()));
    return true;
}

TDqConnection BuildShuffleConnection(TPositionHandle pos, TCoLambda keySelector, TDqCnUnionAll dqUnion, TExprContext& ctx) {
    const bool isConstKey = keySelector.Body().Ref().IsComplete();
    if (isConstKey) {
        return Build<TDqCnUnionAll>(ctx, pos)
            .Output()
                .Stage(dqUnion.Output().Stage())
                .Index(dqUnion.Output().Index())
            .Build()
            .Done();
    }

    TVector<TExprBase> keyElements;
    if (auto maybeTuple = keySelector.Body().template Maybe<TExprList>()) {
        auto tuple = maybeTuple.Cast();
        for (const auto& element : tuple) {
            keyElements.push_back(element);
        }
    } else {
        keyElements.push_back(keySelector.Body());
    }

    TVector<TCoAtom> keyColumns;
    keyColumns.reserve(keyElements.size());
    for (auto& element : keyElements) {
        auto member = element.Cast<TCoMember>();
        YQL_ENSURE(member.Struct().Raw() == keySelector.Args().Arg(0).Raw(), "Should be handled earlier");
        keyColumns.push_back(element.Cast<TCoMember>().Name());
    }

    return Build<TDqCnHashShuffle>(ctx, pos)
        .Output()
            .Stage(dqUnion.Output().Stage())
            .Index(dqUnion.Output().Index())
        .Build()
        .KeyColumns()
            .Add(keyColumns)
        .Build()
        .Done();
}

template <typename TPartition>
TExprBase DqBuildPartitionsStageStub(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx,
    const TParentsMap& parentsMap, bool allowStageMultiUsage)
{
    auto partitionsInput = node.Maybe<TPartition>().Input();
    const bool isMuxInput = partitionsInput.template Maybe<TCoMux>().IsValid();
    if (!partitionsInput.template Maybe<TDqCnUnionAll>() && !isMuxInput) {
        return node;
    }

    auto partition = node.Cast<TPartition>();
    if (!IsDqCompletePureExpr(partition.KeySelectorLambda()) ||
        !IsDqCompletePureExpr(partition.ListHandlerLambda()) ||
        !IsDqCompletePureExpr(partition.SortKeySelectorLambda()))
    {
        return node;
    }

    // dq splits this type of lambda output into separate stage outputs
    // thus it's impossible to maintain 'node' typing (muxing them ain't an option,
    // cause the only purpose of this optimizer is to push original Mux to the stage)
    if (const auto listItemType = GetSeqItemType(node.Ref().GetTypeAnn());
        !listItemType || listItemType->GetKind() == ETypeAnnotationKind::Variant) {
        return node;
    }

    TVector<TCoArgument> inputArgs;
    TVector<TExprBase> inputConns;
    TExprNode::TPtr newPartitionsInput = nullptr;
    if (isMuxInput) {
        auto maybeMux = ConvertMuxArgumentsToFlows(node.Cast<TPartition>().Input().template Cast<TCoMux>(), ctx);
        if (!maybeMux.IsValid()) {
            return node;
        }
        auto mux = maybeMux.Cast();
        const auto keys = GetCommonKeysFromVariantSelector(partition.KeySelectorLambda());
        TVector<TExprNode::TPtr> keyColumns;
        keyColumns.reserve(keys.size());
        for (const auto& key: keys) {
            keyColumns.push_back(ctx.NewAtom(node.Pos(), key));
        }
        TVector<TExprBase> muxArgs;
        for (auto child: mux.Input().template Cast<TExprList>()) {
            auto conn = child.template Maybe<TDqConnection>();
            if (!conn.IsValid()) {
                muxArgs.push_back(child);
                continue;
            }
            if (!IsSingleConsumerConnection(conn.Cast(), parentsMap, allowStageMultiUsage)) {
                return node;
            }
            TCoArgument programArg = Build<TCoArgument>(ctx, conn.Cast().Pos())
                .Name("arg")
                .Done();
            auto newConnection = conn.Cast();
            if (!keyColumns.empty()) {
                newConnection = Build<TDqCnHashShuffle>(ctx, node.Pos())
                    .Output()
                        .Stage(conn.Cast().Output().Stage())
                        .Index(conn.Cast().Output().Index())
                        .Build()
                    .KeyColumns()
                        .Add(keyColumns)
                        .Build()
                    .Done();
            } else if (!conn.template Maybe<TDqCnUnionAll>().IsValid()) {
                return node;
            }
            inputConns.push_back(newConnection);
            inputArgs.push_back(programArg);
            muxArgs.push_back(programArg);
        };
        newPartitionsInput = Build<TCoMux>(ctx, node.Pos())
            .template Input<TExprList>()
                .Add(muxArgs)
                .Build()
            .Done().Ptr();
    } else {
        auto dqUnion = partition.Input().template Cast<TDqCnUnionAll>();

        if (!IsSingleConsumerConnection(dqUnion, parentsMap, allowStageMultiUsage)) {
            return node;
        }

        auto keyLambda = partition.KeySelectorLambda();
        TCoLambda stageLambda = BuildIdentityLambda(node.Pos(), ctx);
        TCoLambda handlerLambda = partition.ListHandlerLambda();
        if (PrepareKeySelectorToStage(keyLambda, stageLambda, handlerLambda, ctx)) {
            auto newConn = DqPushLambdaToStageUnionAll(dqUnion, stageLambda, {}, ctx, optCtx);
            if (!newConn) {
                return node;
            }

            return Build<TPartition>(ctx, node.Pos())
                .InitFrom(partition)
                .Input(newConn.Cast())
                .KeySelectorLambda(keyLambda)
                .ListHandlerLambda(handlerLambda)
                .Done();
        }

        TDqConnection newConnection = BuildShuffleConnection(node.Pos(), keyLambda, dqUnion, ctx);
        TCoArgument programArg = Build<TCoArgument>(ctx, node.Pos())
            .Name("arg")
            .Done();
        inputConns.push_back(newConnection);
        inputArgs.push_back(programArg);
        newPartitionsInput = programArg.Ptr();
    }

    auto handler = partition.ListHandlerLambda();

    if constexpr(std::is_base_of<TCoPartitionsByKeys, TPartition>::value) {
        if (ETypeAnnotationKind::List == partition.Input().Ref().GetTypeAnn()->GetKind()) {
            handler = Build<TCoLambda>(ctx, handler.Pos())
                .Args({"flow"})
                .template Body<TCoToFlow>()
                    .template Input<TExprApplier>()
                        .Apply(handler)
                        .template With<TCoForwardList>(0)
                            .Stream("flow")
                        .Build()
                    .Build()
                .Build().Done();
        }
    }

    auto partitionStage = Build<TDqStage>(ctx, node.Pos())
        .Inputs()
            .Add(inputConns)
            .Build()
        .Program()
            .Args(inputArgs)
            .Body<TPartition>()
                .Input(newPartitionsInput)
                .KeySelectorLambda(ctx.DeepCopyLambda(partition.KeySelectorLambda().Ref()))
                .SortDirections(partition.SortDirections())
                .SortKeySelectorLambda(partition.SortKeySelectorLambda().template Maybe<TCoLambda>()
                    ? ctx.DeepCopyLambda(partition.SortKeySelectorLambda().Ref())
                    : partition.SortKeySelectorLambda().Ptr())
                .ListHandlerLambda()
                    .Args({"list"})
                    .template Body<TExprApplier>()
                        .Apply(handler)
                            .With(handler.Args().Arg(0), "list")
                        .Build()
                    .Build()
                .Build()
            .Build()
        .Settings(TDqStageSettings().SetPartitionMode(TDqStageSettings::EPartitionMode::Aggregate).BuildNode(ctx, node.Pos()))
        .Done();

    return Build<TDqCnUnionAll>(ctx, node.Pos())
        .Output()
            .Stage(partitionStage)
            .Index().Build("0")
            .Build()
        .Done();
}

template <typename TMembersFilter>
TExprBase DqPushMembersFilterToStage(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx,
    const TParentsMap& parentsMap, bool allowStageMultiUsage)
{
    if (!node.Maybe<TMembersFilter>().Input().template Maybe<TDqCnUnionAll>()) {
        return node;
    }

    auto filter = node.Cast<TMembersFilter>();
    auto dqUnion = filter.Input().template Cast<TDqCnUnionAll>();

    if (!IsSingleConsumerConnection(dqUnion, parentsMap, allowStageMultiUsage)) {
        return node;
    }

    if (auto connToPushableStage = DqBuildPushableStage(dqUnion, ctx)) {
        return TExprBase(ctx.ChangeChild(*node.Raw(), TMembersFilter::idx_Input, std::move(connToPushableStage)));
    }

    auto lambda = Build<TCoLambda>(ctx, filter.Pos())
            .Args({"stream"})
            .template Body<TMembersFilter>()
                .Input("stream")
                .Members(filter.Members())
                .Build()
            .Done();

    auto result = DqPushLambdaToStageUnionAll(dqUnion, lambda, {}, ctx, optCtx);
    if (!result) {
        return node;
    }

    return result.Cast();
}

TExprNode::TListType FindLambdaPrecomputes(const TCoLambda& lambda) {
    auto predicate = [](const TExprNode::TPtr& node) {
        return TMaybeNode<TDqPrecompute>(node).IsValid();
    };

    TExprNode::TListType result;
    VisitExpr(lambda.Body().Ptr(), [&result, &predicate] (const TExprNode::TPtr& node) {
        if (predicate(node)) {
            result.emplace_back(node);
            return false;
        }

        return true;
    });

    return result;
}

TMaybeNode<TDqStage> DqPushFlatMapInnerConnectionsToStageInput(TCoFlatMapBase& flatmap,
    TVector<NNodes::TDqConnection>&& innerConnections, const TParentsMap& parentsMap, TExprContext& ctx)
{
    auto innerPrecomputes = FindLambdaPrecomputes(flatmap.Lambda());
    if (!innerPrecomputes.empty()) {
        return {};
    }

    TVector<TDqConnection> inputs;
    TNodeOnNodeOwnedMap replaceMap;

    // prepare inputs (inner connections + flatmap input)
    inputs.reserve(innerConnections.size() + 1);
    inputs.push_back(flatmap.Input().Cast<TDqConnection>());
    for (auto& cn : innerConnections) {
        if (!cn.Maybe<TDqCnUnionAll>() && !cn.Maybe<TDqCnMerge>()) {
            return {};
        }

        if (!IsSingleConsumerConnection(cn, parentsMap, true)) {
            return {};
        }

        inputs.push_back(cn);
    }

    auto args = PrepareArgumentsReplacement(flatmap.Input(), inputs, ctx, replaceMap);
    auto newFlatMap = ctx.ReplaceNodes(flatmap.Ptr(), replaceMap);

    auto buildDqBroadcastCn = [&ctx](auto& cn) {
        auto collectStage = Build<TDqStage>(ctx, cn.Pos())
            .Inputs()
                .Add(cn)
                .Build()
            .Program()
                .Args({"stream"})
                .Body("stream")
                .Build()
            .Settings(TDqStageSettings().BuildNode(ctx, cn.Pos()))
            .Done();

        return Build<TDqCnBroadcast>(ctx, cn.Pos())
            .Output()
                .Stage(collectStage)
                .Index().Build("0")
                .Build()
            .Done();
    };

    TVector<TExprBase> stageInputs;
    stageInputs.reserve(inputs.size());
    auto mapCn = Build<TDqCnMap>(ctx, flatmap.Input().Pos())
        .Output(inputs[0].Output())
        .Done();
    stageInputs.emplace_back(std::move(mapCn));

    // gather all elements from stream inputs (skip flatmap input)
    for (ui32 inputId = 1; inputId < inputs.size(); ++inputId) {
        auto argAsList = ctx.NewArgument(inputs[inputId].Pos(), TStringBuilder() << "_dq_list_arg" << inputId);
        auto stageInput = buildDqBroadcastCn(inputs[inputId]);

        newFlatMap = Build<TCoFlatMap>(ctx, stageInput.Pos())
             .Input<TCoSqueezeToList>()
                 .Stream(args[inputId])
                 .Build()
             .Lambda()
                .Args({argAsList})
                .Body(ctx.ReplaceNode(std::move(newFlatMap), args[inputId].Ref(), argAsList))
                .Build()
             .Done().Ptr();

        stageInputs.emplace_back(std::move(stageInput));
    }

    return Build<TDqStage>(ctx, flatmap.Pos())
        .Inputs()
            .Add(std::move(stageInputs))
            .Build()
        .Program()
            .Args(args)
            .Body(newFlatMap)
            .Build()
        .Settings(TDqStageSettings().BuildNode(ctx, flatmap.Pos()))
        .Done();
}

TMaybeNode<TDqStage> DqPushLambdasToStage(const TDqStage& stage, const std::map<ui32, TCoLambda>& lambdas,
    const TVector<TDqConnection>& lambdaInputs, TExprContext& ctx, IOptimizationContext& optCtx)
{
    auto program = stage.Program();
    ui32 branchesCount = GetStageOutputsCount(stage);

    TExprNode::TPtr newProgram;
    if (branchesCount == 1) {
        newProgram = ctx.FuseLambdas(lambdas.at(0U).Ref(), program.Ref());
    } else {
        auto dqReplicate = program.Body().Cast<TDqReplicate>();

        TNodeOnNodeOwnedMap argReplaces;
        auto newArgs = PrepareArgumentsReplacement(program.Args(), {}, ctx, argReplaces);

//        for (auto& x : argReplaces) {
//            YQL_CLOG(TRACE, CoreDq) << "-- replace arg #" << x.first->UniqueId() << " -> #" << x.second->UniqueId();
//        }

        TVector<TExprNode::TPtr> newBranchLambdas;
        newBranchLambdas.reserve(branchesCount);

        for (size_t i = 0; i < branchesCount; ++i) {
            auto branchLambda = dqReplicate.Arg(/* input */ 1 + i).Cast<TCoLambda>();
            YQL_ENSURE(branchLambda.Args().Size() == 1);

            TExprNode::TPtr newBranchProgram;
            if (const auto it = lambdas.find(i); lambdas.cend() != it) {
                newBranchProgram = ctx.FuseLambdas(it->second.Ref(), branchLambda.Ref());
            } else {
                newBranchProgram = ctx.DeepCopyLambda(branchLambda.Ref());
            }

            newBranchLambdas.emplace_back(ctx.ReplaceNodes(std::move(newBranchProgram), argReplaces));
        }

        newProgram = Build<TCoLambda>(ctx, dqReplicate.Pos())
            .Args(newArgs)
            .Body<TDqReplicate>()
                .Input(ctx.ReplaceNodes(dqReplicate.Input().Ptr(), argReplaces))
                .FreeArgs()
                    .Add(newBranchLambdas)
                    .Build()
                .Build()
            .Done().Ptr();

        // YQL_CLOG(TRACE, CoreDq) << "-- newProgram: " << newProgram->Dump();
    }

    TNodeOnNodeOwnedMap inputArgReplaces;
    TVector<TCoArgument> newArgs = PrepareArgumentsReplacement(TCoLambda(newProgram).Args(), lambdaInputs, ctx, inputArgReplaces);
    TVector<TExprBase> inputNodes;

    inputNodes.reserve(newArgs.size());
    inputNodes.insert(inputNodes.end(), stage.Inputs().begin(), stage.Inputs().end());
    inputNodes.insert(inputNodes.end(), lambdaInputs.begin(), lambdaInputs.end());

    YQL_ENSURE(newArgs.size() == inputNodes.size(), "" << newArgs.size() << " != " << inputNodes.size());

    auto newStage = Build<TDqStage>(ctx, stage.Pos())
            .Inputs()
                .Add(inputNodes)
            .Build()
            .Program()
                .Args(newArgs)
                .Body(ctx.ReplaceNodes(newProgram->TailPtr(), inputArgReplaces))
            .Build()
            .Settings(TDqStageSettings().BuildNode(ctx, stage.Pos()))
            .Done();

    optCtx.RemapNode(stage.Ref(), newStage.Ptr());

    return newStage;
}

} // namespace

TMaybeNode<TDqStage> DqPushLambdaToStage(const TDqStage& stage, const TCoAtom& outputIndex, const TCoLambda& lambda,
    const TVector<TDqConnection>& lambdaInputs, TExprContext& ctx, IOptimizationContext& optCtx)
{
    YQL_CLOG(TRACE, CoreDq) << "stage #" << stage.Ref().UniqueId() << ": " << PrintDqStageOnly(stage, ctx)
        << ", add lambda to output #" << outputIndex.Value();

    if (IsDqDependsOnStage(lambda, stage)) {
        YQL_CLOG(TRACE, CoreDq) << "Lambda " << lambda.Ref().Dump() << " depends on stage: " << PrintDqStageOnly(stage, ctx);
        return {};
    }

    const auto index = FromString<ui32>(outputIndex.Value());
    return DqPushLambdasToStage(stage, {{index, lambda}}, lambdaInputs, ctx, optCtx);
}

TExprNode::TPtr DqBuildPushableStage(const NNodes::TDqConnection& connection, TExprContext& ctx) {
    auto stage = connection.Output().Stage().Cast<TDqStage>();
    auto program = stage.Program();
    if (GetStageOutputsCount(stage) < 2 || program.Body().Maybe<TDqReplicate>()) {
        return {};
    }

    auto newStage = Build<TDqStage>(ctx, stage.Pos())
        .Inputs()
            .Add(connection)
            .Build()
        .Program()
            .Args({"arg"})
            .Body("arg")
            .Build()
        .Settings(TDqStageSettings().BuildNode(ctx, stage.Pos()))
        .Done();

    auto output = Build<TDqOutput>(ctx, connection.Pos())
        .Stage(newStage)
        .Index().Build(BuildAtom("0", connection.Output().Index().Pos(), ctx))
        .Done();

    return ctx.ChangeChild(connection.Ref(), TDqConnection::idx_Output, output.Ptr());
}

TMaybeNode<TDqConnection> DqPushLambdaToStageUnionAll(const TDqConnection& connection, const TCoLambda& lambda,
    const TVector<TDqConnection>& lambdaInputs, TExprContext& ctx, IOptimizationContext& optCtx)
{
    auto stage = connection.Output().Stage().Cast<TDqStage>();
    auto newStage = DqPushLambdaToStage(stage, connection.Output().Index(), lambda, lambdaInputs, ctx, optCtx);
    if (!newStage) {
        return {};
    }

    auto output = Build<TDqOutput>(ctx, connection.Pos())
        .Stage(newStage.Cast())
        .Index().Build(connection.Output().Index())
        .Done();

    return TDqConnection(ctx.ChangeChild(connection.Ref(), TDqConnection::idx_Output, output.Ptr()));
}

void DqPushLambdasToStagesUnionAll(std::vector<std::pair<TDqCnUnionAll, TCoLambda>>& items, TExprContext& ctx, IOptimizationContext& optCtx)
{
    TNodeMap<std::pair<std::map<ui32, TCoLambda>, TDqStage>> map(items.size());

    for (const auto& item: items) {
        const auto& output = item.first.Output();
        const auto index = FromString<ui32>(output.Index().Value());
        const auto ins = map.emplace(output.Stage().Raw(), std::make_pair(std::map<ui32, TCoLambda>(), output.Stage().Cast<TDqStage>())).first;
        ins->second.first.emplace(index, item.second);
    }

    for (auto& item : map) {
        item.second.second = DqPushLambdasToStage(item.second.second, item.second.first, {}, ctx, optCtx).Cast();
    }

    for (auto& item: items) {
        const auto& output = item.first.Output();
        item.first = Build<TDqCnUnionAll>(ctx, item.first.Pos())
            .Output<TDqOutput>()
                .Stage(map.find(output.Stage().Raw())->second.second)
                .Index(output.Index())
                .Build()
            .Done();
    }
}

TExprBase DqPushSkipNullMembersToStage(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx,
    const TParentsMap& parentsMap, bool allowStageMultiUsage)
{
    return DqPushMembersFilterToStage<TCoSkipNullMembers>(node, ctx, optCtx, parentsMap, allowStageMultiUsage);
}

TExprBase DqPushExtractMembersToStage(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx,
    const TParentsMap& parentsMap, bool allowStageMultiUsage)
{
    return DqPushMembersFilterToStage<TCoExtractMembers>(node, ctx, optCtx, parentsMap, allowStageMultiUsage);
}

TExprBase DqBuildPureFlatmapStage(TExprBase node, TExprContext& ctx) {
    if (!node.Maybe<TCoFlatMapBase>()) {
        return node;
    }

    auto flatmap = node.Cast<TCoFlatMapBase>();

    if (!IsDqCompletePureExpr(flatmap.Input()) || !IsDqSelfContainedExpr(flatmap.Lambda())) {
        return node;
    }

    bool isPure;
    TVector<TDqConnection> innerConnections;
    FindDqConnections(flatmap.Lambda(), innerConnections, isPure);
    if (!isPure || innerConnections.empty()) {
        return node;
    }

    auto inputStage = Build<TDqStage>(ctx, flatmap.Input().Pos())
        .Inputs()
            .Build()
        .Program()
            .Args({})
            .Body<TCoIterator>()
                .List(flatmap.Input())
                .Build()
            .Build()
        .Settings(TDqStageSettings::New()
            .SetPartitionMode(TDqStageSettings::EPartitionMode::Single)
            .BuildNode(ctx, flatmap.Input().Pos()))
        .Done();

    auto inputConnection = Build<TDqCnUnionAll>(ctx, flatmap.Pos())
        .Output()
            .Stage(inputStage)
            .Index().Build("0")
            .Build()
        .Done();

    return TExprBase(ctx.ChangeChild(flatmap.Ref(), TCoFlatMapBase::idx_Input, inputConnection.Ptr()));
}

TExprBase DqBuildFlatmapStage(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx,
    const TParentsMap& parentsMap, bool allowStageMultiUsage)
{
    if (!node.Maybe<TCoFlatMapBase>().Input().Maybe<TDqCnUnionAll>()) {
        return node;
    }

    auto flatmap = node.Cast<TCoFlatMapBase>();
    if (!IsDqSelfContainedExpr(flatmap.Lambda())) {
        return node;
    }
    auto dqUnion = flatmap.Input().Cast<TDqCnUnionAll>();
    if (!IsSingleConsumerConnection(dqUnion, parentsMap, allowStageMultiUsage)) {
        return node;
    }

    bool isPure;
    TVector<TDqConnection> innerConnections;
    FindDqConnections(flatmap.Lambda(), innerConnections, isPure);
    if (!isPure) {
        return node;
    }

    TMaybeNode<TDqStage> flatmapStage;
    if (!innerConnections.empty()) {
        flatmapStage = DqPushFlatMapInnerConnectionsToStageInput(flatmap, std::move(innerConnections), parentsMap, ctx);
        if (!flatmapStage) {
            return node;
        }
    } else {
        if (auto connToPushableStage = DqBuildPushableStage(dqUnion, ctx)) {
            return TExprBase(ctx.ChangeChild(*node.Raw(), TCoFlatMapBase::idx_Input, std::move(connToPushableStage)));
        }

        auto lambda = TCoLambda(ctx.Builder(flatmap.Lambda().Pos())
            .Lambda()
                .Param("stream")
                .Callable(flatmap.Ref().Content())
                    .Arg(0, "stream")
                    .Add(1, ctx.DeepCopyLambda(flatmap.Lambda().Ref()))
                .Seal()
            .Seal().Build());

        auto pushResult = DqPushLambdaToStageUnionAll(dqUnion, lambda, {}, ctx, optCtx);
        if (pushResult) {
            return pushResult.Cast();
        }

        flatmapStage = Build<TDqStage>(ctx, flatmap.Pos())
            .Inputs()
                .Add<TDqCnMap>()
                    .Output(dqUnion.Output())
                    .Build()
                .Build()
            .Program(lambda)
            .Settings(TDqStageSettings().BuildNode(ctx, flatmap.Pos()))
            .Done();
    }

    return Build<TDqCnUnionAll>(ctx, node.Pos())
        .Output()
            .Stage(flatmapStage.Cast())
            .Index().Build("0")
            .Build()
        .Done();
}

template <typename BaseLMap>
TExprBase DqPushBaseLMapToStage(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx,
    const TParentsMap& parentsMap, bool allowStageMultiUsage = true)
{
    if (!node.Maybe<BaseLMap>().Input().template Maybe<TDqCnUnionAll>()) {
        return node;
    }

    auto lmap = node.Cast<BaseLMap>();
    auto dqUnion = lmap.Input().template Cast<TDqCnUnionAll>();
    if (!IsSingleConsumerConnection(dqUnion, parentsMap, allowStageMultiUsage)) {
        return node;
    }

    if (!CanPushDqExpr(lmap.Lambda(), dqUnion)) {
        return node;
    }

    if (auto connToPushableStage = DqBuildPushableStage(dqUnion, ctx)) {
        return TExprBase(ctx.ChangeChild(*node.Raw(), BaseLMap::idx_Input, std::move(connToPushableStage)));
    }

    auto lambda = Build<TCoLambda>(ctx, lmap.Lambda().Pos())
        .Args({"arg"})
        .template Body<TCoToFlow>()
            .template Input<TExprApplier>()
                .Apply(lmap.Lambda())
                .template With<TCoFromFlow>(0)
                    .Input("arg")
                .Build()
            .Build()
        .Build()
        .Done();

    auto result = DqPushLambdaToStageUnionAll(dqUnion, lambda, {}, ctx, optCtx);
    if (!result) {
        return node;
    }

    const auto& lmapItemTy = GetSeqItemType(*lmap.Ref().GetTypeAnn());
    if (lmapItemTy.GetKind() == ETypeAnnotationKind::Variant) {
        // preserve typing by Mux'ing several stage outputs into one
        const auto variantItemTy = lmapItemTy.template Cast<TVariantExprType>();
        const auto stageOutputNum = variantItemTy->GetUnderlyingType()->template Cast<TTupleExprType>()->GetSize();
        TVector<TExprBase> muxParts;
        muxParts.reserve(stageOutputNum);
        for (auto i = 0U; i < stageOutputNum; i++) {
            const auto muxPart = Build<TDqCnUnionAll>(ctx, lmap.Lambda().Pos())
                .Output()
                    .Stage(result.Output().Stage().Cast())
                    .Index().Build(i)
                .Build()
                .Done();
            muxParts.emplace_back(muxPart);
        }
        return Build<TCoMux>(ctx, result.Cast().Pos())
            .template Input<TExprList>()
                .Add(muxParts)
            .Build()
            .Done();
    }
    return result.Cast();
}

TExprBase DqPushOrderedLMapToStage(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx,
    const TParentsMap& parentsMap, bool allowStageMultiUsage)
{
    return DqPushBaseLMapToStage<TCoOrderedLMap>(node, ctx, optCtx, parentsMap, allowStageMultiUsage);
}

TExprBase DqPushLMapToStage(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx,
    const TParentsMap& parentsMap, bool allowStageMultiUsage)
{
    return DqPushBaseLMapToStage<TCoLMap>(node, ctx, optCtx, parentsMap, allowStageMultiUsage);
}

template <typename BaseLMap>
TExprBase DqBuildLMapOverMuxStageStub(TExprBase node, TExprContext& ctx, NYql::IOptimizationContext&, const NYql::TParentsMap& parentsMap) {
    if (!node.Maybe<BaseLMap>()) {
        return node;
    }
    auto lmap = node.Cast<BaseLMap>();
    auto maybeMux = lmap.Input().template Maybe<TCoMux>();
    if (!maybeMux.IsValid()) {
        return node;
    }
    maybeMux = ConvertMuxArgumentsToFlows(maybeMux.Cast(), ctx);
    if (!maybeMux.IsValid()) {
        return node;
    }
    auto mux = maybeMux.Cast();
    const TTypeAnnotationNode* listItemType = GetSeqItemType(node.Ref().GetTypeAnn());
    if (!listItemType) {
        return node;
    }
    // dq splits this type of lambda output into separate stage outputs
    // thus it's impossible to maintain 'node' typing (muxing them ain't an option, cause the only purpose of this optimizer is to push original Mux to the stage)
    if (listItemType->GetKind() == ETypeAnnotationKind::Variant) {
        return node;
    }

    if (!IsDqCompletePureExpr(lmap.Lambda())) {
        return node;
    }

    YQL_CLOG(DEBUG, CoreDq) << "DqBuildLMapOverMuxStage";
    TVector<TCoArgument> inputArgs;
    TVector<TExprBase> inputConns;
    TVector<TExprBase> muxArgs;
    for (auto child: mux.Input().template Cast<TExprList>()) {
        auto conn = child.template Maybe<TDqConnection>();
        if (!conn.IsValid()) {
            muxArgs.push_back(child);
            continue;
        }
        if (!IsSingleConsumerConnection(conn.Cast(), parentsMap)) {
            return node;
        }
        TCoArgument programArg = Build<TCoArgument>(ctx, conn.Cast().Pos())
            .Name("arg")
            .Done();
        inputConns.push_back(conn.Cast());
        inputArgs.push_back(programArg);
        muxArgs.push_back(programArg);
    };

    auto newMux = Build<TCoMux>(ctx, lmap.Input().Pos())
            .template Input<TExprList>()
            .Add(muxArgs)
            .Build()
        .Done().Ptr();
    auto lmapLambda = ctx.DeepCopyLambda(lmap.Lambda().Ref());
    Y_ABORT_UNLESS(lmapLambda->Child(0)->ChildrenSize() == 1, "unexpected number of arguments in lmap lambda");
    auto newBody = ctx.ReplaceNodes(lmapLambda->Child(1), {{lmapLambda->Child(0)->Child(0), newMux}});
    auto lmapStage = Build<TDqStage>(ctx, lmap.Pos())
        .Inputs()
            .Add(inputConns)
            .Build()
        .Program()
            .Args(inputArgs)
            .Body(newBody)
            .Build()
        .Settings(TDqStageSettings().BuildNode(ctx, node.Pos()))
        .Done();

    return Build<TDqCnUnionAll>(ctx, node.Pos())
        .Output()
            .Stage(lmapStage)
            .Index().Build("0")
            .Build()
        .Done();
}

TExprBase DqBuildOrderedLMapOverMuxStage(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx,
    const TParentsMap& parentsMap)
{
    return DqBuildLMapOverMuxStageStub<TCoOrderedLMap>(node, ctx, optCtx, parentsMap);
}

TExprBase DqBuildLMapOverMuxStage(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx,
    const TParentsMap& parentsMap)
{
    return DqBuildLMapOverMuxStageStub<TCoLMap>(node, ctx, optCtx, parentsMap);
}

TExprBase DqPushCombineToStage(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx,
    const TParentsMap& parentsMap, bool allowStageMultiUsage)
{
    if (!node.Maybe<TCoCombineByKey>().Input().Maybe<TDqCnUnionAll>()) {
        return node;
    }

    auto combine = node.Cast<TCoCombineByKey>();
    auto dqUnion = combine.Input().Cast<TDqCnUnionAll>();
    if (!IsSingleConsumerConnection(dqUnion, parentsMap, allowStageMultiUsage)) {
        return node;
    }

    if (!IsDqCompletePureExpr(combine.PreMapLambda()) ||
        !IsDqCompletePureExpr(combine.KeySelectorLambda()) ||
        !IsDqCompletePureExpr(combine.InitHandlerLambda()) ||
        !IsDqCompletePureExpr(combine.UpdateHandlerLambda()) ||
        !IsDqCompletePureExpr(combine.FinishHandlerLambda()))
    {
        return node;
    }

    if (auto connToPushableStage = DqBuildPushableStage(dqUnion, ctx)) {
        return TExprBase(ctx.ChangeChild(*node.Raw(), TCoCombineByKey::idx_Input, std::move(connToPushableStage)));
    }

    auto lambda = Build<TCoLambda>(ctx, combine.Pos())
            .Args({"stream"})
            .Body<TCoCombineByKey>()
                .Input("stream")
                .PreMapLambda(ctx.DeepCopyLambda(combine.PreMapLambda().Ref()))
                .KeySelectorLambda(ctx.DeepCopyLambda(combine.KeySelectorLambda().Ref()))
                .InitHandlerLambda(ctx.DeepCopyLambda(combine.InitHandlerLambda().Ref()))
                .UpdateHandlerLambda(ctx.DeepCopyLambda(combine.UpdateHandlerLambda().Ref()))
                .FinishHandlerLambda(ctx.DeepCopyLambda(combine.FinishHandlerLambda().Ref()))
                .Build()
            .Done();

    if (HasContextFuncs(*lambda.Ptr())) {
        lambda = Build<TCoLambda>(ctx, combine.Pos())
            .Args({ TStringBuf("stream") })
            .Body<TCoWithContext>()
                .Input<TExprApplier>()
                    .Apply(lambda)
                    .With(0, TStringBuf("stream"))
                .Build()
                .Name()
                    .Value("Agg")
                .Build()
            .Build()
            .Done();
    }

    if (IsDqDependsOnStage(combine.PreMapLambda(), dqUnion.Output().Stage()) ||
        IsDqDependsOnStage(combine.KeySelectorLambda(), dqUnion.Output().Stage()) ||
        IsDqDependsOnStage(combine.InitHandlerLambda(), dqUnion.Output().Stage()) ||
        IsDqDependsOnStage(combine.UpdateHandlerLambda(), dqUnion.Output().Stage()) ||
        IsDqDependsOnStage(combine.FinishHandlerLambda(), dqUnion.Output().Stage()))
    {
        return Build<TDqCnUnionAll>(ctx, combine.Pos())
            .Output()
                .Stage<TDqStage>()
                    .Inputs()
                        .Add(dqUnion)
                        .Build()
                    .Program(lambda)
                    .Settings(TDqStageSettings().BuildNode(ctx, node.Pos()))
                    .Build()
                .Index().Build("0")
                .Build()
            .Done();
    }

    auto result = DqPushLambdaToStageUnionAll(dqUnion, lambda, {}, ctx, optCtx);
    if (!result) {
        return node;
    }

    return result.Cast();
}

NNodes::TExprBase DqPushAggregateCombineToStage(NNodes::TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx,
    const TParentsMap& parentsMap, bool allowStageMultiUsage)
{
    if (!node.Maybe<TCoAggregateCombine>().Input().Maybe<TDqCnUnionAll>()) {
        return node;
    }

    auto aggCombine = node.Cast<TCoAggregateCombine>();
    auto dqUnion = aggCombine.Input().Cast<TDqCnUnionAll>();
    if (!IsSingleConsumerConnection(dqUnion, parentsMap, allowStageMultiUsage)) {
        return node;
    }

    auto lambda = Build<TCoLambda>(ctx, aggCombine.Pos())
            .Args({"stream"})
            .Body<TCoAggregateCombine>()
                .Input("stream")
                .Keys(aggCombine.Keys())
                .Handlers(aggCombine.Handlers())
                .Settings(aggCombine.Settings())
                .Build()
            .Done();

    if (HasContextFuncs(*lambda.Ptr())) {
        lambda = Build<TCoLambda>(ctx, aggCombine.Pos())
            .Args({ TStringBuf("stream") })
            .Body<TCoWithContext>()
                .Input<TExprApplier>()
                    .Apply(lambda)
                    .With(0, TStringBuf("stream"))
                .Build()
                .Name()
                    .Value("Agg")
                .Build()
            .Build()
        .Done();
    }

    auto result = DqPushLambdaToStageUnionAll(dqUnion, lambda, {}, ctx, optCtx);
    if (!result) {
        return node;
    }

    return result.Cast();
}

TExprBase DqBuildPartitionsStage(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx,
    const TParentsMap& parentsMap, bool allowStageMultiUsage)
{
    return DqBuildPartitionsStageStub<TCoPartitionsByKeys>(std::move(node), ctx, optCtx, parentsMap, allowStageMultiUsage);
}

TExprBase DqBuildPartitionStage(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx,
    const TParentsMap& parentsMap, bool allowStageMultiUsage)
{
    return DqBuildPartitionsStageStub<TCoPartitionByKey>(std::move(node), ctx, optCtx, parentsMap, allowStageMultiUsage);
}

TExprBase DqBuildShuffleStage(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx,
    const TParentsMap& parentsMap, bool allowStageMultiUsage)
{
    auto shuffleInput = node.Maybe<TCoShuffleByKeys>().Input();
    if (!shuffleInput.Maybe<TDqCnUnionAll>()) {
        return node;
    }

    auto shuffle = node.Cast<TCoShuffleByKeys>();
    if (!IsDqCompletePureExpr(shuffle.KeySelectorLambda()) ||
        !IsDqCompletePureExpr(shuffle.ListHandlerLambda()))
    {
        return node;
    }

    auto dqUnion = shuffle.Input().Cast<TDqCnUnionAll>();

    if (!IsSingleConsumerConnection(dqUnion, parentsMap, allowStageMultiUsage)) {
        return node;
    }

    auto keyLambda = shuffle.KeySelectorLambda();
    TCoLambda stageLambda = BuildIdentityLambda(node.Pos(), ctx);
    TCoLambda handlerLambda = shuffle.ListHandlerLambda();
    if (PrepareKeySelectorToStage(keyLambda, stageLambda, handlerLambda, ctx)) {
        auto newConn = DqPushLambdaToStageUnionAll(dqUnion, stageLambda, {}, ctx, optCtx);
        if (!newConn) {
            return node;
        }

        return Build<TCoShuffleByKeys>(ctx, node.Pos())
            .Input(newConn.Cast())
            .KeySelectorLambda(keyLambda)
            .ListHandlerLambda(handlerLambda)
            .Done();
    }

    auto connection = BuildShuffleConnection(node.Pos(), keyLambda, dqUnion, ctx);
    TCoArgument programArg = Build<TCoArgument>(ctx, node.Pos())
        .Name("arg")
        .Done();

    TVector<TCoArgument> inputArgs;
    TVector<TExprBase> inputConns;

    inputConns.push_back(connection);
    inputArgs.push_back(programArg);

    auto handler = shuffle.ListHandlerLambda();

    auto shuffleStage = Build<TDqStage>(ctx, node.Pos())
        .Inputs()
            .Add(inputConns)
            .Build()
        .Program()
            .Args(inputArgs)
            .Body<TCoToStream>()
                .Input<TExprApplier>()
                    .Apply(handler)
                    .With(handler.Args().Arg(0), programArg)
                    .Build()
                .Build()
            .Build()
        .Settings(TDqStageSettings().SetPartitionMode(TDqStageSettings::EPartitionMode::Aggregate).BuildNode(ctx, node.Pos()))
        .Done();

    return Build<TDqCnUnionAll>(ctx, node.Pos())
        .Output()
            .Stage(shuffleStage)
            .Index().Build("0")
            .Build()
        .Done();
}

NNodes::TExprBase DqBuildHashShuffleByKeyStage(NNodes::TExprBase node, TExprContext& ctx, const TParentsMap& /*parentsMap*/) {
    if (!node.Maybe<TDqCnHashShuffle>()) {
        return node;
    }
    auto cnHash = node.Cast<TDqCnHashShuffle>();
    auto stage = cnHash.Output().Stage();
    if (!stage.Program().Body().Maybe<TCoExtend>()) {
        return node;
    }
    auto extend = stage.Program().Body().Cast<TCoExtend>();
    TNodeSet nodes;
    for (auto&& i : stage.Program().Args()) {
        nodes.emplace(i.Raw());
    }
    for (auto&& i : extend) {
        if (nodes.erase(i.Raw()) != 1) {
            return node;
        }
    }
    if (!nodes.empty()) {
        return node;
    }
    TExprNode::TListType nodesTuple;
    for (auto&& i : stage.Inputs()) {
        if (!i.Maybe<TDqCnUnionAll>()) {
            return node;
        }
        auto uAll = i.Cast<TDqCnUnionAll>();
        nodesTuple.emplace_back(ctx.ChangeChild(node.Ref(), 0, uAll.Output().Ptr()));
    }
    auto stageCopy = ctx.ChangeChild(stage.Ref(), 0, ctx.NewList(node.Pos(), std::move(nodesTuple)));
    auto output =
        Build<TDqOutput>(ctx, node.Pos())
            .Stage(stageCopy)
            .Index().Build("0")
        .Done();
    auto outputCnMap =
        Build<TDqCnMap>(ctx, node.Pos())
            .Output(output)
        .Done();

    return TExprBase(outputCnMap);
}

TExprBase DqBuildFinalizeByKeyStage(TExprBase node, TExprContext& ctx,
    const TParentsMap& parentsMap, bool allowStageMultiUsage)
{
    auto finalizeInput = node.Maybe<TCoFinalizeByKey>().Input();
    if (!finalizeInput.Maybe<TDqCnUnionAll>()) {
        return node;
    }

    auto finalize = node.Cast<TCoFinalizeByKey>();
    auto dqUnion = finalize.Input().Cast<TDqCnUnionAll>();

    if (!IsSingleConsumerConnection(dqUnion, parentsMap, allowStageMultiUsage)) {
        return node;
    }

    auto keyLambda = finalize.KeySelectorLambda();

    TVector<TCoArgument> inputArgs;
    TVector<TExprBase> inputConns;

    inputConns.push_back(dqUnion);

    auto finalizeStage = Build<TDqStage>(ctx, node.Pos())
        .Inputs()
            .Add(inputConns)
            .Build()
        .Program()
            .Args({ "input" })
            .Body<TCoToStream>()
                .Input<TCoFinalizeByKey>()
                    .Input("input")
                    .PreMapLambda(finalize.PreMapLambda())
                    .KeySelectorLambda(finalize.KeySelectorLambda())
                    .InitHandlerLambda(finalize.InitHandlerLambda())
                    .UpdateHandlerLambda(finalize.UpdateHandlerLambda())
                    .FinishHandlerLambda(finalize.FinishHandlerLambda())
                    .Build()
                .Build()
            .Build()
        .Settings(TDqStageSettings().BuildNode(ctx, node.Pos()))
        .Done();

    return Build<TDqCnUnionAll>(ctx, node.Pos())
        .Output()
            .Stage(finalizeStage)
            .Index().Build("0")
            .Build()
        .Done();
}

/*
 * Optimizer rule which handles a switch to scalar expression context for aggregation results.
 * This switch happens for full aggregations, such as @code select sum(column) from table @endcode).
 * The result of such aggregations has a following expression pattern:
 * @code
 *   (AsList (AsStruct '((<StructMember>)+) ...)
 *   StructMember_i := function_i(ToOptional (TDqCnUnionAll ...))
 * @endcode
 *
 * Each list item (AsStruct) represents a single aggregation result.
 * If for all `i` function `function_i` depends on single connection via `ToOptional` callable
 * then we do the following stuff:
 *
 * For each list item create a new stage with `Condense` callable:
 *
 * 1. with `initState` (it is the _default_ _value_ in case of empty input stream)
 * @code
 *   emptyList := EmptyList of type `AsStruct` from the snippet above
 *   initState := Apply `AsStruct` emptyList
 * @endcode
 *
 * 2. with `updateHandler`
 * @code
 *   updateHandler := `AsStruct` callable with all `ToOptional (DqCnUnionAll))` replaced with input state argument
 * @endocde
 *
 * If there are more than a single list item, create a separate stage to merge all the results from previous stages.
 */

// TODO: Creating a separate stage for each AsList element is redundant, it's better to use
//       a single stage with something like MultiCondense here.

// TODO: The way such context switch is presented in the expression graph is very implicit, so it is
//       hard to work with. We should consider making it more explicit, something like ProcessScalar on the
//       top level of expression graph.

TExprBase DqBuildAggregationResultStage(TExprBase node, TExprContext& ctx, IOptimizationContext&) {
    if (!node.Maybe<TCoAsList>()) {
        return node;
    }

    const auto asList = node.Cast<TCoAsList>();

    TVector<TExprBase> resultConnections;
    for (const auto& listItem : asList) {
        if (!listItem.Maybe<TCoAsStruct>()) {
            return node;
        }

        const auto asStruct = listItem.Cast<TCoAsStruct>();

        TExprNode::TPtr connection;
        bool hasDirectConnection = false;
        bool dependsOnManyConnections = false;
        bool valueConnection = false;

        VisitExpr(asStruct.Ptr(), [&](const TExprNode::TPtr& exprPtr) {
            // Do not try to visit any other nodes, it is useless.
            if (hasDirectConnection || dependsOnManyConnections) {
                return false;
            }

            TExprBase expr{exprPtr};

            if (expr.Maybe<TCoToOptional>().List().Maybe<TDqCnUnionAll>()) {
                if (connection && (connection != expr.Cast<TCoToOptional>().List().Ptr())) {
                    dependsOnManyConnections = true;
                    return false;
                }

                connection = expr.Cast<TCoToOptional>().List().Ptr();
                return false;
            }

            if (expr.Maybe<TCoHead>().Input().Maybe<TDqCnUnionAll>()) {
                if (connection && (connection != expr.Cast<TCoHead>().Input().Ptr())) {
                    dependsOnManyConnections = true;
                    return false;
                }

                connection = expr.Cast<TCoHead>().Input().Ptr();
                return false;
            }

            if (expr.Maybe<TDqPhyPrecompute>().IsValid()) {
                auto precompute = expr.Cast<TDqPhyPrecompute>();
                auto maybeConnection = precompute.Connection().Maybe<TDqCnValue>();

                // Here we should catch only TDqPhyPrecompute(DqCnValue)
                if (!maybeConnection.IsValid()) {
                    return true;
                }

                if (connection && (connection != maybeConnection.Cast().Ptr())) {
                    dependsOnManyConnections = true;
                    return false;
                }

                connection = precompute.Ptr();
                valueConnection = true;
                return false;
            }

            if (expr.Maybe<TDqConnection>()) {
                hasDirectConnection = true;
                return false;
            }

            return true;
        });

        if (!connection) {
            return node;
        }

        if (hasDirectConnection || dependsOnManyConnections) {
            return node;
        }

        const auto pos = listItem.Pos();
        auto newArg = ctx.NewArgument(pos, "result");
        auto lambda = ctx.NewLambda(pos,
            ctx.NewArguments(pos, {newArg}),
            ctx.ReplaceNode(asStruct.Ptr(), *connection, std::move(newArg))
        );
        auto programArg = TCoArgument(ctx.NewArgument(pos, "stage_lambda_arg"));
        TExprNode::TPtr mapInput;

        if (valueConnection) {
            // DqCnValue send only one element, need to convert it to stream
            mapInput = Build<TCoToStream>(ctx, pos)
                .Input<TCoAsList>()
                    .Add(programArg)
                    .Build()
                .Done().Ptr();
        } else {
            // Input came from UnionAll, thus need to gather all elements
            mapInput = Build<TCoCondense>(ctx, pos)
                .Input(programArg)
                .State<TCoList>()
                    .ListType(ExpandType(pos, *connection->GetTypeAnn(), ctx))
                    .Build()
                .SwitchHandler()
                    .Args({"item", "stub"})
                    .Body(MakeBool<false>(pos, ctx))
                    .Build()
                .UpdateHandler()
                    .Args({"item", "stub"})
                    .Body<TCoAsList>()
                        .Add("item")
                        .Build()
                    .Build()
                .Done().Ptr();
        }

        auto resultConnection = Build<TDqCnUnionAll>(ctx, pos)
            .Output()
                .Stage<TDqStage>()
                    .Inputs()
                        .Add(std::move(connection))
                        .Build()
                    .Program()
                        .Args(programArg)
                        .Body<TCoMap>()
                            .Input(mapInput)
                            .Lambda(std::move(lambda))
                            .Build()
                        .Build()
                    .Settings(TDqStageSettings().BuildNode(ctx, pos))
                    .Build()
                .Index().Build("0")
                .Build()
            .Done();

        resultConnections.push_back(resultConnection);
    }

    YQL_ENSURE(!resultConnections.empty());
    if (resultConnections.size() == 1) {
        return resultConnections[0];
    }

    TVector<TCoArgument> inputArgs;
    TVector<TExprBase> extendArgs;
    inputArgs.reserve(resultConnections.size());
    for (ui32 i = 0; i < resultConnections.size(); ++i) {
        TCoArgument inputArg = Build<TCoArgument>(ctx, node.Pos())
            .Name("input" + ToString(i))
            .Done();
        inputArgs.push_back(inputArg);
        extendArgs.push_back(inputArg);
    }

    auto unionStage = Build<TDqStage>(ctx, node.Pos())
        .Inputs()
            .Add(resultConnections)
            .Build()
        .Program()
            .Args(inputArgs)
            .Body<TCoExtend>() // TODO: check Extend effectiveness
                .Add(extendArgs)
                .Build()
            .Build()
        .Settings(TDqStageSettings().BuildNode(ctx, node.Pos()))
        .Done();

    return Build<TDqCnUnionAll>(ctx, node.Pos())
        .Output()
            .Stage(unionStage)
            .Index().Build("0")
            .Build()
        .Done();
}

namespace {
template <typename TBuilder>
bool AddSortColumn(const TExprBase& key, const TExprBase& ascending, TExprContext& ctx, const TExprBase& node,
    const TCoLambda& sortKeySelector, TBuilder& sortColumnList, TVector<const TTypeAnnotationNode*>& sortKeyTypes)
{
    if (!key.Maybe<TCoMember>() || !ascending.Maybe<TCoBool>()) {
        return false;
    }

    auto member = key.Cast<TCoMember>();
    if (member.Struct().Raw() != sortKeySelector.Args().Arg(0).Raw()) {
        return false;
    }

    sortKeyTypes.push_back(member.Ref().GetTypeAnn());
    bool isAsc = FromString<bool>(ascending.Cast<TCoBool>().Literal().Value());
    sortColumnList.template Add<TDqSortColumn>()
        .Column(member.Name())
        .SortDirection(Build<TCoAtom>(ctx, node.Pos())
            .Value(isAsc ? TTopSortSettings::AscendingSort : TTopSortSettings::DescendingSort).Done()
        )
        .Build();
    return true;
}

TExprBase GetSortDirection(const TExprBase& sortDirections, size_t index) {
    TExprNode::TPtr sortDirection;
    if (sortDirections.Maybe<TExprList>()) {
        YQL_ENSURE(index < sortDirections.Cast<TExprList>().Size());
        sortDirection = sortDirections.Cast<TExprList>().Item(index).Ptr();
    } else {
        sortDirection = sortDirections.Ptr();
    }
    return TExprBase(sortDirection);
}
} // End of anonymous namespace

TExprBase DqBuildTopStage(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx,
    const TParentsMap& parentsMap, bool allowStageMultiUsage)
{
    if (!node.Maybe<TCoTop>().Input().Maybe<TDqCnUnionAll>()) {
        return node;
    }

    const auto top = node.Cast<TCoTop>();
    const auto dqUnion = top.Input().Cast<TDqCnUnionAll>();
    if (!IsSingleConsumerConnection(dqUnion, parentsMap, allowStageMultiUsage)) {
        return node;
    }

    if (!CanPushDqExpr(top.Count(), dqUnion) || !CanPushDqExpr(top.KeySelectorLambda(), dqUnion)) {
        return node;
    }

    if (auto connToPushableStage = DqBuildPushableStage(dqUnion, ctx)) {
        return TExprBase(ctx.ChangeChild(*node.Raw(), TCoTop::idx_Input, std::move(connToPushableStage)));
    }

    const auto result = dqUnion.Output().Stage().Program().Body();

    const auto sortKeySelector = top.KeySelectorLambda();
    const auto sortDirections = top.SortDirections();
    const auto lambda = Build<TCoLambda>(ctx, top.Pos())
            .Args({"stream"})
            .Body<TCoTop>()
                .Input("stream")
                .KeySelectorLambda(ctx.DeepCopyLambda(top.KeySelectorLambda().Ref()))
                .SortDirections(sortDirections)
                .Count(top.Count())
                .Build()
            .Done();

    const auto stage = dqUnion.Output().Stage().Cast<TDqStage>();
    const auto newStage = DqPushLambdaToStage(stage, dqUnion.Output().Index(), lambda, {}, ctx, optCtx);
    if (!newStage) {
        return node;
    }

    return Build<TDqCnUnionAll>(ctx, node.Pos())
        .Output()
            .Stage<TDqStage>()
                .Inputs()
                    .Add<TDqCnUnionAll>()
                        .Output()
                            .Stage(newStage.Cast())
                            .Index(dqUnion.Output().Index())
                            .Build()
                        .Build()
                    .Build()
                .Program()
                    .Args({"stream"})
                    .Body<TCoTop>()
                        .Input("stream")
                        .KeySelectorLambda(ctx.DeepCopyLambda(top.KeySelectorLambda().Ref()))
                        .SortDirections(top.SortDirections())
                        .Count(top.Count())
                        .Build()
                    .Build()
                .Settings(TDqStageSettings().BuildNode(ctx, top.Pos()))
                .Build()
            .Index().Build(0U)
            .Build()
        .Done();
}

TExprBase DqBuildTopSortStage(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx,
    const TParentsMap& parentsMap, bool allowStageMultiUsage)
{
    if (!node.Maybe<TCoTopSort>().Input().Maybe<TDqCnUnionAll>()) {
        return node;
    }

    auto topSort = node.Cast<TCoTopSort>();
    auto dqUnion = topSort.Input().Cast<TDqCnUnionAll>();
    if (!IsSingleConsumerConnection(dqUnion, parentsMap, allowStageMultiUsage)) {
        return node;
    }

    if (!CanPushDqExpr(topSort.Count(), dqUnion) || !CanPushDqExpr(topSort.KeySelectorLambda(), dqUnion)) {
        return node;
    }

    if (auto connToPushableStage = DqBuildPushableStage(dqUnion, ctx)) {
        return TExprBase(ctx.ChangeChild(*node.Raw(), TCoTopSort::idx_Input, std::move(connToPushableStage)));
    }

    const auto result = dqUnion.Output().Stage().Program().Body();

    const auto sortKeySelector = topSort.KeySelectorLambda();
    const auto sortDirections = topSort.SortDirections();

    bool canMerge = true;
    auto sortColumnList = Build<TDqSortColumnList>(ctx, node.Pos());
    TVector<const TTypeAnnotationNode*> sortKeyTypes;

    auto lambdaBody = sortKeySelector.Body();
    if (auto maybeColTuple = lambdaBody.Maybe<TExprList>()) {
        auto tuple = maybeColTuple.Cast();
        sortKeyTypes.reserve(tuple.Size());

        for (size_t i = 0; i < tuple.Size(); ++i) {
            auto sortDirection = GetSortDirection(sortDirections, i);
            if (!AddSortColumn(tuple.Item(i), sortDirection, ctx, node, sortKeySelector, sortColumnList,
                sortKeyTypes))
            {
                canMerge = false;
                break;
            }
        }
    } else {
        canMerge = AddSortColumn(lambdaBody, sortDirections, ctx, node, sortKeySelector, sortColumnList, sortKeyTypes);
    }

    canMerge = canMerge && IsMergeConnectionApplicable(sortKeyTypes);

    auto lambda = Build<TCoLambda>(ctx, topSort.Pos())
            .Args({"stream"})
            .Body<TCoTopBase>()
                .CallableName(canMerge ? TCoTopSort::CallableName() : TCoTop::CallableName())
                .Input("stream")
                .KeySelectorLambda(ctx.DeepCopyLambda(topSort.KeySelectorLambda().Ref()))
                .SortDirections(sortDirections)
                .Count(topSort.Count())
                .Build()
            .Done();

    auto stage = dqUnion.Output().Stage().Cast<TDqStage>();
    auto newStage = DqPushLambdaToStage(stage, dqUnion.Output().Index(), lambda, {}, ctx, optCtx);
    if (!newStage) {
        return node;
    }

    TMaybeNode<TDqStage> outerStage;
    if (canMerge) {
        auto mergeCn = Build<TDqCnMerge>(ctx, node.Pos())
            .Output()
                .Stage(newStage.Cast())
                .Index(dqUnion.Output().Index())
                .Build()
            .SortColumns(sortColumnList.Done())
            .Done();

        // make outer stage to collect all inner stages
        outerStage = Build<TDqStage>(ctx, node.Pos())
            .Inputs()
                .Add(mergeCn)
                .Build()
            .Program()
                .Args({"stream"})
                .Body<TCoTake>()
                    .Input("stream")
                    .Count(topSort.Count())
                    .Build()
                .Build()
            .Settings(TDqStageSettings().BuildNode(ctx, node.Pos()))
            .Done();
    } else {
        auto unionAll = Build<TDqCnUnionAll>(ctx, node.Pos())
            .Output()
                .Stage(newStage.Cast())
                .Index(dqUnion.Output().Index())
                .Build()
            .Done();
        // make outer stage to collect all inner stages
        outerStage = Build<TDqStage>(ctx, node.Pos())
            .Inputs()
                .Add(unionAll)
                .Build()
            .Program()
                .Args({"stream"})
                .Body<TCoTopSort>()
                    .Input("stream")
                    .KeySelectorLambda(ctx.DeepCopyLambda(topSort.KeySelectorLambda().Ref()))
                    .SortDirections(topSort.SortDirections())
                    .Count(topSort.Count())
                    .Build()
                .Build()
            .Settings(TDqStageSettings().BuildNode(ctx, node.Pos()))
            .Done();
    }

    // TODO: Use CnMerge or AssumeSorted for keep constraints.
    return Build<TDqCnUnionAll>(ctx, node.Pos())
        .Output()
            .Stage(outerStage.Cast())
            .Index().Build(0U)
            .Build()
        .Done();
}

TExprBase DqBuildSortStage(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx,
    const TParentsMap& parentsMap, bool allowStageMultiUsage)
{
    if (!node.Maybe<TCoSortBase>().Input().Maybe<TDqCnUnionAll>()) {
        return node;
    }

    auto sort = node.Cast<TCoSortBase>();
    auto dqUnion = sort.Input().Cast<TDqCnUnionAll>();

    if (!IsSingleConsumerConnection(dqUnion, parentsMap, allowStageMultiUsage)) {
        return node;
    }

    auto result = dqUnion.Output().Stage().Program().Body();

    auto sortKeySelector = sort.KeySelectorLambda();
    auto sortDirections = sort.SortDirections();

    bool canMerge = true;
    auto sortColumnList = Build<TDqSortColumnList>(ctx, node.Pos());
    TVector<const TTypeAnnotationNode*> sortKeyTypes;

    auto lambdaBody = sortKeySelector.Body();
    if (IsDqCompletePureExpr(sortKeySelector)) {
        if (auto maybeColTuple = lambdaBody.Maybe<TExprList>()) {
            auto tuple = maybeColTuple.Cast();
            sortKeyTypes.reserve(tuple.Size());

            for (size_t i = 0; i < tuple.Size(); ++i) {
                auto sortDirection = GetSortDirection(sortDirections, i);
                if (!AddSortColumn(tuple.Item(i), TExprBase(sortDirection), ctx, node, sortKeySelector, sortColumnList,
                    sortKeyTypes))
                {
                    canMerge = false;
                    break;
                }
            }
        } else {
            canMerge = AddSortColumn(lambdaBody, sortDirections, ctx, node, sortKeySelector, sortColumnList, sortKeyTypes);
        }
    } else {
        canMerge = false;
    }

    TMaybeNode<TDqStage> outerStage;
    if (canMerge && IsMergeConnectionApplicable(sortKeyTypes)) {
        if (auto connToPushableStage = DqBuildPushableStage(dqUnion, ctx)) {
            return TExprBase(ctx.ChangeChild(*node.Raw(), TCoSortBase::idx_Input, std::move(connToPushableStage)));
        }
        auto lambda = Build<TCoLambda>(ctx, sort.Pos())
            .Args({"stream"})
            .Body<TCoSort>()
                .Input("stream")
                .SortDirections(sort.SortDirections())
                .KeySelectorLambda(ctx.DeepCopyLambda(sort.KeySelectorLambda().Ref()))
                .Build()
            .Done();

        auto stage = dqUnion.Output().Stage().Cast<TDqStage>();
        auto newStage = DqPushLambdaToStage(stage, dqUnion.Output().Index(), lambda, {}, ctx, optCtx);
        if (!newStage) {
            return node;
        }

        auto mergeCn = Build<TDqCnMerge>(ctx, node.Pos())
            .Output()
                .Stage(newStage.Cast())
                .Index(dqUnion.Output().Index())
                .Build()
            .SortColumns(sortColumnList.Done())
            .Done();

        // make outer stage to collect all inner stages
        outerStage = Build<TDqStage>(ctx, node.Pos())
            .Inputs()
                .Add(mergeCn)
                .Build()
            .Program()
                .Args({"stream"})
                .Body("stream")
                .Build()
            .Settings(TDqStageSettings().BuildNode(ctx, node.Pos()))
            .Done();
    } else {
        outerStage = Build<TDqStage>(ctx, node.Pos())
            .Inputs()
                .Add(dqUnion)
                .Build()
            .Program()
                .Args({"stream"})
                .Body<TCoSort>()
                    .Input("stream")
                    .SortDirections(sort.SortDirections())
                    .KeySelectorLambda(ctx.DeepCopyLambda(sort.KeySelectorLambda().Ref()))
                    .Build()
                .Build()
            .Settings(TDqStageSettings().BuildNode(ctx, node.Pos()))
            .Done();
    }

    return Build<TDqCnUnionAll>(ctx, node.Pos())
        .Output()
            .Stage(outerStage.Cast())
            .Index().Build("0")
            .Build()
        .Done();
}


// will generate smth like this
// (let $7 (DqPhyStage '((DqCnUnionAll (TDqOutput $5 '"0"))) (lambda '($13) (FromFlow (Take (Skip (ToFlow $13) (Uint64 '1)) $6))) '()))
// (let $8 (DqPhyStage '((DqCnUnionAll (TDqOutput $7 '"0"))) (lambda '($14) (FromFlow (Take (ToFlow $14) $6))) '()))
// maybe optimize (Take (Skip ...) ...) ?
TExprBase DqBuildSkipStage(TExprBase node, TExprContext& ctx, IOptimizationContext& /* optCtx */,
    const TParentsMap& parentsMap, bool allowStageMultiUsage)
{
    if (!node.Maybe<TCoSkip>().Input().Maybe<TDqCnUnionAll>()) {
        return node;
    }

    auto skip = node.Cast<TCoSkip>();
    if (!IsDqCompletePureExpr(skip.Count())) {
        return node;
    }

    auto dqUnion = skip.Input().Cast<TDqCnUnionAll>();
    if (!IsSingleConsumerConnection(dqUnion, parentsMap, allowStageMultiUsage)) {
        return node;
    }

    auto stage = Build<TDqStage>(ctx, node.Pos())
            .Inputs()
                .Add(dqUnion)
                .Build()
            .Program()
                .Args({"stream"})
                .Body<TCoSkip>()
                    .Input("stream")
                    .Count(skip.Count())
                    .Build()
                .Build()
            .Settings(TDqStageSettings().BuildNode(ctx, node.Pos()))
            .Done();

    return Build<TDqCnUnionAll>(ctx, node.Pos())
        .Output()
            .Stage(stage)
            .Index().Build("0")
            .Build()
        .Done();
}

TExprBase DqBuildTakeStage(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx,
    const TParentsMap& parentsMap, bool allowStageMultiUsage)
{
    if (!node.Maybe<TCoTakeBase>().Input().Maybe<TDqCnUnionAll>()) {
        return node;
    }

    auto take = node.Cast<TCoTakeBase>();
    auto dqUnion = take.Input().Cast<TDqCnUnionAll>();
    if (!IsSingleConsumerConnection(dqUnion, parentsMap, allowStageMultiUsage)) {
        return node;
    }

    if (!CanPushDqExpr(take.Count(), dqUnion)) {
        return node;
    }

    if (auto connToPushableStage = DqBuildPushableStage(dqUnion, ctx)) {
        return TExprBase(ctx.ChangeChild(*node.Raw(), TCoTakeBase::idx_Input, std::move(connToPushableStage)));
    }

    auto result = dqUnion.Output().Stage().Program().Body();
    auto stage = dqUnion.Output().Stage();

    auto lambda = Build<TCoLambda>(ctx, take.Pos())
            .Args({"stream"})
            .Body<TCoTake>()
                .Input("stream")
                .Count(take.Count())
                .Build()
            .Done();

    auto newDqUnion = DqPushLambdaToStageUnionAll(dqUnion, lambda, {}, ctx, optCtx);
    if (!newDqUnion) {
        return node;
    }

    // make outer stage to collect all inner stages
    auto outerTakeStage = Build<TDqStage>(ctx, node.Pos())
            .Inputs()
                .Add(newDqUnion.Cast())
                .Build()
            .Program()
                .Args({"stream"})
                .Body<TCoTake>()
                    .Input("stream")
                    .Count(take.Count())
                    .Build()
                .Build()
            .Settings(TDqStageSettings().BuildNode(ctx, node.Pos()))
            .Done();

    return Build<TDqCnUnionAll>(ctx, node.Pos())
        .Output()
            .Stage(outerTakeStage)
            .Index().Build("0")
            .Build()
        .Done();
}

TExprBase DqBuildTakeSkipStage(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx,
    const TParentsMap& parentsMap, bool allowStageMultiUsage)
{
    if (!node.Maybe<TCoTakeBase>().Input().Maybe<TCoSkip>().Input().Maybe<TDqCnUnionAll>()) {
        return node;
    }

    auto take = node.Cast<TCoTakeBase>();
    auto skip = take.Input().Cast<TCoSkip>();
    auto dqUnion = skip.Input().Cast<TDqCnUnionAll>();

    if (!IsSingleConsumerConnection(dqUnion, parentsMap, allowStageMultiUsage)) {
        return node;
    }

    if (!CanPushDqExpr(take.Count(), dqUnion)) {
        return node;
    }

    if (!CanPushDqExpr(skip.Count(), dqUnion)) {
        return node;
    }

    if (auto connToPushableStage = DqBuildPushableStage(dqUnion, ctx)) {
        return TExprBase(ctx.ChangeChild(*node.Raw(), TCoTakeBase::idx_Input, std::move(connToPushableStage)));
    }

    auto lambda = Build<TCoLambda>(ctx, node.Pos())
        .Args({"stream"})
        .Body<TCoTake>()
            .Input("stream")
            .Count<TCoAggrAdd>()
                .Left(take.Count())
                .Right(skip.Count())
                .Build()
            .Build()
        .Done();

    auto newDqUnion = DqPushLambdaToStageUnionAll(dqUnion, lambda, {}, ctx, optCtx);
    if (!newDqUnion) {
        return node;
    }

    auto outerTakeSkipStage = Build<TDqStage>(ctx, node.Pos())
        .Inputs()
            .Add(newDqUnion.Cast())
            .Build()
        .Program()
            .Args({"stream"})
            .Body<TCoTake>()
                .Input<TCoSkip>()
                    .Input("stream")
                    .Count(skip.Count())
                    .Build()
                .Count(take.Count())
                .Build()
            .Build()
        .Settings(TDqStageSettings().BuildNode(ctx, node.Pos()))
        .Done();

    return Build<TDqCnUnionAll>(ctx, node.Pos())
        .Output()
            .Stage(outerTakeSkipStage)
            .Index().Build("0")
            .Build()
        .Done();
}

TExprBase DqRewriteLengthOfStageOutput(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx,
    const TParentsMap& parentsMap, bool allowStageMultiUsage)
{
    if (!node.Maybe<TCoLength>().List().Maybe<TDqCnUnionAll>()) {
        return node;
    }

    auto dqUnion = node.Cast<TCoLength>().List().Cast<TDqCnUnionAll>();
    if (!IsSingleConsumerConnection(dqUnion, parentsMap, allowStageMultiUsage)) {
        return node;
    }

    if (auto connToPushableStage = DqBuildPushableStage(dqUnion, ctx)) {
        return TExprBase(ctx.ChangeChild(*node.Raw(), TCoLength::idx_List, std::move(connToPushableStage)));
    }

    auto zero = Build<TCoUint64>(ctx, node.Pos())
        .Literal().Build("0")
        .Done();

    auto field = BuildAtom("_dq_agg_cnt", node.Pos(), ctx);

    auto dqLengthLambda = Build<TCoLambda>(ctx, node.Pos())
        .Args({"stream"})
        .Body<TDqPhyLength>()
            .Input("stream")
            .Name(field)
            .Build()
        .Done();


    auto result = DqPushLambdaToStageUnionAll(dqUnion, dqLengthLambda, {}, ctx, optCtx);
    if (!result) {
        return node;
    }

    auto lengthStage = Build<TDqStage>(ctx, node.Pos())
        .Inputs()
            .Add(result.Cast())
            .Build()
        .Program()
            .Args({"stream"})
            .Body<TCoCondense>()
                .Input("stream")
                .State(zero)
                .SwitchHandler()
                    .Args({"item", "state"})
                    .Body(MakeBool<false>(node.Pos(), ctx))
                    .Build()
                .UpdateHandler()
                    .Args({"item", "state"})
                    .Body<TCoAggrAdd>()
                        .Left("state")
                        .Right<TCoMember>()
                            .Struct("item")
                            .Name(field)
                            .Build()
                        .Build()
                    .Build()
                .Build()
            .Build()
        .Settings(TDqStageSettings().BuildNode(ctx, node.Pos()))
        .Done();

    auto precompute = Build<TDqPhyPrecompute>(ctx, node.Pos())
        .Connection<TDqCnValue>()
            .Output()
                .Stage(lengthStage)
                .Index().Build("0")
                .Build()
            .Build()
        .Done();

    return precompute;
}

TExprBase DqBuildPureExprStage(TExprBase node, TExprContext& ctx) {
    if (!IsDqCompletePureExpr(node)) {
        return node;
    }

    auto stage = Build<TDqStage>(ctx, node.Pos())
        .Inputs()
            .Build()
        .Program()
            .Args({})
            .Body<TCoToStream>()
                .Input(node)
                .Build()
            .Build()
        .Settings(TDqStageSettings::New()
            .SetPartitionMode(TDqStageSettings::EPartitionMode::Single)
            .BuildNode(ctx, node.Pos()))
        .Done();

    return Build<TDqCnUnionAll>(ctx, node.Pos())
        .Output()
            .Stage(stage)
            .Index().Build("0")
            .Build()
        .Done();
}

/*
 * Move (Extend ...) into a separate stage.
 *
 * If overConnsOnly=true stage will be built only if all arguments have TDqConnection type.
 *
 * With overConnsOnly=false non-TDqConnection arguments are left in the same stage. This
 * is needed for handling UNION ALL case, which generates top-level Extend where some arguments
 * can be pure expressions not wrapped in DqStage (e.g. ... UNION ALL SELECT 1).
 */
TExprBase DqBuildExtendStage(TExprBase node, TExprContext& ctx) {
    if (!node.Maybe<TCoExtendBase>()) {
        return node;
    }

    auto extend = node.Cast<TCoExtendBase>();
    TVector<TCoArgument> inputArgs;
    TVector<TExprBase> inputConns;
    TVector<TExprBase> extendArgs;

    for (const auto& arg: extend) {
        if (arg.Maybe<TDqConnection>()) {
            auto conn = arg.Cast<TDqConnection>();
            TCoArgument programArg = Build<TCoArgument>(ctx, conn.Pos())
                .Name("arg")
                .Done();
            inputConns.push_back(conn);
            inputArgs.push_back(programArg);
            extendArgs.push_back(programArg);
        } else if (IsDqCompletePureExpr(arg)) {
            // arg is deemed to be a pure expression so leave it inside (Extend ...)
            extendArgs.push_back(Build<TCoToFlow>(ctx, arg.Pos())
                .Input(arg)
                .Done());
        } else {
            return node;
        }
    }

    if (inputConns.empty()) {
        return node;
    }

    auto stage = Build<TDqStage>(ctx, node.Pos())
        .Inputs()
            .Add(inputConns)
            .Build()
        .Program()
            .Args(inputArgs)
            .Body<TCoExtend>() // TODO: check Extend effectiveness
                .Add(extendArgs)
                .Build()
            .Build()
        .Settings(TDqStageSettings().BuildNode(ctx, node.Pos()))
        .Done();

    return Build<TDqCnUnionAll>(ctx, node.Pos())
        .Output()
            .Stage(stage)
            .Index().Build("0")
            .Build()
        .Done();
}

/*
 * Build physical precompute node.
 */
TExprBase DqBuildPrecompute(TExprBase node, TExprContext& ctx) {
    if (!node.Maybe<TDqPrecompute>()) {
        return node;
    }

    auto input = node.Cast<TDqPrecompute>().Input();

    TExprNode::TPtr connection;
    if (input.Maybe<TDqCnUnionAll>()) {
        connection = input.Ptr();
    } else if (input.Maybe<TDqCnValue>()) {
        connection = input.Ptr();
    } else if (input.Maybe<TCoParameter>()) {
        return input;
    } else if (IsDqCompletePureExpr(input)) {
        if (input.Ref().GetTypeAnn()->GetKind() != ETypeAnnotationKind::List &&
            input.Ref().GetTypeAnn()->GetKind() != ETypeAnnotationKind::Data)
        {
            return node;
        }

        auto dataStage = Build<TDqStage>(ctx, node.Pos())
            .Inputs()
                .Build()
            .Program()
                .Args({})
                .Body<TCoToStream>()
                    .Input<TCoJust>()
                        .Input(input)
                        .Build()
                    .Build()
                .Build()
            .Settings().Build()
            .Done();

        connection = Build<TDqCnValue>(ctx, node.Pos())
            .Output()
                .Stage(dataStage)
                .Index().Build("0")
                .Build()
            .Done().Ptr();

    } else {
        return node;
    }

    auto phyPrecompute = Build<TDqPhyPrecompute>(ctx, node.Pos())
        .Connection(connection)
        .Done();

    return phyPrecompute;
}

TExprBase DqBuildHasItems(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx, const TParentsMap& parentsMap, bool allowStageMultiUsage) {
    if (!node.Maybe<TCoHasItems>()) {
        return node;
    }

    auto hasItems = node.Cast<TCoHasItems>();

    if (!hasItems.List().Maybe<TDqCnUnionAll>()) {
        return node;
    }

    auto unionAll = hasItems.List().Cast<TDqCnUnionAll>();

    if (!IsSingleConsumerConnection(unionAll, parentsMap, allowStageMultiUsage)) {
        return node;
    }

    if (auto connToPushableStage = DqBuildPushableStage(unionAll, ctx)) {
        return TExprBase(ctx.ChangeChild(*node.Raw(), TCoHasItems::idx_List, std::move(connToPushableStage)));
    }

    // Add LIMIT 1 via Take
    auto takeProgram = Build<TCoLambda>(ctx, node.Pos())
        .Args({"take_arg"})
        .Body<TCoTake>()
            .Input({"take_arg"})
            .Count<TCoUint64>()
                .Literal().Build("1")
                .Build()
            .Build()
        .Done();

    auto newUnion = DqPushLambdaToStageUnionAll(unionAll, takeProgram, {}, ctx, optCtx);

    if (!newUnion.IsValid()) {
        return node;
    }

    // Build stage simulating HasItems via Condense
    auto hasItemsProgram = Build<TCoLambda>(ctx, node.Pos())
        .Args({"has_items_arg"})
        .Body<TCoCondense>()
            .Input({"has_items_arg"})
            .State<TCoBool>()
                .Literal().Build("false")
                .Build()
            .SwitchHandler()
                .Args({"item", "state"})
                .Body<TCoBool>()
                    .Literal().Build("false")
                    .Build()
                .Build()
            .UpdateHandler()
                .Args({"item", "state"})
                .Body<TCoBool>()
                    .Literal().Build("true")
                    .Build()
                .Build()
            .Build()
        .Done();

    auto hasItemsStage = Build<TDqStage>(ctx, node.Pos())
        .Inputs()
            .Add(newUnion.Cast())
            .Build()
        .Program(hasItemsProgram)
        .Settings().Build()
        .Done();

    auto precompute = Build<TDqPrecompute>(ctx, node.Pos())
            .Input<TDqCnValue>()
                .Output<TDqOutput>()
                    .Stage(hasItemsStage)
                    .Index().Build("0")
                    .Build()
                .Build()
            .Done();

    return precompute;
}

TExprBase DqBuildSqlIn(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx,
    const TParentsMap& parentsMap, bool allowStageMultiUsage)
{
    if (!node.Maybe<TCoSqlIn>().Collection().Maybe<TDqCnUnionAll>()) {
        return node;
    }

    auto sqlIn = node.Cast<TCoSqlIn>();
    auto unionAll = sqlIn.Collection().Cast<TDqCnUnionAll>();

    if (!IsSingleConsumerConnection(unionAll, parentsMap, allowStageMultiUsage)) {
        return node;
    }

    if (!IsDqCompletePureExpr(sqlIn.Lookup())) {
        return node;
    }

    if (!IsDqSelfContainedExpr(sqlIn.Lookup())) {
        return node;
    }

    if (auto connToPushableStage = DqBuildPushableStage(unionAll, ctx)) {
        return TExprBase(ctx.ChangeChild(*node.Raw(), TCoSqlIn::idx_Collection, std::move(connToPushableStage)));
    }

    auto localProgram = Build<TCoLambda>(ctx, node.Pos())
        .Args({"stream"})
        .Body<TCoMap>()
            .Input<TCoCondense>()
                .Input("stream")
                .State<TCoList>()
                    .ListType(ExpandType(node.Pos(), *unionAll.Ref().GetTypeAnn(), ctx))
                    .Build()
                .SwitchHandler()
                    .Args({"item", "state"})
                    .Body(MakeBool<false>(node.Pos(), ctx))
                    .Build()
                .UpdateHandler()
                    .Args({"item", "state"})
                    .Body<TCoAppend>()
                        .List("state")
                        .Item("item")
                        .Build()
                    .Build()
                .Build()
            .Lambda()
                .Args({"list"})
                .Body<TCoSqlIn>()
                    .Collection("list")
                    .Lookup(sqlIn.Lookup())
                    .Options(sqlIn.Options())
                    .Build()
                .Build()
            .Build()
        .Done();

    auto newUnion = DqPushLambdaToStageUnionAll(unionAll, localProgram, {}, ctx, optCtx);

    if (!newUnion.IsValid()) {
        return node;
    }

    auto resultsArg = Build<TCoArgument>(ctx, node.Pos())
        .Name("results_stream")
        .Done();

    TExprBase finalProgram = Build<TCoCondense>(ctx, node.Pos())
        .Input(resultsArg)
        .State<TCoJust>()
            .Input<TCoBool>()
                .Literal().Build("false")
                .Build()
            .Build()
        .SwitchHandler()
            .Args({"item", "state"})
            .Body(MakeBool<false>(node.Pos(), ctx))
            .Build()
        .UpdateHandler()
            .Args({"item", "state"})
            .Body<TCoIf>()
                .Predicate<TCoExists>()
                    .Optional("state")
                    .Build()
                .ThenValue<TCoIf>()
                    .Predicate<TCoExists>()
                        .Optional("item")
                        .Build()
                    .ThenValue<TCoIf>()
                        .Predicate<TCoCoalesce>()
                            .Predicate("item")
                            .Value(MakeBool<false>(node.Pos(), ctx))
                            .Build()
                        .ThenValue("item")
                        .ElseValue("state")
                        .Build()
                    .ElseValue<TCoNull>().Build()
                    .Build()
                .ElseValue<TCoNull>().Build()
                .Build()
            .Build()
        .Done();

    if (sqlIn.Ref().GetTypeAnn()->GetKind() != ETypeAnnotationKind::Optional) {
        finalProgram = Build<TCoMap>(ctx, node.Pos())
            .Input(finalProgram)
            .Lambda()
                .Args({"result"})
                // Result can't be NULL, double check here.
                .Body<TCoUnwrap>()
                    .Optional("result")
                    .Build()
                .Build()
            .Done();
    }

    auto stage = Build<TDqStage>(ctx, node.Pos())
        .Inputs()
            .Add(newUnion.Cast())
            .Build()
        .Program()
            .Args({resultsArg})
            .Body(finalProgram)
            .Build()
        .Settings(TDqStageSettings().BuildNode(ctx, node.Pos()))
        .Done();

    return Build<TDqPrecompute>(ctx, node.Pos())
        .Input<TDqCnValue>()
            .Output<TDqOutput>()
                .Stage(stage)
                .Index().Build("0")
                .Build()
            .Build()
        .Done();
}

TExprBase DqBuildScalarPrecompute(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx,
    const TParentsMap& parentsMap, bool allowStageMultiUsage)
{
    TMaybeNode<TDqCnUnionAll> maybeUnionAll;
    if (const auto maybeToOptional = node.Maybe<TCoToOptional>()) {
        maybeUnionAll = maybeToOptional.Cast().List().Maybe<TDqCnUnionAll>();
    } else if (const auto maybeHead = node.Maybe<TCoHead>()) {
        maybeUnionAll = maybeHead.Cast().Input().Maybe<TDqCnUnionAll>();
    }

    if (!maybeUnionAll) {
        return node;
    }

    const auto unionAll = maybeUnionAll.Cast();
    if (!IsSingleConsumerConnection(unionAll, parentsMap, allowStageMultiUsage)) {
        return node;
    }

    if (!unionAll.Output().Maybe<TDqOutput>()) {
        return node;
    }

    auto output = unionAll.Output().Cast<TDqOutput>();

    if (!output.Stage().Maybe<TDqStage>()) {
        return node;
    }
    if (auto connToPushableStage = DqBuildPushableStage(unionAll, ctx)) {
        return TExprBase(ctx.ChangeChild(
            *node.Raw(),
            node.Maybe<TCoToOptional>() ? TCoToOptional::idx_List : TCoHead::idx_Input,
            std::move(connToPushableStage)));
    }

    auto stage = output.Stage().Cast<TDqStage>();

    YQL_ENSURE(stage.Program().Ref().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Stream ||
               stage.Program().Ref().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Flow);

    auto lambdaArg = Build<TCoArgument>(ctx, node.Pos())
        .Name("scp_np_arg")
        .Done();

    /*
     * Need to build ToOptional(..) but this callable can't be pushed inside stage, thus simulate it
     * via Condense over Take(X, 1)
     */
    TExprNode::TPtr valueExtractor = Build<TCoCondense>(ctx, node.Pos())
        .Input<TCoTake>()
            .Input(lambdaArg)
            .Count<TCoUint64>()
                .Literal().Build("1")
                .Build()
            .Build()
        .State<TCoNothing>()
            .OptionalType(ExpandType(node.Pos(), *node.Ref().GetTypeAnn(), ctx))
            .Build()
        .SwitchHandler()
            .Args({"item", "state"})
            .Body<TCoBool>()
                .Literal().Build("false")
                .Build()
            .Build()
        .UpdateHandler()
            .Args({"item", "state"})
            .Body<TCoJust>()
                .Input("item")
                .Build()
            .Build()
        .Done().Ptr();

    auto newProgram = Build<TCoLambda>(ctx, node.Pos())
        .Args({lambdaArg})
        .Body(valueExtractor)
        .Done();

    const bool hasSingleTask = AllOf(stage.Inputs(), [](const auto& input) {
        return input.template Maybe<TDqCnMerge>() || input.template Maybe<TDqCnUnionAll>();
    });
    if (!hasSingleTask) {
        // Add LIMIT 1 via Take
        auto takeProgram = Build<TCoLambda>(ctx, node.Pos())
            .Args({"take_arg"})
            // DqOutput expects stream as input, thus form stream with one element
            .Body<TCoToStream>()
                .Input<TCoTake>()
                    .Input({"take_arg"})
                    .Count<TCoUint64>()
                        .Literal().Build("1")
                        .Build()
                    .Build()
                .Build()
            .Done();

        auto newUnion = DqPushLambdaToStageUnionAll(unionAll, takeProgram, {}, ctx, optCtx);
        if (!newUnion) {
            return node;
        }

        auto newStage = Build<TDqStage>(ctx, node.Pos())
            .Inputs()
                .Add(newUnion.Cast())
            .Build()
            .Program(newProgram)
            .Settings().Build()
            .Done();

        return Build<TDqPrecompute>(ctx, node.Pos())
            .Input<TDqCnValue>()
                .Output<TDqOutput>()
                    .Stage(newStage)
                    .Index().Build("0")
                    .Build()
                .Build()
            .Done();
    }

    auto newUnion = DqPushLambdaToStageUnionAll(unionAll, newProgram, {}, ctx, optCtx);

    if (!newUnion.IsValid()) {
        return node;
    }

    // Change connection to DqCnValue in case optional returns one element and wrap to precompute
    auto precompute = Build<TDqPrecompute>(ctx, node.Pos())
        .Input<TDqCnValue>()
            .Output(newUnion.Cast().Output())
            .Build()
        .Done();

    return precompute;
}

// left input should be DqCnUnionAll (with single usage)
// right input should be either DqCnUnionAll (with single usage) or DqPure expression
bool DqValidateJoinInputs(const TExprBase& left, const TExprBase& right, const TParentsMap& parentsMap,
    bool allowStageMultiUsage)
{
    if (!left.Maybe<TDqCnUnionAll>()) {
        return false;
    }
    if (!IsSingleConsumerConnection(left.Cast<TDqCnUnionAll>(), parentsMap, allowStageMultiUsage)) {
        return false;
    }

    if (right.Maybe<TDqCnUnionAll>()) {
        if (!IsSingleConsumerConnection(right.Cast<TDqCnUnionAll>(), parentsMap, allowStageMultiUsage)) {
            return false;
        }
    } else if (IsDqCompletePureExpr(right, /* isPrecomputePure */ true)) {
        // pass
    } else {
        return false;
    }

    return true;
}

TMaybeNode<TDqJoin> DqFlipJoin(const TDqJoin& join, TExprContext& ctx) {
    auto joinType = join.JoinType().Value();

    if (joinType == "Inner"sv || joinType == "Full"sv || joinType == "Exclusion"sv || joinType == "Cross"sv) {
        // pass
    } else if (joinType == "Right"sv) {
        joinType = "Left"sv;
    } else if (joinType == "Left"sv) {
        joinType = "Right"sv;
    } else if (joinType == "RightSemi"sv) {
        joinType = "LeftSemi"sv;
    } else if (joinType == "LeftSemi"sv) {
        joinType = "RightSemi"sv;
    } else if (joinType == "RightOnly"sv) {
        joinType = "LeftOnly"sv;
    } else if (joinType == "LeftOnly"sv) {
        joinType = "RightOnly"sv;
    } else {
        return {};
    }

    auto joinKeysBuilder = Build<TDqJoinKeyTupleList>(ctx, join.Pos());
    for (const auto& keys : join.JoinKeys()) {
        joinKeysBuilder.Add<TDqJoinKeyTuple>()
            .LeftLabel(keys.RightLabel())
            .LeftColumn(keys.RightColumn())
            .RightLabel(keys.LeftLabel())
            .RightColumn(keys.LeftColumn())
            .Build();
    }

    return Build<TDqJoin>(ctx, join.Pos())
        .LeftInput(join.RightInput())
        .LeftLabel(join.RightLabel())
        .RightInput(join.LeftInput())
        .RightLabel(join.LeftLabel())
        .JoinType().Build(joinType)
        .JoinKeys(joinKeysBuilder.Done())
        .LeftJoinKeyNames(join.RightJoinKeyNames())
        .RightJoinKeyNames(join.LeftJoinKeyNames())
        .JoinAlgo(join.JoinAlgo())
        .Done();
}


TExprBase DqBuildJoin(const TExprBase& node, TExprContext& ctx, IOptimizationContext& optCtx,
                      const TParentsMap& parentsMap, bool allowStageMultiUsage, bool pushLeftStage, EHashJoinMode hashJoin, bool shuffleMapJoin)
{
    if (!node.Maybe<TDqJoin>()) {
        return node;
    }

    auto join = node.Cast<TDqJoin>();
    const auto joinType = join.JoinType().Value();
    const bool leftIsUnionAll = join.LeftInput().Maybe<TDqCnUnionAll>().IsValid();
    const bool rightIsUnionAll = join.RightInput().Maybe<TDqCnUnionAll>().IsValid();

    auto joinAlgo = FromString<EJoinAlgoType>(join.JoinAlgo().StringValue());
    const bool mapJoinCanBeApplied = joinType != "Full"sv && joinType != "Exclusion"sv;
    if (joinAlgo == EJoinAlgoType::MapJoin && mapJoinCanBeApplied) {
        hashJoin = EHashJoinMode::Map;
    } else if (joinAlgo == EJoinAlgoType::GraceJoin) {
        hashJoin = EHashJoinMode::GraceAndSelf;
    }

    bool useHashJoin = EHashJoinMode::Off != hashJoin
        && joinType != "Cross"sv
        && leftIsUnionAll
        && rightIsUnionAll;

    if (DqValidateJoinInputs(join.LeftInput(), join.RightInput(), parentsMap, allowStageMultiUsage)) {
        // pass
    } else if (DqValidateJoinInputs(join.RightInput(), join.LeftInput(), parentsMap, allowStageMultiUsage)) {
        if (!useHashJoin) {
            if (const auto maybeFlipJoin = DqFlipJoin(join, ctx)) {
                join = maybeFlipJoin.Cast();
            } else {
                return node;
            }
        }
    } else {
        return node;
    }

    if (useHashJoin && (hashJoin == EHashJoinMode::GraceAndSelf || hashJoin == EHashJoinMode::Grace || shuffleMapJoin)) {
        return DqBuildHashJoin(join, hashJoin, ctx, optCtx);
    }

    if (joinType == "Full"sv || joinType == "Exclusion"sv) {
        return DqBuildJoinDict(join, ctx);
    }

    // NOTE: We don't want to broadcast table data via readsets for data queries, so we need to create a
    // separate stage to receive data from both sides of join.
    // TODO: We can push MapJoin to existing stage for data query, if it doesn't have table reads. This
    //       requires some additional knowledge, probably with use of constraints.
    return DqBuildPhyJoin(join, pushLeftStage, ctx, optCtx);
}

TExprBase DqPrecomputeToInput(const TExprBase& node, TExprContext& ctx) {
    if (!node.Maybe<TDqStageBase>()) {
        return node;
    }

    auto stage = node.Cast<TDqStageBase>();

    TExprNode::TListType innerPrecomputes = FindNodes(stage.Program().Ptr(),
        [](const TExprNode::TPtr& node) {
            return ETypeAnnotationKind::World != node->GetTypeAnn()->GetKind() && !TDqPhyPrecompute::Match(node.Get());
        },
        [](const TExprNode::TPtr& node) {
            return TDqPhyPrecompute::Match(node.Get());
        }
    );

    if (innerPrecomputes.empty()) {
        return node;
    }

    TExprNode::TListType newInputs;
    TExprNode::TListType newArgs;
    TNodeOnNodeOwnedMap replaces;

    TNodeOnNodeOwnedMap inputsReplaces;

    for (ui64 i = 0; i < stage.Inputs().Size(); ++i) {
        newInputs.push_back(stage.Inputs().Item(i).Ptr());
        auto arg = stage.Program().Args().Arg(i).Raw();
        newArgs.push_back(ctx.NewArgument(arg->Pos(), arg->Content()));
        replaces[arg] = newArgs.back();

        inputsReplaces[arg] = stage.Inputs().Item(i).Ptr();
    }

    for (auto& precompute: innerPrecomputes) {
        newInputs.push_back(ctx.ReplaceNodes(TExprNode::TPtr(precompute), inputsReplaces));
        newArgs.push_back(ctx.NewArgument(precompute->Pos(), TStringBuilder() << "_dq_precompute_" << newArgs.size()));
        replaces[precompute.Get()] = newArgs.back();
    }

    TExprNode::TListType children = stage.Ref().ChildrenList();
    children[TDqStageBase::idx_Inputs] = ctx.NewList(stage.Inputs().Pos(), std::move(newInputs));
    children[TDqStageBase::idx_Program] = ctx.NewLambda(stage.Program().Pos(),
        ctx.NewArguments(stage.Program().Args().Pos(), std::move(newArgs)),
        ctx.ReplaceNodes(stage.Program().Body().Ptr(), replaces));

    return TExprBase(ctx.ChangeChildren(stage.Ref(), std::move(children)));
}

TExprBase DqPropagatePrecomuteTake(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx,
    const TParentsMap& parentsMap, bool allowStageMultiUsage)
{
    if (!node.Maybe<TCoTake>().Input().Maybe<TDqPhyPrecompute>()) {
        return node;
    }

    auto take = node.Cast<TCoTake>();
    auto precompute = take.Input().Cast<TDqPhyPrecompute>();

    if (!IsSingleConsumerConnection(precompute.Connection(), parentsMap, allowStageMultiUsage)) {
        return node;
    }

    if (!CanPushDqExpr(take.Count(), precompute.Connection())) {
        return node;
    }

    auto takeLambda = Build<TCoLambda>(ctx, node.Pos())
        .Args({"list_stream"})
        .Body<TCoMap>()
            .Input("list_stream")
            .Lambda()
                .Args({"list"})
                .Body<TCoTake>()
                    .Input("list")
                    .Count(take.Count())
                    .Build()
                .Build()
            .Build()
        .Done();

    auto result = DqPushLambdaToStageUnionAll(precompute.Connection(), takeLambda, {}, ctx, optCtx);
    if (!result) {
        return node;
    }

    return Build<TDqPhyPrecompute>(ctx, node.Pos())
        .Connection(result.Cast())
        .Done();
}

TExprBase DqPropagatePrecomuteFlatmap(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx,
    const TParentsMap& parentsMap, bool allowStageMultiUsage)
{
    if (!node.Maybe<TCoFlatMap>().Input().Maybe<TDqPhyPrecompute>()) {
        return node;
    }

    auto flatmap = node.Cast<TCoFlatMap>();
    auto precompute = flatmap.Input().Cast<TDqPhyPrecompute>();

    if (!IsSingleConsumerConnection(precompute.Connection(), parentsMap, allowStageMultiUsage)) {
        return node;
    }

    if (!CanPushDqExpr(flatmap.Lambda(), precompute.Connection())) {
        return node;
    }

    auto flatmapLambda = Build<TCoLambda>(ctx, node.Pos())
        .Args({"list_stream"})
        .Body<TCoMap>()
            .Input("list_stream")
            .Lambda()
                .Args({"list"})
                .Body<TCoFlatMap>()
                    .Input("list")
                    .Lambda(flatmap.Lambda())
                    .Build()
                .Build()
            .Build()
        .Done();

    auto result = DqPushLambdaToStageUnionAll(precompute.Connection(), flatmapLambda, {}, ctx, optCtx);
    if (!result) {
        return node;
    }

    return Build<TDqPhyPrecompute>(ctx, node.Pos())
        .Connection(result.Cast())
        .Done();
}

NNodes::TExprBase DqBuildStageWithSourceWrap(NNodes::TExprBase node, TExprContext& ctx) {
    const auto wrap = node.Cast<TDqSourceWrap>();
    if (IsSameAnnotation(GetSeqItemType(*wrap.Ref().GetTypeAnn()), GetSeqItemType(*wrap.Input().Ref().GetTypeAnn()))) {
        return Build<TDqCnUnionAll>(ctx, node.Pos())
            .Output()
                .Stage<TDqStage>()
                .Inputs()
                    .Add<TDqSource>()
                        .DataSource(wrap.DataSource())
                        .Settings(wrap.Input())
                        .Build()
                    .Build()
                .Program()
                    .Args({"source"})
                    .Body<TCoToStream>()
                        .Input("source")
                        .Build()
                    .Build()
                .Settings(TDqStageSettings().BuildNode(ctx, node.Pos()))
                .Build()
                .Index().Build("0")
            .Build().Done();
    }
    const auto& items = GetSeqItemType(*wrap.Ref().GetTypeAnn()).Cast<TStructExprType>()->GetItems();
    auto sourceArg = ctx.NewArgument(node.Pos(), "source");
    auto inputType = &GetSeqItemType(*wrap.Input().Ref().GetTypeAnn());
    while (inputType->GetKind() == ETypeAnnotationKind::Tuple) {
        auto tupleType = inputType->Cast<TTupleExprType>();
        if (tupleType->GetSize() > 0) {
            inputType = tupleType->GetItems()[0];
        }
    }

    bool supportsBlocks = inputType->GetKind() == ETypeAnnotationKind::Struct &&
        inputType->Cast<TStructExprType>()->FindItem(BlockLengthColumnName).Defined();

    auto wideWrap = ctx.Builder(node.Pos())
        .Callable(supportsBlocks ? TDqSourceWideBlockWrap::CallableName() : TDqSourceWideWrap::CallableName())
            .Add(0, sourceArg)
            .Add(1, wrap.DataSource().Ptr())
            .Add(2, wrap.RowType().Ptr())
            .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                if (wrap.Settings()) {
                    parent.Add(3, wrap.Settings().Cast().Ptr());
                }

                return parent;
            })
        .Seal()
        .Build();

    if (supportsBlocks) {
        wideWrap = ctx.Builder(node.Pos())
            .Callable("WideFromBlocks")
                .Add(0, wideWrap)
            .Seal()
            .Build();
    }

    auto narrow = ctx.Builder(node.Pos())
        .Callable("NarrowMap")
            .Add(0, wideWrap)
            .Lambda(1)
                .Params("fields", items.size())
                .Callable(TCoAsStruct::CallableName())
                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                        ui32 i = 0U;
                        for (const auto& item : items) {
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
        .Build();

    auto program = ctx.NewLambda(node.Pos(), ctx.NewArguments(node.Pos(), { sourceArg }), std::move(narrow));

    return Build<TDqCnUnionAll>(ctx, node.Pos())
        .Output()
            .Stage<TDqStage>()
            .Inputs()
                .Add<TDqSource>()
                    .DataSource(wrap.DataSource())
                    .Settings(wrap.Input())
                    .Build()
                .Build()
            .Program(program)
            .Settings(TDqStageSettings().BuildNode(ctx, node.Pos()))
            .Build()
            .Index().Build("0")
        .Build().Done();
}

NNodes::TExprBase DqBuildStageWithReadWrap(NNodes::TExprBase node, TExprContext& ctx) {
    const auto wrap = node.Cast<TDqReadWrap>();
    const auto read = Build<TDqReadWideWrap>(ctx, node.Pos())
            .Input(wrap.Input())
            .Flags().Build()
            .Token(wrap.Token())
        .Done();

    const auto structType = GetSeqItemType(*wrap.Ref().GetTypeAnn()).Cast<TStructExprType>();
    auto narrow = ctx.Builder(node.Pos())
        .Lambda()
            .Callable("NarrowMap")
                .Add(0, read.Ptr())
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
        .Seal().Build();

    return Build<TDqCnUnionAll>(ctx, node.Pos())
        .Output()
            .Stage<TDqStage>()
                .Inputs().Build()
                .Program(narrow)
                .Settings(TDqStageSettings().BuildNode(ctx, node.Pos()))
            .Build()
            .Index().Build("0")
        .Build() .Done();
}

} // namespace NYql::NDq

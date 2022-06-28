#include "dq_opt_phy.h"

#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/dq/type_ann/dq_type_ann.h>

namespace NYql::NDq {

using namespace NYql::NNodes;

namespace {

TVector<TCoArgument> PrepareArgumentsReplacement(const TCoArguments& args, const TVector<TDqConnection>& newInputs,
    TExprContext& ctx, TNodeOnNodeOwnedMap& replaceMap)
{
    TVector<TCoArgument> newArgs;
    newArgs.reserve(args.Size() + newInputs.size());
    replaceMap.clear();

    for (size_t i = 0; i < args.Size(); ++i) {
        TCoArgument newArg{ctx.NewArgument(args.Pos(), TStringBuilder()
            << "_dq_replace_arg_" << i)};
        replaceMap[args.Arg(i).Raw()] = newArg.Ptr();
        newArgs.emplace_back(newArg);
    }

    for (size_t i = 0; i < newInputs.size(); ++i) {
        TCoArgument newArg{ctx.NewArgument(args.Pos(), TStringBuilder()
            << "_dq_replace_input_arg_" << args.Size() + i)};
        replaceMap[newInputs[i].Raw()] = newArg.Ptr();
        newArgs.emplace_back(newArg);
    }

    return newArgs;
}

template <typename TPartition>
TExprBase DqBuildPartitionsStageStub(TExprBase node, TExprContext& ctx, const TParentsMap& parentsMap) {
    if (!node.Maybe<TPartition>().Input().template Maybe<TDqCnUnionAll>()) {
        return node;
    }

    auto partition = node.Cast<TPartition>();
    if (!IsDqPureExpr(partition.KeySelectorLambda()) ||
        !IsDqPureExpr(partition.ListHandlerLambda()) ||
        !IsDqPureExpr(partition.SortKeySelectorLambda()))
    {
        return node;
    }
    auto dqUnion = partition.Input().template Cast<TDqCnUnionAll>();

    if (!IsSingleConsumerConnection(dqUnion, parentsMap)) {
        return node;
    }

    auto keyLambda = partition.KeySelectorLambda();
    TVector<TExprBase> keyElements;
    if (auto maybeTuple = keyLambda.Body().template Maybe<TExprList>()) {
        auto tuple = maybeTuple.Cast();
        for (const auto& element : tuple) {
            keyElements.push_back(element);
        }
    } else {
        keyElements.push_back(keyLambda.Body());
    }

    bool allKeysAreMembers = true;

    TVector<TCoAtom> keyColumns;
    keyColumns.reserve(keyElements.size());
    for (auto& element : keyElements) {
        if (!element.Maybe<TCoMember>()) {
            allKeysAreMembers = false;
            break;
        }

        auto member = element.Cast<TCoMember>();
        if (member.Struct().Raw() != keyLambda.Args().Arg(0).Raw()) {
            return node;
        }

        keyColumns.push_back(member.Name());
    }

    TExprNode::TPtr newConnection;

    if (!keyColumns.empty() && allKeysAreMembers) {
        newConnection = Build<TDqCnHashShuffle>(ctx, node.Pos())
            .Output()
                .Stage(dqUnion.Output().Stage())
                .Index(dqUnion.Output().Index())
                .Build()
            .KeyColumns()
                .Add(keyColumns)
                .Build()
            .Done().Ptr();
    } else if (keyColumns.empty()) {
        newConnection = Build<TDqCnUnionAll>(ctx, node.Pos())
            .Output()
                .Stage(dqUnion.Output().Stage())
                .Index(dqUnion.Output().Index())
                .Build()
            .Done().Ptr();
    } else {
        return node;
    }

    auto handler = partition.ListHandlerLambda();

    if constexpr(std::is_base_of<TCoPartitionsByKeys, TPartition>::value) {
        if (ETypeAnnotationKind::List == handler.Ref().GetTypeAnn()->GetKind()) {
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
            .Add(newConnection)
            .Build()
        .Program()
            .Args({"rows"})
            .Body<TPartition>()
                .Input("rows")
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
        .Settings(TDqStageSettings().BuildNode(ctx, node.Pos()))
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

} // namespace

TMaybeNode<TDqStage> DqPushLambdaToStage(const TDqStage& stage, const TCoAtom& outputIndex, TCoLambda& lambda,
    const TVector<TDqConnection>& lambdaInputs, TExprContext& ctx, IOptimizationContext& optCtx)
{
    YQL_CLOG(TRACE, CoreDq) << "stage #" << stage.Ref().UniqueId() << ": " << PrintDqStageOnly(stage, ctx)
        << ", add lambda to output #" << outputIndex.Value();

    if (IsDqDependsOnStage(lambda, stage)) {
        YQL_CLOG(TRACE, CoreDq) << "Lambda " << lambda.Ref().Dump() << " depends on stage: " << PrintDqStageOnly(stage, ctx);
        return {};
    }

    auto program = stage.Program();
    ui32 index = FromString<ui32>(outputIndex.Value());
    ui32 branchesCount = GetStageOutputsCount(stage);

    TExprNode::TPtr newProgram;
    if (branchesCount == 1) {
        newProgram = ctx.FuseLambdas(lambda.Ref(), program.Ref());
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
            if (index == i) {
                newBranchProgram = ctx.FuseLambdas(lambda.Ref(), branchLambda.Ref());
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

TMaybeNode<TDqConnection> DqPushLambdaToStageUnionAll(const TDqConnection& connection, TCoLambda& lambda,
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

TExprBase DqBuildFlatmapStage(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx,
    const TParentsMap& parentsMap, bool allowStageMultiUsage)
{
    if (!node.Maybe<TCoFlatMapBase>().Input().Maybe<TDqCnUnionAll>()) {
        return node;
    }

    auto flatmap = node.Cast<TCoFlatMapBase>();
    auto dqUnion = flatmap.Input().Cast<TDqCnUnionAll>();
    if (!IsSingleConsumerConnection(dqUnion, parentsMap, allowStageMultiUsage)) {
        return node;
    }

    if (!IsDqPureExpr(flatmap.Lambda())) {
        return node;
    }

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

    auto flatmapStage = Build<TDqStage>(ctx, flatmap.Pos())
        .Inputs()
            .Add<TDqCnMap>()
                .Output(dqUnion.Output())
                .Build()
            .Build()
        .Program(lambda)
        .Settings(TDqStageSettings().BuildNode(ctx, flatmap.Pos()))
        .Done();

    return Build<TDqCnUnionAll>(ctx, node.Pos())
        .Output()
            .Stage(flatmapStage)
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
        .Args({"stream"})
        .template Body<TCoToStream>()
            .template Input<TExprApplier>()
                .Apply(lmap.Lambda())
                .With(lmap.Lambda().Args().Arg(0), "stream")
                .Build()
            .Build()
        .Done();

    auto result = DqPushLambdaToStageUnionAll(dqUnion, lambda, {}, ctx, optCtx);
    if (!result) {
        return node;
    }

    const TTypeAnnotationNode* lmapItemTy = GetSeqItemType(lmap.Ref().GetTypeAnn()); 
    if (lmapItemTy->GetKind() == ETypeAnnotationKind::Variant) {
        // preserve typing by Mux'ing several stage outputs into one
        const auto variantItemTy = lmapItemTy->template Cast<TVariantExprType>();
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

    if (!CanPushDqExpr(combine.PreMapLambda(), dqUnion) ||
        !CanPushDqExpr(combine.KeySelectorLambda(), dqUnion) ||
        !CanPushDqExpr(combine.InitHandlerLambda(), dqUnion) ||
        !CanPushDqExpr(combine.UpdateHandlerLambda(), dqUnion) ||
        !CanPushDqExpr(combine.FinishHandlerLambda(), dqUnion))
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

    auto result = DqPushLambdaToStageUnionAll(dqUnion, lambda, {}, ctx, optCtx);
    if (!result) {
        return node;
    }

    return result.Cast();
}

TExprBase DqBuildPartitionsStage(TExprBase node, TExprContext& ctx, const TParentsMap& parentsMap) {
    return DqBuildPartitionsStageStub<TCoPartitionsByKeys>(std::move(node), ctx, parentsMap);
}

TExprBase DqBuildPartitionStage(TExprBase node, TExprContext& ctx, const TParentsMap& parentsMap) {
    return DqBuildPartitionsStageStub<TCoPartitionByKey>(std::move(node), ctx, parentsMap);
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

TExprBase GetSortDirection(TExprBase& sortDirections, size_t index) {
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

    auto result = dqUnion.Output().Stage().Program().Body();

    auto sortKeySelector = topSort.KeySelectorLambda();
    auto sortDirections = topSort.SortDirections();
    auto lambda = Build<TCoLambda>(ctx, topSort.Pos())
            .Args({"stream"})
            .Body<TCoTopSort>()
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

    TMaybeNode<TDqStage> outerStage;
    if (canMerge && IsMergeConnectionApplicable(sortKeyTypes)) {
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

    return Build<TDqCnUnionAll>(ctx, node.Pos())
        .Output()
            .Stage(outerStage.Cast())
            .Index().Build("0")
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
    if (IsDqPureExpr(sortKeySelector)) {
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
    if (!IsDqPureExpr(skip.Count())) {
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
    if (!node.Maybe<TCoTake>().Input().Maybe<TDqCnUnionAll>()) {
        return node;
    }

    auto take = node.Cast<TCoTake>();
    auto dqUnion = take.Input().Cast<TDqCnUnionAll>();
    if (!IsSingleConsumerConnection(dqUnion, parentsMap, allowStageMultiUsage)) {
        return node;
    }

    if (!CanPushDqExpr(take.Count(), dqUnion)) {
        return node;
    }

    if (auto connToPushableStage = DqBuildPushableStage(dqUnion, ctx)) {
        return TExprBase(ctx.ChangeChild(*node.Raw(), TCoTake::idx_Input, std::move(connToPushableStage)));
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
    if (!node.Maybe<TCoTake>().Input().Maybe<TCoSkip>().Input().Maybe<TDqCnUnionAll>()) {
        return node;
    }

    auto take = node.Cast<TCoTake>();
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
        return TExprBase(ctx.ChangeChild(*node.Raw(), TCoTake::idx_Input, std::move(connToPushableStage)));
    }

    auto lambda = Build<TCoLambda>(ctx, node.Pos())
        .Args({"stream"})
        .Body<TCoTake>()
            .Input("stream")
            .Count<TCoPlus>()
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

    auto combineLambda = Build<TCoLambda>(ctx, node.Pos())
        .Args({"stream"})
        .Body<TCoCombineByKey>()
            .Input("stream")
            .PreMapLambda()
                .Args({"item"})
                .Body<TCoJust>()
                    .Input("item")
                    .Build()
                .Build()
            .KeySelectorLambda()
                .Args({"item"})
                .Body(zero)
                .Build()
            .InitHandlerLambda()
                .Args({"key", "item"})
                .Body<TCoUint64>()
                    .Literal().Build("1")
                    .Build()
                .Build()
            .UpdateHandlerLambda()
                .Args({"key", "item", "state"})
                .Body<TCoInc>()
                    .Value("state")
                    .Build()
                .Build()
            .FinishHandlerLambda()
                .Args({"key", "state"})
                .Body<TCoJust>()
                    .Input<TCoAsStruct>()
                        .Add<TCoNameValueTuple>()
                            .Name(field)
                            .Value("state")
                            .Build()
                        .Build()
                    .Build()
                .Build()
            .Build()
        .Done();

    auto result = DqPushLambdaToStageUnionAll(dqUnion, combineLambda, {}, ctx, optCtx);
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

// TODO: Remove once precomputes are supported in YQ
TExprBase DqRewriteLengthOfStageOutputLegacy(TExprBase node, TExprContext& ctx, IOptimizationContext&) {
    if (!node.Maybe<TCoLength>().List().Maybe<TDqCnUnionAll>()) {
        return node;
    }

    auto dqUnion = node.Cast<TCoLength>().List().Cast<TDqCnUnionAll>();

    auto zero = Build<TCoUint64>(ctx, node.Pos())
                    .Literal().Build("0")
                    .Done();

    auto field = BuildAtom("_dq_agg_cnt", node.Pos(), ctx);

    auto combine = Build<TCoCombineByKey>(ctx, node.Pos())
            .Input(dqUnion)
            .PreMapLambda()
                .Args({"item"})
                .Body<TCoJust>()
                    .Input("item")
                    .Build()
                .Build()
            .KeySelectorLambda()
                .Args({"item"})
                .Body(zero)
                .Build()
            .InitHandlerLambda()
                .Args({"key", "item"})
                .Body<TCoUint64>()
                    .Literal().Build("1")
                    .Build()
                .Build()
            .UpdateHandlerLambda()
                .Args({"key", "item", "state"})
                .Body<TCoInc>()
                    .Value("state")
                    .Build()
                .Build()
            .FinishHandlerLambda()
                .Args({"key", "state"})
                .Body<TCoJust>()
                    .Input<TCoAsStruct>()
                        .Add<TCoNameValueTuple>()
                            .Name(field)
                            .Value("state")
                            .Build()
                        .Build()
                    .Build()
                .Build()
            .Done();

    const auto stub = MakeBool<false>(node.Pos(), ctx);

    auto partition = Build<TCoPartitionsByKeys>(ctx, node.Pos())
            .Input(combine)
            .KeySelectorLambda()
                .Args({"item"})
                .Body(stub)
                .Build()
            .SortDirections<TCoVoid>()
                .Build()
            .SortKeySelectorLambda<TCoVoid>()
                .Build()
            .ListHandlerLambda()
                .Args({"list"})
                .Body<TCoCondense1>()
                    .Input("list")
                    .InitHandler(BuildIdentityLambda(node.Pos(), ctx)) // take struct from CombineByKey result
                    .SwitchHandler()
                        .Args({"item", "state"})
                        .Body(stub)
                        .Build()
                    .UpdateHandler()
                        .Args({"item", "state"})
                        .Body<TCoAsStruct>()
                            .Add<TCoNameValueTuple>()
                                .Name(field)
                                .Value<TCoAggrAdd>()
                                    .Left<TCoMember>()
                                        .Struct("state")
                                        .Name(field)
                                        .Build()
                                    .Right<TCoMember>()
                                        .Struct("item")
                                        .Name(field)
                                        .Build()
                                    .Build()
                                .Build()
                            .Build()
                        .Build()
                    .Build()
                .Build()
            .Done();

    auto toOptional = Build<TCoToOptional>(ctx, node.Pos())
            .List(partition)
            .Done();

    auto coalesce = Build<TCoCoalesce>(ctx, node.Pos())
            .Predicate(toOptional)
            .Value<TCoAsStruct>()
                .Add<TCoNameValueTuple>()
                    .Name(field)
                    .Value(zero)
                    .Build()
                .Build()
            .Done();

    return Build<TCoMember>(ctx, node.Pos())
        .Struct(coalesce)
        .Name(field)
        .Done();
}

TExprBase DqBuildPureExprStage(TExprBase node, TExprContext& ctx) {
    if (!IsDqPureExpr(node)) {
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
        } else if (IsDqPureExpr(arg)) {
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
    } else if (IsDqPureExpr(input)) {
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

TExprBase DqBuildHasItems(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx) {
    if (!node.Maybe<TCoHasItems>()) {
        return node;
    }

    auto hasItems = node.Cast<TCoHasItems>();

    if (!hasItems.List().Maybe<TDqCnUnionAll>()) {
        return node;
    }

    auto unionAll = hasItems.List().Cast<TDqCnUnionAll>();

    if (auto connToPushableStage = DqBuildPushableStage(unionAll, ctx)) {
        return TExprBase(ctx.ChangeChild(*node.Raw(), TCoHasItems::idx_List, std::move(connToPushableStage)));
    }

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
    } else if (IsDqPureExpr(right, /* isPrecomputePure */ true)) {
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
        .Done();
}

TExprBase DqBuildJoin(const TExprBase& node, TExprContext& ctx, IOptimizationContext& optCtx,
                      const TParentsMap& parentsMap, bool allowStageMultiUsage, bool pushLeftStage)
{
    if (!node.Maybe<TDqJoin>()) {
        return node;
    }

    auto join = node.Cast<TDqJoin>();

    if (DqValidateJoinInputs(join.LeftInput(), join.RightInput(), parentsMap, allowStageMultiUsage)) {
        // pass
    } else if (DqValidateJoinInputs(join.RightInput(), join.LeftInput(), parentsMap, allowStageMultiUsage)) {
        auto maybeFlipJoin = DqFlipJoin(join, ctx);
        if (!maybeFlipJoin) {
            return node;
        }
        join = maybeFlipJoin.Cast();
    } else {
        return node;
    }

    auto joinType = join.JoinType().Value();

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

    for (ui64 i = 0; i < stage.Inputs().Size(); ++i) {
        newInputs.push_back(stage.Inputs().Item(i).Ptr());
        auto arg = stage.Program().Args().Arg(i).Raw();
        newArgs.push_back(ctx.NewArgument(arg->Pos(), arg->Content()));
        replaces[arg] = newArgs.back();
    }

    for (auto& precompute: innerPrecomputes) {
        newInputs.push_back(precompute);
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

} // namespace NYql::NDq

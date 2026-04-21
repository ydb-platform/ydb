#include "kqp_rbo_physical_query_builder.h"
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/dq/type_ann/dq_type_ann.h>
#include <ydb/library/yql/dq/opt/dq_opt_peephole.h>
#include <ydb/core/kqp/opt/peephole/kqp_opt_peephole.h>
#include <ydb/library/yql/dq/opt/dq_opt_build.h>

using namespace NYql::NNodes;
using namespace NKikimr;
using namespace NKikimr::NKqp;

TExprNode::TPtr TPhysicalQueryBuilder::BuildPhysicalQuery() {
    auto phyStages = BuildPhysicalStageGraph();
    phyStages = EnableWideChannelsPhysicalStages(std::move(phyStages));
    phyStages = PeepHoleOptimizePhysicalStages(std::move(phyStages));
    return BuildPhysicalQuery(std::move(phyStages));
}

TVector<TExprNode::TPtr> TPhysicalQueryBuilder::BuildPhysicalStageGraph() {
    TVector<TExprNode::TPtr> phyStages;
    Graph.TopologicalSort();
    const auto& stageIds = Graph.StageIds;
    const auto& stageInputIds = Graph.StageInputs;
    auto& ctx = RBOCtx.ExprCtx;

    THashMap<ui32, TExprNode::TPtr> finalizedStages;
    for (const auto id : stageIds) {
        YQL_CLOG(TRACE, CoreDq) << "Finalizing stage " << id;

        TVector<TExprNode::TPtr> inputConnections;
        THashSet<ui32> processedInputsIds;
        for (const auto inputStageId : stageInputIds.at(id)) {
            if (processedInputsIds.contains(inputStageId)) {
                continue;
            }
            processedInputsIds.insert(inputStageId);

            auto inputStage = finalizedStages.at(inputStageId);
            const auto connections = Graph.GetConnections(inputStageId, id);
            for (const auto& connection : connections) {
                YQL_CLOG(TRACE, CoreDq) << "Building connection: " << inputStageId << "->" << id << ", " << connection->Type;
                TExprNode::TPtr newStage;
                auto dqConnection = connection->BuildConnection(inputStage, StagePos.at(inputStageId), newStage, ctx);
                if (newStage) {
                    phyStages.emplace_back(newStage);
                }
                YQL_CLOG(TRACE, CoreDq) << "Built connection: " << inputStageId << "->" << id << ", " << connection->Type;
                inputConnections.push_back(dqConnection);
            }
        }

        TExprNode::TPtr stage;
        if (Graph.IsSourceStageRowType(id)) {
            stage = Stages.at(id);
        } else {
            TVector<TExprNode::TPtr> stageInputConnections;
            TVector<TExprNode::TPtr> stageInputArgs;
            if (!Graph.IsSourceStageColumnType(id)) {
                stageInputConnections = inputConnections;
                stageInputArgs = StageArgs.at(id);
            }

            stage = BuildDqPhyStage(stageInputConnections, stageInputArgs, Stages.at(id), NYql::NDq::TDqStageSettings().New().BuildNode(ctx, StagePos.at(id)),
                                    ctx, StagePos.at(id));
            phyStages.emplace_back(stage);
            YQL_CLOG(TRACE, CoreDq) << "Added stage " << stage->UniqueId();
        }

        if (Graph.IsSourceStageColumnType(id)) {
            auto readPtr = FindNode(stage, [](const TExprNode::TPtr& node) { return !!TMaybeNode<TKqpBlockReadOlapTableRanges>(node); });
            if (readPtr) {
                auto read = TExprBase(readPtr).Cast<TKqpBlockReadOlapTableRanges>();
                if (!read.Ranges().Maybe<TCoVoid>()) {
                    auto precomputeResult = BuildMaterialize(read.Ranges().Ptr());
                    // clang-fomrat off
                    auto newRead = Build<TKqpBlockReadOlapTableRanges>(ctx, readPtr->Pos())
                        .Table(read.Table())
                        .Ranges(precomputeResult)
                        .Columns(read.Columns())
                        .Settings(read.Settings())
                        .ExplainPrompt(read.ExplainPrompt())
                        .Process(read.Process())
                    .Done().Ptr();
                    // clang-format on
                    stage = ctx.ReplaceNode(std::move(stage), read.Ref(), newRead);
                }
            }
        }

        finalizedStages[id] = stage;
        YQL_CLOG(TRACE, CoreDq) << "Finalized stage " << id;
    }

    const auto maybeFinalStage = phyStages.back();
    const auto finalStage = GetFinalStage(maybeFinalStage);
    if (finalStage.Get() != maybeFinalStage.Get()) {
        phyStages.push_back(finalStage);
    }

    return phyStages;
}

TExprNode::TPtr TPhysicalQueryBuilder::BuildMaterialize(TExprNode::TPtr node) {
    auto& ctx = RBOCtx.ExprCtx;

    TExprNode::TPtr afterPeephole;
    auto status =
        ::PeepHoleOptimize(TExprBase(node), afterPeephole, ctx, RBOCtx.PeepholeTypeAnnTransformer, RBOCtx.TypeCtx, RBOCtx.KqpCtx.Config, false, true, {});
    if (status != IGraphTransformer::TStatus::Ok) {
        ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), "Peephole optimization failed for materialize in NEW RBO"));
        return nullptr;
    }

    // clang-format off
    auto rangesProgram = Build<TCoToStream>(ctx, node->Pos())
        .Input<TCoJust>()
            .Input<TExprList>()
                .Add({afterPeephole})
            .Build()
        .Build()
    .Done().Ptr();
    // clang-format on

    auto stageSettings = NYql::NDq::TDqStageSettings().New().SetPartitionMode(NYql::NDq::TDqStageSettings::EPartitionMode::Single).BuildNode(ctx, node->Pos());
    auto phyStage = BuildDqPhyStage({}, {}, rangesProgram, std::move(stageSettings), ctx, node->Pos());

    // clang-format off
    auto result = Build<TDqCnValue>(ctx, node->Pos())
        .Output()
            .Stage(phyStage)
            .Index().Build("0")
        .Build()
    .Done().Ptr();
    // clang-format on

    TypeAnnotate(result);
    Y_ENSURE(result->GetTypeAnn());

    // clang-format off
    auto param = Build<TCoParameter>(ctx, node->Pos())
        .Name<TCoAtom>()
            .Value(ParamBindingName + ToString(UniqueParamsId++))
        .Build()
        .Type(ExpandType(node->Pos(), *result->GetTypeAnn(), ctx))
    .Done().Ptr();
    // clang-format on

    Materialize.push_back({param, result});
    return param;
}

TExprNode::TPtr TPhysicalQueryBuilder::GetFinalStage(const TExprNode::TPtr& stage) const {
    auto& ctx = RBOCtx.ExprCtx;
    TExprNode::TPtr finalStage;
    bool needFinalUnionStage = false;
    // Final stage, which is input for DqCnResult, should have only one 1 task.
    for (const auto& input : TDqPhyStage(stage).Inputs()) {
        if (!input.Maybe<TDqCnUnionAll>()) {
            needFinalUnionStage = true;
            break;
        }
    }

    if (needFinalUnionStage) {
        // clang-format off
        auto input = Build<TDqCnUnionAll>(ctx, stage->Pos())
            .Output()
                .Stage(stage)
                .Index().Build("0")
                .Build()
            .Done().Ptr();

        finalStage = Build<TDqPhyStage>(ctx, stage->Pos())
            .Inputs()
                .Add({input})
            .Build()
            .Program<TCoLambda>()
                .Args({"arg"})
                .Body("arg")
            .Build()
            .Settings(NYql::NDq::TDqStageSettings().BuildNode(ctx, stage->Pos()))
        .Done().Ptr();
    // clang-format on
    } else {
        finalStage = stage;
    }
    return finalStage;
}

TVector<TKqpParamBinding> TPhysicalQueryBuilder::CollectParamBindings(const TVector<TExprNode::TPtr>& physicalStages) {
    auto& ctx = RBOCtx.ExprCtx;
    auto pos = Root.Pos;

    TVector<TKqpParamBinding> paramBindings;
    THashSet<TString> paramsCollected;
    for (const auto& physicalStage : physicalStages) {
        const auto params = FindNodes(physicalStage, [](const TExprNode::TPtr& node) { return !!TMaybeNode<TCoParameter>(node); });
        for (const auto& param : params) {
            const auto paramName = TExprBase(param).Cast<TCoParameter>().Name().StringValue();
            if (!paramsCollected.contains(paramName) && paramName.find(ParamBindingName) == TString::npos) {
                // clang-format off
                const auto paramBinding = Build<TKqpParamBinding>(ctx, pos)
                    .Name<TCoAtom>()
                        .Value(paramName)
                    .Build()
                .Done();
                // clang-format on
                paramBindings.push_back(paramBinding);
                paramsCollected.insert(paramName);
            }
        }
    }

    return paramBindings;
}

TExprNode::TPtr TPhysicalQueryBuilder::BuildPhysicalQuery(TVector<TExprNode::TPtr>&& physicalStages) {
    Y_ENSURE(physicalStages.size());
    auto& ctx = RBOCtx.ExprCtx;

    TVector<TExprBase> phyTxs;
    auto paramBindingsMainTx = CollectParamBindings(physicalStages);

    TVector<TExprBase> phyStagesForMaterialize;
    TVector<TExprBase> resultsForMaterialize;
    TVector<TExprBase> paramBindingsForMaterialize;
    // Prepare physical txs and bindings for materialize if needed.
    const ui32 materializeSize = Materialize.size();
    for (ui32 i = 0; i < materializeSize; ++i) {
        auto param = TExprBase(Materialize[i].first).Cast<TCoParameter>();
        auto materializeResult = TExprBase(Materialize[i].second).Cast<TDqCnValue>();

        // clang-format off
        auto resultBinding = Build<TKqpTxResultBinding>(ctx, Root.Pos)
            .Type(ExpandType(Root.Pos, *materializeResult.Ptr()->GetTypeAnn(), ctx))
            .TxIndex().Build("0")
            .ResultIndex().Build(ToString(i))
        .Done();
        // clang-format on

        // clang-format off
        auto paramBinding = Build<TKqpParamBinding>(ctx, Root.Pos)
            .Name(param.Name())
            .Binding(resultBinding.Ptr())
        .Done();
        // clang-format on
        // Binding from materialize to main tx.
        paramBindingsMainTx.emplace_back(paramBinding);

        auto materializeStage = materializeResult.Output().Stage();
        const auto paramBindingsMaterialize = CollectParamBindings({materializeStage.Ptr()});
        // Bindings params in materialize.
        paramBindingsForMaterialize.insert(paramBindingsForMaterialize.end(), paramBindingsMaterialize.begin(), paramBindingsMaterialize.end());
        // Stages for phy tx.
        phyStagesForMaterialize.emplace_back(materializeStage);
        resultsForMaterialize.emplace_back(materializeResult);
    }

    if (materializeSize) {
        TKqpPhyTxSettings txSettings;
        txSettings.Type = EPhysicalTxType::Compute;

        // clang-format off
        auto phyTx = Build<TKqpPhysicalTx>(ctx, Root.Pos)
            .Stages()
                .Add(phyStagesForMaterialize)
            .Build()
            .Results()
                .Add(resultsForMaterialize)
            .Build()
            .ParamBindings()
                .Add(paramBindingsForMaterialize)
            .Build()
            .Settings(txSettings.BuildNode(ctx, Root.Pos))
        .Done().Ptr();
        // clang-format on

        phyTxs.emplace_back(phyTx);
    }

    TVector<TCoAtom> columnAtomList;
    for (const auto& column : Root.ColumnOrder) {
        columnAtomList.push_back(Build<TCoAtom>(ctx, Root.Pos).Value(column).Done());
    }
    const auto columnOrder = Build<TCoAtomList>(ctx, Root.Pos).Add(columnAtomList).Done().Ptr();

    // clang-format off
    // wrap in DqResult
    auto dqResult = Build<TDqCnResult>(ctx, Root.Pos)
        .Output()
            .Stage(physicalStages.back())
            .Index().Build("0")
        .Build()
        .ColumnHints(columnOrder)
    .Done().Ptr();
    // clang-format on

    TypeAnnotate(dqResult);
    YQL_CLOG(TRACE, CoreDq) << "Inferred final type: " << *dqResult->GetTypeAnn();

    auto phyTxSettings = GetPhysicalTxSettings();
    // Build PhysicalTx
    auto mainTx = Build<TKqpPhysicalTx>(ctx, Root.Pos)
        .Stages()
            .Add(physicalStages)
        .Build()
        .Results()
            .Add({dqResult})
        .Build()
        .ParamBindings()
            .Add(paramBindingsMainTx)
        .Build()
        .Settings(phyTxSettings.BuildNode(ctx, Root.Pos))
    .Done().Ptr();
    // clang-format on
    phyTxs.emplace_back(mainTx);

    // If we have materialize tx, main tx is next.
    const TString mainTxIndex = materializeSize ? "1" : "0";
    // clang-format off
    auto mainTxResultBinding = Build<TKqpTxResultBinding>(ctx, Root.Pos)
        .Type(ExpandType(Root.Pos, *dqResult->GetTypeAnn(), ctx))
        .TxIndex().Build(mainTxIndex)
        .ResultIndex().Build("0")
    .Done().Ptr();
    // clang-format on

    auto phyQuerySettings = GetPhysicalQuerySettings();
    // Build Physical query
    // clang-format off
    return Build<TKqpPhysicalQuery>(ctx, Root.Pos)
        .Transactions()
            .Add(phyTxs)
        .Build()
        .Results()
            .Add({mainTxResultBinding})
        .Build()
        .Settings(phyQuerySettings.BuildNode(ctx, Root.Pos))
    .Done().Ptr();
    // clang-format on
}

TKqpPhyQuerySettings TPhysicalQueryBuilder::GetPhysicalQuerySettings() const {
    auto& kqpCtx = RBOCtx.KqpCtx;
    TKqpPhyQuerySettings querySettings;
    switch (kqpCtx.QueryCtx->Type) {
        case EKikimrQueryType::Dml: {
            querySettings.Type = EPhysicalQueryType::Data;
            break;
        }
        case EKikimrQueryType::Query: {
            querySettings.Type = EPhysicalQueryType::GenericQuery;
            break;
        }
        default: {
            // Should fallback to old pipeline.
            YQL_ENSURE(false, "Unsupported query type for NEW RBO " << kqpCtx.QueryCtx->Type);
        }
    }

    return querySettings;
}

TKqpPhyTxSettings TPhysicalQueryBuilder::GetPhysicalTxSettings() const {
    auto& kqpCtx = RBOCtx.KqpCtx;
    TKqpPhyTxSettings txSettings;
    switch (kqpCtx.QueryCtx->Type) {
        case EKikimrQueryType::Dml: {
            txSettings.Type = EPhysicalTxType::Compute;
            break;
        }
        case EKikimrQueryType::Query: {
            txSettings.Type = EPhysicalTxType::Generic;
            break;
        }
        default: {
            YQL_ENSURE(false, "Unsupported tx type for NEW RBO " << kqpCtx.QueryCtx->Type);
        }
    }
    return txSettings;
}

TExprNode::TPtr TPhysicalQueryBuilder::BuildDqPhyStage(const TVector<TExprNode::TPtr>& inputs, const TVector<TExprNode::TPtr>& args,
                                                       TExprNode::TPtr physicalStageBody, NNodes::TCoNameValueTupleList&& settings, TExprContext& ctx,
                                                       TPositionHandle pos) const {
    // clang-format off
    return Build<TDqPhyStage>(ctx, pos)
        .Inputs()
            .Add(inputs)
        .Build()
        .Program()
            .Args(args)
            .Body(physicalStageBody)
        .Build()
        .Settings(settings)
    .Done().Ptr();
    // clang-format on
}

void TPhysicalQueryBuilder::TopologicalSort(TDqPhyStage& dqStage, TVector<TExprNode::TPtr>& result, THashSet<const TExprNode*>& visited) const {
    visited.insert(dqStage.Raw());

    for (const auto& item : dqStage.Inputs()) {
        auto maybeConnection = item.Maybe<TDqConnection>();
        // DataSource stage as input.
        if (!maybeConnection) {
            continue;
        }

        TDqPhyStage inputStage = maybeConnection.Cast().Output().Stage().Cast<TDqPhyStage>();
        if (!visited.contains(inputStage.Raw())) {
            TopologicalSort(inputStage, result, visited);
        }
    }

    result.push_back(dqStage.Ptr());
}

void TPhysicalQueryBuilder::TopologicalSort(TDqPhyStage&& dqStage, TVector<TExprNode::TPtr>& result) const {
    THashSet<const TExprNode*> visited;
    TopologicalSort(dqStage, result, visited);
}

void TPhysicalQueryBuilder::KeepTypeAnnotationForStageAndFirstLevelChilds(TDqPhyStage& newStage, const TDqPhyStage& oldStage) const {
    Y_ENSURE(oldStage.Ref().GetTypeAnn());
    Y_ENSURE(oldStage.Inputs().Size() == newStage.Inputs().Size());

    newStage.MutableRef().SetTypeAnn(oldStage.Ref().GetTypeAnn());
    for (ui32 i = 0; i < newStage.Inputs().Size(); ++i) {
        newStage.Inputs().Item(i).MutableRef().SetTypeAnn(oldStage.Inputs().Item(i).Ref().GetTypeAnn());
        newStage.Program().Args().Arg(i).MutableRef().SetTypeAnn(oldStage.Program().Args().Arg(i).Ref().GetTypeAnn());
        if (newStage.Inputs().Item(i).Maybe<TDqConnection>()) {
            newStage.Inputs().Item(i).Cast<TDqConnection>().Output().Stage().MutableRef().SetTypeAnn(
                oldStage.Inputs().Item(i).Cast<TDqConnection>().Output().Stage().Ref().GetTypeAnn());
        }
    }
}

TVector<TExprNode::TPtr> TPhysicalQueryBuilder::EnableWideChannelsPhysicalStages(TVector<TExprNode::TPtr>&& physicalStages) {
    Y_ENSURE(physicalStages.size());
    auto root = physicalStages.back();
    if (!root->GetTypeAnn()) {
        TypeAnnotate(root);
    }
    auto& ctx = RBOCtx.ExprCtx;

    TNodeOnNodeOwnedMap replaces;
    TExprNode::TPtr rootStage;
    for (auto& stage : physicalStages) {
        auto dqPhyStage = TDqPhyStage(stage);
        // clang-format off
        auto newStage = Build<TDqPhyStage>(ctx, stage->Pos())
            .Inputs(ctx.ReplaceNodes(dqPhyStage.Inputs().Ptr(), replaces))
            .Program(dqPhyStage.Program())
            .Settings(dqPhyStage.Settings())
            .Outputs(dqPhyStage.Outputs())
        .Done().Ptr();
        // clang-format on

        TypeAnnotate(newStage);
        rootStage = NYql::NDq::RebuildStageInputsAsWide(TDqPhyStage(newStage), ctx).Ptr();
        replaces[dqPhyStage.Raw()] = rootStage;
    }

    TypeAnnotate(rootStage);
    YQL_CLOG(TRACE, CoreDq) << "[NEW RBO Wide channels] " << KqpExprToPrettyString(TExprBase(rootStage), ctx);

    TVector<TExprNode::TPtr> stagesTopSorted;
    TopologicalSort(TDqPhyStage(rootStage), stagesTopSorted);
    return stagesTopSorted;
}

// The idea was taken from kqp_opt_peephole with some changes.
// This function assumes that stages already sorted in topological orders.
TVector<TExprNode::TPtr> TPhysicalQueryBuilder::PeepHoleOptimizePhysicalStages(TVector<TExprNode::TPtr>&& physicalStages) {
    Y_ENSURE(physicalStages.size());
    auto root = physicalStages.back();
    // Type is required to wrap stage lambda to `KqpProgram`
    if (!root->GetTypeAnn()) {
        TypeAnnotate(root);
    }
    auto& ctx = RBOCtx.ExprCtx;

    TNodeOnNodeOwnedMap programsMap;
    for (auto& stage : physicalStages) {
        TNodeOnNodeOwnedMap argReplaces;
        auto dqPhyStage = TDqPhyStage(stage);
        auto program = dqPhyStage.Program();

        const bool isSuitableHashShuffleConnections = IsSuitableToPropagateWideBlocksThroughHashShuffleConnections(dqPhyStage);
        TVector<TExprNode::TPtr> stageArgs;
        for (ui32 i = 0, e = dqPhyStage.Inputs().Size(); i < e; ++i) {
            auto stageArg = program.Args().Arg(i);
            auto newStageArg = stageArg.Ptr();
            auto input = dqPhyStage.Inputs().Item(i);

            if (auto maybeConnection = input.Maybe<TDqConnection>();
                isSuitableHashShuffleConnections && maybeConnection && IsSuitableToPropagateWideBlocksThroughConnection(maybeConnection.Cast().Output())) {
                auto inputStage = maybeConnection.Cast().Output().Stage();
                auto program = TCoLambda(programsMap.at(inputStage.Program().Raw()));
                auto body = program.Body().Ptr();

                // If the body of input stage is `FromBlocks` propagate it through connection.
                if (body->IsCallable("WideFromBlocks")) {
                    body = body->ChildPtr(0);

                    // New arg for the current stage has a `Blocks` type, so we need to add `FromBlocks` here.
                    // clang-format off
                    auto fromBlocks = Build<TCoWideFromBlocks>(ctx, stageArg.Pos())
                        .Input<TCoArgument>()
                            .Name("new_stage_arg")
                        .Build()
                    .Done();
                    // clang-format on

                    // Update a stage arg.
                    newStageArg = fromBlocks.Input().Ptr();
                    // Replace an original arg with arg wrapped to `FromBlocks`.
                    argReplaces[stageArg.Raw()] = fromBlocks.Ptr();

                    // clang-format off
                    auto newProgram = Build<TCoLambda>(ctx, program.Pos())
                        .Args(program.Args())
                        .Body(body)
                    .Done().Ptr();
                    // clang-format on

                    // Update the type to `Blocks`, which should be easy since it only works on the lambda and not the entire graph.
                    newProgram = PeepHoleOptimize(newProgram, GetArgsType(program.Ptr()));
                    Y_ENSURE(newProgram->GetTypeAnn());

                    newStageArg->SetTypeAnn(newProgram->GetTypeAnn());
                    // Update map, since stage body was updated.
                    programsMap[inputStage.Program().Raw()] = newProgram;
                }
            }
            stageArgs.push_back(newStageArg);
        }

        // clang-format off
        auto newProgram = Build<TCoLambda>(ctx, stage->Pos())
            .Args(stageArgs)
            .Body(ctx.ReplaceNodes(program.Body().Ptr(), argReplaces))
        .Done().Ptr();
        // clang-format on

        TVector<const TTypeAnnotationNode*> argsType;
        for (const auto& arg : stageArgs) {
            const TTypeAnnotationNode* argTypeAnn = arg->GetTypeAnn();
            Y_ENSURE(argTypeAnn);
            argsType.push_back(argTypeAnn);
        }

        newProgram = PeepHoleOptimize(newProgram, argsType);
        Y_ENSURE(newProgram);
        // Collect program after peephole.
        programsMap[program.Raw()] = newProgram;
    }

    TVector<TExprNode::TPtr> newStages;
    newStages.reserve(physicalStages.size());
    for (ui32 i = 0, e = physicalStages.size(); i < e; ++i) {
        newStages.push_back(ctx.ReplaceNodes(std::move(physicalStages[i]), programsMap));
    }

    YQL_CLOG(TRACE, CoreDq) << "[NEW RBO After peephole] " << KqpExprToPrettyString(TExprBase(newStages.back()), ctx);
    return newStages;
}

bool TPhysicalQueryBuilder::IsSuitableToPropagateWideBlocksThroughHashShuffleConnections(const TDqPhyStage& stage) const {
    // Workaround to mitigate https://github.com/ydb-platform/ydb/issues/20440
    // do not mix scalar and block HashShuffle HashV1 connections,
    // if we find any scalar connection then don't propagate blocks through other connections.
    ui32 scalarHashShuffleCount = 0;
    for (size_t i = 0; i < stage.Inputs().Size(); ++i) {
        auto connection = stage.Inputs().Item(i).Maybe<TDqCnHashShuffle>();
        if (connection) {
            auto hashFuncType = RBOCtx.KqpCtx.Config->GetDqDefaultHashShuffleFuncType();
            if (connection.Cast().HashFunc().IsValid()) {
                hashFuncType = FromString<NDq::EHashShuffleFuncType>(connection.Cast().HashFunc().Cast().StringValue());
            }
            scalarHashShuffleCount += (hashFuncType == NDq::EHashShuffleFuncType::HashV1);
        }
    }
    return scalarHashShuffleCount <= 1;
}

bool TPhysicalQueryBuilder::IsCompatibleWithBlocks(const TStructExprType& type, TPositionHandle pos) const {
    TVector<const TTypeAnnotationNode*> types;
    for (const auto& item : type.GetItems()) {
        types.emplace_back(item->GetItemType());
    }
    auto& ctx = RBOCtx.ExprCtx;

    const auto resolveStatus = RBOCtx.TypeCtx.ArrowResolver->AreTypesSupported(ctx.GetPosition(pos), types, ctx);
    YQL_ENSURE(resolveStatus != IArrowResolver::ERROR);
    return resolveStatus == IArrowResolver::OK;
}

bool TPhysicalQueryBuilder::IsSuitableToPropagateWideBlocksThroughConnection(const TDqOutput& output) const {
    if (RBOCtx.KqpCtx.Config->GetBlockChannelsMode() != NKikimrConfig::TTableServiceConfig_EBlockChannelsMode_BLOCK_CHANNELS_AUTO) {
        return false;
    }

    auto stageSettings = NYql::NDq::TDqStageSettings::Parse(output.Stage());
    return stageSettings.WideChannels && stageSettings.OutputNarrowType &&
           IsCompatibleWithBlocks(*stageSettings.OutputNarrowType, output.Stage().Program().Pos());
}

TVector<const TTypeAnnotationNode*> TPhysicalQueryBuilder::GetArgsType(TExprNode::TPtr input) const {
    Y_ENSURE(input->IsLambda());
    auto lambda = TCoLambda(input);

    TVector<const TTypeAnnotationNode*> argsTypes;
    for (const auto& arg : lambda.Args()) {
        const TTypeAnnotationNode* argTypeAnn = arg.Ptr()->GetTypeAnn();
        Y_ENSURE(argTypeAnn);
        argsTypes.push_back(argTypeAnn);
    }

    return argsTypes;
}

TExprNode::TPtr TPhysicalQueryBuilder::TypeAnnotateProgram(TExprNode::TPtr input, const TVector<const TTypeAnnotationNode*>& argsType) {
    auto lambda = TCoLambda(input);
    auto& ctx = RBOCtx.ExprCtx;
    // clang-format off
    auto program = Build<TKqpProgram>(ctx, input->Pos())
        .Lambda(ctx.DeepCopyLambda(*input.Get()))
        .ArgsType(ExpandType(input->Pos(), *ctx.MakeType<TTupleExprType>(argsType), ctx))
    .Done().Ptr();
    // clang-format on

    TypeAnnotate(program);
    return TKqpProgram(program).Lambda().Ptr();
}

TExprNode::TPtr TPhysicalQueryBuilder::PeepHoleOptimize(TExprNode::TPtr input, const TVector<const TTypeAnnotationNode*>& argsType) const {
    auto lambda = TCoLambda(input);
    auto& ctx = RBOCtx.ExprCtx;

    const bool withFinalStageRules = true;
    // clang-format off
    auto program = Build<TKqpProgram>(ctx, input->Pos())
        .Lambda(ctx.DeepCopyLambda(*input.Get()))
        .ArgsType(ExpandType(input->Pos(), *ctx.MakeType<TTupleExprType>(argsType), ctx))
    .Done();
    // clang-format on

    // auto &ctx = RBOCtx.ExprCtx;
    TExprNode::TPtr newProgram;
    auto status =
        ::PeepHoleOptimize(program, newProgram, ctx, RBOCtx.PeepholeTypeAnnTransformer, RBOCtx.TypeCtx, RBOCtx.KqpCtx.Config, false, withFinalStageRules, {});
    if (status != IGraphTransformer::TStatus::Ok) {
        ctx.AddError(TIssue(ctx.GetPosition(program.Pos()), "Peephole optimization failed for stage in NEW RBO"));
        return nullptr;
    }

    return TKqpProgram(newProgram).Lambda().Ptr();
}

void TPhysicalQueryBuilder::TypeAnnotate(TExprNode::TPtr& input) {
    RBOCtx.TypeAnnTransformer.Rewind();
    TExprNode::TPtr output;
    IGraphTransformer::TStatus status(IGraphTransformer::TStatus::Ok);
    do {
        status = RBOCtx.TypeAnnTransformer.Transform(input, output, RBOCtx.ExprCtx);
    } while (status == IGraphTransformer::TStatus::Repeat);

    if (status != IGraphTransformer::TStatus::Ok) {
        RBOCtx.ExprCtx.AddError(TIssue(RBOCtx.ExprCtx.GetPosition(input->Pos()), "Type inference failed for stage in NEW RBO"));
    }
    Y_ENSURE(status == IGraphTransformer::TStatus::Ok);

    input = output;
}

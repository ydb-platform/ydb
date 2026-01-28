#include "kqp_rbo_physical_query_builder.h"
#include <yql/essentials/core/yql_expr_optimize.h>
#include <ydb/library/yql/dq/type_ann/dq_type_ann.h>
#include <ydb/library/yql/dq/opt/dq_opt_peephole.h>
#include <ydb/core/kqp/opt/peephole/kqp_opt_peephole.h>

using namespace NYql::NNodes;
using namespace NKikimr;
using namespace NKikimr::NKqp;

TExprNode::TPtr TPhysicalQueryBuilder::BuildPhysicalQuery() {
    auto phyStages = BuildPhysicalStageGraph();
    auto phyStagesAfterPeephole = PeepHoleOptimizePhysicalStages(std::move(phyStages));
    return BuildPhysicalQuery(std::move(phyStagesAfterPeephole));
}

TVector<std::pair<TExprNode::TPtr, TPositionHandle>> TPhysicalQueryBuilder::BuildPhysicalStageGraph() {
    TVector<std::pair<TExprNode::TPtr, TPositionHandle>> phyStages;
    Graph.TopologicalSort();
    const auto& stageIds = Graph.StageIds;
    const auto& stageInputIds = Graph.StageInputs;
    auto& ctx = RBOCtx.ExprCtx;

    THashMap<int, TExprNode::TPtr> finalizedStages;
    for (const auto id : stageIds) {
        YQL_CLOG(TRACE, CoreDq) << "Finalizing stage " << id;

        TVector<TExprNode::TPtr> inputConnections;
        THashSet<int> processedInputsIds;
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
                    phyStages.emplace_back(newStage, StagePos.at(inputStageId));
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

            stage = BuildDqPhyStage(stageInputConnections, stageInputArgs, Stages.at(id), ctx, StagePos.at(id));
            phyStages.emplace_back(stage, StagePos.at(id));
            YQL_CLOG(TRACE, CoreDq) << "Added stage " << stage->UniqueId();
        }

        finalizedStages[id] = stage;
        YQL_CLOG(TRACE, CoreDq) << "Finalized stage " << id;
    }
    return phyStages;
}

TExprNode::TPtr TPhysicalQueryBuilder::BuildPhysicalQuery(TVector<TExprNode::TPtr>&& physicalStages) {
    Y_ENSURE(physicalStages.size());

    auto& ctx = RBOCtx.ExprCtx;
    TVector<TCoAtom> columnAtomList;
    for (const auto& column : Root.ColumnOrder) {
        columnAtomList.push_back(Build<TCoAtom>(ctx, Root.Pos).Value(column).Done());
    }
    auto columnOrder = Build<TCoAtomList>(ctx, Root.Pos).Add(columnAtomList).Done().Ptr();

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

    TVector<TExprNode::TPtr> txSettings;
    // clang-format off
    txSettings.push_back(Build<TCoNameValueTuple>(ctx, Root.Pos)
                            .Name().Build("type")
                            .Value<TCoAtom>().Build("compute")
                        .Done().Ptr());
    // Build PhysicalTx
    auto physTx = Build<TKqpPhysicalTx>(ctx, Root.Pos)
        .Stages()
            .Add(physicalStages)
        .Build()
        .Results()
            .Add({dqResult})
        .Build()
        .ParamBindings().Build()
        .Settings()
            .Add(txSettings)
        .Build()
    .Done().Ptr();
    // clang-format on

    TypeAnnotate(dqResult);

    YQL_CLOG(TRACE, CoreDq) << "Inferred final type: " << *dqResult->GetTypeAnn();
    // clang-format off
    TVector<TExprNode::TPtr> querySettings;
    querySettings.push_back(Build<TCoNameValueTuple>(ctx, Root.Pos)
                                .Name().Build("type")
                                .Value<TCoAtom>().Build("data_query")
                            .Done().Ptr());

    auto binding = Build<TKqpTxResultBinding>(ctx, Root.Pos)
        .Type(ExpandType(Root.Pos, *dqResult->GetTypeAnn(), ctx))
        .TxIndex().Build("0")
        .ResultIndex().Build("0")
    .Done();

    // Build Physical query
    return Build<TKqpPhysicalQuery>(ctx, Root.Pos)
        .Transactions()
            .Add({physTx})
        .Build()
        .Results()
            .Add({binding})
        .Build()
        .Settings()
            .Add(querySettings)
        .Build()
    .Done().Ptr();
    // clang-format on
}

bool TPhysicalQueryBuilder::CanApplyPeepHole(TExprNode::TPtr input, const std::initializer_list<std::string_view>& callableNames) {
    auto blackList = [&](const TExprNode::TPtr& node) -> bool {
        if (node->IsCallable(callableNames)) {
            return true;
        }
        return false;
    };
    return !FindNode(input, blackList);
}

TExprNode::TPtr TPhysicalQueryBuilder::BuildDqPhyStage(const TVector<TExprNode::TPtr>& inputs, const TVector<TExprNode::TPtr>& args,
                                                       TExprNode::TPtr physicalStageBody, TExprContext& ctx, TPositionHandle pos) {
    // clang-format off
    return Build<TDqPhyStage>(ctx, pos)
        .Inputs()
            .Add(inputs)
        .Build()
        .Program()
            .Args(args)
            .Body(physicalStageBody)
        .Build()
        .Settings(NYql::NDq::TDqStageSettings::New().BuildNode(ctx, pos))
    .Done().Ptr();
    // clang-format on
}

// This function assumes that stages already sorted in topological orders.
TVector<TExprNode::TPtr> TPhysicalQueryBuilder::PeepHoleOptimizePhysicalStages(TVector<std::pair<TExprNode::TPtr, TPositionHandle>>&& physicalStages) {
    Y_ENSURE(physicalStages.size());
    auto root = physicalStages.back().first;
    // Type is required to wrap stage lambda to `KqpProgram`
    if (!root->GetTypeAnn()) {
        TypeAnnotate(root);
    }
    auto& ctx = RBOCtx.ExprCtx;

    TNodeOnNodeOwnedMap replaces;
    TVector<TExprNode::TPtr> newStages;
    for (auto& [stage, pos] : physicalStages) {
        auto dqPhyStage = TDqPhyStage(stage);
        auto stageLambdaAfterPeephole = PeepHoleOptimizeStageLambda(dqPhyStage.Program().Ptr());
        Y_ENSURE(stageLambdaAfterPeephole);
        // clang-format off
        auto newStage = Build<TDqPhyStage>(ctx, pos)
            .Inputs(ctx.ReplaceNodes(dqPhyStage.Inputs().Ptr(), replaces))
            .Program(stageLambdaAfterPeephole)
            .Settings(dqPhyStage.Settings())
            .Outputs(dqPhyStage.Outputs())
        .Done().Ptr();
        // clang-format on
        newStages.push_back(newStage);
        replaces[dqPhyStage.Raw()] = newStage;
    }
    return newStages;
}

TExprNode::TPtr TPhysicalQueryBuilder::PeepHoleOptimizeStageLambda(TExprNode::TPtr stageLambda) {
    auto lambda = TCoLambda(stageLambda);
    // Compute types of inputs to stage lambda
    TVector<const TTypeAnnotationNode*> argTypes;
    for (const auto& arg : lambda.Args()) {
        const auto* argTypeAnn = arg.Ptr()->GetTypeAnn();
        Y_ENSURE(argTypeAnn);
        argTypes.push_back(argTypeAnn);
    }

    // Yql has a strange bug in final stage peephole for `WideCombiner` with empty keys.
    const bool withFinalStageRules = CanApplyPeepHole(lambda.Body().Ptr(), {"WideCombiner"});
    // clang-format off
    auto program = Build<TKqpProgram>(RBOCtx.ExprCtx, stageLambda->Pos())
        .Lambda(RBOCtx.ExprCtx.DeepCopyLambda(*stageLambda.Get()))
        .ArgsType(ExpandType(stageLambda->Pos(), *RBOCtx.ExprCtx.MakeType<TTupleExprType>(argTypes), RBOCtx.ExprCtx))
    .Done();
    // clang-format on

    TExprNode::TPtr newProgram;
    auto status = PeepHoleOptimize(program, newProgram, RBOCtx.ExprCtx, RBOCtx.PeepholeTypeAnnTransformer.GetRef(), RBOCtx.TypeCtx, RBOCtx.KqpCtx.Config, false,
                                   withFinalStageRules, {});
    if (status != IGraphTransformer::TStatus::Ok) {
        RBOCtx.ExprCtx.AddError(TIssue(RBOCtx.ExprCtx.GetPosition(program.Pos()), "Peephole optimization failed for stage in NEW RBO"));
        return nullptr;
    }

    return TKqpProgram(newProgram).Lambda().Ptr();
}

void TPhysicalQueryBuilder::TypeAnnotate(TExprNode::TPtr input) {
    RBOCtx.TypeAnnTransformer->Rewind();
    IGraphTransformer::TStatus status(IGraphTransformer::TStatus::Ok);
    do {
        status = RBOCtx.TypeAnnTransformer->Transform(input, input, RBOCtx.ExprCtx);
    } while (status == IGraphTransformer::TStatus::Repeat);
    Y_ENSURE(status == IGraphTransformer::TStatus::Ok);
}

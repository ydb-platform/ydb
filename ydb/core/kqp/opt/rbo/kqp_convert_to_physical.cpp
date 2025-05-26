#include "kqp_rbo.h"
#include <yql/essentials/utils/log/log.h>

using namespace NYql::NNodes;

namespace {

    using namespace NKikimr;
    using namespace NKikimr::NKqp; 

    TExprNode::TPtr ReplaceArg(TExprNode::TPtr input, TExprNode::TPtr arg, TExprContext& ctx, bool removeAliases=false) {
        if (input->IsCallable("Member")) {
            auto member = TCoMember(input);
            auto memberName = member.Name();
            if (removeAliases) {
                auto strippedName = memberName.StringValue();
                if (auto idx = strippedName.find_last_of('.'); idx != TString::npos) {
                    strippedName =  strippedName.substr(idx+1);
                }
                memberName = Build<TCoAtom>(ctx, input->Pos()).Value(strippedName).Done();
            }
            return Build<TCoMember>(ctx, input->Pos())
                .Struct(arg)
                .Name(memberName)
                .Done().Ptr();
        }
        else if (input->IsCallable()){
            TVector<TExprNode::TPtr> newChildren;
            for (auto c : input->Children()) {
                newChildren.push_back(ReplaceArg(c, arg, ctx, removeAliases));
            }
            return ctx.Builder(input->Pos()).Callable(input->Content()).Add(std::move(newChildren)).Seal().Build();
        }
        else if(input->IsList()){
            TVector<TExprNode::TPtr> newChildren;
            for (auto c : input->Children()) {
                newChildren.push_back(ReplaceArg(c, arg, ctx, removeAliases));
            }
            return ctx.Builder(input->Pos()).List().Add(std::move(newChildren)).Seal().Build();
        }
        else {
            return input;
        }
    }

}

namespace NKikimr {
namespace NKqp {

TExprNode::TPtr ConvertToPhysical(TOpRoot & root,  TExprContext& ctx, TAutoPtr<IGraphTransformer> typeAnnTransformer) {
    THashMap<int, TExprNode::TPtr> stages;
    THashMap<int, TVector<TExprNode::TPtr>> stageArgs;
    auto & graph = root.PlanProps.StageGraph;
    for (auto id : graph.StageIds) {
        stageArgs[id] = TVector<TExprNode::TPtr>();
    }
    
    int stageInputCounter = 0;

    for (auto iter : root ) {
        auto op = iter.Current;

        auto opStageId = *(op->Props.StageId);

        TExprNode::TPtr currentStageBody;
        if (stages.contains(opStageId)) {
            currentStageBody = stages.at(opStageId);
        }

        if (op->Kind == EOperator::EmptySource) {

            TVector<TExprBase> listElements;
            listElements.push_back(Build<TCoAsStruct>(ctx, root.Node->Pos()).Done());

            currentStageBody = Build<TCoIterator>(ctx, root.Node->Pos())
                .List<TCoAsList>()
                    .Add(listElements)
                .Build()
                .Done().Ptr();
            stages[opStageId] = currentStageBody;
            YQL_CLOG(TRACE, CoreDq) << "Converted Empty Source " << opStageId;
        }
        else if (op->Kind == EOperator::Source) {

            auto read = TKqpOpRead(op->Node);
            auto opSource = CastOperator<TOpRead>(op);

        
            auto source = ctx.NewCallable(root.Node->Pos(), "DataSource", {ctx.NewAtom(root.Node->Pos(), "KqpReadRangesSource")});
            currentStageBody = Build<TDqSource>(ctx, root.Node->Pos())
                .DataSource(source)
                .Settings<TKqpReadRangesSourceSettings>()
                    .Table(read.Table())
                    .Columns(read.Columns())
                    .Settings<TCoNameValueTupleList>().Build()
                    .RangesExpr<TCoVoid>().Build()
                    .ExplainPrompt<TCoNameValueTupleList>().Build()
                .Build()
                .Done().Ptr();

            stages[opStageId] = currentStageBody;
            YQL_CLOG(TRACE, CoreDq) << "Converted Read " << opStageId;
        }
        else if (op->Kind == EOperator::Filter) {

            if (!currentStageBody) {
                auto [stageArg, stageInput] = graph.GenerateStageInput(stageInputCounter, root.Node, ctx, *op->Children[0]->Props.StageId);
                stageArgs[opStageId].push_back(stageArg);
                currentStageBody = stageInput;
            }

            auto filter = TKqpOpFilter(op->Node);
            auto filterBody = filter.Lambda().Body();
            auto arg = Build<TCoArgument>(ctx, root.Node->Pos()).Name("arg").Done().Ptr();
            auto newFilterBody = ReplaceArg(filterBody.Ptr(), arg, ctx);
            newFilterBody = ctx.Builder(root.Node->Pos()).Callable("FromPg").Add(0, newFilterBody).Seal().Build();

            TVector<TExprBase> items;

            for (auto iu : op->GetOutputIUs()) {
                auto memberName = "_alias_" + iu.Alias + "." + iu.ColumnName;
                auto tuple = Build<TCoNameValueTuple>(ctx, root.Node->Pos())
                    .Name().Build(memberName)
                    .Value<TCoMember>()
                        .Struct(arg)
                        .Name().Build(memberName)
                    .Build()
                    .Done();
                
                items.push_back(tuple);
            }

            currentStageBody = Build<TCoFlatMap>(ctx, root.Node->Pos())
                .Input(TExprBase(currentStageBody))
                .Lambda<TCoLambda>()
                    .Args({arg})
                    .Body<TCoOptionalIf>()
                        .Predicate<TCoCoalesce>()
                            .Predicate(newFilterBody)
                            .Value<TCoBool>()
                                .Literal().Build("false")
                                .Build()
                            .Build()
                        .Value<TCoAsStruct>()
                            .Add(items)
                        .Build()
                    .Build()
                .Build()
            .Done().Ptr();

            stages[opStageId] = currentStageBody;
            YQL_CLOG(TRACE, CoreDq) << "Converted Filter " << opStageId;
        }
        else if (op->Kind == EOperator::Map) {

            if (!currentStageBody) {
                auto [stageArg, stageInput] = graph.GenerateStageInput(stageInputCounter, root.Node, ctx, *op->Children[0]->Props.StageId);
                stageArgs[opStageId].push_back(stageArg);
                currentStageBody = stageInput;
            }

            auto map = TKqpOpMap(op->Node);

            TVector<TExprBase> items;
            auto arg = Build<TCoArgument>(ctx, root.Node->Pos()).Name("arg").Done().Ptr();

            for (auto mapElement : map.MapElements()) {
                auto tuple = Build<TCoNameValueTuple>(ctx, root.Node->Pos())
                    .Name().Build(mapElement.Variable())
                    .Value(mapElement.Lambda().Body())
                    .Done();
                
                tuple = TCoNameValueTuple(ReplaceArg(tuple.Ptr(), arg, ctx));
                items.push_back(tuple);
            }

            currentStageBody = Build<TCoFlatMap>(ctx, root.Node->Pos())
                .Input(TExprBase(currentStageBody))
                .Lambda<TCoLambda>()
                    .Args({arg})
                    .Body<TCoJust>()
                        .Input<TCoAsStruct>()
                            .Add(items)
                        .Build()
                    .Build()
                .Build()
                .Done().Ptr();

            stages[opStageId] = currentStageBody;
            YQL_CLOG(TRACE, CoreDq) << "Converted Map " << opStageId;
        }
        else if(op->Kind == EOperator::Limit) {
            if (!currentStageBody) {
                auto [stageArg, stageInput] = graph.GenerateStageInput(stageInputCounter, root.Node, ctx, *op->Children[0]->Props.StageId);
                stageArgs[opStageId].push_back(stageArg);
                currentStageBody = stageInput;
            }

            currentStageBody = Build<TCoTake>(ctx, root.Node->Pos())
                .Input(TExprBase(currentStageBody))
                .Count(TKqpOpLimit(op->Node).Count())
                .Done().Ptr();

            stages[opStageId] = currentStageBody;
            YQL_CLOG(TRACE, CoreDq) << "Converted Limit " << opStageId; 
        }
        else if(op->Kind == EOperator::Join) {
            auto join = CastOperator<TOpJoin>(op);

            auto [leftArg, leftInput] = graph.GenerateStageInput(stageInputCounter, root.Node, ctx, *join->GetLeftInput()->Props.StageId);
            stageArgs[opStageId].push_back(leftArg);
            auto [rightArg, rightInput] = graph.GenerateStageInput(stageInputCounter, root.Node, ctx, *join->GetRightInput()->Props.StageId);
            stageArgs[opStageId].push_back(rightArg);

            if (join->JoinKind == "Cross") {
                currentStageBody = Build<TDqPhyCrossJoin>(ctx, root.Node->Pos())
                    .LeftInput(leftInput)
                    .LeftLabel<TCoVoid>().Build()
                    .RightInput(rightInput)
                    .RightLabel<TCoVoid>().Build()
                    .JoinType<TCoAtom>().Value("Cross").Build()
                    .JoinKeys<TDqJoinKeyTupleList>().Build()
                    .LeftJoinKeyNames<TCoAtomList>().Build()
                    .RightJoinKeyNames<TCoAtomList>().Build()
                    .Done().Ptr();
            }
            else if (join->JoinKind == "Inner") {
                TVector<TDqJoinKeyTuple> joinKeys;
                TVector<TCoAtom> leftKeyColumnNames;
                TVector<TCoAtom> rightKeyColumnNames;

                for (auto p : join->JoinKeys) {
                    TString leftFullName = "_alias_" + p.first.Alias + "." + p.first.ColumnName;
                    TString rightFullName = "_alias_" + p.second.Alias + "." + p.second.ColumnName;

                    joinKeys.push_back(Build<TDqJoinKeyTuple>(ctx, root.Node->Pos())
                        .LeftLabel().Value("_alias_" + p.first.Alias).Build()
                        .LeftColumn().Value(p.first.ColumnName).Build()
                        .RightLabel().Value("_alias_" + p.second.Alias).Build()
                        .RightColumn().Value(p.second.ColumnName).Build()
                        .Done());

                    leftKeyColumnNames.push_back(Build<TCoAtom>(ctx, root.Node->Pos()).Value(leftFullName).Done());
                    rightKeyColumnNames.push_back(Build<TCoAtom>(ctx, root.Node->Pos()).Value(rightFullName).Done());
                }

                currentStageBody = Build<TDqPhyGraceJoin>(ctx, root.Node->Pos())
                    .LeftInput(leftInput)
                    .LeftLabel<TCoVoid>().Build()
                    .RightInput(rightInput)
                    .RightLabel<TCoVoid>().Build()
                    .JoinType<TCoAtom>().Value("Inner").Build()
                    .JoinKeys<TDqJoinKeyTupleList>().Add(joinKeys).Build()
                    .LeftJoinKeyNames<TCoAtomList>().Add(leftKeyColumnNames).Build()
                    .RightJoinKeyNames<TCoAtomList>().Add(rightKeyColumnNames).Build()
                    .Done().Ptr();
                }
            else {
                Y_ENSURE(false, "Unsupported join kind");

            }

            stages[opStageId] = currentStageBody;
            YQL_CLOG(TRACE, CoreDq) << "Converted Join " << opStageId; 
        }
        else {
            return root.Node;
        }
    }

    // We need to build up stages in a topological sort order
    graph.TopologicalSort();
    

    THashMap<int, TExprNode::TPtr> finalizedStages;
    TVector<TExprNode::TPtr> txStages;

    auto stageIds = graph.StageIds;
    auto stageInputIds = graph.StageInputs;

    for (auto id : stageIds) {
        YQL_CLOG(TRACE, CoreDq) << "Finalizing stage " << id;

        TVector<TExprNode::TPtr> inputs;
        for (auto inputStageId : stageInputIds.at(id)) {

            auto inputStage = finalizedStages.at(inputStageId);
            auto c = graph.GetConnection(inputStageId, id);
            YQL_CLOG(TRACE, CoreDq) << "Building connection: " << inputStageId << "->" << id << ", " << c->Type;
            auto conn = c->BuildConnection(inputStage, root.Node, ctx);
            YQL_CLOG(TRACE, CoreDq) << "Built connection: " << inputStageId << "->" << id << ", " << c->Type;
            inputs.push_back(conn);
        }
        TExprNode::TPtr stage;
        if (graph.IsSourceStage(id)) {
            stage = stages.at(id);
        }
        else {
            stage = Build<TDqPhyStage>(ctx, root.Node->Pos())
                .Inputs()
                    .Add(inputs)
                    .Build()
                .Program()
                    .Args(stageArgs.at(id))
                    .Body(stages.at(id))
                    .Build()
                .Settings().Build()
                .Done().Ptr();
            txStages.push_back(stage);
        }

        finalizedStages[id] = stage;
        YQL_CLOG(TRACE, CoreDq) << "Finalized stage " << id;
    }

    // Build a union all for the last stage
    int lastStageIdx = stageIds[stageIds.size()-1];

    auto lastStage = finalizedStages.at(lastStageIdx);

    // wrap in DqResult
    auto dqResult = Build<TDqCnResult>(ctx, root.Node->Pos())
        .Output()
            .Stage(lastStage)
            .Index().Build("0")
        .Build()
        .ColumnHints().Build()
        .Done()
        .Ptr();

    TVector<TExprNode::TPtr> txSettings;
    txSettings.push_back(Build<TCoNameValueTuple>(ctx, root.Node->Pos())
                            .Name().Build("type")
                            .Value<TCoAtom>().Build("compute")
                            .Done()
                            .Ptr());
    // Build PhysicalTx
    auto physTx = Build<TKqpPhysicalTx>(ctx, root.Node->Pos())
        .Stages()
            .Add(txStages)
        .Build()
        .Results()
            .Add({dqResult})
        .Build()
        .ParamBindings().Build()
        .Settings()
            .Add(txSettings)
        .Build()
        .Done()
        .Ptr();

    TVector<TExprNode::TPtr> querySettings;
    querySettings.push_back(Build<TCoNameValueTuple>(ctx, root.Node->Pos())
                            .Name().Build("type")
                            .Value<TCoAtom>().Build("data_query")
                            .Done()
                            .Ptr());

    // Build result type
    
    IGraphTransformer::TStatus status(IGraphTransformer::TStatus::Ok);
    do {
        status = typeAnnTransformer->Transform(dqResult, dqResult, ctx);
    } while (status == IGraphTransformer::TStatus::Repeat);

    typeAnnTransformer->Transform(dqResult, dqResult, ctx);
    YQL_CLOG(TRACE, CoreDq) << "Inferred final type: " << *dqResult->GetTypeAnn();

    auto binding = Build<TKqpTxResultBinding>(ctx, root.Node->Pos())
        .Type(ExpandType(root.Node->Pos(), *dqResult->GetTypeAnn(), ctx))
        .TxIndex().Build("0")
        .ResultIndex().Build("0")
        .Done();

    // Build Physical query
    auto physQuery = Build<TKqpPhysicalQuery>(ctx, root.Node->Pos())
        .Transactions()
            .Add({physTx})
        .Build()
        .Results()
            .Add({binding})
        .Build()
        .Settings()
            .Add(querySettings)
        .Build()
        .Done()
        .Ptr();

    //physQuery->SetTypeAnn(dqResult->GetTypeAnn());

    do {
        status = typeAnnTransformer->Transform(physQuery, physQuery, ctx);
    } while (status == IGraphTransformer::TStatus::Repeat);

    YQL_CLOG(TRACE, CoreDq) << "Final plan built";

    return physQuery;

    /*

    auto result = Build<TDqCnUnionAll>(ctx, root.Node->Pos())
        .Output()
            .Stage(finalizedStages.at(lastStageIdx))
            .Index().Build("0")
        .Build()
        .Done().Ptr();

    return result;
    */

}

}
}
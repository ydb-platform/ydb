#include "kqp_new_rbo.h"
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

    TExprNode::TPtr GenerateStageInput(int & stageInputCounter, TExprNode::TPtr & node, TExprContext& ctx) {
        TString inputName = "input_arg" + std::to_string(stageInputCounter++);
        YQL_CLOG(TRACE, CoreDq) << "Created stage argument " << inputName;
        return Build<TCoArgument>(ctx, node->Pos()).Name(inputName).Done().Ptr();
    }
}

namespace NKikimr {
namespace NKqp {

TExprNode::TPtr ConvertToPhysical(TOpRoot & root,  TExprContext& ctx) {
    THashMap<int, TExprNode::TPtr> stages;
    THashMap<int, TVector<TExprNode::TPtr>> stageArgs;
    for (auto id : root.PlanProps.StageGraph.StageIds) {
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

            // Build the actual read callable
            currentStageBody = Build<TKqpReadTableRanges>(ctx, root.Node->Pos())
                .Table(read.Table())
                .Ranges<TCoVoid>().Build()
                .Columns(read.Columns())
                .Settings<TCoNameValueTupleList>().Build()
                .ExplainPrompt<TCoNameValueTupleList>().Build()
                .Done().Ptr();

            // Add a rename to rename all columns to include aliases
            TVector<TExprBase> items;
            auto arg = Build<TCoArgument>(ctx, root.Node->Pos()).Name("arg").Done().Ptr();

            for (auto iu : opSource->GetOutputIUs()) {
                auto tuple = Build<TCoNameValueTuple>(ctx, root.Node->Pos())
                    .Name().Build("_alias_" + iu.Alias + "." + iu.ColumnName)
                    .Value<TCoMember>()
                        .Struct(arg)
                        .Name().Build(iu.ColumnName)
                    .Build()
                    .Done();
                
                items.push_back(tuple);
            }

            currentStageBody = Build<TCoOrderedFlatMap>(ctx, root.Node->Pos())
                .Input(currentStageBody)
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
            YQL_CLOG(TRACE, CoreDq) << "Converted Read " << opStageId;
        }
        else if (op->Kind == EOperator::Filter) {

            if (!currentStageBody) {
                auto stageInput = GenerateStageInput(stageInputCounter, root.Node, ctx);
                stageArgs[opStageId].push_back(stageInput);
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
                auto stageInput = GenerateStageInput(stageInputCounter, root.Node, ctx);
                stageArgs[opStageId].push_back(stageInput);
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
                auto stageInput = GenerateStageInput(stageInputCounter, root.Node, ctx);
                stageArgs[opStageId].push_back(stageInput);
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

            auto leftInput = GenerateStageInput(stageInputCounter, root.Node, ctx);
            stageArgs[opStageId].push_back(leftInput);
            auto rightInput = GenerateStageInput(stageInputCounter, root.Node, ctx);
            stageArgs[opStageId].push_back(rightInput);

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
    auto graph = root.PlanProps.StageGraph;
    graph.TopologicalSort();
    

    THashMap<int, TExprNode::TPtr> finalizedStages;
    auto stageIds = graph.StageIds;
    auto stageInputIds = graph.StageInputs;

    for (auto id : stageIds) {
        YQL_CLOG(TRACE, CoreDq) << "Finalizing stage " << id;

        TVector<TExprNode::TPtr> inputs;
        for (auto inputStageId : stageInputIds.at(id)) {

            auto inputStage = finalizedStages.at(inputStageId);
            auto c = graph.GetConnection(inputStageId, id);
            auto conn = c->BuildConnection(inputStage, root.Node, ctx);
            YQL_CLOG(TRACE, CoreDq) << "Build connection: " << inputStageId << "->" << id << ", " << c->Type;
            inputs.push_back(conn);
        }

        auto stage = Build<TDqStage>(ctx, root.Node->Pos())
            .Inputs()
                .Add(inputs)
                .Build()
            .Program()
                .Args(stageArgs.at(id))
                .Body(stages.at(id))
                .Build()
            .Settings().Build()
            .Done();

        finalizedStages[id] = stage.Ptr();
        YQL_CLOG(TRACE, CoreDq) << "Finalized stage " << id;
    }

    // Build a union all for the last stage
    int lastStageIdx = stageIds[stageIds.size()-1];

    auto result = Build<TDqCnUnionAll>(ctx, root.Node->Pos())
        .Output()
            .Stage(finalizedStages.at(lastStageIdx))
            .Index().Build("0")
        .Build()
        .Done().Ptr();
    YQL_CLOG(TRACE, CoreDq) << "Final plan built";

    return result;
}

}
}
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
        return Build<TCoArgument>(ctx, node->Pos()).Name("input_arg" + std::to_string(stageInputCounter++)).Done().Ptr();
    }

    // FIXME: Currently builds only unionAll connection and assumes stages have 1 output
    TExprNode::TPtr BuildConnection(TExprNode::TPtr inputStage, TString connectionType, TExprNode::TPtr & node, TExprContext& ctx) {
        return Build<TDqCnUnionAll>(ctx, node->Pos())
            .Output()
                .Stage(inputStage)
                .Index().Build("0")
            .Build()
            .Done().Ptr();
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

            currentStageBody = Build<TKqpReadTableRanges>(ctx, root.Node->Pos())
                .Table(read.Table())
                .Ranges<TCoVoid>().Build()
                .Columns(read.Columns())
                .Settings<TCoNameValueTupleList>().Build()
                .ExplainPrompt<TCoNameValueTupleList>().Build()
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
            auto newFilterBody = ReplaceArg(filterBody.Ptr(), arg, ctx, true);
            newFilterBody = ctx.Builder(root.Node->Pos()).Callable("FromPg").Add(0, newFilterBody).Seal().Build();

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
                        .Value(currentStageBody)
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
                
                tuple = TCoNameValueTuple(ReplaceArg(tuple.Ptr(), arg, ctx, true));
                items.push_back(tuple);
            }

            auto asStruct = Build<TCoAsStruct>(ctx, root.Node->Pos())
                .Add(items)
                .Done();

            TVector<TExprBase> listElements;
            listElements.push_back(asStruct);

            currentStageBody = Build<TCoFlatMap>(ctx, root.Node->Pos())
                .Input(TExprBase(currentStageBody))
                .Lambda<TCoLambda>()
                    .Args({arg})
                    .Body<TCoAsList>()
                        .Add(listElements)
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
        else {
            return root.Node;
        }
    }

    // Build up stages in the sequence in which they were created

    THashMap<int, TExprNode::TPtr> finalizedStages;
    auto graph = root.PlanProps.StageGraph;
    auto stageIds = graph.StageIds;
    auto stageInputIds = graph.StageInputs;

    for (auto id : stageIds) {
        YQL_CLOG(TRACE, CoreDq) << "Finalizing stage " << id;

        TVector<TExprNode::TPtr> inputs;
        for (auto inputStageId : stageInputIds.at(id)) {
            auto inputStage = finalizedStages.at(inputStageId);
            auto conn = BuildConnection(inputStage, graph.GetConnection(inputStageId, id), root.Node, ctx);
            YQL_CLOG(TRACE, CoreDq) << "Build connection" << id;
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

    return Build<TDqCnUnionAll>(ctx, root.Node->Pos())
        .Output()
            .Stage(finalizedStages.at(lastStageIdx))
            .Index().Build("0")
        .Build()
        .Done().Ptr();
}

}
}
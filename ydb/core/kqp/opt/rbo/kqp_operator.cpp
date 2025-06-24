#include "kqp_operator.h"

namespace {
using namespace NKikimr;
using namespace NKqp;
using namespace NYql;
using namespace NNodes;

std::shared_ptr<IOperator> ExprNodeToOperator (TExprNode::TPtr node) {
    if (NYql::NNodes::TKqpOpEmptySource::Match(node.Get())) {
        return std::make_shared<TOpEmptySource>(node);
    }
    else if (NYql::NNodes::TKqpOpRead::Match(node.Get())) {
        return std::make_shared<TOpRead>(node);
    }
    else if (NYql::NNodes::TKqpOpMap::Match(node.Get())) {
        return std::make_shared<TOpMap>(node);
    }
    else if (NYql::NNodes::TKqpOpFilter::Match(node.Get())) {
        return std::make_shared<TOpFilter>(node);
    }
    else if (NYql::NNodes::TKqpOpJoin::Match(node.Get())) {
        return std::make_shared<TOpJoin>(node);
    }
    else if (NYql::NNodes::TKqpOpLimit::Match(node.Get())) {
        return std::make_shared<TOpLimit>(node);
    }
    else if (NYql::NNodes::TKqpOpRoot::Match(node.Get())) {
        return std::make_shared<TOpRoot>(node);
    }
    else {
        YQL_ENSURE(false, "Unknown operator node");
    }
}

void DFS(int vertex, TVector<int>& sortedStages, THashSet<int>& visited, const THashMap<int, TVector<int>> & stageInputs) {
    visited.emplace(vertex);

    for ( auto u : stageInputs.at(vertex)) {
        if (!visited.contains(u)) {
            DFS(u, sortedStages, visited, stageInputs);
        }
    }

    sortedStages.push_back(vertex);
}

TExprNode::TPtr AddRenames(TExprNode::TPtr input, TExprContext& ctx, TVector<TInfoUnit> renames) {
    TVector<TExprBase> items;
    auto arg = Build<TCoArgument>(ctx, input->Pos()).Name("arg").Done().Ptr();

    for (auto iu : renames) {
        auto tuple = Build<TCoNameValueTuple>(ctx, input->Pos())
            .Name().Build("_alias_" + iu.Alias + "." + iu.ColumnName)
            .Value<TCoMember>()
                .Struct(arg)
                .Name().Build(iu.ColumnName)
            .Build()
            .Done();
        
        items.push_back(tuple);
    }

    /*
    return Build<TCoFlatMap>(ctx, input->Pos())
        .Input(input)
        .Lambda<TCoLambda>()
            .Args({arg})
            .Body<TCoJust>()
                .Input<TCoAsStruct>()
                    .Add(items)
                .Build()
            .Build()
        .Build()
        .Done().Ptr();
    */
    
    return Build<TCoMap>(ctx, input->Pos())
        .Input(input)
        .Lambda<TCoLambda>()
            .Args({arg})
            .Body<TCoAsStruct>()
                .Add(items)
            .Build()
        .Build()
        .Done().Ptr();
    
}

TExprNode::TPtr BuildSourceStage(TExprNode::TPtr dqsource, TExprContext& ctx) {
    auto arg = Build<TCoArgument>(ctx, dqsource->Pos()).Name("arg").Done().Ptr();
    return Build<TDqPhyStage>(ctx, dqsource->Pos())
        .Inputs()
            .Add({dqsource})
            .Build()
        .Program()
            .Args({arg})
            .Body(arg)
            .Build()
        .Settings().Build()
        .Done().Ptr();
}

}

namespace NKikimr {
namespace NKqp {

using namespace NYql;
using namespace NNodes;

void GetAllMembers(TExprNode::TPtr node, TVector<TInfoUnit>& IUs) {
    if (node->IsCallable("Member")) {
        auto member = TCoMember(node);
        IUs.push_back(TInfoUnit(member.Name().StringValue()));
        return;
    }

    for (auto c : node->Children()) {
        GetAllMembers(c, IUs);
    }
}

TInfoUnit::TInfoUnit(TString name) {
    if (auto idx = name.find('.'); idx != TString::npos) {
        Alias = name.substr(0, idx);
        if (Alias.StartsWith("_alias_")) {
            Alias = Alias.substr(7);
        }
        ColumnName = name.substr(idx+1);
    }
    else {
        Alias = "";
        ColumnName = name;
    }
}

bool operator == (const TInfoUnit& lhs, const TInfoUnit& rhs) {
    return lhs.Alias == rhs.Alias && lhs.ColumnName == rhs.ColumnName;
}

TExprNode::TPtr TBroadcastConnection::BuildConnection(TExprNode::TPtr inputStage, TExprNode::TPtr & node, TExprNode::TPtr & newStage, TExprContext& ctx) {
    if (FromSourceStage) {
        inputStage = BuildSourceStage(inputStage, ctx);
        newStage = inputStage;
    }
    return Build<TDqCnBroadcast>(ctx, node->Pos())
        .Output()
            .Stage(inputStage)
            .Index().Build("0")
        .Build()
        .Done().Ptr();
}

TExprNode::TPtr TMapConnection::BuildConnection(TExprNode::TPtr inputStage, TExprNode::TPtr & node, TExprNode::TPtr & newStage, TExprContext& ctx) {
    if (FromSourceStage) {
        inputStage = BuildSourceStage(inputStage, ctx);
        newStage = inputStage;
    }
    return Build<TDqCnMap>(ctx, node->Pos())
        .Output()
            .Stage(inputStage)
            .Index().Build("0")
        .Build()
        .Done().Ptr();
}

TExprNode::TPtr TUnionAllConnection::BuildConnection(TExprNode::TPtr inputStage, TExprNode::TPtr & node, TExprNode::TPtr & newStage, TExprContext& ctx) {
    if (FromSourceStage) {
        inputStage = BuildSourceStage(inputStage, ctx);
        newStage = inputStage;
    }
    return Build<TDqCnUnionAll>(ctx, node->Pos())
        .Output()
            .Stage(inputStage)
            .Index().Build("0")
        .Build()
        .Done().Ptr();
}

TExprNode::TPtr TShuffleConnection::BuildConnection(TExprNode::TPtr inputStage, TExprNode::TPtr & node, TExprNode::TPtr & newStage, TExprContext& ctx) {
    TVector<TCoAtom> keyColumns;

    if (FromSourceStage) {
        inputStage = BuildSourceStage(inputStage, ctx);
        newStage = inputStage;
    }

    for ( auto k : Keys ) {
        TString columnName;
        if (FromSourceStage) {
            columnName = k.ColumnName;
        } else {
            columnName = "_alias_" + k.Alias + "." + k.ColumnName;
        }
        auto atom = Build<TCoAtom>(ctx, node->Pos()).Value(columnName).Done();
        keyColumns.push_back(atom);
    }

    return Build<TDqCnHashShuffle>(ctx, node->Pos())
        .Output()
            .Stage(inputStage)
            .Index().Build("0")
        .Build()
        .KeyColumns()
            .Add(keyColumns)
        .Build()
        .Done().Ptr();
}

TExprNode::TPtr TSourceConnection::BuildConnection(TExprNode::TPtr inputStage, TExprNode::TPtr & node, TExprNode::TPtr & newStage, TExprContext& ctx) {
    Y_UNUSED(node);
    Y_UNUSED(newStage);
    Y_UNUSED(ctx);
    return inputStage;
}

std::pair<TExprNode::TPtr,TExprNode::TPtr> TStageGraph::GenerateStageInput(int & stageInputCounter, TExprNode::TPtr & node, TExprContext& ctx, int fromStage)
{
   TString inputName = "input_arg" + std::to_string(stageInputCounter++);
   YQL_CLOG(TRACE, CoreDq) << "Created stage argument " << inputName;
   auto arg = Build<TCoArgument>(ctx, node->Pos()).Name(inputName).Done().Ptr();
   auto output = arg;

   if (IsSourceStage(fromStage)) {
       output = AddRenames(arg, ctx, StageAttributes.at(fromStage));
   }

   return std::make_pair(arg,output);
}

void TStageGraph::TopologicalSort() {
    TVector<int> sortedStages;
    THashSet<int> visited;

    for (auto id : StageIds) {
        if (!visited.contains(id)) {
            DFS(id, sortedStages, visited, StageInputs);
        }
    }

    StageIds = sortedStages;
}

TOpRead::TOpRead(TExprNode::TPtr node) : IOperator(EOperator::Source, node) {
    auto opSource = TKqpOpRead(node);

    auto alias = opSource.Alias().StringValue();
    for (auto c : opSource.Columns()) {
        OutputIUs.push_back(TInfoUnit(alias, c.StringValue()));
    }

    TableName= alias;
}

std::shared_ptr<IOperator> TOpRead::Rebuild(TExprContext& ctx) {
    Y_UNUSED(ctx);
    return std::make_shared<TOpRead>(Node);
}

TOpMap::TOpMap(TExprNode::TPtr node) : IUnaryOperator(EOperator::Map, node) {
    auto opMap = TKqpOpMap(node);

    Children.push_back(ExprNodeToOperator(opMap.Input().Ptr()));

    for (auto mapElement : opMap.MapElements()) {
        OutputIUs.push_back(TInfoUnit("", mapElement.Variable().StringValue()));
    }
}

std::shared_ptr<IOperator> TOpMap::Rebuild(TExprContext& ctx) {
    auto current = TKqpOpMap(Node);
    auto newInput = Children[0]->Rebuild(ctx)->Node;

    TVector<TExprNode::TPtr> newMapElements;

    for (auto mapEl : current.MapElements()) {
        newMapElements.push_back(Build<TKqpOpMapElement>(ctx, Node->Pos())
            .Input(newInput)
            .Variable(mapEl.Variable())
            .Lambda(mapEl.Lambda())
            .Done().Ptr());
    }

    auto node = Build<TKqpOpMap>(ctx, Node->Pos())
        .Input(newInput)
        .MapElements()
            .Add(newMapElements)
        .Build()
        .Done().Ptr();
    return std::make_shared<TOpMap>(node);
}

TOpFilter::TOpFilter(TExprNode::TPtr node) : IUnaryOperator(EOperator::Filter, node) {
    auto opFilter = TKqpOpFilter(node);

    Children.push_back(ExprNodeToOperator(opFilter.Input().Ptr()));

    OutputIUs = Children[0]->GetOutputIUs();
}

TOpFilter::TOpFilter(std::shared_ptr<IOperator> input, TExprNode::TPtr filterLambda, TExprContext& ctx, TPositionHandle pos) : IUnaryOperator(EOperator::Filter) {
    Children.push_back(input);
    Node = Build<TKqpOpFilter>(ctx, pos)
        .Input(Children[0]->Node)
        .Lambda(filterLambda)
        .Done().Ptr();
}

std::shared_ptr<IOperator> TOpFilter::Rebuild(TExprContext& ctx) {
    auto current = TKqpOpFilter(Node);
    auto node = Build<TKqpOpFilter>(ctx, Node->Pos())
        .Input(Children[0]->Rebuild(ctx)->Node)
        .Lambda(current.Lambda())
        .Done().Ptr();
    return std::make_shared<TOpFilter>(node);
}

TVector<TInfoUnit> TOpFilter::GetFilterIUs() const {
    TVector<TInfoUnit> res;

    auto opFilter = TKqpOpFilter(Node);
    auto lambdaBody = opFilter.Lambda().Body();

    GetAllMembers(lambdaBody.Ptr(), res);
    return res;
}

TConjunctInfo TOpFilter::GetConjuctInfo() const {
    TConjunctInfo res;

    auto opFilter = TKqpOpFilter(Node);
    auto lambdaBody = opFilter.Lambda().Body().Ptr();
    if (lambdaBody->IsCallable("ToPg")) {
        lambdaBody = lambdaBody->ChildPtr(0);
        res.ToPg = true;
    }

    if (lambdaBody->IsCallable("And")) {
        for (auto conj : lambdaBody->Children()) {
            auto conjObj = conj;

            if (conj->IsCallable("FromPg")) {
                conjObj = conj->ChildPtr(0);
            }

            if (conjObj->IsCallable("PgResolvedOp") && conjObj->Child(0)->Content()=="=") {
                auto leftArg = conjObj->Child(2);
                auto rightArg = conjObj->Child(3);

                if (leftArg->IsCallable("ToPg")) {
                    leftArg = leftArg->Child(0);
                }
                if (rightArg->IsCallable("ToPg")) {
                    rightArg = rightArg->Child(0);
                }

                if (!leftArg->IsCallable("Member") || !rightArg->IsCallable("Member")) {
                    TVector<TInfoUnit> conjIUs;
                    GetAllMembers(conj, conjIUs);
                    res.Filters.push_back(TFilterInfo(conj, conjIUs));
                }
                else {
                    TVector<TInfoUnit> leftIUs;
                    TVector<TInfoUnit> rightIUs;
                    GetAllMembers(leftArg, leftIUs);
                    GetAllMembers(rightArg, rightIUs);
                    res.JoinConditions.push_back(TJoinConditionInfo(conjObj, leftIUs[0], rightIUs[0]));
                }
            }
            else {
                TVector<TInfoUnit> conjIUs;
                GetAllMembers(conj, conjIUs);
                res.Filters.push_back(TFilterInfo(conj, conjIUs));
            }
        }
    }
    else {
        TVector<TInfoUnit> filterIUs;
        GetAllMembers(lambdaBody, filterIUs);
        res.Filters.push_back(TFilterInfo(lambdaBody, filterIUs));
    }

    return res;
}


TOpJoin::TOpJoin(TExprNode::TPtr node) : IBinaryOperator(EOperator::Join, node) {
    auto opJoin = TKqpOpJoin(node);

    Children.push_back(ExprNodeToOperator(opJoin.LeftInput().Ptr()));
    Children.push_back(ExprNodeToOperator(opJoin.RightInput().Ptr()));

    auto leftInputIUs = Children[0]->GetOutputIUs();
    auto rightInputIUs = Children[1]->GetOutputIUs();

    OutputIUs.insert(OutputIUs.end(), leftInputIUs.begin(), leftInputIUs.end());
    OutputIUs.insert(OutputIUs.end(), rightInputIUs.begin(), rightInputIUs.end());

    JoinKind = opJoin.JoinKind().StringValue();
    for (auto k : opJoin.JoinKeys()) {
        TInfoUnit leftKey(k.LeftLabel().StringValue(), k.LeftColumn().StringValue());
        TInfoUnit rightKey(k.RightLabel().StringValue(), k.RightColumn().StringValue());

        JoinKeys.push_back(std::make_pair(leftKey, rightKey));
    }
}

std::shared_ptr<IOperator> TOpJoin::Rebuild(TExprContext& ctx) {
    auto current = TKqpOpJoin(Node);
    
    TVector<TDqJoinKeyTuple> keys;
    for (auto k : JoinKeys ) {
        keys.push_back(Build<TDqJoinKeyTuple>(ctx, Node->Pos())
                .LeftLabel().Value(k.first.Alias).Build()
                .LeftColumn().Value(k.first.ColumnName).Build()
                .RightLabel().Value(k.second.Alias).Build()
                .RightColumn().Value(k.second.ColumnName).Build()
                .Done());
    }

    auto joinKeys = Build<TDqJoinKeyTupleList>(ctx, Node->Pos()).Add(keys).Done();

    auto node = Build<TKqpOpJoin>(ctx, Node->Pos())
        .LeftInput(Children[0]->Rebuild(ctx)->Node)
        .RightInput(Children[1]->Rebuild(ctx)->Node)
        .JoinKind().Value(JoinKind).Build()
        .JoinKeys(joinKeys)
        .Done().Ptr();
    return std::make_shared<TOpJoin>(node);
}

TOpLimit::TOpLimit(TExprNode::TPtr node) : IUnaryOperator(EOperator::Limit, node) {
    auto opLimit = TKqpOpLimit(node);

    Children.push_back(ExprNodeToOperator(opLimit.Input().Ptr()));

    OutputIUs = Children[0]->GetOutputIUs();
}

std::shared_ptr<IOperator> TOpLimit::Rebuild(TExprContext& ctx) {
    auto current = TKqpOpLimit(Node);
    auto node = Build<TKqpOpLimit>(ctx, Node->Pos())
        .Input(Children[0]->Rebuild(ctx)->Node)
        .Count(current.Count())
        .Done().Ptr();
    return std::make_shared<TOpLimit>(node);
}

TOpRoot::TOpRoot(TExprNode::TPtr node) : IUnaryOperator(EOperator::Root, node) {
    auto opRoot = TKqpOpRoot(node);

    Children.push_back(ExprNodeToOperator(opRoot.Input().Ptr()));

    OutputIUs = Children[0]->GetOutputIUs();
}

std::shared_ptr<IOperator> TOpRoot::Rebuild(TExprContext& ctx) {
    auto node = Build<TKqpOpRoot>(ctx, Node->Pos())
        .Input(Children[0]->Rebuild(ctx)->Node)
        .Done().Ptr();
    return std::make_shared<TOpRoot>(node);
}

TVector<TInfoUnit> IUSetDiff(TVector<TInfoUnit> left, TVector<TInfoUnit> right) {
    TVector<TInfoUnit> res;

    for (auto & unit : left ) {
        if (std::find(right.begin(), right.end(), unit) == right.end()) {
            res.push_back(unit);
        }
    }
    return res;
}

}
}
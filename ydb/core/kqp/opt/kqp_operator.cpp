#include "kqp_operator.h"

namespace {
using namespace NKikimr;
using namespace NKqp;
using namespace NYql;
using namespace NNodes;

std::shared_ptr<IOperator> ExprNodeToOperator (TExprNode::TPtr node) {
    if (NYql::NNodes::TKqpOpEmptySource::Match(node.Get())) {
        return std::make_shared<TOpEmptySource>();
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
    else if (NYql::NNodes::TKqpOpRoot::Match(node.Get())) {
        return std::make_shared<TOpRoot>(node);
    }
    else {
        YQL_ENSURE(false, "Unknown operator node");
    }
}

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

}

namespace NKikimr {
namespace NKqp {

using namespace NYql;
using namespace NNodes;

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

inline bool operator == (const TInfoUnit& lhs, const TInfoUnit& rhs) {
    return lhs.Alias == rhs.Alias && lhs.ColumnName == rhs.ColumnName;
}

TVector<std::shared_ptr<IOperator>*> IOperator::DescendantsDFS() {
    TVector<std::shared_ptr<IOperator>*> res;
    DescendantsDFS_rec(Children, 0, res);
    return res;
}

void IOperator::DescendantsDFS_rec(TVector<std::shared_ptr<IOperator>> & children, size_t index, TVector<std::shared_ptr<IOperator>*> & res) {
    for (size_t i=0; i<Children.size(); i++) {
        children[index]->DescendantsDFS_rec(Children, i, res);
    }
    res.push_back(&children[index]);
} 

TOpRead::TOpRead(TExprNode::TPtr node) : IOperator(EOperator::Source, node) {
    auto opSource = TKqpOpRead(node);

    auto alias = opSource.Alias().StringValue();
    for (auto c : opSource.Columns()) {
        OutputIUs.push_back(TInfoUnit(alias, c.StringValue()));
    }
}

std::shared_ptr<IOperator> TOpRead::Rebuild(TExprContext& ctx) {
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
                    res.Filters.push_back(std::make_pair(conj, conjIUs));
                }
                else {
                    TVector<TInfoUnit> leftIUs;
                    TVector<TInfoUnit> rightIUs;
                    GetAllMembers(leftArg, leftIUs);
                    GetAllMembers(rightArg, rightIUs);
                    res.JoinConditions.push_back(std::make_tuple(conjObj, leftIUs[0], rightIUs[0]));
                }
            }
            else {
                TVector<TInfoUnit> conjIUs;
                GetAllMembers(conj, conjIUs);
                res.Filters.push_back(std::make_pair(conj, conjIUs));
            }
        }
    }
    else {
        TVector<TInfoUnit> filterIUs;
        GetAllMembers(lambdaBody, filterIUs);
        res.Filters.push_back(std::make_pair(lambdaBody, filterIUs));
    }

    return res;
}


TOpJoin::TOpJoin(TExprNode::TPtr node) : IBinaryOperator(EOperator::Join, node) {
    auto opJoin = TKqpOpJoin(node);

    Children.push_back(ExprNodeToOperator(opJoin.LeftInput().Ptr()));
    Children.push_back(ExprNodeToOperator(opJoin.RightInput().Ptr()));

    auto leftInputIUs = Children[0]->GetOutputIUs();
    auto rightInputIUs = Children[1]->GetOutputIUs();

    //FIXME: Check the alias
    OutputIUs.insert(OutputIUs.end(), leftInputIUs.begin(), leftInputIUs.end());
    OutputIUs.insert(OutputIUs.end(), rightInputIUs.begin(), rightInputIUs.end());
}

std::shared_ptr<IOperator> TOpJoin::Rebuild(TExprContext& ctx) {
    auto current = TKqpOpJoin(Node);
    auto node = Build<TKqpOpJoin>(ctx, Node->Pos())
        .LeftInput(Children[0]->Rebuild(ctx)->Node)
        .RightInput(Children[1]->Rebuild(ctx)->Node)
        .LeftLabel(current.LeftLabel())
        .RightLabel(current.RightLabel())
        .JoinKind(current.JoinKind())
        .JoinKeys(current.JoinKeys())
        .Done().Ptr();
    return std::make_shared<TOpJoin>(node);
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
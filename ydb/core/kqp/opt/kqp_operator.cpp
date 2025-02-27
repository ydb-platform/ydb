#include "kqp_operator.h"

namespace {
using namespace NKikimr;
using namespace NKqp;

IOperator ExprNodeToOperator (TExprNode::TPtr node) {
    if (NYql::NNodes::TKqpOpEmptySource::Match(node.Get())) {
        return TOpEmptySource();
    }
    else if (NYql::NNodes::TKqpOpRead::Match(node.Get())) {
        return TOpRead(node);
    }
    else if (NYql::NNodes::TKqpOpMap::Match(node.Get())) {
        return TOpMap(node);
    }
    else if (NYql::NNodes::TKqpOpFilter::Match(node.Get())) {
        return TOpFilter(node);
    }
    else if (NYql::NNodes::TKqpOpJoin::Match(node.Get())) {
        return TOpJoin(node);
    }
    else if (NYql::NNodes::TKqpOpRoot::Match(node.Get())) {
        return TOpRoot(node);
    }
    else {
        YQL_ENSURE(false, "Unknown operator node");
    }
}

}

namespace NKikimr {
namespace NKqp {

using namespace NYql;
using namespace NNodes;

TOpRead::TOpRead(TExprNode::TPtr node) : IOperator(EOperator::Source, node) {
    auto opSource = TKqpOpRead(node);

    auto alias = opSource.Alias().StringValue();
    for (auto c : opSource.Columns()) {
        OutputIUs.push_back(std::make_pair(alias, c.StringValue()));
    }
}

TOpMap::TOpMap(TExprNode::TPtr node) : IOperator(EOperator::Map, node) {
    auto opMap = TKqpOpMap(node);

    Children.push_back(ExprNodeToOperator(opMap.Input().Ptr()));

    for (auto mapElement : opMap.MapElements()) {
        OutputIUs.push_back(std::make_pair("", mapElement.Variable().StringValue()));
    }
}

TOpFilter::TOpFilter(TExprNode::TPtr node) : IOperator(EOperator::Filter, node) {
    auto opFilter = TKqpOpFilter(node);

    Children.push_back(ExprNodeToOperator(opFilter.Input().Ptr()));

    OutputIUs = Children[0].GetOutputIUs();
}

TOpJoin::TOpJoin(TExprNode::TPtr node) : IOperator(EOperator::Join, node) {
    auto opJoin = TKqpOpJoin(node);

    Children.push_back(ExprNodeToOperator(opJoin.LeftInput().Ptr()));
    Children.push_back(ExprNodeToOperator(opJoin.RightInput().Ptr()));

    auto leftInputIUs = Children[0].GetOutputIUs();
    auto rightInputIUs = Children[1].GetOutputIUs();
    //FIXME: Finish output ius

    OutputIUs.insert(OutputIUs.end(), rightInputIUs.begin(), rightInputIUs.end());
}

TOpRoot::TOpRoot(TExprNode::TPtr node) : IOperator(EOperator::Root, node) {
    auto opRoot = TKqpOpRoot(node);

    Children.push_back(ExprNodeToOperator(opRoot.Input().Ptr()));

    OutputIUs = Children[0].GetOutputIUs();
}

}
}
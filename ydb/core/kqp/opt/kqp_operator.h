#pragma once

#include "kqp_opt.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <yql/essentials/ast/yql_expr.h>

namespace NKikimr {
namespace NKqp {

using namespace NYql;

enum EOperator : ui32 {
    EmptySource,
    Source,
    Map,
    Filter,
    Join,
    Root
};

class IOperator {
    public:

    IOperator(EOperator kind, TExprNode::TPtr node) :
        Kind(kind),
        Node(node)
        {}

    virtual ~IOperator() = default;
        
    const TVector<IOperator>& GetChildren() {
        return Children;
    }

    virtual TVector<std::pair<TString, TString>> GetOutputIUs() {
        return OutputIUs;
    }

    const EOperator Kind;
    TExprNode::TPtr Node;
    TVector<IOperator> Children;
    TVector<std::pair<TString, TString>> OutputIUs;
};

class TOpEmptySource : public IOperator {
    public:
    TOpEmptySource() : IOperator(EOperator::EmptySource, nullptr) {}
};

class TOpRead : public IOperator {
    public:
    TOpRead(TExprNode::TPtr node);
};

class TOpMap : public IOperator {
    public:
    TOpMap(TExprNode::TPtr node);
};

class TOpFilter : public IOperator {
    public:
    TOpFilter(TExprNode::TPtr node);
};

class TOpJoin : public IOperator {
    public:
    TOpJoin(TExprNode::TPtr node);
};

class TOpRoot : public IOperator {
    public:
    TOpRoot(TExprNode::TPtr node);
};

}
}
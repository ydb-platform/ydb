#pragma once

#include "kqp_opt.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <yql/essentials/ast/yql_expr.h>
#include <iterator>
#include <cstddef> 

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

struct TInfoUnit {
    TInfoUnit(TString alias, TString column): Alias(alias), ColumnName(column) {}
    TInfoUnit(TString name);

    TString Alias;
    TString ColumnName;

    struct THashFunction
    {
        size_t operator()(const TInfoUnit& c) const
        {
            return THash<TString>{}(c.Alias) ^ THash<TString>{}(c.ColumnName);
        }
    };
};

inline bool operator == (const TInfoUnit& lhs, const TInfoUnit& rhs);

struct TConjunctInfo {
    bool ToPg = false;
    TVector<std::pair<TExprNode::TPtr, TVector<TInfoUnit>>> Filters;
    TVector<std::tuple<TExprNode::TPtr, TInfoUnit, TInfoUnit>> JoinConditions;
};

class IOperator {
    public:

    IOperator(EOperator kind, TExprNode::TPtr node) :
        Kind(kind),
        Node(node)
        {}

    virtual ~IOperator() = default;
        
    const TVector<std::shared_ptr<IOperator>>& GetChildren() {
        return Children;
    }

    virtual TVector<TInfoUnit> GetOutputIUs() {
        return OutputIUs;
    }

    TVector<std::shared_ptr<IOperator>*> DescendantsDFS();
    void DescendantsDFS_rec(TVector<std::shared_ptr<IOperator>> & children, size_t index, TVector<std::shared_ptr<IOperator>*> & vec);

    virtual std::shared_ptr<IOperator> Rebuild(TExprContext& ctx) = 0;

    const EOperator Kind;
    TExprNode::TPtr Node;
    TVector<std::shared_ptr<IOperator>> Children;
    TVector<TInfoUnit> OutputIUs;
};

class IUnaryOperator : public IOperator {
    public:
    IUnaryOperator(EOperator kind, TExprNode::TPtr node) : IOperator(kind, node) {}
    std::shared_ptr<IOperator>& GetInput() { return Children[0]; }
};

class IBinaryOperator : public IOperator {
    public:
    IBinaryOperator(EOperator kind, TExprNode::TPtr node) : IOperator(kind, node) {}
    std::shared_ptr<IOperator>& GetLeftInput() { return Children[0]; }
    std::shared_ptr<IOperator>& GetRightInput() { return Children[1]; }
};

class TOpEmptySource : public IOperator {
    public:
    TOpEmptySource() : IOperator(EOperator::EmptySource, nullptr) {}
    virtual std::shared_ptr<IOperator> Rebuild(TExprContext& ctx) override { return std::make_shared<TOpEmptySource>(); }

};

class TOpRead : public IOperator {
    public:
    TOpRead(TExprNode::TPtr node);
    virtual std::shared_ptr<IOperator> Rebuild(TExprContext& ctx) override;

};

class TOpMap : public IUnaryOperator {
    public:
    TOpMap(TExprNode::TPtr node);
    virtual std::shared_ptr<IOperator> Rebuild(TExprContext& ctx) override;

};

class TOpFilter : public IUnaryOperator {
    public:
    TOpFilter(TExprNode::TPtr node);
    virtual std::shared_ptr<IOperator> Rebuild(TExprContext& ctx) override;

    TVector<TInfoUnit> GetFilterIUs() const;
    TConjunctInfo GetConjuctInfo() const;
};

class TOpJoin : public IBinaryOperator {
    public:
    TOpJoin(TExprNode::TPtr node);
    virtual std::shared_ptr<IOperator> Rebuild(TExprContext& ctx) override;

};

class TOpRoot : public IUnaryOperator {
    public:
    TOpRoot(TExprNode::TPtr node);
    virtual std::shared_ptr<IOperator> Rebuild(TExprContext& ctx) override;
};

TVector<TInfoUnit> IUSetDiff(TVector<TInfoUnit> left, TVector<TInfoUnit> right);

}
}
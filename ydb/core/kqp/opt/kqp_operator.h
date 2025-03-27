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

    struct Iterator
    {
        struct IteratorItem {
            IteratorItem(std::shared_ptr<IOperator> curr, std::shared_ptr<IOperator> parent, size_t idx) : Current(curr)
                ,Parent(parent)
                ,ChildIndex(idx)
                {}

            std::shared_ptr<IOperator> Current;
            std::shared_ptr<IOperator> Parent;
            size_t ChildIndex;
        };

        using iterator_category = std::input_iterator_tag;
        using difference_type   = std::ptrdiff_t;

        Iterator(TOpRoot* ptr) {
            if (!ptr) {
                return;
            }

            auto child = ptr->Children[0];

            while (child) {
                DfsStack.push( std::make_pair(child, 0));
                if (child->Children.size()){
                    ParentElement = child;
                    child = child->Children[0];
                    CurrElement = child;
                }
                else {
                    child = nullptr;
                }
            }
        }


        IteratorItem operator*() const { 
            return IteratorItem(CurrElement, ParentElement, ChildIndex); 
        }

        // Prefix increment
        Iterator& operator++() {
            DfsStack.pop();
            if (DfsStack.empty()) {
                CurrElement = nullptr;
            }
            else {
                auto [op, index] = DfsStack.top();
                if (index < op->Children.size()-1) {
                    auto ptr = op->Children[index+1];
                    DfsStack.pop();
                    DfsStack.push(std::make_pair(op, index+1));
                    ParentElement = op;
                    ChildIndex = index+1;

                    while(ptr) {
                        DfsStack.push( std::make_pair(ptr, size_t(0)));
                        if (ptr->Children.size()){
                            ParentElement = ptr;
                            ChildIndex = 0;
                            ptr = ptr->Children[0];
                            CurrElement = ptr;
                        }
                        else {
                            ptr = nullptr;
                        }
                    }
                }
                else {
                    if (DfsStack.size() >= 2) {
                        auto curr = DfsStack.top();
                        DfsStack.pop();
                        auto parent = DfsStack.top();
                        DfsStack.pop();
                        ParentElement = parent.first;
                        ChildIndex = parent.second;
                        DfsStack.push(parent);
                        DfsStack.push(curr);
                    }
                    else {
                        ParentElement = nullptr;
                        ChildIndex = 0;
                    }
                    CurrElement = op;
                }
            }
            return *this;
        }  

        // Postfix increment
        Iterator operator++(int) { Iterator tmp = *this; ++(*this); return tmp; }

        friend bool operator== (const Iterator& a, const Iterator& b) { return a.CurrElement == b.CurrElement; };
        friend bool operator!= (const Iterator& a, const Iterator& b) { return a.CurrElement != b.CurrElement; }; 

        private:
            std::stack<std::pair<std::shared_ptr<IOperator>,size_t>> DfsStack;
            std::shared_ptr<IOperator> CurrElement = nullptr;
            std::shared_ptr<IOperator> ParentElement = nullptr;
            size_t ChildIndex = 0;
    };

    Iterator begin() { return Iterator(this); }
    Iterator end()   { return Iterator(nullptr); } 
};

TVector<TInfoUnit> IUSetDiff(TVector<TInfoUnit> left, TVector<TInfoUnit> right);

}
}
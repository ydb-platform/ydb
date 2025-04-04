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
    Limit,
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

struct TPhysicalOpProps {
    std::optional<int> StageId;
    std::optional<TString> Algorithm;
};

struct TStageGraph {
    TVector<int> StageIds;
    THashMap<int, TVector<int>> StageInputs;
    THashMap<int, TVector<int>> StageOutputs;
    THashMap<std::pair<int,int>, TString> ConnectionType;

    int AddStage() {
        int newStageId = StageIds.size();
        StageIds.push_back(newStageId);
        StageInputs[newStageId] = TVector<int>();
        StageOutputs[newStageId] = TVector<int>();
        return newStageId;
    }

    void Connect(int from, int to, TString connType) {
        auto & outputs = StageOutputs.at(from);
        outputs.push_back(to);
        auto & inputs = StageInputs.at(to);
        inputs.push_back(from);
        ConnectionType[std::make_pair(from,to)] = connType;
    }

    TString GetConnection(int from, int to) {
        return ConnectionType.at(std::make_pair(from,to));
    }
};

struct TPlanProps {
    TStageGraph StageGraph;
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
    TPhysicalOpProps Props;
    TVector<std::shared_ptr<IOperator>> Children;
    TVector<TInfoUnit> OutputIUs;
};

template <class K>
bool MatchOperator(const std::shared_ptr<IOperator> & op) {
    auto dyn = std::dynamic_pointer_cast<K>(op);
    if (dyn) {
        return true;
    }
    else {
        return false;
    }
}

template <class K>
std::shared_ptr<K> CastOperator(const std::shared_ptr<IOperator> & op) {
    return std::static_pointer_cast<K>(op);      
}

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
    TOpEmptySource(TExprNode::TPtr node) : IOperator(EOperator::EmptySource, node) {}
    virtual std::shared_ptr<IOperator> Rebuild(TExprContext& ctx) override { return std::make_shared<TOpEmptySource>(Node); }

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

class TOpLimit : public IUnaryOperator {
    public:
    TOpLimit(TExprNode::TPtr node);
    virtual std::shared_ptr<IOperator> Rebuild(TExprContext& ctx) override;
};

class TOpRoot : public IUnaryOperator {
    public:
    TOpRoot(TExprNode::TPtr node);
    virtual std::shared_ptr<IOperator> Rebuild(TExprContext& ctx) override;

    TPlanProps PlanProps;

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
                CurrElement = -1;
                return;
            }

            auto child = ptr->Children[0];
            BuildDfsList(child, {}, size_t(0));
            CurrElement = 0;
        }


        IteratorItem operator*() const { 
            return DfsList[CurrElement];
        }

        // Prefix increment
        Iterator& operator++() {
            if (CurrElement >= 0) {
                CurrElement++;
            }
            if (CurrElement == DfsList.size()) {
                CurrElement = -1;
            }
            return *this;
        }  

        // Postfix increment
        Iterator operator++(int) { Iterator tmp = *this; ++(*this); return tmp; }

        friend bool operator== (const Iterator& a, const Iterator& b) { return a.CurrElement == b.CurrElement; };
        friend bool operator!= (const Iterator& a, const Iterator& b) { return a.CurrElement != b.CurrElement; }; 

        private:
            void BuildDfsList(std::shared_ptr<IOperator> current, std::shared_ptr<IOperator> parent, size_t childIdx) {
                for (size_t idx = 0; idx < current->Children.size(); idx++) {
                    BuildDfsList(current->Children[idx], current, idx);
                }
                DfsList.push_back(IteratorItem(current,parent,childIdx));
            }
            TVector<IteratorItem> DfsList;
            size_t CurrElement;
    };

    Iterator begin() { return Iterator(this); }
    Iterator end()   { return Iterator(nullptr); } 
};

TVector<TInfoUnit> IUSetDiff(TVector<TInfoUnit> left, TVector<TInfoUnit> right);

}
}
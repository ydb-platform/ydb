#pragma once

#include <ydb/core/kqp/opt/kqp_opt.h>
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
    Project,
    Filter,
    Join,
    Limit,
    Root
};

struct TInfoUnit {
    TInfoUnit(TString alias, TString column): Alias(alias), ColumnName(column) {}
    TInfoUnit(TString name);

    TString GetFullName() const {
       return ((Alias!="") ? ("_alias_" + Alias + ".") : "" ) + ColumnName;
    }

    TString Alias;
    TString ColumnName;

    bool operator==(const TInfoUnit& other) const {
        return Alias == other.Alias && ColumnName == other.ColumnName;
    }

    struct THashFunction
    {
        size_t operator()(const TInfoUnit& c) const
        {
            return THash<TString>{}(c.Alias) ^ THash<TString>{}(c.ColumnName);
        }
    };
};

void GetAllMembers(TExprNode::TPtr node, TVector<TInfoUnit>& IUs);

struct TFilterInfo {
    TExprNode::TPtr FilterBody;
    TVector<TInfoUnit> FilterIUs;
};

struct TJoinConditionInfo {
    TExprNode::TPtr ConjunctExpr;
    TInfoUnit LeftIU;
    TInfoUnit RightIU;
};

struct TConjunctInfo {
    bool ToPg = false;
    TVector<TFilterInfo> Filters;
    TVector<TJoinConditionInfo> JoinConditions;
};

struct TPhysicalOpProps {
    std::optional<int> StageId;
    std::optional<TString> Algorithm;
};

struct TConnection {
    TConnection(TString type, bool fromSourceStage) : Type(type), FromSourceStage(fromSourceStage) {}
    virtual TExprNode::TPtr BuildConnection(TExprNode::TPtr inputStage, TExprNode::TPtr & node, TExprNode::TPtr & newStage, TExprContext& ctx) = 0;
    virtual ~TConnection() = default;

    TString Type;
    bool FromSourceStage;
};

struct TBroadcastConnection : public TConnection {
    TBroadcastConnection(bool fromSourceStage) : TConnection("Broadcast", fromSourceStage) {}
    virtual TExprNode::TPtr BuildConnection(TExprNode::TPtr inputStage, TExprNode::TPtr & node, TExprNode::TPtr & newStage, TExprContext& ctx) override;

};

struct TMapConnection : public TConnection {
    TMapConnection(bool fromSourceStage) : TConnection("Map", fromSourceStage) {}
    virtual TExprNode::TPtr BuildConnection(TExprNode::TPtr inputStage, TExprNode::TPtr & node, TExprNode::TPtr & newStage, TExprContext& ctx) override;

};

struct TUnionAllConnection : public TConnection {
    TUnionAllConnection(bool fromSourceStage) : TConnection("UnionAll", fromSourceStage) {}
    virtual TExprNode::TPtr BuildConnection(TExprNode::TPtr inputStage, TExprNode::TPtr & node, TExprNode::TPtr & newStage, TExprContext& ctx) override;

};

struct TShuffleConnection : public TConnection {
    TShuffleConnection(TVector<TInfoUnit> keys, bool fromSourceStage) : TConnection("Shuffle", fromSourceStage)
    ,Keys(keys)
    {}

    virtual TExprNode::TPtr BuildConnection(TExprNode::TPtr inputStage, TExprNode::TPtr & node, TExprNode::TPtr & newStage, TExprContext& ctx) override;

    TVector<TInfoUnit> Keys;
};

struct TSourceConnection : public TConnection {
    TSourceConnection() : TConnection("Source", true) {}
    virtual TExprNode::TPtr BuildConnection(TExprNode::TPtr inputStage, TExprNode::TPtr & node, TExprNode::TPtr & newStage, TExprContext& ctx) override;

};

struct TStageGraph {
    TVector<int> StageIds;
    THashMap<int, TVector<TInfoUnit>> StageAttributes;
    THashMap<int, TVector<int>> StageInputs;
    THashMap<int, TVector<int>> StageOutputs;
    THashMap<std::pair<int,int>, std::shared_ptr<TConnection>> Connections;

    int AddStage() {
        int newStageId = StageIds.size();
        StageIds.push_back(newStageId);
        StageInputs[newStageId] = TVector<int>();
        StageOutputs[newStageId] = TVector<int>();
        return newStageId;
    }

    int AddSourceStage(TVector<TInfoUnit> attributes) {
        int res = AddStage();
        StageAttributes[res] = attributes;
        return res;
    }

    bool IsSourceStage(int id) {
        return StageAttributes.contains(id);
    }

    void Connect(int from, int to, std::shared_ptr<TConnection> conn) {
        auto & outputs = StageOutputs.at(from);
        outputs.push_back(to);
        auto & inputs = StageInputs.at(to);
        inputs.push_back(from);
        Connections[std::make_pair(from,to)] = conn;
    }

    std::shared_ptr<TConnection> GetConnection(int from, int to) {
        return Connections.at(std::make_pair(from,to));
    }

    std::pair<TExprNode::TPtr,TExprNode::TPtr> GenerateStageInput(int & stageInputCounter, TExprNode::TPtr & node, TExprContext& ctx, int fromStage);

    void TopologicalSort();
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

    virtual TString ToString() = 0;

    bool IsSingleConsumer() { return Parents.size() <= 1; }

    const EOperator Kind;
    TExprNode::TPtr Node;
    TPhysicalOpProps Props;
    TVector<std::shared_ptr<IOperator>> Children;
    TVector<std::weak_ptr<IOperator>> Parents;
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
    IUnaryOperator(EOperator kind) : IOperator(kind, {}) {}
    IUnaryOperator(EOperator kind, TExprNode::TPtr node) : IOperator(kind, node) {}
    IUnaryOperator(EOperator kind, std::shared_ptr<IOperator> input) : IOperator(kind, {}) { Children.push_back(input); }
    std::shared_ptr<IOperator>& GetInput() { return Children[0]; }
};

class IBinaryOperator : public IOperator {
    public:
    IBinaryOperator(EOperator kind, TExprNode::TPtr node) : IOperator(kind, node) {}
    IBinaryOperator(EOperator kind, std::shared_ptr<IOperator> leftInput, std::shared_ptr<IOperator> rightInput) : IOperator(kind, {}) {
        Children.push_back(leftInput);
        Children.push_back(rightInput);
    }

    std::shared_ptr<IOperator>& GetLeftInput() { return Children[0]; }
    std::shared_ptr<IOperator>& GetRightInput() { return Children[1]; }
};

class TOpEmptySource : public IOperator {
    public:
    TOpEmptySource(TExprNode::TPtr node) : IOperator(EOperator::EmptySource, node) {}
    virtual std::shared_ptr<IOperator> Rebuild(TExprContext& ctx) override {
        Y_UNUSED(ctx);
        return std::make_shared<TOpEmptySource>(Node); 
    }
    virtual TString ToString() override { return "EmptySource"; }

};

class TOpRead : public IOperator {
    public:
    TOpRead(TExprNode::TPtr node);
    TOpRead(TString tableName, TString alias, TString columns);
    virtual std::shared_ptr<IOperator> Rebuild(TExprContext& ctx) override;
    virtual TString ToString() override;

    TString TableName;
    TString Alias;
    TVector<TString> Columns;
};

class TOpMap : public IUnaryOperator {
    public:
    TOpMap(TExprNode::TPtr node);
    virtual std::shared_ptr<IOperator> Rebuild(TExprContext& ctx) override;
    bool HasRenames() const;
    bool HasLambdas() const;
    TVector<std::pair<TInfoUnit, TInfoUnit>> GetRenames() const;
    TVector<std::pair<TInfoUnit, TExprNode::TPtr>> GetLambdas() const;
    virtual TString ToString() override;

    TVector<std::pair<TInfoUnit, std::variant<TInfoUnit, TExprNode::TPtr>>> MapElements;
    bool Project = true;
};

class TOpProject : public IUnaryOperator {
    public:
    TOpProject(TExprNode::TPtr node);
    virtual std::shared_ptr<IOperator> Rebuild(TExprContext& ctx) override;
    virtual TString ToString() override;

    TVector<TInfoUnit> GetProjectList() const;
    TVector<TInfoUnit> ProjectList;
};

class TOpFilter : public IUnaryOperator {
    public:
    TOpFilter(TExprNode::TPtr node);
    TOpFilter(std::shared_ptr<IOperator> input, TExprNode::TPtr filterLambda, TExprContext& ctx, TPositionHandle pos);
    virtual std::shared_ptr<IOperator> Rebuild(TExprContext& ctx) override;
    virtual TString ToString() override;

    TVector<TInfoUnit> GetFilterIUs() const;
    TConjunctInfo GetConjunctInfo() const;
};

class TOpJoin : public IBinaryOperator {
    public:
    TOpJoin(TExprNode::TPtr node);
    TOpJoin(std::shared_ptr<IOperator> leftArg, std::shared_ptr<IOperator> rightArg, TString joinKind, TVector<std::pair<TInfoUnit, TInfoUnit>> joinKeys);
    virtual std::shared_ptr<IOperator> Rebuild(TExprContext& ctx) override;
    virtual TString ToString() override;

    TVector<std::pair<TInfoUnit, TInfoUnit>> JoinKeys;
    TString JoinKind;
};

class TOpLimit : public IUnaryOperator {
    public:
    TOpLimit(TExprNode::TPtr node);
    TOpLimit(std::shared_ptr<IOperator> input, TExprNode::TPtr limitCond);
    virtual std::shared_ptr<IOperator> Rebuild(TExprContext& ctx) override;
    virtual TString ToString() override;

    TExprNode::TPtr LimitCond;
};

class TOpRoot : public IUnaryOperator {
    public:
    TOpRoot(TExprNode::TPtr node);
    TOpRoot(std::shared_ptr<IOperator> input);
    virtual std::shared_ptr<IOperator> Rebuild(TExprContext& ctx) override;
    virtual TString ToString() override;
    void ComputeParents();

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
            std::unordered_set<std::shared_ptr<IOperator>> visited;
            BuildDfsList(child, {}, size_t(0), visited);
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
            void BuildDfsList(std::shared_ptr<IOperator> current, std::shared_ptr<IOperator> parent, size_t childIdx, 
                std::unordered_set<std::shared_ptr<IOperator>>& visited) {
                for (size_t idx = 0; idx < current->Children.size(); idx++) {
                    BuildDfsList(current->Children[idx], current, idx, visited);
                }
                if (!visited.contains(current)) {
                    DfsList.push_back(IteratorItem(current,parent,childIdx));
                }
                visited.insert(current);
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
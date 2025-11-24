#pragma once

#include "kqp_rbo_context.h"
#include <cstddef>
#include <iterator>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/opt/kqp_opt.h>
#include <yql/essentials/ast/yql_expr.h>

namespace NKikimr {
namespace NKqp {

using namespace NYql;

enum EOperator : ui32 { EmptySource, Source, Map, Project, Filter, Join, Aggregate, Limit, UnionAll, Root };

/* Represents aggregation phases. */
enum EAggregationPhase : ui32 {Intermediate, Final};

/**
 * Info Unit is a reference to a column in the plan
 * Currently we only record the name and alias of the column, but we will extend it in the future
 */
struct TInfoUnit {
    TInfoUnit(TString alias, TString column, bool scalarContext=false) : 
        Alias(alias), 
        ColumnName(column), 
        ScalarContext(scalarContext) {}

    TInfoUnit(TString name, bool scalarContext=false);
    TInfoUnit() {}

    TString GetFullName() const { return ((Alias != "") ? ("_alias_" + Alias + ".") : "") + ColumnName; }

    TString Alias;
    TString ColumnName;
    bool ScalarContext = false;

    bool operator==(const TInfoUnit &other) const { return Alias == other.Alias && ColumnName == other.ColumnName; }

    struct THashFunction {
        size_t operator()(const TInfoUnit &c) const { return THash<TString>{}(c.Alias) ^ THash<TString>{}(c.ColumnName); }
    };
};

/**
 * The following structures are used to extract filter information in convenient form from a filter expression
 * The filter is split into conjuncts and they are separated into generic filter conditions and potential join conditions
 */
struct TFilterInfo {
    TExprNode::TPtr FilterBody;
    TVector<TInfoUnit> FilterIUs;
    bool FromPg = false;
};

struct TJoinConditionInfo {
    TExprNode::TPtr ConjunctExpr;
    TInfoUnit LeftIU;
    TInfoUnit RightIU;
};

struct TConjunctInfo {
    TVector<TFilterInfo> Filters;
    TVector<TJoinConditionInfo> JoinConditions;
};

enum EOrderEnforcerAction : ui32 { REQUIRE, MAINTAIN };
enum EOrderEnforcerReason : ui32 { USER, INTERNAL };

struct TSortElement {
    TSortElement(TInfoUnit column, bool asc, bool nullsFirst) : SortColumn(column), Ascending(asc), NullsFirst(nullsFirst) {}
    TInfoUnit SortColumn;
    bool Ascending = true;
    bool NullsFirst = true;
};

struct TOrderEnforcer {
    EOrderEnforcerAction Action;
    EOrderEnforcerReason Reason;
    TVector<TSortElement> SortElements;
};

/**
 * Build key selector for sort and merge operations from the enforcer
 */
std::pair<TExprNode::TPtr, TVector<TExprNode::TPtr>> BuildSortKeySelector(TVector<TSortElement> sortElements, TExprContext &ctx, TPositionHandle pos);

/**
 * Per-operator physical plan properties
 * TODO: Make this more generic and extendable
 */
struct TPhysicalOpProps {
    std::optional<int> StageId;
    std::optional<TString> Algorithm;
    std::optional<TOrderEnforcer> OrderEnforcer;
    bool EnsureAtMostOne = false;
};

/**
 * Connection structs for the Stage graph
 * We make a special case for a Source connection that is required due to the limitation of the Data shard sources
 */
struct TConnection {
    TConnection(TString type, bool fromSourceStage) : Type(type), FromSourceStage(fromSourceStage) {}
    virtual TExprNode::TPtr BuildConnection(TExprNode::TPtr inputStage, TPositionHandle pos, TExprNode::TPtr &newStage,
                                            TExprContext &ctx) = 0;
    virtual ~TConnection() = default;

    TString Type;
    bool FromSourceStage;
};

struct TBroadcastConnection : public TConnection {
    TBroadcastConnection(bool fromSourceStage) : TConnection("Broadcast", fromSourceStage) {}
    virtual TExprNode::TPtr BuildConnection(TExprNode::TPtr inputStage, TPositionHandle pos, TExprNode::TPtr &newStage,
                                            TExprContext &ctx) override;
};

struct TMapConnection : public TConnection {
    TMapConnection(bool fromSourceStage) : TConnection("Map", fromSourceStage) {}
    virtual TExprNode::TPtr BuildConnection(TExprNode::TPtr inputStage, TPositionHandle pos, TExprNode::TPtr &newStage,
                                            TExprContext &ctx) override;
};

struct TUnionAllConnection : public TConnection {
    TUnionAllConnection(bool fromSourceStage) : TConnection("UnionAll", fromSourceStage) {}
    virtual TExprNode::TPtr BuildConnection(TExprNode::TPtr inputStage, TPositionHandle pos, TExprNode::TPtr &newStage,
                                            TExprContext &ctx) override;
};

struct TShuffleConnection : public TConnection {
    TShuffleConnection(TVector<TInfoUnit> keys, bool fromSourceStage) : TConnection("Shuffle", fromSourceStage), Keys(keys) {}

    virtual TExprNode::TPtr BuildConnection(TExprNode::TPtr inputStage, TPositionHandle pos, TExprNode::TPtr &newStage,
                                            TExprContext &ctx) override;

    TVector<TInfoUnit> Keys;
};

struct TMergeConnection : public TConnection {
    TMergeConnection(TVector<TSortElement> order, bool fromSourceStage) : TConnection("Merge", fromSourceStage), Order(order) {}

    virtual TExprNode::TPtr BuildConnection(TExprNode::TPtr inputStage, TPositionHandle pos, TExprNode::TPtr &newStage,
                                            TExprContext &ctx) override;

    TVector<TSortElement> Order;
};

struct TSourceConnection : public TConnection {
    TSourceConnection() : TConnection("Source", true) {}
    virtual TExprNode::TPtr BuildConnection(TExprNode::TPtr inputStage, TPositionHandle pos, TExprNode::TPtr &newStage,
                                            TExprContext &ctx) override;
};

/**
 * Stage graph
 *
 * TODO: Add validation, clean up interfaces
 */

struct TStageGraph {
    TVector<int> StageIds;
    THashMap<int, TVector<TInfoUnit>> StageAttributes;
    THashMap<int, TVector<int>> StageInputs;
    THashMap<int, TVector<int>> StageOutputs;
    THashMap<std::pair<int, int>, std::shared_ptr<TConnection>> Connections;

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

    bool IsSourceStage(int id) { return StageAttributes.contains(id); }

    void Connect(int from, int to, std::shared_ptr<TConnection> conn) {
        auto &outputs = StageOutputs.at(from);
        outputs.push_back(to);
        auto &inputs = StageInputs.at(to);
        inputs.push_back(from);
        Connections[std::make_pair(from, to)] = conn;
    }

    std::shared_ptr<TConnection> GetConnection(int from, int to) { return Connections.at(std::make_pair(from, to)); }

    /**
     * Generate an expression for stage inputs
     * The complication is the special handling of Source stage due to limitation of data shard reader
     */
    std::pair<TExprNode::TPtr, TExprNode::TPtr> GenerateStageInput(int &stageInputCounter, TExprNode::TPtr &node, TExprContext &ctx,
                                                                   int fromStage);

    void TopologicalSort();
};

class IOperator;

struct TScalarSubplans {

    void Add(TInfoUnit iu, std::shared_ptr<IOperator> op) {
        OrderedList.push_back(iu);
        PlanMap[iu] = op;
    }

    TVector<std::shared_ptr<IOperator>> Get() {
        TVector<std::shared_ptr<IOperator>> result;
        for (auto iu : OrderedList) {
            result.push_back(PlanMap.at(iu));
        }
        return result;
    }

    void Remove(TInfoUnit iu) {
        std::erase(OrderedList, iu);
        PlanMap.erase(iu);
    }

    THashMap<TInfoUnit, std::shared_ptr<IOperator>, TInfoUnit::THashFunction> PlanMap;
    TVector<TInfoUnit> OrderedList;
};

/**
 * Global plan properties
 */
struct TPlanProps {
    TStageGraph StageGraph;
    int InternalVarIdx = 1;
    TScalarSubplans ScalarSubplans;
    bool PgSyntax = false;
};


/**
 * Extract all into units from an expression in YQL
 */
void GetAllMembers(TExprNode::TPtr node, TVector<TInfoUnit> &IUs);
void GetAllMembers(TExprNode::TPtr node, TVector<TInfoUnit> &IUs, TPlanProps& props, bool withScalarContext=false);

/**
 * Interface for the operator
 */
class IOperator {
  public:
    IOperator(EOperator kind, TPositionHandle pos) : Kind(kind), Pos(pos) {}

    virtual ~IOperator() = default;

    const TVector<std::shared_ptr<IOperator>> &GetChildren() { return Children; }

    bool HasChildren() const { return Children.size() != 0; }

    /**
     * Get the information units that are in the output of this operator
     * Currently recursively computes the correct values
     * TODO: Add caching with the ability to invalidate
     */
    virtual TVector<TInfoUnit> GetOutputIUs() = 0;

    virtual TVector<TInfoUnit> GetScalarSubplanIUs(TPlanProps& props) { Y_UNUSED(props); return {}; }

    const TTypeAnnotationNode* GetIUType(TInfoUnit iu);

    virtual TVector<TExprNode::TPtr> GetLambdas() { return {}; }

    virtual void ApplyReplaceMap(TNodeOnNodeOwnedMap map, TRBOContext & ctx) { Y_UNUSED(map); Y_UNUSED(ctx); }

    /***
     * Rename information units of this operator using a specified mapping
     */
    virtual void RenameIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction> &renameMap, TExprContext &ctx);

    virtual TString ToString(TExprContext& ctx) = 0;

    bool IsSingleConsumer() { return Parents.size() <= 1; }

    const EOperator Kind;
    TPhysicalOpProps Props;
    TPositionHandle Pos;
    const TTypeAnnotationNode* Type = nullptr;
    TVector<std::shared_ptr<IOperator>> Children;
    TVector<std::weak_ptr<IOperator>> Parents;
};

/***
 * FIXME: This doesn't work correctly
 */
template <class K> bool MatchOperator(const std::shared_ptr<IOperator> &op) {
    auto dyn = std::dynamic_pointer_cast<K>(op);
    if (dyn) {
        return true;
    } else {
        return false;
    }
}

template <class K> std::shared_ptr<K> CastOperator(const std::shared_ptr<IOperator> &op) { return std::static_pointer_cast<K>(op); }

class IUnaryOperator : public IOperator {
  public:
    IUnaryOperator(EOperator kind, TPositionHandle pos) : IOperator(kind, pos) {}
    IUnaryOperator(EOperator kind, TPositionHandle pos, std::shared_ptr<IOperator> input) : IOperator(kind, pos) { Children.push_back(input); }
    std::shared_ptr<IOperator> &GetInput() { return Children[0]; }
};

class IBinaryOperator : public IOperator {
  public:
    IBinaryOperator(EOperator kind, TPositionHandle pos) : IOperator(kind, pos) {}
    IBinaryOperator(EOperator kind, TPositionHandle pos, std::shared_ptr<IOperator> leftInput, std::shared_ptr<IOperator> rightInput) : IOperator(kind, pos) {
        Children.push_back(leftInput);
        Children.push_back(rightInput);
    }

    std::shared_ptr<IOperator> &GetLeftInput() { return Children[0]; }
    std::shared_ptr<IOperator> &GetRightInput() { return Children[1]; }
};

class TOpEmptySource : public IOperator {
  public:
    TOpEmptySource(TPositionHandle pos) : IOperator(EOperator::EmptySource, pos) {}
    virtual TVector<TInfoUnit> GetOutputIUs() override { return {}; }
    virtual TString ToString(TExprContext& ctx) override;
};

class TOpRead : public IOperator {
  public:
    TOpRead(TExprNode::TPtr node);
    virtual TVector<TInfoUnit> GetOutputIUs() override;
    virtual TString ToString(TExprContext& ctx) override;

    TString Alias;
    TVector<TString> Columns;
    TExprNode::TPtr TableCallable;
};

class TOpMap : public IUnaryOperator {
  public:
    TOpMap(std::shared_ptr<IOperator> input, TPositionHandle pos, TVector<std::pair<TInfoUnit, std::variant<TInfoUnit, TExprNode::TPtr>>> mapElements,
           bool project);
    virtual TVector<TInfoUnit> GetOutputIUs() override;
    virtual TVector<TInfoUnit> GetScalarSubplanIUs(TPlanProps& props) override;
    virtual TVector<TExprNode::TPtr> GetLambdas() override;
    virtual void ApplyReplaceMap(TNodeOnNodeOwnedMap map, TRBOContext & ctx) override;

    bool HasRenames() const;
    bool HasLambdas() const;
    TVector<std::pair<TInfoUnit, TInfoUnit>> GetRenames() const;
    TVector<std::pair<TInfoUnit, TExprNode::TPtr>> GetLambdas() const;
    void RenameIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction> &renameMap, TExprContext &ctx) override;

    virtual TString ToString(TExprContext& ctx) override;

    TVector<std::pair<TInfoUnit, std::variant<TInfoUnit, TExprNode::TPtr>>> MapElements;
    bool Project = true;
};

class TOpProject : public IUnaryOperator {
  public:
    TOpProject(std::shared_ptr<IOperator> input, TPositionHandle pos, TVector<TInfoUnit> projectList);
    virtual TVector<TInfoUnit> GetOutputIUs() override;

    void RenameIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction> &renameMap, TExprContext &ctx) override;
    virtual TString ToString(TExprContext& ctx) override;

    TVector<TInfoUnit> ProjectList;
};

struct TOpAggregationTraits {
    TOpAggregationTraits() = default;
    TOpAggregationTraits(const TInfoUnit& originalColName, const TString& aggFunction)
        : OriginalColName(originalColName), AggFunction(aggFunction) {}

    TInfoUnit OriginalColName;
    TString AggFunction;
};

class TOpAggregate : public IUnaryOperator {
  public:
    TOpAggregate(std::shared_ptr<IOperator> input, TVector<TOpAggregationTraits>& aggFunctions, TVector<TInfoUnit>& keyColumns,
                 EAggregationPhase aggPhase, bool distinctAll, TPositionHandle pos);
    virtual TVector<TInfoUnit> GetOutputIUs() override;

    virtual TString ToString(TExprContext& ctx) override;

    TVector<TOpAggregationTraits> AggregationTraitsList;
    TVector<TInfoUnit> KeyColumns;
    EAggregationPhase AggregationPhase;
    bool DistinctAll;
};

class TOpFilter : public IUnaryOperator {
  public:
    TOpFilter(std::shared_ptr<IOperator> input, TPositionHandle pos, TExprNode::TPtr filterLambda);
    virtual TVector<TInfoUnit> GetOutputIUs() override;
    virtual TVector<TInfoUnit> GetScalarSubplanIUs(TPlanProps& props) override;
    virtual TString ToString(TExprContext& ctx) override;
    virtual TVector<TExprNode::TPtr> GetLambdas() override;
    virtual void ApplyReplaceMap(TNodeOnNodeOwnedMap map, TRBOContext & ctx) override;

    TVector<TInfoUnit> GetFilterIUs(TPlanProps& props) const;
    TConjunctInfo GetConjunctInfo(TPlanProps& props) const;
    void RenameIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction> &renameMap, TExprContext &ctx) override;

    TExprNode::TPtr FilterLambda;
};

bool TestAndExtractEqualityPredicate(TExprNode::TPtr pred, TExprNode::TPtr& leftArg, TExprNode::TPtr& rightArg);

class TOpJoin : public IBinaryOperator {
  public:
    TOpJoin(std::shared_ptr<IOperator> leftArg, std::shared_ptr<IOperator> rightArg, TPositionHandle pos, TString joinKind,
            TVector<std::pair<TInfoUnit, TInfoUnit>> joinKeys);
    virtual TVector<TInfoUnit> GetOutputIUs() override;
    void RenameIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction> &renameMap, TExprContext &ctx) override;
    virtual TString ToString(TExprContext& ctx) override;

    TString JoinKind;
    TVector<std::pair<TInfoUnit, TInfoUnit>> JoinKeys;
};

class TOpUnionAll : public IBinaryOperator {
  public:
    TOpUnionAll(std::shared_ptr<IOperator> leftArg, std::shared_ptr<IOperator> rightArg, TPositionHandle pos, bool ordered = false);
    virtual TVector<TInfoUnit> GetOutputIUs() override;
    virtual TString ToString(TExprContext& ctx) override;

    bool Ordered;
};

class TOpLimit : public IUnaryOperator {
  public:
    TOpLimit(std::shared_ptr<IOperator> input, TPositionHandle pos, TExprNode::TPtr limitCond);
    virtual TVector<TInfoUnit> GetOutputIUs() override;
    void RenameIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction> &renameMap, TExprContext &ctx) override;
    virtual TString ToString(TExprContext& ctx) override;

    TExprNode::TPtr LimitCond;
};

class TOpRoot : public IUnaryOperator {
  public:
    TOpRoot(std::shared_ptr<IOperator> input, TPositionHandle pos, TVector<TString> columnOrder);
    virtual TVector<TInfoUnit> GetOutputIUs() override;
    virtual TString ToString(TExprContext& ctx) override;
    void ComputeParents();
    IGraphTransformer::TStatus ComputeTypes(TRBOContext & ctx);


    TString PlanToString(TExprContext& ctx);
    void PlanToStringRec(std::shared_ptr<IOperator> op, TExprContext& ctx, TStringBuilder &builder, int ntabs);

    TPlanProps PlanProps;
    TExprNode::TPtr Node;
    TVector<TString> ColumnOrder;

    struct Iterator {
        struct IteratorItem {
            IteratorItem(std::shared_ptr<IOperator> curr, std::shared_ptr<IOperator> parent, size_t idx)
                : Current(curr), Parent(parent), ChildIndex(idx) {}

            std::shared_ptr<IOperator> Current;
            std::shared_ptr<IOperator> Parent;
            size_t ChildIndex;
        };

        using iterator_category = std::input_iterator_tag;
        using difference_type = std::ptrdiff_t;

        Iterator(TOpRoot *ptr) {
            if (!ptr) {
                CurrElement = -1;
                return;
            }
            Root = ptr;

            std::unordered_set<std::shared_ptr<IOperator>> visited;
            for (auto scalarSubplan : Root->PlanProps.ScalarSubplans.Get()) {
                BuildDfsList(scalarSubplan, {}, size_t(0), visited);
            }
            auto child = ptr->Children[0];
            BuildDfsList(child, {}, size_t(0), visited);
            CurrElement = 0;
        }

        IteratorItem operator*() const { return DfsList[CurrElement]; }

        // Prefix increment
        Iterator &operator++() {
            if (CurrElement >= 0) {
                CurrElement++;
            }
            if (CurrElement == DfsList.size()) {
                CurrElement = -1;
            }
            return *this;
        }

        // Postfix increment
        Iterator operator++(int) {
            Iterator tmp = *this;
            ++(*this);
            return tmp;
        }

        friend bool operator==(const Iterator &a, const Iterator &b) { return a.CurrElement == b.CurrElement; };
        friend bool operator!=(const Iterator &a, const Iterator &b) { return a.CurrElement != b.CurrElement; };

      private:
        void BuildDfsList(std::shared_ptr<IOperator> current, std::shared_ptr<IOperator> parent, size_t childIdx,
                          std::unordered_set<std::shared_ptr<IOperator>> &visited) {
            for (size_t idx = 0; idx < current->Children.size(); idx++) {
                BuildDfsList(current->Children[idx], current, idx, visited);
            }
            if (!visited.contains(current)) {
                DfsList.push_back(IteratorItem(current, parent, childIdx));
            }
            visited.insert(current);
        }
        TVector<IteratorItem> DfsList;
        size_t CurrElement;
        TOpRoot *Root;
    };

    Iterator begin() { return Iterator(this); }
    Iterator end() { return Iterator(nullptr); }
};

TVector<TInfoUnit> IUSetDiff(TVector<TInfoUnit> left, TVector<TInfoUnit> right);

TString PrintRBOExpression(TExprNode::TPtr expr, TExprContext & ctx);

} // namespace NKqp
} // namespace NKikimr
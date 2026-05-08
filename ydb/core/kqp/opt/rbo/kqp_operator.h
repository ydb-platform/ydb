#pragma once

#include "kqp_simple_operator.h"
#include "kqp_info_unit.h"
#include "kqp_expression.h"
#include "kqp_rbo_context.h"
#include "kqp_rbo_statistics.h"

#include <cstddef>
#include <iterator>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/opt/kqp_opt.h>
#include <yql/essentials/ast/yql_expr.h>
#include <ydb/core/kqp/opt/cbo/cbo_optimizer_new.h>
#include <library/cpp/json/writer/json.h>

namespace NKikimr {
namespace NKqp {

using namespace NYql;

enum EOperator : ui32 { EmptySource, Source, Map, AddDependencies, Project, Filter, Join, Aggregate, Limit, Sort, UnionAll, CBOTree, Root };

// clang-format off
#define PHASE_ENUM(X) \
    X(Undefined)     \
    X(Intermediate)   \
    X(Final)

enum class EOpPhase {
#define X(name) name,
    PHASE_ENUM(X)
#undef X
};
// clang-format on

// There are already defined ToString for integers.
TString ToStringPhase(EOpPhase phase);

// clang-format off
enum EPrintPlanOptions: ui32 {
    PrintBasicMetadata = 0x01,
    PrintFullMetadata = 0x02,
    PrintBasicStatistics = 0x04,
    PrintFullStatistics = 0x08
};
// clang-format on

enum EPlanToJsonOptions: ui32 {
    BasicInfo = 0x01
};

enum EOrderEnforcerAction : ui32 { REQUIRE, MAINTAIN };
enum EOrderEnforcerReason : ui32 { USER, INTERNAL };

struct TOrderEnforcer {
    EOrderEnforcerAction Action;
    EOrderEnforcerReason Reason;
    TVector<TSortElement> SortElements;
};

enum ESortDir : ui32 { None = 0x00, Asc = 0x01, Desc = 0x02 };

/**
 * Per-operator physical plan properties
 * TODO: Make this more generic and extendable
 */
struct TPhysicalOpProps {
    std::optional<int> StageId;
    std::optional<TString> Algorithm;
    std::optional<TOrderEnforcer> OrderEnforcer;
    bool EnsureAtMostOne = false;

    std::optional<TRBOMetadata> Metadata;
    std::optional<TRBOStatistics> Statistics;
    std::optional<NKikimr::NKqp::EJoinAlgoType> JoinAlgo;
    std::optional<double> Cost;
};

/**
 * Interface for the operator
 */

class IOperator : public ISimpleOperator {
public:
    IOperator(EOperator kind, TPositionHandle pos)
        : Kind(kind)
        , Pos(pos) {
    }

    IOperator(EOperator kind, TPositionHandle pos, const TPhysicalOpProps& props)
        : Kind(kind)
        , Pos(pos)
        , Props(props) {
    }

    virtual ~IOperator() = default;

    const TVector<TIntrusivePtr<IOperator>>& GetChildren() {
        return Children;
    }

    bool HasChildren() const {
        return Children.size() != 0;
    }

    /**
     * Get the information units that are in the output of this operator
     * Currently recursively computes the correct values
     * TODO: Add caching with the ability to invalidate
     */
    virtual TVector<TInfoUnit> GetOutputIUs() = 0;

    virtual TVector<TInfoUnit> GetUsedIUs(TPlanProps& props) {
        Y_UNUSED(props);
        return {};
    }

    virtual TVector<TInfoUnit> GetSubplanIUs(TPlanProps& props) {
        Y_UNUSED(props);
        return {};
    }

    const TTypeAnnotationNode* GetIUType(const TInfoUnit& iu);

    virtual TVector<std::reference_wrapper<TExpression>> GetExpressions() {
        return {};
    }

    virtual void ApplyReplaceMap(const TNodeOnNodeOwnedMap& map, TRBOContext& ctx) {
        Y_UNUSED(map);
        Y_UNUSED(ctx);
    }

    virtual void ReplaceChild(const TIntrusivePtr<IOperator> oldChild, const TIntrusivePtr<IOperator> newChild);

    /***
     * Rename information units of this operator using a specified mapping
     */
    virtual void RenameIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap, TExprContext& ctx,
                           const THashSet<TInfoUnit, TInfoUnit::THashFunction>& stopList = {});

    virtual void ComputeMetadata(TRBOContext& ctx, TPlanProps& planProps) = 0;
    virtual void ComputeStatistics(TRBOContext& ctx, TPlanProps& planProps) = 0;

    virtual TString GetExplainName() const = 0;
    virtual TString ToString(TExprContext& ctx) = 0;

    virtual NJson::TJsonValue ToJson(ui32 explainFlags);

    bool IsSingleConsumer() const {
        return Parents.size() <= 1;
    }

    ui32 GetNumOfConsumers() const {
        return Parents.size();
    }

    const TTypeAnnotationNode* GetTypeAnn() {
        return Type;
    }

    EOperator GetKind() const {
        return Kind;
    }

    const EOperator Kind;
    TPositionHandle Pos;
    TPhysicalOpProps Props;
    const TTypeAnnotationNode* Type = nullptr;
    TVector<TIntrusivePtr<IOperator>> Children;
    TVector<std::pair<IOperator*, ui32>> Parents;
};

template <class K>
inline bool MatchOperator(const TIntrusivePtr<IOperator> op) {
    return dynamic_cast<K*>(op.get());
}

template <class K, class T>
inline TIntrusivePtr<K> CastOperator(const TIntrusivePtr<T> op) {
    return TIntrusivePtr<K>(static_cast<K*>(op.get()));
}

class IUnaryOperator: public IOperator {
public:
    IUnaryOperator(EOperator kind, TPositionHandle pos)
        : IOperator(kind, pos) {
    }
    IUnaryOperator(EOperator kind, TPositionHandle pos, TIntrusivePtr<IOperator> input)
        : IOperator(kind, pos) {
        Children.push_back(input);
    }
    IUnaryOperator(EOperator kind, TPositionHandle pos, const TPhysicalOpProps& props, TIntrusivePtr<IOperator> input)
        : IOperator(kind, pos, props) {
        Children.push_back(input);
    }
    TIntrusivePtr<IOperator>& GetInput() {
        return Children[0];
    }
    void SetInput(TIntrusivePtr<IOperator> newInput) {
        Children[0] = newInput;
    }

    virtual void ComputeMetadata(TRBOContext& ctx, TPlanProps& planProps) override;
    virtual void ComputeStatistics(TRBOContext& ctx, TPlanProps& planProps) override;
};

class IBinaryOperator: public IOperator {
public:
    IBinaryOperator(EOperator kind, TPositionHandle pos)
        : IOperator(kind, pos) {
    }

    IBinaryOperator(EOperator kind, TPositionHandle pos, TIntrusivePtr<IOperator> leftInput, TIntrusivePtr<IOperator> rightInput)
        : IOperator(kind, pos) {
        Children.push_back(leftInput);
        Children.push_back(rightInput);
    }

    TIntrusivePtr<IOperator>& GetLeftInput() {
        return Children[0];
    }

    TIntrusivePtr<IOperator>& GetRightInput() {
        return Children[1];
    }

    void SetLeftInput(TIntrusivePtr<IOperator> newInput) {
        Children[0] = newInput;
    }

    void SetRightInput(TIntrusivePtr<IOperator> newInput) {
        Children[1] = newInput;
    }
};

class TOpEmptySource: public IOperator {
public:
    TOpEmptySource(TPositionHandle pos)
        : IOperator(EOperator::EmptySource, pos) {
    }
    virtual TVector<TInfoUnit> GetOutputIUs() override {
        return {};
    }
    virtual TString ToString(TExprContext& ctx) override;
    virtual TString GetExplainName() const override { return "EmptySource"; }

    virtual void ComputeMetadata(TRBOContext& ctx, TPlanProps& planProps) override;
    virtual void ComputeStatistics(TRBOContext& ctx, TPlanProps& planProps) override;
};

class TOpRead: public IOperator {
public:
    TOpRead(TExprNode::TPtr node);
    TOpRead(const TString& alias, const TVector<TString>& columns, const TVector<TInfoUnit>& outputIUs, const NYql::EStorageType storageType,
            const TExprNode::TPtr& tableCallable, const TExprNode::TPtr& olapFilterLambda, const TExprNode::TPtr& limit, const TExprNode::TPtr& ranges,
            const std::optional<TExpression>& originalPredicate, const ESortDir sortDireciont, const TPhysicalOpProps& props, TPositionHandle pos);

    virtual TVector<TInfoUnit> GetOutputIUs() override;
    virtual TString ToString(TExprContext& ctx) override;
    virtual TString GetExplainName() const override { return "TableFullScan"; }
    virtual NJson::TJsonValue ToJson(ui32 explainFlags) override;

    void RenameIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap, TExprContext& ctx,
                   const THashSet<TInfoUnit, TInfoUnit::THashFunction>& stopList = {}) override;
    bool NeedsMap() const;

    virtual void ComputeMetadata(TRBOContext& ctx, TPlanProps& planProps) override;
    virtual void ComputeStatistics(TRBOContext& ctx, TPlanProps& planProps) override;
    NYql::EStorageType GetTableStorageType() const;

    TExprNode::TPtr GetRanges() const { return Ranges; }
    TExprNode::TPtr GetTable() const { return TableCallable; }

    // TODO: make it private members, we should not access it directly
    TString Alias;
    TVector<TString> Columns;
    TVector<TInfoUnit> OutputIUs;
    NYql::EStorageType StorageType;

    // TODO: put it in read settings.
    TExprNode::TPtr TableCallable;
    TExprNode::TPtr OlapFilterLambda;
    TExprNode::TPtr Limit;
    TExprNode::TPtr Ranges;
    std::optional<TExpression> OriginalPredicate;
    ESortDir SortDir{ESortDir::None};
};

class TMapElement {
public:
    TMapElement() = default;
    TMapElement(const TInfoUnit& elementName, const TExpression& expr);
    TMapElement(const TInfoUnit& elementName, const TInfoUnit& rename, TPositionHandle pos, TExprContext* ctx, TPlanProps* props = nullptr);

    bool IsRename() const;
    TInfoUnit GetRename() const;

    TInfoUnit GetElementName() const;
    TExpression GetExpression() const;
    TExpression& GetExpressionRef();
    void SetExpression(TExpression expr);

private:
    TInfoUnit ElementName;
    TExpression Expr;
};

class TOpMap: public IUnaryOperator {
public:
    TOpMap(TIntrusivePtr<IOperator> input, TPositionHandle pos, const TVector<TMapElement>& mapElements, bool project, bool ordered = false);
    TOpMap(TIntrusivePtr<IOperator> input, TPositionHandle pos, const TPhysicalOpProps& props, const TVector<TMapElement>& mapElements, bool project,
           bool ordered = false);

    virtual TVector<TInfoUnit> GetOutputIUs() override;
    virtual TVector<TInfoUnit> GetUsedIUs(TPlanProps& props) override;
    virtual TVector<TInfoUnit> GetSubplanIUs(TPlanProps& props) override;
    virtual TVector<std::reference_wrapper<TExpression>> GetExpressions() override;
    virtual TVector<std::reference_wrapper<TExpression>> GetComplexExpressions();
    TVector<std::pair<TInfoUnit, TInfoUnit>> GetRenames() const;
    TVector<std::pair<TInfoUnit, TInfoUnit>> GetRenamesWithTransforms(TPlanProps& props) const;
    virtual void ApplyReplaceMap(const TNodeOnNodeOwnedMap& map, TRBOContext& ctx) override;

    void RenameIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap, TExprContext& ctx,
                   const THashSet<TInfoUnit, TInfoUnit::THashFunction>& stopList = {}) override;

    virtual void ComputeMetadata(TRBOContext& ctx, TPlanProps& planProps) override;
    virtual void ComputeStatistics(TRBOContext& ctx, TPlanProps& planProps) override;

    virtual TString ToString(TExprContext& ctx) override;
    virtual TString GetExplainName() const override { return "Map"; }

    bool IsOrdered() const {
        return Ordered;
    }

    TVector<TMapElement>& GetMapElements() { return MapElements; }

    TVector<TMapElement> MapElements;
    bool Project = true;
    bool Ordered = false;
};

/**
 * OpAddDependencies is a temporary operator to infuse dependencies into a correlated subplan
 * This operator needs to be removed during query decorrelation
 */
class TOpAddDependencies: public IUnaryOperator {
public:
    TOpAddDependencies(TIntrusivePtr<IOperator> input, TPositionHandle pos, const TVector<TInfoUnit>& columns,
                       const TVector<const TTypeAnnotationNode*>& types);
    TOpAddDependencies(TIntrusivePtr<IOperator> input, TPositionHandle pos, const TVector<std::pair<TInfoUnit, const TTypeAnnotationNode*>>& pairs);

    TVector<std::pair<TInfoUnit, const TTypeAnnotationNode*>> GetDependencyPairs();
    void SetDependencyPairs(const TVector<std::pair<TInfoUnit, const TTypeAnnotationNode*>>& pairs);
    virtual TVector<TInfoUnit> GetOutputIUs() override;
    virtual TString ToString(TExprContext& ctx) override;
    virtual TString GetExplainName() const override { return "AddDependencies"; }
    

    TVector<TInfoUnit> Dependencies;
    TVector<const TTypeAnnotationNode*> Types;
};

class TOpProject: public IUnaryOperator {
public:
    TOpProject(TIntrusivePtr<IOperator> input, TPositionHandle pos, const TVector<TInfoUnit>& projectList);
    virtual TVector<TInfoUnit> GetOutputIUs() override;

    void RenameIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap, TExprContext& ctx,
                   const THashSet<TInfoUnit, TInfoUnit::THashFunction>& stopList = {}) override;
    virtual TString ToString(TExprContext& ctx) override;
    virtual TString GetExplainName() const override { return "Project"; }


    TVector<TInfoUnit> ProjectList;
};

struct TOpAggregationTraits {
    TOpAggregationTraits() = default;
    TOpAggregationTraits(const TInfoUnit& originalColName, const TString& aggFunction, const TInfoUnit& resultColName)
        : OriginalColName(originalColName)
        , AggFunction(aggFunction)
        , ResultColName(resultColName) {
    }

    TInfoUnit OriginalColName;
    TString AggFunction;
    TInfoUnit ResultColName;
};

class TOpAggregate: public IUnaryOperator {
public:
    TOpAggregate(TIntrusivePtr<IOperator> input, const TVector<TOpAggregationTraits>& aggFunctions, const TVector<TInfoUnit>& keyColumns,
                 const EOpPhase aggPhase, bool distinctAll, TPositionHandle pos);
    TOpAggregate(TIntrusivePtr<IOperator> input, const TVector<TOpAggregationTraits>& aggFunctions, const TVector<TInfoUnit>& keyColumns,
                 const EOpPhase aggPhase, bool distinctAll, const TPhysicalOpProps& props, TPositionHandle pos);

    virtual TVector<TInfoUnit> GetOutputIUs() override;
    virtual TVector<TInfoUnit> GetUsedIUs(TPlanProps& props) override;

    void RenameIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap, TExprContext& ctx,
                   const THashSet<TInfoUnit, TInfoUnit::THashFunction>& stopList = {}) override;
    virtual TString ToString(TExprContext& ctx) override;
    virtual TString GetExplainName() const override { return "Aggregate"; }

    virtual void ComputeMetadata(TRBOContext& ctx, TPlanProps& planProps) override;
    virtual void ComputeStatistics(TRBOContext& ctx, TPlanProps& planProps) override;

    EOpPhase GetAggregationPhase() const { return AggregationPhase; }
    TVector<TOpAggregationTraits> GetAggregationTraits() const {
        return AggregationTraitsList;
    }
    TVector<TOpAggregationTraits>& GetAggregationTraits() {
        return AggregationTraitsList;
    }
    TVector<TInfoUnit> GetKeyColumns() const { return KeyColumns; }
    TVector<TInfoUnit>& GetKeyColumns() { return KeyColumns; }
    bool IsDistinctAll() const { return DistinctAll; }

    TVector<TOpAggregationTraits> AggregationTraitsList;
    TVector<TInfoUnit> KeyColumns;
    EOpPhase AggregationPhase;
    bool DistinctAll;
};

class TOpFilter: public IUnaryOperator {
public:
    TOpFilter(TIntrusivePtr<IOperator> input, TPositionHandle pos, const TExpression& filterExpr);
    TOpFilter(TIntrusivePtr<IOperator> input, TPositionHandle pos, const TPhysicalOpProps& props, const TExpression& filterExpr);

    virtual TVector<TInfoUnit> GetOutputIUs() override;
    virtual TVector<TInfoUnit> GetUsedIUs(TPlanProps& props) override;
    virtual TVector<TInfoUnit> GetSubplanIUs(TPlanProps& props) override;
    virtual TString ToString(TExprContext& ctx) override;
    virtual TString GetExplainName() const override { return "Filter"; }

    virtual TVector<std::reference_wrapper<TExpression>> GetExpressions() override;
    virtual void ApplyReplaceMap(const TNodeOnNodeOwnedMap& map, TRBOContext& ctx) override;

    TVector<TInfoUnit> GetFilterIUs(TPlanProps& props) const;
    void RenameIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap, TExprContext& ctx,
                   const THashSet<TInfoUnit, TInfoUnit::THashFunction>& stopList = {}) override;

    virtual void ComputeMetadata(TRBOContext& ctx, TPlanProps& planProps) override;
    virtual void ComputeStatistics(TRBOContext& ctx, TPlanProps& planProps) override;
    TExpression GetFilterExpression() const { return FilterExpr; }

    TExpression FilterExpr;
};

bool TestAndExtractEqualityPredicate(TExprNode::TPtr pred, TExprNode::TPtr& leftArg, TExprNode::TPtr& rightArg);

class TOpJoin: public IBinaryOperator {
public:
    TOpJoin(TIntrusivePtr<IOperator> leftArg, TIntrusivePtr<IOperator> rightArg, TPositionHandle pos, TString joinKind,
            const TVector<std::pair<TInfoUnit, TInfoUnit>>& joinKeys);

    TOpJoin(TIntrusivePtr<IOperator> leftArg, TIntrusivePtr<IOperator> rightArg, TPositionHandle pos, TString joinKind,
            const TVector<std::pair<TInfoUnit, TInfoUnit>>& joinKeys, const TVector<TExpression>& joinFilters);

    virtual TVector<TInfoUnit> GetOutputIUs() override;
    virtual TVector<TInfoUnit> GetUsedIUs(TPlanProps& props) override;
    virtual TVector<std::reference_wrapper<TExpression>> GetExpressions() override;

    void RenameIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap, TExprContext& ctx,
                   const THashSet<TInfoUnit, TInfoUnit::THashFunction>& stopList = {}) override;
    virtual TString ToString(TExprContext& ctx) override;
    virtual TString GetExplainName() const override { return "Join"; }

    virtual void ComputeMetadata(TRBOContext& ctx, TPlanProps& planProps) override;
    virtual void ComputeStatistics(TRBOContext& ctx, TPlanProps& planProps) override;

    TString JoinKind;
    TVector<std::pair<TInfoUnit, TInfoUnit>> JoinKeys;
    TVector<TExpression> JoinFilters;
};

class TOpUnionAll: public IBinaryOperator {
public:
    TOpUnionAll(TIntrusivePtr<IOperator> leftArg, TIntrusivePtr<IOperator> rightArg, TPositionHandle pos, bool ordered = false);
    virtual TVector<TInfoUnit> GetOutputIUs() override;
    virtual TString ToString(TExprContext& ctx) override;
    virtual TString GetExplainName() const override { return "UnionAll"; }

    virtual void ComputeMetadata(TRBOContext& ctx, TPlanProps& planProps) override;
    virtual void ComputeStatistics(TRBOContext& ctx, TPlanProps& planProps) override;

    bool Ordered;
};

class TOpLimit: public IUnaryOperator {
public:
    TOpLimit(TIntrusivePtr<IOperator> input, TPositionHandle pos, const TExpression& limitCond, const EOpPhase limitPhase);
    TOpLimit(TIntrusivePtr<IOperator> input, TPositionHandle pos, const TExpression& limitCond, const TExpression& offsetCond, const EOpPhase limitPhase);
    TOpLimit(TIntrusivePtr<IOperator> input, TPositionHandle pos, const TPhysicalOpProps& props, const TExpression& limitCond, const EOpPhase limitPhase);
    TOpLimit(TIntrusivePtr<IOperator> input, TPositionHandle pos, const TPhysicalOpProps& props, const TExpression& limitCond,
             std::optional<TExpression> offsetCond, const EOpPhase limitPhase);

    virtual TVector<TInfoUnit> GetOutputIUs() override;
    void RenameIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap, TExprContext& ctx,
                   const THashSet<TInfoUnit, TInfoUnit::THashFunction>& stopList = {}) override;
    virtual TString ToString(TExprContext& ctx) override;
    virtual TString GetExplainName() const override { return "Limit"; }

    virtual TVector<std::reference_wrapper<TExpression>> GetExpressions() override;

    EOpPhase GetLimitPhase() const {
        return LimitPhase;
    }

    TExpression GetLimitCond() const { return LimitCond; }
    bool HasOffset() const { return OffsetCond.has_value(); }
    std::optional<TExpression> GetOffsetCond() const { return OffsetCond; }

    // Make private.
    TExpression LimitCond;
private:
    std::optional<TExpression> OffsetCond;
    EOpPhase LimitPhase{EOpPhase::Undefined};
};

class TOpSort: public IUnaryOperator {
public:
    TOpSort(TIntrusivePtr<IOperator> input, TPositionHandle pos, const TVector<TSortElement>& sortElements,
            std::optional<TExpression> limitCond = std::nullopt);
    TOpSort(TIntrusivePtr<IOperator> input, TPositionHandle pos, const TPhysicalOpProps& props, const TVector<TSortElement>& sortElements,
            std::optional<TExpression> limitCond, const EOpPhase sortPhase);

    virtual TVector<TInfoUnit> GetOutputIUs() override;
    virtual TVector<TInfoUnit> GetUsedIUs(TPlanProps& props) override;
    void RenameIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap, TExprContext& ctx,
                   const THashSet<TInfoUnit, TInfoUnit::THashFunction>& stopList = {}) override;
    virtual TString ToString(TExprContext& ctx) override;
    EOpPhase GetSortPhase() const {
        return SortPhase;
    }
    void SetSortPhase(const EOpPhase phase) {
        SortPhase = phase;
    }
    TVector<TSortElement> GetSortElements() const {
        return SortElements;
    }
    TVector<TSortElement>& GetSortElements() {
        return SortElements;
    }
    bool IsTopSort() const { return LimitCond.has_value(); }
    
    virtual TString GetExplainName() const override { return "Sort"; }

    TVector<TSortElement> SortElements;
    std::optional<TExpression> LimitCond;

private:
    EOpPhase SortPhase{EOpPhase::Undefined};
};

/***
 * This operator packages a subtree of operators in order to pass them to dynamic programming optimizer
 * Currently it requires that the list of operators TreeNodes is in a post-order traversal of the tree
 * No validation is currently used
 */
class TOpCBOTree: public IOperator {
public:
    TOpCBOTree(TIntrusivePtr<IOperator> treeRoot, TPositionHandle pos);
    TOpCBOTree(TIntrusivePtr<IOperator> treeRoot, TVector<TIntrusivePtr<IOperator>> treeNodes, TPositionHandle pos);

    virtual TVector<TInfoUnit> GetOutputIUs() override {
        return TreeRoot->GetOutputIUs();
    }
    void RenameIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap, TExprContext& ctx,
                   const THashSet<TInfoUnit, TInfoUnit::THashFunction>& stopList = {}) override;
    virtual TString ToString(TExprContext& ctx) override;
    virtual TString GetExplainName() const override { return "CBOTree"; }


    virtual void ComputeMetadata(TRBOContext& ctx, TPlanProps& planProps) override;
    virtual void ComputeStatistics(TRBOContext& ctx, TPlanProps& planProps) override;

    TIntrusivePtr<IOperator> TreeRoot;
    TVector<TIntrusivePtr<IOperator>> TreeNodes;
};

class TOpRoot;

struct TOpIterator {
    struct TIteratorItem {
        TIteratorItem(TIntrusivePtr<IOperator> curr, TIntrusivePtr<IOperator> parent, size_t idx, std::shared_ptr<TInfoUnit> subplanIU)
            : Current(curr)
            , Parent(parent)
            , ChildIndex(idx)
            , SubplanIU(subplanIU) {
        }

        TIntrusivePtr<IOperator> Current;
        TIntrusivePtr<IOperator> Parent;
        size_t ChildIndex;
        std::shared_ptr<TInfoUnit> SubplanIU;
    };

    using iterator_category = std::input_iterator_tag;
    using difference_type = std::ptrdiff_t;

    // Build a default iterator for the root of the plan
    // It will visit all the subplans in their DFS order first and then the main plan
    TOpIterator(TOpRoot* ptr);

    // Build an iterator for traversing the children of specific operator
    TOpIterator(TIntrusivePtr<IOperator> op, TIntrusivePtr<IOperator> parent);

    // Build an iterator to travese the children of a specific iterator, recursing into
    // subplans, as their UIs are encountered
    TOpIterator(TIntrusivePtr<IOperator> op, TIntrusivePtr<IOperator> parent, TPlanProps* props);

    TIteratorItem operator*() const;

    // Prefix increment
    TOpIterator& operator++();

    // Postfix increment
    TOpIterator operator++(int);

    friend bool operator==(const TOpIterator& a, const TOpIterator& b) {
        return a.CurrElement == b.CurrElement;
    };
    friend bool operator!=(const TOpIterator& a, const TOpIterator& b) {
        return a.CurrElement != b.CurrElement;
    };

private:
    void BuildDfsList(TIntrusivePtr<IOperator> current, TIntrusivePtr<IOperator> parent, size_t childIdx, std::unordered_set<IOperator*>& visited,
                        std::shared_ptr<TInfoUnit> subplanIU, bool recurseIntoSubplans = false);

    TVector<TIteratorItem> DfsList;
    size_t CurrElement;
    TPlanProps* PlanProps;
};

class TOpRoot: public IUnaryOperator {
public:
    TOpRoot(TIntrusivePtr<IOperator> input, TPositionHandle pos, const TVector<TString>& columnOrder);
    virtual TVector<TInfoUnit> GetOutputIUs() override;
    virtual TString ToString(TExprContext& ctx) override;
    virtual TString GetExplainName() const override { return "Root"; }

    void ComputeParents();
    IGraphTransformer::TStatus ComputeTypes(TRBOContext& ctx);

    TString PlanToString(TExprContext& ctx, ui32 printOptions = 0x0);
    void PlanToStringRec(TIntrusivePtr<IOperator> op, TExprContext& ctx, TStringBuilder& builder, int ntabs, ui32 printOptions = 0x0) const;

    void ComputePlanMetadata(TRBOContext& ctx);
    void ComputePlanStatistics(TRBOContext& ctx);

    TOpIterator begin() {
        return TOpIterator(this);
    }

    TOpIterator end() {
        return TOpIterator(nullptr);
    }

    NJson::TJsonValue GetExecutionJson(ui64 & nodeCounter, THashMap<IOperator*, ui32>& operatorIds, ui32 explainFlags = 0x00);
    NJson::TJsonValue GetExplainJson(ui64 & nodeCounter, const THashMap<IOperator*, ui32>& operatorIds, ui32 explainFlags = 0x00);

    TPlanProps PlanProps;
    TExprNode::TPtr Node;
    TVector<TString> ColumnOrder;

private:
    void ComputeParentsRec(TIntrusivePtr<IOperator> op, TIntrusivePtr<IOperator> parent, ui32 parentChildIndex) const;
};

} // namespace NKqp
} // namespace NKikimr
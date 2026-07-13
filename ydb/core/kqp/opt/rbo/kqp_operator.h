#pragma once

#include "kqp_simple_operator.h"
#include "kqp_info_unit.h"
#include "kqp_expression.h"
#include "kqp_rbo_context.h"
#include "kqp_rbo_statistics.h"

#include <cstddef>
#include <iterator>
#include <optional>
#include <library/cpp/containers/absl/flat_hash_set.h>
#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/opt/kqp_opt.h>
#include <yql/essentials/ast/yql_expr.h>
#include <ydb/core/kqp/opt/cbo/cbo_optimizer_new.h>
#include <library/cpp/json/writer/json.h>

namespace NKikimr {
namespace NKqp {

using namespace NYql;

enum EOperator : ui32 { EmptySource, Source, Map, AddDependencies, Filter, Join, Aggregate, Limit, Sort, UnionAll, CBOTree, Root };

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

// Recomputable logical analysis state. std::nullopt means the analysis was not
// computed for this operator; computed-but-empty state is represented by an
// engaged empty value.
struct TOperatorAnalysisProps {
    void Clear() {
        LiveOut.reset();
        Aliases.reset();
        NameConstraints.reset();
        InRootAliasRegion = false;
    }

    std::optional<TInfoUnitSet> LiveOut;
    std::optional<TPlanAliases::TAliasMap> Aliases;
    std::optional<TPlanNameConstraints> NameConstraints;
    // True when no alias-class cut (Aggregate, UnionAll) separates this
    // operator's output from the root: only there do root output names pin
    // their alias class. Computed together with plan aliases.
    bool InRootAliasRegion = false;
};

/**
 * Per-operator physical plan properties
 * TODO: Make this more generic and extendable
 */
struct TPhysicalOpProps {
    TPhysicalOpProps() = default;

    TPhysicalOpProps(const TPhysicalOpProps& other) {
        CopyPhysicalFrom(other);
    }

    TPhysicalOpProps(TPhysicalOpProps&& other) {
        CopyPhysicalFrom(other);
    }

    TPhysicalOpProps& operator=(const TPhysicalOpProps& other) {
        if (this != &other) {
            CopyPhysicalFrom(other);
        }
        return *this;
    }

    TPhysicalOpProps& operator=(TPhysicalOpProps&& other) {
        if (this != &other) {
            CopyPhysicalFrom(other);
        }
        return *this;
    }

    void ClearLogicalAnalysis() {
        Analysis.Clear();
    }

    std::optional<int> StageId;
    std::optional<TString> Algorithm;
    std::optional<TOrderEnforcer> OrderEnforcer;
    bool EnsureAtMostOne = false;

    std::optional<TRBOMetadata> Metadata;
    std::optional<TRBOStatistics> Statistics;
    std::optional<NKikimr::NKqp::EJoinAlgoType> JoinAlgo;
    std::optional<double> Cost;

    // CBO decision for this join's input edges.
    // std::nullopt means there was no explicit decision.
    // Empty vector means shuffle is eliminated.
    std::optional<TVector<TInfoUnit>> LeftShuffleBy;
    std::optional<TVector<TInfoUnit>> RightShuffleBy;
    // Cached output information units
    std::optional<TVector<TInfoUnit>> OutputIUs;

    // Recomputable logical analysis state. Copies of physical props intentionally
    // do not preserve these fields; analyses are valid only for the current graph.
    TOperatorAnalysisProps Analysis;

private:
    void CopyPhysicalFrom(const TPhysicalOpProps& other) {
        StageId = other.StageId;
        Algorithm = other.Algorithm;
        OrderEnforcer = other.OrderEnforcer;
        EnsureAtMostOne = other.EnsureAtMostOne;
        Metadata = other.Metadata;
        Statistics = other.Statistics;
        JoinAlgo = other.JoinAlgo;
        Cost = other.Cost;
        LeftShuffleBy = other.LeftShuffleBy;
        RightShuffleBy = other.RightShuffleBy;
        OutputIUs = other.OutputIUs;
        ClearLogicalAnalysis();
    }
};

class IOperator;

class ILivenessContext {
public:
    virtual ~ILivenessContext() = default;

    virtual const TInfoUnitSet& GetLiveOut(IOperator* op) const = 0;
    virtual bool AddLiveColumns(const TIntrusivePtr<IOperator>& op, const TVector<TInfoUnit>& columns) = 0;
    virtual bool AddLiveColumns(const TIntrusivePtr<IOperator>& op, const TInfoUnitSet& columns) = 0;
    virtual void AddExpressionDeps(const TExpression& expr, TInfoUnitSet& target) = 0;
};

/**
 * Interface for the operator
 */

class TOpRoot;


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
    virtual const TVector<TInfoUnit>& GetOutputIUs();

    /**
     * Get local child-output references that can be renamed without changing
     * this operator's produced output names.
     */
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

    /**
     * Rename output/schema names produced and owned by this operator.
     */
    virtual void RenameProducedIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap, TExprContext& ctx);

    /**
     * Rename local child-output references without changing produced output names.
     */
    virtual void RenameUsedIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap, TExprContext& ctx);

    virtual void ComputeMetadata(TRBOContext& ctx, TPlanProps& planProps) = 0;
    virtual void ComputeStatistics(TRBOContext& ctx, TPlanProps& planProps) = 0;
    virtual void PropagateLiveness(ILivenessContext& ctx);
    virtual bool PropagateNameConstraints();
    virtual TPlanAliases::TAliasMap ComputeAliases();

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

protected:
    virtual void ComputeOutputIUs() = 0;
    virtual void ComputeOutputIUsSubtree();

    friend class TOpRoot;
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
    virtual void PropagateLiveness(ILivenessContext& ctx) override;
    virtual bool PropagateNameConstraints() override;
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

    virtual TString ToString(TExprContext& ctx) override;
    virtual TString GetExplainName() const override { return "EmptySource"; }

    virtual void ComputeMetadata(TRBOContext& ctx, TPlanProps& planProps) override;
    virtual void ComputeStatistics(TRBOContext& ctx, TPlanProps& planProps) override;

protected:
    void ComputeOutputIUs() override;
};

class TOpRead: public IOperator {
public:
    // Everything the read carries about pushed-down key ranges. ComputeNode is the source of
    // truth for the ranges themselves; the other fields are range extractor outputs that cannot
    // be recovered from the expression later (explain has no access to table metadata or the
    // extractor settings).
    struct TRangeInfo {
        TExprNode::TPtr ComputeNode;  // ranges expression pushed into the read
        TVector<TString> KeyColumns;  // all table key columns (with or without alias prefix)
        size_t UsedPrefixLen = 0;     // how many leading key columns are range-constrained
        TMaybe<size_t> ExpectedMaxRanges;
    };

    TOpRead(TExprNode::TPtr node);
    TOpRead(const TString& alias, const TVector<TString>& columns, const TVector<TInfoUnit>& outputIUs, const NYql::EStorageType storageType,
            const TExprNode::TPtr& tableCallable, const TExprNode::TPtr& olapFilterLambda, const TExprNode::TPtr& limit, std::optional<TRangeInfo> ranges,
            const std::optional<TExpression>& originalPredicate, const ESortDir sortDireciont, const TPhysicalOpProps& props, TPositionHandle pos);

    virtual void PropagateLiveness(ILivenessContext& ctx) override;
    virtual TString ToString(TExprContext& ctx) override;
    virtual TString GetExplainName() const override { return RangeInfo.has_value() ? "TableRangeScan" : "TableFullScan"; }
    virtual NJson::TJsonValue ToJson(ui32 explainFlags) override;

    void RenameProducedIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap, TExprContext& ctx) override;
    bool NeedsMap() const;

    virtual void ComputeMetadata(TRBOContext& ctx, TPlanProps& planProps) override;
    virtual void ComputeStatistics(TRBOContext& ctx, TPlanProps& planProps) override;
    NYql::EStorageType GetTableStorageType() const;

    TExprNode::TPtr GetRanges() const { return RangeInfo ? RangeInfo->ComputeNode : nullptr; }
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
    std::optional<TExpression> OriginalPredicate;
    ESortDir SortDir{ESortDir::None};
    std::optional<TRangeInfo> RangeInfo;

protected:
    void ComputeOutputIUs() override;
};

class TMapElement {
public:
    TMapElement() = default;
    TMapElement(const TInfoUnit& elementName, const TExpression& expr, bool isRename = false);
    TMapElement(const TInfoUnit& elementName, const TInfoUnit& rename, TPositionHandle pos, TExprContext* ctx, TPlanProps* props = nullptr, bool isRename = true);

    bool IsRename() const;
    void SetIsRename(bool isRename);
    bool IsColumnAccess() const;
    TInfoUnit GetRename() const;
    TInfoUnit GetColumnAccess() const;

    TInfoUnit GetElementName() const;
    void SetElementName(const TInfoUnit& elementName);
    const TExpression& GetExpression() const;
    TExpression& GetExpressionRef();
    bool DependsOnlyOn(const TVector<TInfoUnit>& availableIUs) const;
    void SetExpression(TExpression expr);

private:
    TInfoUnit ElementName;
    TExpression Expr;
    bool Rename = false;
};

class TOpMap: public IUnaryOperator {
public:
    TOpMap(TIntrusivePtr<IOperator> input, TPositionHandle pos, const TVector<TMapElement>& mapElements, bool ordered = false);
    TOpMap(TIntrusivePtr<IOperator> input, TPositionHandle pos, const TPhysicalOpProps& props, const TVector<TMapElement>& mapElements,
           bool ordered = false);

    virtual TVector<TInfoUnit> GetUsedIUs(TPlanProps& props) override;
    virtual TVector<TInfoUnit> GetSubplanIUs(TPlanProps& props) override;
    virtual TVector<std::reference_wrapper<TExpression>> GetExpressions() override;
    virtual void PropagateLiveness(ILivenessContext& ctx) override;
    virtual bool PropagateNameConstraints() override;
    virtual TPlanAliases::TAliasMap ComputeAliases() override;
    TVector<std::pair<TInfoUnit, TInfoUnit>> GetRenames() const;
    TInfoUnitSet GetRenameSources() const;
    TVector<std::pair<TInfoUnit, TInfoUnit>> GetPropertyPreservingMappings(TPlanProps& props) const;
    virtual void ApplyReplaceMap(const TNodeOnNodeOwnedMap& map, TRBOContext& ctx) override;

    void RenameProducedIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap, TExprContext& ctx) override;
    void RenameUsedIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap, TExprContext& ctx) override;

    virtual void ComputeMetadata(TRBOContext& ctx, TPlanProps& planProps) override;
    virtual void ComputeStatistics(TRBOContext& ctx, TPlanProps& planProps) override;

    virtual TString ToString(TExprContext& ctx) override;
    virtual NJson::TJsonValue ToJson(ui32 explainFlags) override;
    virtual TString GetExplainName() const override { return "Map"; }

    bool IsOrdered() const {
        return Ordered;
    }

    TVector<TMapElement>& GetMapElements() { return MapElements; }
    TMapElement* FindOutputElement(const TInfoUnit& output);
    const TMapElement* FindOutputElement(const TInfoUnit& output) const;
    bool HasOutputElement(const TInfoUnit& output) const;
    bool HasRenames() const;

    TVector<TMapElement> MapElements;
    bool Ordered = false;

protected:
    void ComputeOutputIUs() override;
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
    virtual TPlanAliases::TAliasMap ComputeAliases() override;
    void RenameProducedIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap, TExprContext& ctx) override;
    virtual TString ToString(TExprContext& ctx) override;
    virtual TString GetExplainName() const override { return "AddDependencies"; }
    
    TVector<TInfoUnit> Dependencies;
    TVector<const TTypeAnnotationNode*> Types;

protected:
    void ComputeOutputIUs() override;
};

struct TOpAggregationTraits {
    TOpAggregationTraits() = default;
    TOpAggregationTraits(const TInfoUnit& originalColName, const TString& aggFunction, const TInfoUnit& resultColName, bool distinct = false,
                         bool unwrap = false)
        : OriginalColName(originalColName)
        , AggFunction(aggFunction)
        , ResultColName(resultColName)
        , Distinct(distinct)
        , Unwrap(unwrap) {
    }

    TInfoUnit OriginalColName;
    TString AggFunction;
    TInfoUnit ResultColName;
    bool Distinct;
    bool Unwrap;
};

class TOpAggregate: public IUnaryOperator {
public:
    TOpAggregate(TIntrusivePtr<IOperator> input, const TVector<TOpAggregationTraits>& aggTraitsList, const TVector<TInfoUnit>& keyColumns,
                 const EOpPhase aggPhase, bool distinctAll, TPositionHandle pos);
    TOpAggregate(TIntrusivePtr<IOperator> input, const TVector<TOpAggregationTraits>& aggTraitsList, const TVector<TInfoUnit>& keyColumns,
                 const EOpPhase aggPhase, bool distinctAll, const TPhysicalOpProps& props, TPositionHandle pos);

    virtual TVector<TInfoUnit> GetUsedIUs(TPlanProps& props) override;
    virtual void PropagateLiveness(ILivenessContext& ctx) override;
    virtual bool PropagateNameConstraints() override;

    void RenameProducedIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap, TExprContext& ctx) override;
    void RenameUsedIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap, TExprContext& ctx) override;
    virtual TString ToString(TExprContext& ctx) override;
    virtual NJson::TJsonValue ToJson(ui32 explainFlags) override;
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

protected:
    void ComputeOutputIUs() override;
};

class TOpFilter: public IUnaryOperator {
public:
    TOpFilter(TIntrusivePtr<IOperator> input, TPositionHandle pos, const TExpression& filterExpr);
    TOpFilter(TIntrusivePtr<IOperator> input, TPositionHandle pos, const TPhysicalOpProps& props, const TExpression& filterExpr);

    virtual TVector<TInfoUnit> GetUsedIUs(TPlanProps& props) override;
    virtual TVector<TInfoUnit> GetSubplanIUs(TPlanProps& props) override;
    virtual TString ToString(TExprContext& ctx) override;
    virtual NJson::TJsonValue ToJson(ui32 explainFlags) override;
    virtual TString GetExplainName() const override { return "Filter"; }

    virtual TVector<std::reference_wrapper<TExpression>> GetExpressions() override;
    virtual void PropagateLiveness(ILivenessContext& ctx) override;
    virtual TPlanAliases::TAliasMap ComputeAliases() override;
    virtual void ApplyReplaceMap(const TNodeOnNodeOwnedMap& map, TRBOContext& ctx) override;

    TVector<TInfoUnit> GetFilterIUs(TPlanProps& props) const;
    void RenameUsedIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap, TExprContext& ctx) override;

    virtual void ComputeMetadata(TRBOContext& ctx, TPlanProps& planProps) override;
    virtual void ComputeStatistics(TRBOContext& ctx, TPlanProps& planProps) override;
    TExpression GetFilterExpression() const { return FilterExpr; }

    TExpression FilterExpr;

protected:
    void ComputeOutputIUs() override;
};

bool TestAndExtractEqualityPredicate(TExprNode::TPtr pred, TExprNode::TPtr& leftArg, TExprNode::TPtr& rightArg);

class TOpJoin: public IBinaryOperator {
public:
    TOpJoin(TIntrusivePtr<IOperator> leftArg, TIntrusivePtr<IOperator> rightArg, TPositionHandle pos, TString joinKind,
            const TVector<std::pair<TInfoUnit, TInfoUnit>>& joinKeys);

    TOpJoin(TIntrusivePtr<IOperator> leftArg, TIntrusivePtr<IOperator> rightArg, TPositionHandle pos, TString joinKind,
            const TVector<std::pair<TInfoUnit, TInfoUnit>>& joinKeys, const TVector<TExpression>& joinFilters);

    virtual TVector<TInfoUnit> GetUsedIUs(TPlanProps& props) override;
    virtual TVector<std::reference_wrapper<TExpression>> GetExpressions() override;
    virtual void PropagateLiveness(ILivenessContext& ctx) override;
    virtual bool PropagateNameConstraints() override;
    virtual TPlanAliases::TAliasMap ComputeAliases() override;

    void RenameUsedIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap, TExprContext& ctx) override;
    virtual TString ToString(TExprContext& ctx) override;
    virtual NJson::TJsonValue ToJson(ui32 explainFlags) override;
    virtual TString GetExplainName() const override { return "Join"; }

    virtual void ComputeMetadata(TRBOContext& ctx, TPlanProps& planProps) override;
    virtual void ComputeStatistics(TRBOContext& ctx, TPlanProps& planProps) override;

    TVector<TInfoUnit> GetLHSKeys() const;
    TVector<TInfoUnit> GetRHSKeys() const;

    TString JoinKind;
    TVector<std::pair<TInfoUnit, TInfoUnit>> JoinKeys;
    TVector<TExpression> JoinFilters;

protected:
    void ComputeOutputIUs() override;
};

class TOpUnionAll: public IBinaryOperator {
public:
    TOpUnionAll(TIntrusivePtr<IOperator> leftArg, TIntrusivePtr<IOperator> rightArg, TPositionHandle pos,
                TVector<TInfoUnit> columns, bool ordered = false);
    virtual void PropagateLiveness(ILivenessContext& ctx) override;
    virtual bool PropagateNameConstraints() override;
    void RenameProducedIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap, TExprContext& ctx) override;
    virtual TString ToString(TExprContext& ctx) override;
    virtual NJson::TJsonValue ToJson(ui32 explainFlags) override;
    virtual TString GetExplainName() const override { return "UnionAll"; }

    virtual void ComputeMetadata(TRBOContext& ctx, TPlanProps& planProps) override;
    virtual void ComputeStatistics(TRBOContext& ctx, TPlanProps& planProps) override;

    TVector<TInfoUnit> Columns;
    bool Ordered;

protected:
    void ComputeOutputIUs() override;
};

class TOpLimit: public IUnaryOperator {
public:
    TOpLimit(TIntrusivePtr<IOperator> input, TPositionHandle pos, const TExpression& limitCond, const EOpPhase limitPhase);
    TOpLimit(TIntrusivePtr<IOperator> input, TPositionHandle pos, const TExpression& limitCond, const TExpression& offsetCond, const EOpPhase limitPhase);
    TOpLimit(TIntrusivePtr<IOperator> input, TPositionHandle pos, const TPhysicalOpProps& props, const TExpression& limitCond, const EOpPhase limitPhase);
    TOpLimit(TIntrusivePtr<IOperator> input, TPositionHandle pos, const TPhysicalOpProps& props, const TExpression& limitCond,
             std::optional<TExpression> offsetCond, const EOpPhase limitPhase);

    virtual TVector<TInfoUnit> GetUsedIUs(TPlanProps& props) override;
    virtual void PropagateLiveness(ILivenessContext& ctx) override;
    virtual TPlanAliases::TAliasMap ComputeAliases() override;
    void RenameUsedIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap, TExprContext& ctx) override;
    virtual TString ToString(TExprContext& ctx) override;
    virtual NJson::TJsonValue ToJson(ui32 explainFlags) override;
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

protected:
    void ComputeOutputIUs() override;

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

    virtual TVector<TInfoUnit> GetUsedIUs(TPlanProps& props) override;
    virtual void PropagateLiveness(ILivenessContext& ctx) override;
    virtual TPlanAliases::TAliasMap ComputeAliases() override;
    void RenameUsedIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap, TExprContext& ctx) override;
    virtual TString ToString(TExprContext& ctx) override;
    virtual NJson::TJsonValue ToJson(ui32 explainFlags) override;
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

    virtual TString GetExplainName() const override { return IsTopSort() ? "TopSort" : "Sort"; }

    TVector<TSortElement> SortElements;
    std::optional<TExpression> LimitCond;

protected:
    void ComputeOutputIUs() override;

private:
    EOpPhase SortPhase{EOpPhase::Undefined};
};


/***
 * This operator packages a subtree of operators in order to pass them to dynamic programming optimizer
 * Currently it requires that the list of operators TreeNodes is in a post-order traversal of the tree
 *
 * TreeRoot is the root of the packed subtree. TreeNodes are operators inside the
 * packed CBO island. Children is inherited from IOperator and stores only
 * boundary inputs outside TreeNodes:
 *
 *     JoinAB       TreeNodes = [JoinAB]
 *     /   \        Children  = [A, B]
 *    A     B
 *
 * RebuildChildren rebuilds Children by filtering internal TreeNodes from child
 * edges when the packed island is created.
 *
 * No validation is currently used
 */
class TOpCBOTree: public IOperator {
public:
    TOpCBOTree(TIntrusivePtr<IOperator> treeRoot, TPositionHandle pos);
    TOpCBOTree(TIntrusivePtr<IOperator> treeRoot, TVector<TIntrusivePtr<IOperator>> treeNodes, TPositionHandle pos);

    virtual const TVector<TInfoUnit>& GetOutputIUs() override {
        return TreeRoot->GetOutputIUs();
    }
    virtual void PropagateLiveness(ILivenessContext& ctx) override;
    void RenameProducedIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap, TExprContext& ctx) override;
    virtual TString ToString(TExprContext& ctx) override;
    virtual TString GetExplainName() const override { return "CBOTree"; }


    virtual void ComputeMetadata(TRBOContext& ctx, TPlanProps& planProps) override;
    virtual void ComputeStatistics(TRBOContext& ctx, TPlanProps& planProps) override;

    TIntrusivePtr<IOperator> TreeRoot;
    TVector<TIntrusivePtr<IOperator>> TreeNodes;

protected:
    void ComputeOutputIUs() override;

private:
    void RebuildChildren();
};

// End-of-traversal sentinel for TOpIterator
struct TOpEnd {};

// Order in which a traversal emits operators
enum class ETraversalOrder {
    // Referenced subplans and children before the node (dependencies first)
    PostOrder,
    // The node before its referenced subplans and children
    PreOrder,
};

/**
 * Lazy plan traversal with an explicit frame stack: O(depth) state plus a visited
 * set for DAG dedup. Constructed through the range factories: TOpRoot::Iterate,
 * IterateSubtree, IterateSubtreeWithSubplans.
 *
 * Mutation contract: the iterator walks live GetChildren() edges and keeps raw
 * operator pointers in its visited set, so it must not be advanced after the plan
 * is mutated. Mutate, then stop iterating and build a fresh iterator (as the rule
 * engine does), or use a TOpTraversal snapshot when iteration must survive
 * mutation.
 */
struct TOpIterator {
    struct TIteratorItem {
        TIteratorItem() = default;

        TIteratorItem(TIntrusivePtr<IOperator> curr, TIntrusivePtr<IOperator> parent, size_t idx, std::shared_ptr<TInfoUnit> subplanIU)
            : Current(curr)
            , Parent(parent)
            , ChildIndex(idx)
            , SubplanIU(subplanIU) {
        }

        TIntrusivePtr<IOperator> Current;
        // Parent/ChildIndex/SubplanIU describe the traversal edge along which the
        // node was first reached — a property of this traversal, not of the graph.
        // A shared (DAG) node may be reached via a different parent under a
        // different traversal order; use IOperator::Parents for graph structure.
        TIntrusivePtr<IOperator> Parent;
        size_t ChildIndex = 0;
        std::shared_ptr<TInfoUnit> SubplanIU;
    };

private:
    struct TFrame {
        TFrame(TIntrusivePtr<IOperator> current, TIntrusivePtr<IOperator> parent, size_t childIdx, std::shared_ptr<TInfoUnit> subplanIU)
            : Current(current)
            , Parent(parent)
            , ChildIndex(childIdx)
            , SubplanIU(subplanIU) {
        }

        TFrame(const TFrame&) = delete;
        TFrame& operator=(const TFrame&) = delete;
        TFrame(TFrame&&) noexcept = default;
        TFrame& operator=(TFrame&&) noexcept = default;

        TIntrusivePtr<IOperator> Current;
        TIntrusivePtr<IOperator> Parent;
        size_t ChildIndex = 0;
        std::shared_ptr<TInfoUnit> SubplanIU;
        TVector<TInfoUnit> SubplanIUs;
        size_t NextSubplanIdx = 0;
        size_t NextChildIdx = 0;
        bool SubplansLoaded = false;
        // Pre-order only: the node was already emitted when its frame was entered
        bool Emitted = false;
    };

    // Only the range factories construct iterators
    friend class TOpRange;

    TOpIterator(TIntrusivePtr<IOperator> op, TPlanProps* props, bool followSubplans, ETraversalOrder order);

public:
    using iterator_category = std::input_iterator_tag;
    using difference_type = std::ptrdiff_t;
    using value_type = TIteratorItem;
    using reference = const TIteratorItem&;
    using pointer = const TIteratorItem*;

    // The iterator carries a traversal stack and a visited set, so copying is
    // expensive and never needed. Moving relocates only the live stack frames.
    TOpIterator(const TOpIterator&) = delete;
    TOpIterator& operator=(const TOpIterator&) = delete;
    TOpIterator(TOpIterator&& other);
    TOpIterator& operator=(TOpIterator&& other);

    const TIteratorItem& operator*() const;

    const TIteratorItem* operator->() const {
        return &Current;
    }

    // Prefix increment
    TOpIterator& operator++();

    // Postfix increment returns void: returning the pre-increment iterator
    // would require copying the traversal state
    void operator++(int);

    friend bool operator==(const TOpIterator& a, TOpEnd) {
        return a.AtEnd;
    }
    friend bool operator!=(const TOpIterator& a, TOpEnd) {
        return !a.AtEnd;
    }

private:
    bool PushFrame(TIntrusivePtr<IOperator> op, TIntrusivePtr<IOperator> parent, size_t childIdx, std::shared_ptr<TInfoUnit> subplanIU);
    void Advance();

    // Covers more than 90% of measured TPCH/TPCDS traversals without allocating frame storage.
    TStackVec<TFrame, 24> Stack;
    absl::flat_hash_set<IOperator*> Visited;
    TIteratorItem Current;
    TPlanProps* PlanProps = nullptr;
    bool RecurseIntoSubplans = false;
    ETraversalOrder Order = ETraversalOrder::PostOrder;
    bool AtEnd = true;
};

/**
 * Lazy traversal range: a cheap value describing what to traverse and in which
 * order. begin() builds a fresh TOpIterator, end() is the TOpEnd sentinel.
 * Obtain one from TOpRoot::Iterate, IterateSubtree or IterateSubtreeWithSubplans.
 */
class TOpRange {
public:
    TOpIterator begin() const {
        return TOpIterator(Op, Props, FollowSubplans, Order);
    }

    TOpEnd end() const {
        return {};
    }

private:
    friend class TOpRoot;
    friend TOpRange IterateSubtree(TIntrusivePtr<IOperator> op, ETraversalOrder order);
    friend TOpRange IterateSubtreeWithSubplans(TIntrusivePtr<IOperator> op, TPlanProps& props, ETraversalOrder order);

    TOpRange(TIntrusivePtr<IOperator> op, TPlanProps* props, bool followSubplans, ETraversalOrder order)
        : Op(op)
        , Props(props)
        , FollowSubplans(followSubplans)
        , Order(order) {
    }

    TIntrusivePtr<IOperator> Op;
    TPlanProps* Props = nullptr;
    bool FollowSubplans = false;
    ETraversalOrder Order;
};

// Traverse the operators of a single plan, without following subplan references
inline TOpRange IterateSubtree(TIntrusivePtr<IOperator> op, ETraversalOrder order = ETraversalOrder::PostOrder) {
    return TOpRange(op, nullptr, false, order);
}

// Traverse an operator subtree, recursing into subplans referenced by expressions
inline TOpRange IterateSubtreeWithSubplans(TIntrusivePtr<IOperator> op, TPlanProps& props, ETraversalOrder order = ETraversalOrder::PostOrder) {
    return TOpRange(op, &props, true, order);
}

/**
 * Traversal snapshot: drains a lazy iterator into a vector, so it is
 * guaranteed to emit the same sequence as the lazy traversal it was built from.
 * Buys what the lazy iterator cannot offer: reverse iteration, multiple passes,
 * and validity across plan mutation. Obtain one from TOpRoot::SnapshotTraversal.
 */
class TOpTraversal {
public:
    class TIterator {
    public:
        using iterator_category = std::forward_iterator_tag;
        using difference_type = std::ptrdiff_t;
        using value_type = TOpIterator::TIteratorItem;
        using reference = const value_type&;
        using pointer = const value_type*;

        TIterator(const TVector<TOpIterator::TIteratorItem>* items, size_t index)
            : Items(items)
            , Index(index) {
        }

        reference operator*() const {
            return (*Items)[Index];
        }

        pointer operator->() const {
            return &(*Items)[Index];
        }

        TIterator& operator++() {
            ++Index;
            return *this;
        }

        TIterator operator++(int) {
            TIterator tmp = *this;
            ++(*this);
            return tmp;
        }

        friend bool operator==(const TIterator& lhs, const TIterator& rhs) {
            return lhs.Items == rhs.Items && lhs.Index == rhs.Index;
        }

        friend bool operator!=(const TIterator& lhs, const TIterator& rhs) {
            return !(lhs == rhs);
        }

    private:
        const TVector<TOpIterator::TIteratorItem>* Items;
        size_t Index;
    };

    using TReverseIterator = TVector<TOpIterator::TIteratorItem>::const_reverse_iterator;

    explicit TOpTraversal(TOpIterator it) {
        for (; it != TOpEnd{}; ++it) {
            Items.push_back(*it);
        }
    }

    TIterator begin() const {
        return TIterator(&Items, 0);
    }

    TIterator end() const {
        return TIterator(&Items, Items.size());
    }

    TReverseIterator rbegin() const {
        return Items.rbegin();
    }

    TReverseIterator rend() const {
        return Items.rend();
    }

private:
    TVector<TOpIterator::TIteratorItem> Items;
};

class TOpRoot: public IUnaryOperator {
public:
    TOpRoot(TIntrusivePtr<IOperator> input, TPositionHandle pos, const TVector<TString>& columnOrder);
    virtual TString ToString(TExprContext& ctx) override;
    virtual TString GetExplainName() const override { return "Root"; }

    void ComputeParents();
    IGraphTransformer::TStatus ComputeTypes(TRBOContext& ctx);

    TString PlanToString(TExprContext& ctx, ui32 printOptions = 0x0);
    void PlanToStringRec(TIntrusivePtr<IOperator> op, TExprContext& ctx, TStringBuilder& builder, int ntabs, ui32 printOptions = 0x0) const;

    void ComputePlanMetadata(TRBOContext& ctx);
    void ComputePlanStatistics(TRBOContext& ctx);

    // Lazy traversal of the whole plan, following subplan references
    TOpRange Iterate(ETraversalOrder order) {
        return TOpRange(GetInput(), &PlanProps, true, order);
    }

    // Default root traversal preserves the historical dependency-first order.
    TOpIterator begin() {
        return Iterate(ETraversalOrder::PostOrder).begin();
    }

    TOpEnd end() const {
        return {};
    }

    // Snapshot of the whole-plan traversal, following subplan references;
    // supports reverse iteration and stays valid across plan mutation
    TOpTraversal SnapshotTraversal(ETraversalOrder order = ETraversalOrder::PostOrder) {
        return TOpTraversal(Iterate(order).begin());
    }

    NJson::TJsonValue GetExecutionJson(ui64 & nodeCounter, THashMap<IOperator*, ui32>& operatorIds, ui32 explainFlags = 0x00);
    NJson::TJsonValue GetExplainJson(ui64 & nodeCounter, const THashMap<IOperator*, ui32>& operatorIds, ui32 explainFlags = 0x00);

    TPlanProps PlanProps;
    TExprNode::TPtr Node;
    TVector<TString> ColumnOrder;

protected:
    void ComputeOutputIUs() override;
    void ComputeOutputIUsSubtree() override;

    friend void EnsureRequiredProps(TOpRoot& root, ui32 props, ui32& computedProps, TRBOContext& ctx, const TString& stageName);
};

} // namespace NKqp
} // namespace NKikimr

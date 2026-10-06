#pragma once

#include "kqp_info_unit.h"
#include "kqp_expression.h"
#include "kqp_rbo_context.h"
#include "kqp_rbo_statistics.h"

#include <cstddef>
#include <iterator>
#include <memory>
#include <ranges>
#include <optional>
#include <type_traits>
#include <utility>
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

enum EOperator : ui32 { EmptySource, Source, Map, AddDependencies, Filter, Join, DependentJoin, Aggregate, GroupingSets, Window, Limit, Sort, UnionAll, TableLookup, IndexLookupJoin, CBOTree, TableEffect, Root, Replicate };

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
    TSortIUs SortElements;
};

enum ESortDir : ui32 { None = 0x00, Asc = 0x01, Desc = 0x02 };

// Recomputable logical analysis state. std::nullopt means the analysis was not
// computed for this operator; computed-but-empty state is represented by an
// engaged empty value.
struct TOperatorAnalysisProps {
    void Clear() {
        LiveInByChild.reset();
        LiveOut.reset();
    }

    // Input requirements per child edge; output demand from all consumers.
    std::optional<TVector<TUnorderedIUs>> LiveInByChild;
    std::optional<TUnorderedIUs> LiveOut;
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
    // Dense physical output index of a Replicate port; independent of its stable
    // logical ordinal, since pruning may remove arbitrary ports.
    std::optional<ui32> StageOutputIndex;
    std::optional<TString> Algorithm;
    std::optional<TOrderEnforcer> OrderEnforcer;

    std::optional<TRBOMetadata> Metadata;
    std::optional<TRBOStatistics> Statistics;
    std::optional<NKikimr::NKqp::EJoinAlgoType> JoinAlgo;
    // Resolved physical implementation for a join. std::nullopt means that
    // physical join selection has not run yet.
    std::optional<bool> UseBlockHashJoin;
    std::optional<double> Cost;

    // CBO decision for this join's input edges.
    // std::nullopt means there was no explicit decision.
    // Empty vector means shuffle is eliminated.
    std::optional<TOrderedIUs<>> LeftShuffleBy;
    std::optional<TOrderedIUs<>> RightShuffleBy;
    // Cached output information units
    std::optional<TUnorderedIUs> OutputIUs;

    // Recomputable logical analysis state. Copies of physical props intentionally
    // do not preserve these fields; analyses are valid only for the current graph.
    TOperatorAnalysisProps Analysis;

private:
    void CopyPhysicalFrom(const TPhysicalOpProps& other) {
        StageId = other.StageId;
        StageOutputIndex = other.StageOutputIndex;
        Algorithm = other.Algorithm;
        OrderEnforcer = other.OrderEnforcer;
        Metadata = other.Metadata;
        Statistics = other.Statistics;
        JoinAlgo = other.JoinAlgo;
        UseBlockHashJoin = other.UseBlockHashJoin;
        Cost = other.Cost;
        LeftShuffleBy = other.LeftShuffleBy;
        RightShuffleBy = other.RightShuffleBy;
        // OutputIUs depends on both the operator and its current inputs. Props
        // are frequently copied into a newly constructed, rewritten operator,
        // so carrying this cache across the copy can make lazy reads stale.
        OutputIUs.reset();
        ClearLogicalAnalysis();
    }
};

class IOperator;

// Local liveness keeps the inputs of every definition, so a single rewrite may
// rely on it. Global liveness follows demand only: dead definitions lose their
// inputs, so it is valid only for pruning that removes them all together.
enum class ELivenessMode { Local, Global };
// Definitions a global pruning run may remove. Operators outside the scope keep
// every definition, so global liveness keeps the inputs those definitions use.
enum class EPruningScope { AllDefinitions, MapDefinitions };

class ILivenessContext {
public:
    ILivenessContext(ELivenessMode mode, EPruningScope scope)
        : Mode(mode)
        , Scope(scope)
    {}
    virtual ~ILivenessContext() = default;

    virtual const TUnorderedIUs& GetLiveOut(const IOperator* op) const = 0;
    virtual void AddLiveInput(IOperator* op, ui32 childIndex, const TUnorderedIUs& columns) = 0;
    virtual void AddExpressionDeps(const TExpression& expr, TUnorderedIUs& target) = 0;
    // Outer sources captured by the subplan under analysis. Only global mode
    // needs them; local mode takes correlated deps from the calling expression.
    virtual void AddCaptureDeps(const TUnorderedIUs& outer) = 0;

    bool IsGlobal() const { return Mode == ELivenessMode::Global; }
    // Whether a global run may remove definitions of this operator.
    bool PrunesDefinitionsOf(const IOperator* op) const;
    // Whether `op` keeps its definition of `id`, whose inputs are then live:
    // always in local mode; in global mode, unless `id` is dead and prunable.
    bool NeedsDefinition(const IOperator* op, TInfoUnitId id) const {
        return !IsGlobal() || !PrunesDefinitionsOf(op) || GetLiveOut(op).Contains(id);
    }

private:
    const ELivenessMode Mode;
    const EPruningScope Scope;
};

/**
 * Interface for the operator
 */

class TOpRoot;
class TLogicalCopyContext;


class IOperator: public TSimpleRefCount<IOperator> {
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

    IOperator(const IOperator&) = delete;
    IOperator& operator=(const IOperator&) = delete;

    virtual size_t GetChildCount() const { return Children_.size(); }
    virtual TIntrusivePtr<IOperator>& GetChild(size_t index) { return Children_.at(index); }
    virtual const TIntrusivePtr<IOperator>& GetChild(size_t index) const { return Children_.at(index); }

    // CBO boundary inputs forward to the packed tree's slots. This range
    // allocates nothing and exposes the same edges as GetChild.
    auto GetChildren() {
        return std::views::iota(size_t{0}, GetChildCount())
            | std::views::transform([this](size_t index) { return GetChild(index).Get(); });
    }
    auto GetChildren() const {
        return std::views::iota(size_t{0}, GetChildCount())
            | std::views::transform([this](size_t index) { return GetChild(index).Get(); });
    }

    bool HasChildren() const {
        return GetChildCount() != 0;
    }

    TIntrusivePtr<IOperator>& MutableChild(size_t index) Y_LIFETIME_BOUND { return GetChild(index); }
    void SetChild(size_t index, TIntrusivePtr<IOperator> child) {
        Y_ENSURE(child, "Cannot attach a null input");
        MutableChild(index) = std::move(child);
    }

    /**
     * Get the information units that are in the output of this operator
     * Computes and caches missing output IUs for this operator subtree.
     */
    virtual const TUnorderedIUs& GetOutputIUs();

    /**
     * Get the child-output IDs this operator reads. Subplan results are not
     * included; Aggregate omits the keys it forwards.
     */
    virtual TUnorderedIUs GetUsedIUs(TPlanProps& props) {
        Y_UNUSED(props);
        return {};
    }

    /**
     * Get the unique raw input IUs used to discover subplan references. The
     * result is plan-independent and cached by operators that expose it.
     */
    virtual const TUnorderedIUs& GetUniqueRawInputIUs() const {
        static const TUnorderedIUs empty;
        return empty;
    }

    // Resolve cached, plan-independent raw input IUs against the current registry.
    // The result itself is intentionally not cached.
    TUnorderedIUs GetSubplanIUs(const TSubplans& subplans) const;

    const TTypeAnnotationNode* GetIUType(TInfoUnitId iu, TExprContext& ctx) const;

    virtual TVector<std::reference_wrapper<const TExpression>> GetExpressions() const {
        return {};
    }

    // Overrides must also bind stored expressions not exposed by GetExpressions().
    virtual void BindExpressionPlanProps(TPlanProps* props);

    virtual void ApplyReplaceMap(const TNodeOnNodeOwnedMap& map, TRBOContext& ctx) {
        Y_UNUSED(map);
        Y_UNUSED(ctx);
    }

    virtual void ReplaceChild(const TIntrusivePtr<IOperator> oldChild, const TIntrusivePtr<IOperator> newChild);

    // Copy a logical child graph, preserving sharing inside the copy (including
    // Replicate hubs), with fresh definitions unless supplied in `renames`.
    // External references follow `renames`; this overload does not copy subplans.
    // Physical operators and query roots are outside this API. Returns nullptr
    // if any operator is unsupported; allocated registry IDs are not rolled back.
    TIntrusivePtr<IOperator> Copy(TInfoUnitRegistry& registry, TSubstitutions& renames) const;

    // Also copy referenced subplans into the same plan under fresh call IDs.
    TIntrusivePtr<IOperator> Copy(TPlanProps& props, TSubstitutions& renames) const;

    // Rebuild only this operator, using the supplied children and their already
    // recorded substitutions. Replicate ports require graph-level handling.
    TIntrusivePtr<IOperator> CopyWithInputs(TVector<TIntrusivePtr<IOperator>> inputs,
        TInfoUnitRegistry& registry, TSubstitutions& renames) const;

    /**
     * Simultaneously substitute input IDs without changing owned definitions.
     * Forwarded keys change too; external labels and positional contracts do not.
     * The caller must invalidate derived properties across the affected plan.
     */
    virtual void RenameUsedIUs(const TSubstitutions& substitutions);

    virtual void ComputeMetadata(TRBOContext& ctx, TPlanProps& planProps) = 0;
    virtual void ComputeStatistics(TRBOContext& ctx, TPlanProps& planProps) = 0;
    virtual void PropagateLiveness(ILivenessContext& ctx);
    // Coordinated global pruning only: remove owned definitions, not operators
    // or semantic keys. The stage invalidates properties after all nodes agree.
    virtual bool PruneOutputs(const TUnorderedIUs& liveOut, TExprContext& ctx);

    virtual TString GetExplainName() const = 0;
    virtual TString ToString(TExprContext& ctx, const TInfoUnitRegistry& registry) = 0;

    virtual NJson::TJsonValue ToJson(ui32 explainFlags, const TInfoUnitRegistry& registry);

    const TTypeAnnotationNode* GetTypeAnn() const {
        return Type;
    }

    EOperator GetKind() const {
        return Kind;
    }

    const EOperator Kind;
    TPositionHandle Pos;
    TPhysicalOpProps Props;
    const TTypeAnnotationNode* Type = nullptr;
    TVector<std::pair<IOperator*, ui32>> Parents;

protected:
    // Reconstruct logical state and owned definitions over the original inputs.
    // The caller rebinds uses and attaches replacement inputs afterward, once
    // all definitions are known. Base: not copyable.
    virtual TIntrusivePtr<IOperator> CopyImpl(TInfoUnitRegistry& registry, TSubstitutions& renames) const;

    TVector<TIntrusivePtr<IOperator>> Children_;

    // Operators exposing owned/forwarded ID sets need no cached output copy.
    virtual void ComputeOutputIUs() {}
    virtual void ComputeOutputIUsSubtree();

    friend class TOpCBOTree;
    friend class TLogicalCopyContext;
    friend class TOpRoot;
};

template <class K, class T>
inline bool MatchOperator(T* op) {
    return dynamic_cast<const K*>(op) != nullptr;
}

template <class K, class T>
inline auto* CastOperator(T* op) {
    using TResult = std::conditional_t<std::is_const_v<T>, const K, K>;
    return static_cast<TResult*>(op);
}

template <class K>
inline bool MatchOperator(const IOperator& op) {
    return MatchOperator<K>(&op);
}

template <class K>
inline K& CastOperator(IOperator& op) {
    return static_cast<K&>(op);
}

template <class K>
inline const K& CastOperator(const IOperator& op) {
    return static_cast<const K&>(op);
}

template <class K, class T>
inline bool MatchOperator(const TIntrusivePtr<T>& op) {
    return MatchOperator<K>(op.get());
}

template <class K, class T>
inline TIntrusivePtr<K> CastOperator(const TIntrusivePtr<T>& op) {
    return TIntrusivePtr<K>(static_cast<K*>(op.Get()));
}

class IUnaryOperator: public IOperator {
public:
    IUnaryOperator(EOperator kind, TPositionHandle pos)
        : IOperator(kind, pos) {
    }
    IUnaryOperator(EOperator kind, TPositionHandle pos, TIntrusivePtr<IOperator> input)
        : IOperator(kind, pos) {
        Children_.push_back(std::move(input));
    }
    IUnaryOperator(EOperator kind, TPositionHandle pos, const TPhysicalOpProps& props, TIntrusivePtr<IOperator> input)
        : IOperator(kind, pos, props) {
        Children_.push_back(std::move(input));
    }
    TIntrusivePtr<IOperator>& GetInput() Y_LIFETIME_BOUND { return GetChild(0); }
    const TIntrusivePtr<IOperator>& GetInput() const Y_LIFETIME_BOUND { return GetChild(0); }
    void SetInput(TIntrusivePtr<IOperator> newInput) {
        SetChild(0, std::move(newInput));
    }

    virtual void ComputeMetadata(TRBOContext& ctx, TPlanProps& planProps) override;
    virtual void ComputeStatistics(TRBOContext& ctx, TPlanProps& planProps) override;
    virtual void PropagateLiveness(ILivenessContext& ctx) override;
};

class IBinaryOperator: public IOperator {
public:
    IBinaryOperator(EOperator kind, TPositionHandle pos)
        : IOperator(kind, pos) {
    }

    IBinaryOperator(EOperator kind, TPositionHandle pos, TIntrusivePtr<IOperator> leftInput, TIntrusivePtr<IOperator> rightInput)
        : IOperator(kind, pos) {
        Children_.push_back(std::move(leftInput));
        Children_.push_back(std::move(rightInput));
    }

    TIntrusivePtr<IOperator>& GetLeftInput() Y_LIFETIME_BOUND { return GetChild(0); }
    const TIntrusivePtr<IOperator>& GetLeftInput() const Y_LIFETIME_BOUND { return GetChild(0); }
    TIntrusivePtr<IOperator>& GetRightInput() Y_LIFETIME_BOUND { return GetChild(1); }
    const TIntrusivePtr<IOperator>& GetRightInput() const Y_LIFETIME_BOUND { return GetChild(1); }

    void SetLeftInput(TIntrusivePtr<IOperator> newInput) {
        SetChild(0, std::move(newInput));
    }

    void SetRightInput(TIntrusivePtr<IOperator> newInput) {
        SetChild(1, std::move(newInput));
    }
};

/**
 * Operator with an arbitrary number of inputs. The inputs are the children, in order.
 */
class IVariadicOperator: public IOperator {
public:
    IVariadicOperator(EOperator kind, TPositionHandle pos)
        : IOperator(kind, pos) {
    }

    IVariadicOperator(EOperator kind, TPositionHandle pos, TVector<TIntrusivePtr<IOperator>> inputs)
        : IOperator(kind, pos) {
        Children_ = std::move(inputs);
    }

    TVector<TIntrusivePtr<IOperator>>& GetInputs() Y_LIFETIME_BOUND { return Children_; }
    const TVector<TIntrusivePtr<IOperator>>& GetInputs() const Y_LIFETIME_BOUND { return Children_; }
    TIntrusivePtr<IOperator>& GetInput(size_t index) Y_LIFETIME_BOUND { return GetChild(index); }
    const TIntrusivePtr<IOperator>& GetInput(size_t index) const Y_LIFETIME_BOUND { return GetChild(index); }

    void SetInputs(TVector<TIntrusivePtr<IOperator>> newInputs) {
        Children_ = std::move(newInputs);
    }
};

class TOpReplicate;

// Shared binding, not an operator or materialization. All ports expose this
// same input slot, so rewriting the producer updates every consumer.
class TReplicate final: public TSimpleRefCount<TReplicate> {
public:
    // The plan's registry must outlive this Replicate and all its output ports.
    static TIntrusivePtr<TReplicate> Create(TIntrusivePtr<IOperator> input, TPositionHandle pos, TInfoUnitRegistry& registry);
    TReplicate(const TReplicate&) = delete;
    TReplicate& operator=(const TReplicate&) = delete;

    TIntrusivePtr<TOpReplicate> AddOutput();
    TIntrusivePtr<IOperator>& GetInput() Y_LIFETIME_BOUND { return Input_; }
    const TIntrusivePtr<IOperator>& GetInput() const Y_LIFETIME_BOUND { return Input_; }
    void SetInput(TIntrusivePtr<IOperator> input) {
        Y_ENSURE(input);
        Input_ = std::move(input);
    }
    // Reachable ports, rebuilt together with parent edges. Independent of local
    // references and of producer replacements during the ensuing rewrite.
    const TVector<TOpReplicate*>& GetOutputs() const { return Outputs_; }

    const TPositionHandle Pos;

private:
    friend class TOpReplicate;
    friend class TOpRoot;
    friend class TLogicalCopyContext;

    TReplicate(TIntrusivePtr<IOperator> input, TPositionHandle pos, TInfoUnitRegistry& registry);
    TIntrusivePtr<IOperator> Input_;
    TInfoUnitRegistry* Registry_;
    ui32 NextOutputIndex_ = 0;
    TVector<TOpReplicate*> Outputs_; // Non-owning, like IOperator::Parents.
};

// One row schema per consumer, so ordinary operators retain one output set/type.
// Only AddOutput can create a port; its ordinal and existing bindings never move
// when another port disappears or the producer's output set changes.
class TOpReplicate final: public IUnaryOperator {
public:
    TOpReplicate(const TOpReplicate&) = delete;
    TOpReplicate& operator=(const TOpReplicate&) = delete;

    // Call with current parent edges at a stable logical rewrite boundary.
    // A renamed singleton becomes a copy Map, preserving its consumer IDs.
    static bool TryCollapse(TIntrusivePtr<IOperator>& slot, TExprContext& ctx, TPlanProps& props);

    TReplicate& GetReplicate() Y_LIFETIME_BOUND { return *Replicate_; }
    const TReplicate& GetReplicate() const Y_LIFETIME_BOUND { return *Replicate_; }
    size_t GetChildCount() const override { return 1; }
    TIntrusivePtr<IOperator>& GetChild(size_t index) override {
        Y_ENSURE(index == 0);
        return Replicate_->GetInput();
    }
    const TIntrusivePtr<IOperator>& GetChild(size_t index) const override {
        Y_ENSURE(index == 0);
        return Replicate_->GetInput();
    }
    void BindExpressionPlanProps(TPlanProps* props) override { Replicate_->Registry_ = &props->InfoUnitRegistry; }
    ui32 GetIndex() const { return Index_; }
    bool IsPrimary() const { return Index_ == 0; }

    const TUnorderedIUs& GetOutputIUs() override;
    // Source -> port IDs. Empty for the identity port. May include sources absent
    // from the current input; enumerate GetInput()->GetOutputIUs() for the schema.
    const TMappedIUs<TInfoUnitId>& GetRebindings() Y_LIFETIME_BOUND;
    TUnorderedIUs MapToInput(const TUnorderedIUs& outputIUs);

    // Simultaneously rekey the stored source -> local correspondence. Fresh
    // sources retain their local IDs; coalesced sources return local substitutions.
    // The primary port needs no local substitutions. Do not refresh output sets
    // until both producer and consumers have been rewritten.
    TSubstitutions RebindInputs(const TSubstitutions& substitutions);

    void PropagateLiveness(ILivenessContext& ctx) override;
    void ComputeMetadata(TRBOContext& ctx, TPlanProps& planProps) override;
    void ComputeStatistics(TRBOContext& ctx, TPlanProps& planProps) override;
    TString GetExplainName() const override { return "Replicate"; }
    TString ToString(TExprContext& ctx, const TInfoUnitRegistry& registry) override;

private:
    friend class TReplicate;
    friend class TLogicalCopyContext;

    TOpReplicate(TIntrusivePtr<TReplicate> input, ui32 index);
    void RefreshBindings();

    const TIntrusivePtr<TReplicate> Replicate_;
    const ui32 Index_;
    TMappedIUs<TInfoUnitId> Rebindings_;
    TUnorderedIUs InputIUs_;
};

class TOpEmptySource: public IOperator {
public:
    TOpEmptySource(TPositionHandle pos, TExprNode::TPtr input = nullptr, TUnorderedIUs columns = {})
        : IOperator(EOperator::EmptySource, pos)
        , Input(std::move(input))
        , Columns(std::move(columns)) {
        Y_ENSURE(Input || Columns.Empty(), "A unit source cannot define columns");
    }

    virtual TString ToString(TExprContext& ctx, const TInfoUnitRegistry& registry) override;
    virtual TString GetExplainName() const override { return "EmptySource"; }
    const TUnorderedIUs& GetOutputIUs() override { return Columns; }
    virtual NJson::TJsonValue ToJson(ui32 explainFlags, const TInfoUnitRegistry& registry) override;
    virtual void ComputeMetadata(TRBOContext& ctx, TPlanProps& planProps) override;
    virtual void ComputeStatistics(TRBOContext& ctx, TPlanProps& planProps) override;

    // Represents a custom input, basically it is an external param.
    TExprNode::TPtr Input;

protected:
    TIntrusivePtr<IOperator> CopyImpl(TInfoUnitRegistry& registry, TSubstitutions& renames) const override;

private:
    TUnorderedIUs Columns;
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
        size_t PointPrefixLen = 0;    // how many are pinned to a single value
        TMaybe<size_t> ExpectedMaxRanges;
        TExprNode::TPtr Points;
        const TStructExprType* PointsItemType = nullptr;
        TVector<TString> PointColumns;
        TMaybe<size_t> ExpectedMaxPoints;
    };

    // Fresh definitions, allocated together in source-schema order. Conversion
    // memoization reuses an existing Read rather than calling this factory again.
    static TIntrusivePtr<TOpRead> FromExpr(TExprNode::TPtr node, TInfoUnitRegistry& registry);

    // Reconstruction preserves the supplied bindings; it never allocates IDs.
    TOpRead(const TString& alias, TUnorderedIUs columns, const NYql::EStorageType storageType,
            const TExprNode::TPtr& tableCallable, const TExprNode::TPtr& olapFilterLambda, const TExprNode::TPtr& limit, std::optional<TRangeInfo> ranges,
            const std::optional<TExpression>& originalPredicate, const ESortDir sortDirection, const TPhysicalOpProps& props, TPositionHandle pos);

    const TUnorderedIUs& GetColumns() const Y_LIFETIME_BOUND { return Columns_; }
    TUnorderedIUs& GetColumns() Y_LIFETIME_BOUND { return Columns_; }
    // Columns needed by embedded programs, independently of consumer liveness.
    // Inspect the OLAP program; never rewrite it as part of column pruning.
    TUnorderedIUs GetRequiredColumns(TExprContext& ctx) const;

    // The column set is also the output set; there is no second cache.
    const TUnorderedIUs& GetOutputIUs() override { return Columns_; }

    virtual void PropagateLiveness(ILivenessContext& ctx) override;
    bool PruneOutputs(const TUnorderedIUs& liveOut, TExprContext& ctx) override;
    // Names are resolved from the current registry only for presentation.
    TString ToString(TExprContext& ctx, const TInfoUnitRegistry& registry) override;
    virtual TString GetExplainName() const override { return RangeInfo.has_value() ? "TableRangeScan" : "TableFullScan"; }
    NJson::TJsonValue ToJson(ui32 explainFlags, const TInfoUnitRegistry& registry) override;

    virtual void ComputeMetadata(TRBOContext& ctx, TPlanProps& planProps) override;
    virtual void ComputeStatistics(TRBOContext& ctx, TPlanProps& planProps) override;
    NYql::EStorageType GetTableStorageType() const;

    TExprNode::TPtr GetRanges() const { return RangeInfo ? RangeInfo->ComputeNode : nullptr; }
    TExprNode::TPtr GetTable() const { return TableCallable; }

    // TODO: make it private members, we should not access it directly
    TString Alias;
    NYql::EStorageType StorageType;

    // TODO: put it in read settings.
    TExprNode::TPtr TableCallable;
    // Binding atoms and embedded row-schema fields use decimal IU IDs. The
    // program preserves Columns_ membership, but may change values and types.
    TExprNode::TPtr OlapFilterLambda;
    TExprNode::TPtr Limit;
    // Bound to the pre-program input row, also using IDs.
    std::optional<TExpression> OriginalPredicate;
    ESortDir SortDir{ESortDir::None};
    std::optional<TRangeInfo> RangeInfo;

protected:
    TIntrusivePtr<IOperator> CopyImpl(TInfoUnitRegistry& registry, TSubstitutions& renames) const override;

private:
    // Each ID's registry ColumnName is the physical column to fetch. Multiple
    // IDs may name the same column; internal row fields use their decimal IDs.
    // The OLAP program may change values/types while preserving this column set.
    TUnorderedIUs Columns_;
};

// The output ID is the TMappedIUs key, not duplicated inside the value.
class TMapElement {
public:
    explicit TMapElement(TExpression expr)
        : Expr(std::move(expr))
    {}

    bool IsColumnAccess() const {
        return Expr.IsColumnAccess();
    }
    TInfoUnitId GetColumnAccess() const;
    const TExpression& GetExpression() const Y_LIFETIME_BOUND { return Expr; }
    bool DependsOnlyOn(const TUnorderedIUs& available) const {
        return Expr.GetInputIUs(false, true).IsSubsetOf(available);
    }

private:
    TExpression Expr;
};

struct TMapDependencies {
    const TUnorderedIUs& operator()(const TMapElement& element) const {
        // Raw references depend only on the stored AST, never mutable subplans.
        return element.GetExpression().GetRawInputIUs();
    }
};

using TMapIUs = TMappedIUs<TMapElement, TMapDependencies>;

// Logical Maps only append definitions: every input binding survives.
// Source-AST visibility is an import concern; dead columns are pruned by liveness.
class TOpMap: public IUnaryOperator {
public:
    TOpMap(TIntrusivePtr<IOperator> input, TPositionHandle pos, TMapIUs elements, bool needToPush = false);
    TOpMap(TIntrusivePtr<IOperator> input, TPositionHandle pos, const TPhysicalOpProps& props,
        TMapIUs elements);

    TUnorderedIUs GetUsedIUs(TPlanProps& props) override;
    const TUnorderedIUs& GetUniqueRawInputIUs() const override { return MapElements.MappedIUs(); }
    TVector<std::reference_wrapper<const TExpression>> GetExpressions() const override;
    void PropagateLiveness(ILivenessContext& ctx) override;
    bool PruneOutputs(const TUnorderedIUs& liveOut, TExprContext& ctx) override;
    void ApplyReplaceMap(const TNodeOnNodeOwnedMap& map, TRBOContext& ctx) override;

    void RenameUsedIUs(const TSubstitutions& substitutions) override;
    void ComputeMetadata(TRBOContext& ctx, TPlanProps& planProps) override;
    void ComputeStatistics(TRBOContext& ctx, TPlanProps& planProps) override;
    TString ToString(TExprContext& ctx, const TInfoUnitRegistry& registry) override;
    NJson::TJsonValue ToJson(ui32 explainFlags, const TInfoUnitRegistry& registry) override;
    TString GetExplainName() const override { return "Map"; }

    const TMapIUs& GetMapElements() const Y_LIFETIME_BOUND { return MapElements; }
    void SetMapElements(TMapIUs elements);
    void AddMapElement(TInfoUnitId output, TMapElement element);
    void RemoveMapElement(TInfoUnitId output);
    void SetMapElementExpression(TInfoUnitId output, TExpression expression);
    const TMapElement* FindOutputElement(TInfoUnitId output) const Y_LIFETIME_BOUND { return MapElements.Find(output); }

    bool NeedToPush = false;

protected:
    TIntrusivePtr<IOperator> CopyImpl(TInfoUnitRegistry& registry, TSubstitutions& renames) const override;
    void ComputeOutputIUs() override;

private:
    TMapIUs MapElements;
};

/**
 * OpAddDependencies is a temporary operator to infuse dependencies into a correlated subplan
 * This operator needs to be removed during query decorrelation
 */
struct TCapturedIU {
    TInfoUnitId Outer;
    const TTypeAnnotationNode* Type;
};

struct TCaptureDependencies {
    void Validate(const TCapturedIU& capture) const {
        Y_ENSURE(capture.Type && capture.Outer != TUnorderedIUs::InvalidBit, "Invalid captured IU");
    }

    auto operator()(const TCapturedIU& capture) const {
        return std::views::single(capture.Outer);
    }
};

using TDependencyIUs = TMappedIUs<TCapturedIU, TCaptureDependencies>;

class TOpAddDependencies: public IUnaryOperator {
public:
    TOpAddDependencies(TIntrusivePtr<IOperator> input, TPositionHandle pos, TDependencyIUs dependencies);
    const TDependencyIUs& GetDependencies() const Y_LIFETIME_BOUND { return Dependencies; }
    void SetDependencies(TDependencyIUs dependencies);
    // Rebind parameters, never the local definitions or their consumers.
    bool RebindCaptures(const TSubstitutions& substitutions);
    void RenameUsedIUs(const TSubstitutions& substitutions) override;
    void PropagateLiveness(ILivenessContext& ctx) override;
    TString ToString(TExprContext& ctx, const TInfoUnitRegistry& registry) override;
    TString GetExplainName() const override { return "AddDependencies"; }

protected:
    TIntrusivePtr<IOperator> CopyImpl(TInfoUnitRegistry& registry, TSubstitutions& renames) const override;
    void ComputeOutputIUs() override;

private:
    // Fresh local definition -> outer parameter and its supplied type.
    TDependencyIUs Dependencies;
};

struct TOpAggregationTraits {
    TInfoUnitId Input;
    TString AggFunction;
    bool Distinct = false;
    bool Unwrap = false;
};

struct TAggregationDependencies {
    auto operator()(const TOpAggregationTraits& traits) const {
        return std::views::single(traits.Input);
    }
};

using TAggregationIUs = TMappedIUs<TOpAggregationTraits, TAggregationDependencies>;

class TOpAggregate: public IUnaryOperator {
public:
    TOpAggregate(TIntrusivePtr<IOperator> input, TAggregationIUs aggregations, TOrderedIUs<> keys,
        EOpPhase phase, bool distinctAll, TPositionHandle pos);
    TOpAggregate(TIntrusivePtr<IOperator> input, TAggregationIUs aggregations, TOrderedIUs<> keys,
        EOpPhase phase, bool distinctAll, const TPhysicalOpProps& props, TPositionHandle pos);

    TUnorderedIUs GetUsedIUs(TPlanProps& props) override;
    void PropagateLiveness(ILivenessContext& ctx) override;
    bool PruneOutputs(const TUnorderedIUs& liveOut, TExprContext& ctx) override;
    void RenameUsedIUs(const TSubstitutions& substitutions) override;
    TString ToString(TExprContext& ctx, const TInfoUnitRegistry& registry) override;
    NJson::TJsonValue ToJson(ui32 explainFlags, const TInfoUnitRegistry& registry) override;
    TString GetExplainName() const override { return "Aggregate"; }
    void ComputeMetadata(TRBOContext& ctx, TPlanProps& planProps) override;
    void ComputeStatistics(TRBOContext& ctx, TPlanProps& planProps) override;

    EOpPhase GetAggregationPhase() const { return AggregationPhase; }
    const TAggregationIUs& GetAggregationTraits() const Y_LIFETIME_BOUND { return Aggregations; }
    const TOrderedIUs<>& GetKeyColumns() const Y_LIFETIME_BOUND { return KeyColumns; }
    void SetAggregationTraits(TAggregationIUs aggregations) {
        Aggregations = std::move(aggregations);
        Props.OutputIUs.reset();
    }
    void SetKeyColumns(TOrderedIUs<> keys) {
        KeyColumns = std::move(keys);
        Props.OutputIUs.reset();
    }
    bool IsDistinctAll() const { return DistinctAll; }
    // DISTINCT and grouping-only aggregates just remove duplicate rows.
    bool IsDeduplication() const { return IsDistinctAll() || Aggregations.Keys().Empty(); }

protected:
    TIntrusivePtr<IOperator> CopyImpl(TInfoUnitRegistry& registry, TSubstitutions& renames) const override;
    void ComputeOutputIUs() override;

private:
    TAggregationIUs Aggregations;
    TOrderedIUs<> KeyColumns;
    EOpPhase AggregationPhase;
    bool DistinctAll;
};

class TOpGroupingSets: public IUnaryOperator {
public:
    void PropagateLiveness(ILivenessContext& ctx) override;
    bool PruneOutputs(const TUnorderedIUs& liveOut, TExprContext& ctx) override;
    void RenameUsedIUs(const TSubstitutions& substitutions) override;
    void ComputeMetadata(TRBOContext& ctx, TPlanProps& planProps) override;
    void ComputeOutputIUs() override;
    TOpGroupingSets(TIntrusivePtr<TOpAggregate> input, TVector<TUnorderedIUs> groupingSets,
        TMappedIUs<TInfoUnitId> columns, TPositionHandle pos, TMappedIUs<TInfoUnitId> groupingIndicators = {});

    const TMappedIUs<TInfoUnitId>& GetColumns() const Y_LIFETIME_BOUND { return Columns; }
    const TMappedIUs<TInfoUnitId>& GetGroupingIndicators() const Y_LIFETIME_BOUND { return GroupingIndicators; }
    const TVector<TUnorderedIUs>& GetGroupingSets() const Y_LIFETIME_BOUND {
        return GroupingSets;
    }

    virtual TString ToString(TExprContext& ctx, const TInfoUnitRegistry& registry) override;
    // This op is not present is explain, but we have to define a function, because it's a pure virtual.
    virtual TString GetExplainName() const override { return "GroupingSets"; }

protected:
    TIntrusivePtr<IOperator> CopyImpl(TInfoUnitRegistry& registry, TSubstitutions& renames) const override;

private:
    TVector<TUnorderedIUs> GroupingSets;
    // Fresh output -> Aggregate output. GroupingSets still contains input keys;
    // their outward values may be NULL, so they must not share those bindings.
    TMappedIUs<TInfoUnitId> Columns;
    // Fresh indicator output -> grouping key whose absence it reports.
    TMappedIUs<TInfoUnitId> GroupingIndicators;
};

enum class EWindowFuncKind : ui32 {
    Aggregate,
    Native,
};

TString ToStringWindowFuncKind(EWindowFuncKind kind);
EWindowFuncKind WindowFuncKindFromString(const TString& kind);

struct TOpWindowFunc {
    TString Function;
    EWindowFuncKind Kind = EWindowFuncKind::Aggregate;
    TOrderedIUs<> Arguments;
};

struct TWindowDependencies {
    const auto& operator()(const TOpWindowFunc& function) const {
        return function.Arguments.Items();
    }
};
using TWindowIUs = TMappedIUs<TOpWindowFunc, TWindowDependencies>;

enum class EWindowFrameType : ui32 {
    Rows,
    Range,
    Groups,
};

enum class EWindowFrameBound : ui32 {
    UnboundedPreceding,
    Preceding,
    CurrentRow,
    Following,
    UnboundedFollowing,
};

TString ToStringWindowFrameType(EWindowFrameType type);
EWindowFrameType WindowFrameTypeFromString(const TString& type);
TString ToStringWindowFrameBound(EWindowFrameBound bound);
EWindowFrameBound WindowFrameBoundFromString(const TString& bound);

struct TOpWindowFrame {
    EWindowFrameType Type = EWindowFrameType::Rows;
    EWindowFrameBound BeginKind = EWindowFrameBound::UnboundedPreceding;
    ui64 BeginValue = 0;
    EWindowFrameBound EndKind = EWindowFrameBound::CurrentRow;
    ui64 EndValue = 0;

    bool IsPrefixFrame() const {
        return EndKind == EWindowFrameBound::CurrentRow ||
               (EndKind == EWindowFrameBound::Preceding) ||
               (EndKind == EWindowFrameBound::Following && EndValue == 0);
    }

    bool IsWholePartition() const {
        return (Type == EWindowFrameType::Rows || Type == EWindowFrameType::Range) &&
               BeginKind == EWindowFrameBound::UnboundedPreceding && EndKind == EWindowFrameBound::UnboundedFollowing;
    }
};

// Represents a window function.
class TOpWindow: public IUnaryOperator {
public:
    TOpWindow(TIntrusivePtr<IOperator> input, TPositionHandle pos, TWindowIUs functions,
        TOrderedIUs<> partitionKeys, TSortIUs sortKeys, const TOpWindowFrame& frame);

    TUnorderedIUs GetUsedIUs(TPlanProps& props) override;
    void PropagateLiveness(ILivenessContext& ctx) override;
    void RenameUsedIUs(const TSubstitutions& substitutions) override;
    TString ToString(TExprContext& ctx, const TInfoUnitRegistry& registry) override;
    NJson::TJsonValue ToJson(ui32 explainFlags, const TInfoUnitRegistry& registry) override;
    TString GetExplainName() const override { return "Window"; }

    const TWindowIUs& GetWindowFuncs() const Y_LIFETIME_BOUND { return WindowFuncs; }
    const TOrderedIUs<>& GetPartitionKeys() const Y_LIFETIME_BOUND { return PartitionKeys; }
    const TSortIUs& GetSortElements() const Y_LIFETIME_BOUND { return SortElements; }
    const TOpWindowFrame& GetFrame() const Y_LIFETIME_BOUND { return Frame; }
    void SetWindowFuncs(TWindowIUs functions) {
        WindowFuncs = std::move(functions);
        Props.OutputIUs.reset();
    }
    void SetPartitionKeys(TOrderedIUs<> keys) { PartitionKeys = std::move(keys); }
    void SetSortElements(TSortIUs keys) { SortElements = std::move(keys); }

protected:
    TIntrusivePtr<IOperator> CopyImpl(TInfoUnitRegistry& registry, TSubstitutions& renames) const override;
    void ComputeOutputIUs() override;

private:
    TWindowIUs WindowFuncs;
    TOrderedIUs<> PartitionKeys;
    TSortIUs SortElements;
    TOpWindowFrame Frame;
};

class TOpFilter: public IUnaryOperator {
public:
    TOpFilter(TIntrusivePtr<IOperator> input, TPositionHandle pos, const TExpression& filterExpr);
    TOpFilter(TIntrusivePtr<IOperator> input, TPositionHandle pos, const TPhysicalOpProps& props, const TExpression& filterExpr, bool partiallyPushedDown = false);

    // Pass-through outputs and dependencies share their existing storage.
    const TUnorderedIUs& GetOutputIUs() override { return GetInput()->GetOutputIUs(); }
    TUnorderedIUs GetUsedIUs(TPlanProps& props) override;
    const TUnorderedIUs& GetUniqueRawInputIUs() const override { return FilterExpr.GetRawInputIUs(); }
    virtual TString ToString(TExprContext& ctx, const TInfoUnitRegistry& registry) override;
    virtual NJson::TJsonValue ToJson(ui32 explainFlags, const TInfoUnitRegistry& registry) override;
    virtual TString GetExplainName() const override { return "Filter"; }

    virtual TVector<std::reference_wrapper<const TExpression>> GetExpressions() const override;
    virtual void PropagateLiveness(ILivenessContext& ctx) override;
    virtual void ApplyReplaceMap(const TNodeOnNodeOwnedMap& map, TRBOContext& ctx) override;

    TUnorderedIUs GetFilterIUs(TPlanProps& props) const;
    void RenameUsedIUs(const TSubstitutions& substitutions) override;

    virtual void ComputeMetadata(TRBOContext& ctx, TPlanProps& planProps) override;
    virtual void ComputeStatistics(TRBOContext& ctx, TPlanProps& planProps) override;
    const TExpression& GetFilterExpression() const Y_LIFETIME_BOUND { return FilterExpr; }
    void SetFilterExpression(TExpression filterExpr);

    bool PartiallyPushedDown = false;

protected:
    TIntrusivePtr<IOperator> CopyImpl(TInfoUnitRegistry& registry, TSubstitutions& renames) const override;

private:
    TExpression FilterExpr;
};

bool TestAndExtractEqualityPredicate(TExprNode::TPtr pred, TExprNode::TPtr& leftArg, TExprNode::TPtr& rightArg);

// Equality semantics travel with the ID pair through every join rewrite.
struct TJoinKey {
    TInfoUnitId first;
    TInfoUnitId second;
    bool EqualNulls = false;

    TJoinKey(TInfoUnitId left, TInfoUnitId right, bool equalNulls = false)
        : first(left), second(right), EqualNulls(equalNulls) {}
    TJoinKey(const std::pair<TInfoUnitId, TInfoUnitId>& pair)
        : TJoinKey(pair.first, pair.second) {}
    auto operator<=>(const TJoinKey&) const = default;
};

using TJoinIUs = TPairedIUCollection<TJoinKey>;

inline bool HasEqualNullsKey(const TJoinIUs& keys) {
    return std::ranges::any_of(keys.Items(), [](const auto& key) { return key.EqualNulls; });
}

class TOpJoin: public IBinaryOperator {
public:
    TOpJoin(TIntrusivePtr<IOperator> leftArg, TIntrusivePtr<IOperator> rightArg, TPositionHandle pos, TString joinKind,
            TJoinIUs joinKeys);

    TOpJoin(TIntrusivePtr<IOperator> leftArg, TIntrusivePtr<IOperator> rightArg, TPositionHandle pos, TString joinKind,
            TJoinIUs joinKeys, const TVector<TExpression>& joinFilters);

    const TUnorderedIUs& GetUniqueRawInputIUs() const override;
    virtual TUnorderedIUs GetUsedIUs(TPlanProps& props) override;
    virtual TVector<std::reference_wrapper<const TExpression>> GetExpressions() const override;
    virtual void PropagateLiveness(ILivenessContext& ctx) override;

    void RenameUsedIUs(const TSubstitutions& substitutions) override;
    virtual TString ToString(TExprContext& ctx, const TInfoUnitRegistry& registry) override;
    virtual NJson::TJsonValue ToJson(ui32 explainFlags, const TInfoUnitRegistry& registry) override;
    virtual TString GetExplainName() const override { return "Join"; }

    virtual void ComputeMetadata(TRBOContext& ctx, TPlanProps& planProps) override;
    virtual void ComputeStatistics(TRBOContext& ctx, TPlanProps& planProps) override;

    const TUnorderedIUs& GetLHSKeys() const Y_LIFETIME_BOUND { return JoinKeys.Left(); }
    const TUnorderedIUs& GetRHSKeys() const Y_LIFETIME_BOUND { return JoinKeys.Right(); }

    TString JoinKind;
    TJoinIUs JoinKeys;
    TVector<TExpression> JoinFilters;

protected:
    TIntrusivePtr<IOperator> CopyImpl(TInfoUnitRegistry& registry, TSubstitutions& renames) const override;
    void ComputeOutputIUs() override;
private:
    mutable TUnorderedIUs RawInputIUs;

};

/**
 * Dependent join based on Neumann "Unnesting Arbitrary Queries".
 *
 */
class TOpDependentJoin: public IBinaryOperator {
public:
    void RenameUsedIUs(const TSubstitutions& substitutions) override;
    TOpDependentJoin(TIntrusivePtr<IOperator> domain, TIntrusivePtr<IOperator> input, TUnorderedIUs dependencies, TPositionHandle pos,
        TSubstitutions domainColumns = {});

    virtual void PropagateLiveness(ILivenessContext& ctx) override;

    virtual void ComputeMetadata(TRBOContext& ctx, TPlanProps& planProps) override;
    virtual void ComputeStatistics(TRBOContext& ctx, TPlanProps& planProps) override;

    virtual TString ToString(TExprContext& ctx, const TInfoUnitRegistry& registry) override;
    virtual TString GetExplainName() const override { return "DependentJoin"; }

    TIntrusivePtr<IOperator>& GetDomain() Y_LIFETIME_BOUND { return GetLeftInput(); }
    const TIntrusivePtr<IOperator>& GetDomain() const Y_LIFETIME_BOUND { return GetLeftInput(); }
    TIntrusivePtr<IOperator>& GetInput() Y_LIFETIME_BOUND { return GetRightInput(); }
    const TIntrusivePtr<IOperator>& GetInput() const Y_LIFETIME_BOUND { return GetRightInput(); }

    void SetInput(TIntrusivePtr<IOperator> newInput) {
        SetRightInput(std::move(newInput));
    }

    // The domain column carrying a parameter. A copy of a shared domain carries
    // the parameters in its own columns.
    TInfoUnitId GetDomainColumn(TInfoUnitId parameter) const { return Substitute(parameter, DomainColumns); }
    TUnorderedIUs GetDomainColumns() const;

    TUnorderedIUs Dependencies;
    TSubstitutions DomainColumns;

protected:
    TIntrusivePtr<IOperator> CopyImpl(TInfoUnitRegistry& registry, TSubstitutions& renames) const override;
    void ComputeOutputIUs() override;
};

class TOpUnionAll: public IVariadicOperator {
public:
    void RenameUsedIUs(const TSubstitutions& substitutions) override;
    TOpUnionAll(TVector<TIntrusivePtr<IOperator>> inputs, TPositionHandle pos, TUnionAllIUs columns, bool ordered = false);
    TOpUnionAll(TIntrusivePtr<IOperator> left, TIntrusivePtr<IOperator> right, TPositionHandle pos,
        TUnionAllIUs columns, bool ordered = false);

    const TUnorderedIUs& GetOutputIUs() override { return Columns.Keys(); }
    const TUnionAllIUs& GetColumns() const Y_LIFETIME_BOUND { return Columns; }
    void SetColumns(TUnionAllIUs columns) {
        Y_ENSURE(columns.Policy().ChildCount == GetChildCount());
        Columns = std::move(columns);
    }
    void PropagateLiveness(ILivenessContext& ctx) override;
    bool PruneOutputs(const TUnorderedIUs& liveOut, TExprContext& ctx) override;
    TString ToString(TExprContext& ctx, const TInfoUnitRegistry& registry) override;
    NJson::TJsonValue ToJson(ui32 explainFlags, const TInfoUnitRegistry& registry) override;
    TString GetExplainName() const override { return "UnionAll"; }
    void ComputeMetadata(TRBOContext& ctx, TPlanProps& planProps) override;
    void ComputeStatistics(TRBOContext& ctx, TPlanProps& planProps) override;

    bool Ordered;

protected:
    TIntrusivePtr<IOperator> CopyImpl(TInfoUnitRegistry& registry, TSubstitutions& renames) const override;

private:
    TUnionAllIUs Columns;
};

class TOpLimit: public IUnaryOperator {
public:
    const TUnorderedIUs& GetOutputIUs() override { return GetInput()->GetOutputIUs(); }
    TOpLimit(TIntrusivePtr<IOperator> input, TPositionHandle pos, const TExpression& limitCond, const EOpPhase limitPhase);
    TOpLimit(TIntrusivePtr<IOperator> input, TPositionHandle pos, const TExpression& limitCond, const TExpression& offsetCond, const EOpPhase limitPhase);
    TOpLimit(TIntrusivePtr<IOperator> input, TPositionHandle pos, const TPhysicalOpProps& props, const TExpression& limitCond, const EOpPhase limitPhase);
    TOpLimit(TIntrusivePtr<IOperator> input, TPositionHandle pos, const TPhysicalOpProps& props, const TExpression& limitCond,
             std::optional<TExpression> offsetCond, const EOpPhase limitPhase);

    const TUnorderedIUs& GetUniqueRawInputIUs() const override;
    virtual TUnorderedIUs GetUsedIUs(TPlanProps& props) override;
    virtual void PropagateLiveness(ILivenessContext& ctx) override;
    void RenameUsedIUs(const TSubstitutions& substitutions) override;
    virtual TString ToString(TExprContext& ctx, const TInfoUnitRegistry& registry) override;
    virtual NJson::TJsonValue ToJson(ui32 explainFlags, const TInfoUnitRegistry& registry) override;
    virtual TString GetExplainName() const override { return "Limit"; }

    virtual TVector<std::reference_wrapper<const TExpression>> GetExpressions() const override;

    EOpPhase GetLimitPhase() const {
        return LimitPhase;
    }

    TExpression GetLimitCond() const { return LimitCond; }
    bool HasOffset() const { return OffsetCond.has_value(); }
    std::optional<TExpression> GetOffsetCond() const { return OffsetCond; }

    // Make private.
    TExpression LimitCond;

protected:
    TIntrusivePtr<IOperator> CopyImpl(TInfoUnitRegistry& registry, TSubstitutions& renames) const override;

private:
    std::optional<TExpression> OffsetCond;
    EOpPhase LimitPhase{EOpPhase::Undefined};
private:
    mutable TUnorderedIUs RawInputIUs;

};

class TOpSort: public IUnaryOperator {
public:
    const TUnorderedIUs& GetOutputIUs() override { return GetInput()->GetOutputIUs(); }
    TOpSort(TIntrusivePtr<IOperator> input, TPositionHandle pos, TSortIUs keys,
        std::optional<TExpression> limit = std::nullopt);
    TOpSort(TIntrusivePtr<IOperator> input, TPositionHandle pos, const TPhysicalOpProps& props,
        TSortIUs keys, std::optional<TExpression> limit, EOpPhase phase);

    const TUnorderedIUs& GetUniqueRawInputIUs() const override;
    TUnorderedIUs GetUsedIUs(TPlanProps& props) override;
    TVector<std::reference_wrapper<const TExpression>> GetExpressions() const override;
    void PropagateLiveness(ILivenessContext& ctx) override;
    void RenameUsedIUs(const TSubstitutions& substitutions) override;
    TString ToString(TExprContext& ctx, const TInfoUnitRegistry& registry) override;
    NJson::TJsonValue ToJson(ui32 explainFlags, const TInfoUnitRegistry& registry) override;
    EOpPhase GetSortPhase() const { return SortPhase; }
    void SetSortPhase(EOpPhase phase) { SortPhase = phase; }
    const TSortIUs& GetSortElements() const Y_LIFETIME_BOUND { return SortElements; }
    void SetSortElements(TSortIUs keys) { SortElements = std::move(keys); }
    bool IsTopSort() const { return LimitCond.has_value(); }
    TString GetExplainName() const override { return IsTopSort() ? "TopSort" : "Sort"; }

    std::optional<TExpression> LimitCond;

protected:
    TIntrusivePtr<IOperator> CopyImpl(TInfoUnitRegistry& registry, TSubstitutions& renames) const override;

private:
    TSortIUs SortElements;
    EOpPhase SortPhase{EOpPhase::Undefined};
};

enum class ELookupStrategy : ui32 {
    LookupRows,
    LookupJoinRows,
};

class TOpTableLookup: public IUnaryOperator {
public:
    void RenameUsedIUs(const TSubstitutions& substitutions) override;
    // Input IDs paired with storage key names in physical key-prefix order.
    using TLookupKeys = TOrderedIUs<TString>;
    struct TLookupKeyPrefix {
        TExprNode::TPtr Points;
        const TStructExprType* PointsItemType = nullptr;
        TVector<TString> Columns; // Storage fields of the points tuple.
        TLookupKeys Equalities;
    };

    TOpTableLookup(TIntrusivePtr<IOperator> input, TPositionHandle pos, const TExprNode::TPtr& table,
        TUnorderedIUs columns, TLookupKeys keys);
    TOpTableLookup(TIntrusivePtr<IOperator> input, TPositionHandle pos, const TExprNode::TPtr& table,
        TUnorderedIUs columns, TLookupKeys keys, const TString& kind,
        const std::optional<TExpression>& filter, const std::optional<TLookupKeyPrefix>& prefix = std::nullopt,
        TJoinIUs residualKeys = {});

    const TUnorderedIUs& GetUniqueRawInputIUs() const override;
    TUnorderedIUs GetUsedIUs(TPlanProps& props) override;
    TVector<std::reference_wrapper<const TExpression>> GetExpressions() const override;
    void PropagateLiveness(ILivenessContext& ctx) override;
    TString ToString(TExprContext& ctx, const TInfoUnitRegistry& registry) override;
    NJson::TJsonValue ToJson(ui32 explainFlags, const TInfoUnitRegistry& registry) override;
    TString GetExplainName() const override { return IsJoin() ? "TableLookupJoin" : "TableLookup"; }
    void ComputeMetadata(TRBOContext& ctx, TPlanProps& planProps) override;
    bool IsJoin() const { return Strategy == ELookupStrategy::LookupJoinRows; }

    const TUnorderedIUs& GetColumns() const Y_LIFETIME_BOUND { return Columns; }
    void SetColumns(TUnorderedIUs columns) {
        Columns = std::move(columns);
        Props.OutputIUs.reset();
    }
    TExprNode::TPtr Table;
    TLookupKeys LookupKeys;
    TString JoinKind;
    std::optional<TExpression> FetchedRowFilter;
    std::optional<TLookupKeyPrefix> Prefix;
    ELookupStrategy Strategy{ELookupStrategy::LookupRows};
    TJoinIUs ResidualJoinKeys;

protected:
    void ComputeOutputIUs() override;

private:
    // As with Read, registry ColumnName supplies the storage name for each ID.
    TUnorderedIUs Columns;
};

/***
 * Logical representation of index lookup join. In runtime it conusmes a tuple (left row, optional<right row>, cookie).
 * Where cookie is (left row id, first row, last row).
 ***/
class TOpIndexLookupJoin: public IUnaryOperator {
public:
    void RenameUsedIUs(const TSubstitutions& substitutions) override;
    TOpIndexLookupJoin(TIntrusivePtr<IOperator> input, TPositionHandle pos, const TString& joinKind, TJoinIUs joinKeys);
    void PropagateLiveness(ILivenessContext& ctx) override;

    virtual TString ToString(TExprContext& ctx, const TInfoUnitRegistry& registry) override;
    virtual NJson::TJsonValue ToJson(ui32 explainFlags, const TInfoUnitRegistry& registry) override;
    virtual TString GetExplainName() const override { return "IndexLookupJoin"; }

    TOpTableLookup& GetTableLookup() Y_LIFETIME_BOUND;
    const TOpTableLookup& GetTableLookup() const Y_LIFETIME_BOUND;
    const TUnorderedIUs& GetOutputIUs() override;

    TString JoinKind;
    TJoinIUs JoinKeys;
};

/***
 * This operator packages a subtree of operators in order to pass them to dynamic programming optimizer
 * Currently it requires that the list of operators TreeNodes is in a post-order traversal of the tree
 *
 * TreeRoot and TreeNodes retain the packed subtree. The child view exposes
 * only boundary inputs outside TreeNodes:
 *
 *     JoinAB       TreeNodes = [JoinAB]
 *     /   \        Children  = [A, B]
 *    A     B
 *
 * RebuildChildren records the actual owning slots of those boundary edges.
 * Updating a boundary input forwards to its slot in the packed tree.
 *
 * No validation is currently used
 */
class TOpCBOTree: public IOperator {
public:
    TOpCBOTree(TIntrusivePtr<IOperator> treeRoot, TPositionHandle pos);
    TOpCBOTree(TIntrusivePtr<IOperator> treeRoot, TVector<TIntrusivePtr<IOperator>> treeNodes, TPositionHandle pos);
    const TUnorderedIUs& GetOutputIUs() override { return TreeRoot->GetOutputIUs(); }
    size_t GetChildCount() const override { return BoundaryInputs_.size(); }
    TIntrusivePtr<IOperator>& GetChild(size_t index) override;
    const TIntrusivePtr<IOperator>& GetChild(size_t index) const override;

    virtual void PropagateLiveness(ILivenessContext& ctx) override;
    virtual TString ToString(TExprContext& ctx, const TInfoUnitRegistry& registry) override;
    virtual TString GetExplainName() const override { return "CBOTree"; }


    virtual void ComputeMetadata(TRBOContext& ctx, TPlanProps& planProps) override;
    virtual void ComputeStatistics(TRBOContext& ctx, TPlanProps& planProps) override;

    TIntrusivePtr<IOperator> TreeRoot;
    TVector<TIntrusivePtr<IOperator>> TreeNodes;

protected:
    void ComputeOutputIUs() override;

private:
    void RebuildChildren();
    TVector<std::pair<IOperator*, size_t>> BoundaryInputs_;
};

// Table Effects operator inserts/updates/deletes rows based on input tuples

enum class EEffectType : ui32 {
    InsertRows,
    InsertRowsIndex,
    UpdateRows,
    UpdateRowsIndex,
    UpsertRows,
    UpsertRowsIndex,
    DeleteRows,
    DeleteRowsIndex
};

struct TEffectOptions {
    std::optional<TVector<TString>> Columns;
    std::optional<TVector<TString>> ReturningColumns;
    std::optional<TVector<TString>> DefaultColumns;
    std::optional<TString> OnConflict;
    std::optional<bool> IsBatch;
    std::optional<TVector<TExprNode::TPtr>> Settings;
};

class TOpTableEffect: public IUnaryOperator {
public:
    TOpTableEffect(TIntrusivePtr<IOperator> input, TPositionHandle pos, TExprNode::TPtr table,
        EEffectType type, TEffectOptions options, TOrderedIUs<TString> columns, TOrderedIUs<TString> returning);
    TString GetExplainName() const override;
    TUnorderedIUs GetUsedIUs(TPlanProps& props) override;
    const TUnorderedIUs& GetOutputIUs() override { return ReturningColumns_.Unordered(); }
    const TOrderedIUs<TString>& GetColumns() const Y_LIFETIME_BOUND { return Columns_; }
    const TOrderedIUs<TString>& GetReturningColumns() const Y_LIFETIME_BOUND { return ReturningColumns_; }
    void RenameUsedIUs(const TSubstitutions& substitutions) override;
    void PropagateLiveness(ILivenessContext& ctx) override;
    TString ToString(TExprContext& ctx, const TInfoUnitRegistry& registry) override;
    TExprNode::TPtr BuildSettings(TExprContext& ctx);

    TExprNode::TPtr Table;
    EEffectType EffectType;
    const TEffectOptions Options; // Storage settings; these names are not IU identity.

protected:
    TIntrusivePtr<IOperator> CopyImpl(TInfoUnitRegistry& registry, TSubstitutions& renames) const override;

private:
    // Target names and positions are fixed; only the input bindings can change.
    TOrderedIUs<TString> Columns_;
    const TOrderedIUs<TString> ReturningColumns_;
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
 * engine does). A TOpTraversal snapshot can survive insertions/moves that keep
 * every recorded operator alive, but does not extend operator lifetimes.
 */
struct TOpIterator {
    struct TIteratorItem {
        TIteratorItem() = default;

        TIteratorItem(IOperator* curr, IOperator* parent, size_t idx, std::optional<TInfoUnitId> subplanIU)
            : Current(curr)
            , Parent(parent)
            , ChildIndex(idx)
            , SubplanIU(subplanIU) {
        }

        IOperator* Current = nullptr;
        // Parent/ChildIndex/SubplanIU describe the traversal edge along which the
        // node was first reached — a property of this traversal, not of the graph.
        // A shared (DAG) node may be reached via a different parent under a
        // different traversal order; use IOperator::Parents for graph structure.
        IOperator* Parent = nullptr;
        size_t ChildIndex = 0;
        std::optional<TInfoUnitId> SubplanIU;
    };

private:
    struct TFrame {
        TFrame(IOperator* current, IOperator* parent, size_t childIdx, std::optional<TInfoUnitId> subplanIU)
            : Current(current)
            , Parent(parent)
            , ChildIndex(childIdx)
            , SubplanIU(subplanIU) {
        }

        TFrame(const TFrame&) = delete;
        TFrame& operator=(const TFrame&) = delete;
        TFrame(TFrame&&) noexcept = default;
        TFrame& operator=(TFrame&&) noexcept = default;

        IOperator* Current;
        IOperator* Parent;
        size_t ChildIndex = 0;
        std::optional<TInfoUnitId> SubplanIU;
        // Subplan references, copied on entry: descendants' rules may rebuild
        // this operator's dependency cache before the frame resumes.
        std::optional<absl::InlinedVector<TInfoUnitId, 2>> SubplanIUs;
        size_t NextSubplanIU = 0;
        size_t NextChildIdx = 0;
        // Pre-order only: the node was already emitted when its frame was entered
        bool Emitted = false;
    };

    // Only the range factories construct iterators
    friend class TOpRange;

    TOpIterator(IOperator* op, TPlanProps* props, bool followSubplans, ETraversalOrder order);

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
    bool PushFrame(IOperator* op, IOperator* parent, size_t childIdx, std::optional<TInfoUnitId> subplanIU);
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
    friend TOpRange IterateSubtree(IOperator* op, ETraversalOrder order);
    friend TOpRange IterateSubtreeWithSubplans(IOperator* op, TPlanProps& props, ETraversalOrder order);

    TOpRange(IOperator* op, TPlanProps* props, bool followSubplans, ETraversalOrder order)
        : Op(op)
        , Props(props)
        , FollowSubplans(followSubplans)
        , Order(order) {
    }

    IOperator* Op;
    TPlanProps* Props = nullptr;
    bool FollowSubplans = false;
    ETraversalOrder Order;
};

// Traverse the operators of a single plan, without following subplan references
inline TOpRange IterateSubtree(IOperator* op, ETraversalOrder order = ETraversalOrder::PostOrder) {
    return TOpRange(op, nullptr, false, order);
}

// Traverse an operator subtree, recursing into subplans referenced by expressions
inline TOpRange IterateSubtreeWithSubplans(IOperator* op, TPlanProps& props, ETraversalOrder order = ETraversalOrder::PostOrder) {
    return TOpRange(op, &props, true, order);
}

/**
 * Traversal snapshot: drains a lazy iterator into a vector, so it is
 * guaranteed to emit the same sequence as the lazy traversal it was built from.
 * Supports reverse iteration and multiple passes. Entries are borrowed: only
 * non-destructive edits (e.g. inserting a parent) may occur while using it.
 * Removing an operator invalidates its entries, including recorded parents.
 * Obtain one from TOpRoot::SnapshotTraversal.
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
    TOpRoot(TIntrusivePtr<IOperator> input, TPositionHandle pos, TOrderedIUs<TString> columns,
        TVector<TString> queryColumns = {});
    const TUnorderedIUs& GetOutputIUs() override { return Columns_.Unordered(); }
    const TOrderedIUs<TString>& GetColumns() const Y_LIFETIME_BOUND { return Columns_; }
    const TVector<TString>& GetQueryColumns() const Y_LIFETIME_BOUND { return QueryColumns_; }
    void RenameUsedIUs(const TSubstitutions& substitutions) override;
    virtual TString ToString(TExprContext& ctx, const TInfoUnitRegistry& registry) override;
    virtual TString GetExplainName() const override { return "Root"; }

    void ComputeParents();
    IGraphTransformer::TStatus ComputeTypes(TRBOContext& ctx);

    TString PlanToString(TExprContext& ctx, ui32 printOptions = 0x0);
    void PlanToStringRec(IOperator* op, TExprContext& ctx, TStringBuilder& builder, int ntabs, ui32 printOptions = 0x0) const;

    void ComputePlanMetadata(TRBOContext& ctx);
    void ComputePlanStatistics(TRBOContext& ctx);
    void RecomputeOutputIUsSubtree();

    // Lazy traversal of the whole plan, following subplan references
    TOpRange Iterate(ETraversalOrder order) {
        return TOpRange(GetInput().Get(), &PlanProps, true, order);
    }

    // Default root traversal preserves the historical dependency-first order.
    TOpIterator begin() {
        return Iterate(ETraversalOrder::PostOrder).begin();
    }

    TOpEnd end() const {
        return {};
    }

    // Snapshot of the whole-plan traversal, following subplan references;
    // supports reverse iteration, but does not keep removed operators alive.
    TOpTraversal SnapshotTraversal(ETraversalOrder order = ETraversalOrder::PostOrder) {
        return TOpTraversal(Iterate(order).begin());
    }

    NJson::TJsonValue GetExecutionJson(ui64 & nodeCounter, ui32& operatorIdx, THashMap<IOperator*, ui32>& operatorIds, ui32 explainFlags = 0x00);
    NJson::TJsonValue GetExplainJson(ui64 & nodeCounter, const THashMap<IOperator*, ui32>& operatorIds, ui32 explainFlags = 0x00);

    TPlanProps PlanProps;
    TExprNode::TPtr Node;

private:
    // External labels, order and arity never change. Copy elimination may
    // redirect an entry to an equivalent input ID, including repeated IDs.
    TOrderedIUs<TString> Columns_;
    // YQL's optional result hints are distinct from the actual output schema.
    // In particular, an empty hint list must remain empty at export.
    const TVector<TString> QueryColumns_;

protected:
    void ComputeOutputIUsSubtree() override;

    friend void EnsureRequiredProps(TOpRoot& root, ui32 props, ui32& computedProps, TRBOContext& ctx, const TString& stageName);
};

// Call only after releasing borrowed traversals: bypass empty append-only
// operators, invalidate derived properties and refresh output membership.
void FinishLogicalRewrite(TOpRoot& root, TExprContext& ctx);

// Redirect uses of equivalent outputs above a replaced logical subtree, not
// definitions or uses below it. Substitutions are simultaneous; types/values
// must be preserved. Requires current Parents, before CBO/physical assignment.
// Keep oldRoot in its owning slot until this returns; its children may already
// have moved. Then install the replacement before querying the plan. Owning-slot
// rules can retain this boundary without a new rule API.
void RebindConsumers(IOperator& oldRoot, const TSubstitutions& substitutions, TSubplans& subplans);

} // namespace NKqp
} // namespace NKikimr

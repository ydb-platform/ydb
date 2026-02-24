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
#include <yql/essentials/core/yql_cost_function.h>

namespace NKikimr {
namespace NKqp {

using namespace NYql;

enum EOperator : ui32 { EmptySource, Source, Map, AddDependencies, Project, Filter, Join, Aggregate, Limit, Sort, UnionAll, CBOTree, Root };

/* Represents aggregation phases. */
enum EAggregationPhase : ui32 { Intermediate, Final };

// clang-format off
enum EPrintPlanOptions: ui32 {
    PrintBasicMetadata = 0x01,
    PrintFullMetadata = 0x02,
    PrintBasicStatistics = 0x04,
    PrintFullStatistics = 0x08
};
// clang-format on

enum EOrderEnforcerAction : ui32 { REQUIRE, MAINTAIN };
enum EOrderEnforcerReason : ui32 { USER, INTERNAL };

struct TOrderEnforcer {
    EOrderEnforcerAction Action;
    EOrderEnforcerReason Reason;
    TVector<TSortElement> SortElements;
};

/**
 * Per-operator physical plan properties
 * TODO: Make this more generic and extendable
 */
struct TPhysicalOpProps {
    std::optional<int> StageId;
    std::optional<TString> Algorithm;
    std::optional<TOrderEnforcer> OrderEnforcer;
    std::optional<ui32> NumOfConsumers;
    bool EnsureAtMostOne = false;

    std::optional<TRBOMetadata> Metadata;
    std::optional<TRBOStatistics> Statistics;
    std::optional<EJoinAlgoType> JoinAlgo;
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

    virtual TString ToString(TExprContext& ctx) = 0;

    bool IsSingleConsumer() {
        return Parents.size() <= 1;
    }

    const TTypeAnnotationNode* GetTypeAnn() {
        return Type;
    }

    const EOperator Kind;
    TPhysicalOpProps Props;
    TPositionHandle Pos;
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

    virtual void ComputeMetadata(TRBOContext& ctx, TPlanProps& planProps) override;
    virtual void ComputeStatistics(TRBOContext& ctx, TPlanProps& planProps) override;
};

class TOpRead: public IOperator {
public:
    TOpRead(TExprNode::TPtr node);
    TOpRead(const TString& alias, const TVector<TString>& columns, const TVector<TInfoUnit>& outputIUs, const NYql::EStorageType storageType,
            const TExprNode::TPtr& tableCallable, const TExprNode::TPtr& olapFilterLambda, TPositionHandle pos);

    virtual TVector<TInfoUnit> GetOutputIUs() override;
    virtual TString ToString(TExprContext& ctx) override;
    void RenameIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap, TExprContext& ctx,
                   const THashSet<TInfoUnit, TInfoUnit::THashFunction>& stopList = {}) override;
    bool NeedsMap();

    virtual void ComputeMetadata(TRBOContext& ctx, TPlanProps& planProps) override;
    virtual void ComputeStatistics(TRBOContext& ctx, TPlanProps& planProps) override;
    NYql::EStorageType GetTableStorageType() const;

    // TODO: make it private members, we should not access it directly
    TString Alias;
    TVector<TString> Columns;
    TVector<TInfoUnit> OutputIUs;
    NYql::EStorageType StorageType;
    TExprNode::TPtr TableCallable;
    TExprNode::TPtr OlapFilterLambda;
};

class TMapElement {
public:
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
    bool IsOrdered() const {
        return Ordered;
    }

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
    virtual TVector<TInfoUnit> GetOutputIUs() override;
    virtual TString ToString(TExprContext& ctx) override;

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
                 const EAggregationPhase aggPhase, bool distinctAll, TPositionHandle pos);
    virtual TVector<TInfoUnit> GetOutputIUs() override;
    virtual TVector<TInfoUnit> GetUsedIUs(TPlanProps& props) override;

    void RenameIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap, TExprContext& ctx,
                   const THashSet<TInfoUnit, TInfoUnit::THashFunction>& stopList = {}) override;
    virtual TString ToString(TExprContext& ctx) override;

    virtual void ComputeMetadata(TRBOContext& ctx, TPlanProps& planProps) override;
    virtual void ComputeStatistics(TRBOContext& ctx, TPlanProps& planProps) override;

    TVector<TOpAggregationTraits> AggregationTraitsList;
    TVector<TInfoUnit> KeyColumns;
    EAggregationPhase AggregationPhase;
    bool DistinctAll;
};

class TOpFilter: public IUnaryOperator {
public:
    TOpFilter(TIntrusivePtr<IOperator> input, TPositionHandle pos, const TExpression& filterExpr);

    virtual TVector<TInfoUnit> GetOutputIUs() override;
    virtual TVector<TInfoUnit> GetUsedIUs(TPlanProps& props) override;
    virtual TVector<TInfoUnit> GetSubplanIUs(TPlanProps& props) override;
    virtual TString ToString(TExprContext& ctx) override;
    virtual TVector<std::reference_wrapper<TExpression>> GetExpressions() override;
    virtual void ApplyReplaceMap(const TNodeOnNodeOwnedMap& map, TRBOContext& ctx) override;

    TVector<TInfoUnit> GetFilterIUs(TPlanProps& props) const;
    void RenameIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap, TExprContext& ctx,
                   const THashSet<TInfoUnit, TInfoUnit::THashFunction>& stopList = {}) override;

    virtual void ComputeStatistics(TRBOContext& ctx, TPlanProps& planProps) override;

    TExpression FilterExpr;
};

bool TestAndExtractEqualityPredicate(TExprNode::TPtr pred, TExprNode::TPtr& leftArg, TExprNode::TPtr& rightArg);

class TOpJoin: public IBinaryOperator {
public:
    TOpJoin(TIntrusivePtr<IOperator> leftArg, TIntrusivePtr<IOperator> rightArg, TPositionHandle pos, TString joinKind,
            const TVector<std::pair<TInfoUnit, TInfoUnit>>& joinKeys);
    virtual TVector<TInfoUnit> GetOutputIUs() override;
    virtual TVector<TInfoUnit> GetUsedIUs(TPlanProps& props) override;

    void RenameIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap, TExprContext& ctx,
                   const THashSet<TInfoUnit, TInfoUnit::THashFunction>& stopList = {}) override;
    virtual TString ToString(TExprContext& ctx) override;

    virtual void ComputeMetadata(TRBOContext& ctx, TPlanProps& planProps) override;
    virtual void ComputeStatistics(TRBOContext& ctx, TPlanProps& planProps) override;

    TString JoinKind;
    TVector<std::pair<TInfoUnit, TInfoUnit>> JoinKeys;
};

class TOpUnionAll: public IBinaryOperator {
public:
    TOpUnionAll(TIntrusivePtr<IOperator> leftArg, TIntrusivePtr<IOperator> rightArg, TPositionHandle pos, bool ordered = false);
    virtual TVector<TInfoUnit> GetOutputIUs() override;
    virtual TString ToString(TExprContext& ctx) override;

    virtual void ComputeMetadata(TRBOContext& ctx, TPlanProps& planProps) override;
    virtual void ComputeStatistics(TRBOContext& ctx, TPlanProps& planProps) override;

    bool Ordered;
};

class TOpLimit: public IUnaryOperator {
public:
    TOpLimit(TIntrusivePtr<IOperator> input, TPositionHandle pos, const TExpression& limitCond);
    virtual TVector<TInfoUnit> GetOutputIUs() override;
    void RenameIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap, TExprContext& ctx,
                   const THashSet<TInfoUnit, TInfoUnit::THashFunction>& stopList = {}) override;
    virtual TString ToString(TExprContext& ctx) override;

    TExpression LimitCond;
};

class TOpSort: public IUnaryOperator {
public:
    TOpSort(TIntrusivePtr<IOperator> input, TPositionHandle pos, const TVector<TSortElement>& sortElements,
            std::optional<TExpression> limitCond = std::nullopt);
    virtual TVector<TInfoUnit> GetOutputIUs() override;
    virtual TVector<TInfoUnit> GetUsedIUs(TPlanProps& props) override;
    void RenameIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap, TExprContext& ctx,
                   const THashSet<TInfoUnit, TInfoUnit::THashFunction>& stopList = {}) override;
    virtual TString ToString(TExprContext& ctx) override;

    TVector<TSortElement> SortElements;
    std::optional<TExpression> LimitCond;
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

    virtual void ComputeMetadata(TRBOContext& ctx, TPlanProps& planProps) override;
    virtual void ComputeStatistics(TRBOContext& ctx, TPlanProps& planProps) override;

    TIntrusivePtr<IOperator> TreeRoot;
    TVector<TIntrusivePtr<IOperator>> TreeNodes;
};

class TOpRoot: public IUnaryOperator {
public:
    TOpRoot(TIntrusivePtr<IOperator> input, TPositionHandle pos, const TVector<TString>& columnOrder);
    virtual TVector<TInfoUnit> GetOutputIUs() override;
    virtual TString ToString(TExprContext& ctx) override;
    void ComputeParents();
    IGraphTransformer::TStatus ComputeTypes(TRBOContext& ctx);

    TString PlanToString(TExprContext& ctx, ui32 printOptions = 0x0);
    void PlanToStringRec(TIntrusivePtr<IOperator> op, TExprContext& ctx, TStringBuilder& builder, int ntabs, ui32 printOptions = 0x0) const;

    void ComputePlanMetadata(TRBOContext& ctx);
    void ComputePlanStatistics(TRBOContext& ctx);

    TPlanProps PlanProps;
    TExprNode::TPtr Node;
    TVector<TString> ColumnOrder;

    struct Iterator {
        struct IteratorItem {
            IteratorItem(TIntrusivePtr<IOperator> curr, TIntrusivePtr<IOperator> parent, size_t idx, std::shared_ptr<TInfoUnit> subplanIU)
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

        Iterator(TOpRoot* ptr) {
            if (!ptr) {
                CurrElement = -1;
                return;
            }
            Root = ptr;

            std::unordered_set<IOperator*> visited;
            for (const auto& subplan : Root->PlanProps.Subplans.Get()) {
                BuildDfsList(CastOperator<IOperator>(subplan.Plan), nullptr, size_t(0), visited, std::make_shared<TInfoUnit>(subplan.IU));
            }
            auto child = ptr->GetInput();
            BuildDfsList(child, {}, size_t(0), visited, nullptr);
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
        Iterator operator++(int) {
            Iterator tmp = *this;
            ++(*this);
            return tmp;
        }

        friend bool operator==(const Iterator& a, const Iterator& b) {
            return a.CurrElement == b.CurrElement;
        };
        friend bool operator!=(const Iterator& a, const Iterator& b) {
            return a.CurrElement != b.CurrElement;
        };

    private:
        void BuildDfsList(TIntrusivePtr<IOperator> current, TIntrusivePtr<IOperator> parent, size_t childIdx, std::unordered_set<IOperator*>& visited,
                          std::shared_ptr<TInfoUnit> subplanIU) {
            const auto& children = current->GetChildren();
            for (size_t idx = 0, e = children.size(); idx < e; ++idx) {
                BuildDfsList(children[idx], current, idx, visited, subplanIU);
            }
            if (!visited.contains(current.get())) {
                DfsList.push_back(IteratorItem(current, parent, childIdx, subplanIU));
            }
            visited.insert(current.get());
        }

        TVector<IteratorItem> DfsList;
        size_t CurrElement;
        TOpRoot* Root;
    };

    Iterator begin() {
        return Iterator(this);
    }
    Iterator end() {
        return Iterator(nullptr);
    }

private:
    void ComputeParentsRec(TIntrusivePtr<IOperator> op, TIntrusivePtr<IOperator> parent, ui32 parentChildIndex) const;
};

} // namespace NKqp
} // namespace NKikimr
#pragma once

#include "kqp_operator.h"

#include <memory>

/**
 * Convert a plan from ExprNode operators into RBO operators and back
 */
namespace NKikimr {
namespace NKqp {

using namespace NYql;

class PlanConverter {
  public:
    PlanConverter(TTypeAnnotationContext &typeCtx, TExprContext &ctx) : TypeCtx(typeCtx), Ctx(ctx) {}

    // Convert KqpOpRoot to OpRoot.
    TIntrusivePtr<TOpRoot> ConvertRoot(TExprNode::TPtr node, TExprNode::TPtr queryColumns);
    TIntrusivePtr<IOperator> ExprNodeToOperator(TExprNode::TPtr node);

    TIntrusivePtr<IOperator> ConvertTKqpOpEmptySource(TExprNode::TPtr node);
    TIntrusivePtr<IOperator> ConvertTKqpOpMap(TExprNode::TPtr node);
    TIntrusivePtr<IOperator> ConvertTKqpOpFilter(TExprNode::TPtr node);
    TIntrusivePtr<IOperator> ConvertTKqpOpJoin(TExprNode::TPtr node);
    TIntrusivePtr<IOperator> ConvertTKqpOpLimit(TExprNode::TPtr node);
    TIntrusivePtr<IOperator> ConvertTKqpOpProject(TExprNode::TPtr node);
    TIntrusivePtr<IOperator> ConvertTKqpOpSetOp(TExprNode::TPtr node);
    TIntrusivePtr<IOperator> ConvertTKqpOpSort(TExprNode::TPtr node);
    TIntrusivePtr<IOperator> ConvertTKqpOpAggregate(TExprNode::TPtr node);
    TIntrusivePtr<IOperator> ConvertTKqpOpGroupingSets(TExprNode::TPtr node);
    TIntrusivePtr<IOperator> ConvertTKqpOpWindow(TExprNode::TPtr node);
    TIntrusivePtr<IOperator> ConvertTKqpInfuseDependents(TExprNode::TPtr node);
    TIntrusivePtr<IOperator> ConvertTKqpOpReplaceAlias(TExprNode::TPtr node);
    TIntrusivePtr<IOperator> ConvertTKqpOpReplaceColumns(TExprNode::TPtr node);
    TIntrusivePtr<IOperator> ConvertTKqpOpTableEffect(TExprNode::TPtr node);

    // Subqueries are extracted into subplans only in filter and projection expressions.
    TExpression ConvertExpression(TExprNode::TPtr lambda, const TExpression::TBindings& bindings, bool allowSubqueries = true);

    TTypeAnnotationContext &TypeCtx;
    TExprContext &Ctx;
    using TImportKey = std::pair<const TExprNode*, ui64>;
    TPlanProps PlanProps;

private:
    // Import scopes, not optimizer state. Pass-through operators share their
    // child's scope; source spellings are never recovered from registry labels.
    using TBindingScope = std::shared_ptr<const TExpression::TBindings>;
    struct TSharedImport {
        TIntrusivePtr<TReplicate> Hub;
        TBindingScope Bindings;
        std::optional<TOrderedIUs<>> Projection;
    };
    void CountUses(const TExprNode::TPtr& root);
    THashMap<const TExprNode*, size_t> Uses;
    THashMap<TImportKey, TSharedImport> Converted;
    THashSet<TImportKey> Imported;
    THashMap<TImportKey, TBindingScope> OutputBindings;
    // Visible output positions at the import boundary, independent of schema and ID order.
    THashMap<TImportKey, TOrderedIUs<>> Projections;
    THashMap<const TExprNode*, bool> CaptureFreeNodes;
    THashSet<const TExprNode*> SubquerySources;
    TVector<const TExpression::TBindings*> OuterBindings;
    ui64 BindingContext = 0;
    ui64 NextBindingContext = 0;
    std::pair<TIntrusivePtr<IOperator>, TBindingScope> ConvertSubquery(
        TExprNode::TPtr node, const TExpression::TBindings& bindings);
    TBindingScope GetBindings(const TExprNode::TPtr& node) const;
    std::optional<TOrderedIUs<>> GetProjection(const TExprNode::TPtr& node) const;
    void PropagateProjection(const TExprNode::TPtr& input, const TExprNode::TPtr& output,
        const TSubstitutions& substitutions = {});
    TSortIUs ConvertSortKeys(const NNodes::TKqpOpSortList& keys,
        const TExpression::TBindings& bindings, TMapIUs& definitions);

};

} // namespace NKqp
} // namespace NKikimr

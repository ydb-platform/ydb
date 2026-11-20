#include "pg_policy.h"

#include <yql/essentials/core/services/yql_transform_pipeline.h>
#include <yql/essentials/core/type_ann/type_ann_expr.h>
#include <yql/essentials/core/yql_expr_optimize.h>

#include <util/generic/hash_set.h>

namespace NKikimr::NKqp {
namespace {

class TPgTypeVisitor: public NYql::TDefaultTypeAnnotationVisitor {
public:
    bool ContainsPg(const NYql::TTypeAnnotationNode& type) {
        type.Accept(*this);
        return HasPg;
    }

private:
    void Visit(const NYql::TPgExprType&) final {
        HasPg = true;
    }

#define VISIT_COMPOSITE_TYPE(Type) \
    void Visit(const NYql::T##Type##ExprType& type) final { \
        if (!HasPg && Visited.insert(&type).second) { \
            TDefaultTypeAnnotationVisitor::Visit(type); \
        } \
    }

    VISIT_COMPOSITE_TYPE(Multi)
    VISIT_COMPOSITE_TYPE(Tuple)
    VISIT_COMPOSITE_TYPE(Struct)
    VISIT_COMPOSITE_TYPE(Item)
    VISIT_COMPOSITE_TYPE(List)
    VISIT_COMPOSITE_TYPE(Stream)
    VISIT_COMPOSITE_TYPE(Flow)
    VISIT_COMPOSITE_TYPE(Optional)
    VISIT_COMPOSITE_TYPE(Callable)
    VISIT_COMPOSITE_TYPE(Type)
    VISIT_COMPOSITE_TYPE(Dict)
    VISIT_COMPOSITE_TYPE(Tagged)
    VISIT_COMPOSITE_TYPE(Variant)
    VISIT_COMPOSITE_TYPE(Block)
    VISIT_COMPOSITE_TYPE(Scalar)
    VISIT_COMPOSITE_TYPE(Linear)
    VISIT_COMPOSITE_TYPE(DynamicLinear)

#undef VISIT_COMPOSITE_TYPE

    THashSet<const NYql::TTypeAnnotationNode*> Visited;
    bool HasPg = false;
};

bool IsPgValue(const NYql::TTypeAnnotationNode* type) {
    while (type->GetKind() == NYql::ETypeAnnotationKind::Optional) {
        type = type->Cast<NYql::TOptionalExprType>()->GetItemType();
    }
    return type->GetKind() == NYql::ETypeAnnotationKind::Pg;
}

void AddPgCompatibilityIssue(const NYql::TExprNode& node, NYql::TExprContext& ctx, NYql::EYqlIssueCode code) {
    ctx.AddError(NYql::YqlIssue(ctx.GetPosition(node.Pos()), code, "PostgreSQL compatibility is disabled"));
}

class TKqpTypeAnnCallableTransformer: public NYql::IGraphTransformer {
public:
    explicit TKqpTypeAnnCallableTransformer(NYql::TTypeAnnotationContext& types)
        : Inner_(NYql::CreateExtCallableTypeAnnotationTransformer(types))
    {
    }

    TStatus Transform(NYql::TExprNode::TPtr input, NYql::TExprNode::TPtr& output, NYql::TExprContext& ctx) final {
        const auto name = input->Content();
        const bool unsupported = input->IsCallable() && (IsPgCallable(name) ||
            (name == "FromPg" && input->ChildrenSize() == 1 && input->Head().GetTypeAnn() &&
                input->Head().GetTypeAnn()->GetKind() == NYql::ETypeAnnotationKind::Pg));
        if (unsupported) {
            output = input;
            AddPgCompatibilityIssue(*input, ctx, NYql::TIssuesIds::KIKIMR_BAD_REQUEST);
            return TStatus::Error;
        }

        auto status = Inner_->Transform(input, output, ctx);
        if (status == TStatus::Ok && output && output->GetTypeAnn()) {
            const auto& type = *output->GetTypeAnn();
            // An initial read contains the full row. Composite value types
            // are checked after unused fields have been pruned.
            if (IsPgValue(&type) || (name == "ParseType" && TPgTypeVisitor().ContainsPg(type))) {
                AddPgCompatibilityIssue(*output, ctx, NYql::TIssuesIds::KIKIMR_UNSUPPORTED);
                return TStatus::Error;
            }
        }
        return status;
    }

    NThreading::TFuture<void> GetAsyncFuture(const NYql::TExprNode& input) final {
        return Inner_->GetAsyncFuture(input);
    }

    TStatus ApplyAsyncChanges(NYql::TExprNode::TPtr input, NYql::TExprNode::TPtr& output, NYql::TExprContext& ctx) final {
        return Inner_->ApplyAsyncChanges(input, output, ctx);
    }

    void Rewind() final {
        Inner_->Rewind();
    }

    TStatistics GetStatistics() const final {
        return Inner_->GetStatistics();
    }

private:
    TAutoPtr<NYql::IGraphTransformer> Inner_;
};

} // namespace

bool IsPgCallable(TStringBuf name) {
    return name.StartsWith("Pg") || name.StartsWith("BlockPg") || name == "ToPg" ||
        name == "BlockToPg" || name == "BlockFromPg" || name == "RangeToPg";
}

bool ValidateOptimizedKqpPgTypes(const NYql::TExprNode::TPtr& root, NYql::TExprContext& ctx) {
    TPgTypeVisitor visitor;
    if (const auto node = NYql::FindNode(root, [&](const NYql::TExprNode::TPtr& node) {
            return node->GetTypeAnn() && visitor.ContainsPg(*node->GetTypeAnn());
        }))
    {
        AddPgCompatibilityIssue(*node, ctx, NYql::TIssuesIds::KIKIMR_UNSUPPORTED);
        return false;
    }
    return true;
}

NYql::TTypeAnnCallableFactory CreateKqpTypeAnnCallableFactory(NYql::TTypeAnnotationContext& types, bool disablePgCompatibility) {
    if (!disablePgCompatibility) {
        return {};
    }
    return [&types]() -> TAutoPtr<NYql::IGraphTransformer> {
        return new TKqpTypeAnnCallableTransformer(types);
    };
}

bool InstantAnnotateKqpTypes(NYql::TExprNode::TPtr& root, NYql::TExprContext& ctx, NYql::TTypeAnnotationContext& types, bool disablePgCompatibility) {
    if (!disablePgCompatibility) {
        return NYql::InstantAnnotateTypes(root, ctx, false, types);
    }
    auto transformer = NYql::TTransformationPipeline(&types, CreateKqpTypeAnnCallableFactory(types, disablePgCompatibility))
        .AddPreTypeAnnotation()
        .Add(NYql::CreateFunctorTransformer([](const NYql::TExprNode::TPtr& input, NYql::TExprNode::TPtr& output, NYql::TExprContext& ctx) {
            output = input;
            ctx.Step.Done(NYql::TExprStep::ExprEval);
            return NYql::IGraphTransformer::TStatus::Ok;
        }), "EvaluateExpression", NYql::TIssuesIds::CORE_PRE_TYPE_ANN)
        .AddIOAnnotation()
        .AddTypeAnnotationTransformer()
        .Build();
    return NYql::InstantTransform(*transformer, root, ctx) == NYql::IGraphTransformer::TStatus::Ok;
}

} // namespace NKikimr::NKqp

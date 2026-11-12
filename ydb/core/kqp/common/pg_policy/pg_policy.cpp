#include "pg_policy.h"

#include <yql/essentials/core/services/yql_transform_pipeline.h>
#include <yql/essentials/core/type_ann/type_ann_core.h>

namespace NKikimr::NKqp {
namespace {

struct TPgTypeVisitor: public NYql::TDefaultTypeAnnotationVisitor {
    void Visit(const NYql::TPgExprType&) final {
        HasPg = true;
    }

    bool HasPg = false;
};

class TKqpTypeAnnCallableTransformer: public NYql::IGraphTransformer {
public:
    explicit TKqpTypeAnnCallableTransformer(NYql::TTypeAnnotationContext& types)
        : Inner_(NYql::CreateExtCallableTypeAnnotationTransformer(types))
    {
    }

    TStatus Transform(NYql::TExprNode::TPtr input, NYql::TExprNode::TPtr& output, NYql::TExprContext& ctx) final {
        const auto name = input->Content();
        bool unsupported = input->IsCallable() && (name.StartsWith("Pg") || name.StartsWith("BlockPg") || name == "ToPg" || name == "FromPg" ||
                                                  name == "BlockToPg" || name == "BlockFromPg" || name == "RangeToPg");
        TStatus status = TStatus::Ok;
        if (!unsupported) {
            status = Inner_->Transform(input, output, ctx);
            if (status == TStatus::Ok && input->IsCallable("ParseType")) {
                TPgTypeVisitor visitor;
                output->GetTypeAnn()->Accept(visitor);
                unsupported = visitor.HasPg;
            }
        }
        if (unsupported) {
            output = input;
            ctx.AddError(NYql::YqlIssue(ctx.GetPosition(input->Pos()), NYql::TIssuesIds::KIKIMR_BAD_REQUEST,
                "PostgreSQL types are not supported"));
            return TStatus::Error;
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

NYql::TTypeAnnCallableFactory CreateKqpTypeAnnCallableFactory(NYql::TTypeAnnotationContext& types) {
    return [&types]() -> TAutoPtr<NYql::IGraphTransformer> {
        return new TKqpTypeAnnCallableTransformer(types);
    };
}

bool InstantAnnotateKqpTypes(NYql::TExprNode::TPtr& root, NYql::TExprContext& ctx, NYql::TTypeAnnotationContext& types) {
    auto transformer = NYql::TTransformationPipeline(&types, CreateKqpTypeAnnCallableFactory(types))
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

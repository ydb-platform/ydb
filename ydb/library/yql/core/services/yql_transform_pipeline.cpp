#include "yql_transform_pipeline.h"
#include "yql_eval_expr.h"
#include "yql_eval_params.h"
#include "yql_lineage.h"

#include <ydb/library/yql/core/type_ann/type_ann_core.h>
#include <ydb/library/yql/core/type_ann/type_ann_expr.h>
#include <ydb/library/yql/core/yql_execution.h>
#include <ydb/library/yql/core/yql_expr_csee.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/core/yql_expr_constraint.h>
#include <ydb/library/yql/core/yql_gc_transformer.h>
#include <ydb/library/yql/core/common_opt/yql_co_transformer.h>
#include <ydb/library/yql/core/yql_opt_proposed_by_data.h>
#include <ydb/library/yql/core/yql_opt_rewrite_io.h>

#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>

namespace NYql {

TTransformationPipeline::TTransformationPipeline(TIntrusivePtr<TTypeAnnotationContext> ctx)
    : TypeAnnotationContext_(ctx)
{}

TTransformationPipeline& TTransformationPipeline::Add(TAutoPtr<IGraphTransformer> transformer, const TString& stageName,
    EYqlIssueCode issueCode, const TString& issueMessage)
{
    if (transformer) {
        Transformers_.push_back(TTransformStage(transformer, stageName, issueCode, issueMessage));
    }
    return *this;
}

TTransformationPipeline& TTransformationPipeline::Add(IGraphTransformer& transformer, const TString& stageName,
    EYqlIssueCode issueCode, const TString& issueMessage)
{
    Transformers_.push_back(TTransformStage(transformer, stageName, issueCode, issueMessage));
    return *this;
}

TTransformationPipeline& TTransformationPipeline::AddServiceTransformers(EYqlIssueCode issueCode) {
    Transformers_.push_back(TTransformStage(CreateGcNodeTransformer(), "GC", issueCode));
    return *this;
}

TTransformationPipeline& TTransformationPipeline::AddParametersEvaluation(const NKikimr::NMiniKQL::IFunctionRegistry& functionRegistry, EYqlIssueCode issueCode) {
    auto& typeCtx = *TypeAnnotationContext_;
    Transformers_.push_back(TTransformStage(CreateFunctorTransformer(
        [&](const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
        return EvaluateParameters(input, output, typeCtx, ctx, functionRegistry);
    }), "EvaluateParameters", issueCode));

    return *this;
}

TTransformationPipeline& TTransformationPipeline::AddExpressionEvaluation(const NKikimr::NMiniKQL::IFunctionRegistry& functionRegistry,
    IGraphTransformer* calcTransfomer, EYqlIssueCode issueCode) {
    auto& typeCtx = *TypeAnnotationContext_;
    auto& funcReg = functionRegistry;
    Transformers_.push_back(TTransformStage(CreateFunctorTransformer(
        [&typeCtx, &funcReg, calcTransfomer](const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
        return EvaluateExpression(input, output, typeCtx, ctx, funcReg, calcTransfomer);
    }), "EvaluateExpression", issueCode));

    return *this;
}

TTransformationPipeline& TTransformationPipeline::AddPreTypeAnnotation(EYqlIssueCode issueCode) {
    auto& typeCtx = *TypeAnnotationContext_;
    Transformers_.push_back(TTransformStage(CreateFunctorTransformer(&ExpandApply), "ExpandApply",
        issueCode));
    Transformers_.push_back(TTransformStage(CreateFunctorTransformer(
        [&](const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
            return ValidateProviders(input, output, ctx, typeCtx);
        }), "ValidateProviders", issueCode));

    Transformers_.push_back(TTransformStage(
        CreateConfigureTransformer(*TypeAnnotationContext_), "Configure", issueCode));

    return *this;
}

TTransformationPipeline& TTransformationPipeline::AddPreIOAnnotation(bool withEpochsTransformer, EYqlIssueCode issueCode) {
    Transformers_.push_back(TTransformStage(
        CreateIODiscoveryTransformer(*TypeAnnotationContext_), "IODiscovery", issueCode));
    if (withEpochsTransformer) {
        Transformers_.push_back(TTransformStage(
            CreateEpochsTransformer(*TypeAnnotationContext_), "Epochs", issueCode));
    }
    AddIntentDeterminationTransformer();

    return *this;
}

TTransformationPipeline& TTransformationPipeline::AddIOAnnotation(bool withEpochsTransformer, EYqlIssueCode issueCode) {
    AddPreIOAnnotation(withEpochsTransformer, issueCode);
    AddTableMetadataLoaderTransformer();

    auto& typeCtx = *TypeAnnotationContext_;
    Transformers_.push_back(TTransformStage(CreateFunctorTransformer(
        [&](const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
            return RewriteIO(input, output, typeCtx, ctx);
        }), "RewriteIO", issueCode));

    return *this;
}

TTransformationPipeline& TTransformationPipeline::AddTypeAnnotation(EYqlIssueCode issueCode) {
    AddTypeAnnotationTransformer();
    Transformers_.push_back(TTransformStage(
        CreateFunctorTransformer(&CheckWholeProgramType),
        "CheckWholeProgramType", issueCode));
    return *this;
}

TTransformationPipeline& TTransformationPipeline::AddPostTypeAnnotation(bool forSubGraph, bool disableConstraintCheck, EYqlIssueCode issueCode) {
    Transformers_.push_back(TTransformStage(
        CreateConstraintTransformer(*TypeAnnotationContext_, false, forSubGraph, disableConstraintCheck), "Constraints", issueCode));
    Transformers_.push_back(TTransformStage(
        CreateFunctorTransformer(
            [](const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
                return UpdateCompletness(input, output, ctx);
            }
        ),
        "UpdateCompletness",
        issueCode));
    Transformers_.push_back(TTransformStage(
        CreateFunctorTransformer(
            [forSubGraph, coStore = TypeAnnotationContext_->ColumnOrderStorage](const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
                return EliminateCommonSubExpressions(input, output, ctx, forSubGraph, *coStore);
            }
        ),
        "CSEE",
        issueCode));

    return *this;
}

TTransformationPipeline& TTransformationPipeline::AddCommonOptimization(EYqlIssueCode issueCode) {
    // auto instantCallableTransformer =
    //    CreateExtCallableTypeAnnotationTransformer(*TypeAnnotationContext_, true);
    // TypeAnnotationContext_->CustomInstantTypeTransformer =
    //     CreateTypeAnnotationTransformer(instantCallableTransformer, *TypeAnnotationContext_);
    Transformers_.push_back(TTransformStage(
        CreateCommonOptTransformer(TypeAnnotationContext_.Get()),
        "CommonOptimization",
        issueCode));
    return *this;
}

TTransformationPipeline& TTransformationPipeline::AddFinalCommonOptimization(EYqlIssueCode issueCode) {
    Transformers_.push_back(TTransformStage(
        CreateCommonOptFinalTransformer(TypeAnnotationContext_.Get()),
        "FinalCommonOptimization",
        issueCode));
    return *this;
}

TTransformationPipeline& TTransformationPipeline::AddOptimization(bool checkWorld, bool withFinalOptimization, EYqlIssueCode issueCode) {
    AddCommonOptimization(issueCode);
    Transformers_.push_back(TTransformStage(
        CreateRecaptureDataProposalsInspector(*TypeAnnotationContext_, TString{DqProviderName}),
        "RecaptureDataProposals",
        issueCode));
    Transformers_.push_back(TTransformStage(
        CreateStatisticsProposalsInspector(*TypeAnnotationContext_, TString{DqProviderName}),
        "StatisticsProposals",
        issueCode
    ));
    Transformers_.push_back(TTransformStage(
        CreateLogicalDataProposalsInspector(*TypeAnnotationContext_),
        "LogicalDataProposals",
        issueCode));
    Transformers_.push_back(TTransformStage(
        CreatePhysicalDataProposalsInspector(*TypeAnnotationContext_),
        "PhysicalDataProposals",
        issueCode));
    Transformers_.push_back(TTransformStage(
        CreatePhysicalFinalizers(*TypeAnnotationContext_),
        "PhysicalFinalizers",
        issueCode));
    if (withFinalOptimization) {
        AddFinalCommonOptimization(issueCode);
    }
    AddCheckExecution(checkWorld, issueCode);
    return *this;
}

TTransformationPipeline& TTransformationPipeline::AddLineageOptimization(TMaybe<TString>& lineageOut, EYqlIssueCode issueCode) {
    AddCommonOptimization(issueCode);
    Transformers_.push_back(TTransformStage(
        CreateFunctorTransformer(
            [typeCtx = TypeAnnotationContext_, &lineageOut](const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
                output = input;
                lineageOut = CalculateLineage(*input, *typeCtx, ctx);
                return IGraphTransformer::TStatus::Ok;
            }
        ),
        "LineageScanner",
        issueCode));
    return *this;
}

TTransformationPipeline& TTransformationPipeline::AddCheckExecution(bool checkWorld, EYqlIssueCode issueCode) {
    Transformers_.push_back(TTransformStage(
        CreateCheckExecutionTransformer(*TypeAnnotationContext_, checkWorld),
        "CheckExecution",
        issueCode));
    return *this;
}

TTransformationPipeline& TTransformationPipeline::AddRun(TOperationProgressWriter writer, EYqlIssueCode issueCode) {
    Transformers_.push_back(TTransformStage(
        CreateExecutionTransformer(*TypeAnnotationContext_, writer),
        "Execution",
        issueCode));
    return *this;
}

TTransformationPipeline& TTransformationPipeline::AddIntentDeterminationTransformer(EYqlIssueCode issueCode) {
    Transformers_.push_back(TTransformStage(
        CreateIntentDeterminationTransformer(*TypeAnnotationContext_),
        "IntentDetermination",
        issueCode));
    return *this;
}

TTransformationPipeline& TTransformationPipeline::AddTableMetadataLoaderTransformer(EYqlIssueCode issueCode) {
    Transformers_.push_back(TTransformStage(
        CreateTableMetadataLoader(*TypeAnnotationContext_),
        "TableMetadataLoader",
        issueCode));
    return *this;
}

TTransformationPipeline& TTransformationPipeline::AddTypeAnnotationTransformer(
    TAutoPtr<IGraphTransformer> callableTransformer, EYqlIssueCode issueCode)
{
    Transformers_.push_back(TTransformStage(
        CreateTypeAnnotationTransformer(callableTransformer, *TypeAnnotationContext_),
        "TypeAnnotation",
        issueCode));
    return *this;
}

TTransformationPipeline& TTransformationPipeline::AddTypeAnnotationTransformer(EYqlIssueCode issueCode)
{
    auto callableTransformer = CreateExtCallableTypeAnnotationTransformer(*TypeAnnotationContext_);
    return AddTypeAnnotationTransformer(callableTransformer, issueCode);
}

TAutoPtr<IGraphTransformer> TTransformationPipeline::Build(bool useIssueScopes) {
    return CreateCompositeGraphTransformer(Transformers_, useIssueScopes);
}

TAutoPtr<IGraphTransformer> TTransformationPipeline::BuildWithNoArgChecks(bool useIssueScopes) {
    return CreateCompositeGraphTransformerWithNoArgChecks(Transformers_, useIssueScopes);
}

TIntrusivePtr<TTypeAnnotationContext> TTransformationPipeline::GetTypeAnnotationContext() const {
    return TypeAnnotationContext_;
}


} // namespace NYql

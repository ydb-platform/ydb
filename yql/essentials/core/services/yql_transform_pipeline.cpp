#include "yql_transform_pipeline.h"
#include "yql_eval_expr.h"
#include "yql_eval_params.h"
#include "yql_lineage.h"

#include <yql/essentials/core/type_ann/type_ann_core.h>
#include <yql/essentials/core/type_ann/type_ann_expr.h>
#include <yql/essentials/core/yql_execution.h>
#include <yql/essentials/core/yql_expr_csee.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/core/yql_expr_constraint.h>
#include <yql/essentials/core/yql_gc_transformer.h>
#include <yql/essentials/core/common_opt/yql_co_transformer.h>
#include <yql/essentials/core/yql_opt_proposed_by_data.h>
#include <yql/essentials/core/yql_opt_rewrite_io.h>

#include <yql/essentials/providers/common/provider/yql_provider_names.h>

namespace NYql {

TTransformationPipeline::TTransformationPipeline(
    TIntrusivePtr<TTypeAnnotationContext> ctx,
    TTypeAnnCallableFactory typeAnnCallableFactory)
    : TypeAnnotationContext_(ctx)
    , TypeAnnCallableFactory_(typeAnnCallableFactory ? typeAnnCallableFactory : [ctx = ctx.Get()](){
        return CreateExtCallableTypeAnnotationTransformer(*ctx);
    })
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
    auto typeAnnCallableFactory = TypeAnnCallableFactory_;
    Transformers_.push_back(TTransformStage(CreateFunctorTransformer(
        [&typeCtx, &funcReg, calcTransfomer, typeAnnCallableFactory](const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
        return EvaluateExpression(input, output, typeCtx, ctx, funcReg, calcTransfomer, typeAnnCallableFactory);
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

TTransformationPipeline& TTransformationPipeline::AddTypeAnnotation(EYqlIssueCode issueCode, bool twoStages) {
    AddTypeAnnotationTransformer(issueCode, twoStages);
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

TTransformationPipeline& TTransformationPipeline::AddCommonOptimization(bool forPeephole, EYqlIssueCode issueCode) {
    Transformers_.push_back(TTransformStage(
        CreateCommonOptTransformer(forPeephole, TypeAnnotationContext_.Get()),
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
    AddCommonOptimization(false, issueCode);
    Transformers_.push_back(TTransformStage(
        CreateChoiceGraphTransformer(
            [&typesCtx = std::as_const(*TypeAnnotationContext_)](const TExprNode::TPtr&, TExprContext&) {
                return typesCtx.EngineType == EEngineType::Ytflow;
            },
            TTransformStage(
                CreateRecaptureDataProposalsInspector(*TypeAnnotationContext_, TString{YtflowProviderName}),
                "RecaptureDataProposalsYtflow",
                issueCode),
            TTransformStage(
                CreateRecaptureDataProposalsInspector(*TypeAnnotationContext_, TString{DqProviderName}),
                "RecaptureDataProposalsDq",
                issueCode)),
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
    AddCommonOptimization(false, issueCode);
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
    TAutoPtr<IGraphTransformer> callableTransformer, EYqlIssueCode issueCode, ETypeCheckMode mode)
{
    TString stageName;
    TString issue;
    switch (mode) {
    case ETypeCheckMode::Single:
        stageName = "TypeAnnotation";
        issue = "Type annotation";
        break;
    case ETypeCheckMode::Initial:
        stageName = "InitialTypeAnnotation";
        issue = "Type annotation";
        break;
    case ETypeCheckMode::Repeat:
        stageName = "RepeatTypeAnnotation";
        issue = "Type annotation (repeat)";
        break;
    }

    Transformers_.push_back(TTransformStage(
        CreateTypeAnnotationTransformer(callableTransformer, *TypeAnnotationContext_, mode), stageName, issueCode, issue));
    return *this;
}

TTransformationPipeline& TTransformationPipeline::AddTypeAnnotationTransformerWithMode(EYqlIssueCode issueCode, ETypeCheckMode mode) {
    auto callableTransformer = TypeAnnCallableFactory_();
    AddTypeAnnotationTransformer(callableTransformer, issueCode, mode);
    return *this;
}

TTransformationPipeline& TTransformationPipeline::AddTypeAnnotationTransformer(EYqlIssueCode issueCode, bool twoStages)
{
    if (twoStages) {
        std::shared_ptr<IGraphTransformer> callableTransformer(TypeAnnCallableFactory_().Release());
        AddTypeAnnotationTransformer(MakeSharedTransformerProxy(callableTransformer), issueCode, ETypeCheckMode::Initial);
        AddTypeAnnotationTransformer(MakeSharedTransformerProxy(callableTransformer), issueCode, ETypeCheckMode::Repeat);
    } else {
        auto callableTransformer = TypeAnnCallableFactory_();
        AddTypeAnnotationTransformer(callableTransformer, issueCode, ETypeCheckMode::Single);
    }

    return *this;
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

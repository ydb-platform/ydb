#pragma once

#include <yql/essentials/core/yql_graph_transformer.h>
#include <yql/essentials/core/yql_type_annotation.h>
#include <yql/essentials/core/yql_execution.h>
#include <yql/essentials/core/issue/yql_issue.h>
#include <yql/essentials/core/type_ann/type_ann_expr.h>

#include <util/generic/ptr.h>

namespace NKikimr::NMiniKQL {

class IFunctionRegistry;

} // namespace NKikimr::NMiniKQL

namespace NYql {

class TTransformationPipeline {
public:
    explicit TTransformationPipeline(TIntrusivePtr<TTypeAnnotationContext> ctx,
                                     TTypeAnnCallableFactory typeAnnCallableFactory = {});

    TTransformationPipeline& AddServiceTransformers(EYqlIssueCode issueCode = TIssuesIds::CORE_GC);
    TTransformationPipeline& AddParametersEvaluation(const NKikimr::NMiniKQL::IFunctionRegistry& functionRegistry, EYqlIssueCode issueCode = TIssuesIds::CORE_PARAM_EVALUATION);
    TTransformationPipeline& AddPreTypeAnnotation(EYqlIssueCode issueCode = TIssuesIds::CORE_PRE_TYPE_ANN);
    TTransformationPipeline& AddExpressionEvaluation(const NKikimr::NMiniKQL::IFunctionRegistry& functionRegistry,
                                                     IGraphTransformer* calcTransfomer = nullptr, EYqlIssueCode issueCode = TIssuesIds::CORE_EXPR_EVALUATION);
    TTransformationPipeline& AddPreIOAnnotation(bool withEpochsTransformer = true, EYqlIssueCode issueCode = TIssuesIds::CORE_PRE_TYPE_ANN);
    TTransformationPipeline& AddIOAnnotation(bool withEpochsTransformer = true, EYqlIssueCode issueCode = TIssuesIds::CORE_PRE_TYPE_ANN);
    TTransformationPipeline& AddTypeAnnotation(EYqlIssueCode issueCode = TIssuesIds::CORE_TYPE_ANN, bool twoStages = false);
    TTransformationPipeline& AddPostTypeAnnotation(bool forSubGraph = false, bool disableConstraintCheck = false, EYqlIssueCode issueCode = TIssuesIds::CORE_POST_TYPE_ANN);
    TTransformationPipeline& AddCommonOptimization(bool forPeephole = false, EYqlIssueCode issueCode = TIssuesIds::CORE_OPTIMIZATION);
    TTransformationPipeline& AddFinalCommonOptimization(EYqlIssueCode issueCode = TIssuesIds::CORE_OPTIMIZATION);
    TTransformationPipeline& AddOptimization(bool checkWorld = true, bool withFinalOptimization = true, EYqlIssueCode issueCode = TIssuesIds::CORE_OPTIMIZATION);
    TTransformationPipeline& AddProviderOptimization(EYqlIssueCode issueCode = TIssuesIds::CORE_OPTIMIZATION);
    TTransformationPipeline& AddOptimizationWithLineage(bool enableLineage, bool checkWorld = true, bool withFinalOptimization = true, EYqlIssueCode issueCode = TIssuesIds::CORE_OPTIMIZATION);
    TTransformationPipeline& AddLineageOptimization(TMaybe<TString>& lineageOut, EYqlIssueCode issueCode = TIssuesIds::CORE_OPTIMIZATION);
    TTransformationPipeline& AddCheckExecution(bool checkWorld = true, EYqlIssueCode issueCode = TIssuesIds::CORE_OPTIMIZATION);
    TTransformationPipeline& AddRun(TOperationProgressWriter writer, EYqlIssueCode issueCode = TIssuesIds::CORE_EXEC);

    TTransformationPipeline& AddIntentDeterminationTransformer(EYqlIssueCode issueCode = TIssuesIds::CORE_INTENT);
    TTransformationPipeline& AddTableMetadataLoaderTransformer(EYqlIssueCode issueCode = TIssuesIds::CORE_TABLE_METADATA_LOADER);
    TTransformationPipeline& AddTypeAnnotationTransformer(TAutoPtr<IGraphTransformer> callableTransformer, EYqlIssueCode issueCode = TIssuesIds::CORE_TYPE_ANN,
                                                          ETypeCheckMode mode = ETypeCheckMode::Single);
    TTransformationPipeline& AddTypeAnnotationTransformerWithMode(EYqlIssueCode issueCode = TIssuesIds::CORE_TYPE_ANN,
                                                                  ETypeCheckMode mode = ETypeCheckMode::Single);
    TTransformationPipeline& AddTypeAnnotationTransformer(EYqlIssueCode issueCode = TIssuesIds::CORE_TYPE_ANN, bool twoStages = false);

    TTransformationPipeline& Add(TAutoPtr<IGraphTransformer> transformer, const TString& stageName,
                                 EYqlIssueCode issueCode = TIssuesIds::DEFAULT_ERROR, const TString& issueMessage = {});
    TTransformationPipeline& Add(IGraphTransformer& transformer, const TString& stageName,
                                 EYqlIssueCode issueCode = TIssuesIds::DEFAULT_ERROR, const TString& issueMessage = {});

    TAutoPtr<IGraphTransformer> Build(bool useIssueScopes = true);
    TAutoPtr<IGraphTransformer> BuildWithNoArgChecks(bool useIssueScopes = true);

    TIntrusivePtr<TTypeAnnotationContext> GetTypeAnnotationContext() const;

private:
    TIntrusivePtr<TTypeAnnotationContext> TypeAnnotationContext_;
    TTypeAnnCallableFactory TypeAnnCallableFactory_;
    TVector<TTransformStage> Transformers_;
};

struct IPipelineConfigurator {
    virtual ~IPipelineConfigurator() = default;

    virtual void AfterCreate(TTransformationPipeline* pipeline) const = 0;
    virtual void AfterTypeAnnotation(TTransformationPipeline* pipeline) const = 0;
    virtual void AfterOptimize(TTransformationPipeline* pipeline) const = 0;
};

} // namespace NYql

#pragma once

#include <ydb/library/yql/core/yql_graph_transformer.h>
#include <ydb/library/yql/core/yql_type_annotation.h>
#include <ydb/library/yql/core/yql_execution.h>
#include <ydb/library/yql/core/issue/yql_issue.h>

#include <util/generic/ptr.h>

namespace NKikimr {
namespace NMiniKQL {

class IFunctionRegistry;

}
}

namespace NYql {

class TTransformationPipeline
{
public:
    TTransformationPipeline(TIntrusivePtr<TTypeAnnotationContext> ctx);

    TTransformationPipeline& AddServiceTransformers(EYqlIssueCode issueCode = TIssuesIds::CORE_GC);
    TTransformationPipeline& AddParametersEvaluation(const NKikimr::NMiniKQL::IFunctionRegistry& functionRegistry, EYqlIssueCode issueCode = TIssuesIds::CORE_PARAM_EVALUATION);
    TTransformationPipeline& AddPreTypeAnnotation(EYqlIssueCode issueCode = TIssuesIds::CORE_PRE_TYPE_ANN);
    TTransformationPipeline& AddExpressionEvaluation(const NKikimr::NMiniKQL::IFunctionRegistry& functionRegistry,
        IGraphTransformer* calcTransfomer = nullptr, EYqlIssueCode issueCode = TIssuesIds::CORE_EXPR_EVALUATION);
    TTransformationPipeline& AddPreIOAnnotation(bool withEpochsTransformer = true, EYqlIssueCode issueCode = TIssuesIds::CORE_PRE_TYPE_ANN);
    TTransformationPipeline& AddIOAnnotation(bool withEpochsTransformer = true, EYqlIssueCode issueCode = TIssuesIds::CORE_PRE_TYPE_ANN);
    TTransformationPipeline& AddTypeAnnotation(EYqlIssueCode issueCode = TIssuesIds::CORE_TYPE_ANN);
    TTransformationPipeline& AddPostTypeAnnotation(bool forSubGraph = false, bool disableConstraintCheck = false, EYqlIssueCode issueCode = TIssuesIds::CORE_POST_TYPE_ANN);
    TTransformationPipeline& AddCommonOptimization(EYqlIssueCode issueCode = TIssuesIds::CORE_OPTIMIZATION);
    TTransformationPipeline& AddFinalCommonOptimization(EYqlIssueCode issueCode = TIssuesIds::CORE_OPTIMIZATION);
    TTransformationPipeline& AddOptimization(bool checkWorld = true, bool withFinalOptimization = true, EYqlIssueCode issueCode = TIssuesIds::CORE_OPTIMIZATION);
    TTransformationPipeline& AddLineageOptimization(TMaybe<TString>& lineageOut, EYqlIssueCode issueCode = TIssuesIds::CORE_OPTIMIZATION);
    TTransformationPipeline& AddCheckExecution(bool checkWorld = true, EYqlIssueCode issueCode = TIssuesIds::CORE_OPTIMIZATION);
    TTransformationPipeline& AddRun(TOperationProgressWriter writer, EYqlIssueCode issueCode = TIssuesIds::CORE_EXEC);

    TTransformationPipeline& AddIntentDeterminationTransformer(EYqlIssueCode issueCode = TIssuesIds::CORE_INTENT);
    TTransformationPipeline& AddTableMetadataLoaderTransformer(EYqlIssueCode issueCode = TIssuesIds::CORE_TABLE_METADATA_LOADER);
    TTransformationPipeline& AddTypeAnnotationTransformer(TAutoPtr<IGraphTransformer> callableTransformer, EYqlIssueCode issueCode = TIssuesIds::CORE_TYPE_ANN);
    TTransformationPipeline& AddTypeAnnotationTransformer(EYqlIssueCode issueCode = TIssuesIds::CORE_TYPE_ANN);

    TTransformationPipeline& Add(TAutoPtr<IGraphTransformer> transformer, const TString& stageName,
        EYqlIssueCode issueCode = TIssuesIds::DEFAULT_ERROR, const TString& issueMessage = {});
    TTransformationPipeline& Add(IGraphTransformer& transformer, const TString& stageName,
        EYqlIssueCode issueCode = TIssuesIds::DEFAULT_ERROR, const TString& issueMessage = {});

    TAutoPtr<IGraphTransformer> Build(bool useIssueScopes = true);
    TAutoPtr<IGraphTransformer> BuildWithNoArgChecks(bool useIssueScopes = true);

    TIntrusivePtr<TTypeAnnotationContext> GetTypeAnnotationContext() const;

private:
    TIntrusivePtr<TTypeAnnotationContext> TypeAnnotationContext_;
    TVector<TTransformStage> Transformers_;
};

struct IPipelineConfigurator
{
    virtual ~IPipelineConfigurator() = default;

    virtual void AfterCreate(TTransformationPipeline* pipeline) const = 0;
    virtual void AfterTypeAnnotation(TTransformationPipeline* pipeline) const = 0;
    virtual void AfterOptimize(TTransformationPipeline* pipeline) const = 0;
};

} // namspace NYql

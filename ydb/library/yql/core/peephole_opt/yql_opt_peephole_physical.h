#pragma once

#include <ydb/library/yql/core/yql_data_provider.h>
#include <ydb/library/yql/core/yql_type_annotation.h>

namespace NYql {

struct IPipelineConfigurator;

struct TPeepholeSettings {
    const IPipelineConfigurator* CommonConfig = nullptr;
    const IPipelineConfigurator* FinalConfig = nullptr;
    bool WithFinalStageRules = true;
    bool WithNonDeterministicRules = true;
};

IGraphTransformer::TStatus PeepHoleOptimizeNode(const TExprNode::TPtr& input, TExprNode::TPtr& output,
    TExprContext& ctx, TTypeAnnotationContext& types, IGraphTransformer* typeAnnotator,
    bool& hasNonDeterministicFunctions, const TPeepholeSettings& peepholeSettings = {});

THolder<IGraphTransformer> MakePeepholeOptimization(TTypeAnnotationContextPtr typeAnnotationContext, const IPipelineConfigurator* config = nullptr);

}

#pragma once

#include <ydb/core/kqp/opt/kqp_opt.h>

namespace NKikimr::NKqp::NOpt {

TAutoPtr<NYql::IGraphTransformer> CreateKqpTxPeepholeTransformer(
    NYql::IGraphTransformer* typeAnnTransformer,
    NYql::TTypeAnnotationContext& typesCtx, 
    const NYql::TKikimrConfiguration::TPtr& config, 
    bool withFinalStageRules = true,
    TSet<TString> disabledOpts = {}
);

TAutoPtr<NYql::IGraphTransformer> CreateKqpTxsPeepholeTransformer(
    TAutoPtr<NYql::IGraphTransformer> typeAnnTransformer,
    NYql::TTypeAnnotationContext& typesCtx, 
    const NYql::TKikimrConfiguration::TPtr& config
);

NYql::IGraphTransformer::TStatus PeepHoleOptimize(const NYql::NNodes::TExprBase& program, NYql::TExprNode::TPtr& newProgram, NYql::TExprContext& ctx,
    NYql::IGraphTransformer& typeAnnTransformer, NYql::TTypeAnnotationContext& typesCtx, NYql::TKikimrConfiguration::TPtr config,
    bool allowNonDeterministicFunctions, bool withFinalStageRules, TSet<TString> disabledOpts);

} // namespace NKikimr::NKqp::NOpt

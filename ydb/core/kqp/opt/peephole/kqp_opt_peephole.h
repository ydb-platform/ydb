#pragma once

#include <ydb/core/kqp/common/kqp_transform.h>
#include <ydb/core/kqp/opt/kqp_opt.h>

namespace NKikimr::NKqp::NOpt {

TAutoPtr<NYql::IGraphTransformer> CreateKqpTxPeepholeTransformer(NYql::IGraphTransformer* typeAnnTransformer,
    NYql::TTypeAnnotationContext& typesCtx, const NYql::TKikimrConfiguration::TPtr& config, bool withFinalStageRules = true);

TAutoPtr<NYql::IGraphTransformer> CreateKqpTxsPeepholeTransformer(TAutoPtr<NYql::IGraphTransformer> typeAnnTransformer,
    NYql::TTypeAnnotationContext& typesCtx, const NYql::TKikimrConfiguration::TPtr& config);

NYql::IGraphTransformer::TStatus ReplaceNonDetFunctionsWithParams(NYql::TExprNode::TPtr& input, NYql::TExprContext& ctx,
     THashMap<TString, NYql::NNodes::TKqpParamBinding>* paramBindings = nullptr);

} // namespace NKikimr::NKqp::NOpt

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

} // namespace NKikimr::NKqp::NOpt

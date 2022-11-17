#pragma once

#include <ydb/core/kqp/opt/kqp_opt.h>

namespace NKikimr::NKqp::NOpt {

struct TKqpOptimizeContext;

TAutoPtr<NYql::IGraphTransformer> CreateKqpLogOptTransformer(const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx,
    NYql::TTypeAnnotationContext& typesCtx, const NYql::TKikimrConfiguration::TPtr& config);

} // namespace NKikimr::NKqp::NOpt

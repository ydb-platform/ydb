#pragma once

#include <ydb/core/kqp/opt/kqp_opt.h>

namespace NKikimr::NKqp::NOpt {

struct TKqpOptimizeContext;

TAutoPtr<NYql::IGraphTransformer> CreateKqpPhyOptTransformer(const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx,
    NYql::TTypeAnnotationContext& typesCtx, const TKikimrConfiguration::TPtr& config);

} // namespace NKikimr::NKqp::NOpt

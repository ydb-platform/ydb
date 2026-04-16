#pragma once

#include <ydb/core/kqp/opt/kqp_opt.h>

namespace NKikimr::NKqp::NOpt {

struct TKqpOptimizeContext;

TAutoPtr<NYql::IGraphTransformer> CreateKqpPhyOptTransformer(const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx,
    NYql::TTypeAnnotationContext& typesCtx, const NYql::TKikimrConfiguration::TPtr& config, TAutoPtr<NYql::IGraphTransformer> &&typeAnnTransformer);

/// DISTINCT pushdown for hash combine / CombineCore over KqpBlockReadOlapTableRanges (must run on KqpPhysicalQuery).
TAutoPtr<NYql::IGraphTransformer> CreateKqpPushOlapDistinctOnPhysicalQueryTransformer(
    const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx);

} // namespace NKikimr::NKqp::NOpt

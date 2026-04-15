#pragma once

#include <ydb/core/kqp/opt/kqp_opt.h>

#include <yql/essentials/core/yql_type_annotation.h>

namespace NKikimr::NKqp::NOpt {

struct TKqpOptimizeContext;

TAutoPtr<NYql::IGraphTransformer> CreateKqpPhyOptTransformer(const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx,
    NYql::TTypeAnnotationContext& typesCtx, const NYql::TKikimrConfiguration::TPtr& config, TAutoPtr<NYql::IGraphTransformer> &&typeAnnTransformer);

TAutoPtr<NYql::IGraphTransformer> CreateKqpOlapDistinctPrePhyStagesTransformer(
    const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx,
    NYql::TTypeAnnotationContext& typesCtx);

} // namespace NKikimr::NKqp::NOpt

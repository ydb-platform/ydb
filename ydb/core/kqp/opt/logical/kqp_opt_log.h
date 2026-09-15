#pragma once

#include <util/generic/ptr.h>

namespace NYql {

class IGraphTransformer;
struct TTypeAnnotationContext;
struct TKikimrConfiguration;

} // namespace NYql

namespace NKikimr::NKqp::NOpt {

struct TKqpOptimizeContext;

TAutoPtr<NYql::IGraphTransformer> CreateKqpLogOptTransformer(
    TIntrusivePtr<TKqpOptimizeContext>& kqpCtx,
    NYql::TTypeAnnotationContext& typesCtx, 
    const TIntrusivePtr<NYql::TKikimrConfiguration>& config
);

} // namespace NKikimr::NKqp::NOpt

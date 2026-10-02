#pragma once

#include <util/generic/ptr.h>

namespace NYql {

class IGraphTransformer;
struct TTypeAnnotationContext;

} // namespace NYql

namespace NKikimr::NKqp::NOpt {

struct TKqpOptimizeContext;

TAutoPtr<NYql::IGraphTransformer> CreateKqpPhyOptTransformer(const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx, NYql::TTypeAnnotationContext& typesCtx);

} // namespace NKikimr::NKqp::NOpt

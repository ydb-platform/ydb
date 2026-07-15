#pragma once

#include <util/generic/ptr.h>

namespace NYql {

struct TKikimrConfiguration;
class IGraphTransformer;
struct TTypeAnnotationContext;

} // namespace NYql

namespace NKikimr::NKqp {

TAutoPtr<NYql::IGraphTransformer> CreateKqpTxsHashFuncPropagateTransformer(
    NYql::TTypeAnnotationContext& typesCtx,
    const TIntrusivePtr<NYql::TKikimrConfiguration>& config
);

} // namespace NKikimr::NKqp

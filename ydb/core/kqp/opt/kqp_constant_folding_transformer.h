#pragma once

#include <util/generic/ptr.h>

namespace NYql {

class IGraphTransformer;
struct TTypeAnnotationContext;
struct TKikimrConfiguration;

} // namespace NYql

namespace NKikimr::NKqp {

namespace NOpt {

struct TKqpOptimizeContext;

} // namespace NOpt

/**
 * Constant folding transformer finds constant expressions in FlatMaps, evaluates them and
 * substitutes the result in the AST
*/
TAutoPtr<NYql::IGraphTransformer> CreateKqpConstantFoldingTransformer(const TIntrusivePtr<NOpt::TKqpOptimizeContext>& kqpCtx, NYql::TTypeAnnotationContext& typeCtx, const TIntrusivePtr<NYql::TKikimrConfiguration>& config);

} // namespace NKikimr::NKqp

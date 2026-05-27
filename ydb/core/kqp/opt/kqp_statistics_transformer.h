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
struct TKqpProviderContext;

} // namespace NOpt

/***
 * Statistics transformer is a transformer that propagates statistics and costs from
 * the leaves of the plan DAG up to the root of the DAG. It handles a number of operators,
 * but will simply stop propagation if in encounters an operator that it has no rules for.
 * One of such operators is EquiJoin, but there is a special rule to handle EquiJoin.
*/
TAutoPtr<NYql::IGraphTransformer> CreateKqpStatisticsTransformer(const TIntrusivePtr<NOpt::TKqpOptimizeContext>& kqpCtx,
    NYql::TTypeAnnotationContext& typeCtx, const TIntrusivePtr<NYql::TKikimrConfiguration>& config, const NOpt::TKqpProviderContext& pctx);

} // namespace NKikimr::NKqp
#pragma once

#include <yql/essentials/core/yql_graph_transformer.h>
#include <yql/essentials/core/cbo/cbo_optimizer_new.h>

#include <util/generic/ptr.h>

namespace NYql {

struct TDqState;
using TDqStatePtr = TIntrusivePtr<TDqState>;

THolder<IGraphTransformer> CreateDqsStatisticsTransformer(TDqStatePtr state, const IProviderContext& ctx);

} // namespace NYql

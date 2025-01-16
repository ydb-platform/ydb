#pragma once

#include <yql/essentials/core/yql_graph_transformer.h>

#include <util/generic/ptr.h>

namespace NYql {
    struct TDqState;
    using TDqStatePtr = TIntrusivePtr<TDqState>;

    THolder<IGraphTransformer> CreateDqsRecaptureTransformer(TDqStatePtr state);

} // namespace NYql

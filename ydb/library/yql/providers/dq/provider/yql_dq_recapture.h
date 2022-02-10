#pragma once

#include <ydb/library/yql/core/yql_graph_transformer.h>

#include <util/generic/ptr.h>

namespace NYql {
    struct TDqState;
    using TDqStatePtr = TIntrusivePtr<TDqState>;

    THolder<IGraphTransformer> CreateDqsRecaptureTransformer(TDqStatePtr state);

} // namespace NYql

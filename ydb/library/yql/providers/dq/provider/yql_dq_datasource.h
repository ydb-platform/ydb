#pragma once

#include <ydb/library/yql/core/yql_data_provider.h>

#include <util/generic/ptr.h>

namespace NYql {
    struct TDqState;
    using TDqStatePtr = TIntrusivePtr<TDqState>;

    using TExecTransformerFactory = std::function<IGraphTransformer*(const TDqStatePtr& state)>;

    TIntrusivePtr<IDataProvider> CreateDqDataSource(const TDqStatePtr& state, TExecTransformerFactory execTransformerFactory);
} // namespace NYql

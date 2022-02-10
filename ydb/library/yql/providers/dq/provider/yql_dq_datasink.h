#pragma once

#include <ydb/library/yql/core/yql_data_provider.h>

namespace NYql {
    struct TDqState;
    using TDqStatePtr = TIntrusivePtr<TDqState>;

    TIntrusivePtr<IDataProvider> CreateDqDataSink(const TDqStatePtr& state);
} // namespace NYql

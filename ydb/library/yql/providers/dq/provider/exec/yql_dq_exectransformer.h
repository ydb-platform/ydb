#pragma once 
 
#include <ydb/library/yql/core/yql_data_provider.h>
 
#include <util/generic/ptr.h>

namespace NYql { 
    struct TDqState; 
    using TDqStatePtr = TIntrusivePtr<TDqState>;
 
    IGraphTransformer* CreateInMemoryExecTransformer(const TDqStatePtr& state); 
} // namespace NYql 

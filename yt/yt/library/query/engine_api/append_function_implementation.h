#pragma once

#include "public.h"

#include <yt/yt/library/query/base/functions_common.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

void AppendFunctionImplementation(
    const TFunctionProfilerMapPtr& functionProfilers,
    const TAggregateProfilerMapPtr& aggregateProfilers,
    bool functionIsAggregate,
    const TString& functionName,
    const TString& functionSymbolName,
    ECallingConvention functionCallingConvention,
    TSharedRef functionChunkSpecsFingerprint,
    TType functionRepeatedArgType,
    int functionRepeatedArgIndex,
    bool functionUseFunctionContext,
    const TSharedRef& functionImpl);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

#include "builtin_function_profiler.h"

#include "public.h"

#include <yt/yt/core/misc/error.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

const IFunctionCodegenPtr& TFunctionProfilerMap::GetFunction(const TString& functionName) const
{
    auto found = this->find(functionName);
    if (found == this->end()) {
        THROW_ERROR_EXCEPTION("Code generator not found for regular function %Qv",
            functionName);
    }
    return found->second;
}

const IAggregateCodegenPtr& TAggregateProfilerMap::GetAggregate(const TString& functionName) const
{
    auto found = this->find(functionName);
    if (found == this->end()) {
        THROW_ERROR_EXCEPTION("Code generator not found for aggregate function %Qv",
            functionName);
    }
    return found->second;
}

////////////////////////////////////////////////////////////////////////////////

Y_WEAK const TConstFunctionProfilerMapPtr GetBuiltinFunctionProfilers()
{
    // Proper implementation resides in yt/yt/library/query/engine/builtin_function_profiler.cpp.
    YT_ABORT();
}

Y_WEAK const TConstAggregateProfilerMapPtr GetBuiltinAggregateProfilers()
{
    // Proper implementation resides in yt/yt/library/query/engine/builtin_function_profiler.cpp.
    YT_ABORT();
}

Y_WEAK const TConstRangeExtractorMapPtr GetBuiltinRangeExtractors()
{
    // Proper implementation resides in yt/yt/library/query/engine/builtin_function_profiler.cpp.
    YT_ABORT();
}

Y_WEAK const TConstConstraintExtractorMapPtr GetBuiltinConstraintExtractors()
{
    // Proper implementation resides in yt/yt/library/query/engine/builtin_function_profiler.cpp.
    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_WEAK_REFCOUNTED_TYPE(IFunctionCodegen)
DEFINE_WEAK_REFCOUNTED_TYPE(IAggregateCodegen)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

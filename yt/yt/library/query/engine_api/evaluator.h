#pragma once

#include "builtin_function_profiler.h"
#include "public.h"

#include <yt/yt/library/query/base/callbacks.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

struct IEvaluator
    : public virtual TRefCounted
{
    virtual TQueryStatistics Run(
        const TConstBaseQueryPtr& query,
        const ISchemafulUnversionedReaderPtr& reader,
        const IUnversionedRowsetWriterPtr& writer,
        const TJoinSubqueryProfiler& joinProfiler,
        const TConstFunctionProfilerMapPtr& functionProfilers,
        const TConstAggregateProfilerMapPtr& aggregateProfilers,
        const IMemoryChunkProviderPtr& memoryChunkProvider,
        const TQueryBaseOptions& options) = 0;
};

DEFINE_REFCOUNTED_TYPE(IEvaluator)

IEvaluatorPtr CreateEvaluator(
    TExecutorConfigPtr config,
    const NProfiling::TProfiler& profiler = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

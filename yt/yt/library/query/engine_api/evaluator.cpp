#include "evaluator.h"

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

Y_WEAK IEvaluatorPtr CreateEvaluator(TExecutorConfigPtr /*config*/, const NProfiling::TProfiler& /*profiler*/)
{
    // Proper implementation resides in yt/yt/library/query/engine/evaluator.cpp.
    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

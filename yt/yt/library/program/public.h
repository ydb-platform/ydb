#pragma once

#include <yt/yt/core/misc/public.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TBuildInfo)
DECLARE_REFCOUNTED_CLASS(TRpcConfig)
DECLARE_REFCOUNTED_CLASS(TTCMallocConfig)
DECLARE_REFCOUNTED_CLASS(TStockpileConfig)
DECLARE_REFCOUNTED_CLASS(TSingletonsConfig)
DECLARE_REFCOUNTED_CLASS(TSingletonsDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TDiagnosticDumpConfig)
DECLARE_REFCOUNTED_CLASS(THeapSizeLimitConfig)
DECLARE_REFCOUNTED_CLASS(THeapProfilerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

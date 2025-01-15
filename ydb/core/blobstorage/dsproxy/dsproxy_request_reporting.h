#pragma once

#include "defs.h"
#include <array>

#include <ydb/core/util/counted_leaky_bucket.h>

namespace NKikimr {

bool AllowToReport(NKikimrBlobStorage::EPutHandleClass handleClass);
bool AllowToReport(NKikimrBlobStorage::EGetHandleClass handleClass);

IActor* CreateRequestReportingThrottler(const TControlWrapper& bucketSize, const TControlWrapper& leakDurationMs,
    const TControlWrapper& leakRate);

} // namespace NKikimr

#pragma once

#include "defs.h"
#include <array>

namespace NKikimr {

bool AllowToReport(NKikimrBlobStorage::EPutHandleClass handleClass);
bool AllowToReport(NKikimrBlobStorage::EGetHandleClass handleClass);

IActor* CreateRequestReportingThrottler(TDuration updatePermissionsDelay);

} // namespace NKikimr

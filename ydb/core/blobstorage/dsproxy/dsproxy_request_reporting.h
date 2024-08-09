#pragma once

#include "defs.h"
#include <array>

namespace NKikimr {

bool AllowToReportPut(NKikimrBlobStorage::EPutHandleClass& handleClass);
bool AllowToReportGet(NKikimrBlobStorage::EGetHandleClass& handleClass);

IActor* CreateRequestReportingThrottler(TDuration updatePermissionsDelay);

} // namespace NKikimr

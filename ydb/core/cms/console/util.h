#pragma once
#include "defs.h"

#include <ydb/core/base/tablet_pipe.h>

namespace NKikimr::NConsole {

NTabletPipe::TClientRetryPolicy FastConnectRetryPolicy();

TString KindsToString(const TDynBitMap &kinds);

TString KindsToString(const THashSet<ui32> &kinds);

TString KindsToString(const TVector<ui32> &kinds);

} // namespace NKikimr::NConsole

#pragma once
#include "defs.h"

#include <ydb/core/base/tablet_pipe.h>

namespace NKikimr::NConsole {

    NTabletPipe::TClientRetryPolicy FastConnectRetryPolicy();

} // namespace NKikimr::NConsole

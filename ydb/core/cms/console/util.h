#pragma once 
#include "defs.h" 
 
#include <ydb/core/base/tablet_pipe.h>
 
namespace NKikimr { 
namespace NConsole { 
 
    NTabletPipe::TClientRetryPolicy FastConnectRetryPolicy(); 
 
} // namespace NConsole 
} // namespace NKikimr 

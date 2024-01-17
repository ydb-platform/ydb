#include "yt_shutdown.h"

#ifndef KIKIMR_DISABLE_YT
#include <yt/yt/core/misc/shutdown.h>
#endif

void ShutdownYT() {
#ifndef KIKIMR_DISABLE_YT
    NYT::Shutdown();
#endif
}

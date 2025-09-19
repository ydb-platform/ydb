#include "config.h"

namespace NYT::NCoreDump {

////////////////////////////////////////////////////////////////////////////////

void TCoreDumperConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path);
    registrar.Parameter("pattern", &TThis::Pattern)
        .Default("core.%CORE_DATETIME.%CORE_PID.%CORE_SIG.%CORE_THREAD_NAME-%CORE_REASON");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCoreDump

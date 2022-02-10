#include "shuttle.h"

#include "probes.h"

#include <util/system/backtrace.h>
#include <util/stream/str.h>

namespace NLWTrace {
    LWTRACE_USING(LWTRACE_INTERNAL_PROVIDER);

    inline TString CurrentBackTrace() {
        TStringStream ss;
        FormatBackTrace(&ss);
        return ss.Str();
    }

    void TOrbit::LockFailed() {
        LWPROBE(OrbitIsUsedConcurrentlyError, CurrentBackTrace());
    }
} // namespace NLWTrace

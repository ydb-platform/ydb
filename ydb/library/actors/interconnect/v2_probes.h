#pragma once

#include <library/cpp/lwtrace/all.h>

#define INTERCONNECT_V2_PROVIDER(PROBE, EVENT, GROUPS, TYPES, NAMES) \
    PROBE(LoadEvent, GROUPS("ICV2Load"), TYPES(TString, TString, ui64, ui64), NAMES("stage", "driver", "seq", "cookie")) \
    PROBE(LoadUndelivered, GROUPS("ICV2Load"), TYPES(TString, ui64, ui32, ui32, bool), NAMES("driver", "seq", "sourceType", "reason", "unsure")) \
    PROBE(LoadProgress, GROUPS("ICV2Load"), TYPES(TString, ui32, ui32, ui32), NAMES("driver", "sent", "received", "inFly")) \
    PROBE(LoadPending, GROUPS("ICV2Load"), TYPES(TString, ui64, ui32), NAMES("driver", "seq", "attempts")) \
    PROBE(Command, GROUPS("ICV2IO"), TYPES(ui64, ui64, ui64, ui32, ui64), NAMES("session", "seq", "expected", "type", "cookie")) \
    PROBE(IO, GROUPS("ICV2IO"), TYPES(ui64, TString, bool, i64, ui64), NAMES("session", "operation", "xdc", "resultOrSize", "unsent")) \
    PROBE(SessionQueue, GROUPS("ICV2State"), TYPES(ui64, ui64, ui64, ui64, ui64, bool), NAMES("session", "incoming", "expected", "heapSize", "heapMin", "trafficPending")) \
    PROBE(SessionIO, GROUPS("ICV2State"), TYPES(ui64, ui64, ui64, ui64, ui64, bool, bool, bool, bool), NAMES("session", "sent", "received", "unsent", "xdcUnsent", "readPending", "writePending", "xdcReadPending", "xdcWritePending"))

LWTRACE_DECLARE_PROVIDER(INTERCONNECT_V2_PROVIDER)

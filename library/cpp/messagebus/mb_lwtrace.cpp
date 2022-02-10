#include "mb_lwtrace.h"

#include <library/cpp/lwtrace/all.h>

#include <util/generic/singleton.h>

LWTRACE_DEFINE_PROVIDER(LWTRACE_MESSAGEBUS_PROVIDER)

void NBus::InitBusLwtrace() {
    // Function is nop, and needed only to make sure TBusLwtraceInit loaded.
    // It won't be necessary when pg@ implements GLOBAL in arc.
}

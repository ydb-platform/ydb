#include "client_pid.h"
#include "string_helpers.h"

#ifdef _win_
    // copied from util/system/getpid.cpp
    // to avoid extra util dep.
    #include <Windows.h>
    #if defined(NTDDI_WIN8) && (NTDDI_VERSION >= NTDDI_WIN8)
        #include <processthreadsapi.h>
    #endif
#else
    #include <unistd.h>
#endif

namespace NYdb {

namespace {
ui32 GetProcessId() {
#ifdef _win_
    return GetCurrentProcessId();
#else
    return getpid();
#endif
}

}

TStringType GetClientPIDHeaderValue() {
    return ToStringType(std::to_string(GetProcessId()));
}

}
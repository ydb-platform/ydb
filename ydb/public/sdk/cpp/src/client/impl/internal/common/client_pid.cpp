#define INCLUDE_YDB_INTERNAL_H
#include "client_pid.h"

#include <util/system/defaults.h>

#include <cstdint>

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

namespace NYdb::inline Dev {

namespace {
std::uint32_t GetProcessId() {
#ifdef _win_
    return GetCurrentProcessId();
#else
    return getpid();
#endif
}

}

std::string GetClientPIDHeaderValue() {
    return std::to_string(GetProcessId());
}

}

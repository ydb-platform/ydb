#pragma once

#include <library/cpp/lwtrace/all.h>

#include <util/generic/string.h>

#define LWTRACE_MESSAGEBUS_PROVIDER(PROBE, EVENT, GROUPS, TYPES, NAMES)                                          \
    PROBE(Error, GROUPS("MessagebusRare"), TYPES(TString, TString, TString), NAMES("status", "address", "misc")) \
    PROBE(ServerUnknownVersion, GROUPS("MessagebusRare"), TYPES(TString, ui32), NAMES("address", "version"))     \
    PROBE(Accepted, GROUPS("MessagebusRare"), TYPES(TString), NAMES("address"))                                  \
    PROBE(Disconnected, GROUPS("MessagebusRare"), TYPES(TString), NAMES("address"))                              \
    PROBE(Read, GROUPS(), TYPES(ui32), NAMES("size"))                                                            \
    /**/

LWTRACE_DECLARE_PROVIDER(LWTRACE_MESSAGEBUS_PROVIDER)

namespace NBus {
    void InitBusLwtrace();
}

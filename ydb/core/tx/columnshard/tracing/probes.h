#pragma once

#include <library/cpp/lwtrace/all.h>

namespace NKikimr::NColumnShard  {

#define YDB_CS(PROBE, EVENT, GROUPS, TYPES, NAMES) \
    PROBE(EvWrite, \
        GROUPS("Write"), \
        TYPES(ui64, TString, ui64, ui64, TDuration, ui64, TString, bool, bool, TString, TString), \
        NAMES("tabletId", "sender", "cookie", "txId", "writeTimeout", "size", "modificationType", "isBulk", "success", "status", "reason")) \
    PROBE(EvWriteResult, \
        GROUPS("Write"), \
        TYPES(ui64, TString, ui64, ui64, TString, bool, TString), \
        NAMES("tabletId", "sender", "txId" "cookie", "type", "success", "reason")) \

LWTRACE_DECLARE_PROVIDER(YDB_CS)

}
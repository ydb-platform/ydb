#pragma once

#include "defs.h"

namespace NKikimr::NBlobDepot {

    #define KEYVALUE_TABLE(BODY) \
        TABLE_CLASS("table") { \
            TABLEHEAD() { \
                TABLER() { \
                    TABLEH() { s << "Parameter"; } \
                    TABLEH() { s << "Value"; } \
                } \
            } \
            TABLEBODY() { \
                BODY \
            } \
        }

    #define KEYVALUE_P(KEY, VALUE) \
        TABLER() { \
            TABLED() { __stream << (KEY); } \
            TABLED() { __stream << (VALUE); } \
        }

    inline TString FormatByteSize(ui64 size) {
        static const char *suffixes[] = {"B", "KiB", "MiB", "GiB", "TiB", "PiB", nullptr};
        TStringStream s;
        FormatHumanReadable(s, size, 1024, 2, suffixes);
        return s.Str();
    }

} // NKikimr::NBlobDepot

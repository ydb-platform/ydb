#include "local_flags.h"

#include <util/stream/str.h>
#include <util/string/printf.h>

using namespace NBus;
using namespace NBus::NPrivate;

TString NBus::NPrivate::LocalFlagSetToString(ui32 flags0) {
    if (flags0 == 0) {
        return "0";
    }

    ui32 flags = flags0;

    TStringStream ss;
#define P(name, value, ...)          \
    do                               \
        if (flags & value) {         \
            if (!ss.Str().empty()) { \
                ss << "|";           \
            }                        \
            ss << #name;             \
            flags &= ~name;          \
        }                            \
    while (false);
    MESSAGE_LOCAL_FLAGS_MAP(P)
    if (flags != 0) {
        return Sprintf("0x%x", unsigned(flags0));
    }
    return ss.Str();
}

#include "hullbase_barrier.h"

namespace NKikimr {

#define PARSE_NUM(field_def, field_name_str, base) \
    field_def = strtoll(str, &endptr, base); \
    endptr = const_cast<char*>(skipSpaces(endptr)); \
    if (!(endptr && *endptr == ':')) { \
        errorExplanation = "Can't find trailing ':' after " field_name_str; \
        return false; \
    } \
    str = endptr + 1;

    bool TKeyBarrier::Parse(TKeyBarrier &out, const TString &buf, TString &errorExplanation) {
        const char *str = buf.data();
        char *endptr = nullptr;

        auto skipSpaces = [] (const char* str) {
            while (str && *str && *str == ' ')
                ++str;
            return str;
        };

        str = skipSpaces(str);
        if (*str != '[') {
            errorExplanation = "Value doesn't start with '['";
            return false;
        }
        ++str;
        PARSE_NUM(const ui64 tabletID, "tablet id", 10);
        PARSE_NUM(const ui64 channel, "channel", 10);
        PARSE_NUM(const ui64 gen, "generation", 10);

        const ui64 genCounter = strtoll(str, &endptr, 10);

        str = skipSpaces(endptr);
        if (!(str && *str && *str == ']')) {
            errorExplanation = "Can't find trailing ']' after generation counter";
            return false;
        }
        str = skipSpaces(str + 1);
        if (!(str && *str == '\0')) {
            errorExplanation = "Garbage after ']'";
            return false;
        }

        out = TKeyBarrier(tabletID, channel, gen, genCounter, false);
        return true;
    }

} // NKikimr

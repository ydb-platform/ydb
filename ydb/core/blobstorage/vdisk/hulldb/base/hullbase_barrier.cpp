#include "hullbase_barrier.h"

namespace NKikimr {

    static const char *SkipSpaces(const char *str) {
        while (str && *str && *str == ' ')
            ++str;
        return str;
    }


#define PARSE_NUM(field_def, field_name_str, base) \
field_def = strtoll(str, &endptr, base); \
endptr = const_cast<char*>(SkipSpaces(endptr)); \
if (!(endptr && *endptr == ':')) { \
errorExplanation = "Can't find trailing ':' after " field_name_str; \
return false; \
} \
str = endptr + 1;


    bool TKeyBarrier::Parse(TKeyBarrier &out, const TString &buf, TString &errorExplanation) {
        const char *str = buf.data(); 
        char *endptr = nullptr;

        str = SkipSpaces(str);
        if (*str != '[') {
            errorExplanation = "Value doesn't start with '['";
            return false;
        }
        ++str;
        PARSE_NUM(ui64 tabletID, "tablet id", 16);
        PARSE_NUM(ui64 channel, "channel", 10);
        PARSE_NUM(ui64 gen, "generation", 10);
        ui64 genCounter = strtoll(str, &endptr, 10);
        str = SkipSpaces(endptr);

        if (!(str && *str && *str == ']')) {
            errorExplanation = "Can't find trailing ']' after generation counter";
            return false;
        }
        str = SkipSpaces(str + 1);
        if (!(str && *str == '\0')) {
            errorExplanation = "Garbage after ']'";
            return false;
        }

        out = TKeyBarrier(tabletID, channel, gen, genCounter, false);
        return true;
    }

} // NKikimr

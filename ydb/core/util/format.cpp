#include "format.h"
#include <cstdlib>

namespace NKikimr {

    void FormatHumanReadable(IOutputStream& out, ui64 number, ui32 base, unsigned fracw, const char *suffixes[]) {
        Y_ABORT_UNLESS(suffixes[0]);

        ui64 exp = 1;
        ui64 top = base;
        while (suffixes[1] && number >= top) {
            ++suffixes;
            exp = top;
            top *= base;
        }

        const auto& d = std::lldiv(number, exp);
        static ui64 fparts[] = {0, 10, 100, 1000, 10000, 100000, 1000000};
        fracw = exp != 1 ? std::min<unsigned>(fracw, std::size(fparts) - 1) : 0;
        const ui64 frac = fparts[fracw] * d.rem / exp;

        char num[32];
        int numLen = snprintf(num, sizeof(num), "%lld", d.quot);
        const char *in = num + numLen;

        char res[128];
        const int numSpaces = (numLen - 1) / 3;
        char *end = res + numLen + numSpaces;
        {
            char *out = end;
            int r = 0;
            while (numLen--) {
                *--out = *--in;
                if (++r == 3 && numLen) {
                    *--out = ' ';
                    r = 0;
                }
            }
            Y_ABORT_UNLESS(out == res);
        }

        if (fracw) {
            end += snprintf(end, sizeof(res) - (end - res), ".%0*" PRIu64, fracw, frac);
        }

        if (**suffixes) {
            end += snprintf(end, sizeof(res) - (end - res), " %s", *suffixes);
        }

        out << TStringBuf(res, end);
    }

} // NKikimr

#pragma once

#include "util_fmt_desc.h"

namespace NKikimr {
namespace NFmt {

    struct TDelay {
        TDelay(TDuration val): Val(val) { }

        TOut& Do(TOut &out) const noexcept
        {
            const ui64 grid[] =  {
                    60ull,          /* minute                   */
                    3600ull,        /* hour                     */
                    86400ull,       /* day                      */
                    604800ull,      /* week                     */
                    8553600ull,     /* 99 days                  */
                    31557600ull,    /* Julian 365.25 days year  */
                    3155760000ull,  /* Julian 100 years century */
            };

            if (Val == TDuration::Max()) return out << "undef~";

            const auto secs = Val.Seconds();

            if (secs < grid[0]) return Small(out, secs);
            if (secs < grid[1]) return Large(out, secs, 'm', grid[0]);
            if (secs < grid[2]) return Large(out, secs, 'h', grid[1]);
            if (secs < grid[4]) return Large(out, secs, 'd', grid[2]);
            if (secs < grid[5]) return Large(out, secs, 'w', grid[3]);
            if (secs < grid[6]) return Large(out, secs, 'y', grid[5]);

            return Large(out, secs, 'c', grid[6]);
        }

    protected:
        inline TOut& Small(TOut &out, ui64 secs) const noexcept
        {
            char ln_[8];

            auto ms = Val.MilliSeconds() - secs * 1000;

            snprintf(ln_, sizeof(ln_), "%zu.%.03zu", secs, ms);

            return ln_[5] = 's', out.Write(ln_, 6), out;
        }

        TOut& Large(TOut &out, double sec, char suff, ui64 base) const
        {
            char ln_[16];

            snprintf(ln_, sizeof(ln_), "%.03f", sec / base);

            return ln_[5] = suff, out.Write(ln_, 6), out;
        }

        TDuration Val;
    };

    inline TOut& operator<<(TOut &out, const NFmt::TDelay &print)
    {
        return print.Do(out);
    }

}
}

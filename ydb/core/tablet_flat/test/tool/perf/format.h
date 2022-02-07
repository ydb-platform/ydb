#pragma once

#include "desc.h"

#include <util/string/printf.h>
#include <util/system/types.h>
#include <util/datetime/base.h>

namespace NKikiSched {
    namespace NFmt {
        using TOut  = IOutputStream&;

        struct TSerial {
            using TVal  = ui64;

            TSerial(TVal val, size_t dig = 8)
                : Val(val), Dig(Min(dig, TSerial::Lim())) { }

            static constexpr size_t Lim() noexcept
            {
                return sizeof(TVal) * 2;
            }

            TOut& Do(TOut &out) const noexcept
            {
                const char _pad_zero[] = "00000000000000000000";
                const char _pad_ffff[] = "ff..ffffffffffffffff";

                static_assert(TSerial::Lim() + 4 <= sizeof(_pad_ffff), "");

                if (Val != Max<TVal>()) {
                    char ln_[24];

                    auto got = snprintf(ln_, sizeof(ln_), "%" PRIx64, Val);

                    if (got < ssize_t(Dig)) out.Write(_pad_zero, Dig - got);

                    out.Write(ln_, got);

                } else if (Dig >= TSerial::Lim()) {

                    out.Write(_pad_ffff + 4, TSerial::Lim());

                } else {

                    out.Write(_pad_ffff, Max(size_t(6), Dig));
                }

                return out;
            }

            TVal            Val     = 0;
            size_t          Dig     = 0;
        };

        struct TLarge {
            TLarge(ui64 val, bool pad = false): Val(val), Pad(pad) { }

            TOut& Do(TOut &out) const noexcept
            {
                const ui64 grid[] = {
                        0x00000000000003e8, /* K 10 ** 3    */
                        0x00000000000f4240, /* M 10 ** 6    */
                        0x000000003b9aca00, /* G 10 ** 9    */
                        0x000000e8d4a51000, /* T 10 ** 12   */
                        0x00038d7ea4c68000, /* P 10 ** 15   */
                        0x0de0b6b3a7640000, /* E 10 ** 18   */
                };

                if (Val < 10000)   return Small(out);
                if (Val < grid[1]) return Fixed(out, 'K', grid[0]);
                if (Val < grid[2]) return Fixed(out, 'M', grid[1]);
                if (Val < grid[3]) return Fixed(out, 'G', grid[2]);
                if (Val < grid[4]) return Fixed(out, 'T', grid[3]);
                if (Val < grid[5]) return Fixed(out, 'P', grid[4]);

                return Fixed(out, 'E', grid[5]);
            }

        protected:
            inline TOut& Small(TOut &out) const noexcept
            {
                char ln_[8];

                auto got = snprintf(ln_, sizeof(ln_), "   %zu", Val);

                if (got > 7 || got == 0) {
                    out << "?bug";

                } else if (Pad) {
                    out.Write(ln_ + (got - 4), 4);

                } else {
                    out.Write(ln_ + 3, got - 3);
                }

                return out;
            }

            TOut& Fixed(TOut &out, char suff, ui64 base) const noexcept
            {
                char ln_[8];

                snprintf(ln_, sizeof(ln_), "%.2f", double(Val) / base);

                return ln_[3] = suff, out.Write(ln_, 4), out;
            }

            ui64            Val     = 0;
            bool            Pad     = false;
        };


        struct TAverage {
            TAverage(ui64 over, ui64 val, bool pad = true)
                : Pad(pad), Ovr(over), Val(val) { }

            TOut& Do(TOut &out) const noexcept
            {
                if (Val > 0 && Val < Ovr * 100) {
                    char ln[8];

                    snprintf(ln, sizeof(ln), "%.3f", double(Val) / Ovr);

                    return ln[4] = '\0', out << ln;

                } else if (Ovr > 0) {
                    return NFmt::TLarge(Val ? Val / Ovr : 0, Pad).Do(out);

                } else {
                    return out << "+inf";
                }
            }

            bool            Pad     = true;
            ui64            Ovr     = 0;
            ui64            Val     = 0;
        };


        struct TStamp {
            TStamp(TInstant on): On(on) { }

            TOut& Do(TOut &out) const noexcept
            {
                char ln_[24];

                if (On == TInstant::Max()) out << "~inf";

                ui64 sec = On.Seconds(), ms = On.MilliSeconds() - sec * 1000;

                size_t sym = snprintf(ln_, sizeof(ln_), "%zu.%03zus", sec, ms);

                return out.Write(ln_, Min(sym, sizeof(ln_))), out;
            }

        protected:
            TInstant        On;
        };


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

            TDuration       Val;
        };


        struct TDelta {
            TDelta(TInstant on, TInstant to): On(on), To(to) { }

            TOut& Do(TOut &out) const noexcept
            {
                const auto pfx = (On <= To ? '+' : '-');

                return out <<  pfx, TDelay(Max(On, To) - Min(On, To)).Do(out);
            }

        protected:
            TInstant        On;
            TInstant        To;
        };

        inline TOut& operator<<(TOut &out, const NFmt::TSerial &print)
        {
            return print.Do(out);
        }

        inline TOut& operator<<(TOut &out, const NFmt::TLarge &print)
        {
            return print.Do(out);
        }

        inline TOut& operator<<(TOut &out, const NFmt::TAverage &print)
        {
            return print.Do(out);
        }

        inline TOut& operator<<(TOut &out, const NFmt::TStamp &print)
        {
            return print.Do(out);
        }

        inline TOut& operator<<(TOut &out, const NFmt::TDelay &print)
        {
            return print.Do(out);
        }

        inline TOut& operator<<(TOut &out, const NFmt::TDelta &print)
        {
            return print.Do(out);
        }
    }
}

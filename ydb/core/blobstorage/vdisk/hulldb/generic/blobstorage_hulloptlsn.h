#pragma once

#include "defs.h"
#include <util/generic/ylimits.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // TOptLsn
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    class TOptLsn {
    public:
        constexpr TOptLsn() = default;
        TOptLsn(const TOptLsn &) = default;
        TOptLsn &operator =(const TOptLsn &) = default;

        explicit TOptLsn(ui64 lsn) noexcept {
            Y_ABORT_UNLESS(lsn != NotSetLsn);
            Lsn = lsn;
        }

        static TOptLsn CreateFromAny(ui64 lsn) {
            return (lsn == NotSetLsn) ? NotSet : TOptLsn(lsn);
        }

        void SetMax(ui64 lsn) noexcept {
            Y_ABORT_UNLESS(lsn != NotSetLsn);
            if (Lsn == NotSetLsn)
                Lsn = lsn;
            else
                Lsn = Max(Lsn, lsn);
        }

        void SetMax(TOptLsn lsn) noexcept {
            if (Lsn == NotSetLsn) {
                Lsn = lsn.Lsn;
            } else {
                if (lsn.Lsn == NotSetLsn) {
                    // keep current Lsn
                } else {
                    Lsn = Max(Lsn, lsn.Lsn);
                }
            }
        }

        ui64 Value() const noexcept {
            return Lsn;
        }

        void Output(IOutputStream &out) const {
            if (Lsn == NotSetLsn)
                out << "NotSet";
            else
                out << "Set(" << Lsn << ")";
        }

        TString ToString() const {
            TStringStream str;
            Output(str);
            return str.Str();
        }

        bool operator < (ui64 lsn) const noexcept { Y_ABORT_UNLESS(lsn != NotSetLsn); return Lsn != NotSetLsn ? Lsn < lsn : true; }
        bool operator <=(ui64 lsn) const noexcept { Y_ABORT_UNLESS(lsn != NotSetLsn); return Lsn != NotSetLsn ? Lsn <= lsn : true; }
        bool operator > (ui64 lsn) const noexcept { Y_ABORT_UNLESS(lsn != NotSetLsn); return Lsn != NotSetLsn ? Lsn > lsn : false; }
        bool operator >=(ui64 lsn) const noexcept { Y_ABORT_UNLESS(lsn != NotSetLsn); return Lsn != NotSetLsn ? Lsn >= lsn : false; }
        bool operator ==(ui64 lsn) const noexcept { Y_ABORT_UNLESS(lsn != NotSetLsn); return Lsn == lsn; }
        bool operator !=(ui64 lsn) const noexcept { Y_ABORT_UNLESS(lsn != NotSetLsn); return Lsn != lsn; }

        bool operator < (TOptLsn v) const noexcept { return v.Lsn == NotSetLsn ? false : Lsn < v.Lsn; }
        bool operator <=(TOptLsn v) const noexcept { return v.Lsn == NotSetLsn ? Lsn == NotSetLsn : Lsn <= v.Lsn; }

        static const TOptLsn NotSet;
    private:
        // special value for Lsn -- don't have a value
        static constexpr ui64 NotSetLsn = Max<ui64>();
        ui64 Lsn = NotSetLsn;
    };

} // NKikimr


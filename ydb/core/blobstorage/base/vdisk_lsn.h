#pragma once

#include "defs.h"

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////////////////
    // TLsnSeg -- segment of lsns
    // For new records to the recovery log we allocate segments of lsns;
    // Examples:
    // 1. Allocate 2 lsns     --> [356, 357]
    // 2. Allocate single lsn --> [402, 402]
    ////////////////////////////////////////////////////////////////////////////////////////
    struct TLsnSeg {
        ui64 First = 0;
        ui64 Last = 0;

        TLsnSeg() = default;

        TLsnSeg(ui64 first, ui64 last)
            : First(first)
            , Last(last)
        {}

        ui64 Point() const {
            Y_ABORT_UNLESS(First == Last);
            return Last;
        }

        TString ToString() const {
            TStringStream str;
            Output(str);
            return str.Str();
        }

        void Output(IOutputStream &str) const {
            str << "[" << First << ", " << Last << "]";
        }

        bool operator == (const TLsnSeg &seg) const {
            return First == seg.First && Last == seg.Last;
        }

        bool operator <(const TLsnSeg &seg) const {
            return First < seg.First || (First == seg.First && Last < seg.Last);
        }
    };

} // NKikimr

template<>
inline void Out<NKikimr::TLsnSeg>(IOutputStream& os, const NKikimr::TLsnSeg& seg) {
    seg.Output(os);
}


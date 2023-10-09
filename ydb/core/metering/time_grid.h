#pragma once

#include <util/datetime/base.h>

namespace NKikimr {

    // Time grid taking into account the features of billing slots
    struct  TTimeGrid {
        struct TSlot {
            TInstant Start;
            TInstant End;

            TSlot(TInstant start, TInstant end)
                : Start(start)
                , End(end)
            {}
        };

        TDuration Period;

        TTimeGrid(TDuration period)
            : Period(period)
        {
            Y_ABORT_UNLESS(Period >= TDuration::Seconds(1));
            Y_ABORT_UNLESS(Period <= TDuration::Hours(1));
            Y_ABORT_UNLESS(TDuration::Hours(1).MicroSeconds() % Period.MicroSeconds() == 0);
        }

        const TSlot Get(TInstant now) const {
            auto hour = TInstant::Hours(now.Hours());

            auto passedFromHourBegin = now - hour;

            ui64 slotNo = passedFromHourBegin.Seconds() / Period.Seconds();

            return TSlot(hour + slotNo * Period, hour + (slotNo + 1) * Period -  TDuration::Seconds(1));
        }

        const TSlot GetPrev(TSlot p) const {
            return TSlot(p.Start - Period, p.End - Period);
        }

        const TSlot GetNext(TSlot p) const {
            return TSlot(p.Start + Period, p.End + Period);
        }
    };

}   // namespace NKikimr

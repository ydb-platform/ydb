#pragma once

#include "util_fmt_basic.h"

#include <library/cpp/time_provider/time_provider.h>

namespace NKikimr {
namespace NTable {

    class TSpent {
    public:
        /* Basic helper tool for scan operations that helps to account time
            spent for waiting some one resource. It is used by scan actor for
            counting blockage on NBlockIO reads and may be used by IScan impls.
         */

        TSpent(ITimeProvider *prov)
            : Time(prov)
            , Fired(Time->Now())
        {

        }

        void Describe(IOutputStream &out) const noexcept
        {
            const auto now = Time->Now();

            out
                << "Spent{" << NFmt::TDelay(now - Fired)
                << " wa " << NFmt::TDelay(Waits + (now - Since))
                << " cnt " << Interrupts << "}";
        }

        void Alter(bool available) noexcept
        {
            if (bool(available) == bool(Since == TInstant::Max())) {
                /* State isn't changed since last Alter(...) */
            } else if (Since == TInstant::Max()) {
                Since = Time->Now();
                Interrupts++;
            } else {
                Waits += Time->Now() - std::exchange(Since, TInstant::Max());
            }
        }

    private:
        ITimeProvider * const Time = nullptr;

        TInstant Fired = TInstant::Max();
        TInstant Since = TInstant::Max();
        TDuration Waits = TDuration::Zero();
        ui64 Interrupts = 0;
    };

}
}

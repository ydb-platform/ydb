#pragma once

#include "format.h"

namespace NKikimr {
namespace NTable {
namespace NTest {

    class TMeter {
    public:
        TMeter() = delete;

        TMeter(const TMeter&) = delete;

        TMeter(TDuration spent, ui64 least, ui64 limit)
            : Least(least)
            , Limit(Max(least,limit))
            , Start(TInstant::Now())
            , Edge(Start + spent)
        {

        }

        ui64 Count() const
        {
            return Count_;
        }

        ui64 Take(ui64 samples)
        {
            Stop = ((Count_ += samples) >= Limit) || Stop;

            if ((Last += samples) >= 4096 && Count_ > Least) {
                Stop = Stop || Now() >= Edge;
                Last = 0;
            }

            return Stop ? 0 : 4096;
        }

        TString Report() const
        {
            TStringStream out;

            double rate = double(Count_) / (Now() - Start).SecondsFloat();

            out << NKikiSched::NFmt::TLarge(rate);

            return out.Str();
        }

    private:
        const ui64 Least = 1024;
        const ui64 Limit = Max<ui64>();
        const TInstant Start;
        const TInstant Edge;

        bool Stop = false;
        ui64 Count_ = 0;
        ui64 Last = 0;
    };

}
}
}

#include "duration_histogram.h"

#include <util/generic/singleton.h>
#include <util/stream/str.h>

namespace {
    ui64 SecondsRound(TDuration d) {
        if (d.MilliSeconds() % 1000 >= 500) {
            return d.Seconds() + 1;
        } else {
            return d.Seconds();
        }
    }

    ui64 MilliSecondsRound(TDuration d) {
        if (d.MicroSeconds() % 1000 >= 500) {
            return d.MilliSeconds() + 1;
        } else {
            return d.MilliSeconds();
        }
    }

    ui64 MinutesRound(TDuration d) {
        if (d.Seconds() % 60 >= 30) {
            return d.Minutes() + 1;
        } else {
            return d.Minutes();
        }
    }

}

namespace {
    struct TMarks {
        std::array<TDuration, TDurationHistogram::Buckets> Marks;

        TMarks() {
            Marks[0] = TDuration::Zero();
            for (unsigned i = 1; i < TDurationHistogram::Buckets; ++i) {
                if (i >= TDurationHistogram::SecondBoundary) {
                    Marks[i] = TDuration::Seconds(1) * (1 << (i - TDurationHistogram::SecondBoundary));
                } else {
                    Marks[i] = TDuration::Seconds(1) / (1 << (TDurationHistogram::SecondBoundary - i));
                }
            }
        }
    };
}

TString TDurationHistogram::LabelBefore(unsigned i) {
    Y_ABORT_UNLESS(i < Buckets);

    TDuration d = Singleton<TMarks>()->Marks[i];

    TStringStream ss;
    if (d == TDuration::Zero()) {
        ss << "0";
    } else if (d < TDuration::Seconds(1)) {
        ss << MilliSecondsRound(d) << "ms";
    } else if (d < TDuration::Minutes(1)) {
        ss << SecondsRound(d) << "s";
    } else {
        ss << MinutesRound(d) << "m";
    }
    return ss.Str();
}

TString TDurationHistogram::PrintToString() const {
    TStringStream ss;
    for (auto time : Times) {
        ss << time << "\n";
    }
    return ss.Str();
}

#pragma once

#include "defs.h"
#include "top.h"

////////////////////////////////////////////////////////////////////////////
// TBoolWithTimeTracking
////////////////////////////////////////////////////////////////////////////
class TBoolWithTimeTracking {
public:
    struct THistory {
        struct TRec {
            TInstant Time;
            TDuration Duration;
            TRec(TInstant t, TDuration d)
                : Time(t)
                , Duration(d)
            {}
        };

        struct TLess {
            bool operator ()(const TRec &x, const TRec &y) const {
                return x.Duration < y.Duration;
            }
        };

        using TContainer = TVector<TRec>;
        using TTop = ::TTop<TRec, 8, TContainer, TLess>;

        TInstant Start;
        TTop Top;

        void Update(bool curValue, bool newValue) {
            if (!curValue && newValue) {
                Start = NKikimr::TAppData::TimeProvider->Now();
            } else if (curValue && !newValue) {
                TInstant now = NKikimr::TAppData::TimeProvider->Now();
                TDuration d = now - Start;
                Top.Push(TRec(now, d));
            } else
                Y_ABORT("Unexpected case: curValue# %d newValue# %d", int(curValue), int(newValue));
        }

        void Output(IOutputStream &str) {
            TContainer c = Top.GetContainer(); // copy, cause we want to sort it
            auto cmp = [] (const TRec &x, const TRec &y) {
                return x.Duration > y.Duration;
            };
            Sort(c.begin(), c.end(), cmp);
            str << "[History:";
            for (const auto &x : c)
                str << " " << x;
            str << "]";
        }
    };

private:
    bool Value = false;
    THistory History;

public:
    TBoolWithTimeTracking() = default;
    TBoolWithTimeTracking(bool v)
        : Value(v)
    {}

    operator bool() const {
        return Value;
    }

    void operator =(bool v) {
        History.Update(Value, v);
        Value = v;
    }

    void Output(const TString &name, IOutputStream &str) {
        str << name << ": " << (Value ? "true" : "false") << " ";
        History.Output(str);
    }
};

Y_DECLARE_OUT_SPEC(, TBoolWithTimeTracking::THistory::TRec, stream, value) {
    stream << "(" << value.Time << ", " << value.Duration << ")";
}


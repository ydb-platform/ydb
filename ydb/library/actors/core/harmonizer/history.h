#pragma once

#include "defs.h"
#include "debug.h"
#include <ydb/library/actors/util/datetime.h>


namespace NActors {

// TValueHistory takes an accumulating value that only increases
// Then it splits this value by seconds relative to the elapsed time since the last value registration
// Allows finding per-second maximum, minimum, and average values for the last N seconds
// WithTail=false - calculate only fully passed seconds; example: current time is 4.5, GetAvgLastSeconds(2) calculates values from 2.0 to 4.0
// WithTail=true - calculate also partially passed seconds; example: current time is 4.5, GetAvgLastSeconds(2) calculates values from 2.5 to 4.5
// If time passed more seconds than history buffer size, then all values are recalculated by average value

template <ui8 HistoryBufferSize = 8>
struct TValueHistory {

    static constexpr bool CheckBinaryPower(ui64 value) {
        return !(value & (value - 1));
    }

    static_assert(CheckBinaryPower(HistoryBufferSize));

    double History[HistoryBufferSize] = {0.0};
    ui64 HistoryIdx = 0;
    ui64 LastTs = Max<ui64>();
    double LastValue = 0.0;
    double AccumulatedValue = 0.0;
    ui64 AccumulatedTs = 0;

    template <bool WithTail=false>
    double Accumulate(auto op, auto comb, ui8 seconds) const {
        HARMONIZER_HISTORY_PRINT("Accumulate, seconds = ", static_cast<ui16>(seconds), ", WithTail = ", WithTail);
        double acc = AccumulatedValue;
        if (seconds == 0) {
            return acc;
        }
        size_t idx = HistoryIdx;
        ui8 leftSeconds = seconds;
        if constexpr (!WithTail) {
            idx--;
            leftSeconds--;
            if (idx >= HistoryBufferSize) {
                idx = HistoryBufferSize - 1;
            }
            acc = History[idx];
        }
        HARMONIZER_HISTORY_PRINT("Accumulate iteration, acc = ", acc, ", idx = ", static_cast<ui16>(idx), ", leftSeconds = ", static_cast<ui16>(leftSeconds));
        while (leftSeconds) {
            leftSeconds--;
            if (idx == 0) {
                idx = HistoryBufferSize - 1;
            } else {
                idx--;
            }
            if constexpr (!WithTail) {
                acc = op(acc, History[idx]);
            } else if (leftSeconds) {
                acc = op(acc, History[idx]);
            } else {
                ui64 tsInSecond = Us2Ts(1'000'000.0);
                acc = op(acc, History[idx] * (tsInSecond - AccumulatedTs) / tsInSecond);
            }
            HARMONIZER_HISTORY_PRINT("Accumulate iteration, acc = ", acc, ", idx = ", static_cast<ui16>(idx), ", leftSeconds = ", static_cast<ui16>(leftSeconds));
        }
        auto result = comb(acc, seconds);
        HARMONIZER_HISTORY_PRINT("Accumulate return, acc = ", acc, ", duration = ", static_cast<ui16>(seconds), ", result = ", result);
        return result;
    }

    template <bool WithTail=false>
    double GetAvgPartForLastSeconds(ui8 seconds) const {
        auto sum = [](double acc, double value) {
            HARMONIZER_HISTORY_PRINT("calculate sum, acc = ", acc, ", value = ", value, ", acc + value = ", acc + value);
            return acc + value;
        };
        auto avg = [](double sum, double duration) {
            HARMONIZER_HISTORY_PRINT("calculate avg, sum = ", sum, ", duration = ", duration);
            return sum / duration;
        };
        HARMONIZER_HISTORY_PRINT("GetAvgPartForLastSeconds, seconds = ", static_cast<ui16>(seconds), ", WithTail = ", WithTail);
        return Accumulate<WithTail>(sum, avg, seconds);
    }

    double GetAvgPart() const {
        HARMONIZER_HISTORY_PRINT("GetAvgPart, seconds = ", static_cast<ui16>(HistoryBufferSize), ", WithTail = ", true);
        return GetAvgPartForLastSeconds<true>(HistoryBufferSize);
    }

    double GetMaxForLastSeconds(ui8 seconds) const {
        auto max = [](const double& acc, const double& value) {
            HARMONIZER_HISTORY_PRINT("calculate max, acc = ", acc, ", value = ", value, ", Max(acc, value) = ", Max(acc, value));
            return Max(acc, value);
        };
        auto fst = [](const double& value, const double&) { return value; };
        HARMONIZER_HISTORY_PRINT("GetMaxForLastSeconds, seconds = ", static_cast<ui16>(seconds), ", WithTail = ", false);
        return Accumulate<false>(max, fst, seconds);
    }

    double GetMax() const {
        HARMONIZER_HISTORY_PRINT("GetMax, seconds = ", static_cast<ui16>(HistoryBufferSize), ", WithTail = ", false);
        return GetMaxForLastSeconds(HistoryBufferSize);
    }

    double GetMinForLastSeconds(ui8 seconds) const {
        auto min = [](const double& acc, const double& value) {
            HARMONIZER_HISTORY_PRINT("calculate min, acc = ", acc, ", value = ", value, ", Min(acc, value) = ", Min(acc, value));
            return Min(acc, value);
        };
        auto fst = [](const double& value, const double&) { return value; };
        HARMONIZER_HISTORY_PRINT("GetMinForLastSeconds, seconds = ", static_cast<ui16>(seconds), ", WithTail = ", false);
        return Accumulate<false>(min, fst, seconds);
    }

    double GetMin() const {
        HARMONIZER_HISTORY_PRINT("GetMin, seconds = ", static_cast<ui16>(HistoryBufferSize), ", WithTail = ", false);
        return GetMinForLastSeconds(HistoryBufferSize);
    }

    void Register(ui64 ts, double value) {
        HARMONIZER_HISTORY_PRINT("Register, ts = ", ts, ", value = ", value);
        if (ts < LastTs) {
            LastTs = ts;
            LastValue = value;
            AccumulatedValue = 0.0;
            AccumulatedTs = 0;
            for (size_t idx = 0; idx < HistoryBufferSize; ++idx) {
                History[idx] = 0.0;
            }
            HARMONIZER_HISTORY_PRINT("Register backward time, LastTs = ", LastTs, ", LastValue = ", LastValue, ", AccumulatedValue = ", AccumulatedValue, ", AccumulatedTs = ", AccumulatedTs);
            return;
        }
        ui64 lastTs = std::exchange(LastTs, ts);
        ui64 dTs = ts - lastTs;
        double lastValue = std::exchange(LastValue, value);
        double dValue = value - lastValue;

        if (dTs > HistoryBufferSize * Us2Ts(1'000'000.0)) {
            dValue = dValue * 1'000'000.0 / Ts2Us(dTs);
            for (size_t idx = 0; idx < HistoryBufferSize; ++idx) {
                History[idx] = dValue;
            }
            AccumulatedValue = 0.0;
            AccumulatedTs = 0;
            HARMONIZER_HISTORY_PRINT("Register big gap, dTs = ", dTs, ", dValue = ", dValue, ", AccumulatedValue = ", AccumulatedValue, ", AccumulatedTs = ", AccumulatedTs, "lastTs = ", lastTs, ", lastValue = ", lastValue);
            return;
        }

        if (dTs == 0) {
            AccumulatedValue += dValue;
            HARMONIZER_HISTORY_PRINT("Register zero gap, dTs = ", dTs, ", dValue = ", dValue, ", AccumulatedValue = ", AccumulatedValue, ", AccumulatedTs = ", AccumulatedTs, "lastTs = ", lastTs, ", lastValue = ", lastValue);
            return;
        }

        HARMONIZER_HISTORY_PRINT("Register start processing seconds, dTs = ", dTs, ", dValue = ", dValue, ", AccumulatedValue = ", AccumulatedValue, ", AccumulatedTs = ", AccumulatedTs, "lastTs = ", lastTs, ", lastValue = ", lastValue);

        while (dTs > 0) {
            if (AccumulatedTs + dTs < Us2Ts(1'000'000.0)) {
                AccumulatedTs += dTs;
                AccumulatedValue += dValue;
                HARMONIZER_HISTORY_PRINT("Register small gap, dTs = ", dTs, ", dValue = ", dValue, ", AccumulatedValue = ", AccumulatedValue, ", AccumulatedTs = ", AccumulatedTs);
                break;
            } else {
                ui64 addTs = Us2Ts(1'000'000.0) - AccumulatedTs;
                double addValue = dValue * addTs / dTs;
                dTs -= addTs;
                dValue -= addValue;
                History[HistoryIdx] = AccumulatedValue + addValue;
                HistoryIdx = (HistoryIdx + 1) % HistoryBufferSize;
                AccumulatedValue = 0.0;
                AccumulatedTs = 0;
                HARMONIZER_HISTORY_PRINT("Register process one second, dTs = ", dTs,
                    ", dValue = ", dValue,
                    ", addTs = ", addTs,
                    ", addValue = ", addValue,
                    ", AccumulatedValue = ", AccumulatedValue,
                    ", AccumulatedTs = ", AccumulatedTs,
                    ", HistoryIdx = ", HistoryIdx,
                    ", History[HistoryIdx - 1] = ", History[(HistoryIdx + HistoryBufferSize - 1) % HistoryBufferSize]);
            }
        }
    }
}; // struct TValueHistory

} // namespace NActors

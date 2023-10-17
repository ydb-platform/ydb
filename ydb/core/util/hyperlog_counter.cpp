#include "hyperlog_counter.h"

namespace NKikimr {

THyperLogCounter::THyperLogCounter()
    : Value(0)
{}

THyperLogCounter::THyperLogCounter(ui8 value)
    : Value(value)
{
    Y_ABORT_UNLESS(value < 64);
}

ui8 THyperLogCounter::GetValue() const {
    return Value;
}

ui64 THyperLogCounter::GetEstimatedCounter() const {
    ui64 pow2 = 0;
    if (Value != 63)
        pow2 = ui64(1) << (Value + 1);
    return pow2 - 2;
}

bool THyperLogCounter::Increment(IRandomProvider& randomProvider) {
    ui64 rnd = randomProvider.GenRand64();
    ui8 zeroBitsCount = (rnd == 0) ? 63 : CountTrailingZeroBits(rnd);
    if (zeroBitsCount <= Value)
        return false;

    ++Value;
    Y_DEBUG_ABORT_UNLESS(Value < 64);
    return true;
}

bool THyperLogCounter::Add(ui64 addend, IRandomProvider& randomProvider) {
    if (!addend)
        return false;

    ui8 oldValue = Value;
    ui64 originalCounter = GetEstimatedCounter();
    ui64 expectedSum = originalCounter;
    if (expectedSum <= Max<ui64>() - addend) {
        expectedSum += addend;
    } else {
        Value = 63;
        return (oldValue != Value);
    }

    ui64 remain = addend;
    for (;;) {
        if (Value == 63)
            break;

        ui64 nextEstimate = (ui64(1) << (Value + 2)) - 2;
        if (expectedSum < nextEstimate)
            break;

        ui64 toSubtract = ui64(1) << Value;
        remain = (toSubtract > remain) ? 0 : remain - toSubtract;
        ++Value;
    }

    ui64 shift = 0;
    while (remain) {
        if (remain & 1) {
            ui64 rnd = randomProvider.GenRand64();
            ui8 zeroBitsCount = Min<ui8>(63, shift + ((rnd == 0) ? 63 : CountTrailingZeroBits(rnd)));
            if (zeroBitsCount > Value) {
                ++Value;
            }
        }

        ++shift;
        remain >>= 1;
    }

    Y_DEBUG_ABORT_UNLESS(Value < 64);
    return (oldValue != Value);
}

bool THyperLogCounter::Merge(const THyperLogCounter& other, IRandomProvider& randomProvider) {
    return Add(other.GetEstimatedCounter(), randomProvider);
}

THybridLogCounter::THybridLogCounter()
    : Value(0)
{}

THybridLogCounter::THybridLogCounter(ui8 value)
    : Value(value)
{
    if (Value & IsHyperLog) {
        Y_ABORT_UNLESS((value & DataMask) < 64);
    }
}

ui8 THybridLogCounter::GetValue() const {
    return Value;
}

ui64 THybridLogCounter::GetEstimatedCounter() const {
    if (Value & IsHyperLog) {
        return THyperLogCounter(Value & DataMask).GetEstimatedCounter();
    } else {
        return Value & DataMask;
    }
}

bool THybridLogCounter::Increment(IRandomProvider& randomProvider) {
    if (!(Value & IsHyperLog)) {
        ++Value;
        if (Value == IsHyperLog) {
            Value = IsHyperLog | 6; // estimated value for THyperLogCounter
        }

        return true;
    } else {
        THyperLogCounter logCounter(Value & DataMask);
        if (logCounter.Increment(randomProvider)) {
            Value = IsHyperLog | logCounter.GetValue();
            return true;
        }

        return false;
    }
}

bool THybridLogCounter::Add(ui64 addend, IRandomProvider& randomProvider) {
    if (addend == 0)
        return false;

    if (!(Value & IsHyperLog)) {
        if (addend < IsHyperLog) {
            ui64 sum = (Value & DataMask) + addend;
            if (sum < IsHyperLog) {
                Value = sum;
                return true;
            }

            Value = IsHyperLog | ((sum < 0xC0) ? 6 : 7);
            return true;
        }

        THyperLogCounter counter;
        counter.Add(addend, randomProvider);
        counter.Add(Value, randomProvider);
        Value = IsHyperLog | counter.GetValue();
        return true;
    }

    THyperLogCounter counter(Value & DataMask);
    if (!counter.Add(addend, randomProvider))
        return false;

    Value = IsHyperLog | counter.GetValue();
    return true;
}

bool THybridLogCounter::Merge(const THybridLogCounter& other, IRandomProvider& randomProvider) {
    return Add(other.GetEstimatedCounter(), randomProvider);
}

} // NKikimr

#include "microseconds_sliding_window.h"

namespace NKikimr::NPQ {

TMicrosecondsSlidingWindow::TMicrosecondsSlidingWindow(size_t partsNum, TDuration length)
    : 
    Buckets(partsNum, 0),
    Length(length),
    MicroSecondsPerBucket(length.MicroSeconds()/partsNum)
{
}

ui32 TMicrosecondsSlidingWindow::Update(ui32 val, TInstant t) {
    AdvanceTime(t);
    val = Min((ui32)Length.MicroSeconds(), val);
    UpdateCurrentBucket(val);
    return WindowValue;
}

ui32 TMicrosecondsSlidingWindow::Update(TInstant t) {
    AdvanceTime(t);
    return WindowValue;
}

ui32 TMicrosecondsSlidingWindow::GetValue() {
    return WindowValue;
}

void TMicrosecondsSlidingWindow::LocateFirstNotZeroElement() {
    if (WindowValue == 0) {
        return;
    }

    if (FirstNotZeroElem < 0) {
        FirstNotZeroElem = FirstElem;
        while (Buckets[FirstNotZeroElem] == 0) {
            FirstNotZeroElem = (FirstNotZeroElem + 1) % Buckets.size();
        }
    }
}

void TMicrosecondsSlidingWindow::AdjustFirstNonZeroElement() {
    if (WindowValue == 0) {
        return;
    }
    if (FirstNotZeroElem < 0) {
        LocateFirstNotZeroElement();
    }

    size_t offsetFromFirstToNonZero = static_cast<size_t>(FirstNotZeroElem) >= FirstElem ? FirstNotZeroElem - FirstElem + 1 : Buckets.size() - FirstElem + 1 + FirstNotZeroElem;

    ui32& currentVal = Buckets[FirstNotZeroElem];
    ui32 adjustedVal = Min(currentVal, static_cast<ui32>(offsetFromFirstToNonZero * Length.MicroSeconds()/static_cast<ui32>(Buckets.size())));

    if (adjustedVal != currentVal) {
        WindowValue -= currentVal - adjustedVal;
        currentVal = adjustedVal;
    }
}

void TMicrosecondsSlidingWindow::UpdateBucket(size_t index, ui32 newVal) {
    Y_ASSERT(index < Buckets.size());
    Buckets[index] += newVal;
    WindowValue += newVal;
}

void TMicrosecondsSlidingWindow::ClearBuckets(ui32 bucketsToClear) {
    Y_ASSERT(!Buckets.empty());
    Y_ASSERT(bucketsToClear <= Buckets.size());

    size_t firstElemIndex = FirstElem;

    const size_t arraySize = Buckets.size();
    for (ui32 i = 0; i < bucketsToClear; ++i) {
        ui32& curVal = Buckets[firstElemIndex];
        WindowValue -= curVal;
        curVal = 0;
        if (FirstNotZeroElem != -1 && firstElemIndex == (size_t)FirstNotZeroElem) {
            FirstNotZeroElem = -1;
        }
        firstElemIndex = (firstElemIndex + 1) % arraySize;
    }
}

void TMicrosecondsSlidingWindow::UpdateCurrentBucket(ui32 val) {
    const size_t arraySize = Buckets.size();
    const size_t pos = (FirstElem + arraySize - 1) % arraySize;
    UpdateBucket(pos, val);
}

void TMicrosecondsSlidingWindow::AdvanceTime(const TInstant& time) {
    if (time < PeriodStart + Length) {
        return;
    }

    if (PeriodStart.MicroSeconds() == 0) {
        PeriodStart = time - Length;
        return;
    }

    const TInstant& newPeriodStart = time - Length;
    const ui64 tmDiff = (newPeriodStart - PeriodStart).MicroSeconds();
    const size_t bucketsDiff = tmDiff / MicroSecondsPerBucket;
    const size_t arraySize = Buckets.size();
    const ui32 bucketsToClear = Min(bucketsDiff, arraySize);

    ClearBuckets(bucketsToClear);
    FirstElem = (FirstElem + bucketsToClear) % arraySize;
    AdjustFirstNonZeroElement();
    PeriodStart += TDuration::MicroSeconds(bucketsDiff * MicroSecondsPerBucket);

    Y_ASSERT(newPeriodStart >= PeriodStart);
    Y_ASSERT((newPeriodStart - PeriodStart).MicroSeconds() <= MicroSecondsPerBucket);
}

}// NKikimr::NPQ

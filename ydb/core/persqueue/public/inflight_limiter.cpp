#include "inflight_limiter.h"
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NPQ {

namespace NDetail {

void TSlidingWindow::StartRecord() {
    if (RecordingStart != TInstant::Zero()) {
        return;
    }

    RecordingStart = TInstant::Now();
}

void TSlidingWindow::RemoveOldRecords(TInstant now) {
    auto startWindow = now - WindowSize;
    while (!Records.empty() && Records.front().Start < startWindow) {
        auto& firstRecord = Records.front();
        if (firstRecord.Start + firstRecord.Duration > startWindow) {
            firstRecord.Duration -= startWindow - firstRecord.Start;
            firstRecord.Start = startWindow;
            break;
        }
        
        Records.pop_front();
    }
}

size_t TSlidingWindow::GetRecordsCount() const {
    return Records.size();
}

TDuration TSlidingWindow::GetValueOnWindow() {
    auto now = TInstant::Now();
    RemoveOldRecords(now);

    auto carry = TDuration::Zero();
    if (RecordingStart != TInstant::Zero()) {
        RecordingStart = Max(RecordingStart, now - WindowSize);
        carry = now - RecordingStart;
    }

    return std::accumulate(Records.begin(), Records.end(), TDuration::Zero(), [](TDuration acc, const TSlidingWindowRecord& record) {
        return acc + record.Duration;
    }) + carry;
}

void TSlidingWindow::Reset() {
    if (RecordingStart == TInstant::Zero()) {
        return;
    }

    auto now = TInstant::Now();
    RemoveOldRecords(now);

    RecordingStart = Max(RecordingStart, now - WindowSize);

    if (!Records.empty()) {
        auto lastRecordUnitEnd = Records.back().Start + UnitSize;
        if (RecordingStart < lastRecordUnitEnd) {
            // check if we need to add sth to last record
            Records.back().Duration += Min(lastRecordUnitEnd, now) - RecordingStart;
            RecordingStart = Min(lastRecordUnitEnd, now);
        }
    }

    if (RecordingStart < now) {
        Records.push_back(TSlidingWindowRecord{RecordingStart, now - RecordingStart});
    }

    RecordingStart = TInstant::Zero();
}

} // namespace NDetail

TInFlightController::TInFlightController(ui64 MaxAllowedSize)
    : LayoutUnitSize(MaxAllowedSize / MAX_LAYOUT_COUNT)
    , TotalSize(0)
    , MaxAllowedSize(MaxAllowedSize)
{
    if (MaxAllowedSize > 0 && LayoutUnitSize == 0) {
        LayoutUnitSize = 1;
    }
}

bool TInFlightController::Add(ui64 Offset, ui64 Size) {
    if (MaxAllowedSize == 0) {
        // means that there are no limits were set
        return true;
    }

    auto wasMemoryLimitReached = IsMemoryLimitReached();
    if (Size == 0) {
        return !wasMemoryLimitReached;
    }

    AFL_ENSURE(Layout.empty() || Offset > Layout.back());
    if (TotalSize % LayoutUnitSize != 0) {
        AFL_ENSURE(!Layout.empty());
        Layout.back() = Offset;
    }

    auto unitsBefore = (TotalSize + LayoutUnitSize - 1) / LayoutUnitSize;
    TotalSize += Size;
    auto unitsAfter = std::min(MAX_LAYOUT_COUNT, (TotalSize + LayoutUnitSize - 1) / LayoutUnitSize);
    for (auto currentUnits = unitsBefore; currentUnits < unitsAfter; currentUnits++) {
        Layout.push_back(Offset);
    }
    
    AFL_ENSURE(!Layout.empty());
    Layout.back() = Offset;

    bool isMemoryLimitReached = IsMemoryLimitReached();
    if (!wasMemoryLimitReached && isMemoryLimitReached) {
        SlidingWindow.StartRecord();
    }

    return !isMemoryLimitReached;
}

bool TInFlightController::Remove(ui64 Offset) {
    if (MaxAllowedSize == 0) {
        // means that there are no limits were set
        return true;
    }

    auto wasMemoryLimitReached = IsMemoryLimitReached();
    for (auto it = Layout.begin(); it != Layout.end(); it = Layout.erase(it)) {
        if (*it >= Offset) {
            break;
        }

        if (Layout.size() == 1) {
            TotalSize = 0;
        } else {
            AFL_ENSURE(TotalSize >= LayoutUnitSize);
            TotalSize -= LayoutUnitSize;
        }
    }

    auto isMemoryLimitReached = IsMemoryLimitReached();
    if (wasMemoryLimitReached && !isMemoryLimitReached) {
        SlidingWindow.Reset();
    }

    return !isMemoryLimitReached;
}

bool TInFlightController::IsMemoryLimitReached() const {
    if (MaxAllowedSize == 0) {
        // means that there are no limits were set
        return false;
    }

    return TotalSize >= MaxAllowedSize;
}

TDuration TInFlightController::GetOverflowDuration() {
    return SlidingWindow.GetValueOnWindow();
}

} // namespace NKikimr::NPQ
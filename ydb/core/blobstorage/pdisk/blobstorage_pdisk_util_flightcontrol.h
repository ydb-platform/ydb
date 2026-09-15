#pragma once
#include "defs.h"
#include "blobstorage_pdisk_mon.h"

#include <library/cpp/deprecated/atomic/atomic.h>

#include <util/generic/queue.h>
#include <util/generic/vector.h>
#include <util/system/condvar.h>
#include <functional>
#include <variant>

namespace NKikimr {
namespace NPDisk {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TFlightControl
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class TFlightControl {
    TAtomic BeginIdx;
    TAtomic MaxInFlightIdx;
    ui64 EndIdx;
    ui64 MaxSize;
    ui64 Mask;
    TVector<bool> IsCompleteLoop;
    TMutex ScheduleMutex;
    TCondVar ScheduleCondVar;
    TString PDiskLogPrefix;

    void WakeUp();

public:
    TFlightControl(ui64 maxInFlightRequests, ui64 inFlightBytesLimit = 0);

    void Initialize(const TString& logPrefix);

    // Returns 0 in case of scheduling error
    // Operation Idx otherwise
    // May sometimes return 0 when it already can schedule
    ui64 TrySchedule(ui64 size);

    // Blocking version of TrySchedule
    ui64 Schedule(double& blockedMs, ui64 size);

    void MarkComplete(ui64 idx, ui64 size);
    ui64 FirstIncompleteIdx();
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TBytesFlightControl
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class TBytesFlightControl {
    const ui64 InFlightRequestsLimit;
    const ui64 InFlightBytesLimit;

    TAtomic CachedFirstIncompleteIdx;
    ui64 NextScheduleIdx;
    ui64 InFlightRequests;
    ui64 InFlightBytes;
    ui64 FirstIncompleteIdxValue;
    TPriorityQueue<ui64, TVector<ui64>, std::greater<ui64>> CompletedIdx;

    TMutex ScheduleMutex;
    TCondVar ScheduleCondVar;
    TString PDiskLogPrefix;

    ui64 TryScheduleLocked(ui64 size);

public:
    TBytesFlightControl(ui64 inFlightRequestsLimit, ui64 inFlightBytesLimit);

    void Initialize(const TString& logPrefix);

    // Returns 0 in case of scheduling error
    // Operation Idx otherwise
    // May sometimes return 0 when it already can schedule
    ui64 TrySchedule(ui64 size);

    // Blocking version of TrySchedule
    ui64 Schedule(double& blockedMs, ui64 size);

    void MarkComplete(ui64 idx, ui64 size);
    ui64 FirstIncompleteIdx();
};

using TFlightControlVariant = std::variant<TFlightControl, TBytesFlightControl>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TFlightControlFace
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class TFlightControlFace {
    TFlightControlVariant FlightControl;

public:
    TFlightControlFace(ui64 inFlightRequestsLimit, bool useBytesFlightControl,
            ui64 maxBytesInFlightLimit)
        : FlightControl(useBytesFlightControl
                    ? TFlightControlVariant(std::in_place_type<TBytesFlightControl>, inFlightRequestsLimit,
                            maxBytesInFlightLimit)
                    : TFlightControlVariant(std::in_place_type<TFlightControl>, inFlightRequestsLimit,
                            maxBytesInFlightLimit))
    {
    }

    void Initialize(const TString& logPrefix) {
        std::visit(
                [&](auto& control) {
                    control.Initialize(logPrefix);
                },
                FlightControl);
    }

    ui64 TrySchedule(ui64 size) {
        return std::visit(
                [&](auto& control) -> ui64 {
                    return control.TrySchedule(size);
                },
                FlightControl);
    }

    ui64 Schedule(double& blockedMs, ui64 size) {
        return std::visit(
                [&](auto& control) -> ui64 {
                    return control.Schedule(blockedMs, size);
                },
                FlightControl);
    }

    void MarkComplete(ui64 idx, ui64 size) {
        std::visit(
                [&](auto& control) {
                    control.MarkComplete(idx, size);
                },
                FlightControl);
    }

    ui64 FirstIncompleteIdx() {
        return std::visit(
                [&](auto& control) -> ui64 {
                    return control.FirstIncompleteIdx();
                },
                FlightControl);
    }
};

} // NPDisk
} // NKikimr

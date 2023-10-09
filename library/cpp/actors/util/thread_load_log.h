#pragma once

#include "defs.h"

#include <util/system/types.h>

#include <type_traits>
#include <algorithm>
#include <atomic>
#include <limits>
#include <queue>

template <ui64 TIME_SLOT_COUNT, ui64 TIME_SLOT_LENGTH_NS = 131'072, typename Type = std::uint8_t>
class TThreadLoad {
public:
    using TimeSlotType = Type;

private:
    static constexpr auto TIME_SLOT_MAX_VALUE = std::numeric_limits<TimeSlotType>::max();
    static constexpr ui64 TIME_SLOT_PART_COUNT = TIME_SLOT_MAX_VALUE + 1;
    static constexpr auto TIME_SLOT_PART_LENGTH_NS = TIME_SLOT_LENGTH_NS / TIME_SLOT_PART_COUNT;

    template <typename T>
    static void AtomicAddBound(std::atomic<T>& val, i64 inc) {
        if (inc == 0) {
            return;
        }

        auto newVal = val.load();
        auto oldVal = newVal;

        do {
            static constexpr auto MAX_VALUE = std::numeric_limits<T>::max();

            if (oldVal >= MAX_VALUE) {
                return;
            }
            newVal = std::min<i64>(MAX_VALUE, static_cast<i64>(oldVal) + inc);
        } while (!val.compare_exchange_weak(oldVal, newVal));
    }

    template <typename T>
    static void AtomicSubBound(std::atomic<T>& val, i64 sub) {
        if (sub == 0) {
            return;
        }

        auto newVal = val.load();
        auto oldVal = newVal;

        do {
            if (oldVal == 0) {
                return;
            }
            newVal = std::max<i64>(0, static_cast<i64>(oldVal) - sub);
        } while (!val.compare_exchange_weak(oldVal, newVal));
    }

    void UpdateCompleteTimeSlots(ui64 firstSlotNumber, ui64 lastSlotNumber, TimeSlotType timeSlotValue) {
        ui32 firstSlotIndex = firstSlotNumber % TIME_SLOT_COUNT;
        ui32 lastSlotIndex = lastSlotNumber % TIME_SLOT_COUNT;

        const ui64 firstTimeSlotsPass = firstSlotNumber / TIME_SLOT_COUNT;
        const ui64 lastTimeSlotsPass = lastSlotNumber / TIME_SLOT_COUNT;

        if (firstTimeSlotsPass == lastTimeSlotsPass) {
            // first and last time slots are in the same pass
            for (auto slotNumber = firstSlotNumber + 1; slotNumber < lastSlotNumber; ++slotNumber) {
                auto slotIndex = slotNumber % TIME_SLOT_COUNT;
                TimeSlots[slotIndex] = timeSlotValue;
            }
        } else if (firstTimeSlotsPass + 1 == lastTimeSlotsPass) {
            for (auto slotIndex = (firstSlotNumber + 1) % TIME_SLOT_COUNT; firstSlotIndex < slotIndex && slotIndex < TIME_SLOT_COUNT; ++slotIndex) {
                TimeSlots[slotIndex] = timeSlotValue;
            }
            for (auto slotIndex = 0u; slotIndex < lastSlotIndex; ++slotIndex) {
                TimeSlots[slotIndex] = timeSlotValue;
            }
        } else {
            for (auto slotIndex = 0u; slotIndex < TIME_SLOT_COUNT; ++slotIndex) {
                TimeSlots[slotIndex] = timeSlotValue;
            }
        }
    }

public:
    std::atomic<ui64> LastTimeNs;
    std::atomic<TimeSlotType> TimeSlots[TIME_SLOT_COUNT];
    std::atomic<bool> LastRegisteredPeriodIsBusy = false;

    explicit TThreadLoad(ui64 timeNs = 0) {
        static_assert(std::is_unsigned<TimeSlotType>::value);

        LastTimeNs = timeNs;
        for (size_t i = 0; i < TIME_SLOT_COUNT; ++i) {
            TimeSlots[i] = 0;
        }
    }

    static constexpr auto GetTimeSlotCount() {
        return TIME_SLOT_COUNT;
    }

    static constexpr auto GetTimeSlotLengthNs() {
        return TIME_SLOT_LENGTH_NS;
    }

    static constexpr auto GetTimeSlotPartLengthNs() {
        return TIME_SLOT_PART_LENGTH_NS;
    }

    static constexpr auto GetTimeSlotPartCount() {
        return TIME_SLOT_PART_COUNT;
    }

    static constexpr auto GetTimeSlotMaxValue() {
        return TIME_SLOT_MAX_VALUE;
    }

    static constexpr auto GetTimeWindowLengthNs() {
        return TIME_SLOT_COUNT * TIME_SLOT_LENGTH_NS;
    }

    void RegisterBusyPeriod(ui64 timeNs) {
        RegisterBusyPeriod<true>(timeNs, LastTimeNs.load());
    }

    template <bool ModifyLastTime>
    void RegisterBusyPeriod(ui64 timeNs, ui64 lastTimeNs) {
        LastRegisteredPeriodIsBusy = true;

        if (timeNs < lastTimeNs) {
            // when time goes back, mark all time slots as 'free'
            for (size_t i = 0u; i < TIME_SLOT_COUNT; ++i) {
                TimeSlots[i] = 0;
            }

            if (ModifyLastTime) {
                LastTimeNs = timeNs;
            }

            return;
        }

        // lastTimeNs <= timeNs
        ui64 firstSlotNumber = lastTimeNs / TIME_SLOT_LENGTH_NS;
        ui32 firstSlotIndex = firstSlotNumber % TIME_SLOT_COUNT;
        ui64 lastSlotNumber = timeNs / TIME_SLOT_LENGTH_NS;
        ui32 lastSlotIndex = lastSlotNumber % TIME_SLOT_COUNT;

        if (firstSlotNumber == lastSlotNumber) {
            ui32 slotLengthNs = timeNs - lastTimeNs;
            ui32 slotPartsCount = (slotLengthNs + TIME_SLOT_PART_LENGTH_NS - 1) / TIME_SLOT_PART_LENGTH_NS;
            AtomicAddBound(TimeSlots[firstSlotIndex], slotPartsCount);

            if (ModifyLastTime) {
                LastTimeNs = timeNs;
            }
            return;
        }

        ui32 firstSlotLengthNs = TIME_SLOT_LENGTH_NS - (lastTimeNs % TIME_SLOT_LENGTH_NS);
        ui32 firstSlotPartsCount = (firstSlotLengthNs + TIME_SLOT_PART_LENGTH_NS - 1) / TIME_SLOT_PART_LENGTH_NS;
        ui32 lastSlotLengthNs = timeNs % TIME_SLOT_LENGTH_NS;
        ui32 lastSlotPartsCount = (lastSlotLengthNs + TIME_SLOT_PART_LENGTH_NS - 1) / TIME_SLOT_PART_LENGTH_NS;

        // process first time slot
        AtomicAddBound(TimeSlots[firstSlotIndex], firstSlotPartsCount);

        // process complete time slots
        UpdateCompleteTimeSlots(firstSlotNumber, lastSlotNumber, TIME_SLOT_MAX_VALUE);

        // process last time slot
        AtomicAddBound(TimeSlots[lastSlotIndex], lastSlotPartsCount);

        if (ModifyLastTime) {
            LastTimeNs = timeNs;
        }
    }

    void RegisterIdlePeriod(ui64 timeNs) {
        LastRegisteredPeriodIsBusy = false;

        ui64 lastTimeNs = LastTimeNs.load();
        if (timeNs < lastTimeNs) {
            // when time goes back, mark all time slots as 'busy'
            for (size_t i = 0u; i < TIME_SLOT_COUNT; ++i) {
                TimeSlots[i] = TIME_SLOT_MAX_VALUE;
            }
            LastTimeNs = timeNs;
            return;
        }

        // lastTimeNs <= timeNs
        ui64 firstSlotNumber = lastTimeNs / TIME_SLOT_LENGTH_NS;
        ui32 firstSlotIndex = firstSlotNumber % TIME_SLOT_COUNT;
        ui64 lastSlotNumber = timeNs / TIME_SLOT_LENGTH_NS;
        ui32 lastSlotIndex = lastSlotNumber % TIME_SLOT_COUNT;

        if (firstSlotNumber == lastSlotNumber) {
            ui32 slotLengthNs = timeNs - lastTimeNs;
            ui32 slotPartsCount = slotLengthNs / TIME_SLOT_PART_LENGTH_NS;

            AtomicSubBound(TimeSlots[firstSlotIndex], slotPartsCount);

            LastTimeNs = timeNs;
            return;
        }

        ui32 firstSlotLengthNs = TIME_SLOT_LENGTH_NS - (lastTimeNs % TIME_SLOT_LENGTH_NS);
        ui32 firstSlotPartsCount = (firstSlotLengthNs + TIME_SLOT_PART_LENGTH_NS - 1) / TIME_SLOT_PART_LENGTH_NS;
        ui32 lastSlotLengthNs = timeNs % TIME_SLOT_LENGTH_NS;
        ui32 lastSlotPartsCount = (lastSlotLengthNs + TIME_SLOT_PART_LENGTH_NS - 1) / TIME_SLOT_PART_LENGTH_NS;

        // process first time slot
        AtomicSubBound(TimeSlots[firstSlotIndex], firstSlotPartsCount);

        // process complete time slots
        UpdateCompleteTimeSlots(firstSlotNumber, lastSlotNumber, 0);

        // process last time slot
        AtomicSubBound(TimeSlots[lastSlotIndex], lastSlotPartsCount);

        LastTimeNs = timeNs;
    }
};

class TMinusOneThreadEstimator {
private:
    template <typename T, int MaxSize>
    class TArrayQueue {
    public:
        bool empty() const {
            return FrontIndex == -1;
        }

        bool full() const {
            return (RearIndex + 1) % MaxSize == FrontIndex;
        }

        T& front() {
            return Data[FrontIndex];
        }

        bool push(T &&t) {
            if (full()) {
                return false;
            }

            if (FrontIndex == -1) {
                FrontIndex = 0;
            }

            RearIndex = (RearIndex + 1) % MaxSize;
            Data[RearIndex] = std::move(t);
            return true;
        }

        bool pop() {
            if (empty()) {
                return false;
            }

            if (FrontIndex == RearIndex) {
                FrontIndex = RearIndex = -1;
            } else {
                FrontIndex = (FrontIndex + 1) % MaxSize;
            }

            return true;
        }

    private:
        int FrontIndex = -1;
        int RearIndex = -1;
        T Data[MaxSize];
    };

public:
    template <typename T>
    ui64 MaxLatencyIncreaseWithOneLessCpu(T **threadLoads, ui32 threadCount, ui64 timeNs, ui64 periodNs) {
        Y_ABORT_UNLESS(threadCount > 0);

        struct TTimeSlotData {
            typename T::TimeSlotType Load;
            ui64 Index;
        };

        ui64 lastTimeNs = timeNs;
        for (auto threadIndex = 0u; threadIndex < threadCount; ++threadIndex) {
            if (threadLoads[threadIndex]->LastRegisteredPeriodIsBusy.load()) {
                lastTimeNs = std::min(lastTimeNs, threadLoads[threadIndex]->LastTimeNs.load());
            } else {
                // make interval [lastTimeNs, timeNs] 'busy'
                threadLoads[threadIndex]->template RegisterBusyPeriod<false>(timeNs, threadLoads[threadIndex]->LastTimeNs.load());
            }
        }

        periodNs = std::min(T::GetTimeWindowLengthNs(), periodNs);

        ui64 beginTimeNs = periodNs < timeNs ? timeNs - periodNs : 0;

        ui64 firstSlotNumber = beginTimeNs / T::GetTimeSlotLengthNs();
        ui64 lastSlotNumber = (lastTimeNs + T::GetTimeSlotLengthNs() - 1) / T::GetTimeSlotLengthNs();

        ui64 maxTimeSlotShiftCount = 0u;
        TArrayQueue<TTimeSlotData, T::GetTimeSlotCount()> firstThreadLoadDataQueue;

        for (auto slotNumber = firstSlotNumber; slotNumber < lastSlotNumber; ++slotNumber) {
            ui64 slotIndex = slotNumber % T::GetTimeSlotCount();

            typename T::TimeSlotType firstThreadTimeSlotValue = threadLoads[0]->TimeSlots[slotIndex].load();

            // distribute previous load of the first thread by other threads
            auto foundIdleThread = false;

            for (auto threadIndex = 1u; threadIndex < threadCount; ++threadIndex) {
                typename T::TimeSlotType thisThreadAvailableTimeSlotLoad = threadLoads[threadIndex]->GetTimeSlotMaxValue() - threadLoads[threadIndex]->TimeSlots[slotIndex].load();

                while (!firstThreadLoadDataQueue.empty() && thisThreadAvailableTimeSlotLoad > 0) {
                    auto& firstThreadLoadData = firstThreadLoadDataQueue.front();

                    auto distributedLoad = std::min(thisThreadAvailableTimeSlotLoad, firstThreadLoadData.Load);

                    thisThreadAvailableTimeSlotLoad -= distributedLoad;
                    firstThreadLoadData.Load -= distributedLoad;

                    if (firstThreadLoadData.Load == 0) {
                        auto timeSlotShiftCount = slotIndex - firstThreadLoadData.Index;
                        maxTimeSlotShiftCount = std::max(maxTimeSlotShiftCount, timeSlotShiftCount);
                        auto res = firstThreadLoadDataQueue.pop();
                        Y_ABORT_UNLESS(res);
                    }
                }

                if (thisThreadAvailableTimeSlotLoad == threadLoads[threadIndex]->GetTimeSlotMaxValue()) {
                    foundIdleThread = true;
                }
            }

            // distribute current load of the first thread by other threads
            if (firstThreadTimeSlotValue > 0) {
                if (foundIdleThread) {
                    // The current load of the first thead can be
                    // moved to the idle thread so there is nothing to do
                } else {
                    // The current load of the first thread can be later
                    // processed by the following time slots of other threads
                    auto res = firstThreadLoadDataQueue.push({firstThreadTimeSlotValue, slotIndex});
                    Y_ABORT_UNLESS(res);
                }
            }
        }

        if (!firstThreadLoadDataQueue.empty()) {
            const auto& timeSlotData = firstThreadLoadDataQueue.front();
            auto timeSlotShiftCount = T::GetTimeSlotCount() - timeSlotData.Index;
            maxTimeSlotShiftCount = std::max(maxTimeSlotShiftCount, timeSlotShiftCount);
        }

        return maxTimeSlotShiftCount * T::GetTimeSlotLengthNs();
    }
};

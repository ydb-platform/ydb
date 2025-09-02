#pragma once

#include "defs.h"

#include <bit>

static constexpr ui64 BitDurationNs = 131'072;  // A power of 2

template <ui64 DataSize>
struct TCpuLoadLog {
    static constexpr ui64 BitsSize = DataSize * 64;
    std::atomic<ui64> LastTimeNs = 0;
    std::atomic<ui64> Data[DataSize] {};

    TCpuLoadLog() = default;

    TCpuLoadLog(ui64 timeNs) : LastTimeNs(timeNs) {
    }

    void RegisterBusyPeriod(ui64 timeNs) {
        RegisterBusyPeriod<true>(timeNs, LastTimeNs.load(std::memory_order_acquire));
    }

    template <bool ModifyLastTime>
    void RegisterBusyPeriod(ui64 timeNs, ui64 lastTimeNs) {
        timeNs |= 1ull;
        if (timeNs < lastTimeNs) {
            for (ui64 i = 0; i < DataSize; ++i) {
                Data[i].store(~0ull, std::memory_order_release);
            }
            if (ModifyLastTime) {
                LastTimeNs.store(timeNs, std::memory_order_release);
            }
            return;
        }
        const ui64 lastIdx = timeNs / BitDurationNs;
        const ui64 curIdx = lastTimeNs / BitDurationNs;
        ui64 firstElementIdx = curIdx / 64;
        const ui64 firstBitIdx = curIdx % 64;
        const ui64 lastElementIdx = lastIdx / 64;
        const ui64 lastBitIdx = lastIdx % 64;
        if (firstElementIdx == lastElementIdx) {
            ui64 prevValue = 0;
            if (firstBitIdx != 0) {
                prevValue = Data[firstElementIdx % DataSize].load(std::memory_order_acquire);
            }
            const ui64 bits = (((~0ull) << (firstBitIdx + (63-lastBitIdx))) >> (63-lastBitIdx));
            const ui64 newValue = prevValue | bits;
            Data[firstElementIdx % DataSize].store(newValue, std::memory_order_release);
            if (ModifyLastTime) {
                LastTimeNs.store(timeNs, std::memory_order_release);
            }
            return;
        }
        // process the first element
        ui64 prevValue = 0;
        if (firstBitIdx != 0) {
            prevValue = Data[firstElementIdx % DataSize].load(std::memory_order_acquire);
        }
        const ui64 bits = ((~0ull) << firstBitIdx);
        const ui64 newValue = (prevValue | bits);
        Data[firstElementIdx % DataSize].store(newValue, std::memory_order_release);
        ++firstElementIdx;
        // process the fully filled elements
        const ui64 firstLoop = firstElementIdx / DataSize;
        const ui64 lastLoop = lastElementIdx / DataSize;
        const ui64 lastOffset = lastElementIdx % DataSize;
        if (firstLoop < lastLoop) {
            for (ui64 i = firstElementIdx % DataSize; i < DataSize; ++i) {
                Data[i].store(~0ull, std::memory_order_release);
            }
            for (ui64 i = 0; i < lastOffset; ++i) {
                Data[i].store(~0ull, std::memory_order_release);
            }
        } else {
            for (ui64 i = firstElementIdx % DataSize; i < lastOffset; ++i) {
                Data[i].store(~0ull, std::memory_order_release);
            }
        }
        // process the last element
        const ui64 newValue2 = ((~0ull) >> (63-lastBitIdx));
        Data[lastOffset].store(newValue2, std::memory_order_release);
        if (ModifyLastTime) {
            LastTimeNs.store(timeNs, std::memory_order_release);
        }
    }

    void RegisterIdlePeriod(ui64 timeNs) {
        timeNs &= ~1ull;
        ui64 lastTimeNs = LastTimeNs.load(std::memory_order_acquire);
        if (timeNs < lastTimeNs) {
            // Fast check first, slower chec later
            if ((timeNs | 1ull) < lastTimeNs) {
                // Time goes back, dont panic, just mark the whole array 'busy'
                for (ui64 i = 0; i < DataSize; ++i) {
                    Data[i].store(~0ull, std::memory_order_release);
                }
                LastTimeNs.store(timeNs, std::memory_order_release);
                return;
            }
        }
        const ui64 curIdx = lastTimeNs / BitDurationNs;
        const ui64 lastIdx = timeNs / BitDurationNs;
        ui64 firstElementIdx = curIdx / 64;
        const ui64 lastElementIdx = lastIdx / 64;
        if (firstElementIdx >= lastElementIdx) {
            LastTimeNs.store(timeNs, std::memory_order_release);
            return;
        }
        // process the first partially filled element
        ++firstElementIdx;
        // process all other elements
        const ui64 firstLoop = firstElementIdx / DataSize;
        const ui64 lastLoop = lastElementIdx / DataSize;
        const ui64 lastOffset = lastElementIdx % DataSize;
        if (firstLoop < lastLoop) {
            for (ui64 i = firstElementIdx % DataSize; i < DataSize; ++i) {
                Data[i].store(0, std::memory_order_release);
            }
            for (ui64 i = 0; i <= lastOffset; ++i) {
                Data[i].store(0, std::memory_order_release);
            }
        } else {
            for (ui64 i = firstElementIdx % DataSize; i <= lastOffset; ++i) {
                Data[i].store(0, std::memory_order_release);
            }
        }
        LastTimeNs.store(timeNs, std::memory_order_release);
    }
};

template <ui64 DataSize>
class TMinusOneCpuEstimator {
    static constexpr ui64 BitsSize = DataSize * 64;
    ui64 BeginDelayIdx = 0;
    ui64 EndDelayIdx = 0;
    ui64 Idle = 0;
    ui64 Delay[BitsSize] {};
public:
    TMinusOneCpuEstimator() = default;

    ui64 MaxLatencyIncreaseWithOneLessCpu(TCpuLoadLog<DataSize>** logs, i64 logCount, ui64 timeNs, ui64 periodNs) {
        Y_ABORT_UNLESS(logCount > 0);
        ui64 endTimeNs = timeNs;

        ui64 lastTimeNs = timeNs;
        for (i64 log_idx = 0; log_idx < logCount; ++log_idx) {
            ui64 x = logs[log_idx]->LastTimeNs.load(std::memory_order_acquire);
            if ((x & 1) == 1) {
                lastTimeNs = Min(lastTimeNs, x);
            } else {
                logs[log_idx]->template RegisterBusyPeriod<false>(endTimeNs, x);
            }
        }
        const ui64 beginTimeNs = periodNs < timeNs ? timeNs - periodNs : 0;

        ui64 beginIdx = beginTimeNs / BitDurationNs;
        ui64 lastIdx = lastTimeNs / BitDurationNs;
        ui64 beginElementIdx = beginIdx / 64;
        ui64 lastElementIdx = lastIdx / 64;

        BeginDelayIdx = 0;
        EndDelayIdx = 0;
        Idle = 0;
        ui64 maxDelay = 0;
        ui64 bucket = 0;
        for (ui64 idx = beginElementIdx; idx <= lastElementIdx; ++idx) {
            ui64 i = idx % DataSize;
            ui64 input = logs[0]->Data[i].load(std::memory_order_acquire);
            ui64 all_busy = ~0ull;
            for (i64 log_idx = 1; log_idx < logCount; ++log_idx) {
                ui64 x = logs[log_idx]->Data[i].load(std::memory_order_acquire);
                all_busy &= x;
            }
            if (!input) {
                if (!bucket) {
                    Idle += 64 - std::popcount(all_busy);
                    continue;
                }
            }
            for (i64 bit_idx = 0; bit_idx < 64; ++bit_idx) {
                ui64 x = (1ull << bit_idx);
                if (all_busy & x) {
                    if (input & x) {
                        // Push into the queue
                        bucket++;
                        Delay[EndDelayIdx] = EndDelayIdx;
                        ++EndDelayIdx;
                    } else {
                        // All busy
                    }
                } else {
                    if (input & x) {
                        // Move success
                    } else {
                        if (bucket) {
                            // Remove from the queue
                            bucket--;
                            ui64 stored = Delay[BeginDelayIdx];
                            ++BeginDelayIdx;
                            ui64 delay = EndDelayIdx - stored;
                            maxDelay = Max(maxDelay, delay);
                            //Cerr << "bit_idx: " << bit_idx << " stored: " << stored << " delay: " << delay << Endl;
                        } else {
                            Idle++;
                        }
                    }
                }
            }
        }
        if (bucket) {
            ui64 stored = Delay[BeginDelayIdx];
            ui64 delay = EndDelayIdx - stored;
            maxDelay = Max(maxDelay, delay);
            //Cerr << "last stored: " << stored << " delay: " << delay << Endl;
        }
        return maxDelay * BitDurationNs;
    }
};

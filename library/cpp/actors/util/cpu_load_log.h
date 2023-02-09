#pragma once

#include "defs.h"
#include <library/cpp/deprecated/atomic/atomic.h>
#include <library/cpp/pop_count/popcount.h>

static constexpr ui64 BitDurationNs = 131'072;  // A power of 2

template <ui64 DataSize>
struct TCpuLoadLog {
    static constexpr ui64 BitsSize = DataSize * 64;
    TAtomic LastTimeNs = 0;
    ui64 Data[DataSize];

    TCpuLoadLog() {
        LastTimeNs = 0;
        for (size_t i = 0; i < DataSize; ++i) {
            Data[i] = 0;
        }
    }

    TCpuLoadLog(ui64 timeNs) {
        LastTimeNs = timeNs;
        for (size_t i = 0; i < DataSize; ++i) {
            Data[i] = 0;
        }
    }

    void RegisterBusyPeriod(ui64 timeNs) {
        RegisterBusyPeriod<true>(timeNs, AtomicGet(LastTimeNs));
    }

    template <bool ModifyLastTime>
    void RegisterBusyPeriod(ui64 timeNs, ui64 lastTimeNs) {
        timeNs |= 1ull;
        if (timeNs < lastTimeNs) {
            for (ui64 i = 0; i < DataSize; ++i) {
                AtomicSet(Data[i], ~0ull);
            }
            if (ModifyLastTime) {
                AtomicSet(LastTimeNs, timeNs);
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
                prevValue = AtomicGet(Data[firstElementIdx % DataSize]);
            }
            const ui64 bits = (((~0ull) << (firstBitIdx + (63-lastBitIdx))) >> (63-lastBitIdx));
            const ui64 newValue = prevValue | bits;
            AtomicSet(Data[firstElementIdx % DataSize], newValue);
            if (ModifyLastTime) {
                AtomicSet(LastTimeNs, timeNs);
            }
            return;
        }
        // process the first element
        ui64 prevValue = 0;
        if (firstBitIdx != 0) {
            prevValue = AtomicGet(Data[firstElementIdx % DataSize]);
        }
        const ui64 bits = ((~0ull) << firstBitIdx);
        const ui64 newValue = (prevValue | bits);
        AtomicSet(Data[firstElementIdx % DataSize], newValue);
        ++firstElementIdx;
        // process the fully filled elements
        const ui64 firstLoop = firstElementIdx / DataSize;
        const ui64 lastLoop = lastElementIdx / DataSize;
        const ui64 lastOffset = lastElementIdx % DataSize;
        if (firstLoop < lastLoop) {
            for (ui64 i = firstElementIdx % DataSize; i < DataSize; ++i) {
                AtomicSet(Data[i], ~0ull);
            }
            for (ui64 i = 0; i < lastOffset; ++i) {
                AtomicSet(Data[i], ~0ull);
            }
        } else {
            for (ui64 i = firstElementIdx % DataSize; i < lastOffset; ++i) {
                AtomicSet(Data[i], ~0ull);
            }
        }
        // process the last element
        const ui64 newValue2 = ((~0ull) >> (63-lastBitIdx));
        AtomicSet(Data[lastOffset], newValue2);
        if (ModifyLastTime) {
            AtomicSet(LastTimeNs, timeNs);
        }
    }

    void RegisterIdlePeriod(ui64 timeNs) {
        timeNs &= ~1ull;
        ui64 lastTimeNs = AtomicGet(LastTimeNs);
        if (timeNs < lastTimeNs) {
            // Fast check first, slower chec later
            if ((timeNs | 1ull) < lastTimeNs) {
                // Time goes back, dont panic, just mark the whole array 'busy'
                for (ui64 i = 0; i < DataSize; ++i) {
                    AtomicSet(Data[i], ~0ull);
                }
                AtomicSet(LastTimeNs, timeNs);
                return;
            }
        }
        const ui64 curIdx = lastTimeNs / BitDurationNs;
        const ui64 lastIdx = timeNs / BitDurationNs;
        ui64 firstElementIdx = curIdx / 64;
        const ui64 lastElementIdx = lastIdx / 64;
        if (firstElementIdx >= lastElementIdx) {
            AtomicSet(LastTimeNs, timeNs);
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
                AtomicSet(Data[i], 0);
            }
            for (ui64 i = 0; i <= lastOffset; ++i) {
                AtomicSet(Data[i], 0);
            }
        } else {
            for (ui64 i = firstElementIdx % DataSize; i <= lastOffset; ++i) {
                AtomicSet(Data[i], 0);
            }
        }
        AtomicSet(LastTimeNs, timeNs);
    }
};

template <ui64 DataSize>
struct TMinusOneCpuEstimator {
    static constexpr ui64 BitsSize = DataSize * 64;
    ui64 BeginDelayIdx;
    ui64 EndDelayIdx;
    ui64 Idle;
    ui64 Delay[BitsSize];

    ui64 MaxLatencyIncreaseWithOneLessCpu(TCpuLoadLog<DataSize>** logs, i64 logCount, ui64 timeNs, ui64 periodNs) {
        Y_VERIFY(logCount > 0);
        ui64 endTimeNs = timeNs;

        ui64 lastTimeNs = timeNs;
        for (i64 log_idx = 0; log_idx < logCount; ++log_idx) {
            ui64 x = AtomicGet(logs[log_idx]->LastTimeNs);
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
            ui64 input = AtomicGet(logs[0]->Data[i]);
            ui64 all_busy = ~0ull;
            for (i64 log_idx = 1; log_idx < logCount; ++log_idx) {
                ui64 x = AtomicGet(logs[log_idx]->Data[i]);
                all_busy &= x;
            }
            if (!input) {
                if (!bucket) {
                    Idle += 64 - PopCount(all_busy);
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


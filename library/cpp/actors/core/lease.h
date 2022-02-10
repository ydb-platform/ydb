#pragma once

#include "defs.h"

namespace NActors {
    // Value representing specific worker's permission for exclusive use of CPU till specific deadline
    struct TLease {
        // Lower WorkerBits store current fast worker id
        // All other higher bits store expiration (hard preemption) timestamp
        using TValue = ui64;
        TValue Value;

        static constexpr ui64 WorkerIdMask = ui64((1ull << WorkerBits) - 1);
        static constexpr ui64 ExpireTsMask = ~WorkerIdMask;

        explicit constexpr TLease(ui64 value)
            : Value(value)
        {}

        constexpr TLease(TWorkerId workerId, ui64 expireTs)
            : Value((workerId & WorkerIdMask) | (expireTs & ExpireTsMask))
        {}

        TWorkerId GetWorkerId() const {
            return Value & WorkerIdMask;
        }

        TLease NeverExpire() const {
            return TLease(Value | ExpireTsMask);
        }

        bool IsNeverExpiring() const {
            return (Value & ExpireTsMask) == ExpireTsMask;
        }

        ui64 GetExpireTs() const {
            // Do not truncate worker id
            // NOTE: it decrease accuracy, but improves performance
            return Value;
        }

        ui64 GetPreciseExpireTs() const {
            return Value & ExpireTsMask;
        }

        operator TValue() const {
            return Value;
        }
    };

    // Special expire timestamp values
    static constexpr ui64 NeverExpire = ui64(-1);

    // Special hard-preemption-in-progress lease
    static constexpr TLease HardPreemptionLease = TLease(TLease::WorkerIdMask, NeverExpire);
}

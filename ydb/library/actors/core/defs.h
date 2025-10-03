#pragma once

// unique tag to fix pragma once gcc glueing: ./library/actorlib/core/defs.h

#include <ydb/library/actors/util/defs.h>
#include <util/generic/hash.h>
#include <util/string/printf.h>

// Enables collection of
//    event send/receive counts
//    activation time histograms
//    event processing time histograms
#define ACTORSLIB_COLLECT_EXEC_STATS

namespace NActors {
    using TPoolId = ui8;
    using TPoolsMask = ui64;
    static constexpr TPoolId PoolBits = 6;
    static constexpr TPoolId MaxPools = (1 << PoolBits) - 1; // maximum amount of pools (poolid=63 is reserved)
    static constexpr TPoolsMask WaitPoolsFlag = (1ull << MaxPools); // wait-for-slow-workers flag bitmask

    // Special TPoolId values used by TCpuState
    static constexpr TPoolId CpuSpinning = MaxPools; // fast-worker is actively spinning, no slow-workers
    static constexpr TPoolId CpuBlocked = MaxPools + 1; // fast-worker is blocked, no slow-workers
    static constexpr TPoolId CpuStopped = TPoolId(-1); // special value indicating worker should stop
    static constexpr TPoolId CpuShared = MaxPools; // special value for `assigned` meaning balancer disabled, pool scheduler is used instead

    using TPoolWeight = ui16;
    static constexpr TPoolWeight MinPoolWeight = 1;
    static constexpr TPoolWeight DefPoolWeight = 32;
    static constexpr TPoolWeight MaxPoolWeight = 1024;

    using TWorkerId = i16;
    static constexpr TWorkerId WorkerBits = 11;
    static constexpr TWorkerId MaxWorkers = 1 << WorkerBits;

    using TThreadId = ui64;
    static constexpr TThreadId UnknownThreadId = ui64(-1);

    struct TMailboxType {
        enum EType {
            Inherited = -1, // inherit mailbox from parent
            Simple = 0, // simplest queue under producer lock. fastest in no-contention case
            Revolving = 1, // somewhat outdated, tries to be wait-free. replaced by ReadAsFilled
            HTSwap = 2, // other simple lf queue, suggested for low-contention case
            ReadAsFilled = 3, // wait-free queue, suggested for high-contention or latency critical
            TinyReadAsFilled = 4, // same as 3 but with lower overhead
            //Inplace;
            //Direct;
            //Virtual
        };
    };

    struct TScopeId : std::pair<ui64, ui64> {
        using TBase = std::pair<ui64, ui64>;
        using TBase::TBase;
        static const TScopeId LocallyGenerated;
    };

    static inline TString ScopeIdToString(const TScopeId& scopeId) {
        return Sprintf("<%" PRIu64 ":%" PRIu64 ">", scopeId.first, scopeId.second);
    }

    enum class ESendingType {
        Common,
        Lazy,
        Tail,
    };

    enum class EASProfile {
        Default,
        LowCpuConsumption,
        LowLatency,
    };
}

template<>
struct hash<NActors::TScopeId> : hash<std::pair<ui64, ui64>> {};

class TAffinity;

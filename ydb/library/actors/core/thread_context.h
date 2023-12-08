#pragma once

#include "defs.h"

#include <ydb/library/actors/util/datetime.h>

#include <util/system/tls.h>


namespace NActors {

    class IExecutorPool;

    template <typename T>
    struct TWaitingStats;

    struct TTimers {
        NHPTimer::STime Elapsed = 0;
        NHPTimer::STime Parked = 0;
        NHPTimer::STime Blocked = 0;
        NHPTimer::STime HPStart = GetCycleCountFast();
        NHPTimer::STime HPNow;

        void Reset() {
            Elapsed = 0;
            Parked = 0;
            Blocked = 0;
            HPStart = GetCycleCountFast();
            HPNow = HPStart;
        }
    };

    struct TThreadContext {
        IExecutorPool *Pool = nullptr;
        ui32 CapturedActivation = 0;
        ESendingType CapturedType = ESendingType::Lazy;
        ESendingType SendingType = ESendingType::Common;
        bool IsEnoughCpu = true;
        ui32 WriteTurn = 0;
        TWorkerId WorkerId;
        ui16 LocalQueueSize = 0;
        TWaitingStats<ui64> *WaitingStats = nullptr;
        bool IsCurrentRecipientAService = false;
        TTimers Timers;
    };

    extern Y_POD_THREAD(TThreadContext*) TlsThreadContext; // in actor.cpp

}

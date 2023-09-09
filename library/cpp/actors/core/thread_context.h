#pragma once

#include "defs.h"

#include <util/system/tls.h>


namespace NActors {

    class IExecutorPool;

    struct TThreadContext {
        IExecutorPool *Pool = nullptr;
        ui32 CapturedActivation = 0;
        ESendingType CapturedType = ESendingType::Lazy;
        ESendingType SendingType = ESendingType::Common;
        bool IsEnoughCpu = true;
        ui32 WriteTurn = 0;
        TWorkerId WorkerId;
        ui16 LocalQueueSize = 0;
    };

    extern Y_POD_THREAD(TThreadContext*) TlsThreadContext; // in actor.cpp

}

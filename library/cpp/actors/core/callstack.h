#pragma once

#ifndef NDEBUG
//#define ENABLE_ACTOR_CALLSTACK
#endif

#ifdef ENABLE_ACTOR_CALLSTACK
#include "defs.h"
#include <util/system/backtrace.h>
#include <util/stream/str.h>
#include <util/generic/deque.h>
#define USE_ACTOR_CALLSTACK

namespace NActors {
    struct TCallstack {
        struct TTrace {
            static const size_t CAPACITY = 50;
            void* Data[CAPACITY];
            size_t Size;
            size_t LinesToSkip;

            TTrace()
                : Size(0)
                , LinesToSkip(0)
            {
            }
        };

        static const size_t RECORDS = 8;
        static const size_t RECORDS_TO_SKIP = 2;
        TTrace Record[RECORDS];
        size_t BeginIdx;
        size_t Size;
        size_t LinesToSkip;

        TCallstack();
        void SetLinesToSkip();
        void Trace();
        void TraceIfEmpty();
        static TCallstack& GetTlsCallstack();
        static void DumpCallstack(TStringStream& str);
    };

    void EnableActorCallstack();
    void DisableActorCallstack();

}

#else

namespace NActors {
    inline void EnableActorCallstack(){}

    inline void DisableActorCallstack(){}

}

#endif

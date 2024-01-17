#include "callstack.h"
#include <util/thread/singleton.h>

#ifdef USE_ACTOR_CALLSTACK

namespace NActors {
    namespace {
        void (*PreviousFormatBackTrace)(IOutputStream*) = 0;
        ui32 ActorBackTraceEnableCounter = 0;
    }

    void ActorFormatBackTrace(IOutputStream* out) {
        TStringStream str;
        PreviousFormatBackTrace(&str);
        str << Endl;
        TCallstack::DumpCallstack(str);
        *out << str.Str();
    }

    void EnableActorCallstack() {
        if (ActorBackTraceEnableCounter == 0) {
            Y_ABORT_UNLESS(PreviousFormatBackTrace == 0);
            PreviousFormatBackTrace = SetFormatBackTraceFn(ActorFormatBackTrace);
        }

        ++ActorBackTraceEnableCounter;
    }

    void DisableActorCallstack() {
        --ActorBackTraceEnableCounter;

        if (ActorBackTraceEnableCounter == 0) {
            Y_ABORT_UNLESS(PreviousFormatBackTrace);
            SetFormatBackTraceFn(PreviousFormatBackTrace);
            PreviousFormatBackTrace = 0;
        }
    }

    TCallstack::TCallstack()
        : BeginIdx(0)
        , Size(0)
        , LinesToSkip(0)
    {
    }

    void TCallstack::SetLinesToSkip() {
        TTrace record;
        LinesToSkip = BackTrace(record.Data, TTrace::CAPACITY);
    }

    void TCallstack::Trace() {
        size_t currentIdx = (BeginIdx + Size) % RECORDS;
        if (Size == RECORDS) {
            ++BeginIdx;
        } else {
            ++Size;
        }
        TTrace& record = Record[currentIdx];
        record.Size = BackTrace(record.Data, TTrace::CAPACITY);
        record.LinesToSkip = LinesToSkip;
    }

    void TCallstack::TraceIfEmpty() {
        if (Size == 0) {
            LinesToSkip = 0;
            Trace();
        }
    }

    TCallstack& TCallstack::GetTlsCallstack() {
        return *FastTlsSingleton<TCallstack>();
    }

    void TCallstack::DumpCallstack(TStringStream& str) {
        TCallstack& callstack = GetTlsCallstack();
        for (int i = callstack.Size - 1; i >= 0; --i) {
            TTrace& record = callstack.Record[(callstack.BeginIdx + i) % RECORDS];
            str << Endl << "Trace entry " << i << Endl << Endl;
            size_t size = record.Size;
            if (size > record.LinesToSkip && size < TTrace::CAPACITY) {
                size -= record.LinesToSkip;
            }
            if (size > RECORDS_TO_SKIP) {
                FormatBackTrace(&str, &record.Data[RECORDS_TO_SKIP], size - RECORDS_TO_SKIP);
            } else {
                FormatBackTrace(&str, record.Data, size);
            }
            str << Endl;
        }
    }
}

#endif

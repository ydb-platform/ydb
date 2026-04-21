#pragma once

#include <util/generic/noncopyable.h>
#include <util/generic/ptr.h>

#include <ydb/library/actors/trace_data/trace_data.h>

namespace NActors {
    class IActor;
    class IEventHandle;

    namespace NTracing {

        struct TSettings {
            size_t MaxBufferSizePerThread = 10240;
            size_t MaxThreads = 128;
            bool AutoStart = false;
        };

        enum class ETracerState : ui32 {
            Idle = 0,
            Recording = 1,
            Fetching = 2,
            Starting = 3,
        };

        class IActorTracer : TNonCopyable {
        public:
            virtual ~IActorTracer() = default;
            virtual void HandleNew(IActor& actor) = 0;
            virtual void HandleDie(IActor& actor) = 0;
            virtual void HandleSend(IEventHandle& event) = 0;
            virtual void HandleReceive(IActor& recipient, IEventHandle& event) = 0;
            virtual void HandleForward(ui64 oldHandlePtr, IEventHandle& event, ui32 originalType) = 0;
            virtual bool Start() = 0;
            virtual bool Stop() = 0;
            virtual TTraceChunk GetTraceData() = 0;
        };

        THolder<IActorTracer> CreateActorTracer(TSettings settings);

    } // namespace NTracing
} // namespace NActors

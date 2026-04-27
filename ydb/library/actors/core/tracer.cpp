#include "tracer.h"

#include "actor.h"
#include "actorid.h"

#include <util/datetime/base.h>
#include <util/system/thread.h>

#include <algorithm>
#include <atomic>
#include <cstring>
#include <limits>
#include <vector>

namespace NActors::NTracing {

    struct TThreadBuffer {
        std::vector<TTraceEvent> Events;
        std::atomic<ui64> WritePos{0};
        TEventNamesDict EventNames;

        explicit TThreadBuffer(size_t capacity) {
            Events.resize(capacity);
        }
    };

    class TInternalTracer {
    public:
        explicit TInternalTracer(TSettings settings)
            : BufferSize(settings.MaxBufferSizePerThread)
            , MaxThreads(settings.MaxThreads)
        {
            Buffers.reserve(MaxThreads);
            for (size_t i = 0; i < MaxThreads; ++i) {
                Buffers.push_back(std::make_unique<TThreadBuffer>(BufferSize));
            }
        }

        TThreadBuffer* AcquireBuffer(ui32& outIdx) {
            ui32 idx = NextBufferIdx.fetch_add(1, std::memory_order_relaxed);
            if (idx >= MaxThreads) {
                return nullptr;
            }
            outIdx = idx;
            return Buffers[idx].get();
        }

        void UpdateThreadPoolName(ui32 idx) {
            TString threadName = TThread::CurrentThreadName();
            if (threadName) {
                TGuard<TAdaptiveLock> guard(ThreadPoolDictLock);
                ThreadPoolNames[idx] = threadName;
            }
        }

        void AddEvent(TThreadBuffer* buf, TTraceEvent event, ui8 threadIdx) {
            const ui64 nowUs = TInstant::Now().MicroSeconds();
            const ui64 startUs = StartTimestampUs.load(std::memory_order_relaxed);
            const ui64 delta = (nowUs > startUs) ? (nowUs - startUs) : 0;
            event.DeltaUs = static_cast<ui32>(std::min<ui64>(delta, std::numeric_limits<ui32>::max()));
            event.Flags = threadIdx;
            ui64 pos = buf->WritePos.fetch_add(1, std::memory_order_release);
            buf->Events[pos % BufferSize] = event;
            if (pos == 0) {
                UpdateThreadPoolName(threadIdx);
            }
        }

        void RegisterEventTypeName(TThreadBuffer* buf, ui32 typeIndex, const TString& typeName) {
            buf->EventNames.emplace(typeIndex, typeName);
        }

        TTraceChunk GetTraceChunk() {
            const ui64 startTimestampUs = StartTimestampUs.load(std::memory_order_acquire);
            TVector<TTraceEvent> events;
            ui32 usedBuffers = NextBufferIdx.load(std::memory_order_acquire);
            usedBuffers = std::min<ui32>(usedBuffers, MaxThreads);

            TEventNamesDict eventNames;

            for (ui32 i = 0; i < usedBuffers; ++i) {
                auto& buf = *Buffers[i];
                ui64 pos = buf.WritePos.load(std::memory_order_acquire);
                if (pos == 0) continue;

                ui64 first = (pos > BufferSize) ? pos - BufferSize : 0;
                ui64 safeEnd = pos - 1;

                for (ui64 j = first; j < safeEnd; ++j) {
                    events.push_back(buf.Events[j % BufferSize]);
                }

                for (const auto& [typeIndex, typeName] : buf.EventNames) {
                    eventNames.emplace(typeIndex, typeName);
                }
            }

            TThreadPoolDict threadPoolDict;
            {
                TGuard<TAdaptiveLock> guard(ThreadPoolDictLock);
                for (const auto& [idx, name] : ThreadPoolNames) {
                    threadPoolDict.emplace_back(idx, name);
                }
            }

            return {
                .StartTimestampUs = startTimestampUs,
                .ActivityDict = BuildActivityDict(),
                .EventNamesDict = std::move(eventNames),
                .ThreadPoolDict = std::move(threadPoolDict),
                .Events = std::move(events),
            };
        }

        void ResetBuffers() {
            ui32 used = NextBufferIdx.load(std::memory_order_acquire);
            used = std::min<ui32>(used, MaxThreads);
            for (ui32 i = 0; i < used; ++i) {
                Buffers[i]->WritePos.store(0, std::memory_order_release);
            }
        }

        void RecordStartTimestamp() {
            StartTimestampUs.store(TInstant::Now().MicroSeconds(), std::memory_order_release);
        }

    private:
        TActivityDict BuildActivityDict() const {
            auto& registry = TLocalProcessKeyState<TActorActivityTag>::GetInstance();
            TActivityDict dict;
            for (ui32 index = 0; index < registry.GetCount(); ++index) {
                auto name = registry.GetNameByIndex(index);
                if (!name.empty()) {
                    dict.emplace_back(index, TString(name));
                }
            }
            return dict;
        }

    private:
        size_t BufferSize;
        size_t MaxThreads;
        std::vector<std::unique_ptr<TThreadBuffer>> Buffers;
        std::atomic<ui32> NextBufferIdx{0};
        std::atomic<ui64> StartTimestampUs{0};

        TAdaptiveLock ThreadPoolDictLock;
        THashMap<ui32, TString> ThreadPoolNames;
    };

    class TActorTracer : public IActorTracer {
    public:
        explicit TActorTracer(TSettings settings)
            : AutoStart(settings.AutoStart)
            , TracerImpl(std::move(settings))
        {
            if (AutoStart) {
                Start();
            }
        }

        void HandleNew(IActor& actor) override {
            if (State.load(std::memory_order_acquire) != ETracerState::Recording) {
                return;
            }
            auto& ts = GetThreadState();
            if (!ts.Buffer) return;
            TTraceEvent ev{};
            ev.Type = static_cast<ui8>(ETraceEventType::New);
            ev.Actor1 = actor.SelfId().LocalId();
            ev.Extra = static_cast<ui16>(actor.GetActivityType().GetIndex());
            TracerImpl.AddEvent(ts.Buffer, ev, ts.Idx);
        }

        void HandleDie(IActor& actor) override {
            if (State.load(std::memory_order_acquire) != ETracerState::Recording) {
                return;
            }
            auto& ts = GetThreadState();
            if (!ts.Buffer) return;
            TTraceEvent ev{};
            ev.Type = static_cast<ui8>(ETraceEventType::Die);
            ev.Actor1 = actor.SelfId().LocalId();
            TracerImpl.AddEvent(ts.Buffer, ev, ts.Idx);
        }

        void HandleSend(IEventHandle& event) override {
            if (State.load(std::memory_order_acquire) != ETracerState::Recording) {
                return;
            }
            auto& ts = GetThreadState();
            if (!ts.Buffer) return;
            TTraceEvent ev{};
            ev.Type = static_cast<ui8>(ETraceEventType::SendLocal);
            ev.Actor1 = event.Sender.LocalId();
            ev.Actor2 = event.GetRecipientRewrite().LocalId();
            ev.Aux = event.Type;
            ev.Extra = ts.CurrentActivityIndex;
            ev.HandleHash = HashHandlePointer(reinterpret_cast<ui64>(&event));
            TracerImpl.AddEvent(ts.Buffer, ev, ts.Idx);
            TracerImpl.RegisterEventTypeName(ts.Buffer, event.Type, event.GetTypeName());
        }

        void HandleReceive(IActor& recipient, IEventHandle& event) override {
            if (State.load(std::memory_order_acquire) != ETracerState::Recording) {
                return;
            }
            auto& ts = GetThreadState();
            if (!ts.Buffer) return;
            const ui16 activityIndex = static_cast<ui16>(recipient.GetActivityType().GetIndex());
            ts.CurrentActivityIndex = activityIndex;
            TTraceEvent ev{};
            ev.Type = static_cast<ui8>(ETraceEventType::ReceiveLocal);
            ev.Actor1 = event.Sender.LocalId();
            ev.Actor2 = event.GetRecipientRewrite().LocalId();
            ev.Aux = event.Type;
            ev.Extra = activityIndex;
            ev.HandleHash = HashHandlePointer(reinterpret_cast<ui64>(&event));
            TracerImpl.AddEvent(ts.Buffer, ev, ts.Idx);
            TracerImpl.RegisterEventTypeName(ts.Buffer, event.Type, event.GetTypeName());
        }

        void HandleForward(ui64 oldHandlePtr, IEventHandle& event, ui32 originalType) override {
            if (State.load(std::memory_order_acquire) != ETracerState::Recording) {
                return;
            }
            auto& ts = GetThreadState();
            if (!ts.Buffer) return;
            TTraceEvent ev{};
            ev.Type = static_cast<ui8>(ETraceEventType::ForwardLocal);
            ev.Actor1 = HashHandlePointer(reinterpret_cast<ui64>(&event));
            ev.Actor2 = event.GetRecipientRewrite().LocalId();
            ev.Aux = originalType;
            ev.Extra = ts.CurrentActivityIndex;
            ev.HandleHash = HashHandlePointer(oldHandlePtr);
            TracerImpl.AddEvent(ts.Buffer, ev, ts.Idx);
            TracerImpl.RegisterEventTypeName(ts.Buffer, originalType, event.GetTypeName());
        }

        bool Start() override {
            auto expected = ETracerState::Idle;
            if (!State.compare_exchange_strong(expected, ETracerState::Starting)) {
                return false;
            }
            TracerImpl.ResetBuffers();
            TracerImpl.RecordStartTimestamp();
            State.store(ETracerState::Recording, std::memory_order_release);
            return true;
        }

        bool Stop() override {
            auto expected = ETracerState::Recording;
            return State.compare_exchange_strong(expected, ETracerState::Idle);
        }

        TTraceChunk GetTraceData() override {
            auto expected = ETracerState::Idle;
            if (!State.compare_exchange_strong(expected, ETracerState::Fetching)) {
                return {};
            }
            auto chunk = TracerImpl.GetTraceChunk();
            State.store(ETracerState::Idle, std::memory_order_release);
            return chunk;
        }

        ~TActorTracer() override {
            auto st = State.load();
            if (st == ETracerState::Recording) {
                Stop();
            }
        }

    private:
        struct TThreadState {
            TThreadBuffer* Buffer = nullptr;
            ui8 Idx = 0;
            ui16 CurrentActivityIndex = 0;
        };

        TThreadState& GetThreadState() {
            thread_local TThreadState state;
            if (!state.Buffer) {
                ui32 idx = 0;
                state.Buffer = TracerImpl.AcquireBuffer(idx);
                state.Idx = static_cast<ui8>(idx);
            }
            return state;
        }

        bool AutoStart = false;
        TInternalTracer TracerImpl;
        std::atomic<ETracerState> State{ETracerState::Idle};
    };

    THolder<IActorTracer> CreateActorTracer(TSettings settings) {
        return MakeHolder<TActorTracer>(std::move(settings));
    }

} // namespace NActors::NTracing

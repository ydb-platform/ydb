#include "tracer.h"

#include "actor.h"
#include "actorid.h"

#include <library/cpp/containers/concurrent_hash/concurrent_hash.h>
#include <library/cpp/containers/ring_buffer/ring_buffer.h>

#include <util/datetime/base.h>
#include <util/system/thread.h>

namespace NActors::NTracing {

    class TInternalTracer {
    public:
        explicit TInternalTracer(TSettings&& settings)
            : Settings(std::move(settings))
        {}

        void AddEvent(TTraceEvent event) {
            event.Timestamp = TInstant::Now().MicroSeconds();
            auto& threadData = ThreadId2Data.InsertIfAbsentWithInit(
                TThread::CurrentThreadId(),
                [&settings = this->Settings] {
                    return TThreadData{
                        .Buffer = MakeAtomicShared<TRingBuffer>(settings.MaxBufferSizePerThread),
                    };
                }
            );
            threadData.Buffer->PushBack(std::move(event));
        }

        void RegisterEventTypeName(ui32 typeIndex, const TString& typeName) {
            auto threadId = TThread::CurrentThreadId();
            auto& bucket = ThreadId2Data.GetBucketForKey(threadId);
            TThreadIdMapping::TBucketGuard guard(bucket.GetMutex());
            bucket.GetUnsafe(threadId).EventNameDict.emplace(typeIndex, typeName);
        }

        TTraceChunk GetTraceChunk() {
            auto [events, eventNamesDict] = CollectEvents();
            return {
                .ActivityDict = BuildActivityDict(),
                .EventNamesDict = std::move(eventNamesDict),
                .Events = std::move(events),
            };
        }

        void ClearBuffers() {
            for (size_t i = 0; i < DEFAULT_BUCKET_COUNT; ++i) {
                auto& bucket = ThreadId2Data.Buckets[i];
                TThreadIdMapping::TBucketGuard guard(bucket.GetMutex());
                for (auto& [threadId, threadData] : bucket.GetMap()) {
                    threadData.Buffer->Clear();
                    threadData.EventNameDict.clear();
                }
            }
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

        std::pair<TVector<TTraceEvent>, TEventNamesDict> CollectEvents() {
            TVector<TTraceEvent> events;
            TEventNamesDict eventNames;
            for (size_t i = 0; i < DEFAULT_BUCKET_COUNT; ++i) {
                auto& bucket = ThreadId2Data.Buckets[i];
                TThreadIdMapping::TBucketGuard guard(bucket.GetMutex());
                for (auto& [threadId, threadData] : bucket.GetMap()) {
                    auto& buffer = *threadData.Buffer;
                    events.reserve(events.size() + buffer.AvailSize());
                    for (size_t idx = buffer.FirstIndex(); idx < buffer.TotalSize(); ++idx) {
                        events.push_back(buffer[idx]);
                    }
                    eventNames.insert(threadData.EventNameDict.begin(), threadData.EventNameDict.end());
                }
            }
            return {std::move(events), std::move(eventNames)};
        }

    private:
        TSettings Settings;
        using TRingBuffer = TSimpleRingBuffer<TTraceEvent>;
        using TRingBufferPtr = TAtomicSharedPtr<TRingBuffer>;
        struct TThreadData {
            TRingBufferPtr Buffer;
            TEventNamesDict EventNameDict;
        };

        static constexpr size_t DEFAULT_BUCKET_COUNT = 64;
        using TThreadIdMapping = TConcurrentHashMap<TThread::TId, TThreadData, DEFAULT_BUCKET_COUNT, TSpinLock>;
        TThreadIdMapping ThreadId2Data;
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
            if (!Started.load(std::memory_order_acquire)) {
                return;
            }
            TTraceEvent ev{};
            ev.Type = static_cast<ui8>(ETraceEventType::New);
            ev.Actor1 = actor.SelfId().LocalId();
            TracerImpl.AddEvent(std::move(ev));
        }

        void HandleDie(IActor& actor) override {
            if (!Started.load(std::memory_order_acquire)) {
                return;
            }
            TTraceEvent ev{};
            ev.Type = static_cast<ui8>(ETraceEventType::Die);
            ev.Actor1 = actor.SelfId().LocalId();
            TracerImpl.AddEvent(std::move(ev));
        }

        void HandleSend(IEventHandle& event) override {
            if (!Started.load(std::memory_order_acquire)) {
                return;
            }
            TTraceEvent ev{};
            ev.Type = static_cast<ui8>(ETraceEventType::SendLocal);
            ev.Actor1 = event.Sender.LocalId();
            ev.Actor2 = event.GetRecipientRewrite().LocalId();
            ev.Aux = event.GetTypeRewrite();
            TracerImpl.AddEvent(std::move(ev));
            TracerImpl.RegisterEventTypeName(event.GetTypeRewrite(), event.GetTypeName());
        }

        void HandleReceive(IActor& recipient, IEventHandle& event) override {
            if (!Started.load(std::memory_order_acquire)) {
                return;
            }
            TTraceEvent ev{};
            ev.Type = static_cast<ui8>(ETraceEventType::ReceiveLocal);
            ev.Actor1 = event.Sender.LocalId();
            ev.Actor2 = event.GetRecipientRewrite().LocalId();
            ev.Aux = event.GetTypeRewrite();
            ev.Extra = static_cast<ui16>(recipient.GetActivityType().GetIndex());
            TracerImpl.AddEvent(std::move(ev));
            TracerImpl.RegisterEventTypeName(event.GetTypeRewrite(), event.GetTypeName());
        }

        void Start() override {
            TracerImpl.ClearBuffers();
            Started.store(true, std::memory_order_release);
        }

        void Stop() override {
            Started.store(false, std::memory_order_release);
        }

        TTraceChunk GetTraceData() override {
            return TracerImpl.GetTraceChunk();
        }

        ~TActorTracer() override {
            Stop();
        }

    private:
        bool AutoStart = false;
        TInternalTracer TracerImpl;
        std::atomic<bool> Started{false};
    };

    THolder<IActorTracer> CreateActorTracer(TSettings settings) {
        return MakeHolder<TActorTracer>(std::move(settings));
    }

} // namespace NActors::NTracing

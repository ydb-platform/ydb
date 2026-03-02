#include "tracer.h"

#include "actor.h"
#include "actorid.h"

#include <library/cpp/containers/concurrent_hash/concurrent_hash.h>
#include <library/cpp/containers/ring_buffer/ring_buffer.h>
#include <library/cpp/threading/queue/mpsc_htswap.h>

#include <util/system/thread.h>

#ifdef DEBUG_TRACER_MANUAL
#include <util/stream/file.h>
#endif

namespace NActors::NTracing {

    class TInternalTracer {
    private:
        using TMessage = TEvent;
    public:
        explicit TInternalTracer(TSettings&& settings)
            : Settings(std::move(settings))
        {}

        void AddMessage(TMessage&& message) {
            message.Timestamp = TInstant::Now().Seconds();
            auto& threadData = ThreadId2Data.InsertIfAbsentWithInit(TThread::CurrentThreadId(), [&queue=this->Queue, &settings=this->Settings]{
                auto buffer = MakeAtomicShared<TRingBuffer>(settings.MaxBufferSizePerThread);
                auto queueItem = MakeHolder<TRingBufferWithTid>(TRingBufferWithTid{.Buffer = buffer, .ThreadId = TThread::CurrentThreadId()});
                queue.Push(std::move(queueItem));
                return TThreadData{.Buffer = std::move(buffer)};
            });
            threadData.Buffer->PushBack(std::move(message));
        }

        TVector<TStringBuf> GetActivityDict() const {
            auto& activityIndex = TLocalProcessKeyState<TActorActivityTag>::GetInstance();
            TVector<TStringBuf> res(Reserve(activityIndex.GetCount()));
            for (ui32 index = 0; index < activityIndex.GetCount(); ++index) {
                if (auto name = activityIndex.GetNameByIndex(index); !name.empty()) {
                    res.push_back(std::move(name));
                }
            }
            return res;
        }

        void RegisterEventTypeName(ui32 typeIndex, const TString& typeName) {
            auto threadId = TThread::CurrentThreadId();
            auto& bucket = ThreadId2Data.GetBucketForKey(threadId);
            {
                TThreadIdMapping::TBucketGuard guard(bucket.GetMutex());
                bucket.GetUnsafe(threadId).EventNameDict.emplace(typeIndex, typeName);
            }
        }

        TTraceChunk GetTraceChunk() {
            auto [events, eventNamesDict] = GetTraceChunkImpl();
            return {
                .ActivityDict = GetActivityDict(),
                .EventNamesDict = std::move(eventNamesDict),
                .Events = std::move(events)
            };
        }

#ifdef DEBUG_TRACER_MANUAL
        void Dump() {
            TFileOutput fout("actors_dump.bin");
            auto traceChunk = GetTraceChunk();

            auto headerBuffer = SerializeHeader(std::move(traceChunk.ActivityDict), std::move(traceChunk.EventNamesDict));
            fout.Write(headerBuffer.data(), headerBuffer.Size());
            auto dataBuffer = SerializeEvents(std::move(traceChunk.Events));
            fout.Write(dataBuffer.data(), dataBuffer.Size());
            fout.Flush();
        }
#endif

    private:
        std::pair<TTraceChunk::TEvents, TEventNamesDict> GetTraceChunkImpl() {
            TTraceChunk::TEvents events;
            TEventNamesDict eventNames;
            while (!Queue.IsEmpty()) {
                auto bufferItem = Queue.Pop();
                if (!bufferItem) {
                    continue;
                }

                {
                    auto threadData = ThreadId2Data.Remove(bufferItem->ThreadId);
                    eventNames.insert(
                        std::make_move_iterator(threadData.EventNameDict.begin()),
                        std::make_move_iterator(threadData.EventNameDict.end())
                    );
                }
                auto& buffer = *bufferItem->Buffer;
                events.reserve(events.size() + buffer.AvailSize());
                for (size_t idx = buffer.FirstIndex(); idx < buffer.TotalSize(); ++idx) {
                    events.push_back(buffer[idx]);
                }
            }
            return std::make_pair(events, eventNames);
        }

    private:
        TSettings Settings;
        using TRingBuffer = TSimpleRingBuffer<TMessage>;
        using TRingBufferPtr = TAtomicSharedPtr<TRingBuffer>;
        struct TThreadData {
            TRingBufferPtr Buffer;
            TEventNamesDict EventNameDict;
        };

        static constexpr size_t DEFAULT_BUCKET_COUNT = 64;
        using TThreadIdMapping = TConcurrentHashMap<TThread::TId, TThreadData, DEFAULT_BUCKET_COUNT, TSpinLock>;
        TThreadIdMapping ThreadId2Data;
        struct TRingBufferWithTid {
            TRingBufferPtr Buffer;
            TThread::TId ThreadId;
        };
        NThreading::THTSwapQueue<THolder<TRingBufferWithTid>> Queue;
    };

    TLocalActorId ConvertToLocalId(const TActorId& actorId) {
        return {.LocalId = actorId.LocalId()};
    }

    TGlobalActorId ConvertToGlobalId(const TActorId& actorId) {
        return {.LocalId = actorId.LocalId(), .NodeId = actorId.NodeId()};
    }

    class TActorTracer: public IActorTracer {
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
            if (!Started.load(std::memory_order::acquire)) {
                return;
            }

            TEvent message {
                .Event = {
                    .NewEvent = {
                        .ActorId = ConvertToLocalId(actor.SelfId())
                    }
                },
                .Type = TEvent::EType::New
            };
            TracerImpl.AddMessage(std::move(message));
        }

        void HandleDie(IActor& actor) override {
            if (!Started.load(std::memory_order::acquire)) {
                return;
            }

            TEvent message {
                .Event = {
                    .DieEvent = {
                        .ActorId = ConvertToLocalId(actor.SelfId())
                    }
                },
                .Type = TEvent::EType::Die
            };
            TracerImpl.AddMessage(std::move(message));
        }

        void HandleInterconnectSend(IEventHandle& event, ui32 interconnectSequenceId) override {
            if (!Started.load(std::memory_order::acquire)) {
                return;
            }
            TEvent message = {
                .Event = {
                    .SendInterconnectEvent = {
                        .Sender = ConvertToGlobalId(event.Sender),
                        .Recipient = ConvertToGlobalId(event.Recipient),
                        .ObjectId = static_cast<ui64>(reinterpret_cast<ptrdiff_t>(&event)),
                    }
                },
                .Type = TEvent::EType::SendInterconnect
            };
            message.Event.SendInterconnectEvent.InterconnectSequenceId = interconnectSequenceId;
            TracerImpl.AddMessage(std::move(message));
        }

        void HandleInterconnectRecieve(IEventHandle& event, ui32 interconnectSequenceId) override {
            if (!Started.load(std::memory_order::acquire)) {
                return;
            }

            TEvent message = {
                .Event = {
                    .RecieveInterconnectEvent = {
                        .Sender = ConvertToGlobalId(event.Sender),
                        .Recipient = ConvertToGlobalId(event.Recipient),
                        .ObjectId = static_cast<ui64>(reinterpret_cast<ptrdiff_t>(&event)),
                        .MessageType = event.GetTypeRewrite()
                    }
                },
                .Type = TEvent::EType::RecieveInterconnect
            };
            message.Event.RecieveInterconnectEvent.InterconnectSequenceId = interconnectSequenceId;

            TracerImpl.AddMessage(std::move(message));
        }

        void HandleSend(IEventHandle& event) override {
            if (!Started.load(std::memory_order::acquire)) {
                return;
            }

            TEvent message = {
                .Event = {
                    .SendLocalEvent = {
                        .Sender = ConvertToLocalId(event.Sender),
                        .Recipient = ConvertToLocalId(event.Recipient),
                        .ObjectId = static_cast<ui64>(reinterpret_cast<ptrdiff_t>(&event))
                    }
                },
                .Type = TEvent::EType::SendLocal
            };
            TracerImpl.AddMessage(std::move(message));
            TracerImpl.RegisterEventTypeName(event.GetTypeRewrite(), event.GetTypeName());
        }

        void HandleReceive(IActor& recipient, IEventHandle& event) override {
            if (!Started.load(std::memory_order::acquire)) {
                return;
            }

            TEvent message = {
                .Event = {
                    .RecieveLocalEvent = {
                        .Sender = ConvertToLocalId(event.Sender),
                        .Recipient = ConvertToLocalId(event.Recipient),
                        .ObjectId = static_cast<ui64>(reinterpret_cast<ptrdiff_t>(&event)),
                        .MessageType = event.GetTypeRewrite()
                    }
                },
                .Type = TEvent::EType::RecieveLocal
            };
            message.Event.RecieveLocalEvent.ActivityIndex = recipient.GetActivityType().GetIndex();

            TracerImpl.AddMessage(std::move(message));
            TracerImpl.RegisterEventTypeName(event.GetTypeRewrite(), event.GetTypeName());
        }

        void Start() override {
            Started.store(true, std::memory_order::release);
        }

        void Stop() override {
            Started.store(false, std::memory_order::release);
        }

        TTraceChunk GetTraceData() override {
            return TracerImpl.GetTraceChunk();
        }

        ~TActorTracer() override {
            Stop();
#ifdef DEBUG_TRACER_MANUAL
            TracerImpl.Dump();
#endif
        }
    private:
        bool AutoStart = false;
        TInternalTracer TracerImpl;
        std::atomic<bool> Started;
    };

    THolder<IActorTracer> CreateActorTracer(TSettings settings) {
       return MakeHolder<TActorTracer>(std::move(settings));
    }
}

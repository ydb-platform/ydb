#include "interconnect_uring_engine.h"

#include "uring_recv_buffer_pool.h"
#include "uring_context.h" // for TUringContext::IsSupported() / SqThreadIdleMs

#include "v2_event_serializer.h"
#include "interconnect_direct_session.h"
#include "interconnect_uring_event_queue.h"

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/actor.h>

#include <ydb/library/actors/protos/interconnect.pb.h>

// Must be included AFTER YDB headers because linux/uapi headers pulled by
// liburing may define macros that clash with project headers.
#include <ydb/library/uring/liburing_linux.h>

#include <library/cpp/monlib/service/pages/templates.h>

#include <util/system/env.h>
#include <util/system/hp_timer.h>

#include <sys/socket.h>
#include <sys/uio.h>
#include <sys/timerfd.h>
#include <sys/eventfd.h>

#include <cerrno>
#include <deque>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

namespace NActors {

    namespace {
        constexpr ui32 RingQueueDepth = 4096;
        constexpr unsigned CqeBatchSize = 64;
        constexpr size_t ReadBufferSize = 262144;
        constexpr size_t MinReadBufferSize = 65536;
        constexpr size_t MinWriteBufferSize = 4096;
        constexpr size_t MaxWriteBufferSize = 262144;
        constexpr size_t MaxSpansPerWrite = 64;
        constexpr size_t MinSerializeWindowSize = 4096;
        constexpr size_t MaxSerializeWindowSize = 262144;
        constexpr ui32 RebalanceTimerMs = 500; // drives MaybeOffload and also issues ping packet
        constexpr ui32 OffloadBusyThreshold = 700000; // ppm
        constexpr ui32 StealBusyThreshold = 300000; // ppm
    }

    struct TEvUringMonRequest : TEventLocal<TEvUringMonRequest, static_cast<ui32>(ENetwork::EvUringMonRequest)> {
        NMon::TEvHttpInfoRes::TPtr Ev;

        TEvUringMonRequest(NMon::TEvHttpInfoRes::TPtr ev)
            : Ev(std::move(ev))
        {}
    };

    class TUringEngine final : public IUringEngine {
        TActorSystem *ActorSystem = nullptr; // bound after construction via SetActorSystem()
        std::once_flag ActorSystemInitFlag;
        std::atomic_bool Stopping{false};

        enum class EMigrateState : ui8 {
            None = 0,
            Draining,
            HandedOff,
        };

        // Low 3 bits of the session pointer are used as an io_uring user_data op tag; heap allocation
        // alignment of this type is already >= 8 (actually 64 via base/members).
        struct TRegisteredSession : TEventDeserializer::IEventProcessor {
            std::atomic_uint32_t OwnerShard;
            const TIntrusivePtr<NInterconnect::TStreamSocket> Socket;
            const TActorId SessionId;
            const std::function<void(TDisconnectReason)> OnDisconnectCallback;
            TActorSystem* const ActorSystem;
            TEventSerializer Serializer;
            TEventDeserializer Deserializer;
            TRcBuf ReadBuffer;
            bool Terminated = false;
            bool ReadPending = false;
            bool WritePending = false;
            bool UnregisterRequested = false;
            const bool SendPings;
            TRcBuf WriteBuffer;
            size_t WriteBufferSize = MinWriteBufferSize;
            std::deque<TContiguousSpan> OutgoingSpans;
            iovec Iov[MaxSpansPerWrite];
            size_t IovLen = 0;
            size_t UnsentBytes = 0;
            size_t BytesToWriteLastTime = 0;
            int ReadPendingRingIdx = -1;
            ui32 PreferredRingIdx = 0;
            std::atomic_uint64_t IncomingSeqNo{1};
            ui64 ExpectedSeqNo = 1;

            EMigrateState MigrateState = EMigrateState::None;
            ui32 MigrateTargetShard = 0;
            ui32 MigrateSourceShard = 0;

            const std::shared_ptr<std::atomic<int64_t>> ClockSkew;
            const std::shared_ptr<std::atomic<uint64_t>> PingRTT;

            THashMap<TActorId, TIntrusivePtr<IReceiveCallback>> ReceiveCallbacks;
            NMonitoring::TDynamicCounters::TCounterPtr EventsReceived;

            std::vector<TIncomingEventQueue::TRecord> PendingRecordsHeap;

            size_t SerializeWindowSize = MinSerializeWindowSize;

            TRegisteredSession(ui32 shardIdx, TIntrusivePtr<NInterconnect::TStreamSocket> socket,
                    TActorId sessionId, bool checksumming, TScopeId peerScopeId,
                    std::function<void(TDisconnectReason)> onDisconnectCallback, TActorSystem *actorSystem,
                    bool sendPings, std::shared_ptr<std::atomic<int64_t>> clockSkew,
                    std::shared_ptr<std::atomic<uint64_t>> pingRTT)
                : OwnerShard(shardIdx)
                , Socket(std::move(socket))
                , SessionId(sessionId)
                , OnDisconnectCallback(std::move(onDisconnectCallback))
                , ActorSystem(actorSystem)
                , Serializer(checksumming)
                , Deserializer(peerScopeId)
                , SendPings(sendPings)
                , ClockSkew(std::move(clockSkew))
                , PingRTT(std::move(pingRTT))
            {}

            void Disconnect(TDisconnectReason reason) {
                OnDisconnectCallback(reason);
                Terminated = true;
            }

            ////////////////////////////////////////////////////////////////////////////////////////////////////////////
            // deserialization/receiving

            TMutableContiguousSpan GetReadSpan() {
                if (ReadBuffer.size() < MinReadBufferSize) {
                    ReadBuffer = TRcBuf::Uninitialized(ReadBufferSize);
                }
                return ReadBuffer.UnsafeGetContiguousSpanMut();
            }

            void ApplyBytesRead(size_t num) {
                TRcBuf chunk = {TRcBuf::Piece, ReadBuffer.data(), num, ReadBuffer};
                Deserializer.Push(std::move(chunk), this, SessionId);
                Y_ABORT_UNLESS(num <= ReadBuffer.size());
                const size_t remain = ReadBuffer.size() - num;
                ReadBuffer.TrimFront(remain - remain % 64); // make only this number of bytes remaining in buffer
            }

            void PushEvent(std::unique_ptr<IEventHandle> ev) override {
                if (const auto it = ReceiveCallbacks.find(ev->Recipient); it != ReceiveCallbacks.end()) {
                    it->second->Receive(ev.release());
                } else {
                    ActorSystem->Send(ev.release());
                }
                ++*EventsReceived;
            }

            ////////////////////////////////////////////////////////////////////////////////////////////////////////////
            // serialization/sending

            bool Serialize() {
                Serializer.ResetCounters();

                while (UnsentBytes < SerializeWindowSize && OutgoingSpans.size() < MaxSpansPerWrite) {
                    if (WriteBuffer.size() < MinWriteBufferSize) { // (re)allocate write buffer
                        WriteBuffer = TRcBuf::Uninitialized(WriteBufferSize);
                    }
                    const size_t numBytesProduced = Serializer.ProduceOutputStream(WriteBuffer, &OutgoingSpans,
                        SerializeWindowSize - UnsentBytes);

                    if (!numBytesProduced) {
                        break;
                    }
                    UnsentBytes += numBytesProduced;
                }

                const size_t numb = Serializer.GetNumBytesInScratchBuffers();
                if (numb >= WriteBufferSize * 2 && WriteBufferSize < MaxWriteBufferSize) {
                    WriteBufferSize *= 2;
                } else if (numb < WriteBufferSize / 2 && WriteBufferSize > MinWriteBufferSize) {
                    WriteBufferSize /= 2;
                }

                return true;
            }

            bool PrepareIovec() {
                // Build the iovec WITHOUT consuming spans: writev may complete partially, so a span is only
                // dropped once the bytes it covers have actually been confirmed sent (see ApplyBytesWritten).
                IovLen = 0;
                BytesToWriteLastTime = 0;
                for (const TContiguousSpan& span : OutgoingSpans) {
                    if (IovLen >= MaxSpansPerWrite) {
                        break;
                    }
                    Iov[IovLen++] = {
                        .iov_base = const_cast<char*>(span.data()),
                        .iov_len = span.size(),
                    };
                    BytesToWriteLastTime += span.size();
                }
                return IovLen != 0;
            }

            void ApplyBytesWritten(size_t num, std::vector<ui64> *eventToWireTime) {
                // Check if we need to resize serialization window. If we have issued some data and all of it has been
                // successfully written, and it was limited by serialization window, we can increase it. If we have
                // serialized less than the window, we can decrease the window.
                if (num == BytesToWriteLastTime && BytesToWriteLastTime == SerializeWindowSize) {
                    SerializeWindowSize = Min(SerializeWindowSize + MinSerializeWindowSize, MaxSerializeWindowSize);
                }  else if (UnsentBytes < SerializeWindowSize) {
                    SerializeWindowSize = Max(SerializeWindowSize - MinSerializeWindowSize, MinSerializeWindowSize);
                }

                // Advance past exactly the bytes the kernel accepted. A writev can be short (e.g. under
                // backpressure or on a real network), so drop only fully-sent spans and trim the span that
                // straddles the boundary; the rest stay queued and are retried by the next writev.
                for (auto& span : OutgoingSpans) {
                    NSan::CheckMemIsInitialized(span.data(), span.size());
                }
                for (size_t remaining = num; remaining; OutgoingSpans.pop_front()) {
                    Y_DEBUG_ABORT_UNLESS(!OutgoingSpans.empty());
                    if (TContiguousSpan& front = OutgoingSpans.front(); front.size() <= remaining) {
                        remaining -= front.size();
                    } else {
                        front = TContiguousSpan(front.data() + remaining, front.size() - remaining);
                        break;
                    }
                }

                Y_ABORT_UNLESS(num <= UnsentBytes, "num# %zu UnsentBytes# %zu", num, UnsentBytes);
                UnsentBytes -= num;

                Serializer.CommitProducedBytes(num, eventToWireTime);
            }

            ////////////////////////////////////////////////////////////////////////////////////////////////////////////
            // ping/clock skew management

            NHPTimer::STime PingRequestSentTimestamp = 0;
            NHPTimer::STime PingResponseSentTimestamp = 0;

            void SendPingRequest() {
                NActorsInterconnect::TSystemPayloadV2 systemRequest;
                auto *r = systemRequest.AddRequests();
                r->MutablePingRequest();
                Serializer.Push(systemRequest);
                PingRequestSentTimestamp = GetCycleCountFast();
            }

            void Process(NActorsInterconnect::TSystemPayloadV2& systemRequest) override {
                std::optional<NActorsInterconnect::TSystemPayloadV2> response;

                auto addRequest = [&] {
                    if (!response) {
                        response.emplace();
                    }
                    return response->AddRequests();
                };

                const NHPTimer::STime timestamp = GetCycleCountFast();
                const TInstant now = Now();

                auto calculateRoundTripTimeAndSkew = [&](auto& item, NHPTimer::STime sent) {
                    const ui64 rtt = NHPTimer::GetSeconds(timestamp - sent) * 1e6;
                    const i64 skew = item.GetWallClock() + rtt / 2 - now.MicroSeconds();
                    RegisterPingAndSkew(rtt, skew);
                };

                for (const auto& item : systemRequest.GetRequests()) {
                    switch (item.GetRequestCase()) {
                        case NActorsInterconnect::TSystemPayloadV2::TRequest::kPingRequest: {
                            // we have received PingRequest from the peer -- we have to remember when we got it, send
                            // the reply and wait for PingConfirm to make up our ClockSkew value
                            auto *pr = addRequest()->MutablePingResponse();
                            pr->SetWallClock(now.MicroSeconds());
                            PingResponseSentTimestamp = timestamp;
                            break;
                        }

                        case NActorsInterconnect::TSystemPayloadV2::TRequest::kPingResponse: {
                            calculateRoundTripTimeAndSkew(item.GetPingResponse(), PingRequestSentTimestamp);
                            PingRequestSentTimestamp = 0;

                            auto *pc = addRequest()->MutablePingConfirm();
                            pc->SetWallClock(now.MicroSeconds());
                            break;
                        }

                        case NActorsInterconnect::TSystemPayloadV2::TRequest::kPingConfirm:
                            calculateRoundTripTimeAndSkew(item.GetPingConfirm(), PingResponseSentTimestamp);
                            PingResponseSentTimestamp = 0;
                            break;

                        case NActorsInterconnect::TSystemPayloadV2::TRequest::REQUEST_NOT_SET:
                            break;
                    }
                }

                if (response) {
                    Serializer.Push(*response);
                }
            }

            ui64 PingValues[3] = {0, 0, 0};

            void RegisterPingAndSkew(ui64 pingUs, i64 skew) {
                ClockSkew->store(skew);

                // calculate worst ping over three last times
                PingValues[0] = PingValues[1];
                PingValues[1] = PingValues[2];
                PingValues[2] = pingUs;
                PingRTT->store(Max(PingValues[0], PingValues[1], PingValues[2]));
            }

            bool IsMigratable() const {
                return MigrateState == EMigrateState::None
                    && !UnregisterRequested
                    && !Terminated;
            }

            ////////////////////////////////////////////////////////////////////////////////////////////////////////////

            void RenderHtml(IOutputStream& str) const {
                HTML(str) {
                    DIV_CLASS("panel panel-info") {
                        DIV_CLASS("panel-heading") {
                            str << "Uring engine details";
                        }
                        DIV_CLASS("panel-body") {
                            TABLE_CLASS("table") {
                                TABLEHEAD() {
                                    TABLER() {
                                        TABLEH() { str << "Parameter"; }
                                        TABLEH() { str << "Value"; }
                                    }
                                }
                                TABLEBODY() {
#define PARAM2(K, V) TABLER() { TABLED() { str << (K); } TABLED() { str << (V); } }
#define PARAM(P) PARAM2(#P, P)
                                    PARAM2("OwnerShard", OwnerShard.load())
                                    PARAM2("Socket", (int)*Socket)
                                    PARAM(Terminated)
                                    PARAM(ReadPending)
                                    PARAM(WritePending)
                                    PARAM(UnregisterRequested)
                                    PARAM(SendPings)
                                    PARAM2("WriteBuffer size", WriteBuffer.size())
                                    PARAM(WriteBufferSize)
                                    PARAM2("OutgoingSpans size", OutgoingSpans.size())
                                    PARAM(UnsentBytes)
                                    PARAM(ReadPendingRingIdx)
                                    PARAM(PreferredRingIdx)
                                    PARAM2("IncomingSeqNo", IncomingSeqNo.load())
                                    PARAM(ExpectedSeqNo)
                                    PARAM2("MigrateState", (int)MigrateState)
                                    PARAM(MigrateTargetShard)
                                    PARAM(MigrateSourceShard)
                                    PARAM2("ClockSkew", ClockSkew->load())
                                    PARAM2("PingRTT", PingRTT->load())
                                    PARAM2("ReceiveCallbacks size", ReceiveCallbacks.size())
                                    PARAM2("PendingRecordsHeap size", PendingRecordsHeap.size())
                                    PARAM(SerializeWindowSize)
                                    PARAM2("NumBytesInScratchBuffers", Serializer.GetNumBytesInScratchBuffers())
                                }
                            }
                        }
                    }
                }
            }
        };

        // In-process load signal consumed by rebalancing (not the 15s monitoring scrape path).
        struct TShardLoad {
            std::atomic_uint64_t Busy{0};
            std::atomic_uint64_t Total{0};

            ui32 BusyFraction() const {
                const ui64 total = Total.load(std::memory_order_relaxed);
                if (!total) {
                    return 0;
                }
                return Busy.load(std::memory_order_relaxed) * 1'000'000 / total;
            }
        };

        class TShard {
            enum EOperationType {
                kOpEvent = 1,
                kOpRead,
                kOpWrite,
                kOpTimer,
                kOpCancel,
            };
            static const ui64 kOpMask = (1 << 3) - 1;

            struct TRingSlot {
                io_uring Ring{};
                i64 ItemsToSubmit = 0;
            };

            TUringEngine& Engine;
            const ui32 ShardIdx;
            TIncomingEventQueue IncomingEventQueue;
            std::thread Worker;

            std::vector<TRingSlot> Rings;
            int EventFd = -1;
            ui64 EventFdReadBuffer = 0;
            int TimerFd = -1;
            ui64 ReadTimerBuffer;
            std::atomic_bool WaitingForCQ{false};

            size_t OpShift = 0;

            struct TSessionHash {
                size_t operator()(const std::unique_ptr<TRegisteredSession>& p) const { return THash<void*>{}(p.get()); }
                size_t operator()(const TRegisteredSession *p) const { return THash<void*>{}(p); }
                using is_transparent = void;
            };

            struct TSessionEqual {
                using T = std::unique_ptr<TRegisteredSession>;
                bool operator()(const T& x, const T& y) const { return x == y; }
                bool operator()(const TRegisteredSession *x, const T& y) const { return x == y.get(); }
                bool operator()(const T& x, const TRegisteredSession *y) const { return x.get() == y; }
                using is_transparent = void;
            };

            std::unordered_set<std::unique_ptr<TRegisteredSession>, TSessionHash, TSessionEqual> Sessions;
            // conn -> target shard while OwnerShard still points here during handoff
            THashMap<ui64, ui32> MigratingOut;

            NMonitoring::TDynamicCounters::TCounterPtr SessionsRegistered;
            NMonitoring::TDynamicCounters::TCounterPtr SessionsUnregistered;
            NMonitoring::TDynamicCounters::TCounterPtr EventsSent;
            NMonitoring::TDynamicCounters::TCounterPtr EventsReceived;
            NMonitoring::TDynamicCounters::TCounterPtr DirectReceiveCallbacksRegistered;
            NMonitoring::TDynamicCounters::TCounterPtr DirectReceiveCallbacksUnregistered;
            NMonitoring::TDynamicCounters::TCounterPtr BytesSent;
            NMonitoring::TDynamicCounters::TCounterPtr BytesCopied;
            NMonitoring::TDynamicCounters::TCounterPtr BytesAliased;
            NMonitoring::TDynamicCounters::TCounterPtr BytesReceived;
            NMonitoring::TDynamicCounters::TCounterPtr SQEAllocated;
            NMonitoring::TDynamicCounters::TCounterPtr SubmitCount;
            NMonitoring::TDynamicCounters::TCounterPtr CQEProcessed;
            NMonitoring::TDynamicCounters::TCounterPtr EventWakeups;
            NMonitoring::TDynamicCounters::TCounterPtr PushedAsFirst;
            NMonitoring::TDynamicCounters::TCounterPtr PushedTotal;
            NMonitoring::TDynamicCounters::TCounterPtr ReadUnavail;
            NMonitoring::TDynamicCounters::TCounterPtr WriteUnavail;
            NMonitoring::TDynamicCounters::TCounterPtr SessionsMigratedOut;
            NMonitoring::TDynamicCounters::TCounterPtr SessionsMigratedIn;
            NMonitoring::TDynamicCounters::TCounterPtr OutOfOrderCameIn;
            NMonitoring::TDynamicCounters::TCounterPtr OutOfOrderProcessed;

            NMonitoring::TDynamicCounters::TCounterPtr OtherTotalTime;
            NMonitoring::TDynamicCounters::TCounterPtr CompleteWaitTotalTime;
            NMonitoring::TDynamicCounters::TCounterPtr SubmitWaitTotalTime;
            NMonitoring::TDynamicCounters::TCounterPtr ApplyBytesReadTotalTime;
            NMonitoring::TDynamicCounters::TCounterPtr ApplyBytesWrittenTotalTime;
            NMonitoring::TDynamicCounters::TCounterPtr SerializeBufferTotalTime;
            NMonitoring::TDynamicCounters::TCounterPtr SerializeEventTotalTime;

            NMonitoring::THistogramPtr CommandDeliveryTime;
            NMonitoring::THistogramPtr EventToWireTime;
            NMonitoring::THistogramPtr CompletionWaitTime;
            NMonitoring::THistogramPtr CommandExecTime;
            NMonitoring::THistogramPtr SubmitExecTime;
            NMonitoring::THistogramPtr SerializeTime;
            NMonitoring::THistogramPtr CompletionsProcessedAtOnce;
            NMonitoring::THistogramPtr SubmissionsProcessedAtOnce;

            ui64 LastActivitySwitchTimestamp = 0;
            NMonitoring::TDynamicCounters::TCounterPtr *CurrentActivityTime = &OtherTotalTime;

            const double Freq = 1e9 * NHPTimer::GetSeconds(1); // nanoseconds per cycle

            std::vector<ui64> EventToWireTimeVec;

            TShardLoad& Load;

        private:
            class TActivityMeasure {
                TShard& Shard;
                NMonitoring::TDynamicCounters::TCounterPtr *PrevActivityTime;

            public:
                TActivityMeasure(TShard& shard, NMonitoring::TDynamicCounters::TCounterPtr *activityTime)
                    : Shard(shard)
                    , PrevActivityTime(std::exchange(shard.CurrentActivityTime, activityTime))
                {
                    **PrevActivityTime += UpdateTimestamp();
                }

                ~TActivityMeasure() {
                    const ui64 delta = UpdateTimestamp();
                    if (Shard.CurrentActivityTime) {
                        **Shard.CurrentActivityTime += delta;
                    }
                    Shard.CurrentActivityTime = PrevActivityTime;
                }

                ui64 UpdateTimestamp() {
                    const ui64 prevTimestamp = std::exchange(Shard.LastActivitySwitchTimestamp, GetCycleCountFast());
                    return (Shard.LastActivitySwitchTimestamp - prevTimestamp) * Shard.Freq;
                }
            };

#define ACTIVITY(NAME) if (TActivityMeasure __measure{*this, NAME}; false); else

            void InitRing(TRingSlot& slot, bool sqpoll, ui32 sqThreadIdleMs, TRingSlot *shareWith) {
                auto tryIt = [&](std::optional<ui32> sqThreadIdleMs) {
                    io_uring_params params{};
                    if (sqpoll) {
                        params.flags |= IORING_SETUP_SQPOLL;
                        if (sqThreadIdleMs) {
                            params.sq_thread_idle = *sqThreadIdleMs;
                        }
                    }
                    if (shareWith) {
                        params.flags |= IORING_SETUP_ATTACH_WQ;
                        params.wq_fd = shareWith->Ring.ring_fd;
                    }
                    return io_uring_queue_init_params(RingQueueDepth, &slot.Ring, &params) == 0;
                };
                if (!tryIt(sqThreadIdleMs) && !tryIt(std::nullopt)) {
                    Y_ABORT("failed to initialize ring");
                }
            }

            void PublishLoadSample(ui64 busyDelta, ui64 totalDelta) {
                // Keep a short EWMA-like window by decaying prior samples and adding the latest slice.
                constexpr ui64 DecayNum = 3;
                constexpr ui64 DecayDen = 4;
                const ui64 prevBusy = Load.Busy.load(std::memory_order_relaxed);
                const ui64 prevTotal = Load.Total.load(std::memory_order_relaxed);
                Load.Busy.store(prevBusy * DecayNum / DecayDen + busyDelta, std::memory_order_relaxed);
                Load.Total.store(prevTotal * DecayNum / DecayDen + totalDelta, std::memory_order_relaxed);
            }

        public:
            static NMonitoring::IHistogramCollectorPtr TimeCollector() {
                return NMonitoring::ExponentialHistogram(22, 2, 1000);
            }

            TShard(TUringEngine& engine, ui32 shardIdx, const NMonitoring::TDynamicCounterPtr& shardCounters, bool sqpoll,
                    ui32 ringsPerShard, ui32 sqThreadIdleMs, TShardLoad& load, TShard *shareRingsWith)
#define COUNTER(NAME, DERIV) NAME(shardCounters->GetCounter(#NAME, DERIV))
                : Engine(engine)
                , ShardIdx(shardIdx)
                , COUNTER(SessionsRegistered, true)
                , COUNTER(SessionsUnregistered, true)
                , COUNTER(EventsSent, true)
                , COUNTER(EventsReceived, true)
                , COUNTER(DirectReceiveCallbacksRegistered, true)
                , COUNTER(DirectReceiveCallbacksUnregistered, true)
                , COUNTER(BytesSent, true)
                , COUNTER(BytesCopied, true)
                , COUNTER(BytesAliased, true)
                , COUNTER(BytesReceived, true)
                , COUNTER(SQEAllocated, true)
                , COUNTER(SubmitCount, true)
                , COUNTER(CQEProcessed, true)
                , COUNTER(EventWakeups, true)
                , COUNTER(PushedAsFirst, true)
                , COUNTER(PushedTotal, true)
                , COUNTER(ReadUnavail, true)
                , COUNTER(WriteUnavail, true)
                , COUNTER(SessionsMigratedOut, true)
                , COUNTER(SessionsMigratedIn, true)
                , COUNTER(OutOfOrderCameIn, true)
                , COUNTER(OutOfOrderProcessed, true)
#define TOTAL_TIME(NAME) NAME(shardCounters->GetCounter("TotalTime/" #NAME, true))
                , TOTAL_TIME(OtherTotalTime)
                , TOTAL_TIME(CompleteWaitTotalTime)
                , TOTAL_TIME(SubmitWaitTotalTime)
                , TOTAL_TIME(ApplyBytesReadTotalTime)
                , TOTAL_TIME(ApplyBytesWrittenTotalTime)
                , TOTAL_TIME(SerializeBufferTotalTime)
                , TOTAL_TIME(SerializeEventTotalTime)
                , CommandDeliveryTime(shardCounters->GetNamedHistogram("sensor", "CommandDeliveryTime", TimeCollector()))
                , EventToWireTime(shardCounters->GetNamedHistogram("sensor", "EventToWireTime", TimeCollector()))
                , CompletionWaitTime(shardCounters->GetNamedHistogram("sensor", "CompletionWaitTime", TimeCollector()))
                , CommandExecTime(shardCounters->GetNamedHistogram("sensor", "CommandExecTime", TimeCollector()))
                , SubmitExecTime(shardCounters->GetNamedHistogram("sensor", "SubmitExecTime", TimeCollector()))
                , SerializeTime(shardCounters->GetNamedHistogram("sensor", "SerializeTime", TimeCollector()))
                , CompletionsProcessedAtOnce(shardCounters->GetNamedHistogram("sensor", "CompletionsProcessedAtOnce", NMonitoring::ExponentialHistogram(10, 2)))
                , SubmissionsProcessedAtOnce(shardCounters->GetNamedHistogram("sensor", "SubmissionsProcessedAtOnce", NMonitoring::ExponentialHistogram(12, 2)))
                , Load(load)
#undef TOTAL_TIME
#undef COUNTER
            {
                EventFd = eventfd(0, 0);
                if (EventFd == -1) {
                    Y_ABORT("eventfd() failed: %s", strerror(errno));
                }

                Rings.resize(ringsPerShard);
                for (ui32 i = 0; i < Rings.size(); ++i) {
                    auto& slot = Rings[i];
                    InitRing(slot, sqpoll, sqThreadIdleMs, shareRingsWith ? &shareRingsWith->Rings[i] : nullptr);
                    if (i > 0) {
                        if (int res = io_uring_register_eventfd(&slot.Ring, EventFd); res < 0) {
                            Y_ABORT("failed to register eventfd along with ring: %s", strerror(-res));
                        }
                    }
                }

                TimerFd = timerfd_create(CLOCK_MONOTONIC, 0);
                Y_ABORT_UNLESS(TimerFd != -1);

                // Keep the timer armed for the shard lifetime. Disarming while an io_uring read is
                // outstanding would leave a stuck SQE; idle CPU is controlled via sq_thread_idle instead.
                itimerspec spec{};
                spec.it_interval.tv_sec = RebalanceTimerMs / 1000;
                spec.it_interval.tv_nsec = 1'000'000 * (RebalanceTimerMs % 1000);
                spec.it_value = spec.it_interval;
                if (timerfd_settime(TimerFd, 0, &spec, nullptr) < 0) {
                    Y_ABORT("timerfd_settime failed: %s", strerror(errno));
                }
            }

            ~TShard() {
                Stop(); // joins the worker thread, so no completion will be dispatched after this point
                for (auto& slot : Rings) {
                    io_uring_queue_exit(&slot.Ring);
                }
                DrainQueue(); // free commands that were enqueued after the worker stopped (teardown races)
                close(EventFd);
                close(TimerFd);
                // remaining registered sessions are freed as the Sessions container is destroyed
            }

            void Start() {
                Worker = std::thread(std::bind(&TShard::WorkerThread, this));
            }

            void Register(std::unique_ptr<TRegisteredSession> session) {
                ++*SessionsRegistered;

                // this would be the session's first event, so its sequencing is not the problem
                SendInternal(reinterpret_cast<ui64>(session.release()), static_cast<ui32>(ENetwork::EvRegisterSession),
                    {}, nullptr, false);
            }

            void AcceptMigrated(std::unique_ptr<TRegisteredSession> session) {
                ++*SessionsMigratedIn;

                // this event isn't the first one, but its sequence doesn't matter, because all further events are kept
                // in order and forwarded to the new processor
                SendInternal(reinterpret_cast<ui64>(session.release()), static_cast<ui32>(ENetwork::EvRegisterSession),
                    {}, nullptr, false);
            }

            void Enqueue(TIncomingEventQueue::TRecord&& record) {
                const bool first = IncomingEventQueue.Push(std::move(record));
                if (first) {
                    ++*PushedAsFirst;
                }
                ++*PushedTotal;
                if (first && WaitingForCQ.load(std::memory_order_acquire)) {
                    // first command while waiting on CQ: kick the worker via the pipe on ring 0
                    const ui64 value = 1; // this commands adds 1 to the counter stored in eventfd
                    ssize_t res;
                    while ((res = write(EventFd, &value, sizeof(value))) != sizeof(value)) {
                        if (res == -1 && errno == EINTR) {
                            continue;
                        } else {
                            Y_ABORT("write() to eventfd failed: %s", strerror(errno));
                        }
                    }
                    ++*EventWakeups;
                }
            }

            void Send(ui64 conn, std::unique_ptr<IEventHandle> ev, TIntrusivePtr<IReceiveCallback> replyCallback) {
                ++*EventsSent;

                // this event is strictly sequenced
                SendImpl(conn, std::move(ev), std::move(replyCallback), true);
            }

            void Unregister(ui64 conn) {
                ++*SessionsUnregistered;

                // this event is sequenced too
                SendInternal(conn, static_cast<ui32>(ENetwork::EvUnregisterSession), {}, nullptr, true);
            }

            void RegisterReceiveCallback(ui64 conn, TActorId localActorId, TIntrusivePtr<IReceiveCallback> callback) {
                ++*(callback ? DirectReceiveCallbacksRegistered : DirectReceiveCallbacksUnregistered);

                // this event's ordering is important
                SendInternal(conn, static_cast<ui32>(ENetwork::EvRegisterCallback), localActorId, std::move(callback),
                    true);
            }

            void NotifyMigrateDone(ui64 conn) {
                SendInternal(conn, static_cast<ui32>(ENetwork::EvMigrateDone), {}, nullptr, false);
            }

            void Stop() {
                if (Worker.joinable()) {
                    SendInternal(0, static_cast<ui32>(ENetwork::EvStop), {}, nullptr, false);
                    Worker.join();
                    // The worker is stopped, so it is now safe to touch the sessions directly. Shut every
                    // socket down so the peer observes the disconnect promptly instead of only when this shard
                    // (and thus the sockets it still references) is finally destroyed. The session objects
                    // themselves stay alive until the shard is destroyed.
                    for (const auto& session : Sessions) {
                        if (session->Socket) {
                            session->Socket->Shutdown(SHUT_RDWR);
                        }
                    }
                }
            }

            void IssueMonRequest(ui64 conn, NMon::TEvHttpInfoRes::TPtr ev) {
                SendImpl(conn, std::make_unique<IEventHandle>(TActorId(), TActorId(), new TEvUringMonRequest(std::move(ev))),
                    nullptr, false);
            }

        private:
            // Pops and frees any commands still sitting in the queue after the worker has stopped. Mirrors
            // the ownership handling of the worker loop: destroys the embedded TEventPayload and reclaims a
            // TRegisteredSession handed off via an unprocessed EvRegisterSession.
            void DrainQueue() {
                while (auto record = IncomingEventQueue.Pop()) {
                    if (record->Ev->Type == static_cast<ui32>(ENetwork::EvRegisterSession)) {
                        delete reinterpret_cast<TRegisteredSession*>(record->Conn);
                    }
                }
            }

            void SendImpl(ui64 conn, std::unique_ptr<IEventHandle> ev, TIntrusivePtr<IReceiveCallback> replyCallback,
                    bool ensureSequence) {
                ui64 seqNo = 0;
                if (Y_LIKELY(ensureSequence)) {
                    Y_DEBUG_ABORT_UNLESS(conn);
                    auto& session = *reinterpret_cast<TRegisteredSession*>(conn);
                    seqNo = session.IncomingSeqNo.fetch_add(1);
                }
                Enqueue(TIncomingEventQueue::TRecord{
                    std::move(ev),
                    conn,
                    std::move(replyCallback),
                    GetCycleCountFast(),
                    seqNo,
                });
            }

            void SendInternal(ui64 conn, ui32 type, TActorId sender, TIntrusivePtr<IReceiveCallback> callback,
                    bool ensureSequence) {
                SendImpl(conn, std::make_unique<IEventHandle>(type, 0, TActorId(), sender, nullptr, 0),
                    std::move(callback), ensureSequence);
            }

            bool ForwardIfMigratingOut(TIncomingEventQueue::TRecord *record) {
                if (Y_LIKELY(MigratingOut.empty())) {
                    return false;
                }
                if (const auto it = MigratingOut.find(record->Conn); it != MigratingOut.end()) {
                    Engine.Shards[it->second]->Enqueue(std::move(*record));
                    return true;
                }
                return false;
            }

            // GetSQE returns next available SQ entry, setting up ItemsToSubmit counter in order to commence submission
            // on the end of the worker loop. Event/timer control ops always use ring 0.
            io_uring_sqe *GetSQE(TRegisteredSession *session, EOperationType op, int ringIdx = -1) {
                io_uring_sqe *sqe = nullptr;

                if (ringIdx != -1) {
                    TRingSlot& ring = Rings[ringIdx];
                    sqe = io_uring_get_sqe(&ring.Ring);
                    if (!sqe) { // submit queue is full: try to submit something to free it up
                        DoSubmit(ring);
                        sqe = io_uring_get_sqe(&ring.Ring);
                    }
                } else {
                    for (size_t i = 0; !sqe && i < Rings.size(); ++i) {
                        ringIdx = (i + OpShift) % Rings.size();
                        TRingSlot& ring = Rings[ringIdx];
                        sqe = io_uring_get_sqe(&ring.Ring);
                    }
                    for (size_t i = 0; !sqe && i < Rings.size(); ++i) {
                        ringIdx = (i + OpShift) % Rings.size();
                        TRingSlot& ring = Rings[ringIdx];
                        DoSubmit(ring);
                        sqe = io_uring_get_sqe(&ring.Ring);
                    }
                    if (sqe) {
                        ++OpShift;
                    }
                }

                if (sqe) {
                    Y_DEBUG_ABORT_UNLESS(ringIdx != -1);
                    if (op == kOpRead) {
                        Y_DEBUG_ABORT_UNLESS(session);
                        Y_DEBUG_ABORT_UNLESS(session->ReadPendingRingIdx == -1);
                        session->ReadPendingRingIdx = ringIdx;
                    }
                    ++Rings[ringIdx].ItemsToSubmit;
                    uintptr_t sessionId = reinterpret_cast<uintptr_t>(session);
                    Y_ABORT_UNLESS((sessionId & kOpMask) == 0);
                    io_uring_sqe_set_data64(sqe, sessionId | op);
                    Y_DEBUG_ABORT_UNLESS(op == kOpEvent || op == kOpTimer ? session == nullptr : session != nullptr);
                    ++*SQEAllocated;
                }
                return sqe;
            }

            void PutEventReadRequest() {
                io_uring_sqe *sqe = GetSQE(nullptr, kOpEvent, 0 /* first ring strict */);
                Y_ABORT_UNLESS(sqe, "failed to obtain event SQE: SQ overflow");
                io_uring_prep_read(sqe, EventFd, &EventFdReadBuffer, sizeof(EventFdReadBuffer), -1);
            }

            void PutTimer() {
                io_uring_sqe *sqe = GetSQE(nullptr, kOpTimer);
                Y_ABORT_UNLESS(sqe, "failed to obtain timer SQE: SQ overflow");
                io_uring_prep_read(sqe, TimerFd, &ReadTimerBuffer, sizeof(ReadTimerBuffer), -1);
            }

            // DoSubmit performs actual io_uring submit operation for all allocated entries during the worker loop
            void DoSubmit(TRingSlot& slot) {
                ui64 enterTimestamp;

                ACTIVITY(&SubmitWaitTotalTime) {
                    enterTimestamp = LastActivitySwitchTimestamp;

                    for (;;) {
                        int res = io_uring_submit(&slot.Ring);
                        if (res == -EINTR) {
                            continue;
                        }
                        if (res < 0) {
                            Y_ABORT("io_uring_submit() failed: %s", strerror(-res));
                        }
                        break;
                    }
                }

                ++*SubmitCount;
                SubmitExecTime->Collect((LastActivitySwitchTimestamp - enterTimestamp) * Freq);
                SubmissionsProcessedAtOnce->Collect(slot.ItemsToSubmit, 1u);
                slot.ItemsToSubmit = 0;
            }

            void WorkerThread() {
                pthread_setname_np(pthread_self(), "IC_uring");

                LastActivitySwitchTimestamp = GetCycleCountFast();
                ui64 loopStartTimestamp = LastActivitySwitchTimestamp;

                // Arm pipe + timer
                PutEventReadRequest();
                PutTimer();

                for (;;) {
                    // submit any pending SQ's (if we have any)
                    for (auto& slot : Rings) {
                        if (slot.ItemsToSubmit) {
                            DoSubmit(slot);
                        }
                    }

                    // process pending CQ events from every ring
                    bool progress = ProcessCompletions();

                    // process pending events and commands
                    bool stopping = false;
                    progress |= ProcessPendingCommands(&stopping);
                    if (stopping) {
                        break;
                    }

                    ui64 waitStartTimestamp = 0;
                    if (!progress) { // wait for something to happen -- no progress were made in this loop
                        WaitingForCQ.store(true, std::memory_order_release);

                        // it is critical we first set WaitingForCQ, and then rechecking the queue
                        if (IncomingEventQueue.IsEmpty()) {
                            io_uring_cqe *cqe;
                            ui64 enterTimestamp;
                            ACTIVITY(&CompleteWaitTotalTime) {
                                waitStartTimestamp = enterTimestamp = LastActivitySwitchTimestamp;
                                // wait for the ring waiting for EventFd
                                if (int res = io_uring_wait_cqe(&Rings.front().Ring, &cqe); res && res != -EINTR) {
                                    Y_ABORT("io_uring_wait_cqe() failed: %s", strerror(-res));
                                }
                            }
                            CompletionWaitTime->Collect((LastActivitySwitchTimestamp - enterTimestamp) * Freq);
                        }

                        WaitingForCQ.store(false, std::memory_order_relaxed);
                    }

                    const ui64 loopEndTimestamp = GetCycleCountFast();
                    const ui64 total = loopEndTimestamp - loopStartTimestamp;
                    const ui64 wait = waitStartTimestamp ? loopEndTimestamp - waitStartTimestamp : 0;
                    const ui64 work = wait < total ? total - wait : 0;
                    PublishLoadSample(work, total);
                    loopStartTimestamp = loopEndTimestamp;
                }
            }

            ////////////////////////////////////////////////////////////////////////////////////////////////////////////
            // command processing

            bool ProcessPendingCommands(bool *stopping) {
                bool progress = false;
                ui64 cycleCountOnEnter = 0;

                while (auto record = IncomingEventQueue.Pop()) {
                    progress = true;
                    if (record->Ev->Type == static_cast<ui32>(ENetwork::EvStop)) {
                        *stopping = true;
                        break;
                    }

                    if (!cycleCountOnEnter) {
                        cycleCountOnEnter = GetCycleCountFast();
                    }

                    if (Y_LIKELY(record->SeqNo)) {
                        if (ForwardIfMigratingOut(&record.value())) {
                            // this record is just sent to the other thread
                        } else if (auto& session = GetSession(record->Conn); record->SeqNo != session.ExpectedSeqNo) {
                            Y_DEBUG_ABORT_UNLESS(session.ExpectedSeqNo < record->SeqNo);
                            session.PendingRecordsHeap.push_back(std::move(*record));
                            std::ranges::push_heap(session.PendingRecordsHeap, std::greater<ui64>{},
                                &TIncomingEventQueue::TRecord::SeqNo);
                            ++*OutOfOrderCameIn;
                        } else {
                            // process this event
                            const bool isUnregister = record->Ev->Type == static_cast<ui32>(ENetwork::EvUnregisterSession);
                            ProcessIncomingEvent(&record.value());
                            if (!isUnregister) {
                                ++session.ExpectedSeqNo;

                                // check if there are other events in the process queue
                                if (auto& heap = session.PendingRecordsHeap; Y_UNLIKELY(!heap.empty())) {
                                    while (!heap.empty() && heap.front().SeqNo == session.ExpectedSeqNo) {
                                        std::ranges::pop_heap(heap, std::greater<ui64>{}, &TIncomingEventQueue::TRecord::SeqNo);
                                        ProcessIncomingEvent(&heap.back());
                                        ++session.ExpectedSeqNo;
                                        heap.pop_back();
                                        ++*OutOfOrderProcessed;
                                    }
                                    if (heap.empty()) {
                                        heap.shrink_to_fit();
                                    }
                                }
                            }
                        }
                    } else {
                        // process unsequenced event
                        ProcessIncomingEvent(&record.value());
                    }

                    const ui64 cycleCountOnExit = GetCycleCountFast();
                    CommandDeliveryTime->Collect((cycleCountOnEnter - record->ReceivedTimestamp) * Freq);
                    CommandExecTime->Collect((cycleCountOnExit - cycleCountOnEnter) * Freq);
                    cycleCountOnEnter = cycleCountOnExit;
                }

                return progress;
            }

            void ProcessIncomingEvent(TIncomingEventQueue::TRecord *record) {
                switch (record->Ev->Type) {
                    case static_cast<ui32>(ENetwork::EvRegisterCallback):
                        if (TRegisteredSession& session = GetSession(record->Conn); record->Callback) {
                            session.ReceiveCallbacks[record->Ev->Sender] = std::move(record->Callback);
                        } else {
                            session.ReceiveCallbacks.erase(record->Ev->Sender);
                        }
                        break;

                    case static_cast<ui32>(ENetwork::EvRegisterSession): {
                        std::unique_ptr<TRegisteredSession> session(reinterpret_cast<TRegisteredSession*>(record->Conn));
                        if (session->MigrateState == EMigrateState::HandedOff) {
                            session->OwnerShard.store(ShardIdx, std::memory_order_release); // all new events will arrive here from now on
                            Engine.Shards[session->MigrateSourceShard]->NotifyMigrateDone(record->Conn);
                            session->MigrateState = EMigrateState::None;
                        } else {
                            Y_DEBUG_ABORT_UNLESS(session->MigrateState == EMigrateState::None);
                        }
                        session->PreferredRingIdx = OpShift++ % Rings.size();
                        const auto [it, inserted] = Sessions.emplace(std::move(session));
                        Y_ABORT_UNLESS(inserted);
                        (*it)->EventsReceived = EventsReceived;
                        IssueReadForSession(**it);
                        break;
                    }

                    case static_cast<ui32>(ENetwork::EvUnregisterSession): {
                        TRegisteredSession& session = GetSession(record->Conn);
                        // Do NOT free the session while it still has an armed recv or an in-flight
                        // writev: their io_uring completions carry a raw pointer to this object and
                        // would dereference freed memory. Mark it terminated (so no new ops are armed)
                        // and erase only once both are drained. The session actor has already shut the
                        // socket down before requesting unregistration, so the pending ops complete
                        // promptly (EOF/EPIPE).
                        if (session.MigrateState != EMigrateState::None) {
                            session.MigrateState = EMigrateState::None; // unregister wins over migrate
                        }
                        session.Terminated = true;
                        session.UnregisterRequested = true;
                        if (session.ReadPending) { // cancel pending read in order to unregister the session
                            CancelOp(session, kOpRead, session.ReadPendingRingIdx);
                        }
                        MaybeEraseSession(session);
                        break;
                    }

                    case static_cast<ui32>(ENetwork::EvMigrateDone): {
                        const size_t num = MigratingOut.erase(record->Conn);
                        Y_DEBUG_ABORT_UNLESS(num == 1);
                        break;
                    }

                    case static_cast<ui32>(ENetwork::EvStop):
                        Y_ABORT();

                    case static_cast<ui32>(ENetwork::EvUringMonRequest):
                        ProcessMonRequest(GetSession(record->Conn), std::move(record->Ev->Get<TEvUringMonRequest>()->Ev));
                        break;

                    default: {
                        TRegisteredSession& session = GetSession(record->Conn);
                        if (record->Callback) { // register callback coming along with the message
                            session.ReceiveCallbacks[record->Ev->Sender] = std::move(record->Callback);
                        }
                        session.Serializer.Push(std::move(record->Ev));
                        IssueWritesForSession(session);
                        break;
                    }
                }
            }

            ////////////////////////////////////////////////////////////////////////////////////////////////////////////
            // monitoring

            void ProcessMonRequest(TRegisteredSession& session, NMon::TEvHttpInfoRes::TPtr ev) {
                TStringOutput str(const_cast<TString&>(static_cast<NMon::TEvHttpInfoRes*>(ev->Get())->Answer));
                session.RenderHtml(str);
                Engine.ActorSystem->Send(ev.Release());
            }

            ////////////////////////////////////////////////////////////////////////////////////////////////////////////
            // io_uring completion handlers

            bool ProcessCompletions() {
                bool progress = false;
                i64 completionsProcessedAtOnce = 0;
                for (auto& slot : Rings) {
                    io_uring_cqe *cqes[CqeBatchSize];
                    while (const unsigned n = io_uring_peek_batch_cqe(&slot.Ring, cqes, CqeBatchSize)) {
                        progress = true; // we did something with the queues
                        for (unsigned i = 0; i < n; ++i) {
                            DispatchCompletion(*cqes[i]);
                        }
                        io_uring_cq_advance(&slot.Ring, n);
                        completionsProcessedAtOnce += n;
                        if (n < CqeBatchSize) {
                            break;
                        }
                    }
                }
                if (completionsProcessedAtOnce) {
                    CompletionsProcessedAtOnce->Collect(completionsProcessedAtOnce, 1u);
                }
                return progress;
            }

            void DispatchCompletion(io_uring_cqe& cqe) {
                auto *session = reinterpret_cast<TRegisteredSession*>(uintptr_t(cqe.user_data) & ~uintptr_t(kOpMask));
                Y_ABORT_UNLESS(!(cqe.flags & IORING_CQE_F_MORE)); // not expecting multiple completions
                const auto op = static_cast<EOperationType>(cqe.user_data & kOpMask);
                switch (op) {
                    case kOpEvent:
                        Y_DEBUG_ABORT_UNLESS(session == nullptr);
                        Y_DEBUG_ABORT_UNLESS(cqe.res == sizeof(EventFdReadBuffer));
                        PutEventReadRequest();
                        break;

                    case kOpRead:
                        Y_DEBUG_ABORT_UNLESS(session != nullptr);
                        Y_DEBUG_ABORT_UNLESS(session->ReadPendingRingIdx != -1);
                        session->ReadPendingRingIdx = -1;
                        DispatchRead(*session, cqe.res);
                        break;

                    case kOpWrite:
                        Y_DEBUG_ABORT_UNLESS(session != nullptr);
                        DispatchWrite(*session, cqe.res);
                        break;

                    case kOpTimer:
                        Y_DEBUG_ABORT_UNLESS(session == nullptr);
                        DispatchTimer();
                        PutTimer();
                        break;

                    case kOpCancel:
                        // Original op completes with -ECANCELED; the cancel SQE itself needs no action.
                        break;
                }
                ++*CQEProcessed;
            }

            void DispatchTimer() {
                for (auto& session : Sessions) {
                    if (!session->Terminated && session->MigrateState == EMigrateState::None && session->SendPings &&
                            session->PingRequestSentTimestamp == 0) {
                        session->SendPingRequest();
                        IssueWritesForSession(*session);
                    }
                }

                // Rebalance at most once per ping period to limit churn under short load spikes.
                MaybeOffload();
            }

            void MaybeOffload() {
                if (Engine.Shards.size() < 2 || Sessions.size() < 2 || Load.BusyFraction() < OffloadBusyThreshold) {
                    return;
                }

                ui32 bestTarget = ShardIdx;
                ui32 bestBusy = Max<ui32>();
                for (ui32 i = 0; i < Engine.Shards.size(); ++i) {
                    if (i == ShardIdx) {
                        continue;
                    }
                    if (const ui32 busy = Engine.ShardLoads[i].BusyFraction(); busy < bestBusy) {
                        bestBusy = busy;
                        bestTarget = i;
                    }
                }
                if (bestTarget == ShardIdx || bestBusy > StealBusyThreshold) {
                    return;
                }

                TRegisteredSession *candidate = nullptr;
                for (auto& session : Sessions) {
                    if (session->IsMigratable()) {
                        candidate = session.get();
                        break;
                    }
                }
                if (!candidate) {
                    return;
                }

                candidate->MigrateState = EMigrateState::Draining;
                candidate->MigrateTargetShard = bestTarget;
                candidate->MigrateSourceShard = ShardIdx;
                Y_DEBUG_ABORT_UNLESS(candidate->MigrateTargetShard != candidate->MigrateSourceShard);

                if (candidate->ReadPending) {
                    CancelOp(*candidate, kOpRead, candidate->ReadPendingRingIdx);
                }

                MaybeFinishMigrate(*candidate);
            }

            void CancelOp(TRegisteredSession& session, EOperationType op, int ringIdx) {
                Y_DEBUG_ABORT_UNLESS(ringIdx != -1);
                io_uring_sqe *sqe = GetSQE(&session, kOpCancel, ringIdx);
                Y_ABORT_UNLESS(sqe, "failed to obtain cancel SQE");
                const ui64 targetUserData = reinterpret_cast<uintptr_t>(&session) | op;
                io_uring_prep_cancel64(sqe, targetUserData, 0);
            }

            bool MaybeFinishMigrate(TRegisteredSession& session) {
                if (session.MigrateState != EMigrateState::Draining || session.ReadPending || session.WritePending ||
                        session.UnregisterRequested || session.Terminated) {
                    return false;
                }

                const ui64 conn = reinterpret_cast<ui64>(&session);
                const ui32 target = session.MigrateTargetShard;
                session.MigrateState = EMigrateState::HandedOff;

                auto it = Sessions.find(&session);
                Y_DEBUG_ABORT_UNLESS(it != Sessions.end());
                auto node = Sessions.extract(it);
                Y_DEBUG_ABORT_UNLESS(node);

                MigratingOut[conn] = target;
                ++*SessionsMigratedOut;
                Engine.Shards[target]->AcceptMigrated(std::move(node.value()));

                return true;
            }

            void DispatchRead(TRegisteredSession& session, i32 res) {
                Y_DEBUG_ABORT_UNLESS(session.ReadPending);
                session.ReadPending = false;

                if (session.Terminated) {
                    // teardown in progress: don't process further data or re-arm; just let the session drain
                    // toward erasure below
                } else if (res == -ECANCELED) {
                    // cancelled without migrate (should be rare); re-arm unless terminating
                    IssueReadForSession(session);
                } else if (res == -EAGAIN) {
                    ++*ReadUnavail;
                    IssueReadForSession(session);
                } else if (res < 0) {
                    session.Disconnect(TDisconnectReason::FromErrno(-res));
                } else if (res == 0) {
                    session.Disconnect(TDisconnectReason::EndOfStream());
                } else {
                    *BytesReceived += res;
                    ACTIVITY(&ApplyBytesReadTotalTime) {
                        session.ApplyBytesRead(res);
                    }
                    IssueReadForSession(session);
                    IssueWritesForSession(session);
                }

                if (MaybeFinishMigrate(session)) { // NB: may free/move `session`
                    return;
                }
                MaybeEraseSession(session); // NB: may free `session`; must be the last use
            }

            void IssueReadForSession(TRegisteredSession& session) {
                if (session.Terminated || session.MigrateState != EMigrateState::None) {
                    return;
                }
                Y_DEBUG_ABORT_UNLESS(!session.ReadPending);
                TMutableContiguousSpan span = session.GetReadSpan();
                io_uring_sqe *sqe = GetSQE(&session, kOpRead, session.PreferredRingIdx);
                Y_ABORT_UNLESS(sqe);
                io_uring_prep_read(sqe, *session.Socket, span.data(), span.size(), -1);
                session.ReadPending = true;
            }

            void DispatchWrite(TRegisteredSession& session, i32 res) {
                Y_ABORT_UNLESS(session.WritePending);
                session.WritePending = false;

                if (session.Terminated) {
                    // teardown in progress: don't retry the write or re-arm; just let the session drain
                    // toward erasure below
                } else if (res == -ECANCELED) {
                    IssueWritesForSession(session);
                } else if (res == -EAGAIN) {
                    ++*WriteUnavail;
                    SubmitIovec(session);
                } else if (res < 0) {
                    session.Disconnect(TDisconnectReason::FromErrno(-res));
                } else if (res == 0) {
                    session.Disconnect(TDisconnectReason::EndOfStream());
                } else {
                    *BytesSent += res;
                    ACTIVITY(&ApplyBytesWrittenTotalTime) {
                        session.ApplyBytesWritten(res, &EventToWireTimeVec);
                        for (const ui64 time : EventToWireTimeVec) {
                            EventToWireTime->Collect(time * Freq, 1u);
                        }
                        EventToWireTimeVec.clear();
                    }
                    IssueWritesForSession(session);
                }

                if (MaybeFinishMigrate(session)) {
                    return;
                }
                MaybeEraseSession(session); // NB: may free `session`; must be the last use
            }

            void IssueWritesForSession(TRegisteredSession& session) {
                if (session.WritePending || session.Terminated || session.MigrateState != EMigrateState::None ||
                        !session.Serializer.IsTrafficPending()) {
                    return;
                }
                if (session.Serialize()) {
                    const ui64 serializeBufferTime = session.Serializer.GetSerializeBufferTime();
                    const ui64 serializeEventTime = session.Serializer.GetSerializeEventTime();
                    const ui64 prevTimestamp = std::exchange(LastActivitySwitchTimestamp, GetCycleCountFast());
                    **CurrentActivityTime += (LastActivitySwitchTimestamp - prevTimestamp) * Freq - (serializeBufferTime + serializeEventTime);
                    *SerializeBufferTotalTime += serializeBufferTime;
                    *SerializeEventTotalTime += serializeEventTime;
                    *BytesCopied += session.Serializer.GetBytesCopied();
                    *BytesAliased += session.Serializer.GetBytesAliased();
                }
                if (session.PrepareIovec()) {
                    SubmitIovec(session);
                }
            }

            void SubmitIovec(TRegisteredSession& session) {
                if (session.MigrateState != EMigrateState::None) {
                    return;
                }
                io_uring_sqe *sqe = GetSQE(&session, kOpWrite, session.PreferredRingIdx);
                Y_ABORT_UNLESS(sqe);
                io_uring_prep_writev(sqe, *session.Socket, session.Iov, session.IovLen, -1);
                session.WritePending = true;
            }

            ////////////////////////////////////////////////////////////////////////////////////////////////////////////
            // commands from outer threads

            TRegisteredSession& GetSession(ui64 conn) const {
                TRegisteredSession *ptr = reinterpret_cast<TRegisteredSession*>(conn);
                Y_ABORT_UNLESS(Sessions.find(ptr) != Sessions.end());
                return *ptr;
            }

            // Frees an unregistered session once it has no io_uring operation in flight. It is unsafe to
            // erase earlier because any pending read/write completion references the session by raw pointer.
            void MaybeEraseSession(TRegisteredSession& session) {
                if (session.UnregisterRequested && !session.ReadPending && !session.WritePending) {
                    auto it = Sessions.find(&session);
                    Y_ABORT_UNLESS(it != Sessions.end());
                    Sessions.erase(it);
                }
            }
        };

        std::vector<std::unique_ptr<TShard>> Shards;
        std::vector<TShardLoad> ShardLoads;
        std::atomic_uint64_t NextShardIdx;

        NMonitoring::TDynamicCounterPtr UringCounters;

    public:
        TUringEngine(ui32 numShards, NMonitoring::TDynamicCounterPtr counters, bool sqpoll, ui32 ringsPerShard,
                ui32 sqThreadIdleMs, bool shareRingsAmongThreads)
            : UringCounters(std::move(counters))
        {
            ShardLoads = std::vector<TShardLoad>(numShards);
            Shards.reserve(numShards);
            for (ui32 i = 0; i < numShards; ++i) {
                Shards.push_back(std::make_unique<TShard>(*this,
                    i, // shardIdx
                    UringCounters->GetSubgroup("shard", "0" /*ToString(i)*/),
                    sqpoll,
                    ringsPerShard,
                    sqThreadIdleMs,
                    ShardLoads[i],
                    shareRingsAmongThreads && !Shards.empty() ? Shards.front().get() : nullptr));
            }
            for (auto& shard : Shards) {
                shard->Start();
            }
        }

        ~TUringEngine() {
            Stop();
        }

        void SetActorSystem(TActorSystem* actorSystem) override {
            Y_ABORT_UNLESS(actorSystem);
            ActorSystem = actorSystem;
            // Stop the reaper threads while the actor system is still up, so no completion is posted to a
            // torn-down system.
            actorSystem->DeferPreStop([self = TIntrusivePtr<IUringEngine>(this)] { self->Stop(); });
        }

        ui64 Register(TIntrusivePtr<NInterconnect::TStreamSocket> socket, const TActorId& sessionActorId,
                bool checksumming, TScopeId peerScopeId, std::function<void(TDisconnectReason)> onDisconnectCallback,
                bool sendPings, std::shared_ptr<std::atomic<int64_t>> clockSkew,
                std::shared_ptr<std::atomic<uint64_t>> pingRTT) override {
            if (Stopping) {
                return 0; // engine is shutting down; caller treats 0 as a failed registration and terminates
            }
            Y_ABORT_UNLESS(ActorSystem);

            // Prefer the currently least-loaded shard (in-process signal); fall back to round-robin.
            ui32 shardIdx = NextShardIdx++ % Shards.size();
            ui32 bestBusy = ShardLoads[shardIdx].BusyFraction();
            for (ui32 i = 0; i < ShardLoads.size(); ++i) {
                if (const ui32 busy = ShardLoads[i].BusyFraction(); busy < bestBusy) {
                    bestBusy = busy;
                    shardIdx = i;
                }
            }

            auto session = std::make_unique<TRegisteredSession>(shardIdx, std::move(socket), sessionActorId,
                checksumming, peerScopeId, std::move(onDisconnectCallback), ActorSystem, sendPings, std::move(clockSkew),
                std::move(pingRTT));
            const ui64 conn = reinterpret_cast<ui64>(session.get());
            Shards[shardIdx]->Register(std::move(session));
            return conn;
        }

        TShard& GetShard(ui64 conn) const {
            // OwnerShard is the indirection for dynamic mapping: stable conn handle, mutable owner.
            return *Shards[reinterpret_cast<TRegisteredSession*>(conn)->OwnerShard.load(std::memory_order_acquire)];
        }

        void Send(ui64 conn, std::unique_ptr<IEventHandle> ev, TIntrusivePtr<IReceiveCallback> replyCallback) override {
            // Sessions may still forward events during actor-system teardown (DeferPreStop runs Stop() before
            // executor threads are joined), so drop rather than abort. Checking Stopping before touching the
            // shard/conn keeps this safe: Stop() only joins workers, it never frees shards or sessions.
            if (Stopping) {
                return;
            }
            GetShard(conn).Send(conn, std::move(ev), std::move(replyCallback));
        }

        void Unregister(ui64 conn) override {
            if (Stopping) {
                return;
            }
            GetShard(conn).Unregister(conn);
        }

        void RegisterReceiveCallback(ui64 conn, TActorId localActorId, TIntrusivePtr<IReceiveCallback> callback) override {
            if (Stopping) {
                return;
            }
            GetShard(conn).RegisterReceiveCallback(conn, localActorId, std::move(callback));
        }

        void Stop() override {
            // Quiesce the reaper/worker threads (so no completion is posted to a torn-down actor system) but
            // keep shards and their registered sessions alive: executor threads may still be running and may
            // call in with live conn pointers. The memory is released later in the destructor, once the actor
            // system is fully stopped and no more calls can arrive.
            if (!Stopping.exchange(true)) {
                for (auto& shard : Shards) {
                    shard->Stop();
                }
            }
        }

        void IssueMonRequest(ui64 conn, NMon::TEvHttpInfoRes::TPtr ev) override {
            GetShard(conn).IssueMonRequest(conn, std::move(ev));
        }
    };

    TUringEnginePtr CreateUringEngine(ui32 numShards, NMonitoring::TDynamicCounterPtr counters, bool sqpoll,
            ui32 ringsPerShard, ui32 sqThreadIdleMs, bool shareRingsAmongThreads) {
        if (!TUringContext::IsAvailable()) {
            return nullptr;
        }
        if (numShards < 1) {
            numShards = 1;
        }
        if (ringsPerShard < 1) {
            ringsPerShard = 1;
        }
        if (sqThreadIdleMs < 1) {
            sqThreadIdleMs = TUringContext::SqThreadIdleMs;
        }
        return MakeIntrusive<TUringEngine>(numShards, std::move(counters), sqpoll, ringsPerShard, sqThreadIdleMs,
            shareRingsAmongThreads);
    }

} // namespace NActors

#include "interconnect_uring_engine.h"

#include "uring_context.h" // for TUringContext::IsAvailable() / SqThreadIdleMs

#include "v2_event_serializer.h"
#include "interconnect_common.h"
#include "interconnect_direct_session.h"
#include "interconnect_uring_event_queue.h"

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/actor.h>

#include <ydb/library/actors/protos/interconnect.pb.h>

// Must be included AFTER YDB headers because linux/uapi headers pulled by
// liburing may define macros that clash with project headers.
#include <ydb/library/uring/liburing_linux.h>

#include <library/cpp/monlib/service/pages/templates.h>

#include <util/datetime/cputimer.h>
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
        constexpr size_t MaxSpansPerWrite = 64;
        constexpr TDuration RebalancePeriod = TDuration::MilliSeconds(500); // how often MaybeOffload runs
        constexpr ui32 OffloadBusyThreshold = 700000; // ppm
        constexpr ui32 StealBusyThreshold = 300000; // ppm
        constexpr int ProvidedBufGroupId = 0;

        // The shard timer is the only timing source for pings and dead-peer detection, so its period caps
        // how late either of them can be. Keep it at or below this bound to limit the jitter.
        constexpr TDuration MaxTickPeriod = TDuration::Seconds(1);
        constexpr TDuration MinTickPeriod = TDuration::MilliSeconds(100);
        // Number of ping opportunities that must fit into the dead-peer window, so that a single lost or
        // late ping cannot bring a healthy link down.
        constexpr ui32 PingsPerDeadPeerTimeout = 4;

        ui32 NextPowerOfTwo(ui32 n) {
            if (n <= 1) {
                return 1;
            }
            --n;
            n |= n >> 1;
            n |= n >> 2;
            n |= n >> 4;
            n |= n >> 8;
            n |= n >> 16;
            return n + 1;
        }

        TDuration CalculateDeadPeerTimeout(const TInterconnectSettings& settings) {
            return settings.DeadPeer ? settings.DeadPeer : DEFAULT_DEADPEER_TIMEOUT;
        }

        TDuration CalculatePingPeriod(const TInterconnectSettings& settings, TDuration deadPeerTimeout) {
            TDuration period = deadPeerTimeout / PingsPerDeadPeerTimeout;
            if (settings.PingPeriod) {
                period = Min(period, settings.PingPeriod);
            }
            return Max(period, MinTickPeriod);
        }

        TDuration CalculateTickPeriod(TDuration pingPeriod) {
            return Max(Min(pingPeriod, MaxTickPeriod), MinTickPeriod);
        }
    }

    struct TEvUringMonRequest : TEventLocal<TEvUringMonRequest, static_cast<ui32>(ENetwork::EvUringMonRequest)> {
        NMon::TEvHttpInfoRes::TPtr Ev;

        TEvUringMonRequest(NMon::TEvHttpInfoRes::TPtr ev)
            : Ev(std::move(ev))
        {}
    };

    struct TEvDestroyEvents : TEventLocal<TEvDestroyEvents, 0> {
        std::vector<std::unique_ptr<IEventBase>> Events;
        std::vector<TIntrusivePtr<TEventSerializedData>> Buffers;
        size_t Bytes = 0;
        std::shared_ptr<std::atomic<TAtomicBase>> Counter;

        ~TEvDestroyEvents() {
            if (Counter) {
                Counter->fetch_sub(Bytes, std::memory_order_relaxed);
            }
        }

        size_t CalculateTotalSize() const {
            size_t bytes = 0;
            for (const auto& ev : Events) {
                bytes += ev->CalculateSerializedSizeCached();
            }
            for (const auto& buffer : Buffers) {
                bytes += buffer->GetSize();
            }
            return bytes;
        }
    };

    class TUringEngine final : public IUringEngine {
        TActorSystem *ActorSystem = nullptr; // bound after construction via SetActorSystem()
        std::once_flag ActorSystemInitFlag;
        std::atomic_bool Stopping{false};

        // Shared interconnect parameters this engine was created with. Common owns the engine as well, so
        // the reference cycle is broken in Stop(); everything the data plane needs from Settings is derived
        // into the const members below at construction time and Common is not dereferenced afterwards.
        TIntrusivePtr<TInterconnectProxyCommon> Common;

        const bool ChecksumEvents;

        // Liveness timing. TickPeriod is the shard timer period and thus the granularity (and worst-case
        // lateness) of both ping issuance and the dead-peer verdict.
        const TDuration DeadPeerTimeout;
        const TDuration PingPeriod;
        const TDuration TickPeriod;
        const ui64 DeadPeerTimeoutCycles;
        const ui64 PingPeriodCycles;

        // Low 3 bits of the session pointer are used as an io_uring user_data op tag; heap allocation
        // alignment of this type is already >= 8 (actually 64 via base/members).
        struct TSession
            : TEventDeserializer::IEventProcessor
            , TIntrusiveListItem<TSession>
        {
            std::atomic_uint32_t OwnerShard;
            const TIntrusivePtr<NInterconnect::TStreamSocket> Socket;
            const TActorId SessionId;
            const std::function<void(TDisconnectReason)> OnDisconnectCallback;
            TActorSystem* const ActorSystem;
            TEventSerializer Serializer;
            TEventDeserializer Deserializer;
            TRcBuf ReadBuffer;
            size_t ReadBufferSize = 0;
            bool Terminated = false;
            bool ReadPending = false;
            bool WritePending = false;
            bool UnregisterRequested = false;
            const bool SendPings;
            TRcBuf WriteBuffer;
            size_t WriteBufferSize = 0;
            std::vector<TContiguousSpan> OutgoingSpans;
            iovec Iov[MaxSpansPerWrite];
            size_t IovLen = 0;
            size_t UnsentBytes = 0;
            size_t BytesToWriteLastTime = 0;
            int ReadPendingRingIdx = -1;
            ui32 PreferredRingIdx = 0;
            // Index into the preferred ring's fixed-file table, or -1 when using the raw socket fd.
            int FixedFileIndex = -1;
            std::atomic_uint64_t IncomingSeqNo{1};
            ui64 ExpectedSeqNo = 1;

            ui32 MigrateTargetShard = 0;

            const std::shared_ptr<std::atomic<int64_t>> ClockSkew;
            const std::shared_ptr<std::atomic<uint64_t>> PingRTT;

            THashMap<TActorId, TIntrusivePtr<IReceiveCallback>> ReceiveCallbacks;

            std::vector<TIncomingEventQueue::TRecord> PendingRecordsHeap;

            size_t SerializeWindowSize = 0;

            ui64 BytesSent = 0;
            ui64 BytesReceived = 0;

            ui64 ReceiveCycles = 0;
            ui64 EventsReceivedCallback = 0;
            ui64 EventsReceivedActorSystem = 0;

            std::atomic_uint64_t TotalOutputQueueSize{0};

            TSession(ui32 shardIdx, TIntrusivePtr<NInterconnect::TStreamSocket> socket,
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
                , MigrateTargetShard(shardIdx)
                , ClockSkew(std::move(clockSkew))
                , PingRTT(std::move(pingRTT))
            {}

            void Disconnect(TDisconnectReason reason) {
                OnDisconnectCallback(reason);
                Terminated = true;
            }

            ////////////////////////////////////////////////////////////////////////////////////////////////////////////
            // deserialization/receiving

            TMutableContiguousSpan GetReadSpan(size_t minReadBufferSize) {
                if (ReadBuffer.size() < minReadBufferSize) {
                    ReadBuffer = TRcBuf::Uninitialized(ReadBufferSize);
                    NSan::Poison(ReadBuffer.data(), ReadBuffer.size());
                }
                return ReadBuffer.UnsafeGetContiguousSpanMut();
            }

            void ApplyBytesRead(size_t num, size_t minReadBufferSize, size_t maxReadBufferSize) {
                BytesReceived += num;
                Y_DEBUG_ABORT_UNLESS(num <= ReadBuffer.size());
                NSan::Unpoison(ReadBuffer.data(), num);
                Deserializer.Push(num == ReadBuffer.size()
                        ? std::move(ReadBuffer)
                        : TRcBuf(TRcBuf::Piece, ReadBuffer.data(), num, ReadBuffer),
                    this,
                    SessionId);
                const size_t readSpanSize = ReadBuffer.size();
                const size_t remain = readSpanSize - num;
                ReadBuffer.TrimFront(remain - remain % 64); // make only this number of bytes remaining in buffer

                if (num == readSpanSize && num >= ReadBufferSize / 2 && ReadBufferSize < maxReadBufferSize) {
                    // we have read all the provided buffer and it's more than a half of original read buffer
                    ReadBufferSize *= 2;
                } else if (num < readSpanSize && readSpanSize >= ReadBufferSize / 2 && ReadBufferSize > minReadBufferSize) {
                    // we haven't read all the provided buffer and we have asked for more than its half
                    ReadBufferSize /= 2;
                    if (ReadBufferSize == minReadBufferSize) {
                        // reset read buffer so the reads go into the pool
                        ReadBuffer = {};
                    }
                }
            }

            // Copy-out path for shared-pool completions: pool memory cannot be moved into the deserializer.
            void ApplyBytesReadCopy(const char *data, size_t num, size_t minReadBufferSize, size_t maxReadBufferSize) {
                BytesReceived += num;
                NSan::Unpoison(data, num);
                Deserializer.Push(TRcBuf::Copy({data, num}), this, SessionId);

                Y_DEBUG_ABORT_UNLESS(num <= minReadBufferSize);
                if (num == minReadBufferSize && ReadBufferSize < maxReadBufferSize) {
                    // we have read all the provided buffer and probably have more, so double it
                    ReadBufferSize *= 2;
                }
            }

            void PushEvent(std::unique_ptr<IEventHandle> ev) override {
                ReceiveCycles -= GetCycleCountFast();
                if (const auto it = ReceiveCallbacks.find(ev->Recipient); it != ReceiveCallbacks.end()) {
                    it->second->Receive(ev.release());
                    ++EventsReceivedCallback;
                } else {
                    ActorSystem->Send(ev.release());
                    ++EventsReceivedActorSystem;
                }
                ReceiveCycles += GetCycleCountFast();
            }

            ////////////////////////////////////////////////////////////////////////////////////////////////////////////
            // serialization/sending

            void Serialize(size_t minWriteBufferSize, size_t maxWriteBufferSize) {
                Serializer.ResetCounters();

                while (UnsentBytes < SerializeWindowSize && OutgoingSpans.size() < MaxSpansPerWrite) {
                    if (WriteBuffer.size() < minWriteBufferSize) { // (re)allocate write buffer
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
                if (numb >= WriteBufferSize * 2 && WriteBufferSize < maxWriteBufferSize) {
                    WriteBufferSize *= 2;
                } else if (numb < WriteBufferSize / 2 && WriteBufferSize > minWriteBufferSize) {
                    WriteBufferSize /= 2;
                }
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

            void ApplyBytesWritten(size_t num, size_t minSerializeWindowSize, size_t maxSerializeWindowSize,
                    std::vector<ui64> *eventToWireTime, std::vector<std::unique_ptr<IEventBase>> *events,
                    std::vector<TIntrusivePtr<TEventSerializedData>> *buffers) {
                BytesSent += num;

                // Check if we need to resize serialization window. If we have issued some data and all of it has been
                // successfully written, and it was limited by serialization window, we can increase it. If we have
                // serialized less than the window, we can decrease the window.
                if (num == BytesToWriteLastTime && BytesToWriteLastTime == SerializeWindowSize) {
                    SerializeWindowSize = Min(SerializeWindowSize + minSerializeWindowSize, maxSerializeWindowSize);
                }  else if (UnsentBytes < SerializeWindowSize) {
                    SerializeWindowSize = Max(SerializeWindowSize - minSerializeWindowSize, minSerializeWindowSize);
                }

                // Advance past exactly the bytes the kernel accepted. A writev can be short (e.g. under
                // backpressure or on a real network), so drop only fully-sent spans and trim the span that
                // straddles the boundary; the rest stay queued and are retried by the next writev.
                if constexpr (NSan::MSanIsOn()) {
                    for (auto& span : OutgoingSpans) {
                        NSan::CheckMemIsInitialized(span.data(), span.size());
                    }
                }
                size_t index = 0;
                for (size_t remaining = num; remaining; ++index) {
                    Y_DEBUG_ABORT_UNLESS(index < OutgoingSpans.size());
                    if (TContiguousSpan& front = OutgoingSpans[index]; front.size() <= remaining) {
                        remaining -= front.size();
                    } else {
                        front = TContiguousSpan(front.data() + remaining, front.size() - remaining);
                        break;
                    }
                }
                OutgoingSpans.erase(OutgoingSpans.begin(), OutgoingSpans.begin() + index);

                Y_ABORT_UNLESS(num <= UnsentBytes, "num# %zu UnsentBytes# %zu", num, UnsentBytes);
                UnsentBytes -= num;

                size_t numEvents = events->size();
                size_t numBuffers = buffers->size();
                size_t bytes = 0;
                Serializer.CommitProducedBytes(num, eventToWireTime, events, buffers);
                for (size_t i = numEvents, count = events->size(); i < count; ++i) {
                    bytes += (*events)[i]->CalculateSerializedSizeCached();
                }
                for (size_t i = numBuffers, count = buffers->size(); i < count; ++i) {
                    bytes += (*buffers)[i]->GetSize();
                }
                TotalOutputQueueSize.fetch_sub(bytes, std::memory_order_relaxed);
            }

            ////////////////////////////////////////////////////////////////////////////////////////////////////////////
            // ping/clock skew management

            NHPTimer::STime PingRequestSentTimestamp = 0;
            NHPTimer::STime PingResponseSentTimestamp = 0;

            // When the last ping request was issued. Unlike PingRequestSentTimestamp (which is reset as soon
            // as the reply arrives) this one keeps the ping cadence independent of the round-trip time.
            ui64 LastPingSentTimestamp = 0;

            // When we last got anything at all from the peer; drives dead-peer detection.
            ui64 LastInputActivityTimestamp = GetCycleCountFast();

            void SendPingRequest(ui64 timestamp) {
                NActorsInterconnect::TSystemPayloadV2 systemRequest;
                auto *r = systemRequest.AddRequests();
                r->MutablePingRequest();
                Serializer.Push(systemRequest);
                PingRequestSentTimestamp = timestamp;
                LastPingSentTimestamp = timestamp;
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

            bool IsMigratable(ui32 shardIdx) const {
                return !Terminated && MigrateTargetShard == shardIdx;
            }

            ////////////////////////////////////////////////////////////////////////////////////////////////////////////

            void RenderHtml(IOutputStream& str, TDuration deadPeerTimeout, TDuration pingPeriod) const {
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
                                    PARAM(FixedFileIndex)
                                    PARAM2("ReadBuffer size", ReadBuffer.size())
                                    PARAM(ReadBufferSize)
                                    PARAM2("IncomingSeqNo", IncomingSeqNo.load())
                                    PARAM(ExpectedSeqNo)
                                    PARAM(MigrateTargetShard)
                                    PARAM2("ClockSkew", ClockSkew->load())
                                    PARAM2("PingRTT", PingRTT->load())
                                    PARAM2("SinceLastInputActivity",
                                        CyclesToDuration(GetCycleCountFast() - LastInputActivityTimestamp))
                                    PARAM2("DeadPeerTimeout", deadPeerTimeout)
                                    PARAM2("PingPeriod", pingPeriod)
                                    PARAM2("ReceiveCallbacks size", ReceiveCallbacks.size())
                                    PARAM2("PendingRecordsHeap size", PendingRecordsHeap.size())
                                    PARAM(SerializeWindowSize)
                                    PARAM2("NumBytesInScratchBuffers", Serializer.GetNumBytesInScratchBuffers())
                                    PARAM(BytesSent)
                                    PARAM(BytesReceived)
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
                kOpProvideBuffers,
            };
            static const ui64 kOpMask = (1 << 3) - 1;

            struct TRingSlot {
                io_uring Ring{};
                i64 ItemsToSubmit = 0;
                bool FixedFilesEnabled = false;
                std::vector<int> FreeIndices;

                // Shared provided-buffer pool (buf_ring and/or legacy provide_buffers).
                bool ProvidedBuffersEnabled = false;
                bool BufRingEnabled = false;
                io_uring_buf_ring *BufRing = nullptr;
                unsigned BufRingEntries = 0;
                int BufGroupId = ProvidedBufGroupId;
                ui32 PoolBufCount = 0;
                char *PoolMemory = nullptr; // PoolBufCount * MinReadBufferSize contiguous slab

                ~TRingSlot() {
                    free(PoolMemory);
                }
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
            ui64 TicksPerRebalance = 1;
            ui64 TicksSinceRebalance = 0;

            size_t OpShift = 0;

            struct TSessionHash {
                size_t operator()(const std::unique_ptr<TSession>& p) const { return THash<void*>{}(p.get()); }
                size_t operator()(const TSession *p) const { return THash<void*>{}(p); }
                using is_transparent = void;
            };

            struct TSessionEqual {
                using T = std::unique_ptr<TSession>;
                bool operator()(const T& x, const T& y) const { return x == y; }
                bool operator()(const TSession *x, const T& y) const { return x == y.get(); }
                bool operator()(const T& x, const TSession *y) const { return x.get() == y; }
                using is_transparent = void;
            };

            std::unordered_set<std::unique_ptr<TSession>, TSessionHash, TSessionEqual> Sessions;
            TIntrusiveList<TSession> TouchedSessions;

            NMonitoring::TDynamicCounters::TCounterPtr SessionsRegistered;
            NMonitoring::TDynamicCounters::TCounterPtr SessionsUnregistered;
            NMonitoring::TDynamicCounters::TCounterPtr EventsSent;
            NMonitoring::TDynamicCounters::TCounterPtr EventsReceivedCallback;
            NMonitoring::TDynamicCounters::TCounterPtr EventsReceivedActorSystem;
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
            NMonitoring::TDynamicCounters::TCounterPtr ReadsToPool;
            NMonitoring::TDynamicCounters::TCounterPtr ReadsToBuffer;
            NMonitoring::TDynamicCounters::TCounterPtr ReadsNoBufs;
            NMonitoring::TDynamicCounters::TCounterPtr SessionsMigratedOut;
            NMonitoring::TDynamicCounters::TCounterPtr SessionsMigratedIn;
            NMonitoring::TDynamicCounters::TCounterPtr OutOfOrderCameIn;
            NMonitoring::TDynamicCounters::TCounterPtr OutOfOrderProcessed;
            NMonitoring::TDynamicCounters::TCounterPtr DeadPeersDetected;

            NMonitoring::TDynamicCounters::TCounterPtr CompleteWaitTotalTime;
            NMonitoring::TDynamicCounters::TCounterPtr SubmitTotalTime;
            NMonitoring::TDynamicCounters::TCounterPtr ApplyBytesReadTotalTime;
            NMonitoring::TDynamicCounters::TCounterPtr ApplyBytesWrittenTotalTime;
            NMonitoring::TDynamicCounters::TCounterPtr SerializeEventTotalTime;
            NMonitoring::TDynamicCounters::TCounterPtr ReceiveCallbackTotalTime;
            NMonitoring::TDynamicCounters::TCounterPtr ProcessCompletionsTotalTime;
            NMonitoring::TDynamicCounters::TCounterPtr ProcessPendingCommandsTotalTime;
            NMonitoring::TDynamicCounters::TCounterPtr ProcessTouchedSessionsTotalTime;
            NMonitoring::TDynamicCounters::TCounterPtr SerializeTotalTime;
            NMonitoring::TDynamicCounters::TCounterPtr DestroyEventsTotalTime;

            NMonitoring::THistogramPtr CommandDeliveryTime;
            NMonitoring::THistogramPtr EventToWireTime;
            NMonitoring::THistogramPtr CommandExecTime;
            NMonitoring::THistogramPtr SubmitExecTime;
            NMonitoring::THistogramPtr SerializeTime;
            NMonitoring::THistogramPtr CompletionsProcessedAtOnce;
            NMonitoring::THistogramPtr SubmissionsProcessedAtOnce;

            ui64 LastActivitySwitchTimestamp = 0;
            NMonitoring::TDynamicCounters::TCounterPtr *CurrentActivityTime = nullptr;

            const double Freq = 1e9 * NHPTimer::GetSeconds(1); // nanoseconds per cycle

            std::vector<ui64> EventToWireTimeVec;

            TShardLoad& Load;

            std::unique_ptr<TEvDestroyEvents> EvDestroyEvents = std::make_unique<TEvDestroyEvents>();

            const ui32 MinReadBufferSize;
            const ui32 MaxReadBufferSize;
            const ui32 MinWriteBufferSize;
            const ui32 MaxWriteBufferSize;
            const ui32 MinSerializeWindowSize;
            const ui32 MaxSerializeWindowSize;

        private:
            NMonitoring::TDynamicCounters::TCounterPtr *SwitchActivity(
                    NMonitoring::TDynamicCounters::TCounterPtr *newActivity, ui64 currentTimestamp) {
                if (CurrentActivityTime != newActivity) {
                    const ui64 timestamp = std::exchange(LastActivitySwitchTimestamp, currentTimestamp);
                    if (CurrentActivityTime) {
                        **CurrentActivityTime += (LastActivitySwitchTimestamp - timestamp) * Freq;
                    }
                }
                return std::exchange(CurrentActivityTime, newActivity);
            }

            NMonitoring::TDynamicCounters::TCounterPtr *SwitchActivity(
                    NMonitoring::TDynamicCounters::TCounterPtr *newActivity) {
                return SwitchActivity(newActivity, GetCycleCountFast());
            }

            class TActivityMeasure {
                TShard& Shard;
                NMonitoring::TDynamicCounters::TCounterPtr *PrevActivityTime;

            public:
                TActivityMeasure(TShard& shard, NMonitoring::TDynamicCounters::TCounterPtr *activityTime)
                    : Shard(shard)
                    , PrevActivityTime(Shard.SwitchActivity(activityTime))
                {}

                ~TActivityMeasure() {
                    Shard.SwitchActivity(PrevActivityTime);
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

            // Reserve a sparse fixed-file table on the ring before the worker starts. Prefer the 5.19+
            // sparse helper; fall back to registering an array of -1 (supported since ~5.5, covers 5.13).
            void InitFixedFiles(TRingSlot& slot, ui32 fixedFilesPerRing) {
                if (!fixedFilesPerRing) {
                    return;
                }
                int ret = io_uring_register_files_sparse(&slot.Ring, fixedFilesPerRing);
                if (ret != 0) {
                    std::vector<int> minusOnes(fixedFilesPerRing, -1);
                    ret = io_uring_register_files(&slot.Ring, minusOnes.data(), minusOnes.size());
                }
                if (ret != 0) {
                    return;
                }
                slot.FixedFilesEnabled = true;
                slot.FreeIndices.resize(fixedFilesPerRing);
                std::iota(slot.FreeIndices.rbegin(), slot.FreeIndices.rend(), 0);
            }

            void BindSessionFixedFile(TSession& session) {
                Y_DEBUG_ABORT_UNLESS(session.FixedFileIndex == -1);
                Y_DEBUG_ABORT_UNLESS(session.PreferredRingIdx < Rings.size());
                auto& slot = Rings[session.PreferredRingIdx];
                if (!slot.FixedFilesEnabled || slot.FreeIndices.empty()) {
                    return;
                }
                session.FixedFileIndex = slot.FreeIndices.back();
                const int fd = *session.Socket;
                const int ret = io_uring_register_files_update(&slot.Ring, session.FixedFileIndex, &fd, 1);
                if (ret != 1) {
                    session.FixedFileIndex = -1;
                } else {
                    slot.FreeIndices.pop_back();
                }
            }

            void UnbindSessionFixedFile(TSession& session) {
                if (session.FixedFileIndex == -1) {
                    return;
                }
                Y_DEBUG_ABORT_UNLESS(session.PreferredRingIdx < Rings.size());
                auto& slot = Rings[session.PreferredRingIdx];
                Y_DEBUG_ABORT_UNLESS(slot.FixedFilesEnabled);
                const int clear = -1;
                const int ret = io_uring_register_files_update(&slot.Ring, session.FixedFileIndex, &clear, 1);
                Y_ABORT_UNLESS(ret == 1, "io_uring_register_files_update(clear) failed: %s", strerror(-ret));
                slot.FreeIndices.push_back(session.FixedFileIndex);
                session.FixedFileIndex = -1;
            }

            // Shared recv pool: prefer buf_ring (5.19+), else legacy provide_buffers (5.7+ / 5.13).
            void InitProvidedBuffers(ui32 ringIdx, TRingSlot& slot, ui32 poolBufCount, bool allowBufRing) {
                if (!poolBufCount) {
                    return;
                }
                slot.PoolBufCount = poolBufCount;
                slot.BufGroupId = ProvidedBufGroupId;
                size_t numb = static_cast<size_t>(MinReadBufferSize) * poolBufCount;
                slot.PoolMemory = static_cast<char*>(malloc(numb));
                NSan::Poison(slot.PoolMemory, numb);

                if (allowBufRing) {
                    const unsigned entries = NextPowerOfTwo(poolBufCount);
                    int err = 0;
                    slot.BufRing = io_uring_setup_buf_ring(&slot.Ring, entries, slot.BufGroupId, 0, &err);
                    if (slot.BufRing) {
                        slot.BufRingEntries = entries;
                        slot.BufRingEnabled = true;
                        const int mask = io_uring_buf_ring_mask(entries);
                        for (size_t i = 0; i < poolBufCount; ++i) {
                            char *addr = slot.PoolMemory + i * MinReadBufferSize;
                            io_uring_buf_ring_add(slot.BufRing, addr, MinReadBufferSize, i, mask, i);
                        }
                        io_uring_buf_ring_advance(slot.BufRing, poolBufCount);
                        slot.ProvidedBuffersEnabled = true;
                        return;
                    }
                }

                // Legacy provide_buffers: one SQE installs the whole contiguous slab.
                io_uring_sqe *sqe = GetSQE(nullptr, kOpProvideBuffers, ringIdx);
                Y_ABORT_UNLESS(sqe);
                io_uring_prep_provide_buffers(sqe, slot.PoolMemory, MinReadBufferSize, poolBufCount, slot.BufGroupId, 0);
                slot.ProvidedBuffersEnabled = true;
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

            TShard(TUringEngine& engine, ui32 shardIdx, const NMonitoring::TDynamicCounterPtr& shardCounters,
                    const TInterconnectSettings::TV2& v2, TShardLoad& load, TShard *shareRingsWith)
#define COUNTER(NAME, DERIV) NAME(shardCounters->GetCounter(#NAME, DERIV))
                : Engine(engine)
                , ShardIdx(shardIdx)
                , COUNTER(SessionsRegistered, true)
                , COUNTER(SessionsUnregistered, true)
                , COUNTER(EventsSent, true)
                , COUNTER(EventsReceivedCallback, true)
                , COUNTER(EventsReceivedActorSystem, true)
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
                , COUNTER(ReadsToPool, true)
                , COUNTER(ReadsToBuffer, true)
                , COUNTER(ReadsNoBufs, true)
                , COUNTER(SessionsMigratedOut, true)
                , COUNTER(SessionsMigratedIn, true)
                , COUNTER(OutOfOrderCameIn, true)
                , COUNTER(OutOfOrderProcessed, true)
                , COUNTER(DeadPeersDetected, true)
#define TOTAL_TIME(NAME) NAME(shardCounters->GetCounter("TotalTime/" #NAME, true))
                , TOTAL_TIME(CompleteWaitTotalTime)
                , TOTAL_TIME(SubmitTotalTime)
                , TOTAL_TIME(ApplyBytesReadTotalTime)
                , TOTAL_TIME(ApplyBytesWrittenTotalTime)
                , TOTAL_TIME(SerializeEventTotalTime)
                , TOTAL_TIME(ReceiveCallbackTotalTime)
                , TOTAL_TIME(ProcessCompletionsTotalTime)
                , TOTAL_TIME(ProcessPendingCommandsTotalTime)
                , TOTAL_TIME(ProcessTouchedSessionsTotalTime)
                , TOTAL_TIME(SerializeTotalTime)
                , TOTAL_TIME(DestroyEventsTotalTime)
                , CommandDeliveryTime(shardCounters->GetNamedHistogram("sensor", "CommandDeliveryTime", TimeCollector()))
                , EventToWireTime(shardCounters->GetNamedHistogram("sensor", "EventToWireTime", TimeCollector()))
                , CommandExecTime(shardCounters->GetNamedHistogram("sensor", "CommandExecTime", TimeCollector()))
                , SubmitExecTime(shardCounters->GetNamedHistogram("sensor", "SubmitExecTime", TimeCollector()))
                , SerializeTime(shardCounters->GetNamedHistogram("sensor", "SerializeTime", TimeCollector()))
                , CompletionsProcessedAtOnce(shardCounters->GetNamedHistogram("sensor", "CompletionsProcessedAtOnce", NMonitoring::ExponentialHistogram(10, 2)))
                , SubmissionsProcessedAtOnce(shardCounters->GetNamedHistogram("sensor", "SubmissionsProcessedAtOnce", NMonitoring::ExponentialHistogram(12, 2)))
#undef TOTAL_TIME
#undef COUNTER
                , Load(load)
                , MinReadBufferSize(v2.MinReadBufferSize)
                , MaxReadBufferSize(v2.MaxReadBufferSize)
                , MinWriteBufferSize(v2.MinWriteBufferSize)
                , MaxWriteBufferSize(v2.MaxWriteBufferSize)
                , MinSerializeWindowSize(v2.MinSerializeWindowSize)
                , MaxSerializeWindowSize(v2.MaxSerializeWindowSize)
            {
                const ui32 sqThreadIdleMs = v2.SqThreadIdleMs
                    ? v2.SqThreadIdleMs
                    : TUringContext::SqThreadIdleMs;

                const bool allowBufRing = !GetEnv("YDB_IC_V2_DISABLE_BUF_RING");

                EventFd = eventfd(0, 0);
                if (EventFd == -1) {
                    Y_ABORT("eventfd() failed: %s", strerror(errno));
                }

                Rings.resize(v2.RingsPerShard);
                for (ui32 i = 0; i < Rings.size(); ++i) {
                    auto& slot = Rings[i];

                    InitRing(slot, v2.EnableSQPOLL, sqThreadIdleMs, shareRingsWith ? &shareRingsWith->Rings[i] : nullptr);

                    // Must run before Start(): io_uring_register waits for the ring to idle, which deadlocks
                    // if a worker/SQPOLL thread already holds a ref inside io_uring_enter.
                    if (v2.EnableFixedFiles) {
                        InitFixedFiles(slot, v2.FixedFilesPerRing);
                    }

                    if (v2.EnableProvidedBuffers) {
                        InitProvidedBuffers(i, slot, v2.PoolBufCount, allowBufRing);
                    }

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
                const TDuration tickPeriod = Engine.TickPeriod;
                itimerspec spec{};
                spec.it_interval.tv_sec = tickPeriod.Seconds();
                spec.it_interval.tv_nsec = tickPeriod.NanoSecondsOfSecond();
                spec.it_value = spec.it_interval;
                if (timerfd_settime(TimerFd, 0, &spec, nullptr) < 0) {
                    Y_ABORT("timerfd_settime failed: %s", strerror(errno));
                }

                // The tick drives liveness, which may be much more frequent than rebalancing needs to be.
                TicksPerRebalance = Max<ui64>(1, RebalancePeriod.GetValue() / tickPeriod.GetValue());
            }

            ~TShard() {
                Stop(); // joins the worker thread, so no completion will be dispatched after this point
                for (auto& slot : Rings) {
                    if (slot.BufRing) {
                        io_uring_free_buf_ring(&slot.Ring, slot.BufRing, slot.BufRingEntries, slot.BufGroupId);
                        slot.BufRing = nullptr;
                    }
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

            void Register(std::unique_ptr<TSession> session) {
                ++*SessionsRegistered;

                // this would be the session's first event, so its sequencing is not the problem
                SendInternal(reinterpret_cast<ui64>(session.release()), static_cast<ui32>(ENetwork::EvRegisterSession),
                    {}, nullptr, false);
            }

            void AcceptMigrated(std::unique_ptr<TSession> session) {
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

                // handle output queue size here
                const size_t size = ev->HasBuffer() ? ev->GetChainBuffer()->GetSize() :
                    ev->HasEvent() ? ev->GetBase()->CalculateSerializedSizeCached() : 0;
                if (size > Engine.Common->Settings.MaxSerializedEventSize) {
                    return SendInternal(conn, static_cast<ui32>(ENetwork::EvUringEventTooLarge), {}, nullptr, true);
                }
                auto& session = *reinterpret_cast<TSession*>(conn);
                const ui64 newQueueSize = size + session.TotalOutputQueueSize.fetch_add(size, std::memory_order_relaxed);
                if (const ui64 limit = Engine.Common->Settings.SendBufferDieLimitInMB; limit && newQueueSize > limit * 1_MB) {
                    return SendInternal(conn, static_cast<ui32>(ENetwork::EvUringQueueOverload), {}, nullptr, true);
                }

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
                // send this event serializable, otherwise we can get a race
                SendImpl(conn, std::make_unique<IEventHandle>(TActorId(), TActorId(),
                    new TEvUringMonRequest(std::move(ev))), nullptr, true);
            }

        private:
            // Pops and frees any commands still sitting in the queue after the worker has stopped. Mirrors
            // the ownership handling of the worker loop: destroys the embedded TEventPayload and reclaims a
            // TSession handed off via an unprocessed EvRegisterSession.
            void DrainQueue() {
                while (auto record = IncomingEventQueue.Pop()) {
                    if (record->Ev->Type == static_cast<ui32>(ENetwork::EvRegisterSession)) {
                        delete reinterpret_cast<TSession*>(record->Conn);
                    }
                }
            }

            void SendImpl(ui64 conn, std::unique_ptr<IEventHandle> ev, TIntrusivePtr<IReceiveCallback> replyCallback,
                    bool ensureSequence) {
                ui64 seqNo = 0;
                if (Y_LIKELY(ensureSequence)) {
                    Y_DEBUG_ABORT_UNLESS(conn);
                    auto& session = *reinterpret_cast<TSession*>(conn);
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

            // GetSQE returns next available SQ entry, setting up ItemsToSubmit counter in order to commence submission
            // on the end of the worker loop. Event/timer control ops always use ring 0.
            io_uring_sqe *GetSQE(TSession *session, EOperationType op, int ringIdx = -1) {
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
                    Y_DEBUG_ABORT_UNLESS(op == kOpEvent || op == kOpTimer || op == kOpProvideBuffers
                        ? session == nullptr
                        : session != nullptr);
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

                ACTIVITY(&SubmitTotalTime) {
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

                // Arm pipe + timer
                PutEventReadRequest();
                PutTimer();

                LastActivitySwitchTimestamp = GetCycleCountFast();
                ui64 loopStartTimestamp = LastActivitySwitchTimestamp;

                for (;;) {
                    // submit any pending SQ's (if we have any)
                    SwitchActivity(&SubmitTotalTime, loopStartTimestamp);
                    for (auto& slot : Rings) {
                        if (slot.ItemsToSubmit) {
                            DoSubmit(slot);
                        }
                    }

                    // process pending CQ events from every ring
                    SwitchActivity(&ProcessCompletionsTotalTime);
                    bool progress = ProcessCompletions();

                    // process pending events and commands
                    SwitchActivity(&ProcessPendingCommandsTotalTime);
                    bool stopping = false;
                    progress |= ProcessPendingCommands(&stopping);
                    if (stopping) {
                        break;
                    }

                    // process touched sessions
                    SwitchActivity(&ProcessTouchedSessionsTotalTime);
                    while (!TouchedSessions.Empty()) {
                        TSession *session = TouchedSessions.PopFront();
                        MaybeIssueReadForSession(*session);
                        MaybeIssueWriteForSession(*session);
                        if (MaybeFinishMigrate(*session)) {
                            continue;
                        }
                        MaybeEraseSession(*session);
                    }

                    // discard pending events/buffers, if any
                    SwitchActivity(&DestroyEventsTotalTime);
                    if (!EvDestroyEvents->Events.empty() || !EvDestroyEvents->Buffers.empty()) {
                        const size_t bytes = EvDestroyEvents->CalculateTotalSize();
                        const auto& counter = Engine.Common->DestructorQueueSize;
                        const size_t max = Engine.Common->MaxDestructorQueueSize;
                        EvDestroyEvents->Counter = counter;
                        EvDestroyEvents->Bytes = bytes;
                        if (Y_LIKELY(!counter || counter->fetch_add(bytes, std::memory_order_relaxed) + bytes <= max)) {
                            Engine.ActorSystem->Send(new IEventHandle(Engine.DestructorActorId, {}, EvDestroyEvents.release()));
                        }
                        EvDestroyEvents = std::make_unique<TEvDestroyEvents>();
                    }

                    // wait for CQ in there is no progress
                    SwitchActivity(&CompleteWaitTotalTime);
                    const ui64 waitStartTimestamp = LastActivitySwitchTimestamp;
                    if (!progress) {
                        WaitingForCQ.store(true, std::memory_order_release);

                        // it is critical we first set WaitingForCQ, and then rechecking the queue
                        if (IncomingEventQueue.IsEmpty()) {
                            // wait for the ring waiting for EventFd
                            io_uring_cqe *cqe;
                            if (int res = io_uring_wait_cqe(&Rings.front().Ring, &cqe); res && res != -EINTR) {
                                Y_ABORT("io_uring_wait_cqe() failed: %s", strerror(-res));
                            }
                        }

                        WaitingForCQ.store(false, std::memory_order_relaxed);
                    }

                    const ui64 loopEndTimestamp = GetCycleCountFast();
                    const ui64 total = loopEndTimestamp - loopStartTimestamp;
                    const ui64 wait = loopEndTimestamp - waitStartTimestamp;
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
                        auto& session = *reinterpret_cast<TSession*>(record->Conn);
                        if (Sessions.find(&session) == Sessions.end()) { // forward event to other shard
                            Engine.GetShard(record->Conn).Enqueue(std::move(*record));
                        } else if (record->SeqNo != session.ExpectedSeqNo) {
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
                        if (TSession& session = GetSession(record); record->Callback) {
                            session.ReceiveCallbacks[record->Ev->Sender] = std::move(record->Callback);
                        } else {
                            session.ReceiveCallbacks.erase(record->Ev->Sender);
                        }
                        break;

                    case static_cast<ui32>(ENetwork::EvRegisterSession): {
                        std::unique_ptr<TSession> session(reinterpret_cast<TSession*>(record->Conn));
                        session->PreferredRingIdx = OpShift++ % Rings.size();
                        session->OwnerShard.store(ShardIdx, std::memory_order_release);
                        BindSessionFixedFile(*session);
                        const auto [it, inserted] = Sessions.emplace(std::move(session));
                        Y_ABORT_UNLESS(inserted);
                        TouchedSessions.PushBack(it->get());
                        break;
                    }

                    case static_cast<ui32>(ENetwork::EvUnregisterSession): {
                        TSession& session = GetSession(record);
                        // Do NOT free the session while it still has an armed recv or an in-flight
                        // writev: their io_uring completions carry a raw pointer to this object and
                        // would dereference freed memory. Mark it terminated (so no new ops are armed)
                        // and erase only once both are drained. The session actor has already shut the
                        // socket down before requesting unregistration, so the pending ops complete
                        // promptly (EOF/EPIPE).
                        session.Terminated = true;
                        session.UnregisterRequested = true;
                        if (session.ReadPending) { // cancel pending read in order to unregister the session
                            CancelOp(session, kOpRead, session.ReadPendingRingIdx);
                        }
                        TouchedSessions.PushBack(&session);
                        break;
                    }

                    case static_cast<ui32>(ENetwork::EvStop):
                        Y_ABORT();

                    case static_cast<ui32>(ENetwork::EvUringMonRequest):
                        ProcessMonRequest(GetSession(record), std::move(record->Ev->Get<TEvUringMonRequest>()->Ev));
                        break;

                    case static_cast<ui32>(ENetwork::EvUringQueueOverload): {
                        TSession& session = GetSession(record);
                        session.Disconnect(TDisconnectReason::QueueOverload());
                        break;
                    }

                    case static_cast<ui32>(ENetwork::EvUringEventTooLarge): {
                        TSession& session = GetSession(record);
                        session.Disconnect(TDisconnectReason::EventTooLarge());
                        break;
                    }

                    default: {
                        TSession& session = GetSession(record);
                        if (record->Callback) { // register callback coming along with the message
                            session.ReceiveCallbacks[record->Ev->Sender] = std::move(record->Callback);
                        }
                        session.Serializer.Push(std::move(record->Ev));
                        if (!session.WritePending) {
                            TouchedSessions.PushBack(&session);
                        }
                        break;
                    }
                }
            }

            ////////////////////////////////////////////////////////////////////////////////////////////////////////////
            // monitoring

            void ProcessMonRequest(TSession& session, NMon::TEvHttpInfoRes::TPtr ev) {
                TStringOutput str(const_cast<TString&>(static_cast<NMon::TEvHttpInfoRes*>(ev->Get())->Answer));
                session.RenderHtml(str, Engine.DeadPeerTimeout, Engine.PingPeriod);
                Engine.ActorSystem->Send(ev.Release());
            }

            ////////////////////////////////////////////////////////////////////////////////////////////////////////////
            // io_uring completion handlers

            bool ProcessCompletions() {
                bool progress = false;
                i64 completionsProcessedAtOnce = 0;
                for (ui32 ringIdx = 0; ringIdx < Rings.size(); ++ringIdx) {
                    auto& slot = Rings[ringIdx];
                    io_uring_cqe *cqes[CqeBatchSize];
                    while (const unsigned n = io_uring_peek_batch_cqe(&slot.Ring, cqes, CqeBatchSize)) {
                        progress = true; // we did something with the queues
                        for (unsigned i = 0; i < n; ++i) {
                            DispatchCompletion(*cqes[i], ringIdx);
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

            void DispatchCompletion(io_uring_cqe& cqe, ui32 ringIdx) {
                auto *session = reinterpret_cast<TSession*>(uintptr_t(cqe.user_data) & ~uintptr_t(kOpMask));
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
                        DispatchRead(*session, cqe.res, cqe.flags, ringIdx);
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

                    case kOpProvideBuffers:
                        // Legacy pool recycle completed; nothing else to do (failure just leaks one slot).
                        break;
                }
                ++*CQEProcessed;
            }

            void DispatchTimer() {
                const ui64 now = GetCycleCountFast();

                for (auto& session : Sessions) {
                    if (session->Terminated || session->MigrateTargetShard != ShardIdx) {
                        continue;
                    }

                    // Anything arriving from the peer -- payload, ping request, ping response -- counts as
                    // proof of life; when nothing has for the whole timeout, the link is declared dead. The
                    // peer keeps the stream flowing either by pinging us or by answering our pings, so this
                    // works on both sides of the connection.
                    if (now - session->LastInputActivityTimestamp >= Engine.DeadPeerTimeoutCycles) {
                        ++*DeadPeersDetected;
                        session->Disconnect(TDisconnectReason::DeadPeer());
                        continue;
                    }

                    if (session->SendPings && session->PingRequestSentTimestamp == 0 &&
                            now - session->LastPingSentTimestamp >= Engine.PingPeriodCycles) {
                        session->SendPingRequest(now);
                        TouchedSessions.PushBack(session.get());
                    }
                }

                // Rebalancing is far less urgent than liveness, so it runs on its own, coarser period to
                // limit churn under short load spikes.
                if (++TicksSinceRebalance >= TicksPerRebalance) {
                    TicksSinceRebalance = 0;
                    MaybeOffload();
                }
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

                TSession *candidate = nullptr;
                for (auto& session : Sessions) {
                    if (session->IsMigratable(ShardIdx)) {
                        candidate = session.get();
                        break;
                    }
                }
                if (!candidate) {
                    return;
                }

                candidate->MigrateTargetShard = bestTarget;

                if (candidate->ReadPending) {
                    CancelOp(*candidate, kOpRead, candidate->ReadPendingRingIdx);
                }

                TouchedSessions.PushBack(candidate);
            }

            void CancelOp(TSession& session, EOperationType op, int ringIdx) {
                Y_DEBUG_ABORT_UNLESS(ringIdx != -1);
                io_uring_sqe *sqe = GetSQE(&session, kOpCancel, ringIdx);
                Y_ABORT_UNLESS(sqe, "failed to obtain cancel SQE");
                const ui64 targetUserData = reinterpret_cast<uintptr_t>(&session) | op;
                io_uring_prep_cancel64(sqe, targetUserData, 0);
            }

            bool MaybeFinishMigrate(TSession& session) {
                if (session.MigrateTargetShard == ShardIdx || session.ReadPending || session.WritePending || session.Terminated) {
                    return false;
                }

                // Drop the fixed-file binding on this shard before handing the session off; the destination
                // rebinds in its EvRegisterSession handler. Safe: no ops are in flight.
                UnbindSessionFixedFile(session);

                auto it = Sessions.find(&session);
                Y_DEBUG_ABORT_UNLESS(it != Sessions.end());
                auto node = Sessions.extract(it);
                Y_DEBUG_ABORT_UNLESS(node);

                ++*SessionsMigratedOut;
                Engine.Shards[session.MigrateTargetShard]->AcceptMigrated(std::move(node.value()));

                return true;
            }

            void DispatchRead(TSession& session, i32 res, ui32 cqeFlags, ui32 ringIdx) {
                Y_DEBUG_ABORT_UNLESS(session.ReadPending);
                session.ReadPending = false;

                TRingSlot& slot = Rings[ringIdx];

                unsigned poolBid = 0;
                char *poolData = nullptr;
                if (cqeFlags & IORING_CQE_F_BUFFER) {
                    poolBid = cqeFlags >> IORING_CQE_BUFFER_SHIFT;
                    Y_ABORT_UNLESS(poolBid < slot.PoolBufCount);
                    poolData = slot.PoolMemory + static_cast<size_t>(poolBid) * MinReadBufferSize;
                }

                if (session.Terminated) {
                    // teardown in progress: don't retry the read or re-arm; just let the session drain toward erasure below
                } else if (res == -ECANCELED) {
                    // cancelled without migrate (should be rare); re-arm unless terminating
                } else if (res == -EAGAIN) {
                    ++*ReadUnavail;
                } else if (res == -ENOBUFS) {
                    // no pool buffer available: allocate buffer for ordinary read and retry
                    ++*ReadsNoBufs;
                    session.GetReadSpan(MinReadBufferSize);
                } else if (res < 0) {
                    session.Disconnect(TDisconnectReason::FromErrno(-res));
                } else if (res == 0) {
                    session.Disconnect(TDisconnectReason::EndOfStream());
                } else {
                    *BytesReceived += res;

                    session.ReceiveCycles = 0;
                    session.EventsReceivedCallback = 0;
                    session.EventsReceivedActorSystem = 0;
                    ACTIVITY(&ApplyBytesReadTotalTime) {
                        // remember the last time when something came -- used for DeadPeer logic
                        session.LastInputActivityTimestamp = LastActivitySwitchTimestamp;

                        if (poolData) {
                            session.ApplyBytesReadCopy(poolData, res, MinReadBufferSize, MaxReadBufferSize);
                            ++*ReadsToPool;
                        } else {
                            session.ApplyBytesRead(res, MinReadBufferSize, MaxReadBufferSize);
                            ++*ReadsToBuffer;
                        }

                        LastActivitySwitchTimestamp += session.ReceiveCycles;
                        *ReceiveCallbackTotalTime += session.ReceiveCycles * Freq;
                        if (const ui64 n = session.EventsReceivedCallback) {
                            *EventsReceivedCallback += n;
                        }
                        if (const ui64 n = session.EventsReceivedActorSystem) {
                            *EventsReceivedActorSystem += n;
                        }
                    }
                }

                if (poolData) { // recycle processed pool entry, if any
                    if (slot.BufRingEnabled) {
                        const int mask = io_uring_buf_ring_mask(slot.BufRingEntries);
                        io_uring_buf_ring_add(slot.BufRing, poolData, MinReadBufferSize, poolBid, mask, 0);
                        io_uring_buf_ring_advance(slot.BufRing, 1);
                    } else {
                        // Legacy path: re-provide via SQE on the owning ring (completed as kOpProvideBuffers).
                        io_uring_sqe *sqe = GetSQE(nullptr, kOpProvideBuffers, ringIdx);
                        Y_ABORT_UNLESS(sqe);
                        io_uring_prep_provide_buffers(sqe, poolData, MinReadBufferSize, 1, slot.BufGroupId, poolBid);
                    }
                }

                TouchedSessions.PushBack(&session);
            }

            void MaybeIssueReadForSession(TSession& session) {
                if (session.ReadPending || session.Terminated || session.MigrateTargetShard != ShardIdx) {
                    return;
                }

                auto& slot = Rings[session.PreferredRingIdx];
                io_uring_sqe *sqe = GetSQE(&session, kOpRead, session.PreferredRingIdx);
                Y_ABORT_UNLESS(sqe);

                TMutableContiguousSpan span(nullptr, MinReadBufferSize);

                if (session.ReadBufferSize == MinReadBufferSize && slot.ProvidedBuffersEnabled && session.ReadBuffer.empty()) {
                    // we're reading into automatically located pool buffer
                    sqe->flags |= IOSQE_BUFFER_SELECT;
                    sqe->buf_group = slot.BufGroupId;
                } else {
                    // we're reading into session's ReadBuffer, so we need to possibly allocate buffer and get its read span
                    span = session.GetReadSpan(MinReadBufferSize);
                }

                Y_DEBUG_ABORT_UNLESS(span.size());

                io_uring_prep_read(sqe, *session.Socket, span.data(), span.size(), -1);

                if (session.FixedFileIndex >= 0) {
                    sqe->fd = session.FixedFileIndex;
                    sqe->flags |= IOSQE_FIXED_FILE;
                }

                session.ReadPending = true;
            }

            void DispatchWrite(TSession& session, i32 res) {
                Y_ABORT_UNLESS(session.WritePending);
                session.WritePending = false;

                if (session.Terminated) {
                    // teardown in progress: don't retry the write or re-arm; just let the session drain toward erasure below
                } else if (res == -ECANCELED) {
                    // we should probably restart operation
                } else if (res == -EAGAIN) {
                    ++*WriteUnavail;
                } else if (res < 0) {
                    session.Disconnect(TDisconnectReason::FromErrno(-res));
                } else if (res == 0) {
                    session.Disconnect(TDisconnectReason::EndOfStream());
                } else {
                    *BytesSent += res;
                    ACTIVITY(&ApplyBytesWrittenTotalTime) {
                        session.ApplyBytesWritten(res, MinSerializeWindowSize, MaxSerializeWindowSize,
                            &EventToWireTimeVec, &EvDestroyEvents->Events, &EvDestroyEvents->Buffers);
                        for (const ui64 time : EventToWireTimeVec) {
                            EventToWireTime->Collect(time * Freq, 1u);
                        }
                        EventToWireTimeVec.clear();
                    }
                }

                TouchedSessions.PushBack(&session);
            }

            void MaybeIssueWriteForSession(TSession& session) {
                if (session.WritePending || session.Terminated || session.MigrateTargetShard != ShardIdx) {
                    return;
                }
                if (session.Serializer.IsTrafficPending()) {
                    ACTIVITY(&SerializeTotalTime) {
                        session.Serialize(MinWriteBufferSize, MaxWriteBufferSize);
                        const ui64 serializeEventTime = session.Serializer.GetSerializeEventTime();
                        LastActivitySwitchTimestamp += serializeEventTime;
                        *SerializeEventTotalTime += serializeEventTime * Freq;
                        *BytesCopied += session.Serializer.GetBytesCopied();
                        *BytesAliased += session.Serializer.GetBytesAliased();
                    }
                }
                if (session.PrepareIovec()) {
                    io_uring_sqe *sqe = GetSQE(&session, kOpWrite, session.PreferredRingIdx);
                    Y_ABORT_UNLESS(sqe);
                    const int fdOrIndex = session.FixedFileIndex >= 0
                        ? session.FixedFileIndex
                        : static_cast<int>(*session.Socket);
                    io_uring_prep_writev(sqe, fdOrIndex, session.Iov, session.IovLen, -1);
                    if (session.FixedFileIndex >= 0) {
                        sqe->flags |= IOSQE_FIXED_FILE;
                    }
                    session.WritePending = true;
                }
            }

            ////////////////////////////////////////////////////////////////////////////////////////////////////////////
            // commands from outer threads

            TSession& GetSession(TIncomingEventQueue::TRecord *record) {
                TSession *ptr = reinterpret_cast<TSession*>(record->Conn);
                Y_ABORT_UNLESS(Sessions.find(ptr) != Sessions.end());
                return *ptr;
            }

            // Frees an unregistered session once it has no io_uring operation in flight. It is unsafe to
            // erase earlier because any pending read/write completion references the session by raw pointer.
            void MaybeEraseSession(TSession& session) {
                if (session.UnregisterRequested && !session.ReadPending && !session.WritePending) {
                    // Release the fixed-file slot while the socket is still alive, then drop the session
                    // (which closes the fd via TStreamSocket's destructor).
                    UnbindSessionFixedFile(session);
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
        TActorId DestructorActorId;

    public:
        TUringEngine(TIntrusivePtr<TInterconnectProxyCommon> common)
            : Common(std::move(common))
            , ChecksumEvents(Common->Settings.V2.ChecksumEvents)
            , DeadPeerTimeout(CalculateDeadPeerTimeout(Common->Settings))
            , PingPeriod(CalculatePingPeriod(Common->Settings, DeadPeerTimeout))
            , TickPeriod(CalculateTickPeriod(PingPeriod))
            , DeadPeerTimeoutCycles(DurationToCycles(DeadPeerTimeout))
            , PingPeriodCycles(DurationToCycles(PingPeriod))
            , UringCounters(Common->MonCounters->GetSubgroup("subsystem", "uring"))
        {
            const auto& v2 = Common->Settings.V2;
            ShardLoads = std::vector<TShardLoad>(v2.Threads);
            Shards.reserve(v2.Threads);
            for (ui32 i = 0; i < v2.Threads; ++i) {
                Shards.push_back(std::make_unique<TShard>(*this,
                    i, // shardIdx
                    UringCounters->GetSubgroup("shard", "0" /*ToString(i)*/),
                    v2,
                    ShardLoads[i],
                    v2.ShareRingsAmongThreads && !Shards.empty() ? Shards.front().get() : nullptr));
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
            // DestructorId is filled in after the engine is created, so it is picked up here rather than in
            // the constructor. Published together with ActorSystem, i.e. before the first Register and thus
            // before any shard worker can reach the code using either of them.
            DestructorActorId = Common->DestructorId;
            ActorSystem = actorSystem;
            // Stop the reaper threads while the actor system is still up, so no completion is posted to a
            // torn-down system.
            actorSystem->DeferPreStop([self = TIntrusivePtr<IUringEngine>(this)] { self->Stop(); });
        }

        ui64 Register(TIntrusivePtr<NInterconnect::TStreamSocket> socket, const TActorId& sessionActorId,
                TScopeId peerScopeId, std::function<void(TDisconnectReason)> onDisconnectCallback,
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

            const auto& v2 = Common->Settings.V2;

            auto session = std::make_unique<TSession>(shardIdx, std::move(socket), sessionActorId,
                ChecksumEvents, peerScopeId, std::move(onDisconnectCallback), ActorSystem, sendPings,
                std::move(clockSkew), std::move(pingRTT));
            session->ReadBufferSize = v2.MinReadBufferSize;
            session->WriteBufferSize = v2.MinWriteBufferSize;
            session->SerializeWindowSize = v2.MinSerializeWindowSize;
            const ui64 conn = reinterpret_cast<ui64>(session.get());
            Shards[shardIdx]->Register(std::move(session));
            return conn;
        }

        TShard& GetShard(ui64 conn) const {
            // OwnerShard is the indirection for dynamic mapping: stable conn handle, mutable owner.
            return *Shards[reinterpret_cast<TSession*>(conn)->OwnerShard.load(std::memory_order_acquire)];
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
                // Common holds this engine, so keeping a reference to it here would make the pair
                // unreclaimable. The workers are joined by now and nothing dereferences Common past this
                // point (all of its data the engine needs is cached in const members).
                Common.Reset();
            }
        }

        void IssueMonRequest(ui64 conn, NMon::TEvHttpInfoRes::TPtr ev) override {
            GetShard(conn).IssueMonRequest(conn, std::move(ev));
        }

        ui64 GetTotalOutputQueueSize(ui64 conn) override {
            return reinterpret_cast<TSession*>(conn)->TotalOutputQueueSize.load(std::memory_order_relaxed);
        }
    };

    TUringEnginePtr CreateUringEngine(TIntrusivePtr<TInterconnectProxyCommon> common) {
        if (!TUringContext::IsAvailable()) {
            return nullptr;
        }
        Y_ABORT_UNLESS(common);
        Y_ABORT_UNLESS(common->MonCounters);
        return MakeIntrusive<TUringEngine>(std::move(common));
    }

} // namespace NActors

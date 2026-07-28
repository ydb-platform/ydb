#pragma once

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/event.h>
#include <ydb/library/actors/core/event_load.h>
#include <ydb/library/actors/core/event_pb.h>
#include <ydb/library/actors/wilson/wilson_trace.h>
#include <ydb/library/actors/util/rc_buf.h>

#define XXH_INLINE_ALL
#include <contrib/libs/xxhash/xxhash.h>

#include <deque>

namespace NActorsInterconnect {
    class TSystemPayloadV2;
}

namespace NActors {

    class TEventSerializer {
    public:
#pragma pack(push, 1)
        struct TEventHeader {
            ui32 Type;
            ui32 Flags;
            ui64 Cookie;
            ui64 Checksum; // checksum (optional) for the whole event, including this header (with zero checksum)
            TActorId Sender;
            TActorId Recipient;
            NWilson::TTraceId::TSerializedTraceId TraceId;
        };

        struct TChunkHeader {
            ui16 Length;
            ui16 TypeChannel;

            static constexpr size_t ChannelBits = 12;

            static constexpr ui16 ChannelMask = (1 << ChannelBits) - 1;
            static constexpr ui16 TypeMask = ~ChannelMask;

            static constexpr ui16 SystemChannel = 0x1000;

            enum : ui16 {
                kEventChunk = 0 << ChannelBits,
                kEventHeader = 1 << ChannelBits,
                kSystem = 2 << ChannelBits,
            };

            ui16 GetChannel() const {
                return TypeChannel != kSystem ? TypeChannel & ChannelMask : SystemChannel;
            }

            ui16 GetType() const {
                return TypeChannel & TypeMask;
            }
        };
#pragma pack(pop)

    private:
        const bool Checksumming;

        struct TPerChannelQuota {
            ui16 Channel; // channel number
            ui16 Quota; // quota in bytes to produce
        };
        std::vector<TPerChannelQuota> PerChannelQuotaHeap;
        static constexpr ui16 DefaultQuota = 4096;

        static constexpr size_t MinUsefulQuota = sizeof(TChunkHeader) + sizeof(ui32);

        static_assert(MinUsefulQuota <= DefaultQuota);

        static constexpr size_t NumDefaultChannels = 16;

        enum class ESerializeStage {
            kInitial,
            kBufferSerializer,
            kChunkSerializer,
            kHeader,
        };
        class TEventQueue {
            IEventHandle *First = nullptr;
            IEventHandle *Last = nullptr;

        public:
            ~TEventQueue() {
                while (Peek()) {
                    Pop();
                }
            }

            bool Push(std::unique_ptr<IEventHandle> ev) {
                const bool res = !First; // was this the first one
                ev->NextLinkPtr.store(0, std::memory_order_relaxed);
                if (Last) {
                    Last->NextLinkPtr.store(reinterpret_cast<uintptr_t>(ev.get()), std::memory_order_relaxed);
                } else {
                    First = ev.get();
                }
                Last = ev.release();
                return res;
            }

            IEventHandle *Peek() const {
                return First;
            }

            void Pop() {
                Y_DEBUG_ABORT_UNLESS(Last && First);
                std::unique_ptr<IEventHandle> temp(First);
                First = reinterpret_cast<IEventHandle*>(First->NextLinkPtr.load(std::memory_order_relaxed));
                if (!First) {
                    Last = nullptr;
                }
            }
        };
        struct TPerChannelQueue {
            TEventQueue Events;
            std::deque<TRcBuf> SystemRequests;
            TEventHeader EventHeader;
            size_t EventHeaderOffset = 0;
            TIntrusivePtr<TEventSerializedData> Buffer;
            TRope::TConstIterator Iter;
            TCoroutineChunkSerializer CoroutineChunkSerializer;
            ESerializeStage SerializeStage = ESerializeStage::kInitial;
            TEventSerializationInfo EvSerInfoHolder;
            const TEventSerializationInfo *EvSerInfo;
            ui16 Quota = 0; // must be the same as TPerChannelQuota for this channel
            XXH3_state_t ChecksumState;
        };
        std::array<TPerChannelQueue, NumDefaultChannels> PerChannelQueue;
        THashMap<ui16, TPerChannelQueue> PerChannelQueueMap;
        TPerChannelQueue SystemChannelQueue;

        // Refcounted objects tracking. An event's serialized bytes may be scattered across the output
        // stream (interleaved with other channels) and across several pipelined write batches, so we can
        // only release an event once *all* of its bytes have been sent. We track that with absolute stream
        // offsets: an event's memory is freed once the total number of committed (sent) bytes reaches the
        // stream offset just past the event's last produced byte. Releasing on a plain FIFO byte count is
        // wrong -- committed bytes belonging to one channel would be charged against a different event at
        // the head of the queue, freeing it while a later, still-in-flight batch aliases its memory.
        struct TRefcountItem {
            ui64 EndOffset = 0; // total produced bytes at the moment this event was fully produced
            TIntrusivePtr<TEventSerializedData> Buffer;
            std::unique_ptr<IEventBase> Event;
            TRcBuf Scratch;
            ui64 EventReceivedTimestamp;
        };
        std::deque<TRefcountItem> RefcountItems;
        ui64 CumulativeProduced = 0; // total bytes ever produced into the output stream
        ui64 CumulativeCommitted = 0; // total bytes ever reported as sent via CommitProducedBytes

        ui64 Timestamp;
        ui64 SerializeBufferTime = 0;
        ui64 SerializeEventTime = 0;
        ui64 BytesCopied = 0;
        ui64 BytesAliased = 0;

        const double Freq = 1e9 * NHPTimer::GetSeconds(1);

    public:
        TEventSerializer(bool checksumming);

        void Push(std::unique_ptr<IEventHandle> ev);

        void Push(NActorsInterconnect::TSystemPayloadV2& systemRequest);

        bool IsTrafficPending() const { return !PerChannelQuotaHeap.empty(); }
        bool HasOutOfBandTraffic() const { return !SystemChannelQueue.SystemRequests.empty(); }

        // this function generates output stream for transmission; it returns number of bytes added to output spans
        size_t ProduceOutputStream(TRcBuf& buffer, std::deque<TContiguousSpan> *out, size_t maxBytesToProduce = Max<size_t>());

        // notification issued when N produced bytes have been sent to the other party
        void CommitProducedBytes(size_t numBytes, std::vector<ui64> *eventToWireTime = nullptr);

        void ResetCounters() {
            SerializeBufferTime = 0;
            SerializeEventTime = 0;
            BytesCopied = 0;
            BytesAliased = 0;
        }

        ui64 GetSerializeBufferTime() const { return SerializeBufferTime; }
        ui64 GetSerializeEventTime() const { return SerializeEventTime; }
        ui64 GetBytesCopied() const { return BytesCopied; }
        ui64 GetBytesAliased() const { return BytesAliased; }

    private:
        TPerChannelQueue& GetQueue(ui16 channel) {
            return channel < NumDefaultChannels ? PerChannelQueue[channel] :
                channel == TChunkHeader::SystemChannel ? SystemChannelQueue : PerChannelQueueMap[channel];
        }

        size_t ProduceOutputStreamForQueue(ui16 channel, TPerChannelQueue& queue, size_t maxBytesToProduce, TRcBuf& buffer,
            std::deque<TContiguousSpan> *out, ui64 *bufferProduced);

        ui64 UpdateTimestamp();
    };

    class TEventDeserializer {
        static constexpr size_t NumDefaultChannels = 16;

        using TEventHeader = TEventSerializer::TEventHeader;
        using TChunkHeader = TEventSerializer::TChunkHeader;

        const TScopeId PeerScopeId;

        struct TPerChannelQueue {
            TRope Accum;
            TEventSerializationInfo EvSerInfo;
            TEventHeader EventHeader;
            size_t EventHeaderOffset = 0;
        };
        std::array<TPerChannelQueue, NumDefaultChannels> PerChannelQueue;
        THashMap<ui16, TPerChannelQueue> PerChannelQueueMap;

        TRope Accum;

    public:
        struct IEventProcessor {
            virtual ~IEventProcessor() = default;
            virtual void PushEvent(std::unique_ptr<IEventHandle> ev) = 0;
            virtual void Process(NActorsInterconnect::TSystemPayloadV2& systemRequest) = 0;
        };

    public:
        TEventDeserializer(TScopeId peerScopeId);
        void Push(TRcBuf buffer, IEventProcessor *eventProcessor, TActorId sessionId);

    private:
        TPerChannelQueue& GetQueue(ui16 channel) {
            return channel < NumDefaultChannels ? PerChannelQueue[channel] : PerChannelQueueMap[channel];
        }
    };

} // NActors

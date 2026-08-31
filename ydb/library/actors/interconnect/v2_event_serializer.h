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

    // Allocate a section buffer with the requested headroom/tailroom, aligning the payload pointer
    // (TRcBuf::GetData()) to `alignment` when alignment > 1. Used by the v2 XDC receive path so
    // GetPayloadWithHeader can take the reserved-headroom fast path.
    TRcBuf AllocateXdcSectionBuffer(size_t size, size_t headroom, size_t tailroom, size_t alignment);

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
                kXdcDeclare = 3 << ChannelBits,
                kXdcPush = 4 << ChannelBits,
            };

            ui16 GetChannel() const {
                return TypeChannel != kSystem ? TypeChannel & ChannelMask : SystemChannel;
            }

            ui16 GetType() const {
                return TypeChannel & TypeMask;
            }
        };

        struct TXdcSection {
            ui32 Headroom = 0;
            ui32 Size = 0;
            ui32 Tailroom = 0;
            ui32 Alignment = 0;
            ui8 Flags = 0;

            static constexpr ui8 FlagInline = 1;
        };
#pragma pack(pop)

        static_assert(sizeof(TXdcSection) == 17);

        static constexpr size_t XdcPushFraming = sizeof(TChunkHeader) + sizeof(ui16);

    private:
        const bool Checksumming;
        const bool UseExternalDataChannel;

        struct TPerChannelQuota {
            ui16 Channel; // channel number
            ui16 Quota; // quota in bytes to produce
        };
        std::vector<TPerChannelQuota> PerChannelQuotaHeap;
        static constexpr ui16 DefaultQuota = 4096;

        // Must cover a DECLARE record (header + one TXdcSection). A smaller floor lets a channel
        // sit at the front of the quota heap unable to emit DECLARE, which stalls every channel.
        static constexpr size_t MinUsefulQuota = sizeof(TChunkHeader) + sizeof(TXdcSection);

        static_assert(MinUsefulQuota <= DefaultQuota);

        static constexpr size_t NumDefaultChannels = 16;

        enum class ESerializeStage {
            kInitial,
            kXdcDeclare,
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
            size_t SerializedBytesPending = 0;
            size_t EventHeaderOffset = 0;
            TIntrusivePtr<TEventSerializedData> Buffer;
            TRope::TConstIterator Iter;
            TCoroutineChunkSerializer CoroutineChunkSerializer;
            ESerializeStage SerializeStage = ESerializeStage::kInitial;
            TEventSerializationInfo EvSerInfoHolder;
            const TEventSerializationInfo *EvSerInfo;
            ui16 Quota = 0; // must be the same as TPerChannelQuota for this channel
            XXH3_state_t ChecksumState;

            bool UseXdcForEvent = false;
            bool CurrentIsInline = true;
            size_t SectionIndex = 0;
            size_t SectionBytesRemain = 0;
            size_t XdcDeclareIndex = 0;
        };
        std::array<TPerChannelQueue, NumDefaultChannels> PerChannelQueue;
        THashMap<ui16, TPerChannelQueue> PerChannelQueueMap;
        TPerChannelQueue SystemChannelQueue;

        // Refcounted objects tracking. An event's serialized bytes may be scattered across the output
        // stream (interleaved with other channels) and across several pipelined write batches, so we can
        // only release an event once *all* of its bytes have been sent. We track that with absolute stream
        // offsets: an event's memory is freed once the total number of committed (sent) bytes on *both*
        // the main and XDC streams reach the offsets just past the event's last produced byte on each.
        // Releasing on a plain FIFO byte count is wrong -- committed bytes belonging to one channel would
        // be charged against a different event at the head of the queue, freeing it while a later,
        // still-in-flight batch aliases its memory.
        struct TRefcountItem {
            ui64 MainEndOffset = 0;
            ui64 XdcEndOffset = 0;
            TIntrusivePtr<TEventSerializedData> Buffer;
            std::unique_ptr<IEventBase> Event;
            TRcBuf Scratch;
            size_t ScratchBytesUsed = 0;
            ui64 EventReceivedTimestamp = 0;
            std::vector<y_absl::Cord> Cords; // keeping ownership of the following cords referring the data
        };
        std::deque<TRefcountItem> RefcountItems;
        size_t NumBytesInScratchBuffers = 0;
        ui64 CumulativeProducedMain = 0;
        ui64 CumulativeProducedXdc = 0;
        ui64 CumulativeCommittedMain = 0;
        ui64 CumulativeCommittedXdc = 0;

        ui64 Timestamp = 0;
        ui64 SerializeEventTime = 0;
        ui64 BytesCopied = 0;
        ui64 BytesAliased = 0;

    public:
        TEventSerializer(bool checksumming, bool useExternalDataChannel = false);

        void Push(std::unique_ptr<IEventHandle> ev);

        void Push(NActorsInterconnect::TSystemPayloadV2& systemRequest);

        bool IsTrafficPending() const { return !PerChannelQuotaHeap.empty(); }
        bool HasOutOfBandTraffic() const { return !SystemChannelQueue.SystemRequests.empty(); }

        // Generates output for transmission. Returns total bytes added to main and (if provided) XDC spans.
        // Quota is charged for both media so logical channels stay fair regardless of which socket carries
        // the payload. xdcBuffer/xdcOut may be null when XDC is not in use.
        size_t ProduceOutputStream(TRcBuf& buffer, std::vector<TContiguousSpan> *out,
            size_t maxBytesToProduce = Max<size_t>());
        size_t ProduceOutputStream(TRcBuf& buffer, std::vector<TContiguousSpan> *out,
            TRcBuf *xdcBuffer, std::vector<TContiguousSpan> *xdcOut,
            size_t maxBytesToProduce = Max<size_t>());

        // Notification issued when produced bytes have been sent. Pass XDC bytes as the second argument
        // when that socket completed a write; existing single-stream callers can omit it (defaults to 0).
        void CommitProducedBytes(size_t numMainBytes, size_t numXdcBytes = 0,
            std::vector<ui64> *eventToWireTime = nullptr,
            std::vector<std::unique_ptr<IEventBase>> *events = nullptr,
            std::vector<TIntrusivePtr<TEventSerializedData>> *buffers = nullptr);

        void ResetCounters() {
            SerializeEventTime = 0;
            BytesCopied = 0;
            BytesAliased = 0;
        }

        ui64 GetSerializeEventTime() const { return SerializeEventTime; }
        ui64 GetBytesCopied() const { return BytesCopied; }
        ui64 GetBytesAliased() const { return BytesAliased; }

        size_t GetNumBytesInScratchBuffers() const { return NumBytesInScratchBuffers; }

        ui64 GetCumulativeProducedMain() const { return CumulativeProducedMain; }
        ui64 GetCumulativeProducedXdc() const { return CumulativeProducedXdc; }

    private:
        TPerChannelQueue& GetQueue(ui16 channel) {
            return channel < NumDefaultChannels ? PerChannelQueue[channel] :
                channel == TChunkHeader::SystemChannel ? SystemChannelQueue : PerChannelQueueMap[channel];
        }

        struct TStreamState {
            TMutableContiguousSpan Buffer;
            TContiguousSpan BufferOrig;
            std::vector<TContiguousSpan> *Out = nullptr;
            const char *LastSpanEnd = nullptr;
            ui64 *CumulativeProduced = nullptr;
            ui64 *BufferProduced = nullptr;
            bool Enabled = false;
        };

        size_t ProduceOutputStreamForQueue(ui16 channel, TPerChannelQueue& queue, size_t maxBytesToProduce,
            TStreamState& main, TStreamState *xdc);

        ui64 UpdateTimestamp();
        static bool HasExternalSections(const TEventSerializationInfo *info);
        static void EnsureSection(TPerChannelQueue& queue);
        void ResetEventState(TPerChannelQueue& queue);
    };

    class TEventDeserializer {
        static constexpr size_t NumDefaultChannels = 16;

        using TEventHeader = TEventSerializer::TEventHeader;
        using TChunkHeader = TEventSerializer::TChunkHeader;
        using TXdcSection = TEventSerializer::TXdcSection;

        const TScopeId PeerScopeId;

        struct TPendingEvent {
            TRope InternalPayload;
            TRope ExternalPayload;
            TEventSerializationInfo EvSerInfo;
            TEventHeader EventHeader;
            size_t EventHeaderOffset = 0;
            size_t XdcSizeLeft = 0;
            std::deque<TMutableContiguousSpan> XdcBuffers;
            bool HeaderComplete = false;
            bool Declared = false;
        };

        struct TPerChannelQueue {
            std::deque<TPendingEvent> Pending;
        };
        std::array<TPerChannelQueue, NumDefaultChannels> PerChannelQueue;
        THashMap<ui16, TPerChannelQueue> PerChannelQueueMap;

        TRope Accum;

        struct TXdcInputItem {
            ui16 Channel = 0;
            TMutableContiguousSpan Span;
        };
        std::deque<TXdcInputItem> XdcInputQ;

    public:
        struct IEventProcessor {
            virtual ~IEventProcessor() = default;
            virtual void PushEvent(std::unique_ptr<IEventHandle> ev) = 0;
            virtual void Process(NActorsInterconnect::TSystemPayloadV2& systemRequest) = 0;
        };

    public:
        TEventDeserializer(TScopeId peerScopeId);
        void Push(TRcBuf buffer, IEventProcessor *eventProcessor, TActorId sessionId);

        bool HasXdcReadPending() const { return !XdcInputQ.empty(); }
        size_t PrepareXdcReadv(TIoVec *iov, size_t maxIov, size_t maxBytes);
        void CommitXdcBytes(size_t n, IEventProcessor *eventProcessor, TActorId sessionId);

    private:
        TPerChannelQueue& GetQueue(ui16 channel) {
            return channel < NumDefaultChannels ? PerChannelQueue[channel] : PerChannelQueueMap[channel];
        }

        TPendingEvent& CurrentEvent(TPerChannelQueue& queue);
        void ApplyXdcDeclare(TPendingEvent& ev, const char *data, size_t length);
        void ApplyXdcPush(ui16 channel, TPendingEvent& ev, ui16 nbytes);
        void TryDeliver(TPerChannelQueue& queue, IEventProcessor *eventProcessor, TActorId sessionId);
        static TRope Unshuffle(TPendingEvent& ev);
    };

} // NActors

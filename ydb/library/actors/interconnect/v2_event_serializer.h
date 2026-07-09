#pragma once

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/event.h>
#include <ydb/library/actors/core/event_load.h>
#include <ydb/library/actors/core/event_pb.h>
#include <ydb/library/actors/wilson/wilson_trace.h>
#include <ydb/library/actors/util/rc_buf.h>

#include <deque>

namespace NActors {

    class TEventSerializer {
    public:
#pragma pack(push, 1)
        struct TEventHeader {
            ui32 Type;
            ui32 Flags;
            ui64 Cookie;
            TActorId Sender;
            TActorId Recipient;
            NWilson::TTraceId::TSerializedTraceId TraceId;
        };

        struct TChunkHeader {
            ui16 TypeLength;
            ui16 Channel;

            static constexpr ui16 TypeMask = 0xc000;
            static constexpr ui16 LengthMask = 0x3fff;

            enum : ui16 {
                kEventChunk = 0x0000,
                kEventHeader = 0x4000,
            };
        };
#pragma pack(pop)

    private:
        struct TPerChannelQuota {
            ui16 Channel; // channel number
            ui16 Quota; // quota in bytes to produce
        };
        std::vector<TPerChannelQuota> PerChannelQuotaHeap;
        static constexpr ui16 DefaultQuota = 4096;

        static constexpr size_t NumDefaultChannels = 16;

        enum class ESerializeStage {
            kInitial,
            kBufferSerializer,
            kChunkSerializer,
            kHeader,
        };
        struct TPerChannelQueue {
            std::deque<std::unique_ptr<IEventHandle>> Events;
            TEventHeader EventHeader;
            size_t EventHeaderOffset = 0;
            TIntrusivePtr<TEventSerializedData> Buffer;
            TRope::TConstIterator Iter;
            TCoroutineChunkSerializer CoroutineChunkSerializer;
            ESerializeStage SerializeStage = ESerializeStage::kInitial;
            TEventSerializationInfo EvSerInfoHolder;
            const TEventSerializationInfo *EvSerInfo;
            size_t EventProducedSize = 0;
            ui16 Quota = 0; // must be the same as TPerChannelQuota for this channel
        };
        std::array<TPerChannelQueue, NumDefaultChannels> PerChannelQueue;
        THashMap<ui16, TPerChannelQueue> PerChannelQueueMap;

        // refcounted objects tracking
        struct TRefcountItem {
            size_t NumBytesRemaining = 0;
            TIntrusivePtr<TEventSerializedData> Buffer;
            std::unique_ptr<IEventBase> Event;
        };
        std::deque<TRefcountItem> RefcountItems;
        size_t OverproducedBytes = 0;

    public:
        void Push(std::unique_ptr<IEventHandle> ev);

        // this function generates output stream for transmission; it returns number of bytes added to output spans
        size_t ProduceOutputStream(TMutableContiguousSpan *buffer, std::vector<TContiguousSpan> *out);

        // notification issued when N produced bytes have been sent to the other party
        void CommitProducedBytes(size_t numBytes);

    private:
        TPerChannelQueue& GetQueue(ui16 channel) {
            return channel < NumDefaultChannels ? PerChannelQueue[channel] : PerChannelQueueMap[channel];
        }

        size_t ProduceOutputStreamForQueue(TPerChannelQueue& queue, size_t maxBytesToProduce,
            TMutableContiguousSpan *buffer, std::vector<TContiguousSpan> *out);
    };

    class TEventDeserializer {
        static constexpr size_t NumDefaultChannels = 16;

        struct TPerChannelQueue {
            TRope Accum;
            TEventSerializationInfo EvSerInfo;
        };
        std::array<TPerChannelQueue, NumDefaultChannels> PerChannelQueue;
        THashMap<ui16, TPerChannelQueue> PerChannelQueueMap;

        TRope Accum;

        using TEventHeader = TEventSerializer::TEventHeader;
        using TChunkHeader = TEventSerializer::TChunkHeader;

    public:
        struct IEventProcessor {
            virtual ~IEventProcessor() = default;
            virtual void PushEvent(std::unique_ptr<IEventHandle> ev) = 0;
        };

    public:
        void Push(TRcBuf buffer, IEventProcessor *eventProcessor);

    private:
        TPerChannelQueue& GetQueue(ui16 channel) {
            return channel < NumDefaultChannels ? PerChannelQueue[channel] : PerChannelQueueMap[channel];
        }
    };

} // NActors

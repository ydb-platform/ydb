#pragma once

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/event_load.h>
#include <ydb/library/actors/util/rope.h>
#include <util/generic/deque.h>
#include <util/generic/vector.h>
#include <util/generic/map.h>
#include <util/stream/walk.h>
#include <ydb/library/actors/wilson/wilson_span.h>

#include "interconnect_common.h"
#include "interconnect_counters.h"
#include "packet.h"
#include "event_holder_pool.h"

namespace NInterconnect {
    class IZcGuard;
}

namespace NActors {
#pragma pack(push, 1)

    struct TChannelPart {
        ui16 ChannelFlags;
        ui16 Size;

        static constexpr ui16 LastPartFlag = 0x8000;
        static constexpr ui16 XdcFlag = 0x4000;
        static constexpr ui16 ChannelMask = (1 << IEventHandle::ChannelBits) - 1;

        static_assert((LastPartFlag & ChannelMask) == 0);
        static_assert((XdcFlag & ChannelMask) == 0);

        ui16 GetChannel() const { return ChannelFlags & ChannelMask; }
        bool IsLastPart() const { return ChannelFlags & LastPartFlag; }
        bool IsXdc() const { return ChannelFlags & XdcFlag; }

        TString ToString() const {
            return TStringBuilder() << "{Channel# " << GetChannel()
                << " IsLastPart# " << IsLastPart()
                << " IsXdc# " << IsXdc()
                << " Size# " << Size << "}";
        }
    };

#pragma pack(pop)

    enum class EXdcCommand : ui8 {
        DECLARE_SECTION = 1,
        PUSH_DATA,
        DECLARE_SECTION_INLINE,
    };

    struct TExSerializedEventTooLarge : std::exception {
        const ui32 Type;

        TExSerializedEventTooLarge(ui32 type)
            : Type(type)
        {}
    };

    class TEventOutputChannel : public TInterconnectLoggingBase {
    public:
        TEventOutputChannel(ui16 id, ui32 peerNodeId, ui32 maxSerializedEventSize,
                std::shared_ptr<IInterconnectMetrics> metrics, TSessionParams params)
            : TInterconnectLoggingBase(Sprintf("OutputChannel %" PRIu16 " [node %" PRIu32 "]", id, peerNodeId))
            , PeerNodeId(peerNodeId)
            , ChannelId(id)
            , Metrics(std::move(metrics))
            , Params(std::move(params))
            , MaxSerializedEventSize(maxSerializedEventSize)
        {}

        ~TEventOutputChannel() {
        }

        std::pair<ui32, TEventHolder*> Push(IEventHandle& ev, TEventHolderPool& pool) {
            TEventHolder& event = pool.Allocate(Queue);
            const ui32 bytes = event.Fill(ev) + sizeof(TEventDescr2);
            OutputQueueSize += bytes;
            if (event.Span = NWilson::TSpan(15 /*max verbosity*/, NWilson::TTraceId(ev.TraceId), "Interconnect.Queue")) {
                event.Span
                    .Attribute("OutputQueueItems", static_cast<i64>(Queue.size()))
                    .Attribute("OutputQueueSize", static_cast<i64>(OutputQueueSize));
            }
            return std::make_pair(bytes, &event);
        }

        void DropConfirmed(ui64 confirm, TEventHolderPool& pool);

        bool FeedBuf(TTcpPacketOutTask& task, ui64 serial);

        bool IsEmpty() const {
            return Queue.empty();
        }

        bool IsWorking() const {
            return !IsEmpty();
        }

        ui32 GetQueueSize() const {
            return (ui32)Queue.size();
        }

        ui64 GetBufferedAmountOfData() const {
            return OutputQueueSize;
        }

        void ProcessUndelivered(TEventHolderPool& pool, NInterconnect::IZcGuard* zg);

        const ui32 PeerNodeId;
        const ui16 ChannelId;
        std::shared_ptr<IInterconnectMetrics> Metrics;
        const TSessionParams Params;
        const ui32 MaxSerializedEventSize;
        ui64 UnaccountedTraffic = 0;
        ui64 EqualizeCounterOnPause = 0;
        ui64 WeightConsumedOnPause = 0;

        enum class EState {
            INITIAL,
            BODY,
            DESCRIPTOR,
            SECTIONS,
        };
        EState State = EState::INITIAL;

    protected:
        ui64 OutputQueueSize = 0;

        std::list<TEventHolder> Queue;
        std::list<TEventHolder> NotYetConfirmed;
        TRope::TConstIterator Iter;
        TCoroutineChunkSerializer Chunker;
        TEventSerializationInfo SerializationInfoContainer;
        const TEventSerializationInfo *SerializationInfo = nullptr;
        bool IsPartInline = false;
        size_t PartLenRemain = 0;
        size_t SectionIndex = 0;
        std::vector<char> XdcData;

        template<bool External>
        bool SerializeEvent(TTcpPacketOutTask& task, TEventHolder& event, size_t *bytesSerialized);

        bool FeedPayload(TTcpPacketOutTask& task, TEventHolder& event);
        std::optional<bool> FeedInlinePayload(TTcpPacketOutTask& task, TEventHolder& event);
        std::optional<bool> FeedExternalPayload(TTcpPacketOutTask& task, TEventHolder& event);

        bool FeedDescriptor(TTcpPacketOutTask& task, TEventHolder& event);

        void AccountTraffic() {
            if (const ui64 amount = std::exchange(UnaccountedTraffic, 0)) {
                Metrics->UpdateOutputChannelTraffic(ChannelId, amount);
            }
        }

        friend class TInterconnectSessionTCP;
    };
}

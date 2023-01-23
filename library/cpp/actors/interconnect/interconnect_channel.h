#pragma once

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/actors/core/actorsystem.h>
#include <library/cpp/actors/core/event_load.h>
#include <library/cpp/actors/util/rope.h>
#include <util/generic/deque.h>
#include <util/generic/vector.h>
#include <util/generic/map.h>
#include <util/stream/walk.h>
#include <library/cpp/actors/wilson/wilson_span.h>

#include "interconnect_common.h"
#include "interconnect_counters.h"
#include "packet.h"
#include "event_holder_pool.h"

namespace NActors {
#pragma pack(push, 1)
    struct TChannelPart {
        ui16 Channel;
        ui16 Size;

        static constexpr ui16 LastPartFlag = ui16(1) << 15;

        TString ToString() const {
            return TStringBuilder() << "{Channel# " << (Channel & ~LastPartFlag)
                << " LastPartFlag# " << ((Channel & LastPartFlag) ? "true" : "false")
                << " Size# " << Size << "}";
        }
    };
#pragma pack(pop)

    struct TExSerializedEventTooLarge : std::exception {
        const ui32 Type;

        TExSerializedEventTooLarge(ui32 type)
            : Type(type)
        {}
    };

    class TEventOutputChannel : public TInterconnectLoggingBase {
    public:
        TEventOutputChannel(TEventHolderPool& pool, ui16 id, ui32 peerNodeId, ui32 maxSerializedEventSize,
                std::shared_ptr<IInterconnectMetrics> metrics, TSessionParams params)
            : TInterconnectLoggingBase(Sprintf("OutputChannel %" PRIu16 " [node %" PRIu32 "]", id, peerNodeId))
            , Pool(pool)
            , PeerNodeId(peerNodeId)
            , ChannelId(id)
            , Metrics(std::move(metrics))
            , Params(std::move(params))
            , MaxSerializedEventSize(maxSerializedEventSize)
        {}

        ~TEventOutputChannel() {
        }

        std::pair<ui32, TEventHolder*> Push(IEventHandle& ev) {
            TEventHolder& event = Pool.Allocate(Queue);
            const ui32 bytes = event.Fill(ev) + (Params.UseExtendedTraceFmt ? sizeof(TEventDescr2) : sizeof(TEventDescr1));
            OutputQueueSize += bytes;
            if (event.Span = NWilson::TSpan(15 /*max verbosity*/, NWilson::TTraceId(ev.TraceId), "Interconnect.Queue")) {
                event.Span
                    .Attribute("OutputQueueItems", static_cast<i64>(Queue.size()))
                    .Attribute("OutputQueueSize", static_cast<i64>(OutputQueueSize));
            }
            return std::make_pair(bytes, &event);
        }

        void DropConfirmed(ui64 confirm);

        bool FeedBuf(TTcpPacketOutTask& task, ui64 serial, ui64 *weightConsumed);

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

        void NotifyUndelivered();

        TEventHolderPool& Pool;
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
            CHUNKER,
            BUFFER,
            DESCRIPTOR,
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

        bool FeedDescriptor(TTcpPacketOutTask& task, TEventHolder& event, ui64 *weightConsumed);

        void AccountTraffic() {
            if (const ui64 amount = std::exchange(UnaccountedTraffic, 0)) {
                Metrics->UpdateOutputChannelTraffic(ChannelId, amount);
            }
        }

        friend class TInterconnectSessionTCP;
    };
}

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_pb.h>
#include <ydb/library/actors/interconnect/v2_event_serializer.h>
#include <ydb/library/actors/protos/unittests.pb.h>

#include <util/string/cast.h>

using namespace NActors;

struct TEvPrivate {
    enum {
        EvTest = EventSpaceBegin(TEvents::ES_PRIVATE),
    };

    struct TEvProto : public TEventPB<TEvProto, TMessageWithPayload, EvTest> {};
};

struct TEventProcessor : TEventDeserializer::IEventProcessor {
    std::deque<std::unique_ptr<IEventHandle>> Events;

    void PushEvent(std::unique_ptr<IEventHandle> event) override {
        Events.push_back(std::move(event));
    }
};

// deterministic payload for the event with the given index, so that the receiver can validate consistency
TString MakeSomeData(ui64 index, size_t len) {
    return TString(len, static_cast<char>('A' + index % 26));
}

TString MakePayloadData(ui64 index, size_t len) {
    return TString(len, static_cast<char>('a' + index % 26));
}

std::unique_ptr<IEventHandle> MakeIndexedEvent(ui16 channel, ui64 index, size_t dataLen, bool withPayload) {
    auto ev = std::make_unique<TEvPrivate::TEvProto>();
    ev->Record.SetMeta(ToString(index));
    ev->Record.AddSomeData(MakeSomeData(index, dataLen));
    if (withPayload) {
        ev->AddPayload(TRope(MakePayloadData(index, dataLen)));
    }
    TActorId sender(1, 2, 3, static_cast<ui32>(index));
    TActorId recipient(2, 3, 4, 5);
    // the cookie carries the event index so we can restore delivery order on the receiving side
    return std::make_unique<IEventHandle>(recipient, sender, ev.release(),
        IEventHandle::MakeFlags(channel, 0), index /*cookie*/);
}

void CheckIndexedEvent(IEventHandle& outEv, ui16 channel, size_t dataLen, bool withPayload) {
    const ui64 index = outEv.Cookie;
    UNIT_ASSERT_VALUES_EQUAL(outEv.GetChannel(), channel);
    UNIT_ASSERT_VALUES_EQUAL(outEv.Type, static_cast<ui32>(TEvPrivate::EvTest));
    UNIT_ASSERT(outEv.Sender == TActorId(1, 2, 3, static_cast<ui32>(index)));
    UNIT_ASSERT(outEv.Recipient == TActorId(2, 3, 4, 5));
    auto& out = *outEv.Get<TEvPrivate::TEvProto>();
    UNIT_ASSERT(out.Record.HasMeta());
    UNIT_ASSERT_VALUES_EQUAL(out.Record.GetMeta(), ToString(index));
    UNIT_ASSERT_VALUES_EQUAL(out.Record.SomeDataSize(), 1);
    UNIT_ASSERT_VALUES_EQUAL(out.Record.GetSomeData(0), MakeSomeData(index, dataLen));
    UNIT_ASSERT_VALUES_EQUAL(out.GetPayloadCount(), withPayload ? 1 : 0);
    if (withPayload) {
        UNIT_ASSERT_VALUES_EQUAL(out.GetPayload(0).ConvertToString(), MakePayloadData(index, dataLen));
    }
}

std::vector<TContiguousSpan> Serialize(TEventSerializer& ser, std::vector<TRcBuf>& bufs) {
    std::vector<TContiguousSpan> spans;

    for (;;) {
        if (bufs.empty() || bufs.back().size() < 1024) {
            bufs.push_back(TRcBuf::Uninitialized(65536));
        }
        TRcBuf& buffer = bufs.back();

        TMutableContiguousSpan bufferSpan = buffer.UnsafeGetContiguousSpanMut();
        const size_t produced = ser.ProduceOutputStream(&bufferSpan, &spans);
        UNIT_ASSERT_EQUAL(bufferSpan.data() + bufferSpan.size(), buffer.data() + buffer.size());
        buffer.TrimFront(bufferSpan.size());
        if (!produced) {
            break;
        }
    }

    return spans;
}

void CheckSerializeThenDeserialize(bool withPayload, bool buffer, ui32 metaLength) {
    auto ev = std::make_unique<TEvPrivate::TEvProto>();
    ev->Record.SetMeta(TString(metaLength, 'X'));
    ev->Record.AddSomeData("world");
    TActorId sender(1, 2, 3, 4);
    TActorId recipient(2, 3, 4, 5);
    ui64 cookie = 12387134;
    if (withPayload) {
        ev->AddPayload(TRope(TString("hello, world")));
    }
    auto h = std::make_unique<IEventHandle>(recipient, sender, ev.release(), 0, cookie);
    if (buffer) {
        h = std::make_unique<IEventHandle>(h->Type, h->Flags, h->Recipient, h->Sender, h->ReleaseChainBuffer(),
            h->Cookie, nullptr, std::move(h->TraceId));
    }

    TEventSerializer ser;
    ser.Push(std::move(h));

    std::vector<TRcBuf> bufs;
    std::vector<TContiguousSpan> spans = Serialize(ser, bufs);

    TEventDeserializer deser;
    TEventProcessor processor;
    for (TContiguousSpan span : spans) {
        UNIT_ASSERT(processor.Events.empty());
        deser.Push(TRcBuf::Copy(span), &processor);
    }
    UNIT_ASSERT(processor.Events.size() == 1);
    auto& outEv = *processor.Events.front();
    UNIT_ASSERT(outEv.Sender == sender);
    UNIT_ASSERT(outEv.Recipient == recipient);
    UNIT_ASSERT(outEv.Cookie == cookie);
    UNIT_ASSERT(outEv.Type == TEvPrivate::EvTest);
    auto& out = *outEv.Get<TEvPrivate::TEvProto>();
    UNIT_ASSERT(out.Record.HasMeta());
    UNIT_ASSERT_VALUES_EQUAL(out.Record.GetMeta(), TString(metaLength, 'X'));
    UNIT_ASSERT_VALUES_EQUAL(out.Record.SomeDataSize(), 1);
    UNIT_ASSERT_VALUES_EQUAL(out.Record.GetSomeData(0), "world");
    UNIT_ASSERT_VALUES_EQUAL(out.GetPayloadCount(), withPayload ? 1 : 0);
    if (withPayload) {
        UNIT_ASSERT_VALUES_EQUAL(out.GetPayload(0).ConvertToString(), "hello, world");
    }
}

Y_UNIT_TEST_SUITE(EventSerializerV2) {

    Y_UNIT_TEST(CheckSerializeThenDeserializeProtoWithoutPayload) {
        CheckSerializeThenDeserialize(false, false, 10);
    }

    Y_UNIT_TEST(CheckSerializeThenDeserializeProtoWithPayload) {
        CheckSerializeThenDeserialize(true, false, 10);
    }

    Y_UNIT_TEST(CheckSerializeThenDeserializeBufferWithoutPayload) {
        CheckSerializeThenDeserialize(false, true, 10);
    }

    Y_UNIT_TEST(CheckSerializeThenDeserializeBufferWithPayload) {
        CheckSerializeThenDeserialize(true, true, 10);
    }

    Y_UNIT_TEST(CheckSerializeThenDeserializeProtoWithoutPayloadLong) {
        CheckSerializeThenDeserialize(false, false, 1000);
    }

    Y_UNIT_TEST(CheckSerializeThenDeserializeProtoWithPayloadLong) {
        CheckSerializeThenDeserialize(true, false, 1000);
    }

    Y_UNIT_TEST(CheckSerializeThenDeserializeBufferWithoutPayloadLong) {
        CheckSerializeThenDeserialize(false, true, 1000);
    }

    Y_UNIT_TEST(CheckSerializeThenDeserializeBufferWithPayloadLong) {
        CheckSerializeThenDeserialize(true, true, 1000);
    }

    // Push a lot of events into one channel and only a few into another one, then serialize the whole thing into a
    // stream and deserialize it back. Verifies that:
    //   * every event is delivered exactly once, in FIFO order within each channel;
    //   * every event survives the round-trip unchanged (consistency);
    //   * the flooded channel does not starve the other one -- its few events are delivered early instead of being
    //     stuck behind the whole flood (equal bandwidth distribution).
    Y_UNIT_TEST(FairBandwidthDistributionAcrossChannels) {
        constexpr ui16 floodChannel = 3;
        constexpr ui16 otherChannel = 7;
        constexpr size_t numFlood = 1000;
        constexpr size_t numOther = 30;
        constexpr size_t floodDataLen = 512;
        constexpr size_t otherDataLen = 8;

        TEventSerializer ser;
        for (ui64 i = 0; i < numFlood; ++i) {
            ser.Push(MakeIndexedEvent(floodChannel, i, floodDataLen, true /*withPayload*/));
        }
        for (ui64 i = 0; i < numOther; ++i) {
            ser.Push(MakeIndexedEvent(otherChannel, i, otherDataLen, false /*withPayload*/));
        }

        std::vector<TRcBuf> bufs;
        std::vector<TContiguousSpan> spans = Serialize(ser, bufs);

        // feed the produced stream to the deserializer chunk by chunk and record the exact delivery order
        TEventDeserializer deser;
        TEventProcessor processor;

        struct TDelivered {
            ui16 Channel;
            ui64 Cookie;
        };
        std::vector<TDelivered> delivered;

        for (TContiguousSpan span : spans) {
            deser.Push(TRcBuf::Copy(span), &processor);
            while (!processor.Events.empty()) {
                auto ev = std::move(processor.Events.front());
                processor.Events.pop_front();
                const ui16 channel = ev->GetChannel();
                const bool withPayload = channel == floodChannel;
                const size_t dataLen = channel == floodChannel ? floodDataLen : otherDataLen;
                CheckIndexedEvent(*ev, channel, dataLen, withPayload);
                delivered.push_back({channel, ev->Cookie});
            }
        }

        // every event must be delivered exactly once, preserving FIFO order within each channel
        UNIT_ASSERT_VALUES_EQUAL(delivered.size(), numFlood + numOther);
        ui64 nextFlood = 0;
        ui64 nextOther = 0;
        size_t lastOtherPos = 0;
        size_t floodBeforeLastOther = 0;
        for (size_t pos = 0; pos < delivered.size(); ++pos) {
            const TDelivered& d = delivered[pos];
            if (d.Channel == floodChannel) {
                UNIT_ASSERT_VALUES_EQUAL(d.Cookie, nextFlood++);
            } else {
                UNIT_ASSERT_VALUES_EQUAL(d.Channel, otherChannel);
                UNIT_ASSERT_VALUES_EQUAL(d.Cookie, nextOther++);
                lastOtherPos = pos;
                floodBeforeLastOther = nextFlood;
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(nextFlood, numFlood);
        UNIT_ASSERT_VALUES_EQUAL(nextOther, numOther);

        // fairness: by the time the last small-channel event is delivered, only a small fraction of the flood must have
        // been delivered -- otherwise the flood would be blocking the other channel's receivers
        Cerr << "lastOtherPos# " << lastOtherPos << " floodBeforeLastOther# " << floodBeforeLastOther << Endl;
        UNIT_ASSERT_C(floodBeforeLastOther < numFlood / 4,
            "the flooded channel blocks the other one: floodBeforeLastOther# " << floodBeforeLastOther);
    }

}

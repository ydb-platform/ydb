#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_pb.h>
#include <ydb/library/actors/interconnect/v2_event_serializer.h>
#include <ydb/library/actors/protos/unittests.pb.h>

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
        buffer.TrimFront(bufferSpan.data() - buffer.data());
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
    Cerr << "Buffer.size# " << outEv.GetChainBuffer()->GetSize() << Endl;
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

}

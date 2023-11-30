#include "event_pb.h"
#include "events.h"

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/actors/protos/unittests.pb.h>

using namespace NActors;

enum {
    EvMessageWithPayload = EventSpaceBegin(TEvents::ES_PRIVATE),
    EvArenaMessage,
    EvArenaMessageBig,
    EvMessageWithPayloadPreSerialized
};

struct TEvMessageWithPayload : TEventPB<TEvMessageWithPayload, TMessageWithPayload, EvMessageWithPayload> {
    TEvMessageWithPayload() = default;
    explicit TEvMessageWithPayload(const TMessageWithPayload& p)
        : TEventPB<TEvMessageWithPayload, TMessageWithPayload, EvMessageWithPayload>(p)
    {}
};

struct TEvMessageWithPayloadPreSerialized : TEventPreSerializedPB<TEvMessageWithPayloadPreSerialized, TMessageWithPayload, EvMessageWithPayloadPreSerialized> {
};


TRope MakeStringRope(const TString& message) {
    return message ? TRope(message) : TRope();
}

TString MakeString(size_t len) {
    TString res;
    for (size_t i = 0; i < len; ++i) {
        res += RandomNumber<char>();
    }
    return res;
}

Y_UNIT_TEST_SUITE(TEventProtoWithPayload) {

    template <class TEventFrom, class TEventTo>
    void TestSerializeDeserialize(size_t size1, size_t size2) {
        static_assert(TEventFrom::EventType == TEventTo::EventType, "Must be same event type");

        TEventFrom msg;
        msg.Record.SetMeta("hello, world!");
        msg.Record.AddPayloadId(msg.AddPayload(MakeStringRope(MakeString(size1))));
        msg.Record.AddPayloadId(msg.AddPayload(MakeStringRope(MakeString(size2))));
        msg.Record.AddSomeData(MakeString((size1 + size2) % 50 + 11));

        auto serializer = MakeHolder<TAllocChunkSerializer>();
        msg.SerializeToArcadiaStream(serializer.Get());
        auto buffers = serializer->Release(msg.CreateSerializationInfo());
        UNIT_ASSERT_VALUES_EQUAL(buffers->GetSize(), msg.CalculateSerializedSize());
        TString ser = buffers->GetString();

        TString chunkerRes;
        TCoroutineChunkSerializer chunker;
        chunker.SetSerializingEvent(&msg);
        while (!chunker.IsComplete()) {
            char buffer[4096];
            auto range = chunker.FeedBuf(buffer, sizeof(buffer));
            for (auto [data, size] : range) {
                chunkerRes += TString(data, size);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(chunkerRes, ser);

        THolder<IEventBase> ev2 = THolder(TEventTo::Load(buffers.Get()));
        TEventTo& msg2 = static_cast<TEventTo&>(*ev2);
        UNIT_ASSERT_VALUES_EQUAL(msg2.Record.GetMeta(), msg.Record.GetMeta());
        UNIT_ASSERT_EQUAL(msg2.GetPayload(msg2.Record.GetPayloadId(0)), msg.GetPayload(msg.Record.GetPayloadId(0)));
        UNIT_ASSERT_EQUAL(msg2.GetPayload(msg2.Record.GetPayloadId(1)), msg.GetPayload(msg.Record.GetPayloadId(1)));
    }

    template <class TEvent>
    void TestAllSizes(size_t step1 = 100, size_t step2 = 111) {
        for (size_t size1 = 0; size1 < 10000; size1 += step1) {
            for (size_t size2 = 0; size2 < 10000; size2 += step2) {
                TestSerializeDeserialize<TEvent, TEvent>(size1, size2);
            }
        }
    }

#if (!defined(_tsan_enabled_))
    Y_UNIT_TEST(SerializeDeserialize) {
        TestAllSizes<TEvMessageWithPayload>();
    }
#endif


    struct TEvArenaMessage : TEventPBWithArena<TEvArenaMessage, TMessageWithPayload, EvArenaMessage> {
    };

    Y_UNIT_TEST(SerializeDeserializeArena) {
        TestAllSizes<TEvArenaMessage>(500, 111);
    }


    struct TEvArenaMessageBig : TEventPBWithArena<TEvArenaMessageBig, TMessageWithPayload, EvArenaMessageBig, 4000, 32000> {
    };

    Y_UNIT_TEST(SerializeDeserializeArenaBig) {
        TestAllSizes<TEvArenaMessageBig>(111, 500);
    }


    // Compatible with TEvArenaMessage but doesn't use arenas
    struct TEvArenaMessageWithoutArena : TEventPB<TEvArenaMessageWithoutArena, TMessageWithPayload, EvArenaMessage> {
    };

    Y_UNIT_TEST(Compatibility) {
        TestSerializeDeserialize<TEvArenaMessage, TEvArenaMessageWithoutArena>(200, 14010);
        TestSerializeDeserialize<TEvArenaMessageWithoutArena, TEvArenaMessage>(2000, 4010);
    }

    Y_UNIT_TEST(PreSerializedCompatibility) {
        // ensure TEventPreSerializedPB and TEventPB are interchangable with no compatibility issues
        TMessageWithPayload msg;
        msg.SetMeta("hello, world!");
        msg.AddPayloadId(123);
        msg.AddPayloadId(999);
        msg.AddSomeData("abc");
        msg.AddSomeData("xyzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz");

        TEvMessageWithPayloadPreSerialized e1;
        Y_PROTOBUF_SUPPRESS_NODISCARD msg.SerializeToString(&e1.PreSerializedData);

        auto serializer1 = MakeHolder<TAllocChunkSerializer>();
        e1.SerializeToArcadiaStream(serializer1.Get());
        auto buffers1 = serializer1->Release(e1.CreateSerializationInfo());
        UNIT_ASSERT_VALUES_EQUAL(buffers1->GetSize(), e1.CalculateSerializedSize());
        TString ser1 = buffers1->GetString();

        TEvMessageWithPayload e2(msg);
        auto serializer2 = MakeHolder<TAllocChunkSerializer>();
        e2.SerializeToArcadiaStream(serializer2.Get());
        auto buffers2 = serializer2->Release(e2.CreateSerializationInfo());
        UNIT_ASSERT_VALUES_EQUAL(buffers2->GetSize(), e2.CalculateSerializedSize());
        TString ser2 = buffers2->GetString();
        UNIT_ASSERT_VALUES_EQUAL(ser1, ser2);

        // deserialize
        auto data = MakeIntrusive<TEventSerializedData>(ser1, TEventSerializationInfo{});
        THolder<TEvMessageWithPayloadPreSerialized> parsedEvent(static_cast<TEvMessageWithPayloadPreSerialized*>(TEvMessageWithPayloadPreSerialized::Load(data.Get())));
        UNIT_ASSERT_VALUES_EQUAL(parsedEvent->PreSerializedData, ""); // this field is empty after deserialization
        auto& record = parsedEvent->GetRecord();
        UNIT_ASSERT_VALUES_EQUAL(record.GetMeta(), msg.GetMeta());
        UNIT_ASSERT_VALUES_EQUAL(record.PayloadIdSize(), msg.PayloadIdSize());
        UNIT_ASSERT_VALUES_EQUAL(record.PayloadIdSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(record.GetPayloadId(0), msg.GetPayloadId(0));
        UNIT_ASSERT_VALUES_EQUAL(record.GetPayloadId(1), msg.GetPayloadId(1));
    }
}

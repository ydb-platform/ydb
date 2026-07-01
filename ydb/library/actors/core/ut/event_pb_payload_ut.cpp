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
        auto buffers = serializer->Release(msg.CreateSerializationInfo(false));
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

        THolder<TEventTo> ev2 = THolder(TEventTo::Load(buffers.Get()));
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
        auto buffers1 = serializer1->Release(e1.CreateSerializationInfo(false));
        UNIT_ASSERT_VALUES_EQUAL(buffers1->GetSize(), e1.CalculateSerializedSize());
        TString ser1 = buffers1->GetString();

        TEvMessageWithPayload e2(msg);
        auto serializer2 = MakeHolder<TAllocChunkSerializer>();
        e2.SerializeToArcadiaStream(serializer2.Get());
        auto buffers2 = serializer2->Release(e2.CreateSerializationInfo(false));
        UNIT_ASSERT_VALUES_EQUAL(buffers2->GetSize(), e2.CalculateSerializedSize());
        TString ser2 = buffers2->GetString();
        UNIT_ASSERT_VALUES_EQUAL(ser1, ser2);

        // deserialize
        auto data = MakeIntrusive<TEventSerializedData>(ser1, TEventSerializationInfo{});
        THolder<TEvMessageWithPayloadPreSerialized> parsedEvent(TEvMessageWithPayloadPreSerialized::Load(data.Get()));
        UNIT_ASSERT_VALUES_EQUAL(parsedEvent->PreSerializedData, ""); // this field is empty after deserialization
        auto& record = parsedEvent->GetRecord();
        UNIT_ASSERT_VALUES_EQUAL(record.GetMeta(), msg.GetMeta());
        UNIT_ASSERT_VALUES_EQUAL(record.PayloadIdSize(), msg.PayloadIdSize());
        UNIT_ASSERT_VALUES_EQUAL(record.PayloadIdSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(record.GetPayloadId(0), msg.GetPayloadId(0));
        UNIT_ASSERT_VALUES_EQUAL(record.GetPayloadId(1), msg.GetPayloadId(1));
    }

    Y_UNIT_TEST(MalformedEventType) {
        // First, verify the happy path
        {
            auto ev = MakeHolder<IEventHandle>(
                EvMessageWithPayload, 0, TActorId(), TActorId(),
                MakeIntrusive<TEventSerializedData>(), 0);
            UNIT_ASSERT(ev->Get<TEvMessageWithPayload>() != nullptr);
        }
        // Next, verify the type mismatch
        {
            auto ev = MakeHolder<IEventHandle>(
                EvArenaMessage, 0, TActorId(), TActorId(),
                MakeIntrusive<TEventSerializedData>(), 0);
            UNIT_ASSERT_EXCEPTION(ev->Get<TEvMessageWithPayload>(), yexception);
        }
    }

    Y_UNIT_TEST(MalformedEventData) {
        auto ev = MakeHolder<IEventHandle>(
            EvMessageWithPayload, 0, TActorId(), TActorId(),
            MakeIntrusive<TEventSerializedData>(TString("\xff", 1), TEventSerializationInfo{}), 0);
        UNIT_ASSERT_EXCEPTION(ev->Get<TEvMessageWithPayload>(), yexception);
    }

    Y_UNIT_TEST(MalformedEventPayload) {
        // Invalid marker
        {
            auto ev = MakeHolder<IEventHandle>(
                EvMessageWithPayload, 0, TActorId(), TActorId(),
                MakeIntrusive<TEventSerializedData>(TString("\xff", 1), TEventSerializationInfo{.IsExtendedFormat = true}), 0);
            UNIT_ASSERT_EXCEPTION(ev->Get<TEvMessageWithPayload>(), yexception);
        }
        // Valid marker, missing payload count
        {
            auto ev = MakeHolder<IEventHandle>(
                EvMessageWithPayload, 0, TActorId(), TActorId(),
                MakeIntrusive<TEventSerializedData>(TString("\x07", 1), TEventSerializationInfo{.IsExtendedFormat = true}), 0);
            UNIT_ASSERT_EXCEPTION(ev->Get<TEvMessageWithPayload>(), yexception);
        }
        // Valid marker, one payload, missing payload length
        {
            auto ev = MakeHolder<IEventHandle>(
                EvMessageWithPayload, 0, TActorId(), TActorId(),
                MakeIntrusive<TEventSerializedData>(TString("\x07\x01", 2), TEventSerializationInfo{.IsExtendedFormat = true}), 0);
            UNIT_ASSERT_EXCEPTION(ev->Get<TEvMessageWithPayload>(), yexception);
        }
        // Valid marker, one payload, valid length, not enough data
        {
            auto ev = MakeHolder<IEventHandle>(
                EvMessageWithPayload, 0, TActorId(), TActorId(),
                MakeIntrusive<TEventSerializedData>(TString("\x07\x01\x02\x00", 4), TEventSerializationInfo{.IsExtendedFormat = true}), 0);
            UNIT_ASSERT_EXCEPTION(ev->Get<TEvMessageWithPayload>(), yexception);
        }
        // Valid marker, one payload, valid length, enough data, message malformed
        {
            auto ev = MakeHolder<IEventHandle>(
                EvMessageWithPayload, 0, TActorId(), TActorId(),
                MakeIntrusive<TEventSerializedData>(TString("\x07\x01\x02\x00\x00\xff", 6), TEventSerializationInfo{.IsExtendedFormat = true}), 0);
            UNIT_ASSERT_EXCEPTION(ev->Get<TEvMessageWithPayload>(), yexception);
        }
    }

    struct TEvAligned4 : TEventPB<TEvAligned4, TMessageWithPayload, EvMessageWithPayload> {
        static constexpr size_t GetPayloadAlignment() { return 4; }
    };

    struct TEvAligned4096 : TEventPB<TEvAligned4096, TMessageWithPayload, EvMessageWithPayload> {
        static constexpr size_t GetPayloadAlignment() { return 4096; }
    };

    Y_UNIT_TEST(PayloadAlignmentPropagation) {
        auto check = [](auto& ev, size_t expectedAlignment) {
            ev.Record.SetMeta("test");
            ev.Record.AddPayloadId(ev.AddPayload(MakeStringRope(MakeString(3000))));
            ev.Record.AddPayloadId(ev.AddPayload(MakeStringRope(MakeString(3000))));

            const TEventSerializationInfo info = ev.CreateSerializationInfo(true);
            UNIT_ASSERT(info.IsExtendedFormat);
            // Sections: [0]=header(inline), [1]=payload0, [2]=payload1, [3]=protobuf(inline)
            UNIT_ASSERT_VALUES_EQUAL(info.Sections.size(), 4u);

            UNIT_ASSERT_VALUES_EQUAL(info.Sections[0].IsInline, true);
            UNIT_ASSERT_VALUES_EQUAL(info.Sections[0].Alignment, 0u);

            UNIT_ASSERT_VALUES_EQUAL(info.Sections[1].IsInline, false);
            UNIT_ASSERT_VALUES_EQUAL(info.Sections[1].Alignment, expectedAlignment);

            UNIT_ASSERT_VALUES_EQUAL(info.Sections[2].IsInline, false);
            UNIT_ASSERT_VALUES_EQUAL(info.Sections[2].Alignment, expectedAlignment);

            UNIT_ASSERT_VALUES_EQUAL(info.Sections[3].IsInline, true);
            UNIT_ASSERT_VALUES_EQUAL(info.Sections[3].Alignment, 0u);
        };

        TEvAligned4 ev4;
        check(ev4, 4);

        TEvAligned4096 ev4096;
        check(ev4096, 4096);
    }

    struct TEvAligned4Header8 : TEventPB<TEvAligned4Header8, TMessageWithPayload, EvMessageWithPayload> {
        static constexpr size_t GetPayloadAlignment() { return 4; }
        static constexpr size_t GetPayloadHeaderSize() { return 8; }
    };

    struct TEvAligned4096Header4096 : TEventPB<TEvAligned4096Header4096, TMessageWithPayload, EvMessageWithPayload> {
        static constexpr size_t GetPayloadAlignment() { return 4096; }
        static constexpr size_t GetPayloadHeaderSize() { return 4096; }
    };

    Y_UNIT_TEST(PayloadHeaderPropagation) {
        auto check = [](auto& ev, size_t expectedAlignment, size_t expectedHeader) {
            ev.Record.SetMeta("test");
            ev.Record.AddPayloadId(ev.AddPayload(MakeStringRope(MakeString(3000))));
            ev.Record.AddPayloadId(ev.AddPayload(MakeStringRope(MakeString(3000))));

            const TEventSerializationInfo info = ev.CreateSerializationInfo(true);
            UNIT_ASSERT(info.IsExtendedFormat);
            // Sections: [0]=header(inline), [1]=payload0, [2]=payload1, [3]=protobuf(inline)
            UNIT_ASSERT_VALUES_EQUAL(info.Sections.size(), 4u);

            // header lives in the reserved headroom right before each payload section
            UNIT_ASSERT_VALUES_EQUAL(info.Sections[0].Headroom, 0u);

            UNIT_ASSERT_VALUES_EQUAL(info.Sections[1].Headroom, expectedHeader);
            UNIT_ASSERT_VALUES_EQUAL(info.Sections[1].Alignment, expectedAlignment);

            UNIT_ASSERT_VALUES_EQUAL(info.Sections[2].Headroom, expectedHeader);
            UNIT_ASSERT_VALUES_EQUAL(info.Sections[2].Alignment, expectedAlignment);

            UNIT_ASSERT_VALUES_EQUAL(info.Sections[3].Headroom, 0u);
        };

        TEvAligned4Header8 ev1;
        check(ev1, 4, 8);

        TEvAligned4096Header4096 ev2;
        check(ev2, 4096, 4096);
    }

    struct TEvHeader16 : TEventPB<TEvHeader16, TMessageWithPayload, EvMessageWithPayload> {
        static constexpr size_t GetPayloadHeaderSize() { return 16; }
    };

    Y_UNIT_TEST(GetPayloadWithHeaderZeroCopy) {
        const size_t headerSize = 16;
        const TString payloadStr = MakeString(3000);

        TEvHeader16 ev;
        // Build a payload buffer that already has reserved headroom in front (as the interconnect receive path does).
        TRcBuf buf = TRcBuf::Copy(payloadStr.data(), payloadStr.size(), headerSize, 0);
        const char* payloadPtr = buf.GetData();
        const ui32 id = ev.AddPayload(TRope(std::move(buf)));

        TRcBuf withHeader = ev.GetPayloadWithHeader(id);
        UNIT_ASSERT_VALUES_EQUAL(withHeader.GetSize(), headerSize + payloadStr.size());
        // zero-copy: payload still lives at the same address, header is the bytes right before it
        UNIT_ASSERT(withHeader.GetData() + headerSize == payloadPtr);
        UNIT_ASSERT_EQUAL(0, memcmp(withHeader.GetData() + headerSize, payloadStr.data(), payloadStr.size()));

        // writing the header must not affect the payload-only accessor
        auto span = withHeader.UnsafeGetContiguousSpanMut();
        memset(span.data(), 0xAB, headerSize);
        UNIT_ASSERT_EQUAL(ev.GetPayload(id), MakeStringRope(payloadStr));
    }

    Y_UNIT_TEST(GetPayloadWithHeaderSecondCallFallback) {
        const size_t headerSize = 16;
        const TString payloadStr = MakeString(3000);

        TEvHeader16 ev;
        TRcBuf buf = TRcBuf::Copy(payloadStr.data(), payloadStr.size(), headerSize, 0);
        const char* payloadPtr = buf.GetData();
        const ui32 id = ev.AddPayload(TRope(std::move(buf)));

        TRcBuf first = ev.GetPayloadWithHeader(id);
        UNIT_ASSERT(first.GetData() + headerSize == payloadPtr);

        // Second call consumes no headroom (cookies moved on first call) and allocates a fresh buffer.
        TRcBuf second = ev.GetPayloadWithHeader(id);
        UNIT_ASSERT_VALUES_EQUAL(second.GetSize(), headerSize + payloadStr.size());
        UNIT_ASSERT(second.GetData() + headerSize != payloadPtr);
        UNIT_ASSERT_EQUAL(0, memcmp(second.GetData() + headerSize, payloadStr.data(), payloadStr.size()));
        UNIT_ASSERT_EQUAL(ev.GetPayload(id), MakeStringRope(payloadStr));
    }

    Y_UNIT_TEST(GetPayloadWithHeaderFallbackMultiChunk) {
        constexpr size_t headerSize = 16;
        const TString part1 = MakeString(1000);
        const TString part2 = MakeString(2000);

        TRope multiChunk;
        multiChunk.Insert(multiChunk.End(), TRope(TRcBuf::Copy(part1)));
        multiChunk.Insert(multiChunk.End(), TRope(TRcBuf::Copy(part2)));
        UNIT_ASSERT(!multiChunk.IsContiguous());

        TEvHeader16 ev;
        const ui32 id = ev.AddPayload(std::move(multiChunk));

        TRcBuf withHeader = ev.GetPayloadWithHeader(id);
        UNIT_ASSERT_VALUES_EQUAL(withHeader.GetSize(), headerSize + part1.size() + part2.size());
        UNIT_ASSERT_EQUAL(0, memcmp(withHeader.GetData() + headerSize, part1.data(), part1.size()));
        UNIT_ASSERT_EQUAL(0, memcmp(withHeader.GetData() + headerSize + part1.size(), part2.data(), part2.size()));

        TRope expected;
        expected.Insert(expected.End(), TRope(TRcBuf::Copy(part1)));
        expected.Insert(expected.End(), TRope(TRcBuf::Copy(part2)));
        UNIT_ASSERT_EQUAL(ev.GetPayload(id), expected);
    }

    Y_UNIT_TEST(GetPayloadWithHeaderFallbackCopyAligned) {
        const size_t headerSize = 4096;
        const size_t alignment = 4096;
        const TString payloadStr = MakeString(3000);

        TEvAligned4096Header4096 ev;
        // Plain payload without reserved headroom -> fallback allocates an aligned [header|payload] buffer and copies.
        const ui32 id = ev.AddPayload(MakeStringRope(payloadStr));

        TRcBuf withHeader = ev.GetPayloadWithHeader(id);
        UNIT_ASSERT_VALUES_EQUAL(withHeader.GetSize(), headerSize + payloadStr.size());
        UNIT_ASSERT_VALUES_EQUAL(reinterpret_cast<uintptr_t>(withHeader.GetData()) % alignment, 0u);
        UNIT_ASSERT_EQUAL(0, memcmp(withHeader.GetData() + headerSize, payloadStr.data(), payloadStr.size()));
        // payload-only accessor remains valid and unchanged
        UNIT_ASSERT_EQUAL(ev.GetPayload(id), MakeStringRope(payloadStr));
    }

    Y_UNIT_TEST(GetPayloadWithHeaderZeroHeaderSharedNonFront) {
        const TString payloadStr = MakeString(3000);
        constexpr size_t headroom = 16;

        // Payload buffer with headroom, plus a sibling so the backend is shared (not private).
        TRcBuf buf = TRcBuf::Copy(payloadStr.data(), payloadStr.size(), headroom, 0);
        const char* payloadPtr = buf.GetData();
        TRcBuf sibling(buf);

        // Claim the headroom on one buffer, moving the cookie front edge away from payloadPtr, so the
        // sibling view is now both shared and non-front.
        TRcBuf claimed = buf.ExpandFront(headroom);
        UNIT_ASSERT(claimed.GetData() + headroom == payloadPtr);
        UNIT_ASSERT_VALUES_EQUAL(sibling.Headroom(), 0u);

        // Default event => GetPayloadHeaderSize() == 0 => GetPayloadWithHeader calls ExpandFront(0).
        // This must not abort and should return a plain view of the payload.
        TEvMessageWithPayload ev;
        const ui32 id = ev.AddPayload(TRope(sibling));

        TRcBuf withHeader = ev.GetPayloadWithHeader(id);
        UNIT_ASSERT_VALUES_EQUAL(withHeader.GetSize(), payloadStr.size());
        UNIT_ASSERT(withHeader.GetData() == payloadPtr);
        UNIT_ASSERT_EQUAL(ev.GetPayload(id), MakeStringRope(payloadStr));
    }

    Y_UNIT_TEST(GetPayloadWithHeaderEmptyPayload) {
        constexpr size_t headerSize = 16;

        TEvHeader16 ev;
        const ui32 id = ev.AddPayload(TRope());

        // Fast path is skipped (empty payload); fallback allocates a header-only buffer.
        TRcBuf withHeader = ev.GetPayloadWithHeader(id);
        UNIT_ASSERT_VALUES_EQUAL(withHeader.GetSize(), headerSize);
        UNIT_ASSERT_EQUAL(ev.GetPayload(id), TRope());
    }

    Y_UNIT_TEST(GetPayloadWithHeaderMisalignedHeadroomFallback) {
        constexpr size_t headerSize = 4096;
        constexpr size_t alignment = 4096;
        const TString payloadStr = MakeString(3000);

        // Ample headroom, but deliberately misalign the header start (the RDMA-like case).
        TRcBuf buf = TRcBuf::Copy(payloadStr.data(), payloadStr.size(), headerSize + alignment, 0);
        const uintptr_t headerStart = reinterpret_cast<uintptr_t>(buf.GetData()) - headerSize;
        if (headerStart % alignment == 0) {
            // Nudge the payload start forward so the header start becomes misaligned.
            buf.TrimFront(buf.GetSize() - 8);
        }
        UNIT_ASSERT_VALUES_UNEQUAL(
            (reinterpret_cast<uintptr_t>(buf.GetData()) - headerSize) % alignment, 0u);

        const char* payloadPtr = buf.GetData();
        const size_t curSize = buf.GetSize();
        TEvAligned4096Header4096 ev;
        const ui32 id = ev.AddPayload(TRope(std::move(buf)));

        TRcBuf withHeader = ev.GetPayloadWithHeader(id);
        UNIT_ASSERT_VALUES_EQUAL(withHeader.GetSize(), headerSize + curSize);
        // Fallback allocates a fresh aligned buffer; it is not the zero-copy in-place result.
        UNIT_ASSERT_VALUES_EQUAL(reinterpret_cast<uintptr_t>(withHeader.GetData()) % alignment, 0u);
        UNIT_ASSERT(withHeader.GetData() + headerSize != payloadPtr);
        UNIT_ASSERT_EQUAL(0, memcmp(withHeader.GetData() + headerSize, payloadPtr, curSize));
    }

    Y_UNIT_TEST(GetPayloadWithHeaderZeroCopyAlignedCookieless) {
        constexpr size_t headerSize = 4096;
        constexpr size_t alignment = 4096;
        const size_t payloadSize = 3000;
        const TString payloadStr = MakeString(payloadSize);

        // Mimic the interconnect aligned receive path: a cookieless TRopeAlignedBuffer with reserved
        // aligned headroom in front of the payload (see TInputSessionTCP::AllocateRcBuf).
        const size_t extra = alignment - 1;
        TRcBuf buffer = TRcBuf(TRopeAlignedBuffer::Allocate(payloadSize + headerSize + extra));
        const uintptr_t ptr = reinterpret_cast<uintptr_t>(buffer.GetData()) + headerSize;
        const size_t misalignment = ptr & (alignment - 1);
        const size_t shift = misalignment ? alignment - misalignment : 0;
        const size_t tailroom = extra - shift;
        buffer.TrimFront(payloadSize + tailroom);
        buffer.TrimBack(payloadSize);
        UNIT_ASSERT_VALUES_EQUAL(reinterpret_cast<uintptr_t>(buffer.GetData()) % alignment, 0u);
        std::memcpy(buffer.UnsafeGetDataMut(), payloadStr.data(), payloadSize);
        const char* payloadPtr = buffer.GetData();

        TEvAligned4096Header4096 ev;
        const ui32 id = ev.AddPayload(TRope(std::move(buffer)));

        TRcBuf withHeader = ev.GetPayloadWithHeader(id);
        UNIT_ASSERT_VALUES_EQUAL(withHeader.GetSize(), headerSize + payloadSize);
        // Zero-copy over a cookieless aligned backend: payload stays in place, header sits right before it.
        UNIT_ASSERT(withHeader.GetData() + headerSize == payloadPtr);
        UNIT_ASSERT_VALUES_EQUAL(reinterpret_cast<uintptr_t>(withHeader.GetData()) % alignment, 0u);
        UNIT_ASSERT_EQUAL(0, memcmp(withHeader.GetData() + headerSize, payloadStr.data(), payloadSize));

        // Writing the header must not affect the payload-only accessor.
        auto span = withHeader.UnsafeGetContiguousSpanMut();
        memset(span.data(), 0xCD, headerSize);
        UNIT_ASSERT_EQUAL(ev.GetPayload(id), MakeStringRope(payloadStr));
    }
}

#include <ydb/library/actors/trace_data/trace_data.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/strbuf.h>

#include <cstring>
#include <limits>

using namespace NActors::NTracing;

namespace {

    constexpr size_t MetadataSizeOffset = 12;
    constexpr size_t EventCountOffset = 16;

    template <typename T>
    void OverwriteValue(TBuffer& buffer, size_t offset, T value) {
        UNIT_ASSERT_C(offset <= buffer.Size() && sizeof(value) <= buffer.Size() - offset, "Invalid test offset");
        std::memcpy(buffer.Data() + offset, &value, sizeof(value));
    }

    TTraceChunk MakeSentinelChunk() {
        TTraceChunk chunk;
        chunk.StartTimestampUs = 42;
        chunk.ActivityDict = {{1, "activity"}};
        chunk.EventNamesDict = {{2, "event"}};
        chunk.ThreadPoolDict = {{3, "thread"}};

        TTraceEvent event{};
        event.Sender = 10;
        event.Type = static_cast<ui8>(ETraceEventType::New);
        chunk.Events.push_back(event);
        return chunk;
    }

} // anonymous namespace

Y_UNIT_TEST_SUITE(TraceData) {

    Y_UNIT_TEST(EventSize) {
        UNIT_ASSERT_VALUES_EQUAL(sizeof(TTraceEvent), 32u);
    }

    Y_UNIT_TEST(SerializeDeserializeRoundTrip) {
        TTraceChunk chunk;
        chunk.StartTimestampUs = 1700000000000000ULL;
        chunk.ActivityDict = {{0, "ACTOR_A"}, {3, "ACTOR_B"}, {10, "ACTOR_C"}};
        chunk.EventNamesDict = {{100, "TEvRequest"}, {200, "TEvResponse"}};
        chunk.ThreadPoolDict = {{0, "System"}, {2, "User"}};

        TTraceEvent ev1{};
        ev1.DeltaUs = 1000000;
        ev1.Sender = 42;
        ev1.Type = static_cast<ui8>(ETraceEventType::New);
        chunk.Events.push_back(ev1);

        TTraceEvent ev2{};
        ev2.DeltaUs = 1000010;
        ev2.Sender = 1;
        ev2.Recipient = 42;
        ev2.MessageType = 100;
        ev2.Type = static_cast<ui8>(ETraceEventType::SendLocal);
        ev2.HandleHash = 0x11111111;
        chunk.Events.push_back(ev2);

        TTraceEvent ev3{};
        ev3.DeltaUs = 1000020;
        ev3.Sender = 1;
        ev3.Recipient = 42;
        ev3.MessageType = 100;
        ev3.ActivityIndex = 3;
        ev3.Type = static_cast<ui8>(ETraceEventType::ReceiveLocal);
        ev3.HandleHash = 0x11111111;
        ev3.ThreadIdx = 2;
        chunk.Events.push_back(ev3);

        TTraceEvent ev4{};
        ev4.DeltaUs = 1000030;
        ev4.Sender = 42;
        ev4.Type = static_cast<ui8>(ETraceEventType::Die);
        chunk.Events.push_back(ev4);

        TTraceEvent ev5{};
        ev5.DeltaUs = 1000040;
        ev5.Sender = 0x22222222;
        ev5.Recipient = 42;
        ev5.MessageType = 100;
        ev5.ActivityIndex = 3;
        ev5.Type = static_cast<ui8>(ETraceEventType::ForwardLocal);
        ev5.HandleHash = 0x11111111;
        ev5.ThreadIdx = 2;
        chunk.Events.push_back(ev5);

        auto buf = SerializeTrace(chunk, 1);
        UNIT_ASSERT(buf.Size() > 0);

        TTraceChunk restored;
        ui32 nodeId = 0;
        UNIT_ASSERT(DeserializeTrace(buf, restored, nodeId));

        UNIT_ASSERT_VALUES_EQUAL(nodeId, 1u);
        UNIT_ASSERT_VALUES_EQUAL(restored.StartTimestampUs, chunk.StartTimestampUs);
        UNIT_ASSERT(restored.ActivityDict == chunk.ActivityDict);
        UNIT_ASSERT(restored.EventNamesDict == chunk.EventNamesDict);
        UNIT_ASSERT(restored.ThreadPoolDict == chunk.ThreadPoolDict);
        UNIT_ASSERT_VALUES_EQUAL(restored.Events.size(), chunk.Events.size());
        UNIT_ASSERT_VALUES_EQUAL(restored.Events[0].Type, static_cast<ui8>(ETraceEventType::New));
        UNIT_ASSERT_VALUES_EQUAL(restored.Events[1].Type, static_cast<ui8>(ETraceEventType::SendLocal));
        UNIT_ASSERT_VALUES_EQUAL(restored.Events[2].Type, static_cast<ui8>(ETraceEventType::ReceiveLocal));
        UNIT_ASSERT_VALUES_EQUAL(restored.Events[3].Type, static_cast<ui8>(ETraceEventType::Die));
        UNIT_ASSERT_VALUES_EQUAL(restored.Events[4].Type, static_cast<ui8>(ETraceEventType::ForwardLocal));
        UNIT_ASSERT_VALUES_EQUAL(restored.Events[0].DeltaUs, 1000000u);
        UNIT_ASSERT_VALUES_EQUAL(restored.Events[4].DeltaUs, 1000040u);
        UNIT_ASSERT_VALUES_EQUAL(restored.Events[1].HandleHash, 0x11111111u);
        UNIT_ASSERT_VALUES_EQUAL(restored.Events[4].HandleHash, 0x11111111u);
        UNIT_ASSERT_VALUES_EQUAL(restored.Events[4].Sender, 0x22222222ull);
        UNIT_ASSERT_VALUES_EQUAL(restored.Events[2].ThreadIdx, 2u);
        UNIT_ASSERT_VALUES_EQUAL(restored.Events[4].ThreadIdx, 2u);
    }

    Y_UNIT_TEST(DeserializeRejectsInvalidHeader) {
        auto badMagic = SerializeTrace(TTraceChunk{}, 1);
        badMagic.Data()[0] = 'G';

        auto badVersion = SerializeTrace(TTraceChunk{}, 1);
        OverwriteValue(badVersion, sizeof(ui32), TraceFileVersion + 1);

        TTraceChunk chunk;
        ui32 nodeId = 0;
        UNIT_ASSERT(!DeserializeTrace(badMagic, chunk, nodeId));
        UNIT_ASSERT(!DeserializeTrace(badVersion, chunk, nodeId));
    }

    Y_UNIT_TEST(WireFormatIsLittleEndian) {
        TTraceChunk chunk;
        chunk.StartTimestampUs = 0x0102030405060708;

        TTraceEvent event{};
        event.Sender = 0x1112131415161718;
        event.Type = static_cast<ui8>(ETraceEventType::SendLocal);
        chunk.Events.push_back(event);

        const auto buffer = SerializeTrace(chunk, 0x01020304);
        UNIT_ASSERT_VALUES_EQUAL(TStringBuf(buffer.Data(), 4), "YTRA");
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui8>(buffer.Data()[4]), 1u);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui8>(buffer.Data()[8]), 4u);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui8>(buffer.Data()[9]), 3u);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui8>(buffer.Data()[10]), 2u);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui8>(buffer.Data()[11]), 1u);

        constexpr size_t eventOffset = sizeof(TTraceFileHeader) + 3 * sizeof(ui32);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui8>(buffer.Data()[eventOffset]), 0x18u);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui8>(buffer.Data()[eventOffset + 7]), 0x11u);
    }

    Y_UNIT_TEST(DeserializeRejectsEveryTruncation) {
        const auto source = MakeSentinelChunk();
        const auto buffer = SerializeTrace(source, 1);
        for (size_t size = 0; size < buffer.Size(); ++size) {
            TBuffer truncated(buffer.Data(), size);
            TTraceChunk restored;
            ui32 nodeId = 0;
            UNIT_ASSERT_C(!DeserializeTrace(truncated, restored, nodeId),
                "Accepted trace truncated to " << size << " bytes");
        }
    }

    Y_UNIT_TEST(DeserializeRejectsInvalidSizesAndTrailingData) {
        auto badMetadataSize = SerializeTrace(TTraceChunk{}, 1);
        OverwriteValue(badMetadataSize, MetadataSizeOffset, std::numeric_limits<ui32>::max());

        auto badEventCount = SerializeTrace(TTraceChunk{}, 1);
        OverwriteValue(badEventCount, EventCountOffset, std::numeric_limits<ui64>::max());

        auto badDictionaryCount = SerializeTrace(TTraceChunk{}, 1);
        OverwriteValue(badDictionaryCount, sizeof(TTraceFileHeader), std::numeric_limits<ui32>::max());

        TTraceChunk withString;
        withString.ActivityDict = {{1, "A"}};
        auto badStringSize = SerializeTrace(withString, 1);
        OverwriteValue(
            badStringSize,
            sizeof(TTraceFileHeader) + 2 * sizeof(ui32),
            std::numeric_limits<ui32>::max());

        auto trailingData = SerializeTrace(TTraceChunk{}, 1);
        trailingData.Append('x');

        TTraceChunk restored;
        ui32 nodeId = 0;
        UNIT_ASSERT(!DeserializeTrace(badMetadataSize, restored, nodeId));
        UNIT_ASSERT(!DeserializeTrace(badEventCount, restored, nodeId));
        UNIT_ASSERT(!DeserializeTrace(badDictionaryCount, restored, nodeId));
        UNIT_ASSERT(!DeserializeTrace(badStringSize, restored, nodeId));
        UNIT_ASSERT(!DeserializeTrace(trailingData, restored, nodeId));
    }

    Y_UNIT_TEST(DeserializeFailureDoesNotModifyOutput) {
        TTraceChunk output = MakeSentinelChunk();
        const TTraceChunk expected = output;
        ui32 nodeId = 42;

        auto invalid = SerializeTrace(MakeSentinelChunk(), 7);
        OverwriteValue(invalid, invalid.Size() - 2, ui8{255});

        UNIT_ASSERT(!DeserializeTrace(invalid, output, nodeId));
        UNIT_ASSERT_VALUES_EQUAL(nodeId, 42u);
        UNIT_ASSERT_VALUES_EQUAL(output.StartTimestampUs, expected.StartTimestampUs);
        UNIT_ASSERT(output.ActivityDict == expected.ActivityDict);
        UNIT_ASSERT(output.EventNamesDict == expected.EventNamesDict);
        UNIT_ASSERT(output.ThreadPoolDict == expected.ThreadPoolDict);
        UNIT_ASSERT_VALUES_EQUAL(output.Events.size(), expected.Events.size());
        UNIT_ASSERT_VALUES_EQUAL(output.Events[0].Sender, expected.Events[0].Sender);
        UNIT_ASSERT_VALUES_EQUAL(output.Events[0].Type, expected.Events[0].Type);
    }

    Y_UNIT_TEST(DeserializeSuccessReplacesOutput) {
        TTraceChunk output = MakeSentinelChunk();
        ui32 nodeId = 42;

        const auto emptyTrace = SerializeTrace(TTraceChunk{}, 7);
        UNIT_ASSERT(DeserializeTrace(emptyTrace, output, nodeId));
        UNIT_ASSERT_VALUES_EQUAL(nodeId, 7u);
        UNIT_ASSERT_VALUES_EQUAL(output.StartTimestampUs, 0u);
        UNIT_ASSERT(output.ActivityDict.empty());
        UNIT_ASSERT(output.EventNamesDict.empty());
        UNIT_ASSERT(output.ThreadPoolDict.empty());
        UNIT_ASSERT(output.Events.empty());
    }

    Y_UNIT_TEST(EmptyTrace) {
        TTraceChunk chunk;
        auto buf = SerializeTrace(chunk, 5);
        TTraceChunk restored;
        ui32 nodeId = 0;
        UNIT_ASSERT(DeserializeTrace(buf, restored, nodeId));
        UNIT_ASSERT_VALUES_EQUAL(nodeId, 5u);
        UNIT_ASSERT(restored.Events.empty());
    }
}

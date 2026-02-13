#include <ydb/library/actors/core/tracer.h>
#include <ydb/library/actors/trace_data/trace_data.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NActors::NTracing;

Y_UNIT_TEST_SUITE(TracerTest) {

    Y_UNIT_TEST(SerializeDeserializeRoundTrip) {
        TTraceChunk chunk;
        chunk.ActivityDict = {{0, "ACTOR_A"}, {3, "ACTOR_B"}, {10, "ACTOR_C"}};
        chunk.EventNamesDict = {{100, "TEvRequest"}, {200, "TEvResponse"}};

        TTraceEvent ev1{};
        ev1.Timestamp = 1000000;
        ev1.Actor1 = 42;
        ev1.Type = static_cast<ui8>(ETraceEventType::New);
        chunk.Events.push_back(ev1);

        TTraceEvent ev2{};
        ev2.Timestamp = 1000010;
        ev2.Actor1 = 1;
        ev2.Actor2 = 42;
        ev2.Aux = 100;
        ev2.Type = static_cast<ui8>(ETraceEventType::SendLocal);
        chunk.Events.push_back(ev2);

        TTraceEvent ev3{};
        ev3.Timestamp = 1000020;
        ev3.Actor1 = 1;
        ev3.Actor2 = 42;
        ev3.Aux = 100;
        ev3.Extra = 3;
        ev3.Type = static_cast<ui8>(ETraceEventType::ReceiveLocal);
        chunk.Events.push_back(ev3);

        TTraceEvent ev4{};
        ev4.Timestamp = 1000030;
        ev4.Actor1 = 42;
        ev4.Type = static_cast<ui8>(ETraceEventType::Die);
        chunk.Events.push_back(ev4);

        auto buf = SerializeTrace(chunk, 1);
        UNIT_ASSERT(buf.Size() > 0);

        TTraceChunk restored;
        ui32 nodeId = 0;
        UNIT_ASSERT(DeserializeTrace(buf, restored, nodeId));

        UNIT_ASSERT_VALUES_EQUAL(nodeId, 1u);
        UNIT_ASSERT_VALUES_EQUAL(restored.ActivityDict.size(), 3u);
        UNIT_ASSERT_VALUES_EQUAL(restored.ActivityDict[0].first, 0u);
        UNIT_ASSERT_VALUES_EQUAL(restored.ActivityDict[0].second, "ACTOR_A");
        UNIT_ASSERT_VALUES_EQUAL(restored.ActivityDict[1].first, 3u);
        UNIT_ASSERT_VALUES_EQUAL(restored.ActivityDict[1].second, "ACTOR_B");
        UNIT_ASSERT_VALUES_EQUAL(restored.ActivityDict[2].first, 10u);
        UNIT_ASSERT_VALUES_EQUAL(restored.ActivityDict[2].second, "ACTOR_C");

        UNIT_ASSERT_VALUES_EQUAL(restored.EventNamesDict.size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(restored.EventNamesDict.at(100), "TEvRequest");
        UNIT_ASSERT_VALUES_EQUAL(restored.EventNamesDict.at(200), "TEvResponse");

        UNIT_ASSERT_VALUES_EQUAL(restored.Events.size(), 4u);

        UNIT_ASSERT_VALUES_EQUAL(restored.Events[0].Timestamp, 1000000u);
        UNIT_ASSERT_VALUES_EQUAL(restored.Events[0].Actor1, 42u);
        UNIT_ASSERT_VALUES_EQUAL(restored.Events[0].Type, static_cast<ui8>(ETraceEventType::New));

        UNIT_ASSERT_VALUES_EQUAL(restored.Events[1].Timestamp, 1000010u);
        UNIT_ASSERT_VALUES_EQUAL(restored.Events[1].Actor1, 1u);
        UNIT_ASSERT_VALUES_EQUAL(restored.Events[1].Actor2, 42u);
        UNIT_ASSERT_VALUES_EQUAL(restored.Events[1].Aux, 100u);
        UNIT_ASSERT_VALUES_EQUAL(restored.Events[1].Type, static_cast<ui8>(ETraceEventType::SendLocal));

        UNIT_ASSERT_VALUES_EQUAL(restored.Events[2].Extra, 3u);
        UNIT_ASSERT_VALUES_EQUAL(restored.Events[2].Type, static_cast<ui8>(ETraceEventType::ReceiveLocal));

        UNIT_ASSERT_VALUES_EQUAL(restored.Events[3].Type, static_cast<ui8>(ETraceEventType::Die));
    }

    Y_UNIT_TEST(DeserializeRejectsBadMagic) {
        TBuffer buf;
        buf.Append("GARBAGE_DATA_HERE_1234567890", 28);

        TTraceChunk chunk;
        ui32 nodeId = 0;
        UNIT_ASSERT(!DeserializeTrace(buf, chunk, nodeId));
    }

    Y_UNIT_TEST(DeserializeRejectsTruncatedData) {
        TTraceChunk chunk;
        chunk.Events.push_back(TTraceEvent{});
        auto buf = SerializeTrace(chunk, 1);

        TBuffer truncated;
        truncated.Append(buf.Data(), buf.Size() / 2);

        TTraceChunk restored;
        ui32 nodeId = 0;
        UNIT_ASSERT(!DeserializeTrace(truncated, restored, nodeId));
    }

    Y_UNIT_TEST(EmptyTrace) {
        TTraceChunk chunk;
        auto buf = SerializeTrace(chunk, 5);

        TTraceChunk restored;
        ui32 nodeId = 0;
        UNIT_ASSERT(DeserializeTrace(buf, restored, nodeId));
        UNIT_ASSERT_VALUES_EQUAL(nodeId, 5u);
        UNIT_ASSERT(restored.Events.empty());
        UNIT_ASSERT(restored.ActivityDict.empty());
        UNIT_ASSERT(restored.EventNamesDict.empty());
    }

    Y_UNIT_TEST(EventSize) {
        UNIT_ASSERT_VALUES_EQUAL(sizeof(TTraceEvent), 32u);
    }
}

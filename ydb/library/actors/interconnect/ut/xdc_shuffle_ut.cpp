#include <ydb/library/actors/core/event_pb.h>
#include <ydb/library/actors/interconnect/events_local.h>
#include <ydb/library/actors/interconnect/interconnect_channel.h>
#include <ydb/library/actors/interconnect/ut/protos/interconnect_test.pb.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NActors;

namespace {

struct TEvEmptyRecordWithPayload
    : TEventPB<TEvEmptyRecordWithPayload, NInterconnectTest::TEvTestSerialization, 0x10001>
{};

} // namespace

Y_UNIT_TEST_SUITE(InterconnectXdcShuffle) {

    Y_UNIT_TEST(EmptyProtoRecordCompletesAfterPayload) {
        TEvEmptyRecordWithPayload event;
        const TString payload(5000, 'x');
        event.AddPayload(TRope(payload));

        UNIT_ASSERT_VALUES_EQUAL(event.Record.ByteSize(), 0);

        const TEventSerializationInfo info = event.CreateSerializationInfo(true);
        UNIT_ASSERT_VALUES_EQUAL(info.Sections.size(), 3u);
        UNIT_ASSERT(info.Sections[0].IsInline);
        UNIT_ASSERT_VALUES_UNEQUAL(info.Sections[0].Size, 0u);
        UNIT_ASSERT(!info.Sections[1].IsInline);
        UNIT_ASSERT_VALUES_EQUAL(info.Sections[1].Size, payload.size());
        UNIT_ASSERT(info.Sections[2].IsInline);
        UNIT_ASSERT_VALUES_EQUAL(info.Sections[2].Size, 0u);

        auto getSerializedSize = [](std::span<TCoroutineChunkSerializer::TChunk> chunks) {
            size_t result = 0;
            for (const auto& [_, size] : chunks) {
                result += size;
            }
            return result;
        };

        TCoroutineChunkSerializer chunker;
        chunker.SetSerializingEvent(&event);

        TString headerBuffer(info.Sections[0].Size, '\0');
        auto chunks = chunker.FeedBuf(headerBuffer.begin(), headerBuffer.size());
        UNIT_ASSERT_VALUES_EQUAL(getSerializedSize(chunks), info.Sections[0].Size);
        UNIT_ASSERT(!chunker.IsComplete());

        TString payloadBuffer(payload.size(), '\0');
        chunks = chunker.FeedBuf(payloadBuffer.begin(), payloadBuffer.size());
        UNIT_ASSERT_VALUES_EQUAL(getSerializedSize(chunks), payload.size());
        UNIT_ASSERT(chunker.IsComplete());
        UNIT_ASSERT(chunker.IsSuccessfull());
    }

    Y_UNIT_TEST(EmptyProtoRecordAfterXdcPayloadDoesNotAbort) {
        auto common = MakeIntrusive<TInterconnectProxyCommon>();
        common->MonCounters = MakeIntrusive<NMonitoring::TDynamicCounters>();

        std::shared_ptr<IInterconnectMetrics> metrics = CreateInterconnectCounters(common);
        metrics->SetPeerInfo("peer", "1", "peer");

        auto destroyCallback = [](THolder<IEventBase>) {};
        TEventHolderPool pool(common, destroyCallback);

        TSessionParams params;
        params.UseExternalDataChannel = true;
        params.UseXdcShuffle = true;

        TEventOutputChannel channel(1, 1, 64 << 20, metrics, params, nullptr);

        auto* event = new TEvEmptyRecordWithPayload;
        const TString payload(5000, 'x');
        event->AddPayload(TRope(payload));

        const TEventSerializationInfo info = event->CreateSerializationInfo(true);
        UNIT_ASSERT_VALUES_EQUAL(info.Sections.size(), 3u);
        UNIT_ASSERT(info.Sections[0].IsInline);
        UNIT_ASSERT_VALUES_UNEQUAL(info.Sections[0].Size, 0u);
        UNIT_ASSERT(!info.Sections[1].IsInline);
        UNIT_ASSERT_VALUES_EQUAL(info.Sections[1].Size, payload.size());
        UNIT_ASSERT(info.Sections[2].IsInline);
        UNIT_ASSERT_VALUES_EQUAL(info.Sections[2].Size, 0u);

        auto handle = MakeHolder<IEventHandle>(TActorId(), TActorId(), event);
        channel.Push(*handle, pool, TInstant::Zero());

        NInterconnect::TOutgoingStream main;
        NInterconnect::TOutgoingStream xdc;
        TTcpPacketOutTask task(params, main, xdc);

        UNIT_ASSERT(channel.FeedBuf(task, 1));
    }

    Y_UNIT_TEST(EmptyProtoRecordAfterXdcPayloadAllowsNextEvent) {
        auto common = MakeIntrusive<TInterconnectProxyCommon>();
        common->MonCounters = MakeIntrusive<NMonitoring::TDynamicCounters>();

        std::shared_ptr<IInterconnectMetrics> metrics = CreateInterconnectCounters(common);
        metrics->SetPeerInfo("peer", "1", "peer");

        auto destroyCallback = [](THolder<IEventBase>) {};
        TEventHolderPool pool(common, destroyCallback);

        TSessionParams params;
        params.UseExternalDataChannel = true;
        params.UseXdcShuffle = true;

        TEventOutputChannel channel(1, 1, 64 << 20, metrics, params, nullptr);

        auto* first = new TEvEmptyRecordWithPayload;
        const TString payload(5000, 'x');
        first->AddPayload(TRope(payload));
        UNIT_ASSERT_VALUES_EQUAL(first->Record.ByteSize(), 0);

        auto* second = new TEvEmptyRecordWithPayload;
        second->Record.SetBlobID(42);
        UNIT_ASSERT_VALUES_UNEQUAL(second->Record.ByteSize(), 0);

        auto firstHandle = MakeHolder<IEventHandle>(TActorId(), TActorId(), first);
        auto secondHandle = MakeHolder<IEventHandle>(TActorId(), TActorId(), second);
        channel.Push(*firstHandle, pool, TInstant::Zero());
        channel.Push(*secondHandle, pool, TInstant::Zero());

        NInterconnect::TOutgoingStream main;
        NInterconnect::TOutgoingStream xdc;
        TTcpPacketOutTask task(params, main, xdc);

        UNIT_ASSERT(channel.FeedBuf(task, 1));
        UNIT_ASSERT(channel.FeedBuf(task, 2));
    }

}

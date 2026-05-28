#include <ydb/library/actors/interconnect/interconnect_channel.h>
#include <ydb/library/actors/interconnect/interconnect_counters.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NActors;

Y_UNIT_TEST_SUITE(InterconnectChannel) {

    SIMPLE_UNIT_FORKED_TEST(SerializedBufferWithEmptyLeadingChunk) {
        auto common = MakeIntrusive<TInterconnectProxyCommon>();
        common->MonCounters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        std::shared_ptr<IInterconnectMetrics> metrics = CreateInterconnectCounters(common);
        metrics->SetPeerInfo("peer", "1", "peer");

        TEventHolderPool pool(common, [](THolder<IEventBase>) {});
        TSessionParams params;
        TEventOutputChannel channel(1, 1, 64 << 20, metrics, params, nullptr);

        TRcBuf backing = TRcBuf::Uninitialized(1);
        TRcBuf emptyChunk(TRcBuf::Piece, backing.data(), backing.data(), backing);
        TRope payload(emptyChunk);
        payload.Insert(payload.End(), TRope(TString("payload")));
        UNIT_ASSERT_VALUES_EQUAL(payload.GetSize(), 7);

        auto event = MakeHolder<IEventHandle>(
            TEvents::TEvBlob::EventType,
            0,
            TActorId(),
            TActorId(),
            MakeIntrusive<TEventSerializedData>(std::move(payload), TEventSerializationInfo{}),
            0);

        channel.Push(*event, pool, TInstant::Zero());

        NInterconnect::TOutgoingStream stream;
        TTcpPacketOutTask task(params, stream, stream);

        UNIT_ASSERT(channel.FeedBuf(task, 1));
        UNIT_ASSERT(channel.IsEmpty());
    }

}

#include "event.h"

#include <ydb/core/testlib/actors/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NBsQueue {
namespace {

class TCountingVGet : public TEvBlobStorage::TEvVGet {
    std::shared_ptr<ui32> SerializationCount;

public:
    TCountingVGet(std::unique_ptr<TEvBlobStorage::TEvVGet> event, std::shared_ptr<ui32> serializationCount)
        : SerializationCount(std::move(serializationCount))
    {
        Record.Swap(&event->Record);
    }

    bool SerializeToArcadiaStream(TChunkSerializer* serializer) const override {
        ++*SerializationCount;
        return TEvBlobStorage::TEvVGet::SerializeToArcadiaStream(serializer);
    }
};

TEvBlobStorage::TEvVGet::TPtr MakeEvent(const TActorId& recipient, const TActorId& sender,
        const std::shared_ptr<ui32>& serializationCount) {
    auto event = TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(TVDiskID(1, 2, 3, 4, 5), TInstant::Max(),
        NKikimrBlobStorage::EGetHandleClass::AsyncRead, TEvBlobStorage::TEvVGet::EFlags::None, 123,
        {TLogoBlobID(1, 2, 3, 0, 10, 0)});
    TAutoPtr<IEventHandle> handle = new IEventHandle(recipient, sender,
        new TCountingVGet(std::move(event), serializationCount));
    return IEventHandle::Downcast<TEvBlobStorage::TEvVGet>(std::move(handle));
}

TTestActorRuntime::TEgg MakeRuntimeEgg() {
    return {new TAppData(0, 0, 0, 0, {}, nullptr, nullptr, nullptr, nullptr), nullptr, nullptr, {}, {}};
}

struct TSendState {
    ui32 SerializationsBeforeSend = 0;
    ui32 SerializationsAfterFirstSend = 0;
    ui32 SerializationsAfterSecondSend = 0;
    ui64 SerializedItemsAfterFirstSend = 0;
    ui64 SerializedItemsAfterSecondSend = 0;
    ui64 SerializedBytesAfterFirstSend = 0;
    ui64 SerializedBytesAfterSecondSend = 0;
};

class TSendActor : public TActorBootstrapped<TSendActor> {
    const TActorId Recipient;
    const std::shared_ptr<ui32> SerializationCount;
    const std::shared_ptr<TSendState> State;

public:
    TSendActor(TActorId recipient, std::shared_ptr<ui32> serializationCount, std::shared_ptr<TSendState> state)
        : Recipient(recipient)
        , SerializationCount(std::move(serializationCount))
        , State(std::move(state))
    {}

    void Bootstrap(const TActorContext& ctx) {
        auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
        auto serItems = counters->GetCounter("SerializedItems", true);
        auto serBytes = counters->GetCounter("SerializedBytes", true);
        TBSProxyContextPtr bspctx = new TBSProxyContext(counters);
        auto event = MakeEvent(ctx.SelfID, ctx.SelfID, SerializationCount);
        TEventHolder holder(event, bspctx, 0, false);
        TBSQueueTimer timer(false);

        State->SerializationsBeforeSend = *SerializationCount;
        holder.SendToVDisk(ctx, Recipient, 1, 10, 20, true, {}, NBackpressure::TQueueClientId(), timer,
            serItems, serBytes);
        State->SerializationsAfterFirstSend = *SerializationCount;
        State->SerializedItemsAfterFirstSend = *serItems;
        State->SerializedBytesAfterFirstSend = *serBytes;

        holder.SendToVDisk(ctx, Recipient, 2, 11, 21, false, {}, NBackpressure::TQueueClientId(), timer,
            serItems, serBytes);
        State->SerializationsAfterSecondSend = *SerializationCount;
        State->SerializedItemsAfterSecondSend = *serItems;
        State->SerializedBytesAfterSecondSend = *serBytes;

        PassAway();
    }
};

void CheckMessage(const TEvBlobStorage::TEvVGet& event, ui64 msgId, ui64 sequenceId,
        bool sendMeCostSettings) {
    UNIT_ASSERT_VALUES_EQUAL(event.Record.GetCookie(), 123);
    UNIT_ASSERT_VALUES_EQUAL(event.Record.ExtremeQueriesSize(), 1);
    const auto& msgQoS = event.Record.GetMsgQoS();
    UNIT_ASSERT_VALUES_EQUAL(msgQoS.GetMsgId().GetMsgId(), msgId);
    UNIT_ASSERT_VALUES_EQUAL(msgQoS.GetMsgId().GetSequenceId(), sequenceId);
    UNIT_ASSERT_VALUES_EQUAL(msgQoS.GetSendMeCostSettings(), sendMeCostSettings);
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(TEventHolderTest) {
    Y_UNIT_TEST(DoesNotSerializeRejectedEvent) {
        auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
        auto serializationCount = std::make_shared<ui32>();
        auto event = MakeEvent(TActorId(1, 1), TActorId(1, 2), serializationCount);
        {
            TEventHolder holder(event, new TBSProxyContext(counters), 0, false);

            UNIT_ASSERT_VALUES_EQUAL(*serializationCount, 0);
            std::unique_ptr<IEventBase> response(holder.MakeErrorReply(NKikimrProto::DEADLINE, "deadline exceeded",
                counters->GetCounter("DeserializedItems", true), counters->GetCounter("DeserializedBytes", true)));
            UNIT_ASSERT_VALUES_EQUAL(*serializationCount, 0);
            UNIT_ASSERT_VALUES_EQUAL(static_cast<TEvBlobStorage::TEvVGetResult&>(*response).Record.GetStatus(),
                NKikimrProto::DEADLINE);
        }
        UNIT_ASSERT_VALUES_EQUAL(*serializationCount, 0);
    }

    Y_UNIT_TEST(SerializesRemoteEventOnEverySend) {
        TTestActorRuntime runtime;
        runtime.Initialize(MakeRuntimeEgg());
        runtime.SetDispatchTimeout(TDuration::Seconds(5));
        const TActorId recipient = runtime.AllocateEdgeActor();
        auto serializationCount = std::make_shared<ui32>();
        auto state = std::make_shared<TSendState>();
        runtime.Register(new TSendActor(recipient, serializationCount, state));

        TAutoPtr<IEventHandle> firstHandle;
        const auto* first = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvVGet>(firstHandle);
        CheckMessage(*first, 10, 20, true);

        TAutoPtr<IEventHandle> secondHandle;
        const auto* second = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvVGet>(secondHandle);
        CheckMessage(*second, 11, 21, false);

        UNIT_ASSERT_VALUES_EQUAL(state->SerializationsBeforeSend, 0);
        UNIT_ASSERT_VALUES_EQUAL(state->SerializationsAfterFirstSend, 1);
        UNIT_ASSERT_VALUES_EQUAL(state->SerializationsAfterSecondSend, 2);
        UNIT_ASSERT_VALUES_EQUAL(state->SerializedItemsAfterFirstSend, 1);
        UNIT_ASSERT_VALUES_EQUAL(state->SerializedItemsAfterSecondSend, 2);
        UNIT_ASSERT_GT(state->SerializedBytesAfterFirstSend, 0);
        UNIT_ASSERT_VALUES_EQUAL(state->SerializedBytesAfterSecondSend,
            2 * state->SerializedBytesAfterFirstSend);
    }
}

} // namespace NKikimr::NBsQueue

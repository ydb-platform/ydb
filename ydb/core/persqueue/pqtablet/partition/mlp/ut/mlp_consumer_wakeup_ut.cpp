#include "mlp.h"
#include "mlp_message_enricher.h"
#include "mlp_storage.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/keyvalue/keyvalue_events.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/protos/grpc_pq_old.pb.h>
#include <ydb/core/protos/msgbus.pb.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/events.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NPQ::NMLP {

namespace {

constexpr ui64 kTabletId = 100;
constexpr const char* kConsumer = "mlp-consumer";
constexpr const char* kTopic = "topic";

// Must match EWakeUpTag in mlp_consumer.cpp.
constexpr ui64 kWakeupRegular = 1;
constexpr ui64 kWakeupProcessing = 2;

NKikimrPQ::TPQTabletConfig MakeTopicConfig() {
    NKikimrPQ::TPQTabletConfig config;
    config.SetTopicName(kTopic);
    config.SetTopicPath("/Root/topic");

    auto* partition = config.AddAllPartitions();
    partition->SetPartitionId(0);
    partition->SetTabletId(kTabletId);
    partition->SetStatus(NKikimrPQ::ETopicPartitionStatus::Active);

    auto* consumer = config.AddConsumers();
    consumer->SetName(kConsumer);
    consumer->SetType(NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP);
    consumer->SetKeepMessageOrder(false);
    consumer->SetGeneration(1);
    return config;
}

NKikimrPQ::TPQTabletConfig::TConsumer MakeConsumerConfig() {
    NKikimrPQ::TPQTabletConfig::TConsumer consumer;
    consumer.SetName(kConsumer);
    consumer.SetType(NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP);
    consumer.SetKeepMessageOrder(false);
    consumer.SetGeneration(1);
    return consumer;
}

THolder<TEvKeyValue::TEvResponse> MakeEmptySnapshotResponse(ui64 cookie) {
    auto response = MakeHolder<TEvKeyValue::TEvResponse>();
    response->Record.SetStatus(NMsgBusProxy::MSTATUS_OK);
    response->Record.SetCookie(cookie);
    response->Record.AddReadResult()->SetStatus(NKikimrProto::NODATA);
    response->Record.AddReadRangeResult()->SetStatus(NKikimrProto::NODATA);
    return response;
}

THolder<TEvKeyValue::TEvResponse> MakeKvWriteOk(ui64 cookie) {
    auto response = MakeHolder<TEvKeyValue::TEvResponse>();
    response->Record.SetStatus(NMsgBusProxy::MSTATUS_OK);
    response->Record.SetCookie(cookie);
    response->Record.AddWriteResult()->SetStatus(NKikimrProto::OK);
    return response;
}

THolder<TEvPersQueue::TEvResponse> MakeFetchResponse(ui64 firstOffset, ui64 count, TInstant writeTimestamp) {
    auto response = MakeHolder<TEvPersQueue::TEvResponse>();
    response->Record.SetStatus(NMsgBusProxy::MSTATUS_OK);
    response->Record.SetErrorCode(NPersQueue::NErrorCode::OK);

    NKikimrPQClient::TDataChunk chunk;
    chunk.SetChunkType(NKikimrPQClient::TDataChunk::REGULAR);
    const auto data = chunk.SerializeAsString();

    for (ui64 i = 0; i < count; ++i) {
        auto* result = response->Record.MutablePartitionResponse()->MutableCmdReadResult()->AddResult();
        result->SetOffset(firstOffset + i);
        result->SetLogicalMessageCount(1);
        result->SetWriteTimestampMS(writeTimestamp.MilliSeconds());
        result->SetData(data);
    }
    return response;
}

struct TPipeCacheState {
    bool HoldForwards = false;
    TActorId Actor;
};

// Answers enricher reads so the consumer can wait for TEvMLPEnricherFinished.
// HoldForwards keeps the read until TEvWakeup, which is how the queue-gate test
// observes that processing does not continue while the enricher is in flight.
class TPipeCacheStub : public TActorBootstrapped<TPipeCacheStub> {
public:
    explicit TPipeCacheStub(TPipeCacheState* state)
        : State(state)
    {
    }

    void Bootstrap() {
        State->Actor = SelfId();
        Become(&TThis::StateWork);
    }

    void ReplyForward(TEvPipeCache::TEvForward::TPtr& ev) {
        ui64 offset = 0;
        if (ev->Get()->Ev && ev->Get()->Ev->Type() == TEvPersQueue::TEvRequest::EventType) {
            const auto* request = static_cast<const TEvPersQueue::TEvRequest*>(ev->Get()->Ev.Get());
            if (request->Record.GetPartitionRequest().HasCmdRead()) {
                offset = request->Record.GetPartitionRequest().GetCmdRead().GetOffset();
            }
        }
        Send(ev->Sender, MakeFetchResponse(offset, 1, TInstant::MilliSeconds(1)).Release());
    }

    void HandleForward(TEvPipeCache::TEvForward::TPtr& ev) {
        if (State->HoldForwards) {
            Held.push_back(std::move(ev));
            return;
        }
        ReplyForward(ev);
    }

    void ReleaseHeld() {
        State->HoldForwards = false;
        auto held = std::exchange(Held, {});
        for (auto& ev : held) {
            ReplyForward(ev);
        }
    }

    STRICT_STFUNC(StateWork,
        hFunc(TEvPipeCache::TEvForward, HandleForward);
        IgnoreFunc(TEvPipeCache::TEvUnlink);
        sFunc(TEvents::TEvWakeup, ReleaseHeld);
    )

private:
    TPipeCacheState* State;
    std::deque<TEvPipeCache::TEvForward::TPtr> Held;
};

struct TConsumerEnv {
    TPipeCacheState PipeState;
    TTestBasicRuntime Runtime;
    TActorId Tablet;
    TActorId Partition;
    TActorId Reader;
    TActorId Consumer;
    NMonitoring::TDynamicCounterPtr Counters;
    ui32 RegularWakeupsAllowed = 0;
    bool DropProcessingWakeups = false;
    bool CountKvRequests = false;
    ui32 KvRequestsSeen = 0;

    TConsumerEnv()
        : Runtime(1, false)
        , Counters(new NMonitoring::TDynamicCounters())
    {
        Runtime.Initialize(TAppPrepare().Unwrap());
        Runtime.SetScheduledLimit(10000);
        Runtime.SetLogPriority(NKikimrServices::PQ_MLP_CONSUMER, NLog::PRI_DEBUG);

        auto pipeCache = Runtime.Register(new TPipeCacheStub(&PipeState));
        Runtime.EnableScheduleForActor(pipeCache);
        Runtime.RegisterService(MakePipePerNodeCacheID(false), pipeCache);

        Tablet = Runtime.AllocateEdgeActor();
        Partition = Runtime.AllocateEdgeActor();
        Reader = Runtime.AllocateEdgeActor();

        Runtime.SetObserverFunc([this](TAutoPtr<IEventHandle>& ev) {
            if (CountKvRequests && ev->GetTypeRewrite() == TEvKeyValue::TEvRequest::EventType) {
                ++KvRequestsSeen;
            }
            if (ev->GetTypeRewrite() == TEvents::TEvWakeup::EventType) {
                const auto* wakeup = ev->Get<TEvents::TEvWakeup>();
                if (!wakeup) {
                    return TTestActorRuntime::EEventAction::PROCESS;
                }
                // Collapsed test time would fire the enricher deadline as soon as the
                // queue is idle. These tests hold that read on purpose.
                if (wakeup->Tag == MessageEnricherDeadlineWakeupTag) {
                    return TTestActorRuntime::EEventAction::DROP;
                }
                if (wakeup->Tag == kWakeupRegular) {
                    if (RegularWakeupsAllowed == 0) {
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                    --RegularWakeupsAllowed;
                } else if (wakeup->Tag == kWakeupProcessing && DropProcessingWakeups) {
                    return TTestActorRuntime::EEventAction::DROP;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        Consumer = Runtime.Register(CreateConsumerActor(
            "/Root",
            kTabletId,
            Tablet,
            0,
            Partition,
            1,
            MakeTopicConfig(),
            MakeConsumerConfig(),
            std::nullopt,
            0,
            Counters
        ));
        Runtime.EnableScheduleForActor(Consumer);

        auto kvReq = Runtime.GrabEdgeEvent<TEvKeyValue::TEvRequest>(TDuration::Seconds(5));
        UNIT_ASSERT(kvReq);
        Runtime.Send(new IEventHandle(Consumer, Tablet, MakeEmptySnapshotResponse(kvReq->Record.GetCookie()).Release()));
    }

    TInstant Now() {
        return Runtime.GetTimeProvider()->Now();
    }

    void Pump() {
        ui32 cycles = 0;
        TDispatchOptions options;
        options.CustomFinalCondition = [&cycles] { return ++cycles >= 20; };
        Runtime.DispatchEvents(options);
    }

    THolder<TEvKeyValue::TEvRequest> GrabKvRequest(TDuration timeout = TDuration::Seconds(5)) {
        auto kvReq = Runtime.GrabEdgeEvent<TEvKeyValue::TEvRequest>(timeout);
        UNIT_ASSERT(kvReq);
        return kvReq;
    }

    void ReplyKv(const TEvKeyValue::TEvRequest& kvReq, bool pump = true) {
        Runtime.Send(new IEventHandle(Consumer, Tablet, MakeKvWriteOk(kvReq.Record.GetCookie()).Release()));
        if (pump) {
            Pump();
        }
    }

    void ReplyKvWrite() {
        ReplyKv(*GrabKvRequest());
    }

    void FetchMessages(ui64 count, ui64 firstOffset = 0) {
        Runtime.Send(new IEventHandle(Consumer, Tablet, new TEvPQ::TEvEndOffsetChanged(firstOffset + count)));
        auto fetch = Runtime.GrabEdgeEvent<TEvPersQueue::TEvRequest>(TDuration::Seconds(5));
        UNIT_ASSERT(fetch);
        Runtime.Send(new IEventHandle(Consumer, Tablet, MakeFetchResponse(firstOffset, count, Now()).Release()));
        ReplyKvWrite();
    }

    void FetchOneMessage() {
        FetchMessages(1);
    }

    void SendRead(TDuration wait, TDuration processingTimeout) {
        Runtime.Send(new IEventHandle(
            Consumer,
            Reader,
            new TEvPQ::TEvMLPReadRequest(
                kTopic,
                kConsumer,
                0,
                Now() + wait,
                processingTimeout,
                1,
                {})));
    }

    void SendCommit(ui64 offset) {
        Runtime.Send(new IEventHandle(
            Consumer,
            Reader,
            new TEvPQ::TEvMLPCommitRequest(kTopic, kConsumer, 0, {offset})));
    }

    void SendUnlock(ui64 offset) {
        Runtime.Send(new IEventHandle(
            Consumer,
            Reader,
            new TEvPQ::TEvMLPUnlockRequest(kTopic, kConsumer, 0, {offset})));
    }

    void ReleaseEnricher() {
        PipeState.HoldForwards = false;
        Runtime.Send(new IEventHandle(PipeState.Actor, Consumer, new TEvents::TEvWakeup()));
        Pump();
    }

    void AssertNoPersistWhileEnricherHeld(const char* message) {
        KvRequestsSeen = 0;
        CountKvRequests = true;
        Pump();
        Runtime.SimulateSleep(TDuration::MilliSeconds(200));
        Pump();
        UNIT_ASSERT_VALUES_EQUAL_C(KvRequestsSeen, 0u, message);
        CountKvRequests = false;
    }

    void SendWakeup(ui64 tag) {
        Runtime.Send(new IEventHandle(Consumer, Consumer, new TEvents::TEvWakeup(tag)), 0, true);
    }

    void DeliverProcessing() {
        SendWakeup(kWakeupProcessing);
        Pump();
    }

    THolder<TEvPQ::TEvGetMLPConsumerStateResponse> GetState() {
        Runtime.Send(new IEventHandle(Consumer, Reader, new TEvPQ::TEvGetMLPConsumerStateRequest(kTopic, kConsumer, 0)));
        auto state = Runtime.GrabEdgeEvent<TEvPQ::TEvGetMLPConsumerStateResponse>(TDuration::Seconds(5));
        UNIT_ASSERT(state);
        return state;
    }

    void AssertStatus(ui64 offset, TStorage::EMessageStatus status) {
        auto state = GetState();
        for (const auto& message : state->Messages) {
            if (message.Offset == offset) {
                UNIT_ASSERT_VALUES_EQUAL(message.Status, static_cast<ui32>(status));
                return;
            }
        }
        // Compact drops a committed prefix, so a successful commit may
        // disappear from the in-memory iterator.
        if (status == TStorage::EMessageStatus::Committed) {
            return;
        }
        UNIT_FAIL("offset not found in consumer state");
    }

    THolder<TEvKeyValue::TEvRequest> ExpectPersist(const char* message) {
        Runtime.SetDispatchTimeout(TDuration::Seconds(3));
        auto kv = Runtime.GrabEdgeEvent<TEvKeyValue::TEvRequest>(TDuration::Seconds(2));
        UNIT_ASSERT_C(kv, message);
        return kv;
    }

    void AssertPersisted(ui64 offset, TStorage::EMessageStatus status, const char* message) {
        auto kv = ExpectPersist(message);
        AssertStatus(offset, status);
        ReplyKv(*kv);
    }

    void AssertQueueStillProcessesAfterCommit(ui64 committedOffset, ui64 nextOffset) {
        AssertPersisted(committedOffset, TStorage::EMessageStatus::Committed,
            "queued commit must be applied");
        DropProcessingWakeups = false;
        SendRead(TDuration::Seconds(60), TDuration::Seconds(30));
        AssertPersisted(nextOffset, TStorage::EMessageStatus::Locked,
            "queue must still serve a later read");
    }

    void AssertQueueStillProcessesAfterUnlock(ui64 offset) {
        AssertPersisted(offset, TStorage::EMessageStatus::Unprocessed,
            "queued unlock must be applied");
        DropProcessingWakeups = false;
        SendRead(TDuration::Seconds(60), TDuration::Seconds(30));
        AssertPersisted(offset, TStorage::EMessageStatus::Locked,
            "queue must still lock the message after unlock");
    }
};

} // namespace

Y_UNIT_TEST_SUITE(TMLPConsumerWakeupTests) {

Y_UNIT_TEST(RegularPersistDoesNotStickProcessingScheduled) {
    TConsumerEnv env;
    env.FetchOneMessage();

    env.SendRead(TDuration::Seconds(60), TDuration::Seconds(1));
    env.ReplyKvWrite();

    // Leave a long-poll in ReadRequestsQueue so ScheduleProcessing() after Persist()
    // does not early-return: one waiter takes the unlocked message, the other stays queued.
    env.SendRead(TDuration::Seconds(60), TDuration::Seconds(30));
    env.SendRead(TDuration::Seconds(60), TDuration::Seconds(30));
    env.Pump();

    // Wait for the visibility timeout of the locked message and for the storage vacuum
    // interval. Storage takes time from AppData, so simulated time is enough.
    env.Runtime.SimulateSleep(TDuration::Seconds(3));

    env.RegularWakeupsAllowed = 1;
    env.DropProcessingWakeups = true;
    env.SendWakeup(kWakeupRegular);

    auto unlockKv = env.Runtime.GrabEdgeEvent<TEvKeyValue::TEvRequest>(TDuration::Seconds(5));
    UNIT_ASSERT_C(unlockKv, "Regular wakeup must persist expired visibility locks");

    env.DropProcessingWakeups = false;
    env.DeliverProcessing();

    env.ReplyKv(*unlockKv, false);

    env.SendCommit(0);
    env.AssertPersisted(0, TStorage::EMessageStatus::Committed,
        "ProcessingScheduled stayed true after Processing wakeup in StateWrite; later requests never persist");

    // Fetch does not persist by itself; the next Processing wakeup writes the new
    // message together with the lock. Do not consume that KV before the assert.
    env.SendRead(TDuration::Seconds(60), TDuration::Seconds(30));
    env.Runtime.Send(new IEventHandle(env.Consumer, env.Tablet, new TEvPQ::TEvEndOffsetChanged(2)));
    auto fetch = env.Runtime.GrabEdgeEvent<TEvPersQueue::TEvRequest>(TDuration::Seconds(5));
    UNIT_ASSERT(fetch);
    env.Runtime.Send(new IEventHandle(
        env.Consumer,
        env.Tablet,
        MakeFetchResponse(1, 1, env.Now()).Release()));
    env.AssertPersisted(1, TStorage::EMessageStatus::Locked,
        "queue must still lock a later message after the stuck ProcessingScheduled race");
}

Y_UNIT_TEST(RegularServesQueuedReadAfterVisibilityTimeout) {
    TConsumerEnv env;
    env.FetchOneMessage();

    env.SendRead(TDuration::Seconds(60), TDuration::Seconds(1));
    env.ReplyKvWrite();

    // The message is locked, so the second read stays in ReadRequestsQueue. Nothing
    // schedules a wakeup for the visibility timeout, so the Regular tick is the only
    // event that unlocks the message and serves the waiting read.
    env.SendRead(TDuration::Seconds(60), TDuration::Seconds(30));
    env.Pump();

    env.Runtime.SimulateSleep(TDuration::Seconds(3));

    env.RegularWakeupsAllowed = 1;
    env.SendWakeup(kWakeupRegular);

    env.AssertPersisted(0, TStorage::EMessageStatus::Locked,
        "Regular wakeup must unlock the expired message and serve the queued read");

    env.SendUnlock(0);
    env.AssertQueueStillProcessesAfterUnlock(0);
}

Y_UNIT_TEST(DelayedProcessingAfterKvReplyPersistsQueuedCommit) {
    TConsumerEnv env;
    env.FetchMessages(2);

    env.SendRead(TDuration::Seconds(60), TDuration::Seconds(30));
    env.ReplyKvWrite();

    env.DropProcessingWakeups = true;
    env.SendCommit(0);
    env.Pump();

    env.DropProcessingWakeups = false;
    env.SendWakeup(kWakeupProcessing);

    env.AssertQueueStillProcessesAfterCommit(0, 1);
}

Y_UNIT_TEST(CommitQueuedDuringWriteIsPersistedAfterKv) {
    TConsumerEnv env;
    env.FetchMessages(2);

    env.SendRead(TDuration::Seconds(60), TDuration::Seconds(30));
    auto lockKv = env.GrabKvRequest();
    env.SendCommit(0);
    env.ReplyKv(*lockKv);

    env.AssertQueueStillProcessesAfterCommit(0, 1);
}

Y_UNIT_TEST(UnlockQueuedDuringWriteIsPersistedAfterKv) {
    TConsumerEnv env;
    env.FetchOneMessage();

    env.SendRead(TDuration::Seconds(60), TDuration::Seconds(30));
    auto lockKv = env.GrabKvRequest();
    env.SendUnlock(0);
    env.ReplyKv(*lockKv);

    env.AssertQueueStillProcessesAfterUnlock(0);
}

Y_UNIT_TEST(ReadQueuedDuringWriteLocksNextMessageAfterKv) {
    TConsumerEnv env;
    env.FetchMessages(3);

    env.SendRead(TDuration::Seconds(60), TDuration::Seconds(30));
    auto firstLockKv = env.GrabKvRequest();
    env.SendRead(TDuration::Seconds(60), TDuration::Seconds(30));
    env.ReplyKv(*firstLockKv);

    env.AssertPersisted(1, TStorage::EMessageStatus::Locked,
        "A read queued in StateWrite must lock the next message after KV completes");
    env.AssertStatus(0, TStorage::EMessageStatus::Locked);

    env.SendCommit(0);
    env.AssertQueueStillProcessesAfterCommit(0, 1);
}

Y_UNIT_TEST(DelayedFetchServesAlreadyQueuedRead) {
    TConsumerEnv env;
    env.SendRead(TDuration::Seconds(60), TDuration::Seconds(30));
    env.Pump();

    env.FetchMessages(2);
    env.AssertStatus(0, TStorage::EMessageStatus::Locked);

    env.SendCommit(0);
    env.AssertQueueStillProcessesAfterCommit(0, 1);
}

Y_UNIT_TEST(RegularDuringWriteDoesNotDropQueuedCommit) {
    TConsumerEnv env;
    env.FetchMessages(2);

    env.SendRead(TDuration::Seconds(60), TDuration::Seconds(30));
    auto lockKv = env.GrabKvRequest();
    env.SendCommit(0);

    env.RegularWakeupsAllowed = 1;
    env.SendWakeup(kWakeupRegular);
    env.Pump();

    env.ReplyKv(*lockKv);

    env.AssertQueueStillProcessesAfterCommit(0, 1);
}

Y_UNIT_TEST(QueueWaitsUntilEnricherFinishes) {
    TConsumerEnv env;
    env.FetchMessages(2);
    env.PipeState.HoldForwards = true;

    env.SendRead(TDuration::Seconds(60), TDuration::Seconds(30));
    env.ReplyKvWrite();

    env.SendCommit(0);
    env.AssertNoPersistWhileEnricherHeld("commit must wait until the enricher finishes");
    env.AssertStatus(0, TStorage::EMessageStatus::Locked);

    env.ReleaseEnricher();
    env.AssertPersisted(0, TStorage::EMessageStatus::Committed,
        "commit is applied after the enricher finishes");
}

Y_UNIT_TEST(UnlockWaitsUntilEnricherFinishes) {
    TConsumerEnv env;
    env.FetchOneMessage();
    env.PipeState.HoldForwards = true;

    env.SendRead(TDuration::Seconds(60), TDuration::Seconds(30));
    env.ReplyKvWrite();

    env.SendUnlock(0);
    env.AssertNoPersistWhileEnricherHeld("unlock must wait until the enricher finishes");
    env.AssertStatus(0, TStorage::EMessageStatus::Locked);

    env.ReleaseEnricher();
    env.AssertPersisted(0, TStorage::EMessageStatus::Unprocessed,
        "unlock is applied after the enricher finishes");
}

Y_UNIT_TEST(NextReadWaitsUntilEnricherFinishes) {
    TConsumerEnv env;
    env.FetchMessages(2);
    env.PipeState.HoldForwards = true;

    env.SendRead(TDuration::Seconds(60), TDuration::Seconds(30));
    env.ReplyKvWrite();

    env.SendRead(TDuration::Seconds(60), TDuration::Seconds(30));
    env.AssertNoPersistWhileEnricherHeld("the next read must wait until the enricher finishes");
    env.AssertStatus(0, TStorage::EMessageStatus::Locked);
    env.AssertStatus(1, TStorage::EMessageStatus::Unprocessed);

    env.ReleaseEnricher();
    env.AssertPersisted(1, TStorage::EMessageStatus::Locked,
        "the next read locks a message after the enricher finishes");
}

Y_UNIT_TEST(VisibilityTimeoutWaitsForEnricher) {
    TConsumerEnv env;
    env.FetchOneMessage();
    env.PipeState.HoldForwards = true;

    env.SendRead(TDuration::Seconds(60), TDuration::Seconds(1));
    env.ReplyKvWrite();

    env.Runtime.SimulateSleep(TDuration::Seconds(3));
    env.RegularWakeupsAllowed = 1;
    env.KvRequestsSeen = 0;
    env.CountKvRequests = true;
    env.SendWakeup(kWakeupRegular);
    env.Pump();
    env.Runtime.SimulateSleep(TDuration::MilliSeconds(200));
    env.Pump();
    UNIT_ASSERT_VALUES_EQUAL_C(env.KvRequestsSeen, 0u, "expired visibility must wait until the enricher finishes");
    env.CountKvRequests = false;
    env.AssertStatus(0, TStorage::EMessageStatus::Locked);

    env.ReleaseEnricher();
    env.AssertPersisted(0, TStorage::EMessageStatus::Unprocessed,
        "expired visibility is applied after the enricher finishes");
}

Y_UNIT_TEST(EmptyReadDoesNotBlockQueue) {
    TConsumerEnv env;
    env.PipeState.HoldForwards = true;

    env.SendRead(TDuration::Zero(), TDuration::Seconds(30));
    env.Pump();

    env.FetchOneMessage();
    env.SendCommit(0);
    env.AssertPersisted(0, TStorage::EMessageStatus::Committed,
        "an empty read must not wait for an enricher");
}

Y_UNIT_TEST(StaleProcessingInWriteThenKvPersistsCommit) {
    TConsumerEnv env;
    env.FetchMessages(2);

    env.SendRead(TDuration::Seconds(60), TDuration::Seconds(30));
    auto lockKv = env.GrabKvRequest();
    env.DeliverProcessing();
    env.DeliverProcessing();
    env.ReplyKv(*lockKv);

    env.SendCommit(0);
    env.AssertQueueStillProcessesAfterCommit(0, 1);
}

} // Y_UNIT_TEST_SUITE(TMLPConsumerWakeupTests)

} // namespace NKikimr::NPQ::NMLP

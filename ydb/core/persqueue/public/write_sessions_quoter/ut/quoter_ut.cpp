#include <ydb/core/persqueue/public/write_sessions_quoter/quoter.h>

#include <ydb/core/base/appdata.h>
#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/testlib/actors/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NPQ {
namespace {

using namespace NActors;
using TEvQuoter = TEvWriteSessionsQuoter;

struct TBucketKey {
    TString Topic = "/Root/topic";
    ui32 Partition = 0;
    ui32 Generation = 1;
};

class TWriteSessionsQuoterTest : public NUnitTest::TBaseFixture {
public:
    void SetUp(NUnitTest::TTestContext&) override;

protected:
    void Notify(const TBucketKey& key = {});
    void Remove(const TBucketKey& key = {});
    TActorId Acquire(const TBucketKey& key = {});
    void ExpectReplies(const TActorId& actor, size_t count);
    void WakeupAfter(TDuration elapsed);
    void CheckIndependentBucket(const TBucketKey& other);

    TTestActorRuntime Runtime;
    TActorId Quoter;
};

void TWriteSessionsQuoterTest::SetUp(NUnitTest::TTestContext&) {
    TTestActorRuntime::TEgg egg;
    egg.App0 = new TAppData(0, 0, 0, 0, {}, nullptr, nullptr, nullptr, nullptr);
    egg.App0->PQConfig.SetWriteSessionsInitRps(1);
    Runtime.Initialize(std::move(egg));
    Runtime.SetScheduledEventFilter([](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event, TDuration, TInstant&) {
        return event->GetTypeRewrite() != TEvents::TEvWakeup::EventType;
    });
    Quoter = Runtime.Register(CreateWriteSessionsQuoter());
    Runtime.RegisterService(MakeWriteSessionsQuoterId(), Quoter);
    TDispatchOptions options;
    options.FinalEvents.emplace_back([this](IEventHandle& event) {
        return event.GetRecipientRewrite() == Quoter && event.GetTypeRewrite() == TEvents::TSystem::Bootstrap;
    });
    UNIT_ASSERT(Runtime.DispatchEvents(options, TDuration::Seconds(1)));
}

void TWriteSessionsQuoterTest::Notify(const TBucketKey& key) {
    const auto edge = Runtime.AllocateEdgeActor();
    Runtime.Send(new IEventHandle(MakeWriteSessionsQuoterId(), edge,
        new TEvQuoter::TEvNotify(key.Topic, key.Partition, key.Generation)));
    auto reply = Runtime.GrabEdgeEvent<TEvQuoter::TEvQuoterInitialized>(edge, TDuration::MilliSeconds(10));
    UNIT_ASSERT(reply);
    UNIT_ASSERT_VALUES_EQUAL(reply->Sender, Quoter);
}

void TWriteSessionsQuoterTest::Remove(const TBucketKey& key) {
    Runtime.Send(new IEventHandle(Quoter, Runtime.AllocateEdgeActor(),
        new TEvQuoter::TEvRemove(key.Topic, key.Partition, key.Generation)));
}

TActorId TWriteSessionsQuoterTest::Acquire(const TBucketKey& key) {
    const auto edge = Runtime.AllocateEdgeActor();
    // Deliver synchronously so negative assertions do not advance virtual time.
    Runtime.Send(new IEventHandle(Quoter, edge,
        new TEvQuoter::TEvAcquireQuota(key.Topic, key.Partition, key.Generation)));
    return edge;
}

void TWriteSessionsQuoterTest::ExpectReplies(const TActorId& actor, size_t count) {
    auto events = Runtime.CaptureMailboxEvents(actor.Hint(), actor.NodeId());
    UNIT_ASSERT_VALUES_EQUAL(events.size(), count);
    for (const auto& event : events) {
        UNIT_ASSERT_VALUES_EQUAL(event->GetTypeRewrite(), TEvQuoter::TEvQuotaAcquired::EventType);
        UNIT_ASSERT_VALUES_EQUAL(event->Sender, Quoter);
        UNIT_ASSERT_VALUES_EQUAL(event->Recipient, actor);
    }
}

void TWriteSessionsQuoterTest::WakeupAfter(TDuration elapsed) {
    Runtime.AdvanceCurrentTime(elapsed);
    Runtime.Send(new IEventHandle(Quoter, Runtime.AllocateEdgeActor(), new TEvents::TEvWakeup()));
}

void TWriteSessionsQuoterTest::CheckIndependentBucket(const TBucketKey& other) {
    Notify();
    ExpectReplies(Acquire(), 1);
    const auto pending = Acquire();
    ExpectReplies(pending, 0);

    Notify(other);
    ExpectReplies(Acquire(other), 1);
    ExpectReplies(Acquire(other), 0);
    ExpectReplies(pending, 0);
}

Y_UNIT_TEST_SUITE(TWriteSessionsQuoterTests) {

Y_UNIT_TEST_F(AcknowledgesNotificationViaLocalService, TWriteSessionsQuoterTest) {
    Notify();
}

Y_UNIT_TEST_F(GrantsOnlyConfiguredInitialBurst, TWriteSessionsQuoterTest) {
    Runtime.GetAppData().PQConfig.SetWriteSessionsInitRps(2);
    Notify();
    ExpectReplies(Acquire(), 1);
    ExpectReplies(Acquire(), 1);
    ExpectReplies(Acquire(), 0);
}

Y_UNIT_TEST_F(TopicsHaveIndependentQuota, TWriteSessionsQuoterTest) {
    CheckIndependentBucket({"/Root/other", 0, 1});
}

Y_UNIT_TEST_F(PartitionsHaveIndependentQuota, TWriteSessionsQuoterTest) {
    CheckIndependentBucket({"/Root/topic", 1, 1});
}

Y_UNIT_TEST_F(GenerationsHaveIndependentQuota, TWriteSessionsQuoterTest) {
    CheckIndependentBucket({"/Root/topic", 0, 2});
}

Y_UNIT_TEST_F(RepeatedNotifyDoesNotResetQuota, TWriteSessionsQuoterTest) {
    Notify();
    ExpectReplies(Acquire(), 1);
    const auto pending = Acquire();
    Notify();
    ExpectReplies(Acquire(), 0);
    ExpectReplies(pending, 0);
}

Y_UNIT_TEST_F(WakeupWithoutElapsedTimeDoesNotGrantQuota, TWriteSessionsQuoterTest) {
    Notify();
    ExpectReplies(Acquire(), 1);
    const auto pending = Acquire();
    WakeupAfter(TDuration::Zero());
    ExpectReplies(pending, 0);
}

Y_UNIT_TEST_F(WakeupsDrainQueueInFifoOrder, TWriteSessionsQuoterTest) {
    Notify();
    ExpectReplies(Acquire(), 1);
    const auto first = Acquire();
    const auto second = Acquire();
    const auto third = Acquire();
    ExpectReplies(first, 0);
    ExpectReplies(second, 0);
    ExpectReplies(third, 0);

    WakeupAfter(TDuration::Seconds(1));
    ExpectReplies(first, 1);
    ExpectReplies(second, 0);
    ExpectReplies(third, 0);

    WakeupAfter(TDuration::Seconds(1));
    ExpectReplies(first, 0);
    ExpectReplies(second, 1);
    ExpectReplies(third, 0);

    WakeupAfter(TDuration::Seconds(1));
    ExpectReplies(first, 0);
    ExpectReplies(second, 0);
    ExpectReplies(third, 1);
}

Y_UNIT_TEST_F(ScheduledWakeupsDrainQueueWithoutNewRequests, TWriteSessionsQuoterTest) {
    Notify();
    ExpectReplies(Acquire(), 1);
    const auto first = Acquire();
    const auto second = Acquire();

    Runtime.SimulateSleep(TDuration::MilliSeconds(1100));
    ExpectReplies(first, 1);
    ExpectReplies(second, 0);

    Runtime.SimulateSleep(TDuration::Seconds(1));
    ExpectReplies(first, 0);
    ExpectReplies(second, 1);
}

Y_UNIT_TEST_F(RefillIsProportionalToElapsedTime, TWriteSessionsQuoterTest) {
    Runtime.GetAppData().PQConfig.SetWriteSessionsInitRps(2);
    Notify();
    ExpectReplies(Acquire(), 1);
    ExpectReplies(Acquire(), 1);
    const auto first = Acquire();
    const auto second = Acquire();

    WakeupAfter(TDuration::MilliSeconds(500));
    ExpectReplies(first, 1);
    ExpectReplies(second, 0);

    WakeupAfter(TDuration::MilliSeconds(500));
    ExpectReplies(first, 0);
    ExpectReplies(second, 1);
}

Y_UNIT_TEST_F(LongIdleDoesNotAccumulateMoreThanBucketCapacity, TWriteSessionsQuoterTest) {
    Runtime.GetAppData().PQConfig.SetWriteSessionsInitRps(2);
    Notify();
    ExpectReplies(Acquire(), 1);
    ExpectReplies(Acquire(), 1);
    const auto first = Acquire();
    const auto second = Acquire();
    const auto third = Acquire();

    WakeupAfter(TDuration::Seconds(10));
    ExpectReplies(first, 1);
    ExpectReplies(second, 1);
    ExpectReplies(third, 0);
    ExpectReplies(Acquire(), 0);
}

Y_UNIT_TEST_F(RemoveAllowsFreshRegistration, TWriteSessionsQuoterTest) {
    Notify();
    ExpectReplies(Acquire(), 1);
    Remove();
    Notify();
    ExpectReplies(Acquire(), 1);
    ExpectReplies(Acquire(), 0);
}

Y_UNIT_TEST_F(RemoveIsIdempotentAndAcceptsUnknownKeys, TWriteSessionsQuoterTest) {
    Remove();
    Notify();
    ExpectReplies(Acquire(), 1);
    Remove();
    Remove();
    Notify();
    ExpectReplies(Acquire(), 1);
}

Y_UNIT_TEST_F(RemoveOldGenerationPreservesNewGenerationQuota, TWriteSessionsQuoterTest) {
    const TBucketKey nextGeneration{"/Root/topic", 0, 2};
    Notify();
    Notify(nextGeneration);
    ExpectReplies(Acquire(), 1);
    ExpectReplies(Acquire(nextGeneration), 1);

    Remove();
    Notify(nextGeneration);
    ExpectReplies(Acquire(nextGeneration), 0);
    Notify();
    ExpectReplies(Acquire(), 1);
}

Y_UNIT_TEST_F(RemoveDoesNotAffectOtherTopicsOrPartitions, TWriteSessionsQuoterTest) {
    const TBucketKey otherTopic{"/Root/other", 0, 1};
    const TBucketKey otherPartition{"/Root/topic", 1, 1};
    Notify();
    Notify(otherTopic);
    Notify(otherPartition);
    ExpectReplies(Acquire(otherTopic), 1);
    ExpectReplies(Acquire(otherPartition), 1);

    Remove();
    Notify(otherTopic);
    Notify(otherPartition);
    ExpectReplies(Acquire(otherTopic), 0);
    ExpectReplies(Acquire(otherPartition), 0);
}

} // Y_UNIT_TEST_SUITE

} // namespace
} // namespace NKikimr::NPQ

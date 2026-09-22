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
    void AcquireFrom(const TActorId& actor, const TBucketKey& key = {});
    void ExpectReplies(const TActorId& actor, size_t count, ui32 eventType = TEvQuoter::TEvQuotaAcquired::EventType);
    void ExpectDeclined(const TActorId& actor);
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
    TDispatchOptions options;
    options.FinalEvents.emplace_back([edge](IEventHandle& event) {
        return event.Sender == edge && event.GetTypeRewrite() == TEvQuoter::TEvNotify::EventType;
    });
    UNIT_ASSERT(Runtime.DispatchEvents(options, TDuration::Seconds(1)));
    ExpectReplies(edge, 0);
}

void TWriteSessionsQuoterTest::Remove(const TBucketKey& key) {
    Runtime.Send(new IEventHandle(Quoter, Runtime.AllocateEdgeActor(),
        new TEvQuoter::TEvRemove(key.Topic, key.Partition, key.Generation)));
}

TActorId TWriteSessionsQuoterTest::Acquire(const TBucketKey& key) {
    const auto edge = Runtime.AllocateEdgeActor();
    AcquireFrom(edge, key);
    return edge;
}

void TWriteSessionsQuoterTest::AcquireFrom(const TActorId& actor, const TBucketKey& key) {
    // Deliver synchronously so negative assertions do not advance virtual time.
    Runtime.Send(new IEventHandle(Quoter, actor,
        new TEvQuoter::TEvAcquireQuota(key.Topic, key.Partition, key.Generation)));
}

void TWriteSessionsQuoterTest::ExpectReplies(const TActorId& actor, size_t count, ui32 eventType) {
    auto events = Runtime.CaptureMailboxEvents(actor.Hint(), actor.NodeId());
    UNIT_ASSERT_VALUES_EQUAL(events.size(), count);
    for (const auto& event : events) {
        UNIT_ASSERT_VALUES_EQUAL(event->GetTypeRewrite(), eventType);
        UNIT_ASSERT_VALUES_EQUAL(event->Sender, Quoter);
        UNIT_ASSERT_VALUES_EQUAL(event->Recipient, actor);
    }
}

void TWriteSessionsQuoterTest::ExpectDeclined(const TActorId& actor) {
    ExpectReplies(actor, 1, TEvQuoter::TEvQuotaDeclined::EventType);
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

Y_UNIT_TEST_F(RegistersBucketViaLocalServiceWithoutReply, TWriteSessionsQuoterTest) {
    Notify();
    ExpectReplies(Acquire(), 1);
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

Y_UNIT_TEST_F(UnknownKeyIsDeclinedWithoutCreatingBucket, TWriteSessionsQuoterTest) {
    ExpectDeclined(Acquire());
    WakeupAfter(TDuration::Seconds(1));
    ExpectDeclined(Acquire());
    Notify();
    ExpectReplies(Acquire(), 1);
}

Y_UNIT_TEST_F(RemoveDeclinesPendingAndSubsequentRequests, TWriteSessionsQuoterTest) {
    Notify();
    const auto granted = Acquire();
    ExpectReplies(granted, 1);
    const auto first = Acquire();
    const auto second = Acquire();
    Remove();
    ExpectDeclined(first);
    ExpectDeclined(second);
    ExpectReplies(granted, 0);
    ExpectDeclined(Acquire());

    Remove();
    WakeupAfter(TDuration::Seconds(1));
    ExpectReplies(first, 0);
    ExpectReplies(second, 0);
    ExpectDeclined(Acquire());

    Notify();
    WakeupAfter(TDuration::Seconds(1));
    ExpectReplies(first, 0);
    ExpectReplies(second, 0);
    ExpectReplies(Acquire(), 1);
}

Y_UNIT_TEST_F(RemovingOldGenerationDoesNotDeclineNewGenerationWaiters, TWriteSessionsQuoterTest) {
    const TBucketKey newer{"/Root/topic", 0, 2};
    Notify();
    Notify(newer);
    ExpectReplies(Acquire(), 1);
    ExpectReplies(Acquire(newer), 1);
    const auto oldWaiter = Acquire();
    const auto newWaiter = Acquire(newer);

    Remove();
    ExpectDeclined(oldWaiter);
    ExpectReplies(newWaiter, 0);
    ExpectDeclined(Acquire());

    WakeupAfter(TDuration::Seconds(1));
    ExpectReplies(oldWaiter, 0);
    ExpectReplies(newWaiter, 1);
}

Y_UNIT_TEST_F(ShutdownDeclinesAllPendingRequests, TWriteSessionsQuoterTest) {
    const TBucketKey other{"/Root/topic", 1, 1};
    Notify();
    Notify(other);
    const auto granted = Acquire();
    ExpectReplies(granted, 1);
    ExpectReplies(Acquire(other), 1);
    const auto first = Acquire();
    const auto second = Acquire();
    const auto third = Acquire(other);

    Runtime.Send(new IEventHandle(Quoter, Runtime.AllocateEdgeActor(), new TEvents::TEvPoison()));
    ExpectDeclined(first);
    ExpectDeclined(second);
    ExpectDeclined(third);
    ExpectReplies(granted, 0);
}

Y_UNIT_TEST_F(ExhaustedBucketDoesNotBlockOtherQueues, TWriteSessionsQuoterTest) {
    const TBucketKey other{"/Root/topic", 1, 1};
    Notify();
    Notify(other);
    ExpectReplies(Acquire(), 1);
    ExpectReplies(Acquire(other), 1);
    const auto first = Acquire();
    const auto second = Acquire();
    const auto otherFirst = Acquire(other);
    const auto otherSecond = Acquire(other);

    WakeupAfter(TDuration::Seconds(1));
    ExpectReplies(first, 1);
    ExpectReplies(second, 0);
    ExpectReplies(otherFirst, 1);
    ExpectReplies(otherSecond, 0);

    WakeupAfter(TDuration::Seconds(1));
    ExpectReplies(second, 1);
    ExpectReplies(otherSecond, 1);
}

Y_UNIT_TEST_F(NewRequestDoesNotOvertakePendingRequestAfterRefill, TWriteSessionsQuoterTest) {
    Notify();
    ExpectReplies(Acquire(), 1);
    const auto first = Acquire();
    Runtime.AdvanceCurrentTime(TDuration::Seconds(1));
    const auto second = Acquire();
    ExpectReplies(first, 1);
    ExpectReplies(second, 0);
    WakeupAfter(TDuration::Seconds(1));
    ExpectReplies(first, 0);
    ExpectReplies(second, 1);
}

Y_UNIT_TEST_F(QueueCanBeRecreatedAfterDraining, TWriteSessionsQuoterTest) {
    Notify();
    ExpectReplies(Acquire(), 1);
    const auto first = Acquire();
    WakeupAfter(TDuration::Seconds(1));
    ExpectReplies(first, 1);
    const auto second = Acquire();
    ExpectReplies(second, 0);
    WakeupAfter(TDuration::Seconds(1));
    ExpectReplies(first, 0);
    ExpectReplies(second, 1);
}

Y_UNIT_TEST_F(QueueLimitRejectsOverflowAndReusesFreedCapacity, TWriteSessionsQuoterTest) {
    constexpr size_t queueLimit = 1'000'000;
    Notify();
    ExpectReplies(Acquire(), 1);

    // Reuse a sender to test the real limit without allocating a million actors.
    const auto queued = Runtime.AllocateEdgeActor();
    for (size_t i = 0; i < queueLimit - 1; ++i) {
        AcquireFrom(queued);
    }
    const auto lastSlot = Acquire();
    ExpectReplies(queued, 0);
    ExpectReplies(lastSlot, 0);
    const auto rejected = Acquire();
    ExpectDeclined(rejected);

    const TBucketKey other{"/Root/topic", 1, 1};
    Notify(other);
    ExpectReplies(Acquire(other), 1);
    const auto otherPending = Acquire(other);
    ExpectReplies(otherPending, 0);

    Runtime.AdvanceCurrentTime(TDuration::Seconds(1));
    const auto admittedAfterRefill = Acquire();
    ExpectReplies(queued, 1);
    ExpectReplies(admittedAfterRefill, 0);
    ExpectDeclined(Acquire());

    WakeupAfter(TDuration::Seconds(1));
    ExpectReplies(queued, 1);
    ExpectReplies(otherPending, 1);
    ExpectReplies(Acquire(), 0);
    ExpectDeclined(Acquire());
    ExpectReplies(rejected, 0);
    ExpectReplies(lastSlot, 0);
}

} // Y_UNIT_TEST_SUITE

} // namespace
} // namespace NKikimr::NPQ

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
    void Release(const TActorId& actor, const TBucketKey& key = {});
    void CheckIndependentBucket(const TBucketKey& other);

    TTestActorRuntime Runtime;
    TActorId Quoter;
};

void TWriteSessionsQuoterTest::SetUp(NUnitTest::TTestContext&) {
    TTestActorRuntime::TEgg egg;
    egg.App0 = new TAppData(0, 0, 0, 0, {}, nullptr, nullptr, nullptr, nullptr);
    egg.App0->PQConfig.SetMaxConcurrentWriteSessionInitializations(1);
    Runtime.Initialize(std::move(egg));
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

void TWriteSessionsQuoterTest::Release(const TActorId& actor, const TBucketKey& key) {
    Runtime.Send(new IEventHandle(Quoter, actor,
        new TEvQuoter::TEvReleaseQuota(key.Topic, key.Partition, key.Generation)));
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

Y_UNIT_TEST_F(LimitsConcurrentHolders, TWriteSessionsQuoterTest) {
    Runtime.GetAppData().PQConfig.SetMaxConcurrentWriteSessionInitializations(2);
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

Y_UNIT_TEST_F(UnknownReleaseDoesNotGrantQuota, TWriteSessionsQuoterTest) {
    Notify();
    ExpectReplies(Acquire(), 1);
    const auto pending = Acquire();
    Release(Runtime.AllocateEdgeActor());
    ExpectReplies(pending, 0);
}

Y_UNIT_TEST_F(ReleasesDrainQueueInFifoOrder, TWriteSessionsQuoterTest) {
    Notify();
    const auto holder = Acquire();
    ExpectReplies(holder, 1);
    const auto first = Acquire();
    const auto second = Acquire();
    const auto third = Acquire();
    Release(holder);
    ExpectReplies(first, 1);
    ExpectReplies(second, 0);
    ExpectReplies(third, 0);
    Release(first);
    ExpectReplies(second, 1);
    ExpectReplies(third, 0);
    Release(second);
    ExpectReplies(third, 1);
}

Y_UNIT_TEST_F(ElapsedTimeDoesNotReleaseQuota, TWriteSessionsQuoterTest) {
    Notify();
    ExpectReplies(Acquire(), 1);
    const auto pending = Acquire();
    Runtime.AdvanceCurrentTime(TDuration::Hours(1));
    ExpectReplies(Acquire(), 0);
    ExpectReplies(pending, 0);
}

Y_UNIT_TEST_F(EachReleaseFreesOneSlot, TWriteSessionsQuoterTest) {
    Runtime.GetAppData().PQConfig.SetMaxConcurrentWriteSessionInitializations(2);
    Notify();
    const auto firstHolder = Acquire();
    const auto secondHolder = Acquire();
    ExpectReplies(firstHolder, 1);
    ExpectReplies(secondHolder, 1);
    const auto first = Acquire();
    const auto second = Acquire();
    Release(secondHolder);
    ExpectReplies(first, 1);
    ExpectReplies(second, 0);
    Release(firstHolder);
    ExpectReplies(second, 1);
}

Y_UNIT_TEST_F(RepeatedReleaseDoesNotIncreaseCapacity, TWriteSessionsQuoterTest) {
    Notify();
    const auto holder = Acquire();
    ExpectReplies(holder, 1);
    const auto first = Acquire();
    const auto second = Acquire();
    Release(holder);
    ExpectReplies(first, 1);
    Release(holder);
    ExpectReplies(first, 0);
    ExpectReplies(second, 0);
    Release(first);
    ExpectReplies(second, 1);
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
    ExpectReplies(first, 0);
    ExpectReplies(second, 0);
    ExpectDeclined(Acquire());

    Notify();
    ExpectReplies(first, 0);
    ExpectReplies(second, 0);
    ExpectReplies(Acquire(), 1);
}

Y_UNIT_TEST_F(RemovingOldGenerationDoesNotDeclineNewGenerationWaiters, TWriteSessionsQuoterTest) {
    const TBucketKey newer{"/Root/topic", 0, 2};
    Notify();
    Notify(newer);
    const auto oldHolder = Acquire();
    const auto newHolder = Acquire(newer);
    ExpectReplies(oldHolder, 1);
    ExpectReplies(newHolder, 1);
    const auto oldWaiter = Acquire();
    const auto newWaiter = Acquire(newer);
    Remove();
    ExpectDeclined(oldWaiter);
    Release(oldHolder);
    Release(newHolder); // Wrong generation must not release the new holder.
    ExpectReplies(newWaiter, 0);
    Release(newHolder, newer);
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
    const auto holder = Acquire();
    const auto otherHolder = Acquire(other);
    ExpectReplies(holder, 1);
    ExpectReplies(otherHolder, 1);
    const auto first = Acquire();
    const auto otherFirst = Acquire(other);
    Release(otherHolder, other);
    ExpectReplies(otherFirst, 1);
    ExpectReplies(first, 0);
    Release(holder);
    ExpectReplies(first, 1);
}

Y_UNIT_TEST_F(NewRequestDoesNotOvertakePendingRequestAfterLimitIncrease, TWriteSessionsQuoterTest) {
    Notify();
    ExpectReplies(Acquire(), 1);
    const auto first = Acquire();
    Runtime.GetAppData().PQConfig.SetMaxConcurrentWriteSessionInitializations(2);
    const auto second = Acquire();
    ExpectReplies(first, 1);
    ExpectReplies(second, 0);
    Release(first);
    ExpectReplies(second, 1);
}

Y_UNIT_TEST_F(QueueCanBeRecreatedAfterDraining, TWriteSessionsQuoterTest) {
    Notify();
    const auto holder = Acquire();
    ExpectReplies(holder, 1);
    const auto first = Acquire();
    Release(holder);
    ExpectReplies(first, 1);
    const auto second = Acquire();
    ExpectReplies(second, 0);
    Release(first);
    ExpectReplies(second, 1);
}

Y_UNIT_TEST_F(QueueLimitRejectsOverflowAndReusesFreedCapacity, TWriteSessionsQuoterTest) {
    constexpr size_t queueLimit = 1'000'000;
    Notify();
    ExpectReplies(Acquire(), 1);

    // Use distinct senders without allocating a million actors. They remain queued.
    for (size_t i = 0; i < queueLimit - 1; ++i) {
        AcquireFrom(TActorId(Quoter.NodeId(), 0, Max<ui64>() - i, 0));
    }
    const auto lastSlot = Acquire();
    ExpectReplies(lastSlot, 0);
    ExpectDeclined(Acquire());

    const TBucketKey other{"/Root/topic", 1, 1};
    Notify(other);
    ExpectReplies(Acquire(other), 1);

    Release(lastSlot);
    const auto replacement = Acquire();
    ExpectReplies(replacement, 0);
    ExpectDeclined(Acquire());
}


Y_UNIT_TEST_F(CancelPendingDoesNotReleaseHolderAndPreservesOrder, TWriteSessionsQuoterTest) {
    Notify();
    const auto holder = Acquire();
    ExpectReplies(holder, 1);
    const auto first = Acquire();
    const auto cancelled = Acquire();
    const auto last = Acquire();
    Release(cancelled);
    Release(cancelled);
    ExpectReplies(first, 0);
    ExpectReplies(last, 0);
    Release(holder);
    ExpectReplies(first, 1);
    ExpectReplies(cancelled, 0);
    ExpectReplies(last, 0);
    Release(first);
    ExpectReplies(last, 1);
}

Y_UNIT_TEST_F(CancelOnlyWaiterAllowsRecreatingQueue, TWriteSessionsQuoterTest) {
    Notify();
    const auto holder = Acquire();
    ExpectReplies(holder, 1);
    const auto cancelled = Acquire();
    Release(cancelled);
    const auto next = Acquire();
    Release(holder);
    ExpectReplies(cancelled, 0);
    ExpectReplies(next, 1);
}

Y_UNIT_TEST_F(ReleaseBeforeReceivingGrantReturnsSlot, TWriteSessionsQuoterTest) {
    Notify();
    const auto holder = Acquire();
    ExpectReplies(holder, 1);
    const auto cancelled = Acquire();
    const auto next = Acquire();
    Release(holder); // The reply to cancelled is still in its mailbox.
    Release(cancelled);
    ExpectReplies(cancelled, 1);
    ExpectReplies(next, 1);
    Release(cancelled);
    ExpectReplies(Acquire(), 0);
}

Y_UNIT_TEST_F(DuplicateAcquireDoesNotDuplicateHolderOrWaiter, TWriteSessionsQuoterTest) {
    Notify();
    const auto holder = Acquire();
    ExpectReplies(holder, 1);
    AcquireFrom(holder);
    ExpectReplies(holder, 0);
    const auto waiter = Acquire();
    AcquireFrom(waiter);
    const auto next = Acquire();
    Release(holder);
    ExpectReplies(waiter, 1);
    Release(waiter);
    ExpectReplies(waiter, 0);
    ExpectReplies(next, 1);
}

Y_UNIT_TEST_F(WrongKeyDoesNotCancelPendingRequest, TWriteSessionsQuoterTest) {
    const TBucketKey other{"/Root/other", 0, 1};
    Notify();
    Notify(other);
    const auto holder = Acquire();
    ExpectReplies(holder, 1);
    const auto pending = Acquire();
    Release(pending, other);
    Release(holder);
    ExpectReplies(pending, 1);
}

Y_UNIT_TEST_F(RemoveClearsIndexesAndLateReleaseDoesNotAffectNewHolders, TWriteSessionsQuoterTest) {
    Notify();
    const auto holder = Acquire();
    ExpectReplies(holder, 1);
    const auto pending = Acquire();
    Remove();
    ExpectDeclined(pending);
    Notify();
    const auto newHolder = Acquire();
    ExpectReplies(newHolder, 1);
    Release(holder);
    Release(pending);
    AcquireFrom(holder);
    AcquireFrom(pending);
    ExpectReplies(holder, 0);
    ExpectReplies(pending, 0);
    Release(newHolder);
    ExpectReplies(holder, 1);
    Release(holder);
    ExpectReplies(pending, 1);
}

Y_UNIT_TEST_F(ZeroLimitDoesNotGrantQuotaOnCancel, TWriteSessionsQuoterTest) {
    Runtime.GetAppData().PQConfig.SetMaxConcurrentWriteSessionInitializations(0);
    Notify();
    const auto cancelled = Acquire();
    const auto pending = Acquire();
    Release(cancelled);
    Release(cancelled);
    ExpectReplies(cancelled, 0);
    ExpectReplies(pending, 0);
}

} // Y_UNIT_TEST_SUITE

} // namespace
} // namespace NKikimr::NPQ

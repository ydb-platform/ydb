#include "pqrb_ut_common.h"

#include <optional>

#include <util/generic/vector.h>

#include <util/generic/utility.h>

#include <ydb/core/testlib/actors/block_events.h>

#include <ydb/core/persqueue/events/internal.h>

#include <ydb/core/base/tablet_pipe.h>

#include <ydb/core/base/tablet.h>

#include <ydb/core/persqueue/pqrb/read_balancer__balancing.h>

namespace NKikimr::NPQ {

Y_UNIT_TEST_SUITE(TPqrbBalancing) {

Y_UNIT_TEST(UninitializedPartitionAcceptsFirstCommit) {
    NBalancing::TPartition partition;
    UNIT_ASSERT_VALUES_EQUAL(partition.PartitionGeneration, 0u);
    UNIT_ASSERT_VALUES_EQUAL(partition.PartitionCookie, 0u);
    UNIT_ASSERT(partition.SetCommittedState(1, 1));
    UNIT_ASSERT(partition.Commited);
    UNIT_ASSERT_VALUES_EQUAL(partition.PartitionGeneration, 1u);
    UNIT_ASSERT_VALUES_EQUAL(partition.PartitionCookie, 1u);
}

Y_UNIT_TEST(PartitionStateMachine) {
    NBalancing::TPartition partition;

    UNIT_ASSERT(!partition.IsInactive());
    UNIT_ASSERT(partition.NeedReleaseChildren());
    UNIT_ASSERT(!partition.BalanceToOtherPipe());

    UNIT_ASSERT(partition.SetCommittedState(1, 1));
    UNIT_ASSERT(partition.Commited);
    UNIT_ASSERT(partition.IsInactive());
    UNIT_ASSERT(!partition.NeedReleaseChildren());
    UNIT_ASSERT(!partition.SetCommittedState(2, 1));
    UNIT_ASSERT_VALUES_EQUAL(partition.PartitionGeneration, 2u);
    UNIT_ASSERT(!partition.SetCommittedState(1, 99));
    UNIT_ASSERT_VALUES_EQUAL(partition.PartitionGeneration, 2u);

    UNIT_ASSERT(partition.Reset());
    UNIT_ASSERT(!partition.Commited);
    UNIT_ASSERT(!partition.IsInactive());

    UNIT_ASSERT(partition.SetFinishedState(/*scaleAwareSDK=*/true, /*startedReadingFromEndOffset=*/false));
    UNIT_ASSERT(partition.IsInactive());
    UNIT_ASSERT(partition.NeedReleaseChildren());
    UNIT_ASSERT(partition.StartReading());
    UNIT_ASSERT(!partition.IsInactive());

    UNIT_ASSERT(!partition.SetFinishedState(/*scaleAwareSDK=*/false, /*startedReadingFromEndOffset=*/false));
    UNIT_ASSERT(!partition.IsInactive());
    UNIT_ASSERT(partition.BalanceToOtherPipe());
    UNIT_ASSERT(!partition.NeedReleaseChildren());
    UNIT_ASSERT(partition.StopReading());
}

Y_UNIT_TEST(DestroyFreeFamilyOnRereadParentDoesNotCrash) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    PQTabletPrepare({}, {}, tc);
    SendBalancerUpdate(tc, TBalancerUpdate{
        .Partitions = {
            {0, {tc.TabletId, 1}},
            {1, {tc.TabletId, 2}},
            {2, {tc.TabletId, 3}},
        },
        .Strategy = NKikimrPQ::TPQTabletConfig::CAN_SPLIT,
        .ParentPartitionIds = {{1, {0}}, {2, {0}}},
        .ChildPartitionIds = {{0, {1, 2}}},
        .NextPartitionId = 3,
    });

    auto pipe = RegisterReadSession("session-0", tc);
    auto lock = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvLockPartition>(TDuration::Seconds(10));
    UNIT_ASSERT(lock);
    UNIT_ASSERT_VALUES_EQUAL(lock->Record.GetPartition(), 0u);

    tc.Runtime->SendToPipe(
        tc.BalancerTabletId,
        tc.Edge,
        new TEvPersQueue::TEvReadingPartitionFinishedRequest(pipe, "user", 0, /*scaleAwareSDK=*/false, /*startedReadingFromEndOffset=*/true),
        0,
        GetPipeConfigWithRetries(),
        pipe
    );
    DispatchFor(tc);

    tc.Runtime->SendToPipe(
        tc.BalancerTabletId,
        tc.Edge,
        new TEvPersQueue::TEvReadingPartitionStartedRequest(pipe, "user", 0),
        0,
        GetPipeConfigWithRetries(),
        pipe
    );
    DispatchFor(tc);

    auto sessions = MakeHolder<TEvPersQueue::TEvGetReadSessionsInfo>();
    sessions->Record.SetClientId("user");
    tc.Runtime->SendToPipe(tc.BalancerTabletId, tc.Edge, sessions.Release(), 0, GetPipeConfigWithRetries());
    auto info = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvReadSessionsInfoResponse>(TDuration::Seconds(10));
    UNIT_ASSERT(info);
    UNIT_ASSERT_VALUES_EQUAL(info->Record.ReadSessionsSize(), 1u);
}

Y_UNIT_TEST(SecondSessionTriggersRebalance) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    PQTabletPrepare({}, {}, tc);
    SendBalancerUpdate(tc, TBalancerUpdate{
        .Partitions = {
            {0, {tc.TabletId, 1}},
            {1, {tc.TabletId, 2}},
            {2, {tc.TabletId, 3}},
        },
        .NextPartitionId = 3,
    });

    auto pipe0 = RegisterReadSession("session-0", tc);
    absl::flat_hash_set<ui32> lockedByFirst;
    for (ui32 i = 0; i < 3; ++i) {
        auto lock = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvLockPartition>(TDuration::Seconds(10));
        UNIT_ASSERT(lock);
        lockedByFirst.insert(lock->Record.GetPartition());
    }
    UNIT_ASSERT_VALUES_EQUAL(lockedByFirst.size(), 3u);

    auto pipe1 = RegisterReadSession("session-1", tc);
    Y_UNUSED(pipe1);

    auto release = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvReleasePartition>(TDuration::Seconds(10));
    UNIT_ASSERT(release);
    UNIT_ASSERT_VALUES_EQUAL(release->Record.GetSession(), TString("session-0"));
    UNIT_ASSERT(release->Record.GetGroup() >= 1);
    const ui32 releasedPartition = release->Record.GetGroup() - 1;
    UNIT_ASSERT(lockedByFirst.contains(releasedPartition));

    auto released = MakeHolder<TEvPersQueue::TEvPartitionReleased>();
    released->Record.SetSession("session-0");
    released->Record.SetPartition(releasedPartition);
    released->Record.SetTopic("topic");
    released->Record.SetClientId("user");
    ActorIdToProto(pipe0, released->Record.MutablePipeClient());
    tc.Runtime->SendToPipe(
        tc.BalancerTabletId,
        tc.Edge,
        released.Release(),
        0,
        GetPipeConfigWithRetries(),
        pipe0
    );

    auto lockOnSecond = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvLockPartition>(TDuration::Seconds(10));
    UNIT_ASSERT(lockOnSecond);
    UNIT_ASSERT_VALUES_EQUAL(lockOnSecond->Record.GetSession(), TString("session-1"));
    UNIT_ASSERT_VALUES_EQUAL(lockOnSecond->Record.GetPartition(), releasedPartition);
}

Y_UNIT_TEST(BalancingUnsubscribeRemovesOnlyMatchingSubscription) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    PQTabletPrepare({}, {}, tc);
    PQBalancerPrepare("topic", {{0, {tc.TabletId, 1}}}, /*ssId=*/1, tc, false, false);

    const TActorId subscriberA = tc.Runtime->AllocateEdgeActor();
    const TActorId subscriberB = tc.Runtime->AllocateEdgeActor();
    TActorId pipe = tc.Runtime->ConnectToPipe(tc.BalancerTabletId, tc.Edge, 0, GetPipeConfigWithRetries());

    tc.Runtime->SendToPipe(
        tc.BalancerTabletId,
        tc.Edge,
        new TEvPersQueue::TEvBalancingSubscribe(subscriberA, "/Root/topic", "user"),
        0,
        GetPipeConfigWithRetries(),
        pipe
    );
    tc.Runtime->SendToPipe(
        tc.BalancerTabletId,
        tc.Edge,
        new TEvPersQueue::TEvBalancingSubscribe(subscriberB, "/Root/topic", "user"),
        0,
        GetPipeConfigWithRetries(),
        pipe
    );

    auto notifyA = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvBalancingSubscribeNotify>(subscriberA, TDuration::Seconds(10));
    auto notifyB = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvBalancingSubscribeNotify>(subscriberB, TDuration::Seconds(10));
    UNIT_ASSERT(notifyA);
    UNIT_ASSERT(notifyB);

    tc.Runtime->SendToPipe(
        tc.BalancerTabletId,
        tc.Edge,
        new TEvPersQueue::TEvBalancingUnsubscribe(subscriberA, "/Root/topic", "user"),
        0,
        GetPipeConfigWithRetries(),
        pipe
    );
    DispatchFor(tc);

    auto pipeClient = RegisterReadSession("session-notify", tc);
    Y_UNUSED(pipeClient);

    auto afterUnsubscribeA = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvBalancingSubscribeNotify>(
        subscriberA,
        TDuration::MilliSeconds(200)
    );
    UNIT_ASSERT_C(!afterUnsubscribeA, "Unsubscribed actor must not receive further notifications");

    auto afterUnsubscribeB = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvBalancingSubscribeNotify>(
        subscriberB,
        TDuration::Seconds(10)
    );
    UNIT_ASSERT(afterUnsubscribeB);
    UNIT_ASSERT(afterUnsubscribeB->Get()->Record.GetStatus() == NKikimrPQ::TEvBalancingSubscribeNotify::BALANCING);
}

} // Y_UNIT_TEST_SUITE(TPqrbBalancing)

struct TScaleEnv {
    TTestContext tc;
    absl::flat_hash_map<TString, TActorId> Pipes;
    absl::flat_hash_map<ui32, TString> LockedBy;
    absl::flat_hash_map<ui32, TVector<ui32>> ParentPartitionIds;
    absl::flat_hash_map<ui32, TVector<ui32>> ChildPartitionIds;
    ui32 NextPartitionId = 0;
    NKikimrPQ::TPQTabletConfig::TPartitionStrategyType PartitionStrategy =
        NKikimrPQ::TPQTabletConfig::CAN_SPLIT_AND_MERGE;

    explicit TScaleEnv(NKikimrPQ::TPQTabletConfig::TPartitionStrategyType strategy =
        NKikimrPQ::TPQTabletConfig::CAN_SPLIT_AND_MERGE)
        : PartitionStrategy(strategy)
    {
        tc.Prepare();
        tc.Runtime->SetScheduledLimit(10000);
        PQTabletPrepare({}, {}, tc);
    }

    void Publish() {
        TBalancerUpdate update;
        update.Strategy = PartitionStrategy;
        update.NextPartitionId = NextPartitionId;
        update.ParentPartitionIds = ParentPartitionIds;
        update.ChildPartitionIds = ChildPartitionIds;
        for (ui32 i = 0; i < NextPartitionId; ++i) {
            update.Partitions.push_back({i, {tc.TabletId, i + 1}});
        }
        SendBalancerUpdate(tc, update);
        Pump();
    }

    void CreateParents(ui32 count = 2) {
        NextPartitionId = count;
        Publish();
    }

    ui32 Merge(ui32 left, ui32 right) {
        const ui32 child = NextPartitionId++;
        ParentPartitionIds[child] = {left, right};
        ChildPartitionIds[left].push_back(child);
        ChildPartitionIds[right].push_back(child);
        Publish();
        return child;
    }

    std::pair<ui32, ui32> Split(ui32 parent) {
        const ui32 left = NextPartitionId++;
        const ui32 right = NextPartitionId++;
        ParentPartitionIds[left] = {parent};
        ParentPartitionIds[right] = {parent};
        ChildPartitionIds[parent].push_back(left);
        ChildPartitionIds[parent].push_back(right);
        Publish();
        return {left, right};
    }

    ui32 AddRoot() {
        return NextPartitionId++, Publish(), NextPartitionId - 1;
    }

    TActorId RegisterSession(const TString& name, const TVector<ui32>& groups = {}, bool pump = true) {
        auto pipe = RegisterReadSession(name, tc, groups);
        Pipes[name] = pipe;
        if (pump) {
            Pump();
        } else {
            DispatchFor(tc, TDuration::MilliSeconds(50));
        }
        return pipe;
    }

    void Finish(const TString& session, ui32 partition, bool scaleAware = true, bool fromEnd = true, bool pump = true) {
        auto it = Pipes.find(session);
        UNIT_ASSERT_C(it != Pipes.end(), session);
        tc.Runtime->SendToPipe(
            tc.BalancerTabletId,
            tc.Edge,
            new TEvPersQueue::TEvReadingPartitionFinishedRequest(it->second, "user", partition, scaleAware, fromEnd),
            0,
            GetPipeConfigWithRetries(),
            it->second
        );
        if (pump) {
            Pump();
        } else {
            DispatchFor(tc, TDuration::MilliSeconds(50));
        }
    }

    void StartReading(const TString& session, ui32 partition) {
        auto it = Pipes.find(session);
        UNIT_ASSERT_C(it != Pipes.end(), session);
        tc.Runtime->SendToPipe(
            tc.BalancerTabletId,
            tc.Edge,
            new TEvPersQueue::TEvReadingPartitionStartedRequest(it->second, "user", partition),
            0,
            GetPipeConfigWithRetries(),
            it->second
        );
        Pump();
    }

    void Commit(ui32 partition, ui32 generation = 1, ui64 cookie = 1, bool pump = true) {
        tc.Runtime->SendToPipe(
            tc.BalancerTabletId,
            tc.Edge,
            new TEvPQ::TEvReadingPartitionStatusRequest("user", partition, generation, cookie),
            0,
            GetPipeConfigWithRetries()
        );
        if (pump) {
            Pump();
        } else {
            DispatchFor(tc, TDuration::MilliSeconds(50));
        }
    }

    void InjectFinish(const TActorId& pipe, ui32 partition, bool scaleAware = true, bool fromEnd = true) {
        ForwardToTablet(
            *tc.Runtime,
            tc.BalancerTabletId,
            tc.Edge,
            new TEvPersQueue::TEvReadingPartitionFinishedRequest(pipe, "user", partition, scaleAware, fromEnd)
        );
        DispatchFor(tc, TDuration::MilliSeconds(50));
    }

    void InjectCommit(ui32 partition, ui32 generation = 1, ui64 cookie = 1) {
        ForwardToTablet(
            *tc.Runtime,
            tc.BalancerTabletId,
            tc.Edge,
            new TEvPQ::TEvReadingPartitionStatusRequest("user", partition, generation, cookie)
        );
        DispatchFor(tc, TDuration::MilliSeconds(50));
    }

    void CloseSession(const TString& session) {
        auto it = Pipes.find(session);
        UNIT_ASSERT_C(it != Pipes.end(), session);
        tc.Runtime->ClosePipe(it->second, tc.Edge, 0);
        Pipes.erase(it);
        for (auto jt = LockedBy.begin(); jt != LockedBy.end();) {
            if (jt->second == session) {
                LockedBy.erase(jt++);
            } else {
                ++jt;
            }
        }
        Pump();
    }

    void AckRelease(const TActorId& pipe, ui32 partition, const TString& session) {
        auto released = MakeHolder<TEvPersQueue::TEvPartitionReleased>();
        released->Record.SetSession(session);
        released->Record.SetPartition(partition);
        released->Record.SetTopic("topic");
        released->Record.SetClientId("user");
        ActorIdToProto(pipe, released->Record.MutablePipeClient());
        tc.Runtime->SendToPipe(
            tc.BalancerTabletId,
            tc.Edge,
            released.Release(),
            0,
            GetPipeConfigWithRetries(),
            pipe
        );
    }

    void Pump(TDuration wait = TDuration::MilliSeconds(50)) {
        DispatchFor(tc, wait);
        for (;;) {
            auto release = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvReleasePartition>(TDuration::MilliSeconds(5));
            if (release) {
                const ui32 partition = release->Record.GetGroup() - 1;
                const TString session = release->Record.GetSession();
                auto pipeIt = Pipes.find(session);
                if (pipeIt == Pipes.end()) {
                    LockedBy.erase(partition);
                    continue;
                }
                AckRelease(pipeIt->second, partition, session);
                LockedBy.erase(partition);
                DispatchFor(tc, TDuration::MilliSeconds(10));
                continue;
            }
            auto lock = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvLockPartition>(TDuration::MilliSeconds(5));
            if (lock) {
                const TString session = lock->Record.GetSession();
                const ui32 partition = lock->Record.GetPartition();
                if (Pipes.contains(session)) {
                    LockedBy[partition] = session;
                } else {
                    LockedBy.erase(partition);
                }
                continue;
            }
            break;
        }
        DispatchFor(tc, wait);
    }

    struct TPendingRelease {
        ui32 Partition = 0;
        TString Session;
    };

    std::optional<TPendingRelease> WaitRelease(TDuration timeout = TDuration::Seconds(5)) {
        DispatchFor(tc, TDuration::MilliSeconds(50));
        auto release = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvReleasePartition>(timeout);
        if (!release) {
            return std::nullopt;
        }
        return TPendingRelease{
            .Partition = release->Record.GetGroup() - 1,
            .Session = release->Record.GetSession(),
        };
    }

    std::pair<TString, TString> TwoSessionsOnParents() {
        RegisterSession("session-0");
        RegisterSession("session-1");
        AssertLocked(0);
        AssertLocked(1);
        const TString a = SessionOf(0);
        const TString b = SessionOf(1);
        UNIT_ASSERT_VALUES_UNEQUAL(a, b);
        return {a, b};
    }

    THolder<TEvPersQueue::TEvReadSessionsInfoResponse> SessionsInfo() {
        auto sessions = MakeHolder<TEvPersQueue::TEvGetReadSessionsInfo>();
        sessions->Record.SetClientId("user");
        tc.Runtime->SendToPipe(tc.BalancerTabletId, tc.Edge, sessions.Release(), 0, GetPipeConfigWithRetries());
        auto info = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvReadSessionsInfoResponse>(TDuration::Seconds(10));
        UNIT_ASSERT(info);
        return info;
    }

    TString SessionOf(ui32 partition) {
        Pump(TDuration::MilliSeconds(10));
        auto info = SessionsInfo();
        for (const auto& pi : info->Record.GetPartitionInfo()) {
            if (pi.GetPartition() == partition) {
                return pi.GetSession();
            }
        }
        return {};
    }

    void AssertLocked(ui32 partition, const TString& expectedSession = {}) {
        Pump();
        TString session;
        if (auto it = LockedBy.find(partition); it != LockedBy.end()) {
            session = it->second;
        }
        if (session.empty()) {
            session = SessionOf(partition);
        }
        UNIT_ASSERT_C(!session.empty(), "partition " << partition << " must be locked");
        if (!expectedSession.empty()) {
            UNIT_ASSERT_VALUES_EQUAL_C(expectedSession, session, "partition " << partition);
        }
    }

    void AssertNotLocked(ui32 partition) {
        Pump();
        UNIT_ASSERT_C(!LockedBy.contains(partition),
            "partition " << partition << " must not be locked, session=" << LockedBy[partition]);
        const TString session = SessionOf(partition);
        UNIT_ASSERT_C(session.empty(), "partition " << partition << " must not be locked, session=" << session);
    }

    void AssertSameSession(const std::vector<ui32>& partitions) {
        UNIT_ASSERT(!partitions.empty());
        const TString session = SessionOf(partitions.front());
        UNIT_ASSERT_C(!session.empty(), "partition " << partitions.front() << " must be locked");
        for (auto partition : partitions) {
            UNIT_ASSERT_VALUES_EQUAL_C(session, SessionOf(partition), "family must stay on one session");
        }
    }

    void AssertEvenDistribution(ui32 partitionCount, ui32 sessionCount) {
        Pump();
        auto info = SessionsInfo();
        absl::flat_hash_map<TString, ui32> counts;
        absl::flat_hash_set<ui32> seen;
        ui32 assigned = 0;
        for (const auto& pi : info->Record.GetPartitionInfo()) {
            if (pi.GetSession().empty()) {
                continue;
            }
            UNIT_ASSERT_C(seen.insert(pi.GetPartition()).second,
                "partition " << pi.GetPartition() << " listed twice in sessions info");
            counts[pi.GetSession()]++;
            ++assigned;
        }
        UNIT_ASSERT_VALUES_EQUAL_C(assigned, partitionCount, "every readable partition must be assigned");
        UNIT_ASSERT_VALUES_EQUAL_C(counts.size(), sessionCount, "every session must get a share");
        ui32 minCount = partitionCount;
        ui32 maxCount = 0;
        for (const auto& [session, count] : counts) {
            Y_UNUSED(session);
            minCount = Min(minCount, count);
            maxCount = Max(maxCount, count);
        }
        UNIT_ASSERT_LE_C(maxCount - minCount, 1u,
            "families must go to the least loaded sessions, counts=" << counts.size());
    }

    void SendToBalancerActor(const TActorId& actor, IEventBase* ev, const TActorId& sender) {
        tc.Runtime->Send(new IEventHandle(actor, sender, ev), 0, true);
    }

    TActorId RebootBalancerAndHoldRestored(NActors::TBlockEvents<TEvTablet::TEvRestored>& restored) {
        ForwardToTablet(*tc.Runtime, tc.BalancerTabletId, tc.Edge, new TEvents::TEvPoisonPill());
        TDispatchOptions boot;
        boot.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot, 1));
        tc.Runtime->DispatchEvents(boot);
        InvalidateTabletResolverCache(*tc.Runtime, tc.BalancerTabletId);

        for (ui32 i = 0; i < 100 && restored.empty(); ++i) {
            DispatchFor(tc, TDuration::MilliSeconds(10));
        }
        UNIT_ASSERT_C(!restored.empty(), "PQRB TEvRestored must be held while the tablet is in StateInit");
        LockedBy.clear();
        Pipes.clear();
        return restored.front()->GetRecipientRewrite();
    }

    TActorId InjectSessionDuringInit(const TActorId& pqrb, const TString& session) {
        const TActorId pipe = tc.Runtime->AllocateEdgeActor();
        SendToBalancerActor(
            pqrb,
            new TEvTabletPipe::TEvServerConnected(tc.BalancerTabletId, pipe, pipe),
            pipe
        );

        auto request = MakeHolder<TEvPersQueue::TEvRegisterReadSession>();
        request->Record.SetSession(session);
        request->Record.SetClientId("user");
        ActorIdToProto(pipe, request->Record.MutablePipeClient());
        SendToBalancerActor(pqrb, request.Release(), tc.Edge);

        Pipes[session] = pipe;
        return pipe;
    }
};

Y_UNIT_TEST_SUITE(TPqrbMergeBalancing) {

Y_UNIT_TEST(ChildNotLockedUntilBothParentsFinished_OneSession) {
    TScaleEnv env;
    env.CreateParents(2);
    env.RegisterSession("session-0");
    env.AssertLocked(0);
    env.AssertLocked(1);

    env.Merge(0, 1);
    env.AssertNotLocked(2);

    env.Finish("session-0", 0);
    env.AssertNotLocked(2);

    env.Finish("session-0", 1);
    env.AssertLocked(2, "session-0");
    env.AssertSameSession({0, 1, 2});
}

Y_UNIT_TEST(PreferredPartitionsConflictDoesNotLockChildEarly) {
    TScaleEnv env;
    env.CreateParents(2);
    env.RegisterSession("session-0", {1});
    env.RegisterSession("session-1", {2});
    env.AssertLocked(0, "session-0");
    env.AssertLocked(1, "session-1");

    env.Merge(0, 1);
    env.Finish("session-0", 0);
    env.Finish("session-1", 1);
    env.AssertNotLocked(2);

    env.RegisterSession("session-2", {3});
    env.AssertLocked(2, "session-2");
}

Y_UNIT_TEST(PreferredChildDoesNotStealFromCommonParentSession) {
    TScaleEnv env;
    env.CreateParents(2);
    env.RegisterSession("session-0");
    env.Merge(0, 1);
    env.RegisterSession("session-child", {3});
    env.AssertNotLocked(2);

    env.Finish("session-0", 0);
    env.AssertNotLocked(2);
    env.Finish("session-0", 1);
    env.AssertLocked(2, "session-0");
}

Y_UNIT_TEST(SessionClosedBeforeSecondParentFinished) {
    TScaleEnv env;
    env.CreateParents(2);
    env.RegisterSession("session-0");
    env.RegisterSession("session-1");
    env.Merge(0, 1);

    const TString first = env.SessionOf(0);
    env.Finish(first, 0);
    env.AssertNotLocked(2);
    env.CloseSession(first);
    env.AssertNotLocked(2);

    const TString remaining = env.SessionOf(1);
    UNIT_ASSERT_C(!remaining.empty(), "surviving session must keep the other parent");
    env.Finish(remaining, 1);
    env.AssertLocked(2);
}
Y_UNIT_TEST(ChildNotLockedUntilBothParentsFinished_TwoSessions) {
    TScaleEnv env;
    env.CreateParents(2);
    env.RegisterSession("session-0");
    env.RegisterSession("session-1");
    env.AssertLocked(0);
    env.AssertLocked(1);
    UNIT_ASSERT_VALUES_UNEQUAL(env.SessionOf(0), env.SessionOf(1));

    env.Merge(0, 1);
    env.AssertNotLocked(2);

    const TString session0 = env.SessionOf(0);
    env.Finish(session0, 0);
    env.AssertNotLocked(2);

    const TString session1 = env.SessionOf(1);
    env.Finish(session1, 1);
    env.AssertLocked(2);
    env.AssertSameSession({0, 1, 2});
}

Y_UNIT_TEST(ResetMergeAttachesChildWhenTargetFamilyDied) {
    TScaleEnv env;
    env.CreateParents(2);
    env.Merge(0, 1);
    env.RegisterSession("session-0", {1});
    env.RegisterSession("session-1", {2, 3});
    env.AssertLocked(0, "session-0");
    env.AssertLocked(1, "session-1");
    env.AssertNotLocked(2);

    env.Finish("session-0", 0);
    env.Finish("session-1", 1, /*scaleAware=*/true, /*fromEnd=*/true, /*pump=*/false);

    auto pending = env.WaitRelease();
    UNIT_ASSERT_C(pending, "second finished parent must trigger a family release to merge");
    const TString releasing = pending->Session;
    const TString target = releasing == "session-0" ? "session-1" : "session-0";
    UNIT_ASSERT_C(env.Pipes.contains(releasing), "releasing session must still be connected");

    env.CloseSession(target);
    env.RegisterSession("session-keep");

    auto pipeIt = env.Pipes.find(releasing);
    UNIT_ASSERT(pipeIt != env.Pipes.end());
    env.AckRelease(pipeIt->second, pending->Partition, releasing);
    env.Pump();

    env.AssertLocked(2);
}

Y_UNIT_TEST(DelayedMergeDisconnectTargetBeforeUnlockLocksChild) {
    TScaleEnv env;
    env.CreateParents(2);
    auto [s0, s1] = env.TwoSessionsOnParents();
    env.Merge(0, 1);

    env.Finish(s0, 0);
    env.Finish(s1, 1, /*scaleAware=*/true, /*fromEnd=*/true, /*pump=*/false);

    auto pending = env.WaitRelease();
    UNIT_ASSERT_C(pending, "second finished parent must trigger a family release to merge");
    const TString releasing = pending->Session;
    const TString target = releasing == s0 ? s1 : s0;
    UNIT_ASSERT_C(env.Pipes.contains(releasing), "releasing session must still be connected");

    env.CloseSession(target);

    auto pipeIt = env.Pipes.find(releasing);
    UNIT_ASSERT(pipeIt != env.Pipes.end());
    env.AckRelease(pipeIt->second, pending->Partition, releasing);
    env.Pump();

    env.AssertLocked(2);
}

Y_UNIT_TEST(PipeBreakDuringReleaseOfMergedFamily) {
    TScaleEnv env;
    env.CreateParents(2);
    auto [s0, s1] = env.TwoSessionsOnParents();
    env.Merge(0, 1);

    env.Finish(s0, 0);
    env.Finish(s1, 1, /*scaleAware=*/true, /*fromEnd=*/true, /*pump=*/false);

    auto pending = env.WaitRelease();
    UNIT_ASSERT_C(pending, "second finished parent must trigger a family release to merge");
    env.CloseSession(pending->Session);

    env.AssertLocked(2);
}

Y_UNIT_TEST(PipeBreakOfTargetFamilyDuringRelease) {
    TScaleEnv env;
    env.CreateParents(2);
    auto [s0, s1] = env.TwoSessionsOnParents();
    env.Merge(0, 1);

    env.Finish(s0, 0);
    env.Finish(s1, 1, /*scaleAware=*/true, /*fromEnd=*/true, /*pump=*/false);

    auto pending = env.WaitRelease();
    UNIT_ASSERT_C(pending, "second finished parent must trigger a family release to merge");
    const TString target = pending->Session == s0 ? s1 : s0;
    env.CloseSession(target);
    env.CloseSession(pending->Session);

    env.RegisterSession("session-new");
    env.Finish("session-new", 0);
    env.AssertNotLocked(2);
    env.Finish("session-new", 1);
    env.AssertLocked(2, "session-new");
}
} // Y_UNIT_TEST_SUITE(TPqrbMergeBalancing)

Y_UNIT_TEST_SUITE(TPqrbSplitBalancing) {

Y_UNIT_TEST(PreferredChildNotLockedBeforeParentFinished) {
    TScaleEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.Split(0);
    env.RegisterSession("session-child", {2});
    env.AssertNotLocked(1);
    env.AssertNotLocked(2);

    env.Finish("session-0", 0);
    env.AssertLocked(1, "session-0");
    env.AssertLocked(2, "session-0");
}

Y_UNIT_TEST(PreferredChildTakenAfterParentCommitted) {
    TScaleEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.Split(0);
    env.RegisterSession("session-child", {2});
    env.Commit(0);
    env.AssertLocked(1, "session-child");
}

} // Y_UNIT_TEST_SUITE(TPqrbSplitBalancing)

Y_UNIT_TEST_SUITE(TPqrbBalancingInvariants) {

Y_UNIT_TEST(StaleFinishAfterPipeBreakIsIgnored) {
    TScaleEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.Split(0);
    const auto pipe = env.Pipes["session-0"];
    env.CloseSession("session-0");
    env.InjectFinish(pipe, 0);
    env.RegisterSession("session-new");
    env.AssertNotLocked(1);
    env.Finish("session-new", 0);
    env.AssertLocked(1);
    env.AssertLocked(2);
}

Y_UNIT_TEST(FinishThenImmediatePipeBreakKeepsConsumerIfOtherSessionAlive) {
    TScaleEnv env;
    env.CreateParents(2);
    auto [s0, s1] = env.TwoSessionsOnParents();
    env.Split(0);
    env.Finish(s0, 0, /*scaleAware=*/true, /*fromEnd=*/true, /*pump=*/false);
    env.CloseSession(s1);
    env.AssertLocked(2);
    env.AssertLocked(3);
}

} // Y_UNIT_TEST_SUITE(TPqrbBalancingInvariants)

} // namespace NKikimr::NPQ

#include "pqrb_ut_common.h"

#include <ydb/core/base/tablet.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/persqueue/pqrb/read_balancer__balancing.h>
#include <ydb/core/testlib/actors/block_events.h>

#include <util/generic/utility.h>
#include <util/generic/vector.h>

#include <optional>

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

Y_UNIT_TEST(MessagesBeforeFirstConfigDoNotCrash) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    auto pipe = RegisterReadSession("session-0", tc);
    DispatchFor(tc);

    tc.Runtime->SendToPipe(
        tc.BalancerTabletId,
        tc.Edge,
        new TEvPersQueue::TEvReadingPartitionFinishedRequest(pipe, "user", 0, true, true),
        0,
        GetPipeConfigWithRetries(),
        pipe
    );
    tc.Runtime->SendToPipe(
        tc.BalancerTabletId,
        tc.Edge,
        new TEvPQ::TEvReadingPartitionStatusRequest("user", 0, 1, 1),
        0,
        GetPipeConfigWithRetries()
    );
    tc.Runtime->SendToPipe(
        tc.BalancerTabletId,
        tc.Edge,
        new TEvPersQueue::TEvReadingPartitionStartedRequest(pipe, "user", 0),
        0,
        GetPipeConfigWithRetries(),
        pipe
    );
    auto released = MakeHolder<TEvPersQueue::TEvPartitionReleased>();
    released->Record.SetSession("session-0");
    released->Record.SetPartition(0);
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

    auto sessions = MakeHolder<TEvPersQueue::TEvGetReadSessionsInfo>();
    sessions->Record.SetClientId("user");
    tc.Runtime->SendToPipe(tc.BalancerTabletId, tc.Edge, sessions.Release(), 0, GetPipeConfigWithRetries());
    auto info = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvReadSessionsInfoResponse>(TDuration::Seconds(10));
    UNIT_ASSERT(info);

    SendBalancerUpdate(tc, TBalancerUpdate{
        .Partitions = {{0, {tc.TabletId, 1}}, {1, {tc.TabletId, 2}}},
        .NextPartitionId = 2,
    });
    DispatchFor(tc, TDuration::MilliSeconds(200));

    absl::flat_hash_set<TString> lockedSessions;
    absl::flat_hash_set<ui32> locked;
    for (;;) {
        auto lock = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvLockPartition>(TDuration::MilliSeconds(200));
        if (!lock) {
            break;
        }
        locked.insert(lock->Record.GetPartition());
        lockedSessions.insert(lock->Record.GetSession());
    }
    UNIT_ASSERT_C(!locked.empty(), "session registered before config must receive partitions after init");
    UNIT_ASSERT(lockedSessions.contains("session-0"));
}

Y_UNIT_TEST(SessionRegisteredBeforeConfigGetsNewPartitions) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    auto pipe = RegisterReadSession("session-0", tc);
    Y_UNUSED(pipe);
    DispatchFor(tc);

    SendBalancerUpdate(tc, TBalancerUpdate{
        .Partitions = {{0, {tc.TabletId, 1}}},
        .NextPartitionId = 1,
    });

    auto lock = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvLockPartition>(TDuration::Seconds(10));
    UNIT_ASSERT(lock);
    UNIT_ASSERT_VALUES_EQUAL(lock->Record.GetPartition(), 0u);
    UNIT_ASSERT_VALUES_EQUAL(lock->Record.GetSession(), TString("session-0"));
}

Y_UNIT_TEST(PipeDisconnectBeforeFirstConfigDoNotCrash) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    auto pipe = RegisterReadSession("session-0", tc);
    DispatchFor(tc);
    tc.Runtime->ClosePipe(pipe, tc.Edge, 0);
    DispatchFor(tc, TDuration::MilliSeconds(200));
    while (tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvLockPartition>(TDuration::MilliSeconds(50))) {
    }

    SendBalancerUpdate(tc, TBalancerUpdate{
        .Partitions = {{0, {tc.TabletId, 1}}},
        .NextPartitionId = 1,
    });
    DispatchFor(tc);

    auto pipe2 = RegisterReadSession("session-1", tc);
    Y_UNUSED(pipe2);
    auto lock = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvLockPartition>(TDuration::Seconds(10));
    UNIT_ASSERT(lock);
    UNIT_ASSERT_VALUES_EQUAL(lock->Record.GetSession(), TString("session-1"));
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

    void InjectStartReading(const TActorId& pipe, ui32 partition) {
        ForwardToTablet(
            *tc.Runtime,
            tc.BalancerTabletId,
            tc.Edge,
            new TEvPersQueue::TEvReadingPartitionStartedRequest(pipe, "user", partition)
        );
        DispatchFor(tc, TDuration::MilliSeconds(50));
    }

    void Advance(TDuration delay) {
        tc.Runtime->AdvanceCurrentTime(delay);
        Pump(TDuration::MilliSeconds(100));
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

struct TSplitEnv : TScaleEnv {
    TSplitEnv()
        : TScaleEnv(NKikimrPQ::TPQTabletConfig::CAN_SPLIT)
    {
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

Y_UNIT_TEST(ChildLockedOnSameSessionWhenBothParentsWereTogether) {
    TScaleEnv env;
    env.CreateParents(2);
    env.RegisterSession("session-0");
    env.Merge(0, 1);
    env.Finish("session-0", 0);
    env.Finish("session-0", 1);
    env.AssertLocked(2, "session-0");
}

Y_UNIT_TEST(ChildIndependentFamilyAfterBothParentsCommitted) {
    TScaleEnv env;
    env.CreateParents(2);
    env.RegisterSession("session-0");
    env.RegisterSession("session-1");
    env.Merge(0, 1);

    env.Commit(0);
    env.AssertNotLocked(2);
    env.Commit(1);
    env.AssertLocked(2);
}

Y_UNIT_TEST(CommitThenRebalanceKeepsMergeChildIndependent) {
    TScaleEnv env;
    env.CreateParents(2);
    env.RegisterSession("session-0");
    env.Merge(0, 1);
    env.Finish("session-0", 0);
    env.Finish("session-0", 1);
    env.AssertSameSession({0, 1, 2});

    env.Commit(0);
    env.Commit(1);
    env.RegisterSession("session-1");
    env.AssertEvenDistribution(3, 2);
}

Y_UNIT_TEST(CommitThenParentReleaseDoesNotReattachMergeChild) {
    TScaleEnv env;
    env.CreateParents(2);
    env.RegisterSession("session-0");
    env.Merge(0, 1);
    env.Finish("session-0", 0);
    env.Finish("session-0", 1);
    env.Commit(0);
    env.Commit(1);

    env.RegisterSession("session-child", {3});
    env.AssertLocked(2, "session-child");

    env.RegisterSession("session-pref", {1});
    env.AssertLocked(0, "session-pref");
    env.AssertLocked(2, "session-child");
    UNIT_ASSERT_C(!env.SessionOf(1).empty(), "the other merge parent must stay assigned");
}

Y_UNIT_TEST(DelayedMergeThenCommitThenParentReleaseKeepsChildIndependent) {
    TScaleEnv env;
    env.CreateParents(2);
    auto [s0, s1] = env.TwoSessionsOnParents();
    env.Merge(0, 1);

    env.Finish(s0, 0);
    env.Finish(s1, 1, /*scaleAware=*/true, /*fromEnd=*/true, /*pump=*/false);

    auto pending = env.WaitRelease();
    UNIT_ASSERT_C(pending, "second finished parent must trigger a family release to merge");
    auto pipeIt = env.Pipes.find(pending->Session);
    UNIT_ASSERT(pipeIt != env.Pipes.end());
    env.AckRelease(pipeIt->second, pending->Partition, pending->Session);
    env.Pump();
    env.AssertLocked(2);

    env.Commit(0);
    env.Commit(1);
    env.RegisterSession("session-child", {3});
    env.AssertLocked(2, "session-child");

    env.RegisterSession("session-pref", {1});
    env.AssertLocked(0, "session-pref");
    env.AssertLocked(2, "session-child");
}

Y_UNIT_TEST(ChildNotLockedIfOnlyOneParentCommitted) {
    TScaleEnv env;
    env.CreateParents(2);
    env.RegisterSession("session-0");
    env.Merge(0, 1);
    env.Commit(0);
    env.AssertNotLocked(2);
    env.Finish("session-0", 1);
    env.AssertLocked(2);
}

Y_UNIT_TEST(OldSdkFinishedParentsAllowIndependentChild) {
    TScaleEnv env;
    env.CreateParents(2);
    env.RegisterSession("session-0");
    env.Merge(0, 1);
    env.Finish("session-0", 0, /*scaleAware=*/false, /*fromEnd=*/true);
    env.AssertNotLocked(2);
    env.Finish("session-0", 1, /*scaleAware=*/false, /*fromEnd=*/true);
    env.AssertLocked(2);
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

Y_UNIT_TEST(ThirdSessionDoesNotSplitUncommittedMergeFamily) {
    TScaleEnv env;
    env.CreateParents(2);
    env.RegisterSession("session-0");
    env.Merge(0, 1);
    env.Finish("session-0", 0);
    env.Finish("session-0", 1);
    env.AssertSameSession({0, 1, 2});

    env.RegisterSession("session-1");
    env.AssertSameSession({0, 1, 2});
}

Y_UNIT_TEST(UnaffectedPartitionBalancesIndependently) {
    TScaleEnv env;
    env.CreateParents(3);
    env.RegisterSession("session-0");
    env.Merge(0, 1);

    env.Finish("session-0", 0);
    env.AssertNotLocked(3);
    UNIT_ASSERT_C(!env.SessionOf(2).empty(), "unaffected partition 2 must stay readable");

    env.Finish("session-0", 1);
    env.AssertLocked(3);
    UNIT_ASSERT_C(!env.SessionOf(2).empty(), "unaffected partition 2 must stay readable after merge");
}

Y_UNIT_TEST(ChainedMergeWaitsForAllAncestors) {
    TScaleEnv env;
    env.CreateParents(3);
    env.RegisterSession("session-0");
    env.Merge(0, 1);
    env.Finish("session-0", 0);
    env.Finish("session-0", 1);
    env.AssertLocked(3);

    env.Merge(3, 2);
    env.AssertNotLocked(4);
    env.Finish("session-0", 3);
    env.AssertNotLocked(4);
    env.Finish("session-0", 2);
    env.AssertLocked(4, "session-0");
}

Y_UNIT_TEST(SplitThenMergeChildrenWaitsForBothChildren) {
    TScaleEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.AssertLocked(0);

    env.Split(0);
    env.Finish("session-0", 0);
    env.AssertLocked(1);
    env.AssertLocked(2);

    env.Merge(1, 2);
    env.AssertNotLocked(3);
    env.Finish(env.SessionOf(1), 1);
    env.AssertNotLocked(3);
    env.Finish(env.SessionOf(2), 2);
    env.AssertLocked(3);
}

Y_UNIT_TEST(MergeAfterParentsAlreadyFinished) {
    TScaleEnv env;
    env.CreateParents(2);
    env.RegisterSession("session-0");
    env.Finish("session-0", 0);
    env.Finish("session-0", 1);
    env.Merge(0, 1);
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

Y_UNIT_TEST(PipeBreakBeforeMergeConfig) {
    TScaleEnv env;
    env.CreateParents(2);
    auto [s0, s1] = env.TwoSessionsOnParents();

    env.CloseSession(s0);
    env.Merge(0, 1);
    env.AssertNotLocked(2);

    env.RegisterSession("session-new");
    env.Finish(env.SessionOf(0), 0);
    env.Finish(env.SessionOf(1), 1);
    env.AssertLocked(2);
}

Y_UNIT_TEST(PipeBreakAfterMergeBeforeParentsFinished) {
    TScaleEnv env;
    env.CreateParents(2);
    auto [s0, s1] = env.TwoSessionsOnParents();
    env.Merge(0, 1);
    env.AssertNotLocked(2);

    env.CloseSession(s0);
    env.AssertNotLocked(2);

    env.RegisterSession("session-new");
    env.Finish(env.SessionOf(0), 0);
    env.AssertNotLocked(2);
    env.Finish(env.SessionOf(1), 1);
    env.AssertLocked(2);
}

Y_UNIT_TEST(PipeBreakOfUnreadParentAfterFirstFinished) {
    TScaleEnv env;
    env.CreateParents(2);
    auto [s0, s1] = env.TwoSessionsOnParents();
    env.Merge(0, 1);

    env.Finish(s0, 0);
    env.AssertNotLocked(2);
    env.CloseSession(s1);
    env.AssertNotLocked(2);

    env.RegisterSession("session-new");
    const TString remaining = env.SessionOf(1);
    UNIT_ASSERT_C(!remaining.empty(), "unread parent must be taken by the new session");
    env.Finish(remaining, 1);
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
    // Crash: closing the merge-target session, then the releasing session,
    // unregistered Sessions before Reset(Merge). Balance had re-locked the
    // target family onto the dying pipe; AttachePartitions then asserted
    // "family session is not registered".
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

Y_UNIT_TEST(PipeBreakAfterChildAttached) {
    TScaleEnv env;
    env.CreateParents(2);
    auto [s0, s1] = env.TwoSessionsOnParents();
    env.Merge(0, 1);
    env.Finish(s0, 0);
    env.Finish(s1, 1);
    env.AssertLocked(2);

    const TString holder = env.SessionOf(2);
    UNIT_ASSERT_C(!holder.empty(), "child must have a holder before the pipe break");
    env.CloseSession(holder);

    if (env.Pipes.empty()) {
        env.RegisterSession("session-new");
        env.AssertLocked(2, "session-new");
    } else {
        env.AssertLocked(2);
    }
}

Y_UNIT_TEST(PipeBreakBothSessionsAfterMergeBeforeFinish) {
    TScaleEnv env;
    env.CreateParents(2);
    auto [s0, s1] = env.TwoSessionsOnParents();
    env.Merge(0, 1);
    env.AssertNotLocked(2);

    env.CloseSession(s0);
    env.CloseSession(s1);
    env.AssertNotLocked(2);

    env.RegisterSession("session-new");
    env.Finish("session-new", 0);
    env.AssertNotLocked(2);
    env.Finish("session-new", 1);
    env.AssertLocked(2, "session-new");
}

Y_UNIT_TEST(PipeBreakSameSessionAfterFirstParentFinished) {
    TScaleEnv env;
    env.CreateParents(2);
    env.RegisterSession("session-0");
    env.Merge(0, 1);
    env.Finish("session-0", 0);
    env.AssertNotLocked(2);

    env.CloseSession("session-0");
    env.AssertNotLocked(2);

    env.RegisterSession("session-new");
    env.Finish("session-new", 0);
    env.AssertNotLocked(2);
    env.Finish("session-new", 1);
    env.AssertLocked(2, "session-new");
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

Y_UNIT_TEST(DisconnectAfterUncommittedMergeDoesNotDoubleLock) {
    TScaleEnv env;
    env.CreateParents(2);
    env.RegisterSession("session-0");
    env.Merge(0, 1);
    env.Finish("session-0", 0);
    env.Finish("session-0", 1);
    env.AssertSameSession({0, 1, 2});

    env.RegisterSession("session-1");
    env.AssertSameSession({0, 1, 2});

    const TString holder = env.SessionOf(2);
    env.CloseSession(holder);
    env.AssertSameSession({0, 1, 2});
    env.AssertEvenDistribution(3, 1);
}

Y_UNIT_TEST(DisconnectAfterDelayedMergeDoesNotDoubleLock) {
    TScaleEnv env;
    env.CreateParents(2);
    auto [s0, s1] = env.TwoSessionsOnParents();
    env.Merge(0, 1);

    env.Finish(s0, 0);
    env.Finish(s1, 1, /*scaleAware=*/true, /*fromEnd=*/true, /*pump=*/false);

    auto pending = env.WaitRelease();
    UNIT_ASSERT_C(pending, "second finished parent must trigger a family release to merge");
    auto pipeIt = env.Pipes.find(pending->Session);
    UNIT_ASSERT(pipeIt != env.Pipes.end());
    env.AckRelease(pipeIt->second, pending->Partition, pending->Session);
    env.Pump();
    env.AssertLocked(2);

    const TString holder = env.SessionOf(2);
    UNIT_ASSERT_C(!holder.empty(), "merged child must stay assigned");
    UNIT_ASSERT_C(env.Pipes.size() > 1, "a second session must keep the consumer alive");
    env.CloseSession(holder);
    env.AssertSameSession({0, 1, 2});
    env.AssertEvenDistribution(3, 1);
}

Y_UNIT_TEST(ChainedMergeDescendantsStayTogetherThenReconnectOnce) {
    TScaleEnv env;
    env.CreateParents(3);
    env.RegisterSession("session-0");
    env.Merge(0, 1);
    env.Finish("session-0", 0);
    env.Finish("session-0", 1);
    env.AssertLocked(3);

    env.Merge(3, 2);
    env.AssertNotLocked(4);
    env.Finish("session-0", 3);
    env.Finish("session-0", 2);
    env.AssertSameSession({0, 1, 2, 3, 4});

    env.RegisterSession("session-1");
    env.AssertSameSession({0, 1, 2, 3, 4});

    const TString holder = env.SessionOf(4);
    env.CloseSession(holder);
    env.AssertSameSession({0, 1, 2, 3, 4});
    env.AssertEvenDistribution(5, 1);
}

} // Y_UNIT_TEST_SUITE(TPqrbMergeBalancing)

Y_UNIT_TEST_SUITE(TPqrbSplitBalancing) {

Y_UNIT_TEST(ChildrenNotLockedUntilParentFinished) {
    TScaleEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.AssertLocked(0);

    env.Split(0);
    env.AssertNotLocked(1);
    env.AssertNotLocked(2);

    env.Finish("session-0", 0);
    env.AssertLocked(1, "session-0");
    env.AssertLocked(2, "session-0");
    env.AssertSameSession({0, 1, 2});
}

Y_UNIT_TEST(ThirdSessionDoesNotSplitUncommittedFamily) {
    TScaleEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.Split(0);
    env.Finish("session-0", 0);
    env.AssertSameSession({0, 1, 2});

    env.RegisterSession("session-1");
    env.AssertSameSession({0, 1, 2});
}

Y_UNIT_TEST(ChildrenIndependentAfterParentCommitted) {
    TScaleEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.Split(0);
    env.AssertNotLocked(1);
    env.AssertNotLocked(2);

    env.Commit(0);
    env.AssertLocked(1);
    env.AssertLocked(2);
}

Y_UNIT_TEST(CommitBreaksUpFamilySoSecondSessionCanTakeChild) {
    TScaleEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.Split(0);
    env.Finish("session-0", 0);
    env.AssertSameSession({0, 1, 2});

    env.Commit(0);
    env.RegisterSession("session-child", {2});
    env.AssertLocked(1, "session-child");
    UNIT_ASSERT_C(!env.SessionOf(2).empty(), "the other split child must stay assigned");
}

Y_UNIT_TEST(ScaleAwareFinishCommitThenSecondCommonSessionKeepsChildrenIndependent) {
    TScaleEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.Split(0);
    env.Finish("session-0", 0);
    env.AssertSameSession({0, 1, 2});

    env.Commit(0);
    env.RegisterSession("session-1");
    env.AssertEvenDistribution(3, 2);

    const TString parentSession = env.SessionOf(0);
    UNIT_ASSERT_C(!parentSession.empty(), "parent must stay assigned after rebalance");
    absl::flat_hash_set<TString> childSessions;
    ui32 childrenWithParent = 0;
    for (ui32 child : {1u, 2u}) {
        const TString childSession = env.SessionOf(child);
        UNIT_ASSERT_C(!childSession.empty(), "child " << child << " must stay assigned");
        childSessions.insert(childSession);
        if (childSession == parentSession) {
            ++childrenWithParent;
        }
    }
    UNIT_ASSERT_C(childrenWithParent < 2,
        "committed split children must stay in their own families, not glued back to the parent");
    UNIT_ASSERT_C(childSessions.size() == 2 || childrenWithParent == 0,
        "independent child families must be able to sit on a different session than the parent");
}

Y_UNIT_TEST(FinishCommitThenCloseParentPipeKeepsChildrenOnOtherSession) {
    TScaleEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.Split(0);
    env.Finish("session-0", 0);
    env.Commit(0);
    env.RegisterSession("session-1");
    env.AssertEvenDistribution(3, 2);

    const TString parentSession = env.SessionOf(0);
    UNIT_ASSERT_C(!parentSession.empty(), "parent must be locked before the pipe break");
    UNIT_ASSERT_C(env.Pipes.contains(parentSession), parentSession);
    env.CloseSession(parentSession);

    env.AssertLocked(1);
    env.AssertLocked(2);
    env.AssertLocked(0);
    UNIT_ASSERT_VALUES_UNEQUAL(env.SessionOf(0), parentSession);
}

Y_UNIT_TEST(FinishParentWhileFamilyReleasingAttachesChildrenAfterUnlock) {
    TScaleEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.Split(0);
    env.AssertLocked(0, "session-0");
    env.AssertNotLocked(1);
    env.AssertNotLocked(2);

    env.RegisterSession("session-pref", {1}, /*pump=*/false);
    auto pending = env.WaitRelease();
    UNIT_ASSERT_C(pending, "preferred session must release the parent family");
    UNIT_ASSERT_VALUES_EQUAL(pending->Partition, 0u);

    env.Finish(pending->Session, 0, /*scaleAware=*/true, /*fromEnd=*/true, /*pump=*/false);

    auto pipeIt = env.Pipes.find(pending->Session);
    UNIT_ASSERT(pipeIt != env.Pipes.end());
    env.AckRelease(pipeIt->second, pending->Partition, pending->Session);
    env.Pump();

    env.AssertLocked(1);
    env.AssertLocked(2);
    env.AssertLocked(0);
    env.AssertSameSession({0, 1, 2});
}

Y_UNIT_TEST(CommitThenRebalanceKeepsChildrenIndependent) {
    TScaleEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.Split(0);
    env.Finish("session-0", 0);
    env.AssertSameSession({0, 1, 2});

    env.Commit(0);
    env.RegisterSession("session-1");
    env.AssertEvenDistribution(3, 2);

    const TString parentSession = env.SessionOf(0);
    UNIT_ASSERT_C(!parentSession.empty(), "parent must stay assigned");
    ui32 childrenWithParent = 0;
    for (ui32 child : {1u, 2u}) {
        const TString childSession = env.SessionOf(child);
        UNIT_ASSERT_C(!childSession.empty(), "child " << child << " must stay assigned");
        if (childSession == parentSession) {
            ++childrenWithParent;
        }
    }
    UNIT_ASSERT_C(childrenWithParent < 2,
        "committed split children must not be glued back to the parent family on rebalance");
}

Y_UNIT_TEST(CommitThenParentReleaseDoesNotReattachSplitChildren) {
    TScaleEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.Split(0);
    env.Finish("session-0", 0);
    env.AssertSameSession({0, 1, 2});

    env.Commit(0);
    env.RegisterSession("session-child", {2});
    env.AssertLocked(1, "session-child");

    env.RegisterSession("session-pref", {1});
    env.AssertLocked(0, "session-pref");
    env.AssertLocked(1, "session-child");
    UNIT_ASSERT_C(!env.SessionOf(2).empty(), "the other split child must stay assigned");
    UNIT_ASSERT_VALUES_UNEQUAL(env.SessionOf(2), env.SessionOf(0));
}

Y_UNIT_TEST(RereadParentThenSecondSessionDoesNotReattachChildren) {
    TScaleEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.Split(0);
    env.Finish("session-0", 0);
    env.AssertSameSession({0, 1, 2});

    env.StartReading("session-0", 0);
    env.RegisterSession("session-1");
    env.AssertLocked(0);
    env.AssertNotLocked(1);
    env.AssertNotLocked(2);
}

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

Y_UNIT_TEST(OldSdkFromEndAllowsIndependentChildren) {
    TScaleEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.Split(0);
    env.Finish("session-0", 0, /*scaleAware=*/false, /*fromEnd=*/true);
    env.AssertLocked(1);
    env.AssertLocked(2);
}

Y_UNIT_TEST(OldSdkNotFromEndDoesNotLockChildren) {
    TScaleEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.Split(0);
    env.Finish("session-0", 0, /*scaleAware=*/false, /*fromEnd=*/false);
    env.AssertNotLocked(1);
    env.AssertNotLocked(2);
}

Y_UNIT_TEST(UnaffectedPartitionStaysReadableDuringSplit) {
    TScaleEnv env;
    env.CreateParents(2);
    env.RegisterSession("session-0");
    env.Split(0);
    UNIT_ASSERT_C(!env.SessionOf(1).empty(), "unaffected partition 1 must stay readable");
    env.AssertNotLocked(2);
    env.AssertNotLocked(3);

    env.Finish("session-0", 0);
    env.AssertLocked(2);
    env.AssertLocked(3);
    UNIT_ASSERT_C(!env.SessionOf(1).empty(), "unaffected partition 1 must stay readable after split");
}

Y_UNIT_TEST(ChainedSplitWaitsForAncestors) {
    TScaleEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.Split(0);
    env.AssertNotLocked(1);
    env.Finish("session-0", 0);
    env.AssertLocked(1);
    env.AssertLocked(2);

    env.Split(1);
    env.AssertNotLocked(3);
    env.AssertNotLocked(4);
    env.Finish(env.SessionOf(1), 1);
    env.AssertLocked(3);
    env.AssertLocked(4);
}

Y_UNIT_TEST(ChainedSplitDescendantsStayTogetherThenIndependentAfterCommit) {
    TScaleEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.Split(0);
    env.Finish("session-0", 0);
    env.Split(1);
    env.Finish(env.SessionOf(1), 1);
    env.AssertSameSession({0, 1, 2, 3, 4});

    env.RegisterSession("session-1");
    env.AssertSameSession({0, 1, 2, 3, 4});

    env.Commit(0);
    env.Commit(1);
    env.AssertEvenDistribution(5, 2);
}

Y_UNIT_TEST(DisconnectAfterSplitThenMergeDoesNotDoubleLock) {
    TScaleEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.Split(0);
    env.Finish("session-0", 0);
    env.Merge(1, 2);
    env.Finish(env.SessionOf(1), 1);
    env.Finish(env.SessionOf(2), 2);
    env.AssertLocked(3);
    env.AssertSameSession({0, 1, 2, 3});

    env.RegisterSession("session-1");
    env.AssertSameSession({0, 1, 2, 3});

    const TString holder = env.SessionOf(3);
    env.CloseSession(holder);
    env.AssertSameSession({0, 1, 2, 3});
    env.AssertEvenDistribution(4, 1);
}

Y_UNIT_TEST(SplitAfterParentAlreadyFinished) {
    TScaleEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.Finish("session-0", 0);
    env.Split(0);
    env.AssertLocked(1);
    env.AssertLocked(2);
}

Y_UNIT_TEST(RereadParentAfterIndependentChildren) {
    TScaleEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.Split(0);
    env.Finish("session-0", 0, /*scaleAware=*/false, /*fromEnd=*/true);
    env.AssertLocked(1);
    env.AssertLocked(2);

    env.StartReading("session-0", 0);
    env.AssertNotLocked(1);
    env.AssertNotLocked(2);
    env.AssertLocked(0, "session-0");
}

Y_UNIT_TEST(RereadParentAfterScaleAwareChildren) {
    TScaleEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.Split(0);
    env.Finish("session-0", 0);
    env.AssertSameSession({0, 1, 2});

    env.StartReading("session-0", 0);
    env.AssertNotLocked(1);
    env.AssertNotLocked(2);
    env.AssertLocked(0, "session-0");
}

Y_UNIT_TEST(PipeBreakBeforeSplitConfig) {
    TScaleEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.CloseSession("session-0");

    env.Split(0);
    env.RegisterSession("session-new");
    env.AssertNotLocked(1);
    env.AssertNotLocked(2);
    env.Finish("session-new", 0);
    env.AssertLocked(1);
    env.AssertLocked(2);
}

Y_UNIT_TEST(PipeBreakAfterSplitBeforeParentFinished) {
    TScaleEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.Split(0);
    env.AssertNotLocked(1);
    env.CloseSession("session-0");
    env.AssertNotLocked(1);
    env.AssertNotLocked(2);

    env.RegisterSession("session-new");
    env.Finish("session-new", 0);
    env.AssertLocked(1);
    env.AssertLocked(2);
}

Y_UNIT_TEST(PipeBreakAfterChildrenAttached) {
    TScaleEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.RegisterSession("session-1");
    env.Split(0);

    const TString holder = env.SessionOf(0);
    UNIT_ASSERT_C(!holder.empty(), "parent must be locked before finish");
    env.Finish(holder, 0);
    env.AssertLocked(1);
    env.AssertLocked(2);

    env.CloseSession(holder);
    if (env.Pipes.empty()) {
        env.RegisterSession("session-new");
        env.Finish("session-new", 0);
    }
    env.AssertLocked(1);
    env.AssertLocked(2);
}

Y_UNIT_TEST(FinishAndCommitDuringReleaseDoesNotStealChildMapping) {
    // Autopart test_commit_reread: split + commit while the parent family is Releasing.
    // Finish of a preferred session cannot attach children 1,2, so
    // AttachePartitions only AppendUniqueRoots. Commit then sees a lonely
    // family and CreateFamily's those children. AfterRelease remaps
    // RootPartitions onto the parent and steals mapping; Balance StartReading
    // of the leftover child family aborts: "partition mapping mismatch".
    TScaleEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0", {1});
    env.RegisterSession("session-common");
    env.Split(0);
    env.AssertLocked(0, "session-0");

    NActors::TBlockEvents<TEvPQ::TEvBalanceConsumer> blockBalance(*env.tc.Runtime);

    env.Finish("session-0", 0, /*scaleAware=*/true, /*fromEnd=*/true, /*pump=*/false);
    auto pending = env.WaitRelease();
    UNIT_ASSERT_C(pending, "attaching unread children must release the parent family");
    UNIT_ASSERT_VALUES_EQUAL(pending->Partition, 0u);

    // Deliver Commit and Unlock while Balance is blocked so AfterRelease
    // steals mapping before leftover child families StartReading.
    ForwardToTablet(
        *env.tc.Runtime,
        env.tc.BalancerTabletId,
        env.tc.Edge,
        new TEvPQ::TEvReadingPartitionStatusRequest("user", 0, 1, 1)
    );

    auto pipeIt = env.Pipes.find("session-0");
    UNIT_ASSERT(pipeIt != env.Pipes.end());
    {
        auto released = MakeHolder<TEvPersQueue::TEvPartitionReleased>();
        released->Record.SetSession(pending->Session);
        released->Record.SetPartition(pending->Partition);
        released->Record.SetTopic("topic");
        released->Record.SetClientId("user");
        ActorIdToProto(pipeIt->second, released->Record.MutablePipeClient());
        ForwardToTablet(
            *env.tc.Runtime,
            env.tc.BalancerTabletId,
            env.tc.Edge,
            released.Release()
        );
    }
    DispatchFor(env.tc, TDuration::MilliSeconds(50));

    blockBalance.Unblock();
    blockBalance.Stop();
    env.Pump();

    env.AssertLocked(1);
    env.AssertLocked(2);
}

Y_UNIT_TEST(PipeBreakAfterParentCommitted) {
    TScaleEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.Split(0);
    env.Commit(0);
    env.AssertLocked(1);
    env.AssertLocked(2);

    env.CloseSession("session-0");
    env.RegisterSession("session-new");
    env.Commit(0);
    env.AssertLocked(1);
    env.AssertLocked(2);
}

Y_UNIT_TEST(NestedSplitRereadInnerParentThenFinishLocksGrandchildren) {
    // Nested parent 1 is not a family root (root is 0). StartReading(1) must
    // pull 3,4 out of the parent family; otherwise they stay unreadable inside
    // it and Finish(1) never locks them.
    TScaleEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.Split(0);
    env.Finish("session-0", 0);
    env.AssertLocked(1);
    env.AssertLocked(2);

    env.Split(1);
    env.Finish("session-0", 1);
    env.AssertLocked(3);
    env.AssertLocked(4);

    env.StartReading("session-0", 1);
    env.Finish("session-0", 1);
    env.AssertLocked(3);
    env.AssertLocked(4);
    env.AssertSameSession({0, 1, 3, 4});
}

Y_UNIT_TEST(RereadOneMergeParentThenFinishAgainLocksChild) {
    TScaleEnv env;
    env.CreateParents(2);
    env.RegisterSession("session-0");
    env.Merge(0, 1);
    env.Finish("session-0", 0);
    env.Finish("session-0", 1);
    env.AssertLocked(2, "session-0");

    env.StartReading("session-0", 0);
    env.Finish("session-0", 0);
    env.AssertLocked(2, "session-0");
    env.AssertSameSession({0, 1, 2});
}

Y_UNIT_TEST(NestedSplitRereadRootThenFinishRelocksWholeTree) {
    // Crash: StartReading(0) after 0→1,2 and 1→3,4 all finished used to call
    // ActivatePartition on every descendant. Reset() is true for still-active
    // children (NeedReleaseChildren), so InactivePartitionCount underflowed.
    TScaleEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.Split(0);
    env.Finish("session-0", 0);
    env.Split(1);
    env.Finish(env.SessionOf(1), 1);
    env.AssertLocked(3);
    env.AssertLocked(4);

    env.StartReading("session-0", 0);
    env.AssertLocked(0, "session-0");
    env.AssertNotLocked(1);
    env.AssertNotLocked(2);
    env.AssertNotLocked(3);
    env.AssertNotLocked(4);

    env.Finish("session-0", 0);
    env.Finish("session-0", 1);
    env.AssertLocked(2);
    env.AssertLocked(3);
    env.AssertLocked(4);
    env.AssertSameSession({0, 1, 2, 3, 4});
}

Y_UNIT_TEST(SecondFinishOfMergedParentsDoesNotAbort) {
    // Finish of parents whose child is already in the same family used to hit
    // MergeFamilies(lhs, lhs).
    TScaleEnv env;
    env.CreateParents(2);
    env.RegisterSession("session-0");
    env.Merge(0, 1);
    env.Finish("session-0", 0);
    env.Finish("session-0", 1);
    env.AssertLocked(2, "session-0");

    env.Finish("session-0", 0);
    env.Finish("session-0", 1);
    env.AssertLocked(2, "session-0");
    env.AssertSameSession({0, 1, 2});
}

Y_UNIT_TEST(FinishBeforeSplitLocksChildren) {
    // SDK can Finish a sealed parent before the balancer has the new graph.
    TScaleEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.Finish("session-0", 0);
    env.Split(0);
    env.AssertLocked(1);
    env.AssertLocked(2);
}

Y_UNIT_TEST(CommitBeforeSplitLocksChildren) {
    TScaleEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.Commit(0);
    env.Split(0);
    env.AssertLocked(1);
    env.AssertLocked(2);
}

Y_UNIT_TEST(RereadClearsPendingFinishBeforeSplit) {
    TScaleEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.Finish("session-0", 0);
    env.StartReading("session-0", 0);
    env.Split(0);
    env.AssertNotLocked(1);
    env.AssertNotLocked(2);
    env.Finish("session-0", 0);
    env.AssertLocked(1);
    env.AssertLocked(2);
}

Y_UNIT_TEST(InFlightFinishAfterHolderDisconnectLocksChildren) {
    // Live consumer: Finish still in flight after the holder pipe dies.
    TScaleEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.RegisterSession("session-hold");
    env.Split(0);
    const TString holder = env.SessionOf(0);
    UNIT_ASSERT_C(!holder.empty(), "parent must be locked before disconnect");
    const auto pipe = env.Pipes[holder];
    env.CloseSession(holder);
    env.InjectFinish(pipe, 0);
    env.AssertLocked(1);
    env.AssertLocked(2);
}

Y_UNIT_TEST(InjectFinishOfSecondMergeParentAfterSessionCloseLocksChild) {
    // Autopart test_commit_reread: Finish of the second merge parent arrives on a
    // dead pipe while the consumer still has another session. MergeFamilies must
    // attach the child without aborting.
    TScaleEnv env;
    env.CreateParents(2);
    auto [s0, s1] = env.TwoSessionsOnParents();
    env.Merge(0, 1);
    env.Finish(s0, 0);
    env.AssertNotLocked(2);
    const auto pipe = env.Pipes[s1];
    env.CloseSession(s1);
    env.InjectFinish(pipe, 1);
    env.AssertLocked(2);
}

Y_UNIT_TEST(DisconnectOfReaderKeepsFinishSoChildrenMigrateWithParent) {
    // Live consumer: Finish stays on the partitions the dying session read.
    // The uncommitted family migrates as a unit onto the remaining session.
    TScaleEnv env;
    env.CreateParents(2);
    env.RegisterSession("session-0");
    env.RegisterSession("session-hold");
    env.Split(0);
    const TString reader = env.SessionOf(0);
    env.Finish(reader, 0);
    env.AssertLocked(2);
    env.AssertLocked(3);

    env.CloseSession(reader);
    env.AssertLocked(0);
    env.AssertLocked(2);
    env.AssertLocked(3);
    env.AssertSameSession({0, 2, 3});
}

Y_UNIT_TEST(SessionMigrateAfterFinishKeepsChildrenOnNewSession) {
    // Live consumer: Finish stays, re-lock of 0 is membership, children remain
    // readable and migrate with the parent family.
    TScaleEnv env;
    env.CreateParents(2);
    env.RegisterSession("session-0");
    env.RegisterSession("session-hold");
    env.Split(0);
    const TString reader = env.SessionOf(0);
    env.Finish(reader, 0);
    env.AssertLocked(2);
    env.AssertLocked(3);

    env.CloseSession(reader);
    env.AssertLocked(0);
    env.AssertLocked(2);
    env.AssertLocked(3);
    env.AssertSameSession({0, 2, 3});
}

Y_UNIT_TEST(LastSessionGoneAfterFinishStartsFromRoot) {
    // Last session destroys TConsumer: Finish is lost. The next session starts
    // from the root until it Finish-es (or Commit-s) again.
    TSplitEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.Split(0);
    env.Finish("session-0", 0);
    env.AssertLocked(1);
    env.AssertLocked(2);

    env.CloseSession("session-0");
    env.RegisterSession("session-new");
    env.AssertLocked(0, "session-new");
    env.AssertNotLocked(1);
    env.AssertNotLocked(2);
    env.Finish("session-new", 0);
    env.AssertLocked(1);
    env.AssertLocked(2);
    env.AssertSameSession({0, 1, 2});
}

Y_UNIT_TEST(LastSessionGoneAfterCommitStartsFromRoot) {
    TSplitEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.Split(0);
    env.Commit(0);
    env.AssertLocked(1);
    env.AssertLocked(2);

    env.CloseSession("session-0");
    env.RegisterSession("session-new");
    env.AssertLocked(0, "session-new");
    env.AssertNotLocked(1);
    env.AssertNotLocked(2);
    env.Commit(0);
    env.AssertLocked(1);
    env.AssertLocked(2);
}

Y_UNIT_TEST(RereadThenFinishAgainLocksChildren) {
    TSplitEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.Split(0);
    env.Finish("session-0", 0);
    env.AssertSameSession({0, 1, 2});

    env.StartReading("session-0", 0);
    env.AssertLocked(0, "session-0");
    env.AssertNotLocked(1);
    env.AssertNotLocked(2);

    env.Finish("session-0", 0);
    env.AssertLocked(1, "session-0");
    env.AssertLocked(2, "session-0");
    env.AssertSameSession({0, 1, 2});
}

Y_UNIT_TEST(RereadThenCommitLocksChildren) {
    TSplitEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.Split(0);
    env.Finish("session-0", 0);
    env.StartReading("session-0", 0);
    env.AssertNotLocked(1);
    env.AssertNotLocked(2);

    env.Commit(0);
    env.AssertLocked(1);
    env.AssertLocked(2);
}

Y_UNIT_TEST(RereadAfterCommitUnlocksChildrenUntilFinishAgain) {
    // README Split §4: reread of 0 drops 1 and 2 until 0 Finish or Commit again.
    TSplitEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.Split(0);
    env.Finish("session-0", 0);
    env.Commit(0);
    env.AssertLocked(1);
    env.AssertLocked(2);

    env.StartReading("session-0", 0);
    env.AssertLocked(0, "session-0");
    env.AssertNotLocked(1);
    env.AssertNotLocked(2);

    env.Finish("session-0", 0);
    env.AssertLocked(1, "session-0");
    env.AssertLocked(2, "session-0");
}

Y_UNIT_TEST(OldSdkFromEndChildrenAreIndependentFamilies) {
    // Old SDK + StartedReadingFromEndOffset: children are separate families,
    // so a second session may take a child without Commit.
    TSplitEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.Split(0);
    env.Finish("session-0", 0, /*scaleAware=*/false, /*fromEnd=*/true);
    env.AssertLocked(1);
    env.AssertLocked(2);

    env.RegisterSession("session-1");
    env.AssertEvenDistribution(3, 2);
}

Y_UNIT_TEST(OldSdkNotFromEndDelayThenOtherSessionFromEndLocksChildren) {
    TSplitEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.RegisterSession("session-1");
    env.Split(0);
    const TString first = env.SessionOf(0);
    UNIT_ASSERT_C(!first.empty(), "parent must be locked");

    env.Finish(first, 0, /*scaleAware=*/false, /*fromEnd=*/false);
    env.AssertNotLocked(1);
    env.AssertNotLocked(2);

    env.tc.Runtime->AdvanceCurrentTime(TDuration::Seconds(3));
    auto pending = env.WaitRelease(TDuration::Seconds(2));
    UNIT_ASSERT_C(pending, "old SDK Finish without from-end must release the parent after delay");
    UNIT_ASSERT_VALUES_EQUAL(pending->Partition, 0u);
    auto pipeIt = env.Pipes.find(pending->Session);
    UNIT_ASSERT(pipeIt != env.Pipes.end());
    env.AckRelease(pipeIt->second, pending->Partition, pending->Session);
    env.LockedBy.erase(pending->Partition);
    env.Pump();

    const TString afterDelay = env.SessionOf(0);
    UNIT_ASSERT_C(!afterDelay.empty(), "parent must be re-locked after the heuristic release");
    env.AssertNotLocked(1);
    env.AssertNotLocked(2);

    env.Finish(afterDelay, 0, /*scaleAware=*/false, /*fromEnd=*/true);
    env.AssertLocked(1);
    env.AssertLocked(2);
}

Y_UNIT_TEST(CommitOfResetInnerParentDuringDestroyUnderflowsActiveCount) {
    // Nested split 0→1,2 then 1→3,4. Finish+Commit 0 and 1. Reread of 0 Reset()s
    // inner parent 1 and DestroyFamily({1}) starts Release. Until Unlock, FindFamily(1)
    // still points at that family. A newer tablet Commit must not underflow
    // ActivePartitionCount: Reset left the family counted inactive, so
    // wasInactive=false + InactivatePartition used to abort.
    // test_commit_reread[old_sdk] hits this: rewind/session churn rereads an ancestor
    // while PQ still sends EndOffset commits for sealed inner parents.
    TSplitEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.Split(0);
    env.Finish("session-0", 0, /*scaleAware=*/false, /*fromEnd=*/true);
    env.Commit(0);
    const TString holder1 = env.SessionOf(1);
    UNIT_ASSERT_C(!holder1.empty(), "inner parent must be locked after first split");
    env.Split(1);
    env.Finish(holder1, 1, /*scaleAware=*/false, /*fromEnd=*/true);
    env.Commit(1, /*generation=*/1, /*cookie=*/2);

    env.InjectStartReading(env.Pipes["session-0"], 0);
    auto pending = env.WaitRelease();
    UNIT_ASSERT_C(pending, "reread of root must release a descendant family");

    env.Commit(1, /*generation=*/1, /*cookie=*/3);
    env.Finish("session-0", 0, /*scaleAware=*/false, /*fromEnd=*/true);
    env.AssertLocked(2);
    env.AssertLocked(3);
    env.AssertLocked(4);
}

Y_UNIT_TEST(StaleStartReadingAfterLastSessionGoneDoesNotFinishParent) {
    TSplitEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.Split(0);
    const auto pipe = env.Pipes["session-0"];
    env.CloseSession("session-0");
    env.InjectStartReading(pipe, 0);
    env.RegisterSession("session-new");
    env.AssertLocked(0, "session-new");
    env.AssertNotLocked(1);
    env.AssertNotLocked(2);
    env.Finish("session-new", 0);
    env.AssertLocked(1);
    env.AssertLocked(2);
}

} // Y_UNIT_TEST_SUITE(TPqrbSplitBalancing)

Y_UNIT_TEST_SUITE(TPqrbBalancingInvariants) {

Y_UNIT_TEST(EachPartitionLockedByExactlyOneSession) {
    TScaleEnv env;
    env.CreateParents(4);
    env.RegisterSession("session-0");
    env.RegisterSession("session-1");
    env.AssertEvenDistribution(4, 2);
}

Y_UNIT_TEST(FamiliesGoToLeastLoadedSessions) {
    TScaleEnv env;
    env.CreateParents(6);
    env.RegisterSession("session-0");
    env.RegisterSession("session-1");
    env.RegisterSession("session-2");
    env.AssertEvenDistribution(6, 3);
}

Y_UNIT_TEST(UncommittedFamilyIsBalancedAsOneUnit) {
    TScaleEnv env;
    env.CreateParents(3);
    env.RegisterSession("session-0");
    env.Merge(0, 1);
    env.Finish("session-0", 0);
    env.Finish("session-0", 1);
    env.AssertSameSession({0, 1, 3});

    env.RegisterSession("session-1");
    env.AssertSameSession({0, 1, 3});
    UNIT_ASSERT_C(!env.SessionOf(2).empty(), "unaffected partition must stay assigned");
    UNIT_ASSERT_VALUES_UNEQUAL(env.SessionOf(2), env.SessionOf(3));
}

Y_UNIT_TEST(ScaleAwareFinishNotFromEndGivesChildren) {
    TScaleEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.Split(0);
    env.AssertNotLocked(1);
    env.Finish("session-0", 0, /*scaleAware=*/true, /*fromEnd=*/false);
    env.AssertLocked(1, "session-0");
    env.AssertLocked(2, "session-0");
}

Y_UNIT_TEST(CommitOfUnreadableChildIsIgnored) {
    TScaleEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.Split(0);
    env.Commit(1);
    env.AssertNotLocked(1);
    env.AssertNotLocked(2);
    env.Finish("session-0", 0);
    env.AssertLocked(1);
    env.AssertLocked(2);
}

Y_UNIT_TEST(FinishThenCommitIsIdempotent) {
    TScaleEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.Split(0);
    env.Finish("session-0", 0);
    env.Commit(0);
    env.AssertLocked(1);
    env.AssertLocked(2);
}

Y_UNIT_TEST(CommitThenFinishIsIdempotent) {
    TScaleEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.Split(0);
    env.Commit(0);
    env.Finish("session-0", 0);
    env.AssertLocked(1);
    env.AssertLocked(2);
}

Y_UNIT_TEST(DoubleFinishDoesNotLoseChildren) {
    TScaleEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.Split(0);
    env.Finish("session-0", 0);
    env.Finish("session-0", 0);
    env.AssertLocked(1, "session-0");
    env.AssertLocked(2, "session-0");
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

Y_UNIT_TEST(CommitThenImmediatePipeBreakOnLastSessionRequiresNewCommit) {
    TScaleEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.Split(0);
    env.Commit(0, 1, 1, /*pump=*/false);
    env.CloseSession("session-0");
    env.RegisterSession("session-new");
    env.AssertNotLocked(1);
    env.Commit(0);
    env.AssertLocked(1);
    env.AssertLocked(2);
}

Y_UNIT_TEST(FinishAndCommitRaceThenPipeBreak) {
    TScaleEnv env;
    env.CreateParents(2);
    auto [s0, s1] = env.TwoSessionsOnParents();
    env.Merge(0, 1);
    env.Finish(s0, 0, /*scaleAware=*/true, /*fromEnd=*/true, /*pump=*/false);
    env.Commit(1, 1, 1, /*pump=*/false);
    env.CloseSession(s0);
    env.Finish(s1, 1);
    env.AssertLocked(2);
}

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

Y_UNIT_TEST(StaleCommitAfterLastSessionGoneIsIgnoredUntilReconnect) {
    TScaleEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.Split(0);
    env.CloseSession("session-0");
    env.InjectCommit(0);
    env.RegisterSession("session-new");
    env.AssertNotLocked(1);
    env.Commit(0);
    env.AssertLocked(1);
}

Y_UNIT_TEST(NewRootPartitionGoesToLeastLoadedSession) {
    TScaleEnv env;
    env.CreateParents(2);
    env.RegisterSession("session-0");
    env.RegisterSession("session-1");
    env.AssertEvenDistribution(2, 2);

    const ui32 added = env.AddRoot();
    env.AssertLocked(added);
    env.AssertEvenDistribution(3, 2);
}

Y_UNIT_TEST(NewRootPartitionWhileNoSessions) {
    TScaleEnv env;
    env.CreateParents(1);
    const ui32 added = env.AddRoot();
    env.RegisterSession("session-0");
    env.AssertLocked(0);
    env.AssertLocked(added);
}

Y_UNIT_TEST(NewRootPartitionDuringRebalanceRelease) {
    TScaleEnv env;
    env.CreateParents(2);
    env.RegisterSession("session-0");
    env.AssertLocked(0);
    env.AssertLocked(1);

    env.RegisterSession("session-1", {}, /*pump=*/false);
    auto pending = env.WaitRelease();
    UNIT_ASSERT_C(pending, "second session must trigger a release");
    const ui32 added = env.AddRoot();
    env.Pump();
    env.AssertLocked(added);
    env.AssertEvenDistribution(3, 2);
}

Y_UNIT_TEST(NewRootPartitionThenImmediateSessionClose) {
    TScaleEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.RegisterSession("session-1");
    const ui32 added = env.AddRoot();
    env.CloseSession("session-0");
    env.AssertLocked(0);
    env.AssertLocked(added);
}

Y_UNIT_TEST(PreferredSessionGetsNewMatchingPartition) {
    TScaleEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.AssertLocked(0, "session-0");
    const ui32 added = env.AddRoot();
    UNIT_ASSERT_VALUES_EQUAL(added, 1u);
    env.RegisterSession("session-pref", {2});
    env.AssertLocked(1, "session-pref");
    env.AssertLocked(0, "session-0");
}

Y_UNIT_TEST(NewPartitionAddedWhileParentFinishInFlight) {
    TScaleEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.Split(0);
    env.Finish("session-0", 0, /*scaleAware=*/true, /*fromEnd=*/true, /*pump=*/false);
    const ui32 added = env.AddRoot();
    env.Pump();
    env.AssertLocked(1);
    env.AssertLocked(2);
    env.AssertLocked(added);
}

Y_UNIT_TEST(FinishDuringBalancerRestartIsIgnored) {
    TScaleEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.Split(0);
    env.AssertLocked(0, "session-0");
    env.AssertNotLocked(1);
    env.AssertNotLocked(2);

    NActors::TBlockEvents<TEvTablet::TEvRestored> restored(*env.tc.Runtime, [&](auto& ev) {
        return ev->Get()->TabletID == env.tc.BalancerTabletId
            && ev->GetRecipientRewrite() == ev->Get()->UserTabletActor;
    });

    const TActorId pqrb = env.RebootBalancerAndHoldRestored(restored);
    const TActorId pipe = env.InjectSessionDuringInit(pqrb, "session-0");
    env.SendToBalancerActor(
        pqrb,
        new TEvPersQueue::TEvReadingPartitionFinishedRequest(pipe, "user", 0, true, true),
        env.tc.Edge
    );
    DispatchFor(env.tc);

    restored.Unblock();
    restored.Stop();
    DispatchFor(env.tc, TDuration::MilliSeconds(200));

    env.AssertNotLocked(1);
    env.AssertNotLocked(2);

    env.SendToBalancerActor(
        pqrb,
        new TEvPersQueue::TEvReadingPartitionFinishedRequest(pipe, "user", 0, true, true),
        env.tc.Edge
    );
    DispatchFor(env.tc, TDuration::MilliSeconds(200));
    env.AssertLocked(1);
    env.AssertLocked(2);
}

Y_UNIT_TEST(CommitDuringBalancerRestartMakesChildrenReadable) {
    TScaleEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.Split(0);
    env.AssertLocked(0, "session-0");
    env.AssertNotLocked(1);
    env.AssertNotLocked(2);

    NActors::TBlockEvents<TEvTablet::TEvRestored> restored(*env.tc.Runtime, [&](auto& ev) {
        return ev->Get()->TabletID == env.tc.BalancerTabletId
            && ev->GetRecipientRewrite() == ev->Get()->UserTabletActor;
    });

    const TActorId pqrb = env.RebootBalancerAndHoldRestored(restored);
    env.InjectSessionDuringInit(pqrb, "session-0");
    env.SendToBalancerActor(
        pqrb,
        new TEvPQ::TEvReadingPartitionStatusRequest("user", 0, 1, 1),
        env.tc.Edge
    );
    DispatchFor(env.tc);

    restored.Unblock();
    restored.Stop();
    DispatchFor(env.tc, TDuration::MilliSeconds(200));

    env.AssertLocked(1);
    env.AssertLocked(2);
}

Y_UNIT_TEST(FinishThenDisconnectDuringBalancerRestartIsIgnored) {
    TScaleEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.Split(0);
    env.AssertLocked(0, "session-0");
    env.AssertNotLocked(1);
    env.AssertNotLocked(2);

    NActors::TBlockEvents<TEvTablet::TEvRestored> restored(*env.tc.Runtime, [&](auto& ev) {
        return ev->Get()->TabletID == env.tc.BalancerTabletId
            && ev->GetRecipientRewrite() == ev->Get()->UserTabletActor;
    });

    const TActorId pqrb = env.RebootBalancerAndHoldRestored(restored);
    const TActorId pipe = env.InjectSessionDuringInit(pqrb, "session-0");
    env.SendToBalancerActor(
        pqrb,
        new TEvPersQueue::TEvReadingPartitionFinishedRequest(pipe, "user", 0, true, true),
        env.tc.Edge
    );
    env.SendToBalancerActor(
        pqrb,
        new TEvTabletPipe::TEvServerDisconnected(env.tc.BalancerTabletId, pipe, pipe),
        pipe
    );
    DispatchFor(env.tc);

    restored.Unblock();
    restored.Stop();
    DispatchFor(env.tc, TDuration::MilliSeconds(200));

    env.RegisterSession("session-new");
    env.AssertLocked(0, "session-new");
    env.AssertNotLocked(1);
    env.AssertNotLocked(2);
}

} // Y_UNIT_TEST_SUITE(TPqrbBalancingInvariants)

struct TClassicEnv : TScaleEnv {
    TClassicEnv()
        : TScaleEnv(NKikimrPQ::TPQTabletConfig::DISABLED)
    {
    }
};

Y_UNIT_TEST_SUITE(TPqrbClassicBalancing) {

Y_UNIT_TEST(OneSessionGetsAllPartitions) {
    TClassicEnv env;
    env.CreateParents(4);
    env.RegisterSession("session-0");
    env.AssertEvenDistribution(4, 1);
}

Y_UNIT_TEST(TwoSessionsSplitEvenly) {
    TClassicEnv env;
    env.CreateParents(4);
    env.RegisterSession("session-0");
    env.RegisterSession("session-1");
    env.AssertEvenDistribution(4, 2);
}

Y_UNIT_TEST(ThreeSessionsSplitEvenly) {
    TClassicEnv env;
    env.CreateParents(6);
    env.RegisterSession("session-0");
    env.RegisterSession("session-1");
    env.RegisterSession("session-2");
    env.AssertEvenDistribution(6, 3);
}

Y_UNIT_TEST(UnevenRemainderDiffersByAtMostOne) {
    TClassicEnv env;
    env.CreateParents(5);
    env.RegisterSession("session-0");
    env.RegisterSession("session-1");
    env.AssertEvenDistribution(5, 2);
}

Y_UNIT_TEST(EachPartitionLockedByExactlyOneSession) {
    TClassicEnv env;
    env.CreateParents(4);
    env.RegisterSession("session-0");
    env.RegisterSession("session-1");
    env.AssertEvenDistribution(4, 2);
    absl::flat_hash_set<TString> holders;
    for (ui32 i = 0; i < 4; ++i) {
        const TString session = env.SessionOf(i);
        UNIT_ASSERT_C(!session.empty(), "partition " << i << " must be locked");
        holders.insert(session);
    }
    UNIT_ASSERT_VALUES_EQUAL(holders.size(), 2u);
}

Y_UNIT_TEST(ClosingOneSessionReturnsPartitionsToTheOther) {
    TClassicEnv env;
    env.CreateParents(4);
    env.RegisterSession("session-0");
    env.RegisterSession("session-1");
    env.AssertEvenDistribution(4, 2);
    env.CloseSession("session-1");
    env.AssertEvenDistribution(4, 1);
    env.AssertLocked(0, "session-0");
    env.AssertLocked(1, "session-0");
    env.AssertLocked(2, "session-0");
    env.AssertLocked(3, "session-0");
}

Y_UNIT_TEST(ThreeSessionsThenOneLeavesRemainEven) {
    TClassicEnv env;
    env.CreateParents(6);
    env.RegisterSession("session-0");
    env.RegisterSession("session-1");
    env.RegisterSession("session-2");
    env.AssertEvenDistribution(6, 3);
    env.CloseSession("session-1");
    env.AssertEvenDistribution(6, 2);
}

Y_UNIT_TEST(NewSessionAfterLastDisconnectGetsAllWithoutFinish) {
    TClassicEnv env;
    env.CreateParents(3);
    env.RegisterSession("session-0");
    env.AssertEvenDistribution(3, 1);
    env.CloseSession("session-0");
    env.RegisterSession("session-new");
    env.AssertEvenDistribution(3, 1);
}

Y_UNIT_TEST(PipeBreakDuringRebalanceDoesNotDropPartition) {
    TClassicEnv env;
    env.CreateParents(2);
    env.RegisterSession("session-0");
    env.AssertLocked(0);
    env.AssertLocked(1);

    env.RegisterSession("session-1", {}, /*pump=*/false);
    auto pending = env.WaitRelease();
    UNIT_ASSERT_C(pending, "second session must trigger a release");
    env.CloseSession(pending->Session);
    env.Pump();
    env.AssertLocked(0);
    env.AssertLocked(1);
}

Y_UNIT_TEST(FinishDoesNotHideOrMovePartitions) {
    TClassicEnv env;
    env.CreateParents(4);
    env.RegisterSession("session-0");
    env.RegisterSession("session-1");
    env.AssertEvenDistribution(4, 2);

    absl::flat_hash_map<ui32, TString> before;
    for (ui32 i = 0; i < 4; ++i) {
        before[i] = env.SessionOf(i);
        env.Finish(before[i], i);
    }
    env.AssertEvenDistribution(4, 2);
    for (ui32 i = 0; i < 4; ++i) {
        UNIT_ASSERT_VALUES_EQUAL_C(before[i], env.SessionOf(i), "finish must not reassign partition " << i);
    }
}

Y_UNIT_TEST(CommitDoesNotChangeAssignment) {
    TClassicEnv env;
    env.CreateParents(4);
    env.RegisterSession("session-0");
    env.RegisterSession("session-1");
    env.AssertEvenDistribution(4, 2);

    absl::flat_hash_map<ui32, TString> before;
    for (ui32 i = 0; i < 4; ++i) {
        before[i] = env.SessionOf(i);
        env.Commit(i);
    }
    env.AssertEvenDistribution(4, 2);
    for (ui32 i = 0; i < 4; ++i) {
        UNIT_ASSERT_VALUES_EQUAL_C(before[i], env.SessionOf(i), "commit must not reassign partition " << i);
    }
}

Y_UNIT_TEST(FinishThenStartReadingKeepsPartitionLocked) {
    TClassicEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.AssertLocked(0, "session-0");
    env.Finish("session-0", 0);
    env.StartReading("session-0", 0);
    env.AssertLocked(0, "session-0");
}

Y_UNIT_TEST(ParentLinksInConfigAreIgnoredWhenScalingDisabled) {
    TClassicEnv env;
    env.NextPartitionId = 3;
    env.ParentPartitionIds[1] = {0};
    env.ParentPartitionIds[2] = {0};
    env.ChildPartitionIds[0] = {1, 2};
    env.Publish();
    env.RegisterSession("session-0");
    env.AssertLocked(0);
    env.AssertLocked(1);
    env.AssertLocked(2);
}

Y_UNIT_TEST(PreferredSessionGetsRequestedPartition) {
    TClassicEnv env;
    env.CreateParents(3);
    env.RegisterSession("session-0");
    env.RegisterSession("session-pref", {2});
    env.AssertLocked(1, "session-pref");
    UNIT_ASSERT_VALUES_UNEQUAL(env.SessionOf(0), TString("session-pref"));
    UNIT_ASSERT_VALUES_UNEQUAL(env.SessionOf(2), TString("session-pref"));
}

Y_UNIT_TEST(PreferredAndCommonSessionsCoverAllPartitions) {
    TClassicEnv env;
    env.CreateParents(3);
    env.RegisterSession("session-common");
    env.RegisterSession("session-pref", {1});
    env.AssertLocked(0, "session-pref");
    env.AssertLocked(1, "session-common");
    env.AssertLocked(2, "session-common");
}

Y_UNIT_TEST(TwoPreferredSessionsTakeDisjointPartitions) {
    TClassicEnv env;
    env.CreateParents(3);
    env.RegisterSession("session-common");
    env.RegisterSession("session-a", {1});
    env.RegisterSession("session-b", {3});
    env.AssertLocked(0, "session-a");
    env.AssertLocked(2, "session-b");
    env.AssertLocked(1, "session-common");
}

Y_UNIT_TEST(PreferredConflictDoesNotDoubleLock) {
    TClassicEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-a", {1});
    env.RegisterSession("session-b", {1});
    env.AssertLocked(0);
    const TString holder = env.SessionOf(0);
    UNIT_ASSERT_C(holder == "session-a" || holder == "session-b", holder);
    env.AssertEvenDistribution(1, 1);
}

Y_UNIT_TEST(PreferredUnknownGroupIsRejected) {
    TClassicEnv env;
    env.CreateParents(2);
    env.RegisterSession("session-0");
    env.RegisterSession("session-bad", {5}, /*pump=*/false);
    auto error = env.tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvError>(TDuration::Seconds(10));
    UNIT_ASSERT(error);
    env.Pump();
    env.AssertEvenDistribution(2, 1);
}

Y_UNIT_TEST(PreferredSessionTakesMatchingPartitionAfterItAppears) {
    TClassicEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.AssertLocked(0, "session-0");
    const ui32 added = env.AddRoot();
    UNIT_ASSERT_VALUES_EQUAL(added, 1u);
    env.RegisterSession("session-pref", {2});
    env.AssertLocked(1, "session-pref");
    env.AssertLocked(0, "session-0");
}

Y_UNIT_TEST(NewPartitionGoesToLeastLoadedSession) {
    TClassicEnv env;
    env.CreateParents(2);
    env.RegisterSession("session-0");
    env.RegisterSession("session-1");
    env.AssertEvenDistribution(2, 2);
    const ui32 added = env.AddRoot();
    env.AssertLocked(added);
    env.AssertEvenDistribution(3, 2);
}

Y_UNIT_TEST(NewPartitionWhileNoSessions) {
    TClassicEnv env;
    env.CreateParents(1);
    const ui32 added = env.AddRoot();
    env.RegisterSession("session-0");
    env.AssertLocked(0);
    env.AssertLocked(added);
}

Y_UNIT_TEST(NewPartitionDuringRebalanceRelease) {
    TClassicEnv env;
    env.CreateParents(2);
    env.RegisterSession("session-0");
    env.AssertLocked(0);
    env.AssertLocked(1);

    env.RegisterSession("session-1", {}, /*pump=*/false);
    auto pending = env.WaitRelease();
    UNIT_ASSERT_C(pending, "second session must trigger a release");
    const ui32 added = env.AddRoot();
    env.Pump();
    env.AssertLocked(added);
    env.AssertEvenDistribution(3, 2);
}

Y_UNIT_TEST(StaleFinishAfterPipeBreakDoesNotAffectLocks) {
    TClassicEnv env;
    env.CreateParents(2);
    env.RegisterSession("session-0");
    const auto pipe = env.Pipes["session-0"];
    env.CloseSession("session-0");
    env.InjectFinish(pipe, 0);
    env.RegisterSession("session-new");
    env.AssertEvenDistribution(2, 1);
}

Y_UNIT_TEST(StaleReleaseAfterPipeBreakIsIgnored) {
    TClassicEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.AssertLocked(0, "session-0");
    const auto pipe = env.Pipes["session-0"];
    env.CloseSession("session-0");
    env.AckRelease(pipe, 0, "session-0");
    DispatchFor(env.tc, TDuration::MilliSeconds(50));
    env.RegisterSession("session-new");
    env.AssertLocked(0, "session-new");
}

Y_UNIT_TEST(StaleFinishFromAliveOtherSessionDoesNotStealPartition) {
    TClassicEnv env;
    env.CreateParents(2);
    env.RegisterSession("session-0");
    env.RegisterSession("session-1");
    env.AssertEvenDistribution(2, 2);
    const TString holder0 = env.SessionOf(0);
    const TString other = holder0 == "session-0" ? "session-1" : "session-0";
    env.Finish(other, 0);
    UNIT_ASSERT_VALUES_EQUAL(holder0, env.SessionOf(0));
    env.AssertLocked(1);
}

Y_UNIT_TEST(StaleStartReadingAfterPipeBreakIsIgnored) {
    TClassicEnv env;
    env.CreateParents(2);
    env.RegisterSession("session-0");
    env.AssertEvenDistribution(2, 1);
    const auto pipe = env.Pipes["session-0"];
    env.CloseSession("session-0");
    env.InjectStartReading(pipe, 0);
    env.RegisterSession("session-new");
    env.AssertEvenDistribution(2, 1);
    env.AssertLocked(0, "session-new");
    env.AssertLocked(1, "session-new");
}

Y_UNIT_TEST(StaleCommitAfterPipeBreakDoesNotStealPartition) {
    TClassicEnv env;
    env.CreateParents(2);
    env.RegisterSession("session-0");
    env.RegisterSession("session-1");
    env.AssertEvenDistribution(2, 2);
    env.CloseSession("session-1");
    env.InjectCommit(0);
    env.AssertLocked(0);
    env.AssertLocked(1);
    env.AssertEvenDistribution(2, 1);
}

Y_UNIT_TEST(LastSessionGoneThenNewSessionGetsAllRootsWithoutFinishOrCommit) {
    TClassicEnv env;
    env.CreateParents(3);
    env.RegisterSession("session-0");
    env.Finish("session-0", 0);
    env.Commit(1);
    env.CloseSession("session-0");
    env.RegisterSession("session-new");
    env.AssertEvenDistribution(3, 1);
}

Y_UNIT_TEST(FinishOnLeafDoesNotHidePartitionAfterHolderDisconnect) {
    TClassicEnv env;
    env.CreateParents(2);
    env.RegisterSession("session-0");
    env.RegisterSession("session-1");
    env.AssertEvenDistribution(2, 2);
    const TString holder0 = env.SessionOf(0);
    env.Finish(holder0, 0);
    env.CloseSession(holder0);
    env.AssertLocked(0);
    env.AssertLocked(1);
    env.AssertEvenDistribution(2, 1);
}

} // Y_UNIT_TEST_SUITE(TPqrbClassicBalancing)

Y_UNIT_TEST_SUITE(TPqrbStalePipeAndOrder) {

Y_UNIT_TEST(StaleUnlockFromOldPipeAfterNewSessionLockedIsIgnored) {
    TClassicEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.AssertLocked(0, "session-0");
    const auto oldPipe = env.Pipes["session-0"];
    env.CloseSession("session-0");
    env.RegisterSession("session-new");
    env.AssertLocked(0, "session-new");

    env.AckRelease(oldPipe, 0, "session-0");
    DispatchFor(env.tc, TDuration::MilliSeconds(50));
    env.Pump();
    env.AssertLocked(0, "session-new");
}

Y_UNIT_TEST(StaleFinishFromOldPipeAfterNewSessionLockedDoesNotMovePartition) {
    TClassicEnv env;
    env.CreateParents(2);
    env.RegisterSession("session-0");
    env.RegisterSession("session-1");
    env.AssertEvenDistribution(2, 2);
    const TString holder0 = env.SessionOf(0);
    const auto oldPipe = env.Pipes[holder0];
    env.CloseSession(holder0);
    env.AssertLocked(0);
    env.AssertLocked(1);

    env.InjectFinish(oldPipe, 0);
    env.AssertEvenDistribution(2, 1);
    UNIT_ASSERT_VALUES_UNEQUAL(env.SessionOf(0), holder0);
}

Y_UNIT_TEST(StaleStartReadingFromOldPipeAfterNewSessionLockedDoesNotReread) {
    TSplitEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.Split(0);
    env.Finish("session-0", 0);
    env.AssertLocked(1, "session-0");
    const auto oldPipe = env.Pipes["session-0"];
    env.CloseSession("session-0");
    env.RegisterSession("session-new");
    env.Finish("session-new", 0);
    env.AssertLocked(1, "session-new");
    env.AssertLocked(2, "session-new");

    env.InjectStartReading(oldPipe, 0);
    env.AssertLocked(1, "session-new");
    env.AssertLocked(2, "session-new");
}

Y_UNIT_TEST(OldTabletCommitWithLowerGenerationIsIgnoredAfterReread) {
    TSplitEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.Split(0);
    env.Finish("session-0", 0);
    env.Commit(0, /*generation=*/2, /*cookie=*/10);
    env.AssertLocked(1);
    env.AssertLocked(2);

    env.StartReading("session-0", 0);
    env.AssertNotLocked(1);
    env.AssertNotLocked(2);

    env.Commit(0, /*generation=*/1, /*cookie=*/99);
    env.AssertNotLocked(1);
    env.AssertNotLocked(2);

    env.Finish("session-0", 0);
    env.AssertLocked(1);
    env.AssertLocked(2);
}

Y_UNIT_TEST(OldTabletCommitWithLowerCookieIsIgnoredAfterReread) {
    TSplitEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.Split(0);
    env.Finish("session-0", 0);
    env.Commit(0, /*generation=*/3, /*cookie=*/50);
    env.StartReading("session-0", 0);
    env.AssertNotLocked(1);
    env.AssertNotLocked(2);

    env.Commit(0, /*generation=*/3, /*cookie=*/1);
    env.AssertNotLocked(1);
    env.AssertNotLocked(2);

    env.Finish("session-0", 0);
    env.AssertLocked(1);
    env.AssertLocked(2);
}

Y_UNIT_TEST(NewerTabletCommitAfterOlderCommitStillLocksChildren) {
    TSplitEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.Split(0);
    env.Commit(0, /*generation=*/1, /*cookie=*/1);
    env.AssertLocked(1);
    env.AssertLocked(2);
    env.Commit(0, /*generation=*/4, /*cookie=*/2);
    env.AssertLocked(1);
    env.AssertLocked(2);
}

Y_UNIT_TEST(MembershipStartReadingAfterMigrateDoesNotRereadParent) {
    TSplitEnv env;
    env.CreateParents(2);
    env.RegisterSession("session-0");
    env.RegisterSession("session-hold");
    env.Split(0);
    const TString reader = env.SessionOf(0);
    env.Finish(reader, 0);
    env.AssertLocked(2);
    env.AssertLocked(3);
    env.CloseSession(reader);
    env.AssertSameSession({0, 2, 3});

    const TString holder = env.SessionOf(0);
    UNIT_ASSERT_C(!holder.empty(), "parent must migrate to the remaining session");
    env.StartReading(holder, 0);
    env.AssertLocked(2);
    env.AssertLocked(3);
    env.AssertSameSession({0, 2, 3});
}

Y_UNIT_TEST(StartReadingBeforeFirstFinishDoesNotSkipFinish) {
    TSplitEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.Split(0);
    env.StartReading("session-0", 0);
    env.AssertNotLocked(1);
    env.AssertNotLocked(2);
    env.Finish("session-0", 0);
    env.AssertLocked(1, "session-0");
    env.AssertLocked(2, "session-0");
}

Y_UNIT_TEST(FinishThenCommitThenStartReadingThenFinishRelocksChildren) {
    TSplitEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.Split(0);
    env.Finish("session-0", 0);
    env.Commit(0);
    env.StartReading("session-0", 0);
    env.AssertNotLocked(1);
    env.AssertNotLocked(2);
    env.Finish("session-0", 0);
    env.AssertLocked(1);
    env.AssertLocked(2);
}

Y_UNIT_TEST(CommitThenFinishThenPipeBreakOnLastSessionRequiresNewCommit) {
    TSplitEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.Split(0);
    env.Commit(0, 1, 1, /*pump=*/false);
    env.Finish("session-0", 0, /*scaleAware=*/true, /*fromEnd=*/true, /*pump=*/false);
    env.CloseSession("session-0");
    env.RegisterSession("session-new");
    env.AssertNotLocked(1);
    env.AssertNotLocked(2);
    env.Commit(0);
    env.AssertLocked(1);
    env.AssertLocked(2);
}

Y_UNIT_TEST(FinishInFlightThenLastSessionGoneDropsFinish) {
    TSplitEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.Split(0);
    env.Finish("session-0", 0, /*scaleAware=*/true, /*fromEnd=*/true, /*pump=*/false);
    env.CloseSession("session-0");
    env.RegisterSession("session-new");
    env.AssertLocked(0, "session-new");
    env.AssertNotLocked(1);
    env.AssertNotLocked(2);
    env.Finish("session-new", 0);
    env.AssertLocked(1);
    env.AssertLocked(2);
}

Y_UNIT_TEST(StaleUnlockDuringRebalanceDoesNotDropPartitionFromNewSession) {
    TClassicEnv env;
    env.CreateParents(2);
    env.RegisterSession("session-0");
    env.AssertLocked(0);
    env.AssertLocked(1);
    const auto oldPipe = env.Pipes["session-0"];

    env.RegisterSession("session-1", {}, /*pump=*/false);
    auto pending = env.WaitRelease();
    UNIT_ASSERT_C(pending, "second session must trigger a release");
    env.CloseSession("session-0");
    env.AckRelease(oldPipe, pending->Partition, pending->Session);
    env.Pump();
    env.AssertLocked(0);
    env.AssertLocked(1);
}

} // Y_UNIT_TEST_SUITE(TPqrbStalePipeAndOrder)

} // namespace NKikimr::NPQ



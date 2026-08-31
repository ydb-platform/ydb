#include "pqrb_ut_common.h"

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
    UNIT_ASSERT(!partition.ReadingFinished);
    UNIT_ASSERT(!partition.BalanceToOtherPipe());
}

Y_UNIT_TEST(StopReadingClearsFinishAndKeepsCommit) {
    NBalancing::TPartition partition;

    UNIT_ASSERT(partition.SetFinishedState(/*scaleAwareSDK=*/true, /*startedReadingFromEndOffset=*/false));
    UNIT_ASSERT(partition.SetCommittedState(1, 1));
    UNIT_ASSERT(partition.ReadingFinished);
    UNIT_ASSERT(partition.Commited);
    UNIT_ASSERT(partition.IsInactive());

    UNIT_ASSERT(!partition.StopReading());
    UNIT_ASSERT(!partition.ReadingFinished);
    UNIT_ASSERT(partition.Commited);
    UNIT_ASSERT(partition.IsInactive());
    UNIT_ASSERT(!partition.NeedReleaseChildren());
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

Y_UNIT_TEST_SUITE(TPqrbMergeBalancing) {

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

Y_UNIT_TEST(DisconnectResetsFinishUntilParentsAreFinishedAgain) {
    TScaleEnv env;
    env.CreateParents(2);
    env.RegisterSession("session-0");
    env.Merge(0, 1);
    env.Finish("session-0", 0);
    env.Finish("session-0", 1);
    env.AssertSameSession({0, 1, 2});

    env.RegisterSession("session-1");
    env.AssertSameSession({0, 1, 2});

    const TString owner = env.SessionOf(0);
    UNIT_ASSERT_C(owner == "session-0" || owner == "session-1", owner);
    const TString survivor = owner == "session-0" ? "session-1" : "session-0";

    env.DisconnectSession(owner);
    env.AssertLocked(0, survivor);
    env.AssertLocked(1, survivor);
    env.AssertNotLocked(2);

    env.Finish(survivor, 0);
    env.Finish(survivor, 1);
    env.AssertSameSession({0, 1, 2});
}

Y_UNIT_TEST(SpecialParentJoinsCommonFamilyOnMerge) {
    TScaleEnv env;
    env.CreateParents(2);
    env.RegisterSession("session-common");
    env.RegisterSession("session-pref", {2});
    env.Merge(0, 1);
    env.AssertLocked(0, "session-common");
    env.AssertLocked(1, "session-pref");
    env.Finish("session-pref", 1);
    env.Finish("session-common", 0);
    env.AssertLocked(0, "session-common");
    env.AssertLocked(1, "session-common");
    env.AssertLocked(2, "session-common");
}

Y_UNIT_TEST(SpecialParentJoinsCommonFamilyOnMergeAfterCommonFinish) {
    TScaleEnv env;
    env.CreateParents(2);
    env.RegisterSession("session-common");
    env.RegisterSession("session-pref", {2});
    env.Merge(0, 1);
    env.AssertLocked(0, "session-common");
    env.AssertLocked(1, "session-pref");
    env.Finish("session-common", 0);
    env.Finish("session-pref", 1);
    env.AssertLocked(0, "session-common");
    env.AssertLocked(1, "session-common");
    env.AssertLocked(2, "session-common");
}

Y_UNIT_TEST(MergeFreeFamilyIntoReleasingKeepsPartitionMapping) {
    TScaleEnv env;
    env.CreateParents(2);
    env.RegisterSession("session-common");
    env.RegisterSession("session-pref", {2});
    env.Merge(0, 1);
    env.AssertLocked(0, "session-common");
    env.AssertLocked(1, "session-pref");

    env.Finish("session-common", 0);

    // A preferred session for the common parent forces that family to start
    // releasing. Do not ack the release: the family stays Releasing.
    env.RegisterSession("session-steal", {1}, /*pump=*/false);

    env.Finish("session-pref", 1, /*scaleAware=*/true, /*fromEnd=*/true, /*pump=*/false);

    // Completing the special family's merge-release while the common parent is
    // still Releasing used to Destroy the free family without remapping, then
    // crash in GetReadSessionsInfo: "Use of destroyed hash table".
    env.AckRelease(env.Pipes.at("session-pref"), 1, "session-pref");
    DispatchFor(env.tc);
    env.SessionsInfo();

    env.Pump();
    env.AssertSameSession({0, 1});
    UNIT_ASSERT_C(!env.SessionOf(2).empty(), "merge child must be assigned");
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

} // Y_UNIT_TEST_SUITE(TPqrbMergeBalancing)

Y_UNIT_TEST_SUITE(TPqrbSplitBalancing) {

Y_UNIT_TEST(ChildrenStayUnlockedWhileParentIsActive) {
    TScaleEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.Split(0);
    env.AssertLocked(0, "session-0");
    env.AssertNotLocked(1);
    env.AssertNotLocked(2);
}

Y_UNIT_TEST(ScaleAwareFinishLocksChildrenOnSameSession) {
    TScaleEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.Split(0);
    env.Finish("session-0", 0);
    env.AssertSameSession({0, 1, 2});
}

Y_UNIT_TEST(OldSdkFinishWithoutFromEndLeavesChildrenUnlocked) {
    TScaleEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.Split(0);
    env.Finish("session-0", 0, /*scaleAware=*/false, /*fromEnd=*/false);
    env.AssertLocked(0, "session-0");
    env.AssertNotLocked(1);
    env.AssertNotLocked(2);
}

Y_UNIT_TEST(OldSdkFinishFromEndKeepsChildrenInSeparateFamilies) {
    TScaleEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.Split(0);
    env.Finish("session-0", 0, /*scaleAware=*/false, /*fromEnd=*/true);
    env.AssertLocked(0, "session-0");
    env.AssertLocked(1);
    env.AssertLocked(2);

    env.RegisterSession("session-1");
    env.AssertEvenDistribution(3, 2);
}

Y_UNIT_TEST(RereadLonelyParentWithoutCommitUnlocksChildren) {
    TScaleEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.Split(0);
    env.Finish("session-0", 0, /*scaleAware=*/false, /*fromEnd=*/true);
    env.AssertLocked(0, "session-0");
    env.AssertLocked(1);
    env.AssertLocked(2);

    env.Started("session-0", 0);
    env.AssertLocked(0, "session-0");
    env.AssertNotLocked(1);
    env.AssertNotLocked(2);

    env.Finish("session-0", 0, /*scaleAware=*/false, /*fromEnd=*/true);
    env.AssertLocked(0, "session-0");
    env.AssertLocked(1);
    env.AssertLocked(2);
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

Y_UNIT_TEST(PreferredUnreadableChildIsLockedForPreferredSession) {
    TScaleEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.Split(0);
    env.AssertLocked(0, "session-0");
    env.AssertNotLocked(1);
    env.AssertNotLocked(2);

    env.RegisterSession("session-child", {2});
    env.AssertLocked(0, "session-0");
    env.AssertLocked(1, "session-child");
    env.AssertNotLocked(2);

    env.Finish("session-0", 0);
    env.AssertLocked(0, "session-0");
    env.AssertLocked(1, "session-child");
    env.AssertLocked(2, "session-0");
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

Y_UNIT_TEST(DisconnectResetsFinishUntilParentIsFinishedAgain) {
    TScaleEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.Split(0);
    env.Finish("session-0", 0);
    env.AssertSameSession({0, 1, 2});

    env.RegisterSession("session-1");
    env.AssertSameSession({0, 1, 2});

    const TString owner = env.SessionOf(0);
    UNIT_ASSERT_C(owner == "session-0" || owner == "session-1", owner);
    const TString survivor = owner == "session-0" ? "session-1" : "session-0";

    env.DisconnectSession(owner);
    env.AssertLocked(0, survivor);
    env.AssertNotLocked(1);
    env.AssertNotLocked(2);

    env.Finish(survivor, 0);
    env.AssertSameSession({0, 1, 2});
    env.AssertLocked(0, survivor);
    env.AssertLocked(1, survivor);
    env.AssertLocked(2, survivor);
}

Y_UNIT_TEST(DisconnectKeepsCommitSoChildrenStayReadable) {
    TScaleEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.Split(0);
    env.Finish("session-0", 0);
    env.AssertSameSession({0, 1, 2});
    env.Commit(0);

    env.RegisterSession("session-1");
    env.AssertEvenDistribution(3, 2);

    env.DisconnectSession("session-0");
    env.AssertLocked(0);
    env.AssertLocked(1);
    env.AssertLocked(2);
    UNIT_ASSERT_VALUES_EQUAL(env.SessionOf(0), TString("session-1"));
    UNIT_ASSERT_VALUES_EQUAL(env.SessionOf(1), TString("session-1"));
    UNIT_ASSERT_VALUES_EQUAL(env.SessionOf(2), TString("session-1"));
}

Y_UNIT_TEST(LastSessionDisconnectDropsFinishAndCommit) {
    TScaleEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.Split(0);
    env.Finish("session-0", 0);
    env.Commit(0);
    env.AssertLocked(0, "session-0");
    env.AssertLocked(1);
    env.AssertLocked(2);

    env.DisconnectSession("session-0");
    env.RegisterSession("session-1");
    env.AssertLocked(0, "session-1");
    env.AssertNotLocked(1);
    env.AssertNotLocked(2);

    env.Finish("session-1", 0);
    env.AssertSameSession({0, 1, 2});
}

Y_UNIT_TEST(OldSdkDisconnectResetsFinishOnParentSession) {
    TScaleEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-0");
    env.Split(0);
    env.Finish("session-0", 0, /*scaleAware=*/false, /*fromEnd=*/true);
    env.RegisterSession("session-1");

    const TString parentSession = env.SessionOf(0);
    UNIT_ASSERT_C(!parentSession.empty(), "parent must stay assigned after old-SDK finish");
    env.DisconnectSession(parentSession);

    const TString survivor = parentSession == "session-0" ? "session-1" : "session-0";
    env.AssertLocked(0, survivor);
    env.AssertNotLocked(1);
    env.AssertNotLocked(2);
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

} // Y_UNIT_TEST_SUITE(TPqrbSplitBalancing)

Y_UNIT_TEST_SUITE(TPqrbExplicitPartitionBalancing) {

Y_UNIT_TEST(PreferredParentDoesNotGrowOnSplitFinish) {
    TScaleEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-common");
    env.RegisterSession("session-pref", {1});
    env.Split(0);
    env.AssertLocked(0, "session-pref");
    env.AssertNotLocked(1);
    env.AssertNotLocked(2);

    env.Finish("session-pref", 0);
    env.AssertLocked(0, "session-pref");
    env.AssertLocked(1, "session-common");
    env.AssertLocked(2, "session-common");
    UNIT_ASSERT_VALUES_UNEQUAL(env.SessionOf(1), TString("session-pref"));
    UNIT_ASSERT_VALUES_UNEQUAL(env.SessionOf(2), TString("session-pref"));
}

Y_UNIT_TEST(PreferredSplitChildrenStayLockedWhileParentIsActive) {
    TScaleEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-common");
    env.Split(0);
    env.RegisterSession("session-left", {2});
    env.RegisterSession("session-right", {3});
    env.AssertLocked(0, "session-common");
    env.AssertLocked(1, "session-left");
    env.AssertLocked(2, "session-right");

    env.Finish("session-common", 0);
    env.AssertLocked(0, "session-common");
    env.AssertLocked(1, "session-left");
    env.AssertLocked(2, "session-right");
}

Y_UNIT_TEST(DisconnectPreferredUnreadableChildDoesNotAssignItToCommon) {
    TScaleEnv env;
    env.CreateParents(1);
    env.RegisterSession("session-common");
    env.Split(0);
    env.RegisterSession("session-child", {2});
    env.AssertLocked(0, "session-common");
    env.AssertLocked(1, "session-child");
    env.AssertNotLocked(2);

    env.DisconnectSession("session-child");
    env.AssertLocked(0, "session-common");
    env.AssertNotLocked(1);
    env.AssertNotLocked(2);
}

Y_UNIT_TEST(TwoSpecialMergeParentsDoNotAbsorbEachOther) {
    TScaleEnv env;
    env.CreateParents(2);
    env.RegisterSession("session-left", {1});
    env.RegisterSession("session-right", {2});
    env.Merge(0, 1);
    env.AssertLocked(0, "session-left");
    env.AssertLocked(1, "session-right");
    env.AssertNotLocked(2);

    env.Finish("session-left", 0);
    env.Finish("session-right", 1);
    env.AssertLocked(0, "session-left");
    env.AssertLocked(1, "session-right");
    env.AssertNotLocked(2);

    env.RegisterSession("session-common");
    env.AssertLocked(0, "session-left");
    env.AssertLocked(1, "session-right");
    env.AssertLocked(2, "session-common");
}

} // Y_UNIT_TEST_SUITE(TPqrbExplicitPartitionBalancing)

} // namespace NKikimr::NPQ

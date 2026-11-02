#include "statestorage_impl.h"
#include "tabletid.h"

#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/library/services/services.pb.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace {

// Exercise the real proxy with controlled replica replies and nondelivery.
// Pending replicas stay silent until the test explicitly responds for them.
class TLookupFixture {
public:
    using TOptions = TEvStateStorage::TProxyOptions;
    static constexpr ui64 TabletId = 100;
    static constexpr ui64 Cookie = 42;

    TTestActorRuntimeBase Runtime{1};
    TActorId Edge;
    TActorId Leader;
    TActorId OtherLeader;
    TVector<TActorId> Requests;
    TVector<TActorId> Replicas;
    TVector<ui64> ReplicaCookies;
    THashSet<TActorId> DropChildRepliesFrom;
    bool DropChildPassAway = false;
    ui32 DroppedChildReplyCount = 0;
    ui32 ReplyCount = 0;
    ui32 SignatureUpdateCount = 0;
    NKikimrProto::EReplyStatus Status = NKikimrProto::UNKNOWN;
    TActorId ReplyLeader;
    TEvStateStorage::TSignature Signatures;
    TVector<TEvStateStorage::TEvInfo::TFollowerInfo> Followers;

    explicit TLookupFixture(TOptions::ESigWaitMode mode = TOptions::SigNone, ui32 ringGroupCount = 1,
            const TVector<ERingGroupState>& ringGroupStates = {}, bool waitForAllRingGroups = false) {
        Runtime.Initialize();
        Runtime.GetLogSettings(0)->Append(NKikimrServices::EServiceKikimr_MIN,
            NKikimrServices::EServiceKikimr_MAX,
            [](int component) -> const TString& { return NKikimrServices::EServiceKikimr_Name(component); });
        Runtime.SetScheduledEventFilter([](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>&,
                                           TDuration, TInstant&) { return false; });
        Runtime.SetDispatchTimeout(TDuration::Seconds(5));
        Edge = Runtime.AllocateEdgeActor();
        Leader = Runtime.AllocateEdgeActor();
        OtherLeader = Runtime.AllocateEdgeActor();

        auto info = MakeIntrusive<TStateStorageInfo>();
        info->RingGroups.resize(ringGroupCount);
        for (ui32 ringGroupIndex = 0; ringGroupIndex < info->RingGroups.size(); ++ringGroupIndex) {
            auto& group = info->RingGroups[ringGroupIndex];
            group.State = ringGroupStates.empty() ? ERingGroupState::PRIMARY : ringGroupStates.at(ringGroupIndex);
            group.NToSelect = 5;
            group.Rings.resize(5);
            for (auto& ring : group.Rings) {
                Replicas.push_back(Runtime.AllocateEdgeActor());
                ring.Replicas.push_back(Replicas.back());
            }
        }
        auto proxy = Runtime.Register(CreateStateStorageProxy(info, nullptr, nullptr));
        Runtime.SetObserverFunc([this](TAutoPtr<IEventHandle>& event) {
            if (event->Recipient != Edge) {
                if (event->GetTypeRewrite() == TEvStateStorage::TEvInfo::EventType
                        && DropChildRepliesFrom.contains(event->Sender)) {
                    ++DroppedChildReplyCount;
                    return TTestActorRuntimeBase::EEventAction::DROP;
                }
                if (event->GetTypeRewrite() == TEvStateStorage::TEvRingGroupPassAway::EventType
                        && DropChildPassAway) {
                    return TTestActorRuntimeBase::EEventAction::DROP;
                }
            }
            if (event->Recipient == Edge) {
                if (event->GetTypeRewrite() == TEvStateStorage::TEvInfo::EventType) {
                    auto* reply = event->Get<TEvStateStorage::TEvInfo>();
                    UNIT_ASSERT_VALUES_EQUAL(reply->TabletID, TabletId);
                    UNIT_ASSERT_VALUES_EQUAL(reply->Cookie, Cookie);
                    ++ReplyCount;
                    Status = reply->Status;
                    ReplyLeader = reply->CurrentLeader;
                    Signatures = reply->Signature;
                    Followers = reply->Followers;
                    return TTestActorRuntimeBase::EEventAction::DROP;
                } else if (event->GetTypeRewrite() == TEvStateStorage::TEvUpdateSignature::EventType) {
                    auto* reply = event->Get<TEvStateStorage::TEvUpdateSignature>();
                    for (const auto& replica : Replicas) {
                        if (Signatures.HasReplicaSignature(replica)) {
                            UNIT_ASSERT(reply->Signature.HasReplicaSignature(replica));
                            UNIT_ASSERT_VALUES_EQUAL(reply->Signature.GetReplicaSignature(replica),
                                Signatures.GetReplicaSignature(replica));
                        }
                    }
                    ++SignatureUpdateCount;
                    Signatures = reply->Signature;
                    return TTestActorRuntimeBase::EEventAction::DROP;
                }
            }
            return TTestActorRuntimeBase::EEventAction::PROCESS;
        });
        TOptions options(mode);
        options.WaitForAllRingGroups = waitForAllRingGroups;
        Runtime.SendAsync(new IEventHandle(proxy, Edge, new TEvStateStorage::TEvLookup(TabletId, Cookie, options)));
        for (const auto& replica : Replicas) {
            auto request = Runtime.GrabEdgeEvent<TEvStateStorage::TEvReplicaLookup>(replica);
            UNIT_ASSERT(request);
            Requests.push_back(request->Sender);
            ReplicaCookies.push_back(request->Get()->Record.GetCookie());
        }
    }

    void Reply(ui32 index, THolder<TEvStateStorage::TEvReplicaInfo> reply) {
        reply->Record.SetCookie(ReplicaCookies[index]);
        reply->Record.SetSignature(index + 1);
        Runtime.SendAsync(new IEventHandle(Requests[index], Replicas[index], reply.Release()));
    }

    void Empty(ui32 index, NKikimrProto::EReplyStatus status = NKikimrProto::ERROR) {
        // Legacy replicas use ERROR for an absent tablet, newer ones may use NODATA.
        Reply(index, MakeHolder<TEvStateStorage::TEvReplicaInfo>(TabletId, status, 0, 0));
    }

    void KnownLeader(ui32 index) {
        KnownLeader(index, Leader, 100);
    }

    void KnownLeader(ui32 index, const TActorId& leader, ui32 generation) {
        Reply(index, MakeHolder<TEvStateStorage::TEvReplicaInfo>(TabletId, leader, TActorId(), generation, 0, false, 0, 0, 0));
    }

    void KnownLeaderWithFollower(ui32 index, const TActorId& follower, const TActorId& followerTablet, ui32 followerId) {
        auto reply = MakeHolder<TEvStateStorage::TEvReplicaInfo>(TabletId, Leader, TActorId(), 100, 0, false, 0, 0, 0);
        auto* info = reply->Record.AddFollowerInfo();
        ActorIdToProto(follower, info->MutableFollower());
        ActorIdToProto(followerTablet, info->MutableFollowerTablet());
        info->SetFollowerId(followerId);
        Reply(index, std::move(reply));
    }

    void Fail(ui32 index) {
        Runtime.SendAsync(new IEventHandle(Requests[index], Replicas[index], new TEvents::TEvUndelivered(
            TEvStateStorage::TEvReplicaLookup::EventType, TEvents::TEvUndelivered::Disconnected),
            0, ReplicaCookies[index]));
    }

    void Elapse(TDuration duration = TDuration::MilliSeconds(1)) {
        Runtime.Schedule(new IEventHandle(Edge, Edge, new TEvents::TEvWakeup()), duration);
        UNIT_ASSERT(Runtime.GrabEdgeEvent<TEvents::TEvWakeup>(Edge, duration + TDuration::Seconds(1)));
    }

    void ExpectPending() {
        Elapse();
        UNIT_ASSERT_VALUES_EQUAL(ReplyCount, 0);
    }

    void ExpectReply(NKikimrProto::EReplyStatus status) {
        Elapse();
        UNIT_ASSERT_VALUES_EQUAL(ReplyCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(Status, status);
    }
};

class TLockFixture {
public:
    static constexpr ui64 TabletId = 100;
    static constexpr ui64 Cookie = 42;
    static constexpr ui32 Generation = 100;

    TTestActorRuntimeBase Runtime{1};
    TActorId Edge;
    TActorId Leader;
    TActorId OtherLeader;
    TVector<TActorId> Requests;
    TVector<TActorId> Replicas;
    TVector<ui64> ReplicaCookies;
    THashSet<TActorId> DropChildRepliesFrom;
    bool DropChildPassAway = false;
    ui32 DroppedChildReplyCount = 0;
    ui32 ReplyCount = 0;
    NKikimrProto::EReplyStatus Status = NKikimrProto::UNKNOWN;

    explicit TLockFixture(TEvStateStorage::TProxyOptions::ESigWaitMode mode = TEvStateStorage::TProxyOptions::SigNone) {
        Runtime.Initialize();
        Runtime.GetLogSettings(0)->Append(NKikimrServices::EServiceKikimr_MIN,
            NKikimrServices::EServiceKikimr_MAX,
            [](int component) -> const TString& { return NKikimrServices::EServiceKikimr_Name(component); });
        Runtime.SetScheduledEventFilter([](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>&,
                                           TDuration, TInstant&) { return false; });
        Runtime.SetDispatchTimeout(TDuration::Seconds(5));
        Edge = Runtime.AllocateEdgeActor();
        Leader = Runtime.AllocateEdgeActor();
        OtherLeader = Runtime.AllocateEdgeActor();

        auto info = MakeIntrusive<TStateStorageInfo>();
        info->RingGroups.resize(2);
        info->RingGroups[0].State = ERingGroupState::PRIMARY;
        info->RingGroups[1].State = ERingGroupState::SYNCHRONIZED;
        for (auto& group : info->RingGroups) {
            group.NToSelect = 3;
            group.Rings.resize(3);
            for (auto& ring : group.Rings) {
                Replicas.push_back(Runtime.AllocateEdgeActor());
                ring.Replicas.push_back(Replicas.back());
            }
        }

        auto proxy = Runtime.Register(CreateStateStorageProxy(info, nullptr, nullptr));
        Runtime.SetObserverFunc([this](TAutoPtr<IEventHandle>& event) {
            if (event->Recipient != Edge) {
                if (event->GetTypeRewrite() == TEvStateStorage::TEvInfo::EventType
                        && DropChildRepliesFrom.contains(event->Sender)) {
                    ++DroppedChildReplyCount;
                    return TTestActorRuntimeBase::EEventAction::DROP;
                }
                if (event->GetTypeRewrite() == TEvStateStorage::TEvRingGroupPassAway::EventType
                        && DropChildPassAway) {
                    return TTestActorRuntimeBase::EEventAction::DROP;
                }
            }
            if (event->Recipient == Edge && event->GetTypeRewrite() == TEvStateStorage::TEvInfo::EventType) {
                auto* reply = event->Get<TEvStateStorage::TEvInfo>();
                ++ReplyCount;
                Status = reply->Status;
                return TTestActorRuntimeBase::EEventAction::DROP;
            }
            return TTestActorRuntimeBase::EEventAction::PROCESS;
        });

        Runtime.SendAsync(new IEventHandle(proxy, Edge,
            new TEvStateStorage::TEvLock(TabletId, Cookie, Leader, Generation, {}, mode)));
        for (const auto& replica : Replicas) {
            auto request = Runtime.GrabEdgeEvent<TEvStateStorage::TEvReplicaLock>(replica);
            UNIT_ASSERT(request);
            Requests.push_back(request->Sender);
            ReplicaCookies.push_back(request->Get()->Record.GetCookie());
        }
    }

    void Reply(ui32 index, const TActorId& leader, ui32 generation) {
        auto reply = MakeHolder<TEvStateStorage::TEvReplicaInfo>(
            TabletId, leader, TActorId(), generation, 0, true, 0, 0, 0);
        reply->Record.SetCookie(ReplicaCookies[index]);
        reply->Record.SetSignature(index + 1);
        Runtime.SendAsync(new IEventHandle(Requests[index], Replicas[index], reply.Release()));
    }

    void Elapse(TDuration duration = TDuration::MilliSeconds(1)) {
        Runtime.Schedule(new IEventHandle(Edge, Edge, new TEvents::TEvWakeup()), duration);
        UNIT_ASSERT(Runtime.GrabEdgeEvent<TEvents::TEvWakeup>(Edge, duration + TDuration::Seconds(1)));
    }
};

void CheckEmptyQuorum(TEvStateStorage::TProxyOptions::ESigWaitMode mode) {
    TLookupFixture f(mode);
    f.Empty(0);
    f.Empty(1, NKikimrProto::NODATA);
    f.ExpectPending();
    f.Empty(2);
    f.ExpectReply(NKikimrProto::NODATA);
    UNIT_ASSERT(!f.ReplyLeader);
    UNIT_ASSERT_VALUES_EQUAL(f.Signatures.Size(), 3);
    UNIT_ASSERT_VALUES_EQUAL(f.Signatures.GetReplicaSignature(f.Replicas[3]), 0);
    UNIT_ASSERT_VALUES_EQUAL(f.Signatures.GetReplicaSignature(f.Replicas[4]), 0);
}

void CheckEarlyLookup(TEvStateStorage::TProxyOptions::ESigWaitMode mode, NKikimrProto::EReplyStatus status) {
    TLookupFixture f(mode, 2, {ERingGroupState::PRIMARY, ERingGroupState::SYNCHRONIZED});
    if (status == NKikimrProto::OK) {
        f.KnownLeader(5);
        f.KnownLeader(6);
        f.ExpectPending();
        f.KnownLeader(7);
    } else {
        f.Empty(5);
        f.Empty(6);
        f.KnownLeader(7);
        f.ExpectPending();
        f.Empty(8);
    }
    f.ExpectReply(status);
    UNIT_ASSERT_VALUES_EQUAL(f.ReplyLeader, f.Leader);
    UNIT_ASSERT_VALUES_EQUAL(f.SignatureUpdateCount, 0);

    f.KnownLeader(0);
    f.KnownLeader(1);
    f.KnownLeader(2);
    f.Elapse();
    UNIT_ASSERT_VALUES_EQUAL(f.ReplyCount, 1);
    if (mode == TEvStateStorage::TProxyOptions::SigNone) {
        UNIT_ASSERT_VALUES_EQUAL(f.SignatureUpdateCount, 0);
        return;
    }

    UNIT_ASSERT(f.SignatureUpdateCount > 0);
    for (ui32 i = 0; i < 3; ++i) {
        UNIT_ASSERT_VALUES_EQUAL(f.Signatures.GetReplicaSignature(f.Replicas[i]), i + 1);
    }
    auto updateCount = f.SignatureUpdateCount;
    f.KnownLeader(3);
    f.KnownLeader(4);
    f.Elapse();
    UNIT_ASSERT(f.SignatureUpdateCount > updateCount);
    for (ui32 i = 0; i < 5; ++i) {
        UNIT_ASSERT_VALUES_EQUAL(f.Signatures.GetReplicaSignature(f.Replicas[i]), i + 1);
    }

    updateCount = f.SignatureUpdateCount;
    if (status == NKikimrProto::OK) {
        f.KnownLeader(8);
    }
    f.KnownLeader(9);
    f.Elapse();
    UNIT_ASSERT(f.SignatureUpdateCount > updateCount);
    UNIT_ASSERT_VALUES_EQUAL(f.ReplyCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(f.Signatures.Size(), 10);
    for (ui32 i = 0; i < 10; ++i) {
        UNIT_ASSERT_VALUES_EQUAL(f.Signatures.GetReplicaSignature(f.Replicas[i]), i + 1);
    }
}

void CheckSuccessfulLockWaits(TEvStateStorage::TProxyOptions::ESigWaitMode mode) {
    TLockFixture f(mode);
    f.Reply(3, f.Leader, TLockFixture::Generation);
    f.Reply(4, f.Leader, TLockFixture::Generation);
    f.Elapse();
    UNIT_ASSERT_VALUES_EQUAL(f.ReplyCount, 0);

    f.Reply(0, f.Leader, TLockFixture::Generation);
    f.Reply(1, f.Leader, TLockFixture::Generation);
    f.Elapse();
    UNIT_ASSERT_VALUES_EQUAL(f.ReplyCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(f.Status, NKikimrProto::OK);
}

void CheckIncompleteLockShutdown(bool safetyTimeout, bool partialSuccess, bool race = false) {
    TLockFixture f;
    // Suppress child exit notifications to exercise the parent's safety timer.
    f.DropChildPassAway = safetyTimeout;
    f.DropChildRepliesFrom.insert(f.Requests[3]);
    if (partialSuccess) {
        const auto& leader = race ? f.OtherLeader : f.Leader;
        const ui32 generation = TLockFixture::Generation + (race ? 1 : 0);
        f.Reply(0, leader, generation);
        f.Reply(1, leader, generation);
        f.Elapse();
        UNIT_ASSERT_VALUES_EQUAL(f.ReplyCount, 0);
    } else {
        f.DropChildRepliesFrom.insert(f.Requests[0]);
    }

    f.Elapse(TDuration::Seconds(30));
    f.Elapse();
    UNIT_ASSERT_VALUES_EQUAL(f.DroppedChildReplyCount, partialSuccess ? 1 : 2);
    if (safetyTimeout) {
        UNIT_ASSERT_VALUES_EQUAL(f.ReplyCount, 0);
        f.Elapse(TDuration::Seconds(30));
        f.Elapse();
    }
    UNIT_ASSERT_VALUES_EQUAL(f.ReplyCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(f.Status, race ? NKikimrProto::RACE : NKikimrProto::TIMEOUT);
}

} // namespace

Y_UNIT_TEST_SUITE(TStateStorageProxyLookup) {
    Y_UNIT_TEST(NodataQuorumSigNone) {
        CheckEmptyQuorum(TEvStateStorage::TProxyOptions::SigNone);
    }

    Y_UNIT_TEST(NodataQuorumSigAsync) {
        CheckEmptyQuorum(TEvStateStorage::TProxyOptions::SigAsync);
    }

    Y_UNIT_TEST(DeliveryFailuresDoNotCountAsEmptyReplies) {
        TLookupFixture f;
        f.Empty(0);
        f.Fail(1);
        f.Fail(2);
        f.ExpectPending();
        f.Empty(3);
        f.ExpectPending();
        f.Empty(4);
        f.ExpectReply(NKikimrProto::NODATA);
    }

    Y_UNIT_TEST(AllRepliesWithoutNegativeQuorumReturnError) {
        TLookupFixture f;
        f.KnownLeader(0);
        f.Empty(1);
        f.Empty(2);
        f.Fail(3);
        f.ExpectPending();
        f.Fail(4);
        f.ExpectReply(NKikimrProto::ERROR);
        UNIT_ASSERT_VALUES_EQUAL(f.ReplyLeader, f.Leader);
    }

    Y_UNIT_TEST(FollowersSurviveErrorsWithoutQuorumInEveryRingGroup) {
        TLookupFixture f(TEvStateStorage::TProxyOptions::SigNone, 2,
            {ERingGroupState::PRIMARY, ERingGroupState::SYNCHRONIZED});
        const TVector<TActorId> followers = {f.Runtime.AllocateEdgeActor(), f.Runtime.AllocateEdgeActor()};
        const TVector<TActorId> followerTablets = {f.Runtime.AllocateEdgeActor(), f.Runtime.AllocateEdgeActor()};
        for (ui32 group = 0; group < 2; ++group) {
            const ui32 first = group * 5;
            f.KnownLeaderWithFollower(first, followers[group], followerTablets[group], group + 1);
            f.Empty(first + 1);
            f.Empty(first + 2);
            f.Fail(first + 3);
            f.ExpectPending();
            f.Fail(first + 4);
            if (group == 0) {
                f.ExpectPending();
            }
        }
        f.ExpectReply(NKikimrProto::ERROR);
        UNIT_ASSERT_VALUES_EQUAL(f.Followers.size(), 2);
        for (const auto& info : f.Followers) {
            UNIT_ASSERT(info.FollowerId.Defined());
            const ui32 group = info.FollowerId.Get() - 1;
            UNIT_ASSERT(group < followers.size());
            UNIT_ASSERT_VALUES_EQUAL(info.Follower, followers[group]);
            UNIT_ASSERT_VALUES_EQUAL(info.FollowerTablet, followerTablets[group]);
        }
    }

    Y_UNIT_TEST(TimeoutWithoutNegativeQuorum) {
        TLookupFixture f;
        f.Empty(0);
        f.Fail(1);
        f.Fail(2);
        f.ExpectPending();
        f.Elapse(TDuration::Seconds(30));
        f.ExpectReply(NKikimrProto::TIMEOUT);
    }

    Y_UNIT_TEST(SigSyncStillWaitsForAllResults) {
        TLookupFixture f(TEvStateStorage::TProxyOptions::SigSync);
        f.Empty(0);
        f.Empty(1);
        f.Empty(2);
        f.ExpectPending();
        f.Empty(3);
        f.ExpectPending();
        f.Fail(4);
        f.ExpectReply(NKikimrProto::NODATA);
        UNIT_ASSERT_VALUES_EQUAL(f.Signatures.GetReplicaSignature(f.Replicas[4]), 0);
    }

    Y_UNIT_TEST(SigAsyncCollectsLateSignatures) {
        TLookupFixture f(TEvStateStorage::TProxyOptions::SigAsync);
        f.Empty(0);
        f.Empty(1);
        f.Empty(2);
        f.ExpectReply(NKikimrProto::NODATA);
        f.KnownLeader(3);
        f.Fail(4);
        f.Elapse();
        UNIT_ASSERT_VALUES_EQUAL(f.ReplyCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(f.SignatureUpdateCount, 1);
        for (ui32 i = 0; i < 4; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(f.Signatures.GetReplicaSignature(f.Replicas[i]), i + 1);
        }
        UNIT_ASSERT_VALUES_EQUAL(f.Signatures.GetReplicaSignature(f.Replicas[4]), 0);
    }

    Y_UNIT_TEST(SigAsyncCountsRepeatedReplicaFailureOnce) {
        TLookupFixture f(TEvStateStorage::TProxyOptions::SigAsync);
        f.Empty(0);
        f.Empty(1);
        f.Empty(2);
        f.ExpectReply(NKikimrProto::NODATA);

        f.Fail(3);
        f.Fail(3);
        f.Elapse();
        UNIT_ASSERT_VALUES_EQUAL(f.SignatureUpdateCount, 0);

        f.KnownLeader(4);
        f.Elapse();
        UNIT_ASSERT_VALUES_EQUAL(f.SignatureUpdateCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(f.Signatures.GetReplicaSignature(f.Replicas[4]), 5);
    }

    Y_UNIT_TEST(EachRingGroupRequiresItsOwnNegativeQuorum) {
        TLookupFixture f(TEvStateStorage::TProxyOptions::SigNone, 2);
        f.Empty(0);
        f.Empty(1);
        f.Empty(5);
        f.ExpectPending();
        f.Empty(2);
        f.ExpectPending();
        f.Empty(6);
        f.ExpectPending();
        f.Empty(7);
        f.ExpectReply(NKikimrProto::NODATA);
        UNIT_ASSERT_VALUES_EQUAL(f.Signatures.Size(), 6);
    }

    Y_UNIT_TEST(EmptyPrimaryWaitsForSynchronizedState) {
        TLookupFixture f(TEvStateStorage::TProxyOptions::SigNone, 2,
            {ERingGroupState::PRIMARY, ERingGroupState::SYNCHRONIZED});
        f.Empty(0);
        f.Empty(1);
        f.Empty(2);
        f.ExpectPending();
        f.KnownLeader(5);
        f.KnownLeader(6);
        f.KnownLeader(7);
        f.ExpectReply(NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(f.ReplyLeader, f.Leader);
    }

    Y_UNIT_TEST(SigSyncWaitsForEveryRingGroupBeforeReturningState) {
        TLookupFixture f(TEvStateStorage::TProxyOptions::SigSync, 2,
            {ERingGroupState::PRIMARY, ERingGroupState::SYNCHRONIZED});
        f.KnownLeader(5);
        f.KnownLeader(6);
        f.KnownLeader(7);
        f.KnownLeader(8);
        f.KnownLeader(9);
        f.ExpectPending();

        f.Empty(0);
        f.Empty(1);
        f.Empty(2);
        f.Empty(3);
        f.Empty(4);
        f.ExpectReply(NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(f.ReplyLeader, f.Leader);
        UNIT_ASSERT_VALUES_EQUAL(f.SignatureUpdateCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(f.Signatures.Size(), 10);
    }

    Y_UNIT_TEST(SigSyncWaitsForEveryRingGroupBeforeReturningRace) {
        TLookupFixture f(TEvStateStorage::TProxyOptions::SigSync, 2,
            {ERingGroupState::PRIMARY, ERingGroupState::SYNCHRONIZED});
        f.Empty(5);
        f.Empty(6);
        f.KnownLeader(7);
        f.Empty(8);
        f.Empty(9);
        f.ExpectPending();

        for (ui32 i = 0; i < 5; ++i) {
            f.Empty(i);
        }
        f.ExpectReply(NKikimrProto::RACE);
        UNIT_ASSERT_VALUES_EQUAL(f.ReplyLeader, f.Leader);
        UNIT_ASSERT_VALUES_EQUAL(f.Signatures.Size(), 10);
        UNIT_ASSERT_VALUES_EQUAL(f.SignatureUpdateCount, 0);
    }

    Y_UNIT_TEST(SigNoneReturnsOkWithoutWaitingForOtherRingGroups) {
        CheckEarlyLookup(TEvStateStorage::TProxyOptions::SigNone, NKikimrProto::OK);
    }

    Y_UNIT_TEST(SigNoneReturnsRaceWithoutWaitingForOtherRingGroups) {
        CheckEarlyLookup(TEvStateStorage::TProxyOptions::SigNone, NKikimrProto::RACE);
    }

    Y_UNIT_TEST(SigAsyncReturnsOkWithoutWaitingForOtherRingGroups) {
        CheckEarlyLookup(TEvStateStorage::TProxyOptions::SigAsync, NKikimrProto::OK);
    }

    Y_UNIT_TEST(SigAsyncReturnsRaceWithoutWaitingForOtherRingGroups) {
        CheckEarlyLookup(TEvStateStorage::TProxyOptions::SigAsync, NKikimrProto::RACE);
    }

    Y_UNIT_TEST(SigAsyncPublishesLastRingGroupSignaturesWithoutFurtherUpdates) {
        TLookupFixture f(TEvStateStorage::TProxyOptions::SigAsync, 2,
            {ERingGroupState::PRIMARY, ERingGroupState::SYNCHRONIZED});
        f.KnownLeader(0);
        f.KnownLeader(1);
        f.KnownLeader(2);
        f.ExpectReply(NKikimrProto::OK);
        f.KnownLeader(3);
        f.KnownLeader(4);
        f.Elapse();
        UNIT_ASSERT_VALUES_EQUAL(f.Signatures.Size(), 5);
        const ui32 updateCount = f.SignatureUpdateCount;

        f.Empty(5);
        f.Empty(6);
        f.Fail(7);
        f.Fail(8);
        f.Fail(9);
        f.Elapse();
        UNIT_ASSERT_VALUES_EQUAL(f.ReplyCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(f.SignatureUpdateCount, updateCount + 1);
        UNIT_ASSERT_VALUES_EQUAL(f.Signatures.Size(), 7);
        for (ui32 i = 0; i < 7; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(f.Signatures.GetReplicaSignature(f.Replicas[i]), i + 1);
        }
        for (ui32 i = 7; i < 10; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(f.Signatures.GetReplicaSignature(f.Replicas[i]), 0);
        }
    }

    Y_UNIT_TEST(SigAsyncCanWaitForQuorumSignaturesInEveryRingGroup) {
        TLookupFixture f(TEvStateStorage::TProxyOptions::SigAsync, 2,
            {ERingGroupState::PRIMARY, ERingGroupState::SYNCHRONIZED}, true);
        f.KnownLeader(5);
        f.KnownLeader(6);
        f.KnownLeader(7);
        f.ExpectPending();

        f.KnownLeader(0);
        f.KnownLeader(1);
        f.ExpectPending();
        f.KnownLeader(2);
        f.ExpectReply(NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(f.Signatures.Size(), 6);
        UNIT_ASSERT_VALUES_EQUAL(f.SignatureUpdateCount, 0);
        for (ui32 i : {0u, 1u, 2u, 5u, 6u, 7u}) {
            UNIT_ASSERT_VALUES_EQUAL(f.Signatures.GetReplicaSignature(f.Replicas[i]), i + 1);
        }

        f.KnownLeader(3);
        f.KnownLeader(4);
        f.KnownLeader(8);
        f.KnownLeader(9);
        f.Elapse();
        UNIT_ASSERT_VALUES_EQUAL(f.ReplyCount, 1);
        UNIT_ASSERT(f.SignatureUpdateCount > 0);
        UNIT_ASSERT_VALUES_EQUAL(f.Signatures.Size(), 10);
        for (ui32 i = 0; i < 10; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(f.Signatures.GetReplicaSignature(f.Replicas[i]), i + 1);
        }
    }

    Y_UNIT_TEST(DifferentStateIsDiagnosticOnly) {
        TLookupFixture f(TEvStateStorage::TProxyOptions::SigNone, 2,
            {ERingGroupState::PRIMARY, ERingGroupState::SYNCHRONIZED});
        f.KnownLeader(5);
        f.KnownLeader(6);
        f.KnownLeader(7);
        f.ExpectReply(NKikimrProto::OK);

        f.KnownLeader(0, f.OtherLeader, 101);
        f.KnownLeader(1, f.OtherLeader, 101);
        f.KnownLeader(2, f.OtherLeader, 101);
        f.ExpectReply(NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(f.ReplyCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(f.ReplyLeader, f.Leader);
    }

    Y_UNIT_TEST(DuplicateEmptyReplyDoesNotCountTwice) {
        TLookupFixture f;
        f.Empty(0);
        f.Empty(0);
        f.Empty(1);
        f.ExpectPending();
        f.Empty(2);
        f.ExpectReply(NKikimrProto::NODATA);
    }

    Y_UNIT_TEST(PositiveQuorumStillReturnsLeader) {
        TLookupFixture f;
        f.KnownLeader(0);
        f.KnownLeader(1);
        f.ExpectPending();
        f.KnownLeader(2);
        f.ExpectReply(NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(f.ReplyLeader, f.Leader);
    }

    Y_UNIT_TEST(NewLeaderStillCausesRace) {
        TLookupFixture f;
        f.Empty(0);
        f.Empty(1);
        f.KnownLeader(2);
        f.ExpectPending();
        f.Empty(3);
        f.ExpectReply(NKikimrProto::RACE);
        UNIT_ASSERT_VALUES_EQUAL(f.ReplyLeader, f.Leader);
    }

    Y_UNIT_TEST(ShutdownKeepsKnownStateWhenAnotherRingGroupReplyIsLost) {
        for (bool safetyTimeout : {false, true}) {
            TLookupFixture f(TEvStateStorage::TProxyOptions::SigSync, 3);
            f.DropChildPassAway = safetyTimeout;
            f.DropChildRepliesFrom.insert(f.Requests[10]);
            for (ui32 i = 0; i < 5; ++i) {
                f.KnownLeader(i);
                f.Fail(i + 5);
            }
            f.ExpectPending();
            for (ui32 i = 10; i < 15; ++i) {
                f.Fail(i);
            }
            if (safetyTimeout) {
                f.ExpectPending();
                f.Elapse(TDuration::Seconds(60));
            }
            f.ExpectReply(NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(f.DroppedChildReplyCount, 1);
            UNIT_ASSERT_VALUES_EQUAL(f.ReplyLeader, f.Leader);
        }
    }
}

Y_UNIT_TEST_SUITE(TStateStorageRingGroupProxyMutation) {
    Y_UNIT_TEST(SigNoneWaitsForEveryRingGroupAfterSuccessfulQuorum) {
        CheckSuccessfulLockWaits(TEvStateStorage::TProxyOptions::SigNone);
    }

    Y_UNIT_TEST(SigAsyncWaitsForEveryRingGroupAfterSuccessfulQuorum) {
        CheckSuccessfulLockWaits(TEvStateStorage::TProxyOptions::SigAsync);
    }

    Y_UNIT_TEST(WaitsForEveryRingGroupAndKeepsRace) {
        TLockFixture f;
        f.Reply(3, f.OtherLeader, TLockFixture::Generation + 1);
        f.Reply(4, f.OtherLeader, TLockFixture::Generation + 1);
        f.Elapse();
        UNIT_ASSERT_VALUES_EQUAL(f.ReplyCount, 0);

        f.Reply(0, f.Leader, TLockFixture::Generation);
        f.Reply(1, f.Leader, TLockFixture::Generation);
        f.Elapse();
        UNIT_ASSERT_VALUES_EQUAL(f.ReplyCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(f.Status, NKikimrProto::RACE);
    }

    Y_UNIT_TEST(SafetyTimeoutWithoutChildRepliesReturnsTimeout) {
        CheckIncompleteLockShutdown(true, false);
    }

    Y_UNIT_TEST(SafetyTimeoutAfterPartialSuccessReturnsTimeout) {
        CheckIncompleteLockShutdown(true, true);
    }

    Y_UNIT_TEST(AllChildrenExitWithoutRepliesReturnsTimeout) {
        CheckIncompleteLockShutdown(false, false);
    }

    Y_UNIT_TEST(AllChildrenExitAfterPartialSuccessReturnsTimeout) {
        CheckIncompleteLockShutdown(false, true);
    }

    Y_UNIT_TEST(ShutdownKeepsRaceWhenAnotherRingGroupReplyIsLost) {
        for (bool safetyTimeout : {false, true}) {
            CheckIncompleteLockShutdown(safetyTimeout, true, true);
        }
    }

    Y_UNIT_TEST(ShutdownDoesNotReplyAgainAfterSuccess) {
        for (bool safetyTimeout : {false, true}) {
            TLockFixture f(TEvStateStorage::TProxyOptions::SigAsync);
            f.DropChildPassAway = safetyTimeout;
            for (ui32 i : {0u, 1u, 3u, 4u}) {
                f.Reply(i, f.Leader, TLockFixture::Generation);
            }
            f.Elapse();
            UNIT_ASSERT_VALUES_EQUAL(f.ReplyCount, 1);
            UNIT_ASSERT_VALUES_EQUAL(f.Status, NKikimrProto::OK);

            f.Elapse(TDuration::Seconds(60));
            f.Elapse();
            UNIT_ASSERT_VALUES_EQUAL(f.ReplyCount, 1);
            UNIT_ASSERT_VALUES_EQUAL(f.Status, NKikimrProto::OK);
        }
    }
}

// a tablet running when the pile is out has its leader in the ring groups of the other piles only; once the ring group
// of the pile is back in the configuration, the guardian of the tablet, subscribed to the proxy, writes the leader there
// too, so the tablet is found through that ring group after a failover to the pile
Y_UNIT_TEST_SUITE(TStateStorageGuardianRingGroups) {
    Y_UNIT_TEST(LeaderWrittenToRingGroupComingBack) {
        static constexpr ui64 TabletId = 100;
        static constexpr ui32 Generation = 2;

        TTestActorRuntimeBase runtime{1};
        runtime.Initialize();
        runtime.GetLogSettings(0)->Append(NKikimrServices::EServiceKikimr_MIN,
            NKikimrServices::EServiceKikimr_MAX,
            [](int component) -> const TString& { return NKikimrServices::EServiceKikimr_Name(component); });
        runtime.SetDispatchTimeout(TDuration::Seconds(5));
        const TActorId leader = runtime.AllocateEdgeActor();
        const TActorId tabletLeader = runtime.AllocateEdgeActor();

        // ring group 0 is the one of the primary pile, ring group 1 is the one of the pile being out
        TVector<TActorId> replicas;
        auto makeInfo = [&](ERingGroupState secondState) {
            auto info = MakeIntrusive<TStateStorageInfo>();
            info->RingGroups.resize(2);
            for (ui32 index = 0; index < 2; ++index) {
                auto& group = info->RingGroups[index];
                group.State = index ? secondState : ERingGroupState::PRIMARY;
                group.NToSelect = 1;
                group.Rings.resize(1);
                group.Rings[0].Replicas.push_back(replicas[index]);
            }
            return info;
        };
        for (ui32 index = 0; index < 2; ++index) {
            replicas.push_back(runtime.AllocateEdgeActor());
        }
        const TActorId proxy = runtime.Register(CreateStateStorageProxy(makeInfo(ERingGroupState::DISCONNECTED), nullptr,
            nullptr));
        runtime.RegisterService(MakeStateStorageProxyID(), proxy);

        // the guardian of the running tablet finds its leader in the replica of ring group 0
        runtime.Register(CreateStateStorageTabletGuardian(TabletId, leader, tabletLeader, Generation));
        auto lookup = runtime.GrabEdgeEvent<TEvStateStorage::TEvReplicaLookup>(replicas[0]);
        UNIT_ASSERT(lookup);
        auto reply = MakeHolder<TEvStateStorage::TEvReplicaInfo>(TabletId, leader, tabletLeader, Generation, 0, false, 0,
            0, 0);
        reply->Record.SetCookie(lookup->Get()->Record.GetCookie());
        runtime.Send(new IEventHandle(lookup->Sender, replicas[0], reply.Release()));

        // the pile comes back, and its ring group is part of the configuration again
        runtime.Send(new IEventHandle(proxy, TActorId(), new TEvStateStorage::TEvUpdateGroupConfig(
            makeInfo(ERingGroupState::NOT_SYNCHRONIZED), nullptr, nullptr)));
        lookup = runtime.GrabEdgeEvent<TEvStateStorage::TEvReplicaLookup>(replicas[1]);
        UNIT_ASSERT_C(lookup, "the guardian doesn't watch the replica of the ring group coming back");
        UNIT_ASSERT_VALUES_EQUAL(lookup->Get()->Record.GetTabletID(), TabletId);

        // the replica knows nothing of the tablet, so the guardian writes the leader there
        reply = MakeHolder<TEvStateStorage::TEvReplicaInfo>(TabletId, NKikimrProto::ERROR, 0, 0);
        reply->Record.SetCookie(lookup->Get()->Record.GetCookie());
        runtime.Send(new IEventHandle(lookup->Sender, replicas[1], reply.Release()));
        auto update = runtime.GrabEdgeEvent<TEvStateStorage::TEvReplicaUpdate>(replicas[1]);
        UNIT_ASSERT(update);
        const auto& record = update->Get()->Record;
        UNIT_ASSERT_VALUES_EQUAL(record.GetTabletID(), TabletId);
        UNIT_ASSERT_VALUES_EQUAL(record.GetProposedGeneration(), Generation);
        UNIT_ASSERT_VALUES_EQUAL(ActorIdFromProto(record.GetProposedLeader()), leader);
        UNIT_ASSERT_VALUES_EQUAL(ActorIdFromProto(record.GetProposedLeaderTablet()), tabletLeader);
    }
}

} // namespace NKikimr

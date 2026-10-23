#include "statestorage_impl.h"

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
    TVector<TActorId> Requests;
    TVector<TActorId> Replicas;
    TVector<ui64> ReplicaCookies;
    ui32 ReplyCount = 0;
    ui32 SignatureUpdateCount = 0;
    NKikimrProto::EReplyStatus Status = NKikimrProto::UNKNOWN;
    TActorId ReplyLeader;
    ui32 ReplyGeneration = 0;
    bool ReplyLocked = false;
    size_t ReplyFollowers = 0;
    TEvStateStorage::TSignature Signatures;

    explicit TLookupFixture(TOptions::ESigWaitMode mode = TOptions::SigNone,
            const TVector<ERingGroupState>& ringGroups = {ERingGroupState::PRIMARY}, bool lock = false) {
        Runtime.Initialize();
        Runtime.GetLogSettings(0)->Append(NKikimrServices::EServiceKikimr_MIN,
            NKikimrServices::EServiceKikimr_MAX,
            [](int component) -> const TString& { return NKikimrServices::EServiceKikimr_Name(component); });
        Runtime.SetScheduledEventFilter([](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>&,
                                           TDuration, TInstant&) { return false; });
        Runtime.SetDispatchTimeout(TDuration::Seconds(5));
        Edge = Runtime.AllocateEdgeActor();
        Leader = Runtime.AllocateEdgeActor();

        auto info = MakeIntrusive<TStateStorageInfo>();
        for (ERingGroupState state : ringGroups) {
            auto& group = info->RingGroups.emplace_back();
            group.State = state;
            group.NToSelect = 5;
            group.Rings.resize(5);
            for (auto& ring : group.Rings) {
                Replicas.push_back(Runtime.AllocateEdgeActor());
                ring.Replicas.push_back(Replicas.back());
            }
        }
        auto proxy = Runtime.Register(CreateStateStorageProxy(info, nullptr, nullptr));
        Runtime.SetObserverFunc([this](TAutoPtr<IEventHandle>& event) {
            if (event->Recipient == Edge) {
                if (event->GetTypeRewrite() == TEvStateStorage::TEvInfo::EventType) {
                    auto* reply = event->Get<TEvStateStorage::TEvInfo>();
                    UNIT_ASSERT_VALUES_EQUAL(reply->TabletID, TabletId);
                    UNIT_ASSERT_VALUES_EQUAL(reply->Cookie, Cookie);
                    ++ReplyCount;
                    Status = reply->Status;
                    ReplyLeader = reply->CurrentLeader;
                    ReplyGeneration = reply->CurrentGeneration;
                    ReplyLocked = reply->Locked;
                    ReplyFollowers = reply->Followers.size();
                    Signatures = reply->Signature;
                    return TTestActorRuntimeBase::EEventAction::DROP;
                } else if (event->GetTypeRewrite() == TEvStateStorage::TEvUpdateSignature::EventType) {
                    auto* reply = event->Get<TEvStateStorage::TEvUpdateSignature>();
                    ++SignatureUpdateCount;
                    Signatures = reply->Signature;
                    return TTestActorRuntimeBase::EEventAction::DROP;
                }
            }
            return TTestActorRuntimeBase::EEventAction::PROCESS;
        });
        if (lock) {
            Runtime.SendAsync(new IEventHandle(proxy, Edge, new TEvStateStorage::TEvLock(TabletId, Cookie, Leader, 100, {}, mode)));
            GrabRequests<TEvStateStorage::TEvReplicaLock>();
        } else {
            Runtime.SendAsync(new IEventHandle(proxy, Edge, new TEvStateStorage::TEvLookup(TabletId, Cookie, mode)));
            GrabRequests<TEvStateStorage::TEvReplicaLookup>();
        }
    }

    template <class TRequest>
    void GrabRequests() {
        for (const auto& replica : Replicas) {
            auto request = Runtime.GrabEdgeEvent<TRequest>(replica);
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

    void KnownLeader(ui32 index, ui32 generation = 100, bool locked = false) {
        Reply(index, MakeHolder<TEvStateStorage::TEvReplicaInfo>(TabletId, Leader, TActorId(), generation, 0, locked, 0, 0, 0));
    }

    // A majority of the 5 replicas of a ring group replies
    void EmptyQuorum(ui32 group) {
        for (ui32 i = 0; i < 3; ++i) {
            Empty(group * 5 + i);
        }
    }

    void LeaderQuorum(ui32 group, ui32 generation = 100) {
        for (ui32 i = 0; i < 3; ++i) {
            KnownLeader(group * 5 + i, generation);
        }
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

// Ring groups of a 2-pile bridge cluster
const TVector<ERingGroupState> BridgeRingGroups = {ERingGroupState::SYNCHRONIZED, ERingGroupState::PRIMARY};
constexpr ui32 SyncGroup = 0;
constexpr ui32 PrimaryGroup = 1;

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

    Y_UNIT_TEST(EachRingGroupRequiresItsOwnNegativeQuorum) {
        TLookupFixture f(TEvStateStorage::TProxyOptions::SigNone, {ERingGroupState::PRIMARY, ERingGroupState::PRIMARY});
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

    Y_UNIT_TEST(EmptyPrimaryDoesNotHideSynchronizedLeader) {
        TLookupFixture f(TEvStateStorage::TProxyOptions::SigNone, BridgeRingGroups);
        f.EmptyQuorum(PrimaryGroup);
        f.ExpectPending();
        f.LeaderQuorum(SyncGroup);
        f.ExpectReply(NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(f.ReplyLeader, f.Leader);
        UNIT_ASSERT_VALUES_EQUAL(f.ReplyGeneration, 100);
    }

    Y_UNIT_TEST(EmptySynchronizedDoesNotHidePrimaryLeader) {
        TLookupFixture f(TEvStateStorage::TProxyOptions::SigNone, BridgeRingGroups);
        f.LeaderQuorum(PrimaryGroup);
        f.ExpectPending();
        f.EmptyQuorum(SyncGroup);
        f.ExpectReply(NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(f.ReplyLeader, f.Leader);
    }

    Y_UNIT_TEST(LookupTieTakesEntryOfChosenStatus) {
        TLookupFixture f(TEvStateStorage::TProxyOptions::SigNone, BridgeRingGroups);
        // A minority of the synchronized group saw a lock at the same generation: RACE
        f.Empty(0);
        f.Empty(1);
        f.KnownLeader(2, 100, /* locked */ true);
        f.ExpectPending();
        f.Empty(3);
        f.ExpectPending();
        f.LeaderQuorum(PrimaryGroup);
        f.ExpectReply(NKikimrProto::OK);
        UNIT_ASSERT(!f.ReplyLocked);
    }

    Y_UNIT_TEST(FollowersOfEveryRingGroupAreReported) {
        TLookupFixture f(TEvStateStorage::TProxyOptions::SigNone, BridgeRingGroups);
        // The synchronized group is behind but knows a follower
        for (ui32 i = 0; i < 3; ++i) {
            auto reply = MakeHolder<TEvStateStorage::TEvReplicaInfo>(TLookupFixture::TabletId, f.Leader, TActorId(), 99, 0, false, 0, 0, 0);
            ActorIdToProto(f.Edge, reply->Record.AddFollowerInfo()->MutableFollower());
            f.Reply(i, std::move(reply));
        }
        f.ExpectPending();
        f.LeaderQuorum(PrimaryGroup);
        f.ExpectReply(NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(f.ReplyGeneration, 100);
        UNIT_ASSERT_VALUES_EQUAL(f.ReplyFollowers, 1);
    }

    Y_UNIT_TEST(LockRaceInAnyRingGroupWins) {
        TLookupFixture f(TEvStateStorage::TProxyOptions::SigNone, BridgeRingGroups, /* lock */ true);
        f.LeaderQuorum(SyncGroup, 101);
        f.ExpectPending();
        f.LeaderQuorum(PrimaryGroup);
        f.ExpectReply(NKikimrProto::RACE);
        UNIT_ASSERT_VALUES_EQUAL(f.ReplyGeneration, 101);
    }
}

} // namespace NKikimr

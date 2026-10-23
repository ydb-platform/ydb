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
    enum class ERequest { Lookup, Lock, Update };
    static constexpr ui64 TabletId = 100;
    static constexpr ui64 Cookie = 42;

    TTestActorRuntimeBase Runtime{1};
    TActorId Edge;
    TActorId Leader;
    TVector<TActorId> Requests;
    TVector<TActorId> Replicas;
    TVector<ui64> ReplicaCookies;
    ui32 RequestEventType = 0;
    ui32 ReplyCount = 0;
    ui32 SignatureUpdateCount = 0;
    NKikimrProto::EReplyStatus Status = NKikimrProto::UNKNOWN;
    TActorId ReplyLeader;
    TEvStateStorage::TSignature Signatures;

    explicit TLookupFixture(TOptions::ESigWaitMode mode = TOptions::SigNone,
                            ERequest kind = ERequest::Lookup, ui32 ringGroupCount = 1) {
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
        info->RingGroups.resize(ringGroupCount);
        TEvStateStorage::TSignature signature;
        for (auto& group : info->RingGroups) {
            group.State = ERingGroupState::PRIMARY;
            group.NToSelect = 5;
            group.Rings.resize(5);
            for (auto& ring : group.Rings) {
                Replicas.push_back(Runtime.AllocateEdgeActor());
                ring.Replicas.push_back(Replicas.back());
                signature.SetReplicaSignature(Replicas.back(), Replicas.size());
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
        IEventBase* event = nullptr;
        switch (kind) {
        case ERequest::Lookup:
            event = new TEvStateStorage::TEvLookup(TabletId, Cookie, mode);
            RequestEventType = TEvStateStorage::TEvReplicaLookup::EventType;
            break;
        case ERequest::Lock:
            event = new TEvStateStorage::TEvLock(TabletId, Cookie, Leader, 100, signature, mode);
            RequestEventType = TEvStateStorage::TEvReplicaLock::EventType;
            break;
        case ERequest::Update:
            event = new TEvStateStorage::TEvUpdate(TabletId, Cookie, Leader, TActorId(), 100, 0, signature, mode);
            RequestEventType = TEvStateStorage::TEvReplicaUpdate::EventType;
            break;
        }
        Runtime.SendAsync(new IEventHandle(proxy, Edge, event));
        Requests.resize(Replicas.size());
        ReplicaCookies.resize(Replicas.size());
        for (ui32 i = 0; i < Replicas.size(); ++i) {
            switch (kind) {
            case ERequest::Lookup: CaptureRequest<TEvStateStorage::TEvReplicaLookup>(i); break;
            case ERequest::Lock: CaptureRequest<TEvStateStorage::TEvReplicaLock>(i); break;
            case ERequest::Update: CaptureRequest<TEvStateStorage::TEvReplicaUpdate>(i); break;
            }
        }
    }

    template<typename TEvent>
    void CaptureRequest(ui32 index) {
        auto request = Runtime.GrabEdgeEvent<TEvent>(Replicas[index]);
        UNIT_ASSERT(request);
        Requests[index] = request->Sender;
        ReplicaCookies[index] = request->Get()->Record.GetCookie();
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
        Reply(index, MakeHolder<TEvStateStorage::TEvReplicaInfo>(TabletId, Leader, TActorId(), 100, 0, false, 0, 0, 0));
    }

    void Fail(ui32 index) {
        Runtime.SendAsync(new IEventHandle(Requests[index], Replicas[index], new TEvents::TEvUndelivered(
            RequestEventType, TEvents::TEvUndelivered::Disconnected, true),
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

void CheckLateReply(TEvStateStorage::TProxyOptions::ESigWaitMode mode) {
    TLookupFixture f(mode);
    f.Empty(0);
    f.Fail(1);
    f.Fail(2);
    f.ExpectPending();
    f.Empty(1);
    f.ExpectPending();
    f.Fail(3);
    // Four distinct replicas have completed; the fifth can still give a quorum.
    f.ExpectPending();
    f.Empty(4);
    f.ExpectReply(NKikimrProto::NODATA);
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
        TLookupFixture f(TEvStateStorage::TProxyOptions::SigNone, TLookupFixture::ERequest::Lookup, 2);
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

    Y_UNIT_TEST(LateReplyAfterUndeliveredSigNone) {
        CheckLateReply(TEvStateStorage::TProxyOptions::SigNone);
    }

    Y_UNIT_TEST(LateReplyAfterUndeliveredSigAsync) {
        CheckLateReply(TEvStateStorage::TProxyOptions::SigAsync);
    }

    Y_UNIT_TEST(LateReplyAfterUndeliveredSigSync) {
        CheckLateReply(TEvStateStorage::TProxyOptions::SigSync);
    }

    Y_UNIT_TEST(AsyncLateReplyReplacesFailureBeforeInitialReply) {
        TLookupFixture f(TEvStateStorage::TProxyOptions::SigAsync);
        f.Fail(0);
        f.KnownLeader(1);
        f.KnownLeader(2);
        f.KnownLeader(3);
        f.ExpectReply(NKikimrProto::OK);
        f.KnownLeader(0);
        f.Elapse();
        UNIT_ASSERT_VALUES_EQUAL(f.SignatureUpdateCount, 0);
        f.KnownLeader(4);
        f.Elapse();
        UNIT_ASSERT_VALUES_EQUAL(f.SignatureUpdateCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(f.Signatures.Size(), 5);
    }

    Y_UNIT_TEST(AsyncDuplicateFailureAndLateReplyCountOnce) {
        TLookupFixture f(TEvStateStorage::TProxyOptions::SigAsync);
        f.KnownLeader(0);
        f.KnownLeader(1);
        f.KnownLeader(2);
        f.ExpectReply(NKikimrProto::OK);
        f.Fail(3);
        f.Fail(3);
        f.Elapse();
        UNIT_ASSERT_VALUES_EQUAL(f.SignatureUpdateCount, 0);
        f.KnownLeader(3);
        f.Elapse();
        UNIT_ASSERT_VALUES_EQUAL(f.SignatureUpdateCount, 0);
        f.KnownLeader(4);
        f.Elapse();
        UNIT_ASSERT_VALUES_EQUAL(f.SignatureUpdateCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(f.Signatures.Size(), 5);
    }

    Y_UNIT_TEST(AsyncTimeoutIncludesSignatureReplacingEarlierFailure) {
        TLookupFixture f(TEvStateStorage::TProxyOptions::SigAsync);
        f.Fail(0);
        f.KnownLeader(1);
        f.KnownLeader(2);
        f.KnownLeader(3);
        f.ExpectReply(NKikimrProto::OK);
        f.KnownLeader(0);
        f.Elapse();
        UNIT_ASSERT_VALUES_EQUAL(f.SignatureUpdateCount, 0);
        f.Elapse(TDuration::Seconds(30));
        UNIT_ASSERT_VALUES_EQUAL(f.SignatureUpdateCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(f.Signatures.Size(), 4);
        UNIT_ASSERT_VALUES_EQUAL(f.Signatures.GetReplicaSignature(f.Replicas[0]), 1);
    }

    Y_UNIT_TEST(AsyncTimeoutAfterDeliveryFailureStillSendsUpdate) {
        TLookupFixture f(TEvStateStorage::TProxyOptions::SigAsync);
        f.KnownLeader(0);
        f.KnownLeader(1);
        f.KnownLeader(2);
        f.ExpectReply(NKikimrProto::OK);
        f.Fail(3);
        // The last replica stays pending; only a delivery failure was added.
        f.Elapse(TDuration::Seconds(30));
        UNIT_ASSERT_VALUES_EQUAL(f.SignatureUpdateCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(f.Signatures.Size(), 3);
    }

    Y_UNIT_TEST(AsyncTimeoutWithoutFurtherEventsDoesNotSendUpdate) {
        TLookupFixture f(TEvStateStorage::TProxyOptions::SigAsync);
        f.KnownLeader(0);
        f.KnownLeader(1);
        f.KnownLeader(2);
        f.ExpectReply(NKikimrProto::OK);
        f.Elapse(TDuration::Seconds(30));
        UNIT_ASSERT_VALUES_EQUAL(f.SignatureUpdateCount, 0);
    }

    Y_UNIT_TEST(LockAndUpdateSigSyncCountLateReplyOnce) {
        for (auto kind : {TLookupFixture::ERequest::Lock, TLookupFixture::ERequest::Update}) {
            TLookupFixture f(TEvStateStorage::TProxyOptions::SigSync, kind);
            f.Fail(0);
            f.KnownLeader(1);
            f.KnownLeader(2);
            f.KnownLeader(3);
            f.ExpectPending();
            f.KnownLeader(0);
            f.ExpectPending();
            f.KnownLeader(4);
            f.ExpectReply(NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(f.Signatures.Size(), 5);
        }
    }
}

} // namespace NKikimr

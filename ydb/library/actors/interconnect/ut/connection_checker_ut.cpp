#include "lib/ic_test_cluster.h"
#include "lib/test_events.h"
#include "lib/test_actors.h"

#include <ydb/library/actors/interconnect/interconnect_tcp_proxy.h>

#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/system/event.h>
#include <util/system/sanitizers.h>

namespace {

    class TPinger : public TActorBootstrapped<TPinger> {
        const TActorId PongerId;
        ui32 Counter;
        TManualEvent *Event;
        std::atomic_uint32_t* const NumConnected;
        std::atomic_uint32_t* const NumDisconnected;

    public:
        TPinger(TActorId pongerId, TManualEvent *event, std::atomic_uint32_t *numConnected, std::atomic_uint32_t *numDisconnected,
                ui32 count = 100)
            : PongerId(pongerId)
            , Counter(count)
            , Event(event)
            , NumConnected(numConnected)
            , NumDisconnected(numDisconnected)
        {}

        void Bootstrap() {
            Become(&TThis::StateFunc);
            Send(PongerId, new TEvTest(1, "ping"), IEventHandle::FlagSubscribeOnSession);
        }

        void HandleTest() {
            if (!--Counter) {
                Event->Signal();
            } else {
                Bootstrap();
            }
        }

        void HandleConnected() {
            ++*NumConnected;
        }

        void HandleDisconnected() {
            ++*NumDisconnected;
            HandleTest();
        }

        STRICT_STFUNC(StateFunc,
            cFunc(TEvInterconnect::EvNodeConnected, HandleConnected)
            cFunc(TEvInterconnect::EvNodeDisconnected, HandleDisconnected)
            cFunc(EvTest, HandleTest)
        )
    };

    class TPonger : public TActor<TPonger> {
    public:
        TPonger()
            : TActor(&TThis::StateFunc)
        {}

        void Handle(TAutoPtr<IEventHandle> ev) {
            const TActorId sender = ev->Sender;
            TActivationContext::Send(IEventHandle::Forward(ev, sender));
        }

        STRICT_STFUNC(StateFunc,
            fFunc(EvTest, Handle)
        )
    };

    class TCheckerActor : public TActor<TCheckerActor> {
        const ui32 NodeId;
        std::atomic_uint32_t* const NumIncoming;
        std::atomic_uint32_t* const NumOutgoing;
        std::atomic_uint32_t* const NumNotify;
        std::atomic_uint32_t* const NumNotifySuccess;
        const bool Reject;
        const bool CheckReply;
        TManualEvent* const ReplyRejected;
        TManualEvent* const PauseIncoming;
        TEvInterconnect::TEvCheckIncomingConnection::TPtr Pending;

    public:
        TCheckerActor(ui32 nodeId, std::atomic_uint32_t *numIncoming, std::atomic_uint32_t *numOutgoing,
                std::atomic_uint32_t *numNotify, std::atomic_uint32_t *numNotifySuccess, bool reject = false,
                bool checkReply = false, TManualEvent* replyRejected = nullptr, TManualEvent* pauseIncoming = nullptr)
            : TActor(&TThis::StateFunc)
            , NodeId(nodeId)
            , NumIncoming(numIncoming)
            , NumOutgoing(numOutgoing)
            , NumNotify(numNotify)
            , NumNotifySuccess(numNotifySuccess)
            , Reject(reject)
            , CheckReply(checkReply)
            , ReplyRejected(replyRejected)
            , PauseIncoming(pauseIncoming)
        {}

        void Handle(TEvInterconnect::TEvPrepareOutgoingConnection::TPtr ev) {
            auto& msg = *ev->Get();
            Y_ABORT_UNLESS(msg.PeerNodeId == 3 - NodeId);

            THashMap<TString, TString> params;
            params.emplace("recipient", NodeId == 1 ? "pinger" : "ponger");
            Send(ev->Sender, new TEvInterconnect::TEvPrepareOutgoingConnectionResult(std::move(params), CheckReply), 0, ev->Cookie);

            ++*NumOutgoing;
        }

        void ProcessIncoming(TEvInterconnect::TEvCheckIncomingConnection::TPtr& ev) {
            auto& msg = *ev->Get();
            Y_ABORT_UNLESS(msg.PeerNodeId == 3 - NodeId);
            Y_ABORT_UNLESS(msg.Params["recipient"] == (NodeId == 1 ? "ponger" : "pinger"));
            Y_ABORT_UNLESS(msg.Params.size() == 1);

            if (Reject) {
                Send(ev->Sender, new TEvInterconnect::TEvCheckIncomingConnectionResult("rejected", {{"hello", "world"}}),
                    0, ev->Cookie);
            } else {
                THashMap<TString, TString> params;
                params.emplace("hello", "world");
                Send(ev->Sender, new TEvInterconnect::TEvCheckIncomingConnectionResult(std::nullopt, std::move(params)), 0, ev->Cookie);
            }

            ++*NumIncoming;
        }

        void Handle(TEvInterconnect::TEvCheckIncomingConnection::TPtr ev) {
            if (PauseIncoming && !Pending) {
                Pending = std::move(ev);
                PauseIncoming->Signal();
                return;
            }
            ProcessIncoming(ev);
        }

        void HandleResume(TEvents::TEvWakeup::TPtr) {
            if (Pending) {
                auto ev = std::move(Pending);
                ProcessIncoming(ev);
                Pending.Reset();
            }
        }

        void Handle(TEvInterconnect::TEvNotifyOutgoingConnectionEstablished::TPtr ev) {
            auto& msg = *ev->Get();
            Y_ABORT_UNLESS(msg.Params["hello"] == "world");
            Y_ABORT_UNLESS(msg.Params.size() == 1);

            ++*NumNotify;
            *NumNotifySuccess += msg.Success;
        }

        void Handle(TEvInterconnect::TEvCheckOutgoingConnection::TPtr ev) {
            UNIT_ASSERT(CheckReply);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Params.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Params.at("hello"), "world");
            Send(ev->Sender, new TEvInterconnect::TEvCheckOutgoingConnectionResult("outgoing check rejected"), 0, ev->Cookie);
            if (ReplyRejected) {
                ReplyRejected->Signal();
            }
        }

        STRICT_STFUNC(StateFunc,
            hFunc(TEvInterconnect::TEvPrepareOutgoingConnection, Handle)
            hFunc(TEvInterconnect::TEvCheckIncomingConnection, Handle)
            hFunc(TEvents::TEvWakeup, HandleResume)
            hFunc(TEvInterconnect::TEvNotifyOutgoingConnectionEstablished, Handle)
            hFunc(TEvInterconnect::TEvCheckOutgoingConnection, Handle)
        )
    };

    class TAdmissionRecheckProbe : public TActorBootstrapped<TAdmissionRecheckProbe> {
        const TActorId Proxy;
        const TActorId Checker;
        std::atomic<ui32>& RequiredLevel;
        TManualEvent& Done;

    public:
        TAdmissionRecheckProbe(TActorId proxy, TActorId checker,
                std::atomic<ui32>& requiredLevel, TManualEvent& done)
            : Proxy(proxy)
            , Checker(checker)
            , RequiredLevel(requiredLevel)
            , Done(done)
        {}

        void Bootstrap() {
            Become(&TThis::StateFunc);
            RequiredLevel.store(1);
            Send(Proxy, new TEvInterconnect::TEvRecheckConnectionAdmission, 0, 42);
        }

        void Handle(TEvInterconnect::TEvRecheckConnectionAdmissionResult::TPtr ev) {
            UNIT_ASSERT_VALUES_EQUAL(ev->Cookie, 42);
            RequiredLevel.store(0);
            Send(Checker, new TEvents::TEvWakeup);
            Done.Signal();
        }

        STRICT_STFUNC(StateFunc,
            hFunc(TEvInterconnect::TEvRecheckConnectionAdmissionResult, Handle)
        )
    };

    class TConnectionAdmissionProbe : public TActorBootstrapped<TConnectionAdmissionProbe> {
        const TActorId Proxy;
        std::atomic<ui32>& RequiredLevel;
        TManualEvent& Done;
        bool Disconnected = false;
        bool ReconnectRejected = false;

    public:
        TConnectionAdmissionProbe(TActorId proxy, std::atomic<ui32>& requiredLevel, TManualEvent& done)
            : Proxy(proxy)
            , RequiredLevel(requiredLevel)
            , Done(done)
        {}

        void Bootstrap() {
            Become(&TThis::StateFunc);
            Send(Proxy, new TEvInterconnect::TEvConnectNode, 0, 1);
        }

        void Handle(TEvInterconnect::TEvNodeConnected::TPtr ev) {
            UNIT_ASSERT_VALUES_EQUAL(ev->Cookie, 1);
            RequiredLevel.store(1);
            Send(Proxy, new TEvInterconnect::TEvRecheckConnectionAdmission, 0, 2);
        }

        void Handle(TEvInterconnect::TEvRecheckConnectionAdmissionResult::TPtr ev) {
            UNIT_ASSERT_VALUES_EQUAL(ev->Cookie, 2);
            Send(Proxy, new TEvInterconnect::TEvConnectNode, 0, 3);
        }

        void Handle(TEvInterconnect::TEvNodeDisconnected::TPtr ev) {
            UNIT_ASSERT(ev->Cookie == 1 || ev->Cookie == 3);
            Disconnected |= ev->Cookie == 1;
            ReconnectRejected |= ev->Cookie == 3;
            if (Disconnected && ReconnectRejected) {
                Done.Signal();
            }
        }

        STRICT_STFUNC(StateFunc,
            hFunc(TEvInterconnect::TEvNodeConnected, Handle)
            hFunc(TEvInterconnect::TEvNodeDisconnected, Handle)
            hFunc(TEvInterconnect::TEvRecheckConnectionAdmissionResult, Handle)
        )
    };

}

Y_UNIT_TEST_SUITE(ConnectionChecker) {
    using namespace NActors;

    Y_UNIT_TEST(CheckParams) {
        std::atomic_uint32_t numConnected = 0;
        std::atomic_uint32_t numDisconnected = 0;
        std::atomic_uint32_t numIncoming = 0;
        std::atomic_uint32_t numOutgoing = 0;
        std::atomic_uint32_t numNotify = 0;
        std::atomic_uint32_t numNotifySuccess = 0;
        auto checkerFactory = [&](ui32 nodeId) { return new TCheckerActor(nodeId, &numIncoming, &numOutgoing, &numNotify, &numNotifySuccess); };
        TTestICCluster testCluster(2, {}, nullptr, nullptr, TTestICCluster::DISABLE_RDMA, checkerFactory);
        auto pongerId = testCluster.RegisterActor(new TPonger, 1);
        TManualEvent ev;
        testCluster.RegisterActor(new TPinger(pongerId, &ev, &numConnected, &numDisconnected), 2);
        ev.WaitI();
        Y_ABORT_UNLESS(numConnected.load() == 100);
        Y_ABORT_UNLESS(numDisconnected.load() == 0);
        Y_ABORT_UNLESS(numIncoming.load() > 0);
        Y_ABORT_UNLESS(numOutgoing.load() > 0);
        Y_ABORT_UNLESS(numNotify.load() > 0);
        Y_ABORT_UNLESS(numNotifySuccess.load() == numNotify.load());
    }

    Y_UNIT_TEST(CheckReject) {
        std::atomic_uint32_t numConnected = 0;
        std::atomic_uint32_t numDisconnected = 0;
        std::atomic_uint32_t numIncoming = 0;
        std::atomic_uint32_t numOutgoing = 0;
        std::atomic_uint32_t numNotify = 0;
        std::atomic_uint32_t numNotifySuccess = 0;
        auto checkerFactory = [&](ui32 nodeId) { return new TCheckerActor(nodeId, &numIncoming, &numOutgoing, &numNotify, &numNotifySuccess, true); };
        TTestICCluster testCluster(2, {}, nullptr, nullptr, TTestICCluster::DISABLE_RDMA, checkerFactory);
        auto pongerId = testCluster.RegisterActor(new TPonger, 1);
        TManualEvent ev;
        testCluster.RegisterActor(new TPinger(pongerId, &ev, &numConnected, &numDisconnected), 2);
        ev.WaitI();
        Y_ABORT_UNLESS(numConnected.load() == 0);
        Y_ABORT_UNLESS(numDisconnected.load() == 100);
        Y_ABORT_UNLESS(numIncoming.load() > 0);
        Y_ABORT_UNLESS(numOutgoing.load() > 0);
        Y_ABORT_UNLESS(numNotify.load() > 0);
        Y_ABORT_UNLESS(numNotifySuccess.load() == 0);
    }

    Y_UNIT_TEST(OutgoingReplyCanBeRejectedBeforePublishingSession) {
        std::atomic_uint32_t numConnected = 0;
        std::atomic_uint32_t numDisconnected = 0;
        std::atomic_uint32_t numIncoming = 0;
        std::atomic_uint32_t numOutgoing = 0;
        std::atomic_uint32_t numNotify = 0;
        std::atomic_uint32_t numNotifySuccess = 0;
        TManualEvent replyRejected;
        TManualEvent done;
        auto checkerFactory = [&](ui32 nodeId) {
            return new TCheckerActor(nodeId, &numIncoming, &numOutgoing, &numNotify, &numNotifySuccess, false,
                nodeId == 2, &replyRejected);
        };
        TTestICCluster testCluster(2, {}, nullptr, nullptr,
            TTestICCluster::Flags(TTestICCluster::USE_TLS | TTestICCluster::DISABLE_RDMA), checkerFactory);
        const auto pongerId = testCluster.RegisterActor(new TPonger, 1);
        testCluster.RegisterActor(new TPinger(pongerId, &done, &numConnected, &numDisconnected, 1), 2);

        UNIT_ASSERT(done.WaitT(TDuration::Seconds(30)));
        UNIT_ASSERT(replyRejected.WaitT(TDuration::Seconds(10)));
        UNIT_ASSERT_VALUES_EQUAL(numConnected.load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(numDisconnected.load(), 1);
        UNIT_ASSERT_VALUES_EQUAL(numNotifySuccess.load(), 0);
    }

    Y_UNIT_TEST(AdmissionRecheckDisconnectsAndFencesReconnect) {
        TManualEvent done;
        TTestICCluster testCluster(2, {}, nullptr, nullptr,
            TTestICCluster::Flags(TTestICCluster::DISABLE_RDMA | TTestICCluster::TRACK_ADMISSION));
        testCluster.RegisterActor(new TConnectionAdmissionProbe(testCluster.InterconnectProxy(1, 2),
            testCluster.GetNode(2)->GetConnectionAdmissionLevel(), done), 2);
        UNIT_ASSERT(done.WaitT(TDuration::Seconds(30)));
    }

    Y_UNIT_TEST(AdmissionRecheckDropsOutstandingHandshake) {
        std::atomic_uint32_t numConnected = 0;
        std::atomic_uint32_t numDisconnected = 0;
        std::atomic_uint32_t numIncoming = 0;
        std::atomic_uint32_t numOutgoing = 0;
        std::atomic_uint32_t numNotify = 0;
        std::atomic_uint32_t numNotifySuccess = 0;
        TManualEvent entered;
        TManualEvent rechecked;
        TManualEvent done;
        auto checkerFactory = [&](ui32 nodeId) -> IActor* {
            if (nodeId == 1) {
                return new TCheckerActor(nodeId, &numIncoming, &numOutgoing, &numNotify, &numNotifySuccess, false,
                    false, nullptr, &entered);
            }
            return new TCheckerActor(nodeId, &numIncoming, &numOutgoing, &numNotify, &numNotifySuccess);
        };
        TTestICCluster testCluster(2, {}, nullptr, nullptr,
            TTestICCluster::Flags(TTestICCluster::DISABLE_RDMA | TTestICCluster::TRACK_ADMISSION), checkerFactory);
        const auto pongerId = testCluster.RegisterActor(new TPonger, 1);
        testCluster.RegisterActor(new TPinger(pongerId, &done, &numConnected, &numDisconnected, 1), 2);
        UNIT_ASSERT(entered.WaitT(TDuration::Seconds(30)));

        testCluster.RegisterActor(new TAdmissionRecheckProbe(testCluster.InterconnectProxy(2, 1),
            testCluster.GetNode(1)->GetConnectionCheckerActorId(), testCluster.GetNode(1)->GetConnectionAdmissionLevel(),
            rechecked), 1);
        UNIT_ASSERT(rechecked.WaitT(TDuration::Seconds(30)));
        UNIT_ASSERT(done.WaitT(TDuration::Seconds(30)));
        UNIT_ASSERT_VALUES_EQUAL(numConnected.load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(numDisconnected.load(), 1);
    }

}

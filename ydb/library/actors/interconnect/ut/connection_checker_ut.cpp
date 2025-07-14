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
        ui32 Counter = 100;
        TManualEvent *Event;
        std::atomic_uint32_t* const NumConnected;
        std::atomic_uint32_t* const NumDisconnected;

    public:
        TPinger(TActorId pongerId, TManualEvent *event, std::atomic_uint32_t *numConnected, std::atomic_uint32_t *numDisconnected)
            : PongerId(pongerId)
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

    public:
        TCheckerActor(ui32 nodeId, std::atomic_uint32_t *numIncoming, std::atomic_uint32_t *numOutgoing,
                std::atomic_uint32_t *numNotify, std::atomic_uint32_t *numNotifySuccess, bool reject = false)
            : TActor(&TThis::StateFunc)
            , NodeId(nodeId)
            , NumIncoming(numIncoming)
            , NumOutgoing(numOutgoing)
            , NumNotify(numNotify)
            , NumNotifySuccess(numNotifySuccess)
            , Reject(reject)
        {}

        void Handle(TEvInterconnect::TEvPrepareOutgoingConnection::TPtr ev) {
            auto& msg = *ev->Get();
            Y_ABORT_UNLESS(msg.PeerNodeId == 3 - NodeId);

            THashMap<TString, TString> params;
            params.emplace("recipient", NodeId == 1 ? "pinger" : "ponger");
            Send(ev->Sender, new TEvInterconnect::TEvPrepareOutgoingConnectionResult(std::move(params)), 0, ev->Cookie);

            ++*NumOutgoing;
        }

        void Handle(TEvInterconnect::TEvCheckIncomingConnection::TPtr ev) {
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

        void Handle(TEvInterconnect::TEvNotifyOutgoingConnectionEstablished::TPtr ev) {
            auto& msg = *ev->Get();
            Y_ABORT_UNLESS(msg.Params["hello"] == "world");
            Y_ABORT_UNLESS(msg.Params.size() == 1);

            ++*NumNotify;
            *NumNotifySuccess += msg.Success;
        }

        STRICT_STFUNC(StateFunc,
            hFunc(TEvInterconnect::TEvPrepareOutgoingConnection, Handle)
            hFunc(TEvInterconnect::TEvCheckIncomingConnection, Handle)
            hFunc(TEvInterconnect::TEvNotifyOutgoingConnectionEstablished, Handle)
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
        TTestICCluster testCluster(2, {}, nullptr, nullptr, TTestICCluster::EMPTY, checkerFactory);
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
        TTestICCluster testCluster(2, {}, nullptr, nullptr, TTestICCluster::EMPTY, checkerFactory);
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

}

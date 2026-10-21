#include "distconf_selfheal.h"

#include <ydb/core/util/actorsys_test/testactorsys.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NStorage {

Y_UNIT_TEST_SUITE(TDistconfStateStorageSelfhealTest) {
    struct TSetup {
        TTestActorSystem Runtime{1};
        TActorId Client;
        TActorId NodeWarden;
        TActorId Selfheal;

        TSetup() {
            Runtime.Start();
            Client = Runtime.AllocateEdgeActor(1);
            NodeWarden = Runtime.AllocateEdgeActor(1);
            Runtime.RegisterService(MakeBlobStorageNodeWardenID(1), NodeWarden);
            Selfheal = Runtime.Register(new TStateStorageReassignNodeSelfhealActor(
                Client, 42, TDuration::Seconds(1), 10, 11, true, false, true), 1);
        }

        ~TSetup() {
            Runtime.Stop();
        }

        void ExpectRequest(bool disableRing) {
            auto request = Runtime.WaitForEdgeActorEvent<TEvNodeConfigInvokeOnRoot>(NodeWarden, false);
            UNIT_ASSERT_VALUES_EQUAL(request->Sender, Selfheal);
            UNIT_ASSERT(request->Get()->Record.HasReassignStateStorageNode());
            const auto& command = request->Get()->Record.GetReassignStateStorageNode();
            UNIT_ASSERT_VALUES_EQUAL(command.GetFrom(), 10);
            UNIT_ASSERT_VALUES_EQUAL(command.GetTo(), 11);
            UNIT_ASSERT(command.GetStateStorage());
            UNIT_ASSERT(!command.GetStateStorageBoard());
            UNIT_ASSERT(command.GetSchemeBoard());
            UNIT_ASSERT_VALUES_EQUAL(command.GetDisableRing(), disableRing);
        }

        void ReplyOK() {
            auto result = std::make_unique<TEvNodeConfigInvokeOnRootResult>();
            result->Record.SetStatus(NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK);
            Runtime.Send(new IEventHandle(Selfheal, NodeWarden, result.release()), 1);
            Runtime.Sim([&] { return Runtime.HasImmediateEvents(); });
        }
    };

    Y_UNIT_TEST(ReassignmentCompletesWithoutCancellation) {
        TSetup setup;
        setup.ExpectRequest(true);
        setup.ReplyOK();
        setup.ExpectRequest(false);
        setup.ReplyOK();
        const auto result = setup.Runtime.WaitForEdgeActorEvent<TEvNodeConfigInvokeOnRootResult>(setup.Client);
        UNIT_ASSERT_C(result->Get()->Record.GetStatus() ==
            NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK, result->Get()->Record.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(result->Cookie, 42);
    }

    Y_UNIT_TEST(CancelledReassignmentDoesNotResume) {
        TSetup setup;
        setup.ExpectRequest(true);
        setup.ReplyOK();
        setup.Runtime.Send(new IEventHandle(setup.Selfheal, setup.Client, new TEvents::TEvPoison), 1);
        setup.Runtime.Schedule(TDuration::Seconds(3),
            new IEventHandle(setup.NodeWarden, setup.Client, new TEvents::TEvWakeup), nullptr, 1);
        setup.Runtime.WaitForEdgeActorEvent<TEvents::TEvWakeup>(setup.NodeWarden);
        setup.Runtime.Send(new IEventHandle(setup.Client, {}, new TEvents::TEvWakeup), 1);
        setup.Runtime.WaitForEdgeActorEvent<TEvents::TEvWakeup>(setup.Client);
    }
}

}

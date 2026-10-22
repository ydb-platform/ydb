#include "distconf_selfheal.h"

#include <ydb/core/util/actorsys_test/testactorsys.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/scope.h>

namespace NKikimr::NStorage {
namespace {

void CheckReassignCancellation(ui32 phase) {
    TTestActorSystem runtime(1);
    runtime.Start();
    Y_DEFER { runtime.Stop(); };

    const TActorId edge = runtime.AllocateEdgeActor(1);
    runtime.RegisterService(MakeBlobStorageNodeWardenID(1), edge);
    const TActorId actor = runtime.Register(new TStateStorageReassignNodeSelfhealActor(
        edge, 123, TDuration::Minutes(1), 1, 2, true, true, true), 1);
    auto disable = runtime.WaitForEdgeActorEvent<TEvNodeConfigInvokeOnRoot>(edge, false);
    UNIT_ASSERT(disable->Get()->Record.GetReassignStateStorageNode().GetDisableRing());

    auto acknowledge = [&] {
        auto reply = std::make_unique<TEvNodeConfigInvokeOnRootResult>();
        reply->Record.SetStatus(NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK);
        runtime.Send(new IEventHandle(actor, edge, reply.release()), 1);
    };
    if (phase >= 1) {
        acknowledge();
    }
    if (phase >= 2) {
        runtime.Send(new IEventHandle(actor, edge, new TEvents::TEvWakeup), 1);
        auto replace = runtime.WaitForEdgeActorEvent<TEvNodeConfigInvokeOnRoot>(edge, false);
        UNIT_ASSERT(!replace->Get()->Record.GetReassignStateStorageNode().GetDisableRing());
    }
    if (phase >= 3) {
        acknowledge();
    }

    // UnbecomeRoot and a superseding selfheal request cancel this actor with
    // Poison. It must die quietly, including after applying the replacement
    // while its final timer is still pending.
    runtime.Send(new IEventHandle(actor, edge, new TEvents::TEvPoison), 1);
    runtime.Send(new IEventHandle(actor, edge, new TEvents::TEvPing,
        IEventHandle::FlagTrackDelivery), 1);
    auto undelivered = runtime.WaitForEdgeActorEvent<TEvents::TEvUndelivered>(edge);
    UNIT_ASSERT_VALUES_EQUAL(undelivered->Get()->SourceType, TEvents::TEvPing::EventType);
    UNIT_ASSERT_VALUES_EQUAL(undelivered->Get()->Reason, TEvents::TEvUndelivered::ReasonActorUnknown);
}

} // namespace

Y_UNIT_TEST_SUITE(TStateStorageReassignSelfhealCancellation) {
    Y_UNIT_TEST(BeforeDisableCompletes) {
        CheckReassignCancellation(0);
    }

    Y_UNIT_TEST(WhileWaitingToReplace) {
        CheckReassignCancellation(1);
    }

    Y_UNIT_TEST(BeforeReplaceCompletes) {
        CheckReassignCancellation(2);
    }

    Y_UNIT_TEST(AfterReplaceCompletes) {
        CheckReassignCancellation(3);
    }
}

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

} // namespace NKikimr::NStorage

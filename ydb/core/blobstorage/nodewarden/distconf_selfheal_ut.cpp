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

} // namespace NKikimr::NStorage

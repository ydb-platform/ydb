#include "dst_alterer.h"
#include "dst_remover.h"

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NReplication::NController {

Y_UNIT_TEST_SUITE(DstDeliveryRetry) {
    void CheckDeliveryRetry(bool alter, bool detach = false) {
        TTestActorRuntime runtime;
        runtime.Initialize(TAppPrepare().Unwrap());
        const auto parent = runtime.AllocateEdgeActor();
        const auto allocator = runtime.AllocateEdgeActor();
        const auto pipeCache = runtime.AllocateEdgeActor();
        runtime.RegisterService(MakeTxProxyID(), allocator);

        const TPathId pathId(1, 1);
        const auto actor = runtime.Register(alter
            ? CreateDstAlterer(parent, pathId.OwnerId, 1, 1,
                TReplication::ETargetKind::Table, pathId, TReplication::EState::Done)
            : CreateDstRemover(parent, pathId.OwnerId, {}, 1, 1,
                TReplication::ETargetKind::Table, pathId, detach ? pathId : TPathId()));

        NTxProxy::TTxProxyServices services;
        services.LeaderPipeCache = pipeCache;
        auto allocate = [&](ui64 txId) {
            runtime.GrabEdgeEvent<TEvTxUserProxy::TEvAllocateTxId>(allocator);
            runtime.Send(actor, allocator,
                new TEvTxUserProxy::TEvAllocateTxIdResult(txId, services, {}));
            const auto proposal = runtime.GrabEdgeEvent<TEvPipeCache::TEvForward>(pipeCache);
            UNIT_ASSERT_VALUES_EQUAL(proposal->Get()->TabletId, pathId.OwnerId);
            UNIT_ASSERT_VALUES_EQUAL(proposal->Get()->Ev->Type(),
                NSchemeShard::TEvSchemeShard::TEvModifySchemeTransaction::EventType);
        };
        allocate(100);

        runtime.SetScheduledEventFilter([](TTestActorRuntimeBase& runtime,
                TAutoPtr<IEventHandle>&, TDuration delay, TInstant& deadline) {
            deadline = runtime.GetCurrentTime() + delay;
            return false;
        });

        runtime.Send(actor, pipeCache,
            new TEvPipeCache::TEvDeliveryProblem(pathId.OwnerId + 1, false));
        UNIT_ASSERT(runtime.CaptureScheduledEvents().empty());

        runtime.Send(actor, pipeCache,
            new TEvPipeCache::TEvDeliveryProblem(pathId.OwnerId, false));
        const auto scheduled = runtime.CaptureScheduledEvents();
        UNIT_ASSERT_VALUES_EQUAL(scheduled.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(scheduled.begin()->Event->Type, ui32(TEvents::TSystem::Wakeup));
        UNIT_ASSERT_VALUES_EQUAL(scheduled.begin()->Deadline,
            runtime.GetCurrentTime() + TDuration::Seconds(10));

        // Deliver the captured retry and verify that the destination operation is proposed again.
        runtime.Send(actor, parent, new TEvents::TEvWakeup());
        allocate(101);
        runtime.Send(actor, parent, new TEvents::TEvPoison());
    }

    Y_UNIT_TEST(AlterRetriesOwnSchemeShard) {
        CheckDeliveryRetry(true);
    }

    Y_UNIT_TEST(DropRetriesOwnSchemeShard) {
        CheckDeliveryRetry(false);
    }

    Y_UNIT_TEST(DetachRetriesOwnSchemeShard) {
        CheckDeliveryRetry(false, true);
    }
}

}

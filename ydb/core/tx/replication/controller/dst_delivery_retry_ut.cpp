#include "dst_alterer.h"
#include "dst_remover.h"
#include "private_events.h"

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

Y_UNIT_TEST_SUITE(DstAlterRetry) {
    namespace TEvSchemeShard = NSchemeShard::TEvSchemeShard;

    struct TEnv {
        TTestActorRuntime Runtime;
        TActorId Parent;
        TActorId Allocator;
        TActorId PipeCache;
        TActorId Alterer;
        const TPathId PathId{1, 1};

        TEnv() {
            Runtime.Initialize(TAppPrepare().Unwrap());
            Parent = Runtime.AllocateEdgeActor();
            Allocator = Runtime.AllocateEdgeActor();
            PipeCache = Runtime.AllocateEdgeActor();
            Runtime.RegisterService(MakeTxProxyID(), Allocator);
            auto* alterer = CreateDstAlterer(
                Parent,
                PathId.OwnerId,
                1,
                1,
                TReplication::ETargetKind::Table,
                PathId,
                TReplication::EState::Done);
            Alterer = Runtime.Register(alterer);
            Runtime.SetScheduledEventFilter([](
                TTestActorRuntimeBase& runtime,
                TAutoPtr<IEventHandle>&,
                TDuration delay,
                TInstant& deadline)
            {
                deadline = runtime.GetCurrentTime() + delay;
                return false;
            });
            Allocate(100);
        }

        void Allocate(ui64 txId) {
            Runtime.GrabEdgeEvent<TEvTxUserProxy::TEvAllocateTxId>(Allocator);
            NTxProxy::TTxProxyServices services;
            services.LeaderPipeCache = PipeCache;
            Runtime.Send(
                Alterer,
                Allocator,
                new TEvTxUserProxy::TEvAllocateTxIdResult(txId, services, {}));
            const auto proposal = Runtime.GrabEdgeEvent<TEvPipeCache::TEvForward>(PipeCache);
            UNIT_ASSERT_VALUES_EQUAL(
                proposal->Get()->Ev->Type(),
                TEvSchemeShard::TEvModifySchemeTransaction::EventType);
            const auto* request = static_cast<TEvSchemeShard::TEvModifySchemeTransaction*>(
                proposal->Get()->Ev.Get());
            UNIT_ASSERT_VALUES_EQUAL(request->Record.GetTxId(), txId);
        }

        void Reply(NKikimrScheme::EStatus status, ui64 txId = 100) {
            auto* response = new TEvSchemeShard::TEvModifySchemeTransactionResult(
                status,
                txId,
                PathId.OwnerId,
                "injected rejection");
            Runtime.Send(Alterer, PipeCache, response);
        }

        void Complete(ui64 txId) {
            Runtime.Send(Alterer, PipeCache, new TEvSchemeShard::TEvNotifyTxCompletionResult(txId));
        }
    };

    Y_UNIT_TEST(RetryTransientRejectionIgnoresObsoleteReplies) {
        const auto statuses = {NKikimrScheme::StatusMultipleModifications, NKikimrScheme::StatusResourceExhausted};
        for (const auto status : statuses) {
            TEnv env;
            env.Reply(status);
            const auto scheduled = env.Runtime.CaptureScheduledEvents();
            UNIT_ASSERT_VALUES_EQUAL(scheduled.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(
                scheduled.begin()->Deadline,
                env.Runtime.GetCurrentTime() + TDuration::Seconds(10));

            // These replies belong to the rejected attempt. A second delivery
            // failure must not create another timer during its backoff either.
            env.Complete(100);
            env.Reply(NKikimrScheme::StatusAccepted);
            env.Runtime.Send(
                env.Alterer,
                env.PipeCache,
                new TEvPipeCache::TEvDeliveryProblem(env.PathId.OwnerId, false));
            UNIT_ASSERT(env.Runtime.CaptureScheduledEvents().empty());
            env.Runtime.Send(env.Alterer, env.Parent, new TEvents::TEvWakeup());
            env.Allocate(101);

            env.Reply(NKikimrScheme::StatusAccepted);
            env.Reply(NKikimrScheme::StatusAccessDenied);
            env.Complete(100);
            // Even the current transaction cannot complete before acceptance.
            env.Complete(101);
            env.Reply(NKikimrScheme::StatusAccepted, 101);
            const auto subscribe = env.Runtime.GrabEdgeEvent<TEvPipeCache::TEvForward>(env.PipeCache);
            UNIT_ASSERT_VALUES_EQUAL(
                subscribe->Get()->Ev->Type(),
                TEvSchemeShard::TEvNotifyTxCompletion::EventType);
            const auto* request = static_cast<TEvSchemeShard::TEvNotifyTxCompletion*>(subscribe->Get()->Ev.Get());
            UNIT_ASSERT_VALUES_EQUAL(request->Record.GetTxId(), 101);
            env.Complete(100);
            UNIT_ASSERT_C(env.Runtime.FindActor(env.Alterer), "Obsolete completion finished the current attempt");
            env.Complete(101);
            UNIT_ASSERT(env.Runtime.GrabEdgeEvent<TEvPrivate::TEvAlterDstResult>(env.Parent)->Get()->IsSuccess());
        }
    }

    Y_UNIT_TEST(PermanentRejectionRemainsError) {
        TEnv env;
        env.Reply(NKikimrScheme::StatusAccessDenied);
        const auto result = env.Runtime.GrabEdgeEvent<TEvPrivate::TEvAlterDstResult>(env.Parent);
        UNIT_ASSERT(!result->Get()->IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, NKikimrScheme::StatusAccessDenied);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Error, "injected rejection");
        UNIT_ASSERT(env.Runtime.CaptureScheduledEvents().empty());
    }
}

Y_UNIT_TEST_SUITE(DstRemoval) {
    Y_UNIT_TEST(DetachAfterDestinationNameConflict) {
        for (const auto status : {NKikimrScheme::StatusPathDoesNotExist,
                NKikimrScheme::StatusSuccess, NKikimrScheme::StatusSchemeError}) {
            TTestActorRuntime runtime;
            runtime.Initialize(TAppPrepare().Unwrap());
            const auto parent = runtime.AllocateEdgeActor();
            const auto allocator = runtime.AllocateEdgeActor();
            const auto pipeCache = runtime.AllocateEdgeActor();
            runtime.RegisterService(MakeTxProxyID(), allocator);
            const TPathId pendingPathId(1, 2);
            const auto remover = runtime.Register(CreateDstRemover(
                parent,
                pendingPathId.OwnerId,
                {},
                1,
                1,
                TReplication::ETargetKind::Table,
                {},
                pendingPathId));

            runtime.GrabEdgeEvent<TEvTxUserProxy::TEvAllocateTxId>(allocator);
            NTxProxy::TTxProxyServices services;
            services.LeaderPipeCache = pipeCache;
            runtime.Send(
                remover,
                allocator,
                new TEvTxUserProxy::TEvAllocateTxIdResult(100, services, {}));
            const auto alter = runtime.GrabEdgeEvent<TEvPipeCache::TEvForward>(pipeCache);
            UNIT_ASSERT_VALUES_EQUAL(
                alter->Get()->Ev->Type(),
                NSchemeShard::TEvSchemeShard::TEvModifySchemeTransaction::EventType);
            const TString error = "path is empty";
            runtime.Send(
                remover,
                pipeCache,
                new NSchemeShard::TEvSchemeShard::TEvModifySchemeTransactionResult(
                    NKikimrScheme::StatusNameConflict,
                    100,
                    pendingPathId.OwnerId,
                    error));

            const auto describe = runtime.GrabEdgeEvent<TEvPipeCache::TEvForward>(pipeCache);
            UNIT_ASSERT_VALUES_EQUAL(describe->Get()->TabletId, pendingPathId.OwnerId);
            UNIT_ASSERT_VALUES_EQUAL(
                describe->Get()->Ev->Type(),
                NSchemeShard::TEvSchemeShard::TEvDescribeScheme::EventType);
            const auto& request = static_cast<NSchemeShard::TEvSchemeShard::TEvDescribeScheme*>(
                describe->Get()->Ev.Get())->Record;
            UNIT_ASSERT_VALUES_EQUAL(request.GetSchemeshardId(), pendingPathId.OwnerId);
            UNIT_ASSERT_VALUES_EQUAL(request.GetPathId(), pendingPathId.LocalPathId);
            auto description = MakeHolder<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResultBuilder>();
            description->Record.SetStatus(status);
            runtime.Send(remover, pipeCache, description.Release());

            const auto result = runtime.GrabEdgeEvent<TEvPrivate::TEvDropDstResult>(parent);
            UNIT_ASSERT_VALUES_EQUAL(
                result->Get()->IsSuccess(),
                status == NKikimrScheme::StatusPathDoesNotExist);
            if (!result->Get()->IsSuccess()) {
                UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, NKikimrScheme::StatusNameConflict);
                UNIT_ASSERT_VALUES_EQUAL(result->Get()->Error, error);
            }
        }
    }
}

}

#include "controller_impl.h"

#include <ydb/core/base/counters.h>
#include <ydb/core/tablet_flat/shared_cache_counters.h>
#include <ydb/core/tablet_flat/shared_cache_events.h>
#include <ydb/core/tablet_flat/shared_sausagecache.h>
#include <ydb/core/tx/replication/ut_helpers/mock_service.h>
#include <ydb/core/tx/replication/ut_helpers/test_env.h>
#include <ydb/core/tx/replication/ut_helpers/test_table.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NReplication::NController {
namespace {

// Materialize a cold Workers row without registering the corresponding worker
// in memory. RunWorker must still execute its real Select and fault/retry.
class TTxColdWorker: public TController::TTxBase {
    const TWorkerId Worker;
    const TActorId Notify;
public:
    TTxColdWorker(TController* self, TWorkerId worker, TActorId notify)
        : TTxBase("ColdWorker", self), Worker(worker), Notify(notify) {}

    bool Execute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext&) override {
        NIceDb::TNiceDb db(txc.DB);
        // A one-page part can keep its data in the embedded B-tree root.
        // Padding creates ordinary evictable data pages; these fixture rows
        // are not registered as live workers and never enter the quorum.
        for (ui64 offset = 0; offset <= 8192; ++offset) {
            db.Table<TControllerSchema::Workers>().Key(Worker.ReplicationId(), Worker.TargetId(), Worker.WorkerId() + offset).Update(
                NIceDb::TUpdate<TControllerSchema::Workers::HeartbeatVersionStep>(0),
                NIceDb::TUpdate<TControllerSchema::Workers::HeartbeatVersionTxId>(0));
        }
        return true;
    }
    void Complete(const TActorContext& ctx) override {
        ctx.Send(Notify, new TEvents::TEvWakeup());
    }
};

} // namespace

Y_UNIT_TEST_SUITE(ReplicationApplyProgressPageFault) {
    Y_UNIT_TEST(RegistrationFaultDoesNotFormPartialCommitQuorum) {
        using namespace NTestHelpers;
        using TProgress = Ydb::Replication::ReplicationApplyProgress;
        TEnv<> env(true, ERuntimeMode::Simulated);
        auto& runtime = env.GetRuntime();
        runtime.RegisterService(MakeReplicationServiceId(runtime.GetNodeId(0)),
            runtime.Register(CreateReplicationMockService(env.GetSender())));
        TVector<TEvService::TEvRunWorker::TPtr> registrations;
        TVector<TEvPrivate::TEvCompleteWorkerSet::TPtr> enumerations;
        bool holdRegistration = true;
        bool holdEnumeration = true;
        auto registrationObserver = runtime.AddObserver<TEvService::TEvRunWorker>([&](auto& ev) {
            if (holdRegistration && dynamic_cast<TController*>(runtime.FindActor(ev->GetRecipientRewrite()))) {
                registrations.push_back(std::move(ev));
            }
        });
        auto enumerationObserver = runtime.AddObserver([&](IEventHandle::TPtr& ev) {
            if (holdEnumeration && ev->HasEvent()
                && dynamic_cast<TEvPrivate::TEvCompleteWorkerSet*>(ev->GetBase()))
            {
                enumerations.emplace_back(static_cast<TEventHandle<TEvPrivate::TEvCompleteWorkerSet>*>(ev.Release()));
            }
        });
        for (const TString& name : {TString("one"), TString("two")}) {
            env.CreateTable("/Root", *MakeTableDescription(TTestTableDescription{
                .Name = name, .KeyColumns = {"key"},
                .Columns = {{.Name = "key", .Type = "Uint32"}},
                .ReplicationConfig = Nothing(),
            }));
        }
        env.RunCall([&] {
            NYdb::NTable::TTableClient client(env.GetDriver());
            auto session = client.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(TStringBuilder()
                << "CREATE ASYNC REPLICATION replication FOR `/Root/one` AS `/Root/r1`, `/Root/two` AS `/Root/r2` "
                << "WITH (CONNECTION_STRING = \"grpc://" << env.GetEndpoint()
                << "/?database=/Root\", CONSISTENCY_LEVEL = \"GLOBAL\", COMMIT_INTERVAL = Interval(\"PT10S\"));")
                .GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        });
        auto handshake = runtime.GrabEdgeEvent<TEvService::TEvHandshake>(env.GetSender());
        const auto controllerId = handshake->Sender;
        auto* controller = dynamic_cast<TController*>(runtime.FindActor(controllerId));
        UNIT_ASSERT(controller);
        auto* executor = dynamic_cast<NTabletFlatExecutor::NFlatExecutorSetup::IExecutor*>(runtime.FindActor(controller->ExecutorID()));
        UNIT_ASSERT(executor);
        env.SendAsync(controllerId, new TEvService::TEvStatus());
        runtime.WaitFor("two pending registrations and enumerations", [&] {
            return registrations.size() == 2 && enumerations.size() == 2;
        });
        const auto first = registrations[0]->Get()->Record.GetWorker();
        const auto second = registrations[1]->Get()->Record.GetWorker();
        UNIT_ASSERT(first.GetTargetId() != second.GetTargetId());
        const auto pathId = env.GetPathId("/Root/replication");
        const auto describe = [&] {
            auto request = new TEvController::TEvDescribeReplication();
            pathId.ToProto(request->Record.MutablePathId());
            request->Record.MutableRequestedAppliedVersion()->set_step(10000);
            return env.Send<TEvController::TEvDescribeReplicationResult>(controllerId, request)->Get()->Record.GetApplyProgress();
        };
        const auto heartbeat = [&](const auto& worker) {
            auto event = new TEvService::TEvHeartbeat(TRowVersion(10000, 0));
            event->Record.MutableWorker()->CopyFrom(worker);
            env.SendAsync(controllerId, event);
        };
        holdRegistration = false;
        runtime.Send(registrations[0].Release());
        runtime.GrabEdgeEvent<TEvService::TEvRunWorker>(env.GetSender());
        auto ids = env.Send<TEvService::TEvTxIdResult>(controllerId,
            new TEvService::TEvGetTxId(TVector<TRowVersion>{TRowVersion(1, 0)}));
        const auto writeTxId = ids->Get()->Record.GetVersionTxIds(0).GetTxId();
        heartbeat(first);
        UNIT_ASSERT_VALUES_EQUAL(describe().workers_at_boundary(), 1);

        // Simulated runtime has no concurrent actor execution. Submit the test
        // transaction under an activation context; the executor owns its retry.
        const auto txDone = runtime.AllocateEdgeActor();
        runtime.Register(new TFunctorActor([&] {
            executor->Execute(new TTxColdWorker(controller, TWorkerId::Parse(second), txDone), TActivationContext::AsActorContext());
        }, env.GetSender()));
        runtime.GrabEdgeEvent<TEvents::TEvWakeup>(txDone);
        runtime.GrabEdgeEvent<TEvents::TEvWakeup>(env.GetSender());
        ui64 compaction = 0;
        runtime.Register(new TFunctorActor([&] {
            compaction = executor->CompactTable(TControllerSchema::Workers::TableId);
        }, env.GetSender()));
        runtime.GrabEdgeEvent<TEvents::TEvWakeup>(env.GetSender());
        UNIT_ASSERT(compaction);
        runtime.WaitFor("Workers compaction", [&] {
            return executor->GetFinishedCompactionInfo(TControllerSchema::Workers::TableId).Edge >= compaction;
        });

        ui64 droppedPages = 0;
        auto dropObserver = runtime.AddObserver<NSharedCache::TEvUpdated>([&](auto& ev) {
            if (ev->GetRecipientRewrite() == controller->ExecutorID()) {
                for (const auto& [collection, pages] : ev->Get()->DroppedPages) {
                    if (collection.TabletID() == controller->TabletID()) {
                        droppedPages += pages.size();
                    }
                }
            }
        });
        NSharedCache::TSharedPageCacheCounters cacheCounters(
            GetServiceCounters(runtime.GetDynamicCounters(), "tablets")->GetSubgroup("type", "S_CACHE"));
        env.SendAsync(NSharedCache::MakeSharedPageCacheId(), new NMemory::TEvConsumerLimit(0));
        // Limit decreases are incremental. Merely observing the first GC
        // event does not mean the page cache has reached the requested limit.
        runtime.WaitFor("zero cache limit and controller page eviction", [&] {
            return cacheCounters.ActiveLimitBytes->Val() == 0 && droppedPages > 0;
        });

        TVector<NSharedCache::TEvRequest::TPtr> pages;
        bool holdPages = true;
        auto pageObserver = runtime.AddObserver<NSharedCache::TEvRequest>([&](auto& ev) {
            if (holdPages && ev->Get()->PageCollection->Label().TabletID() == controller->TabletID()
                && ev->Cookie == static_cast<ui64>(NSharedCache::ERequestTypeCookie::Transaction))
            {
                pages.push_back(std::move(ev));
            }
        });
        TVector<TEvTxUserProxy::TEvProposeTransaction::TPtr> commits;
        auto commitObserver = runtime.AddObserver<TEvTxUserProxy::TEvProposeTransaction>([&](auto& ev) {
            if (ev->Get()->Record.GetTransaction().HasCommitWrites()) {
                commits.push_back(std::move(ev));
            }
        });
        runtime.Send(registrations[1].Release());
        runtime.WaitFor("RunWorker Select page fault", [&] { return !pages.empty(); });
        holdEnumeration = false;
        for (auto& enumeration : enumerations) {
            runtime.Send(enumeration.Release());
        }
        // Describe Complete is ordered after the enumeration redo, so both
        // fresh rosters are installed before exercising commit selection.
        describe();
        heartbeat(first);
        auto progress = describe();
        UNIT_ASSERT_VALUES_EQUAL_C(ui32(progress.state()), ui32(TProgress::INITIALIZING), progress.DebugString());
        UNIT_ASSERT(!progress.worker_set_complete());
        UNIT_ASSERT_VALUES_EQUAL(progress.expected_workers(), 1);
        UNIT_ASSERT_VALUES_EQUAL(progress.workers_at_boundary(), 1);
        UNIT_ASSERT_VALUES_EQUAL(progress.assigned_transactions(), 1);
        UNIT_ASSERT_C(!progress.commit_in_progress() && commits.empty(), "Partial registration committed before the page fault completed");

        holdPages = false;
        for (auto& page : pages) {
            runtime.Send(page.Release());
        }
        runtime.GrabEdgeEvent<TEvService::TEvRunWorker>(env.GetSender());
        progress = describe();
        UNIT_ASSERT(progress.worker_set_complete());
        UNIT_ASSERT_VALUES_EQUAL(ui32(progress.state()), ui32(TProgress::WAITING_HEARTBEATS));
        heartbeat(second);
        runtime.WaitFor("commit after complete registration", [&] { return commits.size() == 1; });
        UNIT_ASSERT_VALUES_EQUAL(ui32(describe().state()), ui32(TProgress::WAITING_COMMITS));
        auto status = new TEvTxUserProxy::TEvProposeTransactionStatus();
        status->Record.SetStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete);
        runtime.Send(new IEventHandle(controllerId, env.GetSender(), status, 0, writeTxId));
        UNIT_ASSERT_VALUES_EQUAL(ui32(describe().state()), ui32(TProgress::APPLIED));
    }
}
} // namespace NKikimr::NReplication::NController

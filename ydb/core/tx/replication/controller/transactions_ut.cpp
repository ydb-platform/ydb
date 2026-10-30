#include "controller_impl.h"

#include <ydb/core/base/hive.h>
#include <ydb/core/tx/replication/ut_helpers/mock_service.h>
#include <ydb/core/tx/replication/ut_helpers/test_env.h>
#include <ydb/core/tx/replication/ut_helpers/test_table.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/public/api/protos/draft/ydb_replication.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_replication.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/string/printf.h>

namespace NKikimr::NReplication::NController {
namespace {
using namespace NTestHelpers;

// Gate a real actor protocol operation while forwarding unrelated traffic.
class TEventGate: public TActorBootstrapped<TEventGate> {
public:
    TEventGate(TActorId destination, TActorId edge, std::function<bool(IEventHandle&)> predicate)
        : Destination(destination)
        , Edge(edge)
        , Predicate(std::move(predicate))
    {}

    void Bootstrap() { Become(&TThis::StateWork); }

    STATEFN(StateWork) {
        if (ev->GetTypeRewrite() == TEvents::TEvWakeup::EventType) {
            Holding = false;
            for (auto& held : Held) {
                Send(held->Forward(Destination));
            }
            Held.clear();
        } else if (Holding && Predicate(*ev)) {
            Held.emplace_back(ev.Release());
            Send(Edge, new TEvents::TEvWakeup());
        } else {
            Send(ev->Forward(Destination));
        }
    }

private:
    const TActorId Destination;
    const TActorId Edge;
    const std::function<bool(IEventHandle&)> Predicate;
    bool Holding = true;
    TVector<THolder<IEventHandle>> Held;
};

class TDescribeReplyCatcher: public TActorBootstrapped<TDescribeReplyCatcher> {
public:
    explicit TDescribeReplyCatcher(NThreading::TPromise<NKikimrReplication::TEvDescribeReplicationResult> promise)
        : Promise(std::move(promise))
    {}

    void Bootstrap() { Become(&TThis::StateWork); }

    STATEFN(StateWork) {
        if (ev->GetTypeRewrite() == TEvController::TEvDescribeReplicationResult::EventType) {
            Promise.SetValue(ev->Get<TEvController::TEvDescribeReplicationResult>()->Record);
            PassAway();
        }
    }

private:
    NThreading::TPromise<NKikimrReplication::TEvDescribeReplicationResult> Promise;
};

struct TTransactionsEnv: TEnv<> {
    ui64 TabletId;
    TPathId PathId;
    TActorId Controller;
    NKikimrReplication::TWorkerIdentity Worker;

    explicit TTransactionsEnv(bool global = true, ERuntimeMode mode = ERuntimeMode::RealThreads)
        : TEnv<>(true, mode)
    {
        GetRuntime().RegisterService(MakeReplicationServiceId(GetRuntime().GetNodeId(0)),
            GetRuntime().Register(CreateReplicationMockService(GetSender())));
        CreateTable("/Root", *MakeTableDescription(TTestTableDescription{
            .Name = "table", .KeyColumns = {"key"},
            .Columns = {{.Name = "key", .Type = "Uint32"}, {.Name = "value", .Type = "Utf8"}},
            .ReplicationConfig = Nothing(),
        }));
        RunCall([&] {
            NYdb::NTable::TTableClient client(GetDriver());
            auto session = client.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(Sprintf(R"(
                CREATE ASYNC REPLICATION `replication` FOR `/Root/table` AS `/Root/replica`
                WITH (CONNECTION_STRING = "grpc://%s/?database=/Root", %s);
            )", GetEndpoint().c_str(), global
                ? "CONSISTENCY_LEVEL = \"GLOBAL\", COMMIT_INTERVAL = Interval(\"PT10S\")"
                : "CONSISTENCY_LEVEL = \"ROW\"")).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        });
        TabletId = GetDescription("/Root/replication").GetPathDescription().GetReplicationDescription().GetControllerId();
        PathId = GetPathId("/Root/replication");
        Handshake();
        auto run = GetRuntime().GrabEdgeEvent<TEvService::TEvRunWorker>(GetSender());
        Worker = run->Get()->Record.GetWorker();
    }

    void Handshake() {
        auto event = GetRuntime().GrabEdgeEvent<TEvService::TEvHandshake>(GetSender());
        UNIT_ASSERT(event->Sender != Controller);
        Controller = event->Sender;
        SendAsync(Controller, new TEvService::TEvStatus());
    }

    TEvController::TEvDescribeReplication* Request() const {
        auto* request = new TEvController::TEvDescribeReplication();
        PathId.ToProto(request->Record.MutablePathId());
        return request;
    }

    Ydb::Replication::DescribeReplicationResult::Transactions Describe() {
        auto response = Send<TEvController::TEvDescribeReplicationResult>(TabletId, Request());
        const auto& record = response->Get()->Record;
        UNIT_ASSERT_VALUES_EQUAL(ui32(record.GetStatus()), ui32(NKikimrReplication::TEvDescribeReplicationResult::SUCCESS));
        UNIT_ASSERT(record.HasTransactions());
        UNIT_ASSERT(!record.HasApplyStatus());
        return record.GetTransactions();
    }

    size_t Count() {
        return Describe().assigned_size();
    }

    NYdb::NReplication::TReplicationDescription SdkDescription(bool stats) {
        return RunCall([&] {
            NYdb::NReplication::TReplicationClient client(GetDriver());
            auto result = client.DescribeReplication("/Root/replication",
                NYdb::NReplication::TDescribeReplicationSettings().IncludeStats(stats)).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            return result.GetReplicationDescription();
        });
    }

    std::optional<NYdb::NReplication::TReplicationDescription::TTransactions> SdkTransactions(bool stats) {
        return SdkDescription(stats).GetTransactions();
    }

    void ReportActivity(TMaybe<bool> inflight) {
        auto* event = new TEvService::TEvWorkerStatus(
            TWorkerId::Parse(Worker), TDuration::Zero(), inflight);
        SendAsync(Controller, event);
    }

    ui64 Assign(ui64 step) {
        auto result = Send<TEvService::TEvTxIdResult>(TabletId,
            new TEvService::TEvGetTxId(TVector<TRowVersion>{TRowVersion(step, 0)}));
        return result->Get()->Record.GetVersionTxIds(0).GetTxId();
    }

    void Heartbeat() {
        auto* heartbeat = new TEvService::TEvHeartbeat(TRowVersion(10000, 0));
        heartbeat->Record.MutableWorker()->CopyFrom(Worker);
        SendAsync(Controller, heartbeat);
    }

    void CommitStatus(ui64 txId, TEvTxUserProxy::TEvProposeTransactionStatus::EStatus status) {
        auto* event = new TEvTxUserProxy::TEvProposeTransactionStatus();
        event->Record.SetStatus(status);
        GetRuntime().Send(new IEventHandle(Controller, GetSender(), event, 0, txId));
    }

    void HoldCommits() {
        auto& runtime = GetRuntime();
        const auto proxy = runtime.GetLocalServiceId(MakeTxProxyID());
        runtime.RegisterService(MakeTxProxyID(), runtime.Register(new TEventGate(proxy, GetSender(), [](IEventHandle& ev) {
            return ev.GetTypeRewrite() == TEvTxUserProxy::TEvProposeTransaction::EventType
                && ev.Get<TEvTxUserProxy::TEvProposeTransaction>()->Record.GetTransaction().HasCommitWrites();
        })));
    }
};
} // namespace

Y_UNIT_TEST_SUITE(ReplicationTransactions) {
    void Check(TTransactionsEnv& env, const TMap<ui64, ui64>& assigned, const TVector<ui64>& pending = {}) {
        const auto transactions = env.Describe();
        UNIT_ASSERT_VALUES_EQUAL(transactions.assigned_size(), assigned.size());
        UNIT_ASSERT_VALUES_EQUAL(transactions.pending_size(), pending.size());
        size_t i = 0;
        for (const auto& [step, txId] : assigned) {
            const auto& item = transactions.assigned(i++);
            UNIT_ASSERT_VALUES_EQUAL(item.upper_bound_barrier().plan_step(), step);
            UNIT_ASSERT_VALUES_EQUAL(item.upper_bound_barrier().tx_id(), 0);
            UNIT_ASSERT_VALUES_EQUAL(item.write_tx_id(), txId);
        }
        for (i = 0; i < pending.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(transactions.pending(i).plan_step(), pending[i]);
            UNIT_ASSERT_VALUES_EQUAL(transactions.pending(i).tx_id(), 0);
        }
        for (bool stats : {false, true}) {
            const auto result = env.SdkTransactions(stats);
            UNIT_ASSERT(result);
            UNIT_ASSERT_VALUES_EQUAL(result->Assigned.size(), assigned.size());
            UNIT_ASSERT_VALUES_EQUAL(result->Pending.size(), pending.size());
            i = 0;
            for (const auto& [step, txId] : assigned) {
                const auto& item = result->Assigned[i++];
                UNIT_ASSERT_VALUES_EQUAL(item.UpperBoundBarrier.PlanStep, step);
                UNIT_ASSERT_VALUES_EQUAL(item.UpperBoundBarrier.TxId, 0);
                UNIT_ASSERT_VALUES_EQUAL(item.WriteTxId, txId);
            }
            for (i = 0; i < pending.size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL(result->Pending[i].PlanStep, pending[i]);
                UNIT_ASSERT_VALUES_EQUAL(result->Pending[i].TxId, 0);
            }
        }
    }

    Y_UNIT_TEST(ReportsAssignedTransactionsWithAndWithoutStats) {
        TTransactionsEnv env;
        Check(env, {});
        const auto first = env.Assign(1);
        Check(env, {{10000, first}});
        UNIT_ASSERT_VALUES_EQUAL(env.Assign(2), first);
        Check(env, {{10000, first}});
        const auto second = env.Assign(10001);
        UNIT_ASSERT(second != first);
        Check(env, {{10000, first}, {20000, second}});
    }

    Y_UNIT_TEST(ReportsPendingVersionsAndTheirAssignment) {
        TTransactionsEnv env(true, ERuntimeMode::Simulated);
        auto& runtime = env.GetRuntime();
        TVector<TEvTxAllocatorClient::TEvAllocateResult::TPtr> allocations;
        bool hold = true;
        auto observer = runtime.AddObserver<TEvTxAllocatorClient::TEvAllocateResult>([&](auto& ev) {
            if (hold && ev->GetRecipientRewrite() == env.Controller && ev->Sender != env.GetSender()) {
                allocations.push_back(std::move(ev));
            }
        });
        // Unsorted requests, including duplicate versions within an interval.
        env.SendAsync(env.TabletId, new TEvService::TEvGetTxId(TVector<TRowVersion>{
            TRowVersion(20001, 7), TRowVersion(1, 3), TRowVersion(10001, 1), TRowVersion(2, 9),
        }));
        runtime.WaitFor("allocation held", [&] { return allocations.size() == 1; });
        Check(env, {}, {10000, 20000, 30000});
        // Supply just one of the real allocated IDs. The next allocation stays
        // held, exposing assigned and pending entries in the same snapshot.
        const auto firstTxId = allocations[0]->Get()->TxIds.front();
        runtime.Send(new IEventHandle(env.Controller, env.GetSender(),
            new TEvTxAllocatorClient::TEvAllocateResult(firstTxId)));
        runtime.WaitFor("refill held", [&] { return allocations.size() == 2; });
        auto first = runtime.GrabEdgeEvent<TEvService::TEvTxIdResult>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(first->Get()->Record.VersionTxIdsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(first->Get()->Record.GetVersionTxIds(0).GetTxId(), firstTxId);
        Check(env, {{10000, firstTxId}}, {20000, 30000});
        hold = false;
        runtime.Send(allocations[1].Release());
        auto assigned = runtime.GrabEdgeEvent<TEvService::TEvTxIdResult>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(assigned->Get()->Record.VersionTxIdsSize(), 2);
        TMap<ui64, ui64> expected{{10000, firstTxId}};
        for (const auto& item : assigned->Get()->Record.GetVersionTxIds()) {
            expected.emplace(item.GetVersion().GetStep(), item.GetTxId());
        }
        Check(env, expected);
    }

    Y_UNIT_TEST(RowConsistencyDoesNotReportGlobalTransactions) {
        TTransactionsEnv env(false);
        auto response = env.Send<TEvController::TEvDescribeReplicationResult>(env.TabletId, env.Request());
        UNIT_ASSERT(!response->Get()->Record.HasTransactions());
        UNIT_ASSERT(!env.SdkTransactions(false));
        UNIT_ASSERT(!env.SdkTransactions(true));
    }

    void CheckApplyStatus(TTransactionsEnv& env, ui64 workers, ui64 inflight, ui64 unknown) {
        auto response = env.Send<TEvController::TEvDescribeReplicationResult>(env.TabletId, env.Request());
        const auto& record = response->Get()->Record;
        UNIT_ASSERT(!record.HasTransactions());
        UNIT_ASSERT(record.HasApplyStatus());
        const auto& status = record.GetApplyStatus();
        UNIT_ASSERT_VALUES_EQUAL(status.workers(), workers);
        UNIT_ASSERT_VALUES_EQUAL(status.inflight_workers(), inflight);
        UNIT_ASSERT_VALUES_EQUAL(status.unknown_workers(), unknown);
        for (const bool stats : {false, true}) {
            const auto description = env.SdkDescription(stats);
            UNIT_ASSERT(!description.GetTransactions());
            UNIT_ASSERT(description.GetApplyStatus());
            const auto& sample = *description.GetApplyStatus();
            UNIT_ASSERT_VALUES_EQUAL(sample.Workers, workers);
            UNIT_ASSERT_VALUES_EQUAL(sample.InflightWorkers, inflight);
            UNIT_ASSERT_VALUES_EQUAL(sample.UnknownWorkers, unknown);
        }
    }

    Y_UNIT_TEST(RowActivityReportsBusyIdleAndMissingSamples) {
        TTransactionsEnv env(false);
        CheckApplyStatus(env, 1, 0, 1);
        env.ReportActivity(true);
        CheckApplyStatus(env, 1, 1, 0);
        // A duplicate sample is not another unfinished batch.
        env.ReportActivity(true);
        CheckApplyStatus(env, 1, 1, 0);
        env.ReportActivity(false);
        CheckApplyStatus(env, 1, 0, 0);
        // Older services report lag without activity information.
        env.ReportActivity(Nothing());
        CheckApplyStatus(env, 1, 0, 1);
        env.ReportActivity(true);
        CheckApplyStatus(env, 1, 1, 0);
        env.SendAsync(env.TabletId, new TEvents::TEvPoisonPill());
        env.Handshake();
        env.GetRuntime().GrabEdgeEvent<TEvService::TEvRunWorker>(env.GetSender());
        CheckApplyStatus(env, 1, 0, 1);
        env.ReportActivity(false);
        CheckApplyStatus(env, 1, 0, 0);
    }

    Y_UNIT_TEST(RowActivityExpiresWithoutReports) {
        TTransactionsEnv env(false, ERuntimeMode::Simulated);
        env.ReportActivity(false);
        CheckApplyStatus(env, 1, 0, 0);
        env.GetRuntime().SimulateSleep(TDuration::Seconds(61));
        CheckApplyStatus(env, 1, 0, 1);
        env.ReportActivity(true);
        CheckApplyStatus(env, 1, 1, 0);
    }

    Y_UNIT_TEST(SdkPreservesApplyStatusPresence) {
        Ydb::Replication::DescribeReplicationResult proto;
        UNIT_ASSERT(!NYdb::NReplication::TReplicationDescription(proto).GetApplyStatus());
        auto* status = proto.mutable_apply_status();
        const auto empty = NYdb::NReplication::TReplicationDescription(proto).GetApplyStatus();
        UNIT_ASSERT(empty);
        UNIT_ASSERT_VALUES_EQUAL(empty->Workers, 0);
        UNIT_ASSERT_VALUES_EQUAL(empty->InflightWorkers, 0);
        UNIT_ASSERT_VALUES_EQUAL(empty->UnknownWorkers, 0);
        status->set_workers(7);
        status->set_inflight_workers(3);
        status->set_unknown_workers(2);
        const auto result = NYdb::NReplication::TReplicationDescription(proto).GetApplyStatus();
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(result->Workers, 7);
        UNIT_ASSERT_VALUES_EQUAL(result->InflightWorkers, 3);
        UNIT_ASSERT_VALUES_EQUAL(result->UnknownWorkers, 2);
    }

    Y_UNIT_TEST(CommitInProgressAndRetryRemainCounted) {
        TTransactionsEnv env;
        const auto txId = env.Assign(1);
        env.HoldCommits();
        env.Heartbeat();
        env.GetRuntime().GrabEdgeEvent<TEvents::TEvWakeup>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(env.Count(), 1);
        env.CommitStatus(txId, TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecError);
        UNIT_ASSERT_VALUES_EQUAL(env.Count(), 1);
        env.GetRuntime().GrabEdgeEvent<TEvents::TEvWakeup>(env.GetSender());
        env.CommitStatus(txId, TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete);
        UNIT_ASSERT_VALUES_EQUAL(env.Count(), 0);
        UNIT_ASSERT_VALUES_EQUAL(env.SdkTransactions(true).value().Assigned.size(), 0);
    }

    Y_UNIT_TEST(RestoresAssignedTransactionsAfterRestart) {
        TTransactionsEnv env;
        const auto first = env.Assign(1);
        const auto second = env.Assign(10001);
        Check(env, {{10000, first}, {20000, second}});
        env.SendAsync(env.TabletId, new TEvents::TEvPoisonPill());
        env.Handshake();
        // Stored interval versions and destination IDs survive restart unchanged.
        Check(env, {{10000, first}, {20000, second}});
    }

    void CheckDeletionPersistence(bool restart) {
        TTransactionsEnv env;
        auto& runtime = env.GetRuntime();
        const auto txId = env.Assign(1);
        env.HoldCommits();
        env.Heartbeat();
        runtime.GrabEdgeEvent<TEvents::TEvWakeup>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(env.Count(), 1);
        auto info = env.Send<TEvHive::TEvGetTabletStorageInfoResult>(Tests::Hive,
            new TEvHive::TEvGetTabletStorageInfo(env.TabletId));
        UNIT_ASSERT_VALUES_EQUAL(info->Get()->Record.GetStatus(), NKikimrProto::OK);
        const auto& channel = info->Get()->Record.GetInfo().GetChannels(0);
        const auto service = MakeBlobStorageProxyID(channel.GetHistory(channel.HistorySize() - 1).GetGroupID());
        const auto original = runtime.GetLocalServiceId(service);
        const auto tabletId = env.TabletId;
        const auto gate = runtime.Register(new TEventGate(original, env.GetSender(), [tabletId](IEventHandle& ev) {
            return ev.GetTypeRewrite() == TEvBlobStorage::TEvPut::EventType
                && ev.Get<TEvBlobStorage::TEvPut>()->Id.TabletID() == tabletId;
        }));
        runtime.RegisterService(service, gate);
        env.CommitStatus(txId, TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete);
        runtime.GrabEdgeEvent<TEvents::TEvWakeup>(env.GetSender());
        auto promise = NThreading::NewPromise<NKikimrReplication::TEvDescribeReplicationResult>();
        auto future = promise.GetFuture();
        runtime.Send(new IEventHandle(env.Controller, runtime.Register(new TDescribeReplyCatcher(promise)), env.Request()));
        UNIT_ASSERT_C(!future.Wait(TDuration::MilliSeconds(100)), "Describe returned zero before TxIds deletion was durable");
        runtime.RegisterService(service, original);
        if (restart) {
            // Discard the old generation's unsaved deletion by keeping its Put held.
            env.SendAsync(env.TabletId, new TEvents::TEvPoisonPill());
            env.Handshake();
            UNIT_ASSERT_VALUES_EQUAL(env.Count(), 1);
        } else {
            env.SendAsync(gate, new TEvents::TEvWakeup());
            UNIT_ASSERT(future.Wait(TDuration::Seconds(30)));
            const auto result = future.GetValueSync();
            UNIT_ASSERT(result.HasTransactions());
            UNIT_ASSERT_VALUES_EQUAL(result.GetTransactions().assigned_size(), 0);
            env.SendAsync(env.TabletId, new TEvents::TEvPoisonPill());
            env.Handshake();
            UNIT_ASSERT_VALUES_EQUAL(env.Count(), 0);
        }
    }

    Y_UNIT_TEST(ZeroWaitsForDurableDeletion) { CheckDeletionPersistence(false); }
    Y_UNIT_TEST(RestartBeforeDeletionRestoresTransaction) { CheckDeletionPersistence(true); }

    Y_UNIT_TEST(SdkPreservesMissingEmptyAndPopulatedLists) {
        Ydb::Replication::DescribeReplicationResult proto;
        UNIT_ASSERT(!NYdb::NReplication::TReplicationDescription(proto).GetTransactions());
        proto.mutable_transactions();
        const auto empty = NYdb::NReplication::TReplicationDescription(proto).GetTransactions();
        UNIT_ASSERT(empty);
        UNIT_ASSERT(empty->Assigned.empty());
        UNIT_ASSERT(empty->Pending.empty());
        auto* item = proto.mutable_transactions()->add_assigned();
        item->mutable_upper_bound_barrier()->set_plan_step(10000);
        item->mutable_upper_bound_barrier()->set_tx_id(42);
        item->set_write_tx_id(123);
        auto* pending = proto.mutable_transactions()->add_pending();
        pending->set_plan_step(20000);
        pending->set_tx_id(43);
        const auto result = NYdb::NReplication::TReplicationDescription(proto).GetTransactions();
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(result->Assigned.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(result->Assigned[0].UpperBoundBarrier.PlanStep, 10000);
        UNIT_ASSERT_VALUES_EQUAL(result->Assigned[0].UpperBoundBarrier.TxId, 42);
        UNIT_ASSERT_VALUES_EQUAL(result->Assigned[0].WriteTxId, 123);
        UNIT_ASSERT_VALUES_EQUAL(result->Pending.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(result->Pending[0].PlanStep, 20000);
        UNIT_ASSERT_VALUES_EQUAL(result->Pending[0].TxId, 43);
    }
}
} // namespace NKikimr::NReplication::NController

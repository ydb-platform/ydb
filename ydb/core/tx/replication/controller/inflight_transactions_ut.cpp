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

struct TInflightEnv: TEnv<> {
    ui64 TabletId;
    TPathId PathId;
    TActorId Controller;
    NKikimrReplication::TWorkerIdentity Worker;

    explicit TInflightEnv(bool global = true) {
        GetRuntime().RegisterService(MakeReplicationServiceId(GetRuntime().GetNodeId(0)),
            GetRuntime().Register(CreateReplicationMockService(GetSender())));
        CreateTable("/Root", *MakeTableDescription(TTestTableDescription{
            .Name = "table", .KeyColumns = {"key"},
            .Columns = {{.Name = "key", .Type = "Uint32"}, {.Name = "value", .Type = "Utf8"}},
            .ReplicationConfig = Nothing(),
        }));
        NYdb::NTable::TTableClient client(GetDriver());
        auto session = client.CreateSession().GetValueSync().GetSession();
        auto result = session.ExecuteSchemeQuery(Sprintf(R"(
            CREATE ASYNC REPLICATION `replication` FOR `/Root/table` AS `/Root/replica`
            WITH (CONNECTION_STRING = "grpc://%s/?database=/Root", %s);
        )", GetEndpoint().c_str(), global
            ? "CONSISTENCY_LEVEL = \"GLOBAL\", COMMIT_INTERVAL = Interval(\"PT10S\")"
            : "CONSISTENCY_LEVEL = \"ROW\"")).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
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

    ui64 Count() {
        auto response = Send<TEvController::TEvDescribeReplicationResult>(TabletId, Request());
        const auto& record = response->Get()->Record;
        UNIT_ASSERT_VALUES_EQUAL(ui32(record.GetStatus()), ui32(NKikimrReplication::TEvDescribeReplicationResult::SUCCESS));
        UNIT_ASSERT(record.HasInflightTransactions());
        return record.GetInflightTransactions();
    }

    std::optional<uint64_t> SdkCount(bool stats) {
        NYdb::NReplication::TReplicationClient client(GetDriver());
        auto result = client.DescribeReplication("/Root/replication",
            NYdb::NReplication::TDescribeReplicationSettings().IncludeStats(stats)).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        return result.GetReplicationDescription().GetInflightTransactions();
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

Y_UNIT_TEST_SUITE(ReplicationInflightTransactions) {
    Y_UNIT_TEST(ReportsAssignedTransactionsWithAndWithoutStats) {
        TInflightEnv env;
        UNIT_ASSERT_VALUES_EQUAL(env.Count(), 0);
        UNIT_ASSERT_VALUES_EQUAL(env.SdkCount(false).value(), 0);
        const auto first = env.Assign(1);
        UNIT_ASSERT_VALUES_EQUAL(env.Count(), 1);
        UNIT_ASSERT_VALUES_EQUAL(env.Assign(2), first);
        UNIT_ASSERT_VALUES_EQUAL(env.Count(), 1);
        UNIT_ASSERT(env.Assign(10001) != first);
        UNIT_ASSERT_VALUES_EQUAL(env.Count(), 2);
        UNIT_ASSERT_VALUES_EQUAL(env.SdkCount(false).value(), 2);
        UNIT_ASSERT_VALUES_EQUAL(env.SdkCount(true).value(), 2);
    }

    Y_UNIT_TEST(RowConsistencyDoesNotReportGlobalTransactions) {
        TInflightEnv env(false);
        auto response = env.Send<TEvController::TEvDescribeReplicationResult>(env.TabletId, env.Request());
        UNIT_ASSERT(!response->Get()->Record.HasInflightTransactions());
        UNIT_ASSERT(!env.SdkCount(false));
        UNIT_ASSERT(!env.SdkCount(true));
    }

    Y_UNIT_TEST(CommitInProgressAndRetryRemainCounted) {
        TInflightEnv env;
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
        UNIT_ASSERT_VALUES_EQUAL(env.SdkCount(true).value(), 0);
    }

    Y_UNIT_TEST(RestoresAssignedTransactionsAfterRestart) {
        TInflightEnv env;
        env.Assign(1);
        env.Assign(10001);
        UNIT_ASSERT_VALUES_EQUAL(env.Count(), 2);
        env.SendAsync(env.TabletId, new TEvents::TEvPoisonPill());
        env.Handshake();
        // No heartbeat or fresh worker enumeration is needed to count stored TxIds.
        UNIT_ASSERT_VALUES_EQUAL(env.Count(), 2);
        UNIT_ASSERT_VALUES_EQUAL(env.SdkCount(false).value(), 2);
    }

    void CheckDeletionPersistence(bool restart) {
        TInflightEnv env;
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
            UNIT_ASSERT(result.HasInflightTransactions());
            UNIT_ASSERT_VALUES_EQUAL(result.GetInflightTransactions(), 0);
            env.SendAsync(env.TabletId, new TEvents::TEvPoisonPill());
            env.Handshake();
            UNIT_ASSERT_VALUES_EQUAL(env.Count(), 0);
        }
    }

    Y_UNIT_TEST(ZeroWaitsForDurableDeletion) { CheckDeletionPersistence(false); }
    Y_UNIT_TEST(RestartBeforeDeletionRestoresTransaction) { CheckDeletionPersistence(true); }

    Y_UNIT_TEST(SdkPreservesMissingZeroAndNonzero) {
        Ydb::Replication::DescribeReplicationResult proto;
        UNIT_ASSERT(!NYdb::NReplication::TReplicationDescription(proto).GetInflightTransactions());
        proto.set_inflight_transactions(0);
        UNIT_ASSERT_VALUES_EQUAL(NYdb::NReplication::TReplicationDescription(proto).GetInflightTransactions().value(), 0);
        proto.set_inflight_transactions(3);
        UNIT_ASSERT_VALUES_EQUAL(NYdb::NReplication::TReplicationDescription(proto).GetInflightTransactions().value(), 3);
    }
}
} // namespace NKikimr::NReplication::NController

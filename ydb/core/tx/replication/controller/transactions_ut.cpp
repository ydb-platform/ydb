#include "controller_impl.h"

#include <ydb/core/base/hive.h>
#include <ydb/core/tx/replication/ut_helpers/mock_service.h>
#include <ydb/core/tx/replication/ut_helpers/test_env.h>
#include <ydb/core/tx/replication/ut_helpers/test_table.h>
#include <ydb/core/tx/replication/private_client/private_client.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/public/api/protos/draft/ydb_replication.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_replication.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/string/hex.h>
#include <util/string/printf.h>
#include <util/generic/guid.h>

namespace NKikimr::NReplication::NController {
namespace {
using namespace NTestHelpers;

Ydb::Replication::StreamChecksumTablesInternalRequest MakeSourceChecksumRequest(bool probeOnly = false) {
    Ydb::Replication::StreamChecksumTablesInternalRequest request;
    auto* table = request.add_tables();
    table->set_id(1);
    table->set_path("/Root/table");
    request.set_probe_only(probeOnly);
    return request;
}

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

    explicit TTransactionsEnv(bool global = true, ERuntimeMode mode = ERuntimeMode::RealThreads, bool periodic = false)
        : TEnv<>(true, mode)
    {
        GetRuntime().GetAppData().FeatureFlags.SetEnableDataShardSnapshotLocking(true);
        GetRuntime().GetAppData().FeatureFlags.SetEnableAsyncReplicationChecksumCheck(true);
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
                WITH (CONNECTION_STRING = "grpc://%s/?database=/Root", %s%s);
            )", GetEndpoint().c_str(), global
                ? "CONSISTENCY_LEVEL = \"GLOBAL\", COMMIT_INTERVAL = Interval(\"PT10S\")"
                : "CONSISTENCY_LEVEL = \"ROW\"", periodic ? ", CHECKSUM_INTERVAL = Interval(\"PT2S\")" : "")).GetValueSync();
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
        auto* status = new TEvService::TEvStatus();
        status->Record.SetSupportsChecksumRanges(true);
        SendAsync(Controller, status);
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
        return record.GetTransactions();
    }

    size_t Count() {
        return Describe().assigned_size();
    }

    std::optional<NYdb::NReplication::TReplicationDescription::TTransactions> SdkTransactions(bool stats) {
        return RunCall([&] {
            NYdb::NReplication::TReplicationClient client(GetDriver());
            auto result = client.DescribeReplication("/Root/replication",
                NYdb::NReplication::TDescribeReplicationSettings().IncludeStats(stats)).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            return result.GetReplicationDescription().GetTransactions();
        });
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

    TString RequestChecksum(TString requestId = {}) {
        if (requestId.empty()) {
            requestId = CreateGuidAsString();
        }
        auto request = MakeHolder<TEvController::TEvCheckTable>();
        GetPathId("/Root/replica").ToProto(request->Record.MutableDestinationPathId());
        request->Record.SetRequestId(requestId);
        SendAsync(TabletId, request.Release());
        return requestId;
    }

    NKikimrReplication::TEvCheckTableResult WaitChecksum() {
        const auto deadline = TInstant::Now() + TDuration::Seconds(45);
        for (;;) {
            auto event = GetRuntime().GrabEdgeEvent<TEvController::TEvCheckTableResult>(GetSender(), TDuration::Seconds(45));
            UNIT_ASSERT(event);
            const auto response = event->Get()->Record;
            if (!response.GetDeferred() || response.GetStatus() != Ydb::StatusIds::OVERLOADED) {
                return response;
            }
            UNIT_ASSERT(TInstant::Now() < deadline);
            RequestChecksum(response.GetRequestId());
        }
    }

    std::optional<NYdb::NReplication::TChecksumStatus> SdkChecksum(bool stats = false) {
        return RunCall([&] {
            NYdb::NReplication::TReplicationClient client(GetDriver());
            auto result = client.DescribeReplication("/Root/replication",
                NYdb::NReplication::TDescribeReplicationSettings().IncludeStats(stats)).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            const auto items = result.GetReplicationDescription().GetItems();
            UNIT_ASSERT_VALUES_EQUAL(items.size(), 1);
            return items.at(0).ChecksumStatus;
        });
    }

    TString ExpectedChecksumHash(const TString& tablePath) {
        return RunCall([&] {
            NYdb::NTable::TTableClient client(GetDriver());
            auto session = client.CreateSession().GetValueSync().GetSession();
            const auto result = session.ExecuteDataQuery(
                Sprintf("CHECKSUM TABLE `%s` WITH (ALGORITHM = \"CityHash64\");", tablePath.c_str()),
                NYdb::NTable::TTxControl::BeginTx(NYdb::NTable::TTxSettings::SnapshotRO()).CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            NYdb::TResultSetParser rows(result.GetResultSet(0));
            UNIT_ASSERT(rows.TryNextRow());
            const auto hash = rows.ColumnParser("hash").GetString();
            UNIT_ASSERT_VALUES_EQUAL(hash.size(), sizeof(ui64));
            UNIT_ASSERT(!rows.TryNextRow());
            return HexEncode(hash.data(), hash.size());
        });
    }

    void ChecksumHeartbeat() {
        auto* heartbeat = new TEvService::TEvHeartbeat(TRowVersion(TInstant::Now().MilliSeconds() + 60000, 0));
        heartbeat->Record.MutableWorker()->CopyFrom(Worker);
        SendAsync(Controller, heartbeat);
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

    Y_UNIT_TEST(ChecksumRejectsRowConsistency) {
        TTransactionsEnv env(false);
        env.RequestChecksum();
        const auto result = env.WaitChecksum();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), Ydb::StatusIds::PRECONDITION_FAILED);
        UNIT_ASSERT(!env.SdkChecksum());
    }

    Y_UNIT_TEST(ChecksumUnknownDestination) {
        TTransactionsEnv env;
        auto request = MakeHolder<TEvController::TEvCheckTable>();
        env.GetPathId("/Root/table").ToProto(request->Record.MutableDestinationPathId());
        request->Record.SetRequestId("unknown-destination");
        env.SendAsync(env.TabletId, request.Release());
        UNIT_ASSERT_VALUES_EQUAL(env.WaitChecksum().GetStatus(), Ydb::StatusIds::SCHEME_ERROR);
        UNIT_ASSERT(!env.SdkChecksum());
    }

    Y_UNIT_TEST(ChecksumBusyAndCancellation) {
        TTransactionsEnv env;
        const auto first = env.RequestChecksum();
        const auto second = env.RequestChecksum();
        const auto busy = env.GetRuntime().GrabEdgeEvent<TEvController::TEvCheckTableResult>(env.GetSender(), TDuration::Seconds(30));
        UNIT_ASSERT(busy);
        UNIT_ASSERT_VALUES_EQUAL(busy->Get()->Record.GetRequestId(), second);
        UNIT_ASSERT_VALUES_EQUAL(busy->Get()->Record.GetStatus(), Ydb::StatusIds::OVERLOADED);
        UNIT_ASSERT(busy->Get()->Record.GetDeferred());
        UNIT_ASSERT(!env.SdkChecksum());
        auto cancel = MakeHolder<TEvController::TEvCancelCheckTable>();
        env.GetPathId("/Root/replica").ToProto(cancel->Record.MutableDestinationPathId());
        cancel->Record.SetRequestId(first);
        env.SendAsync(env.TabletId, cancel.Release());
        const auto cancelled = env.WaitChecksum();
        UNIT_ASSERT_VALUES_EQUAL(cancelled.GetRequestId(), first);
        UNIT_ASSERT_VALUES_EQUAL(cancelled.GetStatus(), Ydb::StatusIds::CANCELLED);
        UNIT_ASSERT(!cancelled.GetChecksumStatus().has_last_result());
    }

    Y_UNIT_TEST(ChecksumWaitsForAdmissionWithoutPausingReplication) {
        TTransactionsEnv env(true, ERuntimeMode::Simulated);
        auto& runtime = env.GetRuntime();
        const auto source = env.GetPathId("/Root/table");
        const auto destination = env.GetPathId("/Root/replica");
        TVector<NSchemeShard::TEvSchemeShard::TEvChecksumAdmissionResult::TPtr> grants;
        bool hold = true;
        auto admissionObserver = runtime.AddObserver<NSchemeShard::TEvSchemeShard::TEvChecksumAdmissionResult>([&](auto& ev) {
            if (hold && ev->GetRecipientRewrite() == env.Controller
                && ev->Get()->Record.GetGrantedShards() && !ev->Get()->Record.GetReleased())
            {
                grants.push_back(std::move(ev));
            }
        });
        ui32 snapshots = 0;
        auto snapshotObserver = runtime.AddObserver<NLongTxService::TEvLongTxService::TEvAcquireReadSnapshot>([&](auto& ev) {
            for (const auto& table : ev->Get()->TableIds) {
                if (table.PathId == source || table.PathId == destination) {
                    ++snapshots;
                }
            }
        });
        env.RequestChecksum();
        runtime.WaitFor("checksum admission held", [&] { return !grants.empty(); });
        UNIT_ASSERT(env.Assign(1));
        UNIT_ASSERT(!env.SdkChecksum());
        UNIT_ASSERT_VALUES_EQUAL(snapshots, 0);

        hold = false;
        for (auto& grant : grants) {
            runtime.Send(grant.Release());
        }
        env.ChecksumHeartbeat();
        const auto result = env.WaitChecksum();
        UNIT_ASSERT_C(result.GetStatus() == Ydb::StatusIds::SUCCESS, result.GetIssue());
        UNIT_ASSERT(result.GetChecksumStatus().last_result().matches());
        UNIT_ASSERT(snapshots >= 2);
    }

    Y_UNIT_TEST(ChecksumExpiredAdmission) {
        TTransactionsEnv env(true, ERuntimeMode::Simulated);
        auto& runtime = env.GetRuntime();
        auto observer = runtime.AddObserver<NSchemeShard::TEvSchemeShard::TEvChecksumAdmissionResult>([&](auto& ev) {
            auto& response = ev->Get()->Record;
            if (response.GetStatus() == Ydb::StatusIds::SUCCESS && response.GetGrantedShards() && !response.GetReleased()) {
                response.SetDeadlineUs(runtime.GetCurrentTime().MicroSeconds());
            }
        });
        env.RequestChecksum();
        const auto result = env.WaitChecksum();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), Ydb::StatusIds::TIMEOUT);
        UNIT_ASSERT(!result.GetChecksumStatus().has_last_result());
        UNIT_ASSERT_VALUES_EQUAL(env.SdkChecksum()->Status, NYdb::EStatus::TIMEOUT);
    }

    Y_UNIT_TEST(ChecksumFeatureFlag) {
        TTransactionsEnv env;
        env.GetRuntime().GetAppData().FeatureFlags.SetEnableAsyncReplicationChecksumCheck(false);
        env.RequestChecksum();
        UNIT_ASSERT_VALUES_EQUAL(env.WaitChecksum().GetStatus(), Ydb::StatusIds::PRECONDITION_FAILED);
        UNIT_ASSERT(!env.SdkChecksum());
        env.RunCall([&] {
            TPrivateClient client(env.GetDriver(), NYdb::TCommonClientSettings().AuthToken("root@builtin"));
            auto stream = client.StreamChecksumTablesInternal(MakeSourceChecksumRequest(), TDuration::Seconds(30)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(stream.ReadNext().ExtractValueSync().GetStatus(), NYdb::EStatus::PRECONDITION_FAILED);
        });
    }

    Y_UNIT_TEST(ChecksumRequiresDataShardSnapshotRetention) {
        TTransactionsEnv env;
        env.GetRuntime().GetAppData().FeatureFlags.SetEnableDataShardSnapshotLocking(false);
        env.RequestChecksum();
        UNIT_ASSERT_VALUES_EQUAL(env.WaitChecksum().GetStatus(), Ydb::StatusIds::PRECONDITION_FAILED);
        env.RunCall([&] {
            TPrivateClient client(env.GetDriver(), NYdb::TCommonClientSettings().AuthToken("root@builtin"));
            auto stream = client.StreamChecksumTablesInternal(MakeSourceChecksumRequest(), TDuration::Seconds(30)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(stream.ReadNext().ExtractValueSync().GetStatus(), NYdb::EStatus::PRECONDITION_FAILED);
        });
    }

    Y_UNIT_TEST(ChecksumSourceStreamEmptyTable) {
        TTransactionsEnv env;
        const auto expectedHash = env.ExpectedChecksumHash("/Root/table");
        env.RunCall([&] {
            TPrivateClient client(env.GetDriver(), NYdb::TCommonClientSettings().AuthToken("root@builtin"));
            auto stream = client.StreamChecksumTablesInternal(MakeSourceChecksumRequest(), TDuration::Seconds(30)).ExtractValueSync();
            UNIT_ASSERT_C(stream.IsSuccess(), stream.GetIssues().ToString());
            auto snapshot = stream.ReadNext().ExtractValueSync();
            UNIT_ASSERT_C(snapshot.IsSuccess(), snapshot.GetIssues().ToString());
            UNIT_ASSERT(snapshot.Record.has_snapshot());
            auto table = stream.ReadNext().ExtractValueSync();
            UNIT_ASSERT_C(table.IsSuccess(), table.GetIssues().ToString());
            UNIT_ASSERT(table.Record.has_table());
            UNIT_ASSERT_VALUES_EQUAL(table.Record.table().id(), 1);
            UNIT_ASSERT_VALUES_EQUAL(table.Record.table().checksum().row_count(), 0);
            UNIT_ASSERT_VALUES_EQUAL(table.Record.table().checksum().hash(), expectedHash);
            UNIT_ASSERT(!table.Record.table().schema().empty());
            UNIT_ASSERT(stream.ReadNext().ExtractValueSync().Record.complete());
            UNIT_ASSERT_VALUES_EQUAL(stream.ReadNext().ExtractValueSync().GetStatus(), NYdb::EStatus::CLIENT_OUT_OF_RANGE);
        });
    }

    Y_UNIT_TEST(ChecksumSourceProbeDoesNotAcquireSnapshot) {
        TTransactionsEnv env;
        env.RunCall([&] {
            TPrivateClient client(env.GetDriver(), NYdb::TCommonClientSettings().AuthToken("root@builtin"));
            auto stream = client.StreamChecksumTablesInternal(MakeSourceChecksumRequest(true), TDuration::Seconds(30)).ExtractValueSync();
            const auto result = stream.ReadNext().ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT(result.Record.complete());
            UNIT_ASSERT(!result.Record.has_snapshot());
            UNIT_ASSERT(!result.Record.has_table());
        });
    }

    Y_UNIT_TEST(ChecksumIdleReplicationMatchesAndPersists) {
        TTransactionsEnv env;
        const auto expectedHash = env.ExpectedChecksumHash("/Root/table");
        UNIT_ASSERT(!env.SdkChecksum());
        const auto requestId = env.RequestChecksum();
        env.ChecksumHeartbeat();
        const auto response = env.WaitChecksum();
        UNIT_ASSERT_VALUES_EQUAL(response.GetRequestId(), requestId);
        UNIT_ASSERT_C(response.GetStatus() == Ydb::StatusIds::SUCCESS, response.GetIssue());
        const auto& result = response.GetChecksumStatus().last_result();
        UNIT_ASSERT(result.matches());
        UNIT_ASSERT_VALUES_EQUAL(result.source().row_count(), 0);
        UNIT_ASSERT_VALUES_EQUAL(result.source().hash(), expectedHash);
        UNIT_ASSERT_VALUES_EQUAL(result.destination().hash(), expectedHash);
        UNIT_ASSERT(result.has_finished_at());
        for (const bool stats : {false, true}) {
            const auto status = env.SdkChecksum(stats);
            UNIT_ASSERT(status && status->LastResult && status->LastResult->Matches);
            UNIT_ASSERT_VALUES_EQUAL(status->LastResult->Source.Hash, expectedHash);
            UNIT_ASSERT_VALUES_EQUAL(status->LastResult->Destination.Hash, expectedHash);
        }
        env.SendAsync(env.TabletId, new TEvents::TEvPoisonPill());
        env.Handshake();
        env.RequestChecksum(requestId);
        UNIT_ASSERT_VALUES_EQUAL(env.WaitChecksum().GetChecksumStatus().SerializeAsString(), response.GetChecksumStatus().SerializeAsString());
    }

    Y_UNIT_TEST(ChecksumMismatchAndSuccessfulRecheck) {
        TTransactionsEnv env;
        const auto write = [&](const TString& sql) {
            NYdb::NTable::TTableClient client(env.GetDriver());
            auto session = client.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteDataQuery(sql, NYdb::NTable::TTxControl::BeginTx(
                NYdb::NTable::TTxSettings::SerializableRW()).CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        };
        write("UPSERT INTO `/Root/table` (key, value) VALUES (1u, 'one');");
        const auto expectedHash = env.ExpectedChecksumHash("/Root/table");
        const auto expectedEmptyHash = env.ExpectedChecksumHash("/Root/replica");
        env.RequestChecksum();
        env.ChecksumHeartbeat();
        const auto mismatch = env.WaitChecksum();
        UNIT_ASSERT_C(mismatch.GetStatus() == Ydb::StatusIds::SUCCESS, mismatch.GetIssue());
        UNIT_ASSERT(!mismatch.GetChecksumStatus().last_result().matches());
        UNIT_ASSERT_VALUES_EQUAL(mismatch.GetChecksumStatus().last_result().source().row_count(), 1);
        UNIT_ASSERT_VALUES_EQUAL(mismatch.GetChecksumStatus().last_result().source().hash(), expectedHash);
        UNIT_ASSERT_VALUES_EQUAL(mismatch.GetChecksumStatus().last_result().destination().row_count(), 0);
        UNIT_ASSERT_VALUES_EQUAL(mismatch.GetChecksumStatus().last_result().destination().hash(), expectedEmptyHash);
        const auto status = env.SdkChecksum();
        UNIT_ASSERT(status && status->LastResult);
        UNIT_ASSERT_VALUES_EQUAL(status->LastResult->Source.Hash, expectedHash);
        UNIT_ASSERT_VALUES_EQUAL(status->LastResult->Destination.Hash, expectedEmptyHash);
        write("DELETE FROM `/Root/table` WHERE key = 1u;");
        env.RequestChecksum();
        env.ChecksumHeartbeat();
        const auto match = env.WaitChecksum();
        UNIT_ASSERT_C(match.GetStatus() == Ydb::StatusIds::SUCCESS, match.GetIssue());
        UNIT_ASSERT(match.GetChecksumStatus().last_result().matches());
        UNIT_ASSERT_VALUES_EQUAL(match.GetChecksumStatus().last_result().source().hash(), expectedEmptyHash);
        UNIT_ASSERT_VALUES_EQUAL(match.GetChecksumStatus().last_result().destination().hash(), expectedEmptyHash);
    }

    Y_UNIT_TEST(ChecksumSplitsTransactionRangesAtCapacity) {
        TTransactionsEnv env;
        for (ui64 step = 1; step < 50000; step += 10000) {
            env.Assign(step);
        }
        env.RequestChecksum();
        const auto deadline = TInstant::Now() + TDuration::Seconds(30);
        TRowVersion boundary;
        for (;;) {
            const auto transactions = env.Describe();
            if (transactions.assigned_size() > 5) {
                const auto& version = transactions.assigned(transactions.assigned_size() - 1).upper_bound_barrier();
                boundary = NChecksum::FromProto(version);
                break;
            }
            UNIT_ASSERT(TInstant::Now() < deadline);
        }
        auto response = env.Send<TEvService::TEvTxIdResult>(env.TabletId,
            new TEvService::TEvGetTxId(TVector<TRowVersion>{boundary.Prev(), boundary}));
        const auto& ranges = response->Get()->Record.GetVersionTxIds();
        UNIT_ASSERT_VALUES_EQUAL(ranges.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(TRowVersion::FromProto(ranges[0].GetVersion()), boundary);
        UNIT_ASSERT_VALUES_EQUAL(TRowVersion::FromProto(ranges[1].GetBegin()), boundary);
        UNIT_ASSERT(ranges[0].GetTxId() != ranges[1].GetTxId());
    }

    Y_UNIT_TEST(ChecksumRejectsOldReplicationService) {
        TTransactionsEnv env;
        auto* status = new TEvService::TEvStatus();
        status->Record.SetSupportsChecksumRanges(false);
        env.SendAsync(env.Controller, status);
        env.RequestChecksum();
        UNIT_ASSERT_VALUES_EQUAL(env.WaitChecksum().GetStatus(), Ydb::StatusIds::PRECONDITION_FAILED);
    }

    Y_UNIT_TEST(ChecksumRecoveryPreservesCompletedResult) {
        TTransactionsEnv env;
        env.RequestChecksum();
        env.ChecksumHeartbeat();
        const auto completed = env.WaitChecksum();
        UNIT_ASSERT_C(completed.GetStatus() == Ydb::StatusIds::SUCCESS, completed.GetIssue());
        env.HoldCommits();
        const auto requestId = env.RequestChecksum();
        // Wait until a real boundary commit is blocked, then interrupt the tablet.
        UNIT_ASSERT(env.GetRuntime().GrabEdgeEvent<TEvents::TEvWakeup>(env.GetSender(), TDuration::Seconds(45)));
        env.SendAsync(env.TabletId, new TEvents::TEvPoisonPill());
        env.Handshake();
        env.RequestChecksum(requestId);
        const auto interrupted = env.WaitChecksum();
        UNIT_ASSERT_VALUES_EQUAL(interrupted.GetStatus(), Ydb::StatusIds::UNAVAILABLE);
        UNIT_ASSERT_VALUES_EQUAL(interrupted.GetChecksumStatus().last_result().SerializeAsString(),
            completed.GetChecksumStatus().last_result().SerializeAsString());
        const auto status = env.SdkChecksum();
        UNIT_ASSERT(status && status->LastResult && status->LastResult->Matches);
        UNIT_ASSERT_VALUES_EQUAL(status->Status, NYdb::EStatus::UNAVAILABLE);
    }

    Y_UNIT_TEST(ChecksumDuplicateRequestReplaysResult) {
        TTransactionsEnv env;
        const auto requestId = env.RequestChecksum();
        env.RequestChecksum(requestId);
        env.ChecksumHeartbeat();
        const auto first = env.WaitChecksum();
        UNIT_ASSERT_C(first.GetStatus() == Ydb::StatusIds::SUCCESS, first.GetIssue());
        env.RequestChecksum(requestId);
        const auto replay = env.WaitChecksum();
        UNIT_ASSERT_VALUES_EQUAL(first.SerializeAsString(), replay.SerializeAsString());
    }

    Y_UNIT_TEST(ChecksumPeriodicChecks) {
        TTransactionsEnv env(true, ERuntimeMode::RealThreads, true);
        env.ChecksumHeartbeat();
        const auto deadline = TInstant::Now() + TDuration::Seconds(45);
        TInstant first;
        for (;;) {
            const auto status = env.SdkChecksum();
            if (status) {
                UNIT_ASSERT_C(status->Status == NYdb::EStatus::SUCCESS, status->Issues.ToString());
                UNIT_ASSERT(status->LastResult && status->LastResult->Matches);
                if (!first) {
                    first = status->FinishedAt;
                } else if (status->FinishedAt > first) {
                    UNIT_ASSERT(status->FinishedAt - first >= TDuration::Seconds(2));
                    break;
                }
            }
            UNIT_ASSERT(TInstant::Now() < deadline);
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

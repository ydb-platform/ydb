#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/tx/replication/controller/public_events.h>
#include <ydb/core/tx/replication/service/service.h>
#include <ydb/core/tx/replication/ut_helpers/test_env.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

#include <util/generic/scope.h>
#include <util/string/printf.h>

#include <optional>

using namespace NKikimr;
using namespace NKikimr::NReplication;
using namespace NSchemeShardUT_Private;

namespace {

using TRows = TMap<ui32, std::pair<TString, TString>>;

std::optional<TRows> ReadRows(const NYdb::NTable::TDataQueryResult& result) {
    TRows rows;
    auto parser = result.GetResultSetParser(0);
    while (parser.TryNextRow()) {
        const auto key = parser.ColumnParser("key").GetOptionalUint32().value();
        const TString value(parser.ColumnParser("value").GetOptionalUtf8().value());
        const TString payload(parser.ColumnParser("payload").GetOptionalUtf8().value());
        if (!rows.emplace(key, std::make_pair(value, payload)).second) {
            return std::nullopt; // Async removals and insertions may arrive separately.
        }
    }
    return rows;
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(TRebuildReplicationTests) {
    void RunningReplication(NKikimrSchemeOp::EIndexType type) {
        const bool async = type == NKikimrSchemeOp::EIndexTypeGlobalAsync;
        NReplication::NTestHelpers::TEnv<> pipeline(true, NReplication::NTestHelpers::ERuntimeMode::Simulated);
        auto& runtime = pipeline.GetRuntime();
        // Let SDK callbacks progress in wall time before simulated topic deadlines expire.
        runtime.SetFastSimulatedTime(false);
        runtime.GetAppData().FeatureFlags.SetEnableReplication(true);
        runtime.GetAppData().FeatureFlags.SetEnableAsyncIndexReplication(true);
        runtime.GetAppData().FeatureFlags.SetEnableChangefeedsOnIndexTables(true);
        runtime.GetAppData().FeatureFlags.SetEnableAddUniqueIndex(true);
        runtime.GetAppData().FeatureFlags.SetEnableOnlineAddUniqueIndex(true);
        const auto service = runtime.Register(CreateReplicationService());
        runtime.RegisterService(MakeReplicationServiceId(runtime.GetNodeId(0)), service);
        Y_DEFER {
            runtime.Send(new IEventHandle(service, pipeline.GetSender(), new TEvents::TEvPoison()));
            runtime.WaitFor("replication service stopped", [&] { return !runtime.FindActor(service); }, TDuration::Seconds(10));
        };

        NYdb::NTable::TTableClient client(pipeline.GetDriver());
        auto session = pipeline.RunCall([&] {
            const auto result = client.CreateSession(NYdb::NTable::TCreateSessionSettings()
                .ClientTimeout(TDuration::Seconds(10))).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            return result.GetSession();
        });
        const auto scheme = [&](const TString& query) {
            const auto result = pipeline.RunCall([&] {
                return session.ExecuteSchemeQuery(query, NYdb::NTable::TExecSchemeQuerySettings()
                    .ClientTimeout(TDuration::Seconds(30))).GetValueSync();
            });
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        };
        const auto execute = [&](const TString& query, bool stale = false) {
            return pipeline.RunCall([&] {
                return session.ExecuteDataQuery(query, NYdb::NTable::TTxControl::BeginTx(
                    stale ? NYdb::NTable::TTxSettings::StaleRO() : NYdb::NTable::TTxSettings::SerializableRW())
                    .CommitTx(), NYdb::NTable::TExecDataQuerySettings()
                        .ClientTimeout(TDuration::Seconds(5))).GetValueSync();
            });
        };
        const auto write = [&](const TString& query) {
            const auto deadline = TInstant::Now() + TDuration::Seconds(30);
            for (ui32 attempt = 0; attempt < 50 && TInstant::Now() < deadline; ++attempt) {
                const auto result = execute(query);
                if (result.IsSuccess()) {
                    return;
                }
                const auto status = result.GetStatus();
                UNIT_ASSERT_C(status == NYdb::EStatus::ABORTED || status == NYdb::EStatus::SCHEME_ERROR
                    || status == NYdb::EStatus::OVERLOADED, result.GetIssues().ToString());
                runtime.SimulateSleep(TDuration::MilliSeconds(50));
            }
            UNIT_FAIL("Source write retry limit reached: " << query);
        };

        scheme(Sprintf(R"(
            CREATE TABLE `/Root/source` (
                key Uint32, value Utf8, payload Utf8, PRIMARY KEY (key),
                INDEX by_value GLOBAL %s ON (value) COVER (payload)
            );
        )", async ? "ASYNC" : type == NKikimrSchemeOp::EIndexTypeGlobalUnique ? "UNIQUE SYNC" : "SYNC"));
        write(R"(UPSERT INTO `/Root/source` (key, value, payload) VALUES
            (1u, "old"u, "keep"u), (2u, "deleted"u, "remove"u);)");
        scheme(Sprintf(R"(
            CREATE ASYNC REPLICATION `/Root/replication`
                FOR `/Root/source` AS `/Root/replica`
                WITH (CONNECTION_STRING="grpc://%s/?database=/Root", TOKEN="root@builtin",
                      CONSISTENCY_LEVEL="ROW", INITIAL_SCAN=TRUE);
        )", pipeline.GetEndpoint().c_str()));

        const auto replicationPath = pipeline.GetPathId("/Root/replication");
        const ui64 controller = pipeline.GetDescription("/Root/replication")
            .GetPathDescription().GetReplicationDescription().GetControllerId();
        bool replicationActive = true;
        const auto checkController = [&] {
            if (!replicationActive) {
                return;
            }
            auto request = MakeHolder<TEvController::TEvDescribeReplication>();
            replicationPath.ToProto(request->Record.MutablePathId());
            const auto result = pipeline.Send<TEvController::TEvDescribeReplicationResult>(controller, std::move(request));
            UNIT_ASSERT_C(result->Get()->Record.GetStatus() == NKikimrReplication::TEvDescribeReplicationResult::SUCCESS,
                result->Get()->Record.DebugString());
            UNIT_ASSERT_C(!result->Get()->Record.GetState().HasError(), result->Get()->Record.DebugString());
        };
        const auto waitRows = [&](const TString& table, const TRows& expected) {
            std::optional<TRows> rows;
            std::optional<TRows> indexed;
            const auto deadline = TInstant::Now() + TDuration::Seconds(30);
            do {
                checkController();
                const auto plainResult = execute(TStringBuilder()
                    << "SELECT key, value, payload FROM `" << table << "` ORDER BY key;");
                const auto indexResult = execute(TStringBuilder()
                    << "SELECT key, value, payload FROM `" << table << "` VIEW by_value ORDER BY key;", async);
                if (plainResult.IsSuccess()) {
                    rows = ReadRows(plainResult);
                }
                if (indexResult.IsSuccess()) {
                    indexed = ReadRows(indexResult);
                }
                if (rows && indexed && *rows == expected && *indexed == expected) {
                    return;
                }
                runtime.SimulateSleep(TDuration::MilliSeconds(50));
            } while (TInstant::Now() < deadline);
            UNIT_ASSERT_C(rows && indexed, "Cannot read table and index: " << table);
            UNIT_ASSERT_VALUES_EQUAL_C(*rows, expected, table);
            UNIT_ASSERT_VALUES_EQUAL_C(*indexed, expected, table << " VIEW by_value");
        };

        TRows expected = {{1, {"old", "keep"}}, {2, {"deleted", "remove"}}};
        waitRows("/Root/replica", expected); // Initial scan must copy pre-existing rows.
        const ui64 schemeShard = pipeline.GetSchemeshardId("/Root/source");
        const auto subscriber = CreateNotificationSubscriber(runtime, schemeShard);
        const TString sourceIndex = "/Root/source/by_value";
        const TString replicaIndex = "/Root/replica/by_value";
        const auto replicaDescription = DescribePrivatePath(runtime, schemeShard, replicaIndex);
        TestDescribeResult(replicaDescription, {NLs::IndexType(type),
            NLs::IndexState(NKikimrSchemeOp::EIndexStateReady)});
        const ui64 replicaIndexId = replicaDescription.GetPathDescription().GetSelf().GetPathId();
        const auto sourceDescription = DescribePath(runtime, schemeShard, "/Root/source");
        UNIT_ASSERT_VALUES_EQUAL(sourceDescription.GetPathDescription().GetTable().CdcStreamsSize(), 1);
        const TString streamName = sourceDescription.GetPathDescription().GetTable().GetCdcStreams(0).GetName();
        const TString streamPath = "/Root/source/" + streamName;
        const ui64 streamId = DescribePrivatePath(runtime, schemeShard, streamPath).GetPathDescription().GetSelf().GetPathId();
        TMap<TString, ui64> indexPaths;
        TString indexStream;
        if (!async) {
            const TString impl = sourceIndex + "/indexImplTable";
            const auto description = DescribePrivatePath(runtime, schemeShard, impl);
            UNIT_ASSERT_VALUES_EQUAL(description.GetPathDescription().GetTable().CdcStreamsSize(), 1);
            indexStream = impl + "/" + description.GetPathDescription().GetTable().GetCdcStreams(0).GetName();
            for (const auto& path : {sourceIndex, impl, indexStream, indexStream + "/streamImpl"}) {
                const auto described = DescribePrivatePath(runtime, schemeShard, path);
                TestDescribeResult(described, {NLs::PathExist});
                indexPaths[path] = described.GetPathDescription().GetSelf().GetPathId();
            }
        }

        ui64 txId = 100500;
        const auto start = [&](Ydb::StatusIds::StatusCode expectedStatus) {
            Ydb::Table::TableIndex index;
            index.set_name("by_value");
            if (async) {
                index.mutable_global_async_index();
            } else if (type == NKikimrSchemeOp::EIndexTypeGlobalUnique) {
                index.mutable_global_unique_index();
            } else {
                index.mutable_global_index();
            }
            const ui64 build = ++txId;
            auto request = CreateBuildIndexRequest(build, "/Root", "/Root/source", index);
            request->Record.MutableSettings()->set_is_rebuild(true);
            const auto sender = runtime.AllocateEdgeActor();
            runtime.SendToPipe(schemeShard, sender, request);
            const auto created = runtime.GrabEdgeEventRethrow<TEvIndexBuilder::TEvCreateResponse>(sender);
            UNIT_ASSERT_VALUES_EQUAL_C(created->Get()->Record.GetStatus(), expectedStatus,
                created->Get()->Record.DebugString());
            if (expectedStatus == Ydb::StatusIds::PRECONDITION_FAILED) {
                UNIT_ASSERT_STRING_CONTAINS(created->Get()->Record.DebugString(),
                    "REBUILD INDEX cannot replace an index with active changefeeds");
            }
            return build;
        };
        const auto finish = [&](ui64 build, ui64 previousIndexId) {
            TestWaitNotification(runtime, {build}, subscriber);
            UNIT_ASSERT_VALUES_EQUAL(TestGetBuildIndex(runtime, schemeShard, "/Root", build).GetIndexBuild().GetState(),
                Ydb::Table::IndexBuildState::STATE_DONE);
            const auto rebuilt = DescribePrivatePath(runtime, schemeShard, sourceIndex);
            TestDescribeResult(rebuilt, {NLs::IndexType(type),
                NLs::IndexState(NKikimrSchemeOp::EIndexStateReady)});
            UNIT_ASSERT(rebuilt.GetPathDescription().GetSelf().GetPathId() != previousIndexId);
            const auto& metadata = rebuilt.GetPathDescription().GetTableIndex();
            UNIT_ASSERT_VALUES_EQUAL(metadata.KeyColumnNamesSize(), 1);
            UNIT_ASSERT_VALUES_EQUAL(metadata.GetKeyColumnNames(0), "value");
            UNIT_ASSERT_VALUES_EQUAL(metadata.DataColumnNamesSize(), 1);
            UNIT_ASSERT_VALUES_EQUAL(metadata.GetDataColumnNames(0), "payload");
            TestDescribeResult(DescribePrivatePath(runtime, schemeShard, TStringBuilder() << "/Root/source/__ydb_rebuild_" << build),
                {NLs::PathNotExist});
            TestDescribeResult(DescribePath(runtime, schemeShard, "/Root/source"), {NLs::IndexesCount(1)});
            TestForgetBuildIndex(runtime, ++txId, schemeShard, "/Root", build);
            UNIT_ASSERT_VALUES_EQUAL(TestListBuildIndex(runtime, schemeShard, "/Root").EntriesSize(), 0);
        };
        const auto checkIndexPaths = [&] {
            for (const auto& [path, id] : indexPaths) {
                const auto description = DescribePrivatePath(runtime, schemeShard, path);
                TestDescribeResult(description, {NLs::PathExist});
                UNIT_ASSERT_VALUES_EQUAL_C(description.GetPathDescription().GetSelf().GetPathId(), id, path);
            }
            TestDescribeResult(DescribePrivatePath(runtime, schemeShard, sourceIndex),
                {NLs::IndexType(type), NLs::IndexState(NKikimrSchemeOp::EIndexStateReady)});
        };
        for (ui32 iteration = 0; iteration < 2; ++iteration) {
            const ui64 previousIndexId = DescribePrivatePath(runtime, schemeShard, sourceIndex)
                .GetPathDescription().GetSelf().GetPathId();
            TBlockEvents<TEvDataShard::TEvBuildIndexCreateRequest> filling(runtime);
            Y_DEFER { filling.Stop().Unblock(); };
            const ui64 build = start(async ? Ydb::StatusIds::SUCCESS : Ydb::StatusIds::PRECONDITION_FAILED);
            if (async) {
                runtime.WaitFor("source index rebuild scan", [&] { return !filling.empty(); }, TDuration::Seconds(30));
            } else {
                UNIT_ASSERT(filling.empty());
                TestDescribeResult(DescribePrivatePath(runtime, schemeShard,
                    TStringBuilder() << "/Root/source/__ydb_rebuild_" << build), {NLs::PathNotExist});
                UNIT_ASSERT_VALUES_EQUAL(TestListBuildIndex(runtime, schemeShard, "/Root").EntriesSize(), 0);
                checkIndexPaths();
            }

            write(Sprintf(R"(
                UPSERT INTO `/Root/source` (key, value, payload) VALUES
                    (1u, "new-%u"u, "keep-%u"u), (%uu, "inserted-%u"u, "fresh-%u"u);
                DELETE FROM `/Root/source` WHERE key = %uu;
            )", iteration, iteration, 3 + iteration, iteration, iteration, 2 + iteration));
            expected[1] = {Sprintf("new-%u", iteration), Sprintf("keep-%u", iteration)};
            expected.erase(2 + iteration);
            expected[3 + iteration] = {Sprintf("inserted-%u", iteration), Sprintf("fresh-%u", iteration)};
            waitRows("/Root/source", expected);
            waitRows("/Root/replica", expected);
            UNIT_ASSERT_VALUES_EQUAL(DescribePrivatePath(runtime, schemeShard, sourceIndex)
                .GetPathDescription().GetSelf().GetPathId(), previousIndexId);
            if (async) {
                UNIT_ASSERT_VALUES_EQUAL(TestGetBuildIndex(runtime, schemeShard, "/Root", build).GetIndexBuild().GetState(),
                    Ydb::Table::IndexBuildState::STATE_TRANSFERING_DATA);
                filling.Stop().Unblock();
                finish(build, previousIndexId);
            } else {
                checkIndexPaths();
            }

            write(Sprintf(R"(UPSERT INTO `/Root/source` (key, value, payload) VALUES
                (%uu, "after-%u"u, "after-payload-%u"u);)", 100 + iteration, iteration, iteration));
            expected[100 + iteration] = {Sprintf("after-%u", iteration), Sprintf("after-payload-%u", iteration)};
            waitRows("/Root/source", expected);
            waitRows("/Root/replica", expected);
            UNIT_ASSERT_VALUES_EQUAL(DescribePrivatePath(runtime, schemeShard, replicaIndex)
                .GetPathDescription().GetSelf().GetPathId(), replicaIndexId);
            UNIT_ASSERT_VALUES_EQUAL(DescribePrivatePath(runtime, schemeShard, streamPath)
                .GetPathDescription().GetSelf().GetPathId(), streamId);
            TestDescribeResult(DescribePath(runtime, schemeShard, "/Root/replica"),
                {NLs::ReplicationMode(NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_READ_ONLY)});
            checkController();
        }
        scheme("DROP ASYNC REPLICATION `/Root/replication`;");
        replicationActive = false;
        TestDescribeResult(DescribePrivatePath(runtime, schemeShard, streamPath), {NLs::PathNotExist});
        if (!async) {
            TestDescribeResult(DescribePrivatePath(runtime, schemeShard, indexStream), {NLs::PathNotExist});
            TestDescribeResult(DescribePrivatePath(runtime, schemeShard, indexStream + "/streamImpl"), {NLs::PathNotExist});
            const ui64 build = start(Ydb::StatusIds::SUCCESS);
            finish(build, indexPaths.at(sourceIndex));
            waitRows("/Root/source", expected);
        }
    }

    Y_UNIT_TEST(AsyncIndexWithRunningReplication) {
        RunningReplication(NKikimrSchemeOp::EIndexTypeGlobalAsync);
    }

    Y_UNIT_TEST(SyncIndexWithRunningReplicationRejectsRebuild) {
        RunningReplication(NKikimrSchemeOp::EIndexTypeGlobal);
    }

    Y_UNIT_TEST(UniqueIndexWithRunningReplicationRejectsRebuild) {
        RunningReplication(NKikimrSchemeOp::EIndexTypeGlobalUnique);
    }
}

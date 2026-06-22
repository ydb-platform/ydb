#include <ydb/core/statistics/ut_common/ut_common.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/library/yql/dq/actors/protos/dq_status_codes.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/operation/operation.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/value/value.h>

#include <atomic>
#include <thread>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NTable;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpAnalyze) {

using namespace NStat;

Y_UNIT_TEST_TWIN(AnalyzeTable, ColumnStore) {
    TTestEnv env(1, 1, true);

    CreateDatabase(env, "Database");

    TTableClient client(env.GetDriver());
    auto session = client.CreateSession().GetValueSync().GetSession();

    TString createTable = Sprintf(R"(
            CREATE TABLE `%s` (
                Key Uint64 NOT NULL,
                Value String,
                PRIMARY KEY (Key)
            )
        )", "Root/Database/Table");
    if (ColumnStore) {
        createTable +=
            R"(
                PARTITION BY HASH(Key)
                WITH (
                    STORE = COLUMN,
                    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 16
                )
            )";
    }

    auto result = session.ExecuteSchemeQuery(createTable).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

    TValueBuilder rows;
    rows.BeginList();
    for (size_t i = 0; i < 1500; ++i) {
        auto key = TValueBuilder().Uint64(i).Build();
        auto value = TValueBuilder().OptionalString("Hello,world!").Build();

        rows.AddListItem();
            rows.BeginStruct();
                rows.AddMember("Key", key);
                rows.AddMember("Value", value);
            rows.EndStruct();
    }
    rows.EndList();

    result = client.BulkUpsert("Root/Database/Table", rows.Build()).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

    result = session.ExecuteSchemeQuery(
        Sprintf(R"(ANALYZE `Root/%s/%s`)", "Database", "Table")
    ).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

    auto& runtime = *env.GetServer().GetRuntime();
    ui64 saTabletId;
    auto pathId = ResolvePathId(runtime, "/Root/Database/Table", nullptr, &saTabletId);

    auto countMin = ExtractCountMin(runtime, pathId, 2);
    TString value = "Hello,world!";
    auto stat = countMin->Probe(value.data(), value.size());
    UNIT_ASSERT_C(stat >= 1500, ToString(stat));
}

Y_UNIT_TEST(AnalyzeError) {
    TTestEnv env(1, 1);
    auto& runtime = *env.GetServer().GetRuntime();
    CreateDatabase(env, "Database");

    TTableClient client(env.GetDriver());
    auto session = env.RunInThreadPool([&] {
        return client.CreateSession().GetValueSync().GetSession();
    });

    {
        // Create table
        TString createTable = R"(
            CREATE TABLE `Root/Database/Table` (
                Key Uint64 NOT NULL,
                Value String,
                PRIMARY KEY (Key)
            )
        )";

        auto result = env.RunInThreadPool([&] {
            return session.ExecuteSchemeQuery(createTable).GetValueSync();
        });
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    // Simulate an ANALYZE error coming from the StatisticsAggregator tablet.
    auto observer = runtime.AddObserver<TEvStatistics::TEvAnalyzeResponse>(
        [&](TEvStatistics::TEvAnalyzeResponse::TPtr& ev) {
        auto& record = ev->Get()->Record;
        record.SetStatus(NKikimrStat::TEvAnalyzeResponse::STATUS_ERROR);
        NYql::TIssue issue("mock issue");
        NYql::IssueToMessage(issue, record.AddIssues());
    });

    {
        // Run ANALYZE and check that the issue is reported.
        auto result = env.RunInThreadPool([&] {
            return session.ExecuteSchemeQuery("ANALYZE `Root/Database/Table`").GetValueSync();
        });
        UNIT_ASSERT(!result.IsSuccess());
        UNIT_ASSERT_C(
            HasIssue(result.GetIssues(), NYql::TIssuesIds::DEFAULT_ERROR,
                [](const auto& issue) {
                    return issue.GetMessage() == "mock issue";
                }),
            result.GetIssues().ToString());
    }
}

} // suite

Y_UNIT_TEST_SUITE(KqpAnalyzeOperations) {

using namespace NStat;

Y_UNIT_TEST(AnalyzeOperationsLifecycle) {
    TTestEnv env(1, 1, true);
    CreateDatabase(env, "Database");

    // Use the same driver for both the session and operation client to ensure database name consistency.
    // DiscoveryMode::Off avoids discovery against the dynamic tenant (which races test setup);
    // requests go directly to the static node's gRPC port, which routes by the database header.
    NYdb::TDriver opDriver(NYdb::TDriverConfig()
        .SetEndpoint(env.GetEndpoint())
        .SetDatabase("/Root/Database")
        .SetDiscoveryMode(NYdb::EDiscoveryMode::Off));
    NYdb::NOperation::TOperationClient operationClient(opDriver);

    TTableClient tableClient(opDriver);
    auto session = tableClient.CreateSession().GetValueSync().GetSession();

    // Empty list before any ANALYZE
    {
        auto result = operationClient.List<NYdb::NTable::TAnalyzeOperation>().GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS,
            result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(result.GetList().size(), 0);
    }

    // Create and populate a table in the tenant database
    {
        auto result = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `Root/Database/AnalyzeTest` (
                Key Uint64 NOT NULL,
                Value String,
                PRIMARY KEY (Key)
            );
        )").GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    {
        auto result = session.ExecuteDataQuery(R"(
            UPSERT INTO `Root/Database/AnalyzeTest` (Key, Value)
                VALUES (1, "a"), (2, "b"), (3, "c");
        )", TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    // Run ANALYZE TABLE (blocking from caller's view)
    {
        auto result = session.ExecuteSchemeQuery(
            "ANALYZE `Root/Database/AnalyzeTest`"
        ).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    // After ANALYZE completes, the operation is retained as DONE
    NYdb::TOperation::TOperationId opId;
    {
        auto listResult = operationClient.List<NYdb::NTable::TAnalyzeOperation>().GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(listResult.GetStatus(), NYdb::EStatus::SUCCESS,
            listResult.GetIssues().ToString());
        UNIT_ASSERT_GE(listResult.GetList().size(), 1);

        const auto& op = listResult.GetList()[0];
        opId = op.Id();
        UNIT_ASSERT_C(op.Ready(), op.Status().GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(op.Status().GetStatus(), NYdb::EStatus::SUCCESS);

        const auto& meta = op.Metadata();
        UNIT_ASSERT_VALUES_EQUAL(meta.State, NYdb::NTable::EAnalyzeState::Done);
        UNIT_ASSERT_DOUBLES_EQUAL(meta.Progress, 100.0f, 0.1f);
        UNIT_ASSERT_GE(meta.Paths.size(), 1u);
        UNIT_ASSERT(meta.InProgressPaths.empty());
        UNIT_ASSERT_VALUES_EQUAL(meta.DonePaths.size(), meta.Paths.size());
    }

    // Get by ID matches
    {
        auto getResult = operationClient.Get<NYdb::NTable::TAnalyzeOperation>(opId).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(getResult.Status().GetStatus(), NYdb::EStatus::SUCCESS,
            getResult.Status().GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(getResult.Metadata().State, NYdb::NTable::EAnalyzeState::Done);
        UNIT_ASSERT_DOUBLES_EQUAL(getResult.Metadata().Progress, 100.0f, 0.1f);
    }

    // Cancel of a terminal op is idempotent
    {
        auto cancelResult = operationClient.Cancel(opId).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(cancelResult.GetStatus(), NYdb::EStatus::SUCCESS,
            cancelResult.GetIssues().ToString());
    }

    // Forget removes it from history
    {
        auto forgetResult = operationClient.Forget(opId).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(forgetResult.GetStatus(), NYdb::EStatus::SUCCESS,
            forgetResult.GetIssues().ToString());
    }

    // Now Get returns NOT_FOUND
    {
        auto getAfterForget = operationClient.Get<NYdb::NTable::TAnalyzeOperation>(opId).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(getAfterForget.Status().GetStatus(), NYdb::EStatus::NOT_FOUND,
            getAfterForget.Status().GetIssues().ToString());
    }

    opDriver.Stop(true);
}

Y_UNIT_TEST(AnalyzeContinuesOnQueryAbort) {
    TTestEnv env(1, 1);
    auto& runtime = *env.GetServer().GetRuntime();
    CreateDatabase(env, "Database");

    NYdb::TDriver opDriver(NYdb::TDriverConfig()
        .SetEndpoint(env.GetEndpoint())
        .SetDatabase("/Root/Database")
        .SetDiscoveryMode(NYdb::EDiscoveryMode::Off));
    NYdb::NOperation::TOperationClient operationClient(opDriver);

    TTableClient tableClient(opDriver);
    auto session = env.RunInThreadPool([&]{
        return tableClient.CreateSession().GetValueSync().GetSession();
    });

    {
        auto result = env.RunInThreadPool([&]{
            return session.ExecuteSchemeQuery(R"(
                CREATE TABLE `Root/Database/AnalyzeTest` (
                    Key Uint64 NOT NULL,
                    Value String,
                    PRIMARY KEY (Key)
                );
            )").GetValueSync();
        });
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }
    {
        auto result = env.RunInThreadPool([&]{
            return session.ExecuteDataQuery(R"(
                UPSERT INTO `Root/Database/AnalyzeTest` (Key, Value)
                    VALUES (1, "a"), (2, "b"), (3, "c");
            )", TTxControl::BeginTx().CommitTx()).GetValueSync();
        });
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    // No TEvAnalyzeCancel must reach the SA under the new policy.
    std::atomic<int> cancelCount{0};
    auto cancelObs = runtime.AddObserver<NStat::TEvStatistics::TEvAnalyzeCancel>(
        [&](NStat::TEvStatistics::TEvAnalyzeCancel::TPtr&) {
            cancelCount.fetch_add(1);
        });

    // The instant TAnalyzeActor's TEvAnalyze is observed on its way to the SA,
    // inject TEvAbortExecution back to it (the sender) to simulate a session
    // timeout. TEvAnalyze itself is not blocked; the SA still processes it and
    // runs the traversal to completion in the background.
    std::atomic<bool> aborted{false};
    auto analyzeObs = runtime.AddObserver<NStat::TEvStatistics::TEvAnalyze>(
        [&](NStat::TEvStatistics::TEvAnalyze::TPtr& ev) {
            if (aborted.exchange(true)) {
                return;
            }
            const TActorId target = ev->Sender;
            auto* handle = new IEventHandle(
                target, TActorId(),
                new TEvKqp::TEvAbortExecution(
                    NYql::NDqProto::StatusIds::TIMEOUT,
                    "test injected query timeout", NYql::TIssues{}));
            const ui32 nodeIdx = target.NodeId() - runtime.GetFirstNodeId();
            runtime.Send(handle, nodeIdx, /*viaActorSystem=*/true);
        });

    auto analyzeResult = env.RunInThreadPool([&]{
        return session.ExecuteSchemeQuery(
            "ANALYZE `Root/Database/AnalyzeTest`").GetValueSync();
    });
    UNIT_ASSERT_C(!analyzeResult.IsSuccess(),
        "expected ANALYZE to fail due to injected abort, got: "
            << analyzeResult.GetIssues().ToString());

    // Advance simulated time so the SA receives SchemeShard stats, schedules the
    // traversal, and completes it. With our policy change, no TEvAnalyzeCancel was
    // sent on abort, so the operation must reach STATE_DONE — not STATE_CANCELLED.
    runtime.SimulateSleep(TDuration::Seconds(60));

    auto listResult = env.RunInThreadPool([&]{
        return operationClient.List<NYdb::NTable::TAnalyzeOperation>().GetValueSync();
    });
    UNIT_ASSERT_VALUES_EQUAL_C(listResult.GetStatus(), NYdb::EStatus::SUCCESS,
        listResult.GetIssues().ToString());

    bool foundDone = false;
    for (const auto& op : listResult.GetList()) {
        UNIT_ASSERT_VALUES_UNEQUAL_C(
            op.Metadata().State, NYdb::NTable::EAnalyzeState::Cancelled,
            "operation must not be cancelled by query abort");
        if (op.Metadata().State == NYdb::NTable::EAnalyzeState::Done) {
            foundDone = true;
        }
    }
    UNIT_ASSERT_C(foundDone, "ANALYZE long-running op did not reach DONE after abort");
    UNIT_ASSERT_VALUES_EQUAL_C(cancelCount.load(), 0,
        "TEvAnalyzeCancel must not be sent on query abort");

    opDriver.Stop(true);
}

} // suite KqpAnalyzeOperations

} // namespace NKqp
} // namespace NKikimr

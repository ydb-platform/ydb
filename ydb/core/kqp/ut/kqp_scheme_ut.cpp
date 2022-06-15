#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

#include <library/cpp/threading/local_executor/local_executor.h>

#include <util/generic/serialized_enum.h>
#include <util/string/printf.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpScheme) {
    Y_UNIT_TEST(UseUnauthorizedTable) {
        TKikimrRunner kikimr("test_user@builtin");
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto& tableSettings = kikimr.GetTestServer().GetSettings().AppConfig.GetTableServiceConfig();
        bool useSchemeCacheMeta = tableSettings.GetUseSchemeCacheMetadata();

        auto result = session.ExecuteDataQuery(R"(
            SELECT * FROM [/Root/KeyValue];
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(),
            useSchemeCacheMeta ? EStatus::SCHEME_ERROR : EStatus::UNAUTHORIZED, result.GetIssues().ToString());

        result = session.ExecuteDataQuery(R"(
            SELECT * FROM [/Root/NonExistent];
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(),
            useSchemeCacheMeta ? EStatus::SCHEME_ERROR : EStatus::UNAUTHORIZED, result.GetIssues().ToString());
    }

    Y_UNIT_TEST(UseNonexistentTable) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(
            SELECT * FROM [/Root/KeyValue];
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        result = session.ExecuteDataQuery(R"(
            SELECT * FROM [/Root/NonExistent];
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
    }

    Y_UNIT_TEST(UseDroppedTable) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(
            SELECT * FROM [/Root/KeyValue];
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        auto schemeResult = session.ExecuteSchemeQuery(R"(
            DROP TABLE [/Root/KeyValue];
        )").ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(schemeResult.GetStatus(), EStatus::SUCCESS);

        result = session.ExecuteDataQuery(R"(
            SELECT * FROM [/Root/KeyValue];
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
    }

    Y_UNIT_TEST(CreateDroppedTable) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto schemeResult = session.ExecuteSchemeQuery(R"(
            DROP TABLE [/Root/KeyValue];
        )").ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(schemeResult.GetStatus(), EStatus::SUCCESS);

        schemeResult = session.ExecuteSchemeQuery(R"(
            CREATE TABLE [/Root/KeyValue] (
                Key Uint32,
                Value String,
                PRIMARY KEY(Key)
            );
        )").ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(schemeResult.GetStatus(), EStatus::SUCCESS);

        auto result = session.ExecuteDataQuery(R"(
            SELECT * FROM [/Root/KeyValue];
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    Y_UNIT_TEST(CreateDropTableMultipleTime) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();

        const size_t inflight = 4;
        const size_t limit = 1000;

        const static TString createTableQuery = R"(
            CREATE TABLE [/Root/Test1234/KeyValue] (
                Key Uint32,
                Value String,
                PRIMARY KEY(Key)
            );
        )";

        const static TString dropTableQuery = R"(
            DROP TABLE [/Root/Test1234/KeyValue];
        )";

        NPar::LocalExecutor().RunAdditionalThreads(inflight);
        NPar::LocalExecutor().ExecRange([=, &db](int /*id*/) mutable {
            size_t i = limit;
            while (--i) {
                auto session = db.GetSession().GetValueSync().GetSession();
                {
                    auto result = session.ExecuteSchemeQuery(dropTableQuery)
                        .ExtractValueSync();
                    if (!(result.GetStatus() == EStatus::SUCCESS ||
                          result.GetStatus() == EStatus::SCHEME_ERROR ||
                          result.GetStatus() == EStatus::OVERLOADED)) {
                        UNIT_ASSERT_C(false, "status: " << result.GetStatus()
                            << " issues: " << result.GetIssues().ToString());
                    }
                }
                {
                    auto result = session.ExecuteSchemeQuery(createTableQuery)
                        .ExtractValueSync();
                    if (!(result.GetStatus() == EStatus::SUCCESS ||
                          result.GetStatus() == EStatus::SCHEME_ERROR ||
                          result.GetStatus() == EStatus::OVERLOADED)) {
                        UNIT_ASSERT_C(false, "status: " << result.GetStatus()
                            << " issues: " << result.GetIssues().ToString());
                    }
                }

            }
        }, 0, inflight, NPar::TLocalExecutor::WAIT_COMPLETE | NPar::TLocalExecutor::MED_PRIORITY);
    }

    Y_UNIT_TEST(CreateDropTableViaApiMultipleTime) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();

        const size_t inflight = 4;
        const size_t limit = 1000;
        const static TString tableName = "/Root/Test1234/KeyValue";

        NPar::LocalExecutor().RunAdditionalThreads(inflight);
        NPar::LocalExecutor().ExecRange([=, &db](int /*id*/) mutable {
            size_t i = limit;
            while (--i) {
                auto session = db.GetSession().GetValueSync().GetSession();
                {
                    auto result = session.DropTable(tableName)
                        .ExtractValueSync();
                    if (!(result.GetStatus() == EStatus::SUCCESS ||
                          result.GetStatus() == EStatus::SCHEME_ERROR ||
                          result.GetStatus() == EStatus::OVERLOADED)) {
                        UNIT_ASSERT_C(false, "status: " << result.GetStatus()
                            << " issues: " << result.GetIssues().ToString());
                    }
                }
                {
                    auto desc = TTableBuilder()
                        .AddNullableColumn("Key", EPrimitiveType::Uint32)
                        .AddNullableColumn("Value", EPrimitiveType::String)
                        .SetPrimaryKeyColumn("Key")
                        .Build();
                    auto result = session.CreateTable(tableName, std::move(desc))
                        .ExtractValueSync();
                    if (!(result.GetStatus() == EStatus::SUCCESS ||
                          result.GetStatus() == EStatus::SCHEME_ERROR ||
                          result.GetStatus() == EStatus::OVERLOADED)) {
                        UNIT_ASSERT_C(false, "status: " << result.GetStatus()
                            << " issues: " << result.GetIssues().ToString());
                    }
                }

            }
        }, 0, inflight, NPar::TLocalExecutor::WAIT_COMPLETE | NPar::TLocalExecutor::MED_PRIORITY);
    }

    Y_UNIT_TEST(QueryWithAlter) {
        auto kikimr = std::make_shared<TKikimrRunner>();
        auto db = kikimr->GetTableClient();

        const ui32 Inflight = 10;
        const TDuration WaitDuration = TDuration::Seconds(1);

        TAtomic finishing = false;
        NPar::LocalExecutor().RunAdditionalThreads(Inflight + 1);
        NPar::LocalExecutor().ExecRange([=, &db, &finishing](int id) mutable {
            if (id == Inflight) {
                Sleep(WaitDuration);

                auto status = db.RetryOperationSync([](TSession session) {
                    return session.ExecuteSchemeQuery(R"(
                        ALTER TABLE [/Root/EightShard] DROP COLUMN Data;
                    )").ExtractValueSync();
                });
                UNIT_ASSERT_VALUES_EQUAL_C(status.GetStatus(), EStatus::SUCCESS, status.GetIssues().ToString());

                Sleep(WaitDuration);

                AtomicSet(finishing, true);
                return;
            }

            auto retrySettings = TRetryOperationSettings().MaxRetries(100);

            while (!AtomicGet(finishing)) {
                if (id % 2) {
                    // Immediate
                    auto status = db.RetryOperationSync([](TSession session) {
                        return session.ExecuteDataQuery(R"(
                            SELECT * FROM [/Root/EightShard] WHERE Key = 501u;
                        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                    }, retrySettings);
                    UNIT_ASSERT_VALUES_EQUAL_C(status.GetStatus(), EStatus::SUCCESS, status.GetIssues().ToString());
                } else {
                    // Planned
                    auto status = db.RetryOperationSync([](TSession session) {
                        return session.ExecuteDataQuery(R"(
                            SELECT * FROM [/Root/EightShard];
                        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                    }, retrySettings);
                    UNIT_ASSERT_VALUES_EQUAL_C(status.GetStatus(), EStatus::SUCCESS, status.GetIssues().ToString());
                }
            }
        }, 0, Inflight + 1, NPar::TLocalExecutor::WAIT_COMPLETE | NPar::TLocalExecutor::MED_PRIORITY);
    }

    Y_UNIT_TEST_NEW_ENGINE(SchemaVersionMissmatch) {
        TKikimrRunner kikimr;

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        const TString query = Q_(R"(
            SELECT * FROM [/Root/KeyValue] WHERE Value = "New";
        )");

        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.KeepInQueryCache(true);
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

        {
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(),
                TExecDataQuerySettings().KeepInQueryCache(true)).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = session.ExecuteDataQuery(query,
                TTxControl::BeginTx().CommitTx(), execSettings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
            UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), true);
        }

        {
            auto type = TTypeBuilder()
                .BeginOptional()
                    .Primitive(EPrimitiveType::Utf8)
                .EndOptional()
                .Build();

            auto result = session.AlterTable("/Root/KeyValue", TAlterTableSettings()
                .AppendAddColumns(TColumn{"NewColumn", type})).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = session.ExecuteDataQuery(query,
                TTxControl::BeginTx().CommitTx(), execSettings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
            UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), false);
        }
    }

    template <bool UseNewEngine>
    void SchemaVersionMissmatchWithIndexTest(bool write) {
        //KIKIMR-14282
        //YDBREQUESTS-1324
        //some cases fail

        TKikimrRunner kikimr;

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        TString query;
        if (write) {
            query = Q_(R"(
                UPSERT INTO [/Root/KeyValue] (Key, Value) VALUES (10u, "New");
            )");
        } else {
            query = Q1_(R"(
                SELECT * FROM `/Root/KeyValue` VIEW `value_index` WHERE Value = "New";
            )");
        }

        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.KeepInQueryCache(true);
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

        {
            TString create_index_query = Q1_(R"(
                ALTER TABLE `/Root/KeyValue` ADD INDEX value_index GLOBAL SYNC ON (`Value`);
            )");
            auto result = session.ExecuteSchemeQuery(create_index_query).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx(),
                execSettings).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
            UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), false);
        }

        {
            auto result = session.ExecuteDataQuery(query,
                TTxControl::BeginTx(), execSettings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
            UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), true);
        }

        {
            kikimr.GetTestServer().GetRuntime()->GetAppData().AdministrationAllowedSIDs.push_back("root@builtin");
            TString alter_scheme = R"(
                    Name: "indexImplTable"
                    PartitionConfig {
                        PartitioningPolicy {
                            SizeToSplit: 1000
                            MinPartitionsCount: 1
                            MaxPartitionsCount: 100
                        }
                    }
            )";
            auto reply = kikimr.GetTestClient().AlterTable("/Root/KeyValue/value_index", alter_scheme, "root@builtin");
            const NKikimrClient::TResponse &response = reply->Record;
            UNIT_ASSERT_VALUES_EQUAL((NMsgBusProxy::EResponseStatus)response.GetStatus(), NMsgBusProxy::MSTATUS_OK);
        }

        {
            auto result = session.ExecuteDataQuery(query,
                TTxControl::BeginTx(), execSettings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), (write || UseNewEngine) ? EStatus::SUCCESS : EStatus::ABORTED, result.GetIssues().ToString());

            auto commit = result.GetTransaction()->Commit().GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(commit.GetStatus(), (write || UseNewEngine) ? EStatus::SUCCESS : EStatus::BAD_REQUEST, commit.GetIssues().ToString());
        }

        {
            auto result = session.ExecuteDataQuery(query,
                TTxControl::BeginTx(), execSettings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto commit = result.GetTransaction()->Commit().GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(commit.GetStatus(), EStatus::SUCCESS, commit.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST_NEW_ENGINE(SchemaVersionMissmatchWithIndexRead) {
        SchemaVersionMissmatchWithIndexTest<UseNewEngine>(false);
    }

    Y_UNIT_TEST_NEW_ENGINE(SchemaVersionMissmatchWithIndexWrite) {
        SchemaVersionMissmatchWithIndexTest<UseNewEngine>(true);
    }

    void CheckInvalidationAfterDropCreateTable(bool withCompatSchema) {
        TKikimrRunner kikimr;

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        const TString sql = "UPSERT INTO [/Root/KeyValue] (Key, Value) VALUES(1, \"One\")";

        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.KeepInQueryCache(true);
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

        {
            auto result = session.ExecuteDataQuery(sql,
                TTxControl::BeginTx().CommitTx(), TExecDataQuerySettings().KeepInQueryCache(true)).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        {
            auto result = session.ExecuteDataQuery(sql,
                TTxControl::BeginTx().CommitTx(), execSettings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
            UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), true);
        }

        {
            auto result = session.DropTable("/Root/KeyValue").ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        {
            auto desc = withCompatSchema
                ? TTableBuilder()
                    .AddNullableColumn("Key", EPrimitiveType::Uint64)
                    .AddNullableColumn("Value", EPrimitiveType::String)
                    .AddNullableColumn("Value2", EPrimitiveType::String)
                    .SetPrimaryKeyColumns({"Key"})
                    .Build()
                : TTableBuilder()
                    .AddNullableColumn("Key", EPrimitiveType::Uint64)
                    .AddNullableColumn("Value", EPrimitiveType::String)
                    .SetPrimaryKeyColumns({"Key", "Value"})
                    .Build();

            auto result =  session.CreateTable("/Root/KeyValue",
                std::move(desc)).GetValueSync();

            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        {
            auto result = session.ExecuteDataQuery(sql,
                TTxControl::BeginTx().CommitTx(), execSettings).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
            UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), false);
        }

        {
            // New session
            auto session2 = db.CreateSession().GetValueSync().GetSession();
            auto result = session2.ExecuteDataQuery(sql,
                TTxControl::BeginTx().CommitTx(), TExecDataQuerySettings().KeepInQueryCache(true)).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }
    }

    Y_UNIT_TEST(InvalidationAfterDropCreate) {
        CheckInvalidationAfterDropCreateTable(false);
    }

    Y_UNIT_TEST(InvalidationAfterDropCreateCompatSchema) {
        CheckInvalidationAfterDropCreateTable(true);
    }

    void CheckInvalidationAfterDropCreateTable2(bool multistageTx, bool select) {
        TKikimrRunner kikimr(TString(), KikimrDefaultUtDomainRoot, 1);

        auto db = kikimr.GetTableClient();
        const TString sql = select
            ? "SELECT * FROM [/Root/KeyValue];"
            : "UPSERT INTO [/Root/KeyValue] (Key, Value) VALUES(1, \"One\")";

        auto action = [db, sql, multistageTx]() mutable {
            return db.RetryOperationSync(
                [&](NYdb::NTable::TSession session) -> NYdb::TStatus {
                    auto prepareResult = session.PrepareDataQuery(sql).GetValueSync();

                    if (!prepareResult.IsSuccess()) {
                        return prepareResult;
                    }
                    auto dataQuery = prepareResult.GetQuery();

                    auto transaction = multistageTx
                        ? NYdb::NTable::TTxControl::BeginTx(NYdb::NTable::TTxSettings::SerializableRW())
                        : NYdb::NTable::TTxControl::BeginTx(NYdb::NTable::TTxSettings::SerializableRW()).CommitTx();
                    auto result = dataQuery.Execute(transaction).GetValueSync();

                    if (multistageTx) {
                        if (!result.IsSuccess()) {
                            return result;
                        }
                        return result.GetTransaction()->Commit().GetValueSync();
                    } else {
                        return result;
                    }
                }
            );
        };

        {
            auto result = action();

            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        {
            auto result = db.GetSession().GetValueSync().GetSession().DropTable("/Root/KeyValue").ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        {
            auto desc = TTableBuilder()
                .AddNullableColumn("Key", EPrimitiveType::Uint64)
                .AddNullableColumn("Value", EPrimitiveType::String)
                .AddNullableColumn("Value2", EPrimitiveType::String)
                .SetPrimaryKeyColumns({"Key"})
                .Build();

            auto result = db.GetSession().GetValueSync().GetSession().CreateTable("/Root/KeyValue",
                std::move(desc)).GetValueSync();

            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        {
            auto result = action();

            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }
    }

    Y_UNIT_TEST(InvalidationAfterDropCreateTable2) {
        CheckInvalidationAfterDropCreateTable2(false, false);
    }

    Y_UNIT_TEST(InvalidationAfterDropCreateTable2MultiStageTx) {
        CheckInvalidationAfterDropCreateTable2(true, false);
    }

    Y_UNIT_TEST(InvalidationAfterDropCreateTable2NoEffects) {
        CheckInvalidationAfterDropCreateTable2(false, true);
    }

    Y_UNIT_TEST(InvalidationAfterDropCreateTable2MultiStageTxNoEffects) {
        CheckInvalidationAfterDropCreateTable2(true, true);
    }

    Y_UNIT_TEST(CreateTableWithDefaultSettings) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString tableName = "/Root/TableWithPartitioningBySize";
        auto query = TStringBuilder() << R"(
            CREATE TABLE `)" << tableName << R"(` (
                Key Uint64,
                Value String,
                PRIMARY KEY (Key)
            );)";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        {
            TDescribeTableResult describe = session.DescribeTable(tableName).GetValueSync();
            UNIT_ASSERT_EQUAL(describe.GetStatus(), EStatus::SUCCESS);
            const auto& partSettings = describe.GetTableDescription().GetPartitioningSettings();
            UNIT_ASSERT(partSettings.GetPartitioningBySize().Defined());
            UNIT_ASSERT_VALUES_EQUAL(partSettings.GetPartitioningBySize().GetRef(), false);
            UNIT_ASSERT(partSettings.GetPartitioningByLoad().Defined());
            UNIT_ASSERT_VALUES_EQUAL(partSettings.GetPartitioningByLoad().GetRef(), false);
        }
    }

    void AlterTableSetttings(
            NYdb::NTable::TSession& session, const TString& tableName,
            const THashMap<TString, TString>& settings, bool compat,
            EStatus expectedStatus = EStatus::SUCCESS, const TString& expectedMessage = {}) {

        auto query = TStringBuilder()
            << "--!syntax_v1" << Endl
            << "ALTER TABLE `" << tableName << "` ";

        if (compat) {
            query << "SET (";
        }

        bool needComma = false;
        for (const auto& [key, value] : settings) {
            if (needComma) {
                query << ",";
            }

            needComma = true;

            if (compat) {
                query << key << "=" << value;
            } else {
                query << " SET " << key << " " << value;
            }
        }

        if (compat) {
            query << ")";
        }

        const auto result = session.ExecuteSchemeQuery(query << ";").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), expectedStatus, result.GetIssues().ToString());
        if (expectedMessage) {
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), expectedMessage, "Unexpected error message");
        }
    }

    void CreateTableWithCompactionPolicy(bool compat) {
        TKikimrRunner kikimr = TKikimrRunner();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString tableName = "/Root/TableWithCompactionPolicy";
        auto query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE TABLE `)" << tableName << R"(` (
                Key Uint64,
                Value String,
                PRIMARY KEY (Key)
            )
            WITH (
                COMPACTION_POLICY = "compaction2"
            );)";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        {
            auto describeResult = kikimr.GetTestClient().Ls(tableName);
            UNIT_ASSERT_VALUES_EQUAL(describeResult->Record.GetPathDescription().GetTable()
                .GetPartitionConfig().GetCompactionPolicy().GetGeneration().size(), 2);
        }

        AlterTableSetttings(session, tableName, {{"COMPACTION_POLICY", "\"default\""}}, compat);
        {
            auto describeResult = kikimr.GetTestClient().Ls(tableName);
            UNIT_ASSERT_VALUES_EQUAL(describeResult->Record.GetPathDescription().GetTable()
                .GetPartitionConfig().GetCompactionPolicy().GetGeneration().size(), 3);
        }
    }

    Y_UNIT_TEST(CreateTableWithCompactionPolicyUncompat) {
        CreateTableWithCompactionPolicy(false);
    }

    Y_UNIT_TEST(CreateTableWithCompactionPolicyCompat) {
        CreateTableWithCompactionPolicy(true);
    }

    Y_UNIT_TEST(CreateAndAlterTableWithPartitionBy) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString tableName = "/Root/TableWithPartitionBy";
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE TABLE `)" << tableName << R"(` (
                Key1 Uint64,
                Key2 Uint64,
                Key3 Uint64,
                Value String,
                PRIMARY KEY (Key1, Key2, Key3),
                PARTITION BY (Key1, Key2, Key3, Value)
            );)";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(),
                "\"Partition by\" contains more columns than primary key does",
                "Unexpected error message");
        }

        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE TABLE `)" << tableName << R"(` (
                Key1 Uint64,
                Key2 Uint64,
                Key3 Uint64,
                Value String,
                PRIMARY KEY (Key1, Key2, Key3),
                PARTITION BY (Key1, Key3, Key2)
            );)";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(),
                "\"Partition by\" doesn't match primary key",
                "Unexpected error message");
        }

        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE TABLE `)" << tableName << R"(` (
                Key1 Uint64,
                Key2 Uint64,
                Key3 Uint64,
                Value String,
                PRIMARY KEY (Key1, Key2, Key3),
                PARTITION BY (Key1, Key2)
            );)";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(),
                "\"Partition by\" is not supported yet",
                "Unexpected error message");
        }

        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE TABLE `)" << tableName << R"(` (
                Key1 Uint64,
                Key2 Uint64,
                Key3 Uint64,
                Value String,
                PRIMARY KEY (Key1, Key2, Key3),
                PARTITION BY (Key1, Key2, Key3)
            );)";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
    }

    void CreateAndAlterTableWithPartitioningBySize(bool compat) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString tableName = "/Root/TableWithPartitioningBySize";
        auto query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE TABLE `)" << tableName << R"(` (
                Key Uint64,
                Value String,
                PRIMARY KEY (Key)
            )
            WITH (
                AUTO_PARTITIONING_BY_SIZE = ENABLED
            );)";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        {
            TDescribeTableResult describe = session.DescribeTable(tableName).GetValueSync();
            UNIT_ASSERT_EQUAL(describe.GetStatus(), EStatus::SUCCESS);
            const auto& partSettings = describe.GetTableDescription().GetPartitioningSettings();
            UNIT_ASSERT(partSettings.GetPartitioningBySize().Defined());
            UNIT_ASSERT_VALUES_EQUAL(partSettings.GetPartitioningBySize().GetRef(), true);
            UNIT_ASSERT(partSettings.GetPartitioningByLoad().Defined());
            UNIT_ASSERT_VALUES_EQUAL(partSettings.GetPartitioningByLoad().GetRef(), false);
            UNIT_ASSERT_VALUES_EQUAL(partSettings.GetMinPartitionsCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(partSettings.GetPartitionSizeMb(), 2048);
        }

        AlterTableSetttings(session, tableName, {{"AUTO_PARTITIONING_BY_SIZE", "DISABLED"}}, compat);
        {
            TDescribeTableResult describe = session.DescribeTable(tableName).GetValueSync();
            UNIT_ASSERT_EQUAL(describe.GetStatus(), EStatus::SUCCESS);
            const auto& partSettings = describe.GetTableDescription().GetPartitioningSettings();
            UNIT_ASSERT(partSettings.GetPartitioningBySize().Defined());
            UNIT_ASSERT_VALUES_EQUAL(partSettings.GetPartitioningBySize().GetRef(), false);
            UNIT_ASSERT(partSettings.GetPartitioningByLoad().Defined());
            UNIT_ASSERT_VALUES_EQUAL(partSettings.GetPartitioningByLoad().GetRef(), false);
        }
    }

    Y_UNIT_TEST(CreateAndAlterTableWithPartitioningBySizeUncompat) {
        CreateAndAlterTableWithPartitioningBySize(false);
    }

    Y_UNIT_TEST(CreateAndAlterTableWithPartitioningBySizeCompat) {
        CreateAndAlterTableWithPartitioningBySize(true);
    }

    void CreateAndAlterTableWithPartitionSize(bool compat) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString tableName = "/Root/TableWithPartitionSize";
        auto queryCreate1 = TStringBuilder() << R"(
            --!syntax_v1
            CREATE TABLE `)" << tableName << R"(` (
                Key Uint64,
                Value String,
                PRIMARY KEY (Key)
            )
            WITH (
                AUTO_PARTITIONING_PARTITION_SIZE_MB = 0
            );)";
        auto resultCreate1 = session.ExecuteSchemeQuery(queryCreate1).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(resultCreate1.GetStatus(), EStatus::GENERIC_ERROR, resultCreate1.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS_C(resultCreate1.GetIssues().ToString(), "Can't set preferred partition size to 0",
            "Unexpected error message");

        auto queryCreate2 = TStringBuilder() << R"(
            --!syntax_v1
            CREATE TABLE `)" << tableName << R"(` (
                Key Uint64,
                Value String,
                PRIMARY KEY (Key)
            )
            WITH (
                AUTO_PARTITIONING_PARTITION_SIZE_MB = 1000
            );)";
        auto resultCreate2 = session.ExecuteSchemeQuery(queryCreate2).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(resultCreate2.GetStatus(), EStatus::SUCCESS, resultCreate2.GetIssues().ToString());

        {
            auto describeResult = kikimr.GetTestClient().Ls(tableName);
            size_t sizeToSplit = describeResult->Record.GetPathDescription().GetTable()
                .GetPartitionConfig().GetPartitioningPolicy().GetSizeToSplit();
            UNIT_ASSERT_VALUES_EQUAL(sizeToSplit, 1000 * 1024 * 1024);
        }

        AlterTableSetttings(session, tableName, {{"AUTO_PARTITIONING_PARTITION_SIZE_MB", "0"}}, compat,
            EStatus::GENERIC_ERROR, "Can't set preferred partition size to 0");

        AlterTableSetttings(session, tableName, {{"AUTO_PARTITIONING_BY_SIZE", "DISABLED"}}, compat);
        {
            auto describeResult = kikimr.GetTestClient().Ls(tableName);
            size_t sizeToSplit = describeResult->Record.GetPathDescription().GetTable()
                .GetPartitionConfig().GetPartitioningPolicy().GetSizeToSplit();
            UNIT_ASSERT_VALUES_EQUAL(sizeToSplit, 0);
        }

        AlterTableSetttings(session, tableName, {{"AUTO_PARTITIONING_PARTITION_SIZE_MB", "500"}}, compat);
        {
            auto describeResult = kikimr.GetTestClient().Ls(tableName);
            size_t sizeToSplit = describeResult->Record.GetPathDescription().GetTable()
                .GetPartitionConfig().GetPartitioningPolicy().GetSizeToSplit();
            UNIT_ASSERT_VALUES_EQUAL(sizeToSplit, 500 * 1024 * 1024);
        }
    }

    Y_UNIT_TEST(CreateAndAlterTableWithPartitionSizeUncompat) {
        CreateAndAlterTableWithPartitionSize(false);
    }

    Y_UNIT_TEST(CreateAndAlterTableWithPartitionSizeCompat) {
        CreateAndAlterTableWithPartitionSize(true);
    }

    Y_UNIT_TEST(RenameTable) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        {
            TString query = R"(
            --!syntax_v1
            CREATE TABLE `/Root/table` (
                Key Uint64,
                Value String,
                PRIMARY KEY (Key)
            );
            )";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            ALTER TABLE `/Root/table` RENAME TO `/Root/moved`;
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto describeResult = session.DescribeTable("/Root/moved").GetValueSync();
            UNIT_ASSERT_C(describeResult.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            ALTER TABLE `moved` RENAME TO `/Root/table`
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto describeResult = session.DescribeTable("/Root/table").GetValueSync();
            UNIT_ASSERT_C(describeResult.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE TABLE `/Root/second` (
                Key Uint64,
                Value String,
                PRIMARY KEY (Key)
            );
            )";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            ALTER TABLE `table` RENAME TO `/Root/moved`;
            ALTER TABLE `/Root/second` RENAME TO `movedsecond`;
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            {
                auto describeResult = session.DescribeTable("/Root/moved").GetValueSync();
                UNIT_ASSERT_C(describeResult.IsSuccess(), result.GetIssues().ToString());
            }
            {
                auto describeResult = session.DescribeTable("/Root/movedsecond").GetValueSync();
                UNIT_ASSERT_C(describeResult.IsSuccess(), result.GetIssues().ToString());
            }
        }

        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            ALTER TABLE `/Root/moved` RENAME TO `/Root/table`;
            ALTER TABLE `/Root/movedsecond` RENAME TO `/Root/second`;
            )";

            const auto result = session.ExecuteSchemeQuery(query << ";").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            DROP TABLE `/Root/second`;
            ALTER TABLE `/Root/table` RENAME TO `/Root/second`;
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            {
                auto describeResult = session.DescribeTable("/Root/second").GetValueSync();
                UNIT_ASSERT_C(describeResult.IsSuccess(), result.GetIssues().ToString());
            }

            {
                auto describeResult = session.DescribeTable("/Root/table").GetValueSync();
                UNIT_ASSERT_C(!describeResult.IsSuccess(), result.GetIssues().ToString());
            }
        }

    }

    void CreateAndAlterTableWithPartitioningByLoad(bool compat) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString tableName = "/Root/TableWithPartitioningByLoad";
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE TABLE `)" << tableName << R"(` (
                Key Uint64,
                Value String,
                PRIMARY KEY (Key)
            )
            WITH (
                AUTO_PARTITIONING_BY_LOAD = ENABLED
            );)";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto describeResult = kikimr.GetTestClient().Ls(tableName);
            bool enabled = describeResult->Record.GetPathDescription().GetTable()
                    .GetPartitionConfig().GetPartitioningPolicy().GetSplitByLoadSettings().GetEnabled();
            UNIT_ASSERT_VALUES_EQUAL(enabled, true);
        }

        AlterTableSetttings(session, tableName, {{"AUTO_PARTITIONING_BY_LOAD", "DISABLED"}}, compat);
        {
            auto describeResult = kikimr.GetTestClient().Ls(tableName);
            bool enabled = describeResult->Record.GetPathDescription().GetTable()
                    .GetPartitionConfig().GetPartitioningPolicy().GetSplitByLoadSettings().GetEnabled();
            UNIT_ASSERT_VALUES_EQUAL(enabled, false);
        }

        AlterTableSetttings(session, tableName, {{"AUTO_PARTITIONING_BY_LOAD", "ENABLED"}}, compat);
        {
            auto describeResult = kikimr.GetTestClient().Ls(tableName);
            bool enabled = describeResult->Record.GetPathDescription().GetTable()
                    .GetPartitionConfig().GetPartitioningPolicy().GetSplitByLoadSettings().GetEnabled();
            UNIT_ASSERT_VALUES_EQUAL(enabled, true);
        }
    }

    Y_UNIT_TEST(CreateAndAlterTableWithPartitioningByLoadUncompat) {
        CreateAndAlterTableWithPartitioningByLoad(false);
    }

    Y_UNIT_TEST(CreateAndAlterTableWithPartitioningByLoadCompat) {
        CreateAndAlterTableWithPartitioningByLoad(true);
    }

    void CreateAndAlterTableWithMinMaxPartitions(bool compat) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString tableName = "/Root/TableWithMinMaxPartitions";
        auto queryCreate1 = TStringBuilder() << R"(
            --!syntax_v1
            CREATE TABLE `)" << tableName << R"(` (
                Key Uint64,
                Value String,
                PRIMARY KEY (Key)
            )
            WITH (
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 0,
                AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 100
            );)";
        auto resultCreate1 = session.ExecuteSchemeQuery(queryCreate1).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(resultCreate1.GetStatus(), EStatus::GENERIC_ERROR, resultCreate1.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS_C(resultCreate1.GetIssues().ToString(), "Can't set min partition count to 0",
            "Unexpected error message");

        auto queryCreate2 = TStringBuilder() << R"(
            --!syntax_v1
            CREATE TABLE `)" << tableName << R"(` (
                Key Uint64,
                Value String,
                PRIMARY KEY (Key)
            )
            WITH (
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 10,
                AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 100
            );)";
        auto resultCreate2 = session.ExecuteSchemeQuery(queryCreate2).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(resultCreate2.GetStatus(), EStatus::SUCCESS, resultCreate2.GetIssues().ToString());

        {
            auto describeResult = kikimr.GetTestClient().Ls(tableName);
            UNIT_ASSERT_VALUES_EQUAL(describeResult->Record.GetPathDescription().GetTable().GetPartitionConfig()
                .GetPartitioningPolicy().GetMinPartitionsCount(), 10);
            UNIT_ASSERT_VALUES_EQUAL(describeResult->Record.GetPathDescription().GetTable().GetPartitionConfig()
                .GetPartitioningPolicy().GetMaxPartitionsCount(), 100);
        }

        AlterTableSetttings(session, tableName, {
            {"AUTO_PARTITIONING_MIN_PARTITIONS_COUNT", "20"},
            {"AUTO_PARTITIONING_MAX_PARTITIONS_COUNT", "0"}
        }, compat, EStatus::GENERIC_ERROR, "Can't set max partition count to 0");

        AlterTableSetttings(session, tableName, {
            {"AUTO_PARTITIONING_MIN_PARTITIONS_COUNT", "20"},
            {"AUTO_PARTITIONING_MAX_PARTITIONS_COUNT", "50"}
        }, compat);

        {
            auto describeResult = kikimr.GetTestClient().Ls(tableName);
            UNIT_ASSERT_VALUES_EQUAL(describeResult->Record.GetPathDescription().GetTable().GetPartitionConfig()
                .GetPartitioningPolicy().GetMinPartitionsCount(), 20);
            UNIT_ASSERT_VALUES_EQUAL(describeResult->Record.GetPathDescription().GetTable().GetPartitionConfig()
                .GetPartitioningPolicy().GetMaxPartitionsCount(), 50);
        }
    }

    Y_UNIT_TEST(CreateAndAlterTableWithMinMaxPartitionsUncompat) {
        CreateAndAlterTableWithMinMaxPartitions(false);
    }

    Y_UNIT_TEST(CreateAndAlterTableWithMinMaxPartitionsCompat) {
        CreateAndAlterTableWithMinMaxPartitions(true);
    }

    void CreateAndAlterTableWithBloomFilter(bool compat) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString tableName = "/Root/TableWithBloomFilter";
        auto query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE TABLE `)" << tableName << R"(` (
                Key Uint64,
                Value String,
                PRIMARY KEY (Key)
            )
            WITH (
                KEY_BLOOM_FILTER = ENABLED
            );)";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        {
            auto describeResult = kikimr.GetTestClient().Ls(tableName);
            UNIT_ASSERT_VALUES_EQUAL(describeResult->Record.GetPathDescription().GetTable().GetPartitionConfig()
                .GetEnableFilterByKey(), true);
        }

        AlterTableSetttings(session, tableName, {{"KEY_BLOOM_FILTER", "DISABLED"}}, compat);
        {
            auto describeResult = kikimr.GetTestClient().Ls(tableName);
            UNIT_ASSERT_VALUES_EQUAL(describeResult->Record.GetPathDescription().GetTable().GetPartitionConfig()
                .GetEnableFilterByKey(), false);
        }

        AlterTableSetttings(session, tableName, {{"KEY_BLOOM_FILTER", "ENABLED"}}, compat);
        {
            auto describeResult = kikimr.GetTestClient().Ls(tableName);
            UNIT_ASSERT_VALUES_EQUAL(describeResult->Record.GetPathDescription().GetTable().GetPartitionConfig()
                .GetEnableFilterByKey(), true);
        }
    }

    Y_UNIT_TEST(CreateAndAlterTableWithBloomFilterUncompat) {
        CreateAndAlterTableWithBloomFilter(false);
    }

    Y_UNIT_TEST(CreateAndAlterTableWithBloomFilterCompat) {
        CreateAndAlterTableWithBloomFilter(true);
    }

    void CreateTableWithReadReplicas(bool compat) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString tableName = "/Root/TableWithReadReplicas";
        {
            auto queryCreate = TStringBuilder() << R"(
            --!syntax_v1
            CREATE TABLE `)" << tableName << R"(` (
                Key Uint64,
                Value String,
                PRIMARY KEY (Key)
            )
            WITH (
                READ_REPLICAS_SETTINGS = "SOME_AZ:2"
            );)";
            auto resultCreate = session.ExecuteSchemeQuery(queryCreate).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(resultCreate.GetStatus(), EStatus::GENERIC_ERROR, resultCreate.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(resultCreate.GetIssues().ToString(),
                "Specifying read replicas count for each AZ in cluster is not supported yet",
                "Unexpected error message");
        }
        {
            auto queryCreate = TStringBuilder() << R"(
            --!syntax_v1
            CREATE TABLE `)" << tableName << R"(` (
                Key Uint64,
                Value String,
                PRIMARY KEY (Key)
            )
            WITH (
                READ_REPLICAS_SETTINGS = "VLA: 2, MAN:1 "
            );)";
            auto resultCreate = session.ExecuteSchemeQuery(queryCreate).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(resultCreate.GetStatus(), EStatus::GENERIC_ERROR, resultCreate.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(resultCreate.GetIssues().ToString(),
                "Specifying read replicas count for each AZ in cluster is not supported yet",
                "Unexpected error message");
        }
        {
            auto queryCreate = TStringBuilder() << R"(
            --!syntax_v1
            CREATE TABLE `)" << tableName << R"(` (
                Key Uint64,
                Value String,
                PRIMARY KEY (Key)
            )
            WITH (
                READ_REPLICAS_SETTINGS = "any_az:1"
            );)";
            auto resultCreate = session.ExecuteSchemeQuery(queryCreate).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(resultCreate.GetStatus(), EStatus::SUCCESS, resultCreate.GetIssues().ToString());
        }

        {
            const auto tableDesc = session.DescribeTable(tableName).GetValueSync().GetTableDescription();
            const auto readReplicasSettings = tableDesc.GetReadReplicasSettings();
            UNIT_ASSERT(readReplicasSettings);
            UNIT_ASSERT(readReplicasSettings->GetMode() == NYdb::NTable::TReadReplicasSettings::EMode::AnyAz);
            UNIT_ASSERT_VALUES_EQUAL(readReplicasSettings->GetReadReplicasCount(), 1);
        }

        AlterTableSetttings(session, tableName, {{"READ_REPLICAS_SETTINGS", "\"PER_AZ:2\""}}, compat);
        {
            const auto tableDesc = session.DescribeTable(tableName).GetValueSync().GetTableDescription();
            const auto readReplicasSettings = tableDesc.GetReadReplicasSettings();
            UNIT_ASSERT(readReplicasSettings);
            UNIT_ASSERT(readReplicasSettings->GetMode() == NYdb::NTable::TReadReplicasSettings::EMode::PerAz);
            UNIT_ASSERT_VALUES_EQUAL(readReplicasSettings->GetReadReplicasCount(), 2);
        }
    }

    Y_UNIT_TEST(CreateTableWithReadReplicasUncompat) {
        CreateTableWithReadReplicas(false);
    }

    Y_UNIT_TEST(CreateTableWithReadReplicasCompat) {
        CreateTableWithReadReplicas(true);
    }

    void CreateTableWithTtlSettings(bool compat) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString tableName = "/Root/TableWithTtlSettings";

        auto createTable = [&](const TString& ttlSettings, EStatus expectedStatus = EStatus::SUCCESS, const TString& expectedMessage = {}) {
            auto query = TStringBuilder() << R"(
                --!syntax_v1
                CREATE TABLE `)" << tableName << R"(` (
                    Key Uint64,
                    Ts Timestamp,
                    StringValue String,
                    Uint32Value Uint32,
                    Uint64Value Uint64,
                    DyNumberValue DyNumber,
                    PRIMARY KEY (Key)
                ) WITH (
                    TTL = )" << ttlSettings << R"(
                ))";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), expectedStatus, result.GetIssues().ToString());
            if (expectedMessage) {
                UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), expectedMessage, "Unexpected error message");
            }
        };

        createTable(R"(DateTime::IntervalFromDays(1) ON Ts)", EStatus::GENERIC_ERROR, "Literal of Interval type is expected for TTL");

        createTable(R"("P1D" ON Ts)", EStatus::GENERIC_ERROR, "Literal of Interval type is expected for TTL");

        createTable(R"(Interval("-P1D") ON Ts)", EStatus::GENERIC_ERROR, "Interval value cannot be negative");

        createTable(R"(Interval("P1D") ON CreatedAt)", EStatus::GENERIC_ERROR, "Cannot enable TTL on unknown column");

        createTable(R"(Interval("P1D") ON StringValue)", EStatus::GENERIC_ERROR, "Unsupported column type");

        createTable(R"(Interval("P1D") ON Uint32Value)", EStatus::GENERIC_ERROR, "'ValueSinceUnixEpochModeSettings' should be specified");
        createTable(R"(Interval("P1D") ON Uint64Value)", EStatus::GENERIC_ERROR, "'ValueSinceUnixEpochModeSettings' should be specified");
        createTable(R"(Interval("P1D") ON DyNumberValue)", EStatus::GENERIC_ERROR, "'ValueSinceUnixEpochModeSettings' should be specified");

        createTable(R"(Interval("P1D") ON Ts)");
        {
            auto result = kikimr.GetTestClient().Ls(tableName);
            const auto& table = result->Record.GetPathDescription().GetTable();
            UNIT_ASSERT(table.HasTTLSettings());

            const auto& ttl = table.GetTTLSettings();
            UNIT_ASSERT(ttl.HasEnabled());
            UNIT_ASSERT_VALUES_EQUAL(ttl.GetEnabled().GetColumnName(), "Ts");
            UNIT_ASSERT_VALUES_EQUAL(ttl.GetEnabled().GetExpireAfterSeconds(), 86'400);
        }

        {
            auto query = TStringBuilder() << R"(
                --!syntax_v1
                ALTER TABLE `)" << tableName << R"(` RESET (TTL);)";
            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = kikimr.GetTestClient().Ls(tableName);
            const auto& table = result->Record.GetPathDescription().GetTable();
            UNIT_ASSERT(table.HasTTLSettings());

            const auto& ttl = table.GetTTLSettings();
            UNIT_ASSERT(ttl.HasDisabled());
        }

        AlterTableSetttings(session, tableName, {{"TTL", R"(DateTime::IntervalFromDays(1) ON Ts)"}}, compat,
            EStatus::GENERIC_ERROR, "Literal of Interval type is expected for TTL");

        AlterTableSetttings(session, tableName, {{"TTL", R"("-P1D" ON Ts)"}}, compat,
            EStatus::GENERIC_ERROR, "Literal of Interval type is expected for TTL");

        AlterTableSetttings(session, tableName, {{"TTL", R"(Interval("-P1D") ON Ts)"}}, compat,
            EStatus::GENERIC_ERROR, "Interval value cannot be negative");

        AlterTableSetttings(session, tableName, {{"TTL", R"(Interval("P0D") ON CreatedAt)"}}, compat,
            EStatus::GENERIC_ERROR, "Cannot enable TTL on unknown column");

        AlterTableSetttings(session, tableName, {{"TTL", R"(Interval("P0D") ON StringValue)"}}, compat,
            EStatus::GENERIC_ERROR, "Unsupported column type");

        AlterTableSetttings(session, tableName, {{"TTL", R"(Interval("P0D") ON Uint32Value)"}}, compat,
            EStatus::GENERIC_ERROR, "'ValueSinceUnixEpochModeSettings' should be specified");
        AlterTableSetttings(session, tableName, {{"TTL", R"(Interval("P0D") ON Uint64Value)"}}, compat,
            EStatus::GENERIC_ERROR, "'ValueSinceUnixEpochModeSettings' should be specified");
        AlterTableSetttings(session, tableName, {{"TTL", R"(Interval("P0D") ON DyNumberValue)"}}, compat,
            EStatus::GENERIC_ERROR, "'ValueSinceUnixEpochModeSettings' should be specified");

        AlterTableSetttings(session, tableName, {{"TTL", R"(Interval("P0D") ON Ts)"}}, compat);
        {
            auto result = kikimr.GetTestClient().Ls(tableName);
            const auto& table = result->Record.GetPathDescription().GetTable();
            UNIT_ASSERT(table.HasTTLSettings());

            const auto& ttl = table.GetTTLSettings();
            UNIT_ASSERT(ttl.HasEnabled());
            UNIT_ASSERT_VALUES_EQUAL(ttl.GetEnabled().GetColumnName(), "Ts");
            UNIT_ASSERT_VALUES_EQUAL(ttl.GetEnabled().GetExpireAfterSeconds(), 0);
        }

        {
            auto query = TStringBuilder() << R"(
                --!syntax_v1
                ALTER TABLE `)" << tableName << R"(` DROP COLUMN Ts;)";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Can't drop TTL column");
        }

        {
            auto query = TStringBuilder() << R"(
                --!syntax_v1
                ALTER TABLE `)" << tableName << R"(` RESET (TTL);)";
            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto query = TStringBuilder() << R"(
                --!syntax_v1
                ALTER TABLE `)" << tableName << R"(` DROP COLUMN Ts;)";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(CreateTableWithTtlSettingsUncompat) {
        CreateTableWithTtlSettings(false);
    }

    Y_UNIT_TEST(CreateTableWithTtlSettingsCompat) {
        CreateTableWithTtlSettings(true);
    }

    void CreateTableWithUniformPartitions(bool compat) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString tableName = "/Root/TableWithUniformPartitions";
        auto query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE TABLE `)" << tableName << R"(` (
                Key Uint64,
                Value String,
                PRIMARY KEY (Key)
            )
            WITH (
                UNIFORM_PARTITIONS = 4
            );)";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto describeResult = session.DescribeTable(tableName,
            NYdb::NTable::TDescribeTableSettings().WithTableStatistics(true)).GetValueSync();
        UNIT_ASSERT_C(describeResult.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(describeResult.GetTableDescription().GetPartitionsCount(), 4);

        AlterTableSetttings(session, tableName, {{"UNIFORM_PARTITIONS", "8"}}, compat,
            EStatus::GENERIC_ERROR, "UNIFORM_PARTITIONS alter is not supported");
    }

    Y_UNIT_TEST(CreateTableWithUniformPartitionsUncompat) {
        CreateTableWithUniformPartitions(false);
    }

    Y_UNIT_TEST(CreateTableWithUniformPartitionsCompat) {
        CreateTableWithUniformPartitions(true);
    }

    void CreateTableWithPartitionAtKeysSimple(bool compat) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString tableName = "/Root/TableWithPartitionAtKeysSimple";
        auto query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE TABLE `)" << tableName << R"(` (
                Key1 Uint64,
                Key2 String,
                Value String,
                PRIMARY KEY (Key1, Key2)
            )
            WITH (
                PARTITION_AT_KEYS = (10, 100, 1000, 10000)
            );)";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto describeResult = session.DescribeTable(tableName,
            TDescribeTableSettings().WithTableStatistics(true).WithKeyShardBoundary(true)).GetValueSync();
        UNIT_ASSERT_C(describeResult.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(describeResult.GetTableDescription().GetPartitionsCount(), 5);

        auto extractValue = [](const TValue& val) {
            auto parser = TValueParser(val);
            parser.OpenTuple();
            UNIT_ASSERT(parser.TryNextElement());
            return parser.GetOptionalUint64().GetRef();
        };

        const TVector<TKeyRange>& keyRanges = describeResult.GetTableDescription().GetKeyRanges();

        size_t n = 0;
        const TVector<ui64> expectedRanges = { 10ul, 100ul, 1000ul, 10000ul };

        for (const auto& range : keyRanges) {
            if (n == 0) {
                UNIT_ASSERT(!range.From());
            } else {
                UNIT_ASSERT(range.From()->IsInclusive());
                auto left = extractValue(range.From()->GetValue());
                UNIT_ASSERT_VALUES_EQUAL(left, expectedRanges[n - 1]);
            }
            if (n == expectedRanges.size()) {
                UNIT_ASSERT(!range.To());
            } else {
                UNIT_ASSERT(!range.To()->IsInclusive());
                auto right = extractValue(range.To()->GetValue());
                UNIT_ASSERT_VALUES_EQUAL(right, expectedRanges[n]);
            }
            ++n;
        }

        AlterTableSetttings(session, tableName, {{"PARTITION_AT_KEYS", "(100, 500, 1000, 10000)"}}, compat,
            EStatus::GENERIC_ERROR, "PARTITION_AT_KEYS alter is not supported");
    }

    Y_UNIT_TEST(CreateTableWithPartitionAtKeysSimpleUncompat) {
        CreateTableWithPartitionAtKeysSimple(false);
    }

    Y_UNIT_TEST(CreateTableWithPartitionAtKeysSimpleCompat) {
        CreateTableWithPartitionAtKeysSimple(true);
    }

    Y_UNIT_TEST(CreateTableWithPartitionAtKeysComplex) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString tableName = "/Root/TableWithPartitionAtKeysComplex";
        auto query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE TABLE `)" << tableName << R"(` (
                Key1 Uint64,
                Key2 String,
                Value String,
                PRIMARY KEY (Key1, Key2)
            )
            WITH (
                PARTITION_AT_KEYS = ((10), (100, "123"), (1000, "cde"))
            );)";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto describeResult = session.DescribeTable(tableName,
            TDescribeTableSettings().WithTableStatistics(true).WithKeyShardBoundary(true)).GetValueSync();
        UNIT_ASSERT_C(describeResult.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(describeResult.GetTableDescription().GetPartitionsCount(), 4);

        auto extractValue = [](const TValue& val) {
            auto parser = TValueParser(val);
            parser.OpenTuple();
            UNIT_ASSERT(parser.TryNextElement());
            ui64 pk1 = parser.GetOptionalUint64().GetRef();
            UNIT_ASSERT(parser.TryNextElement());
            auto pk2 = parser.GetOptionalString();
            return std::pair<ui64, TMaybe<TString>>(pk1, pk2);
        };

        const TVector<TKeyRange>& keyRanges = describeResult.GetTableDescription().GetKeyRanges();

        size_t n = 0;
        const TVector<std::pair<ui64, TString>> expectedRanges = {
            { 10ul, "" },
            { 100ul, "123" },
            { 1000ul, "cde" }
        };

        for (const auto& range : keyRanges) {
            if (n == 0) {
                UNIT_ASSERT(!range.From());
            } else {
                UNIT_ASSERT(range.From()->IsInclusive());
                const auto& [pk1, pk2] = extractValue(range.From()->GetValue());
                const auto& [expectedPk1, expectedPk2] = expectedRanges[n - 1];

                UNIT_ASSERT_VALUES_EQUAL(pk1, expectedPk1);
                if (pk2.Defined()) {
                    UNIT_ASSERT_VALUES_EQUAL(pk2.GetRef(), expectedPk2);
                } else {
                    UNIT_ASSERT_VALUES_EQUAL("", expectedPk2);
                }
            }
            if (n == expectedRanges.size()) {
                UNIT_ASSERT(!range.To());
            } else {
                UNIT_ASSERT(!range.To()->IsInclusive());
                const auto&[pk1, pk2] = extractValue(range.To()->GetValue());
                const auto&[expectedPk1, expectedPk2] = expectedRanges[n];

                UNIT_ASSERT_VALUES_EQUAL(pk1, expectedPk1);
                if (pk2.Defined()) {
                    UNIT_ASSERT_VALUES_EQUAL(pk2.GetRef(), expectedPk2);
                } else {
                    UNIT_ASSERT_VALUES_EQUAL("", expectedPk2);
                }
            }
            ++n;
        }
    }

    Y_UNIT_TEST(CreateTableWithWrongPartitionAtKeys) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        auto query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE TABLE `/Root/TableWithWrongPartitionAtKeysComplex` (
                Key Uint64,
                Value String,
                PRIMARY KEY (Key)
            )
            WITH (
                PARTITION_AT_KEYS = ((10), (100, "123"), (1000, "cde"))
            );)";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(),
            "Partition at keys has 2 key values while there are only 1 key columns", "Unexpected error message");
    }

    Y_UNIT_TEST(CreateTableWithFamiliesRegular) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString tableName = "/Root/TableWithFamiliesRegular";
        auto query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE TABLE `)" << tableName << R"(` (
                Key Uint64,
                Value1 String FAMILY Family1,
                Value2 Uint32 FAMILY Family2,
                PRIMARY KEY (Key),
                FAMILY Family1 (
                     DATA = "test",
                     COMPRESSION = "off"
                ),
                FAMILY Family2 ()
            );)";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto describeResult = session.DescribeTable(tableName, NYdb::NTable::TDescribeTableSettings()).GetValueSync();
        UNIT_ASSERT_C(describeResult.IsSuccess(), result.GetIssues().ToString());
        const auto& columnFamilies = describeResult.GetTableDescription().GetColumnFamilies();
        UNIT_ASSERT_VALUES_EQUAL(columnFamilies.size(), 3);
        for (const auto& family : columnFamilies) {
            if (family.GetName() == "Family1") {
                UNIT_ASSERT_VALUES_EQUAL(family.GetData(), "test");
                UNIT_ASSERT_VALUES_EQUAL(family.GetCompression(), EColumnFamilyCompression::None);
            } else {
                UNIT_ASSERT(family.GetName() == "default" || family.GetName() == "Family2");
            }
        }
    }

    Y_UNIT_TEST(CreateTableWithDefaultFamily) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString tableName = "/Root/TableWithDefaultFamily";
        auto query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE TABLE `)" << tableName << R"(` (
                Key Uint64,
                Value1 String FAMILY Family1,
                Value2 String,
                PRIMARY KEY (Key),
                FAMILY default (
                     DATA = "test",
                     COMPRESSION = "lz4"
                ),
                FAMILY Family1 (
                     DATA = "test",
                     COMPRESSION = "off"
                )
            );)";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        {
            auto describeResult = session.DescribeTable(tableName, NYdb::NTable::TDescribeTableSettings()).GetValueSync();
            UNIT_ASSERT_C(describeResult.IsSuccess(), result.GetIssues().ToString());
            const auto& columnFamilies = describeResult.GetTableDescription().GetColumnFamilies();
            UNIT_ASSERT_VALUES_EQUAL(columnFamilies.size(), 2);
            for (const auto& family : columnFamilies) {
                if (family.GetName() == "Family1") {
                    UNIT_ASSERT_VALUES_EQUAL(family.GetData(), "test");
                    UNIT_ASSERT_VALUES_EQUAL(family.GetCompression(), EColumnFamilyCompression::None);
                } else {
                    UNIT_ASSERT(family.GetName() == "default");
                    UNIT_ASSERT_VALUES_EQUAL(family.GetData(), "test");
                    UNIT_ASSERT_VALUES_EQUAL(family.GetCompression(), EColumnFamilyCompression::LZ4);
                }
            }
        }

        auto queryAlter1 = TStringBuilder() << R"(
            --!syntax_v1
            ALTER TABLE `)" << tableName << R"(`
                ADD FAMILY  Family2 (
                     DATA = "test",
                     COMPRESSION = "off"
                ),
                ADD COLUMN Value3 Uint32 FAMILY Family1,
                ADD COLUMN Value4 Uint32 FAMILY Family2,
                DROP COLUMN Value2,
                ALTER COLUMN Value1 SET FAMILY Family2,
                ALTER FAMILY Family1 SET COMPRESSION "LZ4";)";
        auto resultAlter1 = session.ExecuteSchemeQuery(queryAlter1).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(resultAlter1.GetStatus(), EStatus::SUCCESS, resultAlter1.GetIssues().ToString());

        {
            auto describeResult = session.DescribeTable(tableName, NYdb::NTable::TDescribeTableSettings()).GetValueSync();
            UNIT_ASSERT_C(describeResult.IsSuccess(), result.GetIssues().ToString());
            const auto& columnFamilies = describeResult.GetTableDescription().GetColumnFamilies();
            UNIT_ASSERT_VALUES_EQUAL(columnFamilies.size(), 3);
            for (const auto& family : columnFamilies) {
                if (family.GetName() == "Family1") {
                    UNIT_ASSERT_VALUES_EQUAL(family.GetData(), "test");
                    UNIT_ASSERT_VALUES_EQUAL(family.GetCompression(), EColumnFamilyCompression::LZ4);
                } else if (family.GetName() == "Family2") {
                    UNIT_ASSERT_VALUES_EQUAL(family.GetData(), "test");
                    UNIT_ASSERT_VALUES_EQUAL(family.GetCompression(), EColumnFamilyCompression::None);
                } else {
                    UNIT_ASSERT(family.GetName() == "default");
                    UNIT_ASSERT_VALUES_EQUAL(family.GetData(), "test");
                    UNIT_ASSERT_VALUES_EQUAL(family.GetCompression(), EColumnFamilyCompression::LZ4);
                }
            }
            const auto& columns = describeResult.GetTableDescription().GetColumns();
            UNIT_ASSERT_VALUES_EQUAL(columns.size(), 4);
            for (const auto& column : columns) {
                UNIT_ASSERT_C(column.Name == "Key" || column.Name == "Value1"
                    || column.Name == "Value3" || column.Name == "Value4", column.Name);
            }
        }
    }

    Y_UNIT_TEST(CreateAndAlterTableComplex) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        auto query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE TABLE `/Root/TableToAlter` (
                Key Uint64,
                Value1 String FAMILY Family1,
                PRIMARY KEY (Key),
                FAMILY Family1 (
                     DATA = "test",
                     COMPRESSION = "off"
                ),
                FAMILY Family2 (
                     DATA = "test",
                     COMPRESSION = "lz4"
                )
            );)";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto queryAddColumn = TStringBuilder() << R"(
            --!syntax_v1
            ALTER TABLE `/Root/TableToAlter` ADD COLUMN Value2 Uint32 FAMILY Family2;)";
        auto resultAddColumn = session.ExecuteSchemeQuery(queryAddColumn).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(resultAddColumn.GetStatus(), EStatus::SUCCESS, resultAddColumn.GetIssues().ToString());

        auto queryDropColumn = TStringBuilder() << R"(
            --!syntax_v1
            ALTER TABLE `/Root/TableToAlter` DROP COLUMN Value2;)";
        auto resultDropColumn = session.ExecuteSchemeQuery(queryDropColumn).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(resultDropColumn.GetStatus(), EStatus::SUCCESS, resultDropColumn.GetIssues().ToString());
    }

    Y_UNIT_TEST(AddDropColumn) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteSchemeQuery(R"(
            --!syntax_v1
            ALTER TABLE `/Root/KeyValue` ADD Value2 String;
        )").ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto describeResult = session.DescribeTable("/Root/KeyValue").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(describeResult.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(describeResult.GetTableDescription().GetColumns().size(), 3);

        result = session.ExecuteSchemeQuery(R"(
            --!syntax_v1
            ALTER TABLE `/Root/KeyValue` DROP Value;
        )").ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        describeResult = session.DescribeTable("/Root/KeyValue").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(describeResult.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(describeResult.GetTableDescription().GetColumns().size(), 2);
    }

    Y_UNIT_TEST(DropKeyColumn) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteSchemeQuery(R"(
            --!syntax_v1
            ALTER TABLE `/Root/KeyValue` DROP Key;
        )").ExtractValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);

        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::CORE_TYPE_ANN));
        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::DEFAULT_ERROR));
    }

    Y_UNIT_TEST(DropIndexDataColumn) {
        auto setting = NKikimrKqp::TKqpSetting();
        TKikimrRunner kikimr({setting});
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        CreateSampleTablesWithIndex(session);

        const TString query1(R"(
            ALTER TABLE `/Root/SecondaryWithDataColumns` DROP COLUMN Value;
        )");

        auto result = session.ExecuteSchemeQuery(
            query1)
        .ExtractValueSync();

        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::PRECONDITION_FAILED);
    }

    Y_UNIT_TEST(PathWithNoRoot) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteSchemeQuery(R"(
            --!syntax_v1
            CREATE TABLE `/TablePathWithNoRoot` (
                Id Uint32,
                Value String,
                PRIMARY KEY (Id)
            );
        )").ExtractValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);

        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::CORE_EXEC));
        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::DEFAULT_ERROR));
    }

    Y_UNIT_TEST(CreateTableWithDecimalColumn) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString tableName = "/Root/TableWithDecimalColumn";
        {
            auto query = TStringBuilder() << R"(
            CREATE TABLE `)" << tableName << R"(` (
                Key Uint64,
                Value Decimal(35,9),
                PRIMARY KEY (Key)
            );)";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
            UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_BAD_COLUMN_TYPE));
        }
        {
            auto query = TStringBuilder() << R"(
            CREATE TABLE `)" << tableName << R"(` (
                Key Uint64,
                Value Decimal(22,20),
                PRIMARY KEY (Key)
            );)";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
            UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_BAD_COLUMN_TYPE));
        }
        {
            auto query = TStringBuilder() << R"(
            CREATE TABLE `)" << tableName << R"(` (
                Key Uint64,
                Value Decimal(22,9),
                PRIMARY KEY (Key)
            );)";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            TDescribeTableResult describe = session.DescribeTable(tableName).GetValueSync();
            UNIT_ASSERT_EQUAL(describe.GetStatus(), EStatus::SUCCESS);
            auto tableDesc = describe.GetTableDescription();
            TVector<TTableColumn> columns = tableDesc.GetTableColumns();
            UNIT_ASSERT_VALUES_EQUAL(columns.size(), 2);
            TType valueType = columns[1].Type;
            TTypeParser parser(valueType);
            auto optionalKind = parser.GetKind();
            UNIT_ASSERT_EQUAL(optionalKind, TTypeParser::ETypeKind::Optional);
            parser.OpenOptional();
            auto kind = parser.GetKind();
            UNIT_ASSERT_EQUAL(kind, TTypeParser::ETypeKind::Decimal);
            TDecimalType decimalType = parser.GetDecimal();
            UNIT_ASSERT_EQUAL(decimalType.Precision, 22);
            UNIT_ASSERT_EQUAL(decimalType.Scale, 9);
        }
    }

    void AlterTableAddIndex(EIndexTypeSql type, bool enableAsyncIndexes = false) {
        TKikimrRunner kikimr(TKikimrSettings().SetEnableAsyncIndexes(enableAsyncIndexes));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        CreateSampleTablesWithIndex(session);

        const auto typeStr = IndexTypeSqlString(type);
        const auto expectedStatus = (type == EIndexTypeSql::GlobalAsync)
            ? (enableAsyncIndexes ? EStatus::SUCCESS : EStatus::UNSUPPORTED)
            : EStatus::SUCCESS;

        {
            auto status = session.ExecuteSchemeQuery(Sprintf(R"(
                --!syntax_v1
                ALTER TABLE `/Root/Test` ADD INDEX NameIndex %s ON (Name);
            )", typeStr.data())).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(status.GetStatus(), expectedStatus, status.GetIssues().ToString());
        }

        {
            TDescribeTableResult describe = session.DescribeTable("/Root/Test").GetValueSync();
            UNIT_ASSERT_EQUAL(describe.GetStatus(), EStatus::SUCCESS);
            auto indexDesc = describe.GetTableDescription().GetIndexDescriptions();

            if (expectedStatus != EStatus::SUCCESS) {
                UNIT_ASSERT_VALUES_EQUAL(indexDesc.size(), 0);
                return;
            }

            UNIT_ASSERT_VALUES_EQUAL(indexDesc.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.back().GetIndexName(), "NameIndex");
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.back().GetIndexType(), IndexTypeSqlToIndexType(type));
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.back().GetIndexColumns().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.back().GetDataColumns().size(), 0);
        }

        {
            auto status = session.ExecuteSchemeQuery(R"(
                --!syntax_v1
                ALTER TABLE `/Root/Test` DROP INDEX NameIndex;
            )").ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(status.GetStatus(), EStatus::SUCCESS, status.GetIssues().ToString());
        }

        {
            TDescribeTableResult describe = session.DescribeTable("/Root/Test").GetValueSync();
            UNIT_ASSERT_EQUAL(describe.GetStatus(), EStatus::SUCCESS);
            auto indexDesc = describe.GetTableDescription().GetIndexDescriptions();
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.size(), 0);
        }

        {
            auto status = session.ExecuteSchemeQuery(Sprintf(R"(
                --!syntax_v1
                ALTER TABLE `/Root/Test` ADD INDEX NameIndex %s ON (Name) COVER (Amount);
            )", typeStr.data())).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(status.GetStatus(), EStatus::SUCCESS, status.GetIssues().ToString());
        }

        {
            TDescribeTableResult describe = session.DescribeTable("/Root/Test").GetValueSync();
            UNIT_ASSERT_EQUAL(describe.GetStatus(), EStatus::SUCCESS);
            auto indexDesc = describe.GetTableDescription().GetIndexDescriptions();
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.back().GetIndexName(), "NameIndex");
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.back().GetIndexType(), IndexTypeSqlToIndexType(type));
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.back().GetIndexColumns().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.back().GetDataColumns().size(), 1);
        }
    }

    Y_UNIT_TEST(AlterTableAddImplicitSyncIndex) {
        AlterTableAddIndex(EIndexTypeSql::Global);
    }

    Y_UNIT_TEST(AlterTableAddExplicitSyncIndex) {
        AlterTableAddIndex(EIndexTypeSql::GlobalSync);
    }

    Y_UNIT_TEST(AlterTableAddAsyncIndexShouldFail) {
        AlterTableAddIndex(EIndexTypeSql::GlobalAsync);
    }

    Y_UNIT_TEST(AlterTableAddAsyncIndexShouldSucceed) {
        AlterTableAddIndex(EIndexTypeSql::GlobalAsync, true);
    }

    Y_UNIT_TEST(AlterTableWithDecimalColumn) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString tableName = "/Root/TableWithDecimalColumn";
        {
            auto query = TStringBuilder() << R"(
            CREATE TABLE `)" << tableName << R"(` (
                Key Uint64,
                Value1 String,
                PRIMARY KEY (Key)
            );)";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto query = TStringBuilder() << R"(
            ALTER TABLE `)" << tableName << R"(`
                 ADD COLUMN Value2 Decimal(35,9);
            )";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
            UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_BAD_COLUMN_TYPE));
        }
        {
            auto query = TStringBuilder() << R"(
            ALTER TABLE `)" << tableName << R"(`
                 ADD COLUMN Value2 Decimal(22,20);
            )";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
            UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_BAD_COLUMN_TYPE));
        }
        {
            auto query = TStringBuilder() << R"(
            ALTER TABLE `)" << tableName << R"(`
                 ADD COLUMN Value2 Decimal(22,9);
            )";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            TDescribeTableResult describe = session.DescribeTable(tableName).GetValueSync();
            UNIT_ASSERT_EQUAL(describe.GetStatus(), EStatus::SUCCESS);
            auto tableDesc = describe.GetTableDescription();
            TVector<TTableColumn> columns = tableDesc.GetTableColumns();
            UNIT_ASSERT_VALUES_EQUAL(columns.size(), 3);
            TType valueType = columns[2].Type;
            TTypeParser parser(valueType);
            auto optionalKind = parser.GetKind();
            UNIT_ASSERT_EQUAL(optionalKind, TTypeParser::ETypeKind::Optional);
            parser.OpenOptional();
            auto kind = parser.GetKind();
            UNIT_ASSERT_EQUAL(kind, TTypeParser::ETypeKind::Decimal);
            TDecimalType decimalType = parser.GetDecimal();
            UNIT_ASSERT_EQUAL(decimalType.Precision, 22);
            UNIT_ASSERT_EQUAL(decimalType.Scale, 9);
        }
    }

    Y_UNIT_TEST(CreateUserWithPassword) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE USER user1 ENCRYPTED PASSWORD 'password1';
            )";
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE USER user1 PASSWORD NULL;
            )";
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(CreateUserWithoutPassword) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE USER user1;
            )";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(CreateAndDropUser) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        {
            // Drop non-existing user force
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            DROP USER IF EXISTS user1;
            )";
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE USER user1 PASSWORD 'password1';
            )";
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            // Drop existing user
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            DROP USER user1;
            )";
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE USER user1 PASSWORD NULL;
            )";
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            // Drop existing user force
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            DROP USER IF EXISTS user1;
            )";
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            // Drop non-existing user
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            DROP USER user1;
            )";
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(AlterUser) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE USER user1 PASSWORD 'password1';
            )";
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            ALTER USER user1 WITH PASSWORD 'password2';
            )";
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            ALTER USER user1 WITH ENCRYPTED PASSWORD 'password3';
            )";
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            ALTER USER user1 WITH PASSWORD NULL;
            )";
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(CreateAndDropGroup) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        {
            // Drop non-existing group force
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            DROP GROUP IF EXISTS group1;
            )";
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE GROUP group1;
            )";
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            // Drop existing group
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            DROP GROUP group1;
            )";
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE GROUP group1;
            )";
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            // Drop existing group force
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            DROP GROUP IF EXISTS group1;
            )";
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            // Drop non-existing group
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            DROP GROUP group1;
            )";
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(AlterGroup) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE USER user1 PASSWORD 'password1';
            CREATE USER user2 PASSWORD 'password2';
            CREATE USER user3;
            )";
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE GROUP group1;
            )";
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            ALTER GROUP group1 ADD USER user1;
            )";
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            ALTER GROUP group1 DROP USER user1;
            )";
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            ALTER GROUP group1 ADD USER user1, user2;
            )";
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            ALTER GROUP group1 DROP USER user1, user2;
            )";
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
    }

    static NKikimrPQ::TPQConfig DefaultPQConfig() {
        NKikimrPQ::TPQConfig pqConfig;
        pqConfig.SetEnabled(true);
        pqConfig.SetEnableProtoSourceIdInfo(true);
        pqConfig.SetTopicsAreFirstClassCitizen(true);
        pqConfig.AddClientServiceType()->SetName("data-streams");
        return pqConfig;
    }

    static const char* ModeToString(EChangefeedMode mode) {
        switch (mode) {
        case EChangefeedMode::KeysOnly:
            return "KEYS_ONLY";
        case EChangefeedMode::Updates:
            return "UPDATES";
        case EChangefeedMode::NewImage:
            return "NEW_IMAGE";
        case EChangefeedMode::OldImage:
            return "OLD_IMAGE";
        case EChangefeedMode::NewAndOldImages:
            return "NEW_AND_OLD_IMAGES";
        case EChangefeedMode::Unknown:
            UNIT_ASSERT(false);
            return "";
        }
    }

    static const char* FormatToString(EChangefeedFormat format) {
        switch (format) {
        case EChangefeedFormat::Json:
            return "JSON";
        case EChangefeedFormat::Unknown:
            UNIT_ASSERT(false);
            return "";
        }
    }

    void AddChangefeed(EChangefeedMode mode, EChangefeedFormat format) {
        TKikimrRunner kikimr(TKikimrSettings()
            .SetPQConfig(DefaultPQConfig())
            .SetEnableChangefeeds(true));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        {
            auto query = R"(
                --!syntax_v1
                CREATE TABLE `/Root/table` (
                    Key Uint64,
                    Value String,
                    PRIMARY KEY (Key)
                );
            )";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto query = Sprintf(R"(
                --!syntax_v1
                ALTER TABLE `/Root/table` ADD CHANGEFEED `feed` WITH (
                    MODE = '%s', FORMAT = '%s'
                );
            )", ModeToString(mode), FormatToString(format));

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto describeResult = session.DescribeTable("/Root/table").GetValueSync();
            UNIT_ASSERT_C(describeResult.IsSuccess(), describeResult.GetIssues().ToString());

            const auto& changefeeds = describeResult.GetTableDescription().GetChangefeedDescriptions();
            UNIT_ASSERT_VALUES_EQUAL(changefeeds.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(changefeeds.at(0), TChangefeedDescription("feed", mode, format));
        }

        {
            auto query = R"(
                --!syntax_v1
                ALTER TABLE `/Root/table` DROP CHANGEFEED `feed`;
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(AddChangefeed) {
        for (auto mode : GetEnumAllValues<EChangefeedMode>()) {
            if (mode == EChangefeedMode::Unknown) {
                continue;
            }

            for (auto format : GetEnumAllValues<EChangefeedFormat>()) {
                if (format == EChangefeedFormat::Unknown) {
                    continue;
                }

                AddChangefeed(mode, format);
            }
        }
    }

    Y_UNIT_TEST(AddChangefeedWhenDisabled) {
        TKikimrRunner kikimr(TKikimrSettings()
            .SetPQConfig(DefaultPQConfig())
            .SetEnableChangefeeds(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        {
            auto query = R"(
                --!syntax_v1
                CREATE TABLE `/Root/table` (
                    Key Uint64,
                    Value String,
                    PRIMARY KEY (Key)
                );
            )";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto query = R"(
                --!syntax_v1
                ALTER TABLE `/Root/table` ADD CHANGEFEED `feed` WITH (
                    MODE = 'KEYS_ONLY', FORMAT = 'JSON'
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::UNSUPPORTED, result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(AddChangefeedNegative) {
        TKikimrRunner kikimr(TKikimrSettings()
            .SetPQConfig(DefaultPQConfig())
            .SetEnableChangefeeds(true));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        {
            auto query = R"(
                --!syntax_v1
                ALTER TABLE `/Root/table` ADD CHANGEFEED `feed` WITH (
                    MODE = 'KEYS_ONLY', FORMAT = 'JSON'
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SCHEME_ERROR, result.GetIssues().ToString());
        }

        {
            auto query = R"(
                --!syntax_v1
                CREATE TABLE `/Root/table` (
                    Key Uint64,
                    Value String,
                    PRIMARY KEY (Key)
                );
            )";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto query = R"(
                --!syntax_v1
                ALTER TABLE `/Root/table` ADD CHANGEFEED `feed` WITH (
                    MODE = 'FOO', FORMAT = 'JSON'
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
        }

        {
            auto query = R"(
                --!syntax_v1
                ALTER TABLE `/Root/table` ADD CHANGEFEED `feed` WITH (
                    MODE = 'KEYS_ONLY', FORMAT = 'BAR'
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
        }

        {
            auto query = R"(
                --!syntax_v1
                ALTER TABLE `/Root/table` ADD CHANGEFEED `feed` WITH (
                    MODE = 'KEYS_ONLY', FORMAT = 'JSON', BAZ = 'BAR'
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(DropChangefeedNegative) {
        TKikimrRunner kikimr(TKikimrSettings()
            .SetPQConfig(DefaultPQConfig())
            .SetEnableChangefeeds(true));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        {
            auto query = R"(
                --!syntax_v1
                ALTER TABLE `/Root/table` DROP CHANGEFEED `feed`;
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SCHEME_ERROR, result.GetIssues().ToString());
        }

        {
            auto query = R"(
                --!syntax_v1
                CREATE TABLE `/Root/table` (
                    Key Uint64,
                    Value String,
                    PRIMARY KEY (Key)
                );
            )";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto query = R"(
                --!syntax_v1
                ALTER TABLE `/Root/table` DROP CHANGEFEED `feed`;
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SCHEME_ERROR, result.GetIssues().ToString());
        }
    }
}

} // namespace NKqp
} // namespace NKikimr

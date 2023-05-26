#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>
#include <ydb/public/sdk/cpp/client/ydb_scheme/scheme.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>
#include <ydb/public/sdk/cpp/client/draft/ydb_long_tx.h>
#include <ydb/core/testlib/cs_helper.h>

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
            SELECT * FROM `/Root/KeyValue`;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(),
            useSchemeCacheMeta ? EStatus::SCHEME_ERROR : EStatus::UNAUTHORIZED, result.GetIssues().ToString());

        result = session.ExecuteDataQuery(R"(
            SELECT * FROM `/Root/NonExistent`;
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
            SELECT * FROM `/Root/KeyValue`;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        result = session.ExecuteDataQuery(R"(
            SELECT * FROM `/Root/NonExistent`;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
    }

    Y_UNIT_TEST(UseDroppedTable) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(
            SELECT * FROM `/Root/KeyValue`;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        auto schemeResult = session.ExecuteSchemeQuery(R"(
            DROP TABLE `/Root/KeyValue`;
        )").ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(schemeResult.GetStatus(), EStatus::SUCCESS);

        result = session.ExecuteDataQuery(R"(
            SELECT * FROM `/Root/KeyValue`;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
    }

    Y_UNIT_TEST(CreateDroppedTable) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto schemeResult = session.ExecuteSchemeQuery(R"(
            DROP TABLE `/Root/KeyValue`;
        )").ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(schemeResult.GetStatus(), EStatus::SUCCESS);

        schemeResult = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/KeyValue` (
                Key Uint32,
                Value String,
                PRIMARY KEY(Key)
            );
        )").ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(schemeResult.GetStatus(), EStatus::SUCCESS);

        auto result = session.ExecuteDataQuery(R"(
            SELECT * FROM `/Root/KeyValue`;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    Y_UNIT_TEST(CreateDropTableMultipleTime) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();

        const size_t inflight = 4;
        const size_t limit = 1000;

        const static TString createTableQuery = R"(
            CREATE TABLE `/Root/Test1234/KeyValue` (
                Key Uint32,
                Value String,
                PRIMARY KEY(Key)
            );
        )";

        const static TString dropTableQuery = R"(
            DROP TABLE `/Root/Test1234/KeyValue`;
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
                        ALTER TABLE `/Root/EightShard` DROP COLUMN Data;
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
                            SELECT * FROM `/Root/EightShard` WHERE Key = 501u;
                        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                    }, retrySettings);
                    UNIT_ASSERT_VALUES_EQUAL_C(status.GetStatus(), EStatus::SUCCESS, status.GetIssues().ToString());
                } else {
                    // Planned
                    auto status = db.RetryOperationSync([](TSession session) {
                        return session.ExecuteDataQuery(R"(
                            SELECT * FROM `/Root/EightShard`;
                        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                    }, retrySettings);
                    UNIT_ASSERT_VALUES_EQUAL_C(status.GetStatus(), EStatus::SUCCESS, status.GetIssues().ToString());
                }
            }
        }, 0, Inflight + 1, NPar::TLocalExecutor::WAIT_COMPLETE | NPar::TLocalExecutor::MED_PRIORITY);
    }

    void SchemaVersionMissmatchWithTest(bool write) {
        TKikimrRunner kikimr;

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        TString query;
        if (write) {
            query = Q_(R"(
                UPSERT INTO `/Root/KeyValue` (Key, Value) VALUES (10u, "New");
            )");
        } else {
            query = Q_(R"(
                SELECT * FROM `/Root/KeyValue` WHERE Value = "New";
            )");
        }

        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.KeepInQueryCache(true);
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

        {
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(),
                execSettings).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
            UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), false);
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
                UPSERT INTO `/Root/KeyValue` (Key, Value) VALUES (10u, "New");
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
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto commit = result.GetTransaction()->Commit().GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(commit.GetStatus(), EStatus::SUCCESS, commit.GetIssues().ToString());
        }

        {
            auto result = session.ExecuteDataQuery(query,
                TTxControl::BeginTx(), execSettings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto commit = result.GetTransaction()->Commit().GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(commit.GetStatus(), EStatus::SUCCESS, commit.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(SchemaVersionMissmatchWithRead) {
        SchemaVersionMissmatchWithTest(false);
    }

    Y_UNIT_TEST(SchemaVersionMissmatchWithWrite) {
        SchemaVersionMissmatchWithTest(true);
    }

    Y_UNIT_TEST(SchemaVersionMissmatchWithIndexRead) {
        SchemaVersionMissmatchWithIndexTest(false);
    }

    Y_UNIT_TEST(SchemaVersionMissmatchWithIndexWrite) {
        SchemaVersionMissmatchWithIndexTest(true);
    }

    void TouchIndexAfterMoveIndex(bool write, bool replace) {
        TKikimrRunner kikimr;

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        TString query1;
        TString query2;
        if (write) {
            query1 = Q_(R"(
                UPSERT INTO `/Root/KeyValue` (Key, Value) VALUES (10u, "New");
            )");
            query2 = query1;
        } else {
            query1 = Q1_(R"(
                SELECT * FROM `/Root/KeyValue` VIEW `value_index` WHERE Value = "New";
            )");
            query2 = Q1_(R"(
                SELECT * FROM `/Root/KeyValue` VIEW `moved_value_index` WHERE Value = "New";
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

        if (replace) {
            TString create_index_query = Q1_(R"(
                ALTER TABLE `/Root/KeyValue` ADD INDEX moved_value_index GLOBAL SYNC ON (`Value`);
            )");
            auto result = session.ExecuteSchemeQuery(create_index_query).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = session.ExecuteDataQuery(query1, TTxControl::BeginTx(),
                execSettings).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
            UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), false);
        }

        {
            kikimr.GetTestServer().GetRuntime()->GetAppData().AdministrationAllowedSIDs.push_back("root@builtin");
            auto reply = kikimr.GetTestClient().MoveIndex("/Root/KeyValue", "value_index", "moved_value_index", true, "root@builtin");
            const NKikimrClient::TResponse &response = reply->Record;
            UNIT_ASSERT_VALUES_EQUAL((NMsgBusProxy::EResponseStatus)response.GetStatus(), NMsgBusProxy::MSTATUS_OK);
        }

        {
            auto result = session.ExecuteDataQuery(query2,
                TTxControl::BeginTx(), execSettings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            if (write) {
                auto commit = result.GetTransaction()->Commit().GetValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(commit.GetStatus(), EStatus::SUCCESS, commit.GetIssues().ToString());
            }
        }

        {
            auto result = session.ExecuteDataQuery(query2,
                TTxControl::BeginTx(), execSettings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto commit = result.GetTransaction()->Commit().GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(commit.GetStatus(), EStatus::SUCCESS, commit.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(TouchIndexAfterMoveIndexRead) {
        TouchIndexAfterMoveIndex(false, false);
    }

    Y_UNIT_TEST(TouchIndexAfterMoveIndexWrite) {
        TouchIndexAfterMoveIndex(true, false);
    }

    Y_UNIT_TEST(TouchIndexAfterMoveIndexReadReplace) {
        TouchIndexAfterMoveIndex(false, true);
    }

    Y_UNIT_TEST(TouchIndexAfterMoveIndexWriteReplace) {
        TouchIndexAfterMoveIndex(true, true);
    }

    void TouchIndexAfterMoveTable(bool write) {
        TKikimrRunner kikimr;

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        TString query1;
        TString query2;
        if (write) {
            query1 = Q_(R"(
                UPSERT INTO `/Root/KeyValue` (Key, Value) VALUES (10u, "New");
            )");
            query2 = Q_(R"(
                UPSERT INTO `/Root/KeyValueMoved` (Key, Value) VALUES (10u, "New");
            )");
        } else {
            query1 = Q1_(R"(
                SELECT * FROM `/Root/KeyValue` VIEW `value_index` WHERE Value = "New";
            )");
            query2 = Q1_(R"(
                SELECT * FROM `/Root/KeyValueMoved` VIEW `value_index` WHERE Value = "New";
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
            auto result = session.ExecuteDataQuery(query1, TTxControl::BeginTx(),
                execSettings).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
            UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), false);
        }

        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            ALTER TABLE `/Root/KeyValue` RENAME TO `/Root/KeyValueMoved`;
            )";

            const auto result = session.ExecuteSchemeQuery(query << ";").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = session.ExecuteDataQuery(query2,
                TTxControl::BeginTx(), execSettings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto commit = result.GetTransaction()->Commit().GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(commit.GetStatus(), EStatus::SUCCESS, commit.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(TouchIndexAfterMoveTableRead) {
        TouchIndexAfterMoveTable(false);
    }

    Y_UNIT_TEST(TouchIndexAfterMoveTableWrite) {
        TouchIndexAfterMoveTable(true);
    }

    void CheckInvalidationAfterDropCreateTable(bool withCompatSchema) {
        TKikimrRunner kikimr;

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        const TString sql = "UPSERT INTO `/Root/KeyValue` (Key, Value) VALUES(1, \"One\")";

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
            ? "SELECT * FROM `/Root/KeyValue`;"
            : "UPSERT INTO `/Root/KeyValue` (Key, Value) VALUES(1, \"One\")";

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
            ALTER TABLE `/Root/table` RENAME TO `/Root/second`;
            )";

            const auto result = session.ExecuteSchemeQuery(query << ";").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
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
            EStatus::BAD_REQUEST, "Cannot enable TTL on unknown column");

        AlterTableSetttings(session, tableName, {{"TTL", R"(Interval("P0D") ON StringValue)"}}, compat,
            EStatus::BAD_REQUEST, "Unsupported column type");

        AlterTableSetttings(session, tableName, {{"TTL", R"(Interval("P0D") ON Uint32Value)"}}, compat,
            EStatus::BAD_REQUEST, "'ValueSinceUnixEpochModeSettings' should be specified");
        AlterTableSetttings(session, tableName, {{"TTL", R"(Interval("P0D") ON Uint64Value)"}}, compat,
            EStatus::BAD_REQUEST, "'ValueSinceUnixEpochModeSettings' should be specified");
        AlterTableSetttings(session, tableName, {{"TTL", R"(Interval("P0D") ON DyNumberValue)"}}, compat,
            EStatus::BAD_REQUEST, "'ValueSinceUnixEpochModeSettings' should be specified");

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
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
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

    Y_UNIT_TEST(CreateTableWithPartitionAtKeysSigned) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString tableName = "/Root/TableWithPartitionAtKeysSigned";
        auto query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE TABLE `)" << tableName << R"(` (
                Key1 Int64,
                Key2 String,
                Value String,
                PRIMARY KEY (Key1, Key2)
            )
            WITH (
                PARTITION_AT_KEYS = ( 0, 10, 10000 )
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
            return parser.GetOptionalInt64().GetRef();
        };

        const TVector<TKeyRange>& keyRanges = describeResult.GetTableDescription().GetKeyRanges();

        size_t n = 0;
        const TVector<i64> expectedRanges = { 0l, 10l, 10000l };

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

    void AlterTableAddIndex(EIndexTypeSql type) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        CreateSampleTablesWithIndex(session);

        const auto typeStr = IndexTypeSqlString(type);

        {
            auto status = session.ExecuteSchemeQuery(Sprintf(R"(
                --!syntax_v1
                ALTER TABLE `/Root/Test` ADD INDEX NameIndex %s ON (Name);
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

    Y_UNIT_TEST(AlterTableAddExplicitAsyncIndex) {
        AlterTableAddIndex(EIndexTypeSql::GlobalAsync);
    }

    Y_UNIT_TEST(AlterTableRenameIndex) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        CreateSampleTablesWithIndex(session);
        {
            auto status = session.ExecuteSchemeQuery(R"(
                --!syntax_v1
                ALTER TABLE `/Root/SecondaryKeys` RENAME INDEX Index TO RenamedIndex;
            )").ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(status.GetStatus(), EStatus::SUCCESS, status.GetIssues().ToString());
        }

        {
            TDescribeTableResult describe = session.DescribeTable("/Root/SecondaryKeys").GetValueSync();
            UNIT_ASSERT_EQUAL(describe.GetStatus(), EStatus::SUCCESS);
            auto indexDesc = describe.GetTableDescription().GetIndexDescriptions();
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.back().GetIndexName(), "RenamedIndex");
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.back().GetIndexColumns().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.back().GetDataColumns().size(), 0);
        }
    }

    Y_UNIT_TEST(AlterTableReplaceIndex) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        CreateSampleTablesWithIndex(session);

        {
            TString create_index_query = R"(
                --!syntax_v1
                ALTER TABLE `/Root/SecondaryKeys` ADD INDEX ValueIndex GLOBAL SYNC ON (`Value`);
            )";
            auto result = session.ExecuteSchemeQuery(create_index_query).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto status = session.ExecuteSchemeQuery(R"(
                --!syntax_v1
                ALTER TABLE `/Root/SecondaryKeys` RENAME INDEX Index TO ValueIndex;
            )").ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(status.GetStatus(), EStatus::SCHEME_ERROR, status.GetIssues().ToString());
        }
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

    Y_UNIT_TEST(FamilyColumnTest) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString tableName = "/Root/TableFamiliesTest";
        auto query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE TABLE `)" << tableName << R"(` (
                Key Uint64,
                Value String,
                FAMILY Uint32,
                PRIMARY KEY (Key)
            );)";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto describeResult = session.DescribeTable(tableName, NYdb::NTable::TDescribeTableSettings()).GetValueSync();
        UNIT_ASSERT_C(describeResult.IsSuccess(), result.GetIssues().ToString());
        const auto tableDesc = session.DescribeTable(tableName).GetValueSync().GetTableDescription();
        TVector<TTableColumn> columns = tableDesc.GetTableColumns();
        UNIT_ASSERT_VALUES_EQUAL(columns.size(), 3);
        TTableColumn& familyColumn = columns[2];
        UNIT_ASSERT_EQUAL(familyColumn.Name, "FAMILY");
        TTypeParser parser(familyColumn.Type);
        auto optionalKind = parser.GetKind();
        UNIT_ASSERT_EQUAL(optionalKind, TTypeParser::ETypeKind::Optional);
        parser.OpenOptional();
        auto kind = parser.GetKind();
        UNIT_ASSERT_EQUAL(kind, TTypeParser::ETypeKind::Primitive);
        auto primitive = parser.GetPrimitive();
        UNIT_ASSERT_EQUAL(primitive, EPrimitiveType::Uint32);

        const auto& columnFamilies = tableDesc.GetColumnFamilies();
        UNIT_ASSERT_VALUES_EQUAL(columnFamilies.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(columnFamilies[0].GetName(), "default");
    }

    Y_UNIT_TEST(UnknownFamilyTest) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString tableName = "/Root/TableFamiliesTest";
        auto query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE TABLE `)" << tableName << R"(` (
                Key Uint64,
                Value1 String FAMILY Family1,
                PRIMARY KEY (Key)
            );)";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
    }

    Y_UNIT_TEST(TwoSimilarFamiliesTest) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString tableName = "/Root/TableFamiliesTest";
        auto query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE TABLE `)" << tableName << R"(` (
                Key Uint64,
                Value1 String FAMILY Family1,
                PRIMARY KEY (Key),
                FAMILY Family1 (),
                FAMILY Family1 ()
            );)";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
    }

    static NKikimrPQ::TPQConfig DefaultPQConfig() {
        NKikimrPQ::TPQConfig pqConfig;
        pqConfig.SetEnabled(true);
        pqConfig.SetEnableProtoSourceIdInfo(true);
        pqConfig.SetTopicsAreFirstClassCitizen(true);
        pqConfig.SetRequireCredentialsInNewProtocol(false);
        pqConfig.AddClientServiceType()->SetName("data-streams");
        return pqConfig;
    }

    void AddChangefeed(EChangefeedMode mode, EChangefeedFormat format) {
        TKikimrRunner kikimr(TKikimrSettings()
            .SetPQConfig(DefaultPQConfig())
            .SetEnableChangefeedDynamoDBStreamsFormat(true));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        {
            auto builder = TTableBuilder()
                .AddNullableColumn("Key", EPrimitiveType::Uint64)
                .AddNullableColumn("Value", EPrimitiveType::String)
                .SetPrimaryKeyColumn("Key");

            if (format == EChangefeedFormat::DynamoDBStreamsJson) {
                builder.AddAttribute("__document_api_version", "1");
            }

            auto result = session.CreateTable("/Root/table", builder.Build()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        auto execOpts = TExecSchemeQuerySettings();
        if (format == EChangefeedFormat::DynamoDBStreamsJson) {
            execOpts.RequestType("_document_api_request");
        }

        {
            auto query = Sprintf(R"(
                --!syntax_v1
                ALTER TABLE `/Root/table` ADD CHANGEFEED `feed` WITH (
                    MODE = '%s', FORMAT = '%s'
                );
            )", ToString(mode).c_str(), ToString(format).c_str());

            const auto result = session.ExecuteSchemeQuery(query, execOpts).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto describeResult = session.DescribeTable("/Root/table").GetValueSync();
            UNIT_ASSERT_C(describeResult.IsSuccess(), describeResult.GetIssues().ToString());

            const auto& changefeeds = describeResult.GetTableDescription().GetChangefeedDescriptions();
            UNIT_ASSERT_VALUES_EQUAL(changefeeds.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(changefeeds.at(0), TChangefeedDescription("feed", mode, format));
            UNIT_ASSERT_VALUES_EQUAL(changefeeds.at(0).GetState(), EChangefeedState::Enabled);
        }

        {
            auto query = R"(
                --!syntax_v1
                ALTER TABLE `/Root/table` DROP CHANGEFEED `feed`;
            )";

            const auto result = session.ExecuteSchemeQuery(query, execOpts).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(AddChangefeed) {
        for (auto mode : GetEnumAllValues<EChangefeedMode>()) {
            if (mode == EChangefeedMode::Unknown) {
                continue;
            }

            for (auto format : GetEnumAllValues<EChangefeedFormat>()) {
                switch (format) {
                case EChangefeedFormat::Unknown:
                    continue;
                case EChangefeedFormat::DynamoDBStreamsJson:
                    if (mode == EChangefeedMode::Updates) {
                        continue;
                    }
                    break;
                default:
                    break;
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
            .SetEnableChangefeedDynamoDBStreamsFormat(true));
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

        {
            auto result = session.CreateTable("/Root/document-table", TTableBuilder()
                .AddNullableColumn("Key", EPrimitiveType::Uint64)
                .AddNullableColumn("Value", EPrimitiveType::String)
                .SetPrimaryKeyColumn("Key")
                .AddAttribute("__document_api_version", "1")
                .Build()
            ).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto query = R"(
                --!syntax_v1
                ALTER TABLE `/Root/document-table` ADD CHANGEFEED `feed` WITH (
                    MODE = 'UPDATES', FORMAT = 'DYNAMODB_STREAMS_JSON'
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query, TExecSchemeQuerySettings().RequestType("_document_api_request")).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(ChangefeedAwsRegion) {
        TKikimrRunner kikimr(TKikimrSettings()
            .SetPQConfig(DefaultPQConfig())
            .SetEnableChangefeedDynamoDBStreamsFormat(true));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        {
            auto result = session.CreateTable("/Root/table", TTableBuilder()
                .AddNullableColumn("Key", EPrimitiveType::Uint64)
                .AddNullableColumn("Value", EPrimitiveType::String)
                .SetPrimaryKeyColumn("Key")
                .AddAttribute("__document_api_version", "1")
                .Build()
            ).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto query = R"(
                --!syntax_v1
                ALTER TABLE `/Root/table` ADD CHANGEFEED `feed` WITH (
                    MODE = 'NEW_AND_OLD_IMAGES', FORMAT = 'DYNAMODB_STREAMS_JSON', AWS_REGION = 'aws:region'
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query, TExecSchemeQuerySettings().RequestType("_document_api_request")).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto describeResult = session.DescribeTable("/Root/table").GetValueSync();
            UNIT_ASSERT_C(describeResult.IsSuccess(), describeResult.GetIssues().ToString());

            const auto& changefeeds = describeResult.GetTableDescription().GetChangefeedDescriptions();
            UNIT_ASSERT_VALUES_EQUAL(changefeeds.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(changefeeds.at(0).GetAwsRegion(), "aws:region");
        }
    }

    Y_UNIT_TEST(DropChangefeedNegative) {
        TKikimrRunner kikimr(TKikimrSettings().SetPQConfig(DefaultPQConfig()));
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

    Y_UNIT_TEST(ChangefeedRetentionPeriod) {
        using namespace NTopic;

        TKikimrRunner kikimr(TKikimrSettings().SetPQConfig(DefaultPQConfig()));
        auto pq = TTopicClient(kikimr.GetDriver(), TTopicClientSettings().Database("/Root"));
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

        { // default (1d)
            auto query = R"(
                --!syntax_v1
                ALTER TABLE `/Root/table` ADD CHANGEFEED `feed_1` WITH (
                    MODE = 'KEYS_ONLY', FORMAT = 'JSON'
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto desc = pq.DescribeTopic("/Root/table/feed_1").ExtractValueSync();
            UNIT_ASSERT_C(desc.IsSuccess(), desc.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(desc.GetTopicDescription().GetRetentionPeriod(), TDuration::Days(1));
        }

        { // custom (1h)
            auto query = R"(
                --!syntax_v1
                ALTER TABLE `/Root/table` ADD CHANGEFEED `feed_2` WITH (
                    MODE = 'KEYS_ONLY', FORMAT = 'JSON', RETENTION_PERIOD = Interval("PT1H")
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto desc = pq.DescribeTopic("/Root/table/feed_2").ExtractValueSync();
            UNIT_ASSERT_C(desc.IsSuccess(), desc.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(desc.GetTopicDescription().GetRetentionPeriod(), TDuration::Hours(1));
        }

        { // non-positive (invalid)
            auto query = R"(
                --!syntax_v1
                ALTER TABLE `/Root/table` ADD CHANGEFEED `feed_3` WITH (
                    MODE = 'KEYS_ONLY', FORMAT = 'JSON', RETENTION_PERIOD = Interval("PT0H")
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
        }

        { // too much (32d)
            auto query = R"(
                --!syntax_v1
                ALTER TABLE `/Root/table` ADD CHANGEFEED `feed_3` WITH (
                    MODE = 'KEYS_ONLY', FORMAT = 'JSON', RETENTION_PERIOD = Interval("P32D")
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(ChangefeedAttributes) {
        TKikimrRunner kikimr(TKikimrSettings().SetPQConfig(DefaultPQConfig()));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().ExtractValueSync().GetSession();

        {
            auto result = session.CreateTable("/Root/table", TTableBuilder()
                .AddNullableColumn("Key", EPrimitiveType::Uint64)
                .AddNullableColumn("Value", EPrimitiveType::String)
                .SetPrimaryKeyColumn("Key")
                .Build()
            ).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        const auto changefeed = TChangefeedDescription("feed", EChangefeedMode::KeysOnly, EChangefeedFormat::Json)
            .AddAttribute("key", "value");

        {
            auto result = session.AlterTable("/Root/table", TAlterTableSettings()
                .AppendAddChangefeeds(changefeed)
            ).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = session.DescribeTable("/Root/table").ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            const auto& changefeeds = result.GetTableDescription().GetChangefeedDescriptions();
            UNIT_ASSERT_VALUES_EQUAL(changefeeds.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(changefeeds.at(0), changefeed);
            UNIT_ASSERT_VALUES_EQUAL(changefeeds.at(0).GetAttributes(), changefeed.GetAttributes());
        }
    }

    Y_UNIT_TEST(CreatedAt) {
        TKikimrRunner kikimr(TKikimrSettings().SetPQConfig(DefaultPQConfig()));
        auto scheme = NYdb::NScheme::TSchemeClient(kikimr.GetDriver(), TCommonClientSettings().Database("/Root"));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        {
            auto query = R"(
                --!syntax_v1
                CREATE TABLE `/Root/dir/table` (
                    Key Uint64,
                    Value String,
                    PRIMARY KEY (Key)
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        NYdb::NScheme::TVirtualTimestamp createdAt;

        { // describe table
            auto desc = session.DescribeTable("/Root/dir/table").ExtractValueSync();
            UNIT_ASSERT_C(desc.IsSuccess(), desc.GetIssues().ToString());

            createdAt = desc.GetEntry().CreatedAt;
            UNIT_ASSERT(createdAt.PlanStep > 0);
            UNIT_ASSERT(createdAt.TxId != 0);
        }

        { // describe dir
            auto desc = scheme.DescribePath("/Root/dir").ExtractValueSync();
            UNIT_ASSERT_C(desc.IsSuccess(), desc.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(desc.GetEntry().CreatedAt, createdAt);
        }

        { // list dir
            auto desc = scheme.ListDirectory("/Root/dir").ExtractValueSync();
            UNIT_ASSERT_C(desc.IsSuccess(), desc.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(desc.GetEntry().CreatedAt, createdAt);

            UNIT_ASSERT_VALUES_EQUAL(desc.GetChildren().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(desc.GetChildren()[0].CreatedAt, createdAt);
        }

        { // copy table
            const auto result = session.CopyTable("/Root/dir/table", "/Root/dir/copy").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto desc = session.DescribeTable("/Root/dir/copy").ExtractValueSync();
            UNIT_ASSERT_C(desc.IsSuccess(), desc.GetIssues().ToString());
            UNIT_ASSERT(desc.GetEntry().CreatedAt > createdAt);
        }
    }

    Y_UNIT_TEST(CreateAlterDropTableStore) {
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TKikimrRunner kikimr(runnerSettings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString tableStoreName = "/Root/TableStoreTest";
        auto query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE TABLESTORE `)" << tableStoreName << R"(` (
                Key Uint64,
                Value1 String,
                PRIMARY KEY (Key)
            )
            WITH (
                STORE = COLUMN,
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 10
            );)";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

#if 0 // TODO
        auto query2 = TStringBuilder() << R"(
            --!syntax_v1
            ALTER TABLESTORE `)" << tableStoreName << R"(`
                SET (AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 10)
            ;)";
        result = session.ExecuteSchemeQuery(query2).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
#endif
        auto query3 = TStringBuilder() << R"(
            --!syntax_v1
            DROP TABLESTORE `)" << tableStoreName << R"(`;)";
        result = session.ExecuteSchemeQuery(query3).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

#if 0 // TODO
    Y_UNIT_TEST(CreateDropInheritedColumnTable) {
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TKikimrRunner kikimr(runnerSettings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString tableName = "/Root/TableTest";
        TString tableStoreName = "/Root/TableStoreTest";
        TString columnTableName = "/Root/TableStoreTest/ColumnTableTest";
        auto query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE TABLE `)" << tableName << R"(` (
                Key Uint64,
                Value1 String,
                PRIMARY KEY (Key)
            )
            PARTITION BY HASH(Key)
            WITH (
                STORE = ROW,
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 10
            );)";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto query2 = TStringBuilder() << R"(
            --!syntax_v1
            CREATE TABLESTORE `)" << tableStoreName << R"(` (
                Key Uint64,
                Value1 String,
                PRIMARY KEY (Key)
            )
            WITH (
                STORE = COLUMN,
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 10
            );)";
        result = session.ExecuteSchemeQuery(query2).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto query3 = TStringBuilder() << R"(
            --!syntax_v1
            CREATE TABLE `)" << columnTableName << R"(` ()
            INHERITS `)" << tableName << R"(`
            PARTITION BY HASH(Key)
            WITH (
                STORE = COLUMN,
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 5
            )
            TABLESTORE `)" << tableStoreName << R"(`;)";
        result = session.ExecuteSchemeQuery(query3).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto query4 = TStringBuilder() << R"(
            --!syntax_v1
            DROP TABLE `)" << tableName << R"(`;)";
        result = session.ExecuteSchemeQuery(query4).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto query5 = TStringBuilder() << R"(
            --!syntax_v1
            DROP TABLE `)" << columnTableName << R"(`;)";
        result = session.ExecuteSchemeQuery(query5).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }
#endif

    Y_UNIT_TEST(CreateTableStoreNegative) {
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TKikimrRunner kikimr(runnerSettings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString tableStoreName = "/Root/TableStoreTest";
        auto query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE TABLESTORE `)" << tableStoreName << R"(` (
                Key Uint64,
                Value1 String,
                PRIMARY KEY (Key)
            )
            WITH (
                STORE = ROW,
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 10
            );)";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());

        auto query2 = TStringBuilder() << R"(
            --!syntax_v1
            CREATE TABLESTORE `)" << tableStoreName << R"(` (
                Key Uint64,
                Value1 String,
                PRIMARY KEY (Key)
            )
            WITH (
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 10
            );)";
        result = session.ExecuteSchemeQuery(query2).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());

        auto query3 = TStringBuilder() << R"(
            --!syntax_v1
            CREATE TABLESTORE `)" << tableStoreName << R"(` (
                Key Uint64,
                Value1 String,
                PRIMARY KEY (Key)
            )
            WITH (
                STORE = COLUMN
            );)";
        result = session.ExecuteSchemeQuery(query3).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
    }

    Y_UNIT_TEST(CreateAlterDropColumnTableInStore) {
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TKikimrRunner kikimr(runnerSettings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString tableStoreName = "/Root/TableStoreTest";
        auto query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE TABLESTORE `)" << tableStoreName << R"(` (
                Key Uint64,
                Value1 String,
                PRIMARY KEY (Key)
            )
            WITH (
                STORE = COLUMN,
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 10
            );)";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        TString tableName = "/Root/TableStoreTest/ColumnTableTest";
        auto query2 = TStringBuilder() << R"(
            --!syntax_v1
            CREATE TABLE `)" << tableName << R"(` (
                Key Uint64,
                Value1 String,
                PRIMARY KEY (Key)
            )
            PARTITION BY HASH (Key)
            WITH (
                STORE = COLUMN,
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 10
            );)";
        result = session.ExecuteSchemeQuery(query2).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
#if 0 // TODO
        auto query3 = TStringBuilder() << R"(
            --!syntax_v1
            ALTER TABLE `)" << tableName << R"(`;)";
        result = session.ExecuteSchemeQuery(query3).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
#endif
        auto query4 = TStringBuilder() << R"(
            --!syntax_v1
            DROP TABLE `)" << tableName << R"(`;)";
        result = session.ExecuteSchemeQuery(query4).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto query5 = TStringBuilder() << R"(
            --!syntax_v1
            DROP TABLESTORE `)" << tableStoreName << R"(`;)";
        result = session.ExecuteSchemeQuery(query5).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    Y_UNIT_TEST(CreateDropColumnTable) {
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TKikimrRunner kikimr(runnerSettings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString tableName = "/Root/ColumnTableTest";

        auto query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE TABLE `)" << tableName << R"(` (
                Key Uint64 NOT NULL,
                Value1 String,
                Value2 Int64 NOT NULL,
                PRIMARY KEY (Key)
            )
            PARTITION BY HASH(Key)
            WITH (
                STORE = COLUMN,
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 10
            );)";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        {
            auto settings = TDescribeTableSettings()
                .WithTableStatistics(true);
            auto describeResult = session.DescribeTable("/Root/ColumnTableTest", settings).GetValueSync();
            UNIT_ASSERT_C(describeResult.IsSuccess(), describeResult.GetIssues().ToString());

            const auto& description = describeResult.GetTableDescription();
            // TODO: table type
            auto columns = description.GetTableColumns();
            UNIT_ASSERT_VALUES_EQUAL(columns.size(), 3);
            UNIT_ASSERT_VALUES_EQUAL(description.GetPartitionsCount(), 10);
            UNIT_ASSERT_VALUES_EQUAL(description.GetPrimaryKeyColumns().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(description.GetPrimaryKeyColumns()[0], "Key");

            for (auto& column : columns) {
                auto type = column.Type.ToString();
                UNIT_ASSERT(column.Name != "Key" || type == "Uint64"); // NOT NULL
                UNIT_ASSERT(column.Name != "Value1" || type == "String?");
                UNIT_ASSERT(column.Name != "Value2" || type == "Int64"); // NOT NULL
            }

            auto partSettings = description.GetPartitioningSettings().GetProto();
            auto& partition_by = partSettings.partition_by();

            UNIT_ASSERT_VALUES_EQUAL(partition_by.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(partition_by[0], "Key");
            // TODO: TTL
        }

        auto query2 = TStringBuilder() << R"(
            --!syntax_v1
            DROP TABLE `)" << tableName << R"(`;)";
        result = session.ExecuteSchemeQuery(query2).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    Y_UNIT_TEST(OlapSharding_KeyOnly) {
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TKikimrRunner kikimr(runnerSettings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString tableName = "/Root/ColumnTableTest";

        auto query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE TABLE `)" << tableName << R"(` (
                Key Uint64 NOT NULL,
                Value1 String,
                Value2 Int64 NOT NULL,
                PRIMARY KEY (Key)
            )
            PARTITION BY HASH(Key, Value1)
            WITH (
                STORE = COLUMN,
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 10
            );)";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
    }

    Y_UNIT_TEST(CreateDropColumnTableNegative) {
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TKikimrRunner kikimr(runnerSettings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString tableName = "/Root/ColumnTableTest";

        { // zero partitions count
            auto query = TStringBuilder() << R"(
                --!syntax_v1
                CREATE TABLE `)" << tableName << R"(` (
                    Key Uint64 NOT NULL,
                    Value1 String,
                    PRIMARY KEY (Key)
                )
                PARTITION BY HASH (Key)
                WITH (
                    STORE = COLUMN,
                    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 0
                );)";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
        }

        { // no partition by
            auto query = TStringBuilder() << R"(
                --!syntax_v1
                CREATE TABLE `)" << tableName << R"(` (
                    Key Uint64,
                    Value1 String,
                    PRIMARY KEY (Key)
                )
                WITH (
                    STORE = COLUMN,
                    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1
                );)";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
        }

        { // disallow nullable key
            auto query = TStringBuilder() << R"(
                --!syntax_v1
                CREATE TABLE `)" << tableName << R"(` (
                    Key Uint64,
                    Value1 String,
                    PRIMARY KEY (Key)
                )
                PARTITION BY HASH (Key)
                WITH (
                    STORE = COLUMN,
                    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1
                );)";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(AlterColumnTableTtl) {
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TKikimrRunner kikimr(runnerSettings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString tableName = "/Root/ColumnTableTest";

        auto query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE TABLE `)" << tableName << R"(` (
                Key Timestamp NOT NULL,
                Value1 Date,
                Value2 Datetime NOT NULL,
                PRIMARY KEY (Key)
            )
            PARTITION BY HASH(Key)
            WITH (
                STORE = COLUMN,
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 10
            );)";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto query2 = TStringBuilder() << R"(
            --!syntax_v1
            ALTER TABLE `)" << tableName << R"(` SET(TTL = Interval("P1D") ON Key);)";
        result = session.ExecuteSchemeQuery(query2).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto query3 = TStringBuilder() << R"(
            --!syntax_v1
            ALTER TABLE `)" << tableName << R"(` RESET (TTL);)";
        result = session.ExecuteSchemeQuery(query3).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto query4 = TStringBuilder() << R"(
            --!syntax_v1
            ALTER TABLE `)" << tableName << R"(` SET(TTL = Interval("P1D") ON Value1);)";
        result = session.ExecuteSchemeQuery(query4).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto query5 = TStringBuilder() << R"(
            --!syntax_v1
            ALTER TABLE `)" << tableName << R"(` SET(TTL = Interval("P1D") ON Value2);)";
        result = session.ExecuteSchemeQuery(query5).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto query6 = TStringBuilder() << R"(
            --!syntax_v1
            DROP TABLE `)" << tableName << R"(`;)";
        result = session.ExecuteSchemeQuery(query6).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    Y_UNIT_TEST(AlterColumnTableTiering) {
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TKikimrRunner kikimr(runnerSettings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString tableName = "/Root/ColumnTableTest";

        auto query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE TABLE `)" << tableName << R"(` (
                Key Uint64 NOT NULL,
                Value1 String,
                Value2 Int64 NOT NULL,
                PRIMARY KEY (Key)
            )
            PARTITION BY HASH(Key)
            WITH (
                STORE = COLUMN,
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 10,
                TIERING = 'tiering1'
            );)";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
#if 0 // TODO
        { // describe table
            auto desc = session.DescribeTable(tableName).ExtractValueSync();
            UNIT_ASSERT_C(desc.IsSuccess(), desc.GetIssues().ToString());

            auto tiering = desc.GetTableDescription().GetTiering();
            UNIT_ASSERT(tiering);
            UNIT_ASSERT_VALUES_EQUAL(*tiering, "tiering1");
        }
#endif
        auto query2 = TStringBuilder() << R"(
            --!syntax_v1
            ALTER TABLE `)" << tableName << R"(` SET(TIERING = 'tiering2');)";
        result = session.ExecuteSchemeQuery(query2).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        { // describe table
            auto desc = session.DescribeTable(tableName).ExtractValueSync();
            UNIT_ASSERT_C(desc.IsSuccess(), desc.GetIssues().ToString());

            auto tiering = desc.GetTableDescription().GetTiering();
            UNIT_ASSERT(tiering);
            UNIT_ASSERT_VALUES_EQUAL(*tiering, "tiering2");
        }

        auto query3 = TStringBuilder() << R"(
            --!syntax_v1
            ALTER TABLE `)" << tableName << R"(` RESET (TIERING);)";
        result = session.ExecuteSchemeQuery(query3).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        { // describe table
            auto desc = session.DescribeTable(tableName).ExtractValueSync();
            UNIT_ASSERT_C(desc.IsSuccess(), desc.GetIssues().ToString());

            auto tiering = desc.GetTableDescription().GetTiering();
            UNIT_ASSERT(!tiering);
        }

        auto query4 = TStringBuilder() << R"(
            --!syntax_v1
            ALTER TABLE `)" << tableName << R"(` SET (TIERING = 'tiering1');)";
        result = session.ExecuteSchemeQuery(query4).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        { // describe table
            auto desc = session.DescribeTable(tableName).ExtractValueSync();
            UNIT_ASSERT_C(desc.IsSuccess(), desc.GetIssues().ToString());

            auto tiering = desc.GetTableDescription().GetTiering();
            UNIT_ASSERT(tiering);
            UNIT_ASSERT_VALUES_EQUAL(*tiering, "tiering1");
        }

        auto query5 = TStringBuilder() << R"(
            --!syntax_v1
            DROP TABLE `)" << tableName << R"(`;)";
        result = session.ExecuteSchemeQuery(query5).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    Y_UNIT_TEST(Int8Int16) {
        TKikimrRunner kikimr;

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        TString tableName("/Root/Types");
        auto createTable = Sprintf(R"(
            CREATE TABLE `%s` (
                CUint8 Uint8,
                CInt8 Int8,
                CUint16 Uint16,
                CInt16 Int16,
                PRIMARY KEY (CUint8, CInt8, CUint16, CInt16))
        )", tableName.c_str());

        auto result = session.ExecuteSchemeQuery(createTable).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        NYdb::TValueBuilder rows;
        rows.BeginList();
        for (size_t i = 0; i < 10; ++i) {
            rows.AddListItem()
                .BeginStruct()
                .AddMember("CUint8").Uint8(i)
                .AddMember("CInt8").Int8(i)
                .AddMember("CUint16").Uint16(i)
                .AddMember("CInt16").Int16(i)
                .EndStruct();
        }
        rows.EndList();

        result = db.BulkUpsert(tableName, rows.Build()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        auto readSettings = TReadTableSettings()
            .AppendColumns("CUint8")
            .AppendColumns("CInt8")
            .AppendColumns("CUint16")
            .AppendColumns("CInt16");

        auto it = session.ReadTable(tableName, readSettings).GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), result.GetIssues().ToString());

        bool eos = false;
        while (!eos) {
            auto part = it.ReadNext().ExtractValueSync();
            if (!part.IsSuccess()) {
                eos = true;
                UNIT_ASSERT_C(part.EOS(), result.GetIssues().ToString());
                continue;
            }
            auto resultSet = part.ExtractPart();
            TResultSetParser parser(resultSet);
            for (size_t i = 0; parser.TryNextRow(); ++i) {
                {
                    auto& c = parser.ColumnParser("CUint8");
                    UNIT_ASSERT_VALUES_EQUAL(i, *c.GetOptionalUint8().Get());
                }
                {
                    auto& c = parser.ColumnParser("CInt8");
                    UNIT_ASSERT_VALUES_EQUAL(i, *c.GetOptionalInt8().Get());
                }
                {
                    auto& c = parser.ColumnParser("CUint16");
                    UNIT_ASSERT_VALUES_EQUAL(i, *c.GetOptionalUint16().Get());
                }
                {
                    auto& c = parser.ColumnParser("CInt16");
                    UNIT_ASSERT_VALUES_EQUAL(i, *c.GetOptionalInt16().Get());
                }
            }
        }

        session.Close().GetValueSync();
    }

    Y_UNIT_TEST(Int8Int16Olap) {
        TKikimrRunner kikimr;

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        TString tableName("/Root/Types");
        auto createTable = Sprintf(R"(
            CREATE TABLE `%s` (
                CUint8 Uint8 NOT NULL,
                CInt8 Int8 NOT NULL,
                CUint16 Uint16 NOT NULL,
                CInt16 Int16 NOT NULL,
                PRIMARY KEY (CUint8, CInt8, CUint16, CInt16))
            PARTITION BY HASH(CUint8, CInt8, CUint16, CInt16)
            WITH (
                STORE = COLUMN,
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1
            )
        )", tableName.c_str());

        auto result = session.ExecuteSchemeQuery(createTable).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        auto schema = std::make_shared<arrow::Schema>(
            std::vector<std::shared_ptr<arrow::Field>>{
                arrow::field("CUint8", arrow::uint8()),
                arrow::field("CInt8", arrow::int8()),
                arrow::field("CUint16", arrow::uint16()),
                arrow::field("CInt16", arrow::int16())
            });

        size_t rowsCount = 10;
        auto builders = NArrow::MakeBuilders(schema, rowsCount);
        for (size_t i = 0; i < rowsCount; ++i) {
            Y_VERIFY(NArrow::Append<arrow::UInt8Type>(*builders[0], i));
            Y_VERIFY(NArrow::Append<arrow::Int8Type>(*builders[1], i));
            Y_VERIFY(NArrow::Append<arrow::UInt16Type>(*builders[2], i));
            Y_VERIFY(NArrow::Append<arrow::Int16Type>(*builders[3], i));
        }
        auto batch = arrow::RecordBatch::Make(schema, rowsCount, NArrow::Finish(std::move(builders)));

        TString strSchema = NArrow::SerializeSchema(*schema);
        TString strBatch = NArrow::SerializeBatchNoCompression(batch);

        result = db.BulkUpsert(tableName, NYdb::NTable::EDataFormat::ApacheArrow,
            strBatch, strSchema).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        auto scan = Sprintf("SELECT * FROM `%s` ORDER BY CUint8", tableName.c_str());
        auto it = db.StreamExecuteScanQuery(scan).GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), result.GetIssues().ToString());

        bool eos = false;
        while (!eos) {
            auto part = it.ReadNext().ExtractValueSync();
            if (!part.IsSuccess()) {
                eos = true;
                UNIT_ASSERT_C(part.EOS(), result.GetIssues().ToString());
                continue;
            }
            auto resultSet = part.ExtractResultSet();
            TResultSetParser parser(resultSet);
            for (size_t i = 0; parser.TryNextRow(); ++i) {
                {
                    auto& c = parser.ColumnParser("CUint8");
                    UNIT_ASSERT_VALUES_EQUAL(i, c.GetUint8());
                }
                {
                    auto& c = parser.ColumnParser("CInt8");
                    UNIT_ASSERT_VALUES_EQUAL(i, c.GetInt8());
                }
                {
                    auto& c = parser.ColumnParser("CUint16");
                    UNIT_ASSERT_VALUES_EQUAL(i, c.GetUint16());
                }
                {
                    auto& c = parser.ColumnParser("CInt16");
                    UNIT_ASSERT_VALUES_EQUAL(i, c.GetInt16());
                }
            }
        }

        session.Close().GetValueSync();
    }

    Y_UNIT_TEST(CreateExternalDataSource) {
        TKikimrRunner kikimr;
        kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableExternalDataSources(true);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString externalDataSourceName = "/Root/ExternalDataSource";
        auto query = TStringBuilder() << R"(
            CREATE EXTERNAL DATA SOURCE `)" << externalDataSourceName << R"(` WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="my-bucket",
                AUTH_METHOD="NONE"
            );)";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        auto externalDataSourceDesc = Navigate(runtime, runtime.AllocateEdgeActor(), externalDataSourceName, NSchemeCache::TSchemeCacheNavigate::EOp::OpUnknown);
        const auto& externalDataSource = externalDataSourceDesc->ResultSet.at(0);
        UNIT_ASSERT_EQUAL(externalDataSource.Kind, NSchemeCache::TSchemeCacheNavigate::EKind::KindExternalDataSource);
        UNIT_ASSERT(externalDataSource.ExternalDataSourceInfo);
        UNIT_ASSERT_VALUES_EQUAL(externalDataSource.ExternalDataSourceInfo->Description.GetSourceType(), "ObjectStorage");
        UNIT_ASSERT_VALUES_EQUAL(externalDataSource.ExternalDataSourceInfo->Description.GetInstallation(), "");
        UNIT_ASSERT_VALUES_EQUAL(externalDataSource.ExternalDataSourceInfo->Description.GetLocation(), "my-bucket");
        UNIT_ASSERT_VALUES_EQUAL(externalDataSource.ExternalDataSourceInfo->Description.GetName(), SplitPath(externalDataSourceName).back());
        UNIT_ASSERT(externalDataSource.ExternalDataSourceInfo->Description.GetAuth().HasNone());
    }

    Y_UNIT_TEST(DisableCreateExternalDataSource) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString externalDataSourceName = "/Root/ExternalDataSource";
        auto query = TStringBuilder() << R"(
            CREATE EXTERNAL DATA SOURCE `)" << externalDataSourceName << R"(` WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="my-bucket",
                AUTH_METHOD="NONE"
            );)";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::INTERNAL_ERROR);
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "External data sources are disabled. Please contact your system administrator to enable it");
    }

    Y_UNIT_TEST(CreateExternalDataSourceValidation) {
        TKikimrRunner kikimr;
        kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableExternalDataSources(true);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString externalDataSourceName = "/Root/ExternalDataSource";
        auto query = TStringBuilder() << R"(
            CREATE EXTERNAL DATA SOURCE `)" << externalDataSourceName << R"(` WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="my-bucket",
                AUTH_METHOD="UNKNOWN"
            );)";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Authorization method not specified");
    }

    Y_UNIT_TEST(DropExternalDataSource) {
        TKikimrRunner kikimr;
        kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableExternalDataSources(true);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString externalDataSourceName = "/Root/ExternalDataSource";
        {
            auto query = TStringBuilder() << R"(
                CREATE EXTERNAL DATA SOURCE `)" << externalDataSourceName << R"(` WITH (
                    SOURCE_TYPE="ObjectStorage",
                    LOCATION="my-bucket",
                    AUTH_METHOD="NONE"
                );)";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto query = TStringBuilder() << R"( DROP EXTERNAL DATA SOURCE `)" << externalDataSourceName << "`";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        auto externalDataSourceDesc = Navigate(runtime, runtime.AllocateEdgeActor(), externalDataSourceName, NSchemeCache::TSchemeCacheNavigate::EOp::OpUnknown);
        const auto& externalDataSource = externalDataSourceDesc->ResultSet.at(0);
        UNIT_ASSERT_EQUAL(externalDataSourceDesc->ErrorCount, 1);
        UNIT_ASSERT_EQUAL(externalDataSource.Kind, NSchemeCache::TSchemeCacheNavigate::EKind::KindUnknown);
    }

    Y_UNIT_TEST(DisableDropExternalDataSource) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString externalDataSourceName = "/Root/ExternalDataSource";
        auto query = TStringBuilder() << R"( DROP EXTERNAL DATA SOURCE `)" << externalDataSourceName << "`";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "External data sources are disabled. Please contact your system administrator to enable it");
    }

    Y_UNIT_TEST(DoubleCreateExternalDataSource) {
        TKikimrRunner kikimr;
        kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableExternalDataSources(true);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString externalDataSourceName = "/Root/ExternalDataSource";
        {
            auto query = TStringBuilder() << R"(
                CREATE EXTERNAL DATA SOURCE `)" << externalDataSourceName << R"(` WITH (
                    SOURCE_TYPE="ObjectStorage",
                    LOCATION="my-bucket",
                    AUTH_METHOD="NONE"
                );)";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto& runtime = *kikimr.GetTestServer().GetRuntime();
            auto externalDataSourceDesc = Navigate(runtime, runtime.AllocateEdgeActor(), externalDataSourceName, NSchemeCache::TSchemeCacheNavigate::EOp::OpUnknown);
            const auto& externalDataSource = externalDataSourceDesc->ResultSet.at(0);
            UNIT_ASSERT_EQUAL(externalDataSource.Kind, NSchemeCache::TSchemeCacheNavigate::EKind::KindExternalDataSource);
            UNIT_ASSERT(externalDataSource.ExternalDataSourceInfo);
            UNIT_ASSERT_VALUES_EQUAL(externalDataSource.ExternalDataSourceInfo->Description.GetSourceType(), "ObjectStorage");
            UNIT_ASSERT_VALUES_EQUAL(externalDataSource.ExternalDataSourceInfo->Description.GetInstallation(), "");
            UNIT_ASSERT_VALUES_EQUAL(externalDataSource.ExternalDataSourceInfo->Description.GetLocation(), "my-bucket");
            UNIT_ASSERT_VALUES_EQUAL(externalDataSource.ExternalDataSourceInfo->Description.GetName(), SplitPath(externalDataSourceName).back());
            UNIT_ASSERT(externalDataSource.ExternalDataSourceInfo->Description.GetAuth().HasNone());
        }

        {
            auto query = TStringBuilder() << R"(
                CREATE EXTERNAL DATA SOURCE `)" << externalDataSourceName << R"(` WITH (
                    SOURCE_TYPE="ObjectStorage",
                    LOCATION="my-bucket",
                    AUTH_METHOD="NONE"
                );)";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Check failed: path: '/Root/ExternalDataSource', error: path exist");
        }
    }

    Y_UNIT_TEST(CreateExternalTable) {
        TKikimrRunner kikimr;
        kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableExternalDataSources(true);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString externalDataSourceName = "/Root/ExternalDataSource";
        TString externalTableName = "/Root/ExternalTable";
        auto query = TStringBuilder() << R"(
            CREATE EXTERNAL DATA SOURCE `)" << externalDataSourceName << R"(` WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="my-bucket",
                AUTH_METHOD="NONE"
            );
            CREATE EXTERNAL TABLE `)" << externalTableName << R"(` (
                Key Uint64,
                Value String
            ) WITH (
                DATA_SOURCE=")" << externalDataSourceName << R"(",
                LOCATION="/"
            );)";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_C(result.GetStatus() == EStatus::SUCCESS, result.GetIssues().ToString());

        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        auto externalTableDesc = Navigate(runtime, runtime.AllocateEdgeActor(), externalTableName, NKikimr::NSchemeCache::TSchemeCacheNavigate::EOp::OpUnknown);
        const auto& externalTable = externalTableDesc->ResultSet.at(0);
        UNIT_ASSERT_EQUAL(externalTable.Kind, NKikimr::NSchemeCache::TSchemeCacheNavigate::EKind::KindExternalTable);
        UNIT_ASSERT(externalTable.ExternalTableInfo);
        UNIT_ASSERT_VALUES_EQUAL(externalTable.ExternalTableInfo->Description.ColumnsSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(externalTable.ExternalTableInfo->Description.GetDataSourcePath(), externalDataSourceName);
        UNIT_ASSERT_VALUES_EQUAL(externalTable.ExternalTableInfo->Description.GetLocation(), "/");
        UNIT_ASSERT_VALUES_EQUAL(externalTable.ExternalTableInfo->Description.GetSourceType(), "ObjectStorage");
    }

    Y_UNIT_TEST(DisableCreateExternalTable) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        auto query = TStringBuilder() << R"(
            CREATE EXTERNAL TABLE `/Root/ExternalTable` (
                Key Uint64,
                Value String
            ) WITH (
                DATA_SOURCE="/Root/ExternalDataSource",
                LOCATION="/"
            );)";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "External table are disabled. Please contact your system administrator to enable it");
    }

    Y_UNIT_TEST(CreateExternalTableCheckPrimaryKey) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        auto query = TStringBuilder() << R"(
            CREATE EXTERNAL TABLE `/Root/ExternalTable` (
                Key Uint64,
                Value String,
                PRIMARY KEY(Key)
            ) WITH (
                DATA_SOURCE="/Root/MyDataSource",
                LOCATION="/"
            );)";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_UNEQUAL(result.GetStatus(), EStatus::SUCCESS);
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "PRIMARY KEY is not supported for external table");
    }

    Y_UNIT_TEST(CreateExternalTableValidation) {
        TKikimrRunner kikimr;
        kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableExternalDataSources(true);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        auto query = TStringBuilder() << R"(
            CREATE EXTERNAL TABLE `/Root/ExternalTable` (
                Key Uint64,
                Value String,
                PRIMARY KEY(Key)
            ) WITH (
                LOCATION="/"
            );)";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "DATA_SOURCE requires key");
    }

    Y_UNIT_TEST(DropExternalTable) {
        TKikimrRunner kikimr;
        kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableExternalDataSources(true);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString externalDataSourceName = "/Root/ExternalDataSource";
        TString externalTableName = "/Root/ExternalTable";
        {
            auto query = TStringBuilder() << R"(
                CREATE EXTERNAL DATA SOURCE `)" << externalDataSourceName << R"(` WITH (
                    SOURCE_TYPE="ObjectStorage",
                    LOCATION="my-bucket",
                    AUTH_METHOD="NONE"
                );
                CREATE EXTERNAL TABLE `)" << externalTableName << R"(` (
                    Key Uint64,
                    Value String
                ) WITH (
                    DATA_SOURCE=")" << externalDataSourceName << R"(",
                    LOCATION="/"
                );)";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto query = TStringBuilder() << R"( DROP EXTERNAL TABLE `)" << externalTableName << "`";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            auto& runtime = *kikimr.GetTestServer().GetRuntime();
            auto externalTableDesc = Navigate(runtime, runtime.AllocateEdgeActor(), externalTableName, NSchemeCache::TSchemeCacheNavigate::EOp::OpUnknown);
            const auto& externalTable = externalTableDesc->ResultSet.at(0);
            UNIT_ASSERT_EQUAL(externalTableDesc->ErrorCount, 1);
            UNIT_ASSERT_EQUAL(externalTable.Kind, NSchemeCache::TSchemeCacheNavigate::EKind::KindUnknown);
        }

        {
            auto query = TStringBuilder() << R"( DROP EXTERNAL DATA SOURCE `)" << externalDataSourceName << "`";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            auto& runtime = *kikimr.GetTestServer().GetRuntime();
            auto externalDataSourceDesc = Navigate(runtime, runtime.AllocateEdgeActor(), externalDataSourceName, NSchemeCache::TSchemeCacheNavigate::EOp::OpUnknown);
            const auto& externalDataSource = externalDataSourceDesc->ResultSet.at(0);
            UNIT_ASSERT_EQUAL(externalDataSourceDesc->ErrorCount, 1);
            UNIT_ASSERT_EQUAL(externalDataSource.Kind, NSchemeCache::TSchemeCacheNavigate::EKind::KindUnknown);
        }
    }

    Y_UNIT_TEST(DisableDropExternalTable) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        auto query = TStringBuilder() << R"( DROP EXTERNAL TABLE `/Root/ExternalDataSource`)";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "External table are disabled. Please contact your system administrator to enable it");
    }

    Y_UNIT_TEST(CreateExternalTableWithSettings) {
        TKikimrRunner kikimr;
        kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableExternalDataSources(true);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString externalDataSourceName = "/Root/ExternalDataSource";
        TString externalTableName = "/Root/ExternalTable";
        auto query = TStringBuilder() << R"(
            CREATE EXTERNAL DATA SOURCE `)" << externalDataSourceName << R"(` WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="my-bucket",
                AUTH_METHOD="NONE"
            );
            CREATE EXTERNAL TABLE `)" << externalTableName << R"(` (
                Key Uint64,
                Value String,
                year Int64 NOT NULL,
                month Int64 NOT NULL
            ) WITH (
                DATA_SOURCE=")" << externalDataSourceName << R"(",
                LOCATION="/folder1/*",
                FORMAT="json_as_string",
                `projection.enabled`="true",
                `projection.year.type`="integer",
                `projection.year.min`="2010",
                `projection.year.max`="2022",
                `projection.year.interval`="1",
                `projection.month.type`="integer",
                `projection.month.min`="1",
                `projection.month.max`="12",
                `projection.month.interval`="1",
                `projection.month.digits`="2",
                `storage.location.template`="${year}/${month}",
                PARTITIONED_BY = "[year, month]"
            );)";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        auto externalTableDesc = Navigate(runtime, runtime.AllocateEdgeActor(), externalTableName, NKikimr::NSchemeCache::TSchemeCacheNavigate::EOp::OpUnknown);
        const auto& externalTable = externalTableDesc->ResultSet.at(0);
        UNIT_ASSERT_EQUAL(externalTable.Kind, NKikimr::NSchemeCache::TSchemeCacheNavigate::EKind::KindExternalTable);
        UNIT_ASSERT(externalTable.ExternalTableInfo);
        UNIT_ASSERT_VALUES_EQUAL(externalTable.ExternalTableInfo->Description.ColumnsSize(), 4);
        UNIT_ASSERT_VALUES_EQUAL(externalTable.ExternalTableInfo->Description.GetDataSourcePath(), externalDataSourceName);
        UNIT_ASSERT_VALUES_EQUAL(externalTable.ExternalTableInfo->Description.GetLocation(), "/folder1/*");
    }

    Y_UNIT_TEST(DoubleCreateExternalTable) {
        TKikimrRunner kikimr;
        kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableExternalDataSources(true);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString externalDataSourceName = "/Root/ExternalDataSource";
        TString externalTableName = "/Root/ExternalTable";
        {
            auto query = TStringBuilder() << R"(
                CREATE EXTERNAL DATA SOURCE `)" << externalDataSourceName << R"(` WITH (
                    SOURCE_TYPE="ObjectStorage",
                    LOCATION="my-bucket",
                    AUTH_METHOD="NONE"
                );
                CREATE EXTERNAL TABLE `)" << externalTableName << R"(` (
                    Key Uint64,
                    Value String
                ) WITH (
                    DATA_SOURCE=")" << externalDataSourceName << R"(",
                    LOCATION="/"
                );)";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto& runtime = *kikimr.GetTestServer().GetRuntime();
            auto externalTableDesc = Navigate(runtime, runtime.AllocateEdgeActor(), externalTableName, NKikimr::NSchemeCache::TSchemeCacheNavigate::EOp::OpUnknown);
            const auto& externalTable = externalTableDesc->ResultSet.at(0);
            UNIT_ASSERT_EQUAL(externalTable.Kind, NKikimr::NSchemeCache::TSchemeCacheNavigate::EKind::KindExternalTable);
            UNIT_ASSERT(externalTable.ExternalTableInfo);
            UNIT_ASSERT_VALUES_EQUAL(externalTable.ExternalTableInfo->Description.ColumnsSize(), 2);
            UNIT_ASSERT_VALUES_EQUAL(externalTable.ExternalTableInfo->Description.GetDataSourcePath(), externalDataSourceName);
            UNIT_ASSERT_VALUES_EQUAL(externalTable.ExternalTableInfo->Description.GetLocation(), "/");
            UNIT_ASSERT_VALUES_EQUAL(externalTable.ExternalTableInfo->Description.GetSourceType(), "ObjectStorage");
        }

        {
            auto query = TStringBuilder() << R"(
                CREATE EXTERNAL TABLE `)" << externalTableName << R"(` (
                    Key Uint64,
                    Value String
                ) WITH (
                    DATA_SOURCE=")" << externalDataSourceName << R"(",
                    LOCATION="/"
                );)";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Check failed: path: '/Root/ExternalTable', error: path exist");
        }
    }

    Y_UNIT_TEST(DropDependentExternalDataSource) {
        TKikimrRunner kikimr;
        kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableExternalDataSources(true);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString externalDataSourceName = "/Root/ExternalDataSource";
        TString externalTableName = "/Root/ExternalTable";
        auto query = TStringBuilder() << R"(
            CREATE EXTERNAL DATA SOURCE `)" << externalDataSourceName << R"(` WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="my-bucket",
                AUTH_METHOD="NONE"
            );
            CREATE EXTERNAL TABLE `)" << externalTableName << R"(` (
                Key Uint64,
                Value String
            ) WITH (
                DATA_SOURCE=")" << externalDataSourceName << R"(",
                LOCATION="/"
            );)";
        {
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_C(result.GetStatus() == EStatus::SUCCESS, result.GetIssues().ToString());

            auto& runtime = *kikimr.GetTestServer().GetRuntime();
            auto externalTableDesc = Navigate(runtime, runtime.AllocateEdgeActor(), externalTableName, NKikimr::NSchemeCache::TSchemeCacheNavigate::EOp::OpUnknown);
            const auto& externalTable = externalTableDesc->ResultSet.at(0);
            UNIT_ASSERT_EQUAL(externalTable.Kind, NKikimr::NSchemeCache::TSchemeCacheNavigate::EKind::KindExternalTable);
            UNIT_ASSERT(externalTable.ExternalTableInfo);
            UNIT_ASSERT_VALUES_EQUAL(externalTable.ExternalTableInfo->Description.ColumnsSize(), 2);
            UNIT_ASSERT_VALUES_EQUAL(externalTable.ExternalTableInfo->Description.GetDataSourcePath(), externalDataSourceName);
            UNIT_ASSERT_VALUES_EQUAL(externalTable.ExternalTableInfo->Description.GetLocation(), "/");
            UNIT_ASSERT_VALUES_EQUAL(externalTable.ExternalTableInfo->Description.GetSourceType(), "ObjectStorage");
        }

        {
            auto query = TStringBuilder() << R"( DROP EXTERNAL DATA SOURCE `)" << externalDataSourceName << "`";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Other entities depend on this data source, please remove them at the beginning: /Root/ExternalTable");
        }
    }
}

Y_UNIT_TEST_SUITE(KqpOlapScheme) {
    class TTestHelper {
    public:
        class TColumnSchema {
            YDB_ACCESSOR_DEF(TString, Name);
            YDB_ACCESSOR_DEF(NScheme::TTypeId, Type);
            YDB_FLAG_ACCESSOR(Nullable, true);
        public:
            TString BuildQuery() const {
                auto str = TStringBuilder() << Name << " " << NScheme::GetTypeName(Type);
                if (!NullableFlag) {
                    str << " NOT NULL";
                }
                return str;
            }
        };

        class TUpdatesBuilder {
            std::vector<std::unique_ptr<arrow::ArrayBuilder>> Builders;
            std::shared_ptr<arrow::Schema> Schema;
            ui32 RowsCount = 0;
        public:
            class TRowBuilder {
                TUpdatesBuilder& Owner;
                YDB_READONLY(ui32, Index, 0);
            public:
                TRowBuilder(ui32 index, TUpdatesBuilder& owner)
                    : Owner(owner)
                    , Index(index)
                {}

                template <class T>
                TRowBuilder Add(const T& data) {
                    Y_VERIFY(Index < Owner.Builders.size());
                    auto dataScalar = arrow::MakeScalar(data);
                    auto res = Owner.Builders[Index]->AppendScalar(*dataScalar);
                    return TRowBuilder(Index + 1, Owner);
                }

                TRowBuilder AddNull() {
                    Y_VERIFY(Index < Owner.Builders.size());
                    auto res = Owner.Builders[Index]->AppendNull();
                    return TRowBuilder(Index + 1, Owner);
                }
            };

            TUpdatesBuilder(std::shared_ptr<arrow::Schema> schema)
                : Schema(schema)
            {
                Builders = NArrow::MakeBuilders(schema);
                Y_VERIFY(Builders.size() == schema->fields().size());
            }

            TRowBuilder AddRow() {
                ++RowsCount;
                return TRowBuilder(0, *this);
            }

            std::shared_ptr<arrow::RecordBatch> BuildArrow() {
                TVector<std::shared_ptr<arrow::Array>> columns;
                columns.reserve(Builders.size());
                for (auto&& builder : Builders) {
                    auto arrayDataRes = builder->Finish();
                    Y_VERIFY(arrayDataRes.ok());
                    columns.push_back(*arrayDataRes);
                }
                return arrow::RecordBatch::Make(Schema, RowsCount, columns);
            }
        };

        class TColumnTableBase {
            YDB_ACCESSOR_DEF(TString, Name);
            YDB_ACCESSOR_DEF(TVector<TColumnSchema>, Schema);
            YDB_ACCESSOR_DEF(TVector<TString>, PrimaryKey);
            YDB_ACCESSOR_DEF(TVector<TString>, Sharding);
            YDB_ACCESSOR(ui32, MinPartitionsCount, 1);
        public:
            TString BuildQuery() const {
                auto str = TStringBuilder() << "CREATE " << GetObjectType() << " `" << Name << "`";
                str << " (" << BuildColumnsStr(Schema) <<  ", PRIMARY KEY (" << JoinStrings(PrimaryKey, ", ") << "))";
                if (!Sharding.empty()) {
                    str << " PARTITION BY HASH(" << JoinStrings(Sharding, ", ") << ")";
                }
                str << " WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT =" << MinPartitionsCount << ");";
                return str;
            }

            std::shared_ptr<arrow::Schema> GetArrowSchema(const TVector<TColumnSchema>& columns) {
                std::vector<std::shared_ptr<arrow::Field>> result;
                for (auto&& col : columns) {
                    result.push_back(BuildField(col.GetName(), col.GetType()));
                }
                return std::make_shared<arrow::Schema>(result);
            }

        private:
            virtual TString GetObjectType() const = 0;
            TString BuildColumnsStr(const TVector<TColumnSchema>& clumns) const {
                TVector<TString> columnStr;
                for (auto&& c : clumns) {
                    columnStr.push_back(c.BuildQuery());
                }
                return JoinStrings(columnStr, ", ");
            }

            std::shared_ptr<arrow::Field> BuildField(const TString name, const NScheme::TTypeId& typeId) const {
                switch(typeId) {
                case NScheme::NTypeIds::Bool:
                    return arrow::field(name, arrow::boolean());
                case NScheme::NTypeIds::Int8:
                    return arrow::field(name, arrow::int8());
                case NScheme::NTypeIds::Int16:
                    return arrow::field(name, arrow::int16());
                case NScheme::NTypeIds::Int32:
                    return arrow::field(name, arrow::int32());
                case NScheme::NTypeIds::Int64:
                    return arrow::field(name, arrow::int64());
                case NScheme::NTypeIds::Uint8:
                    return arrow::field(name, arrow::uint8());
                case NScheme::NTypeIds::Uint16:
                    return arrow::field(name, arrow::uint16());
                case NScheme::NTypeIds::Uint32:
                    return arrow::field(name, arrow::uint32());
                case NScheme::NTypeIds::Uint64:
                    return arrow::field(name, arrow::uint64());
                case NScheme::NTypeIds::Float:
                    return arrow::field(name, arrow::float32());
                case NScheme::NTypeIds::Double:
                    return arrow::field(name, arrow::float64());
                case NScheme::NTypeIds::String:
                    return arrow::field(name, arrow::binary());
                case NScheme::NTypeIds::Utf8:
                    return arrow::field(name, arrow::utf8());
                case NScheme::NTypeIds::Json:
                    return arrow::field(name, arrow::binary());
                case NScheme::NTypeIds::Yson:
                    return arrow::field(name, arrow::binary());
                case NScheme::NTypeIds::Date:
                    return arrow::field(name, arrow::uint16());
                case NScheme::NTypeIds::Datetime:
                    return arrow::field(name, arrow::uint32());
                case NScheme::NTypeIds::Timestamp:
                    return arrow::field(name, arrow::timestamp(arrow::TimeUnit::TimeUnit::MICRO));
                case NScheme::NTypeIds::Interval:
                    return arrow::field(name, arrow::duration(arrow::TimeUnit::TimeUnit::MICRO));
                case NScheme::NTypeIds::JsonDocument:
                    return arrow::field(name, arrow::binary());
                }
                return nullptr;
            }
        };

        class TColumnTable : public TColumnTableBase {
        private:
            TString GetObjectType() const override {
                return "TABLE";
            }
        };

        class TColumnTableStore : public TColumnTableBase {
        private:
            TString GetObjectType() const override {
                return "TABLESTORE";
            }
        };

    private:
        TKikimrRunner Kikimr;
        NYdb::NTable::TTableClient TableClient;
        NYdb::NLongTx::TClient LongTxClient;
        NYdb::NTable::TSession Session;

    public:
        TTestHelper(const TKikimrSettings& settings)
            : Kikimr(settings)
            , TableClient(Kikimr.GetTableClient())
            , LongTxClient(Kikimr.GetDriver())
            , Session(TableClient.CreateSession().GetValueSync().GetSession())
        {}

        NYdb::NTable::TSession& GetSession() {
            return Session;
        }

        void CreateTable(const TColumnTableBase& table) {
            std::cerr << (table.BuildQuery()) << std::endl;
            auto result = Session.ExecuteSchemeQuery(table.BuildQuery()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        void InsertData(const TColumnTable& table, TTestHelper::TUpdatesBuilder& updates, const std::function<void()> onBeforeCommit = {}, const EStatus opStatus = EStatus::SUCCESS) {
            NLongTx::TLongTxBeginResult resBeginTx = LongTxClient.BeginWriteTx().GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(resBeginTx.Status().GetStatus(), EStatus::SUCCESS, resBeginTx.Status().GetIssues().ToString());

            auto txId = resBeginTx.GetResult().tx_id();
            auto batch = updates.BuildArrow();
            TString data = NArrow::SerializeBatchNoCompression(batch);

            NLongTx::TLongTxWriteResult resWrite =
                    LongTxClient.Write(txId, table.GetName(), txId, data, Ydb::LongTx::Data::APACHE_ARROW).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(resWrite.Status().GetStatus(), opStatus, resWrite.Status().GetIssues().ToString());

            if (onBeforeCommit) {
                onBeforeCommit();
            }

            NLongTx::TLongTxCommitResult resCommitTx = LongTxClient.CommitTx(txId).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(resCommitTx.Status().GetStatus(), EStatus::SUCCESS, resCommitTx.Status().GetIssues().ToString());
        }

        void ReadData(const TString& query, const TString& expected) {
            auto it = TableClient.StreamExecuteScanQuery(query).GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            UNIT_ASSERT_NO_DIFF(ReformatYson(result), ReformatYson(expected));
        }

        void RebootTablets(const TString& tableName) {
            auto runtime = Kikimr.GetTestServer().GetRuntime();
            TActorId sender = runtime->AllocateEdgeActor();
            TVector<ui64> shards;
            {
                auto describeResult = DescribeTable(&Kikimr.GetTestServer(), sender, tableName);
                for (auto shard : describeResult.GetPathDescription().GetColumnTableDescription().GetSharding().GetColumnShards()) {
                    shards.push_back(shard);
                }
            }
            for (auto shard : shards) {
                RebootTablet(*runtime, shard, sender);
            }
        }
    };

    Y_UNIT_TEST(AddColumn) {
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper testHelper(runnerSettings);

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("resource_id").SetType(NScheme::NTypeIds::Utf8),
            TTestHelper::TColumnSchema().SetName("level").SetType(NScheme::NTypeIds::Int32)
        };

        TTestHelper::TColumnTable testTable;

        testTable.SetName("/Root/ColumnTableTest").SetPrimaryKey({"id"}).SetSharding({"id"}).SetSchema(schema);
        testHelper.CreateTable(testTable);

        {
            TTestHelper::TUpdatesBuilder tableInserter(testTable.GetArrowSchema(schema));
            tableInserter.AddRow().Add(1).Add("test_res_1").AddNull();
            tableInserter.AddRow().Add(2).Add("test_res_2").Add(123);
            testHelper.InsertData(testTable, tableInserter);
        }

        testHelper.ReadData("SELECT * FROM `/Root/ColumnTableTest` WHERE id=1", "[[1;#;[\"test_res_1\"]]]");

        {
            schema.push_back(TTestHelper::TColumnSchema().SetName("new_column").SetType(NScheme::NTypeIds::Uint64));
            auto alterQuery = TStringBuilder() << "ALTER TABLE `" << testTable.GetName() << "` ADD COLUMN new_column Uint64;";
            auto alterResult = testHelper.GetSession().ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }

        {
            auto settings = TDescribeTableSettings().WithTableStatistics(true);
            auto describeResult = testHelper.GetSession().DescribeTable("/Root/ColumnTableTest", settings).GetValueSync();
            UNIT_ASSERT_C(describeResult.IsSuccess(), describeResult.GetIssues().ToString());

            const auto& description = describeResult.GetTableDescription();
            auto columns = description.GetTableColumns();
            UNIT_ASSERT_VALUES_EQUAL(columns.size(), 4);
        }

        testHelper.ReadData("SELECT * FROM `/Root/ColumnTableTest` WHERE id=1", "[[1;#;#;[\"test_res_1\"]]]");
        testHelper.ReadData("SELECT new_column FROM `/Root/ColumnTableTest` WHERE id=1", "[[#]]");
        testHelper.ReadData("SELECT resource_id FROM `/Root/ColumnTableTest` WHERE id=1", "[[[\"test_res_1\"]]]");

        {
            TTestHelper::TUpdatesBuilder tableInserter(testTable.GetArrowSchema(schema));
            tableInserter.AddRow().Add(3).Add("test_res_3").Add(123).Add<uint64_t>(200);
            testHelper.InsertData(testTable, tableInserter);
        }

        testHelper.ReadData("SELECT * FROM `/Root/ColumnTableTest` WHERE id=3", "[[3;[123];[200u];[\"test_res_3\"]]]");
        testHelper.ReadData("SELECT new_column FROM `/Root/ColumnTableTest` WHERE id=3", "[[[200u]]]");
        testHelper.ReadData("SELECT resource_id FROM `/Root/ColumnTableTest` WHERE id=3", "[[[\"test_res_3\"]]]");
        testHelper.ReadData("SELECT new_column FROM `/Root/ColumnTableTest`", "[[#];[#];[[200u]]]");
    }

    Y_UNIT_TEST(AddColumnOldScheme) {
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper testHelper(runnerSettings);

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("resource_id").SetType(NScheme::NTypeIds::Utf8),
            TTestHelper::TColumnSchema().SetName("level").SetType(NScheme::NTypeIds::Int32)
        };

        TTestHelper::TColumnTable testTable;

        testTable.SetName("/Root/ColumnTableTest").SetPrimaryKey({"id"}).SetSharding({"id"}).SetSchema(schema);
        testHelper.CreateTable(testTable);
        {
            auto alterQuery = TStringBuilder() << "ALTER TABLE `" << testTable.GetName() << "` ADD COLUMN new_column Uint64;";
            auto alterResult = testHelper.GetSession().ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }
        {
            TTestHelper::TUpdatesBuilder tableInserter(testTable.GetArrowSchema(schema));
            tableInserter.AddRow().Add(1).Add("test_res_1").AddNull();
            testHelper.InsertData(testTable, tableInserter, {}, EStatus::SCHEME_ERROR);
        }
    }

    Y_UNIT_TEST(AddColumnOnSchemeChange) {
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper testHelper(runnerSettings);

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("resource_id").SetType(NScheme::NTypeIds::Utf8),
            TTestHelper::TColumnSchema().SetName("level").SetType(NScheme::NTypeIds::Int32)
        };

        TTestHelper::TColumnTable testTable;

        testTable.SetName("/Root/ColumnTableTest").SetPrimaryKey({"id"}).SetSharding({"id"}).SetSchema(schema);
        testHelper.CreateTable(testTable);

        {
            TTestHelper::TUpdatesBuilder tableInserter(testTable.GetArrowSchema(schema));
            tableInserter.AddRow().Add(1).Add("test_res_1").AddNull();
            testHelper.InsertData(testTable, tableInserter, [&testTable, &testHelper]() {
                auto alterQuery = TStringBuilder() << "ALTER TABLE `" << testTable.GetName() << "` ADD COLUMN new_column Uint64;";
                auto alterResult = testHelper.GetSession().ExecuteSchemeQuery(alterQuery).GetValueSync();
            });
        }
        testHelper.ReadData("SELECT * FROM `/Root/ColumnTableTest` WHERE id=1", "[[1;#;#;[\"test_res_1\"]]]");
    }

    Y_UNIT_TEST(AddColumnWithStore) {
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper testHelper(runnerSettings);

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("resource_id").SetType(NScheme::NTypeIds::Utf8),
            TTestHelper::TColumnSchema().SetName("level").SetType(NScheme::NTypeIds::Int32)
        };

        TTestHelper::TColumnTableStore testTableStore;

        testTableStore.SetName("/Root/TableStoreTest").SetPrimaryKey({"id"}).SetSchema(schema);
        testHelper.CreateTable(testTableStore);
        TTestHelper::TColumnTable testTable;
        testTable.SetName("/Root/TableStoreTest/ColumnTableTest").SetPrimaryKey({"id"}).SetSharding({"id"}).SetSchema(schema);
        testHelper.CreateTable(testTable);

        {
            TTestHelper::TUpdatesBuilder tableInserter(testTable.GetArrowSchema(schema));
            tableInserter.AddRow().Add(1).Add("test_res_1").AddNull();
            tableInserter.AddRow().Add(2).Add("test_res_2").Add(123);
            testHelper.InsertData(testTable, tableInserter);
        }

        testHelper.ReadData("SELECT * FROM `/Root/TableStoreTest/ColumnTableTest` WHERE id=1", "[[1;#;[\"test_res_1\"]]]");

        {
            schema.push_back(TTestHelper::TColumnSchema().SetName("new_column").SetType(NScheme::NTypeIds::Uint64));
            auto alterQuery = TStringBuilder() << "ALTER OBJECT `" << testTableStore.GetName() << "` (TYPE TABLESTORE) SET (ACTION=NEW_COLUMN, NAME=new_column, TYPE=Uint64);";

            auto alterResult = testHelper.GetSession().ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }

        {
            auto settings = TDescribeTableSettings().WithTableStatistics(true);
            auto describeResult = testHelper.GetSession().DescribeTable("/Root/TableStoreTest/ColumnTableTest", settings).GetValueSync();
            UNIT_ASSERT_C(describeResult.IsSuccess(), describeResult.GetIssues().ToString());

            const auto& description = describeResult.GetTableDescription();
            auto columns = description.GetTableColumns();
            UNIT_ASSERT_VALUES_EQUAL(columns.size(), 4);
        }

        testHelper.ReadData("SELECT * FROM `/Root/TableStoreTest/ColumnTableTest` WHERE id=1", "[[1;#;#;[\"test_res_1\"]]]");
        testHelper.ReadData("SELECT new_column FROM `/Root/TableStoreTest/ColumnTableTest` WHERE id=1", "[[#]]");
        testHelper.ReadData("SELECT resource_id FROM `/Root/TableStoreTest/ColumnTableTest` WHERE id=1", "[[[\"test_res_1\"]]]");

        {
            TTestHelper::TUpdatesBuilder tableInserter(testTable.GetArrowSchema(schema));
            tableInserter.AddRow().Add(3).Add("test_res_3").Add(123).Add<uint64_t>(200);
            testHelper.InsertData(testTable, tableInserter);
        }

        testHelper.ReadData("SELECT * FROM `/Root/TableStoreTest/ColumnTableTest` WHERE id=3", "[[3;[123];[200u];[\"test_res_3\"]]]");
        testHelper.ReadData("SELECT new_column FROM `/Root/TableStoreTest/ColumnTableTest` WHERE id=3", "[[[200u]]]");
        testHelper.ReadData("SELECT resource_id FROM `/Root/TableStoreTest/ColumnTableTest` WHERE id=3", "[[[\"test_res_3\"]]]");
        testHelper.ReadData("SELECT new_column FROM `/Root/TableStoreTest/ColumnTableTest`", "[[#];[#];[[200u]]]");

    //    testHelper.RebootTablets(testTable.GetName());
    //    testHelper.ReadData("SELECT new_column FROM `/Root/TableStoreTest/ColumnTableTest`", "[[#];[#];[[200u]]]");
    }

    Y_UNIT_TEST(AddColumnErrors) {
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper testHelper(runnerSettings);

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("resource_id").SetType(NScheme::NTypeIds::Utf8),
            TTestHelper::TColumnSchema().SetName("level").SetType(NScheme::NTypeIds::Int32)
        };
        TTestHelper::TColumnTable testTable;

        testTable.SetName("/Root/ColumnTableTest").SetPrimaryKey({"id"}).SetSharding({"id"}).SetSchema(schema);
        testHelper.CreateTable(testTable);

        {
            schema.push_back(TTestHelper::TColumnSchema().SetName("new_column").SetType(NScheme::NTypeIds::Uint64).SetNullable(false));
            auto alterQuery = TStringBuilder() << "ALTER TABLE `" << testTable.GetName() << "`ADD COLUMN new_column Uint64 NOT NULL;";
            auto alterResult = testHelper.GetSession().ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), EStatus::SCHEME_ERROR, alterResult.GetIssues().ToString());
        }
    }
}


} // namespace NKqp
} // namespace NKikimr

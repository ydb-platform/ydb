#include <ydb/core/kqp/gateway/behaviour/resource_pool_classifier/fetcher.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/ut/common/columnshard.h>
#include <ydb/core/kqp/workload_service/actors/actors.h>
#include <ydb/core/kqp/workload_service/ut/common/kqp_workload_service_ut_common.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/test_helper/controllers.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb-cpp-sdk/client/draft/ydb_replication.h>
#include <ydb-cpp-sdk/client/proto/accessor.h>
#include <ydb-cpp-sdk/client/scheme/scheme.h>
#include <ydb-cpp-sdk/client/topic/client.h>
#include <ydb/core/testlib/cs_helper.h>
#include <ydb/core/testlib/common_helper.h>
#include <yql/essentials/types/uuid/uuid.h>
#include <yql/essentials/types/binary_json/write.h>

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

        auto result = session.ExecuteDataQuery(R"(
            SELECT * FROM `/Root/KeyValue`;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL_C(
            result.GetStatus(), EStatus::SCHEME_ERROR, result.GetIssues().ToString());

        result = session.ExecuteDataQuery(R"(
            SELECT * FROM `/Root/NonExistent`;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL_C(
            result.GetStatus(), EStatus::SCHEME_ERROR, result.GetIssues().ToString());
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

    Y_UNIT_TEST(MoveTableWithSerialTypes) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString tableName = "/Root/TableWithSerial";
        TString newTableName = "/Root/TableWithSerialMoved";
        {
            auto query = TStringBuilder() << R"(
                CREATE TABLE `)" << tableName << R"(` (
                    Key Serial,
                    Value Int32,
                    PRIMARY KEY (Key)
                );)";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto queryUpsert = TStringBuilder() << R"(
                INSERT INTO `)" << tableName << R"(` (Value) VALUES (1), (2), (3);
            )";
            auto result = session.ExecuteDataQuery(queryUpsert, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto querySelect = TStringBuilder() << R"(
                SELECT * FROM `)" << tableName << R"(`;
            )";
            auto result = session.ExecuteDataQuery(querySelect, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_C(!result.GetResultSets().empty(), "results are empty");
            CompareYson(R"([
                [1;[1]];[2;[2]];[3;[3]]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
        {
            TDescribeTableResult describe = session.DescribeTable(tableName, TDescribeTableSettings().WithSetVal(true)).GetValueSync();
            UNIT_ASSERT_EQUAL(describe.GetStatus(), EStatus::SUCCESS);
            const auto& tableDescription = describe.GetTableDescription();
            bool hasSerial = false;
            for (const auto& column : tableDescription.GetTableColumns()) {
                if (column.Name == "Key") {
                    UNIT_ASSERT(column.SequenceDescription.has_value());
                    hasSerial = true;
                    break;
                }
            }
            UNIT_ASSERT(hasSerial);
        }
        {
            auto query = TStringBuilder() << R"(
                ALTER TABLE `)" << tableName << R"(` RENAME TO `)" << newTableName << R"(`;
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto describeResult = session.DescribeTable(tableName).GetValueSync();
            UNIT_ASSERT(!describeResult.IsSuccess());
        }
        {
            TDescribeTableResult describe = session.DescribeTable(newTableName, TDescribeTableSettings().WithSetVal(true)).GetValueSync();
            UNIT_ASSERT_EQUAL(describe.GetStatus(), EStatus::SUCCESS);
            const auto& tableDescription = describe.GetTableDescription();
            bool hasSerial = false;
            for (const auto& column : tableDescription.GetTableColumns()) {
                if (column.Name == "Key") {
                    UNIT_ASSERT(column.SequenceDescription.has_value());
                    UNIT_ASSERT(column.SequenceDescription->SetVal.has_value());
                    UNIT_ASSERT_VALUES_EQUAL(column.SequenceDescription->SetVal->NextValue, 4);
                    UNIT_ASSERT_VALUES_EQUAL(column.SequenceDescription->SetVal->NextUsed, false);
                    hasSerial = true;
                    break;
                }
            }
            UNIT_ASSERT(hasSerial);
        }
        {
            auto queryUpsert = TStringBuilder() << R"(
                INSERT INTO `)" << newTableName << R"(` (Value) VALUES (4), (5), (6);
            )";
            auto result = session.ExecuteDataQuery(queryUpsert, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto querySelect = TStringBuilder() << R"(
                SELECT * FROM `)" << newTableName << R"(`;
            )";
            auto result = session.ExecuteDataQuery(querySelect, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_C(!result.GetResultSets().empty(), "results are empty");
            CompareYson(R"([
                [1;[1]];[2;[2]];[3;[3]];[4;[4]];[5;[5]];[6;[6]]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
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
            UNIT_ASSERT(partSettings.GetPartitioningBySize().has_value());
            UNIT_ASSERT_VALUES_EQUAL(partSettings.GetPartitioningBySize().value(), false);
            UNIT_ASSERT(partSettings.GetPartitioningByLoad().has_value());
            UNIT_ASSERT_VALUES_EQUAL(partSettings.GetPartitioningByLoad().value(), false);
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
            UNIT_ASSERT(partSettings.GetPartitioningBySize().has_value());
            UNIT_ASSERT_VALUES_EQUAL(partSettings.GetPartitioningBySize().value(), true);
            UNIT_ASSERT(partSettings.GetPartitioningByLoad().has_value());
            UNIT_ASSERT_VALUES_EQUAL(partSettings.GetPartitioningByLoad().value(), false);
            UNIT_ASSERT_VALUES_EQUAL(partSettings.GetMinPartitionsCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(partSettings.GetPartitionSizeMb(), 2048);
        }

        AlterTableSetttings(session, tableName, {{"AUTO_PARTITIONING_BY_SIZE", "DISABLED"}}, compat);
        {
            TDescribeTableResult describe = session.DescribeTable(tableName).GetValueSync();
            UNIT_ASSERT_EQUAL(describe.GetStatus(), EStatus::SUCCESS);
            const auto& partSettings = describe.GetTableDescription().GetPartitioningSettings();
            UNIT_ASSERT(partSettings.GetPartitioningBySize().has_value());
            UNIT_ASSERT_VALUES_EQUAL(partSettings.GetPartitioningBySize().value(), false);
            UNIT_ASSERT(partSettings.GetPartitioningByLoad().has_value());
            UNIT_ASSERT_VALUES_EQUAL(partSettings.GetPartitioningByLoad().value(), false);
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
            UNIT_ASSERT_C(describeResult.IsSuccess(), describeResult.GetIssues().ToString());
        }

        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            ALTER TABLE `moved` RENAME TO `/Root/table`
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto describeResult = session.DescribeTable("/Root/table").GetValueSync();
            UNIT_ASSERT_C(describeResult.IsSuccess(), describeResult.GetIssues().ToString());
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
                UNIT_ASSERT_C(describeResult.IsSuccess(), describeResult.GetIssues().ToString());
            }
            {
                auto describeResult = session.DescribeTable("/Root/movedsecond").GetValueSync();
                UNIT_ASSERT_C(describeResult.IsSuccess(), describeResult.GetIssues().ToString());
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
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SCHEME_ERROR, result.GetIssues().ToString());
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
                UNIT_ASSERT_C(describeResult.IsSuccess(), describeResult.GetIssues().ToString());
            }

            {
                auto describeResult = session.DescribeTable("/Root/table").GetValueSync();
                UNIT_ASSERT_C(!describeResult.IsSuccess(), describeResult.GetIssues().ToString());
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

        createTable(R"(Interval("P1D") ON CreatedAt)", EStatus::SCHEME_ERROR, "Cannot enable TTL on unknown column");

        createTable(R"(Interval("P1D") ON StringValue)", EStatus::SCHEME_ERROR, "Unsupported column type");

        createTable(R"(Interval("P1D") ON Uint32Value)", EStatus::SCHEME_ERROR, "'ValueSinceUnixEpochModeSettings' should be specified");
        createTable(R"(Interval("P1D") ON Uint64Value)", EStatus::SCHEME_ERROR, "'ValueSinceUnixEpochModeSettings' should be specified");
        createTable(R"(Interval("P1D") ON DyNumberValue)", EStatus::SCHEME_ERROR, "'ValueSinceUnixEpochModeSettings' should be specified");

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
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Can't drop TTL column", result.GetIssues().ToString());
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

    void CreateTableWithTtlOnIntColumn(TValueSinceUnixEpochModeSettings::EUnit unit) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString tableName = "/Root/TableWithTtlSettings";

        auto query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE TABLE `)" << tableName << R"(` (
                Key Uint64,
                IntColumn Uint64,
                PRIMARY KEY (Key)
            ) WITH (
                TTL = Interval("P1D") ON IntColumn AS )" << unit << R"(
            ))";
        {
            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto result = session.DescribeTable(tableName).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetTableDescription().GetTtlSettings()->GetValueSinceUnixEpoch().GetColumnUnit(), unit);
        }
    }

    Y_UNIT_TEST(CreateTableWithTtlOnIntColumn) {
        const auto cases = TVector<TValueSinceUnixEpochModeSettings::EUnit>{
            TValueSinceUnixEpochModeSettings::EUnit::Seconds,
            TValueSinceUnixEpochModeSettings::EUnit::MilliSeconds,
            TValueSinceUnixEpochModeSettings::EUnit::MicroSeconds,
            TValueSinceUnixEpochModeSettings::EUnit::NanoSeconds,
        };

        for (auto unit : cases) {
            CreateTableWithTtlOnIntColumn(unit);
        }
    }

    Y_UNIT_TEST(CreateTableWithTtlOnDatetime64Column) {
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableTableDatetime64(true);

        auto settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetFeatureFlags(featureFlags);

        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString tableName = "/Root/TableWithTtlSettings";

        auto query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE TABLE `)" << tableName << R"(` (
                Key Uint64,
                Datetime64Column Datetime64,
                PRIMARY KEY (Key)
            ) WITH (
                TTL = Interval("P1D") ON Datetime64Column
            ))";
            Cerr << query << Endl;
        {
            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
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
        UNIT_ASSERT_C(describeResult.IsSuccess(), describeResult.GetIssues().ToString());
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
        UNIT_ASSERT_C(describeResult.IsSuccess(), describeResult.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(describeResult.GetTableDescription().GetPartitionsCount(), 5);

        auto extractValue = [](const TValue& val) {
            auto parser = TValueParser(val);
            parser.OpenTuple();
            UNIT_ASSERT(parser.TryNextElement());
            return parser.GetOptionalUint64().value();
        };

        const std::vector<TKeyRange>& keyRanges = describeResult.GetTableDescription().GetKeyRanges();

        size_t n = 0;
        const std::vector<ui64> expectedRanges = { 10ul, 100ul, 1000ul, 10000ul };

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
        UNIT_ASSERT_C(describeResult.IsSuccess(), describeResult.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(describeResult.GetTableDescription().GetPartitionsCount(), 4);

        auto extractValue = [](const TValue& val) {
            auto parser = TValueParser(val);
            parser.OpenTuple();
            UNIT_ASSERT(parser.TryNextElement());
            return parser.GetOptionalInt64().value();
        };

        const std::vector<TKeyRange>& keyRanges = describeResult.GetTableDescription().GetKeyRanges();

        size_t n = 0;
        const std::vector<i64> expectedRanges = { 0l, 10l, 10000l };

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
        UNIT_ASSERT_C(describeResult.IsSuccess(), describeResult.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(describeResult.GetTableDescription().GetPartitionsCount(), 4);

        auto extractValue = [](const TValue& val) {
            auto parser = TValueParser(val);
            parser.OpenTuple();
            UNIT_ASSERT(parser.TryNextElement());
            ui64 pk1 = parser.GetOptionalUint64().value();
            UNIT_ASSERT(parser.TryNextElement());
            auto pk2 = parser.GetOptionalString();
            return std::pair<ui64, std::optional<std::string>>(pk1, pk2);
        };

        const std::vector<TKeyRange>& keyRanges = describeResult.GetTableDescription().GetKeyRanges();

        size_t n = 0;
        const std::vector<std::pair<ui64, TString>> expectedRanges = {
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
                if (pk2.has_value()) {
                    UNIT_ASSERT_VALUES_EQUAL(pk2.value(), expectedPk2);
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
                if (pk2.has_value()) {
                    UNIT_ASSERT_VALUES_EQUAL(pk2.value(), expectedPk2);
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

    struct testData {
        TString condition;
        ui64 resultRows;
        ui64 touchedPartitions;
    };

    void uuidInsertAndCheck(TSession &session, TString tableName, TVector<testData> expectedPartitions) {
        TVector<TString> uuids {
                {"AAAAAA00-0000-458F-ABE9-4A0CD520903B"},
                {"AAAAAA11-0000-458F-ABE9-4A0CD520903B"},
                {"AAAAAA22-0000-458F-ABE9-4A0CD520903B"},
                {"AAAAAA33-0000-458F-ABE9-4A0CD520903B"},
                {"AAAAAA44-0000-458F-ABE9-4A0CD520903B"},
                {"AAAAAA55-0000-458F-ABE9-4A0CD520903B"},
                {"AAAAAA66-0000-458F-ABE9-4A0CD520903B"},
                {"AAAAAA77-0000-458F-ABE9-4A0CD520903B"},
                {"AAAAAA88-0000-458F-ABE9-4A0CD520903B"},
                {"AAAAAA99-0000-458F-ABE9-4A0CD520903B"},
                {"AAAAAAAA-0000-458F-ABE9-4A0CD520903B"},
                {"AAAAAABB-0000-458F-ABE9-4A0CD520903B"},
                {"AAAAAACC-0000-458F-ABE9-4A0CD520903B"},
                {"AAAAAADD-0000-458F-ABE9-4A0CD520903B"},
                {"AAAAAAEE-0000-458F-ABE9-4A0CD520903B"},
                {"AAAAAAFF-0000-458F-ABE9-4A0CD520903B"},
        };
        {
            TStringBuilder builder;
            builder << "REPLACE INTO `" << tableName << "` (Key, Value) VALUES ";
            for (ui32 i = 0; i < uuids.size() - 1; ++i) {
                builder << "(Uuid(\"" << uuids[i] << "\"), " << i << "),";
            }
            builder << "(Uuid(\"" << uuids[uuids.size() - 1] << "\"), " << uuids.size() - 1 << ");";
            TString query = builder;

            auto replaceResult = session.ExecuteDataQuery(query,
                                                          TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(replaceResult.GetStatus(), EStatus::SUCCESS,
                                       replaceResult.GetIssues().ToString());
        }

        for (auto &test: expectedPartitions) {
            TString query = Sprintf("SELECT Key as cnt from `%s` WHERE %s;", tableName.data(), test.condition.data());

            NYdb::NTable::TExecDataQuerySettings execSettings;
            execSettings.CollectQueryStats(ECollectQueryStatsMode::Profile);

            auto selectResult = session.ExecuteDataQuery(query,
                                                         TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(),
                                                         execSettings).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(selectResult.GetStatus(), EStatus::SUCCESS,
                                       selectResult.GetIssues().ToString());

            UNIT_ASSERT_VALUES_EQUAL(selectResult.GetResultSets().size(), 1);

            auto& stats = NYdb::TProtoAccessor::GetProto(*selectResult.GetStats());
            UNIT_ASSERT_VALUES_EQUAL(selectResult.GetResultSet(0).RowsCount(), test.resultRows);
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access(0).partitions_count(), test.touchedPartitions);
        }
    }

    Y_UNIT_TEST(CreateTableWithPartitionAtKeysUuid) {
        TKikimrSettings kikimrSettings;
        TKikimrRunner kikimr(kikimrSettings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString tableName = "/Root/TableWithPartitionAtKeysSimpleUuid";

        {
            auto builder = TTableBuilder()
                    .AddNonNullableColumn("Key", EPrimitiveType::Uuid)
                    .AddNullableColumn("Value", EPrimitiveType::Int32)
                    .SetPrimaryKeyColumn("Key");

            // Ordering is not lexicographic as UUID is stored in binary form with the following byte order
            // from original hex pairs: [3 2 1 0 5 4 7 6 8 9 a b c d e f]
            // String UUID (with spaces added) 00 11 22 33-44 55-66 77-88 99-AA BB CC DD EE FF
            // becomes                         33 22 11 00 55 44 77 66 88 99 AA BB CC DD EE FF
            const TVector <TUuidValue> expectedRanges = {
                    TUuidValue("FFFFFF11-C00F-458F-ABE9-4A0CD520903B"),
                    TUuidValue("FFFFFFDD-AF48-428B-9D13-893C220118C4")
            };
            auto explicitPartitions = TExplicitPartitions();
            for (ui32 i = 0; i < expectedRanges.size(); i++) {
                explicitPartitions.AppendSplitPoints(
                        TValueBuilder().BeginTuple().AddElement()
                                .OptionalUuid(expectedRanges[i]).EndTuple().Build()
                );
            }
            auto result = session.CreateTable(tableName,
                                              builder.Build(),
                                              TCreateTableSettings()
                                                      .PartitioningPolicy(TPartitioningPolicy().ExplicitPartitions(explicitPartitions))
            ).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        // See comment above for comparison explanation.
        TVector<testData> inputs = {
                {"Key > Cast(\"00000000-FFFF-FFFF-ABE9-4A0CD520903B\" as Uuid)",16, 3},
                {"Key > Cast(\"FFFFFF11-C00F-458F-ABE9-4A0CD520903B\" as Uuid)", 14, 2},
                {"Key < Cast(\"FFFFFF11-C00F-458F-ABE9-4A0CD520903B\" as Uuid)", 2, 1}
        };
        uuidInsertAndCheck(session, tableName, inputs);
    }

    Y_UNIT_TEST(CreateTableWithUniformPartitionsUuid) {
        TKikimrSettings kikimrSettings;
        TKikimrRunner kikimr(kikimrSettings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString tableName = "/Root/TableWithPartitionAtKeysSimpleUuid";

        {
            auto builder = TTableBuilder()
                    .AddNonNullableColumn("Key", EPrimitiveType::Uuid)
                    .AddNullableColumn("Value", EPrimitiveType::Int32)
                    .SetPrimaryKeyColumn("Key");

            auto result = session.CreateTable(tableName,
                                              builder.Build(),
                                              TCreateTableSettings()
                                                  .PartitioningPolicy(TPartitioningPolicy().UniformPartitions(4))
            ).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        // See comment above for comparison explanation.
        TVector<testData> inputs = {
                {"Key > Cast(\"00000000-FFFF-FFFF-ABE9-4A0CD520903B\" as Uuid)",16, 4},
                {"Key < Cast(\"0000003F-C00F-458F-ABE9-4A0CD520903B\" as Uuid)", 4, 1},
                {"Key < Cast(\"0000007F-C00F-458F-ABE9-4A0CD520903B\" as Uuid)", 8, 2},
                {"Key < Cast(\"000000BF-C00F-458F-ABE9-4A0CD520903B\" as Uuid)", 12, 3},
                {"Key < Cast(\"000000FF-C00F-458F-ABE9-4A0CD520903B\" as Uuid)", 15, 4}
        };
        uuidInsertAndCheck(session, tableName, inputs);
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
        UNIT_ASSERT_C(describeResult.IsSuccess(), describeResult.GetIssues().ToString());
        const auto& columnFamilies = describeResult.GetTableDescription().GetColumnFamilies();
        UNIT_ASSERT_VALUES_EQUAL(columnFamilies.size(), 3);
        for (const auto& family : columnFamilies) {
            if (family.GetName() == "Family1") {
                UNIT_ASSERT_VALUES_EQUAL(family.GetData(), "test");
                UNIT_ASSERT_VALUES_EQUAL(family.GetCompression().value(), EColumnFamilyCompression::None);
            } else {
                UNIT_ASSERT(family.GetName() == "default" || family.GetName() == "Family2");
            }
        }
    }

    Y_UNIT_TEST(CreateFamilyWithCompressionLevel) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString tableName = "/Root/TableWithCompressionLevel";
        auto query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE TABLE `)" << tableName
                                      << R"(` (
                Key Uint64,
                Value1 String,
                Value2 Uint32,
                PRIMARY KEY (Key),
                FAMILY Family1 (
                     DATA = "test",
                     COMPRESSION = "lz4",
                     COMPRESSION_LEVEL = 5
                ),
            );)";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Field `COMPRESSION_LEVEL` is not supported for OLTP tables", result.GetIssues().ToString());
    }

    Y_UNIT_TEST(AlterCompressionLevelInColumnFamily) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString tableName = "/Root/TableWithCompressionLevel";
        auto query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE TABLE `)" << tableName
                                      << R"(` (
                Key Uint64,
                Value1 String FAMILY Family1,
                Value2 Uint32,
                PRIMARY KEY (Key),
                FAMILY Family1 (
                     DATA = "test",
                     COMPRESSION = "lz4"
                ),
            );)";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto queryAlter = TStringBuilder() << R"(
            --!syntax_v1
            ALTER TABLE `)" << tableName << R"(`
                ALTER FAMILY Family1 SET COMPRESSION_LEVEL 5;)";
        auto resultAlter = session.ExecuteSchemeQuery(queryAlter).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(resultAlter.GetStatus(), EStatus::BAD_REQUEST, resultAlter.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS_C(resultAlter.GetIssues().ToString(), "Field `COMPRESSION_LEVEL` is not supported for OLTP tables", resultAlter.GetIssues().ToString());
    }

    Y_UNIT_TEST(AddColumnFamilyWithCompressionLevel) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString tableName = "/Root/TableWithCompressionLevel";
        auto query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE TABLE `)" << tableName
                                      << R"(` (
                Key Uint64,
                Value1 String FAMILY Family1,
                Value2 Uint32,
                PRIMARY KEY (Key),
                FAMILY Family1 (
                     DATA = "test",
                     COMPRESSION = "lz4"
                ),
            );)";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto queryAlter = TStringBuilder() << R"(
            --!syntax_v1
            ALTER TABLE `)" << tableName << R"(`
                ADD FAMILY Family2 (
                     DATA = "test",
                     COMPRESSION = "lz4",
                     COMPRESSION_LEVEL = 5
                );)";
        auto resultAlter = session.ExecuteSchemeQuery(queryAlter).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(resultAlter.GetStatus(), EStatus::BAD_REQUEST, resultAlter.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS_C(resultAlter.GetIssues().ToString(), "Field `COMPRESSION_LEVEL` is not supported for OLTP tables", resultAlter.GetIssues().ToString());
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
            UNIT_ASSERT_C(describeResult.IsSuccess(), describeResult.GetIssues().ToString());
            const auto& columnFamilies = describeResult.GetTableDescription().GetColumnFamilies();
            UNIT_ASSERT_VALUES_EQUAL(columnFamilies.size(), 2);
            for (const auto& family : columnFamilies) {
                if (family.GetName() == "Family1") {
                    UNIT_ASSERT_VALUES_EQUAL(family.GetData(), "test");
                    UNIT_ASSERT_VALUES_EQUAL(family.GetCompression().value(), EColumnFamilyCompression::None);
                } else {
                    UNIT_ASSERT(family.GetName() == "default");
                    UNIT_ASSERT_VALUES_EQUAL(family.GetData(), "test");
                    UNIT_ASSERT_VALUES_EQUAL(family.GetCompression().value(), EColumnFamilyCompression::LZ4);
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
            UNIT_ASSERT_C(describeResult.IsSuccess(), describeResult.GetIssues().ToString());
            const auto& columnFamilies = describeResult.GetTableDescription().GetColumnFamilies();
            UNIT_ASSERT_VALUES_EQUAL(columnFamilies.size(), 3);
            for (const auto& family : columnFamilies) {
                if (family.GetName() == "Family1") {
                    UNIT_ASSERT_VALUES_EQUAL(family.GetData(), "test");
                    UNIT_ASSERT_VALUES_EQUAL(family.GetCompression().value(), EColumnFamilyCompression::LZ4);
                } else if (family.GetName() == "Family2") {
                    UNIT_ASSERT_VALUES_EQUAL(family.GetData(), "test");
                    UNIT_ASSERT_VALUES_EQUAL(family.GetCompression().value(), EColumnFamilyCompression::None);
                } else {
                    UNIT_ASSERT(family.GetName() == "default");
                    UNIT_ASSERT_VALUES_EQUAL(family.GetData(), "test");
                    UNIT_ASSERT_VALUES_EQUAL(family.GetCompression().value(), EColumnFamilyCompression::LZ4);
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

    Y_UNIT_TEST(CreateTableWithStoreExternalBlobs) {
        TKikimrRunner kikimr;
        kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnablePublicApiExternalBlobs(true);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString tableName = "/Root/TableWithStoreExternalBlobs";
        auto query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE TABLE `)" << tableName << R"(` (
                Key Uint64,
                Value String,
                PRIMARY KEY (Key)
            )
            WITH (
                STORE_EXTERNAL_BLOBS = ENABLED
            );)";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto describeResult = session.DescribeTable(tableName).GetValueSync();
        UNIT_ASSERT_C(describeResult.IsSuccess(), describeResult.GetIssues().ToString());
        UNIT_ASSERT(describeResult.GetTableDescription().GetStorageSettings().GetStoreExternalBlobs().value_or(false));
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

        auto createAndCheck = [&](ui32 precision, ui32 scale) {
            TString tableName = TStringBuilder() << "/Root/TableWithDecimalColumn" << precision << scale;
            auto createQuery = TStringBuilder() << Sprintf(R"(
            CREATE TABLE `%s` (
                Key Uint64,
                Value Decimal(%u,%u),
                PRIMARY KEY (Key)
            );)", tableName.c_str(), precision, scale);
            auto createResult = session.ExecuteSchemeQuery(createQuery).GetValueSync();

            if (precision == 0) {
                UNIT_ASSERT_VALUES_EQUAL_C(createResult.GetStatus(), EStatus::GENERIC_ERROR, createResult.GetIssues().ToString());
                UNIT_ASSERT_STRING_CONTAINS_C(createResult.GetIssues().ToString(), "Invalid decimal precision", createResult.GetIssues().ToString());
                return;
            }
            if (precision == 33) {
                UNIT_ASSERT_VALUES_EQUAL_C(createResult.GetStatus(), EStatus::GENERIC_ERROR, createResult.GetIssues().ToString());
                UNIT_ASSERT_STRING_CONTAINS_C(createResult.GetIssues().ToString(), "Invalid decimal parameters", createResult.GetIssues().ToString());
                return;
            }
            if (precision == 36) {
                UNIT_ASSERT_VALUES_EQUAL_C(createResult.GetStatus(), EStatus::GENERIC_ERROR, createResult.GetIssues().ToString());
                UNIT_ASSERT_STRING_CONTAINS_C(createResult.GetIssues().ToString(), "Invalid decimal precision", createResult.GetIssues().ToString());
                return;
            }
            if (precision == 999) {
                UNIT_ASSERT_VALUES_EQUAL_C(createResult.GetStatus(), EStatus::GENERIC_ERROR, createResult.GetIssues().ToString());
                UNIT_ASSERT_STRING_CONTAINS_C(createResult.GetIssues().ToString(), " Invalid decimal precision", createResult.GetIssues().ToString());
                return;
            }

            UNIT_ASSERT_VALUES_EQUAL_C(createResult.GetStatus(), EStatus::SUCCESS, createResult.GetIssues().ToString());

            TDescribeTableResult describe = session.DescribeTable(tableName).GetValueSync();
            UNIT_ASSERT_EQUAL_C(describe.GetStatus(), EStatus::SUCCESS, describe.GetIssues().ToString());
            auto tableDesc = describe.GetTableDescription();
            std::vector<TTableColumn> columns = tableDesc.GetTableColumns();
            UNIT_ASSERT_VALUES_EQUAL(columns.size(), 2);
            TType valueType = columns[1].Type;
            TTypeParser parser(valueType);
            auto optionalKind = parser.GetKind();
            UNIT_ASSERT_EQUAL(optionalKind, TTypeParser::ETypeKind::Optional);
            parser.OpenOptional();
            auto kind = parser.GetKind();
            UNIT_ASSERT_EQUAL(kind, TTypeParser::ETypeKind::Decimal);
            TDecimalType decimalType = parser.GetDecimal();
            UNIT_ASSERT_VALUES_EQUAL(decimalType.Precision, precision);
            UNIT_ASSERT_VALUES_EQUAL(decimalType.Scale, scale);
        };

        createAndCheck(0, 0);
        createAndCheck(1, 0);
        createAndCheck(2, 1);
        createAndCheck(22, 9);
        createAndCheck(35, 10);
        createAndCheck(22, 20);
        createAndCheck(33, 34);
        createAndCheck(36, 35);
        createAndCheck(999, 99);
    }

    Y_UNIT_TEST(CreateTableWithPgColumn) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto createAndCheck = [&](const TString& typeName) {
            TString tableName = TStringBuilder() << "/Root/TableWithPgColumn_" << typeName;
            auto createQuery = TStringBuilder() << Sprintf(R"(
            CREATE TABLE `%s` (
                Key Uint64,
                Value %s,
                PRIMARY KEY (Key)
            );)", tableName.c_str(), typeName.c_str());
            auto createResult = session.ExecuteSchemeQuery(createQuery).GetValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(createResult.GetStatus(), EStatus::SUCCESS, createResult.GetIssues().ToString());

            TDescribeTableResult describe = session.DescribeTable(tableName).GetValueSync();
            UNIT_ASSERT_EQUAL_C(describe.GetStatus(), EStatus::SUCCESS, describe.GetIssues().ToString());
            auto tableDesc = describe.GetTableDescription();
            std::vector<TTableColumn> columns = tableDesc.GetTableColumns();
            UNIT_ASSERT_VALUES_EQUAL(columns.size(), 2);
            TType valueType = columns[1].Type;
            TTypeParser parser(valueType);
            auto kind = parser.GetKind();
            UNIT_ASSERT_EQUAL(kind, TTypeParser::ETypeKind::Pg);
            const auto& pgType = parser.GetPg();
            UNIT_ASSERT_VALUES_EQUAL(pgType.TypeName, typeName);
        };

        createAndCheck("pgbool");
        createAndCheck("pgint2");
        createAndCheck("pgint4");
        createAndCheck("pgint8");
        createAndCheck("pgnumeric");
        createAndCheck("pgfloat4");
        createAndCheck("pgfloat8");
        createAndCheck("pgtext");
        createAndCheck("pgjson");
        createAndCheck("pgtimestamp");
        createAndCheck("pgvarchar");
    }

    void AlterTableAddIndex(EIndexTypeSql type) {
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableVectorIndex(true);
        auto settings = TKikimrSettings().SetFeatureFlags(featureFlags);
        TKikimrRunner kikimr(settings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_DEBUG);

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        CreateSampleTablesWithIndex(session);

        const auto typeStr = IndexTypeSqlString(type).data();
        const auto subtypeStr = IndexSubtypeSqlString(type).data();
        const auto withStr = IndexWithSqlString(type).data();

        // Non-covered index, single column
        {
            auto status = session.ExecuteSchemeQuery(Sprintf(R"(
                --!syntax_v1
                ALTER TABLE `/Root/Test` ADD INDEX NameIndex %s %s ON (Name) %s;
            )", typeStr, subtypeStr, withStr)).ExtractValueSync();
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

            if (type == EIndexTypeSql::GlobalVectorKMeansTree) {
                const auto& vectorIndexSettings = std::get<TKMeansTreeSettings>(indexDesc.back().GetIndexSettings()).Settings;
                UNIT_ASSERT_VALUES_EQUAL(vectorIndexSettings.Metric, TVectorIndexSettings::EMetric::InnerProduct);
                UNIT_ASSERT_VALUES_EQUAL(vectorIndexSettings.VectorType, TVectorIndexSettings::EVectorType::Float);
                UNIT_ASSERT_VALUES_EQUAL(vectorIndexSettings.VectorDimension, 1024);

                describe = session.DescribeTable(TString{"/Root/Test/NameIndex/"} + NTableIndex::NTableVectorKmeansTreeIndex::LevelTable).GetValueSync();
                UNIT_ASSERT_EQUAL(describe.GetStatus(), EStatus::SUCCESS);
                describe = session.DescribeTable(TString{"/Root/Test/NameIndex/"} + NTableIndex::NTableVectorKmeansTreeIndex::PostingTable).GetValueSync();
                UNIT_ASSERT_EQUAL(describe.GetStatus(), EStatus::SUCCESS);
            }
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

        // Non-covered index, multiple columns
        {
            auto status = session.ExecuteSchemeQuery(Sprintf(R"(
                --!syntax_v1
                ALTER TABLE `/Root/Test` ADD INDEX CommentIndex %s %s ON (Group, Comment) %s;
            )", typeStr, subtypeStr, withStr)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(status.GetStatus(), EStatus::SUCCESS, status.GetIssues().ToString());
        }

        {
            TDescribeTableResult describe = session.DescribeTable("/Root/Test").GetValueSync();
            UNIT_ASSERT_EQUAL(describe.GetStatus(), EStatus::SUCCESS);
            auto indexDesc = describe.GetTableDescription().GetIndexDescriptions();

            UNIT_ASSERT_VALUES_EQUAL(indexDesc.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.back().GetIndexName(), "CommentIndex");
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.back().GetIndexType(), IndexTypeSqlToIndexType(type));
            std::vector<std::string> indexColumns{"Group", "Comment"};
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.back().GetIndexColumns(), indexColumns);
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.back().GetDataColumns().size(), 0);

            if (type == EIndexTypeSql::GlobalVectorKMeansTree) {
                const auto& vectorIndexSettings = std::get<TKMeansTreeSettings>(indexDesc.back().GetIndexSettings()).Settings;
                UNIT_ASSERT_VALUES_EQUAL(vectorIndexSettings.Metric, TVectorIndexSettings::EMetric::InnerProduct);
                UNIT_ASSERT_VALUES_EQUAL(vectorIndexSettings.VectorType, TVectorIndexSettings::EVectorType::Float);
                UNIT_ASSERT_VALUES_EQUAL(vectorIndexSettings.VectorDimension, 1024);

                describe = session.DescribeTable(TString{"/Root/Test/CommentIndex/"} + NTableIndex::NTableVectorKmeansTreeIndex::LevelTable).GetValueSync();
                UNIT_ASSERT_EQUAL(describe.GetStatus(), EStatus::SUCCESS);
                describe = session.DescribeTable(TString{"/Root/Test/CommentIndex/"} + NTableIndex::NTableVectorKmeansTreeIndex::PostingTable).GetValueSync();
                UNIT_ASSERT_EQUAL(describe.GetStatus(), EStatus::SUCCESS);
                describe = session.DescribeTable(TString{"/Root/Test/CommentIndex/"} + NTableIndex::NTableVectorKmeansTreeIndex::PrefixTable).GetValueSync();
                UNIT_ASSERT_EQUAL(describe.GetStatus(), EStatus::SUCCESS);
            }
        }

        {
            auto status = session.ExecuteSchemeQuery(R"(
                --!syntax_v1
                ALTER TABLE `/Root/Test` DROP INDEX CommentIndex;
            )").ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(status.GetStatus(), EStatus::SUCCESS, status.GetIssues().ToString());
        }

        {
            TDescribeTableResult describe = session.DescribeTable("/Root/Test").GetValueSync();
            UNIT_ASSERT_EQUAL(describe.GetStatus(), EStatus::SUCCESS);
            auto indexDesc = describe.GetTableDescription().GetIndexDescriptions();
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.size(), 0);
        }

        // Covered index, single column
        {
            auto status = session.ExecuteSchemeQuery(Sprintf(R"(
                --!syntax_v1
                ALTER TABLE `/Root/Test` ADD INDEX NameIndex %s %s ON (Name) COVER (Amount) %s;
            )", typeStr, subtypeStr, withStr)).ExtractValueSync();
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

            if (type == EIndexTypeSql::GlobalVectorKMeansTree) {
                const auto& vectorIndexSettings = std::get<TKMeansTreeSettings>(indexDesc.back().GetIndexSettings()).Settings;
                UNIT_ASSERT_VALUES_EQUAL(vectorIndexSettings.Metric, TVectorIndexSettings::EMetric::InnerProduct);
                UNIT_ASSERT_VALUES_EQUAL(vectorIndexSettings.VectorType, TVectorIndexSettings::EVectorType::Float);
                UNIT_ASSERT_VALUES_EQUAL(vectorIndexSettings.VectorDimension, 1024);

                describe = session.DescribeTable(TString{"/Root/Test/NameIndex/"} + NTableIndex::NTableVectorKmeansTreeIndex::LevelTable).GetValueSync();
                UNIT_ASSERT_EQUAL(describe.GetStatus(), EStatus::SUCCESS);
                describe = session.DescribeTable(TString{"/Root/Test/NameIndex/"} + NTableIndex::NTableVectorKmeansTreeIndex::PostingTable).GetValueSync();
                UNIT_ASSERT_EQUAL(describe.GetStatus(), EStatus::SUCCESS);
           }
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

        // Covered index, multiple columns
        {
            auto status = session.ExecuteSchemeQuery(Sprintf(R"(
                --!syntax_v1
                ALTER TABLE `/Root/Test` ADD INDEX CommentIndex %s %s ON (Group, Comment) COVER (Amount) %s;
            )", typeStr, subtypeStr, withStr)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(status.GetStatus(), EStatus::SUCCESS, status.GetIssues().ToString());
        }

        {
            TDescribeTableResult describe = session.DescribeTable("/Root/Test").GetValueSync();
            UNIT_ASSERT_EQUAL(describe.GetStatus(), EStatus::SUCCESS);
            auto indexDesc = describe.GetTableDescription().GetIndexDescriptions();
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.back().GetIndexName(), "CommentIndex");
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.back().GetIndexType(), IndexTypeSqlToIndexType(type));
            std::vector<std::string> indexColumns{"Group", "Comment"};
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.back().GetIndexColumns(), indexColumns);
            std::vector<std::string> dataColumns{"Amount"};
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.back().GetDataColumns(), dataColumns);

            if (type == EIndexTypeSql::GlobalVectorKMeansTree) {
                const auto& vectorIndexSettings = std::get<TKMeansTreeSettings>(indexDesc.back().GetIndexSettings()).Settings;
                UNIT_ASSERT_VALUES_EQUAL(vectorIndexSettings.Metric, TVectorIndexSettings::EMetric::InnerProduct);
                UNIT_ASSERT_VALUES_EQUAL(vectorIndexSettings.VectorType, TVectorIndexSettings::EVectorType::Float);
                UNIT_ASSERT_VALUES_EQUAL(vectorIndexSettings.VectorDimension, 1024);

                describe = session.DescribeTable(TString{"/Root/Test/CommentIndex/"} + NTableIndex::NTableVectorKmeansTreeIndex::LevelTable).GetValueSync();
                UNIT_ASSERT_EQUAL(describe.GetStatus(), EStatus::SUCCESS);
                describe = session.DescribeTable(TString{"/Root/Test/CommentIndex/"} + NTableIndex::NTableVectorKmeansTreeIndex::PostingTable).GetValueSync();
                UNIT_ASSERT_EQUAL(describe.GetStatus(), EStatus::SUCCESS);
                describe = session.DescribeTable(TString{"/Root/Test/CommentIndex/"} + NTableIndex::NTableVectorKmeansTreeIndex::PrefixTable).GetValueSync();
                UNIT_ASSERT_EQUAL(describe.GetStatus(), EStatus::SUCCESS);
            }
        }

        {
            auto status = session.ExecuteSchemeQuery(R"(
                --!syntax_v1
                ALTER TABLE `/Root/Test` DROP INDEX CommentIndex;
            )").ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(status.GetStatus(), EStatus::SUCCESS, status.GetIssues().ToString());
        }

        {
            TDescribeTableResult describe = session.DescribeTable("/Root/Test").GetValueSync();
            UNIT_ASSERT_EQUAL(describe.GetStatus(), EStatus::SUCCESS);
            auto indexDesc = describe.GetTableDescription().GetIndexDescriptions();
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.size(), 0);
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

    Y_UNIT_TEST(AlterTableAddExplicitSyncVectorKMeansTreeIndex) {
        AlterTableAddIndex(EIndexTypeSql::GlobalVectorKMeansTree);
    }

    Y_UNIT_TEST(AlterTableAlterIndex) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        CreateSampleTablesWithIndex(session);

        constexpr int minPartitionsCount = 10;
        {
            auto result = session.ExecuteSchemeQuery(Sprintf(R"(
                        ALTER TABLE `/Root/SecondaryKeys` ALTER INDEX Index SET AUTO_PARTITIONING_MIN_PARTITIONS_COUNT %d;
                    )", minPartitionsCount
                )
            ).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            auto describe = session.DescribeTable("/Root/SecondaryKeys/Index/indexImplTable").GetValueSync();
            UNIT_ASSERT_C(describe.IsSuccess(), describe.GetIssues().ToString());
            auto indexDesc = describe.GetTableDescription();
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.GetPartitioningSettings().GetMinPartitionsCount(), minPartitionsCount);
        }

        constexpr int partitionSizeMb = 555;
        {
          auto result = session.ExecuteSchemeQuery(Sprintf(R"(
                        ALTER TABLE `/Root/SecondaryKeys` ALTER INDEX Index SET AUTO_PARTITIONING_PARTITION_SIZE_MB %d;
                    )", partitionSizeMb)
            ).ExtractValueSync();
          UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            auto describe = session.DescribeTable("/Root/SecondaryKeys/Index/indexImplTable").GetValueSync();
            UNIT_ASSERT_C(describe.IsSuccess(), describe.GetIssues().ToString());
            auto indexDesc = describe.GetTableDescription();
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.GetPartitioningSettings().GetPartitionSizeMb(), partitionSizeMb);
        }
    }

    Y_UNIT_TEST(AlterTableAlterVectorIndex) {
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableVectorIndex(true);
        auto settings = TKikimrSettings().SetFeatureFlags(featureFlags);
        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        {
            TString create_index_query = R"(
                --!syntax_v1
                CREATE TABLE `/Root/TestTable` (
                    Key Uint64,
                    Embedding String,
                    PRIMARY KEY (Key),
                    INDEX vector_idx
                        GLOBAL USING vector_kmeans_tree
                        ON (Embedding)
                        WITH (similarity=cosine, vector_type=bit, vector_dimension=1)
                );
            )";
            auto result = session.ExecuteSchemeQuery(create_index_query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto describe = session.DescribeTable("/Root/TestTable/vector_idx/indexImplPostingTable").GetValueSync();
            UNIT_ASSERT_C(describe.IsSuccess(), describe.GetIssues().ToString());
            auto indexDesc = describe.GetTableDescription();
            constexpr int defaultPartitionSizeMb = 2048;
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.GetPartitioningSettings().GetPartitionSizeMb(), defaultPartitionSizeMb);
        }
        {
            auto result = session.ExecuteSchemeQuery(R"(
                        ALTER TABLE `/Root/TestTable` ALTER INDEX vector_idx SET AUTO_PARTITIONING_MIN_PARTITIONS_COUNT 1;
                    )").ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Only index with one impl table is supported", result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(AlterTableAlterMissedIndex) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        CreateSampleTablesWithIndex(session);
        {
            auto result = session.ExecuteSchemeQuery(R"(
                        ALTER TABLE `/Root/SecondaryKeys` ALTER INDEX WrongIndexName SET AUTO_PARTITIONING_MIN_PARTITIONS_COUNT 1;
                    )").ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SCHEME_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Unknown index name: WrongIndexName", result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(AlterIndexImplTable) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        CreateSampleTablesWithIndex(session);

        constexpr int minPartitionsCount = 10;
        {
            auto result = session.ExecuteSchemeQuery(Sprintf(R"(
                        ALTER TABLE `/Root/SecondaryKeys/Index/indexImplTable` SET AUTO_PARTITIONING_MIN_PARTITIONS_COUNT %d;
                    )", minPartitionsCount
                )
            ).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SCHEME_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(),
                "Error: Cannot find table 'db.[/Root/SecondaryKeys/Index/indexImplTable]' because it does not exist or you do not have access permissions.",
                result.GetIssues().ToString()
            );
        }
    }

    Y_UNIT_TEST(AlterIndexImplTableUsingPublicAPI) {
        TKikimrRunner kikimr;
        kikimr.GetTestClient().GrantConnect("user@builtin");

        auto adminSession = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        CreateSampleTablesWithIndex(adminSession);

        auto grantPermissions = [&adminSession](const char* permissions, const char* path, const char* user) {
            auto grantQuery = Sprintf(R"(
                    GRANT %s ON `%s` TO `%s`;
                )",
                permissions, path, user
            );
            auto result = adminSession.ExecuteSchemeQuery(grantQuery).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            // It was discovered that TModifyACL scheme operation returns successfully without waiting for
            // SchemeBoard replicas to acknowledge the path updates. This can cause the SchemeCache to reply
            // with outdated entries, even if the SyncVersion flag is enabled.
            // For more details, please refer to the PR description of this change.
            Sleep(TDuration::MilliSeconds(300));
        };

        // a user which does not have any implicit permissions
        auto userClient = NYdb::NTable::TTableClient(kikimr.GetDriver(), NYdb::NTable::TClientSettings()
            .AuthToken("user@builtin")
        );
        auto userSession = userClient.CreateSession().GetValueSync().GetSession();

        constexpr int minPartitionsCount = 10;
        auto tableSettings = NYdb::NTable::TAlterTableSettings()
            .BeginAlterPartitioningSettings()
                .SetMinPartitionsCount(minPartitionsCount)
            .EndAlterPartitioningSettings();

        // try altering indexImplTable without ALTER SCHEMA permission
        {
            auto result = userSession.AlterTable("/Root/SecondaryKeys/Index/indexImplTable", tableSettings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::UNAUTHORIZED, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(),
                "Error: Access denied for user@builtin on path /Root/SecondaryKeys/Index/indexImplTable",
                result.GetIssues().ToString()
            );
        }
        // grant necessary permission
        {
            grantPermissions("ALTER SCHEMA", "/Root/SecondaryKeys", "user@builtin");
            auto result = userSession.AlterTable("/Root/SecondaryKeys/Index/indexImplTable", tableSettings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        // check result
        {
            grantPermissions("DESCRIBE SCHEMA", "/Root/SecondaryKeys", "user@builtin");
            auto describe = userSession.DescribeTable("/Root/SecondaryKeys/Index/indexImplTable").ExtractValueSync();
            UNIT_ASSERT_C(describe.IsSuccess(), describe.GetIssues().ToString());
            auto indexDesc = describe.GetTableDescription();
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.GetPartitioningSettings().GetMinPartitionsCount(), minPartitionsCount);
        }

        // try altering non-partitioning setting of indexImplTable as non-superuser
        auto forbiddenSettings = NYdb::NTable::TAlterTableSettings().SetCompactionPolicy("default");
        {
            auto result = userSession.AlterTable("/Root/SecondaryKeys/Index/indexImplTable", forbiddenSettings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SCHEME_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(),
                "Error: Check failed: path: '/Root/SecondaryKeys/Index/indexImplTable', error: path is not a common path",
                result.GetIssues().ToString()
            );
        }
        // become superuser
        {
            kikimr.GetTestServer().GetRuntime()->GetAppData().AdministrationAllowedSIDs.emplace_back("user@builtin");
            auto result = userSession.AlterTable("/Root/SecondaryKeys/Index/indexImplTable", forbiddenSettings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
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

    Y_UNIT_TEST(CreateTableWithUniqConstraint) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        {
            TString create_index_query = R"(
                --!syntax_v1
                CREATE TABLE `/Root/TestTable` (
                    Key Uint64,
                    Value String,
                    PRIMARY KEY (Key),
                    INDEX uniq_value_idx GLOBAL UNIQUE SYNC ON (`Value`)
                );
            )";
            auto result = session.ExecuteSchemeQuery(create_index_query).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto result = session.DescribeTable("/Root/TestTable").ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NYdb::EStatus::SUCCESS);

            auto indexDesc = result.GetTableDescription().GetIndexDescriptions();

            UNIT_ASSERT_VALUES_EQUAL(indexDesc.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.back().GetIndexName(), "uniq_value_idx");
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.back().GetIndexType(), EIndexType::GlobalUnique);
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.back().GetIndexColumns().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.back().GetDataColumns().size(), 0);
        }
    }

    Y_UNIT_TEST(CreateTableWithUniqConstraintPublicApi) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        {
            auto builder = TTableBuilder()
                .AddNullableColumn("Key", EPrimitiveType::Uint64)
                .AddNullableColumn("Value", EPrimitiveType::String)
                .SetPrimaryKeyColumn("Key")
                .AddUniqueSecondaryIndex("uniq_value_idx", {"Value"});

            auto result = session.CreateTable("/Root/TestTable", builder.Build()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto result = session.DescribeTable("/Root/TestTable").ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NYdb::EStatus::SUCCESS);

            auto indexDesc = result.GetTableDescription().GetIndexDescriptions();

            UNIT_ASSERT_VALUES_EQUAL(indexDesc.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.back().GetIndexName(), "uniq_value_idx");
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.back().GetIndexType(), EIndexType::GlobalUnique);
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.back().GetIndexColumns().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.back().GetDataColumns().size(), 0);
        }
    }

    Y_UNIT_TEST(CreateTableWithVectorIndex) {
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableVectorIndex(true);
        auto settings = TKikimrSettings().SetFeatureFlags(featureFlags);
        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        {
            TString create_index_query = R"(
                --!syntax_v1
                CREATE TABLE `/Root/TestTable` (
                    Key Uint64,
                    Embedding String,
                    PRIMARY KEY (Key),
                    INDEX vector_idx
                        GLOBAL USING vector_kmeans_tree
                        ON (Embedding)
                        WITH (similarity=inner_product, vector_type=float, vector_dimension=1024)
                );
            )";

            auto result = session.ExecuteSchemeQuery(create_index_query).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto result = session.DescribeTable("/Root/TestTable").ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NYdb::EStatus::SUCCESS);

            UNIT_ASSERT_VALUES_EQUAL(result.GetTableDescription().GetIndexDescriptions().size(), 1);
            auto indexDesc = result.GetTableDescription().GetIndexDescriptions()[0];
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.GetIndexName(), "vector_idx");
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.GetIndexType(), EIndexType::GlobalVectorKMeansTree);
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.GetIndexColumns().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.GetIndexColumns()[0], "Embedding");
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.GetDataColumns().size(), 0);
            const auto& vectorIndexSettings = std::get<TKMeansTreeSettings>(indexDesc.GetIndexSettings()).Settings;
            UNIT_ASSERT_VALUES_EQUAL(vectorIndexSettings.Metric, NYdb::NTable::TVectorIndexSettings::EMetric::InnerProduct);
            UNIT_ASSERT_VALUES_EQUAL(vectorIndexSettings.VectorType, NYdb::NTable::TVectorIndexSettings::EVectorType::Float);
            UNIT_ASSERT_VALUES_EQUAL(vectorIndexSettings.VectorDimension, 1024);
        }
        {
            auto describeLevelTable = session.DescribeTable("/Root/TestTable/vector_idx/indexImplLevelTable").GetValueSync();
            UNIT_ASSERT_C(describeLevelTable.IsSuccess(), describeLevelTable.GetIssues().ToString());
            auto describePostingTable = session.DescribeTable("/Root/TestTable/vector_idx/indexImplPostingTable").GetValueSync();
            UNIT_ASSERT_C(describePostingTable.IsSuccess(), describePostingTable.GetIssues().ToString());
        }
    }


    Y_UNIT_TEST(CreateTableWithVectorIndexCovered) {
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableVectorIndex(true);
        auto settings = TKikimrSettings().SetFeatureFlags(featureFlags);
        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        {
            TString create_index_query = R"(
                --!syntax_v1
                CREATE TABLE `/Root/TestTable` (
                    Key Uint64,
                    Embedding String,
                    Covered String,
                    PRIMARY KEY (Key),
                    INDEX vector_idx
                        GLOBAL USING vector_kmeans_tree
                        ON (Embedding)
                        COVER (Covered)
                        WITH (similarity=inner_product, vector_type=float, vector_dimension=1024)
                );
            )";
            auto result = session.ExecuteSchemeQuery(create_index_query).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto result = session.DescribeTable("/Root/TestTable").ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NYdb::EStatus::SUCCESS);

            UNIT_ASSERT_VALUES_EQUAL(result.GetTableDescription().GetIndexDescriptions().size(), 1);
            auto indexDesc = result.GetTableDescription().GetIndexDescriptions()[0];
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.GetIndexName(), "vector_idx");
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.GetIndexType(), EIndexType::GlobalVectorKMeansTree);
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.GetIndexColumns().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.GetIndexColumns()[0], "Embedding");
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.GetDataColumns().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.GetDataColumns()[0], "Covered");
            const auto& vectorIndexSettings = std::get<TKMeansTreeSettings>(indexDesc.GetIndexSettings()).Settings;
            UNIT_ASSERT_VALUES_EQUAL(vectorIndexSettings.Metric, NYdb::NTable::TVectorIndexSettings::EMetric::InnerProduct);
            UNIT_ASSERT_VALUES_EQUAL(vectorIndexSettings.VectorType, NYdb::NTable::TVectorIndexSettings::EVectorType::Float);
            UNIT_ASSERT_VALUES_EQUAL(vectorIndexSettings.VectorDimension, 1024);
        }
    }


    Y_UNIT_TEST(CreateTableWithVectorIndexCaseIncentive) {
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableVectorIndex(true);
        auto settings = TKikimrSettings().SetFeatureFlags(featureFlags);
        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        {
            TString create_index_query = R"(
                --!syntax_v1
                CREATE TABLE `/Root/TestTable` (
                    Key Uint64,
                    Embedding String,
                    PRIMARY KEY (Key),
                    INDEX vector_idx
                        GLOBAL USING vector_KMEANS_tree
                        ON (Embedding)
                        WITH (similarity=COSINE, VECTOR_TYPE=float, vector_DIMENSION=1024)
                );
            )";
            auto result = session.ExecuteSchemeQuery(create_index_query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(CreateTableWithVectorIndexNoFeatureFlag) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        {
            TString create_index_query = R"(
                --!syntax_v1
                CREATE TABLE `/Root/TestTable` (
                    Key Uint64,
                    Embedding String,
                    Covered String,
                    PRIMARY KEY (Key),
                    INDEX vector_idx
                        GLOBAL USING vector_kmeans_tree
                        ON (Embedding)
                        WITH (similarity=inner_product, vector_type=float, vector_dimension=1024)
                );
            )";
            auto result = session.ExecuteSchemeQuery(create_index_query).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(CreateTableWithVectorIndexPublicApi) {
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableVectorIndex(true);
        auto settings = TKikimrSettings().SetFeatureFlags(featureFlags);
        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        {
            auto builder = TTableBuilder()
                .AddNullableColumn("Key", EPrimitiveType::Uint64)
                .AddNullableColumn("Embedding", EPrimitiveType::String)
                .SetPrimaryKeyColumn("Key")
                .AddVectorKMeansTreeIndex("vector_idx", {"Embedding"}, {TVectorIndexSettings{
                    NYdb::NTable::TVectorIndexSettings::EMetric::CosineDistance,
                    NYdb::NTable::TVectorIndexSettings::EVectorType::Float,
                    1024,
                }});

            auto result = session.CreateTable("/Root/TestTable", builder.Build()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto result = session.DescribeTable("/Root/TestTable").ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NYdb::EStatus::SUCCESS);

            UNIT_ASSERT_VALUES_EQUAL(result.GetTableDescription().GetIndexDescriptions().size(), 1);
            auto indexDesc = result.GetTableDescription().GetIndexDescriptions()[0];
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.GetIndexName(), "vector_idx");
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.GetIndexType(), EIndexType::GlobalVectorKMeansTree);
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.GetIndexColumns().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.GetIndexColumns()[0], "Embedding");
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.GetDataColumns().size(), 0);
            const auto& vectorIndexSettings = std::get<TKMeansTreeSettings>(indexDesc.GetIndexSettings()).Settings;
            UNIT_ASSERT_VALUES_EQUAL(vectorIndexSettings.Metric, NYdb::NTable::TVectorIndexSettings::EMetric::CosineDistance);
            UNIT_ASSERT_VALUES_EQUAL(vectorIndexSettings.VectorType, NYdb::NTable::TVectorIndexSettings::EVectorType::Float);
            UNIT_ASSERT_VALUES_EQUAL(vectorIndexSettings.VectorDimension, 1024);
        }
    }

    Y_UNIT_TEST(CreateTableWithVectorIndexCoveredPublicApi) {
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableVectorIndex(true);
        auto settings = TKikimrSettings().SetFeatureFlags(featureFlags);
        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        {
            auto builder = TTableBuilder()
                .AddNullableColumn("Key", EPrimitiveType::Uint64)
                .AddNullableColumn("Embedding", EPrimitiveType::String)
                .AddNullableColumn("Covered", EPrimitiveType::String)
                .SetPrimaryKeyColumn("Key")
                .AddVectorKMeansTreeIndex("vector_idx", {"Embedding"}, {"Covered"}, {TVectorIndexSettings{
                    NYdb::NTable::TVectorIndexSettings::EMetric::CosineDistance,
                    NYdb::NTable::TVectorIndexSettings::EVectorType::Float,
                    1024,
                }});

            auto result = session.CreateTable("/Root/TestTable", builder.Build()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto result = session.DescribeTable("/Root/TestTable").ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NYdb::EStatus::SUCCESS);

            UNIT_ASSERT_VALUES_EQUAL(result.GetTableDescription().GetIndexDescriptions().size(), 1);
            auto indexDesc = result.GetTableDescription().GetIndexDescriptions()[0];
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.GetIndexName(), "vector_idx");
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.GetIndexType(), EIndexType::GlobalVectorKMeansTree);
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.GetIndexColumns().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.GetIndexColumns()[0], "Embedding");
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.GetDataColumns().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.GetDataColumns()[0], "Covered");
            const auto& vectorIndexSettings = std::get<TKMeansTreeSettings>(indexDesc.GetIndexSettings()).Settings;
            UNIT_ASSERT_VALUES_EQUAL(vectorIndexSettings.Metric, NYdb::NTable::TVectorIndexSettings::EMetric::CosineDistance);
            UNIT_ASSERT_VALUES_EQUAL(vectorIndexSettings.VectorType, NYdb::NTable::TVectorIndexSettings::EVectorType::Float);
            UNIT_ASSERT_VALUES_EQUAL(vectorIndexSettings.VectorDimension, 1024);
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
                Value String,
                PRIMARY KEY (Key)
            );)";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        auto addColumn = [&] (ui32 precision, ui32 scale) {
            TString columnName = TStringBuilder() << "Column" << precision << scale;
            auto query = TStringBuilder() << Sprintf(R"(
            ALTER TABLE `%s`
                 ADD COLUMN %s Decimal(%u,%u)
            )", tableName.c_str(), columnName.c_str(), precision, scale);
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();

            if (precision == 0) {
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
                UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Invalid decimal precision", result.GetIssues().ToString());
                return;
            }
            if (precision == 33) {
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
                UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Invalid decimal parameters", result.GetIssues().ToString());
                return;
            }
            if (precision == 36) {
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
                UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Invalid decimal precision", result.GetIssues().ToString());
                return;
            }
            if (precision == 999) {
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
                UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), " Invalid decimal precision", result.GetIssues().ToString());
                return;
            }

            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        };

        addColumn(0, 0);
        addColumn(1, 0);
        addColumn(2, 1);
        addColumn(22, 9);
        addColumn(35, 10);
        addColumn(22, 20);
        addColumn(33, 34);
        addColumn(36, 35);
        addColumn(999, 99);

        TDescribeTableResult describe = session.DescribeTable(tableName).GetValueSync();
        UNIT_ASSERT_EQUAL_C(describe.GetStatus(), EStatus::SUCCESS, describe.GetIssues().ToString());
        auto tableDesc = describe.GetTableDescription();
        std::vector<TTableColumn> columns = tableDesc.GetTableColumns();
        UNIT_ASSERT_VALUES_EQUAL(columns.size(), 7);

        auto checkColumn = [&] (ui64 columnIdx, ui32 precision, ui32 scale) {
            TType valueType = columns[columnIdx].Type;
            TTypeParser parser(valueType);
            auto optionalKind = parser.GetKind();
            UNIT_ASSERT_EQUAL(optionalKind, TTypeParser::ETypeKind::Optional);
            parser.OpenOptional();
            auto kind = parser.GetKind();
            UNIT_ASSERT_VALUES_EQUAL(kind, TTypeParser::ETypeKind::Decimal);
            TDecimalType decimalType = parser.GetDecimal();
            UNIT_ASSERT_VALUES_EQUAL(decimalType.Precision, precision);
            UNIT_ASSERT_VALUES_EQUAL(decimalType.Scale, scale);
        };

        checkColumn(0,22, 20);
        checkColumn(3, 1, 0);
        checkColumn(4, 2, 1);
        checkColumn(5, 22,9);
        checkColumn(6, 35, 10);
    }

    Y_UNIT_TEST(AlterTableWithPgColumn) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString tableName = "/Root/TableWithPgColumn";
        {
            auto query = TStringBuilder() << R"(
            CREATE TABLE `)" << tableName << R"(` (
                Key Uint64,
                Value String,
                PRIMARY KEY (Key)
            );)";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        auto addColumn = [&] (const TString& typeName) {
            TString columnName = TStringBuilder() << "Column_" << typeName;
            auto query = TStringBuilder() << Sprintf(R"(
            ALTER TABLE `%s`
                 ADD COLUMN %s %s
            )", tableName.c_str(), columnName.c_str(), typeName.c_str());
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        };

        addColumn("pgbool");
        addColumn("pgint2");
        addColumn("pgint4");
        addColumn("pgint8");
        addColumn("pgnumeric");
        addColumn("pgfloat4");
        addColumn("pgfloat8");
        addColumn("pgtext");
        addColumn("pgjson");
        addColumn("pgtimestamp");
        addColumn("pgvarchar");

        TDescribeTableResult describe = session.DescribeTable(tableName).GetValueSync();
        UNIT_ASSERT_EQUAL_C(describe.GetStatus(), EStatus::SUCCESS, describe.GetIssues().ToString());
        auto tableDesc = describe.GetTableDescription();
        std::vector<TTableColumn> columns = tableDesc.GetTableColumns();
        UNIT_ASSERT_VALUES_EQUAL(columns.size(), 13);

        auto checkColumn = [&] (ui64 columnIdx, const TString& typeName) {
            TType valueType = columns[columnIdx].Type;
            TTypeParser parser(valueType);
            auto kind = parser.GetKind();
            UNIT_ASSERT_VALUES_EQUAL(kind, TTypeParser::ETypeKind::Pg);
            const auto& pgType = parser.GetPg();
            UNIT_ASSERT_VALUES_EQUAL(pgType.TypeName, typeName);
        };

        checkColumn(2, "pgbool");
        checkColumn(3, "pgint2");
        checkColumn(4, "pgint4");
        checkColumn(5, "pgint8");
        checkColumn(6, "pgnumeric");
        checkColumn(7, "pgfloat4");
        checkColumn(8, "pgfloat8");
        checkColumn(9, "pgtext");
        checkColumn(10, "pgjson");
        checkColumn(11, "pgtimestamp");
        checkColumn(12, "pgvarchar");
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
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(CreateAlterUserWithHash) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
                CREATE USER user1 HASH '{
                    "hash": "p4ffeMugohqyBwyckYCK1TjJfz3LIHbKiGL+t+oEhzw=",
                    "salt": "U+tzBtgo06EBQCjlARA6Jg==",
                    "type": "argon2id"
                }';
            )";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
                CREATE USER user2 HASH '{
                    "hash": "p4ffeMugohqyBwyckYCK1TjJfz3LIHbKiGL+t+oEhzw=",
                    "salt": "wrongSaltLength",
                    "type": "argon2id"
                }';
            )";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Length of field \'salt\' is 15, but it must be equal 24");
        }
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
                CREATE USER user3 HASH '{
                    "hash": "wrongHashLength",
                    "salt": "U+tzBtgo06EBQCjlARA6Jg==",
                    "type": "argon2id"
                }';
            )";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Length of field \'hash\' is 15, but it must be equal 44");
        }
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
                CREATE USER user4 HASH '{
                    "hash": "p4ffeMugohqyBwyckYCK1TjJfz3LIHbKiGL+t+oEhzw=",
                    "salt": "U+tzBtgo06EBQCjlARA6Jg==",
                    "type": "wrongtype"
                }';
            )";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Field \'type\' must be equal \"argon2id\"");
        }

        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
                CREATE USER user5 HASH '{{{{}}}
                    "hash": "p4ffeMugohqyBwyckYCK1TjJfz3LIHbKiGL+t+oEhzw=",
                    "salt": "U+tzBtgo06EBQCjlARA6Jg==",
                    "type": "argon2id"
                ';
            )";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Cannot parse hash value; it should be in JSON-format");
        }

        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
                CREATE USER user6 HASH '{
                    "hash": "p4ffeMugohqyBwyckYCK1TjJfz3LIHbKiGL+t+oEhzw=",
                    "salt": "U+tzBtgo06EBQCjlARA6Jg==",
                    "type": "argon2id",
                    "some_strange_field": "some_strange_value"
                }';
            )";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "There should be strictly three fields here: salt, hash and type");
        }
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
                CREATE USER user7 HASH '{
                    "hash": "Field not in base64format but with 44 length",
                    "salt": "U+tzBtgo06EBQCjlARA6Jg==",
                    "type": "argon2id"
                }';
            )";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Field \'hash\' must be in base64 format");
        }
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
                CREATE USER user8 HASH '{
                    "hash": "p4ffeMugohqyBwyckYCK1TjJfz3LIHbKiGL+t+oEhzw=",
                    "salt": "Not in base64 format =) ",
                    "type": "argon2id"
                }';
            )";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Field \'salt\' must be in base64 format");
        }


        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
                CREATE USER user9;
                ALTER USER user9 HASH '{
                    "hash": "p4ffeMugohqyBwyckYCK1TjJfz3LIHbKiGL+t+oEhzw=",
                    "salt": "U+tzBtgo06EBQCjlARA6Jg==",
                    "type": "argon2id"
                }';
            )";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
                CREATE USER user10;
                ALTER USER user10 HASH '{
                    "hash": "p4ffeMugohqyBwyckYCK1TjJfz3LIHbKiGL+t+oEhzw=",
                    "salt": "wrongSaltLength",
                    "type": "argon2id"
                }';
            )";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Length of field \'salt\' is 15, but it must be equal 24");
        }
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
                CREATE USER user11;
                ALTER USER user11 HASH '{
                    "hash": "wrongHashLength",
                    "salt": "U+tzBtgo06EBQCjlARA6Jg==",
                    "type": "argon2id"
                }';
            )";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Length of field \'hash\' is 15, but it must be equal 44");
        }
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
                CREATE USER user12;
                ALTER USER user12 HASH '{
                    "hash": "p4ffeMugohqyBwyckYCK1TjJfz3LIHbKiGL+t+oEhzw=",
                    "salt": "U+tzBtgo06EBQCjlARA6Jg==",
                    "type": "wrongtype"
                }';
            )";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Field \'type\' must be equal \"argon2id\"");
        }

        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
                CREATE USER user13;
                ALTER USER user13 HASH '{{{{}}}
                    "hash": "p4ffeMugohqyBwyckYCK1TjJfz3LIHbKiGL+t+oEhzw=",
                    "salt": "U+tzBtgo06EBQCjlARA6Jg==",
                    "type": "argon2id"
                ';
            )";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Cannot parse hash value; it should be in JSON-format");
        }

        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
                CREATE USER user14;
                ALTER USER user14 HASH '{
                    "hash": "p4ffeMugohqyBwyckYCK1TjJfz3LIHbKiGL+t+oEhzw=",
                    "salt": "U+tzBtgo06EBQCjlARA6Jg==",
                    "type": "argon2id",
                    "some_strange_field": "some_strange_value"
                }';
            )";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "There should be strictly three fields here: salt, hash and type");
        }
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
                CREATE USER user15;
                ALTER USER user15 HASH '{
                    "hash": "Field not in base64format but with 44 length",
                    "salt": "U+tzBtgo06EBQCjlARA6Jg==",
                    "type": "argon2id"
                }';
            )";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Field \'hash\' must be in base64 format");
        }
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
                CREATE USER user16;
                ALTER USER user16 HASH '{
                    "hash": "p4ffeMugohqyBwyckYCK1TjJfz3LIHbKiGL+t+oEhzw=",
                    "salt": "Not in base64 format =) ",
                    "type": "argon2id"
                }';
            )";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Field \'salt\' must be in base64 format");
        }
    }

    Y_UNIT_TEST(CreateAlterUserLoginNoLogin) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        {
            auto query = TStringBuilder() << R"(
                --!syntax_v1
                CREATE USER user1 ENCRYPTED PASSWORD '123' LOGIN;
            )";
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto query = TStringBuilder() << R"(
                --!syntax_v1
                CREATE USER user2 ENCRYPTED PASSWORD '123' NOLOGIN;
            )";
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto query = TStringBuilder() << R"(
                --!syntax_v1
                CREATE USER user3 ENCRYPTED PASSWORD '123';
                ALTER USER user3 NOLOGIN;
            )";
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto query = TStringBuilder() << R"(
                --!syntax_v1
                CREATE USER user4 ENCRYPTED PASSWORD '123' NOLOGIN;
                ALTER USER user4 LOGIN;
            )";
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto query = TStringBuilder() << R"(
                --!syntax_v1
                CREATE USER user5 someNonExistentOption;
            )";
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "extraneous input \'someNonExistentOption\'");
        }

        {
            auto query = TStringBuilder() << R"(
                --!syntax_v1
                CREATE USER user6;
                ALTER USER user6 someNonExistentOption;
            )";
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "mismatched input \'someNonExistentOption\'");
        }
    }

    struct ExpectedPermissions {
        TString Path;
        THashMap<TString, TVector<TString>> Permissions;
    };

    void CheckPermissions(TSession& session, TVector<ExpectedPermissions>&& expectedPermissionsValues) {
        for (auto& value : expectedPermissionsValues) {
            TDescribeTableResult describe = session.DescribeTable(value.Path).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(describe.GetStatus(), EStatus::SUCCESS);
            auto tableDesc = describe.GetTableDescription();
            const auto& permissions = tableDesc.GetPermissions();

            THashMap<TString, TVector<TString>> describePermissions;
            for (const auto& permission : permissions) {
                auto& permissionNames = describePermissions[permission.Subject];
                permissionNames.insert(permissionNames.end(), permission.PermissionNames.begin(), permission.PermissionNames.end());
            }

            auto& expectedPermissions = value.Permissions;
            UNIT_ASSERT_VALUES_EQUAL_C(expectedPermissions.size(), describePermissions.size(), "Number of user names does not equal on path: " + value.Path);
            for (auto& item : expectedPermissions) {
                auto& expectedPermissionNames = item.second;
                auto& describedPermissionNames = describePermissions[item.first];
                UNIT_ASSERT_VALUES_EQUAL_C(expectedPermissionNames.size(), describedPermissionNames.size(), "Number of permissions for " + item.first + " does not equal on path: " + value.Path);
                sort(expectedPermissionNames.begin(), expectedPermissionNames.end());
                sort(describedPermissionNames.begin(), describedPermissionNames.end());
                UNIT_ASSERT_VALUES_EQUAL_C(expectedPermissionNames, describedPermissionNames, "Permissions are not equal on path: " + value.Path);
            }
        }
    }

    Y_UNIT_TEST(ModifyPermissions) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE USER user1 PASSWORD 'password1';
            CREATE USER user2 PASSWORD 'password2';
            CREATE USER user3 PASSWORD 'password3';
            CREATE USER user4 PASSWORD 'password4';
            CREATE USER user5 PASSWORD 'password5';
            CREATE USER user6 PASSWORD 'password6';
            CREATE USER user7 PASSWORD 'password7';
            CREATE USER user8 PASSWORD 'password8';
            CREATE USER user9 PASSWORD 'password9';
            )";
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE TABLE `)" << "/Root/table1" << R"(` (
                Key Uint64,
                Value String,
                PRIMARY KEY (Key)
            );)";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            query.clear();
            query << R"(
            --!syntax_v1
            CREATE TABLE `)" << "/Root/table2" << R"(` (
                Key Uint64,
                Value String,
                PRIMARY KEY (Key)
            );)";
            result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            GRANT CONNECT ON `/Root` TO user1;
            )";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CheckPermissions(session, {
                                            {.Path = "/Root",
                                                .Permissions = {
                                                            {"user1", {"ydb.database.connect"}}
                                                            }
                                            },
                                            {
                                                .Path = "/Root/table1",
                                                .Permissions = {}
                                            },
                                            {
                                                .Path = "/Root/table2",
                                                .Permissions = {}
                                            }
                                        });
        }
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            REVOKE "ydb.database.connect" ON `/Root` FROM user1;
            )";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CheckPermissions(session, {
                                            {
                                                .Path = "/Root",
                                                .Permissions = {}
                                            },
                                            {
                                                .Path = "/Root/table1",
                                                .Permissions = {}
                                            },
                                            {
                                                .Path = "/Root/table2",
                                                .Permissions = {}
                                            }
                                        });
        }
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            GRANT MODIFY TABLES, 'ydb.tables.read' ON `/Root/table1`, `/Root/table2` TO user2;
            )";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CheckPermissions(session, {
                                            {
                                                .Path = "/Root",
                                                .Permissions = {}
                                            },
                                            {
                                                .Path = "/Root/table1",
                                                .Permissions = {
                                                    {"user2", {"ydb.tables.read", "ydb.tables.modify"}}
                                                }
                                            },
                                            {
                                                .Path = "/Root/table2",
                                                .Permissions = {
                                                    {"user2", {"ydb.tables.read", "ydb.tables.modify"}}
                                                }
                                            }
                                        });
        }
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            REVOKE SELECT TABLES, "ydb.tables.modify", ON `/Root/table2` FROM user2;
            )";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CheckPermissions(session, {
                                            {
                                                .Path = "/Root",
                                                .Permissions = {}
                                            },
                                            {
                                                .Path = "/Root/table1",
                                                .Permissions = {
                                                    {"user2", {"ydb.tables.read", "ydb.tables.modify"}}
                                                }
                                            },
                                            {
                                                .Path = "/Root/table2",
                                                .Permissions = {}
                                            }
                                        });
        }
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            GRANT "ydb.generic.read", LIST, "ydb.generic.write", USE LEGACY ON `/Root` TO user3, user4, user5;
            )";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CheckPermissions(session, {
                                            {
                                                .Path = "/Root",
                                                .Permissions = {
                                                    {"user3", {"ydb.generic.read", "ydb.generic.list", "ydb.generic.write", "ydb.generic.use_legacy"}},
                                                    {"user4", {"ydb.generic.read", "ydb.generic.list", "ydb.generic.write", "ydb.generic.use_legacy"}},
                                                    {"user5", {"ydb.generic.read", "ydb.generic.list", "ydb.generic.write", "ydb.generic.use_legacy"}}
                                                }
                                            },
                                            {
                                                .Path = "/Root/table1",
                                                .Permissions = {
                                                    {"user2", {"ydb.tables.read", "ydb.tables.modify"}}
                                                }
                                            },
                                            {
                                                .Path = "/Root/table2",
                                                .Permissions = {}
                                            }
                                        });
        }
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            REVOKE "ydb.generic.use_legacy", SELECT, "ydb.generic.list", INSERT ON `/Root` FROM user4, user3;
            )";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CheckPermissions(session, {
                                            {
                                                .Path = "/Root",
                                                .Permissions = {
                                                    {"user5", {"ydb.generic.read", "ydb.generic.list", "ydb.generic.write", "ydb.generic.use_legacy"}}
                                                }
                                            },
                                            {
                                                .Path = "/Root/table1",
                                                .Permissions = {
                                                    {"user2", {"ydb.tables.read", "ydb.tables.modify"}}
                                                }
                                            },
                                            {
                                                .Path = "/Root/table2",
                                                .Permissions = {}
                                            }
                                        });
        }
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            GRANT ALL ON `/Root` TO user6;
            )";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CheckPermissions(session, {
                                            {
                                                .Path = "/Root",
                                                .Permissions = {
                                                    {"user5", {"ydb.generic.read", "ydb.generic.list", "ydb.generic.write", "ydb.generic.use_legacy"}},
                                                    {"user6", {"ydb.generic.full"}}
                                                }
                                            },
                                            {
                                                .Path = "/Root/table1",
                                                .Permissions = {
                                                    {"user2", {"ydb.tables.read", "ydb.tables.modify"}}
                                                }
                                            },
                                            {
                                                .Path = "/Root/table2",
                                                .Permissions = {}
                                            }
                                        });
        }
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            REVOKE ALL PRIVILEGES ON `/Root` FROM user6;
            )";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CheckPermissions(session, {
                                            {
                                                .Path = "/Root",
                                                .Permissions = {
                                                    {"user5", {"ydb.generic.read", "ydb.generic.list", "ydb.generic.write", "ydb.generic.use_legacy"}}
                                                }
                                            },
                                            {
                                                .Path = "/Root/table1",
                                                .Permissions = {
                                                    {"user2", {"ydb.tables.read", "ydb.tables.modify"}}
                                                }
                                            },
                                            {
                                                .Path = "/Root/table2",
                                                .Permissions = {}
                                            }
                                        });
        }
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            GRANT "ydb.generic.use", "ydb.generic.manage" ON `/Root` TO user7 WITH GRANT OPTION;
            )";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CheckPermissions(session, {
                                            {
                                                .Path = "/Root",
                                                .Permissions = {
                                                    {"user5", {"ydb.generic.read", "ydb.generic.list", "ydb.generic.write", "ydb.generic.use_legacy"}},
                                                    {"user7", {"ydb.generic.use", "ydb.generic.manage", "ydb.access.grant"}}
                                                }
                                            },
                                            {
                                                .Path = "/Root/table1",
                                                .Permissions = {
                                                    {"user2", {"ydb.tables.read", "ydb.tables.modify"}}
                                                }
                                            },
                                            {
                                                .Path = "/Root/table2",
                                                .Permissions = {}
                                            }
                                        });
        }
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            REVOKE GRANT OPTION FOR USE, MANAGE ON `/Root` FROM user7;
            )";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CheckPermissions(session, {
                                            {
                                                .Path = "/Root",
                                                .Permissions = {
                                                    {"user5", {"ydb.generic.read", "ydb.generic.list", "ydb.generic.write", "ydb.generic.use_legacy"}}
                                                }
                                            },
                                            {
                                                .Path = "/Root/table1",
                                                .Permissions = {
                                                    {"user2", {"ydb.tables.read", "ydb.tables.modify"}}
                                                }
                                            },
                                            {
                                                .Path = "/Root/table2",
                                                .Permissions = {}
                                            }
                                        });
        }
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            GRANT USE LEGACY, FULL LEGACY, FULL, CREATE, DROP, GRANT,
                  SELECT ROW, UPDATE ROW, ERASE ROW, SELECT ATTRIBUTES,
                  MODIFY ATTRIBUTES, CREATE DIRECTORY, CREATE TABLE, CREATE QUEUE,
                  REMOVE SCHEMA, DESCRIBE SCHEMA, ALTER SCHEMA ON `/Root` TO user8;
            )";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CheckPermissions(session, {
                                            {
                                                .Path = "/Root",
                                                .Permissions = {
                                                    {"user5", {"ydb.generic.read", "ydb.generic.list", "ydb.generic.write", "ydb.generic.use_legacy"}},
                                                    {"user8", {"ydb.generic.use_legacy", "ydb.generic.full_legacy", "ydb.generic.full",
                                                                   "ydb.database.create", "ydb.database.drop", "ydb.access.grant", "ydb.granular.select_row",
                                                                   "ydb.granular.update_row", "ydb.granular.erase_row", "ydb.granular.read_attributes",
                                                                  "ydb.granular.write_attributes", "ydb.granular.create_directory", "ydb.granular.create_table",
                                                                  "ydb.granular.create_queue", "ydb.granular.remove_schema", "ydb.granular.describe_schema", "ydb.granular.alter_schema"}}
                                                }
                                            },
                                            {
                                                .Path = "/Root/table1",
                                                .Permissions = {
                                                    {"user2", {"ydb.tables.read", "ydb.tables.modify"}}
                                                }
                                            },
                                            {
                                                .Path = "/Root/table2",
                                                .Permissions = {}
                                            }
                                        });
        }
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            REVOKE "ydb.granular.write_attributes", "ydb.granular.create_directory", "ydb.granular.create_table", "ydb.granular.create_queue",
                   "ydb.granular.select_row", "ydb.granular.update_row", "ydb.granular.erase_row", "ydb.granular.read_attributes",
                   "ydb.generic.use_legacy", "ydb.generic.full_legacy", "ydb.generic.full", "ydb.database.create", "ydb.database.drop", "ydb.access.grant",
                   "ydb.granular.remove_schema", "ydb.granular.describe_schema", "ydb.granular.alter_schema" ON `/Root` FROM user8;
            )";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CheckPermissions(session, {
                                            {
                                                .Path = "/Root",
                                                .Permissions = {
                                                    {"user5", {"ydb.generic.read", "ydb.generic.list", "ydb.generic.write", "ydb.generic.use_legacy"}}
                                                }
                                            },
                                            {
                                                .Path = "/Root/table1",
                                                .Permissions = {
                                                    {"user2", {"ydb.tables.read", "ydb.tables.modify"}}
                                                }
                                            },
                                            {
                                                .Path = "/Root/table2",
                                                .Permissions = {}
                                            }
                                            });
        }
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            REVOKE LIST, INSERT ON `/Root` FROM user9, user4, user5;
            )";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CheckPermissions(session, {
                                            {
                                                .Path = "/Root",
                                                .Permissions = {{"user5", {"ydb.generic.read", "ydb.generic.use_legacy"}}
                                                }
                                            },
                                            {
                                                .Path = "/Root/table1",
                                                .Permissions = {
                                                    {"user2", {"ydb.tables.read", "ydb.tables.modify"}}
                                                }
                                            },
                                            {
                                                .Path = "/Root/table2",
                                                .Permissions = {}
                                            }
                                        });
        }
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            REVOKE ALL ON `/Root`, `/Root/table1` FROM user9, user4, user5, user2;
            )";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CheckPermissions(session, {
                                            {
                                                .Path = "/Root",
                                                .Permissions = {}
                                            },
                                            {
                                                .Path = "/Root/table1",
                                                .Permissions = {}
                                            },
                                            {
                                                .Path = "/Root/table2",
                                                .Permissions = {}
                                            }
                                        });
        }
    }

    Y_UNIT_TEST(ModifyUnknownPermissions) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE USER user1 PASSWORD 'password1';
            )";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            GRANT ROW SELECT ON `/Root` TO user1;
            )";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "extraneous input 'ROW'", result.GetIssues().ToString());
            CheckPermissions(session, {{.Path = "/Root", .Permissions = {}}});
        }

        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            GRANT `ydb.database.connect` ON `/Root` TO user1;
            )";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "mismatched input '`ydb.database.connect`'", result.GetIssues().ToString());
            CheckPermissions(session, {{.Path = "/Root", .Permissions = {}}});
        }

        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            GRANT CONNECT, READ ON `/Root` TO user1;
            )";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "extraneous input 'READ'", result.GetIssues().ToString());
            CheckPermissions(session, {{.Path = "/Root", .Permissions = {}}});
        }

        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            GRANT "" ON `/Root` TO user1;
            )";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Unknown permission name: ", result.GetIssues().ToString());
            CheckPermissions(session, {{.Path = "/Root", .Permissions = {}}});
        }
    }

    Y_UNIT_TEST(ModifyPermissionsByRelativePath) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE USER ydbuser PASSWORD 'password1';
            )";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            {
                const TString query = R"(
                    CREATE TABLE `MyApp/Orders` (
                        id Int32 NOT NULL,
                        value Int32,
                        PRIMARY KEY (id)
                    );
                )";

                auto result = session.ExecuteSchemeQuery(query).GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            }

            {
                const TString query = R"(
                    GRANT SELECT ON `MyApp/Orders` TO ydbuser;
                )";

                auto result = session.ExecuteSchemeQuery(query).GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            }

            {
                const TString query = R"(
                    REVOKE SELECT ON `MyApp/Orders` FROM ydbuser;
                )";

                auto result = session.ExecuteSchemeQuery(query).GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            }
        }
    }

    Y_UNIT_TEST(ModifyPermissionsByRelativePathQueryClient) {
        NKikimrConfig::TAppConfig appConfig;
        auto runnerSettings = TKikimrSettings().SetAppConfig(appConfig);
        TTestHelper testHelper(runnerSettings);
        auto client = testHelper.GetKikimr().GetQueryClient();
        auto db = testHelper.GetKikimr().GetTableClient();

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("value").SetType(NScheme::NTypeIds::Int32)
        };

        TTestHelper::TColumnTable testTable;
        testTable.SetName("/Root/MyApp/Orders").SetPrimaryKey({ "id" }).SetSchema(schema);
        testHelper.CreateTable(testTable);

        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE USER ydbuser PASSWORD 'password1';
            )";
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            {
                const TString query = R"(
                    GRANT SELECT ON `MyApp/Orders` TO ydbuser;
                )";

                auto result = client.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            }

            {
                const TString query = R"(
                    REVOKE SELECT ON `MyApp/Orders` FROM ydbuser;
                )";

                auto result = client.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            }
        }
    }

    Y_UNIT_TEST(ModifyPermissionsByIncorrectPaths) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE USER user1 PASSWORD 'password1';
            )";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            GRANT CONNECT, LIST ON `/Root`, `/UnknownPath` TO user1;
            )";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Path does not exist", result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Error for the path: /UnknownPath", result.GetIssues().ToString());
            CheckPermissions(session, {{.Path = "/Root", .Permissions = {{"user1", {"ydb.database.connect", "ydb.generic.list"}}}}});
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

    Y_UNIT_TEST_TWIN(CreateAndDropUser, StrictAclCheck) {
        TKikimrRunner kikimr;
        kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableStrictAclCheck(StrictAclCheck);
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
            auto query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE USER user1 PASSWORD NULL;
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
            // Drop user with ACL
            auto session = db.CreateSession().GetValueSync().GetSession();

            TString query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE USER user2 PASSWORD NULL;
            )";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            query = TStringBuilder() << R"(
            --!syntax_v1
            GRANT ALL ON `/Root` TO user2;
            )";
            result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            query = TStringBuilder() << R"(
            --!syntax_v1
            DROP USER user2;
            )";
            result = session.ExecuteSchemeQuery(query).GetValueSync();
            if (!StrictAclCheck) {
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            } else {
                UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::PRECONDITION_FAILED);
                UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Error: User user2 has an ACL record on /Root and can't be removed");
            }
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
            // Drop existing group
            auto query1 = TStringBuilder() << R"(
            --!syntax_v1
            CREATE GROUP group1;
            )";
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(query1).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto query2 = TStringBuilder() << R"(
            --!syntax_v1
            DROP GROUP group1;
            )";
            result = session.ExecuteSchemeQuery(query2).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
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
        UNIT_ASSERT_C(describeResult.IsSuccess(), describeResult.GetIssues().ToString());
        const auto tableDesc = session.DescribeTable(tableName).GetValueSync().GetTableDescription();
        std::vector<TTableColumn> columns = tableDesc.GetTableColumns();
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
            .SetEnableChangefeedDynamoDBStreamsFormat(true)
            .SetEnableChangefeedDebeziumJsonFormat(true));
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
                case EChangefeedFormat::DebeziumJson:
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
            .SetEnableChangefeedDynamoDBStreamsFormat(true)
            .SetEnableChangefeedDebeziumJsonFormat(true));
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

        {
            auto query = R"(
                --!syntax_v1
                ALTER TABLE `/Root/table` ADD CHANGEFEED `feed` WITH (
                    MODE = 'UPDATES', FORMAT = 'DEBEZIUM_JSON'
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
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

        { // alter
            auto query = R"(
                --!syntax_v1
                ALTER TOPIC `/Root/table/feed_2` SET (
                    RETENTION_PERIOD = Interval("PT2H")
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto desc = pq.DescribeTopic("/Root/table/feed_2").ExtractValueSync();
            UNIT_ASSERT_C(desc.IsSuccess(), desc.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(desc.GetTopicDescription().GetRetentionPeriod(), TDuration::Hours(2));
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

    Y_UNIT_TEST(ChangefeedTopicPartitions) {
        using namespace NTopic;

        TKikimrRunner kikimr(TKikimrSettings().SetPQConfig(DefaultPQConfig()));
        auto pq = TTopicClient(kikimr.GetDriver(), TTopicClientSettings().Database("/Root"));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        { // Uint64 key
            auto query = R"(
                --!syntax_v1
                CREATE TABLE `/Root/table_1` (
                    Key Uint64,
                    Value String,
                    PRIMARY KEY (Key)
                );
            )";

            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        { // default
            auto query = R"(
                --!syntax_v1
                ALTER TABLE `/Root/table_1` ADD CHANGEFEED `feed_1` WITH (
                    MODE = 'KEYS_ONLY', FORMAT = 'JSON'
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto desc = pq.DescribeTopic("/Root/table_1/feed_1").ExtractValueSync();
            UNIT_ASSERT_C(desc.IsSuccess(), desc.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(desc.GetTopicDescription().GetPartitions().size(), 1);
        }

        { // custom
            auto query = R"(
                --!syntax_v1
                ALTER TABLE `/Root/table_1` ADD CHANGEFEED `feed_2` WITH (
                    MODE = 'KEYS_ONLY', FORMAT = 'JSON', TOPIC_MIN_ACTIVE_PARTITIONS = 10
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto desc = pq.DescribeTopic("/Root/table_1/feed_2").ExtractValueSync();
            UNIT_ASSERT_C(desc.IsSuccess(), desc.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(desc.GetTopicDescription().GetPartitions().size(), 10);
        }

        { // non-positive (invalid)
            auto query = R"(
                --!syntax_v1
                ALTER TABLE `/Root/table_1` ADD CHANGEFEED `feed_3` WITH (
                    MODE = 'KEYS_ONLY', FORMAT = 'JSON', TOPIC_MIN_ACTIVE_PARTITIONS = 0
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
        }

        { // Utf8 key
            auto query = R"(
                --!syntax_v1
                CREATE TABLE `/Root/table_2` (
                    Key Utf8,
                    Value String,
                    PRIMARY KEY (Key)
                );
            )";

            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        { // default
            auto query = R"(
                --!syntax_v1
                ALTER TABLE `/Root/table_2` ADD CHANGEFEED `feed_1` WITH (
                    MODE = 'KEYS_ONLY', FORMAT = 'JSON'
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto desc = pq.DescribeTopic("/Root/table_2/feed_1").ExtractValueSync();
            UNIT_ASSERT_C(desc.IsSuccess(), desc.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(desc.GetTopicDescription().GetPartitions().size(), 1);
        }

        { // custom
            auto query = R"(
                --!syntax_v1
                ALTER TABLE `/Root/table_2` ADD CHANGEFEED `feed_2` WITH (
                    MODE = 'KEYS_ONLY', FORMAT = 'JSON', TOPIC_MIN_ACTIVE_PARTITIONS = 10
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(ChangefeedTopicAutoPartitioning) {
        using namespace NTopic;

        TKikimrRunner kikimr(TKikimrSettings().SetPQConfig(DefaultPQConfig()));
        auto pq = TTopicClient(kikimr.GetDriver(), TTopicClientSettings().Database("/Root"));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        { // Uint64 key
            auto query = R"(
                --!syntax_v1
                CREATE TABLE `/Root/table_tap` (
                    Key Uint64,
                    Value String,
                    PRIMARY KEY (Key)
                );
            )";

            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        { // default
            auto query = R"(
                --!syntax_v1
                ALTER TABLE `/Root/table_tap` ADD CHANGEFEED `feed_1` WITH (
                    MODE = 'KEYS_ONLY', FORMAT = 'JSON', TOPIC_MIN_ACTIVE_PARTITIONS = 7,  TOPIC_MAX_ACTIVE_PARTITIONS = 777, TOPIC_AUTO_PARTITIONING = 'ENABLED'
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto desc = pq.DescribeTopic("/Root/table_tap/feed_1").ExtractValueSync();
            UNIT_ASSERT_C(desc.IsSuccess(), desc.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(desc.GetTopicDescription().GetPartitions().size(), 7);
            UNIT_ASSERT_VALUES_EQUAL(desc.GetTopicDescription().GetPartitioningSettings().GetMinActivePartitions(), 7);
            UNIT_ASSERT_VALUES_EQUAL(desc.GetTopicDescription().GetPartitioningSettings().GetMaxActivePartitions(), 777);
            UNIT_ASSERT_VALUES_EQUAL(desc.GetTopicDescription().GetPartitioningSettings().GetAutoPartitioningSettings().GetStrategy(), NYdb::NTopic::EAutoPartitioningStrategy::ScaleUp);
        }

        { // disabled
            auto query = R"(
                --!syntax_v1
                ALTER TABLE `/Root/table_tap` ADD CHANGEFEED `feed_2` WITH (
                    MODE = 'KEYS_ONLY', FORMAT = 'JSON', TOPIC_AUTO_PARTITIONING='DISABLED'
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto desc = pq.DescribeTopic("/Root/table_tap/feed_2").ExtractValueSync();
            UNIT_ASSERT_C(desc.IsSuccess(), desc.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(desc.GetTopicDescription().GetPartitioningSettings().GetAutoPartitioningSettings().GetStrategy(), NYdb::NTopic::EAutoPartitioningStrategy::Disabled);
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
            UNIT_ASSERT(changefeeds.at(0).GetAttributes() == changefeed.GetAttributes());
        }
    }

    Y_UNIT_TEST(ChangefeedOnIndexTable) {
        TKikimrRunner kikimr(TKikimrSettings()
            .SetPQConfig(DefaultPQConfig())
            .SetEnableChangefeedsOnIndexTables(true));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        {
            auto query = R"(
                --!syntax_v1
                CREATE TABLE `/Root/table` (
                    Key Uint64,
                    Value String,
                    PRIMARY KEY (Key),
                    INDEX SyncIndex GLOBAL SYNC ON (`Value`),
                    INDEX AsyncIndex GLOBAL ASYNC ON (`Value`)
                );
            )";

            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        const auto changefeed = TChangefeedDescription("feed", EChangefeedMode::KeysOnly, EChangefeedFormat::Json);
        {
            auto result = session.AlterTable("/Root/table/AsyncIndex", TAlterTableSettings()
                .AppendAddChangefeeds(changefeed)
            ).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
        }
        {
            auto result = session.AlterTable("/Root/table/SyncIndex", TAlterTableSettings()
                .AppendAddChangefeeds(changefeed)
            ).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(DescribeIndexTable) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto scheme = NYdb::NScheme::TSchemeClient(kikimr.GetDriver(), TCommonClientSettings().Database("/Root"));
        auto session = db.CreateSession().GetValueSync().GetSession();

        {
            auto query = R"(
                --!syntax_v1
                CREATE TABLE `/Root/table` (
                    Key Uint64,
                    Value String,
                    PRIMARY KEY (Key),
                    INDEX SyncIndex GLOBAL SYNC ON (`Value`)
                );
            )";

            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto desc = scheme.DescribePath("/Root/table/SyncIndex").ExtractValueSync();
            UNIT_ASSERT_C(desc.IsSuccess(), desc.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(desc.GetEntry().Name, "SyncIndex");
        }
        {
            auto desc = session.DescribeTable("/Root/table/SyncIndex").ExtractValueSync();
            UNIT_ASSERT_C(desc.IsSuccess(), desc.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(desc.GetEntry().Name, "SyncIndex");
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

    Y_UNIT_TEST(NEG_CreateTableWithUnsupportedStoreType) {
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TKikimrRunner kikimr(runnerSettings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString tableStoreName = "/Root/TableStoreTest";
        auto query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE TABLE SomeTable (
                Key Timestamp NOT NULL,
                Value1 String,
                Value2 Int64 NOT NULL,
                PRIMARY KEY (Key)
            )
            WITH (
                STORE = UNSUPPORTED
            );
        )";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
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
                Key Uint64 NOT NULL,
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
                Key Uint64 NOT NULL,
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
                Key Uint64 NOT NULL,
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
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SCHEME_ERROR, result.GetIssues().ToString());
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
                Key Uint64 NOT NULL,
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
                Key Uint64 NOT NULL,
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
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SCHEME_ERROR, result.GetIssues().ToString());
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

        { // nullable pk columns are disabled by default
            kikimr.GetTestServer().GetRuntime()->GetAppData().ColumnShardConfig.SetAllowNullableColumnsInPK(false);
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
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SCHEME_ERROR, result.GetIssues().ToString());
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
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SCHEME_ERROR, result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(AlterColumnTableTtl) {
        auto runnerSettings = TKikimrSettings()
            .SetColumnShardAlterObjectEnabled(true)
            .SetWithSampleTables(false);
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

        {
            auto query2 = TStringBuilder() << R"(
            --!syntax_v1
            ALTER OBJECT `)" << tableName << R"(` (TYPE TABLE) SET (ACTION=UPSERT_INDEX,
                NAME=max_value1, TYPE=MAX, FEATURES=`{\"column_name\": \"Value1\"}`))";
            result = session.ExecuteSchemeQuery(query2).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto query2 = TStringBuilder() << R"(
            --!syntax_v1
            ALTER OBJECT `)" << tableName << R"(` (TYPE TABLE) SET (ACTION=UPSERT_INDEX,
                NAME=max_value2, TYPE=MAX, FEATURES=`{\"column_name\": \"Value2\"}`))";
            result = session.ExecuteSchemeQuery(query2).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

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
        runnerSettings.SetEnableTieringInColumnShard(true);
        TTestHelper testHelper(runnerSettings);
        auto db = testHelper.GetKikimr().GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString tableName = "/Root/ColumnTableTest";

        testHelper.CreateTier("tier1");
        testHelper.CreateTier("tier2");

        auto query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE TABLE `)" << tableName << R"(` (
                Key Timestamp NOT NULL,
                Value1 String,
                Value2 Int64 NOT NULL,
                PRIMARY KEY (Key)
            )
            PARTITION BY HASH(Key)
            WITH (
                STORE = COLUMN,
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 10,
                TTL = Interval("PT10S") TO EXTERNAL DATA SOURCE `/Root/tier1` ON Key
            );)";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        { // describe table
            auto desc = session.DescribeTable(tableName).ExtractValueSync();
            UNIT_ASSERT_C(desc.IsSuccess(), desc.GetIssues().ToString());

            UNIT_ASSERT(desc.GetTableDescription().GetTtlSettings());
            auto ttl = desc.GetTableDescription().GetTtlSettings();
            UNIT_ASSERT_VALUES_EQUAL(ttl->GetTiers().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(std::get<TTtlEvictToExternalStorageAction>(ttl->GetTiers()[0].GetAction()).GetStorage(), "/Root/tier1");
            UNIT_ASSERT_VALUES_EQUAL(std::get<TDateTypeColumnModeSettings>(ttl->GetTiers()[0].GetExpression()).GetExpireAfter(), TDuration::Seconds(10));
        }
        auto query2 = TStringBuilder() << R"(
            --!syntax_v1
            ALTER TABLE `)" << tableName << R"(` SET (TTL = Interval("PT10S") TO EXTERNAL DATA SOURCE `/Root/tier2` ON Key);)";
        result = session.ExecuteSchemeQuery(query2).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        { // describe table
            auto desc = session.DescribeTable(tableName).ExtractValueSync();
            UNIT_ASSERT_C(desc.IsSuccess(), desc.GetIssues().ToString());

            UNIT_ASSERT(desc.GetTableDescription().GetTtlSettings());
            auto ttl = desc.GetTableDescription().GetTtlSettings();
            UNIT_ASSERT_VALUES_EQUAL(ttl->GetTiers().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(std::get<TTtlEvictToExternalStorageAction>(ttl->GetTiers()[0].GetAction()).GetStorage(), "/Root/tier2");
            UNIT_ASSERT_VALUES_EQUAL(std::get<TDateTypeColumnModeSettings>(ttl->GetTiers()[0].GetExpression()).GetExpireAfter(), TDuration::Seconds(10));
        }

        auto query3 = TStringBuilder() << R"(
            --!syntax_v1
            ALTER TABLE `)" << tableName << R"(` RESET (TTL);)";
        result = session.ExecuteSchemeQuery(query3).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        { // describe table
            auto desc = session.DescribeTable(tableName).ExtractValueSync();
            UNIT_ASSERT_C(desc.IsSuccess(), desc.GetIssues().ToString());

            auto ttl = desc.GetTableDescription().GetTtlSettings();
            UNIT_ASSERT(!ttl);
        }

        auto query4 = TStringBuilder() << R"(
            --!syntax_v1
            ALTER TABLE `)" << tableName << R"(` SET (TTL = Interval("PT10S") TO EXTERNAL DATA SOURCE `/Root/tier1` ON Key);)";
        result = session.ExecuteSchemeQuery(query4).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        { // describe table
            auto desc = session.DescribeTable(tableName).ExtractValueSync();
            UNIT_ASSERT_C(desc.IsSuccess(), desc.GetIssues().ToString());

            UNIT_ASSERT(desc.GetTableDescription().GetTtlSettings());
            auto ttl = desc.GetTableDescription().GetTtlSettings();
            UNIT_ASSERT_VALUES_EQUAL(ttl->GetTiers().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(std::get<TTtlEvictToExternalStorageAction>(ttl->GetTiers()[0].GetAction()).GetStorage(), "/Root/tier1");
            UNIT_ASSERT_VALUES_EQUAL(std::get<TDateTypeColumnModeSettings>(ttl->GetTiers()[0].GetExpression()).GetExpireAfter(), TDuration::Seconds(10));
        }

        auto query5 = TStringBuilder() << R"(
            --!syntax_v1
            DROP TABLE `)" << tableName << R"(`;)";
        result = session.ExecuteSchemeQuery(query5).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    Y_UNIT_TEST(AlterSequence) {
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TKikimrRunner kikimr(runnerSettings);
        auto client = kikimr.GetQueryClient();

        TString tableName = "/Root/TableTest";

        {
            auto session = client.GetSession().GetValueSync().GetSession();
            auto id = session.GetId();

            auto query = TStringBuilder() << R"(
                --!syntax_v1
                CREATE TABLE `)" << tableName << R"(` (
                    Key Serial,
                    Value String,
                    PRIMARY KEY (Key)
                );
            )";

            auto result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        TString sequencePath = "/Root/TableTest/_serial_column_Key";

        {
            auto session = client.GetSession().GetValueSync().GetSession();
            auto id = session.GetId();

            auto query = TStringBuilder() << R"(
                --!syntax_v1
                ALTER SEQUENCE IF EXISTS `)" << sequencePath << R"(`
                    START WITH 50
                    INCREMENT BY 11;
            )";

            auto result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto runtime = kikimr.GetTestServer().GetRuntime();
            TActorId sender = runtime->AllocateEdgeActor();
            auto describeResult = DescribeTable(&kikimr.GetTestServer(), sender, sequencePath);
            UNIT_ASSERT_VALUES_EQUAL(describeResult.GetStatus(), NKikimrScheme::StatusSuccess);
            auto& sequenceDescription = describeResult.GetPathDescription().GetSequenceDescription();
            UNIT_ASSERT_VALUES_EQUAL(sequenceDescription.GetName(), "_serial_column_Key");
            UNIT_ASSERT_VALUES_EQUAL(sequenceDescription.GetMinValue(), 1);
            UNIT_ASSERT_VALUES_EQUAL(sequenceDescription.GetMaxValue(), 9223372036854775807);
            UNIT_ASSERT_VALUES_EQUAL(sequenceDescription.GetStartValue(), 50);
            UNIT_ASSERT_VALUES_EQUAL(sequenceDescription.GetCache(), 1);
            UNIT_ASSERT_VALUES_EQUAL(sequenceDescription.GetIncrement(), 11);
            UNIT_ASSERT_VALUES_EQUAL(sequenceDescription.GetCycle(), false);
            UNIT_ASSERT_VALUES_EQUAL(sequenceDescription.GetDataType(), "Int64");
        }

        {
            auto session = client.GetSession().GetValueSync().GetSession();
            auto id = session.GetId();

            auto query = TStringBuilder() << R"(
                --!syntax_v1
                ALTER SEQUENCE IF EXISTS `)" << sequencePath << R"(`
                    RESTART WITH 1000;
            )";

            auto result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto session = client.GetSession().GetValueSync().GetSession();
            auto id = session.GetId();

            auto query = TStringBuilder() << R"(
                --!syntax_v1
                ALTER SEQUENCE IF EXISTS `/Root/seq`
                    RESTART WITH 1000;
            )";

            auto result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto session = client.GetSession().GetValueSync().GetSession();
            auto id = session.GetId();

            auto query = TStringBuilder() << R"(
                --!syntax_v1
                ALTER SEQUENCE `/Root/seq`
                    START WITH 2000;
            )";

            auto result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT(!result.IsSuccess());
        }
    }

    Y_UNIT_TEST(AlterSequenceRestartWith) {
       TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TKikimrRunner kikimr(runnerSettings);
        auto client = kikimr.GetQueryClient();

        TString tableName = "/Root/TableTest";

        auto session = client.GetSession().GetValueSync().GetSession();

        auto tableClient = kikimr.GetTableClient();
        auto tableClientSession = tableClient.CreateSession().GetValueSync().GetSession();

        {
            auto query = TStringBuilder() << R"(
                --!syntax_v1
                CREATE TABLE `)" << tableName << R"(` (
                    Key Int64,
                    Value Serial,
                    PRIMARY KEY (Key)
                );
            )";

            auto result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            const auto query = TStringBuilder() << R"(
                --!syntax_v1
                INSERT INTO `)" << tableName << R"(` (Key, Value) VALUES (1, 1);
            )";

            auto result = tableClientSession.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            const auto query = TStringBuilder() << R"(
                --!syntax_v1
                SELECT * FROM `)" << tableName << R"(`;
            )";

            auto result = tableClientSession.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            UNIT_ASSERT_C(!result.GetResultSets().empty(), "results are empty");
            CompareYson(R"(
                [[[1];1]]
            )", FormatResultSetYson(result.GetResultSet(0)));
        }

        {
            const auto query = TStringBuilder() << R"(
                --!syntax_v1
                INSERT INTO `)" << tableName << R"(` (Key) VALUES (2), (3);
            )";

            auto result = tableClientSession.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            const auto query = TStringBuilder() << R"(
                --!syntax_v1
                SELECT * FROM `)" << tableName << R"(`;
            )";

            auto result = tableClientSession.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            UNIT_ASSERT_C(!result.GetResultSets().empty(), "results are empty");
            CompareYson(R"(
                [[[1];1];[[2];1];[[3];2]]
            )", FormatResultSetYson(result.GetResultSet(0)));
        }

        TString sequencePath = "/Root/TableTest/_serial_column_Value";

        {
            const auto queryAlter = TStringBuilder() << R"(
                --!syntax_v1
                ALTER SEQUENCE IF EXISTS `)" << sequencePath << R"(`
                    RESTART 105
                    INCREMENT 2;
            )";

            auto resultAlter = session.ExecuteQuery(queryAlter, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(resultAlter.IsSuccess(), resultAlter.GetIssues().ToString());
        }

        {
            const auto query = TStringBuilder() << R"(
                --!syntax_v1
                INSERT INTO `)" << tableName << R"(` (Key) VALUES (105), (107);
            )";

            auto result = tableClientSession.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            const auto query = TStringBuilder() << R"(
                --!syntax_v1
                SELECT * FROM `)" << tableName << R"(`;
            )";

            auto result = tableClientSession.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            UNIT_ASSERT_C(!result.GetResultSets().empty(), "results are empty");
            CompareYson(R"(
                [[[1];1];[[2];1];[[3];2];[[105];105];[[107];107]]
            )", FormatResultSetYson(result.GetResultSet(0)));
        }

        {
            const auto queryAlter = TStringBuilder() << R"(
                --!syntax_v1
                ALTER SEQUENCE IF EXISTS `)" << sequencePath << R"(`
                    START 206;
            )";

            auto resultAlter = session.ExecuteQuery(queryAlter, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(resultAlter.IsSuccess(), resultAlter.GetIssues().ToString());
        }

        {
            const auto queryAlter = TStringBuilder() << R"(
                --!syntax_v1
                ALTER SEQUENCE IF EXISTS `)" << sequencePath << R"(`
                    RESTART;
            )";

            auto resultAlter = session.ExecuteQuery(queryAlter, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(resultAlter.IsSuccess(), resultAlter.GetIssues().ToString());
        }

        {
            const auto query = TStringBuilder() << R"(
                --!syntax_v1
                INSERT INTO `)" << tableName << R"(` (Key) VALUES (206), (208);
            )";

            auto result = tableClientSession.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            const auto query = TStringBuilder() << R"(
                --!syntax_v1
                SELECT * FROM `)" << tableName << R"(`;
            )";

            auto result = tableClientSession.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            UNIT_ASSERT_C(!result.GetResultSets().empty(), "results are empty");
            CompareYson(R"(
                [[[1];1];[[2];1];[[3];2];[[105];105];[[107];107];[[206];206];[[208];208]]
            )", FormatResultSetYson(result.GetResultSet(0)));
        }
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
                    UNIT_ASSERT_VALUES_EQUAL(i, c.GetOptionalUint8().value());
                }
                {
                    auto& c = parser.ColumnParser("CInt8");
                    UNIT_ASSERT_VALUES_EQUAL(i, c.GetOptionalInt8().value());
                }
                {
                    auto& c = parser.ColumnParser("CUint16");
                    UNIT_ASSERT_VALUES_EQUAL(i, c.GetOptionalUint16().value());
                }
                {
                    auto& c = parser.ColumnParser("CInt16");
                    UNIT_ASSERT_VALUES_EQUAL(i, c.GetOptionalInt16().value());
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
                arrow::field("CUint8", arrow::uint8(), false),
                arrow::field("CInt8", arrow::int8(), false),
                arrow::field("CUint16", arrow::uint16(), false),
                arrow::field("CInt16", arrow::int16(), false)
            });

        size_t rowsCount = 10;
        auto builders = NArrow::MakeBuilders(schema, rowsCount);
        for (size_t i = 0; i < rowsCount; ++i) {
            Y_ABORT_UNLESS(NArrow::Append<arrow::UInt8Type>(*builders[0], i));
            Y_ABORT_UNLESS(NArrow::Append<arrow::Int8Type>(*builders[1], i));
            Y_ABORT_UNLESS(NArrow::Append<arrow::UInt16Type>(*builders[2], i));
            Y_ABORT_UNLESS(NArrow::Append<arrow::Int16Type>(*builders[3], i));
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

    Y_UNIT_TEST(DisableExternalDataSourcesOnServerless) {
        auto ydb = NWorkload::TYdbSetupSettings()
            .CreateSampleTenants(true)
            .EnableExternalDataSourcesOnServerless(false)
            .Create();

        auto checkDisabled = [](const auto& result, NYdb::EStatus status) {
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), status, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "External data sources are disabled for serverless domains. Please contact your system administrator to enable it", result.GetIssues().ToString());
        };

        auto checkNotFound = [](const auto& result, NYdb::EStatus status) {
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), status, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Path does not exist", result.GetIssues().ToString());
        };

        const auto& createSourceSql = R"(
            CREATE EXTERNAL DATA SOURCE MyExternalDataSource WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="my-bucket",
                AUTH_METHOD="NONE"
            );)";

        const auto& createTableSql = R"(
            CREATE EXTERNAL TABLE MyExternalTable (
                Key Uint64,
                Value String
            ) WITH (
                DATA_SOURCE="MyExternalDataSource",
                LOCATION="/"
            );)";

        const auto& dropSourceSql = "DROP EXTERNAL DATA SOURCE MyExternalDataSource;";

        const auto& dropTableSql = "DROP EXTERNAL TABLE MyExternalTable;";

        auto settings = NWorkload::TQueryRunnerSettings().PoolId("");

        // Dedicated, enabled
        settings.Database(ydb->GetSettings().GetDedicatedTenantName()).NodeIndex(1);
        NWorkload::TSampleQueries::CheckSuccess(ydb->ExecuteQuery(createSourceSql, settings));
        NWorkload::TSampleQueries::CheckSuccess(ydb->ExecuteQuery(createTableSql, settings));
        NWorkload::TSampleQueries::CheckSuccess(ydb->ExecuteQuery(dropTableSql, settings));
        NWorkload::TSampleQueries::CheckSuccess(ydb->ExecuteQuery(dropSourceSql, settings));

        // Shared, enabled
        settings.Database(ydb->GetSettings().GetSharedTenantName()).NodeIndex(2);
        NWorkload::TSampleQueries::CheckSuccess(ydb->ExecuteQuery(createSourceSql, settings));
        NWorkload::TSampleQueries::CheckSuccess(ydb->ExecuteQuery(createTableSql, settings));
        NWorkload::TSampleQueries::CheckSuccess(ydb->ExecuteQuery(dropTableSql, settings));
        NWorkload::TSampleQueries::CheckSuccess(ydb->ExecuteQuery(dropSourceSql, settings));

        // Serverless, disabled
        settings.Database(ydb->GetSettings().GetServerlessTenantName()).NodeIndex(2);
        checkDisabled(ydb->ExecuteQuery(createSourceSql, settings), NYdb::EStatus::GENERIC_ERROR);
        checkDisabled(ydb->ExecuteQuery(createTableSql, settings), NYdb::EStatus::PRECONDITION_FAILED);
        checkNotFound(ydb->ExecuteQuery(dropTableSql, settings), NYdb::EStatus::SCHEME_ERROR);
        checkNotFound(ydb->ExecuteQuery(dropSourceSql, settings), NYdb::EStatus::GENERIC_ERROR);
    }

    Y_UNIT_TEST(CreateExternalDataSource) {
        NKikimrConfig::TAppConfig appCfg;
        appCfg.MutableQueryServiceConfig()->AddHostnamePatterns("my-bucket");
        appCfg.MutableQueryServiceConfig()->AddAvailableExternalDataSources("ObjectStorage");

        TKikimrRunner kikimr(appCfg);
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

    Y_UNIT_TEST(CreateExternalDataSourceWithSa) {
        TKikimrRunner kikimr;
        kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableExternalDataSources(true);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        TString externalDataSourceName = "/Root/ExternalDataSource";
        auto query = TStringBuilder() << R"(
            CREATE OBJECT mysasignature (TYPE SECRET) WITH (value = "mysasignaturevalue");

            CREATE EXTERNAL DATA SOURCE `)" << externalDataSourceName << R"(` WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="my-bucket",
                AUTH_METHOD="SERVICE_ACCOUNT",
                SERVICE_ACCOUNT_ID="mysa",
                SERVICE_ACCOUNT_SECRET_NAME="mysasignature"
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
        UNIT_ASSERT(externalDataSource.ExternalDataSourceInfo->Description.GetAuth().HasServiceAccount());
        UNIT_ASSERT_VALUES_EQUAL(externalDataSource.ExternalDataSourceInfo->Description.GetAuth().GetServiceAccount().GetId(), "mysa");
        UNIT_ASSERT_VALUES_EQUAL(externalDataSource.ExternalDataSourceInfo->Description.GetAuth().GetServiceAccount().GetSecretName(), "mysasignature");
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
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::UNSUPPORTED);
        UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "External data sources are disabled. Please contact your system administrator to enable it", result.GetIssues().ToString());
    }

    Y_UNIT_TEST(DisableS3ExternalDataSource) {
        NKikimrConfig::TAppConfig appCfg;
        appCfg.MutableQueryServiceConfig()->AddAvailableExternalDataSources("PostgreSQL");
        TKikimrRunner kikimr(appCfg);
        kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableExternalDataSources(true);
        {
            auto db = kikimr.GetTableClient();
            auto session = db.CreateSession().GetValueSync().GetSession();
            TString externalDataSourceName = "/Root/ExternalDataSource2";
            auto query = TStringBuilder() << R"(
                CREATE EXTERNAL DATA SOURCE `)" << externalDataSourceName << R"(` WITH (
                    SOURCE_TYPE="ObjectStorage",
                    LOCATION="my-bucket",
                    AUTH_METHOD="NONE"
                );)";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "External source with type ObjectStorage is disabled. Please contact your system administrator to enable it", result.GetIssues().ToString());

            auto query2 = TStringBuilder() << R"(
                CREATE OBJECT `baz2` (TYPE SECRET) WITH value=`MySecretData`;

                CREATE EXTERNAL DATA SOURCE `)" << externalDataSourceName << R"(` WITH (
                    SOURCE_TYPE="PostgreSQL",
                    LOCATION="my-bucket",
                    AUTH_METHOD="BASIC",
                    LOGIN="admin",
                    PASSWORD_SECRET_NAME = "baz2",
                    DATABASE_NAME="cheburashka"
                );)";
            result = session.ExecuteSchemeQuery(query2).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto client = kikimr.GetQueryClient();
            auto session = client.GetSession().GetValueSync().GetSession();
            TString externalDataSourceName = "/Root/ExternalDataSource";
            auto query = TStringBuilder() << R"(
                CREATE EXTERNAL DATA SOURCE `)" << externalDataSourceName << R"(` WITH (
                    SOURCE_TYPE="ObjectStorage",
                    LOCATION="my-bucket",
                    AUTH_METHOD="NONE"
                );)";
            auto result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "External source with type ObjectStorage is disabled. Please contact your system administrator to enable it", result.GetIssues().ToString());

            auto query2 = TStringBuilder() << R"(
                CREATE OBJECT `baz` (TYPE SECRET) WITH value=`MySecretData`;

                CREATE EXTERNAL DATA SOURCE `)" << externalDataSourceName << R"(` WITH (
                    SOURCE_TYPE="PostgreSQL",
                    LOCATION="my-bucket",
                    AUTH_METHOD="BASIC",
                    LOGIN="admin",
                    PASSWORD_SECRET_NAME = "baz",
                    DATABASE_NAME="cheburashka"
                );)";
            result = session.ExecuteQuery(query2, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(CreateExternalDataSourceValidationAuthMethod) {
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
        UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Unknown AUTH_METHOD = UNKNOWN", result.GetIssues().ToString());
    }

    Y_UNIT_TEST(CreateExternalDataSourceValidationLocation) {
        NKikimrConfig::TAppConfig appCfg;
        appCfg.MutableQueryServiceConfig()->AddHostnamePatterns("common-bucket");
        appCfg.MutableQueryServiceConfig()->AddAvailableExternalDataSources("ObjectStorage");
        TKikimrRunner kikimr(appCfg);
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
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
        UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "It is not allowed to access hostname 'my-bucket'", result.GetIssues().ToString());
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
        UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "External data sources are disabled. Please contact your system administrator to enable it", result.GetIssues().ToString());
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
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Check failed: path: '/Root/ExternalDataSource', error: path exist", result.GetIssues().ToString());
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
        UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "External tables are disabled. Please contact your system administrator to enable it", result.GetIssues().ToString());
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
        UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "PRIMARY KEY is not supported for external table", result.GetIssues().ToString());
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
        UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "DATA_SOURCE requires key", result.GetIssues().ToString());
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
        UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "External table are disabled. Please contact your system administrator to enable it", result.GetIssues().ToString());
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
                LOCATION="/folder1/",
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
        UNIT_ASSERT_VALUES_EQUAL(externalTable.ExternalTableInfo->Description.GetLocation(), "/folder1/");
    }

    Y_UNIT_TEST(CreateExternalTableWithUpperCaseSettings) {
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
                Year Int64 NOT NULL,
                Month Int64 NOT NULL
            ) WITH (
                DATA_SOURCE=")" << externalDataSourceName << R"(",
                LOCATION="/folder1/",
                FORMAT="json_as_string",
                `projection.enabled`="true",
                `projection.Year.type`="integer",
                `projection.Year.min`="2010",
                `projection.Year.max`="2022",
                `projection.Year.interval`="1",
                `projection.Month.type`="integer",
                `projection.Month.min`="1",
                `projection.Month.max`="12",
                `projection.Month.interval`="1",
                `projection.Month.digits`="2",
                `storage.location.template`="${Year}/${Month}",
                PARTITIONED_BY = "[Year, Month]"
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
        UNIT_ASSERT_VALUES_EQUAL(externalTable.ExternalTableInfo->Description.GetLocation(), "/folder1/");
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
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Check failed: path: '/Root/ExternalTable', error: path exist", result.GetIssues().ToString());
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
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Other entities depend on this data source, please remove them at the beginning: /Root/ExternalTable", result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(DropNonExistingExternalDataSource) {
        TKikimrRunner kikimr;
        kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableExternalDataSources(true);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        auto resultSuccess = session.ExecuteSchemeQuery("DROP EXTERNAL DATA SOURCE test").GetValueSync();
        UNIT_ASSERT_C(resultSuccess.GetStatus() == EStatus::SCHEME_ERROR, TStringBuilder{} << resultSuccess.GetStatus() << " " << resultSuccess.GetIssues().ToString());
    }

    Y_UNIT_TEST(CreateAsyncReplication) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        // negative
        {
            auto query = R"(
                --!syntax_v1
                CREATE ASYNC REPLICATION `/Root/replication` FOR
                    `/Root/table` AS `/Root/replica`
                WITH (
                    CONNECTION_STRING = "grpc://localhost:2135/?database=/Root",
                    ENDPOINT = "localhost:2135",
                    DATABASE = "/Root"
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToOneLineString(), "CONNECTION_STRING and ENDPOINT/DATABASE are mutually exclusive", result.GetIssues().ToOneLineString());
        }
        {
            auto query = R"(
                --!syntax_v1
                CREATE ASYNC REPLICATION `/Root/replication` FOR
                    `/Root/table` AS `/Root/replica`
                WITH (
                    ENDPOINT = "localhost:2135"
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToOneLineString(), "Neither CONNECTION_STRING nor ENDPOINT/DATABASE are provided", result.GetIssues().ToOneLineString());
        }
        {
            auto query = R"(
                --!syntax_v1
                CREATE ASYNC REPLICATION `/Root/replication` FOR
                    `/Root/table` AS `/Root/replica`
                WITH (
                    DATABASE = "/Root"
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToOneLineString(), "Neither CONNECTION_STRING nor ENDPOINT/DATABASE are provided", result.GetIssues().ToOneLineString());
        }
        {
            auto query = R"(
                --!syntax_v1
                CREATE ASYNC REPLICATION `/Root/replication` FOR
                    `/Root/table` AS `/Root/replica`
                WITH (
                    CONNECTION_STRING = "grpc://localhost:2135/?database=/Root",
                    TOKEN = "foo",
                    USER = "user",
                    PASSWORD = "bar"
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToOneLineString(), "TOKEN and USER/PASSWORD are mutually exclusive", result.GetIssues().ToOneLineString());
        }
        {
            auto query = R"(
                --!syntax_v1
                CREATE ASYNC REPLICATION `/Root/replication` FOR
                    `/Root/table` AS `/Root/replica`
                WITH (
                    CONNECTION_STRING = "grpc://localhost:2135/?database=/Root",
                    TOKEN = "foo",
                    TOKEN_SECRET_NAME = "bar"
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToOneLineString(), "TOKEN and TOKEN_SECRET_NAME are mutually exclusive", result.GetIssues().ToOneLineString());
        }
        {
            auto query = R"(
                --!syntax_v1
                CREATE ASYNC REPLICATION `/Root/replication` FOR
                    `/Root/table` AS `/Root/replica`
                WITH (
                    CONNECTION_STRING = "grpc://localhost:2135/?database=/Root",
                    USER = "user",
                    PASSWORD = "bar",
                    PASSWORD_SECRET_NAME = "baz"
                );
            )";
            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToOneLineString(), "PASSWORD and PASSWORD_SECRET_NAME are mutually exclusive", result.GetIssues().ToOneLineString());
        }
        {
            auto query = R"(
                --!syntax_v1
                CREATE ASYNC REPLICATION `/Root/replication` FOR
                    `/Root/table` AS `/Root/replica`
                WITH (
                    CONNECTION_STRING = "grpc://localhost:2135/?database=/Root",
                    PASSWORD = "bar"
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToOneLineString(), "USER is not provided", result.GetIssues().ToOneLineString());
        }
        {
            auto query = R"(
                --!syntax_v1
                CREATE ASYNC REPLICATION `/Root/replication` FOR
                    `/Root/table` AS `/Root/replica`
                WITH (
                    CONNECTION_STRING = "grpc://localhost:2135/?database=/Root",
                    USER = "user"
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToOneLineString(), "PASSWORD or PASSWORD_SECRET_NAME are not provided", result.GetIssues().ToOneLineString());
        }
        {
            auto query = R"(
                --!syntax_v1
                CREATE ASYNC REPLICATION `/Root/replication` FOR
                    `/Root/table` AS `/Root/replica`
                WITH (
                    CONNECTION_STRING = "grpc://localhost:2135/?database=/Root",
                    STATE = "DONE"
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToOneLineString(), "STATE is not supported in CREATE", result.GetIssues().ToOneLineString());
        }
        {
            auto query = R"(
                --!syntax_v1
                CREATE ASYNC REPLICATION `/Root/replication` FOR
                    `/Root/table` AS `/Root/replica`
                WITH (
                    CONNECTION_STRING = "grpc://localhost:2135/?database=/Root",
                    CONSISTENCY_LEVEL = "FOO"
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToOneLineString(), "Unknown consistency level", result.GetIssues().ToOneLineString());
        }
        {
            auto query = R"(
                --!syntax_v1
                CREATE ASYNC REPLICATION `/Root/replication` FOR
                    `/Root/table` AS `/Root/replica`
                WITH (
                    CONNECTION_STRING = "grpc://localhost:2135/?database=/Root",
                    CONSISTENCY_LEVEL = "ROW",
                    COMMIT_INTERVAL = Interval("PT10S")
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToOneLineString(), "Ambiguous consistency level", result.GetIssues().ToOneLineString());
        }

        // positive
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
                CREATE ASYNC REPLICATION `/Root/replication` FOR
                    `/Root/table` AS `/Root/replica`
                WITH (
                    ENDPOINT = "%s",
                    DATABASE = "/Root"
                );
            )", kikimr.GetEndpoint().c_str());

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(CreateAsyncReplicationWithSecret) {
        using namespace NReplication;

        TKikimrRunner kikimr("root@builtin");
        auto repl = TReplicationClient(kikimr.GetDriver(), TCommonClientSettings().Database("/Root"));
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

        // ok
        {
            auto query = Sprintf(R"(
                --!syntax_v1
                CREATE OBJECT mysecret (TYPE SECRET) WITH (value = "root@builtin");
                CREATE ASYNC REPLICATION `/Root/replication` FOR
                    `/Root/table` AS `/Root/replica`
                WITH (
                    ENDPOINT = "%s",
                    DATABASE = "/Root",
                    TOKEN_SECRET_NAME = "mysecret"
                );
            )", kikimr.GetEndpoint().c_str());

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        while (true) {
            auto describe = session.DescribeTable("/Root/replica").GetValueSync();
            if (describe.GetStatus() == EStatus::SUCCESS) {
                break;
            }

            Sleep(TDuration::Seconds(1));
        }

        while (true) {
            auto settings = TDescribeReplicationSettings().IncludeStats(true);
            const auto result = repl.DescribeReplication("/Root/replication", settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            const auto& desc = result.GetReplicationDescription();
            UNIT_ASSERT_VALUES_EQUAL(desc.GetState(), TReplicationDescription::EState::Running);

            const auto& total = desc.GetRunningState().GetStats();
            if (!total.GetInitialScanProgress() || *total.GetInitialScanProgress() < 100) {
                Sleep(TDuration::Seconds(1));
                continue;
            }

            UNIT_ASSERT(total.GetInitialScanProgress());
            UNIT_ASSERT_DOUBLES_EQUAL(*total.GetInitialScanProgress(), 100.0, 0.01);

            const auto& items = desc.GetItems();
            UNIT_ASSERT_VALUES_EQUAL(items.size(), 1);
            const auto& item = items.at(0).Stats;

            UNIT_ASSERT(item.GetInitialScanProgress());
            UNIT_ASSERT_DOUBLES_EQUAL(*item.GetInitialScanProgress(), *total.GetInitialScanProgress(), 0.01);

            // TODO: check lag too
            break;
        }
    }

    Y_UNIT_TEST(AlterAsyncReplication) {
        using namespace NReplication;

        TKikimrRunner kikimr;
        auto repl = TReplicationClient(kikimr.GetDriver(), TCommonClientSettings().Database("/Root"));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::REPLICATION_CONTROLLER, NActors::NLog::PRI_TRACE);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::REPLICATION_SERVICE, NActors::NLog::PRI_TRACE);

        // path does not exist
        {
            auto query = R"(
                --!syntax_v1
                ALTER ASYNC REPLICATION `/Root/replication`
                SET (
                    STATE = "DONE"
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SCHEME_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToOneLineString(), "Check failed: path: '/Root/replication', error: path hasn't been resolved, nearest resolved path: '/Root'", result.GetIssues().ToOneLineString());
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
            auto query = Sprintf(R"(
                --!syntax_v1
                CREATE ASYNC REPLICATION `/Root/replication` FOR
                    `/Root/table` AS `/Root/replica`
                WITH (
                    ENDPOINT = "%s",
                    DATABASE = "/Root",
                    TOKEN = "root@builtin"
                );
            )", kikimr.GetEndpoint().c_str());

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        // invalid state
        {
            auto query = R"(
                --!syntax_v1
                ALTER ASYNC REPLICATION `/Root/replication`
                SET (
                    STATE = "foo"
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToOneLineString(), "Unknown replication state: foo", result.GetIssues().ToOneLineString());
        }

        // invalid failover mode
        {
            auto query = R"(
                --!syntax_v1
                ALTER ASYNC REPLICATION `/Root/replication`
                SET (
                    FAILOVER_MODE = "foo"
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToOneLineString(), "Unknown failover mode: foo", result.GetIssues().ToOneLineString());
        }

        // alter config in StandBy state
        {
            auto query = R"(
                --!syntax_v1
                ALTER ASYNC REPLICATION `/Root/replication`
                SET (
                    CONNECTION_STRING = "grpc://localhost:2135/?database=/Root"
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToOneLineString(), "Please ensure the replication is not in StandBy state before attempting to modify its settings. Modifications are not allowed in StandBy state", result.GetIssues().ToOneLineString());
        }

        // alter state and config
        {
            auto query = R"(
                --!syntax_v1
                ALTER ASYNC REPLICATION `/Root/replication`
                SET (
                    STATE = "DONE",
                    FAILOVER_MODE = "FORCE",
                    CONNECTION_STRING = "grpc://localhost:2135/?database=/Root"
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToOneLineString(), "It is not allowed to change both settings and the state of the replication in the same query. Please submit separate queries for each action", result.GetIssues().ToOneLineString());
        }

        // check alter state
        {
            auto query = R"(
                --!syntax_v1
                ALTER ASYNC REPLICATION `/Root/replication`
                SET (
                    STATE = "DONE",
                    FAILOVER_MODE = "FORCE"
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            for (size_t i = 10; i--;) {
                const auto result = repl.DescribeReplication("/Root/replication").ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

                const auto& desc = result.GetReplicationDescription();
                if (desc.GetState() == TReplicationDescription::EState::Done) {
                    break;
                }

                UNIT_ASSERT_C(i, "Alter timeout");
                Sleep(TDuration::Seconds(1));
            }
        }

        // Connection string and Endpoint/Database are mutually exclusive
        {
            auto query = R"(
                --!syntax_v1
                ALTER ASYNC REPLICATION `/Root/replication`
                SET (
                    CONNECTION_STRING = "grpc://localhost:2135/?database=/local",
                    ENDPOINT = "localhost:2135",
                    DATABASE = "/local"
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToOneLineString(), "CONNECTION_STRING and ENDPOINT/DATABASE are mutually exclusive", result.GetIssues().ToOneLineString());
        }

        // alter connection params
        {
            auto query = R"(
                --!syntax_v1
                ALTER ASYNC REPLICATION `/Root/replication`
                SET (
                    DATABASE = "/local"
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto query = R"(
                --!syntax_v1
                ALTER ASYNC REPLICATION `/Root/replication`
                SET (
                    ENDPOINT = "localhost:2136"
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto query = R"(
                --!syntax_v1
                ALTER ASYNC REPLICATION `/Root/replication`
                SET (
                    CONNECTION_STRING = "grpc://localhost:2135/?database=/Root"
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        // Token and User/Password are mutually exclusive
        {
            auto query = R"(
                --!syntax_v1
                ALTER ASYNC REPLICATION `/Root/replication`
                SET (
                    TOKEN = "foo",
                    USER = "user",
                    PASSWORD = "password"
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToOneLineString(), "TOKEN and USER/PASSWORD are mutually exclusive", result.GetIssues().ToOneLineString());
        }

        // TOKEN and TOKEN_SECRET_NAME are mutually exclusive
        {
            auto query = R"(
                --!syntax_v1
                ALTER ASYNC REPLICATION `/Root/replication`
                SET (
                    TOKEN = "token",
                    TOKEN_SECRET_NAME = "token_secret_name"
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToOneLineString(), "TOKEN and TOKEN_SECRET_NAME are mutually exclusive", result.GetIssues().ToOneLineString());
        }

        // PASSWORD and PASSWORD_SECRET_NAME are mutually exclusive
        {
            auto query = R"(
                --!syntax_v1
                ALTER ASYNC REPLICATION `/Root/replication`
                SET (
                    USER = "user",
                    PASSWORD = "password",
                    PASSWORD_SECRET_NAME = "password_secret_name"
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToOneLineString(), "PASSWORD and PASSWORD_SECRET_NAME are mutually exclusive", result.GetIssues().ToOneLineString());
        }

        // check alter credentials
        {
            auto query = R"(
                --!syntax_v1
                ALTER ASYNC REPLICATION `/Root/replication`
                SET (
                    TOKEN = "foo"
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto query = R"(
                --!syntax_v1
                ALTER ASYNC REPLICATION `/Root/replication`
                SET (
                    TOKEN_SECRET_NAME = "mysecret"
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        // set password witout user
        {
            auto query = R"(
                --!syntax_v1
                ALTER ASYNC REPLICATION `/Root/replication`
                SET (
                    PASSWORD = "password"
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToOneLineString(), "User is not set", result.GetIssues().ToOneLineString());
        }

        {
            auto query = R"(
                --!syntax_v1
                ALTER ASYNC REPLICATION `/Root/replication`
                SET (
                    PASSWORD_SECRET_NAME = "password_secret_name"
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToOneLineString(), "User is not set", result.GetIssues().ToOneLineString());
        }

        {
            auto query = R"(
                --!syntax_v1
                ALTER ASYNC REPLICATION `/Root/replication`
                SET (
                    USER = "user"
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto query = R"(
                --!syntax_v1
                ALTER ASYNC REPLICATION `/Root/replication`
                SET (
                    PASSWORD = "password"
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto query = R"(
                --!syntax_v1
                ALTER ASYNC REPLICATION `/Root/replication`
                SET (
                    PASSWORD_SECRET_NAME = "password_secret_name"
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto query = R"(
                --!syntax_v1
                ALTER ASYNC REPLICATION `/Root/replication`
                SET (
                    USER = "new_user",
                    PASSWORD = "new_password"
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

    }

    Y_UNIT_TEST(DropAsyncReplication) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        // path does not exist
        {
            auto query = R"(
                --!syntax_v1
                DROP ASYNC REPLICATION `/Root/replication`
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
            auto query = Sprintf(R"(
                --!syntax_v1
                CREATE ASYNC REPLICATION `/Root/replication` FOR
                    `/Root/table` AS `/Root/replica`
                WITH (
                    ENDPOINT = "%s",
                    DATABASE = "/Root"
                );
            )", kikimr.GetEndpoint().c_str());

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        while (true) {
            auto describe = session.DescribeTable("/Root/replica").GetValueSync();
            if (describe.GetStatus() == EStatus::SUCCESS) {
                break;
            }

            Sleep(TDuration::Seconds(1));
        }

        // ok
        {
            auto query = R"(
                --!syntax_v1
                DROP ASYNC REPLICATION `/Root/replication`
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto describe = session.DescribeTable("/Root/replica").GetValueSync();
            UNIT_ASSERT_EQUAL_C(describe.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(DropAsyncReplicationCascade) {
        TKikimrRunner kikimr;
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
                CREATE ASYNC REPLICATION `/Root/replication` FOR
                    `/Root/table` AS `/Root/replica`
                WITH (
                    ENDPOINT = "%s",
                    DATABASE = "/Root"
                );
            )", kikimr.GetEndpoint().c_str());

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        while (true) {
            auto describe = session.DescribeTable("/Root/replica").GetValueSync();
            if (describe.GetStatus() == EStatus::SUCCESS) {
                break;
            }

            Sleep(TDuration::Seconds(1));
        }

        // ok
        {
            auto query = R"(
                --!syntax_v1
                DROP ASYNC REPLICATION `/Root/replication` CASCADE
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto describe = session.DescribeTable("/Root/replica").GetValueSync();
            UNIT_ASSERT_EQUAL_C(describe.GetStatus(), EStatus::SCHEME_ERROR, result.GetIssues().ToString());
        }
    }

    void AsyncReplicationConnectionParams(TKikimrRunner& kikimr, const TString& connectionParam, bool ssl = false) {
        using namespace NReplication;

        auto repl = TReplicationClient(kikimr.GetDriver(), TCommonClientSettings().Database("/Root"));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        {
            auto query = R"(
                --!syntax_v1
                CREATE TABLE `/Root/table` (Key Uint64, Value String, PRIMARY KEY (Key));
            )";

            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto query = Sprintf(R"(
                --!syntax_v1
                CREATE ASYNC REPLICATION `/Root/replication` FOR
                    `/Root/table` AS `/Root/replica`
                WITH (
                    %s
                );
            )", connectionParam.c_str());

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            const auto result = repl.DescribeReplication("/Root/replication").ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            const auto& params = result.GetReplicationDescription().GetConnectionParams();
            UNIT_ASSERT_VALUES_EQUAL(params.GetDiscoveryEndpoint(), kikimr.GetEndpoint());
            UNIT_ASSERT_VALUES_EQUAL(params.GetDatabase(), "/Root");
            UNIT_ASSERT_VALUES_EQUAL(params.GetEnableSsl(), ssl);
        }
    }

    Y_UNIT_TEST(AsyncReplicationConnectionString) {
        TKikimrRunner kikimr;
        AsyncReplicationConnectionParams(kikimr, Sprintf(R"(CONNECTION_STRING = "grpc://%s/?database=/Root")", kikimr.GetEndpoint().c_str()));
    }

    Y_UNIT_TEST(AsyncReplicationConnectionStringWithSsl) {
        TKikimrRunner kikimr;
        AsyncReplicationConnectionParams(kikimr, Sprintf(R"(CONNECTION_STRING = "grpcs://%s/?database=/Root")", kikimr.GetEndpoint().c_str()), true);
    }

    Y_UNIT_TEST(AsyncReplicationEndpointAndDatabase) {
        TKikimrRunner kikimr;
        AsyncReplicationConnectionParams(kikimr, Sprintf(R"(ENDPOINT = "%s", DATABASE = "/Root")", kikimr.GetEndpoint().c_str()));
    }

    Y_UNIT_TEST(CreateTransfer) {
        TKikimrSettings serverSettings;
        serverSettings.FeatureFlags.SetEnableTopicTransfer(true);
        serverSettings.PQConfig.SetRequireCredentialsInNewProtocol(false);
        TKikimrRunner kikimr(serverSettings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        // negative
        {
            auto query = R"(
                --!syntax_v1
                CREATE TRANSFER `/Root/transfer`
                  FROM `/Root/topic` TO `/Root/table`
                WITH (
                    CONNECTION_STRING = "grpc://localhost:2135/?database=/Root",
                    ENDPOINT = "localhost:2135",
                    DATABASE = "/Root"
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToOneLineString(), "CONNECTION_STRING and ENDPOINT/DATABASE are mutually exclusive");
        }
        {
            auto query = R"(
                --!syntax_v1
                CREATE TRANSFER `/Root/transfer`
                  FROM `/Root/topic` TO `/Root/table`
                WITH (
                    ENDPOINT = "localhost:2135"
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToOneLineString(), "Neither CONNECTION_STRING nor ENDPOINT/DATABASE are provided");
        }
        {
            auto query = R"(
                --!syntax_v1
                CREATE TRANSFER `/Root/transfer`
                  FROM `/Root/topic` TO `/Root/table`
                WITH (
                    DATABASE = "/Root"
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToOneLineString(), "Neither CONNECTION_STRING nor ENDPOINT/DATABASE are provided");
        }
        {
            auto query = R"(
                --!syntax_v1
                CREATE TRANSFER `/Root/transfer`
                  FROM `/Root/topic` TO `/Root/table`
                WITH (
                    CONNECTION_STRING = "grpc://localhost:2135/?database=/Root",
                    TOKEN = "foo",
                    USER = "user",
                    PASSWORD = "bar"
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToOneLineString(), "TOKEN and USER/PASSWORD are mutually exclusive");
        }
        {
            auto query = R"(
                --!syntax_v1
                CREATE TRANSFER `/Root/transfer`
                  FROM `/Root/topic` TO `/Root/table`
                WITH (
                    CONNECTION_STRING = "grpc://localhost:2135/?database=/Root",
                    TOKEN = "foo",
                    TOKEN_SECRET_NAME = "bar"
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToOneLineString(), "TOKEN and TOKEN_SECRET_NAME are mutually exclusive");
        }
        {
            auto query = R"(
                --!syntax_v1
                CREATE TRANSFER `/Root/transfer`
                  FROM `/Root/topic` TO `/Root/table`
                WITH (
                    CONNECTION_STRING = "grpc://localhost:2135/?database=/Root",
                    USER = "user",
                    PASSWORD = "bar",
                    PASSWORD_SECRET_NAME = "baz"
                );
            )";
            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToOneLineString(), "PASSWORD and PASSWORD_SECRET_NAME are mutually exclusive");
        }
        {
            auto query = R"(
                --!syntax_v1
                CREATE TRANSFER `/Root/transfer`
                  FROM `/Root/topic` TO `/Root/table`
                WITH (
                    CONNECTION_STRING = "grpc://localhost:2135/?database=/Root",
                    PASSWORD = "bar"
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToOneLineString(), "USER is not provided");
        }
        {
            auto query = R"(
                --!syntax_v1
                CREATE TRANSFER `/Root/transfer`
                  FROM `/Root/topic` TO `/Root/table`
                WITH (
                    CONNECTION_STRING = "grpc://localhost:2135/?database=/Root",
                    USER = "user"
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToOneLineString(), "PASSWORD or PASSWORD_SECRET_NAME are not provided");
        }

        // positive
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
                CREATE TOPIC `/Root/topic`;
            )";

            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto query = Sprintf(R"(
                --!syntax_v1
                CREATE TRANSFER `/Root/transfer`
                  FROM `/Root/topic` TO `/Root/table`
                WITH (
                    ENDPOINT = "%s",
                    DATABASE = "/Root"
                );
            )", kikimr.GetEndpoint().c_str());

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto query = Sprintf(R"(
                --!syntax_v1
                $a = "a"; -- CA РусоТекс1
                ; -- Озер нон ассии
                $b = () -> { -- CB
                    return $a;
                };
                CREATE TRANSFER `/Root/transfer1`
                  FROM `/Root/topic` TO `/Root/table` USING ($x) -> {
                    -- CL
                    RETURN $b($x);
                  }
                  WITH (
                    ENDPOINT = "%s",
                    DATABASE = "/Root"
                );
            )", kikimr.GetEndpoint().c_str());

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(AlterTransfer) {
        using namespace NReplication;

        TKikimrSettings serverSettings;
        serverSettings.FeatureFlags.SetEnableTopicTransfer(true);
        serverSettings.PQConfig.SetRequireCredentialsInNewProtocol(false);
        TKikimrRunner kikimr(serverSettings);
        auto repl = TReplicationClient(kikimr.GetDriver(), TCommonClientSettings().Database("/Root"));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::REPLICATION_CONTROLLER, NActors::NLog::PRI_TRACE);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::REPLICATION_SERVICE, NActors::NLog::PRI_TRACE);

        // path does not exist
        {
            auto query = R"(
                --!syntax_v1
                ALTER TRANSFER `/Root/transfer`
                SET (
                    STATE = "DONE"
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SCHEME_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToOneLineString(), "Check failed: path: '/Root/transfer', error: path hasn't been resolved, nearest resolved path: '/Root'");
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
                CREATE TOPIC `/Root/topic`;
            )";

            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto query = Sprintf(R"(
                --!syntax_v1
                CREATE TRANSFER `/Root/transfer`
                    FROM `/Root/topic` TO `/Root/table`
                WITH (
                    ENDPOINT = "%s",
                    DATABASE = "/Root",
                    TOKEN = "root@builtin"
                );
            )", kikimr.GetEndpoint().c_str());

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        // invalid state
        {
            auto query = R"(
                --!syntax_v1
                ALTER TRANSFER `/Root/transfer`
                SET (
                    STATE = "foo"
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToOneLineString(), "Unknown transfer state: foo");
        }

        // alter state and config
        {
            auto query = R"(
                --!syntax_v1
                ALTER TRANSFER `/Root/transfer`
                SET (
                    STATE = "DONE",
                    CONNECTION_STRING = "grpc://localhost:2135/?database=/Root"
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToOneLineString(), "It is not allowed to change both settings and the state of the replication in the same query. Please submit separate queries for each action");
        }

        // Connection string and Endpoint/Database are mutually exclusive
        {
            auto query = R"(
                --!syntax_v1
                ALTER TRANSFER `/Root/transfer`
                SET (
                    CONNECTION_STRING = "grpc://localhost:2135/?database=/local",
                    ENDPOINT = "localhost:2135",
                    DATABASE = "/local"
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToOneLineString(), "CONNECTION_STRING and ENDPOINT/DATABASE are mutually exclusive");
        }

        // check alter state
        {
            auto query = R"(
                --!syntax_v1
                ALTER TRANSFER `/Root/transfer`
                SET (
                    STATE = "DONE",
                    FAILOVER_MODE = "FORCE"
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            for (size_t i = 10; i--;) {
                const auto result = repl.DescribeReplication("/Root/transfer").ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

                const auto& desc = result.GetReplicationDescription();
                if (desc.GetState() == TReplicationDescription::EState::Done) {
                    break;
                }

                //UNIT_ASSERT_C(i, "Alter timeout");
                Sleep(TDuration::Seconds(1));
            }
        }

        // alter connection params
        {
            auto query = R"(
                --!syntax_v1
                ALTER TRANSFER `/Root/transfer`
                SET (
                    DATABASE = "/local"
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto query = R"(
                --!syntax_v1
                ALTER TRANSFER `/Root/transfer`
                SET (
                    ENDPOINT = "localhost:2136"
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto query = R"(
                --!syntax_v1
                ALTER TRANSFER `/Root/transfer`
                SET (
                    CONNECTION_STRING = "grpc://localhost:2135/?database=/Root"
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        // Token and User/Password are mutually exclusive
        {
            auto query = R"(
                --!syntax_v1
                ALTER TRANSFER `/Root/transfer`
                SET (
                    TOKEN = "foo",
                    USER = "user",
                    PASSWORD = "password"
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToOneLineString(), "TOKEN and USER/PASSWORD are mutually exclusive");
        }

        // TOKEN and TOKEN_SECRET_NAME are mutually exclusive
        {
            auto query = R"(
                --!syntax_v1
                ALTER TRANSFER `/Root/transfer`
                SET (
                    TOKEN = "token",
                    TOKEN_SECRET_NAME = "token_secret_name"
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToOneLineString(), "TOKEN and TOKEN_SECRET_NAME are mutually exclusive");
        }

        // PASSWORD and PASSWORD_SECRET_NAME are mutually exclusive
        {
            auto query = R"(
                --!syntax_v1
                ALTER TRANSFER `/Root/transfer`
                SET (
                    USER = "user",
                    PASSWORD = "password",
                    PASSWORD_SECRET_NAME = "password_secret_name"
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToOneLineString(), "PASSWORD and PASSWORD_SECRET_NAME are mutually exclusive");
        }

        // check alter credentials
        {
            auto query = R"(
                --!syntax_v1
                ALTER TRANSFER `/Root/transfer`
                SET (
                    TOKEN = "foo"
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto query = R"(
                --!syntax_v1
                ALTER TRANSFER `/Root/transfer`
                SET (
                    TOKEN_SECRET_NAME = "mysecret"
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        // set password witout user
        {
            auto query = R"(
                --!syntax_v1
                ALTER TRANSFER `/Root/transfer`
                SET (
                    PASSWORD = "password"
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToOneLineString(), "User is not set");
        }

        {
            auto query = R"(
                --!syntax_v1
                ALTER TRANSFER `/Root/transfer`
                SET (
                    PASSWORD_SECRET_NAME = "password_secret_name"
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToOneLineString(), "User is not set");
        }

        {
            auto query = R"(
                --!syntax_v1
                ALTER TRANSFER `/Root/transfer`
                SET (
                    USER = "user"
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto query = R"(
                --!syntax_v1
                ALTER TRANSFER `/Root/transfer`
                SET (
                    PASSWORD = "password"
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto query = R"(
                --!syntax_v1
                ALTER TRANSFER `/Root/transfer`
                SET (
                    PASSWORD_SECRET_NAME = "password_secret_name"
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto query = R"(
                --!syntax_v1
                ALTER TRANSFER `/Root/transfer`
                SET (
                    USER = "new_user",
                    PASSWORD = "new_password"
                );
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto query = R"(
                --!syntax_v1
                ALTER TRANSFER `/Root/transfer`
                SET USING ($x) -> {
                    RETURN CAST($x as String);
                };
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

    }

    Y_UNIT_TEST(DropTransfer) {
        TKikimrSettings serverSettings;
        serverSettings.FeatureFlags.SetEnableTopicTransfer(true);
        serverSettings.PQConfig.SetRequireCredentialsInNewProtocol(false);
        TKikimrRunner kikimr(serverSettings);
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
                CREATE TOPIC `/Root/topic`;
            )";

            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto query = Sprintf(R"(
                --!syntax_v1
                CREATE TRANSFER `/Root/transfer`
                  FROM `/Root/topic` TO `/Root/table`
                WITH (
                    ENDPOINT = "%s",
                    DATABASE = "/Root"
                );
            )", kikimr.GetEndpoint().c_str());

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        while (true) {
            auto describe = session.DescribeTable("/Root/transfer").GetValueSync();
            if (describe.GetStatus() == EStatus::SUCCESS) {
                break;
            }

            Sleep(TDuration::Seconds(1));
        }

        // ok
        {
            auto query = R"(
                --!syntax_v1
                DROP TRANSFER `/Root/transfer` CASCADE
            )";

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto describe = session.DescribeTable("/Root/transfer").GetValueSync();
            UNIT_ASSERT_EQUAL_C(describe.GetStatus(), EStatus::SCHEME_ERROR, result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(DisableResourcePools) {
        NKikimrConfig::TAppConfig config;
        config.MutableFeatureFlags()->SetEnableResourcePools(false);

        TKikimrRunner kikimr(NKqp::TKikimrSettings()
            .SetAppConfig(config)
            .SetEnableResourcePools(false));

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto checkQuery = [&session](const TString& query, EStatus status, const TString& error) {
            Cerr << "Check query:\n" << query << "\n";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), status);
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), error, result.GetIssues().ToString());
        };

        auto checkDisabled = [checkQuery](const TString& query) {
            checkQuery(query, EStatus::UNSUPPORTED, "Resource pools are disabled. Please contact your system administrator to enable it");
        };

        // CREATE RESOURCE POOL
        checkDisabled(R"(
            CREATE RESOURCE POOL MyResourcePool WITH (
                CONCURRENT_QUERY_LIMIT=20,
                QUEUE_SIZE=1000
            );)");

        // ALTER RESOURCE POOL
        checkDisabled(R"(
            ALTER RESOURCE POOL MyResourcePool
                SET (CONCURRENT_QUERY_LIMIT = 30, QUEUE_SIZE = 100),
                RESET (QUERY_MEMORY_LIMIT_PERCENT_PER_NODE);
            )");

        // DROP RESOURCE POOL
        checkQuery("DROP RESOURCE POOL MyResourcePool;",
            EStatus::SCHEME_ERROR,
            "Path does not exist");
    }

    Y_UNIT_TEST(DisableResourcePoolsOnServerless) {
        auto ydb = NWorkload::TYdbSetupSettings()
            .CreateSampleTenants(true)
            .EnableResourcePoolsOnServerless(false)
            .Create();

        auto checkDisabled = [](const auto& result) {
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Resource pools are disabled for serverless domains. Please contact your system administrator to enable it", result.GetIssues().ToString());
        };

        auto checkNotFound = [](const auto& result) {
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Path does not exist", result.GetIssues().ToString());
        };

        const auto& createSql = R"(
            CREATE RESOURCE POOL MyResourcePool WITH (
                CONCURRENT_QUERY_LIMIT=20,
                QUEUE_SIZE=1000
            );)";

        const auto& alterSql = R"(
            ALTER RESOURCE POOL MyResourcePool
                SET (CONCURRENT_QUERY_LIMIT = 30, QUEUE_SIZE = 100),
                RESET (QUERY_MEMORY_LIMIT_PERCENT_PER_NODE);
            )";

        const auto& dropSql = "DROP RESOURCE POOL MyResourcePool;";

        auto settings = NWorkload::TQueryRunnerSettings().PoolId("");

        // Dedicated, enabled
        settings.Database(ydb->GetSettings().GetDedicatedTenantName()).NodeIndex(1);
        NWorkload::TSampleQueries::CheckSuccess(ydb->ExecuteQuery(createSql, settings));
        NWorkload::TSampleQueries::CheckSuccess(ydb->ExecuteQuery(alterSql, settings));
        NWorkload::TSampleQueries::CheckSuccess(ydb->ExecuteQuery(dropSql, settings));

        // Shared, enabled
        settings.Database(ydb->GetSettings().GetSharedTenantName()).NodeIndex(2);
        NWorkload::TSampleQueries::CheckSuccess(ydb->ExecuteQuery(createSql, settings));
        NWorkload::TSampleQueries::CheckSuccess(ydb->ExecuteQuery(alterSql, settings));
        NWorkload::TSampleQueries::CheckSuccess(ydb->ExecuteQuery(dropSql, settings));

        // Serverless, disabled
        settings.Database(ydb->GetSettings().GetServerlessTenantName()).NodeIndex(2);
        checkDisabled(ydb->ExecuteQuery(createSql, settings));
        checkNotFound(ydb->ExecuteQuery(alterSql, settings));
        checkNotFound(ydb->ExecuteQuery(dropSql, settings));
    }

    Y_UNIT_TEST(ResourcePoolsValidation) {
        NKikimrConfig::TAppConfig config;
        config.MutableFeatureFlags()->SetEnableResourcePools(true);

        TKikimrRunner kikimr(NKqp::TKikimrSettings()
            .SetAppConfig(config)
            .SetEnableResourcePools(true));

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteSchemeQuery(R"(
            CREATE RESOURCE POOL `MyFolder/MyResourcePool` WITH (
                CONCURRENT_QUERY_LIMIT=20
            );)").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::PRECONDITION_FAILED);
        UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Resource pool id should not contain '/' symbol", result.GetIssues().ToString());

        result = session.ExecuteSchemeQuery(R"(
            CREATE RESOURCE POOL MyResourcePool WITH (
                ANOTHER_LIMIT=20
            );)").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::BAD_REQUEST);
        UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Unknown property: another_limit", result.GetIssues().ToString());

        result = session.ExecuteSchemeQuery(R"(
            ALTER RESOURCE POOL MyResourcePool
                SET (ANOTHER_LIMIT = 5),
                RESET (SOME_LIMIT);
            )").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::BAD_REQUEST);
        UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Unknown property: another_limit, some_limit", result.GetIssues().ToString());

        result = session.ExecuteSchemeQuery(R"(
            CREATE RESOURCE POOL MyResourcePool WITH (
                CONCURRENT_QUERY_LIMIT="StringValue"
            );)").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::BAD_REQUEST);
        UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Failed to parse property concurrent_query_limit:", result.GetIssues().ToString());

        result = session.ExecuteSchemeQuery(TStringBuilder() << R"(
            CREATE RESOURCE POOL MyResourcePool WITH (
                CONCURRENT_QUERY_LIMIT=)" << NResourcePool::POOL_MAX_CONCURRENT_QUERY_LIMIT + 1 << R"(
            );)").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
        UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), 
            TStringBuilder() << "Invalid resource pool configuration, concurrent_query_limit is " << NResourcePool::POOL_MAX_CONCURRENT_QUERY_LIMIT + 1 << ", that exceeds limit in " << NResourcePool::POOL_MAX_CONCURRENT_QUERY_LIMIT,
            result.GetIssues().ToString()
        );

        result = session.ExecuteSchemeQuery(R"(
            CREATE RESOURCE POOL MyResourcePool WITH (
                QUEUE_SIZE=1
            );)").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
        UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Invalid resource pool configuration, queue_size unsupported without concurrent_query_limit or database_load_cpu_threshold", result.GetIssues().ToString());
    }

    Y_UNIT_TEST(CreateResourcePool) {
        NKikimrConfig::TAppConfig config;
        config.MutableFeatureFlags()->SetEnableResourcePools(true);

        TKikimrRunner kikimr(NKqp::TKikimrSettings()
            .SetAppConfig(config)
            .SetEnableResourcePools(true));

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto query = R"(
            CREATE RESOURCE POOL MyResourcePool WITH (
                CONCURRENT_QUERY_LIMIT=20,
                QUEUE_SIZE=1000,
                QUERY_MEMORY_LIMIT_PERCENT_PER_NODE=95
            );)";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        auto resourcePoolDesc = Navigate(runtime, runtime.AllocateEdgeActor(), "Root/.metadata/workload_manager/pools/MyResourcePool", NSchemeCache::TSchemeCacheNavigate::EOp::OpUnknown);
        const auto& resourcePool = resourcePoolDesc->ResultSet.at(0);
        UNIT_ASSERT_VALUES_EQUAL(resourcePool.Kind, NSchemeCache::TSchemeCacheNavigate::EKind::KindResourcePool);
        UNIT_ASSERT(resourcePool.ResourcePoolInfo);
        UNIT_ASSERT_VALUES_EQUAL(resourcePool.ResourcePoolInfo->Description.GetName(), "MyResourcePool");
        const auto& properties = resourcePool.ResourcePoolInfo->Description.GetProperties().GetProperties();
        UNIT_ASSERT_VALUES_EQUAL(properties.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(properties.at("concurrent_query_limit"), "20");
        UNIT_ASSERT_VALUES_EQUAL(properties.at("queue_size"), "1000");
        UNIT_ASSERT_VALUES_EQUAL(properties.at("query_memory_limit_percent_per_node"), "95");
    }

    Y_UNIT_TEST(DoubleCreateResourcePool) {
        NKikimrConfig::TAppConfig config;
        config.MutableFeatureFlags()->SetEnableResourcePools(true);

        TKikimrRunner kikimr(NKqp::TKikimrSettings()
            .SetAppConfig(config)
            .SetEnableResourcePools(true));

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        {
            auto query = R"(
                CREATE RESOURCE POOL MyResourcePool WITH (
                    CONCURRENT_QUERY_LIMIT=20
                );)";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto& runtime = *kikimr.GetTestServer().GetRuntime();
            auto resourcePoolDesc = Navigate(runtime, runtime.AllocateEdgeActor(), "Root/.metadata/workload_manager/pools/MyResourcePool", NSchemeCache::TSchemeCacheNavigate::EOp::OpUnknown);
            UNIT_ASSERT_VALUES_EQUAL(resourcePoolDesc->ResultSet.at(0).Kind, NSchemeCache::TSchemeCacheNavigate::EKind::KindResourcePool);
        }

        {
            auto query = R"(
                CREATE RESOURCE POOL MyResourcePool WITH (
                    QUERY_MEMORY_LIMIT_PERCENT_PER_NODE="0.5"
                );)";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Check failed: path: '/Root/.metadata/workload_manager/pools/MyResourcePool', error: path exist", result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(AlterResourcePool) {
        NKikimrConfig::TAppConfig config;
        config.MutableFeatureFlags()->SetEnableResourcePools(true);

        TKikimrRunner kikimr(NKqp::TKikimrSettings()
            .SetAppConfig(config)
            .SetEnableResourcePools(true));

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        {
            auto query = R"(
                CREATE RESOURCE POOL MyResourcePool WITH (
                    CONCURRENT_QUERY_LIMIT=20,
                    QUERY_MEMORY_LIMIT_PERCENT_PER_NODE=95
                );)";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto& runtime = *kikimr.GetTestServer().GetRuntime();
            auto resourcePoolDesc = Navigate(runtime, runtime.AllocateEdgeActor(), "Root/.metadata/workload_manager/pools/MyResourcePool", NSchemeCache::TSchemeCacheNavigate::EOp::OpUnknown);
            const auto& properties = resourcePoolDesc->ResultSet.at(0).ResourcePoolInfo->Description.GetProperties().GetProperties();
            UNIT_ASSERT_VALUES_EQUAL(properties.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(properties.at("concurrent_query_limit"), "20");
            UNIT_ASSERT_VALUES_EQUAL(properties.at("query_memory_limit_percent_per_node"), "95");
        }

        {
            auto query = R"(
                ALTER RESOURCE POOL MyResourcePool
                    SET (CONCURRENT_QUERY_LIMIT = 30, QUEUE_SIZE = 100),
                    RESET (QUERY_MEMORY_LIMIT_PERCENT_PER_NODE);
                )";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto& runtime = *kikimr.GetTestServer().GetRuntime();
            auto resourcePoolDesc = Navigate(runtime, runtime.AllocateEdgeActor(), "Root/.metadata/workload_manager/pools/MyResourcePool", NSchemeCache::TSchemeCacheNavigate::EOp::OpUnknown);
            const auto& properties = resourcePoolDesc->ResultSet.at(0).ResourcePoolInfo->Description.GetProperties().GetProperties();
            UNIT_ASSERT_VALUES_EQUAL(properties.size(), 3);
            UNIT_ASSERT_VALUES_EQUAL(properties.at("concurrent_query_limit"), "30");
            UNIT_ASSERT_VALUES_EQUAL(properties.at("queue_size"), "100");
            UNIT_ASSERT_VALUES_EQUAL(properties.at("query_memory_limit_percent_per_node"), "-1");
        }
    }

    Y_UNIT_TEST(AlterNonExistingResourcePool) {
        NKikimrConfig::TAppConfig config;
        config.MutableFeatureFlags()->SetEnableResourcePools(true);

        TKikimrRunner kikimr(NKqp::TKikimrSettings()
            .SetAppConfig(config)
            .SetEnableResourcePools(true));

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto query = R"(
            ALTER RESOURCE POOL MyResourcePool
                SET (CONCURRENT_QUERY_LIMIT = 30, QUEUE_SIZE = 100),
                RESET (QUERY_MEMORY_LIMIT_PERCENT_PER_NODE);
            )";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SCHEME_ERROR, result.GetIssues().ToString());
    }

    Y_UNIT_TEST(DropResourcePool) {
        NKikimrConfig::TAppConfig config;
        config.MutableFeatureFlags()->SetEnableResourcePools(true);

        TKikimrRunner kikimr(NKqp::TKikimrSettings()
            .SetAppConfig(config)
            .SetEnableResourcePools(true));

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        {
            auto query = R"(
                CREATE RESOURCE POOL MyResourcePool WITH (
                    CONCURRENT_QUERY_LIMIT=20
                );)";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto query = "DROP RESOURCE POOL MyResourcePool";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        auto resourcePoolDesc = Navigate(runtime, runtime.AllocateEdgeActor(), "Root/.metadata/workload_manager/pools/MyResourcePool", NSchemeCache::TSchemeCacheNavigate::EOp::OpUnknown);
        const auto& resourcePool = resourcePoolDesc->ResultSet.at(0);
        UNIT_ASSERT_VALUES_EQUAL(resourcePoolDesc->ErrorCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(resourcePool.Kind, NSchemeCache::TSchemeCacheNavigate::EKind::KindUnknown);
    }

    Y_UNIT_TEST(DropNonExistingResourcePool) {
        NKikimrConfig::TAppConfig config;
        config.MutableFeatureFlags()->SetEnableResourcePools(true);

        TKikimrRunner kikimr(NKqp::TKikimrSettings()
            .SetAppConfig(config)
            .SetEnableResourcePools(true));

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto query = "DROP RESOURCE POOL MyResourcePool";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SCHEME_ERROR, result.GetIssues().ToString());
    }

    Y_UNIT_TEST(DisableResourcePoolClassifiers) {
        NKikimrConfig::TAppConfig config;
        config.MutableFeatureFlags()->SetEnableResourcePools(false);

        TKikimrRunner kikimr(NKqp::TKikimrSettings()
            .SetAppConfig(config)
            .SetEnableResourcePools(false));

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto checkQuery = [&session](const TString& query, EStatus status, const TString& error = "") {
            Cerr << "Check query:\n" << query << "\n";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), status);
            if (status != EStatus::SUCCESS) {
                UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), error, result.GetIssues().ToString());
            }
        };

        auto checkDisabled = [checkQuery](const TString& query) {
            checkQuery(query, EStatus::GENERIC_ERROR, "Resource pool classifiers are disabled. Please contact your system administrator to enable it");
        };

        // CREATE RESOURCE POOL CLASSIFIER
        checkDisabled(R"(
            CREATE RESOURCE POOL CLASSIFIER MyResourcePoolClassifier WITH (
                RANK=20,
                RESOURCE_POOL="test_pool"
            );)");

        // ALTER RESOURCE POOL CLASSIFIER
        checkDisabled(R"(
            ALTER RESOURCE POOL CLASSIFIER MyResourcePoolClassifier
                SET (RANK = 1, RESOURCE_POOL = "test"),
                RESET (MEMBER_NAME);
            )");

        // DROP RESOURCE POOL CLASSIFIER
        checkQuery("DROP RESOURCE POOL CLASSIFIER MyResourcePoolClassifier;",
            EStatus::GENERIC_ERROR,
            "Classifier with name MyResourcePoolClassifier not found in database with id /Root");
    }

    Y_UNIT_TEST(DisableResourcePoolClassifiersOnServerless) {
        auto ydb = NWorkload::TYdbSetupSettings()
            .CreateSampleTenants(true)
            .EnableResourcePoolsOnServerless(false)
            .Create();

        auto checkDisabled = [](const auto& result) {
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Resource pool classifiers are disabled for serverless domains. Please contact your system administrator to enable it", result.GetIssues().ToString());
        };

        auto checkNotFound = [](const auto& result) {
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Classifier with name MyResourcePoolClassifier not found in database", result.GetIssues().ToString());
        };

        const auto& createSql = R"(
            CREATE RESOURCE POOL CLASSIFIER MyResourcePoolClassifier WITH (
                RANK=20,
                RESOURCE_POOL="test_pool"
            );)";

        const auto& alterSql = R"(
            ALTER RESOURCE POOL CLASSIFIER MyResourcePoolClassifier
                SET (RANK = 1, RESOURCE_POOL = "test"),
                RESET (MEMBER_NAME);
            )";

        const auto& dropSql = "DROP RESOURCE POOL CLASSIFIER MyResourcePoolClassifier;";

        auto settings = NWorkload::TQueryRunnerSettings().PoolId("");

        // Dedicated, enabled
        settings.Database(ydb->GetSettings().GetDedicatedTenantName()).NodeIndex(1);
        NWorkload::TSampleQueries::CheckSuccess(ydb->ExecuteQuery(createSql, settings));
        NWorkload::TSampleQueries::CheckSuccess(ydb->ExecuteQuery(alterSql, settings));
        NWorkload::TSampleQueries::CheckSuccess(ydb->ExecuteQuery(dropSql, settings));

        // Shared, enabled
        settings.Database(ydb->GetSettings().GetSharedTenantName()).NodeIndex(2);
        NWorkload::TSampleQueries::CheckSuccess(ydb->ExecuteQuery(createSql, settings));
        NWorkload::TSampleQueries::CheckSuccess(ydb->ExecuteQuery(alterSql, settings));
        NWorkload::TSampleQueries::CheckSuccess(ydb->ExecuteQuery(dropSql, settings));

        // Serverless, disabled
        settings.Database(ydb->GetSettings().GetServerlessTenantName()).NodeIndex(2);
        checkDisabled(ydb->ExecuteQuery(createSql, settings));
        checkDisabled(ydb->ExecuteQuery(alterSql, settings));
        checkNotFound(ydb->ExecuteQuery(dropSql, settings));
    }

    Y_UNIT_TEST(ResourcePoolClassifiersValidation) {
        NKikimrConfig::TAppConfig config;
        config.MutableFeatureFlags()->SetEnableResourcePools(true);

        TKikimrRunner kikimr(NKqp::TKikimrSettings()
            .SetAppConfig(config)
            .SetEnableResourcePools(true));

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteSchemeQuery(R"(
            CREATE RESOURCE POOL CLASSIFIER MyResourcePoolClassifier WITH (
                RESOURCE_POOL="test",
                ANOTHER_PROPERTY=20
            );)").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
        UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Unknown property: another_property", result.GetIssues().ToString());

        result = session.ExecuteSchemeQuery(R"(
            ALTER RESOURCE POOL CLASSIFIER MyResourcePoolClassifier
                SET (ANOTHER_PROPERTY = 5),
                RESET (SOME_PROPERTY);
            )").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
        UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Unknown property: another_property, some_property", result.GetIssues().ToString());

        result = session.ExecuteSchemeQuery(R"(
            CREATE RESOURCE POOL CLASSIFIER MyResourcePoolClassifier WITH (
                RESOURCE_POOL="test",
                RANK="StringValue"
            );)").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
        UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Failed to parse property rank:", result.GetIssues().ToString());

        result = session.ExecuteSchemeQuery(R"(
            CREATE RESOURCE POOL CLASSIFIER MyResourcePoolClassifier WITH (
                RANK="0"
            );)").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
        UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Missing required property resource_pool", result.GetIssues().ToString());

        result = session.ExecuteSchemeQuery(R"(
            ALTER RESOURCE POOL CLASSIFIER MyResourcePoolClassifier
                RESET (RESOURCE_POOL);
            )").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
        UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Cannot reset required property resource_pool", result.GetIssues().ToString());

        result = session.ExecuteSchemeQuery(R"(
            CREATE RESOURCE POOL CLASSIFIER `MyResource/PoolClassifier` WITH (
                RESOURCE_POOL="test"
            );)").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
        UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Symbol '/' is not allowed in the resource pool classifier name 'MyResource/PoolClassifier'", result.GetIssues().ToString());

        result = session.ExecuteSchemeQuery(TStringBuilder() << R"(
            CREATE RESOURCE POOL CLASSIFIER MyResourcePoolClassifier WITH (
                RESOURCE_POOL="test",
                MEMBER_NAME=")" << BUILTIN_ACL_METADATA << R"("
            );)").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
        UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), 
            TStringBuilder() << "Invalid resource pool classifier configuration, cannot create classifier for system user " << BUILTIN_ACL_METADATA,
            result.GetIssues().ToString()
        );
    }

    Y_UNIT_TEST(ResourcePoolClassifiersRankValidation) {
        NKikimrConfig::TAppConfig config;
        config.MutableFeatureFlags()->SetEnableResourcePools(true);

        TKikimrRunner kikimr(NKqp::TKikimrSettings()
            .SetAppConfig(config)
            .SetEnableResourcePools(true));

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        // Create with sample rank
        auto result = session.ExecuteSchemeQuery(R"(
            CREATE RESOURCE POOL CLASSIFIER ClassifierRank42 WITH (
                RESOURCE_POOL="test_pool",
                RANK=42
            );)").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());

        // Try to create with same rank
        result = session.ExecuteSchemeQuery(R"(
            CREATE RESOURCE POOL CLASSIFIER AnotherClassifierRank42 WITH (
                RESOURCE_POOL="test_pool",
                RANK=42
            );)").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
        UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Resource pool classifier rank check failed, status: ALREADY_EXISTS, reason: { <main>: Error: Classifier with rank 42 already exists, its name ClassifierRank42 }", result.GetIssues().ToString());

        // Create with high rank
        result = session.ExecuteSchemeQuery(R"(
            CREATE RESOURCE POOL CLASSIFIER `ClassifierRank2^63` WITH (
                RESOURCE_POOL="test_pool",
                RANK=9223372036854775807
            );)").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());

        // Try to create with auto rank
        result = session.ExecuteSchemeQuery(R"(
            CREATE RESOURCE POOL CLASSIFIER ClassifierRankAuto WITH (
                RESOURCE_POOL="test_pool",
                MEMBER_NAME="test@user"
            );)").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
        UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "The rank could not be set automatically, the maximum rank of the resource pool classifier is too high: 9223372036854775807", result.GetIssues().ToString());

        // Try to alter to exist rank
        result = session.ExecuteSchemeQuery(R"(
            ALTER RESOURCE POOL CLASSIFIER `ClassifierRank2^63` SET (
                RANK=42
            );)").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
        UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Resource pool classifier rank check failed, status: ALREADY_EXISTS, reason: { <main>: Error: Classifier with rank 42 already exists, its name ClassifierRank42 }", result.GetIssues().ToString());

        // Try to reset classifier rank
        result = session.ExecuteSchemeQuery(R"(
            ALTER RESOURCE POOL CLASSIFIER ClassifierRank42 RESET (
                RANK
            );)").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
        UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "The rank could not be set automatically, the maximum rank of the resource pool classifier is too high: 9223372036854775807", result.GetIssues().ToString());
    }

    TString FetchResourcePoolClassifiers(TTestActorRuntime& runtime, ui32 nodeIndex) {
        const TActorId edgeActor = runtime.AllocateEdgeActor(nodeIndex);
        runtime.Send(NMetadata::NProvider::MakeServiceId(runtime.GetNodeId(nodeIndex)), edgeActor, new NMetadata::NProvider::TEvAskSnapshot(std::make_shared<TResourcePoolClassifierSnapshotsFetcher>()), nodeIndex);

        const auto response = runtime.GrabEdgeEvent<NMetadata::NProvider::TEvRefreshSubscriberData>(edgeActor);
        UNIT_ASSERT(response);
        return response->Get()->GetSnapshotAs<TResourcePoolClassifierSnapshot>()->SerializeToString();
    }

    TString FetchResourcePoolClassifiers(TKikimrRunner& kikimr) {
        return FetchResourcePoolClassifiers(*kikimr.GetTestServer().GetRuntime(), 0);
    }

    Y_UNIT_TEST(CreateResourcePoolClassifier) {
        NKikimrConfig::TAppConfig config;
        config.MutableFeatureFlags()->SetEnableResourcePools(true);

        TKikimrRunner kikimr(NKqp::TKikimrSettings()
            .SetAppConfig(config)
            .SetEnableResourcePools(true));

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        // Explicit rank
        auto query = R"(
            CREATE RESOURCE POOL CLASSIFIER MyResourcePoolClassifier WITH (
                RANK=20,
                RESOURCE_POOL="test_pool",
                MEMBER_NAME="test@user"
            );)";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(FetchResourcePoolClassifiers(kikimr), "{\"resource_pool_classifiers\":[{\"rank\":20,\"name\":\"MyResourcePoolClassifier\",\"config\":{\"member_name\":\"test@user\",\"resource_pool\":\"test_pool\"},\"database\":\"\\/Root\"}]}");

        // Auto rank
        query = R"(
            CREATE RESOURCE POOL CLASSIFIER AnotherResourcePoolClassifier WITH (
                RESOURCE_POOL="test_pool",
                MEMBER_NAME="another@user"
            );)";
        result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(FetchResourcePoolClassifiers(kikimr), "{\"resource_pool_classifiers\":[{\"rank\":20,\"name\":\"MyResourcePoolClassifier\",\"config\":{\"member_name\":\"test@user\",\"resource_pool\":\"test_pool\"},\"database\":\"\\/Root\"},{\"rank\":1020,\"name\":\"AnotherResourcePoolClassifier\",\"config\":{\"member_name\":\"another@user\",\"resource_pool\":\"test_pool\"},\"database\":\"\\/Root\"}]}");
    }

    Y_UNIT_TEST(CreateResourcePoolClassifierOnServerless) {
        auto ydb = NWorkload::TYdbSetupSettings()
            .CreateSampleTenants(true)
            .EnableResourcePoolsOnServerless(true)
            .Create();

        const auto& serverlessTenant = ydb->GetSettings().GetServerlessTenantName();
        ydb->ExecuteQueryRetry("Wait EnableResourcePoolsOnServerless", R"(
            CREATE RESOURCE POOL CLASSIFIER MyResourcePoolClassifier WITH (
                RANK=20,
                RESOURCE_POOL="test_pool"
            );)",
            NWorkload::TQueryRunnerSettings()
                .PoolId("")
                .Database(serverlessTenant)
                .NodeIndex(1)
        );

        const auto pathId = ydb->FetchDatabase(serverlessTenant)->Get()->PathId;
        UNIT_ASSERT_VALUES_EQUAL(
            FetchResourcePoolClassifiers(*ydb->GetRuntime(), 1),
            TStringBuilder() << "{\"resource_pool_classifiers\":[{\"rank\":20,\"name\":\"MyResourcePoolClassifier\",\"config\":{\"resource_pool\":\"test_pool\"},\"database\":\"" << pathId.OwnerId << ":" << pathId.LocalPathId << ":\\/Root\\/test-serverless\"}]}"
        );
    }

    Y_UNIT_TEST(DoubleCreateResourcePoolClassifier) {
        NKikimrConfig::TAppConfig config;
        config.MutableFeatureFlags()->SetEnableResourcePools(true);

        TKikimrRunner kikimr(NKqp::TKikimrSettings()
            .SetAppConfig(config)
            .SetEnableResourcePools(true));

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        {
            auto query = R"(
                CREATE RESOURCE POOL CLASSIFIER MyResourcePoolClassifier WITH (
                    RESOURCE_POOL="test_pool",
                    RANK=20
                );)";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto query = R"(
                CREATE RESOURCE POOL CLASSIFIER MyResourcePoolClassifier WITH (
                    RESOURCE_POOL="test_pool",
                    RANK=1
                );)";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Conflict with existing key", result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(AlterResourcePoolClassifier) {
        NKikimrConfig::TAppConfig config;
        config.MutableFeatureFlags()->SetEnableResourcePools(true);

        TKikimrRunner kikimr(NKqp::TKikimrSettings()
            .SetAppConfig(config)
            .SetEnableResourcePools(true));

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        // Create sample pool
        {
            auto query = R"(
                CREATE RESOURCE POOL CLASSIFIER MyResourcePoolClassifier WITH (
                    RANK=20,
                    RESOURCE_POOL="test_pool"
                );)";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(FetchResourcePoolClassifiers(kikimr), "{\"resource_pool_classifiers\":[{\"rank\":20,\"name\":\"MyResourcePoolClassifier\",\"config\":{\"resource_pool\":\"test_pool\"},\"database\":\"\\/Root\"}]}");
        }

        // Test update one property
        {
            auto query = R"(
                ALTER RESOURCE POOL CLASSIFIER MyResourcePoolClassifier
                    SET (MEMBER_NAME = "test@user")
                )";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(FetchResourcePoolClassifiers(kikimr), "{\"resource_pool_classifiers\":[{\"rank\":20,\"name\":\"MyResourcePoolClassifier\",\"config\":{\"member_name\":\"test@user\",\"resource_pool\":\"test_pool\"},\"database\":\"\\/Root\"}]}");
        }

        // Create another pool
        {
            auto query = R"(
                CREATE RESOURCE POOL CLASSIFIER AnotherResourcePoolClassifier WITH (
                    RESOURCE_POOL="test_pool",
                    RANK=42
                );)";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(FetchResourcePoolClassifiers(kikimr), "{\"resource_pool_classifiers\":[{\"rank\":20,\"name\":\"MyResourcePoolClassifier\",\"config\":{\"member_name\":\"test@user\",\"resource_pool\":\"test_pool\"},\"database\":\"\\/Root\"},{\"rank\":42,\"name\":\"AnotherResourcePoolClassifier\",\"config\":{\"resource_pool\":\"test_pool\"},\"database\":\"\\/Root\"}]}");
        }

        // Test reset
        {
            auto query = R"(
                ALTER RESOURCE POOL CLASSIFIER MyResourcePoolClassifier
                    RESET (RANK, MEMBER_NAME);
                )";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(FetchResourcePoolClassifiers(kikimr), "{\"resource_pool_classifiers\":[{\"rank\":1042,\"name\":\"MyResourcePoolClassifier\",\"config\":{\"member_name\":\"\",\"resource_pool\":\"test_pool\"},\"database\":\"\\/Root\"},{\"rank\":42,\"name\":\"AnotherResourcePoolClassifier\",\"config\":{\"resource_pool\":\"test_pool\"},\"database\":\"\\/Root\"}]}");
        }
    }

    Y_UNIT_TEST(AlterNonExistingResourcePoolClassifier) {
        NKikimrConfig::TAppConfig config;
        config.MutableFeatureFlags()->SetEnableResourcePools(true);

        TKikimrRunner kikimr(NKqp::TKikimrSettings()
            .SetAppConfig(config)
            .SetEnableResourcePools(true));

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto query = R"(
            ALTER RESOURCE POOL CLASSIFIER MyResourcePoolClassifier
                SET (RESOURCE_POOL = "test", RANK = 100),
                RESET (MEMBER_NAME);
            )";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Classifier with name MyResourcePoolClassifier not found in database with id /Root", result.GetIssues().ToString());
    }

    Y_UNIT_TEST(DropResourcePoolClassifier) {
        NKikimrConfig::TAppConfig config;
        config.MutableFeatureFlags()->SetEnableResourcePools(true);

        TKikimrRunner kikimr(NKqp::TKikimrSettings()
            .SetAppConfig(config)
            .SetEnableResourcePools(true));

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        {
            auto query = R"(
                CREATE RESOURCE POOL CLASSIFIER MyResourcePoolClassifier WITH (
                    RESOURCE_POOL="test_pool",
                    RANK=20
                );)";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(FetchResourcePoolClassifiers(kikimr), "{\"resource_pool_classifiers\":[{\"rank\":20,\"name\":\"MyResourcePoolClassifier\",\"config\":{\"resource_pool\":\"test_pool\"},\"database\":\"\\/Root\"}]}");
        }

        {
            auto query = "DROP RESOURCE POOL CLASSIFIER MyResourcePoolClassifier";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(FetchResourcePoolClassifiers(kikimr), "{\"resource_pool_classifiers\":[]}");
        }
    }

    Y_UNIT_TEST(DropNonExistingResourcePoolClassifier) {
        NKikimrConfig::TAppConfig config;
        config.MutableFeatureFlags()->SetEnableResourcePools(true);

        TKikimrRunner kikimr(NKqp::TKikimrSettings()
            .SetAppConfig(config)
            .SetEnableResourcePools(true));

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto query = "DROP RESOURCE POOL CLASSIFIER MyResourcePoolClassifier;";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Classifier with name MyResourcePoolClassifier not found in database with id /Root", result.GetIssues().ToString());
    }

    Y_UNIT_TEST(DisableMetadataObjectsOnServerless) {
        auto ydb = NWorkload::TYdbSetupSettings()
            .CreateSampleTenants(true)
            .EnableMetadataObjectsOnServerless(false)
            .Create();

        auto checkDisabled = [](const auto& result) {
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Objects SECRET are disabled for serverless domains. Please contact your system administrator to enable it", result.GetIssues().ToString());
        };

        const auto& createSql = "CREATE OBJECT MySecretObject (TYPE SECRET) WITH (value=\"qwerty\");";
        const auto& alterSql = "ALTER OBJECT MySecretObject (TYPE SECRET) SET value = \"abcde\";";
        const auto& upsertSql = "UPSERT OBJECT MySecretObject (TYPE SECRET) WITH value = \"edcba\";";
        const auto& dropSql = "DROP OBJECT MySecretObject (TYPE SECRET);";

        auto settings = NWorkload::TQueryRunnerSettings().PoolId("");

        // Dedicated, enabled
        settings.Database(ydb->GetSettings().GetDedicatedTenantName()).NodeIndex(1);
        NWorkload::TSampleQueries::CheckSuccess(ydb->ExecuteQuery(createSql, settings));
        NWorkload::TSampleQueries::CheckSuccess(ydb->ExecuteQuery(alterSql, settings));
        NWorkload::TSampleQueries::CheckSuccess(ydb->ExecuteQuery(upsertSql, settings));
        NWorkload::TSampleQueries::CheckSuccess(ydb->ExecuteQuery(dropSql, settings));

        // Shared, enabled
        settings.Database(ydb->GetSettings().GetSharedTenantName()).NodeIndex(2);
        NWorkload::TSampleQueries::CheckSuccess(ydb->ExecuteQuery(createSql, settings));
        NWorkload::TSampleQueries::CheckSuccess(ydb->ExecuteQuery(alterSql, settings));
        NWorkload::TSampleQueries::CheckSuccess(ydb->ExecuteQuery(upsertSql, settings));
        NWorkload::TSampleQueries::CheckSuccess(ydb->ExecuteQuery(dropSql, settings));

        // Serverless, disabled
        settings.Database(ydb->GetSettings().GetServerlessTenantName()).NodeIndex(2);
        checkDisabled(ydb->ExecuteQuery(createSql, settings));
        checkDisabled(ydb->ExecuteQuery(alterSql, settings));
        checkDisabled(ydb->ExecuteQuery(upsertSql, settings));
        NWorkload::TSampleQueries::CheckSuccess(ydb->ExecuteQuery(dropSql, settings));
    }

    Y_UNIT_TEST(CreateBackupCollectionDisabledByDefault) {
        TKikimrRunner kikimr;

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        {
            auto query = R"(
                --!syntax_v1
                CREATE BACKUP COLLECTION `my_collection`
                    (TABLE `table`)
                WITH(
                    STORAGE = 'cluster'
                );
            )";

            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToOneLineString(), "Backup collections are disabled. Please contact your system administrator to enable it", result.GetIssues().ToOneLineString());
        }
    }

    Y_UNIT_TEST(CreateBackupCollection) {
        NKikimrConfig::TAppConfig config;
        config.MutableFeatureFlags()->SetEnableBackupService(true);

        TKikimrRunner kikimr(NKqp::TKikimrSettings()
            .SetAppConfig(config)
            .SetEnableBackupService(true));

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        // negative
        {
            auto query = R"(
                --!syntax_v1
                CREATE BACKUP COLLECTION `/Root/my_collection`
                    (TABLE `/Root/table`)
                WITH(
                    STORAGE = 'cluster',
                    INCREMENTAL_BACKUP_ENABLED = 'true'
                );
            )";

            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SCHEME_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToOneLineString(), "Backup collections must be placed in", result.GetIssues().ToOneLineString());
        }

        // positive
        {
            auto query = R"(
                --!syntax_v1
                CREATE BACKUP COLLECTION `my_collection`
                    (TABLE `/Root/table`)
                WITH(
                    STORAGE = 'cluster',
                    INCREMENTAL_BACKUP_ENABLED = 'true'
                );
            )";

            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto query = R"(
                --!syntax_v1
                CREATE BACKUP COLLECTION `/Root/.backups/collections/my_collection2`
                    (TABLE `/Root/table`)
                WITH(
                    STORAGE = 'cluster',
                    INCREMENTAL_BACKUP_ENABLED = 'true'
                );
            )";

            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
    }
}

Y_UNIT_TEST_SUITE(KqpOlapScheme) {

    Y_UNIT_TEST(DropTable) {
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper testHelper(runnerSettings);

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("id_second").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("level").SetType(NScheme::NTypeIds::Int32),
            TTestHelper::TColumnSchema().SetName("created_at").SetType(NScheme::NTypeIds::Timestamp).SetNullable(false)
        };

        TTestHelper::TColumnTable testTable;
        testTable
            .SetName("/Root/ColumnTableTest")
            .SetPrimaryKey({ "id", "id_second" })
            .SetSharding({ "id" })
            .SetMinPartitionsCount(16)
            .SetSchema(schema);
        testHelper.CreateTable(testTable);
        auto sender = testHelper.GetRuntime().AllocateEdgeActor();
        auto tabletIds = GetColumnTableShards(&testHelper.GetKikimr().GetTestServer(), sender, "/Root/ColumnTableTest");
        for (auto tablet: tabletIds) {
            UNIT_ASSERT_C(testHelper.GetKikimr().GetTestClient().TabletExistsInHive(&testHelper.GetRuntime(), tablet), ToString(tablet) + " not alive");
        }
        testHelper.DropTable("/Root/ColumnTableTest");
        for (auto tablet: tabletIds) {
            testHelper.WaitTabletDeletionInHive(tablet, TDuration::Seconds(5));
            UNIT_ASSERT_C(!testHelper.GetKikimr().GetTestClient().TabletExistsInHive(&testHelper.GetRuntime(), tablet), ToString(tablet) + " is alive");
        }
    }

    Y_UNIT_TEST(AddColumnLongPk) {
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper testHelper(runnerSettings);

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("id_second").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("resource_id").SetType(NScheme::NTypeIds::Utf8),
            TTestHelper::TColumnSchema().SetName("level").SetType(NScheme::NTypeIds::Int32)
        };
        TTestHelper::TColumnTable testTable;

        testTable.SetName("/Root/ColumnTableTest").SetPrimaryKey({"id", "id_second"}).SetSharding({"id"}).SetSchema(schema);
        testHelper.CreateTable(testTable);

        {
            schema.push_back(TTestHelper::TColumnSchema().SetName("new_column").SetType(NScheme::NTypeIds::Uint64));
            auto alterQuery = TStringBuilder() << "ALTER TABLE `" << testTable.GetName() << "` ADD COLUMN new_column Uint64;";
            auto alterResult = testHelper.GetSession().ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(CreateTableWithTtl) {
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper testHelper(runnerSettings);

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("id_second").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("level").SetType(NScheme::NTypeIds::Int32),
            TTestHelper::TColumnSchema().SetName("created_at").SetType(NScheme::NTypeIds::Timestamp).SetNullable(false)
        };

        TTestHelper::TColumnTable testTable;
        testTable.SetName("/Root/ColumnTableTest").SetPrimaryKey({"created_at", "id_second"}).SetSharding({"created_at"}).SetSchema(schema).SetTTL("created_at", "Interval(\"PT1H\")");
        testHelper.CreateTable(testTable);

        {
            auto settings = TDescribeTableSettings().WithTableStatistics(true);
            auto describeResult = testHelper.GetSession().DescribeTable("/Root/ColumnTableTest", settings).GetValueSync();
            UNIT_ASSERT_C(describeResult.IsSuccess(), describeResult.GetIssues().ToString());

            const auto& description = describeResult.GetTableDescription();
            auto columns = description.GetTableColumns();
            UNIT_ASSERT_VALUES_EQUAL(columns.size(), 4);
            UNIT_ASSERT(description.GetTtlSettings());
            UNIT_ASSERT_VALUES_EQUAL(description.GetTtlSettings()->GetDateTypeColumn().GetExpireAfter(), TDuration::Hours(1));
        }
    }

    Y_UNIT_TEST(CreateTableWithoutTtl) {
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper testHelper(runnerSettings);

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("id_second").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("level").SetType(NScheme::NTypeIds::Int32),
            TTestHelper::TColumnSchema().SetName("created_at").SetType(NScheme::NTypeIds::Timestamp).SetNullable(false)
        };

        TTestHelper::TColumnTable testTable;
        testTable.SetName("/Root/ColumnTableTest").SetPrimaryKey({"id", "id_second"}).SetSharding({"id"}).SetSchema(schema);
        testHelper.CreateTable(testTable);

        {
            auto settings = TDescribeTableSettings().WithTableStatistics(true);
            auto describeResult = testHelper.GetSession().DescribeTable("/Root/ColumnTableTest", settings).GetValueSync();
            UNIT_ASSERT_C(describeResult.IsSuccess(), describeResult.GetIssues().ToString());

            const auto& description = describeResult.GetTableDescription();
            auto columns = description.GetTableColumns();
            UNIT_ASSERT_VALUES_EQUAL(columns.size(), 4);
            UNIT_ASSERT(!description.GetTtlSettings());
        }
    }

    Y_UNIT_TEST(InvalidColumnInTieringRule) {
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();

        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper testHelper(runnerSettings);
        Tests::NCommon::TLoggerInit(testHelper.GetKikimr()).Initialize();

        const TString tableName = "/Root/ColumnTableTest";

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("id_second").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("level").SetType(NScheme::NTypeIds::Int32),
            TTestHelper::TColumnSchema().SetName("created_at").SetType(NScheme::NTypeIds::Timestamp).SetNullable(false)
        };

        TTestHelper::TColumnTable testTable;
        testTable.SetName(tableName).SetPrimaryKey({"created_at", "id_second"}).SetSharding({"created_at"}).SetSchema(schema).SetTTL("created_at", "Interval(\"PT1H\")");
        testHelper.CreateTable(testTable);
        testHelper.CreateTier("tier1");

        {
            TTestHelper::TUpdatesBuilder tableInserter(testTable.GetArrowSchema(schema));
            tableInserter.AddRow().Add(1).Add(1).Add(7).Add((TInstant::Now() - TDuration::Days(30)).MilliSeconds());
            tableInserter.AddRow().Add(1).Add(2).Add(7).Add((TInstant::Now() - TDuration::Days(30)).MilliSeconds());
            testHelper.BulkUpsert(testTable, tableInserter);
        }

        testHelper.SetTiering(tableName, "/Root/tier1", "created_at");

        while (csController->GetTieringUpdates().Val() == 0) {
            Cout << "Wait tiering..." << Endl;
            Sleep(TDuration::Seconds(2));
        }

        testHelper.RebootTablets(tableName);
    }

    Y_UNIT_TEST(AddColumnWithTtl) {
        auto settings = TKikimrSettings()
            .SetColumnShardAlterObjectEnabled(true)
            .SetWithSampleTables(false);
        TTestHelper testHelper(settings);

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("id_second").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("level").SetType(NScheme::NTypeIds::Int32),
            TTestHelper::TColumnSchema().SetName("created_at").SetType(NScheme::NTypeIds::Timestamp).SetNullable(false)
        };

        TTestHelper::TColumnTable testTable;
        testTable.SetName("/Root/ColumnTableTest").SetPrimaryKey({"id", "id_second"}).SetSharding({"id"}).SetSchema(schema);
        testHelper.CreateTable(testTable);
        testHelper.CreateTier("tier1");

        {
            auto alterQuery = TStringBuilder() << R"(
            --!syntax_v1
            ALTER OBJECT `)" << testTable.GetName() << R"(` (TYPE TABLE) SET (ACTION=UPSERT_INDEX,
                NAME=max_value1, TYPE=MAX, FEATURES=`{\"column_name\": \"created_at\"}`))";
            auto alterResult = testHelper.GetSession().ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), EStatus::SUCCESS, alterResult.GetIssues().ToString());

        }

        {
            auto alterQuery = TStringBuilder() << "ALTER TABLE `" << testTable.GetName() << "`SET (TTL = Interval(\"PT1H\") ON created_at);";
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
            UNIT_ASSERT_VALUES_EQUAL(description.GetTtlSettings()->GetDateTypeColumn().GetExpireAfter(), TDuration::Hours(1));
        }
        {
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
            UNIT_ASSERT_VALUES_EQUAL(columns.size(), 5);
            UNIT_ASSERT_VALUES_EQUAL(description.GetTtlSettings()->GetDateTypeColumn().GetExpireAfter(), TDuration::Hours(1));
        }
        {
            auto alterQuery = TStringBuilder() << "ALTER TABLE `" << testTable.GetName() << "`SET (TTL = Interval(\"PT10S\") TO EXTERNAL DATA SOURCE `/Root/tier1`, Interval(\"PT1H\") DELETE ON created_at);";
            auto alterResult = testHelper.GetSession().ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }
        {
            auto settings = TDescribeTableSettings().WithTableStatistics(true);
            auto describeResult = testHelper.GetSession().DescribeTable("/Root/ColumnTableTest", settings).GetValueSync();
            UNIT_ASSERT_C(describeResult.IsSuccess(), describeResult.GetIssues().ToString());

            const auto& description = describeResult.GetTableDescription();
            UNIT_ASSERT(describeResult.GetTableDescription().GetTtlSettings());
            auto ttl = describeResult.GetTableDescription().GetTtlSettings();
            UNIT_ASSERT_VALUES_EQUAL(ttl->GetTiers().size(), 2);
            auto evictTier = ttl->GetTiers()[0];
            UNIT_ASSERT(std::holds_alternative<TTtlEvictToExternalStorageAction>(evictTier.GetAction()));
            UNIT_ASSERT_VALUES_EQUAL(std::get<TTtlEvictToExternalStorageAction>(evictTier.GetAction()).GetStorage(), "/Root/tier1");
            UNIT_ASSERT_VALUES_EQUAL(std::get<TDateTypeColumnModeSettings>(evictTier.GetExpression()).GetExpireAfter(), TDuration::Seconds(10));
            auto deleteTier = ttl->GetTiers()[1];
            UNIT_ASSERT(std::holds_alternative<TTtlDeleteAction>(deleteTier.GetAction()));
            UNIT_ASSERT_VALUES_EQUAL(std::get<TDateTypeColumnModeSettings>(deleteTier.GetExpression()).GetExpireAfter(), TDuration::Hours(1));
        }
        {
            auto alterQuery = TStringBuilder() << "ALTER TABLE `" << testTable.GetName() <<  R"(` RESET (TTL);)";
            auto alterResult = testHelper.GetSession().ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }
        {
            auto settings = TDescribeTableSettings().WithTableStatistics(true);
            auto describeResult = testHelper.GetSession().DescribeTable("/Root/ColumnTableTest", settings).GetValueSync();
            UNIT_ASSERT_C(describeResult.IsSuccess(), describeResult.GetIssues().ToString());

            const auto& description = describeResult.GetTableDescription();
            UNIT_ASSERT(!description.GetTtlSettings());
        }
    }

    Y_UNIT_TEST(TtlRunInterval) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().ExtractValueSync().GetSession();

        const auto ttl = TTtlSettings("Ts", TDuration::Zero())
            .SetRunInterval(TDuration::Minutes(30));

        // create with ttl
        {
            auto result = session.CreateTable("/Root/table", TTableBuilder()
                .AddNullableColumn("Key", EPrimitiveType::Uint64)
                .AddNullableColumn("Ts", EPrimitiveType::Timestamp)
                .SetPrimaryKeyColumn("Key")
                .SetTtlSettings(ttl)
                .Build()
            ).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = session.DescribeTable("/Root/table").ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetTableDescription().GetTtlSettings()->GetRunInterval(), ttl.GetRunInterval());
        }

        {
            auto result = session.AlterTable("/Root/table", TAlterTableSettings()
                .BeginAlterTtlSettings()
                    .Drop()
                .EndAlterTtlSettings()
            ).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        // alter table set ttl
        {
            auto result = session.AlterTable("/Root/table", TAlterTableSettings()
                .BeginAlterTtlSettings()
                    .Set(ttl)
                .EndAlterTtlSettings()
            ).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = session.DescribeTable("/Root/table").ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetTableDescription().GetTtlSettings()->GetRunInterval(), ttl.GetRunInterval());
        }
    }

    Y_UNIT_TEST(AddColumn) {
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper testHelper(runnerSettings);

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("resource_id").SetType(NScheme::NTypeIds::Utf8),
            TTestHelper::TColumnSchema().SetName("level").SetType(NScheme::NTypeIds::Int32)
        };

        Tests::NCommon::TLoggerInit(testHelper.GetKikimr()).Initialize();
        TTestHelper::TColumnTable testTable;

        testTable.SetName("/Root/ColumnTableTest").SetPrimaryKey({"id"}).SetSharding({"id"}).SetSchema(schema);
        testHelper.CreateTable(testTable);

        {
            TTestHelper::TUpdatesBuilder tableInserter(testTable.GetArrowSchema(schema));
            tableInserter.AddRow().Add(1).Add("test_res_1").AddNull();
            tableInserter.AddRow().Add(2).Add("test_res_2").Add(123);
            testHelper.BulkUpsert(testTable, tableInserter);
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
        Tests::NCommon::TLoggerInit(testHelper.GetKikimr()).Initialize();
        {
            TTestHelper::TUpdatesBuilder tableInserter(testTable.GetArrowSchema(schema));
            tableInserter.AddRow().Add(3).Add("test_res_3").Add(123).Add<uint64_t>(200);
            testHelper.BulkUpsert(testTable, tableInserter);
        }

        testHelper.ReadData("SELECT * FROM `/Root/ColumnTableTest` WHERE id=3", "[[3;[123];[200u];[\"test_res_3\"]]]");
        testHelper.ReadData("SELECT * FROM `/Root/ColumnTableTest` WHERE new_column=200", "[[3;[123];[200u];[\"test_res_3\"]]]");
        testHelper.ReadData("SELECT new_column FROM `/Root/ColumnTableTest` WHERE id=3", "[[[200u]]]");
        testHelper.ReadData("SELECT resource_id FROM `/Root/ColumnTableTest` WHERE id=3", "[[[\"test_res_3\"]]]");
        testHelper.ReadData("SELECT new_column FROM `/Root/ColumnTableTest`", "[[#];[#];[[200u]]]");
    }

    Y_UNIT_TEST(AddColumnOldSchemeBulkUpsert) {
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
            testHelper.BulkUpsert(testTable, tableInserter);
        }
        testHelper.ReadData("SELECT * FROM `/Root/ColumnTableTest` WHERE id=1", "[[1;#;#;[\"test_res_1\"]]]");
    }

/*
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
*/
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
            testHelper.BulkUpsert(testTable, tableInserter);
        }

        testHelper.ReadData("SELECT * FROM `/Root/TableStoreTest/ColumnTableTest` WHERE id=1", "[[1;#;[\"test_res_1\"]]]");

        {
            schema.push_back(TTestHelper::TColumnSchema().SetName("new_column").SetType(NScheme::NTypeIds::Uint64));
            auto alterQuery = TStringBuilder() << "ALTER TABLESTORE `" << testTableStore.GetName() << "` ADD COLUMN new_column Uint64;";

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
            testHelper.BulkUpsert(testTable, tableInserter);
        }

        testHelper.ReadData("SELECT * FROM `/Root/TableStoreTest/ColumnTableTest` WHERE id=3", "[[3;[123];[200u];[\"test_res_3\"]]]");
        testHelper.ReadData("SELECT new_column FROM `/Root/TableStoreTest/ColumnTableTest` WHERE id=3", "[[[200u]]]");
        testHelper.ReadData("SELECT resource_id FROM `/Root/TableStoreTest/ColumnTableTest` WHERE id=3", "[[[\"test_res_3\"]]]");
        testHelper.ReadData("SELECT new_column FROM `/Root/TableStoreTest/ColumnTableTest`", "[[#];[#];[[200u]]]");

        testHelper.RebootTablets(testTable.GetName());
        testHelper.ReadData("SELECT new_column FROM `/Root/TableStoreTest/ColumnTableTest`", "[[#];[#];[[200u]]]");
    }

    Y_UNIT_TEST(AddPgColumnWithStore) {
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper testHelper(runnerSettings);

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("resource_id").SetType(NScheme::NTypeIds::Utf8),
            TTestHelper::TColumnSchema().SetName("level").SetTypeInfo({NPg::TypeDescFromPgTypeName("pgint4")})
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
            tableInserter.AddRow().Add(2).Add("test_res_2").Add(-123);
            testHelper.BulkUpsert(testTable, tableInserter);
        }

        testHelper.ReadData("SELECT * FROM `/Root/TableStoreTest/ColumnTableTest` WHERE id=1", "[[1;#;[\"test_res_1\"]]]");

        {
            schema.push_back(TTestHelper::TColumnSchema().SetName("new_column").SetTypeInfo({NPg::TypeDescFromPgTypeName("pgfloat4")}));
            auto alterQuery = TStringBuilder() << "ALTER TABLESTORE `" << testTableStore.GetName() << "` ADD COLUMN new_column pgfloat4;";

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
            tableInserter.AddRow().Add(3).Add("test_res_3").Add(-321).Add(-3.14f);
            testHelper.BulkUpsert(testTable, tableInserter);
        }

        testHelper.ReadData("SELECT * FROM `/Root/TableStoreTest/ColumnTableTest` WHERE id=3", "[[3;\"-321\";\"-3.14\";[\"test_res_3\"]]]");
        testHelper.ReadData("SELECT new_column FROM `/Root/TableStoreTest/ColumnTableTest` WHERE id=3", "[[\"-3.14\"]]");
        testHelper.ReadData("SELECT resource_id FROM `/Root/TableStoreTest/ColumnTableTest` WHERE id=3", "[[[\"test_res_3\"]]]");
        testHelper.ReadData("SELECT new_column FROM `/Root/TableStoreTest/ColumnTableTest` ORDER BY new_column", "[[#];[#];[\"-3.14\"]]");

        testHelper.RebootTablets(testTable.GetName());
        testHelper.ReadData("SELECT new_column FROM `/Root/TableStoreTest/ColumnTableTest` ORDER BY new_column", "[[#];[#];[\"-3.14\"]]");
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
            auto alterQuery = TStringBuilder() << "ALTER TABLE `" << testTable.GetName() << "`ADD COLUMN new_column Uint64 NOT NULL;";
            auto alterResult = testHelper.GetSession().ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), EStatus::BAD_REQUEST, alterResult.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(NullColumnError) {
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper testHelper(runnerSettings);

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("resource_id").SetType(NScheme::NTypeIds::Utf8).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("level").SetType(NScheme::NTypeIds::Int32).SetNullable(false)
        };
        TTestHelper::TColumnTable testTable;
        testTable.SetName("/Root/ColumnTableTest").SetPrimaryKey({"id"}).SetSharding({"id"}).SetSchema(schema);
        testHelper.CreateTable(testTable);

        TVector<TTestHelper::TColumnSchema> schemaWithNull = {
            TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("resource_id").SetType(NScheme::NTypeIds::Utf8).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("level").SetType(NScheme::NTypeIds::Int32)
        };
        {
            TTestHelper::TUpdatesBuilder tableInserter(testTable.GetArrowSchema(schemaWithNull));
            tableInserter.AddRow().Add(1).Add("test_res_1").AddNull();
            tableInserter.AddRow().Add(2).Add("test_res_2").Add(123);
            testHelper.BulkUpsert(testTable, tableInserter, Ydb::StatusIds::BAD_REQUEST);
        }
        {
            TTestHelper::TUpdatesBuilder tableInserter(testTable.GetArrowSchema(schemaWithNull));
            tableInserter.AddRow().Add(1).Add("test_res_1").AddNull();
            tableInserter.AddRow().Add(2).Add("test_res_2").Add(123);
            testHelper.BulkUpsert(testTable, tableInserter, Ydb::StatusIds::BAD_REQUEST);
        }

        testHelper.ReadData("SELECT * FROM `/Root/ColumnTableTest` WHERE id=1", "[]");
    }

    Y_UNIT_TEST(BulkError) {
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper testHelper(runnerSettings);

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("level").SetType(NScheme::NTypeIds::Uuid).SetNullable(true)
        };
        TTestHelper::TColumnTable testTable;
        testTable.SetName("/Root/ColumnTableTest").SetPrimaryKey({"id"}).SetSharding({"id"}).SetSchema(schema);
        testHelper.CreateTable(testTable, NYdb::EStatus::SCHEME_ERROR);
    }
    Y_UNIT_TEST(DropColumn) {
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
            testHelper.BulkUpsert(testTable, tableInserter);
        }
        testHelper.ReadData("SELECT * FROM `/Root/ColumnTableTest` WHERE id=1", "[[1;#;[\"test_res_1\"]]]");
        {
            auto alterQuery = TStringBuilder() << "ALTER TABLE `" << testTable.GetName() << "`DROP COLUMN resource_id;";
            auto alterResult = testHelper.GetSession().ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }
        testHelper.ReadData("SELECT * FROM `/Root/ColumnTableTest` WHERE id=1", "[[1;#]]");
        testHelper.ReadData("SELECT resource_id FROM `/Root/ColumnTableTest` ", "[[];[]]", EStatus::GENERIC_ERROR);
        {
            auto alterQuery = TStringBuilder() << "ALTER TABLE `" << testTable.GetName() << "`DROP COLUMN level;";
            auto alterResult = testHelper.GetSession().ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }
        testHelper.ReadData("SELECT * FROM `/Root/ColumnTableTest` ", "[[1];[2]]");
    }
/*
    Y_UNIT_TEST(DropColumnOnSchemeChange) {
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
                auto alterQuery = TStringBuilder() << "ALTER TABLE `" << testTable.GetName() << "`DROP COLUMN resource_id;";
                auto alterResult = testHelper.GetSession().ExecuteSchemeQuery(alterQuery).GetValueSync();
            });
        }
        testHelper.ReadData("SELECT * FROM `/Root/ColumnTableTest` WHERE id=1", "[[1;#]]");
    }
*/

    Y_UNIT_TEST(DropColumnOldSchemeBulkUpsert) {
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
            auto alterQuery = TStringBuilder() << "ALTER TABLE `" << testTable.GetName() << "`DROP COLUMN resource_id;";
            auto alterResult = testHelper.GetSession().ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }
        {
            TTestHelper::TUpdatesBuilder tableInserter(testTable.GetArrowSchema(schema));
            tableInserter.AddRow().Add(1).Add("test_res_1").AddNull();
            testHelper.BulkUpsert(testTable, tableInserter, Ydb::StatusIds::SCHEME_ERROR);
        }
    }

    Y_UNIT_TEST(DropColumnAfterAdd) {
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
            testHelper.BulkUpsert(testTable, tableInserter);
        }
        testHelper.ReadData("SELECT * FROM `/Root/ColumnTableTest` WHERE id=1", "[[1;#;[\"test_res_1\"]]]");
        {
            schema.push_back(TTestHelper::TColumnSchema().SetName("new_column").SetType(NScheme::NTypeIds::Uint64));
            auto alterQuery = TStringBuilder() << "ALTER TABLE `" << testTable.GetName() << "` ADD COLUMN new_column Uint64;";
            auto alterResult = testHelper.GetSession().ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }
        testHelper.ReadData("SELECT * FROM `/Root/ColumnTableTest` WHERE id=1", "[[1;#;#;[\"test_res_1\"]]]");
        {
            auto alterQuery = TStringBuilder() << "ALTER TABLE `" << testTable.GetName() << "`DROP COLUMN new_column;";
            auto alterResult = testHelper.GetSession().ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }
        testHelper.ReadData("SELECT * FROM `/Root/ColumnTableTest` WHERE id=1", "[[1;#;[\"test_res_1\"]]]");
    }

    void TestDropThenAddColumn(bool enableIndexation, bool enableCompaction) {
        if (enableCompaction) {
            Y_ABORT_UNLESS(enableIndexation);
        }

        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();
        csController->DisableBackground(NYDBTest::ICSController::EBackground::Indexation);
        csController->DisableBackground(NYDBTest::ICSController::EBackground::Compaction);

        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper testHelper(runnerSettings);

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("value").SetType(NScheme::NTypeIds::Utf8),
        };

        TTestHelper::TColumnTable testTable;
        testTable.SetName("/Root/ColumnTableTest").SetPrimaryKey({ "id" }).SetSharding({ "id" }).SetSchema(schema);
        testHelper.CreateTable(testTable);

        {
            TTestHelper::TUpdatesBuilder tableInserter(testTable.GetArrowSchema(schema));
            tableInserter.AddRow().Add(1).Add("test_res_1");
            tableInserter.AddRow().Add(2).Add("test_res_2");
            testHelper.BulkUpsert(testTable, tableInserter);
        }

        if (enableCompaction) {
            csController->EnableBackground(NYDBTest::ICSController::EBackground::Indexation);
            csController->EnableBackground(NYDBTest::ICSController::EBackground::Compaction);
            csController->WaitIndexation(TDuration::Seconds(5));
            csController->WaitCompactions(TDuration::Seconds(5));
            csController->DisableBackground(NYDBTest::ICSController::EBackground::Indexation);
            csController->DisableBackground(NYDBTest::ICSController::EBackground::Compaction);
        }

        {
            auto alterQuery = TStringBuilder() << "ALTER TABLE `" << testTable.GetName() << "` DROP COLUMN value;";
            auto alterResult = testHelper.GetSession().ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }
        {
            auto alterQuery = TStringBuilder() << "ALTER TABLE `" << testTable.GetName() << "` ADD COLUMN value Uint64;";
            auto alterResult = testHelper.GetSession().ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }
        schema.back().SetType(NScheme::NTypeIds::Uint64);

        {
            TTestHelper::TUpdatesBuilder tableInserter(testTable.GetArrowSchema(schema));
            tableInserter.AddRow().Add(3).Add(42);
            tableInserter.AddRow().Add(4).Add(43);
            testHelper.BulkUpsert(testTable, tableInserter);
        }

        if (enableIndexation) {
            csController->EnableBackground(NYDBTest::ICSController::EBackground::Indexation);
            csController->WaitIndexation(TDuration::Seconds(5));
        }
        if (enableCompaction) {
            csController->EnableBackground(NYDBTest::ICSController::EBackground::Compaction);
            csController->WaitCompactions(TDuration::Seconds(5));
        }

        testHelper.ReadData("SELECT value FROM `/Root/ColumnTableTest` ORDER BY value", "[[#];[#];[[42u]];[[43u]]]");
    }

    Y_UNIT_TEST(DropThenAddColumn) {
        TestDropThenAddColumn(false, false);
    }

    Y_UNIT_TEST(DropThenAddColumnIndexation) {
        TestDropThenAddColumn(true, true);
    }

    Y_UNIT_TEST(DropThenAddColumnCompaction) {
        TestDropThenAddColumn(true, true);
    }

    Y_UNIT_TEST(DropTtlColumn) {
        auto runnerSettings = TKikimrSettings()
            .SetColumnShardAlterObjectEnabled(true)
            .SetWithSampleTables(false);
        TTestHelper testHelper(runnerSettings);

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("id_second").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("level").SetType(NScheme::NTypeIds::Int32),
            TTestHelper::TColumnSchema().SetName("created_at").SetType(NScheme::NTypeIds::Timestamp).SetNullable(false)
        };

        TTestHelper::TColumnTable testTable;
        testTable.SetName("/Root/ColumnTableTest").SetPrimaryKey({"id"}).SetSharding({"id"}).SetSchema(schema);
        testHelper.CreateTable(testTable);
        {
            auto alterQuery = TStringBuilder() << R"(
            --!syntax_v1
            ALTER OBJECT `)" << testTable.GetName() << R"(` (TYPE TABLE) SET (ACTION=UPSERT_INDEX,
                NAME=max_pk_int, TYPE=MAX, FEATURES=`{\"column_name\": \"created_at\"}`))";
            auto alterResult = testHelper.GetSession().ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }

        {
            auto alterQuery = TStringBuilder() << "ALTER TABLE `" << testTable.GetName() << "`SET (TTL = Interval(\"PT1H\") ON created_at);";
            auto alterResult = testHelper.GetSession().ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }

        {
            auto alterQuery = TStringBuilder() << "ALTER TABLE `" << testTable.GetName() << "`DROP COLUMN created_at;";
            auto alterResult = testHelper.GetSession().ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), EStatus::SCHEME_ERROR, alterResult.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(DropColumnErrors) {
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
            auto alterQuery = TStringBuilder() << "ALTER TABLE `" << testTable.GetName() << "`DROP COLUMN unknown_column;";
            auto alterResult = testHelper.GetSession().ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), EStatus::GENERIC_ERROR, alterResult.GetIssues().ToString());
        }

        {
            auto alterQuery = TStringBuilder() << "ALTER TABLE `" << testTable.GetName() << "`DROP COLUMN id;";
            auto alterResult = testHelper.GetSession().ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), EStatus::GENERIC_ERROR, alterResult.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(DropColumnTableStoreErrors) {
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
            auto alterQuery = TStringBuilder() << "ALTER TABLESTORE `" << testTableStore.GetName() << "`DROP COLUMN id;";
            auto alterResult = testHelper.GetSession().ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), EStatus::SCHEME_ERROR, alterResult.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(TenThousandColumns) {
        using namespace NArrow;

        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper testHelper(runnerSettings);

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Uint64).SetNullable(false)
        };

        for (ui64 i = 0; i < 10000; ++i) {
            schema.emplace_back(TTestHelper::TColumnSchema().SetName("column" + ToString(i)).SetType(NScheme::NTypeIds::Int32).SetNullable(true));
        }

        TTestHelper::TColumnTable testTable;
        testTable.SetName("/Root/ColumnTableTest").SetPrimaryKey({"id"}).SetSharding({"id"}).SetSchema(schema);
        testHelper.CreateTable(testTable);

        TVector<NConstruction::IArrayBuilder::TPtr> dataBuilders;
        dataBuilders.push_back(NConstruction::TSimpleArrayConstructor<NConstruction::TIntSeqFiller<arrow::UInt64Type>>::BuildNotNullable("id", false));
        for (ui64 i = 1; i < schema.size(); ++i) {
            dataBuilders.push_back(std::make_shared<NConstruction::TSimpleArrayConstructor<NConstruction::TIntSeqFiller<arrow::Int32Type>>>(schema[i].GetName()));
        }
        auto batch = NConstruction::TRecordBatchConstructor(dataBuilders).BuildBatch(10000);
        testHelper.BulkUpsert(testTable, batch);

        testHelper.ReadData("SELECT COUNT(*) FROM `/Root/ColumnTableTest`", "[[10000u]]");

        for (ui64 i = 10000; i < 10100; ++i) {
            auto alterQuery = TStringBuilder() << "ALTER TABLE `" << testTable.GetName() << "` ADD COLUMN column" << i << " Uint64;";
            Cerr << alterQuery << Endl;
            auto alterResult = testHelper.GetSession().ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }
        testHelper.ReadData("SELECT COUNT(*) FROM `/Root/ColumnTableTest`", "[[10000u]]");
    }

    Y_UNIT_TEST(NullKeySchema) {
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper testHelper(runnerSettings);

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int32).SetNullable(true),
            TTestHelper::TColumnSchema().SetName("resource_id").SetType(NScheme::NTypeIds::Utf8).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("level").SetType(NScheme::NTypeIds::Int32).SetNullable(false)
        };
        TTestHelper::TColumnTableStore testTableStore;
        testTableStore.SetName("/Root/TableStoreTest").SetPrimaryKey({"id"}).SetSchema(schema);
        testHelper.CreateTable(testTableStore, EStatus::SCHEME_ERROR);

        TTestHelper::TColumnTable testTable;
        testTable.SetName("/Root/ColumnTableTest").SetPrimaryKey({"id"}).SetSchema(schema);
        testHelper.CreateTable(testTable, EStatus::SCHEME_ERROR);
    }

    Y_UNIT_TEST(DropColumnAfterInsert) {
        using namespace NArrow;

        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();
        csController->DisableBackground(NYDBTest::ICSController::EBackground::Indexation);

        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper testHelper(runnerSettings);

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Uint64).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("int_column").SetType(NScheme::NTypeIds::Int32).SetNullable(true)
        };

        TTestHelper::TColumnTable testTable;
        testTable.SetName("/Root/ColumnTableTest").SetPrimaryKey({ "id" }).SetSchema(schema);
        testHelper.CreateTable(testTable);

        TVector<NConstruction::IArrayBuilder::TPtr> dataBuilders;
        dataBuilders.push_back(
            NConstruction::TSimpleArrayConstructor<NConstruction::TIntSeqFiller<arrow::UInt64Type>>::BuildNotNullable("id", false));
        dataBuilders.push_back(
            std::make_shared<NConstruction::TSimpleArrayConstructor<NConstruction::TIntSeqFiller<arrow::Int32Type>>>("int_column"));
        auto batch = NConstruction::TRecordBatchConstructor(dataBuilders).BuildBatch(100);
        testHelper.BulkUpsert(testTable, batch);

        auto alterQueryAdd = TStringBuilder() << "ALTER TABLE `" << testTable.GetName() << "` DROP COLUMN int_column;";
        auto alterAddResult = testHelper.GetSession().ExecuteSchemeQuery(alterQueryAdd).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(alterAddResult.GetStatus(), EStatus::SUCCESS, alterAddResult.GetIssues().ToString());

        csController->EnableBackground(NYDBTest::ICSController::EBackground::Indexation);
        csController->WaitIndexation(TDuration::Seconds(5));
    }

    void TestInsertAddInsertDrop(
        bool autoIndexation, bool indexationAfterInsertAddColumn, bool indexationAfterInsertDropColumn, bool indexationInEnd) {
        using namespace NArrow;

        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();
        if (!autoIndexation) {
            csController->DisableBackground(NYDBTest::ICSController::EBackground::Indexation);
        }

        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper testHelper(runnerSettings);

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Uint64).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("int_column").SetType(NScheme::NTypeIds::Int32).SetNullable(true)
        };

        TTestHelper::TColumnTable testTable;
        testTable.SetName("/Root/ColumnTableTest").SetPrimaryKey({ "id" }).SetSchema(schema);
        testHelper.CreateTable(testTable);

        TVector<NConstruction::IArrayBuilder::TPtr> dataBuilders;
        dataBuilders.push_back(
            NConstruction::TSimpleArrayConstructor<NConstruction::TIntSeqFiller<arrow::UInt64Type>>::BuildNotNullable("id", false));
        dataBuilders.push_back(
            std::make_shared<NConstruction::TSimpleArrayConstructor<NConstruction::TIntSeqFiller<arrow::Int32Type>>>("int_column"));
        auto batch = NConstruction::TRecordBatchConstructor(dataBuilders).BuildBatch(100);

        for (ui32 i = 0; i < 5; i++) {
            testHelper.BulkUpsert(testTable, batch);
            auto alterQueryAdd = TStringBuilder() << "ALTER TABLE `" << testTable.GetName() << "` ADD COLUMN column" << i << " Uint64;";
            auto alterAddResult = testHelper.GetSession().ExecuteSchemeQuery(alterQueryAdd).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterAddResult.GetStatus(), EStatus::SUCCESS, alterAddResult.GetIssues().ToString());

            if (!autoIndexation && indexationAfterInsertAddColumn) {
                csController->EnableBackground(NYDBTest::ICSController::EBackground::Indexation);
                csController->WaitIndexation(TDuration::Seconds(5));
                csController->DisableBackground(NYDBTest::ICSController::EBackground::Indexation);
            }

            testHelper.BulkUpsert(testTable, batch);
            auto alterQueryDrop = TStringBuilder() << "ALTER TABLE `" << testTable.GetName() << "` DROP COLUMN column" << i << ";";
            auto alterDropResult = testHelper.GetSession().ExecuteSchemeQuery(alterQueryDrop).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterDropResult.GetStatus(), EStatus::SUCCESS, alterDropResult.GetIssues().ToString());

            if (!autoIndexation && indexationAfterInsertDropColumn) {
                csController->EnableBackground(NYDBTest::ICSController::EBackground::Indexation);
                csController->WaitIndexation(TDuration::Seconds(5));
                csController->DisableBackground(NYDBTest::ICSController::EBackground::Indexation);
            }
        }

        if (!autoIndexation && indexationInEnd) {
            csController->EnableBackground(NYDBTest::ICSController::EBackground::Indexation);
            csController->WaitIndexation(TDuration::Seconds(5));
        }
    }

    Y_UNIT_TEST(InsertAddInsertDrop) {
        TestInsertAddInsertDrop(true, false, false, false);
        for (i32 i = 0; i < 8; i++) {
            TestInsertAddInsertDrop(false, i & 1, i & 2, i & 3);
        }
    }

    Y_UNIT_TEST(DropTableAfterInsert) {
        using namespace NArrow;

        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();
        csController->DisableBackground(NYDBTest::ICSController::EBackground::Indexation);

        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper testHelper(runnerSettings);

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Uint64).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("int_column").SetType(NScheme::NTypeIds::Int32).SetNullable(true)
        };

        TTestHelper::TColumnTable testTable;
        testTable.SetName("/Root/ColumnTableTest").SetPrimaryKey({ "id" }).SetSchema(schema);
        testHelper.CreateTable(testTable);

        TVector<NConstruction::IArrayBuilder::TPtr> dataBuilders;
        dataBuilders.push_back(
            NConstruction::TSimpleArrayConstructor<NConstruction::TIntSeqFiller<arrow::UInt64Type>>::BuildNotNullable("id", false));
        dataBuilders.push_back(
            std::make_shared<NConstruction::TSimpleArrayConstructor<NConstruction::TIntSeqFiller<arrow::Int32Type>>>("int_column"));
        auto batch = NConstruction::TRecordBatchConstructor(dataBuilders).BuildBatch(100);

        testHelper.BulkUpsert(testTable, batch);

        auto alterQueryDrop = TStringBuilder() << "DROP TABLE `" << testTable.GetName() << "`;";
        auto alterDropResult = testHelper.GetSession().ExecuteSchemeQuery(alterQueryDrop).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(alterDropResult.GetStatus(), EStatus::SUCCESS, alterDropResult.GetIssues().ToString());

        csController->EnableBackground(NYDBTest::ICSController::EBackground::Indexation);
        csController->WaitIndexation(TDuration::Seconds(5));
    }

    Y_UNIT_TEST(InsertDropAddColumn) {
        using namespace NArrow;

        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();
        csController->DisableBackground(NYDBTest::ICSController::EBackground::Indexation);

        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper testHelper(runnerSettings);

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Uint64).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("int_column").SetType(NScheme::NTypeIds::Int32).SetNullable(true)
        };

        TTestHelper::TColumnTable testTable;
        testTable.SetName("/Root/ColumnTableTest").SetPrimaryKey({ "id" }).SetSchema(schema);
        testHelper.CreateTable(testTable);

        TVector<NConstruction::IArrayBuilder::TPtr> dataBuilders;
        dataBuilders.push_back(
            NConstruction::TSimpleArrayConstructor<NConstruction::TIntSeqFiller<arrow::UInt64Type>>::BuildNotNullable("id", false));
        dataBuilders.push_back(
            std::make_shared<NConstruction::TSimpleArrayConstructor<NConstruction::TIntSeqFiller<arrow::Int32Type>>>("int_column"));
        auto batch = NConstruction::TRecordBatchConstructor(dataBuilders).BuildBatch(100);

        testHelper.BulkUpsert(testTable, batch);

        auto alterQueryDrop = TStringBuilder() << "ALTER TABLE `" << testTable.GetName() << "` DROP COLUMN int_column;";
        auto alterDropResult = testHelper.GetSession().ExecuteSchemeQuery(alterQueryDrop).GetValueSync();

        auto alterQueryAdd = TStringBuilder() << "ALTER TABLE `" << testTable.GetName() << "` ADD COLUMN int_column Int32;";
        auto alterAddResult = testHelper.GetSession().ExecuteSchemeQuery(alterQueryAdd).GetValueSync();

        csController->EnableBackground(NYDBTest::ICSController::EBackground::Indexation);
        csController->WaitIndexation(TDuration::Seconds(5));
    }

    Y_UNIT_TEST(CreateWithoutColumnFamily) {
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper testHelper(TKikimrSettings().SetWithSampleTables(false));

        TString tableName = "/Root/TableWithoutColumnFamily";
        {
            TVector<TTestHelper::TColumnSchema> schema = {
                TTestHelper::TColumnSchema().SetName("Key").SetType(NScheme::NTypeIds::Uint64).SetNullable(false),
                TTestHelper::TColumnSchema().SetName("Value1").SetType(NScheme::NTypeIds::String).SetNullable(true),
                TTestHelper::TColumnSchema().SetName("Value2").SetType(NScheme::NTypeIds::Uint32).SetNullable(true)
            };

            TTestHelper::TColumnTable testTable;
            testTable.SetName(tableName).SetPrimaryKey({ "Key" }).SetSchema(schema);
            testHelper.CreateTable(testTable);
        }

        auto& runner = testHelper.GetKikimr();
        auto tableClient = runner.GetTableClient();
        auto session = tableClient.CreateSession().GetValueSync().GetSession();

        auto runtime = runner.GetTestServer().GetRuntime();
        TActorId sender = runtime->AllocateEdgeActor();

        auto describeResult = DescribeTable(&runner.GetTestServer(), sender, tableName);
        auto schema = describeResult.GetPathDescription().GetColumnTableDescription().GetSchema();
        TTestHelper::TColumnFamily defaultFamily = TTestHelper::TColumnFamily().SetId(0).SetFamilyName("default");

        UNIT_ASSERT_EQUAL(schema.ColumnFamiliesSize(), 1);
        TTestHelper::TColumnFamily defaultFromScheme;
        UNIT_ASSERT(defaultFromScheme.DeserializeFromProto(schema.GetColumnFamilies(0)));
        {
            TString errorMessage;
            UNIT_ASSERT_C(defaultFromScheme.IsEqual(defaultFamily, errorMessage), errorMessage);
        }
    }

    // Field `Data` is not used in ColumnFamily for ColumnTable
    Y_UNIT_TEST(ColumnFamilyWithFieldData) {
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper testHelper(TKikimrSettings().SetWithSampleTables(false));
        TString tableName = "/Root/TableWithoutColumnFamily";

        {
            TTestHelper::TCompression plainCompression =
                TTestHelper::TCompression().SetCompressionType(NKikimrSchemeOp::EColumnCodec::ColumnCodecPlain);
            TTestHelper::TColumnFamily defaultFamily =
                TTestHelper::TColumnFamily().SetId(0).SetData("test").SetFamilyName("default").SetCompression(plainCompression);

            TVector<TTestHelper::TColumnSchema> schema = {
                TTestHelper::TColumnSchema().SetName("Key").SetType(NScheme::NTypeIds::Uint64).SetNullable(false),
                TTestHelper::TColumnSchema().SetName("Value1").SetType(NScheme::NTypeIds::String).SetNullable(true),
                TTestHelper::TColumnSchema().SetName("Value2").SetType(NScheme::NTypeIds::Uint32).SetNullable(true)
            };

            TTestHelper::TColumnTable testTable;
            testTable.SetName(tableName).SetPrimaryKey({ "Key" }).SetSchema(schema).SetColumnFamilies({ defaultFamily });
            testHelper.CreateTable(testTable, EStatus::GENERIC_ERROR);
        }

        {
            TTestHelper::TCompression plainCompression =
                TTestHelper::TCompression().SetCompressionType(NKikimrSchemeOp::EColumnCodec::ColumnCodecPlain);
            TTestHelper::TColumnFamily defaultFamily =
                TTestHelper::TColumnFamily().SetId(0).SetFamilyName("default").SetCompression(plainCompression);

            TTestHelper::TCompression lz4Compression =
                TTestHelper::TCompression().SetCompressionType(NKikimrSchemeOp::EColumnCodec::ColumnCodecLZ4);
            TTestHelper::TColumnFamily family1 =
                TTestHelper::TColumnFamily().SetId(1).SetData("test").SetFamilyName("family1").SetCompression(lz4Compression);

            TVector<TTestHelper::TColumnSchema> schema = {
                TTestHelper::TColumnSchema().SetName("Key").SetType(NScheme::NTypeIds::Uint64).SetNullable(false),
                TTestHelper::TColumnSchema()
                    .SetName("Value1")
                    .SetType(NScheme::NTypeIds::String)
                    .SetNullable(true)
                    .SetColumnFamilyName(family1.GetFamilyName()),
                TTestHelper::TColumnSchema().SetName("Value2").SetType(NScheme::NTypeIds::Uint32).SetNullable(true)
            };

            TTestHelper::TColumnTable testTable;
            testTable.SetName(tableName).SetPrimaryKey({ "Key" }).SetSchema(schema).SetColumnFamilies({ defaultFamily, family1 });
            testHelper.CreateTable(testTable, EStatus::GENERIC_ERROR);
        }

        {
            TTestHelper::TCompression plainCompression =
                TTestHelper::TCompression().SetCompressionType(NKikimrSchemeOp::EColumnCodec::ColumnCodecPlain);
            TTestHelper::TColumnFamily defaultFamily =
                TTestHelper::TColumnFamily().SetId(0).SetFamilyName("default").SetCompression(plainCompression);

            TTestHelper::TCompression lz4Compression =
                TTestHelper::TCompression().SetCompressionType(NKikimrSchemeOp::EColumnCodec::ColumnCodecLZ4);
            TTestHelper::TColumnFamily family1 = TTestHelper::TColumnFamily().SetId(1).SetFamilyName("family1").SetCompression(lz4Compression);

            TVector<TTestHelper::TColumnSchema> schema = {
                TTestHelper::TColumnSchema().SetName("Key").SetType(NScheme::NTypeIds::Uint64).SetNullable(false),
                TTestHelper::TColumnSchema()
                    .SetName("Value1")
                    .SetType(NScheme::NTypeIds::String)
                    .SetNullable(true)
                    .SetColumnFamilyName(family1.GetFamilyName()),
                TTestHelper::TColumnSchema().SetName("Value2").SetType(NScheme::NTypeIds::Uint32).SetNullable(true)
            };

            TTestHelper::TColumnTable testTable;
            testTable.SetName(tableName).SetPrimaryKey({ "Key" }).SetSchema(schema).SetColumnFamilies({ defaultFamily, family1 });
            testHelper.CreateTable(testTable);

            auto query = TStringBuilder() << R"(ALTER TABLE `)" << tableName << R"(`
                    ALTER FAMILY ")" << family1.GetFamilyName()
                                          << R"( SET COMPRESSION "lz4";)";
            auto session = testHelper.GetSession();
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(CreateWithDefaultColumnFamily) {
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper testHelper(TKikimrSettings().SetWithSampleTables(false));

        TString tableName = "/Root/TableWithDefaultColumnFamily";
        TTestHelper::TCompression zstdCompression =
            TTestHelper::TCompression().SetCompressionType(NKikimrSchemeOp::EColumnCodec::ColumnCodecZSTD).SetCompressionLevel(5);

        TVector<TTestHelper::TColumnFamily> families = {
            TTestHelper::TColumnFamily().SetId(0).SetFamilyName("default").SetCompression(zstdCompression),
        };

        {
            TVector<TTestHelper::TColumnSchema> schema = {
                TTestHelper::TColumnSchema().SetName("Key").SetType(NScheme::NTypeIds::Uint64).SetNullable(false),
                TTestHelper::TColumnSchema().SetName("Value1").SetType(NScheme::NTypeIds::String).SetNullable(true),
                TTestHelper::TColumnSchema().SetName("Value2").SetType(NScheme::NTypeIds::Uint32).SetNullable(true)
            };

            TTestHelper::TColumnTable testTable;
            testTable.SetName(tableName).SetPrimaryKey({ "Key" }).SetSchema(schema).SetColumnFamilies(families);
            testHelper.CreateTable(testTable);
        }

        auto& runner = testHelper.GetKikimr();
        auto runtime = runner.GetTestServer().GetRuntime();
        TActorId sender = runtime->AllocateEdgeActor();
        auto describeResult = DescribeTable(&runner.GetTestServer(), sender, tableName);
        auto schema = describeResult.GetPathDescription().GetColumnTableDescription().GetSchema();

        UNIT_ASSERT_EQUAL(schema.ColumnFamiliesSize(), families.size());
        TTestHelper::TColumnFamily defaultFromScheme;
        UNIT_ASSERT(defaultFromScheme.DeserializeFromProto(schema.GetColumnFamilies(0)));
        {
            TString errorMessage;
            UNIT_ASSERT_C(defaultFromScheme.IsEqual(families[0], errorMessage), errorMessage);
        }

        for (const auto& column : schema.GetColumns()) {
            UNIT_ASSERT(column.HasSerializer());
            UNIT_ASSERT_EQUAL_C(
                column.GetColumnFamilyId(), 0, TStringBuilder() << "family for column " << column.GetName() << " is not default");
            TTestHelper::TCompression compression;
            UNIT_ASSERT(compression.DeserializeFromProto(column.GetSerializer()));
            TString errorMessage;
            UNIT_ASSERT_C(compression.IsEqual(families[0].GetCompression(), errorMessage), errorMessage);
        }
    }

    Y_UNIT_TEST(CrateWithWrongCodec) {
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper testHelper(TKikimrSettings().SetWithSampleTables(false));

        TString tableName = "/Root/TableWithWrongCodec";
        TTestHelper::TCompression zstdCompression =
            TTestHelper::TCompression().SetCompressionType(NKikimrSchemeOp::EColumnCodec::ColumnCodecZSTD).SetCompressionLevel(100);

        TVector<TTestHelper::TColumnFamily> families = {
            TTestHelper::TColumnFamily().SetId(0).SetFamilyName("default").SetCompression(zstdCompression),
        };

        {
            TVector<TTestHelper::TColumnSchema> schema = {
                TTestHelper::TColumnSchema().SetName("Key").SetType(NScheme::NTypeIds::Uint64).SetNullable(false),
                TTestHelper::TColumnSchema().SetName("Value1").SetType(NScheme::NTypeIds::String).SetNullable(true),
                TTestHelper::TColumnSchema().SetName("Value2").SetType(NScheme::NTypeIds::Uint32).SetNullable(true)
            };

            TTestHelper::TColumnTable testTable;
            testTable.SetName(tableName).SetPrimaryKey({ "Key" }).SetSchema(schema).SetColumnFamilies(families);
            testHelper.CreateTable(testTable, EStatus::SCHEME_ERROR);
        }

        TTestHelper::TCompression lz4Compression =
            TTestHelper::TCompression().SetCompressionType(NKikimrSchemeOp::EColumnCodec::ColumnCodecLZ4).SetCompressionLevel(100);
        families[0].SetCompression(lz4Compression);
        {
            TVector<TTestHelper::TColumnSchema> schema = {
                TTestHelper::TColumnSchema().SetName("Key").SetType(NScheme::NTypeIds::Uint64).SetNullable(false),
                TTestHelper::TColumnSchema().SetName("Value1").SetType(NScheme::NTypeIds::String).SetNullable(true),
                TTestHelper::TColumnSchema().SetName("Value2").SetType(NScheme::NTypeIds::Uint32).SetNullable(true)
            };

            TTestHelper::TColumnTable testTable;
            testTable.SetName(tableName).SetPrimaryKey({ "Key" }).SetSchema(schema).SetColumnFamilies(families);
            testHelper.CreateTable(testTable, EStatus::SCHEME_ERROR);
        }

        {
            auto session = testHelper.GetSession();
            auto createQuery = TStringBuilder() << R"(CREATE TABLE `)" << tableName << R"(` (
                Key Uint64 NOT NULL,
                Value1 String,
                Value2 Uint32,
                PRIMARY KEY (Key),
                FAMILY default (
                    COMPRESSION="snappy"
                )) WITH (STORE = COLUMN);)";
            auto result = session.ExecuteSchemeQuery(createQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(AlterCompressionType) {
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper testHelper(TKikimrSettings().SetWithSampleTables(false));

        TString tableName = "/Root/TableWithColumnFamily";
        TTestHelper::TCompression plainCompression =
            TTestHelper::TCompression().SetCompressionType(NKikimrSchemeOp::EColumnCodec::ColumnCodecPlain);
        TTestHelper::TCompression zstdCompression =
            TTestHelper::TCompression().SetCompressionType(NKikimrSchemeOp::EColumnCodec::ColumnCodecZSTD).SetCompressionLevel(3);
        TTestHelper::TCompression lz4Compression = TTestHelper::TCompression().SetCompressionType(NKikimrSchemeOp::EColumnCodec::ColumnCodecLZ4);

        TVector<TTestHelper::TColumnFamily> families = {
            TTestHelper::TColumnFamily().SetId(0).SetFamilyName("default").SetCompression(plainCompression),
            TTestHelper::TColumnFamily().SetId(1).SetFamilyName("family1").SetCompression(zstdCompression),
            TTestHelper::TColumnFamily().SetId(2).SetFamilyName("family2").SetCompression(lz4Compression),
        };

        {
            TVector<TTestHelper::TColumnSchema> schema = {
                TTestHelper::TColumnSchema().SetName("Key").SetType(NScheme::NTypeIds::Uint64).SetNullable(false),
                TTestHelper::TColumnSchema()
                    .SetName("Value1")
                    .SetType(NScheme::NTypeIds::String)
                    .SetNullable(true)
                    .SetColumnFamilyName(families[1].GetFamilyName()),
                TTestHelper::TColumnSchema()
                    .SetName("Value2")
                    .SetType(NScheme::NTypeIds::Uint32)
                    .SetNullable(true)
                    .SetColumnFamilyName(families[2].GetFamilyName())
            };

            TTestHelper::TColumnTable testTable;
            testTable.SetName(tableName).SetPrimaryKey({ "Key" }).SetSchema(schema).SetColumnFamilies(families);
            testHelper.CreateTable(testTable);
        }

        auto session = testHelper.GetSession();

        families[1].MutableCompression().SetCompressionType(NKikimrSchemeOp::EColumnCodec::ColumnCodecLZ4).SetCompressionLevel({});
        auto query = TStringBuilder() << R"(ALTER TABLE `)" << tableName << R"(`
                ALTER FAMILY family1 SET COMPRESSION "lz4";)";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto& runner = testHelper.GetKikimr();
        auto runtime = runner.GetTestServer().GetRuntime();
        TActorId sender = runtime->AllocateEdgeActor();
        auto describeResult = DescribeTable(&runner.GetTestServer(), sender, tableName);
        auto schema = describeResult.GetPathDescription().GetColumnTableDescription().GetSchema();

        UNIT_ASSERT_EQUAL(schema.ColumnFamiliesSize(), families.size());
        for (ui32 i = 0; i < families.size(); i++) {
            TTestHelper::TColumnFamily familyFromScheme;
            UNIT_ASSERT(familyFromScheme.DeserializeFromProto(schema.GetColumnFamilies(i)));
            TString errorMessage;
            UNIT_ASSERT_C(familyFromScheme.IsEqual(families[i], errorMessage), errorMessage);
        }

        auto columns = schema.GetColumns();
        for (ui32 i = 0; i < schema.ColumnsSize(); i++) {
            UNIT_ASSERT(columns[i].HasSerializer());
            UNIT_ASSERT_EQUAL_C(columns[i].GetColumnFamilyId(), i,
                TStringBuilder() << "family for column `" << columns[i].GetName() << "` is not `" << families[i].GetFamilyName() << "`");
            TTestHelper::TCompression compression;
            UNIT_ASSERT(compression.DeserializeFromProto(columns[i].GetSerializer()));
            TString errorMessage;
            UNIT_ASSERT_C(compression.IsEqual(families[i].GetCompression(), errorMessage), errorMessage);
        }
    }

    Y_UNIT_TEST(AlterCompressionLevel) {
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper testHelper(TKikimrSettings().SetWithSampleTables(false));

        TString tableName = "/Root/TableWithColumnFamily";
        TTestHelper::TCompression zstdCompression =
            TTestHelper::TCompression().SetCompressionType(NKikimrSchemeOp::EColumnCodec::ColumnCodecZSTD).SetCompressionLevel(5);

        TVector<TTestHelper::TColumnFamily> families = {
            TTestHelper::TColumnFamily().SetId(0).SetFamilyName("default").SetCompression(zstdCompression),
        };

        {
            TVector<TTestHelper::TColumnSchema> schema = {
                TTestHelper::TColumnSchema().SetName("Key").SetType(NScheme::NTypeIds::Uint64).SetNullable(false),
                TTestHelper::TColumnSchema().SetName("Value1").SetType(NScheme::NTypeIds::String).SetNullable(true),
                TTestHelper::TColumnSchema().SetName("Value2").SetType(NScheme::NTypeIds::Uint32).SetNullable(true)
            };

            TTestHelper::TColumnTable testTable;
            testTable.SetName(tableName).SetPrimaryKey({ "Key" }).SetSchema(schema).SetColumnFamilies(families);
            testHelper.CreateTable(testTable);
        }

        families[0].MutableCompression().SetCompressionLevel(6);
        auto alterFamilyCompressionLevel = TStringBuilder() << R"(ALTER TABLE `)" << tableName << R"(`
                    ALTER FAMILY default SET COMPRESSION_LEVEL 6;)";
        auto session = testHelper.GetSession();
        auto result = session.ExecuteSchemeQuery(alterFamilyCompressionLevel).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto& runner = testHelper.GetKikimr();
        auto runtime = runner.GetTestServer().GetRuntime();
        TActorId sender = runtime->AllocateEdgeActor();
        auto describeResult = DescribeTable(&runner.GetTestServer(), sender, tableName);
        auto schema = describeResult.GetPathDescription().GetColumnTableDescription().GetSchema();

        UNIT_ASSERT_EQUAL(schema.ColumnFamiliesSize(), families.size());
        TTestHelper::TColumnFamily defaultFromScheme;
        UNIT_ASSERT(defaultFromScheme.DeserializeFromProto(schema.GetColumnFamilies(0)));
        {
            TString errorMessage;
            UNIT_ASSERT_C(defaultFromScheme.IsEqual(families[0], errorMessage), errorMessage);
        }

        for (const auto& column : schema.GetColumns()) {
            UNIT_ASSERT(column.HasSerializer());
            UNIT_ASSERT_EQUAL_C(
                column.GetColumnFamilyId(), 0, TStringBuilder() << "family for column " << column.GetName() << " is not default");
            TTestHelper::TCompression compression;
            UNIT_ASSERT(compression.DeserializeFromProto(column.GetSerializer()));
            TString errorMessage;
            UNIT_ASSERT_C(compression.IsEqual(families[0].GetCompression(), errorMessage), errorMessage);
        }
    }

    Y_UNIT_TEST(AlterCompressionLevelError) {
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper testHelper(TKikimrSettings().SetWithSampleTables(false));

        TString tableName = "/Root/TableWithColumnFamily";
        TTestHelper::TCompression lz4Compression = TTestHelper::TCompression().SetCompressionType(NKikimrSchemeOp::EColumnCodec::ColumnCodecLZ4);

        TVector<TTestHelper::TColumnFamily> families = {
            TTestHelper::TColumnFamily().SetId(0).SetFamilyName("default").SetCompression(lz4Compression),
        };

        {
            TVector<TTestHelper::TColumnSchema> schema = {
                TTestHelper::TColumnSchema().SetName("Key").SetType(NScheme::NTypeIds::Uint64).SetNullable(false),
                TTestHelper::TColumnSchema().SetName("Value1").SetType(NScheme::NTypeIds::String).SetNullable(true),
                TTestHelper::TColumnSchema().SetName("Value2").SetType(NScheme::NTypeIds::Uint32).SetNullable(true)
            };

            TTestHelper::TColumnTable testTable;
            testTable.SetName(tableName).SetPrimaryKey({ "Key" }).SetSchema(schema).SetColumnFamilies(families);
            testHelper.CreateTable(testTable);
        }

        families[0].MutableCompression().SetCompressionLevel(6);
        auto alterFamilyCompressionLevel = TStringBuilder() << R"(ALTER TABLE `)" << tableName << R"(`
                    ALTER FAMILY default SET COMPRESSION_LEVEL 6;)";
        auto session = testHelper.GetSession();
        auto result = session.ExecuteSchemeQuery(alterFamilyCompressionLevel).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SCHEME_ERROR, result.GetIssues().ToString());
    }

    Y_UNIT_TEST(CreateWithColumnFamily) {
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper testHelper(TKikimrSettings().SetWithSampleTables(false));

        TString tableName = "/Root/TableWithColumnFamily";
        TTestHelper::TCompression plainCompression =
            TTestHelper::TCompression().SetCompressionType(NKikimrSchemeOp::EColumnCodec::ColumnCodecPlain);
        TTestHelper::TCompression zstdCompression =
            TTestHelper::TCompression().SetCompressionType(NKikimrSchemeOp::EColumnCodec::ColumnCodecZSTD).SetCompressionLevel(3);
        TTestHelper::TCompression lz4Compression = TTestHelper::TCompression().SetCompressionType(NKikimrSchemeOp::EColumnCodec::ColumnCodecLZ4);

        TVector<TTestHelper::TColumnFamily> families = {
            TTestHelper::TColumnFamily().SetId(0).SetFamilyName("default").SetCompression(plainCompression),
            TTestHelper::TColumnFamily().SetId(1).SetFamilyName("family1").SetCompression(zstdCompression),
            TTestHelper::TColumnFamily().SetId(2).SetFamilyName("family2").SetCompression(lz4Compression),
        };

        {
            TVector<TTestHelper::TColumnSchema> schema = {
                TTestHelper::TColumnSchema().SetName("Key").SetType(NScheme::NTypeIds::Uint64).SetNullable(false),
                TTestHelper::TColumnSchema()
                    .SetName("Value1")
                    .SetType(NScheme::NTypeIds::String)
                    .SetNullable(true)
                    .SetColumnFamilyName(families[1].GetFamilyName()),
                TTestHelper::TColumnSchema()
                    .SetName("Value2")
                    .SetType(NScheme::NTypeIds::Uint32)
                    .SetNullable(true)
                    .SetColumnFamilyName(families[2].GetFamilyName())
            };

            TTestHelper::TColumnTable testTable;
            testTable.SetName(tableName).SetPrimaryKey({ "Key" }).SetSchema(schema).SetColumnFamilies(families);
            testHelper.CreateTable(testTable);
        }

        auto& runner = testHelper.GetKikimr();
        auto runtime = runner.GetTestServer().GetRuntime();
        TActorId sender = runtime->AllocateEdgeActor();
        auto describeResult = DescribeTable(&runner.GetTestServer(), sender, tableName);
        auto schema = describeResult.GetPathDescription().GetColumnTableDescription().GetSchema();

        UNIT_ASSERT_EQUAL(schema.ColumnFamiliesSize(), families.size());
        for (ui32 i = 0; i < families.size(); i++) {
            TTestHelper::TColumnFamily familyFromScheme;
            UNIT_ASSERT(familyFromScheme.DeserializeFromProto(schema.GetColumnFamilies(i)));
            TString errorMessage;
            UNIT_ASSERT_C(familyFromScheme.IsEqual(families[i], errorMessage), errorMessage);
        }

        auto columns = schema.GetColumns();
        for (ui32 i = 0; i < schema.ColumnsSize(); i++) {
            UNIT_ASSERT(columns[i].HasSerializer());
            UNIT_ASSERT_EQUAL_C(columns[i].GetColumnFamilyId(), i,
                TStringBuilder() << "family for column `" << columns[i].GetName() << "` is not `" << families[i].GetFamilyName() << "`");
            TTestHelper::TCompression compression;
            UNIT_ASSERT(compression.DeserializeFromProto(columns[i].GetSerializer()));
            TString errorMessage;
            UNIT_ASSERT_C(compression.IsEqual(families[i].GetCompression(), errorMessage), errorMessage);
        }
    }

    Y_UNIT_TEST(AddColumnFamily) {
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper testHelper(TKikimrSettings().SetWithSampleTables(false));

        TString tableName = "/Root/TableWithColumnFamily";
        TTestHelper::TCompression plainCompression =
            TTestHelper::TCompression().SetCompressionType(NKikimrSchemeOp::EColumnCodec::ColumnCodecPlain);
        TTestHelper::TCompression zstdCompression =
            TTestHelper::TCompression().SetCompressionType(NKikimrSchemeOp::EColumnCodec::ColumnCodecZSTD).SetCompressionLevel(1);
        TTestHelper::TCompression lz4Compression = TTestHelper::TCompression().SetCompressionType(NKikimrSchemeOp::EColumnCodec::ColumnCodecLZ4);

        TVector<TTestHelper::TColumnFamily> families = {
            TTestHelper::TColumnFamily().SetId(0).SetFamilyName("default").SetCompression(plainCompression),
        };

        {
            TVector<TTestHelper::TColumnSchema> schema = {
                TTestHelper::TColumnSchema().SetName("Key").SetType(NScheme::NTypeIds::Uint64).SetNullable(false),
                TTestHelper::TColumnSchema().SetName("Value1").SetType(NScheme::NTypeIds::String).SetNullable(true),
                TTestHelper::TColumnSchema().SetName("Value2").SetType(NScheme::NTypeIds::Uint32).SetNullable(true)
            };

            TTestHelper::TColumnTable testTable;
            testTable.SetName(tableName).SetPrimaryKey({ "Key" }).SetSchema(schema).SetColumnFamilies(families);
            testHelper.CreateTable(testTable);
        }

        auto session = testHelper.GetSession();
        auto& runner = testHelper.GetKikimr();
        auto runtime = runner.GetTestServer().GetRuntime();
        TActorId sender = runtime->AllocateEdgeActor();
        {
            families.push_back(TTestHelper::TColumnFamily().SetId(1).SetFamilyName("family1").SetCompression(zstdCompression));
            auto query = TStringBuilder() << R"(ALTER TABLE `)" << tableName << R"(`
                    ADD FAMILY family1 (
                        COMPRESSION = "zstd");)";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto describeResult = DescribeTable(&runner.GetTestServer(), sender, tableName);
            auto schema = describeResult.GetPathDescription().GetColumnTableDescription().GetSchema();
            UNIT_ASSERT_EQUAL(schema.ColumnFamiliesSize(), families.size());
            for (ui32 i = 0; i < families.size(); i++) {
                TTestHelper::TColumnFamily familyFromScheme;
                UNIT_ASSERT(familyFromScheme.DeserializeFromProto(schema.GetColumnFamilies(i)));
                TString errorMessage;
                UNIT_ASSERT_C(familyFromScheme.IsEqual(families[i], errorMessage), errorMessage);
            }
        }

        {
            families.push_back(TTestHelper::TColumnFamily().SetId(2).SetFamilyName("family2").SetCompression(lz4Compression));
            families.push_back(TTestHelper::TColumnFamily().SetId(3).SetFamilyName("family3").SetCompression(zstdCompression));
            auto query = TStringBuilder() << R"(ALTER TABLE `)" << tableName << R"(`
                    ADD FAMILY family2 (
                        COMPRESSION = "lz4"),
                    ADD FAMILY family3 (
                        COMPRESSION = "zstd");)";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto describeResult = DescribeTable(&runner.GetTestServer(), sender, tableName);
            auto schema = describeResult.GetPathDescription().GetColumnTableDescription().GetSchema();
            UNIT_ASSERT_EQUAL(schema.ColumnFamiliesSize(), families.size());
            for (ui32 i = 0; i < families.size(); i++) {
                TTestHelper::TColumnFamily familyFromScheme;
                UNIT_ASSERT(familyFromScheme.DeserializeFromProto(schema.GetColumnFamilies(i)));
                TString errorMessage;
                UNIT_ASSERT_C(familyFromScheme.IsEqual(families[i], errorMessage), errorMessage);
            }
        }
    }

    Y_UNIT_TEST(AddColumnWithoutColumnFamily) {
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper testHelper(TKikimrSettings().SetWithSampleTables(false));

        TString tableName = "/Root/TableWithColumnFamily";
        TTestHelper::TCompression zstdCompression =
            TTestHelper::TCompression().SetCompressionType(NKikimrSchemeOp::EColumnCodec::ColumnCodecZSTD).SetCompressionLevel(1);

        TVector<TTestHelper::TColumnFamily> families = {
            TTestHelper::TColumnFamily().SetId(0).SetFamilyName("default").SetCompression(zstdCompression),
        };

        {
            TVector<TTestHelper::TColumnSchema> schema = {
                TTestHelper::TColumnSchema().SetName("Key").SetType(NScheme::NTypeIds::Uint64).SetNullable(false),
                TTestHelper::TColumnSchema().SetName("Value1").SetType(NScheme::NTypeIds::String).SetNullable(true),
                TTestHelper::TColumnSchema().SetName("Value2").SetType(NScheme::NTypeIds::Uint32).SetNullable(true)
            };

            TTestHelper::TColumnTable testTable;
            testTable.SetName(tableName).SetPrimaryKey({ "Key" }).SetSchema(schema).SetColumnFamilies(families);
            testHelper.CreateTable(testTable);
        }

        auto session = testHelper.GetSession();
        auto& runner = testHelper.GetKikimr();
        auto runtime = runner.GetTestServer().GetRuntime();
        TActorId sender = runtime->AllocateEdgeActor();

        {
            auto query = TStringBuilder() << R"(ALTER TABLE `)" << tableName << R"(`
                    ADD COLUMN Value3 Uint32;)";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto describeResult = DescribeTable(&runner.GetTestServer(), sender, tableName);
            auto schema = describeResult.GetPathDescription().GetColumnTableDescription().GetSchema();
            UNIT_ASSERT_EQUAL(schema.ColumnFamiliesSize(), families.size());
            for (ui32 i = 0; i < families.size(); i++) {
                TTestHelper::TColumnFamily familyFromScheme;
                UNIT_ASSERT(familyFromScheme.DeserializeFromProto(schema.GetColumnFamilies(i)));
                TString errorMessage;
                UNIT_ASSERT_C(familyFromScheme.IsEqual(families[i], errorMessage), errorMessage);
            }

            auto columns = schema.GetColumns();
            for (ui32 i = 0; i < schema.ColumnsSize(); i++) {
                UNIT_ASSERT(columns[i].HasSerializer());
                UNIT_ASSERT_EQUAL_C(columns[i].GetColumnFamilyId(), 0,
                    TStringBuilder() << "family for column `" << columns[i].GetName() << "` is not `" << families[0].GetFamilyName()
                                     << "`");
                TTestHelper::TCompression compression;
                UNIT_ASSERT(compression.DeserializeFromProto(columns[i].GetSerializer()));
                TString errorMessage;
                UNIT_ASSERT_C(compression.IsEqual(families[0].GetCompression(), errorMessage), errorMessage);
            }
        }

    }

    Y_UNIT_TEST(AddColumnWithColumnFamily) {
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper testHelper(TKikimrSettings().SetWithSampleTables(false));

        TString tableName = "/Root/TableWithColumnFamily";
        TTestHelper::TCompression plainCompression =
            TTestHelper::TCompression().SetCompressionType(NKikimrSchemeOp::EColumnCodec::ColumnCodecPlain);
        TTestHelper::TCompression zstdCompression =
            TTestHelper::TCompression().SetCompressionType(NKikimrSchemeOp::EColumnCodec::ColumnCodecZSTD).SetCompressionLevel(1);
        TTestHelper::TCompression lz4Compression = TTestHelper::TCompression().SetCompressionType(NKikimrSchemeOp::EColumnCodec::ColumnCodecLZ4);

        TVector<TTestHelper::TColumnFamily> families = {
            TTestHelper::TColumnFamily().SetId(0).SetFamilyName("default").SetCompression(plainCompression),
        };

        {
            TVector<TTestHelper::TColumnSchema> schema = {
                TTestHelper::TColumnSchema().SetName("Key").SetType(NScheme::NTypeIds::Uint64).SetNullable(false),
                TTestHelper::TColumnSchema().SetName("Value1").SetType(NScheme::NTypeIds::String).SetNullable(true),
                TTestHelper::TColumnSchema().SetName("Value2").SetType(NScheme::NTypeIds::Uint32).SetNullable(true)
            };

            TTestHelper::TColumnTable testTable;
            testTable.SetName(tableName).SetPrimaryKey({ "Key" }).SetSchema(schema).SetColumnFamilies(families);
            testHelper.CreateTable(testTable);
        }

        auto session = testHelper.GetSession();
        auto& runner = testHelper.GetKikimr();
        auto runtime = runner.GetTestServer().GetRuntime();
        TActorId sender = runtime->AllocateEdgeActor();
        {
            families.push_back(TTestHelper::TColumnFamily().SetId(1).SetFamilyName("family1").SetCompression(zstdCompression));
            auto query = TStringBuilder() << R"(ALTER TABLE `)" << tableName << R"(`
                    ADD FAMILY family1 (
                        COMPRESSION = "zstd");)";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto query = TStringBuilder() << R"(ALTER TABLE `)" << tableName << R"(`
                    ADD COLUMN Value3 Uint32 FAMILY family1;)";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto describeResult = DescribeTable(&runner.GetTestServer(), sender, tableName);
            auto schema = describeResult.GetPathDescription().GetColumnTableDescription().GetSchema();
            UNIT_ASSERT_EQUAL(schema.ColumnFamiliesSize(), families.size());
            for (ui32 i = 0; i < families.size(); i++) {
                TTestHelper::TColumnFamily familyFromScheme;
                UNIT_ASSERT(familyFromScheme.DeserializeFromProto(schema.GetColumnFamilies(i)));
                TString errorMessage;
                UNIT_ASSERT_C(familyFromScheme.IsEqual(families[i], errorMessage), errorMessage);
            }

            auto columns = schema.GetColumns();
            for (ui32 i = 0; i < schema.ColumnsSize(); i++) {
                UNIT_ASSERT(columns[i].HasSerializer());
                ui32 indexFamily = 0;
                if (columns[i].GetName() == "Value3") {
                    indexFamily = 1;
                }
                UNIT_ASSERT_EQUAL_C(columns[i].GetColumnFamilyId(), indexFamily,
                    TStringBuilder() << "family for column `" << columns[i].GetName() << "` is not `" << families[indexFamily].GetFamilyName()
                                     << "`");
                TTestHelper::TCompression compression;
                UNIT_ASSERT(compression.DeserializeFromProto(columns[i].GetSerializer()));
                TString errorMessage;
                UNIT_ASSERT_C(compression.IsEqual(families[indexFamily].GetCompression(), errorMessage), errorMessage);
            }
        }

        {
            families.push_back(TTestHelper::TColumnFamily().SetId(2).SetFamilyName("family2").SetCompression(lz4Compression));
            auto query = TStringBuilder() << R"(ALTER TABLE `)" << tableName << R"(`
                    ADD FAMILY family2 (
                        COMPRESSION = "lz4"),
                    ADD COLUMN Value4 Uint32 FAMILY family2,
                    ADD COLUMN Value5 Uint32 FAMILY family1;)";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto describeResult = DescribeTable(&runner.GetTestServer(), sender, tableName);
            auto schema = describeResult.GetPathDescription().GetColumnTableDescription().GetSchema();
            UNIT_ASSERT_EQUAL(schema.ColumnFamiliesSize(), families.size());
            for (ui32 i = 0; i < families.size(); i++) {
                TTestHelper::TColumnFamily familyFromScheme;
                UNIT_ASSERT(familyFromScheme.DeserializeFromProto(schema.GetColumnFamilies(i)));
                TString errorMessage;
                UNIT_ASSERT_C(familyFromScheme.IsEqual(families[i], errorMessage), errorMessage);
            }

            auto columns = schema.GetColumns();
            for (ui32 i = 0; i < schema.ColumnsSize(); i++) {
                UNIT_ASSERT(columns[i].HasSerializer());
                ui32 indexFamily = 0;
                if (columns[i].GetName() == "Value3" || columns[i].GetName() == "Value5") {
                    indexFamily = 1;
                } else if (columns[i].GetName() == "Value4") {
                    indexFamily = 2;
                }
                UNIT_ASSERT_EQUAL_C(columns[i].GetColumnFamilyId(), indexFamily,
                    TStringBuilder() << "family for column `" << columns[i].GetName() << "` is not `" << families[indexFamily].GetFamilyName()
                                     << "`");
                TTestHelper::TCompression compression;
                UNIT_ASSERT(compression.DeserializeFromProto(columns[i].GetSerializer()));
                TString errorMessage;
                UNIT_ASSERT_C(compression.IsEqual(families[indexFamily].GetCompression(), errorMessage), errorMessage);
            }
        }
    }

    Y_UNIT_TEST(SetColumnFamily) {
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper testHelper(TKikimrSettings().SetWithSampleTables(false));

        TString tableName = "/Root/TableWithColumnFamily";
        TTestHelper::TCompression plainCompression =
            TTestHelper::TCompression().SetCompressionType(NKikimrSchemeOp::EColumnCodec::ColumnCodecPlain);
        TTestHelper::TCompression zstdCompression =
            TTestHelper::TCompression().SetCompressionType(NKikimrSchemeOp::EColumnCodec::ColumnCodecZSTD).SetCompressionLevel(1);
        TTestHelper::TCompression lz4Compression = TTestHelper::TCompression().SetCompressionType(NKikimrSchemeOp::EColumnCodec::ColumnCodecLZ4);

        TVector<TTestHelper::TColumnFamily> families = {
            TTestHelper::TColumnFamily().SetId(0).SetFamilyName("default").SetCompression(plainCompression),
            TTestHelper::TColumnFamily().SetId(1).SetFamilyName("family1").SetCompression(zstdCompression),
        };

        {
            TVector<TTestHelper::TColumnSchema> schema = {
                TTestHelper::TColumnSchema().SetName("Key").SetType(NScheme::NTypeIds::Uint64).SetNullable(false),
                TTestHelper::TColumnSchema().SetName("Value1").SetType(NScheme::NTypeIds::String).SetNullable(true),
                TTestHelper::TColumnSchema().SetName("Value2").SetType(NScheme::NTypeIds::Uint32).SetNullable(true)
            };

            TTestHelper::TColumnTable testTable;
            testTable.SetName(tableName).SetPrimaryKey({ "Key" }).SetSchema(schema).SetColumnFamilies(families);
            testHelper.CreateTable(testTable);
        }

        auto session = testHelper.GetSession();
        auto& runner = testHelper.GetKikimr();
        auto runtime = runner.GetTestServer().GetRuntime();
        TActorId sender = runtime->AllocateEdgeActor();

        {
            auto query = TStringBuilder() << R"(ALTER TABLE `)" << tableName << R"(`
                    ALTER COLUMN Value1 SET FAMILY family1;)";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto describeResult = DescribeTable(&runner.GetTestServer(), sender, tableName);
            auto schema = describeResult.GetPathDescription().GetColumnTableDescription().GetSchema();

            auto columns = schema.GetColumns();
            for (ui32 i = 0; i < schema.ColumnsSize(); i++) {
                UNIT_ASSERT(columns[i].HasSerializer());
                ui32 indexFamily = 0;
                if (columns[i].GetName() == "Value1") {
                    indexFamily = 1;
                }

                UNIT_ASSERT_EQUAL_C(columns[i].GetColumnFamilyId(), indexFamily,
                    TStringBuilder() << "family for column `" << columns[i].GetName() << "` is not `" << families[indexFamily].GetFamilyName()
                                     << "`");
                TTestHelper::TCompression compression;
                UNIT_ASSERT(compression.DeserializeFromProto(columns[i].GetSerializer()));
                TString errorMessage;
                UNIT_ASSERT_C(compression.IsEqual(families[indexFamily].GetCompression(), errorMessage), errorMessage);
            }
        }

        {
            families.push_back(TTestHelper::TColumnFamily().SetId(2).SetFamilyName("family2").SetCompression(lz4Compression));
            auto query = TStringBuilder() << R"(ALTER TABLE `)" << tableName << R"(`
                    ADD FAMILY family2 (
                        COMPRESSION = "lz4"),
                    ALTER COLUMN Value2 SET FAMILY family2;)";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto describeResult = DescribeTable(&runner.GetTestServer(), sender, tableName);
            auto schema = describeResult.GetPathDescription().GetColumnTableDescription().GetSchema();
            UNIT_ASSERT_EQUAL(schema.ColumnFamiliesSize(), families.size());
            for (ui32 i = 0; i < families.size(); i++) {
                TTestHelper::TColumnFamily familyFromScheme;
                UNIT_ASSERT(familyFromScheme.DeserializeFromProto(schema.GetColumnFamilies(i)));
                TString errorMessage;
                UNIT_ASSERT_C(familyFromScheme.IsEqual(families[i], errorMessage), errorMessage);
            }

            auto columns = schema.GetColumns();
            for (ui32 i = 0; i < schema.ColumnsSize(); i++) {
                UNIT_ASSERT(columns[i].HasSerializer());
                ui32 indexFamily = 0;
                if (columns[i].GetName() == "Value1") {
                    indexFamily = 1;
                } else if (columns[i].GetName() == "Value2") {
                    indexFamily = 2;
                }

                UNIT_ASSERT_EQUAL_C(columns[i].GetColumnFamilyId(), indexFamily,
                    TStringBuilder() << "family for column `" << columns[i].GetName() << "` is not `" << families[indexFamily].GetFamilyName()
                                     << "`");
                TTestHelper::TCompression compression;
                UNIT_ASSERT(compression.DeserializeFromProto(columns[i].GetSerializer()));
                TString errorMessage;
                UNIT_ASSERT_C(compression.IsEqual(families[indexFamily].GetCompression(), errorMessage), errorMessage);
            }
        }
    }

    Y_UNIT_TEST(WithoutDefaultColumnFamily) {
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper testHelper(TKikimrSettings().SetWithSampleTables(false));
        TString tableName = "/Root/TableWithFamily";

        TTestHelper::TCompression lz4Compression = TTestHelper::TCompression().SetCompressionType(NKikimrSchemeOp::EColumnCodec::ColumnCodecLZ4);

        TVector<TTestHelper::TColumnFamily> families = {
            TTestHelper::TColumnFamily().SetId(1).SetFamilyName("family1").SetCompression(lz4Compression),
        };

        {
            TVector<TTestHelper::TColumnSchema> schema = {
                TTestHelper::TColumnSchema().SetName("Key").SetType(NScheme::NTypeIds::Uint64).SetNullable(false),
                TTestHelper::TColumnSchema()
                    .SetName("Value1")
                    .SetType(NScheme::NTypeIds::String)
                    .SetNullable(true)
                    .SetColumnFamilyName(families[0].GetFamilyName()),
                TTestHelper::TColumnSchema()
                    .SetName("Value2")
                    .SetType(NScheme::NTypeIds::Uint32)
                    .SetNullable(true)
                    .SetColumnFamilyName(families[0].GetFamilyName())
            };

            TTestHelper::TColumnTable testTable;
            testTable.SetName(tableName).SetPrimaryKey({ "Key" }).SetSchema(schema).SetColumnFamilies(families);
            testHelper.CreateTable(testTable);
        }

        families.push_back(TTestHelper::TColumnFamily().SetId(0).SetFamilyName("default"));
        auto& runner = testHelper.GetKikimr();
        auto runtime = runner.GetTestServer().GetRuntime();
        TActorId sender = runtime->AllocateEdgeActor();
        auto describeResult = DescribeTable(&runner.GetTestServer(), sender, tableName);
        auto schema = describeResult.GetPathDescription().GetColumnTableDescription().GetSchema();
        UNIT_ASSERT_EQUAL(schema.ColumnFamiliesSize(), families.size());
        for (ui32 i = 0; i < families.size(); i++) {
            TTestHelper::TColumnFamily familyFromScheme;
            UNIT_ASSERT(familyFromScheme.DeserializeFromProto(schema.GetColumnFamilies(i)));
            ui32 familyIndex = 0;
            if (familyFromScheme.GetFamilyName() == "default") {
                familyIndex = 1;
            }
            TString errorMessage;
            UNIT_ASSERT_C(familyFromScheme.IsEqual(families[familyIndex], errorMessage), errorMessage);
        }
    }

    Y_UNIT_TEST(UnknownColumnFamily) {
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper testHelper(TKikimrSettings().SetWithSampleTables(false));
        TString tableName = "/Root/TableWithFamily";

        TTestHelper::TCompression plainCompression =
            TTestHelper::TCompression().SetCompressionType(NKikimrSchemeOp::EColumnCodec::ColumnCodecPlain);

        TVector<TTestHelper::TColumnFamily> families = {
            TTestHelper::TColumnFamily().SetId(0).SetFamilyName("default").SetCompression(plainCompression),
        };

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("Key").SetType(NScheme::NTypeIds::Uint64).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("Value1").SetType(NScheme::NTypeIds::String).SetNullable(true).SetColumnFamilyName("family1"),
            TTestHelper::TColumnSchema().SetName("Value2").SetType(NScheme::NTypeIds::Uint32).SetNullable(true).SetColumnFamilyName("family1")
        };

        TTestHelper::TColumnTable testTable;
        testTable.SetName(tableName).SetPrimaryKey({ "Key" }).SetSchema(schema).SetColumnFamilies(families);
        testHelper.CreateTable(testTable, EStatus::GENERIC_ERROR);
    }

    Y_UNIT_TEST(PrimaryKeyNotDefaultColumnFamily) {
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper testHelper(TKikimrSettings().SetWithSampleTables(false));
        TString tableName = "/Root/TableWithFamily";

        TTestHelper::TCompression plainCompression =
            TTestHelper::TCompression().SetCompressionType(NKikimrSchemeOp::EColumnCodec::ColumnCodecPlain);
        TTestHelper::TCompression zstdCompression =
            TTestHelper::TCompression().SetCompressionType(NKikimrSchemeOp::EColumnCodec::ColumnCodecZSTD).SetCompressionLevel(1);

        TVector<TTestHelper::TColumnFamily> families = {
            TTestHelper::TColumnFamily().SetId(0).SetFamilyName("default").SetCompression(plainCompression),
            TTestHelper::TColumnFamily().SetId(1).SetFamilyName("family1").SetCompression(zstdCompression),
        };

        {
            TVector<TTestHelper::TColumnSchema> schema = { TTestHelper::TColumnSchema()
                                                               .SetName("Key")
                                                               .SetType(NScheme::NTypeIds::Uint64)
                                                               .SetColumnFamilyName(families[1].GetFamilyName())
                                                               .SetNullable(false),
                TTestHelper::TColumnSchema()
                    .SetName("Value1")
                    .SetType(NScheme::NTypeIds::String)
                    .SetNullable(true)
                    .SetColumnFamilyName(families[1].GetFamilyName()),
                TTestHelper::TColumnSchema()
                    .SetName("Value2")
                    .SetType(NScheme::NTypeIds::Uint32)
                    .SetNullable(true)
                    .SetColumnFamilyName(families[1].GetFamilyName()) };
            TTestHelper::TColumnTable testTable;
            testTable.SetName(tableName).SetPrimaryKey({ "Key" }).SetSchema(schema).SetColumnFamilies(families);
            testHelper.CreateTable(testTable);
        }

        auto& runner = testHelper.GetKikimr();
        auto runtime = runner.GetTestServer().GetRuntime();
        TActorId sender = runtime->AllocateEdgeActor();
        auto describeResult = DescribeTable(&runner.GetTestServer(), sender, tableName);
        auto schema = describeResult.GetPathDescription().GetColumnTableDescription().GetSchema();

        UNIT_ASSERT_EQUAL(schema.ColumnFamiliesSize(), families.size());
        for (ui32 i = 0; i < families.size(); i++) {
            TTestHelper::TColumnFamily familyFromScheme;
            UNIT_ASSERT(familyFromScheme.DeserializeFromProto(schema.GetColumnFamilies(i)));
            TString errorMessage;
            UNIT_ASSERT_C(familyFromScheme.IsEqual(families[i], errorMessage), errorMessage);
        }

        auto columns = schema.GetColumns();
        for (ui32 i = 0; i < schema.ColumnsSize(); i++) {
            UNIT_ASSERT(columns[i].HasSerializer());
            UNIT_ASSERT_EQUAL_C(columns[i].GetColumnFamilyId(), 1,
                TStringBuilder() << "family for column `" << columns[i].GetName() << "` is not `" << families[1].GetFamilyName() << "`");
            TTestHelper::TCompression compression;
            UNIT_ASSERT(compression.DeserializeFromProto(columns[i].GetSerializer()));
            TString errorMessage;
            UNIT_ASSERT_C(compression.IsEqual(families[1].GetCompression(), errorMessage), errorMessage);
        }
    }

    Y_UNIT_TEST(SetNotDefaultColumnFamilyForPrimaryKey) {
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper testHelper(TKikimrSettings().SetWithSampleTables(false));
        TString tableName = "/Root/TableWithFamily";

        TTestHelper::TCompression plainCompression =
            TTestHelper::TCompression().SetCompressionType(NKikimrSchemeOp::EColumnCodec::ColumnCodecPlain);
        TTestHelper::TCompression zstdCompression =
            TTestHelper::TCompression().SetCompressionType(NKikimrSchemeOp::EColumnCodec::ColumnCodecZSTD).SetCompressionLevel(1);

        TVector<TTestHelper::TColumnFamily> families = {
            TTestHelper::TColumnFamily().SetId(0).SetFamilyName("default").SetCompression(plainCompression),
            TTestHelper::TColumnFamily().SetId(1).SetFamilyName("family1").SetCompression(zstdCompression),
        };

        {
            TVector<TTestHelper::TColumnSchema> schema = {
                TTestHelper::TColumnSchema().SetName("Key").SetType(NScheme::NTypeIds::Uint64).SetNullable(false),
                TTestHelper::TColumnSchema()
                    .SetName("Value1")
                    .SetType(NScheme::NTypeIds::String)
                    .SetNullable(true)
                    .SetColumnFamilyName(families[1].GetFamilyName()),
                TTestHelper::TColumnSchema()
                    .SetName("Value2")
                    .SetType(NScheme::NTypeIds::Uint32)
                    .SetNullable(true)
                    .SetColumnFamilyName(families[1].GetFamilyName())
            };
            TTestHelper::TColumnTable testTable;
            testTable.SetName(tableName).SetPrimaryKey({ "Key" }).SetSchema(schema).SetColumnFamilies(families);
            testHelper.CreateTable(testTable);
        }

        auto session = testHelper.GetSession();
        auto query = TStringBuilder() << R"(ALTER TABLE `)" << tableName << R"(`
                    ALTER COLUMN Key SET FAMILY family1;)";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto& runner = testHelper.GetKikimr();
        auto runtime = runner.GetTestServer().GetRuntime();
        TActorId sender = runtime->AllocateEdgeActor();
        auto describeResult = DescribeTable(&runner.GetTestServer(), sender, tableName);
        auto schema = describeResult.GetPathDescription().GetColumnTableDescription().GetSchema();

        UNIT_ASSERT_EQUAL(schema.ColumnFamiliesSize(), families.size());
        for (ui32 i = 0; i < families.size(); i++) {
            TTestHelper::TColumnFamily familyFromScheme;
            UNIT_ASSERT(familyFromScheme.DeserializeFromProto(schema.GetColumnFamilies(i)));
            TString errorMessage;
            UNIT_ASSERT_C(familyFromScheme.IsEqual(families[i], errorMessage), errorMessage);
        }

        auto columns = schema.GetColumns();
        for (ui32 i = 0; i < schema.ColumnsSize(); i++) {
            UNIT_ASSERT(columns[i].HasSerializer());
            UNIT_ASSERT_EQUAL_C(columns[i].GetColumnFamilyId(), 1,
                TStringBuilder() << "family for column `" << columns[i].GetName() << "` is not `" << families[1].GetFamilyName() << "`");
            TTestHelper::TCompression compression;
            UNIT_ASSERT(compression.DeserializeFromProto(columns[i].GetSerializer()));
            TString errorMessage;
            UNIT_ASSERT_C(compression.IsEqual(families[1].GetCompression(), errorMessage), errorMessage);
        }
    }

    Y_UNIT_TEST(AddExsitsColumnFamily) {
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper testHelper(TKikimrSettings().SetWithSampleTables(false));

        TString tableName = "/Root/TableWithFamily";
        TTestHelper::TCompression plainCompression =
            TTestHelper::TCompression().SetCompressionType(NKikimrSchemeOp::EColumnCodec::ColumnCodecPlain);
        TTestHelper::TCompression lz4Compression = TTestHelper::TCompression().SetCompressionType(NKikimrSchemeOp::EColumnCodec::ColumnCodecLZ4);

        TVector<TTestHelper::TColumnFamily> families = {
            TTestHelper::TColumnFamily().SetId(0).SetFamilyName("default").SetCompression(plainCompression),
            TTestHelper::TColumnFamily().SetId(1).SetFamilyName("family1").SetCompression(lz4Compression),
            TTestHelper::TColumnFamily().SetId(2).SetFamilyName("family2").SetCompression(lz4Compression),
        };

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("Key").SetType(NScheme::NTypeIds::Uint64).SetNullable(false),
            TTestHelper::TColumnSchema()
                .SetName("Value1")
                .SetType(NScheme::NTypeIds::String)
                .SetNullable(true)
                .SetColumnFamilyName(families[1].GetFamilyName()),
            TTestHelper::TColumnSchema()
                .SetName("Value2")
                .SetType(NScheme::NTypeIds::Uint32)
                .SetNullable(true)
                .SetColumnFamilyName(families[1].GetFamilyName())
        };

        TTestHelper::TColumnTable testTable;
        testTable.SetName(tableName).SetPrimaryKey({ "Key" }).SetSchema(schema).SetColumnFamilies(families);
        testHelper.CreateTable(testTable);

        auto session = testHelper.GetSession();
        {
            auto query = TStringBuilder() << R"(ALTER TABLE `)" << tableName << R"(`
                        ADD FAMILY family1 (COMPRESSION = "lz4")";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
        }

        {
            auto query = TStringBuilder() << R"(ALTER TABLE `)" << tableName
                                          << R"(` ADD FAMILY family3 (COMPRESSION = "lz4"), ADD FAMILY family3 (COMPRESSION = "zstd")";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(AddColumnFamilyWithNotSupportedCodec) {
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper testHelper(TKikimrSettings().SetWithSampleTables(false));

        TString tableName = "/Root/TableWithFamily";
        TTestHelper::TCompression plainCompression =
            TTestHelper::TCompression().SetCompressionType(NKikimrSchemeOp::EColumnCodec::ColumnCodecPlain);
        TTestHelper::TCompression lz4Compression = TTestHelper::TCompression().SetCompressionType(NKikimrSchemeOp::EColumnCodec::ColumnCodecLZ4);

        TVector<TTestHelper::TColumnFamily> families = {
            TTestHelper::TColumnFamily().SetId(0).SetFamilyName("default").SetCompression(plainCompression),
            TTestHelper::TColumnFamily().SetId(1).SetFamilyName("family1").SetCompression(lz4Compression),
            TTestHelper::TColumnFamily().SetId(2).SetFamilyName("family2").SetCompression(lz4Compression),
        };

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("Key").SetType(NScheme::NTypeIds::Uint64).SetNullable(false),
            TTestHelper::TColumnSchema()
                .SetName("Value1")
                .SetType(NScheme::NTypeIds::String)
                .SetNullable(true)
                .SetColumnFamilyName(families[1].GetFamilyName()),
            TTestHelper::TColumnSchema()
                .SetName("Value2")
                .SetType(NScheme::NTypeIds::Uint32)
                .SetNullable(true)
                .SetColumnFamilyName(families[1].GetFamilyName())
        };

        TTestHelper::TColumnTable testTable;
        testTable.SetName(tableName).SetPrimaryKey({ "Key" }).SetSchema(schema).SetColumnFamilies(families);
        testHelper.CreateTable(testTable);

        auto session = testHelper.GetSession();
        {
            auto query = TStringBuilder() << R"(ALTER TABLE `)" << tableName << R"(`
                        ADD FAMILY family1 (COMPRESSION = "snappy")";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
        }

        {
            auto query = TStringBuilder() << R"(ALTER TABLE `)" << tableName << R"(`
                        ADD FAMILY family1 (COMPRESSION = "lz4", COMPRESSION_LEVEL = 5)";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(TwoSimilarColumnFamilies) {
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper testHelper(TKikimrSettings().SetWithSampleTables(false));

        TString tableName = "/Root/TableWithFamily";
        TTestHelper::TCompression plainCompression =
            TTestHelper::TCompression().SetCompressionType(NKikimrSchemeOp::EColumnCodec::ColumnCodecPlain);
        TTestHelper::TCompression lz4Compression = TTestHelper::TCompression().SetCompressionType(NKikimrSchemeOp::EColumnCodec::ColumnCodecLZ4);

        TVector<TTestHelper::TColumnFamily> families = {
            TTestHelper::TColumnFamily().SetId(0).SetFamilyName("default").SetCompression(plainCompression),
            TTestHelper::TColumnFamily().SetId(1).SetFamilyName("family1").SetCompression(lz4Compression),
            TTestHelper::TColumnFamily().SetId(2).SetFamilyName("family1").SetCompression(lz4Compression),
        };

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("Key").SetType(NScheme::NTypeIds::Uint64).SetNullable(false),
            TTestHelper::TColumnSchema()
                .SetName("Value1")
                .SetType(NScheme::NTypeIds::String)
                .SetNullable(true)
                .SetColumnFamilyName(families[1].GetFamilyName()),
            TTestHelper::TColumnSchema()
                .SetName("Value2")
                .SetType(NScheme::NTypeIds::Uint32)
                .SetNullable(true)
                .SetColumnFamilyName(families[1].GetFamilyName())
        };

        TTestHelper::TColumnTable testTable;
        testTable.SetName(tableName).SetPrimaryKey({ "Key" }).SetSchema(schema).SetColumnFamilies(families);
        testHelper.CreateTable(testTable, EStatus::GENERIC_ERROR);
    }

    Y_UNIT_TEST(CreateTableStoreWithFamily) {
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper testHelper(TKikimrSettings().SetWithSampleTables(false));

        TString tableName = "/Root/TableStoreWithColumnFamily";
        TTestHelper::TCompression plainCompression =
            TTestHelper::TCompression().SetCompressionType(NKikimrSchemeOp::EColumnCodec::ColumnCodecPlain);
        TTestHelper::TCompression zstdCompression =
            TTestHelper::TCompression().SetCompressionType(NKikimrSchemeOp::EColumnCodec::ColumnCodecZSTD).SetCompressionLevel(1);
        TTestHelper::TCompression lz4Compression = TTestHelper::TCompression().SetCompressionType(NKikimrSchemeOp::EColumnCodec::ColumnCodecLZ4);

        TVector<TTestHelper::TColumnFamily> families = {
            TTestHelper::TColumnFamily().SetId(0).SetFamilyName("default").SetCompression(plainCompression),
            TTestHelper::TColumnFamily().SetId(1).SetFamilyName("family1").SetCompression(zstdCompression),
            TTestHelper::TColumnFamily().SetId(2).SetFamilyName("family2").SetCompression(lz4Compression),
        };

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("Key").SetType(NScheme::NTypeIds::Uint64).SetNullable(false),
            TTestHelper::TColumnSchema()
                .SetName("Value1")
                .SetType(NScheme::NTypeIds::String)
                .SetNullable(true)
                .SetColumnFamilyName(families[1].GetFamilyName()),
            TTestHelper::TColumnSchema()
                .SetName("Value2")
                .SetType(NScheme::NTypeIds::Uint32)
                .SetNullable(true)
                .SetColumnFamilyName(families[2].GetFamilyName())
        };

        TTestHelper::TColumnTableStore testTable;
        testTable.SetName(tableName).SetPrimaryKey({ "Key" }).SetSchema(schema).SetColumnFamilies(families);
        testHelper.CreateTable(testTable, EStatus::GENERIC_ERROR);
    }

    Y_UNIT_TEST(CreateTableWithDefaultFamilyWithoutSettings) {
        TTestHelper testHelper(TKikimrSettings().SetWithSampleTables(false));
        TString tableName = "/Root/ColumnTableTest";
        auto session = testHelper.GetSession();
        auto createQuery = TStringBuilder() << R"(CREATE TABLE `)" << tableName << R"(` (
            Key Uint64 NOT NULL,
            Value1 String,
            Value2 Uint32,
            PRIMARY KEY (Key),
            FAMILY default ())
            WITH (STORE = COLUMN);)";
        auto result = session.ExecuteSchemeQuery(createQuery).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto& runner = testHelper.GetKikimr();
        auto runtime = runner.GetTestServer().GetRuntime();
        TActorId sender = runtime->AllocateEdgeActor();

        auto describeResult = DescribeTable(&runner.GetTestServer(), sender, tableName);
        auto schema = describeResult.GetPathDescription().GetColumnTableDescription().GetSchema();
        TTestHelper::TColumnFamily defaultFamily = TTestHelper::TColumnFamily().SetId(0).SetFamilyName("default");

        UNIT_ASSERT_EQUAL(schema.ColumnFamiliesSize(), 1);
        TTestHelper::TColumnFamily defaultFromScheme;
        UNIT_ASSERT(defaultFromScheme.DeserializeFromProto(schema.GetColumnFamilies(0)));
        {
            TString errorMessage;
            UNIT_ASSERT_C(defaultFromScheme.IsEqual(defaultFamily, errorMessage), errorMessage);
        }
    }

    Y_UNIT_TEST(CreateTableWithFamilyWithOnlyCompressionLevel) {
        TTestHelper testHelper(TKikimrSettings().SetWithSampleTables(false));
        TString tableName = "/Root/ColumnTableTest";
        auto session = testHelper.GetSession();
        auto createQuery = TStringBuilder() << R"(CREATE TABLE `)" << tableName << R"(` (
            Key Uint64 NOT NULL,
            Value1 String,
            Value2 Uint32,
            PRIMARY KEY (Key),
            FAMILY family1 (
                COMPRESSION_LEVEL = 2
            ))
            WITH (STORE = COLUMN);)";
        auto result = session.ExecuteSchemeQuery(createQuery).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
    }

    Y_UNIT_TEST(CreateTableNonDefaultFamilyWithoutCompression) {
        TTestHelper testHelper(TKikimrSettings().SetWithSampleTables(false));
        TString tableName = "/Root/ColumnTableTest";
        auto session = testHelper.GetSession();
        auto createQuery = TStringBuilder() << R"(CREATE TABLE `)" << tableName << R"(` (
            Key Uint64 NOT NULL,
            Value1 String,
            Value2 Uint32,
            PRIMARY KEY (Key),
            FAMILY family1 (
            ))
            WITH (STORE = COLUMN);)";
        auto result = session.ExecuteSchemeQuery(createQuery).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
    }

    Y_UNIT_TEST(DropColumnAndResetTtl) {
        auto runnerSettings = TKikimrSettings()
            .SetColumnShardAlterObjectEnabled(true)
            .SetWithSampleTables(false);
        TTestHelper testHelper(runnerSettings);

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("timestamp").SetType(NScheme::NTypeIds::Timestamp).SetNullable(false)
        };

        TTestHelper::TColumnTable testTable;
        testTable.SetName("/Root/ColumnTableTest").SetPrimaryKey({"id"}).SetSharding({"id"}).SetSchema(schema);
        testHelper.CreateTable(testTable);

        {
            auto alterQuery = TStringBuilder() << R"(
            --!syntax_v1
            ALTER OBJECT `)" << testTable.GetName() << R"(` (TYPE TABLE) SET (ACTION=UPSERT_INDEX,
                NAME=max_pk_int, TYPE=MAX, FEATURES=`{\"column_name\": \"timestamp\"}`))";
            auto alterResult = testHelper.GetSession().ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }

        {
            auto alterQuery = TStringBuilder() << "ALTER TABLE `" << testTable.GetName() << "`SET (TTL = Interval(\"PT1H\") ON timestamp);";
            auto alterResult = testHelper.GetSession().ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }

        {
            auto alterQuery = TStringBuilder() << "ALTER TABLE `" << testTable.GetName() << "` DROP COLUMN timestamp, RESET (TTL);";
            auto alterResult = testHelper.GetSession().ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(InitTtlSettingsOnShardStart) {
        auto runnerSettings = TKikimrSettings()
            .SetColumnShardAlterObjectEnabled(true)
            .SetWithSampleTables(false);
        TTestHelper testHelper(runnerSettings);

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("timestamp").SetType(NScheme::NTypeIds::Timestamp).SetNullable(false)
        };

        TTestHelper::TColumnTable testTable;
        testTable.SetName("/Root/ColumnTableTest").SetPrimaryKey({"id"}).SetSharding({"id"}).SetSchema(schema);
        testHelper.CreateTable(testTable);

        {
            auto alterQuery = TStringBuilder() << R"(
            --!syntax_v1
            ALTER OBJECT `)" << testTable.GetName() << R"(` (TYPE TABLE) SET (ACTION=UPSERT_INDEX,
                NAME=max_pk_int, TYPE=MAX, FEATURES=`{\"column_name\": \"timestamp\"}`))";
            auto alterResult = testHelper.GetSession().ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }

        {
            auto alterQuery = TStringBuilder() << "ALTER TABLE `" << testTable.GetName() << "`SET (TTL = Interval(\"PT1H\") ON timestamp);";
            auto alterResult = testHelper.GetSession().ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }

        {
            auto alterQuery = TStringBuilder() << "ALTER TABLE `" << testTable.GetName() << "` RESET (TTL);";
            auto alterResult = testHelper.GetSession().ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }

        {
            auto alterQuery = TStringBuilder() << "ALTER TABLE `" << testTable.GetName() << "` DROP COLUMN timestamp;";
            auto alterResult = testHelper.GetSession().ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }

        testHelper.RebootTablets("/Root/ColumnTableTest");
    }
}

Y_UNIT_TEST_SUITE(KqpOlapTypes) {

    Y_UNIT_TEST(Timestamp) {
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;

        TTestHelper testHelper(runnerSettings);

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int64).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("timestamp").SetType(NScheme::NTypeIds::Timestamp).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("ui64_type").SetType(NScheme::NTypeIds::Uint64).SetNullable(false)
        };

        TTestHelper::TColumnTable testTable;
        testTable.SetName("/Root/ColumnTableTest").SetPrimaryKey({"id"}).SetSharding({"id"}).SetSchema(schema);
        testHelper.CreateTable(testTable);

        auto ts = TInstant::Now();
        {
            TTestHelper::TUpdatesBuilder tableInserter(testTable.GetArrowSchema(schema));
            tableInserter.AddRow().Add(1).Add(ts.MicroSeconds()).Add(ts.MicroSeconds());
            testHelper.BulkUpsert(testTable, tableInserter);
        }
        testHelper.ReadData("SELECT * FROM `/Root/ColumnTableTest` WHERE id=1", TStringBuilder() << "[[1;" << ts.MicroSeconds() << "u;" << ts.MicroSeconds() << "u]]");
    }

    Y_UNIT_TEST(Decimal) {
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;

        TTestHelper testHelper(runnerSettings);

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int64).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("dec").SetType(NScheme::TDecimalType::Default()).SetNullable(false),
        };

        TTestHelper::TColumnTable testTable;
        testTable.SetName("/Root/ColumnTableTest").SetPrimaryKey({"id", "dec"}).SetSharding({"id", "dec"}).SetSchema(schema);
        testHelper.CreateTable(testTable);

        {
            TValueBuilder builder;
            builder.BeginList();
            builder.AddListItem().BeginStruct()
                .AddMember("id").Int64(1)
                .AddMember("dec").Decimal(TDecimalValue("10.1", 22, 9))
            .EndStruct();
            builder.AddListItem().BeginStruct()
                .AddMember("id").Int64(2)
                .AddMember("dec").Decimal(TDecimalValue("inf", 22, 9))
            .EndStruct();
            builder.AddListItem().BeginStruct()
                .AddMember("id").Int64(3)
                .AddMember("dec").Decimal(TDecimalValue("-inf", 22, 9))
            .EndStruct();
            builder.AddListItem().BeginStruct()
                .AddMember("id").Int64(4)
                .AddMember("dec").Decimal(TDecimalValue("nan", 22, 9))
            .EndStruct();
            builder.AddListItem().BeginStruct()
                .AddMember("id").Int64(5)
                .AddMember("dec").Decimal(TDecimalValue("-nan", 22, 9))
            .EndStruct();
            builder.AddListItem().BeginStruct()
                .AddMember("id").Int64(6)
                .AddMember("dec").Decimal(TDecimalValue("1.1", 22, 9))
            .EndStruct();
            builder.AddListItem().BeginStruct()
                .AddMember("id").Int64(7)
                .AddMember("dec").Decimal(TDecimalValue("12.1", 22, 9))
            .EndStruct();
            builder.AddListItem().BeginStruct()
                .AddMember("id").Int64(8)
                .AddMember("dec").Decimal(TDecimalValue("inf", 22, 9))
            .EndStruct();
            builder.AddListItem().BeginStruct()
                .AddMember("id").Int64(9)
                .AddMember("dec").Decimal(TDecimalValue("-inf", 22, 9))
            .EndStruct();
            builder.AddListItem().BeginStruct()
                .AddMember("id").Int64(10)
                .AddMember("dec").Decimal(TDecimalValue("2.1", 22, 9))
            .EndStruct();
            builder.AddListItem().BeginStruct()
                .AddMember("id").Int64(11)
                .AddMember("dec").Decimal(TDecimalValue("15.1", 22, 9))
            .EndStruct();
            builder.EndList();
            const auto result = testHelper.GetKikimr().GetTableClient().BulkUpsert(testTable.GetName(), builder.Build()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess() , result.GetIssues().ToString());
        }
        testHelper.ReadData("SELECT dec FROM `/Root/ColumnTableTest` WHERE id=1", "[[\"10.1\"]]");
        testHelper.ReadData("SELECT dec FROM `/Root/ColumnTableTest` WHERE id=2", "[[\"inf\"]]");
        testHelper.ReadData("SELECT dec FROM `/Root/ColumnTableTest` WHERE id=3", "[[\"-inf\"]]");
        testHelper.ReadData("SELECT dec FROM `/Root/ColumnTableTest` WHERE id=4", "[[\"nan\"]]");
        testHelper.ReadData("SELECT dec FROM `/Root/ColumnTableTest` WHERE id=5", "[[\"nan\"]]");
        testHelper.ReadData("SELECT id FROM `/Root/ColumnTableTest` WHERE dec=CAST(\"10.1\" As Decimal(22,9))", "[[1]]");
        testHelper.ReadData("SELECT id FROM `/Root/ColumnTableTest` WHERE dec=CAST(\"inf\" As Decimal(22,9)) ORDER BY id", "[[2];[8]]");
        testHelper.ReadData("SELECT id FROM `/Root/ColumnTableTest` WHERE dec=CAST(\"-inf\" As Decimal(22,9)) ORDER BY id", "[[3];[9]]");
        // Nan cannot by find.
        testHelper.ReadData("SELECT id FROM `/Root/ColumnTableTest` WHERE dec=CAST(\"nan\" As Decimal(22,9))", "[]");
        testHelper.ReadData("SELECT id FROM `/Root/ColumnTableTest` WHERE dec=CAST(\"-nan\" As Decimal(22,9))", "[]");
        testHelper.ReadData("SELECT dec FROM `/Root/ColumnTableTest` WHERE id > 5 ORDER BY dec", "[[\"-inf\"];[\"1.1\"];[\"2.1\"];[\"12.1\"];[\"15.1\"];[\"inf\"]]");
    }

    Y_UNIT_TEST(Decimal35) {
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;

        TTestHelper testHelper(runnerSettings);

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int64).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("dec").SetType(NScheme::TDecimalType(35, 10)).SetNullable(false),
        };

        TTestHelper::TColumnTable testTable;
        testTable.SetName("/Root/ColumnTableTest").SetPrimaryKey({"id", "dec"}).SetSharding({"id", "dec"}).SetSchema(schema);
        testHelper.CreateTable(testTable);

        {
            TValueBuilder builder;
            builder.BeginList();
            builder.AddListItem().BeginStruct()
                .AddMember("id").Int64(1)
                .AddMember("dec").Decimal(TDecimalValue("1055555555555555.1", 35, 10))
            .EndStruct();
            builder.AddListItem().BeginStruct()
                .AddMember("id").Int64(2)
                .AddMember("dec").Decimal(TDecimalValue("inf", 35, 10))
            .EndStruct();
            builder.AddListItem().BeginStruct()
                .AddMember("id").Int64(3)
                .AddMember("dec").Decimal(TDecimalValue("-inf", 35, 10))
            .EndStruct();
            builder.AddListItem().BeginStruct()
                .AddMember("id").Int64(4)
                .AddMember("dec").Decimal(TDecimalValue("nan", 35, 10))
            .EndStruct();
            builder.AddListItem().BeginStruct()
                .AddMember("id").Int64(5)
                .AddMember("dec").Decimal(TDecimalValue("-nan", 35, 10))
            .EndStruct();
            builder.AddListItem().BeginStruct()
                .AddMember("id").Int64(6)
                .AddMember("dec").Decimal(TDecimalValue("155555555555555.1", 35, 10))
            .EndStruct();
            builder.AddListItem().BeginStruct()
                .AddMember("id").Int64(7)
                .AddMember("dec").Decimal(TDecimalValue("1255555555555555.1", 35, 10))
            .EndStruct();
            builder.AddListItem().BeginStruct()
                .AddMember("id").Int64(8)
                .AddMember("dec").Decimal(TDecimalValue("inf", 35, 10))
            .EndStruct();
            builder.AddListItem().BeginStruct()
                .AddMember("id").Int64(9)
                .AddMember("dec").Decimal(TDecimalValue("-inf", 35, 10))
            .EndStruct();
            builder.AddListItem().BeginStruct()
                .AddMember("id").Int64(10)
                .AddMember("dec").Decimal(TDecimalValue("255555555555555.1", 35, 10))
            .EndStruct();
            builder.AddListItem().BeginStruct()
                .AddMember("id").Int64(11)
                .AddMember("dec").Decimal(TDecimalValue("1555555555555555.1", 35, 10))
            .EndStruct();
            builder.EndList();
            const auto result = testHelper.GetKikimr().GetTableClient().BulkUpsert(testTable.GetName(), builder.Build()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess() , result.GetIssues().ToString());
        }
        testHelper.ReadData("SELECT dec FROM `/Root/ColumnTableTest` WHERE id=1", "[[\"1055555555555555.1\"]]");
        testHelper.ReadData("SELECT dec FROM `/Root/ColumnTableTest` WHERE id=2", "[[\"inf\"]]");
        testHelper.ReadData("SELECT dec FROM `/Root/ColumnTableTest` WHERE id=3", "[[\"-inf\"]]");
        testHelper.ReadData("SELECT dec FROM `/Root/ColumnTableTest` WHERE id=4", "[[\"nan\"]]");
        testHelper.ReadData("SELECT dec FROM `/Root/ColumnTableTest` WHERE id=5", "[[\"nan\"]]");
        testHelper.ReadData("SELECT id FROM `/Root/ColumnTableTest` WHERE dec=CAST(\"1055555555555555.1\" As Decimal(35, 10))", "[[1]]");
        testHelper.ReadData("SELECT id FROM `/Root/ColumnTableTest` WHERE dec=CAST(\"inf\" As Decimal(35, 10)) ORDER BY id", "[[2];[8]]");
        testHelper.ReadData("SELECT id FROM `/Root/ColumnTableTest` WHERE dec=CAST(\"-inf\" As Decimal(35, 10)) ORDER BY id", "[[3];[9]]");
        // Nan cannot by find.
        testHelper.ReadData("SELECT id FROM `/Root/ColumnTableTest` WHERE dec=CAST(\"nan\" As Decimal(35, 10))", "[]");
        testHelper.ReadData("SELECT id FROM `/Root/ColumnTableTest` WHERE dec=CAST(\"-nan\" As Decimal(35, 10))", "[]");
        testHelper.ReadData("SELECT dec FROM `/Root/ColumnTableTest` WHERE id > 5 ORDER BY dec", "[[\"-inf\"];[\"155555555555555.1\"];[\"255555555555555.1\"];[\"1255555555555555.1\"];[\"1555555555555555.1\"];[\"inf\"]]");
    }

    Y_UNIT_TEST(DecimalCsv) {
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;

        TTestHelper testHelper(runnerSettings);

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int64).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("dec").SetType(NScheme::TDecimalType::Default()).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("dec35").SetType(NScheme::TDecimalType(35, 10)).SetNullable(false),
        };

        TTestHelper::TColumnTable testTable;
        testTable.SetName("/Root/ColumnTableTest").SetPrimaryKey({"id", "dec", "dec35"}).SetSharding({"id", "dec", "dec35"}).SetSchema(schema);
        testHelper.CreateTable(testTable);

        {
            TStringBuilder builder;
            builder << "1, 10.1, 1055555555555555.1" << Endl;
            builder << "6, 1.1, 155555555555555.1" << Endl;
            builder << "7, 12.1, 1255555555555555.1" << Endl;
            builder << "10, 2, 255555555555555" << Endl;
            builder << "11, 15.1, 1555555555555555.1" << Endl;
            const auto result = testHelper.GetKikimr().GetTableClient().BulkUpsert(testTable.GetName(), EDataFormat::CSV, builder).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess() , result.GetIssues().ToString());
        }
        testHelper.ReadData("SELECT dec FROM `/Root/ColumnTableTest` WHERE id=1", "[[\"10.1\"]]");
        testHelper.ReadData("SELECT id FROM `/Root/ColumnTableTest` WHERE dec=CAST(\"10.1\" As Decimal(22,9))", "[[1]]");
        testHelper.ReadData("SELECT dec FROM `/Root/ColumnTableTest` WHERE id > 5 ORDER BY dec", "[[\"1.1\"];[\"2\"];[\"12.1\"];[\"15.1\"]]");
        testHelper.ReadData("SELECT dec35 FROM `/Root/ColumnTableTest` WHERE id=1", "[[\"1055555555555555.1\"]]");
        testHelper.ReadData("SELECT id FROM `/Root/ColumnTableTest` WHERE dec35=CAST(\"1055555555555555.1\" As Decimal(35, 10))", "[[1]]");
        testHelper.ReadData("SELECT dec35 FROM `/Root/ColumnTableTest` WHERE id > 5 ORDER BY dec35", "[[\"155555555555555.1\"];[\"255555555555555\"];[\"1255555555555555.1\"];[\"1555555555555555.1\"]]");
    }

    Y_UNIT_TEST(TimestampCmpErr) {
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;

        TTestHelper testHelper(runnerSettings);

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int64).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("timestamp").SetType(NScheme::NTypeIds::Timestamp).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("timestamp_max").SetType(NScheme::NTypeIds::Timestamp).SetNullable(false)
        };

        TTestHelper::TColumnTable testTable;
        testTable.SetName("/Root/ColumnTableTest").SetPrimaryKey({"id"}).SetSharding({"id"}).SetSchema(schema);
        testHelper.CreateTable(testTable);

        auto ts = TInstant::Max();
        auto now = TInstant::Now();
        {
            TTestHelper::TUpdatesBuilder tableInserter(testTable.GetArrowSchema(schema));
            tableInserter.AddRow().Add(1).Add(ts.MicroSeconds()).Add(now.MicroSeconds());
            testHelper.BulkUpsert(testTable, tableInserter);
        }
        testHelper.ReadData("SELECT timestamp < timestamp_max FROM `/Root/ColumnTableTest` WHERE id=1", "[[\%false]]");
    }

    Y_UNIT_TEST(AttributeNegative) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        {
            auto result = session.AlterTable("/Root", TAlterTableSettings()
                .BeginAlterAttributes()
                    .Add("key", "value")
                .EndAlterAttributes()
            ).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SCHEME_ERROR, result.GetIssues().ToString());
        }

        {
            auto result = session.AlterTable("/Root", TAlterTableSettings()
                .BeginAlterAttributes()
                    .Alter("key", "value")
                .EndAlterAttributes()
            ).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SCHEME_ERROR, result.GetIssues().ToString());
        }

        {
            auto result = session.AlterTable("/Root", TAlterTableSettings()
                .BeginAlterAttributes()
                    .Drop("key")
                .EndAlterAttributes()
            ).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SCHEME_ERROR, result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(JsonImport) {
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;

        TTestHelper testHelper(runnerSettings);

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int64).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("json").SetType(NScheme::NTypeIds::Json).SetNullable(true),
            TTestHelper::TColumnSchema().SetName("json_doc").SetType(NScheme::NTypeIds::JsonDocument).SetNullable(true),
        };

        TTestHelper::TColumnTable testTable;
        testTable.SetName("/Root/ColumnTableTest").SetPrimaryKey({ "id" }).SetSharding({ "id" }).SetSchema(schema);
        testHelper.CreateTable(testTable);
        std::string jsonString = R"({"col1": "val1", "obj": {"obj_col2_int": 16}})";
        auto maybeJsonDoc = NBinaryJson::SerializeToBinaryJson(jsonString);
        Y_ABORT_UNLESS(std::holds_alternative<NBinaryJson::TBinaryJson>(maybeJsonDoc));
        const auto& value = std::get<NBinaryJson::TBinaryJson>(maybeJsonDoc);
        const std::string jsonBin(value.Data(), value.Size());
        {
            TTestHelper::TUpdatesBuilder tableInserter(testTable.GetArrowSchema(schema));
            tableInserter.AddRow().Add(1).AddNull().Add(jsonString);
            tableInserter.AddRow().Add(2).Add(jsonString).Add(jsonBin);
            testHelper.BulkUpsert(testTable, tableInserter);
        }
    }
}

} // namespace NKqp
} // namespace NKikimr

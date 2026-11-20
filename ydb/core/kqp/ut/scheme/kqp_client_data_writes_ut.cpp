#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/core/grpc_services/service_table.h>
#include <ydb/core/grpc_services/service_yql_scripting.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/public/api/protos/ydb_scripting.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/import/import.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_scripting.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/operation/operation.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

namespace NKikimr::NKqp {
namespace {

TKikimrSettings Settings(bool enableClientModifications) {
    TKikimrSettings settings;
    settings.SetWithSampleTables(false);
    settings.FeatureFlags.SetEnableClientModifications(enableClientModifications);
    settings.FeatureFlags.SetEnableTruncateColumnTable(true);
    return settings;
}

void CheckWrite(const NYdb::TStatus& status, bool expectWriteSuccess) {
    UNIT_ASSERT_VALUES_EQUAL_C(status.GetStatus(),
        expectWriteSuccess ? NYdb::EStatus::SUCCESS : NYdb::EStatus::PRECONDITION_FAILED,
        status.GetIssues().ToString());

    if (!expectWriteSuccess) {
        UNIT_ASSERT_STRING_CONTAINS(status.GetIssues().ToString(), "Client data writes are disabled");
    }
}

void CreateTable(NYdb::NTable::TSession& session, bool column = false) {
    const TString sql = TStringBuilder()
        << "CREATE TABLE `/Root/Test` (Key Uint64 NOT NULL, Value Utf8, PRIMARY KEY(Key))"
        << (column ? " PARTITION BY HASH(Key) WITH (STORE = COLUMN)" : "");
    auto result = session.ExecuteSchemeQuery(sql).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}

} // namespace

Y_UNIT_TEST_SUITE(ClientDataWrites) {

    Y_UNIT_TEST_QUAD(SchemaModifications, enableClientModifications, admin) {
        auto settings = Settings(enableClientModifications);
        settings.SetAuthToken("root@builtin");
        TKikimrRunner kikimr(settings);

        // Grant normal schema permissions so that rejection comes from the feature flag.
        kikimr.GetTestClient().SetSecurityToken("root@builtin");
        kikimr.GetTestClient().TestGrant("/", "Root", "user@builtin", NACLib::EAccessRights::GenericFull);

        auto rootClient = kikimr.GetTableClient();
        auto rootSession = rootClient.CreateSession().GetValueSync().GetSession();

        for (const TString& name : {"SchemeSql", "QuerySql", "Rpc", "Data"}) {
            auto result = rootSession.ExecuteSchemeQuery(TStringBuilder()
                << "CREATE TABLE `/Root/" << name << "` (Key Uint64, PRIMARY KEY(Key))").GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        NYdb::NTopic::TTopicClient rootTopics(kikimr.GetDriver());
        auto topic = rootTopics.CreateTopic("/Root/ExistingTopic").GetValueSync();
        UNIT_ASSERT_C(topic.IsSuccess(), topic.GetIssues().ToString());

        NYdb::TDriver driver(NYdb::TDriverConfig().SetEndpoint(kikimr.GetEndpoint())
            .SetDatabase("/Root").SetAuthToken(admin ? "root@builtin" : "user@builtin"));
        NYdb::NTable::TTableClient tables(driver);
        NYdb::NQuery::TQueryClient queries(driver);
        NYdb::NScheme::TSchemeClient scheme(driver);
        NYdb::NTopic::TTopicClient topics(driver);
        auto session = tables.CreateSession().GetValueSync().GetSession();
        const bool allowed = enableClientModifications || admin;

        auto check = [&](const NYdb::TStatus& result) {
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), allowed
                ? NYdb::EStatus::SUCCESS : NYdb::EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());

            if (!allowed) {
                UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(),
                    "Client schema modifications are disabled for non-administrators");
            }
        };

        for (bool generic : {false, true}) {
            const TString name = generic ? "QuerySql" : "SchemeSql";

            for (const TString& query : TVector<TString>{
                TStringBuilder() << "CREATE TABLE `/Root/" << name << "New` (Key Uint64, PRIMARY KEY(Key))",
                TStringBuilder() << "ALTER TABLE `/Root/" << name << "` ADD COLUMN Value String",
                TStringBuilder() << "TRUNCATE TABLE `/Root/" << name << "`",
                TStringBuilder() << "DROP TABLE `/Root/" << name << "`"})
            {
                if (generic) {
                    check(queries.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).GetValueSync());
                } else {
                    check(session.ExecuteSchemeQuery(query).GetValueSync());
                }
            }
        }

        check(session.CreateTable("/Root/RpcNew", NYdb::NTable::TTableBuilder()
            .AddNullableColumn("Key", NYdb::EPrimitiveType::Uint64).SetPrimaryKeyColumn("Key").Build()).GetValueSync());

        auto type = NYdb::TTypeBuilder().BeginOptional().Primitive(NYdb::EPrimitiveType::String).EndOptional().Build();
        check(session.AlterTable("/Root/Rpc", NYdb::NTable::TAlterTableSettings()
            .AppendAddColumns(NYdb::TColumn{"Value", type})).GetValueSync());
        check(session.DropTable("/Root/Rpc").GetValueSync());
        check(scheme.MakeDirectory("/Root/NewDirectory").GetValueSync());
        check(scheme.ModifyPermissions("/Root/ExistingTopic", NYdb::NScheme::TModifyPermissionsSettings()
            .AddGrantPermissions(NYdb::NScheme::TPermissions("reader@builtin", {"ydb.generic.read"}))).GetValueSync());
        check(topics.CreateTopic("/Root/NewTopic").GetValueSync());
        check(topics.AlterTopic("/Root/ExistingTopic", NYdb::NTopic::TAlterTopicSettings()
            .BeginAddConsumer("consumer").EndAddConsumer()).GetValueSync());

        // Describe operations remain available regardless of the flag or administrator status.
        auto describe = topics.DescribeTopic("/Root/ExistingTopic").GetValueSync();
        UNIT_ASSERT_C(describe.IsSuccess(), describe.GetIssues().ToString());

        check(topics.DropTopic("/Root/ExistingTopic").GetValueSync());
        if (!allowed) {
            auto table = session.DescribeTable("/Root/Rpc").GetValueSync();
            UNIT_ASSERT_C(table.IsSuccess(), table.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(table.GetTableDescription().GetColumns().size(), 1);
            auto missing = scheme.DescribePath("/Root/RpcNew").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(missing.GetStatus(), NYdb::EStatus::SCHEME_ERROR);
        }

        // Administrator privileges only bypass the schema restriction.
        CheckWrite(session.ExecuteDataQuery("UPSERT INTO `/Root/Data` (Key) VALUES (1u)",
            NYdb::NTable::TTxControl::BeginTx().CommitTx()).GetValueSync(), enableClientModifications);
    }

    Y_UNIT_TEST_TWIN(CreateTableAs, enableClientModifications) {
        auto settings = Settings(enableClientModifications);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableDataShardCreateTableAs(true);

        TKikimrRunner kikimr(settings);
        auto client = kikimr.GetQueryClient();
        const auto querySettings = NYdb::NQuery::TExecuteQuerySettings().ClientTimeout(std::chrono::seconds(20));

        auto create = client.ExecuteQuery(R"(
            CREATE TABLE `/Root/Copy` (PRIMARY KEY(Key))
            AS SELECT 1ul AS Key, "one"u AS Value;
        )", NYdb::NQuery::TTxControl::NoTx(), querySettings).GetValueSync();
        UNIT_ASSERT_C(create.IsSuccess(), create.GetIssues().ToString());

        auto read = client.ExecuteQuery("SELECT COUNT(*) FROM `/Root/Copy`",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx(), querySettings).GetValueSync();
        UNIT_ASSERT_C(read.IsSuccess(), read.GetIssues().ToString());

        NYdb::TResultSetParser parser(read.GetResultSet(0));
        UNIT_ASSERT(parser.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser(0).GetUint64(), 1);

        CheckWrite(client.ExecuteQuery("UPSERT INTO `/Root/Copy` (Key, Value) VALUES (2ul, 'two')",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx(), querySettings).GetValueSync(), enableClientModifications);
    }

    Y_UNIT_TEST_TWIN(Sql, enableClientModifications) {
        TKikimrRunner kikimr(Settings(enableClientModifications));
        auto tableClient = kikimr.GetTableClient();
        auto session = tableClient.CreateSession().GetValueSync().GetSession();
        auto queryClient = kikimr.GetQueryClient();

        CreateTable(session);

        const TVector<TString> queries = {
            "INSERT INTO `/Root/Test` (Key, Value) VALUES (1u, 'one')",
            "UPSERT INTO `/Root/Test` (Key, Value) VALUES (2u, 'two')",
            "REPLACE INTO `/Root/Test` (Key, Value) VALUES (2u, 'new')",
            "UPDATE `/Root/Test` SET Value = 'updated' WHERE Key = 2u",
            "UPDATE `/Root/Test` ON SELECT 2ul AS Key, 'updated'u AS Value",
            "DELETE FROM `/Root/Test` ON SELECT 2ul AS Key",
            "DELETE FROM `/Root/Test` WHERE Key = 1u",
        };

        for (bool generic : {false, true}) {
            for (const auto& query : queries) {
                if (generic) {
                    CheckWrite(queryClient.ExecuteQuery(query,
                        NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync(), enableClientModifications);
                } else {
                    CheckWrite(session.ExecuteDataQuery(query,
                        NYdb::NTable::TTxControl::BeginTx().CommitTx()).GetValueSync(), enableClientModifications);
                }
            }
        }

        auto read = queryClient.ExecuteQuery("SELECT COUNT(*) FROM `/Root/Test`",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(read.IsSuccess(), read.GetIssues().ToString());

        NYdb::TResultSetParser parser(read.GetResultSet(0));
        UNIT_ASSERT(parser.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser(0).GetUint64(), 0);

        CheckWrite(session.ExecuteSchemeQuery("TRUNCATE TABLE `/Root/Test`").GetValueSync(), true);
        CheckWrite(queryClient.ExecuteQuery("TRUNCATE TABLE `/Root/Test`",
            NYdb::NQuery::TTxControl::NoTx()).GetValueSync(), true);

        auto alter = session.ExecuteSchemeQuery("ALTER TABLE `/Root/Test` ADD COLUMN Extra String").GetValueSync();
        UNIT_ASSERT_C(alter.IsSuccess(), alter.GetIssues().ToString());

        auto drop = session.ExecuteSchemeQuery("DROP TABLE `/Root/Test`").GetValueSync();
        UNIT_ASSERT_C(drop.IsSuccess(), drop.GetIssues().ToString());
    }

    Y_UNIT_TEST_TWIN(BatchAndColumnDml, enableClientModifications) {
        TKikimrRunner kikimr(Settings(enableClientModifications));
        auto tableClient = kikimr.GetTableClient();
        auto session = tableClient.CreateSession().GetValueSync().GetSession();
        auto queryClient = kikimr.GetQueryClient();

        CreateTable(session);
        for (const char* query : {"BATCH UPDATE `/Root/Test` SET Value = 'updated'", "BATCH DELETE FROM `/Root/Test`"}) {
            CheckWrite(queryClient.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).GetValueSync(), enableClientModifications);
        }

        auto drop = session.ExecuteSchemeQuery("DROP TABLE `/Root/Test`").GetValueSync();
        UNIT_ASSERT_C(drop.IsSuccess(), drop.GetIssues().ToString());

        CreateTable(session, true);
        CheckWrite(queryClient.ExecuteQuery("UPSERT INTO `/Root/Test` (Key, Value) VALUES (1u, 'one')",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync(), enableClientModifications);
        CheckWrite(session.ExecuteSchemeQuery("TRUNCATE TABLE `/Root/Test`").GetValueSync(), true);
    }

    Y_UNIT_TEST_QUAD(ScriptInternalCall, internalCall, streaming) {
        TKikimrRunner kikimr(Settings(false));
        auto client = kikimr.GetTableClient();
        auto session = client.CreateSession().GetValueSync().GetSession();

        CreateTable(session);
        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        const TString write = "UPSERT INTO `/Root/Test` (Key, Value) VALUES (1u, 'one');";
        const auto expected = internalCall ? Ydb::StatusIds::SUCCESS : Ydb::StatusIds::PRECONDITION_FAILED;

        if constexpr (streaming) {
            const auto sender = runtime.AllocateEdgeActor();
            auto request = MakeHolder<TEvKqp::TEvQueryRequest>();
            ActorIdToProto(sender, request->Record.MutableRequestActorId());

            auto* query = request->Record.MutableRequest();
            query->SetDatabase("/Root");
            query->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
            query->SetType(NKikimrKqp::QUERY_TYPE_SQL_SCRIPT_STREAMING);
            query->SetQuery(write);
            query->SetIsInternalCall(internalCall);
            runtime.Send(new IEventHandle(MakeKqpProxyID(runtime.GetNodeId()), sender, request.Release()));

            auto response = runtime.GrabEdgeEventRethrow<TEvKqp::TEvQueryResponse>(sender, TDuration::Seconds(30));
            UNIT_ASSERT(response);
            UNIT_ASSERT_VALUES_EQUAL_C(response->Get()->Record.GetYdbStatus(), expected,
                response->Get()->Record.ShortDebugString());
        } else {
            Ydb::Scripting::ExecuteYqlRequest request;
            request.set_script(write);

            using TEvExecute = NGRpcService::TGrpcRequestOperationCall<
                Ydb::Scripting::ExecuteYqlRequest, Ydb::Scripting::ExecuteYqlResponse>;
            auto response = NRpcService::DoLocalRpc<TEvExecute>(std::move(request), "/Root", Nothing(),
                runtime.GetActorSystem(0), internalCall).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(response.operation().status(), expected, response.ShortDebugString());
        }
    }

    Y_UNIT_TEST_TWIN(Scripts, enableClientModifications) {
        TKikimrRunner kikimr(Settings(enableClientModifications));
        auto tableClient = kikimr.GetTableClient();
        auto session = tableClient.CreateSession().GetValueSync().GetSession();

        CreateTable(session);

        NYdb::NScripting::TScriptingClient scripting(kikimr.GetDriver());
        const TString write = "UPSERT INTO `/Root/Test` (Key, Value) VALUES (1u, 'one');";
        CheckWrite(scripting.ExecuteYqlScript(write).GetValueSync(), enableClientModifications);

        auto read = scripting.ExecuteYqlScript("SELECT * FROM `/Root/Test`;").GetValueSync();
        UNIT_ASSERT_C(read.IsSuccess(), read.GetIssues().ToString());

        auto queryClient = kikimr.GetQueryClient();
        NYdb::NOperation::TOperationClient operations(kikimr.GetDriver());

        for (const TString& query : {TString("SELECT * FROM `/Root/Test`;"), write}) {
            auto op = queryClient.ExecuteScript(query).GetValueSync();
            const auto deadline = TInstant::Now() + TDuration::Seconds(30);

            while (!op.Ready() && op.Status().IsSuccess() && TInstant::Now() < deadline) {
                Sleep(TDuration::MilliSeconds(10));
                op = operations.Get<NYdb::NQuery::TScriptExecutionOperation>(op.Id()).GetValueSync();
            }

            UNIT_ASSERT_C(op.Ready() || !op.Status().IsSuccess(), "Script did not finish");
            CheckWrite(op.Status(), query != write || enableClientModifications);
        }
    }

    Y_UNIT_TEST_QUAD(BulkUpsert, enableClientModifications, column) {
        TKikimrRunner kikimr(Settings(enableClientModifications));
        auto client = kikimr.GetTableClient();
        auto session = client.CreateSession().GetValueSync().GetSession();

        CreateTable(session, column);

        auto rows = NYdb::TValueBuilder().BeginList().AddListItem().BeginStruct()
            .AddMember("Key").Uint64(1).AddMember("Value").Utf8("one")
            .EndStruct().EndList().Build();
        CheckWrite(client.BulkUpsert("/Root/Test", std::move(rows)).GetValueSync(), enableClientModifications);

        Ydb::Formats::CsvSettings csv;
        csv.set_header(true);
        CheckWrite(client.BulkUpsert("/Root/Test", NYdb::NTable::EDataFormat::CSV,
            "Key,Value\n2,two\n", {}, NYdb::NTable::TBulkUpsertSettings()
                .FormatSettings(csv.SerializeAsString())).GetValueSync(), enableClientModifications);

        auto schema = arrow::schema({arrow::field("Key", arrow::uint64(), false), arrow::field("Value", arrow::utf8())});
        auto builders = NArrow::MakeBuilders(schema, 1);
        UNIT_ASSERT(builders[0]->AppendScalar(arrow::UInt64Scalar(3)).ok());
        UNIT_ASSERT(builders[1]->AppendScalar(arrow::StringScalar("three")).ok());

        auto batch = arrow::RecordBatch::Make(schema, 1, NArrow::Finish(std::move(builders)));
        CheckWrite(client.BulkUpsert("/Root/Test", NYdb::NTable::EDataFormat::ApacheArrow,
            NArrow::SerializeBatchNoCompression(batch), NArrow::SerializeSchema(*schema)).GetValueSync(), enableClientModifications);
    }

    Y_UNIT_TEST_TWIN(LocalBulkUpsert, enableClientModifications) {
        TKikimrRunner kikimr(Settings(enableClientModifications));
        auto client = kikimr.GetTableClient();
        auto session = client.CreateSession().GetValueSync().GetSession();

        CreateTable(session);

        Ydb::Table::BulkUpsertRequest request;
        request.set_table("/Root/Test");
        request.mutable_csv_settings()->set_header(true);
        request.set_data("Key,Value\n1,one\n");

        using TEvBulkUpsert = NGRpcService::TGrpcRequestOperationCall<Ydb::Table::BulkUpsertRequest, Ydb::Table::BulkUpsertResponse>;
        auto response = NRpcService::DoLocalRpc<TEvBulkUpsert>(
            std::move(request), "/Root", Nothing(), kikimr.GetTestServer().GetRuntime()->GetActorSystem(0)).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(response.operation().status(),
            enableClientModifications ? Ydb::StatusIds::SUCCESS : Ydb::StatusIds::PRECONDITION_FAILED);
    }

    Y_UNIT_TEST_TWIN(ImportData, enableClientModifications) {
        TKikimrRunner kikimr(Settings(enableClientModifications));
        auto client = kikimr.GetTableClient();
        auto session = client.CreateSession().GetValueSync().GetSession();

        CreateTable(session);

        NYdb::NImport::TImportClient import(kikimr.GetDriver());
        CheckWrite(import.ImportData("/Root/Test", TString("1,\"one\"\n"),
            NYdb::NImport::TImportYdbDumpDataSettings().AppendColumns("Key").AppendColumns("Value")).GetValueSync(), enableClientModifications);
    }
}
} // namespace NKikimr::NKqp

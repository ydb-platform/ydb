#include <arrow/api.h>
#include <google/protobuf/util/message_differencer.h>

#include <fmt/format.h>
#include <library/cpp/testing/gmock_in_unittest/gmock.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/system/env.h>

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/ut/federated_query/common/common.h>
#include <ydb/library/yql/providers/common/structured_token/yql_token_builder.h>
#include <ydb/library/yql/providers/generic/connector/libcpp/client.h>
#include <ydb/library/yql/providers/generic/connector/libcpp/external_data_source.h>
#include <ydb/public/sdk/cpp/client/ydb_operation/operation.h>
#include <ydb/public/sdk/cpp/client/ydb_query/query.h>
#include <ydb/public/sdk/cpp/client/ydb_types/status_codes.h>

namespace NKikimr::NKqp {
    using namespace NYdb;
    using namespace NYdb::NQuery;
    using namespace NYql::NConnector;
    using namespace NKikimr::NKqp::NFederatedQueryTest;
    using namespace testing;
    using namespace fmt::literals;

    class TConnectorClientMock: public IClient {
    public:
        MOCK_METHOD(TDescribeTableResult::TPtr, DescribeTable, (const NApi::TDescribeTableRequest& request), (override));
        MOCK_METHOD(TListSplitsResult::TPtr, ListSplits, (const NApi::TListSplitsRequest& request), (override));
        MOCK_METHOD(TReadSplitsResult::TPtr, ReadSplits, (const NApi::TReadSplitsRequest& request), (override));
    };

    MATCHER_P(ProtobufRequestMatcher, expected, "request does not match") {
        return google::protobuf::util::MessageDifferencer::Equals(arg, expected);
    }

    class TDatabaseAsyncResolverMock: public NYql::IDatabaseAsyncResolver {
    public:
        MOCK_METHOD(NThreading::TFuture<NYql::TDatabaseResolverResponse>, ResolveIds, (const TDatabaseAuthMap& ids), (const override));
    };

    MATCHER_P(DatabaseAuthMapMatcher, expected, "database auth map matcher") {
        return arg == expected;
    }

#define PREPARE_RECORD_BATCH(COLUMN_NAME, INPUT, BUILDER_TYPE, ARROW_TYPE, OUTPUT)     \
    {                                                                                  \
        arrow::BUILDER_TYPE builder;                                                   \
        UNIT_ASSERT_EQUAL(builder.AppendValues(INPUT), arrow::Status::OK());           \
        std::shared_ptr<arrow::Array> columnData;                                      \
        UNIT_ASSERT_EQUAL(builder.Finish(&columnData), arrow::Status::OK());           \
        auto field = arrow::field(COLUMN_NAME, ARROW_TYPE());                          \
        auto schema = arrow::schema({field});                                          \
        OUTPUT = arrow::RecordBatch::Make(schema, columnData->length(), {columnData}); \
    }

#define MATCH_RESULT_WITH_INPUT(INPUT, RESULT_SET, GETTER)                      \
    {                                                                           \
        for (const auto& val : INPUT) {                                         \
            UNIT_ASSERT(RESULT_SET.TryNextRow());                               \
            UNIT_ASSERT_VALUES_EQUAL(RESULT_SET.ColumnParser(0).GETTER(), val); \
        }                                                                       \
    }

    Y_UNIT_TEST_SUITE(GenericFederatedQuery) {
        Y_UNIT_TEST(PostgreSQLLocal) {
            // prepare mock
            auto clientMock = std::make_shared<TConnectorClientMock>();
            IClient::TPtr client = clientMock;

            // prepare common fields
            const TString host = "localhost";
            const int port = 5432;
            const TString databaseName = "dqrun";
            const TString login = "crab";
            const TString password = "qwerty12345";
            const TString tableName = "example_1";
            const bool useTls = true;
            const TString protocol = "NATIVE";
            const TString sourceType = "PostgreSQL";

            NApi::TDataSourceInstance dataSourceInstance;
            dataSourceInstance.set_database(databaseName);
            dataSourceInstance.mutable_credentials()->mutable_basic()->set_username(login);
            dataSourceInstance.mutable_credentials()->mutable_basic()->set_password(password);
            dataSourceInstance.mutable_endpoint()->set_host(host);
            dataSourceInstance.mutable_endpoint()->set_port(port);
            dataSourceInstance.set_use_tls(useTls);
            dataSourceInstance.set_kind(NYql::NConnector::NApi::EDataSourceKind::POSTGRESQL);
            dataSourceInstance.set_protocol(NYql::NConnector::NApi::EProtocol::NATIVE);

            // step 1: DescribeTable
            NApi::TDescribeTableRequest describeTableRequest;
            describeTableRequest.set_table(tableName);
            describeTableRequest.mutable_data_source_instance()->CopyFrom(dataSourceInstance);

            TDescribeTableResult::TPtr describeTableResult = std::make_shared<TDescribeTableResult>();
            describeTableResult->Error.set_status(::Ydb::StatusIds_StatusCode::StatusIds_StatusCode_SUCCESS);
            auto col1 = describeTableResult->Schema.add_columns();
            col1->set_name("col1");
            col1->mutable_type()->set_type_id(Ydb::Type::UINT16);

            EXPECT_CALL(*clientMock, DescribeTable(ProtobufRequestMatcher(describeTableRequest))).WillOnce(Return(describeTableResult));

            // step 2: ListSplits
            NApi::TListSplitsRequest listSplitsRequest;

            auto select = listSplitsRequest.add_selects();
            select->mutable_from()->set_table(tableName);
            select->mutable_data_source_instance()->CopyFrom(dataSourceInstance);
            auto item = select->mutable_what() -> add_items();
            item->mutable_column()->CopyFrom(*col1);

            TListSplitsResult::TPtr listSplitsResult = std::make_shared<TListSplitsResult>();
            listSplitsResult->Error.set_status(::Ydb::StatusIds_StatusCode::StatusIds_StatusCode_SUCCESS);
            NApi::TSplit split;
            split.mutable_select()->CopyFrom(*select);
            split.set_description("some binary description");
            listSplitsResult->Splits.emplace_back(std::move(split));

            EXPECT_CALL(*clientMock, ListSplits(ProtobufRequestMatcher(listSplitsRequest))).WillOnce(Return(listSplitsResult));

            // step 3: ReadSplits
            NApi::TReadSplitsRequest readSplitsRequest;
            readSplitsRequest.mutable_data_source_instance()->CopyFrom(dataSourceInstance);
            readSplitsRequest.add_splits()->CopyFrom(listSplitsResult->Splits[0]);
            readSplitsRequest.set_format(NApi::TReadSplitsRequest_EFormat::TReadSplitsRequest_EFormat_ARROW_IPC_STREAMING);

            TReadSplitsResult::TPtr readSplitsResult = std::make_shared<TReadSplitsResult>();
            readSplitsResult->Error.set_status(::Ydb::StatusIds_StatusCode::StatusIds_StatusCode_SUCCESS);
            readSplitsResult->RecordBatches.push_back({});
            std::vector<ui16> colData = {10, 20, 30, 40, 50};

            PREPARE_RECORD_BATCH(col1->name(), colData, UInt16Builder, arrow::uint16, readSplitsResult->RecordBatches[0]);

            EXPECT_CALL(*clientMock, ReadSplits(ProtobufRequestMatcher(readSplitsRequest))).WillOnce(Return(readSplitsResult));

            // run test
            auto kikimr = MakeKikimrRunner(nullptr, client);

            auto tc = kikimr->GetTableClient();
            auto session = tc.CreateSession().GetValueSync().GetSession();
            const TString query1 = fmt::format(
                R"(
                CREATE OBJECT pg_local_password (TYPE SECRET) WITH (value={password});

                CREATE EXTERNAL DATA SOURCE pg_local WITH (
                    SOURCE_TYPE="{source_type}",
                    LOCATION="{host}:{port}",
                    AUTH_METHOD="BASIC",
                    LOGIN="{login}",
                    PASSWORD_SECRET_NAME="pg_local_password",
                    USE_TLS="{use_tls}",
                    PROTOCOL="{protocol}"
                );
            )",
                "host"_a = host,
                "port"_a = port,
                "password"_a = password,
                "login"_a = login,
                "use_tls"_a = useTls ? "TRUE" : "FALSE",
                "protocol"_a = protocol,
                "source_type"_a = sourceType);
            auto result = session.ExecuteSchemeQuery(query1).GetValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

            const TString query2 = fmt::format(
                R"(
                SELECT * FROM pg_local.`{database_name}.{table_name}`;
            )",
                "database_name"_a = databaseName,
                "table_name"_a = tableName);

            auto db = kikimr->GetQueryClient();
            auto scriptExecutionOperation = db.ExecuteScript(query2).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::SUCCESS, scriptExecutionOperation.Status().GetIssues().ToString());
            UNIT_ASSERT(scriptExecutionOperation.Metadata().ExecutionId);

            NYdb::NQuery::TScriptExecutionOperation readyOp = WaitScriptExecutionOperation(scriptExecutionOperation.Id(), kikimr->GetDriver());
            UNIT_ASSERT_C(readyOp.Metadata().ExecStatus == EExecStatus::Completed, readyOp.Status().GetIssues().ToString());
            TFetchScriptResultsResult results = db.FetchScriptResults(scriptExecutionOperation.Id(), 0).ExtractValueSync();
            UNIT_ASSERT_C(results.IsSuccess(), results.GetIssues().ToString());

            TResultSetParser resultSet(results.ExtractResultSet());
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnsCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), colData.size());

            // check every row
            MATCH_RESULT_WITH_INPUT(colData, resultSet, GetUint16);
        }

        Y_UNIT_TEST(ClickHouseManaged) {
            // prepare connector client mock
            auto connectorClientMock = std::make_shared<TConnectorClientMock>();
            IClient::TPtr connectorClient = connectorClientMock;

            // prepare common fields
            const TString clusterId = "ch-managed";
            const TString hostName = "rc1a-d6dv17lv47v5mcop";
            const TString hostFqdn = hostName + ".db.yandex.net";
            const int port = 8443;
            const TString endpoint = TStringBuilder() << hostFqdn << ":" << ToString(port);
            const TString databaseName = "dqrun";
            const TString login = "crab";
            const TString password = "qwerty12345";
            const TString tableName = "example_1";
            const bool useTls = true;
            const auto protocol = NYql::NConnector::NApi::EProtocol::HTTP;
            const auto sourceType = NYql::NConnector::EExternalDataSource::ClickHouse;
            const auto dataSourceKind = NYql::NConnector::NApi::EDataSourceKind::CLICKHOUSE;
            const TString serviceAccountId = "sa";
            const TString serviceAccountIdSignature = "sa_signature";

            NApi::TDataSourceInstance dataSourceInstance;
            dataSourceInstance.set_database(databaseName);
            dataSourceInstance.mutable_credentials()->mutable_basic()->set_username(login);
            dataSourceInstance.mutable_credentials()->mutable_basic()->set_password(password);
            dataSourceInstance.mutable_endpoint()->set_host(hostFqdn);
            dataSourceInstance.mutable_endpoint()->set_port(port);
            dataSourceInstance.set_use_tls(useTls);
            dataSourceInstance.set_kind(dataSourceKind);
            dataSourceInstance.set_protocol(protocol);

            // step 1: DescribeTable
            NApi::TDescribeTableRequest describeTableRequest;
            describeTableRequest.set_table(tableName);
            describeTableRequest.mutable_data_source_instance()->CopyFrom(dataSourceInstance);

            TDescribeTableResult::TPtr describeTableResult = std::make_shared<TDescribeTableResult>();
            describeTableResult->Error.set_status(::Ydb::StatusIds_StatusCode::StatusIds_StatusCode_SUCCESS);
            auto col1 = describeTableResult->Schema.add_columns();
            col1->set_name("col1");
            col1->mutable_type()->set_type_id(Ydb::Type::UINT16);

            EXPECT_CALL(*connectorClientMock, DescribeTable(ProtobufRequestMatcher(describeTableRequest))).WillOnce(Return(describeTableResult));

            // step 2: ListSplits
            NApi::TListSplitsRequest listSplitsRequest;

            auto select = listSplitsRequest.add_selects();
            select->mutable_from()->set_table(tableName);
            select->mutable_data_source_instance()->CopyFrom(dataSourceInstance);
            auto item = select->mutable_what() -> add_items();
            item->mutable_column()->CopyFrom(*col1);

            TListSplitsResult::TPtr listSplitsResult = std::make_shared<TListSplitsResult>();
            listSplitsResult->Error.set_status(::Ydb::StatusIds_StatusCode::StatusIds_StatusCode_SUCCESS);
            NApi::TSplit split;
            split.mutable_select()->CopyFrom(*select);
            split.set_description("some binary description");
            listSplitsResult->Splits.emplace_back(std::move(split));

            EXPECT_CALL(*connectorClientMock, ListSplits(ProtobufRequestMatcher(listSplitsRequest))).WillOnce(Return(listSplitsResult));

            // step 3: ReadSplits
            NApi::TReadSplitsRequest readSplitsRequest;
            readSplitsRequest.mutable_data_source_instance()->CopyFrom(dataSourceInstance);
            readSplitsRequest.add_splits()->CopyFrom(listSplitsResult->Splits[0]);
            readSplitsRequest.set_format(NApi::TReadSplitsRequest_EFormat::TReadSplitsRequest_EFormat_ARROW_IPC_STREAMING);

            TReadSplitsResult::TPtr readSplitsResult = std::make_shared<TReadSplitsResult>();
            readSplitsResult->Error.set_status(::Ydb::StatusIds_StatusCode::StatusIds_StatusCode_SUCCESS);
            readSplitsResult->RecordBatches.push_back({});
            std::vector<ui16> colData = {10, 20, 30, 40, 50};

            PREPARE_RECORD_BATCH(col1->name(), colData, UInt16Builder, arrow::uint16, readSplitsResult->RecordBatches[0]);

            EXPECT_CALL(*connectorClientMock, ReadSplits(ProtobufRequestMatcher(readSplitsRequest))).WillOnce(Return(readSplitsResult));

            // prepare database resolver mock
            auto databaseAsyncResolverMock = std::make_shared<TDatabaseAsyncResolverMock>();
            NYql::IDatabaseAsyncResolver::TPtr databaseAsyncResolver = databaseAsyncResolverMock;

            NYql::IDatabaseAsyncResolver::TDatabaseAuthMap dbResolverReq;
            dbResolverReq[std::make_pair(clusterId, NYql::EDatabaseType::ClickHouse)] =
                NYql::TDatabaseAuth{
                    NYql::TStructuredTokenBuilder().SetServiceAccountIdAuth(serviceAccountId, serviceAccountIdSignature).ToJson(),
                    true,
                    true};

            NYql::TDatabaseResolverResponse::TDatabaseEndpointsMap databaseEndpointsMap;
            databaseEndpointsMap[std::make_pair(clusterId, NYql::EDatabaseType::ClickHouse)] =
                NYql::TDatabaseResolverResponse::TEndpoint{endpoint, clusterId};
            auto dbResolverPromise = NThreading::NewPromise<NYql::TDatabaseResolverResponse>();
            dbResolverPromise.SetValue(NYql::TDatabaseResolverResponse(std::move(databaseEndpointsMap), true));

            EXPECT_CALL(*databaseAsyncResolverMock, ResolveIds(DatabaseAuthMapMatcher(dbResolverReq))).WillOnce(Return(dbResolverPromise.GetFuture()));

            // run test
            auto kikimr = MakeKikimrRunner(nullptr, connectorClient, databaseAsyncResolver);

            auto tc = kikimr->GetTableClient();
            auto session = tc.CreateSession().GetValueSync().GetSession();
            const TString query1 = fmt::format(
                R"(
                CREATE OBJECT sa_signature (TYPE SECRET) WITH (value=sa_signature);
                CREATE OBJECT ch_managed_password (TYPE SECRET) WITH (value={password});

                CREATE EXTERNAL DATA SOURCE ch_managed WITH (
                    SOURCE_TYPE="{source_type}",
                    MDB_CLUSTER_ID="{cluster_id}",
                    AUTH_METHOD="MDB_BASIC",
                    SERVICE_ACCOUNT_ID="{service_account_id}",
                    SERVICE_ACCOUNT_SECRET_NAME="{service_account_id_signature}",
                    LOGIN="{login}",
                    PASSWORD_SECRET_NAME="ch_managed_password",
                    USE_TLS="{use_tls}",
                    PROTOCOL="{protocol}"
                );
            )",
                "cluster_id"_a = clusterId,
                "password"_a = password,
                "login"_a = login,
                "use_tls"_a = useTls ? "TRUE" : "FALSE",
                "protocol"_a = NYql::NConnector::NApi::EProtocol_Name(protocol),
                "service_account_id"_a = serviceAccountId,
                "service_account_id_signature"_a = serviceAccountIdSignature,
                "source_type"_a = ToString(sourceType));
            auto result = session.ExecuteSchemeQuery(query1).GetValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

            const TString query2 = fmt::format(
                R"(
                SELECT * FROM ch_managed.`{database_name}.{table_name}`;
            )",
                "database_name"_a = databaseName,
                "table_name"_a = tableName);

            auto db = kikimr->GetQueryClient();
            auto scriptExecutionOperation = db.ExecuteScript(query2).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::SUCCESS, scriptExecutionOperation.Status().GetIssues().ToString());
            UNIT_ASSERT(scriptExecutionOperation.Metadata().ExecutionId);

            NYdb::NQuery::TScriptExecutionOperation readyOp = WaitScriptExecutionOperation(scriptExecutionOperation.Id(), kikimr->GetDriver());
            UNIT_ASSERT_C(readyOp.Metadata().ExecStatus == EExecStatus::Completed, readyOp.Status().GetIssues().ToString());
            TFetchScriptResultsResult results = db.FetchScriptResults(scriptExecutionOperation.Id(), 0).ExtractValueSync();
            UNIT_ASSERT_C(results.IsSuccess(), results.GetIssues().ToString());

            TResultSetParser resultSet(results.ExtractResultSet());
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnsCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), colData.size());

            // check every row
            MATCH_RESULT_WITH_INPUT(colData, resultSet, GetUint16);
        }
    }
}

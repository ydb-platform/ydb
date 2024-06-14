#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/ut/federated_query/common/common.h>
#include <ydb/library/yql/providers/common/structured_token/yql_token_builder.h>
#include <ydb/library/yql/providers/generic/connector/libcpp/client.h>
#include <ydb/library/yql/providers/generic/connector/libcpp/ut_helpers/connector_client_mock.h>
#include <ydb/library/yql/providers/generic/connector/libcpp/ut_helpers/database_resolver_mock.h>
#include <ydb/library/yql/providers/s3/actors/yql_s3_actors_factory_impl.h>
#include <ydb/public/sdk/cpp/client/ydb_operation/operation.h>
#include <ydb/public/sdk/cpp/client/ydb_query/query.h>
#include <ydb/public/sdk/cpp/client/ydb_types/status_codes.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/defaults.h>
#include <util/system/env.h>

#include <arrow/api.h>

#include <google/protobuf/util/message_differencer.h>

#include <fmt/format.h>

namespace NKikimr::NKqp {
    using namespace NYdb;
    using namespace NYdb::NQuery;
    using namespace NYql::NConnector;
    using namespace NYql::NConnector::NTest;
    using namespace NKikimr::NKqp::NFederatedQueryTest;
    using namespace testing;
    using namespace fmt::literals;

    enum class EProviderType {
        PostgreSQL,
        ClickHouse,
        Ydb,
    };

    NApi::TDataSourceInstance MakeDataSourceInstance(EProviderType providerType) {
        switch (providerType) {
            case EProviderType::PostgreSQL:
                return TConnectorClientMock::TPostgreSQLDataSourceInstanceBuilder<>().GetResult();
            case EProviderType::ClickHouse:
                return TConnectorClientMock::TClickHouseDataSourceInstanceBuilder<>().GetResult();
            case EProviderType::Ydb:
                return TConnectorClientMock::TYdbDataSourceInstanceBuilder<>().GetResult();
        }
    }

    void CreateExternalDataSource(EProviderType providerType, const std::shared_ptr<NKikimr::NKqp::TKikimrRunner>& kikimr) {
        switch (providerType) {
            case EProviderType::PostgreSQL:
                return CreatePostgreSQLExternalDataSource(kikimr);
            case EProviderType::ClickHouse:
                return CreateClickHouseExternalDataSource(kikimr);
            case EProviderType::Ydb:
                return CreateYdbExternalDataSource(kikimr);
        }
    }

    NKikimrConfig::TAppConfig CreateDefaultAppConfig() {
        NKikimrConfig::TAppConfig appConfig;
        NYql::TAttr dateTimeFormat;
        dateTimeFormat.SetName("DateTimeFormat");
        dateTimeFormat.SetValue("string");
        appConfig.MutableQueryServiceConfig()->MutableGeneric()->MutableConnector()->SetUseSsl(false);
        appConfig.MutableQueryServiceConfig()->MutableGeneric()->MutableConnector()->MutableEndpoint()->set_host("localhost");
        appConfig.MutableQueryServiceConfig()->MutableGeneric()->MutableConnector()->MutableEndpoint()->set_port(1234);
        appConfig.MutableQueryServiceConfig()->MutableGeneric()->MutableDefaultSettings()->Add(std::move(dateTimeFormat));
        return appConfig;
    }

    NApi::TTypeMappingSettings MakeTypeMappingSettings(NApi::EDateTimeFormat dateTimeFormat) {
        NApi::TTypeMappingSettings settings;
        settings.set_date_time_format(dateTimeFormat);
        return settings;
    }

    std::shared_ptr<TDatabaseAsyncResolverMock> MakeDatabaseAsyncResolver(EProviderType providerType) {
        std::shared_ptr<TDatabaseAsyncResolverMock> databaseAsyncResolverMock;

        switch (providerType) {
            case EProviderType::ClickHouse:
                // We test access to managed databases only on the example of ClickHouse
                databaseAsyncResolverMock = std::make_shared<TDatabaseAsyncResolverMock>();
                databaseAsyncResolverMock->AddClickHouseCluster();
                break;
            default:
                break;
        }

        return databaseAsyncResolverMock;
    }

    Y_UNIT_TEST_SUITE(GenericFederatedQuery) {
        void TestSelectAllFields(EProviderType providerType) {
            // prepare mock
            auto clientMock = std::make_shared<TConnectorClientMock>();

            const NApi::TDataSourceInstance dataSourceInstance = MakeDataSourceInstance(providerType);

            // step 1: DescribeTable
            // clang-format off
            clientMock->ExpectDescribeTable()
                .DataSourceInstance(dataSourceInstance)
                .TypeMappingSettings(MakeTypeMappingSettings(NYql::NConnector::NApi::STRING_FORMAT))
                .Response()
                    .Column("col1", Ydb::Type::UINT16);

            // step 2: ListSplits
            clientMock->ExpectListSplits()
                .Select()
                    .DataSourceInstance(dataSourceInstance)
                    .What()
                        .Column("col1", Ydb::Type::UINT16)
                        .Done()
                    .Done()
                .Result()
                    .AddResponse(NewSuccess())
                        .Description("some binary description")
                        .Select()
                            .DataSourceInstance(dataSourceInstance)
                            .What()
                                .Column("col1", Ydb::Type::UINT16);

            // step 3: ReadSplits
            std::vector<ui16> colData = {10, 20, 30, 40, 50};
            clientMock->ExpectReadSplits()
                .Split()
                    .Description("some binary description")
                    .Select()
                        .DataSourceInstance(dataSourceInstance)
                        .What()
                            .Column("col1", Ydb::Type::UINT16)
                            .Done()
                        .Done()
                    .Done()
                .Result()
                    .AddResponse(
                        MakeRecordBatch<arrow::UInt16Builder>("col1", colData, arrow::uint16()),
                        NewSuccess());
            // clang-format on

            // prepare database resolver mock
            auto databaseAsyncResolverMock = MakeDatabaseAsyncResolver(providerType);

            // run test
            auto appConfig = CreateDefaultAppConfig();
            auto s3ActorsFactory = NYql::NDq::CreateS3ActorsFactory();
            auto kikimr = MakeKikimrRunner(false, clientMock, databaseAsyncResolverMock, appConfig, s3ActorsFactory);

            CreateExternalDataSource(providerType, kikimr);

            const TString query = fmt::format(
                R"(
                SELECT * FROM {data_source_name}.{table_name};
            )",
                "data_source_name"_a = DEFAULT_DATA_SOURCE_NAME,
                "table_name"_a = DEFAULT_TABLE);

            auto db = kikimr->GetQueryClient();
            auto scriptExecutionOperation = db.ExecuteScript(query).ExtractValueSync();
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

        Y_UNIT_TEST(PostgreSQLOnPremSelectAll) {
            TestSelectAllFields(EProviderType::PostgreSQL);
        }

        Y_UNIT_TEST(ClickHouseManagedSelectAll) {
            TestSelectAllFields(EProviderType::ClickHouse);
        }

        Y_UNIT_TEST(YdbManagedSelectAll) {
            TestSelectAllFields(EProviderType::Ydb);
        }

        void TestSelectConstant(EProviderType providerType) {
            // prepare mock
            auto clientMock = std::make_shared<TConnectorClientMock>();

            const NApi::TDataSourceInstance dataSourceInstance = MakeDataSourceInstance(providerType);

            constexpr size_t ROWS_COUNT = 5;

            // step 1: DescribeTable
            // clang-format off
            clientMock->ExpectDescribeTable()
                .DataSourceInstance(dataSourceInstance)
                .TypeMappingSettings(MakeTypeMappingSettings(NYql::NConnector::NApi::STRING_FORMAT))
                .Response()
                    .Column("col1", Ydb::Type::UINT16)
                    .Column("col2", Ydb::Type::DOUBLE);

            // step 2: ListSplits
            clientMock->ExpectListSplits()
                .Select()
                    .DataSourceInstance(dataSourceInstance)
                    .What()
                        // Empty
                        .Done()
                    .Done()
                .Result()
                    .AddResponse(NewSuccess())
                        .Description("some binary description")
                        .Select()
                            .DataSourceInstance(dataSourceInstance)
                            .What();

            // step 3: ReadSplits
            clientMock->ExpectReadSplits()
                .Split()
                    .Description("some binary description")
                    .Select()
                        .DataSourceInstance(dataSourceInstance)
                        .What()
                            .Done()
                        .Done()
                    .Done()
                .Result()
                    .AddResponse(MakeEmptyRecordBatch(ROWS_COUNT), NewSuccess());
            // clang-format on

            // prepare database resolver mock
            auto databaseAsyncResolverMock = MakeDatabaseAsyncResolver(providerType);

            // run test
            auto appConfig = CreateDefaultAppConfig();
            auto s3ActorsFactory = NYql::NDq::CreateS3ActorsFactory();
            auto kikimr = MakeKikimrRunner(false, clientMock, databaseAsyncResolverMock, appConfig, s3ActorsFactory);

            CreateExternalDataSource(providerType, kikimr);

            const TString query = fmt::format(
                R"(
                SELECT 42 FROM {data_source_name}.{table_name};
                SELECT 42 FROM {data_source_name}.{table_name};
            )",
                "data_source_name"_a = DEFAULT_DATA_SOURCE_NAME,
                "table_name"_a = DEFAULT_TABLE);

            auto db = kikimr->GetQueryClient();
            auto queryResult = db.ExecuteQuery(query, TTxControl::BeginTx().CommitTx(), TExecuteQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(queryResult.GetStatus(), EStatus::SUCCESS, queryResult.GetIssues().ToString());

            std::vector<i32> constants(ROWS_COUNT, 42);

            for (size_t i = 0; i < 2; ++i) {
                TResultSetParser resultSet(queryResult.GetResultSetParser(i));
                UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnsCount(), 1);
                UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), ROWS_COUNT);

                // check every row
                MATCH_RESULT_WITH_INPUT(constants, resultSet, GetInt32);
            }
        }

        Y_UNIT_TEST(PostgreSQLOnPremSelectConstant) {
            TestSelectConstant(EProviderType::PostgreSQL);
        }

        Y_UNIT_TEST(ClickHouseManagedSelectConstant) {
            TestSelectConstant(EProviderType::ClickHouse);
        }

        Y_UNIT_TEST(YdbManagedSelectConstant) {
            TestSelectConstant(EProviderType::Ydb);
        }

        void TestSelectCount(EProviderType providerType) {
            // prepare mock
            auto clientMock = std::make_shared<TConnectorClientMock>();

            const NApi::TDataSourceInstance dataSourceInstance = MakeDataSourceInstance(providerType);

            constexpr size_t ROWS_COUNT = 5;

            // clang-format off
            // step 1: DescribeTable
            clientMock->ExpectDescribeTable()
                .DataSourceInstance(dataSourceInstance)
                .TypeMappingSettings(MakeTypeMappingSettings(NYql::NConnector::NApi::STRING_FORMAT))
                .Response()
                    .Column("col1", Ydb::Type::UINT16)
                    .Column("col2", Ydb::Type::DOUBLE);

            // step 2: ListSplits
            clientMock->ExpectListSplits()
                .Select()
                    .DataSourceInstance(dataSourceInstance)
                    .What()
                        // Empty
                        .Done()
                    .Done()
                .Result()
                    .AddResponse(NewSuccess())
                        .Description("some binary description")
                        .Select()
                            .DataSourceInstance(dataSourceInstance)
                            .What();

            // step 3: ReadSplits
            clientMock->ExpectReadSplits()
                .Split()
                    .Description("some binary description")
                    .Select()
                        .DataSourceInstance(dataSourceInstance)
                        .What()
                            .Done()
                        .Done()
                    .Done()
                .Result()
                    .AddResponse(MakeEmptyRecordBatch(ROWS_COUNT), NewSuccess());
            // clang-format on

            // prepare database resolver mock
            auto databaseAsyncResolverMock = MakeDatabaseAsyncResolver(providerType);

            // run test
            auto appConfig = CreateDefaultAppConfig();
            auto s3ActorsFactory = NYql::NDq::CreateS3ActorsFactory();
            auto kikimr = MakeKikimrRunner(false, clientMock, databaseAsyncResolverMock, appConfig, s3ActorsFactory);

            CreateExternalDataSource(providerType, kikimr);

            const TString query = fmt::format(
                R"(
                SELECT COUNT(*) FROM {data_source_name}.{table_name};
            )",
                "data_source_name"_a = DEFAULT_DATA_SOURCE_NAME,
                "table_name"_a = DEFAULT_TABLE);

            auto db = kikimr->GetQueryClient();
            auto queryResult = db.ExecuteQuery(query, TTxControl::BeginTx().CommitTx(), TExecuteQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(queryResult.GetStatus(), EStatus::SUCCESS, queryResult.GetIssues().ToString());

            TResultSetParser resultSet(queryResult.GetResultSetParser(0));
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnsCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 1);

            // check every row
            std::vector<ui64> result = {ROWS_COUNT};
            MATCH_RESULT_WITH_INPUT(result, resultSet, GetUint64);
        }

        Y_UNIT_TEST(PostgreSQLSelectCount) {
            TestSelectCount(EProviderType::PostgreSQL);
        }

        Y_UNIT_TEST(ClickHouseSelectCount) {
            TestSelectCount(EProviderType::ClickHouse);
        }

        Y_UNIT_TEST(YdbSelectCount) {
            TestSelectCount(EProviderType::Ydb);
        }

        void TestFilterPushdown(EProviderType providerType) {
            // prepare mock
            auto clientMock = std::make_shared<TConnectorClientMock>();

            const NApi::TDataSourceInstance dataSourceInstance = MakeDataSourceInstance(providerType);
            // clang-format off
            const NApi::TSelect select = TConnectorClientMock::TSelectBuilder<>()
                .DataSourceInstance(dataSourceInstance)
                .What()
                    .NullableColumn("data_column", Ydb::Type::STRING)
                    .NullableColumn("filtered_column", Ydb::Type::INT32)
                    .Done()
                .Where()
                    .Filter()
                        .Equal()
                            .Column("filtered_column")
                            .Value<i32>(42)
                            .Done()
                        .Done()
                    .Done()
                .GetResult();
            // clang-format on

            // step 1: DescribeTable
            // clang-format off
            clientMock->ExpectDescribeTable()
                .DataSourceInstance(dataSourceInstance)
                .TypeMappingSettings(MakeTypeMappingSettings(NYql::NConnector::NApi::STRING_FORMAT))
                .Response()
                    .NullableColumn("filtered_column", Ydb::Type::INT32)
                    .NullableColumn("data_column", Ydb::Type::STRING);
            // clang-format on

            // step 2: ListSplits
            // clang-format off
            clientMock->ExpectListSplits()
                .Select(select)
                .Result()
                    .AddResponse(NewSuccess())
                        .Description("some binary description")
                        .Select(select);
            // clang-format on

            // step 3: ReadSplits
            // Return data such that it contains values not satisfying the filter conditions.
            // Then check that, despite that connector reads additional data,
            // our generic provider then filters it out.
            std::vector<std::string> colData = {"Filtered text", "Text"};
            std::vector<i32> filterColumnData = {42, 24};
            // clang-format off
            clientMock->ExpectReadSplits()
                .Split()
                    .Description("some binary description")
                    .Select(select)
                    .Done()
                .Result()
                    .AddResponse(MakeRecordBatch(
                        MakeArray<arrow::BinaryBuilder>("data_column", colData, arrow::binary()),
                        MakeArray<arrow::Int32Builder>("filtered_column", filterColumnData, arrow::int32())),
                        NewSuccess());
            // clang-format on

            // prepare database resolver mock
            auto databaseAsyncResolverMock = MakeDatabaseAsyncResolver(providerType);

            // run test
            auto appConfig = CreateDefaultAppConfig();
            auto s3ActorsFactory = NYql::NDq::CreateS3ActorsFactory();
            auto kikimr = MakeKikimrRunner(false, clientMock, databaseAsyncResolverMock, appConfig, s3ActorsFactory);

            CreateExternalDataSource(providerType, kikimr);

            const TString query = fmt::format(
                R"(
                PRAGMA generic.UsePredicatePushdown="true";
                SELECT data_column FROM {data_source_name}.{table_name} WHERE filtered_column = 42;
            )",
                "data_source_name"_a = DEFAULT_DATA_SOURCE_NAME,
                "table_name"_a = DEFAULT_TABLE);

            auto db = kikimr->GetQueryClient();
            auto queryResult = db.ExecuteQuery(query, TTxControl::BeginTx().CommitTx(), TExecuteQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(queryResult.GetStatus(), EStatus::SUCCESS, queryResult.GetIssues().ToString());

            TResultSetParser resultSet(queryResult.GetResultSetParser(0));
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnsCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 1);

            // check every row
            // Check that, despite returning nonfiltered data in connector, response will be correct
            std::vector<TMaybe<TString>> result = {"Filtered text"}; // Only data satisfying filter conditions
            MATCH_RESULT_WITH_INPUT(result, resultSet, GetOptionalString);
        }

        Y_UNIT_TEST(PostgreSQLFilterPushdown) {
            TestFilterPushdown(EProviderType::PostgreSQL);
        }

        Y_UNIT_TEST(ClickHouseFilterPushdown) {
            TestFilterPushdown(EProviderType::ClickHouse);
        }

        Y_UNIT_TEST(YdbFilterPushdown) {
            TestFilterPushdown(EProviderType::Ydb);
        }
    }
}

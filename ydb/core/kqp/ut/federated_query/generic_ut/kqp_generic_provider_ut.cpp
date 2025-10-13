#include "iceberg_ut_data.h"

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/ut/federated_query/common/common.h>
#include <yql/essentials/providers/common/structured_token/yql_token_builder.h>
#include <ydb/library/yql/providers/generic/connector/api/service/protos/connector.pb.h>
#include <ydb/library/yql/providers/generic/connector/libcpp/client.h>
#include <ydb/library/yql/providers/generic/connector/libcpp/ut_helpers/connector_client_mock.h>
#include <ydb/library/yql/providers/generic/connector/libcpp/ut_helpers/database_resolver_mock.h>
#include <ydb/library/yql/providers/s3/actors/yql_s3_actors_factory_impl.h>
#include <ydb/public/api/protos/ydb_query.pb.h>
#include <ydb/public/api/grpc/ydb_operation_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_query_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/src/library/grpc/client/grpc_client_low.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/operation/operation.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/query.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status_codes.h>

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
        IcebergHiveMetastoreBasic,
        IcebergHiveMetastoreSa,
        IcebergHiveMetastoreToken,
        IcebergHadoopBasic,
        IcebergHadoopSa,
        IcebergHadoopToken,
    };

    NYql::TGenericDataSourceInstance MakeDataSourceInstance(EProviderType providerType) {
        switch (providerType) {
            case EProviderType::PostgreSQL:
                return TConnectorClientMock::TPostgreSQLDataSourceInstanceBuilder<>().GetResult();
            case EProviderType::ClickHouse:
                return TConnectorClientMock::TClickHouseDataSourceInstanceBuilder<>().GetResult();
            case EProviderType::Ydb:
                return TConnectorClientMock::TYdbDataSourceInstanceBuilder<>().GetResult();
            case EProviderType::IcebergHiveMetastoreBasic:
                return NTestUtils::CreateIcebergBasic().CreateDataSourceForHiveMetastore();
            case EProviderType::IcebergHiveMetastoreSa:
                return NTestUtils::CreateIcebergSa().CreateDataSourceForHiveMetastore();
            case EProviderType::IcebergHiveMetastoreToken:
                return NTestUtils::CreateIcebergToken().CreateDataSourceForHiveMetastore();
            case EProviderType::IcebergHadoopBasic:
                return NTestUtils::CreateIcebergBasic().CreateDataSourceForHadoop();
            case EProviderType::IcebergHadoopSa:
                return NTestUtils::CreateIcebergSa().CreateDataSourceForHadoop();
            case EProviderType::IcebergHadoopToken:
                return NTestUtils::CreateIcebergToken().CreateDataSourceForHadoop();
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
            case EProviderType::IcebergHiveMetastoreBasic:
                return NTestUtils::CreateIcebergBasic()
                    .ExecuteCreateHiveMetastoreExternalDataSource(kikimr);
            case EProviderType::IcebergHiveMetastoreSa:
                return NTestUtils::CreateIcebergSa()
                    .ExecuteCreateHiveMetastoreExternalDataSource(kikimr);
            case EProviderType::IcebergHiveMetastoreToken:
                return NTestUtils::CreateIcebergToken()
                    .ExecuteCreateHiveMetastoreExternalDataSource(kikimr);
            case EProviderType::IcebergHadoopBasic:
                return NTestUtils::CreateIcebergBasic()
                    .ExecuteCreateHadoopExternalDataSource(kikimr);
            case EProviderType::IcebergHadoopSa:
                return NTestUtils::CreateIcebergSa()
                    .ExecuteCreateHadoopExternalDataSource(kikimr);
            case EProviderType::IcebergHadoopToken:
                return NTestUtils::CreateIcebergToken()
                    .ExecuteCreateHadoopExternalDataSource(kikimr);
        }
    }

    NKikimrConfig::TAppConfig CreateDefaultAppConfig() {
        NKikimrConfig::TAppConfig appConfig;
        NYql::TAttr dateTimeFormat;
        dateTimeFormat.SetName("DateTimeFormat");
        dateTimeFormat.SetValue("string");

        auto& config = *appConfig.MutableQueryServiceConfig();
        auto& connector = *config.MutableGeneric()->MutableConnector();

        connector.SetUseSsl(false);
        connector.MutableEndpoint()->set_host("localhost");
        connector.MutableEndpoint()->set_port(1234);

        config.MutableGeneric()->MutableDefaultSettings()->Add(std::move(dateTimeFormat));
        config.AddAvailableExternalDataSources("ObjectStorage");
        config.AddAvailableExternalDataSources("ClickHouse");
        config.AddAvailableExternalDataSources("PostgreSQL");
        config.AddAvailableExternalDataSources("MySQL");
        config.AddAvailableExternalDataSources("Ydb");
        config.AddAvailableExternalDataSources("Iceberg");
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

    ///
    /// Fixture that prepares mocks and services for a provider.
    ///
    /// TODO:
    /// Make it reusable, currently it fails if multiple
    /// expects are applied to mock
    ///
    class TQueryExecutorFixture : public NUnitTest::TBaseFixture {
    public:
        TQueryExecutorFixture(EProviderType providerType)
            : DataSourceInstance(MakeDataSourceInstance(providerType))
            , ClientMock(std::make_shared<TConnectorClientMock>())
        {
            auto databaseAsyncResolverMock = MakeDatabaseAsyncResolver(providerType);
            auto appConfig = CreateDefaultAppConfig();
            auto s3ActorsFactory = NYql::NDq::CreateS3ActorsFactory();

            Kikimr = MakeKikimrRunner(
                false,
                ClientMock,
                databaseAsyncResolverMock,
                appConfig,
                s3ActorsFactory,
                {.CredentialsFactory = CreateCredentialsFactory()}
            );

            CreateExternalDataSource(providerType, Kikimr);
            QueryClient = Kikimr->GetQueryClient();
        }

        TQueryClient GetQueryClient() {
            return *QueryClient;
        }

        TAsyncExecuteQueryResult ExecuteQuery(const TString& query) {
            return GetQueryClient()
                .ExecuteQuery(query, TTxControl::BeginTx().CommitTx(), TExecuteQuerySettings());
        }

        NThreading::TFuture<TScriptExecutionOperation> ExecuteScript(const TString& script) {
            return GetQueryClient()
                .ExecuteScript(script);
        }

        TConnectorClientMock::TSelectBuilder<> GetSelectBuilder() {
            TConnectorClientMock::TSelectBuilder<> builder;
            builder.DataSourceInstance(DataSourceInstance);
            return builder;
        }

    public:
        const NYql::TGenericDataSourceInstance DataSourceInstance;
        std::shared_ptr<TConnectorClientMock> ClientMock;

    protected:
        std::shared_ptr<TKikimrRunner> Kikimr;
        std::optional<TQueryClient> QueryClient;
    };

    Y_UNIT_TEST_SUITE(GenericFederatedQuery) {
        void TestSelectAllFields(EProviderType providerType) {
            // prepare mock
            auto clientMock = std::make_shared<TConnectorClientMock>();

            const NYql::TGenericDataSourceInstance dataSourceInstance = MakeDataSourceInstance(providerType);
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
                .Filtering(NYql::NConnector::NApi::TReadSplitsRequest::FILTERING_OPTIONAL)
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
            auto kikimr = MakeKikimrRunner(false, clientMock, databaseAsyncResolverMock, appConfig, s3ActorsFactory,
                {.CredentialsFactory = CreateCredentialsFactory()});

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
            UNIT_ASSERT(!scriptExecutionOperation.Metadata().ExecutionId.empty());

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

        Y_UNIT_TEST(IcebergHiveBasicSelectAll) {
            TestSelectAllFields(EProviderType::IcebergHiveMetastoreBasic);
        }

        Y_UNIT_TEST(IcebergHiveSaSelectAll) {
            TestSelectAllFields(EProviderType::IcebergHiveMetastoreSa);
        }

        Y_UNIT_TEST(IcebergHiveTokenSelectAll) {
            TestSelectAllFields(EProviderType::IcebergHiveMetastoreToken);
        }

        Y_UNIT_TEST(IcebergHadoopBasicSelectAll) {
            TestSelectAllFields(EProviderType::IcebergHadoopBasic);
        }

        Y_UNIT_TEST(IcebergHadoopSaSelectAll) {
            TestSelectAllFields(EProviderType::IcebergHadoopSa);
        }

        Y_UNIT_TEST(IcebergHadoopTokenSelectAll) {
            TestSelectAllFields(EProviderType::IcebergHadoopToken);
        }

        void TestSelectConstant(EProviderType providerType) {
            // prepare mock
            auto clientMock = std::make_shared<TConnectorClientMock>();

            const NYql::TGenericDataSourceInstance dataSourceInstance = MakeDataSourceInstance(providerType);

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
                .Filtering(NYql::NConnector::NApi::TReadSplitsRequest::FILTERING_OPTIONAL)
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
            auto kikimr = MakeKikimrRunner(false, clientMock, databaseAsyncResolverMock, appConfig, s3ActorsFactory,
                {.CredentialsFactory = CreateCredentialsFactory()});

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

        Y_UNIT_TEST(IcebergHiveBasicSelectConstant) {
            TestSelectConstant(EProviderType::IcebergHiveMetastoreBasic);
        }

        Y_UNIT_TEST(IcebergHiveSaSelectConstant) {
            TestSelectConstant(EProviderType::IcebergHiveMetastoreSa);
        }

        Y_UNIT_TEST(IcebergHiveTokenSelectConstant) {
            TestSelectConstant(EProviderType::IcebergHiveMetastoreToken);
        }

        Y_UNIT_TEST(IcebergHadoopBasicSelectConstant) {
            TestSelectConstant(EProviderType::IcebergHadoopBasic);
        }

        Y_UNIT_TEST(IcebergHadoopSaSelectConstant) {
            TestSelectConstant(EProviderType::IcebergHadoopSa);
        }

        Y_UNIT_TEST(IcebergHadoopTokenSelectConstant) {
            TestSelectConstant(EProviderType::IcebergHadoopToken);
        }

        void TestSelectCount(EProviderType providerType) {
            // prepare mock
            auto clientMock = std::make_shared<TConnectorClientMock>();

            const NYql::TGenericDataSourceInstance dataSourceInstance = MakeDataSourceInstance(providerType);

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
                .Filtering(NYql::NConnector::NApi::TReadSplitsRequest::FILTERING_OPTIONAL)
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
            auto kikimr = MakeKikimrRunner(false, clientMock, databaseAsyncResolverMock, appConfig, s3ActorsFactory,
                {.CredentialsFactory = CreateCredentialsFactory()});

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

        Y_UNIT_TEST(IcebergHiveBasicSelectCount) {
            TestSelectCount(EProviderType::IcebergHiveMetastoreBasic);
        }

        Y_UNIT_TEST(IcebergHiveSaSelectCount) {
            TestSelectCount(EProviderType::IcebergHiveMetastoreSa);
        }

        Y_UNIT_TEST(IcebergHiveTokenSelectCount) {
            TestSelectCount(EProviderType::IcebergHiveMetastoreToken);
        }

        Y_UNIT_TEST(IcebergHadoopBasicSelectCount) {
            TestSelectCount(EProviderType::IcebergHadoopBasic);
        }

        Y_UNIT_TEST(IcebergHadoopSaSelectCount) {
            TestSelectCount(EProviderType::IcebergHadoopSa);
        }

        Y_UNIT_TEST(IcebergHadoopTokenSelectCount) {
            TestSelectCount(EProviderType::IcebergHadoopToken);
        }

        ///
        /// Test a filter pushdown for a provider
        ///
        /// @param[in] providerType     Provider's type
        /// @param[in] where            Where clause that will be appended to a sql query
        /// @param[in] expectedWhere    Where clause that will be expected in a list split and read split requests
        ///
        void TestFilterPushdown(EProviderType providerType, const TString& where, NApi::TSelect::TWhere& expectedWhere) {
            auto f = std::make_shared<TQueryExecutorFixture>(providerType);
            auto expectedSelect = f->GetSelectBuilder()
                .What()
                    .NullableColumn("colDate", Ydb::Type::DATE)
                    .NullableColumn("colInt32", Ydb::Type::INT32)
                    .NullableColumn("colString", Ydb::Type::STRING)
                    .Done()
                .Where(expectedWhere)
                .GetResult();

            // step 1: DescribeTable
            f->ClientMock->ExpectDescribeTable()
                .DataSourceInstance(f->DataSourceInstance)
                .TypeMappingSettings(MakeTypeMappingSettings(NYql::NConnector::NApi::STRING_FORMAT))
                .Response()
                    .NullableColumn("colDate", Ydb::Type::DATE)
                    .NullableColumn("colInt32", Ydb::Type::INT32)
                    .NullableColumn("colString", Ydb::Type::STRING);

            // step 2: ListSplits
            f->ClientMock->ExpectListSplits()
                .Select(expectedSelect)
                .Result()
                    .AddResponse(NewSuccess())
                        .Description("some binary description")
                        .Select(expectedSelect);

            // step 3: ReadSplits
            std::vector<std::string> colString = {"Filtered text", "Text"};
            std::vector<ui16> colDate = {20326, 20329};
            std::vector<i32> colInt32 = {42, 24};

            f->ClientMock->ExpectReadSplits()
                .Filtering(NYql::NConnector::NApi::TReadSplitsRequest::FILTERING_OPTIONAL)
                .Split()
                    .Description("some binary description")
                    .Select(expectedSelect)
                    .Done()
                .Result()
                    .AddResponse(MakeRecordBatch(
                        MakeArray<arrow::UInt16Builder>("colDate", colDate, arrow::uint16()),
                        MakeArray<arrow::Int32Builder>("colInt32", colInt32, arrow::int32()),
                        MakeArray<arrow::BinaryBuilder>("colString", colString, arrow::binary())),
                        NewSuccess());

            const TString query = fmt::format(
                R"(
                PRAGMA generic.UsePredicatePushdown="true";
                SELECT colDate, colInt32, colString FROM {data_source_name}.{table_name} WHERE {table_where};
            )",
                "data_source_name"_a = DEFAULT_DATA_SOURCE_NAME,
                "table_name"_a = DEFAULT_TABLE,
                "table_where"_a = where
            );

            auto queryResult = f->ExecuteQuery(query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(queryResult.GetStatus(), EStatus::SUCCESS, queryResult.GetIssues().ToString());

            // Check a query result
            TResultSetParser resultSet(queryResult.GetResultSetParser(0));
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnsCount(), 3);
            UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 1);

            // Check values for the query result
            std::vector<std::optional<TInstant>> colDateResults = {TInstant::Days(20326)};
            std::vector<std::optional<int>> colInt32Result = {42};
            std::vector<std::optional<TString>> colStringResult = {"Filtered text"};

            for (size_t i = 0; i < colDateResults.size(); ++i) {
                resultSet.TryNextRow();

                MATCH_OPT_RESULT_WITH_VAL_IDX(colDateResults[i], resultSet, GetOptionalDate, 0);
                MATCH_OPT_RESULT_WITH_VAL_IDX(colInt32Result[i], resultSet, GetOptionalInt32, 1);
                MATCH_OPT_RESULT_WITH_VAL_IDX(colStringResult[i], resultSet, GetOptionalString, 2);
            }
        }

        ///
        /// Test a filter pushdown for a provider
        ///
        /// @param[in] providerType Provider's type
        ///
        void TestFilterPushdown(EProviderType providerType) {
            using namespace NYql::NConnector::NTest;

            auto expectedWhereInt = TConnectorClientMock::TWhereBuilder<>()
                .Filter().Equal()
                    .Column("colInt32")
                    .Value<i32>(42)
                    .Done()
                .Done()
                .GetResult();

            TestFilterPushdown(providerType, "colInt32 = 42", expectedWhereInt);
            TestFilterPushdown(providerType, "colInt32 = EvaluateExpr(44 - 2)", expectedWhereInt);
            TestFilterPushdown(providerType, "colInt32 = 44 - 2", expectedWhereInt);

            auto expectedWhereDate = TConnectorClientMock::TWhereBuilder<>()
                .Filter().Equal()
                    .Column("colDate")
                    .Value<ui32>(20326, ::Ydb::Type::DATE)
                    .Done()
                .Done()
                .GetResult();

            TestFilterPushdown(providerType, "colDate = Date('2025-08-26')", expectedWhereDate);
            TestFilterPushdown(providerType, "colDate = EvaluateExpr(Date('2025-08-27') - Interval(\"P1D\"))", expectedWhereDate);
            TestFilterPushdown(providerType, "colDate = Date('2025-08-27') - Interval(\"P1D\")", expectedWhereDate);
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

        Y_UNIT_TEST(IcebergHiveBasicFilterPushdown) {
            TestFilterPushdown(EProviderType::IcebergHiveMetastoreBasic);
        }

        Y_UNIT_TEST(IcebergHiveSaFilterPushdown) {
            TestFilterPushdown(EProviderType::IcebergHiveMetastoreSa);
        }

        Y_UNIT_TEST(IcebergHiveTokenFilterPushdown) {
            TestFilterPushdown(EProviderType::IcebergHiveMetastoreToken);
        }

        Y_UNIT_TEST(IcebergHadoopBasicFilterPushdown) {
            TestFilterPushdown(EProviderType::IcebergHadoopBasic);
        }

        Y_UNIT_TEST(IcebergHadoopSaFilterPushdown) {
            TestFilterPushdown(EProviderType::IcebergHadoopSa);
        }

        Y_UNIT_TEST(IcebergHadoopTokenFilterPushdown) {
            TestFilterPushdown(EProviderType::IcebergHadoopToken);
        }

        void TestFailsOnIncorrectScriptExecutionOperation(const TString& operationId, const TString& fetchToken) {
            auto clientMock = std::make_shared<TConnectorClientMock>();
            auto databaseAsyncResolverMock = MakeDatabaseAsyncResolver(EProviderType::Ydb);
            auto appConfig = CreateDefaultAppConfig();
            auto s3ActorsFactory = NYql::NDq::CreateS3ActorsFactory();
            auto kikimr = MakeKikimrRunner(false, clientMock, databaseAsyncResolverMock, appConfig, s3ActorsFactory,
                {.CredentialsFactory = CreateCredentialsFactory()});

            // Create trash query
            NYdbGrpc::TGRpcClientLow clientLow;
            const auto channel = grpc::CreateChannel("localhost:" + ToString(kikimr->GetTestServer().GetGRpcServer().GetPort()), grpc::InsecureChannelCredentials());
            const auto queryServiceStub = Ydb::Query::V1::QueryService::NewStub(channel);
            const auto operationServiceStub = Ydb::Operation::V1::OperationService::NewStub(channel);

            {
                grpc::ClientContext context;
                Ydb::Query::FetchScriptResultsRequest request;
                request.set_operation_id(operationId);
                request.set_fetch_token(fetchToken);
                Ydb::Query::FetchScriptResultsResponse response;
                grpc::Status st = queryServiceStub->FetchScriptResults(&context, request, &response);
                UNIT_ASSERT(st.ok());
                UNIT_ASSERT_VALUES_EQUAL_C(response.status(), Ydb::StatusIds::BAD_REQUEST, response);
            }

            {
                grpc::ClientContext context;
                Ydb::Operations::ForgetOperationRequest request;
                request.set_id(operationId);
                Ydb::Operations::ForgetOperationResponse response;
                grpc::Status st = operationServiceStub->ForgetOperation(&context, request, &response);
                UNIT_ASSERT(st.ok());
                UNIT_ASSERT_VALUES_EQUAL_C(response.status(), Ydb::StatusIds::BAD_REQUEST, response);
            }

            {
                grpc::ClientContext context;
                Ydb::Operations::GetOperationRequest request;
                request.set_id(operationId);
                Ydb::Operations::GetOperationResponse response;
                grpc::Status st = operationServiceStub->GetOperation(&context, request, &response);
                UNIT_ASSERT(st.ok());
                UNIT_ASSERT_VALUES_EQUAL_C(response.operation().status(), Ydb::StatusIds::BAD_REQUEST, response);
            }

            {
                grpc::ClientContext context;
                Ydb::Operations::CancelOperationRequest request;
                request.set_id(operationId);
                Ydb::Operations::CancelOperationResponse response;
                grpc::Status st = operationServiceStub->CancelOperation(&context, request, &response);
                UNIT_ASSERT(st.ok());
                UNIT_ASSERT_VALUES_EQUAL_C(response.status(), Ydb::StatusIds::BAD_REQUEST, response);
            }
        }

        Y_UNIT_TEST(TestFailsOnIncorrectScriptExecutionOperationId1) {
            TestFailsOnIncorrectScriptExecutionOperation("trash", "");
        }

        Y_UNIT_TEST(TestFailsOnIncorrectScriptExecutionOperationId2) {
            TestFailsOnIncorrectScriptExecutionOperation("ydb://scriptexec/9?fd=b214872a-d040e60d-62a1b34-a9be3c3d", "trash");
        }

        Y_UNIT_TEST(TestFailsOnIncorrectScriptExecutionFetchToken) {
            TestFailsOnIncorrectScriptExecutionOperation("", "trash");
        }

        Y_UNIT_TEST(TestConnectorNotConfigured) {
            NKikimrConfig::TAppConfig appConfig;
            appConfig.MutableFeatureFlags()->SetEnableScriptExecutionOperations(true);
            appConfig.MutableFeatureFlags()->SetEnableExternalDataSources(true);

            auto kikimr = std::make_shared<TKikimrRunner>(NKqp::TKikimrSettings(appConfig)
                .SetEnableExternalDataSources(true)
                .SetEnableScriptExecutionOperations(true)
                .SetInitFederatedQuerySetupFactory(true));

            CreateExternalDataSource(EProviderType::Ydb, kikimr);

            const TString query = fmt::format(
                R"(
                SELECT * FROM {data_source_name}.{table_name};
            )",
                "data_source_name"_a = DEFAULT_DATA_SOURCE_NAME,
                "table_name"_a = DEFAULT_TABLE);

            auto db = kikimr->GetQueryClient();
            const auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToOneLineString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Unsupported. Failed to load metadata for table: /Root/external_data_source.[example_1] data source generic doesn't exist, please contact internal support");
        }
    }
}

#include "connector_client_mock.h"

#include <fmt/format.h>

namespace NYql::NConnector::NTest {

    using namespace fmt::literals;

#define DEFINE_SIMPLE_TYPE_SETTER(T, primitiveTypeId, value_name)         \
    template <>                                                           \
    void SetSimpleValue(const T& value, Ydb::TypedValue* proto) {         \
        proto->mutable_type()->set_type_id(::Ydb::Type::primitiveTypeId); \
        proto->mutable_value()->Y_CAT(set_, value_name)(value);           \
    }

    DEFINE_SIMPLE_TYPE_SETTER(bool, BOOL, bool_value);
    DEFINE_SIMPLE_TYPE_SETTER(i32, INT32, int32_value);
    DEFINE_SIMPLE_TYPE_SETTER(ui32, UINT32, uint32_value);

    void CreatePostgreSQLExternalDataSource(
        const std::shared_ptr<NKikimr::NKqp::TKikimrRunner>& kikimr,
        const TString& dataSourceName,
        NApi::EProtocol protocol,
        const TString& host,
        int port,
        const TString& login,
        const TString& password,
        bool useTls,
        const TString& databaseName,
        const TString& schema)
    {
        auto tc = kikimr->GetTableClient();
        auto session = tc.CreateSession().GetValueSync().GetSession();
        const TString query = fmt::format(
            R"(
            CREATE OBJECT {data_source_name}_password (TYPE SECRET) WITH (value={password});

            CREATE EXTERNAL DATA SOURCE {data_source_name} WITH (
                SOURCE_TYPE="{source_type}",
                LOCATION="{host}:{port}",
                AUTH_METHOD="BASIC",
                LOGIN="{login}",
                PASSWORD_SECRET_NAME="{data_source_name}_password",
                USE_TLS="{use_tls}",
                PROTOCOL="{protocol}",
                DATABASE_NAME="{database}",
                SCHEMA="{schema}"
            );
        )",
            "data_source_name"_a = dataSourceName,
            "host"_a = host,
            "port"_a = port,
            "login"_a = login,
            "password"_a = password,
            "use_tls"_a = useTls ? "TRUE" : "FALSE",
            "protocol"_a = NApi::EProtocol_Name(protocol),
            "source_type"_a = PG_SOURCE_TYPE,
            "database"_a = databaseName,
            "schema"_a = schema);
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
    }

    void CreateClickHouseExternalDataSource(
        const std::shared_ptr<NKikimr::NKqp::TKikimrRunner>& kikimr,
        const TString& dataSourceName,
        NApi::EProtocol protocol,
        const TString& clickHouseClusterId,
        const TString& login,
        const TString& password,
        bool useTls,
        const TString& serviceAccountId,
        const TString& serviceAccountIdSignature,
        const TString& databaseName)
    {
        auto tc = kikimr->GetTableClient();
        auto session = tc.CreateSession().GetValueSync().GetSession();
        const TString query = fmt::format(
            R"(
            CREATE OBJECT sa_signature (TYPE SECRET) WITH (value=sa_signature);
            CREATE OBJECT {data_source_name}_password (TYPE SECRET) WITH (value={password});

            CREATE EXTERNAL DATA SOURCE {data_source_name} WITH (
                SOURCE_TYPE="{source_type}",
                MDB_CLUSTER_ID="{cluster_id}",
                AUTH_METHOD="MDB_BASIC",
                SERVICE_ACCOUNT_ID="{service_account_id}",
                SERVICE_ACCOUNT_SECRET_NAME="{service_account_id_signature}",
                LOGIN="{login}",
                PASSWORD_SECRET_NAME="{data_source_name}_password",
                USE_TLS="{use_tls}",
                PROTOCOL="{protocol}",
                DATABASE_NAME="{database}"
            );
        )",
            "cluster_id"_a = clickHouseClusterId,
            "data_source_name"_a = dataSourceName,
            "login"_a = login,
            "password"_a = password,
            "use_tls"_a = useTls ? "TRUE" : "FALSE",
            "protocol"_a = NYql::NConnector::NApi::EProtocol_Name(protocol),
            "service_account_id"_a = serviceAccountId,
            "service_account_id_signature"_a = serviceAccountIdSignature,
            "source_type"_a = ToString(NYql::EDatabaseType::ClickHouse),
            "database"_a = databaseName);
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
    }

    void CreateYdbExternalDataSource(
        const std::shared_ptr<NKikimr::NKqp::TKikimrRunner>& kikimr,
        const TString& dataSourceName,
        const TString& login,
        const TString& password,
        const TString& endpoint,
        bool useTls,
        const TString& databaseName)
    {
        auto tc = kikimr->GetTableClient();
        auto session = tc.CreateSession().GetValueSync().GetSession();
        const TString query = fmt::format(
            R"(
            CREATE OBJECT {data_source_name}_password (TYPE SECRET) WITH (value={password});

            CREATE EXTERNAL DATA SOURCE {data_source_name} WITH (
                SOURCE_TYPE="{source_type}",
                LOCATION="{endpoint}",
                AUTH_METHOD="BASIC",
                LOGIN="{login}",
                DATABASE_NAME="{database}",
                PASSWORD_SECRET_NAME="{data_source_name}_password",
                USE_TLS="{use_tls}"
            );
        )",
            "data_source_name"_a = dataSourceName,
            "login"_a = login,
            "password"_a = password,
            "use_tls"_a = useTls ? "TRUE" : "FALSE",
            "source_type"_a = ToString(NYql::EDatabaseType::Ydb),
            "endpoint"_a = endpoint,
            "database"_a = databaseName);
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
    }

    std::shared_ptr<arrow::RecordBatch> MakeEmptyRecordBatch(size_t rowsCount) {
        return arrow::RecordBatch::Make(
            std::make_shared<arrow::Schema>(arrow::FieldVector()),
            static_cast<i64>(rowsCount),
            std::vector<std::shared_ptr<arrow::Array>>());
    }

} // namespace NYql::NConnector::NTest

#include "iceberg_ut_data.h"

#include <fmt/format.h>
#include <ydb/core/external_sources/iceberg_fields.h>

namespace NTestUtils {

constexpr char VALUE_HIVE_METASTORE_URI[] = "hive_metastore_uri";
constexpr char VALUE_S3_URI[]             = "s3_uri";
constexpr char VALUE_S3_ENDPOINT[]        = "s3_endpoint";
constexpr char VALUE_S3_REGION[]          = "s3_region";

constexpr char VALUE_IAM[] = "IAM";

struct TTestData {
    TTestData(TIcebergTestData* data) 
        : Credentials_(*Result_.mutable_credentials())
        , Options_(*Result_.mutable_iceberg_options())
        , Warehouse_(*Options_.mutable_warehouse())
        , Catalog_(*Options_.mutable_catalog())
    {
        assert(data);

        switch (data->Auth_.Type) {
            case TIcebergTestData::AuthBasic: {
                auto& auth = *Credentials_.mutable_basic();
                auth.set_username(data->Auth_.Id);
                auth.set_password(data->Auth_.Value);
                break;
            }
            case TIcebergTestData::AuthSa:
            case TIcebergTestData::AuthToken: {
                auto& auth = *Credentials_.mutable_token();
                auth.set_type(data->Auth_.Id);
                auth.set_value(data->Auth_.Value);
                break;
            }
        }

        Result_.mutable_endpoint();
        Result_.set_kind(::NYql::EGenericDataSourceKind::ICEBERG);
        Result_.set_database(data->Database_);
        Result_.set_use_tls(data->UseTls_);
        Result_.set_protocol(::NYql::EGenericProtocol::NATIVE);

        auto& s3 = *Warehouse_.mutable_s3();

        s3.set_uri(VALUE_S3_URI);
        s3.set_endpoint(VALUE_S3_ENDPOINT);
        s3.set_region(VALUE_S3_REGION);
    }

    NYql::TGenericDataSourceInstance Result_;
    NYql::TGenericCredentials& Credentials_;
    NYql::TIcebergDataSourceOptions& Options_;
    NYql::TIcebergWarehouse& Warehouse_;
    NYql::TIcebergCatalog& Catalog_;
};

TIcebergTestData::TIcebergTestData(
    TAuth auth,
    const TString& dataSourceName, 
    const TString& database, 
    bool useTls)
    : Auth_(auth)
    , DataSourceName_(dataSourceName)
    , Database_(database)
    , UseTls_(useTls)
{}

NYql::TGenericDataSourceInstance TIcebergTestData::CreateDataSourceForHadoop() {
    TTestData data(this);
    data.Catalog_.mutable_hadoop();
    return data.Result_;
}

NYql::TGenericDataSourceInstance TIcebergTestData::CreateDataSourceForHiveMetastore() {
    TTestData data(this);
    auto& h = *data.Catalog_.mutable_hive_metastore();
    h.set_uri(VALUE_HIVE_METASTORE_URI);
    return data.Result_;
}

TString TIcebergTestData::CreateAuthSection() {
    using namespace fmt::literals;

    switch (Auth_.Type) {
        case TIcebergTestData::AuthBasic:
            return fmt::format(R"(
                AUTH_METHOD="BASIC",
                LOGIN="{login}",
                PASSWORD_SECRET_NAME="{data_source_name}_p"
            )",
                "data_source_name"_a = DataSourceName_,
                "login"_a            = Auth_.Id,
                "password"_a         = Auth_.Value
            );
        case TIcebergTestData::AuthToken:
            return fmt::format(R"(
                AUTH_METHOD="TOKEN",
                TOKEN_SECRET_NAME="{data_source_name}_p"
            )",
                "data_source_name"_a = DataSourceName_
            );
        case TIcebergTestData::AuthSa: 
            return fmt::format(R"(
                AUTH_METHOD="SERVICE_ACCOUNT",
                SERVICE_ACCOUNT_ID="my_sa",
                SERVICE_ACCOUNT_SECRET_NAME="{data_source_name}_p"
            )",
                "data_source_name"_a = DataSourceName_
            );
    };
}

TString TIcebergTestData::CreateQuery(const TString& catalogSection) {
    using namespace fmt::literals;

    return fmt::format(
        R"(
        CREATE OBJECT {data_source_name}_p (TYPE SECRET) WITH (value={secret});

        CREATE EXTERNAL DATA SOURCE {data_source_name} WITH (
            SOURCE_TYPE="{source_type}",
            DATABASE_NAME="{database}",
            WAREHOUSE_TYPE="{s3}",
            WAREHOUSE_S3_REGION="{s3_region}",
            WAREHOUSE_S3_ENDPOINT="{s3_endpoint}",
            WAREHOUSE_S3_URI="{s3_uri}",
            {auth_section},
            {catalog_section},
            USE_TLS="{use_tls}"
        );
    )",
        "auth_section"_a     = CreateAuthSection(),
        "s3"_a               = NKikimr::NExternalSource::NIceberg::VALUE_S3,
        "s3_region"_a        = VALUE_S3_REGION,
        "s3_endpoint"_a      = VALUE_S3_ENDPOINT,
        "s3_uri"_a           = VALUE_S3_URI,
        "data_source_name"_a = DataSourceName_,
        "catalog_section"_a  = catalogSection,
        "secret"_a           = Auth_.Value,
        "use_tls"_a          = UseTls_ ? "TRUE" : "FALSE",
        "source_type"_a      = ToString(NYql::EDatabaseType::Iceberg),
        "database"_a         = Database_
    );
}

void TIcebergTestData::ExecuteQuery(const std::shared_ptr<NKikimr::NKqp::TKikimrRunner>& kikimr, 
                                    const TString& query)
{
    auto c = kikimr->GetTableClient();
    auto session = c.CreateSession().GetValueSync().GetSession();
    auto result = session.ExecuteSchemeQuery(query).GetValueSync();
    UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
}


void TIcebergTestData::ExecuteCreateHiveMetastoreExternalDataSource(const std::shared_ptr<NKikimr::NKqp::TKikimrRunner>& kikimr) {
    using namespace fmt::literals;

    TString hiveMetastoreCatalog = fmt::format(R"(
        CATALOG_TYPE="{type}",
        CATALOG_HIVE_METASTORE_URI="{uri}"
    )",
        "type"_a = NKikimr::NExternalSource::NIceberg::VALUE_HIVE_METASTORE,
        "uri"_a  = VALUE_HIVE_METASTORE_URI
    );

    ExecuteQuery(kikimr, CreateQuery(hiveMetastoreCatalog));
}

void TIcebergTestData::ExecuteCreateHadoopExternalDataSource(const std::shared_ptr<NKikimr::NKqp::TKikimrRunner>& kikimr) {
    using namespace fmt::literals;

    TString hadoopCatalog = fmt::format(R"(
        CATALOG_TYPE="{type}"
    )",
        "type"_a = NKikimr::NExternalSource::NIceberg::VALUE_HADOOP
    );

    ExecuteQuery(kikimr, CreateQuery(hadoopCatalog));
}


TIcebergTestData CreateIcebergBasic(const TString& dataSourceName, const TString& database, const TString& userName, const TString& password){
    return TIcebergTestData({TIcebergTestData::EAuthType::AuthBasic, userName, password}, dataSourceName, database, false);
}

TIcebergTestData CreateIcebergToken(const TString& dataSourceName, const TString& database, const TString& token) {
    return TIcebergTestData({TIcebergTestData::EAuthType::AuthToken, VALUE_IAM , token}, dataSourceName, database, false);
}

TIcebergTestData CreateIcebergSa(const TString& dataSourceName, const TString& database, const TString& token) {
    return TIcebergTestData({TIcebergTestData::EAuthType::AuthSa,VALUE_IAM, token}, dataSourceName, database, false);
}

} // NTestUtils

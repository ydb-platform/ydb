#include <util/string/builder.h>
#include <util/string/strip.h>
#include <contrib/libs/fmt/include/fmt/format.h>
#include <ydb/core/external_sources/iceberg_fields.h>
#include <ydb/core/fq/libs/result_formatter/result_formatter.h>
#include <ydb/core/fq/libs/common/util.h>

#include "iceberg_processor.h"

namespace NFq {

constexpr char VALUE_DEFAULT_REGION[] = "ru-central1";

TString RemoveTrailingSlashes(const TString& str) {
    if (str.empty()) {
        return "";
    }

    const auto first = str.find_first_not_of('/');

    if (TString::npos == first) {
        return "";
    }

    const auto last = str.find_last_not_of('/');
    return str.substr(first, last - first + 1);
}

TIcebergProcessor::TIcebergProcessor(const FederatedQuery::Iceberg& config, NYql::TIssues& issues)
    : Config_(config)
    , Issues_(&issues)
{ }

TIcebergProcessor::TIcebergProcessor(const FederatedQuery::Iceberg& config)
    : Config_(config)
    , Issues_(nullptr)
{ }

void TIcebergProcessor::TIcebergProcessor::Process() {
    if (!Config_.has_warehouse_auth()
        || Config_.warehouse_auth().identity_case() == FederatedQuery::IamAuth::IDENTITY_NOT_SET) {
        DoOnPropertyRequiredError("warehouse.auth");
    }

    ProcessSkipAuth();
}

void TIcebergProcessor::ProcessSkipAuth() {
    if (!Config_.has_warehouse()) {
        DoOnPropertyRequiredError("warehouse");
    } else {
        ProcessWarehouse(Config_.warehouse());
    }

    if (!Config_.has_catalog()) {
        DoOnPropertyRequiredError("catalog");
    } else {
        ProcessCatalog(Config_.catalog());
    }
}

void TIcebergProcessor::DoOnPropertyRequiredError(const TString& property) {
    DoOnError(property, "has to be set");
}

void TIcebergProcessor::DoOnError(const TString& property, const TString& msg) {
    if (!Issues_) {
        throw yexception() << property << ": " << msg;
    }

    auto m = TStringBuilder() 
        << "content.setting.iceberg."
        << property << " "
        << msg;

    Issues_->AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, m));
}

void TIcebergProcessor::ProcessWarehouse(const FederatedQuery::IcebergWarehouse& warehouse) {
    if (warehouse.has_s3()) {
        ProcessWarehouseS3(warehouse.s3());
    } else {
        DoOnPropertyRequiredError("warehouse.type");
    }
}

void TIcebergProcessor::ProcessWarehouseS3(const FederatedQuery::IcebergWarehouse_S3& s3) {
    TString bucket;

    if (!s3.has_bucket()
        || (bucket = RemoveTrailingSlashes(s3.bucket())).empty()) {
        DoOnPropertyRequiredError("warehouse.s3.bucket");
    }

    if (OnS3Callback_ && !HasErrors()) {
        auto uri = TStringBuilder() << bucket;
        auto path = RemoveTrailingSlashes(s3.path());

        if (!path.empty()) {
            uri << "/" << path;
        }

        OnS3Callback_(s3, uri);
    }
}

void TIcebergProcessor::ProcessCatalogHadoop(const FederatedQuery::IcebergCatalog_Hadoop& hadoop) {
    if (!hadoop.has_directory()
        || hadoop.directory().empty()) {
        DoOnPropertyRequiredError("hadoop.directory");
    }

    if (OnHadoopCallback_ && !HasErrors()) {
        OnHadoopCallback_(hadoop);
    }
}

void TIcebergProcessor::ProcessCatalogHiveMetastore(const FederatedQuery::IcebergCatalog_HiveMetastore& hive) {
    if (!hive.has_uri()
        || hive.uri().empty()) {
        DoOnPropertyRequiredError("hive_metastore.uri");
    }

    if (!hive.has_database_name()
        || hive.database_name().empty()) {
        DoOnPropertyRequiredError("hive_metastore.database_name");
    }

    if (OnHiveCallback_ && !HasErrors()) {
        OnHiveCallback_(hive);
    }
}

void TIcebergProcessor::ProcessCatalog(const FederatedQuery::IcebergCatalog& catalog) {
    if (catalog.has_hive_metastore()) {
        ProcessCatalogHiveMetastore(catalog.hive_metastore());
    } else if (catalog.has_hadoop()) {
        ProcessCatalogHadoop(catalog.hadoop());
    } else {
        DoOnPropertyRequiredError("catalog.type");
    }
}

TString MakeIcebergCreateExternalDataSourceProperties(const NConfig::TCommonConfig& yqConfig, const FederatedQuery::Iceberg& config) {
    using namespace fmt::literals;
    using namespace NKikimr::NExternalSource::NIceberg;

    TIcebergProcessor processor(config);

    // warehouse configuration
    TString warehouseSection;

    processor.SetDoOnWarehouseS3([&warehouseSection, &yqConfig](const FederatedQuery::IcebergWarehouse_S3&, const TString& uri) {
        warehouseSection = fmt::format(
            R"(
                {warehouse_type}={warehouse_type_value},
                {warehouse_s3_region}={warehouse_s3_region_value},
                {warehouse_s3_endpoint}={warehouse_s3_endpoint_value},
                {warehouse_s3_uri}={warehouse_s3_uri_value}
            )",
            "warehouse_type"_a              = WAREHOUSE_TYPE,
            "warehouse_type_value"_a        = EncloseAndEscapeString(VALUE_S3, '"'),
            "warehouse_s3_region"_a         = WAREHOUSE_S3_REGION,
            "warehouse_s3_region_value"_a   = EncloseAndEscapeString(VALUE_DEFAULT_REGION, '"'),
            "warehouse_s3_endpoint"_a       = WAREHOUSE_S3_ENDPOINT,
            "warehouse_s3_endpoint_value"_a = EncloseAndEscapeString(yqConfig.GetObjectStorageEndpoint(), '"'),
            "warehouse_s3_uri"_a            = WAREHOUSE_S3_URI,
            "warehouse_s3_uri_value"_a      = EncloseAndEscapeString(uri, '"')
        );
    });

    // catalog configuration
    TString catalogSection;

    processor.SetDoOnCatalogHive([&catalogSection](const FederatedQuery::IcebergCatalog_HiveMetastore& hiveMetastore) {
        catalogSection = fmt::format(
            R"(
                {catalog_type}={catalog_type_value},
                {catalog_hive_metastore_uri}={catalog_hive_metastore_uri_value},
                database_name={database_name}
            )",
            "catalog_type"_a                     = CATALOG_TYPE,
            "catalog_type_value"_a               = EncloseAndEscapeString(VALUE_HIVE_METASTORE, '"'),
            "catalog_hive_metastore_uri"_a       = CATALOG_HIVE_METASTORE_URI,
            "catalog_hive_metastore_uri_value"_a = EncloseAndEscapeString(hiveMetastore.uri(), '"'),
            "database_name"_a                    = EncloseAndEscapeString(hiveMetastore.database_name(), '"')
        );
    });

    processor.SetDoOnCatalogHadoop([&catalogSection](const FederatedQuery::IcebergCatalog_Hadoop& hadoop) {
        catalogSection = fmt::format(
            R"(
                {catalog_type}={catalog_type_value},
                database_name={database_name}
            )",
            "catalog_type"_a            = CATALOG_TYPE,
            "catalog_type_value"_a      = EncloseAndEscapeString(VALUE_HADOOP, '"'),
            "database_name"_a           = EncloseAndEscapeString(hadoop.directory(), '"')
        );
    });

    processor.Process();

    // common configuration for all warehouses and catalogs  
    TString commonSection = fmt::format(
        R"(
            source_type="Iceberg",
            use_tls="{use_tls}"
        )",
        "use_tls"_a = !yqConfig.GetDisableSslForGenericDataSources() ? "true" : "false"
    );

    // merge config
    auto r = fmt::format(
        R"(
            {common_section},
            {warehouse_section},
            {catalog_section}
        )",
        "common_section"_a = commonSection,
        "warehouse_section"_a = warehouseSection,
        "catalog_section"_a = catalogSection
    );

    return r;
}

void FillIcebergGenericClusterConfig(const NConfig::TCommonConfig& yqConfig, const FederatedQuery::Iceberg& config, ::NYql::TGenericClusterConfig& cluster) {
    using namespace NKikimr::NExternalSource::NIceberg;

    TIcebergProcessor processor(config);
    cluster.SetKind(NYql::EGenericDataSourceKind::ICEBERG);

    auto& options = *cluster.MutableDataSourceOptions();

    processor.SetDoOnWarehouseS3([&options, &yqConfig](const FederatedQuery::IcebergWarehouse_S3&, const TString& uri) {
        options[WAREHOUSE_TYPE]         = VALUE_S3;
        options[WAREHOUSE_S3_ENDPOINT]  = yqConfig.GetObjectStorageEndpoint();
        options[WAREHOUSE_S3_REGION]    = VALUE_DEFAULT_REGION;
        options[WAREHOUSE_S3_URI]       = uri;
    });

    processor.SetDoOnCatalogHive([&options, &cluster](const FederatedQuery::IcebergCatalog_HiveMetastore& hiveMetastore) {
        options[CATALOG_TYPE]               = VALUE_HIVE_METASTORE;
        options[CATALOG_HIVE_METASTORE_URI] = hiveMetastore.uri();

        cluster.SetDatabaseName(hiveMetastore.database_name());
    });

    processor.SetDoOnCatalogHadoop([&options, &cluster](const FederatedQuery::IcebergCatalog_Hadoop& hadoop) {
        options[CATALOG_TYPE] = VALUE_HADOOP;

        cluster.SetDatabaseName(hadoop.directory());
    });

    processor.ProcessSkipAuth();
}

} // NFq

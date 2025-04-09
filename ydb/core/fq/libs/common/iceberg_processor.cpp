#include <util/string/builder.h>
#include <util/string/strip.h>
#include <contrib/libs/fmt/include/fmt/format.h>
#include <ydb/core/external_sources/iceberg_fields.h>
#include <ydb/core/fq/libs/result_formatter/result_formatter.h>
#include <ydb/core/fq/libs/common/util.h>

#include "iceberg_processor.h"

namespace NFq {

TIcebergProcessor::TIcebergProcessor(const FederatedQuery::Iceberg& config, NYql::TIssues& issues)
    : Config_(config)
    , Issues_(&issues)
{
}

TIcebergProcessor::TIcebergProcessor(const FederatedQuery::Iceberg& config) 
    : Config_(config)
    , Issues_(nullptr)
{
}

void TIcebergProcessor::TIcebergProcessor::Process() {
    if (!Config_.has_warehouse_auth()
        || Config_.warehouse_auth().identity_case() == FederatedQuery::IamAuth::IDENTITY_NOT_SET) {
        RiseError("warehouse.auth","is not specified");
    }

    ProcessSkipAuth();
}

void TIcebergProcessor::ProcessSkipAuth() {
    if (!Config_.database_name()) {
        RiseError("database","is not specified");
    }

    if (!Config_.has_warehouse()) {
        RiseError("warehouse","is not specified");
    } else {
        ProcessWarehouse(Config_.warehouse());
    }

    if (!Config_.has_catalog()) {
        RiseError("catalog","is not specified");
    } else {
        ProcessCatalog(Config_.catalog());
    }
}

std::vector<NYql::TIssue> TIcebergProcessor::GetIssues() const {
    if (!Issues_) {
        throw yexception() << "Issues is not set";
    }

    std::vector<NYql::TIssue> issues;
    std::copy(Issues_->begin(), Issues_->end(), std::back_inserter(issues));
    return std::move(issues);
}

void TIcebergProcessor::RiseError(const TString& property, const TString& msg) {
    if (!Issues_) {
        throw yexception() << property << ": " << msg;
    }

    auto m = TStringBuilder() 
        << "content.setting.iceberg."
        << property << " "
        << msg;

    Issues_->AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, m));
}

void TIcebergProcessor::ProcessWarehouse(const FederatedQuery::TIcebergWarehouse& warehouse) {
    if (warehouse.has_s3()) {
        ProcessWarehouseS3();
    } else {
        RiseError("warehouse.type", "is not specified");
    }
}

void TIcebergProcessor::ProcessWarehouseS3() {
    const auto& warehouse = Config_.warehouse();

    if (!warehouse.s3().has_endpoint()) {
        RiseError("warehouse.s3.endpoint","is required");
    }

    if (!warehouse.s3().has_uri()) {
        RiseError("warehouse.s3.uri","is required");
    }

    if (!warehouse.s3().has_region()) {
        RiseError("warehouse.s3.region","is required");
    }

    if (OnS3Callback_ && !GetHasErrors()) {
        OnS3Callback_(warehouse.s3());
    }
}

void TIcebergProcessor::ProcessCatalogHadoop(const FederatedQuery::TIcebergCatalog_THadoop& hadoop) {
    if (OnHadoopCallback_ && !GetHasErrors()) {
        OnHadoopCallback_(hadoop);
    }
}

void TIcebergProcessor::ProcessCatalogHive(const FederatedQuery::TIcebergCatalog_THive& hive) {
    if (!hive.has_uri()) {
        RiseError("hive.uri", "is not specified");
    }

    if (OnHiveCallback_ && !GetHasErrors()) {
        OnHiveCallback_(hive);
    }
}

void TIcebergProcessor::ProcessCatalog(const FederatedQuery::TIcebergCatalog& catalog) {
    if (catalog.has_hive()) {
        ProcessCatalogHive(catalog.hive());
    } else if (catalog.has_hadoop()) {
        ProcessCatalogHadoop(catalog.hadoop());
    } else {
        RiseError("catalog.type", "is not specified");
    }
}

TString MakeIcebergCreateExternalDataSourceProperties(const FederatedQuery::Iceberg& config, bool useTls) {
    using namespace fmt::literals;
    using namespace NKikimr::NExternalSource::NIceberg;

    TIcebergProcessor processor(config);

    // warehouse configuration
    TString warehouseSection;

    processor.SetDoOnWarehouseS3([&warehouseSection](const FederatedQuery::TIcebergWarehouse_TS3& s3)-> void {
        warehouseSection = fmt::format(
            R"(
                {warehouse_type}={warehouse_type_value},
                {warehouse_s3_region}={warehouse_s3_region_value},
                {warehouse_s3_endpoint}={warehouse_s3_endpoint_value},
                {warehouse_s3_uri}={warehouse_s3_uri_value}
            )",
            "warehouse_type"_a              = to_upper(TString{WAREHOUSE_TYPE}),
            "warehouse_type_value"_a        = EncloseAndEscapeString(VALUE_S3, '"'),
            "warehouse_s3_region"_a         = to_upper(TString{WAREHOUSE_S3_REGION}),
            "warehouse_s3_region_value"_a   = EncloseAndEscapeString(s3.region(), '"'),
            "warehouse_s3_endpoint"_a       = to_upper(TString{WAREHOUSE_S3_ENDPOINT}),
            "warehouse_s3_endpoint_value"_a = EncloseAndEscapeString(s3.endpoint(), '"'),
            "warehouse_s3_uri"_a            = to_upper(TString{WAREHOUSE_S3_URI}),
            "warehouse_s3_uri_value"_a      = EncloseAndEscapeString(s3.uri(), '"')
        );
    });

    // catalog configuration
    TString catalogSection;

    processor.SetDoOnCatalogHive([&catalogSection](const FederatedQuery::TIcebergCatalog_THive& hive) -> void {
        catalogSection = fmt::format(
            R"(
                {catalog_type}={catalog_type_value},
                {catalog_hive_uri}={catalog_hive_uri_value}
            )",
            "catalog_type"_a            = to_upper(TString{CATALOG_TYPE}),
            "catalog_type_value"_a      = EncloseAndEscapeString(TString{VALUE_HIVE}, '"'),
            "catalog_hive_uri"_a        = to_upper(TString{CATALOG_HIVE_URI}),
            "catalog_hive_uri_value"_a  = EncloseAndEscapeString(hive.uri(), '"')
        );
    });

    processor.SetDoOnCatalogHadoop([&catalogSection](const FederatedQuery::TIcebergCatalog_THadoop&) -> void {
        catalogSection = fmt::format(
            R"(
                {catalog_type}={catalog_type_value}
            )",
            "catalog_type"_a            = to_upper(TString{CATALOG_TYPE}),
            "catalog_type_value"_a      = EncloseAndEscapeString(VALUE_HADOOP, '"')
        );
    });

    processor.Process();

    // common configuration for all warehouses and catalogs  
    TString commonSection = fmt::format(
        R"(
            SOURCE_TYPE="Iceberg",
            DATABASE_NAME={database_name},
            USE_TLS="{use_tls}"
        )",
        "database_name"_a = EncloseAndEscapeString(config.database_name(), '"'),
        "use_tls"_a = useTls ? "true" : "false"
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

void FillIcebergGenericClusterConfig(const FederatedQuery::Iceberg& config, ::NYql::TGenericClusterConfig& cluster) {
    using namespace NKikimr::NExternalSource::NIceberg;

    TIcebergProcessor processor(config);
    cluster.SetKind(NYql::EGenericDataSourceKind::ICEBERG);

    auto& options = *cluster.MutableDataSourceOptions();

    processor.SetDoOnWarehouseS3([&options](const FederatedQuery::TIcebergWarehouse_TS3& s3) -> void{
        options[WAREHOUSE_TYPE]         = VALUE_S3;
        options[WAREHOUSE_S3_ENDPOINT]  = s3.endpoint();
        options[WAREHOUSE_S3_REGION]    = s3.region();
        options[WAREHOUSE_S3_URI]       = s3.uri();
    });

    processor.SetDoOnCatalogHive([&options](const FederatedQuery::TIcebergCatalog_THive& hive) -> void{
        options[CATALOG_TYPE]           = VALUE_HIVE;
        options[CATALOG_HIVE_URI]       = hive.uri();
    });

    processor.SetDoOnCatalogHadoop([&options](const FederatedQuery::TIcebergCatalog_THadoop&) -> void{
        options[CATALOG_TYPE] = VALUE_HADOOP;
    });

    processor.ProcessSkipAuth();
    cluster.SetDatabaseName(config.database_name());
}

} // NFq

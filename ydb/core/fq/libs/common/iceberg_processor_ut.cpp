#include "iceberg_processor.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/result/result.h>
#include <ydb/services/ydb/ydb_common_ut.h>
#include <library/cpp/testing/gtest/gtest.h>

namespace NFq {

namespace {

struct TClusterConfigBuilder {
    TString DatabaseName;
    TString Token;
    TString S3Endpoint;
    TString S3Uri;
    TString S3Region;
    TString HiveUri;
    TString Hadoop;

    TClusterConfigBuilder& FillDb() {
        DatabaseName = "db";
        return *this;
    }

    TClusterConfigBuilder& FillWarehouseWithS3() {
        S3Endpoint = "endpoint";
        S3Uri = "uri";
        S3Region = "region";
        return *this;
    }

    TClusterConfigBuilder& FillAuthToken() {
        Token = "token";
        return *this;
    }

    TClusterConfigBuilder& FillHadoopCatalog() {
        Hadoop = "hadoop";
        return *this;
    }

    TClusterConfigBuilder& FillHiveCatalog() {
        HiveUri = "hive_uri";
        return *this;
    }

    FederatedQuery::Iceberg Build() {
        FederatedQuery::Iceberg cluster;

        if (!DatabaseName.empty()) {
            cluster.set_database_name(DatabaseName);
        }

        if (!Token.empty()) {
            cluster.mutable_warehouse_auth()
                ->mutable_token()
                ->set_token(Token);
        }

        if (!S3Endpoint.empty()) {
            cluster.mutable_warehouse()
                ->mutable_s3()
                ->set_endpoint(S3Endpoint);
        }

        if (!S3Uri.empty()) {
            cluster.mutable_warehouse()
                ->mutable_s3()
                ->set_uri(S3Uri);
        }

        if (!S3Region.empty()) {
            cluster.mutable_warehouse()
                ->mutable_s3()
                ->set_region(S3Region);
        }

        if (!Hadoop.empty()) {
            cluster.mutable_catalog()
                ->mutable_hadoop();
        }

        if (!HiveUri.empty()) {
            cluster.mutable_catalog()
                ->mutable_hive()
                ->set_uri(HiveUri);
        }

        return cluster;
    }

};

std::vector<TString> GetErrorsFromIssues(const NYql::TIssues& issues) {
    std::vector<TString> result;

    std::transform(
        issues.begin(),
        issues.end(),
        std::back_inserter(result),
        [](const NYql::TIssue& issue)-> TString { return issue.GetMessage(); }
    );

    return result;
}

} // unnamed 

Y_UNIT_TEST_SUITE(IcebergClusterProcessor) {

    // Test ddl creation for a hadoop catalog with s3 warehouse
    Y_UNIT_TEST(ValidateDdlCreationForHadoopWithS3) {
        auto cluster = TClusterConfigBuilder()
            .FillDb()
            .FillWarehouseWithS3()
            .FillAuthToken()
            .FillHadoopCatalog()
            .Build();

        auto ddlProperties = MakeIcebergCreateExternalDataSourceProperties(cluster, false);
        SubstGlobal(ddlProperties, " ", "");
        SubstGlobal(ddlProperties, "\n", "");

        UNIT_ASSERT_VALUES_EQUAL(ddlProperties, "SOURCE_TYPE=\"Iceberg\",DATABASE_NAME=\"db\",USE_TLS=\"false\",WAREHOUSE_TYPE=\"s3\",WAREHOUSE_S3_REGION=\"region\",WAREHOUSE_S3_ENDPOINT=\"endpoint\",WAREHOUSE_S3_URI=\"uri\",CATALOG_TYPE=\"hadoop\"");
    }

    // Test ddl creation for a hive catalog with s3 warehouse
    Y_UNIT_TEST(ValidateDdlCreationForHiveWithS3) {
        auto cluster = TClusterConfigBuilder()
            .FillDb()
            .FillWarehouseWithS3()
            .FillAuthToken()
            .FillHiveCatalog()
            .Build();

        auto ddlProperties = MakeIcebergCreateExternalDataSourceProperties(cluster, true);
        SubstGlobal(ddlProperties, " ", "");
        SubstGlobal(ddlProperties, "\n", "");

        UNIT_ASSERT_VALUES_EQUAL(ddlProperties, "SOURCE_TYPE=\"Iceberg\",DATABASE_NAME=\"db\",USE_TLS=\"true\",WAREHOUSE_TYPE=\"s3\",WAREHOUSE_S3_REGION=\"region\",WAREHOUSE_S3_ENDPOINT=\"endpoint\",WAREHOUSE_S3_URI=\"uri\",CATALOG_TYPE=\"hive\",CATALOG_HIVE_URI=\"hive_uri\"");
    }

    // Test parsing for FederatedQuery::IcebergCluster without warehouse
    Y_UNIT_TEST(ValidateConfigurationWithoutWarehouse) {
        NYql::TIssues issues;
        auto cluster = TClusterConfigBuilder()
            .FillDb()
            .FillAuthToken()
            .FillHiveCatalog()
            .Build();

        TIcebergProcessor processor(cluster, issues);
        processor.Process();
        auto r = GetErrorsFromIssues(issues);

        UNIT_ASSERT_VALUES_EQUAL(r.size(), 1);
        UNIT_ASSERT_STRING_CONTAINS(r[0], "warehouse");
    }

    // Test parsing for FederatedQuery::IcebergCluster without catalog
    Y_UNIT_TEST(ValidateConfigurationWithoutCatalog) {
        NYql::TIssues issues;
        auto cluster = TClusterConfigBuilder()
            .FillDb()
            .FillWarehouseWithS3()
            .FillAuthToken()
            .Build();

        TIcebergProcessor processor(cluster, issues);
        processor.Process();
        auto r = GetErrorsFromIssues(issues);

        UNIT_ASSERT_VALUES_EQUAL(r.size(), 1);
        UNIT_ASSERT_STRING_CONTAINS(r[0], "catalog");
    }

    //
    // Test parsing for FederatedQuery::Iceberg
    // 
    // NB: For such test it's better to use parameterized test  
    // Unittest does not support it yet, switch to gtest ?
    // 
    Y_UNIT_TEST(ValidateRiseErrors) {
        // initially fill all params in test cases then
        // remove one by one value for params and expect error 
        NFq::TClusterConfigBuilder params = {
            .DatabaseName   = "db",
            .Token          = "token",
            .S3Endpoint     = "endpoint",
            .S3Uri          = "uri",
            .S3Region       = "region",
            .HiveUri        = "hive.uri",
            .Hadoop         = "hadoop"
        };

        // Defines which errors to expect if param is not set
        std::vector<std::pair<TString*, std::vector<TString>>> cases = {
            {&params.DatabaseName, {"database"}},
            {&params.Token, {"warehouse.auth"}},
            {&params.S3Endpoint,{"s3.endpoint"}},
            {&params.S3Uri, {"s3.uri"}},
            {&params.S3Region, {"s3.region"}},
            // if hive is not set, hadoop is set, so no errors
            {&params.HiveUri, {}},
            // if hadoop is not set, hive is set, so no errors
            {&params.Hadoop, {}},
        };

        // Unset value and expect errors 
        for (auto [propertyValue, waitErrors] : cases) {
            auto oldValue = *propertyValue;
            *propertyValue = "";

            auto cluster = params.Build();
            NYql::TIssues issues;
            TIcebergProcessor processor(cluster,issues);

            processor.Process();
            auto r = GetErrorsFromIssues(issues);

            UNIT_ASSERT_VALUES_EQUAL(r.size(), waitErrors.size());

            for (size_t i = 0; i < waitErrors.size(); ++i) {
                UNIT_ASSERT_STRING_CONTAINS(r[i], waitErrors[i]);
            }

            *propertyValue = oldValue;
        }
    }
}


} // NFq

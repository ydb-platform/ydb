#include "iceberg_processor.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/result/result.h>
#include <ydb/services/ydb/ydb_common_ut.h>
#include <library/cpp/testing/gtest/gtest.h>

namespace NFq {

namespace {

struct TClusterConfigBuilder {
    TString Token;
    TString S3Path;
    TString S3Bucket;
    TString HiveMetastoreUri;
    TString HiveMetastoreDb;
    TString HadoopDir;

    TClusterConfigBuilder& FillWarehouseWithS3() {
        S3Bucket = "//bucket//";
        S3Path = "//path//";
        return *this;
    }

    TClusterConfigBuilder& FillAuthToken() {
        Token = "token";
        return *this;
    }

    TClusterConfigBuilder& FillHadoopCatalog() {
        HadoopDir = "hadoop_dir";
        return *this;
    }

    TClusterConfigBuilder& FillHiveMetastoreCatalog() {
        HiveMetastoreUri = "hive_metastore_uri";
        HiveMetastoreDb = "hive_metastore_db";
        return *this;
    }

    FederatedQuery::Iceberg Build() {
        FederatedQuery::Iceberg cluster;

        if (!Token.empty()) {
            cluster.mutable_warehouse_auth()
                ->mutable_token()
                ->set_token(Token);
        }

        if (!S3Path.empty()) {
            cluster.mutable_warehouse()
                ->mutable_s3()
                ->set_path(S3Path);
        }

        if (!S3Bucket.empty()) {
            cluster.mutable_warehouse()
                ->mutable_s3()
                ->set_bucket(S3Bucket);
        }

        if (!HadoopDir.empty()) {
            cluster.mutable_catalog()
                ->mutable_hadoop()
                ->set_directory(HadoopDir);
        }

        if (!HiveMetastoreUri.empty()) {
            cluster.mutable_catalog()
                ->mutable_hive_metastore()
                ->set_uri(HiveMetastoreUri);
        }

        if (!HiveMetastoreDb.empty()) {
            cluster.mutable_catalog()
                ->mutable_hive_metastore()
                ->set_database_name(HiveMetastoreDb);
        }

        return cluster;
    }

    TClusterConfigBuilder clone() {
        TClusterConfigBuilder bld(*this);
        return bld;
    }

    TClusterConfigBuilder& SetToken(const TString& token) {
        Token = token;
        return *this;
    }

    TClusterConfigBuilder& SetS3Bucket(const TString& bucket) {
        S3Bucket = bucket;
        return *this;
    }

    TClusterConfigBuilder& SetS3Path(const TString& path) {
        S3Path = path;
        return *this;
    }

    TClusterConfigBuilder& SetHiveMetastoreUri(const TString& uri) {
        HiveMetastoreUri = uri;
        return *this;
    }

    TClusterConfigBuilder& SetHiveMetastoreDb(const TString& db) {
        HiveMetastoreDb = db;
        return *this;
    }

    TClusterConfigBuilder& SetHadoopDir(const TString& dir) {
        HadoopDir = dir;
        return *this;
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
            .FillWarehouseWithS3()
            .FillAuthToken()
            .FillHadoopCatalog()
            .Build();

        NConfig::TCommonConfig common;
        common.SetDisableSslForGenericDataSources(true);
        common.SetObjectStorageEndpoint("s3endpoint");

        auto ddlProperties = MakeIcebergCreateExternalDataSourceProperties(common, cluster);
        SubstGlobal(ddlProperties, " ", "");
        SubstGlobal(ddlProperties, "\n", "");

        UNIT_ASSERT_VALUES_EQUAL(ddlProperties, "source_type=\"Iceberg\",use_tls=\"false\",warehouse_type=\"s3\",warehouse_s3_region=\"ru-central1\",warehouse_s3_endpoint=\"s3endpoint\",warehouse_s3_uri=\"bucket/path\",catalog_type=\"hadoop\",database_name=\"hadoop_dir\"");
    }

    // Test ddl creation for a hive catalog with s3 warehouse
    Y_UNIT_TEST(ValidateDdlCreationForHiveWithS3) {
        auto cluster = TClusterConfigBuilder()
            .SetS3Bucket("s3a://iceberg-bucket/")
            .SetS3Path("/storage/")
            .FillAuthToken()
            .FillHiveMetastoreCatalog()
            .Build();

        NConfig::TCommonConfig common;
        common.SetDisableSslForGenericDataSources(false);
        common.SetObjectStorageEndpoint("s3endpoint");

        auto ddlProperties = MakeIcebergCreateExternalDataSourceProperties(common, cluster);
        SubstGlobal(ddlProperties, " ", "");
        SubstGlobal(ddlProperties, "\n", "");

        UNIT_ASSERT_VALUES_EQUAL(ddlProperties, "source_type=\"Iceberg\",use_tls=\"true\",warehouse_type=\"s3\",warehouse_s3_region=\"ru-central1\",warehouse_s3_endpoint=\"s3endpoint\",warehouse_s3_uri=\"s3a://iceberg-bucket/storage\",catalog_type=\"hive_metastore\",catalog_hive_metastore_uri=\"hive_metastore_uri\",database_name=\"hive_metastore_db\"");
    }

    // Test parsing for FederatedQuery::IcebergCluster without warehouse
    Y_UNIT_TEST(ValidateConfigurationWithoutWarehouse) {
        NYql::TIssues issues;
        auto cluster = TClusterConfigBuilder()
            .FillAuthToken()
            .FillHiveMetastoreCatalog()
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
        // initially fill all params, in test cases
        // remove some params and expect error
        NFq::TClusterConfigBuilder params = {
            .Token            = "token",
            .S3Path           = "path",
            .S3Bucket         = "bucket",
            .HiveMetastoreUri = "hive_metastore_uri",
            .HiveMetastoreDb  = "hive_metastore db",
            .HadoopDir        = "hadoop_dir"
        };

        // Defines which errors to expect if param is not set
        std::vector<std::pair<TClusterConfigBuilder, std::vector<TString>>> cases = {
            // token is required expect error
            {params.clone().SetToken(""), {"warehouse.auth"}},
            // s3.path is non required
            {params.clone().SetS3Path(""), {}},
            // s3.bucket is required
            {params.clone().SetS3Bucket(""), {"s3.bucket"}},
            // s3.bucket is required, slashes has to be removed
            {params.clone().SetS3Bucket("///"), {"s3.bucket"}},
            // warehouse is required
            {params.clone().SetS3Bucket("").SetS3Path(""), {"warehouse"}},
            // hive_metastore.uri is required when hadoop is not set
            {params.clone().SetHadoopDir("").SetHiveMetastoreUri(""), {"hive_metastore.uri"}},
            // hive_metastore.db is required when hadoop is not set
            {params.clone().SetHadoopDir("").SetHiveMetastoreDb(""), {"hive_metastore.database_name"}},
            // catalog is required
            {params.clone().SetHadoopDir("").SetHiveMetastoreUri("").SetHiveMetastoreDb(""), {"catalog"}},
            // hadoop.dir is set, hive_metastore is empty, no errors
            {params.clone().SetHiveMetastoreUri("").SetHiveMetastoreDb(""), {}}
        };

        int count = 1;

        // process params and expect errors
        for (auto [params, waitErrors] : cases) {
            auto cluster = params.Build();
            NYql::TIssues issues;
            TIcebergProcessor processor(cluster, issues);

            processor.Process();
            auto r = GetErrorsFromIssues(issues);
            Cerr << "test case: " << count++ << "\n";

            UNIT_ASSERT_VALUES_EQUAL(r.size(), waitErrors.size());

            for (size_t i = 0; i < waitErrors.size(); ++i) {
                UNIT_ASSERT_STRING_CONTAINS(r[i], waitErrors[i]);
            }
        }
    }
}


} // NFq

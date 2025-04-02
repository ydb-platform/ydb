#include "external_source_builder.h"
#include "iceberg_fields.h"

#include <library/cpp/testing/unittest/registar.h>
#include <util/random/random.h>
#include <ydb/core/external_sources/external_source_factory.h>
#include <ydb/core/protos/external_sources.pb.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

namespace NKikimr {

namespace {

NExternalSource::IExternalSource::TPtr CreateTestSource(const std::vector<TString> hostnamePatterns = {}) {
    auto s = ToString(NYql::EDatabaseType::Iceberg);
    auto f = NExternalSource::CreateExternalSourceFactory(
        hostnamePatterns,nullptr,50000, nullptr, false, false, {s});

    return f->GetOrCreate(s);
}

}

Y_UNIT_TEST_SUITE(IcebergDdlTest) {

    // Test ddl for an iceberg table in s3 storage with the hive catalog 
    Y_UNIT_TEST(HiveCatalogWithS3Test) {
        auto source = CreateTestSource();

        NKikimrSchemeOp::TExternalDataSourceDescription proto;
        auto props = proto.MutableProperties()->MutableProperties();

        props->emplace(Iceberg::Warehouse::Fields::TYPE, Iceberg::Warehouse::S3);
        props->emplace(Iceberg::Warehouse::Fields::DB, "db");
        props->emplace(Iceberg::Warehouse::Fields::S3_REGION, "region");
        props->emplace(Iceberg::Warehouse::Fields::S3_ENDPOINT, "endpoint");
        props->emplace(Iceberg::Warehouse::Fields::S3_URI, "uri");
        props->emplace(Iceberg::Catalog::Fields::TYPE, Iceberg::Catalog::HIVE);
        props->emplace(Iceberg::Catalog::Fields::HIVE_URI, "hive_uri");
            
        UNIT_ASSERT_NO_EXCEPTION(source->ValidateExternalDataSource(proto.SerializeAsString()));

        auto authMethods = source->GetAuthMethods(proto.SerializeAsString());

        UNIT_ASSERT_VALUES_EQUAL(authMethods.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(authMethods[0], "BASIC");
        UNIT_ASSERT_VALUES_EQUAL(authMethods[1], "TOKEN");
        UNIT_ASSERT_VALUES_EQUAL(authMethods[2], "SERVICE_ACCOUNT");
    }

    // Test ddl for an iceberg table in s3 storage with the hadoop catalog 
    Y_UNIT_TEST(HadoopCatalogWithS3Test) {
        auto source = CreateTestSource();

        NKikimrSchemeOp::TExternalDataSourceDescription proto;
        auto props = proto.MutableProperties()->MutableProperties();

        props->emplace(Iceberg::Warehouse::Fields::TYPE, Iceberg::Warehouse::S3);
        props->emplace(Iceberg::Warehouse::Fields::DB, "db");
        props->emplace(Iceberg::Warehouse::Fields::S3_REGION, "region");
        props->emplace(Iceberg::Warehouse::Fields::S3_ENDPOINT, "endpoint");
        props->emplace(Iceberg::Warehouse::Fields::S3_URI, "uri");
        props->emplace(Iceberg::Catalog::Fields::TYPE, Iceberg::Catalog::HADOOP);
            
        UNIT_ASSERT_NO_EXCEPTION(source->ValidateExternalDataSource(proto.SerializeAsString()));

        auto authMethods = source->GetAuthMethods(proto.SerializeAsString());

        UNIT_ASSERT_VALUES_EQUAL(authMethods.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(authMethods[0], "BASIC");
        UNIT_ASSERT_VALUES_EQUAL(authMethods[1], "TOKEN");
        UNIT_ASSERT_VALUES_EQUAL(authMethods[2], "SERVICE_ACCOUNT");

    }
 
}

} // NKikimr

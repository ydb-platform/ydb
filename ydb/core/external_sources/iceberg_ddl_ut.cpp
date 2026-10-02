#include "external_source_builder.h"
#include "iceberg_fields.h"

#include <library/cpp/testing/unittest/registar.h>
#include <util/random/random.h>
#include <ydb/core/external_sources/external_source_factory.h>
#include <ydb/core/protos/external_sources.pb.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

namespace NKikimr {

namespace {

class TTestFixture : public NUnitTest::TBaseFixture {
public: 
    TTestFixture()
        : Props(*Proto.MutableProperties()->MutableProperties())
    {
        using namespace NKikimr::NExternalSource::NIceberg;

        auto type = ToString(NYql::EDatabaseType::Iceberg);
        auto factory = NExternalSource::CreateExternalSourceFactory(
            {}, nullptr, 50000, nullptr, false, false, false, {type});

        Source = factory->GetOrCreate(type);

        Props[WAREHOUSE_TYPE]         = VALUE_S3;
        Props[WAREHOUSE_DB]           = "db";
        Props[WAREHOUSE_S3_REGION]    = "region";
        Props[WAREHOUSE_S3_ENDPOINT]  = "endpoint";
        Props[WAREHOUSE_S3_URI]       = "uri";
    }

public:
    void SetUp(NUnitTest::TTestContext& context) override {
        NUnitTest::TBaseFixture::SetUp(context);
    }

protected:
    NExternalSource::IExternalSource::TPtr Source;
    NKikimrSchemeOp::TExternalDataSourceDescription Proto;
    ::google::protobuf::Map<TProtoStringType, TProtoStringType>& Props;
};

} // unnamed

Y_UNIT_TEST_SUITE(IcebergDdlTest) {

    // Test ddl for an iceberg table in s3 storage with the hive catalog 
    Y_UNIT_TEST_F(HiveCatalogWithS3Test, TTestFixture) {
        using namespace NKikimr::NExternalSource::NIceberg;

        Props[CATALOG_TYPE]               = VALUE_HIVE_METASTORE;
        Props[CATALOG_HIVE_METASTORE_URI] = "hive_metastore_uri";
            
        UNIT_ASSERT_NO_EXCEPTION(Source->ValidateExternalDataSource(Proto.SerializeAsString()));

        auto authMethods = Source->GetAuthMethods();

        UNIT_ASSERT_VALUES_EQUAL(authMethods.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(authMethods[0], "BASIC");
        UNIT_ASSERT_VALUES_EQUAL(authMethods[1], "TOKEN");
        UNIT_ASSERT_VALUES_EQUAL(authMethods[2], "SERVICE_ACCOUNT");
    }

    // Test ddl for an iceberg table in s3 storage with the hadoop catalog 
    Y_UNIT_TEST_F(HadoopCatalogWithS3Test, TTestFixture) {
        using namespace NKikimr::NExternalSource::NIceberg;

        Props[CATALOG_TYPE] = VALUE_HADOOP;
            
        UNIT_ASSERT_NO_EXCEPTION(Source->ValidateExternalDataSource(Proto.SerializeAsString()));

        auto authMethods = Source->GetAuthMethods();

        UNIT_ASSERT_VALUES_EQUAL(authMethods.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(authMethods[0], "BASIC");
        UNIT_ASSERT_VALUES_EQUAL(authMethods[1], "TOKEN");
        UNIT_ASSERT_VALUES_EQUAL(authMethods[2], "SERVICE_ACCOUNT");
    }
 
}

} // NKikimr

#include "external_data_source.h"

#include <library/cpp/testing/unittest/registar.h>
#include <util/random/random.h>
#include <ydb/core/protos/external_sources.pb.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

namespace NKikimr {

namespace {

NExternalSource::IExternalSource::TPtr CreateTestSource(std::vector<TRegExMatch> hostnamePatterns = {}) {
    return NExternalSource::CreateExternalDataSource("Test", {"auth1", "auth2"}, {"property"}, std::move(hostnamePatterns));
}

}

Y_UNIT_TEST_SUITE(ExternalDataSourceTest) {
    Y_UNIT_TEST(ValidateName) {
        auto source = CreateTestSource();
        UNIT_ASSERT_VALUES_EQUAL(source->GetName(), "Test");
    }

    Y_UNIT_TEST(ValidatePack) {
        auto source = CreateTestSource();
        NKikimrExternalSources::TSchema schema;
        NKikimrExternalSources::TGeneral general;
        UNIT_ASSERT_EXCEPTION_CONTAINS(source->Pack(schema, general), NExternalSource::TExternalSourceException, "Only external table supports pack operation");
    }

    Y_UNIT_TEST(ValidateAuth) {
        auto source = CreateTestSource();
        const auto authMethods = source->GetAuthMethods();
        UNIT_ASSERT_VALUES_EQUAL(authMethods.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(authMethods[0], "auth1");
        UNIT_ASSERT_VALUES_EQUAL(authMethods[1], "auth2");
    }

    Y_UNIT_TEST(ValidateParameters) {
        auto source = CreateTestSource();
        UNIT_ASSERT_EXCEPTION_CONTAINS(source->GetParameters({}), NExternalSource::TExternalSourceException, "Only external table supports parameters");
    }

    Y_UNIT_TEST(ValidateHasExternalTable) {
        auto source = CreateTestSource();
        UNIT_ASSERT_VALUES_EQUAL(source->HasExternalTable(), false);
    }

    Y_UNIT_TEST(ValidateProperties) {
        NKikimrSchemeOp::TExternalDataSourceDescription proto;
        auto source = CreateTestSource();

        proto.MutableProperties()->MutableProperties()->insert({"property", "value"});
        UNIT_ASSERT_NO_EXCEPTION(source->ValidateExternalDataSource(proto.SerializeAsString()));

        proto.MutableProperties()->MutableProperties()->insert({"another_property", "value"});
        UNIT_ASSERT_EXCEPTION_CONTAINS(source->ValidateExternalDataSource(proto.SerializeAsString()), NExternalSource::TExternalSourceException, "Unsupported property: another_property");
    }

    Y_UNIT_TEST(ValidateLocation) {
        NKikimrSchemeOp::TExternalDataSourceDescription proto;
        auto source = CreateTestSource();

        proto.SetLocation("https://any_location:1234/first/uri");
        UNIT_ASSERT_NO_EXCEPTION(source->ValidateExternalDataSource(proto.SerializeAsString()));

        source = CreateTestSource({ "(test|regex)", "localhost" });

        proto.SetLocation("https://test:1234/first/uri");
        UNIT_ASSERT_NO_EXCEPTION(source->ValidateExternalDataSource(proto.SerializeAsString()));

        proto.SetLocation("regex:1234");
        UNIT_ASSERT_NO_EXCEPTION(source->ValidateExternalDataSource(proto.SerializeAsString()));

        proto.SetLocation("https://localhost/second/uri");
        UNIT_ASSERT_NO_EXCEPTION(source->ValidateExternalDataSource(proto.SerializeAsString()));

        proto.SetLocation("localhost/second/uri");
        UNIT_ASSERT_NO_EXCEPTION(source->ValidateExternalDataSource(proto.SerializeAsString()));

        proto.SetLocation("scheme://anotherhost:1234/uri");
        UNIT_ASSERT_EXCEPTION_CONTAINS(source->ValidateExternalDataSource(proto.SerializeAsString()), NExternalSource::TExternalSourceException, "It is not allowed to access hostname 'anotherhost'");

        proto.SetLocation("anotherhost:1234/uri");
        UNIT_ASSERT_EXCEPTION_CONTAINS(source->ValidateExternalDataSource(proto.SerializeAsString()), NExternalSource::TExternalSourceException, "It is not allowed to access hostname 'anotherhost'");
    }
}

} // NKikimr

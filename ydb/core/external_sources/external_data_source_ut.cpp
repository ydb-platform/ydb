#include "external_data_source.h"
#include "external_service.h"
#include "external_source_factory.h"

#include <ydb/core/protos/external_sources.pb.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/random/random.h>

namespace NKikimr {

namespace {

NExternalSource::IExternalSource::TPtr CreateTestSource(std::vector<TRegExMatch> hostnamePatterns = {}) {
    return NExternalSource::CreateExternalDataSource("Test", { "auth1", "auth2" }, { "property" }, std::move(hostnamePatterns));
}

}   // namespace

Y_UNIT_TEST_SUITE(ExternalDataSourceTest) {
    Y_UNIT_TEST(ExternalServicePublicSettings) {
        const auto source = NExternalSource::CreateExternalSourceFactory({})->GetOrCreate(NYql::EDatabaseType::ExternalService);
        NKikimrSchemeOp::TExternalDataSourceDescription description;
        description.SetSourceType("ExternalService");
        description.SetLocation("https://example.invalid");
        description.MutableAuth()->MutableNone();
        auto& props = *description.MutableProperties()->MutableProperties();
        props["protocol"] = "HTTP";
        UNIT_ASSERT_NO_EXCEPTION(source->ValidateExternalDataSource(description.SerializeAsString()));
        auto settings = NExternalSource::ParseServiceConnection(description);
        UNIT_ASSERT(settings.UseTls);
        UNIT_ASSERT(settings.SecretReference.empty());
        for (const auto* endpoint : { "https://example.invalid/a", "https://example.invalid?token=x", "https://u:p@example.invalid",
                 "http://example.invalid", "https://example.invalid#x" }) {
            description.SetLocation(endpoint);
            UNIT_ASSERT_EXCEPTION(
                source->ValidateExternalDataSource(description.SerializeAsString()), NExternalSource::TExternalSourceException);
        }
        description.SetLocation("example.invalid:1234");
        props["protocol"] = "GRPC";
        props["use_tls"] = "false";
        UNIT_ASSERT_NO_EXCEPTION(source->ValidateExternalDataSource(description.SerializeAsString()));
        props["ca_certificate"] = "ca";
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            source->ValidateExternalDataSource(description.SerializeAsString()), NExternalSource::TExternalSourceException, "CA_CERTIFICATE");
        props.erase("ca_certificate");
        props["tvm_source_id"] = "123";
        UNIT_ASSERT_EXCEPTION(source->ValidateExternalDataSource(description.SerializeAsString()), NExternalSource::TExternalSourceException);
        props["tvm_destination_id"] = "456";
        UNIT_ASSERT_EXCEPTION(source->ValidateExternalDataSource(description.SerializeAsString()), NExternalSource::TExternalSourceException);
        description.MutableAuth()->MutableToken()->SetTokenSecretName("/Root/tvm_secret");
        settings = NExternalSource::ParseServiceConnection(description);
        const auto restored = NExternalSource::TServiceConnectionSettings::Deserialize(settings.Serialize());
        UNIT_ASSERT_VALUES_EQUAL(restored.SecretReference, "/Root/tvm_secret");
        UNIT_ASSERT_VALUES_EQUAL(restored.TvmSourceId, 123);
        UNIT_ASSERT_VALUES_EQUAL(restored.TvmDestinationId, 456);
        UNIT_ASSERT(!restored.UseTls);
        for (const auto* id : { "0", "-1", "4294967296", "x" }) {
            props["tvm_source_id"] = id;
            UNIT_ASSERT_EXCEPTION(
                source->ValidateExternalDataSource(description.SerializeAsString()), NExternalSource::TExternalSourceException);
        }
    }

    Y_UNIT_TEST(ExternalServiceAvailabilityAndHostname) {
        const auto disabled =
            NExternalSource::CreateExternalSourceFactory({}, nullptr, 0, {}, false, false, false, { NYql::EDatabaseType::ObjectStorage });
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            disabled->GetOrCreate(NYql::EDatabaseType::ExternalService), NExternalSource::TExternalSourceException, "disabled");
        const auto restricted =
            NExternalSource::CreateExternalSourceFactory({ "allowed.invalid" })->GetOrCreate(NYql::EDatabaseType::ExternalService);
        NKikimrSchemeOp::TExternalDataSourceDescription description;
        description.SetSourceType("ExternalService");
        description.SetLocation("https://denied.invalid");
        description.MutableAuth()->MutableNone();
        (*description.MutableProperties()->MutableProperties())["protocol"] = "HTTP";
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            restricted->ValidateExternalDataSource(description.SerializeAsString()), NExternalSource::TExternalSourceException, "not allowed");
    }

    Y_UNIT_TEST(ValidateName) {
        auto source = CreateTestSource();
        UNIT_ASSERT_VALUES_EQUAL(source->GetName(), "Test");
    }

    Y_UNIT_TEST(ValidatePack) {
        auto source = CreateTestSource();
        NKikimrExternalSources::TSchema schema;
        NKikimrExternalSources::TGeneral general;
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            source->Pack(schema, general), NExternalSource::TExternalSourceException, "Only external table supports pack operation");
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
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            source->GetParameters({}), NExternalSource::TExternalSourceException, "Only external table supports parameters");
    }

    Y_UNIT_TEST(ValidateHasExternalTable) {
        auto source = CreateTestSource();
        UNIT_ASSERT_VALUES_EQUAL(source->HasExternalTable(), false);
    }

    Y_UNIT_TEST(ValidateProperties) {
        NKikimrSchemeOp::TExternalDataSourceDescription proto;
        auto source = CreateTestSource();

        proto.MutableProperties()->MutableProperties()->insert({ "property", "value" });
        UNIT_ASSERT_NO_EXCEPTION(source->ValidateExternalDataSource(proto.SerializeAsString()));

        proto.MutableProperties()->MutableProperties()->insert({ "another_property", "value" });
        UNIT_ASSERT_EXCEPTION_CONTAINS(source->ValidateExternalDataSource(proto.SerializeAsString()), NExternalSource::TExternalSourceException,
            "Unsupported property: another_property");
    }

    Y_UNIT_TEST(ValidateYdbSourceRequiresDatabase) {
        NKikimrSchemeOp::TExternalDataSourceDescription proto;
        proto.SetSourceType("Ydb");

        auto source = NExternalSource::CreateExternalDataSource(
            "Ydb", { "NONE", "BASIC", "SERVICE_ACCOUNT", "TOKEN", "IAM" }, { "database_name", "use_tls", "database_id", "shared_reading" }, {});

        UNIT_ASSERT_EXCEPTION_CONTAINS(source->ValidateExternalDataSource(proto.SerializeAsString()), NExternalSource::TExternalSourceException,
            "Ydb source must provide a non-empty database_name or database_id");

        proto.MutableProperties()->MutableProperties()->insert({ "database_name", "" });
        UNIT_ASSERT_EXCEPTION_CONTAINS(source->ValidateExternalDataSource(proto.SerializeAsString()), NExternalSource::TExternalSourceException,
            "Ydb source must provide a non-empty database_name or database_id");

        proto.MutableProperties()->MutableProperties()->clear();
        proto.MutableProperties()->MutableProperties()->insert({ "database_id", "" });
        UNIT_ASSERT_EXCEPTION_CONTAINS(source->ValidateExternalDataSource(proto.SerializeAsString()), NExternalSource::TExternalSourceException,
            "Ydb source must provide a non-empty database_name or database_id");

        proto.MutableProperties()->MutableProperties()->clear();
        proto.MutableProperties()->MutableProperties()->insert({ "database_name", "/local" });
        UNIT_ASSERT_NO_EXCEPTION(source->ValidateExternalDataSource(proto.SerializeAsString()));

        proto.MutableProperties()->MutableProperties()->clear();
        proto.MutableProperties()->MutableProperties()->insert({ "database_id", "etn0123456789" });
        UNIT_ASSERT_NO_EXCEPTION(source->ValidateExternalDataSource(proto.SerializeAsString()));

        proto.MutableProperties()->MutableProperties()->insert({ "database_name", "/local" });
        UNIT_ASSERT_NO_EXCEPTION(source->ValidateExternalDataSource(proto.SerializeAsString()));
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
        UNIT_ASSERT_EXCEPTION_CONTAINS(source->ValidateExternalDataSource(proto.SerializeAsString()), NExternalSource::TExternalSourceException,
            "It is not allowed to access hostname 'anotherhost'");

        proto.SetLocation("anotherhost:1234/uri");
        UNIT_ASSERT_EXCEPTION_CONTAINS(source->ValidateExternalDataSource(proto.SerializeAsString()), NExternalSource::TExternalSourceException,
            "It is not allowed to access hostname 'anotherhost'");
    }
}

}   // namespace NKikimr

#include <ydb/library/yql/providers/ydb_external/common/provider_names.h>
#include "external_source_factory.h"

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>

namespace NKikimr::NExternalSource {
namespace {

NKikimrSchemeOp::TExternalDataSourceDescription MakeDescription(const TString& type = "YdbExternal") {
    NKikimrSchemeOp::TExternalDataSourceDescription description;
    description.SetSourceType(type);
    description.SetLocation("localhost:2135");
    description.MutableAuth()->MutableNone();
    (*description.MutableProperties()->MutableProperties())["database_name"] = "/Remote";
    return description;
}

} // namespace

Y_UNIT_TEST_SUITE(YdbExternalSourceFactory) {
    Y_UNIT_TEST(DefaultRoutesYdbTablesToQuerySdk) {
        const auto factory = CreateExternalSourceFactory({});
        UNIT_ASSERT_VALUES_EQUAL(factory->GetOrCreate(NYql::EDatabaseType::Ydb)->GetName(), NYql::YdbExternalProviderName);
        UNIT_ASSERT_VALUES_EQUAL(factory->GetOrCreate(NYql::EDatabaseType::YdbExternal)->GetName(), NYql::YdbExternalProviderName);
        UNIT_ASSERT_VALUES_EQUAL(factory->GetOrCreate(NYql::EDatabaseType::PostgreSQL)->GetName(), NYql::GenericProviderName);
        UNIT_ASSERT_VALUES_EQUAL(factory->GetOrCreate(NYql::EDatabaseType::YT)->GetName(), NYql::YtProviderName);
        UNIT_ASSERT(factory->IsAvailableProvider(TString(NYql::YdbExternalProviderName)));
        UNIT_ASSERT(NYql::GetAllExternalDataSourceTypes().contains("YdbExternal"));
        UNIT_ASSERT(NYql::GetAllExternalDataSourceDatabaseTypes().contains(NYql::EDatabaseType::YdbExternal));
        UNIT_ASSERT(NYql::DatabaseTypeFromString("YdbExternal") == NYql::EDatabaseType::YdbExternal);
        UNIT_ASSERT(NYql::DatabaseTypeToDataSourceKind(NYql::EDatabaseType::Ydb) == NYql::EGenericDataSourceKind::YDB);
        UNIT_ASSERT_EXCEPTION_CONTAINS(NYql::DatabaseTypeToDataSourceKind(NYql::EDatabaseType::YdbExternal),
            yexception, "Unknown database type: YdbExternal");
    }

    Y_UNIT_TEST(AvailabilityIsIndependentForLegacyAndExternal) {
        for (const bool legacy : {false, true}) {
            for (const bool external : {false, true}) {
                std::set<NYql::EDatabaseType> available;
                if (legacy) {
                    available.insert(NYql::EDatabaseType::Ydb);
                }
                if (external) {
                    available.insert(NYql::EDatabaseType::YdbExternal);
                }
                const auto factory = CreateExternalSourceFactory({}, nullptr, 50000, nullptr,
                    false, false, false, available);
                UNIT_ASSERT(!factory->IsAvailableProvider(TString(NYql::GenericProviderName)));
                UNIT_ASSERT_VALUES_EQUAL(factory->IsAvailableProvider(TString(NYql::YdbExternalProviderName)), legacy || external);
                for (const auto type : {NYql::EDatabaseType::Ydb, NYql::EDatabaseType::YdbExternal}) {
                    if (available.contains(type)) {
                        UNIT_ASSERT_NO_EXCEPTION(factory->GetOrCreate(type));
                    } else {
                        UNIT_ASSERT_EXCEPTION_CONTAINS(factory->GetOrCreate(type), TExternalSourceException, "is disabled");
                    }
                }
            }
        }
    }

    Y_UNIT_TEST(LegacyContractIsPreserved) {
        const auto source = CreateExternalSourceFactory({})->GetOrCreate(NYql::EDatabaseType::Ydb);
        UNIT_ASSERT((source->GetAuthMethods() == TVector<TString>{"NONE", "BASIC", "SERVICE_ACCOUNT", "TOKEN", "IAM"}));
        auto description = MakeDescription("Ydb");
        auto& properties = *description.MutableProperties()->MutableProperties();
        properties.erase("database_name");
        properties["database_id"] = "managed-database-id";
        properties["shared_reading"] = "true";
        properties["shared_reading_group"] = "topic-group";
        UNIT_ASSERT_NO_EXCEPTION(source->ValidateExternalDataSource(description.SerializeAsString()));
    }

    Y_UNIT_TEST(ExternalContractAcceptsExplicitConnection) {
        const auto source = CreateExternalSourceFactory({})->GetOrCreate(NYql::EDatabaseType::YdbExternal);
        UNIT_ASSERT((source->GetAuthMethods() == TVector<TString>{"NONE", "TOKEN"}));
        auto description = MakeDescription();
        UNIT_ASSERT_NO_EXCEPTION(source->ValidateExternalDataSource(description.SerializeAsString()));
        auto& properties = *description.MutableProperties()->MutableProperties();
        properties["database_name"] = "/Remote/";
        for (const auto* tls : {"true", "false", "TRUE", "False"}) {
            properties["use_tls"] = tls;
            UNIT_ASSERT_NO_EXCEPTION(source->ValidateExternalDataSource(description.SerializeAsString()));
        }
    }

    Y_UNIT_TEST(ExternalRejectsManagedAndTopicProperties) {
        const auto source = CreateExternalSourceFactory({})->GetOrCreate(NYql::EDatabaseType::YdbExternal);
        for (const auto* property : {"database_id", "mdb_cluster_id", "shared_reading", "shared_reading_group"}) {
            auto description = MakeDescription();
            (*description.MutableProperties()->MutableProperties())[property] = "";
            UNIT_ASSERT_EXCEPTION_CONTAINS(source->ValidateExternalDataSource(description.SerializeAsString()),
                TExternalSourceException, TStringBuilder() << "Unsupported property: " << property);
        }
    }

    Y_UNIT_TEST(ExternalValidatesDatabaseEndpointTlsAndHostname) {
        const auto source = CreateExternalSourceFactory({})->GetOrCreate(NYql::EDatabaseType::YdbExternal);
        for (const auto* database : {"", "Remote", "/Remote/../Other", "/Remote/.", "/Remote//Other", "/Remote\n"}) {
            auto description = MakeDescription();
            (*description.MutableProperties()->MutableProperties())["database_name"] = database;
            UNIT_ASSERT_EXCEPTION_CONTAINS(source->ValidateExternalDataSource(description.SerializeAsString()),
                TExternalSourceException, "YdbExternal requires an absolute DATABASE_NAME");
        }
        auto missingDatabase = MakeDescription();
        missingDatabase.MutableProperties()->MutableProperties()->erase("database_name");
        UNIT_ASSERT_EXCEPTION_CONTAINS(source->ValidateExternalDataSource(missingDatabase.SerializeAsString()),
            TExternalSourceException, "YdbExternal requires an absolute DATABASE_NAME");
        for (const auto* endpoint : {"", "localhost", "localhost:0", "localhost:65536", "grpc://localhost:2135", "user@localhost:2135", "localhost:2135/path", "localhost :2135"}) {
            auto description = MakeDescription();
            description.SetLocation(endpoint);
            UNIT_ASSERT_EXCEPTION_CONTAINS(source->ValidateExternalDataSource(description.SerializeAsString()),
                TExternalSourceException, "YdbExternal requires LOCATION in host:port format");
        }
        auto invalidTls = MakeDescription();
        (*invalidTls.MutableProperties()->MutableProperties())["use_tls"] = "yes";
        UNIT_ASSERT_EXCEPTION_CONTAINS(source->ValidateExternalDataSource(invalidTls.SerializeAsString()),
            TExternalSourceException, "YdbExternal USE_TLS must be true or false");
        const auto restricted = CreateExternalSourceFactory({"allowed-host.invalid"})->GetOrCreate(NYql::EDatabaseType::YdbExternal);
        UNIT_ASSERT_EXCEPTION_CONTAINS(restricted->ValidateExternalDataSource(MakeDescription().SerializeAsString()),
            TExternalSourceException, "host");
    }

    Y_UNIT_TEST(ReadTimeoutPropertyIsRejectedForBothSourceTypes) {
        const auto factory = CreateExternalSourceFactory({});
        for (const auto type : {NYql::EDatabaseType::Ydb, NYql::EDatabaseType::YdbExternal}) {
            const auto source = factory->GetOrCreate(type);
            auto description = MakeDescription(ToString(type));
            UNIT_ASSERT_NO_EXCEPTION(source->ValidateExternalDataSource(description.SerializeAsString()));
            for (const auto* value : {"1", "60000", "120000", "3600000", "", "0", "-1", "3600001", "4294967296", "60s", "1.5", "invalid"}) {
                (*description.MutableProperties()->MutableProperties())["read_timeout_ms"] = value;
                UNIT_ASSERT_EXCEPTION_CONTAINS(source->ValidateExternalDataSource(description.SerializeAsString()),
                    TExternalSourceException, "Unsupported property: read_timeout_ms");
            }
        }
    }
}

} // namespace NKikimr::NExternalSource

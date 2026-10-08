#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include "external_source_factory.h"

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

namespace NKikimr::NExternalSource {
namespace {

NKikimrSchemeOp::TExternalDataSourceDescription MakeDescription() {
    NKikimrSchemeOp::TExternalDataSourceDescription description;
    description.SetSourceType("Ydb");
    description.SetLocation("localhost:2135");
    description.MutableAuth()->MutableNone();
    (*description.MutableProperties()->MutableProperties())["database_name"] = "/Remote";
    return description;
}

} // namespace

Y_UNIT_TEST_SUITE(YdbSourceFactory) {
    Y_UNIT_TEST(DefaultRoutesYdbTablesToQuerySdk) {
        const auto factory = CreateExternalSourceFactory({});
        UNIT_ASSERT_VALUES_EQUAL(factory->GetOrCreate(NYql::EDatabaseType::Ydb)->GetName(), "ydb");
        UNIT_ASSERT_VALUES_EQUAL(factory->GetOrCreate(NYql::EDatabaseType::PostgreSQL)->GetName(), NYql::GenericProviderName);
        UNIT_ASSERT_VALUES_EQUAL(factory->GetOrCreate(NYql::EDatabaseType::YT)->GetName(), NYql::YtProviderName);
        UNIT_ASSERT(factory->IsAvailableProvider(TString(NYql::YdbProviderName)));
        UNIT_ASSERT(NYql::GetAllExternalDataSourceTypes().contains("Ydb"));
        UNIT_ASSERT(NYql::GetAllExternalDataSourceDatabaseTypes().contains(NYql::EDatabaseType::Ydb));
        UNIT_ASSERT(NYql::DatabaseTypeFromString("Ydb") == NYql::EDatabaseType::Ydb);
        UNIT_ASSERT(!NYql::DatabaseTypeFromString("YdbExternal"));
        UNIT_ASSERT(!NYql::GetAllExternalDataSourceTypes().contains("YdbExternal"));
        UNIT_ASSERT(NYql::DatabaseTypeToDataSourceKind(NYql::EDatabaseType::Ydb) == NYql::EGenericDataSourceKind::YDB);
    }

    Y_UNIT_TEST(AvailabilityIsIndependentOfGeneric) {
        for (const bool ydb : {false, true}) {
            for (const bool generic : {false, true}) {
                std::set<NYql::EDatabaseType> available;
                if (ydb) {
                    available.insert(NYql::EDatabaseType::Ydb);
                }
                if (generic) {
                    available.insert(NYql::EDatabaseType::PostgreSQL);
                }
                const auto factory = CreateExternalSourceFactory({}, nullptr, 50000, nullptr,
                    false, false, false, available);
                UNIT_ASSERT_VALUES_EQUAL(factory->IsAvailableProvider(TString(NYql::GenericProviderName)), generic);
                UNIT_ASSERT_VALUES_EQUAL(factory->IsAvailableProvider(TString(NYql::YdbProviderName)), ydb);
                for (const auto type : {NYql::EDatabaseType::Ydb, NYql::EDatabaseType::PostgreSQL}) {
                    if (available.contains(type)) {
                        UNIT_ASSERT_NO_EXCEPTION(factory->GetOrCreate(type));
                    } else {
                        UNIT_ASSERT_EXCEPTION_CONTAINS(factory->GetOrCreate(type), TExternalSourceException, "is disabled");
                    }
                }
            }
        }
    }

    Y_UNIT_TEST(TopicDdlContractIsPreserved) {
        const auto source = CreateExternalSourceFactory({})->GetOrCreate(NYql::EDatabaseType::Ydb);
        UNIT_ASSERT((source->GetAuthMethods() == TVector<TString>{"NONE", "BASIC", "SERVICE_ACCOUNT", "TOKEN", "IAM"}));
        auto description = MakeDescription();
        auto& properties = *description.MutableProperties()->MutableProperties();
        properties.erase("database_name");
        properties["database_id"] = "managed-database-id";
        properties["shared_reading"] = "true";
        properties["shared_reading_group"] = "topic-group";
        UNIT_ASSERT_NO_EXCEPTION(source->ValidateExternalDataSource(description.SerializeAsString()));
    }

    Y_UNIT_TEST(ReadTimeoutPropertyIsRejected) {
        const auto source = CreateExternalSourceFactory({})->GetOrCreate(NYql::EDatabaseType::Ydb);
        auto description = MakeDescription();
        UNIT_ASSERT_NO_EXCEPTION(source->ValidateExternalDataSource(description.SerializeAsString()));
        for (const auto* value : {"1", "60000", "120000", "3600000", "", "0", "-1", "3600001", "4294967296", "60s", "1.5", "invalid"}) {
            (*description.MutableProperties()->MutableProperties())["read_timeout_ms"] = value;
            UNIT_ASSERT_EXCEPTION_CONTAINS(source->ValidateExternalDataSource(description.SerializeAsString()),
                TExternalSourceException, "Unsupported property: read_timeout_ms");
        }
    }
}

} // namespace NKikimr::NExternalSource

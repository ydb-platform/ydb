#include <ydb/library/yql/providers/ydb_remote/common/provider_names.h>
#include "external_source_factory.h"

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>

namespace NKikimr::NExternalSource {

Y_UNIT_TEST_SUITE(NativeYdbExternalSourceFactory) {
    Y_UNIT_TEST(DefaultUsesGenericProvider) {
        const auto factory = CreateExternalSourceFactory({});
        UNIT_ASSERT_VALUES_EQUAL(factory->GetOrCreate("Ydb")->GetName(), NYql::GenericProviderName);
    }

    Y_UNIT_TEST(NativeRoutingPreservesOtherProviders) {
        const auto factory = CreateExternalSourceFactory({}, nullptr, 50000, nullptr,
            false, false, true, {}, true);
        UNIT_ASSERT_VALUES_EQUAL(factory->GetOrCreate("Ydb")->GetName(), NYql::YdbRemoteProviderName);
        UNIT_ASSERT_VALUES_EQUAL(factory->GetOrCreate("PostgreSQL")->GetName(), NYql::GenericProviderName);
        UNIT_ASSERT_VALUES_EQUAL(factory->GetOrCreate("YT")->GetName(), NYql::YtProviderName);
    }

    Y_UNIT_TEST(AvailabilityFollowsSelectedProvider) {
        const auto native = CreateExternalSourceFactory({}, nullptr, 50000, nullptr,
            false, false, false, {"Ydb"}, true);
        UNIT_ASSERT(native->IsAvailableProvider(TString(NYql::YdbRemoteProviderName)));
        UNIT_ASSERT(!native->IsAvailableProvider(TString(NYql::GenericProviderName)));

        const auto legacy = CreateExternalSourceFactory({}, nullptr, 50000, nullptr,
            false, false, false, {"Ydb"}, false);
        UNIT_ASSERT(!legacy->IsAvailableProvider(TString(NYql::YdbRemoteProviderName)));
        UNIT_ASSERT(legacy->IsAvailableProvider(TString(NYql::GenericProviderName)));
    }

    Y_UNIT_TEST(NativeRoutingDoesNotEnableDisabledSource) {
        const auto factory = CreateExternalSourceFactory({}, nullptr, 50000, nullptr,
            false, false, false, {"PostgreSQL"}, true);
        UNIT_ASSERT(!factory->IsAvailableProvider(TString(NYql::YdbRemoteProviderName)));
        UNIT_ASSERT_EXCEPTION_CONTAINS(factory->GetOrCreate("Ydb"), TExternalSourceException, "is disabled");
    }

    Y_UNIT_TEST(ReadTimeoutPropertyIsRejectedIndependentlyOfRouting) {
        // SchemeShard also validates EDS properties through the default factory.
        for (const bool native : {false, true}) {
            const auto factory = CreateExternalSourceFactory({}, nullptr, 50000, nullptr,
                false, false, true, {}, native);
            const auto source = factory->GetOrCreate("Ydb");
            NKikimrSchemeOp::TExternalDataSourceDescription description;
            description.SetSourceType("Ydb");
            description.SetLocation("localhost:2135");
            auto& properties = *description.MutableProperties()->MutableProperties();
            properties["database_name"] = "/Remote";
            UNIT_ASSERT_NO_EXCEPTION(source->ValidateExternalDataSource(description.SerializeAsString()));
            for (const auto* value : {"1", "60000", "120000", "3600000", "", "0", "-1", "3600001", "4294967296", "60s", "1.5", "invalid"}) {
                properties["read_timeout_ms"] = value;
                UNIT_ASSERT_EXCEPTION_CONTAINS(source->ValidateExternalDataSource(description.SerializeAsString()),
                    TExternalSourceException, "Unsupported property: read_timeout_ms");
            }
            UNIT_ASSERT_EXCEPTION_CONTAINS(factory->GetOrCreate("PostgreSQL")->ValidateExternalDataSource(description.SerializeAsString()),
                TExternalSourceException, "Unsupported property: read_timeout_ms");
        }
    }
}

} // namespace NKikimr::NExternalSource

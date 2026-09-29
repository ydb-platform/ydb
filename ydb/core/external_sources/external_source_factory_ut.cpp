#include "external_source_factory.h"

#include <library/cpp/testing/unittest/registar.h>
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
}

} // namespace NKikimr::NExternalSource

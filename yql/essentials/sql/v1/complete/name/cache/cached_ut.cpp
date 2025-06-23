#include "cached.h"

#include <yql/essentials/sql/v1/complete/name/cache/local/cache.h>

#include <library/cpp/testing/unittest/registar.h>

#include <library/cpp/time_provider/monotonic_provider.h>

using namespace NSQLComplete;

Y_UNIT_TEST_SUITE(CachedQueryTests) {

    Y_UNIT_TEST(OnExpired_WhenApplied_ThenDefferedUpdateAndReturnOld) {
        size_t queried = 0;
        auto cache = MakeLocalCache<TString, TString>(
            NMonotonic::CreateDefaultMonotonicTimeProvider(), {.TTL = TDuration::Zero()});
        auto cached = TCachedQuery<TString, TString>(cache, [&](const TString& key) {
            queried += 1;
            return NThreading::MakeFuture<TString>(key);
        });
        cache->Update("1", "2");

        TString value = cached("1").GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL(value, "2");
        UNIT_ASSERT_VALUES_EQUAL(queried, 1);
        UNIT_ASSERT_VALUES_EQUAL(cached("1").GetValueSync(), "1");
    }

} // Y_UNIT_TEST_SUITE(CachedQueryTests)

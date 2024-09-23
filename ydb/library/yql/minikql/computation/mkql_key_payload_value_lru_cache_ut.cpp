#include "mkql_key_payload_value_lru_cache.h"
#include <ydb/library/yql/minikql/mkql_type_builder.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NMiniKQL {

Y_UNIT_TEST_SUITE(TUnboxedKeyValueLruCacheWithTtlTest) {

    Y_UNIT_TEST(Simple) {
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment typeEnv(alloc);
        TTypeBuilder typeBuilder(typeEnv);
        const auto t0 = std::chrono::steady_clock::now();
        const auto dt = std::chrono::seconds(1);
        TUnboxedKeyValueLruCacheWithTtl cache(10, typeBuilder.NewDataType(NUdf::EDataSlot::Int32));
        UNIT_ASSERT_VALUES_EQUAL(0, cache.Size());
        // Insert data
        cache.Update(NUdf::TUnboxedValuePod{10}, NUdf::TUnboxedValuePod{100}, t0 + 10 * dt);
        cache.Update(NUdf::TUnboxedValuePod{20}, NUdf::TUnboxedValuePod{200}, t0 + 20 * dt);
        cache.Update(NUdf::TUnboxedValuePod{30}, NUdf::TUnboxedValuePod{}, t0 + 30 * dt); //empty(absent) value
        UNIT_ASSERT_VALUES_EQUAL(3, cache.Size());
        // Get data
        for (size_t i = 0; i != 3; ++i){
            {
                auto v = cache.Get(NUdf::TUnboxedValuePod{10}, t0 + (i * 3 + 0) * dt );
                UNIT_ASSERT(v);
                UNIT_ASSERT_VALUES_EQUAL(100, v->Get<i32>());
            }
            {
                auto v = cache.Get(NUdf::TUnboxedValuePod{20}, t0 + (i * 3 + 1) * dt);
                UNIT_ASSERT(v);
                UNIT_ASSERT_VALUES_EQUAL(200, v->Get<i32>());
            }
            {
                auto v = cache.Get(NUdf::TUnboxedValuePod{30}, t0 + (i * 3 + 2) * dt);
                UNIT_ASSERT(v);
                UNIT_ASSERT(!v->HasValue());
            }
            UNIT_ASSERT_VALUES_EQUAL(3, cache.Size());
        }
        
        { // Get outdated
            auto v = cache.Get(NUdf::TUnboxedValuePod{10}, t0 + 10 * dt );
            UNIT_ASSERT(!v);
            UNIT_ASSERT_VALUES_EQUAL(2, cache.Size());
        }
        { //still exists
            auto v = cache.Get(NUdf::TUnboxedValuePod{20}, t0 + 10 * dt);
            UNIT_ASSERT(v);
            UNIT_ASSERT_VALUES_EQUAL(200, v->Get<i32>());
        }
        // Get all outdated
        UNIT_ASSERT(!cache.Get(NUdf::TUnboxedValuePod{20}, t0 + 30 * dt));
        UNIT_ASSERT_VALUES_EQUAL(1, cache.Size());

        UNIT_ASSERT(!cache.Get(NUdf::TUnboxedValuePod{30}, t0 + 30 * dt));
        UNIT_ASSERT_VALUES_EQUAL(0, cache.Size());
    }

    Y_UNIT_TEST(LruEviction) {
        //TBD
    }

    Y_UNIT_TEST(GarbageCollection) {
        //TBD
    }
}

} //namespace namespace NKikimr::NMiniKQL

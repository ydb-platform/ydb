#include <library/cpp/testing/unittest/registar.h>
#include "simple_cache.h"

namespace NKikimr {
Y_UNIT_TEST_SUITE(TSimpleCacheTest) {
    Y_UNIT_TEST(TestSimpleCache) {
        TSimpleCache<TString, TString> cache;

        cache.MaxSize = 3;
        TString* ptr = cache.FindPtr("1");
        UNIT_ASSERT(ptr == nullptr);
        cache.Update("1", "one");
        ptr = cache.FindPtr("1");
        UNIT_ASSERT(ptr != nullptr);
        UNIT_ASSERT(*ptr == "one");
        cache.Update("2", "two");
        cache.Update("3", "three");
        ptr = cache.FindPtr("1");
        UNIT_ASSERT(ptr != nullptr);
        UNIT_ASSERT(*ptr == "one");
        cache.Update("4", "four"); // we evicting oldest one - "two"
        ptr = cache.FindPtr("2");
        UNIT_ASSERT(ptr == nullptr);
    }

    struct TValue {
        TString Str;
        bool SafeToRelease = true;

        bool IsSafeToRelease() {
            return SafeToRelease;
        }
    };

    Y_UNIT_TEST(TestNotSoSimpleCache) {
        TNotSoSimpleCache<TString, TValue> cache;

        cache.MaxSize = 3;
        TValue* ptr = cache.FindPtr("1");
        UNIT_ASSERT(ptr == nullptr);
        cache.Update("1", {"one"});
        ptr = cache.FindPtr("1");
        UNIT_ASSERT(ptr != nullptr);
        UNIT_ASSERT(ptr->Str == "one");
        cache.Update("2", {"two", false});
        cache.Update("3", {"three"});
        ptr = cache.FindPtr("1");
        UNIT_ASSERT(ptr != nullptr);
        UNIT_ASSERT(ptr->Str == "one");
        cache.Update("4", {"four"}); // we evicting oldest one - "three" (but not "two" - it's not safe to release)
        ptr = cache.FindPtr("2");
        UNIT_ASSERT(ptr != nullptr);
        ptr = cache.FindPtr("3");
        UNIT_ASSERT(ptr == nullptr);
    }
}

} // NKikimr

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
} 
 
} // NKikimr 

#include <ydb/core/kqp/runtime/kqp_vector_level_cache.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NKqp {

Y_UNIT_TEST_SUITE(KqpVectorLevelCacheTests) {

    Y_UNIT_TEST(CacheBasicOperations) {
        TKqpVectorLevelCache cache(100, TDuration::Seconds(10));
        
        // Create test data
        TVector<ui64> clusterIds = {1, 2, 3};
        TVector<TString> clusterData = {"cluster1", "cluster2", "cluster3"};
        
        Ydb::Table::VectorIndexSettings indexSettings;
        indexSettings.set_distance(Ydb::Table::VectorIndexSettings::DISTANCE_COSINE);
        indexSettings.set_vector_type(Ydb::Table::VectorIndexSettings::VECTOR_TYPE_UINT8);
        indexSettings.set_vector_dimension(128);
        
        auto value = std::make_shared<TCachedClusterLevel>(
            clusterIds, clusterData, indexSettings);
        
        TLevelTableCacheKey key("test_table", 1, 0);
        
        // Test put and get
        cache.Put(key, value);
        auto retrieved = cache.Get(key);
        
        UNIT_ASSERT(retrieved != nullptr);
        UNIT_ASSERT_EQUAL(retrieved->ClusterIds.size(), 3);
        UNIT_ASSERT_EQUAL(retrieved->ClusterData.size(), 3);
        
        // Test cache miss
        TLevelTableCacheKey wrongKey("other_table", 1, 0);
        auto missed = cache.Get(wrongKey);
        UNIT_ASSERT(missed == nullptr);
    }

    Y_UNIT_TEST(CacheInvalidation) {
        TKqpVectorLevelCache cache(100, TDuration::Seconds(10));
        
        TVector<ui64> clusterIds = {1, 2};
        TVector<TString> clusterData = {"cluster1", "cluster2"};
        
        Ydb::Table::VectorIndexSettings indexSettings;
        auto value = std::make_shared<TCachedClusterLevel>(
            clusterIds, clusterData, indexSettings);
        
        TLevelTableCacheKey key1("test_table", 1, 0);
        TLevelTableCacheKey key2("test_table", 1, 1);
        TLevelTableCacheKey key3("other_table", 1, 0);
        
        // Put values for different keys
        cache.Put(key1, value);
        cache.Put(key2, value);
        cache.Put(key3, value);
        
        // Verify all are cached
        UNIT_ASSERT(cache.Get(key1) != nullptr);
        UNIT_ASSERT(cache.Get(key2) != nullptr);
        UNIT_ASSERT(cache.Get(key3) != nullptr);
        
        // Invalidate specific table
        cache.InvalidateTable("test_table");
        
        // Check results
        UNIT_ASSERT(cache.Get(key1) == nullptr);
        UNIT_ASSERT(cache.Get(key2) == nullptr);
        UNIT_ASSERT(cache.Get(key3) != nullptr); // Different table should remain
    }

    Y_UNIT_TEST(CacheKeyStringRepresentation) {
        TLevelTableCacheKey key1("test_table", 1, 0);
        TLevelTableCacheKey key2("test_table", 1, 1);
        TLevelTableCacheKey key3("test_table", 2, 0);
        
        UNIT_ASSERT_UNEQUAL(key1.ToString(), key2.ToString());
        UNIT_ASSERT_UNEQUAL(key1.ToString(), key3.ToString());
        UNIT_ASSERT_UNEQUAL(key2.ToString(), key3.ToString());
        
        // Verify string format
        UNIT_ASSERT_STRING_CONTAINS(key1.ToString(), "test_table");
        UNIT_ASSERT_STRING_CONTAINS(key1.ToString(), "1");
        UNIT_ASSERT_STRING_CONTAINS(key1.ToString(), "0");
    }

} // Y_UNIT_TEST_SUITE

} // namespace NKqp
} // namespace NKikimr
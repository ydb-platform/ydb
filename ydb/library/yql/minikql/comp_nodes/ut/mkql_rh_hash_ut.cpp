#include "../mkql_rh_hash.h"

#include <library/cpp/testing/unittest/registar.h>

#include <unordered_map>
#include <unordered_set>

namespace NKikimr {
namespace NMiniKQL {

Y_UNIT_TEST_SUITE(TMiniKQLRobinHoodHashTest) {
    Y_UNIT_TEST(Map) {
        TRobinHoodHashMap<i32> rh(sizeof(i64));
        std::unordered_map<i32, i64> h;
        for (ui64 i = 0; i < 10000; ++i) {
            auto k = i % 1000;
            auto [it, inserted] = h.emplace(k, 0);
            bool isNew;
            auto iter = rh.Insert(k, isNew);
            UNIT_ASSERT_VALUES_EQUAL(rh.GetKey(iter), k);
            UNIT_ASSERT_VALUES_EQUAL(isNew, inserted);
            it->second += i;
            if (isNew) {
                *(i64*)rh.GetMutablePayload(iter) = i;
                rh.CheckGrow();
            } else {
                *(i64*)rh.GetMutablePayload(iter) += i;
            }

            UNIT_ASSERT_VALUES_EQUAL(h.size(), rh.GetSize());
        }

        for (auto it = rh.Begin(); it != rh.End(); rh.Advance(it)) {
            if (!rh.IsValid(it)) {
                continue;
            }

            auto key = rh.GetKey(it);
            auto hit = h.find(key);
            UNIT_ASSERT(hit != h.end());
            UNIT_ASSERT_VALUES_EQUAL(*(i64*)rh.GetPayload(it), hit->second);
            h.erase(key);
        }

        UNIT_ASSERT(h.empty());
    }

    Y_UNIT_TEST(FixedMap) {
        TRobinHoodHashFixedMap<i32, i64> rh;
        std::unordered_map<i32, i64> h;
        for (ui64 i = 0; i < 10000; ++i) {
            auto k = i % 1000;
            auto [it, inserted] = h.emplace(k, 0);
            bool isNew;
            auto iter = rh.Insert(k, isNew);
            UNIT_ASSERT_VALUES_EQUAL(rh.GetKey(iter), k);
            UNIT_ASSERT_VALUES_EQUAL(isNew, inserted);
            it->second += i;
            if (isNew) {
                *(i64*)rh.GetMutablePayload(iter) = i;
                rh.CheckGrow();
            } else {
                *(i64*)rh.GetMutablePayload(iter) += i;
            }

            UNIT_ASSERT_VALUES_EQUAL(h.size(), rh.GetSize());
        }

        for (auto it = rh.Begin(); it != rh.End(); rh.Advance(it)) {
            if (!rh.IsValid(it)) {
                continue;
            }

            auto key = rh.GetKey(it);
            auto hit = h.find(key);
            UNIT_ASSERT(hit != h.end());
            UNIT_ASSERT_VALUES_EQUAL(*(i64*)rh.GetPayload(it), hit->second);
            h.erase(key);
        }

        UNIT_ASSERT(h.empty());
    }


    Y_UNIT_TEST(Set) {
        TRobinHoodHashSet<i32> rh;
        std::unordered_set<i32> h;
        for (ui64 i = 0; i < 10000; ++i) {
            auto k = i % 1000;
            auto[it, inserted] = h.emplace(k);
            bool isNew;
            auto iter = rh.Insert(k, isNew);
            UNIT_ASSERT_VALUES_EQUAL(rh.GetKey(iter), k);
            UNIT_ASSERT_VALUES_EQUAL(isNew, inserted);
            if (isNew) {
                rh.CheckGrow();
            }

            UNIT_ASSERT_VALUES_EQUAL(h.size(), rh.GetSize());
        }

        for (auto it = rh.Begin(); it != rh.End(); rh.Advance(it)) {
            if (!rh.IsValid(it)) {
                continue;
            }

            auto key = rh.GetKey(it);
            auto hit = h.find(key);
            UNIT_ASSERT(hit != h.end());
            h.erase(key);
        }

        UNIT_ASSERT(h.empty());
    }

    Y_UNIT_TEST(MapBatch) {
        using THashTable = TRobinHoodHashMap<i32>;
        THashTable rh(sizeof(i64));
        std::unordered_map<i32, i64> h;
        std::array<TRobinHoodBatchRequestItem<i32>, PrefetchBatchSize> batch;
        std::array<bool, PrefetchBatchSize> batchInserted;
        std::array<ui64, PrefetchBatchSize> batchI;
        ui32 batchLen = 0;

        auto processBatch = [&]() {
            rh.BatchInsert({batch.data(), batchLen}, [&](size_t i, THashTable::iterator iter, bool isNew) {
                UNIT_ASSERT_VALUES_EQUAL(isNew, batchInserted[i]);
                UNIT_ASSERT_VALUES_EQUAL(rh.GetKey(iter), batch[i].GetKey());
                if (isNew) {
                    *(i64*)rh.GetMutablePayload(iter) = batchI[i];
                } else {
                    *(i64*)rh.GetMutablePayload(iter) += batchI[i];
                }
            });

            UNIT_ASSERT_VALUES_EQUAL(h.size(), rh.GetSize());
        };

        for (ui64 i = 0; i < 10000; ++i) {
            if (batchLen == batch.size()) {
                processBatch();
                batchLen = 0;
            }

            auto k = i % 1000;
            auto [it, inserted] = h.emplace(k, 0);
            batchI[batchLen] = i;
            batchInserted[batchLen] = inserted;
            batch[batchLen].ConstructKey(k);
            ++batchLen;
            it->second += i;
        }

        processBatch();
        for (auto it = rh.Begin(); it != rh.End(); rh.Advance(it)) {
            if (!rh.IsValid(it)) {
                continue;
            }

            auto key = rh.GetKey(it);
            auto hit = h.find(key);
            UNIT_ASSERT(hit != h.end());
            UNIT_ASSERT_VALUES_EQUAL(*(i64*)rh.GetPayload(it), hit->second);
            h.erase(key);
        }

        UNIT_ASSERT(h.empty());
    }

    Y_UNIT_TEST(FixedMapBatch) {
        using THashTable = TRobinHoodHashFixedMap<i32, i64>;
        THashTable rh(sizeof(i64));
        std::unordered_map<i32, i64> h;
        std::array<TRobinHoodBatchRequestItem<i32>, PrefetchBatchSize> batch;
        std::array<bool, PrefetchBatchSize> batchInserted;
        std::array<ui64, PrefetchBatchSize> batchI;
        ui32 batchLen = 0;

        auto processBatch = [&]() {
            rh.BatchInsert({batch.data(), batchLen}, [&](size_t i, THashTable::iterator iter, bool isNew) {
                UNIT_ASSERT_VALUES_EQUAL(isNew, batchInserted[i]);
                UNIT_ASSERT_VALUES_EQUAL(rh.GetKey(iter), batch[i].GetKey());
                if (isNew) {
                    *(i64*)rh.GetMutablePayload(iter) = batchI[i];
                } else {
                    *(i64*)rh.GetMutablePayload(iter) += batchI[i];
                }
            });

            UNIT_ASSERT_VALUES_EQUAL(h.size(), rh.GetSize());
        };

        for (ui64 i = 0; i < 10000; ++i) {
            if (batchLen == batch.size()) {
                processBatch();
                batchLen = 0;
            }

            auto k = i % 1000;
            auto [it, inserted] = h.emplace(k, 0);
            batchI[batchLen] = i;
            batchInserted[batchLen] = inserted;
            batch[batchLen].ConstructKey(k);
            ++batchLen;
            it->second += i;
        }

        processBatch();
        for (auto it = rh.Begin(); it != rh.End(); rh.Advance(it)) {
            if (!rh.IsValid(it)) {
                continue;
            }

            auto key = rh.GetKey(it);
            auto hit = h.find(key);
            UNIT_ASSERT(hit != h.end());
            UNIT_ASSERT_VALUES_EQUAL(*(i64*)rh.GetPayload(it), hit->second);
            h.erase(key);
        }

        UNIT_ASSERT(h.empty());
    }

    Y_UNIT_TEST(SetBatch) {
        using THashTable = TRobinHoodHashSet<i32>;
        THashTable rh(sizeof(i64));
        std::unordered_set<i32> h;
        std::array<TRobinHoodBatchRequestItem<i32>, PrefetchBatchSize> batch;
        std::array<bool, PrefetchBatchSize> batchInserted;
        ui32 batchLen = 0;

        auto processBatch = [&]() {
            rh.BatchInsert({batch.data(), batchLen}, [&](size_t i, THashTable::iterator iter, bool isNew) {
                UNIT_ASSERT_VALUES_EQUAL(isNew, batchInserted[i]);
                UNIT_ASSERT_VALUES_EQUAL(rh.GetKey(iter), batch[i].GetKey());
            });

            UNIT_ASSERT_VALUES_EQUAL(h.size(), rh.GetSize());
        };

        for (ui64 i = 0; i < 10000; ++i) {
            if (batchLen == batch.size()) {
                processBatch();
                batchLen = 0;
            }

            auto k = i % 1000;
            auto [it, inserted] = h.emplace(k);
            batchInserted[batchLen] = inserted;
            batch[batchLen].ConstructKey(k);
            ++batchLen;
        }

        processBatch();
        for (auto it = rh.Begin(); it != rh.End(); rh.Advance(it)) {
            if (!rh.IsValid(it)) {
                continue;
            }

            auto key = rh.GetKey(it);
            auto hit = h.find(key);
            UNIT_ASSERT(hit != h.end());
            h.erase(key);
        }

        UNIT_ASSERT(h.empty());
    }    
}

}
}

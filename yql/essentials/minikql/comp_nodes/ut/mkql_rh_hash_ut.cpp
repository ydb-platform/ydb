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

    Y_UNIT_TEST(Power2Collisions) {
        TRobinHoodHashSet<ui64> rh;
        for (ui64 i = 0; i < 10000; ++i) {
            auto k = i << 32;
            bool isNew;
            auto iter = rh.Insert(k, isNew);
            UNIT_ASSERT_VALUES_EQUAL(rh.GetKey(iter), k);
            if (isNew) {
                rh.CheckGrow();
            }

            UNIT_ASSERT_VALUES_EQUAL(i + 1, rh.GetSize());
        }

        i32 maxDistance = 0;
        for (auto it = rh.Begin(); it != rh.End(); rh.Advance(it)) {
            if (!rh.IsValid(it)) {
                continue;
            }

            auto distance = rh.GetPSL(it).Distance;
            maxDistance = Max(maxDistance, distance);
        }

        Cerr << "maxDistance: " << maxDistance << "\n";
        UNIT_ASSERT(maxDistance < 10);
    }

    Y_UNIT_TEST(RehashCollisions) {
        TRobinHoodHashSet<ui64> rh;
        TVector<ui64> values;
        const ui64 N = 1500000;
        values.reserve(N);
        for (ui64 i = 1; i <= N; ++i) {
            auto k = 64 * (i >> 4) + ((i & 8) ? 32 : 0) + (i & 7);
            values.push_back(k);
        }
        /*
        for (ui64 i = 0; i < 32; ++i) {
            Cerr << values[i] << "\n";
        }

        for (ui64 i = 0; i < 32; ++i) {
            Cerr << values[values.size() - 32 + i] << "\n";
        }*/

        for (ui64 i = 0; i < values.size(); ++i) {
            auto k = values[i];
            bool isNew;
            auto iter = rh.Insert(k, isNew);
            if (rh.GetKey(iter) != k) {
                UNIT_ASSERT_VALUES_EQUAL(rh.GetKey(iter), k);
            }

            if (isNew) {
                rh.CheckGrow();
            }

            if (i + 1 != rh.GetSize()) {
                UNIT_ASSERT_VALUES_EQUAL(i + 1, rh.GetSize());
            }
        }

        TRobinHoodHashSet<ui64> rh2;

        i32 maxDistance1 = 0;
        ui64 j = 0;
        for (auto it = rh.Begin(); it != rh.End(); rh.Advance(it)) {
            if (!rh.IsValid(it)) {
                continue;
            }

            auto distance = rh.GetPSL(it).Distance;
            maxDistance1 = Max(maxDistance1, distance);

            auto k = rh.GetKey(it);
            bool isNew;
            auto iter = rh2.Insert(k, isNew);
            if (rh2.GetKey(iter) != k) {
                UNIT_ASSERT_VALUES_EQUAL(rh2.GetKey(iter), k);
            }

            if (isNew) {
                rh2.CheckGrow();
            }

            if (j + 1 != rh2.GetSize()) {
                UNIT_ASSERT_VALUES_EQUAL(j + 1, rh2.GetSize());
            }

            ++j;
        }

        Cerr << "maxDistance1: " << maxDistance1 << "\n";
        UNIT_ASSERT(maxDistance1 < 20);

        i32 maxDistance2 = 0;
        for (auto it = rh2.Begin(); it != rh2.End(); rh2.Advance(it)) {
            if (!rh2.IsValid(it)) {
                continue;
            }

            auto distance = rh2.GetPSL(it).Distance;
            maxDistance2 = Max(maxDistance2, distance);
        }

        Cerr << "maxDistance2: " << maxDistance2 << "\n";
        UNIT_ASSERT(maxDistance2 < 20);
    }
}

}
}

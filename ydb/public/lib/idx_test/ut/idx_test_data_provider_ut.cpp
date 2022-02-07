#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <ydb/public/lib/idx_test/idx_test_data_provider.h>

using namespace NIdxTest;

Y_UNIT_TEST_SUITE(IdxTestDataProvider) {
    Y_UNIT_TEST(1ShardLimit6bitFromRandomUi8) {
        TRandomValueProvider provider(0, 6);
        ui64 val = Max<ui64>();
        while (val) {
            auto random = provider.RandomUi8();
            UNIT_ASSERT(random <= 63);
            val &= ~(1ull << random);
        }
    }

    Y_UNIT_TEST(1ShardLimit6bitFromRandomUi16) {
        TRandomValueProvider provider(0, 6);
        ui64 val = Max<ui64>();
        while (val) {
            auto random = provider.RandomUi16();
            UNIT_ASSERT(random <= 63);
            val &= ~(1ull << random);
        }
    }

    Y_UNIT_TEST(1ShardLimit6bitFromRandomUi32) {
        TRandomValueProvider provider(0, 6);
        ui64 val = Max<ui64>();
        while (val) {
            auto random = provider.RandomUi32();
            UNIT_ASSERT(random <= 63);
            val &= ~(1ull << random);
        }
    }

    Y_UNIT_TEST(1ShardLimit6bitFromRandomUi64) {
        TRandomValueProvider provider(0, 6);
        ui64 val = Max<ui64>();
        while (val) {
            auto random = provider.RandomUi64();
            UNIT_ASSERT(random <= 63);
            val &= ~(1ull << random);
        }
    }

    Y_UNIT_TEST(4ShardsLimit20bitFromRandomUi64) {
        TRandomValueProvider provider(2, 20);
        size_t count = 10000;
        const auto shardRange = Max<ui64>() / 4 + 1;
        const auto bound0 = 1ull << 20;
        const auto bound1 = bound0 + shardRange;
        const auto bound2 = bound1 + shardRange;
        const auto bound3 = bound2 + shardRange;
        while (count--) {
            auto rnd = provider.RandomUi64();
            UNIT_ASSERT( (rnd >= 0 * shardRange && rnd < bound0) ||
                         (rnd >= 1 * shardRange && rnd < bound1) ||
                         (rnd >= 2 * shardRange && rnd < bound2) ||
                         (rnd >= 3 * shardRange && rnd < bound3) );
        }
    }
}

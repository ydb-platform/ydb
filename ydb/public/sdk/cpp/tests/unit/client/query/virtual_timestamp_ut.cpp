#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/virtual_timestamp.h>

#include <library/cpp/testing/unittest/registar.h>

using NYdb::NScheme::TVirtualTimestamp;

Y_UNIT_TEST_SUITE(VirtualTimestamp) {
    Y_UNIT_TEST(LexicographicLess) {
        UNIT_ASSERT(TVirtualTimestamp(1, 100) < TVirtualTimestamp(2, 1));
        UNIT_ASSERT(TVirtualTimestamp(1, 50) < TVirtualTimestamp(1, 100));
        UNIT_ASSERT(!(TVirtualTimestamp(2, 1) < TVirtualTimestamp(1, 100)));
        UNIT_ASSERT(!(TVirtualTimestamp(1, 100) < TVirtualTimestamp(1, 100)));
    }

    Y_UNIT_TEST(LexicographicGreater) {
        UNIT_ASSERT(TVirtualTimestamp(2, 1) > TVirtualTimestamp(1, 100));
        UNIT_ASSERT(TVirtualTimestamp(1, 100) > TVirtualTimestamp(1, 50));
        UNIT_ASSERT(!(TVirtualTimestamp(1, 100) > TVirtualTimestamp(2, 1)));
        UNIT_ASSERT(!(TVirtualTimestamp(1, 100) > TVirtualTimestamp(1, 100)));
    }

    Y_UNIT_TEST(LessOrEqual) {
        UNIT_ASSERT(TVirtualTimestamp(1, 100) <= TVirtualTimestamp(1, 100));
        UNIT_ASSERT(TVirtualTimestamp(1, 50) <= TVirtualTimestamp(1, 100));
        UNIT_ASSERT(TVirtualTimestamp(1, 100) <= TVirtualTimestamp(2, 1));
        UNIT_ASSERT(!(TVirtualTimestamp(2, 1) <= TVirtualTimestamp(1, 100)));
    }

    Y_UNIT_TEST(GreaterOrEqual) {
        UNIT_ASSERT(TVirtualTimestamp(1, 100) >= TVirtualTimestamp(1, 100));
        UNIT_ASSERT(TVirtualTimestamp(1, 100) >= TVirtualTimestamp(1, 50));
        UNIT_ASSERT(TVirtualTimestamp(2, 1) >= TVirtualTimestamp(1, 100));
        UNIT_ASSERT(!(TVirtualTimestamp(1, 50) >= TVirtualTimestamp(1, 100)));
    }

    Y_UNIT_TEST(Equality) {
        UNIT_ASSERT(TVirtualTimestamp(1, 100) == TVirtualTimestamp(1, 100));
        UNIT_ASSERT(!(TVirtualTimestamp(1, 100) == TVirtualTimestamp(1, 101)));
        UNIT_ASSERT(!(TVirtualTimestamp(1, 100) == TVirtualTimestamp(2, 100)));
        UNIT_ASSERT(TVirtualTimestamp(1, 100) != TVirtualTimestamp(2, 100));
    }
}

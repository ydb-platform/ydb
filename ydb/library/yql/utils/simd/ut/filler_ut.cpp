#include <library/cpp/testing/unittest/registar.h>
#include "filler.h"


// Tests to check if ByteFiller works correctly
Y_UNIT_TEST_SUITE(CheckFiller) {
    Y_UNIT_TEST(CompareRowsTest) {
        i8 expected_result[30] = {
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
        };
        std::vector<size_t> sizes{1, 2, 3, 4};
        ByteFiller bf(sizes, 3);

        UNIT_ASSERT_EQUAL(bf.CompareRows(expected_result), true);
    }

    Y_UNIT_TEST(FillRowsTest) {
        i8 result[30]{};
        std::vector<size_t> sizes{1, 2, 3, 4};
        ByteFiller bf(sizes, 3);
        bf.FillRows(result);

        UNIT_ASSERT_EQUAL(bf.CompareRows(result), true);
    }

    Y_UNIT_TEST(FillColsTest) {
        std::vector<size_t> sizes{1, 2, 3, 4};
        ByteFiller bf(sizes, 3);

        i8 result[30]{};
        bf.FillRows(result);

        i8 d1[3]{};
        i8 d2[6]{};
        i8 d3[9]{};
        i8 d4[12]{};
        i8* data[4] = {
            d1, d2, d3, d4
        };

        bf.FillCols(data);

        UNIT_ASSERT_EQUAL(!memcmp(d1, (i8[3]){0, 0, 0}, 3), true);
        UNIT_ASSERT_EQUAL(!memcmp(d2, (i8[6]){1, 2, 1, 2, 1, 2}, 6), true);
        UNIT_ASSERT_EQUAL(!memcmp(d3, (i8[9]){3, 4, 5, 3, 4, 5, 3, 4, 5}, 9), true);
        UNIT_ASSERT_EQUAL(!memcmp(d4, (i8[12]){6, 7, 8, 9, 6, 7, 8, 9, 6, 7, 8, 9}, 12), true);
    }
}
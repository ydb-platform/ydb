#include "blob_int.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NPQ {

Y_UNIT_TEST_SUITE(BlobTest) {
    Y_UNIT_TEST(Flags_HasPartData) {
        TMessageFlags flags;

        flags.F.HasPartData = 1;
        UNIT_ASSERT_VALUES_EQUAL(flags.V, 1);
    }

    Y_UNIT_TEST(Flags_HasWriteTimestamp) {
        TMessageFlags flags;

        flags.F.HasWriteTimestamp = 1;
        UNIT_ASSERT_VALUES_EQUAL(flags.V, 2);
    }

    Y_UNIT_TEST(Flags_HasCreateTimestamp) {
        TMessageFlags flags;

        flags.F.HasCreateTimestamp = 1;
        UNIT_ASSERT_VALUES_EQUAL(flags.V, 4);
    }

    Y_UNIT_TEST(Flags_HasUncompressedSize) {
        TMessageFlags flags;

        flags.F.HasUncompressedSize = 1;
        UNIT_ASSERT_VALUES_EQUAL(flags.V, 8);
    }

    Y_UNIT_TEST(Flags_HasKinesisData) {
        TMessageFlags flags;

        flags.F.HasKinesisData = 1;
        UNIT_ASSERT_VALUES_EQUAL(flags.V, 16);
    }
}

}

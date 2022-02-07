#include "lz4_data_generator.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
Y_UNIT_TEST_SUITE(CompressionTest) {

size_t CompressedSize(TString data) {
    TString tmp;
    tmp.resize(2 * data.size());
    return LZ4_compress_default(data.Detach(), tmp.Detach(), data.size(), tmp.size());
}

    Y_UNIT_TEST(lz4_generator_basic) {
        for (ui32 size = 1; size < 200; ++size) {
            ui32 compressed = CompressedSize(GenDataForLZ4(size, size));
            UNIT_ASSERT(compressed);
        }
    }

    Y_UNIT_TEST(lz4_generator_deflates) {
        {
            ui32 size = 179;
            ui32 compressed = CompressedSize(GenDataForLZ4(size));
            UNIT_ASSERT_C(compressed * 10 < size,
                    size << " -> " << compressed);
        }
        {
            ui32 size = 382;
            ui32 compressed = CompressedSize(GenDataForLZ4(size));
            UNIT_ASSERT_C(compressed * 20 < size,
                    size << " -> " << compressed);
        }
        {
            ui32 size = 1752;
            ui32 compressed = CompressedSize(GenDataForLZ4(size));
            UNIT_ASSERT_C(compressed * 50 < size,
                    size << " -> " << compressed);
        }
        {
            ui32 size = 4096;
            ui32 compressed = CompressedSize(GenDataForLZ4(size));
            UNIT_ASSERT_C(compressed * 50 < size,
                    size << " -> " << compressed);
        }
        {
            ui32 size = 1052014;
            ui32 compressed = CompressedSize(GenDataForLZ4(size));
            UNIT_ASSERT_C(compressed * 200 < size,
                    size << " -> " << compressed);
        }
    }
}
}

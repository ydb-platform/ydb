#include <ydb/core/blobstorage/storagepoolmon/storagepool_counters.h>

#include <library/cpp/testing/unittest/registar.h> 

namespace NKikimr {
namespace NBlobStorageStoragePoolMonTest {

Y_UNIT_TEST_SUITE(TBlobStorageStoragePoolMonTest) {

Y_UNIT_TEST(SizeClassCalcTest) {
    ui32 expected[12] = {0, 0, 0,  0,   0,   1,   1,    2,     3,       4,         5,         5};
    ui32 input[12] =    {0, 5, 15, 255, 256, 257, 1562, 15969, 300'000, 2'000'000, 5'000'000, 20'000'000};
    for (ui32 i = 0; i < 12; ++i) {
        ui32 sizeClass = TStoragePoolCounters::SizeClassFromSizeBytes(input[i]);
        UNIT_ASSERT_C(sizeClass == expected[i],
                "input# " << input[i]
                << " expected# " << expected[i]
                << " sizeClass# " << sizeClass);
    }
}


} // Y_UNIT_TEST_SUITE TBlobStorageStoragePoolMonTest
} // namespace NBlobStorageStoragePoolMonTest
} // namespace NKikimr

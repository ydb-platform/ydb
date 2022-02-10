#include <library/cpp/testing/unittest/registar.h>


namespace NKikimr {
namespace NPDisk {
extern const ui64 YdbDefaultPDiskSequence;
}

Y_UNIT_TEST_SUITE(TYndxKeysDefaultKeyTest) {
    Y_UNIT_TEST(Smoke) {
        UNIT_ASSERT(NPDisk::YdbDefaultPDiskSequence != 0);
    }
}

}

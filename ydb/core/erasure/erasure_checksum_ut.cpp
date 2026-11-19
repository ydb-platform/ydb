#include "erasure.h"

#include <ydb/core/base/logoblob.h>
#include <ydb/core/erasure/erasure.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(TErasureChecksumTest) {

    Y_UNIT_TEST(IsRequired) {
        const TLogoBlobID logoBlobCrc0(1, 1, 1, 0, 100, 0, 0, TErasureType::CrcModeNone);
        const TLogoBlobID logoBlobCrc1(1, 1, 1, 0, 100, 0, 0, TErasureType::CrcModeWholePart);

        UNIT_ASSERT(!TChecksum::IsRequired(logoBlobCrc0));
        UNIT_ASSERT(TChecksum::IsRequired(logoBlobCrc1));
    }

}

} // NKikimr

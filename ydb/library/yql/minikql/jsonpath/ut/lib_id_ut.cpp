#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/rewrapper/proto/serialization.pb.h>
#include <util/system/platform.h>

/*
 * Paranoid test to check correct regexp library is used
 */

namespace NYql::NJsonPath {

extern ui32 GetReLibId();

Y_UNIT_TEST_SUITE(RegexpLib) {
    Y_UNIT_TEST(DefaultLib) {
#ifdef __x86_64__
        UNIT_ASSERT_VALUES_EQUAL(GetReLibId(), (ui32)NReWrapper::TSerialization::kHyperscan);
#else
        UNIT_ASSERT_VALUES_EQUAL(GetReLibId(), (ui32)NReWrapper::TSerialization::kRe2);
#endif
    }
}

}

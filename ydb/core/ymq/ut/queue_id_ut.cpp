#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/ymq/base/queue_id.h>

using namespace NKikimr::NSQS;

Y_UNIT_TEST_SUITE(TGenerateQueueIdTests) {
    static TString ValidateAndReturn(const TString& resourceId) {
        UNIT_ASSERT_EQUAL(resourceId.size(), 20);

        for (const auto c : resourceId) {
            UNIT_ASSERT(c <= 'v' && c >= '0');
        }

        return resourceId;
    }

    Y_UNIT_TEST(MakeQueueIdBasic) {
        UNIT_ASSERT_STRINGS_EQUAL(MakeQueueId(123, 0, "radix"), ValidateAndReturn("03r0000000000000024u"));
        UNIT_ASSERT_STRINGS_EQUAL(MakeQueueId(123, 1, "radix"), ValidateAndReturn("03r0000000000001024u"));
        UNIT_ASSERT_STRINGS_EQUAL(MakeQueueId(0x7FFF, 1, "radix"), ValidateAndReturn("vvv0000000000001024u"));
        UNIT_ASSERT_STRINGS_EQUAL(MakeQueueId(0, 0xFFFFFFFFFFFFFFFF, "radix"), ValidateAndReturn("000fvvvvvvvvvvvv024u"));
        UNIT_ASSERT_STRINGS_EQUAL(MakeQueueId(0x7FFF, 0xFFFFFFFFFFFFFFFF, "radix"), ValidateAndReturn("vvvfvvvvvvvvvvvv024u"));
        UNIT_ASSERT_STRINGS_EQUAL(MakeQueueId(0, 0, "Man, it is a very long account name, but does anyone really care?"), ValidateAndReturn("000000000000000006at"));
        UNIT_ASSERT_STRINGS_EQUAL(MakeQueueId(123, 16, "radix"), ValidateAndReturn("03r000000000000g024u"));
        UNIT_ASSERT_STRINGS_EQUAL(MakeQueueId(0, 0x097778AD59B52C00, "radix"), ValidateAndReturn("0000itrollcrab00024u"));
    }

}

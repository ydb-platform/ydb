#include <ydb/core/fq/libs/hmac/hmac.h>

#include <library/cpp/string_utils/base64/base64.h>
#include <library/cpp/testing/unittest/registar.h>

using namespace NFq;

namespace {
    constexpr TStringBuf SECRET = "AAAA";
    constexpr TStringBuf DATA = "BBBB";
}

Y_UNIT_TEST_SUITE(HmacSha) {
    Y_UNIT_TEST(HmacSha1) {
        UNIT_ASSERT_VALUES_EQUAL("7d54yAPz39TIawdgQBLcawAlXp8=", Base64Encode(HmacSha1(DATA, SECRET)));
        UNIT_ASSERT_VALUES_EQUAL("7d54yAPz39TIawdgQBLcawAlXp8=", HmacSha1Base64(DATA, SECRET));
    }
}

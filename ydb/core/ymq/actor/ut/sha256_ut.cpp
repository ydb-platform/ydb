#include <ydb/core/ymq/actor/sha256.h>

#include <ydb/core/ymq/base/helpers.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NSQS {

Y_UNIT_TEST_SUITE(SHA256Test) {
    Y_UNIT_TEST(SHA256Test) {
        UNIT_ASSERT_STRINGS_EQUAL(CalcSHA256(""), "E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855");
        UNIT_ASSERT_STRINGS_EQUAL(CalcSHA256("123"), "A665A45920422F9D417E4867EFDC4FB8A04A1F3FFF1FA07E998E86F7F7A27AE3");

        UNIT_ASSERT_STRINGS_UNEQUAL(CalcSHA256("1"), CalcSHA256("2"));
    }
}

} // namespace NKikimr::NSQS

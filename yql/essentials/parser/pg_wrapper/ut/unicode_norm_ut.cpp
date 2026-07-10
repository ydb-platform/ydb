#include "../pg_compat.h"

extern "C" {
#include <yql/essentials/parser/pg_wrapper/postgresql/src/include/postgres.h>
#include <yql/essentials/parser/pg_wrapper/postgresql/src/include/mb/pg_wchar.h>
#include <yql/essentials/parser/pg_wrapper/postgresql/src/include/common/unicode_norm.h>
}

#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/minikql/mkql_terminator.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYql {

namespace {

using namespace NKikimr::NMiniKQL;

bool WcharEquals(const pg_wchar* lhs, const pg_wchar* rhs) {
    while (*lhs || *rhs) {
        if (*lhs++ != *rhs++) {
            return false;
        }
    }
    return true;
}

void Utf8ToWchar(TStringBuf utf8, pg_wchar* out, size_t outSize) {
    Y_ENSURE(outSize > 0);
    const int len = pg_mb2wchar_with_len(utf8.data(), out, utf8.size());
    Y_ENSURE(len >= 0);
    Y_ENSURE(static_cast<size_t>(len) < outSize);
    out[len] = 0;
}

class TUnicodeNormTestContext {
public:
    TUnicodeNormTestContext() {
        SetDatabaseEncoding(PG_UTF8);
    }

private:
    TScopedAlloc Alloc_;
    TThrowingBindTerminator BindTerminator_;
};

} // namespace

Y_UNIT_TEST_SUITE(TUnicodeNormTests) {

Y_UNIT_TEST(TestUnicodeNormalizeNfcNonAscii) {
    TUnicodeNormTestContext ctx;

    // U+0061 U+0308 ("a" + combining diaeresis) -> NFC U+00E4 ("ä").
    constexpr TStringBuf decomposed = "\x61\xCC\x88";
    constexpr TStringBuf expected = "\xC3\xA4";

    pg_wchar input[16];
    pg_wchar expectedWide[16];
    Utf8ToWchar(decomposed, input, Y_ARRAY_SIZE(input));
    Utf8ToWchar(expected, expectedWide, Y_ARRAY_SIZE(expectedWide));

    pg_wchar* normalized = unicode_normalize(UNICODE_NFC, input);
    UNIT_ASSERT(normalized);
    UNIT_ASSERT(WcharEquals(normalized, expectedWide));
    pfree(normalized);
}

Y_UNIT_TEST(TestUnicodeIsNormalizedQuickcheckNonAscii) {
    TUnicodeNormTestContext ctx;

    pg_wchar nfcInput[16];
    Utf8ToWchar("\xC3\xA4\x62\x63", nfcInput, Y_ARRAY_SIZE(nfcInput)); // "äbc"

    UNIT_ASSERT_VALUES_EQUAL(
        unicode_is_normalized_quickcheck(UNICODE_NFC, nfcInput),
        UNICODE_NORM_QC_YES);

    pg_wchar nfdInput[16];
    Utf8ToWchar("\x61\xCC\x88\x62\x63", nfdInput, Y_ARRAY_SIZE(nfdInput)); // decomposed "äbc"

    UNIT_ASSERT_VALUES_EQUAL(
        unicode_is_normalized_quickcheck(UNICODE_NFC, nfdInput),
        UNICODE_NORM_QC_NO);
}

} // Y_UNIT_TEST_SUITE(TUnicodeNormTests)

} // namespace NYql

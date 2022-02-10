#include <library/cpp/yson_pull/detail/cescape.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NYsonPull::NDetail;

namespace {
    void test_roundtrip(const TVector<ui8>& str) {
        TStringBuf str_buf(
            reinterpret_cast<const char*>(str.data()),
            str.size());
        auto tmp = NCEscape::encode(str_buf);
        auto dest = NCEscape::decode(tmp);
        UNIT_ASSERT_VALUES_EQUAL_C(
            str_buf, TStringBuf(dest),
            "A[" << str.size() << "]: " << str_buf << '\n'
                 << "B[" << tmp.size() << "]: " << tmp << '\n'
                 << "C[" << dest.size() << "]: " << dest);
    }

    template <size_t N>
    void test_exhaustive(TVector<ui8>& str) {
        for (int i = 0; i < 256; ++i) {
            str[str.size() - N] = static_cast<char>(i);
            test_exhaustive<N - 1>(str);
        }
    }

    template <>
    void test_exhaustive<0>(TVector<ui8>& str) {
        test_roundtrip(str);
    }

    template <size_t N>
    void test_exhaustive() {
        TVector<ui8> str(N, ' ');
        test_exhaustive<N>(str);
    }

} // anonymous namespace

Y_UNIT_TEST_SUITE(CEscape) {
    Y_UNIT_TEST(ExhaustiveOneChar) {
        test_exhaustive<1>();
    }

    Y_UNIT_TEST(ExhaustiveTwoChars) {
        test_exhaustive<2>();
    }

    Y_UNIT_TEST(ExhaustiveThreeChars) {
        test_exhaustive<3>();
    }

    Y_UNIT_TEST(SpecialEscapeEncode) {
        //UNIT_ASSERT_VALUES_EQUAL(R"(\b)", NCEscape::encode("\b"));
        //UNIT_ASSERT_VALUES_EQUAL(R"(\f)", NCEscape::encode("\f"));
        UNIT_ASSERT_VALUES_EQUAL(R"(\n)", NCEscape::encode("\n"));
        UNIT_ASSERT_VALUES_EQUAL(R"(\r)", NCEscape::encode("\r"));
        UNIT_ASSERT_VALUES_EQUAL(R"(\t)", NCEscape::encode("\t"));
    }

    Y_UNIT_TEST(SpecialEscapeDecode) {
        UNIT_ASSERT_VALUES_EQUAL("\b", NCEscape::decode(R"(\b)"));
        UNIT_ASSERT_VALUES_EQUAL("\f", NCEscape::decode(R"(\f)"));
        UNIT_ASSERT_VALUES_EQUAL("\n", NCEscape::decode(R"(\n)"));
        UNIT_ASSERT_VALUES_EQUAL("\r", NCEscape::decode(R"(\r)"));
        UNIT_ASSERT_VALUES_EQUAL("\t", NCEscape::decode(R"(\t)"));
    }

} // Y_UNIT_TEST_SUITE(CEscape)

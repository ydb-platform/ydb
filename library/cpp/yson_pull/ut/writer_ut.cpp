#include <library/cpp/yson_pull/scalar.h>
#include <library/cpp/yson_pull/detail/writer.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/string.h>

#include <climits>
#include <limits>

using namespace std::string_view_literals;

namespace {
    template <typename Writer, typename Function>
    TString with_writer(Function&& function) {
        TString result;
        auto writer = NYsonPull::NDetail::make_writer<Writer>(
            NYsonPull::NOutput::FromString(&result),
            NYsonPull::EStreamType::Node);

        function(writer);

        return result;
    }

    template <typename Writer>
    TString to_yson_string(const NYsonPull::TScalar& value) {
        return with_writer<Writer>([&](NYsonPull::TWriter& writer) {
            writer.BeginStream().Scalar(value).EndStream();
        });
    }

    template <typename T>
    TString to_yson_binary_string(T&& value) {
        return to_yson_string<NYsonPull::NDetail::TBinaryWriterImpl>(std::forward<T>(value));
    }

    template <typename T>
    TString to_yson_text_string(T&& value) {
        return to_yson_string<NYsonPull::NDetail::TTextWriterImpl>(std::forward<T>(value));
    }

} // anonymous namespace

// =================== Text format =====================

Y_UNIT_TEST_SUITE(Writer) {
    Y_UNIT_TEST(TextEntity) {
        UNIT_ASSERT_VALUES_EQUAL(
            "#",
            to_yson_text_string(NYsonPull::TScalar{}));
    }

    Y_UNIT_TEST(TextBoolean) {
        UNIT_ASSERT_VALUES_EQUAL(
            "%false",
            to_yson_text_string(NYsonPull::TScalar{false}));
        UNIT_ASSERT_VALUES_EQUAL(
            "%true",
            to_yson_text_string(NYsonPull::TScalar{true}));
    }

    Y_UNIT_TEST(TextInt64) {
        UNIT_ASSERT_VALUES_EQUAL(
            "0",
            to_yson_text_string(NYsonPull::TScalar{i64{0}}));
        UNIT_ASSERT_VALUES_EQUAL(
            "200",
            to_yson_text_string(NYsonPull::TScalar{i64{200}}));
        UNIT_ASSERT_VALUES_EQUAL(
            "20000",
            to_yson_text_string(NYsonPull::TScalar{i64{20000}}));
        UNIT_ASSERT_VALUES_EQUAL(
            "200000000",
            to_yson_text_string(NYsonPull::TScalar{i64{200000000}}));
        UNIT_ASSERT_VALUES_EQUAL(
            "20000000000000000",
            to_yson_text_string(NYsonPull::TScalar{i64{20000000000000000}}));
        UNIT_ASSERT_VALUES_EQUAL(
            "9223372036854775807",
            to_yson_text_string(NYsonPull::TScalar{i64{INT64_MAX}}));

        UNIT_ASSERT_VALUES_EQUAL(
            "-200",
            to_yson_text_string(NYsonPull::TScalar{i64{-200}}));
        UNIT_ASSERT_VALUES_EQUAL(
            "-20000",
            to_yson_text_string(NYsonPull::TScalar{i64{-20000}}));
        UNIT_ASSERT_VALUES_EQUAL(
            "-200000000",
            to_yson_text_string(NYsonPull::TScalar{i64{-200000000}}));
        UNIT_ASSERT_VALUES_EQUAL(
            "-20000000000000000",
            to_yson_text_string(NYsonPull::TScalar{i64{-20000000000000000}}));
        UNIT_ASSERT_VALUES_EQUAL(
            "-9223372036854775808",
            to_yson_text_string(NYsonPull::TScalar{i64{INT64_MIN}}));
    }

    Y_UNIT_TEST(TextUInt64) {
        UNIT_ASSERT_VALUES_EQUAL(
            "0u",
            to_yson_text_string(NYsonPull::TScalar{ui64{0}}));
        UNIT_ASSERT_VALUES_EQUAL(
            "200u",
            to_yson_text_string(NYsonPull::TScalar{ui64{200}}));
        UNIT_ASSERT_VALUES_EQUAL(
            "20000u",
            to_yson_text_string(NYsonPull::TScalar{ui64{20000}}));
        UNIT_ASSERT_VALUES_EQUAL(
            "200000000u",
            to_yson_text_string(NYsonPull::TScalar{ui64{200000000}}));
        UNIT_ASSERT_VALUES_EQUAL(
            "20000000000000000u",
            to_yson_text_string(NYsonPull::TScalar{ui64{20000000000000000}}));
        UNIT_ASSERT_VALUES_EQUAL(
            "9223372036854775807u",
            to_yson_text_string(NYsonPull::TScalar{ui64{INT64_MAX}}));
        UNIT_ASSERT_VALUES_EQUAL(
            "18446744073709551615u",
            to_yson_text_string(NYsonPull::TScalar{ui64{UINT64_MAX}}));
    }

    Y_UNIT_TEST(TextFloat64) {
        UNIT_ASSERT_VALUES_EQUAL(
            "%inf",
            to_yson_text_string(NYsonPull::TScalar{std::numeric_limits<double>::infinity()}));
        UNIT_ASSERT_VALUES_EQUAL(
            "%-inf",
            to_yson_text_string(NYsonPull::TScalar{-std::numeric_limits<double>::infinity()}));
        UNIT_ASSERT_VALUES_EQUAL(
            "%nan",
            to_yson_text_string(NYsonPull::TScalar{std::numeric_limits<double>::quiet_NaN()}));
    }

    Y_UNIT_TEST(TextString) {
        UNIT_ASSERT_VALUES_EQUAL(
            R"("")",
            to_yson_text_string(NYsonPull::TScalar{""}));
        UNIT_ASSERT_VALUES_EQUAL(
            R"("hello")",
            to_yson_text_string(NYsonPull::TScalar{"hello"}));
        UNIT_ASSERT_VALUES_EQUAL(
            R"("hello\nworld")",
            to_yson_text_string(NYsonPull::TScalar{"hello\nworld"}));
    }

    // =================== Binary format =====================

    Y_UNIT_TEST(BinaryEntity) {
        UNIT_ASSERT_VALUES_EQUAL(
            "#",
            to_yson_binary_string(NYsonPull::TScalar{}));
    }

    Y_UNIT_TEST(BinaryBoolean) {
        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuf("\x4"),
            to_yson_binary_string(NYsonPull::TScalar{false}));
        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuf("\x5"),
            to_yson_binary_string(NYsonPull::TScalar{true}));
    }

    Y_UNIT_TEST(BinaryInt64) {
        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuf("\x2\0"sv),
            to_yson_binary_string(NYsonPull::TScalar{i64{0}}));
        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuf("\x2\x90\x3"),
            to_yson_binary_string(NYsonPull::TScalar{i64{200}}));
        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuf("\x2\xC0\xB8\x2"),
            to_yson_binary_string(NYsonPull::TScalar{i64{20000}}));
        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuf("\x2\x80\x88\xDE\xBE\x1"),
            to_yson_binary_string(NYsonPull::TScalar{i64{200000000}}));
        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuf("\x2\x80\x80\x90\xF8\x9B\xF9\x86G"),
            to_yson_binary_string(NYsonPull::TScalar{i64{20000000000000000}}));
        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuf("\x2\xFE\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\x1"),
            to_yson_binary_string(NYsonPull::TScalar{i64{INT64_MAX}}));

        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuf("\x2\x8F\x3"),
            to_yson_binary_string(NYsonPull::TScalar{i64{-200}}));
        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuf("\x2\xBF\xB8\x2"),
            to_yson_binary_string(NYsonPull::TScalar{i64{-20000}}));
        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuf("\x2\xFF\x87\xDE\xBE\x1"),
            to_yson_binary_string(NYsonPull::TScalar{i64{-200000000}}));
        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuf("\x2\xFF\xFF\x8F\xF8\x9B\xF9\x86G"),
            to_yson_binary_string(NYsonPull::TScalar{i64{-20000000000000000}}));
        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuf("\x2\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\x1"),
            to_yson_binary_string(NYsonPull::TScalar{i64{INT64_MIN}}));
    }

    Y_UNIT_TEST(BinaryUInt64) {
        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuf("\x6\0"sv),
            to_yson_binary_string(NYsonPull::TScalar{ui64{0}}));
        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuf("\x6\xC8\x1"),
            to_yson_binary_string(NYsonPull::TScalar{ui64{200}}));
        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuf("\x6\xA0\x9C\x1"),
            to_yson_binary_string(NYsonPull::TScalar{ui64{20000}}));
        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuf("\x6\x80\x84\xAF_"),
            to_yson_binary_string(NYsonPull::TScalar{ui64{200000000}}));
        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuf("\x6\x80\x80\x88\xFC\xCD\xBC\xC3#"),
            to_yson_binary_string(NYsonPull::TScalar{ui64{20000000000000000}}));
        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuf("\x6\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\x7F"),
            to_yson_binary_string(NYsonPull::TScalar{ui64{INT64_MAX}}));
        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuf("\x6\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\x1"),
            to_yson_binary_string(NYsonPull::TScalar{ui64{UINT64_MAX}}));
    }

    Y_UNIT_TEST(BinaryFloat64) {
        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuf("\x03\x00\x00\x00\x00\x00\x00\xf0\x7f"sv),
            to_yson_binary_string(NYsonPull::TScalar{std::numeric_limits<double>::infinity()}));
        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuf("\x03\x00\x00\x00\x00\x00\x00\xf0\xff"sv),
            to_yson_binary_string(NYsonPull::TScalar{-std::numeric_limits<double>::infinity()}));
        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuf("\x03\x00\x00\x00\x00\x00\x00\xf8\x7f"sv),
            to_yson_binary_string(NYsonPull::TScalar{std::numeric_limits<double>::quiet_NaN()}));
        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuf("\x03\x9a\x99\x99\x99\x99\x99\xf1\x3f"),
            to_yson_binary_string(NYsonPull::TScalar{double{1.1}}));
        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuf("\x03\x9a\x99\x99\x99\x99\x99\xf1\xbf"),
            to_yson_binary_string(NYsonPull::TScalar{double{-1.1}}));
    }

    Y_UNIT_TEST(BinaryString) {
        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuf("\x1\0"sv),
            to_yson_binary_string(NYsonPull::TScalar{""}));
        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuf("\x1\nhello"),
            to_yson_binary_string(NYsonPull::TScalar{"hello"}));
        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuf("\x1\x16hello\nworld"),
            to_yson_binary_string(NYsonPull::TScalar{"hello\nworld"}));
    }

} // Y_UNIT_TEST_SUITE(Writer)

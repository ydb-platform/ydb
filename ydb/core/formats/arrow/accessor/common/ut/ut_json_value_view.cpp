#include <ydb/core/formats/arrow/accessor/common/json_value_view.h>

#include <library/cpp/testing/unittest/registar.h>

#include <yql/essentials/types/binary_json/write.h>

using namespace NKikimr;
using namespace NKikimr::NArrow::NAccessor;

namespace {

NBinaryJson::TBinaryJson ToBinaryJson(TStringBuf json) {
    auto result = NBinaryJson::SerializeToBinaryJson(json);
    UNIT_ASSERT_C(std::holds_alternative<NBinaryJson::TBinaryJson>(result), json);
    return std::get<NBinaryJson::TBinaryJson>(std::move(result));
}

// Scalar projection rendered as a string, with "<null>" standing in for nullopt.
TString ScalarStr(const TJsonValueView& value) {
    auto scalar = value.GetScalarOptional();
    return scalar ? TString(*scalar) : TString("<null>");
}

TString BlobScalar(TStringBuf json) {
    auto blob = ToBinaryJson(json);
    return ScalarStr(TJsonValueView::OfBinaryJson(TStringBuf(blob.data(), blob.size())));
}

}   // namespace

Y_UNIT_TEST_SUITE(JsonValueView) {
    Y_UNIT_TEST(BinaryJsonScalars) {
        UNIT_ASSERT_VALUES_EQUAL(BlobScalar(R"("abc")"), "abc");
        UNIT_ASSERT_VALUES_EQUAL(BlobScalar("5"), "5");
        UNIT_ASSERT_VALUES_EQUAL(BlobScalar("2.5"), "2.5");
        UNIT_ASSERT_VALUES_EQUAL(BlobScalar("-3"), "-3");
        UNIT_ASSERT_VALUES_EQUAL(BlobScalar("true"), "true");
        UNIT_ASSERT_VALUES_EQUAL(BlobScalar("false"), "false");
    }

    Y_UNIT_TEST(BinaryJsonNonScalarsProduceNullForGetScalarOptional) {
        UNIT_ASSERT_VALUES_EQUAL(BlobScalar("null"), "<null>");
        UNIT_ASSERT_VALUES_EQUAL(BlobScalar(R"({"a":1})"), "<null>");
        UNIT_ASSERT_VALUES_EQUAL(BlobScalar("[1,2]"), "<null>");
        UNIT_ASSERT_VALUES_EQUAL(ScalarStr(TJsonValueView::OfBinaryJson(TStringBuf())), "<null>");
    }

    Y_UNIT_TEST(NativeScalars) {
        UNIT_ASSERT_VALUES_EQUAL(ScalarStr(TJsonValueView::OfString("abc")), "abc");
        UNIT_ASSERT_VALUES_EQUAL(ScalarStr(TJsonValueView::OfString("")), "");
        UNIT_ASSERT_VALUES_EQUAL(ScalarStr(TJsonValueView::OfNumber(5.0)), "5");
        UNIT_ASSERT_VALUES_EQUAL(ScalarStr(TJsonValueView::OfNumber(2.5)), "2.5");
        UNIT_ASSERT_VALUES_EQUAL(ScalarStr(TJsonValueView::OfNumber(-3.0)), "-3");
        UNIT_ASSERT_VALUES_EQUAL(ScalarStr(TJsonValueView::OfBool(true)), "true");
        UNIT_ASSERT_VALUES_EQUAL(ScalarStr(TJsonValueView::OfBool(false)), "false");
    }

    Y_UNIT_TEST(BinaryJsonBlobOnlyForBlobs) {
        auto blob = ToBinaryJson(R"({"a":1})");
        const TStringBuf bytes(blob.data(), blob.size());
        auto got = TJsonValueView::OfBinaryJson(bytes).GetBinaryJsonBlobOptional();
        UNIT_ASSERT(got.has_value());
        UNIT_ASSERT_VALUES_EQUAL(TString(*got), TString(bytes));
        UNIT_ASSERT(!TJsonValueView::OfBinaryJson(TStringBuf()).GetBinaryJsonBlobOptional());
        UNIT_ASSERT(!TJsonValueView::OfString("abc").GetBinaryJsonBlobOptional());
        UNIT_ASSERT(!TJsonValueView::OfNumber(5.0).GetBinaryJsonBlobOptional());
        UNIT_ASSERT(!TJsonValueView::OfBool(true).GetBinaryJsonBlobOptional());
    }

    Y_UNIT_TEST(NativeMatchesBinaryJson) {
        UNIT_ASSERT_VALUES_EQUAL(ScalarStr(TJsonValueView::OfString("hi")), BlobScalar(R"("hi")"));
        UNIT_ASSERT_VALUES_EQUAL(ScalarStr(TJsonValueView::OfNumber(5.0)), BlobScalar("5"));
        UNIT_ASSERT_VALUES_EQUAL(ScalarStr(TJsonValueView::OfNumber(2.5)), BlobScalar("2.5"));
        UNIT_ASSERT_VALUES_EQUAL(ScalarStr(TJsonValueView::OfBool(true)), BlobScalar("true"));
    }

    Y_UNIT_TEST(ToJsonValue) {
        UNIT_ASSERT_VALUES_EQUAL(TJsonValueView::OfString("hi").ToJsonValue().GetString(), "hi");
        UNIT_ASSERT_VALUES_EQUAL(TJsonValueView::OfNumber(2.5).ToJsonValue().GetDouble(), 2.5);
        UNIT_ASSERT(TJsonValueView::OfBool(true).ToJsonValue().GetBoolean());
        const auto blob = ToBinaryJson(R"({"a":1})");
        const auto json = TJsonValueView::OfBinaryJson(TStringBuf(blob.data(), blob.size())).ToJsonValue();
        UNIT_ASSERT_VALUES_EQUAL(json["a"].GetInteger(), 1);
    }
}

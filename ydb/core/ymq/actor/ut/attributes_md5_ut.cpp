#include <ydb/core/ymq/actor/attributes_md5.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NSQS {

void AddStringAttr(google::protobuf::RepeatedPtrField<TMessageAttribute>& attrs, const TString& name, const TString& value, const TString& type = "String") {
    auto* a = attrs.Add();
    a->SetName(name);
    a->SetStringValue(value);
    a->SetDataType(type);
}

void AddBinaryAttr(google::protobuf::RepeatedPtrField<TMessageAttribute>& attrs, const TString& name, const TString& value) {
    auto* a = attrs.Add();
    a->SetName(name);
    a->SetBinaryValue(value);
    a->SetDataType("Binary");
}

Y_UNIT_TEST_SUITE(AttributesMD5Test) {
    Y_UNIT_TEST(AmazonSampleWithString) {
        google::protobuf::RepeatedPtrField<TMessageAttribute> attrs;
        AddStringAttr(attrs, "test_attribute_name_2", "test_attribute_value_2");
        AddStringAttr(attrs, "test_attribute_name_1", "test_attribute_value_1");
        UNIT_ASSERT_STRINGS_EQUAL(CalcMD5OfMessageAttributes(attrs), "d53f3b558fe951154770f25cb63dbba9");
    }

    Y_UNIT_TEST(AmazonSampleWithBinary) {
        google::protobuf::RepeatedPtrField<TMessageAttribute> attrs;
        AddBinaryAttr(attrs, "test_attribute_name", "test_attribute_value");
        UNIT_ASSERT_STRINGS_EQUAL(CalcMD5OfMessageAttributes(attrs), "23f6bd27ea87aab7dfeadcc9aebee495");
    }
}

} // namespace NKikimr::NSQS

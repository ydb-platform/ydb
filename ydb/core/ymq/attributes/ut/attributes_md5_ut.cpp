#include <ydb/core/ymq/attributes/attributes_md5.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/vector.h>

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

void AddStringListAttr(google::protobuf::RepeatedPtrField<TMessageAttribute>& attrs, const TString& name, const TVector<TString>& values) {
    auto* a = attrs.Add();
    a->SetName(name);
    for (const auto& value : values) {
        a->add_stringlistvalues(value);
    }
    a->SetDataType("String.Array");
}

void AddBinaryListAttr(google::protobuf::RepeatedPtrField<TMessageAttribute>& attrs, const TString& name, const TVector<TString>& values) {
    auto* a = attrs.Add();
    a->SetName(name);
    for (const auto& value : values) {
        a->add_binarylistvalues(value);
    }
    a->SetDataType("Binary.Array");
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

    // Reference values are computed with the AWS message-attribute MD5 algorithm
    // (transport type code 3 for string lists, 4 for binary lists).
    Y_UNIT_TEST(SampleWithStringList) {
        google::protobuf::RepeatedPtrField<TMessageAttribute> attrs;
        AddStringListAttr(attrs, "test_attribute_name", {"first", "second", "third"});
        UNIT_ASSERT_STRINGS_EQUAL(CalcMD5OfMessageAttributes(attrs), "178e85f482e9afbc3c566e75d20b0546");
    }

    Y_UNIT_TEST(SampleWithBinaryList) {
        google::protobuf::RepeatedPtrField<TMessageAttribute> attrs;
        AddBinaryListAttr(attrs, "test_attribute_name", {"first-blob", "second-blob", "third-blob"});
        UNIT_ASSERT_STRINGS_EQUAL(CalcMD5OfMessageAttributes(attrs), "2c967037ad2925705931bc471a98e81b");
    }

    Y_UNIT_TEST(SampleWithMixedAttributes) {
        google::protobuf::RepeatedPtrField<TMessageAttribute> attrs;
        AddStringAttr(attrs, "str", "v1");
        AddBinaryAttr(attrs, "bin", "v2");
        AddStringListAttr(attrs, "str_list", {"a", "b"});
        AddBinaryListAttr(attrs, "bin_list", {"x", "y"});
        UNIT_ASSERT_STRINGS_EQUAL(CalcMD5OfMessageAttributes(attrs), "d00a0746613f628f6e9776f39708250a");
    }
}

} // namespace NKikimr::NSQS

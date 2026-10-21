#include <ydb/core/ymq/attributes/attributes.h>

#include <ydb/core/persqueue/public/constants.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/string.h>

using namespace NKikimr::NSQS;

namespace {

Ydb::Ymq::V1::Message MakeMessageWithAllAttributeKinds() {
    Ydb::Ymq::V1::Message message;

    {
        Ydb::Ymq::V1::MessageAttribute attr;
        attr.set_data_type("String");
        attr.set_string_value("string-value");
        (*message.mutable_message_attributes())["str"] = attr;
    }
    {
        Ydb::Ymq::V1::MessageAttribute attr;
        attr.set_data_type("Binary");
        attr.set_binary_value("binary-value");
        (*message.mutable_message_attributes())["bin"] = attr;
    }
    {
        Ydb::Ymq::V1::MessageAttribute attr;
        attr.set_data_type("String.Array");
        attr.add_string_list_values("a");
        attr.add_string_list_values("b");
        (*message.mutable_message_attributes())["str_list"] = attr;
    }
    {
        Ydb::Ymq::V1::MessageAttribute attr;
        attr.set_data_type("Binary.Array");
        attr.add_binary_list_values("x");
        attr.add_binary_list_values("y");
        (*message.mutable_message_attributes())["bin_list"] = attr;
    }

    return message;
}

void AssertAttributeEqual(
    const Ydb::Ymq::V1::MessageAttribute& actual,
    const Ydb::Ymq::V1::MessageAttribute& expected
) {
    UNIT_ASSERT_VALUES_EQUAL(actual.data_type(), expected.data_type());
    UNIT_ASSERT_VALUES_EQUAL(actual.string_value(), expected.string_value());
    UNIT_ASSERT_VALUES_EQUAL(actual.binary_value(), expected.binary_value());
    UNIT_ASSERT_VALUES_EQUAL(actual.string_list_values_size(), expected.string_list_values_size());
    for (int i = 0; i < expected.string_list_values_size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL(actual.string_list_values(i), expected.string_list_values(i));
    }
    UNIT_ASSERT_VALUES_EQUAL(actual.binary_list_values_size(), expected.binary_list_values_size());
    for (int i = 0; i < expected.binary_list_values_size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL(actual.binary_list_values(i), expected.binary_list_values(i));
    }
}

} // namespace

Y_UNIT_TEST_SUITE(UserAttributesSerializationTest) {
    Y_UNIT_TEST(RoundTripAllAttributeKinds) {
        const auto original = MakeMessageWithAllAttributeKinds();
        auto [attributes, md5] = SerializeUserAttributes(original);

        UNIT_ASSERT_VALUES_EQUAL(attributes.count(TString{NKikimr::NPQ::MESSAGE_ATTRIBUTE_ATTRIBUTES}), 1u);
        UNIT_ASSERT(!md5.empty());

        Ydb::Ymq::V1::Message restored;
        UNIT_ASSERT(DeserializeUserAttributes(restored, attributes));
        UNIT_ASSERT_VALUES_EQUAL(restored.m_d_5_of_message_attributes(), md5);
        UNIT_ASSERT_VALUES_EQUAL(restored.message_attributes_size(), original.message_attributes_size());

        for (const auto& [name, expected] : original.message_attributes()) {
            auto it = restored.message_attributes().find(name);
            UNIT_ASSERT_C(it != restored.message_attributes().end(), TStringBuilder() << "missing attribute " << name);
            AssertAttributeEqual(it->second, expected);
        }
    }

    Y_UNIT_TEST(EmptyAttributes) {
        Ydb::Ymq::V1::Message empty;
        auto [attributes, md5] = SerializeUserAttributes(empty);
        UNIT_ASSERT(attributes.empty());
        UNIT_ASSERT(md5.empty());

        Ydb::Ymq::V1::Message restored;
        std::unordered_multimap<TString, TString> noAttrs;
        UNIT_ASSERT(DeserializeUserAttributes(restored, noAttrs));
        UNIT_ASSERT_VALUES_EQUAL(restored.message_attributes_size(), 0);
        UNIT_ASSERT(restored.m_d_5_of_message_attributes().empty());
    }

    Y_UNIT_TEST(DeserializeInvalidPayloadReturnsFalse) {
        std::unordered_multimap<TString, TString> attributes;
        attributes.emplace(TString{NKikimr::NPQ::MESSAGE_ATTRIBUTE_ATTRIBUTES}, "not-a-protobuf");

        Ydb::Ymq::V1::Message restored;
        UNIT_ASSERT(!DeserializeUserAttributes(restored, attributes));
        UNIT_ASSERT_VALUES_EQUAL(restored.message_attributes_size(), 0);
    }

    Y_UNIT_TEST(SerializeMatchesMd5Helper) {
        const auto original = MakeMessageWithAllAttributeKinds();
        auto [attributes, md5] = SerializeUserAttributes(original);
        Y_UNUSED(attributes);

        NKikimr::NSQS::TMessageAttributes packed;
        for (const auto& [name, value] : original.message_attributes()) {
            auto* dst = packed.add_attributes();
            dst->SetName(name);
            dst->SetDataType(value.data_type());
            if (!value.string_value().empty()) {
                dst->SetStringValue(value.string_value());
            } else if (!value.binary_value().empty()) {
                dst->SetBinaryValue(value.binary_value());
            } else if (value.string_list_values_size()) {
                for (const auto& item : value.string_list_values()) {
                    dst->add_stringlistvalues(item);
                }
            } else if (value.binary_list_values_size()) {
                for (const auto& item : value.binary_list_values()) {
                    dst->add_binarylistvalues(item);
                }
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(md5, CalcMD5OfMessageAttributes(packed.attributes()));
    }
}

#include "wb_filter.h"

namespace NKikimr::NViewer {

template <>
i32 TFieldProtoValueExtractor<i32>::ExtractValue(const Reflection& reflection, const Message& element) const {
    return reflection.GetInt32(element, Field);
}

template <>
ui32 TFieldProtoValueExtractor<ui32>::ExtractValue(const Reflection& reflection, const Message& element) const {
    return reflection.GetUInt32(element, Field);
}

template <>
i64 TFieldProtoValueExtractor<i64>::ExtractValue(const Reflection& reflection, const Message& element) const {
    return reflection.GetInt64(element, Field);
}

template <>
ui64 TFieldProtoValueExtractor<ui64>::ExtractValue(const Reflection& reflection, const Message& element) const {
    return reflection.GetUInt64(element, Field);
}

template <>
double TFieldProtoValueExtractor<double>::ExtractValue(const Reflection& reflection, const Message& element) const {
    return reflection.GetDouble(element, Field);
}

template <>
float TFieldProtoValueExtractor<float>::ExtractValue(const Reflection& reflection, const Message& element) const {
    return reflection.GetFloat(element, Field);
}

template <>
bool TFieldProtoValueExtractor<bool>::ExtractValue(const Reflection& reflection, const Message& element) const {
    return reflection.GetBool(element, Field);
}

template <>
TString TFieldProtoValueExtractor<TString>::ExtractValue(const Reflection& reflection, const Message& element) const {
    return reflection.GetString(element, Field);
}

template <>
TEnumValue TFieldProtoValueExtractor<TEnumValue>::ExtractValue(const Reflection& reflection, const Message& element) const {
    return reflection.GetEnum(element, Field);
}

template <>
TMessageValue TFieldProtoValueExtractor<TMessageValue>::ExtractValue(const Reflection& parentReflection, const Message& element) const {
    const Message& message = parentReflection.GetMessage(element, Field);
    const Reflection& reflection = *message.GetReflection();
    const Descriptor& descriptor = *message.GetDescriptor();
    int fieldCount = descriptor.field_count();
    TStringBuilder result;
    for (int idxField = 0; idxField < fieldCount; ++idxField) {
        const FieldDescriptor* field = descriptor.field(idxField);
        if (!result.empty()) {
            result << '-';
        }
        switch (field->cpp_type()) {
        case FieldDescriptor::CPPTYPE_INT32:
            result << reflection.GetInt32(message, field);
            break;
        case FieldDescriptor::CPPTYPE_INT64:
            result << reflection.GetInt64(message, field);
            break;
        case FieldDescriptor::CPPTYPE_UINT32:
            result << reflection.GetUInt32(message, field);
            break;
        case FieldDescriptor::CPPTYPE_UINT64:
            result << reflection.GetUInt64(message, field);
            break;
        case FieldDescriptor::CPPTYPE_DOUBLE:
            result << reflection.GetDouble(message, field);
            break;
        case FieldDescriptor::CPPTYPE_FLOAT:
            result << reflection.GetFloat(message, field);
            break;
        case FieldDescriptor::CPPTYPE_BOOL:
            result << reflection.GetBool(message, field);
            break;
        case FieldDescriptor::CPPTYPE_ENUM:
            result << reflection.GetEnum(message, field)->number();
            break;
        case FieldDescriptor::CPPTYPE_STRING:
            result << reflection.GetString(message, field);
            break;
        case FieldDescriptor::CPPTYPE_MESSAGE:
            result << ExtractValue(reflection, message).Value;
            break;
        }
    }
    return result;
}

}

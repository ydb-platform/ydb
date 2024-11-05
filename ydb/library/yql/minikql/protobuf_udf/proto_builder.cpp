#include "proto_builder.h"

#include <ydb/library/yql/public/udf/udf_value_builder.h>

#include <util/generic/singleton.h>

using namespace google::protobuf;

namespace {
    using namespace NYql::NUdf;

    const EnumValueDescriptor& GetEnumValue(const TUnboxedValuePod& source, const FieldDescriptor& field,
                                            const TProtoInfo& info, TFlags<EFieldFlag> flags) {
        const auto* enumDescriptor = field.enum_type();
        Y_ENSURE(enumDescriptor);
        if (flags.HasFlags(EFieldFlag::EnumInt)) {
            const auto number = source.Get<i64>();
            const auto* result = enumDescriptor->FindValueByNumber(number);
            if (!result) {
                ythrow yexception() << "unknown value " << number
                                    << " for enum type " << enumDescriptor->full_name()
                                    << ", field " << field.full_name();
            }
            return *result;
        } else if (flags.HasFlags(EFieldFlag::EnumString)) {
            const TStringBuf name = source.AsStringRef();
            for (int i = 0; i < enumDescriptor->value_count(); ++i) {
                const auto& value = *enumDescriptor->value(i);
                if (value.name() == name) {
                    return value;
                }
            }
            ythrow yexception() << "unknown value " << name
                                << " for enum type " << enumDescriptor->full_name()
                                << ", field " << field.full_name();
        }
        if (info.EnumFormat == EEnumFormat::Number) {
            const auto number = source.Get<i32>();
            const auto* result = enumDescriptor->FindValueByNumber(number);
            if (!result) {
                ythrow yexception() << "unknown value " << number
                                    << " for enum type " << enumDescriptor->full_name()
                                    << ", field " << field.full_name();
            }
            return *result;
        }
        const TStringBuf name = source.AsStringRef();
        for (int i = 0; i < enumDescriptor->value_count(); ++i) {
            const auto& value = *enumDescriptor->value(i);
            const auto& valueName = info.EnumFormat == EEnumFormat::Name ? value.name() : value.full_name();
            if (valueName == name) {
                return value;
            }
        }
        ythrow yexception() << "unknown value " << name
                            << " for enum type " << enumDescriptor->full_name()
                            << ", field " << field.full_name();
    }

    void FillRepeatedField(const TUnboxedValuePod& source, Message& target,
                           const FieldDescriptor& field, const TProtoInfo& info, TFlags<EFieldFlag> flags) {
        const auto& reflection = *target.GetReflection();
        const auto iter = source.GetListIterator();
        reflection.ClearField(&target, &field);
        for (TUnboxedValue item; iter.Next(item);) {
            switch (field.type()) {
                case FieldDescriptor::TYPE_DOUBLE:
                    reflection.AddDouble(&target, &field, item.Get<double>());
                    break;

                case FieldDescriptor::TYPE_FLOAT:
                    reflection.AddFloat(&target, &field, info.YtMode ? float(item.Get<double>()) : item.Get<float>());
                    break;

                case FieldDescriptor::TYPE_INT64:
                case FieldDescriptor::TYPE_SFIXED64:
                case FieldDescriptor::TYPE_SINT64:
                    reflection.AddInt64(&target, &field, item.Get<i64>());
                    break;

                case FieldDescriptor::TYPE_ENUM:
                    {
                        const auto& enumValue = GetEnumValue(item, field, info, flags);
                        reflection.AddEnum(&target, &field, &enumValue);
                    }
                    break;
                case FieldDescriptor::TYPE_UINT64:
                case FieldDescriptor::TYPE_FIXED64:
                    reflection.AddUInt64(&target, &field, item.Get<ui64>());
                    break;

                case FieldDescriptor::TYPE_INT32:
                case FieldDescriptor::TYPE_SFIXED32:
                case FieldDescriptor::TYPE_SINT32:
                    reflection.AddInt32(&target, &field, item.Get<i32>());
                    break;

                case FieldDescriptor::TYPE_UINT32:
                case FieldDescriptor::TYPE_FIXED32:
                    reflection.AddUInt32(&target, &field, item.Get<ui32>());
                    break;

                case FieldDescriptor::TYPE_BOOL:
                    reflection.AddBool(&target, &field, item.Get<bool>());
                    break;

                case FieldDescriptor::TYPE_STRING:
                    reflection.AddString(&target, &field, TString(item.AsStringRef()));
                    break;

                case FieldDescriptor::TYPE_BYTES:
                    reflection.AddString(&target, &field, TString(item.AsStringRef()));
                    break;

                case FieldDescriptor::TYPE_MESSAGE:
                    {
                        auto* nestedMessage = reflection.AddMessage(&target, &field);
                        if (flags.HasFlags(EFieldFlag::Binary)) {
                            const auto& bytes = TStringBuf(item.AsStringRef());
                            Y_ENSURE(nestedMessage->ParseFromArray(bytes.data(), bytes.size()));
                        } else {
                            FillProtoFromValue(item, *nestedMessage, info);
                        }
                    }
                    break;

                default:
                    ythrow yexception() << "Unsupported protobuf type: "
                                        << field.type_name() << ", field: " << field.name();
            }
        }
    }

    void FillSingleField(const TUnboxedValuePod& source, Message& target,
                         const FieldDescriptor& field, const TProtoInfo& info, TFlags<EFieldFlag> flags) {
        const auto& reflection = *target.GetReflection();
        switch (field.type()) {
            case FieldDescriptor::TYPE_DOUBLE:
                reflection.SetDouble(&target, &field, source.Get<double>());
                break;

            case FieldDescriptor::TYPE_FLOAT:
                reflection.SetFloat(&target, &field, info.YtMode ? float(source.Get<double>()) : source.Get<float>());
                break;

            case FieldDescriptor::TYPE_INT64:
            case FieldDescriptor::TYPE_SFIXED64:
            case FieldDescriptor::TYPE_SINT64:
                reflection.SetInt64(&target, &field, source.Get<i64>());
                break;

            case FieldDescriptor::TYPE_ENUM:
                {
                    const auto& enumValue = GetEnumValue(source, field, info, flags);
                    reflection.SetEnum(&target, &field, &enumValue);
                }
                break;

            case FieldDescriptor::TYPE_UINT64:
            case FieldDescriptor::TYPE_FIXED64:
                reflection.SetUInt64(&target, &field, source.Get<ui64>());
                break;

            case FieldDescriptor::TYPE_INT32:
            case FieldDescriptor::TYPE_SFIXED32:
            case FieldDescriptor::TYPE_SINT32:
                reflection.SetInt32(&target, &field, source.Get<i32>());
                break;

            case FieldDescriptor::TYPE_UINT32:
            case FieldDescriptor::TYPE_FIXED32:
                reflection.SetUInt32(&target, &field, source.Get<ui32>());
                break;

            case FieldDescriptor::TYPE_BOOL:
                reflection.SetBool(&target, &field, source.Get<bool>());
                break;

            case FieldDescriptor::TYPE_STRING:
                reflection.SetString(&target, &field, TString(source.AsStringRef()));
                break;

            case FieldDescriptor::TYPE_BYTES:
                reflection.SetString(&target, &field, TString(source.AsStringRef()));
                break;

            case FieldDescriptor::TYPE_MESSAGE:
                {
                    auto* nestedMessage = reflection.MutableMessage(&target, &field);
                    if (flags.HasFlags(EFieldFlag::Binary)) {
                        const auto& bytes = TStringBuf(source.AsStringRef());
                        Y_ENSURE(nestedMessage->ParseFromArray(bytes.data(), bytes.size()));
                    } else {
                        FillProtoFromValue(source, *nestedMessage, info);
                    }
                }
                break;

            default:
                ythrow yexception() << "Unsupported protobuf type: "
                                    << field.type_name() << ", field: " << field.name();
        }
    }

    void FillMapField(const TUnboxedValuePod& source, Message& target, const FieldDescriptor& field, const TProtoInfo& info, TFlags<EFieldFlag> flags) {
        const auto& reflection = *target.GetReflection();
        reflection.ClearField(&target, &field);
        if (source) {
            const auto noBinaryFlags = TFlags<EFieldFlag>(flags).RemoveFlags(EFieldFlag::Binary);
            const auto iter = source.GetDictIterator();
            for (TUnboxedValue key, value; iter.NextPair(key, value);) {
                auto* nestedMessage = reflection.AddMessage(&target, &field);
                const auto& descriptor = *nestedMessage->GetDescriptor();
                FillSingleField(key, *nestedMessage, *descriptor.map_key(), info, noBinaryFlags);
                FillSingleField(value, *nestedMessage, *descriptor.map_value(), info, flags);
            }
        }
    }
}

namespace NYql::NUdf {

void FillProtoFromValue(const TUnboxedValuePod& source, Message& target, const TProtoInfo& info) {
    const auto& descriptor = *target.GetDescriptor();
    TMessageInfo* messageInfo;
    {
        const auto it = info.Messages.find(descriptor.full_name());
        if (it == info.Messages.end()) {
            ythrow yexception() << "unknown message " << descriptor.full_name();
        }
        messageInfo = it->second.get();
    }

    const auto& reflection = *target.GetReflection();
    for (int i = 0; i < descriptor.field_count(); ++i) {
        const auto& field = *descriptor.field(i);
        const auto it = messageInfo->Fields.find(field.number());
        Y_ENSURE(it != messageInfo->Fields.end());
        auto pos = it->second.Pos;
        TFlags<EFieldFlag> flags = it->second.Flags;
        auto fieldValue = source.GetElement(pos);
        if (field.containing_oneof() && flags.HasFlags(EFieldFlag::Variant)) {
            const ui32* varIndex = messageInfo->VariantIndicies.FindPtr(field.number());
            Y_ENSURE(varIndex);
            if (fieldValue && fieldValue.GetVariantIndex() == *varIndex) {
                fieldValue = fieldValue.GetVariantItem();
            } else {
                reflection.ClearField(&target, &field);
                continue;
            }
        }
        if (flags.HasFlags(EFieldFlag::Void)) {
            reflection.ClearField(&target, &field);
            continue;
        }

        if (!fieldValue) {
            if (field.is_required()) {
                ythrow yexception() << "required field " << field.name() << " has no value";
            }
            reflection.ClearField(&target, &field);
            continue;
        }
        if (field.is_map() && flags.HasFlags(EFieldFlag::Dict)) {
            FillMapField(fieldValue, target, field, info, flags);
        } else if (field.is_repeated()) {
            FillRepeatedField(fieldValue, target, field, info, flags);
        } else {
            FillSingleField(fieldValue, target, field, info, flags);
        }
    }
}

} // namespace NYql::NUdf


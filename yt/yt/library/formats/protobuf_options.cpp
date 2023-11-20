#include "protobuf_options.h"

#include <util/generic/variant.h>

namespace NYT::NFormats {

using ::google::protobuf::Descriptor;
using ::google::protobuf::OneofDescriptor;
using ::google::protobuf::FieldDescriptor;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

using TOneofOption = std::variant<
    EProtobufOneofMode>;

using TFieldOption = std::variant<
    ESpecialProtobufType,
    EProtobufSerializationMode,
    EProtobufListMode,
    EProtobufMapMode,
    EProtobufEnumWritingMode>;

using TMessageOption = std::variant<
    EProtobufFieldSortOrder>;

////////////////////////////////////////////////////////////////////////////////

TFieldOption FieldFlagToOption(EWrapperFieldFlag::Enum flag)
{
    using EFlag = EWrapperFieldFlag;
    switch (flag) {
        case EFlag::SERIALIZATION_PROTOBUF:
            return EProtobufSerializationMode::Protobuf;
        case EFlag::SERIALIZATION_YT:
            return EProtobufSerializationMode::Yt;
        case EFlag::EMBEDDED:
            return EProtobufSerializationMode::Embedded;

        case EFlag::ANY:
            return ESpecialProtobufType::Any;
        case EFlag::OTHER_COLUMNS:
            return ESpecialProtobufType::OtherColumns;
        case EFlag::ENUM_INT:
            return ESpecialProtobufType::EnumInt;
        case EFlag::ENUM_STRING:
            return ESpecialProtobufType::EnumString;

        case EFlag::OPTIONAL_LIST:
            return EProtobufListMode::Optional;
        case EFlag::REQUIRED_LIST:
            return EProtobufListMode::Required;

        case EFlag::MAP_AS_LIST_OF_STRUCTS_LEGACY:
            return EProtobufMapMode::ListOfStructsLegacy;
        case EFlag::MAP_AS_LIST_OF_STRUCTS:
            return EProtobufMapMode::ListOfStructs;
        case EFlag::MAP_AS_DICT:
            return EProtobufMapMode::Dict;
        case EFlag::MAP_AS_OPTIONAL_DICT:
            return EProtobufMapMode::OptionalDict;

        case EFlag::ENUM_SKIP_UNKNOWN_VALUES:
            return EProtobufEnumWritingMode::SkipUnknownValues;
        case EFlag::ENUM_CHECK_VALUES:
            return EProtobufEnumWritingMode::CheckValues;
    }
    Y_ABORT();
}

TMessageOption MessageFlagToOption(EWrapperMessageFlag::Enum flag)
{
    using EFlag = EWrapperMessageFlag;
    switch (flag) {
        case EFlag::DEPRECATED_SORT_FIELDS_AS_IN_PROTO_FILE:
            return EProtobufFieldSortOrder::AsInProtoFile;
        case EFlag::SORT_FIELDS_BY_FIELD_NUMBER:
            return EProtobufFieldSortOrder::ByFieldNumber;
    }
    Y_ABORT();
}

TOneofOption OneofFlagToOption(EWrapperOneofFlag::Enum flag)
{
    using EFlag = EWrapperOneofFlag;
    switch (flag) {
        case EFlag::SEPARATE_FIELDS:
            return EProtobufOneofMode::SeparateFields;
        case EFlag::VARIANT:
            return EProtobufOneofMode::Variant;
    }
    Y_ABORT();
}

TString OptionToFieldFlagName(TFieldOption option)
{
    using EFlag = EWrapperFieldFlag;
    struct TVisitor
    {
        EFlag::Enum operator() (ESpecialProtobufType type)
        {
            switch (type) {
                case ESpecialProtobufType::Any:
                    return EFlag::ANY;
                case ESpecialProtobufType::OtherColumns:
                    return EFlag::OTHER_COLUMNS;
                case ESpecialProtobufType::EnumInt:
                    return EFlag::ENUM_INT;
                case ESpecialProtobufType::EnumString:
                    return EFlag::ENUM_STRING;
            }
            Y_ABORT();
        }
        EFlag::Enum operator() (EProtobufSerializationMode serializationMode)
        {
            switch (serializationMode) {
                case EProtobufSerializationMode::Yt:
                    return EFlag::SERIALIZATION_YT;
                case EProtobufSerializationMode::Protobuf:
                    return EFlag::SERIALIZATION_PROTOBUF;
                case EProtobufSerializationMode::Embedded:
                    return EFlag::EMBEDDED;
            }
            Y_ABORT();
        }
        EFlag::Enum operator() (EProtobufListMode listMode)
        {
            switch (listMode) {
                case EProtobufListMode::Optional:
                    return EFlag::OPTIONAL_LIST;
                case EProtobufListMode::Required:
                    return EFlag::REQUIRED_LIST;
            }
            Y_ABORT();
        }
        EFlag::Enum operator() (EProtobufMapMode mapMode)
        {
            switch (mapMode) {
                case EProtobufMapMode::ListOfStructsLegacy:
                    return EFlag::MAP_AS_LIST_OF_STRUCTS_LEGACY;
                case EProtobufMapMode::ListOfStructs:
                    return EFlag::MAP_AS_LIST_OF_STRUCTS;
                case EProtobufMapMode::Dict:
                    return EFlag::MAP_AS_DICT;
                case EProtobufMapMode::OptionalDict:
                    return EFlag::MAP_AS_OPTIONAL_DICT;
            }
            Y_ABORT();
        }
        EFlag::Enum operator() (EProtobufEnumWritingMode enumWritingMode)
        {
            switch (enumWritingMode) {
                case EProtobufEnumWritingMode::SkipUnknownValues:
                    return EFlag::ENUM_SKIP_UNKNOWN_VALUES;
                case EProtobufEnumWritingMode::CheckValues:
                    return EFlag::ENUM_CHECK_VALUES;
            }
            Y_ABORT();
        }
    };

    return EWrapperFieldFlag_Enum_Name(std::visit(TVisitor(), option));
}

TString OptionToMessageFlagName(TMessageOption option)
{
    using EFlag = EWrapperMessageFlag;
    struct TVisitor
    {
        EFlag::Enum operator() (EProtobufFieldSortOrder sortOrder)
        {
            switch (sortOrder) {
                case EProtobufFieldSortOrder::AsInProtoFile:
                    return EFlag::DEPRECATED_SORT_FIELDS_AS_IN_PROTO_FILE;
                case EProtobufFieldSortOrder::ByFieldNumber:
                    return EFlag::SORT_FIELDS_BY_FIELD_NUMBER;
            }
            Y_ABORT();
        }
    };

    return EWrapperMessageFlag_Enum_Name(std::visit(TVisitor(), option));
}

TString OptionToOneofFlagName(TOneofOption option)
{
    using EFlag = EWrapperOneofFlag;
    struct TVisitor
    {
        EFlag::Enum operator() (EProtobufOneofMode mode)
        {
            switch (mode) {
                case EProtobufOneofMode::SeparateFields:
                    return EFlag::SEPARATE_FIELDS;
                case EProtobufOneofMode::Variant:
                    return EFlag::VARIANT;
            }
            Y_ABORT();
        }
    };

    return EWrapperOneofFlag_Enum_Name(std::visit(TVisitor(), option));
}


template <typename T, typename TOptionToFlagName>
void SetOption(TMaybe<T>& option, T newOption, TOptionToFlagName optionToFlagName)
{
    if (option) {
        if (*option == newOption) {
            ythrow yexception() << "Duplicate protobuf flag " << optionToFlagName(newOption);
        } else {
            ythrow yexception() << "Incompatible protobuf flags " <<
                optionToFlagName(*option) << " and " << optionToFlagName(newOption);
        }
    }
    option = newOption;
}

class TParseProtobufFieldOptionsVisitor
{
public:
    void operator() (ESpecialProtobufType type)
    {
        SetOption(Type, type);
    }

    void operator() (EProtobufSerializationMode serializationMode)
    {
        SetOption(SerializationMode, serializationMode);
    }

    void operator() (EProtobufListMode listMode)
    {
        SetOption(ListMode, listMode);
    }

    void operator() (EProtobufMapMode mapMode)
    {
        SetOption(MapMode, mapMode);
    }

    void operator() (EProtobufEnumWritingMode enumWritingMode)
    {
        SetOption(EnumWritingMode, enumWritingMode);
    }

    template <typename T>
    void SetOption(TMaybe<T>& option, T newOption)
    {
        NYT::NFormats::SetOption(option, newOption, OptionToFieldFlagName);
    }

public:
    TMaybe<ESpecialProtobufType> Type;
    TMaybe<EProtobufSerializationMode> SerializationMode;
    TMaybe<EProtobufListMode> ListMode;
    TMaybe<EProtobufMapMode> MapMode;
    TMaybe<EProtobufEnumWritingMode> EnumWritingMode;
};

class TParseProtobufMessageOptionsVisitor
{
public:
    void operator() (EProtobufFieldSortOrder fieldSortOrder)
    {
        SetOption(FieldSortOrder, fieldSortOrder);
    }

    template <typename T>
    void SetOption(TMaybe<T>& option, T newOption)
    {
        NYT::NFormats::SetOption(option, newOption, OptionToMessageFlagName);
    }

public:
    TMaybe<EProtobufFieldSortOrder> FieldSortOrder;
};

class TParseProtobufOneofOptionsVisitor
{
public:
    void operator() (EProtobufOneofMode mode)
    {
        SetOption(Mode, mode);
    }

    template <typename T>
    void SetOption(TMaybe<T>& option, T newOption)
    {
        NYT::NFormats::SetOption(option, newOption, OptionToOneofFlagName);
    }

public:
    TMaybe<EProtobufOneofMode> Mode;
};

void ParseProtobufFieldOptions(
    const ::google::protobuf::RepeatedField<EWrapperFieldFlag::Enum>& flags,
    TProtobufFieldOptions* fieldOptions)
{
    TParseProtobufFieldOptionsVisitor visitor;
    for (auto flag : flags) {
        std::visit(visitor, FieldFlagToOption(flag));
    }
    if (visitor.Type) {
        fieldOptions->Type = *visitor.Type;
    }
    if (visitor.SerializationMode) {
        fieldOptions->SerializationMode = *visitor.SerializationMode;
    }
    if (visitor.ListMode) {
        fieldOptions->ListMode = *visitor.ListMode;
    }
    if (visitor.MapMode) {
        fieldOptions->MapMode = *visitor.MapMode;
    }
    if (visitor.EnumWritingMode) {
        fieldOptions->EnumWritingMode = *visitor.EnumWritingMode;
    }
}

void ParseProtobufMessageOptions(
    const ::google::protobuf::RepeatedField<EWrapperMessageFlag::Enum>& flags,
    TProtobufMessageOptions* messageOptions)
{
    TParseProtobufMessageOptionsVisitor visitor;
    for (auto flag : flags) {
        std::visit(visitor, MessageFlagToOption(flag));
    }
    if (visitor.FieldSortOrder) {
        messageOptions->FieldSortOrder = *visitor.FieldSortOrder;
    }
}

void ParseProtobufOneofOptions(
    const ::google::protobuf::RepeatedField<EWrapperOneofFlag::Enum>& flags,
    TProtobufOneofOptions* messageOptions)
{
    TParseProtobufOneofOptionsVisitor visitor;
    for (auto flag : flags) {
        std::visit(visitor, OneofFlagToOption(flag));
    }
    if (visitor.Mode) {
        messageOptions->Mode = *visitor.Mode;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

TProtobufFieldOptions GetDefaultFieldOptions(
    const Descriptor* descriptor,
    TProtobufFieldOptions defaultFieldOptions)
{
    ParseProtobufFieldOptions(
        descriptor->file()->options().GetRepeatedExtension(file_default_field_flags),
        &defaultFieldOptions);
    ParseProtobufFieldOptions(
        descriptor->options().GetRepeatedExtension(default_field_flags),
        &defaultFieldOptions);
    return defaultFieldOptions;
}

TProtobufOneofOptions GetDefaultOneofOptions(const Descriptor* descriptor)
{
    TProtobufOneofOptions defaultOneofOptions;
    ParseProtobufOneofOptions(
        descriptor->file()->options().GetRepeatedExtension(file_default_oneof_flags),
        &defaultOneofOptions);
    ParseProtobufOneofOptions(
        descriptor->options().GetRepeatedExtension(default_oneof_flags),
        &defaultOneofOptions);
    switch (defaultOneofOptions.Mode) {
        case EProtobufOneofMode::Variant: {
            auto defaultFieldOptions = GetDefaultFieldOptions(descriptor);
            switch (defaultFieldOptions.SerializationMode) {
                case EProtobufSerializationMode::Protobuf:
                    // For Protobuf serialization mode default is SeparateFields.
                    defaultOneofOptions.Mode = EProtobufOneofMode::SeparateFields;
                    return defaultOneofOptions;
                case EProtobufSerializationMode::Yt:
                case EProtobufSerializationMode::Embedded:
                    return defaultOneofOptions;
            }
            Y_ABORT();
        }
        case EProtobufOneofMode::SeparateFields:
            return defaultOneofOptions;
    }
    Y_ABORT();
}

TProtobufFieldOptions GetFieldOptions(
    const FieldDescriptor* fieldDescriptor,
    const TMaybe<TProtobufFieldOptions>& defaultFieldOptions)
{
    TProtobufFieldOptions options;
    if (defaultFieldOptions) {
        options = *defaultFieldOptions;
    } else {
        options = GetDefaultFieldOptions(fieldDescriptor->containing_type());
    }
    ParseProtobufFieldOptions(fieldDescriptor->options().GetRepeatedExtension(flags), &options);
    return options;
}

TProtobufOneofOptions GetOneofOptions(
    const OneofDescriptor* oneofDescriptor,
    const TMaybe<TProtobufOneofOptions>& defaultOneofOptions)
{
    TProtobufOneofOptions options;
    if (defaultOneofOptions) {
        options = *defaultOneofOptions;
    } else {
        options = GetDefaultOneofOptions(oneofDescriptor->containing_type());
    }
    ParseProtobufOneofOptions(oneofDescriptor->options().GetRepeatedExtension(oneof_flags), &options);

    if (oneofDescriptor->is_synthetic()) {
        options.Mode = EProtobufOneofMode::SeparateFields;
    }

    auto variantFieldName = oneofDescriptor->options().GetExtension(variant_field_name);
    switch (options.Mode) {
        case EProtobufOneofMode::SeparateFields:
            if (variantFieldName) {
                ythrow yexception() << "\"variant_field_name\" requires (NYT.oneof_flags) = VARIANT";
            }
            break;
        case EProtobufOneofMode::Variant:
            if (variantFieldName) {
                options.VariantFieldName = variantFieldName;
            } else {
                options.VariantFieldName = oneofDescriptor->name();
            }
            break;
    }
    return options;
}

TProtobufMessageOptions GetMessageOptions(const Descriptor* descriptor)
{
    TProtobufMessageOptions options;
    ParseProtobufMessageOptions(
        descriptor->file()->options().GetRepeatedExtension(file_default_message_flags),
        &options);
    ParseProtobufMessageOptions(
        descriptor->options().GetRepeatedExtension(message_flags),
        &options);
    return options;
}

TString GetColumnName(const FieldDescriptor* field)
{
    const auto& options = field->options();
    const auto columnName = options.GetExtension(column_name);
    if (!columnName.empty()) {
        return columnName;
    }
    const auto keyColumnName = options.GetExtension(key_column_name);
    if (!keyColumnName.empty()) {
        return keyColumnName;
    }
    return field->name();
}

void ValidateProtobufType(const FieldDescriptor* fieldDescriptor, ESpecialProtobufType protobufType)
{
    const auto fieldType = fieldDescriptor->type();
    auto ensureType = [&] (FieldDescriptor::Type expectedType) {
        Y_ENSURE(fieldType == expectedType,
            "Type of field " << fieldDescriptor->full_name() << "does not match specified field flag " <<
            OptionToFieldFlagName(protobufType) << ": "
            "expected " << FieldDescriptor::TypeName(expectedType) << ", " <<
            "got " << FieldDescriptor::TypeName(fieldType));
    };
    switch (protobufType) {
        case ESpecialProtobufType::Any:
            ensureType(FieldDescriptor::TYPE_BYTES);
            return;
        case ESpecialProtobufType::OtherColumns:
            ensureType(FieldDescriptor::TYPE_BYTES);
            return;
        case ESpecialProtobufType::EnumInt:
            ensureType(FieldDescriptor::TYPE_ENUM);
            return;
        case ESpecialProtobufType::EnumString:
            ensureType(FieldDescriptor::TYPE_ENUM);
            return;
    }
    Y_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats

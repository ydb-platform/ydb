#include "protobuf_format.h"

#include "errors.h"

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt_proto/yt/formats/extension.pb.h>

#include <google/protobuf/text_format.h>

#include <library/cpp/yson/node/node_io.h>

#include <util/generic/hash_set.h>
#include <util/generic/stack.h>
#include <util/generic/overloaded.h>

#include <util/stream/output.h>
#include <util/stream/file.h>

namespace NYT::NDetail {

using ::google::protobuf::Descriptor;
using ::google::protobuf::DescriptorProto;
using ::google::protobuf::EnumDescriptor;
using ::google::protobuf::EnumDescriptorProto;
using ::google::protobuf::FieldDescriptor;
using ::google::protobuf::FieldDescriptorProto;
using ::google::protobuf::OneofDescriptor;
using ::google::protobuf::Message;
using ::google::protobuf::FileDescriptor;
using ::google::protobuf::FileDescriptorProto;
using ::google::protobuf::FileDescriptorSet;
using ::google::protobuf::FieldOptions;
using ::google::protobuf::FileOptions;
using ::google::protobuf::OneofOptions;
using ::google::protobuf::MessageOptions;

using ::ToString;

namespace {

////////////////////////////////////////////////////////////////////////////////

using TOneofOption = std::variant<
    EProtobufOneofMode>;

using TFieldOption = std::variant<
    EProtobufType,
    EProtobufSerializationMode,
    EProtobufListMode,
    EProtobufMapMode,
    EProtobufEnumWritingMode>;

using TMessageOption = std::variant<
    EProtobufFieldSortOrder>;

struct TOtherColumns
{ };

using TValueTypeOrOtherColumns = std::variant<EValueType, TOtherColumns>;

////////////////////////////////////////////////////////////////////////////////

TFieldOption FieldFlagToOption(EWrapperFieldFlag::Enum flag)
{
    using EFlag = EWrapperFieldFlag;
    switch (flag) {
        case EFlag::SERIALIZATION_PROTOBUF:
            return EProtobufSerializationMode::Protobuf;
        case EFlag::SERIALIZATION_YT:
            return EProtobufSerializationMode::Yt;

        case EFlag::ANY:
            return EProtobufType::Any;
        case EFlag::OTHER_COLUMNS:
            return EProtobufType::OtherColumns;
        case EFlag::ENUM_INT:
            return EProtobufType::EnumInt;
        case EFlag::ENUM_STRING:
            return EProtobufType::EnumString;

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
        case EFlag::EMBEDDED:
            return EProtobufSerializationMode::Embedded;

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

EWrapperFieldFlag::Enum OptionToFieldFlag(TFieldOption option)
{
    using EFlag = EWrapperFieldFlag;
    struct TVisitor
    {
        EFlag::Enum operator() (EProtobufType type)
        {
            switch (type) {
                case EProtobufType::Any:
                    return EFlag::ANY;
                case EProtobufType::OtherColumns:
                    return EFlag::OTHER_COLUMNS;
                case EProtobufType::EnumInt:
                    return EFlag::ENUM_INT;
                case EProtobufType::EnumString:
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

    return std::visit(TVisitor(), option);
}

EWrapperMessageFlag::Enum OptionToMessageFlag(TMessageOption option)
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

    return std::visit(TVisitor(), option);
}

EWrapperOneofFlag::Enum OptionToOneofFlag(TOneofOption option)
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

    return std::visit(TVisitor(), option);
}


template <typename T, typename TOptionToFlag>
void SetOption(TMaybe<T>& option, T newOption, TOptionToFlag optionToFlag)
{
    if (option) {
        if (*option == newOption) {
            ythrow yexception() << "Duplicate protobuf flag " << optionToFlag(newOption);
        } else {
            ythrow yexception() << "Incompatible protobuf flags " <<
                optionToFlag(*option) << " and " << optionToFlag(newOption);
        }
    }
    option = newOption;
}

class TParseProtobufFieldOptionsVisitor
{
public:
    void operator() (EProtobufType type)
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
        NYT::NDetail::SetOption(option, newOption, OptionToFieldFlag);
    }

public:
    TMaybe<EProtobufType> Type;
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
        NYT::NDetail::SetOption(option, newOption, OptionToMessageFlag);
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
        NYT::NDetail::SetOption(option, newOption, OptionToOneofFlag);
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

TProtobufFieldOptions GetDefaultFieldOptions(
    const Descriptor* descriptor,
    TProtobufFieldOptions defaultFieldOptions = {})
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

////////////////////////////////////////////////////////////////////////////////

void ValidateProtobufType(const FieldDescriptor& fieldDescriptor, EProtobufType protobufType)
{
    const auto fieldType = fieldDescriptor.type();
    auto ensureType = [&] (FieldDescriptor::Type expectedType) {
        Y_ENSURE(fieldType == expectedType,
            "Type of field " << fieldDescriptor.name() << "does not match specified field flag " <<
            OptionToFieldFlag(protobufType) << ": "
            "expected " << FieldDescriptor::TypeName(expectedType) << ", " <<
            "got " << FieldDescriptor::TypeName(fieldType));
    };
    switch (protobufType) {
        case EProtobufType::Any:
            ensureType(FieldDescriptor::TYPE_BYTES);
            return;
        case EProtobufType::OtherColumns:
            ensureType(FieldDescriptor::TYPE_BYTES);
            return;
        case EProtobufType::EnumInt:
            ensureType(FieldDescriptor::TYPE_ENUM);
            return;
        case EProtobufType::EnumString:
            ensureType(FieldDescriptor::TYPE_ENUM);
            return;
    }
    Y_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

class TCycleChecker
{
private:
    class TGuard
    {
    public:
        TGuard(TCycleChecker* checker, const Descriptor* descriptor)
            : Checker_(checker)
            , Descriptor_(descriptor)
        {
            Checker_->ActiveVertices_.insert(Descriptor_);
            Checker_->Stack_.push(Descriptor_);
        }

        ~TGuard()
        {
            Checker_->ActiveVertices_.erase(Descriptor_);
            Checker_->Stack_.pop();
        }

    private:
        TCycleChecker* Checker_;
        const Descriptor* Descriptor_;
    };

public:
    [[nodiscard]] TGuard Enter(const Descriptor* descriptor)
    {
        if (ActiveVertices_.contains(descriptor)) {
            Y_ABORT_UNLESS(!Stack_.empty());
            ythrow TApiUsageError() << "Cyclic reference found for protobuf messages. " <<
                "Consider removing " << EWrapperFieldFlag::SERIALIZATION_YT << " flag " <<
                "somewhere on the cycle containing " <<
                Stack_.top()->full_name() << " and " << descriptor->full_name();
        }
        return TGuard(this, descriptor);
    }

private:
    THashSet<const Descriptor*> ActiveVertices_;
    TStack<const Descriptor*> Stack_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

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
            if (!variantFieldName.empty()) {
                ythrow TApiUsageError() << "\"variant_field_name\" requires (NYT.oneof_flags) = VARIANT";
            }
            break;
        case EProtobufOneofMode::Variant:
            if (variantFieldName.empty()) {
                options.VariantFieldName = FromProto<TString>(oneofDescriptor->name());
            } else {
                options.VariantFieldName = variantFieldName;
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

TNode MakeEnumerationConfig(const ::google::protobuf::EnumDescriptor* enumDescriptor)
{
    auto config = TNode::CreateMap();
    for (int i = 0; i < enumDescriptor->value_count(); ++i) {
        config[enumDescriptor->value(i)->name()] = enumDescriptor->value(i)->number();
    }
    return config;
}

TString DeduceProtobufType(
    const FieldDescriptor* fieldDescriptor,
    const TProtobufFieldOptions& options)
{
    if (options.Type) {
        ValidateProtobufType(*fieldDescriptor, *options.Type);
        return ToString(*options.Type);
    }
    switch (fieldDescriptor->type()) {
        case FieldDescriptor::TYPE_ENUM:
            return ToString(EProtobufType::EnumString);
        case FieldDescriptor::TYPE_MESSAGE:
            switch (options.SerializationMode) {
                case EProtobufSerializationMode::Protobuf:
                    return "message";
                case EProtobufSerializationMode::Yt:
                    return "structured_message";
                case EProtobufSerializationMode::Embedded:
                    return "embedded_message";
            }
            Y_ABORT();
        default:
            return fieldDescriptor->type_name();
    }
    Y_ABORT();
}

TString GetColumnName(const ::google::protobuf::FieldDescriptor& field)
{
    const auto& options = field.options();
    const auto columnName = FromProto<TString>(options.GetExtension(column_name));
    if (!columnName.empty()) {
        return columnName;
    }
    const auto keyColumnName = FromProto<TString>(options.GetExtension(key_column_name));
    if (!keyColumnName.empty()) {
        return keyColumnName;
    }
    return FromProto<TString>(field.name());
}

TNode MakeProtoFormatMessageFieldsConfig(
    const Descriptor* descriptor,
    TNode* enumerations,
    TCycleChecker& cycleChecker);

TNode MakeProtoFormatMessageFieldsConfig(
    const Descriptor* descriptor,
    TNode* enumerations,
    const TProtobufFieldOptions& defaultFieldOptions,
    const TProtobufOneofOptions& defaultOneofOptions,
    TCycleChecker& cycleChecker);

TNode MakeMapFieldsConfig(
    const FieldDescriptor* fieldDescriptor,
    TNode* enumerations,
    const TProtobufFieldOptions& fieldOptions,
    TCycleChecker& cycleChecker)
{
    Y_ABORT_UNLESS(fieldDescriptor->is_map());
    auto message = fieldDescriptor->message_type();
    switch (fieldOptions.MapMode) {
        case EProtobufMapMode::ListOfStructsLegacy:
            return MakeProtoFormatMessageFieldsConfig(
                message,
                enumerations,
                cycleChecker);
        case EProtobufMapMode::ListOfStructs:
        case EProtobufMapMode::Dict:
        case EProtobufMapMode::OptionalDict: {
            TProtobufFieldOptions defaultFieldOptions;
            defaultFieldOptions.SerializationMode = EProtobufSerializationMode::Yt;
            return MakeProtoFormatMessageFieldsConfig(
                message,
                enumerations,
                defaultFieldOptions,
                TProtobufOneofOptions{},
                cycleChecker);
        }
    }
    Y_ABORT();
}

TNode MakeProtoFormatFieldConfig(
    const FieldDescriptor* fieldDescriptor,
    TNode* enumerations,
    const TProtobufFieldOptions& defaultOptions,
    TCycleChecker& cycleChecker)
{
    auto fieldConfig = TNode::CreateMap();
    fieldConfig["field_number"] = fieldDescriptor->number();
    fieldConfig["name"] = GetColumnName(*fieldDescriptor);

    auto fieldOptions = GetFieldOptions(fieldDescriptor, defaultOptions);

    Y_ENSURE(fieldOptions.SerializationMode != EProtobufSerializationMode::Embedded,
        "EMBEDDED flag is currently supported only with "
        "ProtobufFormatWithDescriptors config option set to true");

    if (fieldDescriptor->is_repeated()) {
        Y_ENSURE_EX(fieldOptions.SerializationMode == EProtobufSerializationMode::Yt,
            TApiUsageError() << "Repeated field \"" << fieldDescriptor->full_name() << "\" " <<
            "must have flag \"" << EWrapperFieldFlag::SERIALIZATION_YT << "\"");
    }
    fieldConfig["repeated"] = fieldDescriptor->is_repeated();
    fieldConfig["packed"] = fieldDescriptor->is_packed();

    fieldConfig["proto_type"] = DeduceProtobufType(fieldDescriptor, fieldOptions);

    if (fieldDescriptor->type() == FieldDescriptor::TYPE_ENUM) {
        auto* enumeration = fieldDescriptor->enum_type();
        (*enumerations)[enumeration->full_name()] = MakeEnumerationConfig(enumeration);
        fieldConfig["enumeration_name"] = FromProto<TString>(enumeration->full_name());
    }

    if (fieldOptions.SerializationMode != EProtobufSerializationMode::Yt) {
        return fieldConfig;
    }

    if (fieldDescriptor->is_map()) {
        fieldConfig["fields"] = MakeMapFieldsConfig(fieldDescriptor, enumerations, fieldOptions, cycleChecker);
        return fieldConfig;
    }

    if (fieldDescriptor->type() == FieldDescriptor::TYPE_MESSAGE) {
        fieldConfig["fields"] = MakeProtoFormatMessageFieldsConfig(
            fieldDescriptor->message_type(),
            enumerations,
            cycleChecker);
    }

    return fieldConfig;
}

void MakeProtoFormatOneofConfig(
    const OneofDescriptor* oneofDescriptor,
    TNode* enumerations,
    const TProtobufFieldOptions& defaultFieldOptions,
    const TProtobufOneofOptions& defaultOneofOptions,
    TCycleChecker& cycleChecker,
    TNode* fields)
{
    auto addFields = [&] (TNode* fields) {
        for (int i = 0; i < oneofDescriptor->field_count(); ++i) {
            fields->Add(MakeProtoFormatFieldConfig(
                oneofDescriptor->field(i),
                enumerations,
                defaultFieldOptions,
                cycleChecker));
        }
    };

    auto oneofOptions = GetOneofOptions(oneofDescriptor, defaultOneofOptions);
    switch (oneofOptions.Mode) {
        case EProtobufOneofMode::SeparateFields:
            addFields(fields);
            return;
        case EProtobufOneofMode::Variant: {
            auto oneofFields = TNode::CreateList();
            addFields(&oneofFields);
            auto oneofField = TNode()
                ("proto_type", "oneof")
                ("name", oneofOptions.VariantFieldName)
                ("fields", std::move(oneofFields));
            fields->Add(std::move(oneofField));
            return;
        }
    }
    Y_ABORT();
}

TNode MakeProtoFormatMessageFieldsConfig(
    const Descriptor* descriptor,
    TNode* enumerations,
    const TProtobufFieldOptions& defaultFieldOptions,
    const TProtobufOneofOptions& defaultOneofOptions,
    TCycleChecker& cycleChecker)
{
    auto fields = TNode::CreateList();
    THashSet<const OneofDescriptor*> visitedOneofs;
    auto guard = cycleChecker.Enter(descriptor);
    for (int fieldIndex = 0; fieldIndex < descriptor->field_count(); ++fieldIndex) {
        auto fieldDescriptor = descriptor->field(fieldIndex);
        auto oneofDescriptor = fieldDescriptor->containing_oneof();
        if (!oneofDescriptor) {
            fields.Add(MakeProtoFormatFieldConfig(
                fieldDescriptor,
                enumerations,
                defaultFieldOptions,
                cycleChecker));
        } else if (!visitedOneofs.contains(oneofDescriptor)) {
            MakeProtoFormatOneofConfig(
                oneofDescriptor,
                enumerations,
                defaultFieldOptions,
                defaultOneofOptions,
                cycleChecker,
                &fields);
            visitedOneofs.insert(oneofDescriptor);
        }
    }
    return fields;
}

TNode MakeProtoFormatMessageFieldsConfig(
    const Descriptor* descriptor,
    TNode* enumerations,
    TCycleChecker& cycleChecker)
{
    return MakeProtoFormatMessageFieldsConfig(
        descriptor,
        enumerations,
        GetDefaultFieldOptions(descriptor),
        GetDefaultOneofOptions(descriptor),
        cycleChecker);
}

TNode MakeProtoFormatConfigWithTables(const TVector<const Descriptor*>& descriptors)
{
    TNode config("protobuf");
    config.Attributes()
        ("enumerations", TNode::CreateMap())
        ("tables", TNode::CreateList());

    auto& enumerations = config.Attributes()["enumerations"];

    for (auto* descriptor : descriptors) {
        TCycleChecker cycleChecker;
        auto columns = MakeProtoFormatMessageFieldsConfig(descriptor, &enumerations, cycleChecker);
        config.Attributes()["tables"].Add(
            TNode()("columns", std::move(columns)));
    }

    return config;
}

////////////////////////////////////////////////////////////////////////////////

class TFileDescriptorSetBuilder
{
public:
    TFileDescriptorSetBuilder()
        : ExtensionFile_(EWrapperFieldFlag::descriptor()->file())
    { }

    void AddDescriptor(const Descriptor* descriptor)
    {
        auto [it, inserted] = AllDescriptors_.insert(descriptor);
        if (!inserted) {
            return;
        }

        const auto* containingType = descriptor->containing_type();
        while (containingType) {
            AddDescriptor(containingType);
            containingType = containingType->containing_type();
        }
        for (int i = 0; i < descriptor->field_count(); ++i) {
            AddField(descriptor->field(i));
        }
    }

    FileDescriptorSet Build()
    {
        THashSet<const FileDescriptor*> visitedFiles;
        TVector<const FileDescriptor*> fileTopoOrder;
        for (const auto* descriptor : AllDescriptors_) {
            TraverseDependencies(descriptor->file(), visitedFiles, fileTopoOrder);
        }

        THashSet<TString> messageTypeNames;
        THashSet<TString> enumTypeNames;
        for (const auto* descriptor : AllDescriptors_) {
            messageTypeNames.insert(FromProto<TString>(descriptor->full_name()));
        }
        for (const auto* enumDescriptor : EnumDescriptors_) {
            enumTypeNames.insert(FromProto<TString>(enumDescriptor->full_name()));
        }
        FileDescriptorSet fileDescriptorSetProto;
        for (const auto* file : fileTopoOrder) {
            auto* fileProto = fileDescriptorSetProto.add_file();
            file->CopyTo(fileProto);
            Strip(fileProto, messageTypeNames, enumTypeNames);
        }
        return fileDescriptorSetProto;
    }

private:
    void AddField(const FieldDescriptor* fieldDescriptor)
    {
        if (fieldDescriptor->message_type()) {
            AddDescriptor(fieldDescriptor->message_type());
        }
        if (fieldDescriptor->enum_type()) {
            AddEnumDescriptor(fieldDescriptor->enum_type());
        }
    }

    void AddEnumDescriptor(const EnumDescriptor* enumDescriptor)
    {
        auto [it, inserted] = EnumDescriptors_.insert(enumDescriptor);
        if (!inserted) {
            return;
        }
        const auto* containingType = enumDescriptor->containing_type();
        while (containingType) {
            AddDescriptor(containingType);
            containingType = containingType->containing_type();
        }
    }

    void TraverseDependencies(
        const FileDescriptor* current,
        THashSet<const FileDescriptor*>& visited,
        TVector<const FileDescriptor*>& topoOrder)
    {
        auto [it, inserted] = visited.insert(current);
        if (!inserted) {
            return;
        }
        for (int i = 0; i < current->dependency_count(); ++i) {
            TraverseDependencies(current->dependency(i), visited, topoOrder);
        }
        topoOrder.push_back(current);
    }

    template <typename TOptions>
    void StripUnknownOptions(TOptions* options)
    {
        std::vector<const FieldDescriptor*> fields;
        auto reflection = options->GetReflection();
        reflection->ListFields(*options, &fields);
        for (auto field : fields) {
            if (field->is_extension() && field->file() != ExtensionFile_) {
                reflection->ClearField(options, field);
            }
        }
    }

    template <typename TRepeatedField, typename TPredicate>
    void RemoveIf(TRepeatedField* repeatedField, TPredicate predicate)
    {
        repeatedField->erase(
            std::remove_if(repeatedField->begin(), repeatedField->end(), predicate),
            repeatedField->end());
    }

    void Strip(
        const TString& containingTypePrefix,
        DescriptorProto* messageProto,
        const THashSet<TString>& messageTypeNames,
        const THashSet<TString>& enumTypeNames)
    {
        const auto prefix = containingTypePrefix + messageProto->name() + '.';

        RemoveIf(messageProto->mutable_nested_type(), [&] (const DescriptorProto& descriptorProto) {
            return !messageTypeNames.contains(prefix + descriptorProto.name());
        });
        RemoveIf(messageProto->mutable_enum_type(), [&] (const EnumDescriptorProto& enumDescriptorProto) {
            return !enumTypeNames.contains(prefix + enumDescriptorProto.name());
        });

        messageProto->clear_extension();
        StripUnknownOptions(messageProto->mutable_options());
        for (auto& fieldProto : *messageProto->mutable_field()) {
            StripUnknownOptions(fieldProto.mutable_options());
        }
        for (auto& oneofProto : *messageProto->mutable_oneof_decl()) {
            StripUnknownOptions(oneofProto.mutable_options());
        }
        for (auto& nestedTypeProto : *messageProto->mutable_nested_type()) {
            Strip(prefix, &nestedTypeProto, messageTypeNames, enumTypeNames);
        }
        for (auto& enumProto : *messageProto->mutable_enum_type()) {
            StripUnknownOptions(enumProto.mutable_options());
            for (auto& enumValue : *enumProto.mutable_value()) {
                StripUnknownOptions(enumValue.mutable_options());
            }
        }
    }

    void Strip(
        FileDescriptorProto* fileProto,
        const THashSet<TString>& messageTypeNames,
        const THashSet<TString>& enumTypeNames)
    {
        const auto prefix = fileProto->package().empty()
            ? ""
            : FromProto<TString>(fileProto->package()) + '.';

        RemoveIf(fileProto->mutable_message_type(), [&] (const DescriptorProto& descriptorProto) {
            return !messageTypeNames.contains(prefix + descriptorProto.name());
        });
        RemoveIf(fileProto->mutable_enum_type(), [&] (const EnumDescriptorProto& enumDescriptorProto) {
            return !enumTypeNames.contains(prefix + enumDescriptorProto.name());
        });

        fileProto->clear_service();
        fileProto->clear_extension();

        StripUnknownOptions(fileProto->mutable_options());
        for (auto& messageProto : *fileProto->mutable_message_type()) {
            Strip(prefix, &messageProto, messageTypeNames, enumTypeNames);
        }
        for (auto& enumProto : *fileProto->mutable_enum_type()) {
            StripUnknownOptions(enumProto.mutable_options());
            for (auto& enumValue : *enumProto.mutable_value()) {
                StripUnknownOptions(enumValue.mutable_options());
            }
        }
    }

private:
    const FileDescriptor* const ExtensionFile_;
    THashSet<const Descriptor*> AllDescriptors_;
    THashSet<const EnumDescriptor*> EnumDescriptors_;
};

TNode MakeProtoFormatConfigWithDescriptors(const TVector<const Descriptor*>& descriptors)
{
    TFileDescriptorSetBuilder builder;
    auto typeNames = TNode::CreateList();
    for (const auto* descriptor : descriptors) {
        builder.AddDescriptor(descriptor);
        typeNames.Add(FromProto<TString>(descriptor->full_name()));
    }

    auto fileDescriptorSetText = FromProto<TString>(builder.Build().ShortDebugString());
    TNode config("protobuf");
    config.Attributes()
        ("file_descriptor_set_text", std::move(fileDescriptorSetText))
        ("type_names", std::move(typeNames));
    return config;
}

////////////////////////////////////////////////////////////////////////////////

using TTypePtrOrOtherColumns = std::variant<NTi::TTypePtr, TOtherColumns>;

struct TMember {
    TString Name;
    TTypePtrOrOtherColumns TypeOrOtherColumns;
};

////////////////////////////////////////////////////////////////////////////////

TValueTypeOrOtherColumns GetScalarFieldType(
    const FieldDescriptor& fieldDescriptor,
    const TProtobufFieldOptions& options)
{
    if (options.Type) {
        switch (*options.Type) {
            case EProtobufType::EnumInt:
                return EValueType::VT_INT64;
            case EProtobufType::EnumString:
                return EValueType::VT_STRING;
            case EProtobufType::Any:
                return EValueType::VT_ANY;
            case EProtobufType::OtherColumns:
                return TOtherColumns{};
        }
        Y_ABORT();
    }

    switch (fieldDescriptor.cpp_type()) {
        case FieldDescriptor::CPPTYPE_INT32:
            return EValueType::VT_INT32;
        case FieldDescriptor::CPPTYPE_INT64:
            return EValueType::VT_INT64;
        case FieldDescriptor::CPPTYPE_UINT32:
            return EValueType::VT_UINT32;
        case FieldDescriptor::CPPTYPE_UINT64:
            return EValueType::VT_UINT64;
        case FieldDescriptor::CPPTYPE_FLOAT:
        case FieldDescriptor::CPPTYPE_DOUBLE:
            return EValueType::VT_DOUBLE;
        case FieldDescriptor::CPPTYPE_BOOL:
            return EValueType::VT_BOOLEAN;
        case FieldDescriptor::CPPTYPE_STRING:
        case FieldDescriptor::CPPTYPE_MESSAGE:
        case FieldDescriptor::CPPTYPE_ENUM:
            return EValueType::VT_STRING;
        default:
            ythrow yexception() <<
                "Unexpected field type '" << fieldDescriptor.cpp_type_name() << "' " <<
                "for field " << fieldDescriptor.name();
    }
}

bool HasNameExtension(const FieldDescriptor& fieldDescriptor)
{
    const auto& options = fieldDescriptor.options();
    return options.HasExtension(column_name) || options.HasExtension(key_column_name);
}

void SortFields(TVector<const FieldDescriptor*>& fieldDescriptors, EProtobufFieldSortOrder fieldSortOrder)
{
    switch (fieldSortOrder) {
        case EProtobufFieldSortOrder::AsInProtoFile:
            return;
        case EProtobufFieldSortOrder::ByFieldNumber:
            SortBy(fieldDescriptors, [] (const FieldDescriptor* fieldDescriptor) {
                return fieldDescriptor->number();
            });
            return;
    }
    Y_ABORT();
}

NTi::TTypePtr CreateStruct(TStringBuf fieldName, TVector<TMember> members)
{
    TVector<NTi::TStructType::TOwnedMember> structMembers;
    structMembers.reserve(members.size());
    for (auto& member : members) {
        std::visit(TOverloaded{
            [&] (TOtherColumns) {
                ythrow TApiUsageError() <<
                    "Could not deduce YT type for field " << member.Name << " of " <<
                    "embedded message field " << fieldName << " " <<
                    "(note that " << EWrapperFieldFlag::OTHER_COLUMNS << " fields " <<
                    "are not allowed inside embedded messages)";
            },
            [&] (NTi::TTypePtr& type) {
                structMembers.emplace_back(std::move(member.Name), std::move(type));
            },
        }, member.TypeOrOtherColumns);
    }
    return NTi::Struct(std::move(structMembers));
}

TMaybe<TVector<TString>> InferColumnFilter(const ::google::protobuf::Descriptor& descriptor)
{
    auto isOtherColumns = [] (const ::google::protobuf::FieldDescriptor& field) {
        return GetFieldOptions(&field).Type == EProtobufType::OtherColumns;
    };

    TVector<TString> result;
    result.reserve(descriptor.field_count());
    for (int i = 0; i < descriptor.field_count(); ++i) {
        const auto& field = *descriptor.field(i);
        if (isOtherColumns(field)) {
            return {};
        }
        result.push_back(GetColumnName(field));
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

class TTableSchemaInferrer
{
public:
    TTableSchemaInferrer(bool keepFieldsWithoutExtension)
        : KeepFieldsWithoutExtension_(keepFieldsWithoutExtension)
    { }

    TTableSchema InferSchema(const Descriptor& messageDescriptor);

private:
    TTypePtrOrOtherColumns GetFieldType(
        const FieldDescriptor& fieldDescriptor,
        const TProtobufFieldOptions& defaultOptions);

    void ProcessOneofField(
        TStringBuf containingFieldName,
        const OneofDescriptor& oneofDescriptor,
        const TProtobufFieldOptions& defaultFieldOptions,
        const TProtobufOneofOptions& defaultOneofOptions,
        EProtobufFieldSortOrder fieldSortOrder,
        TVector<TMember>* members);

    TVector<TMember> GetMessageMembers(
        TStringBuf containingFieldName,
        const Descriptor& fieldDescriptor,
        TProtobufFieldOptions defaultFieldOptions,
        std::optional<EProtobufFieldSortOrder> overrideFieldSortOrder = std::nullopt);

    NTi::TTypePtr GetMessageType(
        const FieldDescriptor& fieldDescriptor,
        TProtobufFieldOptions defaultFieldOptions);

    NTi::TTypePtr GetMapType(
        const FieldDescriptor& fieldDescriptor,
        const TProtobufFieldOptions& fieldOptions);

private:
    void GetMessageMembersImpl(
        TStringBuf containingFieldName,
        const Descriptor& fieldDescriptor,
        TProtobufFieldOptions defaultFieldOptions,
        std::optional<EProtobufFieldSortOrder> overrideFieldSortOrder,
        TVector<TMember>* members);

private:
    const bool KeepFieldsWithoutExtension_;
    TCycleChecker CycleChecker_;
};

void TTableSchemaInferrer::ProcessOneofField(
    TStringBuf containingFieldName,
    const OneofDescriptor& oneofDescriptor,
    const TProtobufFieldOptions& defaultFieldOptions,
    const TProtobufOneofOptions& defaultOneofOptions,
    EProtobufFieldSortOrder fieldSortOrder,
    TVector<TMember>* members)
{
    auto oneofOptions = GetOneofOptions(&oneofDescriptor, defaultOneofOptions);

    auto addFields = [&] (TVector<TMember>* members, bool removeOptionality) {
        TVector<const FieldDescriptor*> fieldDescriptors;
        for (int i = 0; i < oneofDescriptor.field_count(); ++i) {
            fieldDescriptors.push_back(oneofDescriptor.field(i));
        }
        SortFields(fieldDescriptors, fieldSortOrder);
        for (auto innerFieldDescriptor : fieldDescriptors) {
            auto typeOrOtherColumns = GetFieldType(
                *innerFieldDescriptor,
                defaultFieldOptions);
            if (auto* maybeType = std::get_if<NTi::TTypePtr>(&typeOrOtherColumns);
                maybeType && removeOptionality && (*maybeType)->IsOptional())
            {
                typeOrOtherColumns = (*maybeType)->AsOptional()->GetItemType();
            }
            members->push_back(TMember{
                GetColumnName(*innerFieldDescriptor),
                std::move(typeOrOtherColumns),
            });
        }
    };

    switch (oneofOptions.Mode) {
        case EProtobufOneofMode::SeparateFields:
            addFields(members, /* removeOptionality */ false);
            return;
        case EProtobufOneofMode::Variant: {
            TVector<TMember> variantMembers;
            addFields(&variantMembers, /* removeOptionality */ true);
            members->push_back(TMember{
                oneofOptions.VariantFieldName,
                NTi::Optional(
                    NTi::Variant(
                        CreateStruct(containingFieldName, std::move(variantMembers))
                    )
                )
            });
            return;
        }
    }
    Y_ABORT();
}

TVector<TMember> TTableSchemaInferrer::GetMessageMembers(
    TStringBuf containingFieldName,
    const Descriptor& messageDescriptor,
    TProtobufFieldOptions defaultFieldOptions,
    std::optional<EProtobufFieldSortOrder> overrideFieldSortOrder)
{
    TVector<TMember> members;
    GetMessageMembersImpl(
        containingFieldName,
        messageDescriptor,
        defaultFieldOptions,
        overrideFieldSortOrder,
        &members
    );
    return members;
}

void TTableSchemaInferrer::GetMessageMembersImpl(
    TStringBuf containingFieldName,
    const Descriptor& messageDescriptor,
    TProtobufFieldOptions defaultFieldOptions,
    std::optional<EProtobufFieldSortOrder> overrideFieldSortOrder,
    TVector<TMember>* members)
{
    auto guard = CycleChecker_.Enter(&messageDescriptor);
    defaultFieldOptions = GetDefaultFieldOptions(&messageDescriptor, defaultFieldOptions);
    auto messageOptions = GetMessageOptions(&messageDescriptor);
    auto defaultOneofOptions = GetDefaultOneofOptions(&messageDescriptor);

    TVector<const FieldDescriptor*> fieldDescriptors;
    fieldDescriptors.reserve(messageDescriptor.field_count());
    for (int i = 0; i < messageDescriptor.field_count(); ++i) {
        if (!KeepFieldsWithoutExtension_ && !HasNameExtension(*messageDescriptor.field(i))) {
            continue;
        }
        fieldDescriptors.push_back(messageDescriptor.field(i));
    }

    auto fieldSortOrder = overrideFieldSortOrder.value_or(messageOptions.FieldSortOrder);
    SortFields(fieldDescriptors, fieldSortOrder);

    THashSet<const OneofDescriptor*> visitedOneofs;
    for (const auto innerFieldDescriptor : fieldDescriptors) {
        auto oneofDescriptor = innerFieldDescriptor->containing_oneof();
        if (oneofDescriptor) {
            if (visitedOneofs.contains(oneofDescriptor)) {
                continue;
            }
            ProcessOneofField(
                containingFieldName,
                *oneofDescriptor,
                defaultFieldOptions,
                defaultOneofOptions,
                messageOptions.FieldSortOrder,
                members);
            visitedOneofs.insert(oneofDescriptor);
            continue;
        }
        auto fieldOptions = GetFieldOptions(innerFieldDescriptor, defaultFieldOptions);
        if (fieldOptions.SerializationMode == EProtobufSerializationMode::Embedded) {
            Y_ENSURE(innerFieldDescriptor->type() == FieldDescriptor::TYPE_MESSAGE,
                "EMBEDDED column must have message type");
            Y_ENSURE(innerFieldDescriptor->label() == FieldDescriptor::LABEL_REQUIRED,
                "EMBEDDED column must be marked required");
            GetMessageMembersImpl(
                innerFieldDescriptor->full_name(),
                *innerFieldDescriptor->message_type(),
                defaultFieldOptions,
                /*overrideFieldSortOrder*/ std::nullopt,
                members);
        } else {
            auto typeOrOtherColumns = GetFieldType(
                *innerFieldDescriptor,
                defaultFieldOptions);
            members->push_back(TMember{
                GetColumnName(*innerFieldDescriptor),
                std::move(typeOrOtherColumns),
            });
        }
    }
}

NTi::TTypePtr TTableSchemaInferrer::GetMessageType(
    const FieldDescriptor& fieldDescriptor,
    TProtobufFieldOptions defaultFieldOptions)
{
    Y_ABORT_UNLESS(fieldDescriptor.message_type());
    const auto& messageDescriptor = *fieldDescriptor.message_type();
    auto members = GetMessageMembers(
        fieldDescriptor.full_name(),
        messageDescriptor,
        defaultFieldOptions);

    return CreateStruct(fieldDescriptor.full_name(), std::move(members));
}

NTi::TTypePtr TTableSchemaInferrer::GetMapType(
    const FieldDescriptor& fieldDescriptor,
    const TProtobufFieldOptions& fieldOptions)
{
    Y_ABORT_UNLESS(fieldDescriptor.is_map());
    switch (fieldOptions.MapMode) {
        case EProtobufMapMode::ListOfStructsLegacy:
        case EProtobufMapMode::ListOfStructs: {
            TProtobufFieldOptions embeddedOptions;
            if (fieldOptions.MapMode == EProtobufMapMode::ListOfStructs) {
                embeddedOptions.SerializationMode = EProtobufSerializationMode::Yt;
            }
            auto list = NTi::List(GetMessageType(fieldDescriptor, embeddedOptions));
            switch (fieldOptions.ListMode) {
                case EProtobufListMode::Required:
                    return list;
                case EProtobufListMode::Optional:
                    return NTi::Optional(std::move(list));
            }
            Y_ABORT();
        }
        case EProtobufMapMode::Dict:
        case EProtobufMapMode::OptionalDict: {
            auto message = fieldDescriptor.message_type();
            Y_ABORT_UNLESS(message->field_count() == 2);
            auto keyVariant = GetScalarFieldType(*message->field(0), TProtobufFieldOptions{});
            Y_ABORT_UNLESS(std::holds_alternative<EValueType>(keyVariant));
            auto key = std::get<EValueType>(keyVariant);
            TProtobufFieldOptions embeddedOptions;
            embeddedOptions.SerializationMode = EProtobufSerializationMode::Yt;
            auto valueVariant = GetFieldType(*message->field(1), embeddedOptions);
            Y_ABORT_UNLESS(std::holds_alternative<NTi::TTypePtr>(valueVariant));
            auto value = std::get<NTi::TTypePtr>(valueVariant);
            Y_ABORT_UNLESS(value->IsOptional());
            value = value->AsOptional()->GetItemType();
            auto dict = NTi::Dict(ToTypeV3(key, true), value);
            if (fieldOptions.MapMode == EProtobufMapMode::OptionalDict) {
                return NTi::Optional(dict);
            } else {
                return dict;
            }
        }
    }
}

TTypePtrOrOtherColumns TTableSchemaInferrer::GetFieldType(
    const FieldDescriptor& fieldDescriptor,
    const TProtobufFieldOptions& defaultOptions)
{
    auto fieldOptions = GetFieldOptions(&fieldDescriptor, defaultOptions);
    if (fieldOptions.Type) {
        ValidateProtobufType(fieldDescriptor, *fieldOptions.Type);
    }

    auto getScalarType = [&] {
        auto valueTypeOrOtherColumns = GetScalarFieldType(fieldDescriptor, fieldOptions);
        return std::visit(TOverloaded{
            [] (TOtherColumns) -> TTypePtrOrOtherColumns {
                return TOtherColumns{};
            },
            [] (EValueType valueType) -> TTypePtrOrOtherColumns {
                return ToTypeV3(valueType, true);
            }
        }, valueTypeOrOtherColumns);
    };

    auto withFieldLabel = [&] (const TTypePtrOrOtherColumns& typeOrOtherColumns) -> TTypePtrOrOtherColumns {
        switch (fieldDescriptor.label()) {
            case FieldDescriptor::Label::LABEL_REPEATED: {
                Y_ENSURE(fieldOptions.SerializationMode == EProtobufSerializationMode::Yt,
                    "Repeated fields are supported only for YT serialization mode, field \"" + fieldDescriptor.full_name() +
                    "\" has incorrect serialization mode");
                auto* type = std::get_if<NTi::TTypePtr>(&typeOrOtherColumns);
                Y_ENSURE(type, "OTHER_COLUMNS field can not be repeated");
                switch (fieldOptions.ListMode) {
                    case EProtobufListMode::Required:
                        return NTi::TTypePtr(NTi::List(*type));
                    case EProtobufListMode::Optional:
                        return NTi::TTypePtr(NTi::Optional(NTi::List(*type)));
                }
                Y_ABORT();
            }
            case FieldDescriptor::Label::LABEL_OPTIONAL:
                return std::visit(TOverloaded{
                    [] (TOtherColumns) -> TTypePtrOrOtherColumns {
                        return TOtherColumns{};
                    },
                    [] (NTi::TTypePtr type) -> TTypePtrOrOtherColumns {
                        return NTi::TTypePtr(NTi::Optional(std::move(type)));
                    }
                }, typeOrOtherColumns);
            case FieldDescriptor::LABEL_REQUIRED: {
                auto* type = std::get_if<NTi::TTypePtr>(&typeOrOtherColumns);
                Y_ENSURE(type, "OTHER_COLUMNS field can not be required");
                return *type;
            }
        }
        Y_ABORT();
    };

    switch (fieldOptions.SerializationMode) {
        case EProtobufSerializationMode::Protobuf:
            return withFieldLabel(getScalarType());
        case EProtobufSerializationMode::Yt:
            if (fieldDescriptor.type() == FieldDescriptor::TYPE_MESSAGE) {
                if (fieldDescriptor.is_map()) {
                    return GetMapType(fieldDescriptor, fieldOptions);
                } else {
                    return withFieldLabel(GetMessageType(fieldDescriptor, TProtobufFieldOptions{}));
                }
            } else {
                return withFieldLabel(getScalarType());
            }
        case EProtobufSerializationMode::Embedded:
            ythrow yexception() << "EMBEDDED field is not allowed for field "
                << fieldDescriptor.full_name();
    }
    Y_ABORT();
}

TTableSchema TTableSchemaInferrer::InferSchema(const Descriptor& messageDescriptor)
{
    TTableSchema result;

    auto defaultFieldOptions = GetDefaultFieldOptions(&messageDescriptor);
    auto members = GetMessageMembers(
        messageDescriptor.full_name(),
        messageDescriptor,
        defaultFieldOptions,
        // Use special sort order for top level messages.
        /*overrideFieldSortOrder*/ EProtobufFieldSortOrder::AsInProtoFile);

    for (auto& member : members) {
        std::visit(TOverloaded{
            [&] (TOtherColumns) {
                result.Strict(false);
            },
            [&] (NTi::TTypePtr& type) {
                result.AddColumn(TColumnSchema()
                    .Name(std::move(member.Name))
                    .Type(std::move(type))
                );
            },
        }, member.TypeOrOtherColumns);
    }

    return result;
}

TTableSchema CreateTableSchemaImpl(
    const Descriptor& messageDescriptor,
    bool keepFieldsWithoutExtension)
{
    TTableSchemaInferrer inferrer(keepFieldsWithoutExtension);
    return inferrer.InferSchema(messageDescriptor);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail

////////////////////////////////////////////////////////////////////////////////

template <>
void Out<NYT::EWrapperFieldFlag::Enum>(IOutputStream& stream, NYT::EWrapperFieldFlag::Enum value)
{
    stream << NYT::EWrapperFieldFlag_Enum_Name(value);
}

template <>
void Out<NYT::EWrapperMessageFlag::Enum>(IOutputStream& stream, NYT::EWrapperMessageFlag::Enum value)
{
    stream << NYT::EWrapperMessageFlag_Enum_Name(value);
}

template <>
void Out<NYT::EWrapperOneofFlag::Enum>(IOutputStream& stream, NYT::EWrapperOneofFlag::Enum value)
{
    stream << NYT::EWrapperOneofFlag_Enum_Name(value);
}

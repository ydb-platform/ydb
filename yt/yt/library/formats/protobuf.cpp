#include "protobuf.h"

#include "protobuf_options.h"

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/json/json_writer.h>

#include <yt/yt/core/misc/string_builder.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt_proto/yt/formats/extension.pb.h>

#include <google/protobuf/io/tokenizer.h>
#include <google/protobuf/text_format.h>
#include <google/protobuf/wire_format_lite.h>

#include <stack>

namespace NYT::NFormats {

using ::google::protobuf::Descriptor;
using ::google::protobuf::EnumDescriptor;
using ::google::protobuf::OneofDescriptor;
using ::google::protobuf::FieldDescriptor;
using ::google::protobuf::FileDescriptor;
using ::google::protobuf::Message;
using ::google::protobuf::DescriptorPool;
using ::google::protobuf::internal::WireFormatLite;

using namespace NYT::NTableClient;
using namespace NYT::NYTree;

////////////////////////////////////////////////////////////////////////////////

TEnumerationDescription::TEnumerationDescription(const TString& name)
    : Name_(name)
{ }

const TString& TEnumerationDescription::GetEnumerationName() const
{
    return Name_;
}

const TString& TEnumerationDescription::GetValueName(i32 value) const
{
    if (auto valueName = TryGetValueName(value)) {
        return *valueName;
    }
    THROW_ERROR_EXCEPTION("Invalid value for enum")
        << TErrorAttribute("enum_name", GetEnumerationName())
        << TErrorAttribute("value", value);
}

const TString* TEnumerationDescription::TryGetValueName(i32 value) const
{
    auto it = ValueToName_.find(value);
    if (it == ValueToName_.end()) {
        return nullptr;
    }
    return &it->second;
}

i32 TEnumerationDescription::GetValue(TStringBuf valueName) const
{
    if (auto value = TryGetValue(valueName)) {
        return *value;
    }
    THROW_ERROR_EXCEPTION("Invalid value for enum")
        << TErrorAttribute("enum_name", GetEnumerationName())
        << TErrorAttribute("value", valueName);
}

std::optional<i32> TEnumerationDescription::TryGetValue(TStringBuf valueName) const
{
    auto it = NameToValue_.find(valueName);
    if (it == NameToValue_.end()) {
        return std::nullopt;
    }
    return it->second;
}

void TEnumerationDescription::Add(TString name, i32 value)
{
    if (NameToValue_.find(name) != NameToValue_.end()) {
        THROW_ERROR_EXCEPTION("Enumeration %v already has value %v",
            Name_,
            name);
    }
    if (ValueToName_.find(value) != ValueToName_.end()) {
        THROW_ERROR_EXCEPTION("Enumeration %v already has value %v",
            Name_,
            value);
    }
    NameToValue_.emplace(name, value);
    ValueToName_.emplace(value, std::move(name));
}

static IMapNodePtr ConvertToEnumMap(const EnumDescriptor* enumDescriptor)
{
    auto node = BuildYsonNodeFluently().DoMap([&] (TFluentMap fluent) {
        for (int i = 0; i < enumDescriptor->value_count(); ++i) {
            auto valueDescriptor = enumDescriptor->value(i);
            YT_VERIFY(valueDescriptor);
            fluent.Item(valueDescriptor->name()).Value(valueDescriptor->number());
        }
    });

    return node->AsMap();
}

////////////////////////////////////////////////////////////////////////////////

class TParserErrorCollector
    : public ::google::protobuf::io::ErrorCollector
{
public:
    void AddError(
        int line,
        ::google::protobuf::io::ColumnNumber column,
        const TString& message) override
    {
        if (std::ssize(Errors_) < ErrorCountLimit) {
            Errors_.push_back(TError("%v", message)
                << TErrorAttribute("line_number", line)
                << TErrorAttribute("column_number", column));
        }
    }

    std::vector<TError> GetErrors() const
    {
        return Errors_;
    }

private:
    static constexpr int ErrorCountLimit = 100;
    std::vector<TError> Errors_;
};

class TDescriptorPoolErrorCollector
    : public DescriptorPool::ErrorCollector
{
public:
    void AddError(
        const TString& fileName,
        const TString& elementName,
        const Message* /*descriptor*/,
        DescriptorPool::ErrorCollector::ErrorLocation /*location*/,
        const TString& message) override
    {
        if (std::ssize(Errors_) < ErrorCountLimit) {
            Errors_.push_back(TError("%v", message)
                << TErrorAttribute("file_name", fileName)
                << TErrorAttribute("element_name", elementName));
        }
    }

    void ThrowOnError()
    {
        if (!Errors_.empty()) {
            THROW_ERROR_EXCEPTION("Error while building protobuf descriptors")
                << Errors_;
        }
    }

private:
    static constexpr int ErrorCountLimit = 100;
    std::vector<TError> Errors_;
};

////////////////////////////////////////////////////////////////////////////////

static TEnumerationDescription CreateEnumerationMap(
    const TString& enumName,
    const IMapNodePtr& enumerationConfig)
{
    TEnumerationDescription result(enumName);
    for (const auto& enumValue : enumerationConfig->GetChildren()) {
        const auto& name = enumValue.first;
        const auto& valueNode = enumValue.second;
        switch (valueNode->GetType()) {
            case ENodeType::Uint64:
                // TODO(babenko): migrate to std::string
                result.Add(TString(name), valueNode->GetValue<ui64>());
                break;
            case ENodeType::Int64:
                // TODO(babenko): migrate to std::string
                result.Add(TString(name), valueNode->GetValue<i64>());
                break;
            default:
                THROW_ERROR_EXCEPTION("Invalid specification of %Qv enumeration: expected type \"int64\" or \"uint64\", actual type %Qlv",
                    enumName,
                    valueNode->GetType());
        }
    }
    return result;
}

static std::optional<WireFormatLite::FieldType> ConvertFromInternalProtobufType(EProtobufType type)
{
    auto fieldType = [=] () -> std::optional<::google::protobuf::FieldDescriptor::Type>{
        using namespace ::google::protobuf;
        switch (type) {
            case EProtobufType::Double:
                return FieldDescriptor::TYPE_DOUBLE;
            case EProtobufType::Float:
                return FieldDescriptor::TYPE_FLOAT;

            case EProtobufType::Int64:
                return FieldDescriptor::TYPE_INT64;
            case EProtobufType::Uint64:
                return FieldDescriptor::TYPE_UINT64;
            case EProtobufType::Sint64:
                return FieldDescriptor::TYPE_SINT64;
            case EProtobufType::Fixed64:
                return FieldDescriptor::TYPE_FIXED64;
            case EProtobufType::Sfixed64:
                return FieldDescriptor::TYPE_SFIXED64;

            case EProtobufType::Int32:
                return FieldDescriptor::TYPE_INT32;
            case EProtobufType::Uint32:
                return FieldDescriptor::TYPE_UINT32;
            case EProtobufType::Sint32:
                return FieldDescriptor::TYPE_SINT32;
            case EProtobufType::Fixed32:
                return FieldDescriptor::TYPE_FIXED32;
            case EProtobufType::Sfixed32:
                return FieldDescriptor::TYPE_SFIXED32;

            case EProtobufType::Bool:
                return FieldDescriptor::TYPE_BOOL;
            case EProtobufType::String:
                return FieldDescriptor::TYPE_STRING;
            case EProtobufType::Bytes:
                return FieldDescriptor::TYPE_BYTES;

            case EProtobufType::EnumInt:
            case EProtobufType::EnumString:
                return FieldDescriptor::TYPE_ENUM;

            case EProtobufType::Message:
            case EProtobufType::StructuredMessage:
            case EProtobufType::EmbeddedMessage:
                return FieldDescriptor::TYPE_MESSAGE;

            case EProtobufType::Any:
            case EProtobufType::OtherColumns:
                return FieldDescriptor::TYPE_BYTES;

            case EProtobufType::Oneof:
                return std::nullopt;
        }
        YT_ABORT();
    }();
    if (fieldType) {
        return static_cast<WireFormatLite::FieldType>(*fieldType);
    }
    return std::nullopt;
}

static std::optional<EProtobufType> ConvertToInternalProtobufType(
    ::google::protobuf::FieldDescriptor::Type type,
    bool enumAsString)
{
    using namespace ::google::protobuf;
    switch (type) {
        case FieldDescriptor::TYPE_DOUBLE:
            return EProtobufType::Double;
        case FieldDescriptor::TYPE_FLOAT:
            return EProtobufType::Float;
        case FieldDescriptor::TYPE_INT64:
            return EProtobufType::Int64;
        case FieldDescriptor::TYPE_UINT64:
            return EProtobufType::Uint64;
        case FieldDescriptor::TYPE_INT32:
            return EProtobufType::Int32;
        case FieldDescriptor::TYPE_FIXED64:
            return EProtobufType::Fixed64;
        case FieldDescriptor::TYPE_FIXED32:
            return EProtobufType::Fixed32;
        case FieldDescriptor::TYPE_BOOL:
            return EProtobufType::Bool;
        case FieldDescriptor::TYPE_STRING:
            return EProtobufType::String;
        case FieldDescriptor::TYPE_GROUP:
            return std::nullopt;
        case FieldDescriptor::TYPE_MESSAGE:
            return EProtobufType::Message;
        case FieldDescriptor::TYPE_BYTES:
            return EProtobufType::Bytes;
        case FieldDescriptor::TYPE_UINT32:
            return EProtobufType::Uint32;
        case FieldDescriptor::TYPE_ENUM:
            return enumAsString ? EProtobufType::EnumString : EProtobufType::EnumInt;
        case FieldDescriptor::TYPE_SFIXED32:
            return EProtobufType::Sfixed32;
        case FieldDescriptor::TYPE_SFIXED64:
            return EProtobufType::Sfixed64;
        case FieldDescriptor::TYPE_SINT32:
            return EProtobufType::Sint32;
        case FieldDescriptor::TYPE_SINT64:
            return EProtobufType::Sint64;
    }
    // TODO(levysotsky): Throw exception here.
    return std::nullopt;
}

////////////////////////////////////////////////////////////////////////////////

ui32 TProtobufTag::GetFieldNumber() const
{
    return WireFormatLite::GetTagFieldNumber(WireTag);
}

////////////////////////////////////////////////////////////////////////////////

namespace {

DEFINE_ENUM(EIntegerSignednessKind,
    (SignedInteger)
    (UnsignedInteger)
    (Other)
);

} // namespace

void ValidateSimpleType(
    EProtobufType protobufType,
    NTableClient::ESimpleLogicalValueType logicalType)
{
    if (logicalType == ESimpleLogicalValueType::Any) {
        return;
    }

    using EKind = EIntegerSignednessKind;

    auto getLogicalTypeKind = [] (ESimpleLogicalValueType type) {
        switch (type) {
            case ESimpleLogicalValueType::Int8:
            case ESimpleLogicalValueType::Int16:
            case ESimpleLogicalValueType::Int32:
            case ESimpleLogicalValueType::Int64:
                return EKind::SignedInteger;
            case ESimpleLogicalValueType::Uint8:
            case ESimpleLogicalValueType::Uint16:
            case ESimpleLogicalValueType::Uint32:
            case ESimpleLogicalValueType::Uint64:
                return EKind::UnsignedInteger;
            default:
                return EKind::Other;
        }
    };

    auto getProtobufTypeKind = [] (EProtobufType type) {
        switch (type) {
            case EProtobufType::Fixed64:
            case EProtobufType::Uint64:
            case EProtobufType::Uint32:
            case EProtobufType::Fixed32:
                return EKind::UnsignedInteger;

            case EProtobufType::Int64:
            case EProtobufType::Sint64:
            case EProtobufType::Sfixed64:
            case EProtobufType::Sfixed32:
            case EProtobufType::Sint32:
            case EProtobufType::Int32:
            case EProtobufType::EnumInt:
                return EKind::SignedInteger;

            default:
                return EKind::Other;
        }
    };

    auto throwMismatchError = [&] (TStringBuf message) {
        THROW_ERROR_EXCEPTION("Simple logical type %Qlv and protobuf type %Qlv mismatch: %v",
            logicalType,
            protobufType,
            message);
    };

    auto validateLogicalType = [&] (auto... expectedTypes) {
        if ((... && (logicalType != expectedTypes))) {
            auto typeNameList = std::vector<TString>{FormatEnum(expectedTypes)...};
            throwMismatchError(Format("expected logical type to be one of %v", typeNameList));
        }
    };

    switch (protobufType) {
        case EProtobufType::String:
            validateLogicalType(ESimpleLogicalValueType::String, ESimpleLogicalValueType::Utf8);
            return;

        case EProtobufType::Bytes:
        case EProtobufType::Message:
            validateLogicalType(ESimpleLogicalValueType::String);
            return;

        case EProtobufType::Fixed64:
        case EProtobufType::Uint64:
        case EProtobufType::Uint32:
        case EProtobufType::Fixed32:
        case EProtobufType::Int64:
        case EProtobufType::Sint64:
        case EProtobufType::Sfixed64:
        case EProtobufType::Sfixed32:
        case EProtobufType::Sint32:
        case EProtobufType::Int32: {
            auto logicalTypeKind = getLogicalTypeKind(logicalType);
            if (logicalTypeKind == EIntegerSignednessKind::Other) {
                throwMismatchError("integer protobuf type can match only integer type in schema");
            }
            if (logicalTypeKind != getProtobufTypeKind(protobufType)) {
                throwMismatchError("signedness of both types must be the same");
            }
            return;
        }

        case EProtobufType::Float:
            validateLogicalType(ESimpleLogicalValueType::Float, ESimpleLogicalValueType::Double);
            return;

        case EProtobufType::Double:
            validateLogicalType(ESimpleLogicalValueType::Double);
            return;

        case EProtobufType::Bool:
            validateLogicalType(ESimpleLogicalValueType::Boolean);
            return;

        case EProtobufType::EnumInt:
            if (getLogicalTypeKind(logicalType) != EKind::SignedInteger) {
                throwMismatchError("logical type must be signed integer type");
            }
            return;
        case EProtobufType::EnumString:
            if (logicalType != ESimpleLogicalValueType::String &&
                getLogicalTypeKind(logicalType) != EKind::SignedInteger)
            {
                throwMismatchError("logical type must be either signed integer type or string");
            }
            return;

        case EProtobufType::Any:
            // No check here: every type would be OK.
            return;

        case EProtobufType::StructuredMessage:
        case EProtobufType::EmbeddedMessage:
        case EProtobufType::OtherColumns:
        case EProtobufType::Oneof:
            throwMismatchError("protobuf type cannot match any simple type");
    }
    YT_ABORT();
}

static bool CanBePacked(EProtobufType type)
{
    auto wireFieldType = ConvertFromInternalProtobufType(type);
    if (!wireFieldType) {
        return false;
    }
    auto wireType = WireFormatLite::WireTypeForFieldType(*wireFieldType);
    return
        wireType == WireFormatLite::WireType::WIRETYPE_FIXED32 ||
        wireType == WireFormatLite::WireType::WIRETYPE_FIXED64 ||
        wireType == WireFormatLite::WireType::WIRETYPE_VARINT;
}

[[noreturn]] void ThrowSchemaMismatch(
    TStringBuf message,
    const TComplexTypeFieldDescriptor& descriptor,
    const TProtobufTypeConfigPtr& protoTypeConfig)
{
    THROW_ERROR_EXCEPTION("Table schema and protobuf format config mismatch at %v: %v",
        descriptor.GetDescription(),
        message)
        << TErrorAttribute("type_in_schema", ToString(*descriptor.GetType()))
        << TErrorAttribute("protobuf_type", protoTypeConfig);
}

////////////////////////////////////////////////////////////////////////////////

template <typename TType>
void TProtobufFormatDescriptionBase<TType>::DoInit(
    const TProtobufFormatConfigPtr& config,
    const std::vector<NTableClient::TTableSchemaPtr>& schemas)
{
    const bool tablesSpecified = !config->Tables.empty();
    const bool fileDescriptorSetSpecified = !config->FileDescriptorSet.empty();
    const bool fileDescriptorSetStructuredSpecified = config->FileDescriptorSetText.has_value();
    if (tablesSpecified + fileDescriptorSetSpecified + fileDescriptorSetStructuredSpecified != 1) {
        THROW_ERROR_EXCEPTION(R"(Exactly one of "tables", "file_descriptor_set" and "file_descriptor_set_text" )"
            "must be specified in protobuf format");
    }
    if (tablesSpecified) {
        InitFromProtobufSchema(config, schemas);
    } else if (fileDescriptorSetSpecified) {
        InitFromFileDescriptorsLegacy(config);
    } else {
        YT_VERIFY(fileDescriptorSetStructuredSpecified);
        InitFromFileDescriptors(config, schemas);
    }
}

static std::vector<const FileDescriptor*> BuildDescriptorPool(
    const ::google::protobuf::FileDescriptorSet& fileDescriptorSet,
    DescriptorPool* descriptorPool)
{
    std::vector<const FileDescriptor*> fileDescriptors;
    TDescriptorPoolErrorCollector errorCollector;

    for (const auto& fileDescriptorProto : fileDescriptorSet.file()) {
        auto* file = descriptorPool->BuildFileCollectingErrors(
            fileDescriptorProto,
            &errorCollector);
        errorCollector.ThrowOnError();
        if (!file) {
            THROW_ERROR_EXCEPTION("Unknown error building file %v",
                fileDescriptorProto.name());
        }

        fileDescriptors.push_back(file);
    }

    return fileDescriptors;
}

static EProtobufType SpecialToProtobufType(ESpecialProtobufType specialType)
{
    switch (specialType) {
        case ESpecialProtobufType::Any:
            return EProtobufType::Any;
        case ESpecialProtobufType::EnumInt:
            return EProtobufType::EnumInt;
        case ESpecialProtobufType::EnumString:
            return EProtobufType::EnumString;
        case ESpecialProtobufType::OtherColumns:
            return EProtobufType::OtherColumns;
    }
    Y_ABORT();
}

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
            YT_VERIFY(!Stack_.empty());
            THROW_ERROR_EXCEPTION("Cyclic reference found for protobuf messages. "
                "Consider removing %Qv flag somewhere on the cycle containing %Qv and %Qv",
                EWrapperFieldFlag_Enum_Name(EWrapperFieldFlag::SERIALIZATION_YT),
                Stack_.top()->full_name(),
                descriptor->full_name());
        }
        return TGuard(this, descriptor);
    }

private:
    THashSet<const Descriptor*> ActiveVertices_;
    std::stack<const Descriptor*> Stack_;
};

class TProtobufTypeConfigBuilder
{
public:
    TProtobufTypeConfigBuilder(bool enumsAsStrings)
        : EnumsAsStrings_(enumsAsStrings)
        , Enumerations_(GetEphemeralNodeFactory()->CreateMap())
    { }

    TProtobufTypeConfigPtr GetOrCreateTypeConfig(const Descriptor* descriptor)
    {
        if (auto it = DescriptorToTypeConfig_.find(descriptor); it != DescriptorToTypeConfig_.end()) {
            return it->second;
        }
        auto typeConfig = CreateTypeConfig(descriptor, GetDefaultFieldOptions(descriptor));
        DescriptorToTypeConfig_.emplace(descriptor, typeConfig);
        return typeConfig;
    }

    IMapNodePtr GetEnumerations() const
    {
        return Enumerations_;
    }

private:
    const bool EnumsAsStrings_;
    const IMapNodePtr Enumerations_;
    THashMap<const Descriptor*, TProtobufTypeConfigPtr> DescriptorToTypeConfig_;
    TCycleChecker CycleChecker_;

private:
    TProtobufTypeConfigPtr CreateMapConfig(
        const FieldDescriptor* fieldDescriptor,
        const TProtobufFieldOptions& fieldOptions)
    {
        YT_VERIFY(fieldDescriptor->is_map());
        const auto* descriptor = fieldDescriptor->message_type();
        switch (fieldOptions.MapMode) {
            case EProtobufMapMode::ListOfStructsLegacy:
                return GetOrCreateTypeConfig(descriptor);
            case EProtobufMapMode::ListOfStructs:
            case EProtobufMapMode::Dict:
            case EProtobufMapMode::OptionalDict:
                return CreateTypeConfig(
                    descriptor,
                    TProtobufFieldOptions{.SerializationMode = EProtobufSerializationMode::Yt});
        }
        Y_ABORT();
    }

    TProtobufTypeConfigPtr CreateFieldTypeConfig(
        const FieldDescriptor* fieldDescriptor,
        const TProtobufFieldOptions& fieldOptions)
    {
        if (fieldDescriptor->type() == FieldDescriptor::TYPE_MESSAGE) {
            switch (fieldOptions.SerializationMode) {
                case EProtobufSerializationMode::Yt:
                    if (fieldDescriptor->is_map()) {
                        return CreateMapConfig(fieldDescriptor, fieldOptions);
                    } else {
                        return GetOrCreateTypeConfig(fieldDescriptor->message_type());
                    }
                case EProtobufSerializationMode::Embedded: {
                    auto config = GetOrCreateTypeConfig(fieldDescriptor->message_type());
                    config->ProtoType = EProtobufType::EmbeddedMessage;
                    return config;
                }
                case EProtobufSerializationMode::Protobuf:
                    break;
                    // EProtobufSerializationMode::Protobuf handled later
            }
        }

        auto typeConfig = New<TProtobufTypeConfig>();

        EProtobufType type;
        if (fieldOptions.Type) {
            ValidateProtobufType(fieldDescriptor, *fieldOptions.Type);
            type = SpecialToProtobufType(*fieldOptions.Type);
        } else {
            auto optionalType = ConvertToInternalProtobufType(fieldDescriptor->type(), EnumsAsStrings_);
            if (!optionalType) {
                return nullptr;
            }
            type = *optionalType;
        }

        typeConfig->ProtoType = type;

        if (type == EProtobufType::EnumString || type == EProtobufType::EnumInt) {
            auto enumDescriptor = fieldDescriptor->enum_type();
            YT_VERIFY(enumDescriptor);
            const auto& enumName = enumDescriptor->full_name();
            auto child = Enumerations_->FindChild(enumName);
            if (!child) {
                Enumerations_->AddChild(enumName, ConvertToEnumMap(enumDescriptor));
            }
            typeConfig->EnumerationName = enumName;
        }

        return typeConfig;
    }

    TProtobufColumnConfigPtr CreateFieldConfig(
        const FieldDescriptor* fieldDescriptor,
        const TProtobufFieldOptions& defaultOptions)
    {
        auto fieldOptions = GetFieldOptions(fieldDescriptor, defaultOptions);
        auto typeConfig = CreateFieldTypeConfig(fieldDescriptor, fieldOptions);
        if (!typeConfig) {
            return nullptr;
        }

        auto fieldConfig = New<TProtobufColumnConfig>();
        fieldConfig->Name = GetColumnName(fieldDescriptor);
        fieldConfig->FieldNumber = fieldDescriptor->number();
        if (fieldDescriptor->is_repeated() && fieldOptions.SerializationMode != EProtobufSerializationMode::Yt) {
            THROW_ERROR_EXCEPTION("Repeated field %Qv must have flag %Qv",
                fieldDescriptor->full_name(),
                EWrapperFieldFlag_Enum_Name(EWrapperFieldFlag::SERIALIZATION_YT));
        }
        fieldConfig->Repeated = fieldDescriptor->is_repeated();
        fieldConfig->Packed = fieldDescriptor->is_packed();
        fieldConfig->Type = std::move(typeConfig);
        fieldConfig->EnumWritingMode = fieldOptions.EnumWritingMode;
        fieldConfig->Postprocess();
        return fieldConfig;
    }

    std::vector<TProtobufColumnConfigPtr> ProcessOneof(
        const OneofDescriptor* oneofDescriptor,
        const TProtobufFieldOptions& defaultFieldOptions,
        const TProtobufOneofOptions& defaultOneofOptions)
    {
        std::vector<TProtobufColumnConfigPtr> fieldConfigs;
        for (int i = 0; i < oneofDescriptor->field_count(); ++i) {
            auto fieldConfig = CreateFieldConfig(oneofDescriptor->field(i), defaultFieldOptions);
            if (!fieldConfig) {
                continue;
            }
            fieldConfigs.push_back(std::move(fieldConfig));
        }
        if (fieldConfigs.empty()) {
            THROW_ERROR_EXCEPTION("Parsing of oneof field %Qv resulted in zero variants",
                oneofDescriptor->full_name());
        }

        auto oneofOptions = GetOneofOptions(oneofDescriptor, defaultOneofOptions);
        switch (oneofOptions.Mode) {
            case EProtobufOneofMode::SeparateFields:
                return fieldConfigs;
            case EProtobufOneofMode::Variant: {
                auto field = New<TProtobufColumnConfig>();
                field->Name = oneofOptions.VariantFieldName;
                field->Type = New<TProtobufTypeConfig>();
                field->Type->ProtoType = EProtobufType::Oneof;
                field->Type->Fields = std::move(fieldConfigs);
                return {field};
            }
        }
        Y_ABORT();
    }

    // NB: This function does not caches the created config!
    TProtobufTypeConfigPtr CreateTypeConfig(
        const Descriptor* descriptor,
        const TProtobufFieldOptions& defaultFieldOptions)
    {
        auto typeConfig = New<TProtobufTypeConfig>();
        typeConfig->ProtoType = EProtobufType::StructuredMessage;
        auto& fields = typeConfig->Fields;

        auto defaultOneofOptions = GetDefaultOneofOptions(descriptor);

        THashSet<const OneofDescriptor*> visitedOneofs;
        auto guard = CycleChecker_.Enter(descriptor);
        for (int fieldIndex = 0; fieldIndex < descriptor->field_count(); ++fieldIndex) {
            auto fieldDescriptor = descriptor->field(fieldIndex);
            auto oneofDescriptor = fieldDescriptor->containing_oneof();
            if (!oneofDescriptor) {
                auto fieldConfig = CreateFieldConfig(fieldDescriptor, defaultFieldOptions);
                if (!fieldConfig) {
                    continue;
                }
                fields.push_back(std::move(fieldConfig));
            } else if (!visitedOneofs.contains(oneofDescriptor)) {
                auto addedFields = ProcessOneof(oneofDescriptor, defaultFieldOptions, defaultOneofOptions);
                fields.insert(fields.end(), addedFields.begin(), addedFields.end());
                visitedOneofs.insert(oneofDescriptor);
            }
        }

        return typeConfig;
    }
};

static TProtobufFormatConfigPtr CreateConfigWithTypes(
    std::vector<const Descriptor*> descriptors,
    bool enumsAsStrings = true)
{
    auto config = New<TProtobufFormatConfig>();
    TProtobufTypeConfigBuilder builder(enumsAsStrings);
    for (const auto* descriptor : descriptors) {
        auto typeConfig = builder.GetOrCreateTypeConfig(descriptor);
        auto tableConfig = New<TProtobufTableConfig>();
        for (const auto& field : typeConfig->Fields) {
            tableConfig->Columns.push_back(field);
        }
        config->Tables.push_back(std::move(tableConfig));
    }
    config->Enumerations = builder.GetEnumerations();
    return config;
}

template <typename TType>
void TProtobufFormatDescriptionBase<TType>::InitFromFileDescriptorsLegacy(
    const TProtobufFormatConfigPtr& config)
{
    if (config->FileIndices.empty()) {
        THROW_ERROR_EXCEPTION(R"("file_indices" attribute must be non empty in protobuf format)");
    }
    if (config->MessageIndices.size() != config->FileIndices.size()) {
        THROW_ERROR_EXCEPTION(R"("message_indices" and "file_indices" must be of same size in protobuf format)");
    }

    ::google::protobuf::FileDescriptorSet fileDescriptorSet;
    if (!fileDescriptorSet.ParseFromString(config->FileDescriptorSet)) {
        THROW_ERROR_EXCEPTION(R"(Error parsing "file_descriptor_set" in protobuf config)");
    }

    DescriptorPool descriptorPool;
    auto fileDescriptors = BuildDescriptorPool(fileDescriptorSet, &descriptorPool);

    std::vector<const Descriptor*> messageDescriptors;
    for (size_t i = 0; i < config->FileIndices.size(); ++i) {
        if (config->FileIndices[i] >= static_cast<int>(fileDescriptors.size())) {
            THROW_ERROR_EXCEPTION("File index is out of bound")
                << TErrorAttribute("file_index", config->FileIndices[i])
                << TErrorAttribute("file_count", fileDescriptors.size());
        }
        auto* fileDescriptor = fileDescriptors[config->FileIndices[i]];

        if (config->MessageIndices[i] >= fileDescriptor->message_type_count()) {
            THROW_ERROR_EXCEPTION("Message index is out of bound")
                << TErrorAttribute("message_index", config->MessageIndices[i])
                << TErrorAttribute("message_count", fileDescriptor->message_type_count());
        }
        messageDescriptors.push_back(fileDescriptor->message_type(config->MessageIndices[i]));
    }

    auto configWithTypes = CreateConfigWithTypes(messageDescriptors, config->EnumsAsStrings);
    std::vector<TTableSchemaPtr> schemas(messageDescriptors.size(), New<NTableClient::TTableSchema>());
    InitFromProtobufSchema(configWithTypes, schemas);
}

template <typename TType>
void TProtobufFormatDescriptionBase<TType>::InitFromFileDescriptors(
    const TProtobufFormatConfigPtr& config,
    const std::vector<NTableClient::TTableSchemaPtr>& schemas)
{
    if (config->TypeNames.empty()) {
        THROW_ERROR_EXCEPTION(R"("type_names" attribute must be non empty if "file_descriptor_set_text" )"
            "is specified");
    }
    YT_VERIFY(config->FileDescriptorSetText);

    ::google::protobuf::FileDescriptorSet fileDescriptorSet;
    ::google::protobuf::TextFormat::Parser parser;
    // parser.AllowUnknownExtension(true);
    TParserErrorCollector errorCollector;
    parser.RecordErrorsTo(&errorCollector);
    if (!parser.ParseFromString(*config->FileDescriptorSetText, &fileDescriptorSet)) {
        THROW_ERROR_EXCEPTION(R"(Error parsing "file_descriptor_set_text" in protobuf config)")
            << errorCollector.GetErrors();
    }

    DescriptorPool descriptorPool;
    auto fileDescriptors = BuildDescriptorPool(fileDescriptorSet, &descriptorPool);

    std::vector<const Descriptor*> messageDescriptors;
    messageDescriptors.reserve(config->TypeNames.size());
    for (const auto& typeName : config->TypeNames) {
        auto descriptor = descriptorPool.FindMessageTypeByName(typeName);
        if (!descriptor) {
            THROW_ERROR_EXCEPTION("Message type %Qv not found in file descriptor set",
                typeName);
        }
        messageDescriptors.push_back(descriptor);
    }

    auto configWithTypes = CreateConfigWithTypes(messageDescriptors);
    InitFromProtobufSchema(configWithTypes, schemas);
}

template<>
void TProtobufFormatDescriptionBase<TProtobufWriterType>::InitEmbeddedColumn(
    int& fieldIndex,
    const NTableClient::TTableSchemaPtr& tableSchema,
    TProtobufTypeBuilder<TProtobufWriterType>& typeBuilder,
    TTypePtr tableType,
    TProtobufColumnConfigPtr columnConfig,
    TTypePtr parent,
    int parentEmbeddingIndex)
{
    auto embeddingIndex = tableType->AddEmbedding(parentEmbeddingIndex, columnConfig);

    for (auto& fieldConfig: columnConfig->Type->Fields) {
        InitColumn(fieldIndex, tableSchema, typeBuilder, tableType, fieldConfig, parent, embeddingIndex);
    }
}

template<>
void TProtobufFormatDescriptionBase<TProtobufParserType>::InitEmbeddedColumn(
    int& fieldIndex,
    const NTableClient::TTableSchemaPtr& tableSchema,
    TProtobufTypeBuilder<TProtobufParserType>& typeBuilder,
    TTypePtr tableType,
    TProtobufColumnConfigPtr columnConfig,
    TTypePtr parent,
    int parentEmbeddingIndex)
{
    auto child = typeBuilder.CreateField(
            fieldIndex,
            columnConfig,
            std::nullopt,
            /* allowOtherColumns */ true,
            true);

    auto childPtr = child.get();

    parent->AddChild(
            std::nullopt,
            std::move(child), //KMP
            fieldIndex);

    for (auto& fieldConfig: columnConfig->Type->Fields) {
        InitColumn(fieldIndex, tableSchema, typeBuilder, tableType, fieldConfig, childPtr->Type, parentEmbeddingIndex);
    }
}

template<typename TType>
void TProtobufFormatDescriptionBase<TType>::InitColumn(
    int& fieldIndex,
    const NTableClient::TTableSchemaPtr& tableSchema,
    TProtobufTypeBuilder<TType>& typeBuilder,
    TTypePtr tableType,
    TProtobufColumnConfigPtr columnConfig,
    TTypePtr parent, // not used for Writer
    int parentEmbeddingIndex) // not used for Parser
{
    if (columnConfig->Type->ProtoType == EProtobufType::EmbeddedMessage) {
        if (columnConfig->Repeated) {
            THROW_ERROR_EXCEPTION("Protobuf field %Qv of type %Qlv can not be repeated",
            columnConfig->Name,
            EProtobufType::EmbeddedMessage);
        }

        InitEmbeddedColumn(fieldIndex, tableSchema, typeBuilder, tableType, columnConfig, parent, parentEmbeddingIndex);
        return;
    }

    auto columnSchema = tableSchema->FindColumn(columnConfig->Name);
    TLogicalTypePtr logicalType = columnSchema ? columnSchema->LogicalType() : nullptr;
    if (columnConfig->ProtoType == EProtobufType::OtherColumns) {
        if (columnConfig->Repeated) {
            THROW_ERROR_EXCEPTION("Protobuf field %Qv of type %Qlv can not be repeated",
                columnConfig->Name,
                EProtobufType::OtherColumns);
        }
        if (logicalType) {
            THROW_ERROR_EXCEPTION("Protobuf field %Qv of type %Qlv should not match actual column in schema",
                columnConfig->Name,
                EProtobufType::OtherColumns);
        }
    }

    std::optional<TComplexTypeFieldDescriptor> maybeDescriptor;
    if (logicalType) {
        YT_VERIFY(columnSchema);
        maybeDescriptor = TComplexTypeFieldDescriptor(*columnSchema);
    }

    bool needSchema = columnConfig->Repeated
        || columnConfig->ProtoType == EProtobufType::StructuredMessage
        || columnConfig->ProtoType == EProtobufType::Oneof;
    if (!logicalType && needSchema) {
        if (columnConfig->FieldNumber) {
            // Ignore missing column to facilitate schema evolution.
            tableType->IgnoreChild(maybeDescriptor, *columnConfig->FieldNumber);
            return;
        }
        THROW_ERROR_EXCEPTION(
            "Field %Qv of type %Qlv requires a corresponding schematized column",
            columnConfig->Name,
            columnConfig->ProtoType);
    }

    parent->AddChild(
        maybeDescriptor,
        typeBuilder.CreateField(
            fieldIndex,
            columnConfig,
            maybeDescriptor,
            true),
        fieldIndex,
        parentEmbeddingIndex);
    ++fieldIndex;
}

template <typename TType>
void TProtobufFormatDescriptionBase<TType>::InitFromProtobufSchema(
    const TProtobufFormatConfigPtr& config,
    const std::vector<NTableClient::TTableSchemaPtr>& schemas)
{
    if (config->Enumerations) {
        const auto& enumerationConfigMap = config->Enumerations;
        for (const auto& [name_, field] : enumerationConfigMap->GetChildren()) {
            // TODO(babenko): migrate to std::string
            auto name = TString(name_);
            if (field->GetType() != ENodeType::Map) {
                THROW_ERROR_EXCEPTION(R"(Invalid enumeration specification type: expected "map", found %Qlv)",
                    field->GetType());
            }
            const auto& enumerationConfig = field->AsMap();
            EnumerationDescriptionMap_.emplace(name, CreateEnumerationMap(TimestampColumnName, enumerationConfig));
        }
    }

    const auto& tableConfigs = config->Tables;
    if (tableConfigs.size() < schemas.size()) {
        THROW_ERROR_EXCEPTION("Number of schemas is greater than number of tables in protobuf config: %v > %v",
            schemas.size(),
            tableConfigs.size());
    }

    auto typeBuilder = TProtobufTypeBuilder<TType>(EnumerationDescriptionMap_);

    for (size_t tableIndex = 0; tableIndex != schemas.size(); ++tableIndex) {
        auto tableType = New<TType>();
        tableType->ProtoType = EProtobufType::StructuredMessage;

        int fieldIndex = 0;
        const auto& tableConfig = tableConfigs[tableIndex];
        const auto& tableSchema = schemas[tableIndex];

        for (const auto& columnConfig : tableConfig->Columns) {
            InitColumn(fieldIndex, tableSchema, typeBuilder, tableType, columnConfig, tableType, -1);
        }

        AddTable(std::move(tableType));
    }
}

////////////////////////////////////////////////////////////////////////////////

template <typename TType>
TProtobufTypeBuilder<TType>::TProtobufTypeBuilder(const THashMap<TString, TEnumerationDescription>& enumerations)
    : Enumerations_(enumerations)
{ }

template <typename TType>
typename TProtobufTypeBuilder<TType>::TFieldPtr TProtobufTypeBuilder<TType>::CreateField(
    int structFieldIndex,
    const TProtobufColumnConfigPtr& columnConfig,
    std::optional<TComplexTypeFieldDescriptor> maybeDescriptor,
    bool allowOtherColumns,
    bool allowEmbedded)
{
    const auto& typeConfig = columnConfig->Type;
    if (!allowEmbedded && typeConfig->ProtoType == EProtobufType::EmbeddedMessage) {
        THROW_ERROR_EXCEPTION("embedded_message inside of structured_message is not allowed");
    }

    if (!allowOtherColumns && columnConfig->ProtoType == EProtobufType::OtherColumns) {
        YT_VERIFY(maybeDescriptor);
        ThrowSchemaMismatch(
            "protobuf field of type \"other_columns\" is not allowed inside complex types",
            *maybeDescriptor,
            typeConfig);
    }

    if (columnConfig->Packed && !CanBePacked(typeConfig->ProtoType)) {
        YT_VERIFY(maybeDescriptor);
        ThrowSchemaMismatch(
            Format("packed protobuf field must have primitive numeric type, got %Qlv", typeConfig->ProtoType),
            *maybeDescriptor,
            typeConfig);
    }

    bool optional;
    if (maybeDescriptor) {
        if (maybeDescriptor->GetType()->GetMetatype() == ELogicalMetatype::Optional) {
            maybeDescriptor = maybeDescriptor->OptionalElement();
            optional = true;
        } else {
            optional = false;
        }
    } else {
        optional = true;
    }

    if (columnConfig->Repeated) {
        YT_VERIFY(maybeDescriptor);
        if (maybeDescriptor->GetType()->GetMetatype() == ELogicalMetatype::Dict) {
            // Do nothing, this case will be processed in the following switch.
        } else if (maybeDescriptor->GetType()->GetMetatype() == ELogicalMetatype::List) {
            maybeDescriptor = maybeDescriptor->ListElement();
        } else {
            ThrowSchemaMismatch(
                Format("repeated field must correspond to list or dict, got %Qlv", maybeDescriptor->GetType()->GetMetatype()),
                *maybeDescriptor,
                typeConfig);
        }
    }

    auto field = std::make_unique<TField>();

    field->Name = columnConfig->Name;
    field->Repeated = columnConfig->Repeated;
    field->Packed = columnConfig->Packed;
    field->StructFieldIndex = structFieldIndex;
    field->Type = FindOrCreateType(
        typeConfig,
        maybeDescriptor,
        optional,
        columnConfig->Repeated);

    if constexpr (IsWriter) {
        field->EnumWritingMode = columnConfig->EnumWritingMode;
    }

    if (auto wireFieldType = ConvertFromInternalProtobufType(field->Type->ProtoType)) {
        InitTag(*field, *wireFieldType, columnConfig);
    }

    return field;
}

template <typename TType>
typename TProtobufTypeBuilder<TType>::TTypePtr TProtobufTypeBuilder<TType>::FindOrCreateType(
    const TProtobufTypeConfigPtr& typeConfig,
    std::optional<TComplexTypeFieldDescriptor> maybeDescriptor,
    bool optional,
    bool repeated)
{
    // XXX(levysotsky): Currently we always create new types.
    // We may want to deduplicate them too.
    auto type = New<TType>();

    type->ProtoType = typeConfig->ProtoType;
    type->Optional = optional;

    if (typeConfig->ProtoType == EProtobufType::EnumString && !typeConfig->EnumerationName) {
        THROW_ERROR_EXCEPTION("Invalid format config: missing \"enumeration_name\" for %Qlv type",
            EProtobufType::EnumString);
    }
    if (typeConfig->EnumerationName) {
        auto it = Enumerations_.find(*typeConfig->EnumerationName);
        if (it == Enumerations_.end()) {
            THROW_ERROR_EXCEPTION("Invalid format config: cannot find enumeration with name %Qv",
                *typeConfig->EnumerationName);
        }
        type->EnumerationDescription = &it->second;
    }

    if (!maybeDescriptor) {
        return type;
    }

    auto descriptor = std::move(*maybeDescriptor);

    const auto& logicalType = descriptor.GetType();
    YT_VERIFY(logicalType);

    // list<optional<any>> is allowed.
    if (repeated &&
        typeConfig->ProtoType == EProtobufType::Any &&
        logicalType->GetMetatype() == ELogicalMetatype::Optional &&
        logicalType->GetElement()->GetMetatype() == ELogicalMetatype::Simple &&
        logicalType->GetElement()->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::Any)
    {
        return type;
    }

    switch (logicalType->GetMetatype()) {
        case ELogicalMetatype::Struct:
            if (typeConfig->ProtoType != EProtobufType::StructuredMessage) {
                ThrowSchemaMismatch(
                    Format("expected \"structured_message\" protobuf type, got %Qlv", typeConfig->ProtoType),
                    descriptor,
                    typeConfig);
            }
            VisitStruct(type, typeConfig, descriptor);
            return type;
        case ELogicalMetatype::VariantStruct:
            if (typeConfig->ProtoType != EProtobufType::Oneof) {
                ThrowSchemaMismatch(
                    Format("expected \"oneof\" protobuf type, got %Qlv", typeConfig->ProtoType),
                    descriptor,
                    typeConfig);
            }
            VisitStruct(type, typeConfig, descriptor);
            return type;
        case ELogicalMetatype::Dict:
            YT_VERIFY(repeated);
            VisitDict(type, typeConfig, descriptor);
            return type;
        case ELogicalMetatype::Simple:
            ValidateSimpleType(type->ProtoType, logicalType->AsSimpleTypeRef().GetElement());
            return type;
        default:
            ThrowSchemaMismatch(
                Format("unexpected logical metatype %Qlv", logicalType->GetMetatype()),
                descriptor,
                typeConfig);
    }
    YT_ABORT();
}

template <typename TType>
void TProtobufTypeBuilder<TType>:: VisitStruct(
    const TTypePtr& type,
    const TProtobufTypeConfigPtr& typeConfig,
    TComplexTypeFieldDescriptor descriptor)
{
    YT_VERIFY(
        type->ProtoType == EProtobufType::StructuredMessage ||
        type->ProtoType == EProtobufType::Oneof);

    const auto isOneof = (type->ProtoType == EProtobufType::Oneof);

    THashMap<TStringBuf, TProtobufColumnConfigPtr> nameToConfig;
    for (const auto& config : typeConfig->Fields) {
        auto inserted = nameToConfig.emplace(config->Name, config).second;
        if (!inserted) {
            ThrowSchemaMismatch(
                Format("multiple fields with same name (%Qv) are forbidden", config->Name),
                descriptor,
                typeConfig);
        }
    }

    auto processFieldMissingFromConfig = [&] (const TStructField& structField) {
        if (isOneof) {
            // It is OK, we will ignore this alternative when writing.
            return;
        }
        if (structField.Type->GetMetatype() != ELogicalMetatype::Optional && !AreNonOptionalMissingFieldsAllowed) {
            ThrowSchemaMismatch(
                Format("non-optional field %Qv in schema is missing from protobuf config", structField.Name),
                descriptor,
                typeConfig);
        }
    };

    const auto& structFields = descriptor.GetType()->GetFields();
    if (!isOneof) {
        type->StructFieldCount = static_cast<int>(structFields.size());
    }
    for (int fieldIndex = 0; fieldIndex != static_cast<int>(structFields.size()); ++fieldIndex) {
        const auto& structField = structFields[fieldIndex];
        auto configIt = nameToConfig.find(structField.Name);
        if (configIt == nameToConfig.end()) {
            processFieldMissingFromConfig(structField);
            continue;
        }
        auto childDescriptor = isOneof
            ? descriptor.VariantStructField(fieldIndex)
            : descriptor.StructField(fieldIndex);
        if (isOneof && childDescriptor.GetType()->IsNullable()) {
            THROW_ERROR_EXCEPTION("Optional variant field %Qv can not match oneof field in protobuf format",
                childDescriptor.GetDescription());
        }
        type->AddChild(
            descriptor,
            CreateField(fieldIndex, configIt->second, childDescriptor),
            fieldIndex);
        nameToConfig.erase(configIt);
    }

    for (const auto& [name, childConfig] : nameToConfig) {
        if (childConfig->FieldNumber) {
            type->IgnoreChild(descriptor, *childConfig->FieldNumber);
        }
    }
}

template <typename TType>
void TProtobufTypeBuilder<TType>::VisitDict(
    const TTypePtr& type,
    const TProtobufTypeConfigPtr& typeConfig,
    TComplexTypeFieldDescriptor descriptor)
{
    if (type->ProtoType != EProtobufType::StructuredMessage) {
        ThrowSchemaMismatch(
            Format("expected protobuf field of type %Qlv to match %Qlv type in schema",
                EProtobufType::StructuredMessage,
                ELogicalMetatype::Dict),
            descriptor,
            typeConfig);
    }
    const auto& fields = typeConfig->Fields;
    if (fields.size() != 2 ||
        fields[0]->Name != "key" ||
        fields[1]->Name != "value")
    {
        ThrowSchemaMismatch(
            Format(
                "expected protobuf message with exactly fields \"key\" and \"value\" "
                "to match %Qlv type in schema",
                ELogicalMetatype::Dict),
            descriptor,
            typeConfig);
    }
    type->StructFieldCount = 2;
    const auto& keyConfig = fields[0];
    const auto& valueConfig = fields[1];
    type->AddChild(
        descriptor,
        CreateField(0, keyConfig, descriptor.DictKey()),
        /* fieldIndex */ 0);
    type->AddChild(
        descriptor,
        CreateField(1, valueConfig, descriptor.DictValue()),
        /* fieldIndex */ 1);
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
static T& ResizeAndGetElement(std::vector<T>& vector, int index, const T& fill = {})
{
    if (index >= static_cast<int>(vector.size())) {
        vector.resize(index + 1, fill);
    }
    return vector[index];
}

void InitTag(
    TProtobufTag& field,
    WireFormatLite::FieldType wireFieldType,
    const TProtobufColumnConfigPtr& columnConfig)
{
    YT_VERIFY(columnConfig->FieldNumber);
    field.TagSize = WireFormatLite::TagSize(*columnConfig->FieldNumber, wireFieldType);
    WireFormatLite::WireType wireType;
    if (columnConfig->Packed) {
        wireType = WireFormatLite::WireType::WIRETYPE_LENGTH_DELIMITED;
    } else {
        wireType = WireFormatLite::WireTypeForFieldType(wireFieldType);
    }
    field.WireTag = WireFormatLite::MakeTag(*columnConfig->FieldNumber, wireType);
}

int TProtobufWriterType::AddEmbedding(
    int parentEmbeddingIndex,
    const TProtobufColumnConfigPtr& embeddingConfig)
{
    int myEmbeddingIndex = std::ssize(Embeddings);
    auto& embedding = Embeddings.emplace_back();

    embedding.ParentEmbeddingIndex = parentEmbeddingIndex;

    auto wireFieldType = static_cast<WireFormatLite::FieldType>(::google::protobuf::FieldDescriptor::TYPE_MESSAGE);
    InitTag(embedding, wireFieldType, embeddingConfig);

    return myEmbeddingIndex;
}

void TProtobufWriterType::AddChild(
    const std::optional<NTableClient::TComplexTypeFieldDescriptor>& /* descriptor */,
    std::unique_ptr<TProtobufWriterFieldDescription> child,
    std::optional<int> fieldIndex,
    std::optional<int> parentEmbeddingIndex)
{
    if (parentEmbeddingIndex.has_value()) {
        child->ParentEmbeddingIndex = parentEmbeddingIndex.value();
    }
    if (ProtoType == EProtobufType::Oneof) {
        YT_VERIFY(fieldIndex);
        ResizeAndGetElement(AlternativeToChildIndex_, *fieldIndex, InvalidChildIndex) = Children.size();
    }
    Children.push_back(std::move(child));
}

void TProtobufWriterType::IgnoreChild(
    const std::optional<NTableClient::TComplexTypeFieldDescriptor>& /* descriptor */,
    int /* fieldNumber */)
{ }

const TProtobufWriterFieldDescription* TProtobufWriterType::FindAlternative(int alternativeIndex) const
{
    if (alternativeIndex >= static_cast<int>(AlternativeToChildIndex_.size())) {
        return nullptr;
    }
    if (AlternativeToChildIndex_[alternativeIndex] == InvalidChildIndex) {
        return nullptr;
    }
    return Children[AlternativeToChildIndex_[alternativeIndex]].get();
}

void TProtobufWriterFormatDescription::AddTable(TProtobufWriterTypePtr tableType)
{
    auto& table = Tables_.emplace_back();
    table.Type = std::move(tableType);
    for (const auto& column : table.Type->Children) {
        auto [it, inserted] = table.Columns.emplace(column->Name, column.get());
        if (!inserted) {
            THROW_ERROR_EXCEPTION("Multiple fields with same column name %Qv are forbidden in protobuf format",
                column->Name)
                << TErrorAttribute("table_index", std::ssize(Tables_) - 1);
        }
        if (column->Type->ProtoType == EProtobufType::OtherColumns) {
            table.OtherColumnsField = column.get();
        }
    }
    table.Embeddings = table.Type->Embeddings;
}

const TProtobufWriterFormatDescription::TTableDescription&
TProtobufWriterFormatDescription::GetTableDescription(int tableIndex) const
{
    if (Y_UNLIKELY(tableIndex >= std::ssize(Tables_))) {
        THROW_ERROR_EXCEPTION("Table with index %v is missing in format description",
            tableIndex);
    }
    return Tables_[tableIndex];
}

const TProtobufWriterFieldDescription* TProtobufWriterFormatDescription::FindField(
    int tableIndex,
    int fieldIndex,
    const TNameTablePtr& nameTable) const
{
    const auto& table = GetTableDescription(tableIndex);
    auto currentSize = std::ssize(table.FieldIndexToDescription);
    if (currentSize <= fieldIndex) {
        table.FieldIndexToDescription.reserve(fieldIndex + 1);
        for (int i = currentSize; i <= fieldIndex; ++i) {
            YT_VERIFY(std::ssize(table.FieldIndexToDescription) == i);
            auto fieldName = nameTable->GetName(i);
            auto it = table.Columns.find(fieldName);
            if (it == table.Columns.end()) {
                table.FieldIndexToDescription.push_back(nullptr);
            } else {
                table.FieldIndexToDescription.emplace_back(it->second);
            }
        }
    }
    return table.FieldIndexToDescription[fieldIndex];
}

void TProtobufWriterFormatDescription::Init(
    const TProtobufFormatConfigPtr& config,
    const std::vector<NTableClient::TTableSchemaPtr>& schemas)
{
    DoInit(config, schemas);
}

int TProtobufWriterFormatDescription::GetTableCount() const
{
    return std::ssize(Tables_);
}

const TProtobufWriterFieldDescription* TProtobufWriterFormatDescription::FindOtherColumnsField(int tableIndex) const
{
    return GetTableDescription(tableIndex).OtherColumnsField;
}

////////////////////////////////////////////////////////////////////////////////

int TProtobufParserType::AddEmbedding(
    int parentEmbeddingIndex,
    const TProtobufColumnConfigPtr& embeddingConfig)
{
    (void) parentEmbeddingIndex;
    (void) embeddingConfig;
    return 0;
}

void TProtobufParserType::AddChild(
    const std::optional<NTableClient::TComplexTypeFieldDescriptor>& descriptor,
    std::unique_ptr<TProtobufParserFieldDescription> child,
    std::optional<int> fieldIndex,
    std::optional<int> embeddingIndex)
{
    Y_UNUSED(embeddingIndex);
    if (child->Type->ProtoType == EProtobufType::Oneof) {
        if (ProtoType == EProtobufType::Oneof) {
            YT_VERIFY(descriptor);
            THROW_ERROR_EXCEPTION("Invalid protobuf format: oneof group %Qv cannot have a oneof child %Qv",
                descriptor->GetDescription(),
                child->Name);
        }
        YT_VERIFY(!child->Type->Field);
        child->Type->Field = child.get();
        for (auto& grandChild : child->Type->Children) {
            grandChild->StructFieldIndex = child->StructFieldIndex;
            AddChild(descriptor, std::move(grandChild), /* fieldIndex */ {});
        }
        for (auto fieldNumber : child->Type->IgnoredChildFieldNumbers_) {
            IgnoreChild(descriptor, fieldNumber);
        }
        child->Type->Children.clear();
        child->Type->IgnoredChildFieldNumbers_.clear();
        // YT_VERIFY(!child->Type->ContainingType);
        // child->Type->ContainingType = this;
        OneofDescriptions_.push_back(std::move(child));
    } else if (ProtoType == EProtobufType::Oneof) {
        YT_VERIFY(fieldIndex);
        child->AlternativeIndex = *fieldIndex;
        YT_VERIFY(!child->ContainingOneof);
        child->ContainingOneof = this;
        Children.push_back(std::move(child));
    } else {
        SetChildIndex(descriptor, child->GetFieldNumber(), Children.size());
        Children.push_back(std::move(child));
    }
}

void TProtobufParserType::IgnoreChild(
    const std::optional<NTableClient::TComplexTypeFieldDescriptor>& descriptor,
    int fieldNumber)
{
    SetChildIndex(descriptor, fieldNumber, IgnoredChildIndex);
    IgnoredChildFieldNumbers_.push_back(fieldNumber);
}

void TProtobufParserType::SetChildIndex(
    const std::optional<NTableClient::TComplexTypeFieldDescriptor>& descriptor,
    int fieldNumber,
    int childIndex,
    TFieldNumberToChildIndex* store)
{
    if (store == nullptr) {
        store = &FieldNumberToChildIndex_;
    }
    bool isNew = false;
    if (fieldNumber < MaxFieldNumberVectorSize) {
        auto& childIndexRef = ResizeAndGetElement(store->FieldNumberToChildIndexVector, fieldNumber, InvalidChildIndex);
        isNew = (childIndexRef == InvalidChildIndex);
        childIndexRef = childIndex;
    } else {
        isNew = store->FieldNumberToChildIndexMap.emplace(fieldNumber, childIndex).second;
    }
    if (!isNew) {
        THROW_ERROR_EXCEPTION("Invalid protobuf format: duplicate field number %v (child of %Qv)",
            fieldNumber,
            descriptor ? descriptor->GetDescription() : "<root>");
    }
}

void TProtobufParserType::SetEmbeddedChildIndex(
    int fieldNumber,
    int childIndex)
{
    SetChildIndex(std::nullopt, fieldNumber, childIndex, &FieldNumberToEmbeddedChildIndex_);
}

std::optional<int> TProtobufParserType::FieldNumberToChildIndex(int fieldNumber, const TFieldNumberToChildIndex* store) const
{
    if (store == nullptr) {
        store = &FieldNumberToChildIndex_;
    }
    int index;
    if (fieldNumber < std::ssize(store->FieldNumberToChildIndexVector)) {
        index = store->FieldNumberToChildIndexVector[fieldNumber];
        if (Y_UNLIKELY(index == InvalidChildIndex)) {
            THROW_ERROR_EXCEPTION("Unexpected field number %v",
                fieldNumber);
        }
    } else {
        auto it = store->FieldNumberToChildIndexMap.find(fieldNumber);
        if (Y_UNLIKELY(it == store->FieldNumberToChildIndexMap.end())) {
            THROW_ERROR_EXCEPTION("Unexpected field number %v",
                fieldNumber);
        }
        index = it->second;
    }
    if (index == IgnoredChildIndex) {
        return {};
    }
    return index;
}

std::optional<int> TProtobufParserType::FieldNumberToEmbeddedChildIndex(int fieldNumber) const
{
    return FieldNumberToChildIndex(fieldNumber, &FieldNumberToEmbeddedChildIndex_);
}

void TProtobufParserFormatDescription::Init(
    const TProtobufFormatConfigPtr& config,
    const std::vector<NTableClient::TTableSchemaPtr>& schemas)
{
    DoInit(config, schemas);
}

const TProtobufParserTypePtr& TProtobufParserFormatDescription::GetTableType() const
{
    return TableType_;
}

static int Process(
    std::vector<std::pair<ui16, TProtobufParserFieldDescription*>>& ids,
    int globalChildIndex,
    const TNameTablePtr& nameTable,
    const TProtobufParserTypePtr parent,
    const std::unique_ptr<TProtobufParserFieldDescription>& child)
{
    if (child->Type->ProtoType == EProtobufType::EmbeddedMessage) {
        for (const auto& grandChild: child->Type->Children) {
            globalChildIndex = Process(ids, globalChildIndex, nameTable, child->Type, grandChild);
        }
    } else {
        parent->SetEmbeddedChildIndex(child->GetFieldNumber(), globalChildIndex++);
        auto name = child->Name;
        if (child->IsOneofAlternative()) {
            YT_VERIFY(child->ContainingOneof && child->ContainingOneof->Field);
            name = child->ContainingOneof->Field->Name;
        }
        ids.emplace_back(static_cast<ui16>(nameTable->GetIdOrRegisterName(name)), child.get());
    }
    return globalChildIndex;
}

std::vector<std::pair<ui16, TProtobufParserFieldDescription*>> TProtobufParserFormatDescription::CreateRootChildColumnIds(const TNameTablePtr& nameTable) const
{
    std::vector<std::pair<ui16, TProtobufParserFieldDescription*>> ids;
    int globalChildIndex = 0;

    for (const auto& child: TableType_->Children) {
        globalChildIndex = Process(ids, globalChildIndex, nameTable, TableType_, child);
    }

    return ids;
}

void TProtobufParserFormatDescription::AddTable(TProtobufParserTypePtr tableType)
{
    YT_VERIFY(!TableType_);
    TableType_ = std::move(tableType);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats

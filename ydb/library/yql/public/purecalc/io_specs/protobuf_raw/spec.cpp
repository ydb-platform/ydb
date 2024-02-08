#include "proto_holder.h"
#include "spec.h"

#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_custom_list.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/utils/yql_panic.h>
#include <google/protobuf/reflection.h>

using namespace NYql;
using namespace NYql::NPureCalc;
using namespace google::protobuf;
using namespace NKikimr::NUdf;
using namespace NKikimr::NMiniKQL;

TProtobufRawInputSpec::TProtobufRawInputSpec(
    const Descriptor& descriptor,
    const TMaybe<TString>& timestampColumn,
    const TProtoSchemaOptions& options
)
    : Descriptor_(descriptor)
    , TimestampColumn_(timestampColumn)
    , SchemaOptions_(options)
{
}

const TVector<NYT::TNode>& TProtobufRawInputSpec::GetSchemas() const {
    if (SavedSchemas_.size() == 0) {
        SavedSchemas_.push_back(MakeSchemaFromProto(Descriptor_, SchemaOptions_));
        if (TimestampColumn_) {
            auto timestampType = NYT::TNode::CreateList();
            timestampType.Add("DataType");
            timestampType.Add("Uint64");
            auto timestamp = NYT::TNode::CreateList();
            timestamp.Add(*TimestampColumn_);
            timestamp.Add(timestampType);
            SavedSchemas_.back().AsList()[1].AsList().push_back(timestamp);
        }
    }

    return SavedSchemas_;
}

const Descriptor& TProtobufRawInputSpec::GetDescriptor() const {
    return Descriptor_;
}

const TMaybe<TString>& TProtobufRawInputSpec::GetTimestampColumn() const {
    return TimestampColumn_;
}

const TProtoSchemaOptions& TProtobufRawInputSpec::GetSchemaOptions() const {
    return SchemaOptions_;
}

TProtobufRawOutputSpec::TProtobufRawOutputSpec(
    const Descriptor& descriptor,
    MessageFactory* factory,
    const TProtoSchemaOptions& options,
    Arena* arena
)
    : Descriptor_(descriptor)
    , Factory_(factory)
    , SchemaOptions_(options)
    , Arena_(arena)
{
    SchemaOptions_.ListIsOptional = true;
}

const NYT::TNode& TProtobufRawOutputSpec::GetSchema() const {
    if (!SavedSchema_) {
        SavedSchema_ = MakeSchemaFromProto(Descriptor_, SchemaOptions_);
    }

    return SavedSchema_.GetRef();
}

const Descriptor& TProtobufRawOutputSpec::GetDescriptor() const {
    return Descriptor_;
}

void TProtobufRawOutputSpec::SetFactory(MessageFactory* factory) {
    Factory_ = factory;
}

MessageFactory* TProtobufRawOutputSpec::GetFactory() const {
    return Factory_;
}

void TProtobufRawOutputSpec::SetArena(Arena* arena) {
    Arena_ = arena;
}

Arena* TProtobufRawOutputSpec::GetArena() const {
    return Arena_;
}

const TProtoSchemaOptions& TProtobufRawOutputSpec::GetSchemaOptions() const {
    return SchemaOptions_;
}

TProtobufRawMultiOutputSpec::TProtobufRawMultiOutputSpec(
    TVector<const Descriptor*> descriptors,
    TMaybe<TVector<MessageFactory*>> factories,
    const TProtoSchemaOptions& options,
    TMaybe<TVector<Arena*>> arenas
)
    : Descriptors_(std::move(descriptors))
    , SchemaOptions_(options)
{
    if (factories) {
        Y_ENSURE(factories->size() == Descriptors_.size(), "number of factories must match number of descriptors");
        Factories_ = std::move(*factories);
    } else {
        Factories_ = TVector<MessageFactory*>(Descriptors_.size(), nullptr);
    }

    if (arenas) {
        Y_ENSURE(arenas->size() == Descriptors_.size(), "number of arenas must match number of descriptors");
        Arenas_ = std::move(*arenas);
    } else {
        Arenas_ = TVector<Arena*>(Descriptors_.size(), nullptr);
    }
}

const NYT::TNode& TProtobufRawMultiOutputSpec::GetSchema() const {
    if (SavedSchema_.IsUndefined()) {
        SavedSchema_ = MakeVariantSchemaFromProtos(Descriptors_, SchemaOptions_);
    }

    return SavedSchema_;
}

const Descriptor& TProtobufRawMultiOutputSpec::GetDescriptor(ui32 index) const {
    Y_ENSURE(index < Descriptors_.size(), "invalid output index");

    return *Descriptors_[index];
}

void TProtobufRawMultiOutputSpec::SetFactory(ui32 index, MessageFactory* factory) {
    Y_ENSURE(index < Factories_.size(), "invalid output index");

    Factories_[index] = factory;
}

MessageFactory* TProtobufRawMultiOutputSpec::GetFactory(ui32 index) const {
    Y_ENSURE(index < Factories_.size(), "invalid output index");

    return Factories_[index];
}

void TProtobufRawMultiOutputSpec::SetArena(ui32 index, Arena* arena) {
    Y_ENSURE(index < Arenas_.size(), "invalid output index");

    Arenas_[index] = arena;
}

Arena* TProtobufRawMultiOutputSpec::GetArena(ui32 index) const {
    Y_ENSURE(index < Arenas_.size(), "invalid output index");

    return Arenas_[index];
}

ui32 TProtobufRawMultiOutputSpec::GetOutputsNumber() const {
    return static_cast<ui32>(Descriptors_.size());
}

const TProtoSchemaOptions& TProtobufRawMultiOutputSpec::GetSchemaOptions() const {
    return SchemaOptions_;
}

namespace {
    struct TFieldMapping {
        TString Name;
        const FieldDescriptor* Field;
        TVector<TFieldMapping> NestedFields;
    };

    /**
     * Fills a tree of field mappings from the given yql struct type to protobuf message.
     *
     * @param fromType source yql type.
     * @param toType target protobuf message type.
     * @param mappings destination vector will be filled with field descriptors. Order of descriptors will match
     *                the order of field names.
     */
    void FillFieldMappings(
        const TStructType* fromType,
        const Descriptor& toType,
        TVector<TFieldMapping>& mappings,
        const TMaybe<TString>& timestampColumn,
        bool listIsOptional,
        const THashMap<TString, TString>& fieldRenames
    ) {
        THashMap<TString, TString> inverseFieldRenames;

        for (const auto& [source, target]: fieldRenames) {
            auto [iterator, emplaced] = inverseFieldRenames.emplace(target, source);
            Y_ENSURE(emplaced, "Duplicate rename field found: " << source << " -> " << target);
        }

        mappings.resize(fromType->GetMembersCount());
        for (ui32 i = 0; i < fromType->GetMembersCount(); ++i) {
            TString fieldName(fromType->GetMemberName(i));
            if (auto fieldRenamePtr = inverseFieldRenames.FindPtr(fieldName)) {
                fieldName = *fieldRenamePtr;
            }

            mappings[i].Name = fieldName;
            mappings[i].Field = toType.FindFieldByName(fieldName);
            YQL_ENSURE(
                mappings[i].Field || timestampColumn && *timestampColumn == fieldName,
                "Missing field: " << fieldName);

            const auto* fieldType = fromType->GetMemberType(i);
            if (fieldType->GetKind() == NKikimr::NMiniKQL::TType::EKind::List) {
                const auto* listType = static_cast<const NKikimr::NMiniKQL::TListType*>(fieldType);
                fieldType = listType->GetItemType();
            } else if (fieldType->GetKind() == NKikimr::NMiniKQL::TType::EKind::Optional) {
                const auto* optionalType = static_cast<const NKikimr::NMiniKQL::TOptionalType*>(fieldType);
                fieldType = optionalType->GetItemType();

                if (listIsOptional) {
                    if (fieldType->GetKind() == NKikimr::NMiniKQL::TType::EKind::List) {
                        const auto* listType = static_cast<const NKikimr::NMiniKQL::TListType*>(fieldType);
                        fieldType = listType->GetItemType();
                    }
                }
            }
            YQL_ENSURE(fieldType->GetKind() == NKikimr::NMiniKQL::TType::EKind::Struct ||
                       fieldType->GetKind() == NKikimr::NMiniKQL::TType::EKind::Data,
                       "unsupported field kind [" << fieldType->GetKindAsStr() << "], field [" << fieldName << "]");
            if (fieldType->GetKind() ==  NKikimr::NMiniKQL::TType::EKind::Struct) {
                FillFieldMappings(static_cast<const NKikimr::NMiniKQL::TStructType*>(fieldType),
                                  *mappings[i].Field->message_type(),
                                  mappings[i].NestedFields, Nothing(), listIsOptional, {});
            }
        }
    }

    /**
     * Extract field values from the given protobuf message into an array of unboxed values.
     *
     * @param factory to create nested unboxed values.
     * @param source source protobuf message.
     * @param destination destination array of unboxed values. Each element in the array corresponds to a field
     *                    in the protobuf message.
     * @param mappings vector of protobuf field descriptors which denotes relation between fields of the
     *                     source message and elements of the destination array.
     * @param scratch temporary string which will be used during conversion.
     */
    void FillInputValue(
        const THolderFactory& factory,
        const Message* source,
        TUnboxedValue* destination,
        const TVector<TFieldMapping>& mappings,
        const TMaybe<TString>& timestampColumn,
        ITimeProvider* timeProvider,
        EEnumPolicy enumPolicy
    ) {
        TString scratch;
        auto reflection = source->GetReflection();
        for (ui32 i = 0; i < mappings.size(); ++i) {
            auto mapping = mappings[i];
            if (!mapping.Field) {
                YQL_ENSURE(timestampColumn && mapping.Name == *timestampColumn);
                destination[i] = TUnboxedValuePod(timeProvider->Now().MicroSeconds());
                continue;
            }

            const auto type = mapping.Field->type();
            if (mapping.Field->label() == FieldDescriptor::LABEL_REPEATED) {
                const auto size = static_cast<ui32>(reflection->FieldSize(*source, mapping.Field));
                if (size == 0) {
                    destination[i] = factory.GetEmptyContainerLazy();
                } else {
                    TUnboxedValue* inplace = nullptr;
                    destination[i] = factory.CreateDirectArrayHolder(size, inplace);
                    for (ui32 j = 0; j < size; ++j) {
                        switch (type) {
                            case FieldDescriptor::TYPE_DOUBLE:
                                inplace[j] = TUnboxedValuePod(reflection->GetRepeatedDouble(*source, mapping.Field, j));
                                break;

                            case FieldDescriptor::TYPE_FLOAT:
                                inplace[j] = TUnboxedValuePod(reflection->GetRepeatedFloat(*source, mapping.Field, j));
                                break;

                            case FieldDescriptor::TYPE_INT64:
                            case FieldDescriptor::TYPE_SFIXED64:
                            case FieldDescriptor::TYPE_SINT64:
                                inplace[j] = TUnboxedValuePod(reflection->GetRepeatedInt64(*source, mapping.Field, j));
                                break;

                            case FieldDescriptor::TYPE_ENUM:
                                switch (EnumFormatType(*mapping.Field, enumPolicy)) {
                                    case EEnumFormatType::Int32:
                                        inplace[j] = TUnboxedValuePod(reflection->GetRepeatedEnumValue(*source, mapping.Field, j));
                                        break;
                                    case EEnumFormatType::String:
                                        inplace[j] = MakeString(reflection->GetRepeatedEnum(*source, mapping.Field, j)->name());
                                        break;
                                }
                                break;

                            case FieldDescriptor::TYPE_UINT64:
                            case FieldDescriptor::TYPE_FIXED64:
                                inplace[j] = TUnboxedValuePod(reflection->GetRepeatedUInt64(*source, mapping.Field, j));
                                break;

                            case FieldDescriptor::TYPE_INT32:
                            case FieldDescriptor::TYPE_SFIXED32:
                            case FieldDescriptor::TYPE_SINT32:
                                inplace[j] = TUnboxedValuePod(reflection->GetRepeatedInt32(*source, mapping.Field, j));
                                break;

                            case FieldDescriptor::TYPE_UINT32:
                            case FieldDescriptor::TYPE_FIXED32:
                                inplace[j] = TUnboxedValuePod(reflection->GetRepeatedUInt32(*source, mapping.Field, j));
                                break;

                            case FieldDescriptor::TYPE_BOOL:
                                inplace[j] = TUnboxedValuePod(reflection->GetRepeatedBool(*source, mapping.Field, j));
                                break;

                            case FieldDescriptor::TYPE_STRING:
                                inplace[j] = MakeString(reflection->GetRepeatedStringReference(*source, mapping.Field, j, &scratch));
                                break;

                            case FieldDescriptor::TYPE_BYTES:
                                inplace[j] = MakeString(reflection->GetRepeatedStringReference(*source, mapping.Field, j, &scratch));
                                break;

                            case FieldDescriptor::TYPE_MESSAGE:
                                {
                                    const Message& nestedMessage = reflection->GetRepeatedMessage(*source, mapping.Field, j);
                                    TUnboxedValue* nestedValues = nullptr;
                                    inplace[j] = factory.CreateDirectArrayHolder(static_cast<ui32>(mapping.NestedFields.size()),
                                                                                     nestedValues);
                                    FillInputValue(factory, &nestedMessage, nestedValues, mapping.NestedFields, Nothing(), timeProvider, enumPolicy);
                                }
                                break;

                            default:
                                ythrow yexception() << "Unsupported protobuf type: " << mapping.Field->type_name() << ", field: " << mapping.Field->name();
                        }
                    }
                }
            } else {
                if (!reflection->HasField(*source, mapping.Field)) {
                    continue;
                }

                switch (type) {
                    case FieldDescriptor::TYPE_DOUBLE:
                        destination[i] = TUnboxedValuePod(reflection->GetDouble(*source, mapping.Field));
                        break;

                    case FieldDescriptor::TYPE_FLOAT:
                        destination[i] = TUnboxedValuePod(reflection->GetFloat(*source, mapping.Field));
                        break;

                    case FieldDescriptor::TYPE_INT64:
                    case FieldDescriptor::TYPE_SFIXED64:
                    case FieldDescriptor::TYPE_SINT64:
                        destination[i] = TUnboxedValuePod(reflection->GetInt64(*source, mapping.Field));
                        break;

                    case FieldDescriptor::TYPE_ENUM:
                        switch (EnumFormatType(*mapping.Field, enumPolicy)) {
                            case EEnumFormatType::Int32:
                                destination[i] = TUnboxedValuePod(reflection->GetEnumValue(*source, mapping.Field));
                                break;
                            case EEnumFormatType::String:
                                destination[i] = MakeString(reflection->GetEnum(*source, mapping.Field)->name());
                                break;
                        }
                        break;

                    case FieldDescriptor::TYPE_UINT64:
                    case FieldDescriptor::TYPE_FIXED64:
                        destination[i] = TUnboxedValuePod(reflection->GetUInt64(*source, mapping.Field));
                        break;

                    case FieldDescriptor::TYPE_INT32:
                    case FieldDescriptor::TYPE_SFIXED32:
                    case FieldDescriptor::TYPE_SINT32:
                        destination[i] = TUnboxedValuePod(reflection->GetInt32(*source, mapping.Field));
                        break;

                    case FieldDescriptor::TYPE_UINT32:
                    case FieldDescriptor::TYPE_FIXED32:
                        destination[i] = TUnboxedValuePod(reflection->GetUInt32(*source, mapping.Field));
                        break;

                    case FieldDescriptor::TYPE_BOOL:
                        destination[i] = TUnboxedValuePod(reflection->GetBool(*source, mapping.Field));
                        break;

                    case FieldDescriptor::TYPE_STRING:
                        destination[i] = MakeString(reflection->GetStringReference(*source, mapping.Field, &scratch));
                        break;

                    case FieldDescriptor::TYPE_BYTES:
                        destination[i] = MakeString(reflection->GetStringReference(*source, mapping.Field, &scratch));
                        break;
                    case FieldDescriptor::TYPE_MESSAGE:
                        {
                            const Message& nestedMessage = reflection->GetMessage(*source, mapping.Field);
                            TUnboxedValue* nestedValues = nullptr;
                            destination[i] = factory.CreateDirectArrayHolder(static_cast<ui32>(mapping.NestedFields.size()),
                                                                             nestedValues);
                            FillInputValue(factory, &nestedMessage, nestedValues, mapping.NestedFields, Nothing(), timeProvider, enumPolicy);
                        }
                        break;

                    default:
                        ythrow yexception() << "Unsupported protobuf type: " << mapping.Field->type_name()
                                            << ", field: " << mapping.Field->name();
                }
            }
        }
    }


    /**
     * Convert unboxed value to protobuf.
     *
     * @param source        unboxed value to extract data from. Type of the value should be struct. It's UB to pass
     *                      a non-struct value here.
     * @param destination   destination message. Data in this message will be overwritten
     *                      by data from unboxed value.
     * @param mappings      vector of protobuf field descriptors which denotes relation between struct fields
     *                      and message fields. For any i-th element of this vector, type of the i-th element of
     *                      the unboxed structure must match type of the field pointed by descriptor. Size of this
     *                      vector should match the number of fields in the struct.
     */
    void FillOutputMessage(
        const TUnboxedValue& source,
        Message* destination,
        const TVector<TFieldMapping>& mappings,
        EEnumPolicy enumPolicy
    ) {
        auto reflection = destination->GetReflection();
        for (ui32 i = 0; i < mappings.size(); ++i) {
            const auto& mapping = mappings[i];
            const auto& cell = source.GetElement(i);
            if (!cell) {
                reflection->ClearField(destination, mapping.Field);
                continue;
            }
            const auto type = mapping.Field->type();
            if (mapping.Field->label() == FieldDescriptor::LABEL_REPEATED) {
                const auto iter = cell.GetListIterator();
                reflection->ClearField(destination, mapping.Field);
                for (TUnboxedValue item; iter.Next(item);) {
                    switch (mapping.Field->type()) {
                        case FieldDescriptor::TYPE_DOUBLE:
                            reflection->AddDouble(destination, mapping.Field, item.Get<double>());
                            break;

                        case FieldDescriptor::TYPE_FLOAT:
                            reflection->AddFloat(destination, mapping.Field, item.Get<float>());
                            break;

                        case FieldDescriptor::TYPE_INT64:
                        case FieldDescriptor::TYPE_SFIXED64:
                        case FieldDescriptor::TYPE_SINT64:
                            reflection->AddInt64(destination, mapping.Field, item.Get<i64>());
                            break;

                        case FieldDescriptor::TYPE_ENUM: {
                            switch (EnumFormatType(*mapping.Field, enumPolicy)) {
                                case EEnumFormatType::Int32:
                                    reflection->AddEnumValue(destination, mapping.Field, item.Get<i32>());
                                    break;
                                case EEnumFormatType::String: {
                                    auto enumValueDescriptor = mapping.Field->enum_type()->FindValueByName(TString(item.AsStringRef()));
                                    if (!enumValueDescriptor) {
                                        enumValueDescriptor = mapping.Field->default_value_enum();
                                    }
                                    reflection->AddEnum(destination, mapping.Field, enumValueDescriptor);
                                    break;
                                }
                            }
                            break;
                        }

                        case FieldDescriptor::TYPE_UINT64:
                        case FieldDescriptor::TYPE_FIXED64:
                            reflection->AddUInt64(destination, mapping.Field, item.Get<ui64>());
                            break;

                        case FieldDescriptor::TYPE_INT32:
                        case FieldDescriptor::TYPE_SFIXED32:
                        case FieldDescriptor::TYPE_SINT32:
                            reflection->AddInt32(destination, mapping.Field, item.Get<i32>());
                            break;

                        case FieldDescriptor::TYPE_UINT32:
                        case FieldDescriptor::TYPE_FIXED32:
                            reflection->AddUInt32(destination, mapping.Field, item.Get<ui32>());
                            break;

                        case FieldDescriptor::TYPE_BOOL:
                            reflection->AddBool(destination, mapping.Field, item.Get<bool>());
                            break;

                        case FieldDescriptor::TYPE_STRING:
                            reflection->AddString(destination, mapping.Field, TString(item.AsStringRef()));
                            break;

                        case FieldDescriptor::TYPE_BYTES:
                            reflection->AddString(destination, mapping.Field, TString(item.AsStringRef()));
                            break;

                        case FieldDescriptor::TYPE_MESSAGE:
                            {
                                auto* nestedMessage = reflection->AddMessage(destination, mapping.Field);
                                FillOutputMessage(item, nestedMessage, mapping.NestedFields, enumPolicy);
                            }
                            break;

                        default:
                            ythrow yexception() << "Unsupported protobuf type: "
                                                << mapping.Field->type_name() << ", field: " << mapping.Field->name();
                    }
                }
            } else {
                switch (type) {
                    case FieldDescriptor::TYPE_DOUBLE:
                        reflection->SetDouble(destination, mapping.Field, cell.Get<double>());
                        break;

                    case FieldDescriptor::TYPE_FLOAT:
                        reflection->SetFloat(destination, mapping.Field, cell.Get<float>());
                        break;

                    case FieldDescriptor::TYPE_INT64:
                    case FieldDescriptor::TYPE_SFIXED64:
                    case FieldDescriptor::TYPE_SINT64:
                        reflection->SetInt64(destination, mapping.Field, cell.Get<i64>());
                        break;

                    case FieldDescriptor::TYPE_ENUM: {
                        switch (EnumFormatType(*mapping.Field, enumPolicy)) {
                            case EEnumFormatType::Int32:
                                reflection->SetEnumValue(destination, mapping.Field, cell.Get<i32>());
                                break;
                            case EEnumFormatType::String: {
                                auto enumValueDescriptor = mapping.Field->enum_type()->FindValueByName(TString(cell.AsStringRef()));
                                if (!enumValueDescriptor) {
                                    enumValueDescriptor = mapping.Field->default_value_enum();
                                }
                                reflection->SetEnum(destination, mapping.Field, enumValueDescriptor);
                                break;
                            }
                        }
                        break;
                    }

                    case FieldDescriptor::TYPE_UINT64:
                    case FieldDescriptor::TYPE_FIXED64:
                        reflection->SetUInt64(destination, mapping.Field, cell.Get<ui64>());
                        break;

                    case FieldDescriptor::TYPE_INT32:
                    case FieldDescriptor::TYPE_SFIXED32:
                    case FieldDescriptor::TYPE_SINT32:
                        reflection->SetInt32(destination, mapping.Field, cell.Get<i32>());
                        break;

                    case FieldDescriptor::TYPE_UINT32:
                    case FieldDescriptor::TYPE_FIXED32:
                        reflection->SetUInt32(destination, mapping.Field, cell.Get<ui32>());
                        break;

                    case FieldDescriptor::TYPE_BOOL:
                        reflection->SetBool(destination, mapping.Field, cell.Get<bool>());
                        break;

                    case FieldDescriptor::TYPE_STRING:
                        reflection->SetString(destination, mapping.Field, TString(cell.AsStringRef()));
                        break;

                    case FieldDescriptor::TYPE_BYTES:
                        reflection->SetString(destination, mapping.Field, TString(cell.AsStringRef()));
                        break;

                    case FieldDescriptor::TYPE_MESSAGE:
                        {
                            auto* nestedMessage = reflection->MutableMessage(destination, mapping.Field);
                            FillOutputMessage(cell, nestedMessage, mapping.NestedFields, enumPolicy);
                        }
                        break;

                    default:
                        ythrow yexception() << "Unsupported protobuf type: "
                                            << mapping.Field->type_name() << ", field: " << mapping.Field->name();
                }
            }
        }
    }

    /**
     * Converts input messages to unboxed values.
     */
    class TInputConverter {
    protected:
        IWorker* Worker_;
        TVector<TFieldMapping> Mappings_;
        TPlainContainerCache Cache_;
        TMaybe<TString> TimestampColumn_;
        EEnumPolicy EnumPolicy_ = EEnumPolicy::Int32;

    public:
        explicit TInputConverter(const TProtobufRawInputSpec& inputSpec, IWorker* worker)
            : Worker_(worker)
            , TimestampColumn_(inputSpec.GetTimestampColumn())
            , EnumPolicy_(inputSpec.GetSchemaOptions().EnumPolicy)
        {
            FillFieldMappings(
                Worker_->GetInputType(), inputSpec.GetDescriptor(),
                Mappings_, TimestampColumn_,
                inputSpec.GetSchemaOptions().ListIsOptional,
                inputSpec.GetSchemaOptions().FieldRenames
            );
        }

    public:
        void DoConvert(const Message* message, TUnboxedValue& result) {
            auto& holderFactory = Worker_->GetGraph().GetHolderFactory();
            TUnboxedValue* items = nullptr;
            result = Cache_.NewArray(holderFactory, static_cast<ui32>(Mappings_.size()), items);
            FillInputValue(holderFactory, message, items, Mappings_, TimestampColumn_, Worker_->GetTimeProvider(), EnumPolicy_);
        }

        void ClearCache() {
            Cache_.Clear();
        }
    };

    template <typename TOutputSpec>
    using OutputItemType = typename TOutputSpecTraits<TOutputSpec>::TOutputItemType;

    template <typename TOutputSpec>
    class TOutputConverter;

    /**
     * Converts unboxed values to output messages (single-output program case).
     */
    template <>
    class TOutputConverter<TProtobufRawOutputSpec> {
    protected:
        IWorker* Worker_;
        TVector<TFieldMapping> OutputColumns_;
        TProtoHolder<Message> Message_;
        EEnumPolicy EnumPolicy_ = EEnumPolicy::Int32;

    public:
        explicit TOutputConverter(const TProtobufRawOutputSpec& outputSpec, IWorker* worker)
            : Worker_(worker)
            , EnumPolicy_(outputSpec.GetSchemaOptions().EnumPolicy)
        {
            if (!Worker_->GetOutputType()->IsStruct()) {
                ythrow yexception() << "protobuf output spec does not support multiple outputs";
            }

            FillFieldMappings(
                static_cast<const NKikimr::NMiniKQL::TStructType*>(Worker_->GetOutputType()),
                outputSpec.GetDescriptor(),
                OutputColumns_,
                Nothing(),
                outputSpec.GetSchemaOptions().ListIsOptional,
                outputSpec.GetSchemaOptions().FieldRenames
            );

            auto* factory = outputSpec.GetFactory();

            if (!factory) {
                factory = MessageFactory::generated_factory();
            }

            Message_.Reset(factory->GetPrototype(&outputSpec.GetDescriptor())->New(outputSpec.GetArena()));
        }

        OutputItemType<TProtobufRawOutputSpec> DoConvert(TUnboxedValue value) {
            FillOutputMessage(value, Message_.Get(), OutputColumns_, EnumPolicy_);
            return Message_.Get();
        }
    };

    /*
     * Converts unboxed values to output type (multi-output programs case).
     */
    template <>
    class TOutputConverter<TProtobufRawMultiOutputSpec> {
    protected:
        IWorker* Worker_;
        TVector<TVector<TFieldMapping>> OutputColumns_;
        TVector<TProtoHolder<Message>> Messages_;
        EEnumPolicy EnumPolicy_ = EEnumPolicy::Int32;

    public:
        explicit TOutputConverter(const TProtobufRawMultiOutputSpec& outputSpec, IWorker* worker)
            : Worker_(worker)
            , EnumPolicy_(outputSpec.GetSchemaOptions().EnumPolicy)
        {
            const auto* outputType = Worker_->GetOutputType();
            Y_ENSURE(outputType->IsVariant(), "protobuf multi-output spec requires multi-output program");
            const auto* variantType = static_cast<const NKikimr::NMiniKQL::TVariantType*>(outputType);
            Y_ENSURE(
                variantType->GetUnderlyingType()->IsTuple(),
                "protobuf multi-output spec requires variant over tuple as program output type"
            );
            Y_ENSURE(
                outputSpec.GetOutputsNumber() == variantType->GetAlternativesCount(),
                "number of outputs provided by spec does not match number of variant alternatives"
            );

            auto defaultFactory = MessageFactory::generated_factory();

            for (ui32 i = 0; i < variantType->GetAlternativesCount(); ++i) {
                const auto* type = variantType->GetAlternativeType(i);
                Y_ASSERT(type->IsStruct());
                Y_ASSERT(OutputColumns_.size() == i && Messages_.size() == i);

                OutputColumns_.push_back({});

                FillFieldMappings(
                    static_cast<const NKikimr::NMiniKQL::TStructType*>(type),
                    outputSpec.GetDescriptor(i),
                    OutputColumns_.back(),
                    Nothing(),
                    outputSpec.GetSchemaOptions().ListIsOptional,
                    {}
                );

                auto factory = outputSpec.GetFactory(i);
                if (!factory) {
                    factory = defaultFactory;
                }

                Messages_.push_back(TProtoHolder<Message>(
                    factory->GetPrototype(&outputSpec.GetDescriptor(i))->New(outputSpec.GetArena(i))
                ));
            }
        }

        OutputItemType<TProtobufRawMultiOutputSpec> DoConvert(TUnboxedValue value) {
            auto index = value.GetVariantIndex();
            auto msgPtr = Messages_[index].Get();
            FillOutputMessage(value.GetVariantItem(), msgPtr, OutputColumns_[index], EnumPolicy_);
            return {index, msgPtr};
        }
    };

    /**
     * List (or, better, stream) of unboxed values. Used as an input value in pull workers.
     */
    class TProtoListValue final: public TCustomListValue {
    private:
        mutable bool HasIterator_ = false;
        THolder<IStream<Message*>> Underlying_;
        TInputConverter Converter_;
        IWorker* Worker_;
        TScopedAlloc& ScopedAlloc_;

    public:
        TProtoListValue(
            TMemoryUsageInfo* memInfo,
            const TProtobufRawInputSpec& inputSpec,
            THolder<IStream<Message*>> underlying,
            IWorker* worker
        )
            : TCustomListValue(memInfo)
            , Underlying_(std::move(underlying))
            , Converter_(inputSpec, worker)
            , Worker_(worker)
            , ScopedAlloc_(Worker_->GetScopedAlloc())
        {
        }

        ~TProtoListValue() override {
            {
                // This list value stored in the worker's computation graph and destroyed upon the computation
                // graph's destruction. This brings us to an interesting situation: scoped alloc is acquired,
                // worker and computation graph are half-way destroyed, and now it's our turn to die. The problem is,
                // the underlying stream may own another worker. This happens when chaining programs. Now, to destroy
                // that worker correctly, we need to release our scoped alloc (because that worker has its own
                // computation graph and scoped alloc).
                // By the way, note that we shouldn't interact with the worker here because worker is in the middle of
                // its own destruction. So we're using our own reference to the scoped alloc. That reference is alive
                // because scoped alloc destroyed after computation graph.
                auto unguard = Unguard(ScopedAlloc_);
                Underlying_.Destroy();
            }
        }

    public:
        TUnboxedValue GetListIterator() const override {
            YQL_ENSURE(!HasIterator_, "Only one pass over input is supported");
            HasIterator_ = true;
            return TUnboxedValuePod(const_cast<TProtoListValue*>(this));
        }

        bool Next(TUnboxedValue& result) override {
            const Message* message;
            {
                auto unguard = Unguard(ScopedAlloc_);
                message = Underlying_->Fetch();
            }

            if (!message) {
                return false;
            }

            Converter_.DoConvert(message, result);

            return true;
        }

        EFetchStatus Fetch(TUnboxedValue& result) override {
            if (Next(result)) {
                return EFetchStatus::Ok;
            } else {
                return EFetchStatus::Finish;
            }
        }
    };

    /**
     * Consumer which converts messages to unboxed values and relays them to the worker. Used as a return value
     * of the push processor's Process function.
     */
    class TProtoConsumerImpl final: public IConsumer<Message*> {
    private:
        TWorkerHolder<IPushStreamWorker> WorkerHolder_;
        TInputConverter Converter_;

    public:
        explicit TProtoConsumerImpl(
            const TProtobufRawInputSpec& inputSpec,
            TWorkerHolder<IPushStreamWorker> worker
        )
            : WorkerHolder_(std::move(worker))
            , Converter_(inputSpec, WorkerHolder_.Get())
        {
        }

        ~TProtoConsumerImpl() override {
            with_lock(WorkerHolder_->GetScopedAlloc()) {
                Converter_.ClearCache();
            }
        }

    public:
        void OnObject(Message* message) override {
            TBindTerminator bind(WorkerHolder_->GetGraph().GetTerminator());

            with_lock(WorkerHolder_->GetScopedAlloc()) {
                TUnboxedValue result;
                Converter_.DoConvert(message, result);
                WorkerHolder_->Push(std::move(result));
            }
        }

        void OnFinish() override {
            TBindTerminator bind(WorkerHolder_->GetGraph().GetTerminator());

            with_lock(WorkerHolder_->GetScopedAlloc()) {
                WorkerHolder_->OnFinish();
            }
        }
    };

    /**
     * Protobuf input stream for unboxed value streams.
     */
    template <typename TOutputSpec>
    class TRawProtoStreamImpl final: public IStream<OutputItemType<TOutputSpec>> {
    protected:
        TWorkerHolder<IPullStreamWorker> WorkerHolder_;
        TOutputConverter<TOutputSpec> Converter_;

    public:
        explicit TRawProtoStreamImpl(const TOutputSpec& outputSpec, TWorkerHolder<IPullStreamWorker> worker)
            : WorkerHolder_(std::move(worker))
            , Converter_(outputSpec, WorkerHolder_.Get())
        {
        }

    public:
        OutputItemType<TOutputSpec> Fetch() override {
            TBindTerminator bind(WorkerHolder_->GetGraph().GetTerminator());

            with_lock(WorkerHolder_->GetScopedAlloc()) {
                TUnboxedValue value;

                auto status = WorkerHolder_->GetOutput().Fetch(value);

                YQL_ENSURE(status != EFetchStatus::Yield, "Yield is not supported in pull mode");

                if (status == EFetchStatus::Finish) {
                    return TOutputSpecTraits<TOutputSpec>::StreamSentinel;
                }

                return Converter_.DoConvert(value);
            }
        }
    };

    /**
     * Protobuf input stream for unboxed value lists.
     */
    template <typename TOutputSpec>
    class TRawProtoListImpl final: public IStream<OutputItemType<TOutputSpec>> {
    protected:
        TWorkerHolder<IPullListWorker> WorkerHolder_;
        TOutputConverter<TOutputSpec> Converter_;

    public:
        explicit TRawProtoListImpl(const TOutputSpec& outputSpec, TWorkerHolder<IPullListWorker> worker)
            : WorkerHolder_(std::move(worker))
            , Converter_(outputSpec, WorkerHolder_.Get())
        {
        }

    public:
        OutputItemType<TOutputSpec> Fetch() override {
            TBindTerminator bind(WorkerHolder_->GetGraph().GetTerminator());

            with_lock(WorkerHolder_->GetScopedAlloc()) {
                TUnboxedValue value;

                if (!WorkerHolder_->GetOutputIterator().Next(value)) {
                    return TOutputSpecTraits<TOutputSpec>::StreamSentinel;
                }

                return Converter_.DoConvert(value);
            }
        }
    };

    /**
     * Push relay used to convert generated unboxed value to a message and push it to the user's consumer.
     */
    template <typename TOutputSpec>
    class TPushRelayImpl: public IConsumer<const TUnboxedValue*> {
    private:
        THolder<IConsumer<OutputItemType<TOutputSpec>>> Underlying_;
        TOutputConverter<TOutputSpec> Converter_;
        IWorker* Worker_;

    public:
        TPushRelayImpl(
            const TOutputSpec& outputSpec,
            IPushStreamWorker* worker,
            THolder<IConsumer<OutputItemType<TOutputSpec>>> underlying
        )
            : Underlying_(std::move(underlying))
            , Converter_(outputSpec, worker)
            , Worker_(worker)
        {
        }

        // If you've read a comment in the TProtoListValue's destructor, you may be wondering why don't we do the
        // same trick here. Well, that's because in push mode, consumer is destroyed before acquiring scoped alloc and
        // destroying computation graph.

    public:
        void OnObject(const TUnboxedValue* value) override {
            OutputItemType<TOutputSpec> message = Converter_.DoConvert(*value);
            auto unguard = Unguard(Worker_->GetScopedAlloc());
            Underlying_->OnObject(message);
        }

        void OnFinish() override {
            auto unguard = Unguard(Worker_->GetScopedAlloc());
            Underlying_->OnFinish();
        }
    };
}

using ConsumerType = TInputSpecTraits<TProtobufRawInputSpec>::TConsumerType;

void TInputSpecTraits<TProtobufRawInputSpec>::PreparePullStreamWorker(
    const TProtobufRawInputSpec& inputSpec,
    IPullStreamWorker* worker,
    THolder<IStream<Message*>> stream
) {
    with_lock(worker->GetScopedAlloc()) {
        worker->SetInput(
            worker->GetGraph().GetHolderFactory().Create<TProtoListValue>(inputSpec, std::move(stream), worker), 0);
    }
}

void TInputSpecTraits<TProtobufRawInputSpec>::PreparePullListWorker(
    const TProtobufRawInputSpec& inputSpec,
    IPullListWorker* worker,
    THolder<IStream<Message*>> stream
) {
    with_lock(worker->GetScopedAlloc()) {
        worker->SetInput(
            worker->GetGraph().GetHolderFactory().Create<TProtoListValue>(inputSpec, std::move(stream), worker), 0);
    }
}

ConsumerType TInputSpecTraits<TProtobufRawInputSpec>::MakeConsumer(
    const TProtobufRawInputSpec& inputSpec,
    TWorkerHolder<IPushStreamWorker> worker
) {
    return MakeHolder<TProtoConsumerImpl>(inputSpec, std::move(worker));
}

template <typename TOutputSpec>
using PullStreamReturnType = typename TOutputSpecTraits<TOutputSpec>::TPullStreamReturnType;
template <typename TOutputSpec>
using PullListReturnType = typename TOutputSpecTraits<TOutputSpec>::TPullListReturnType;

PullStreamReturnType<TProtobufRawOutputSpec> TOutputSpecTraits<TProtobufRawOutputSpec>::ConvertPullStreamWorkerToOutputType(
    const TProtobufRawOutputSpec& outputSpec,
    TWorkerHolder<IPullStreamWorker> worker
) {
    return MakeHolder<TRawProtoStreamImpl<TProtobufRawOutputSpec>>(outputSpec, std::move(worker));
}

PullListReturnType<TProtobufRawOutputSpec> TOutputSpecTraits<TProtobufRawOutputSpec>::ConvertPullListWorkerToOutputType(
    const TProtobufRawOutputSpec& outputSpec,
    TWorkerHolder<IPullListWorker> worker
) {
    return MakeHolder<TRawProtoListImpl<TProtobufRawOutputSpec>>(outputSpec, std::move(worker));
}

void TOutputSpecTraits<TProtobufRawOutputSpec>::SetConsumerToWorker(
    const TProtobufRawOutputSpec& outputSpec,
    IPushStreamWorker* worker,
    THolder<IConsumer<TOutputItemType>> consumer
) {
    worker->SetConsumer(MakeHolder<TPushRelayImpl<TProtobufRawOutputSpec>>(outputSpec, worker, std::move(consumer)));
}

PullStreamReturnType<TProtobufRawMultiOutputSpec> TOutputSpecTraits<TProtobufRawMultiOutputSpec>::ConvertPullStreamWorkerToOutputType(
    const TProtobufRawMultiOutputSpec& outputSpec,
    TWorkerHolder<IPullStreamWorker> worker
) {
    return MakeHolder<TRawProtoStreamImpl<TProtobufRawMultiOutputSpec>>(outputSpec, std::move(worker));
}

PullListReturnType<TProtobufRawMultiOutputSpec> TOutputSpecTraits<TProtobufRawMultiOutputSpec>::ConvertPullListWorkerToOutputType(
    const TProtobufRawMultiOutputSpec& outputSpec,
    TWorkerHolder<IPullListWorker> worker
) {
    return MakeHolder<TRawProtoListImpl<TProtobufRawMultiOutputSpec>>(outputSpec, std::move(worker));
}

void TOutputSpecTraits<TProtobufRawMultiOutputSpec>::SetConsumerToWorker(
    const TProtobufRawMultiOutputSpec& outputSpec,
    IPushStreamWorker* worker,
    THolder<IConsumer<TOutputItemType>> consumer
) {
    worker->SetConsumer(MakeHolder<TPushRelayImpl<TProtobufRawMultiOutputSpec>>(outputSpec, worker, std::move(consumer)));
}

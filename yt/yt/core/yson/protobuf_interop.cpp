#include "protobuf_interop.h"

#include "config.h"
#include "consumer.h"
#include "forwarding_consumer.h"
#include "null_consumer.h"
#include "parser.h"
#include "protobuf_interop_unknown_fields.h"
#include "writer.h"

#include <yt/yt_proto/yt/core/yson/proto/protobuf_interop.pb.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/ypath/helpers.h>
#include <yt/yt/core/ypath/stack.h>
#include <yt/yt/core/ypath/token.h>
#include <yt/yt/core/ypath/tokenizer.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/ephemeral_node_factory.h>
#include <yt/yt/core/ytree/tree_builder.h>
#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt_proto/yt/core/ytree/proto/attributes.pb.h>

#include <yt/yt/library/syncmap/map.h>

#include <yt/yt/core/concurrency/thread_affinity.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>
#include <library/cpp/yt/memory/leaky_singleton.h>

#include <library/cpp/yt/misc/cast.h>

#include <library/cpp/yt/string/string.h>

#include <library/cpp/yt/threading/fork_aware_spin_lock.h>

#include <library/cpp/yt/coding/varint.h>
#include <library/cpp/yt/coding/zig_zag.h>

#include <util/charset/utf8.h>

#include <google/protobuf/descriptor.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/wire_format.h>

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream.h>

namespace NYT::NYson {

using namespace NYson;
using namespace NYTree;
using namespace NYPath;
using namespace NThreading;
using namespace NConcurrency;
using namespace google::protobuf;
using namespace google::protobuf::io;
using namespace google::protobuf::internal;

////////////////////////////////////////////////////////////////////////////////

class TProtobufField;
class TProtobufEnumType;

static constexpr size_t TypicalFieldCount = 16;
using TFieldNumberList = TCompactVector<int, TypicalFieldCount>;

static constexpr int AttributeDictionaryAttributeFieldNumber = 1;
static constexpr int ProtobufMapKeyFieldNumber = 1;
static constexpr int ProtobufMapValueFieldNumber = 2;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

const NLogging::TLogger Logger("ProtobufInterop");

////////////////////////////////////////////////////////////////////////////////

bool IsSignedIntegralType(FieldDescriptor::Type type)
{
    switch (type) {
        case FieldDescriptor::TYPE_INT32:
        case FieldDescriptor::TYPE_INT64:
        case FieldDescriptor::TYPE_SFIXED32:
        case FieldDescriptor::TYPE_SFIXED64:
        case FieldDescriptor::TYPE_SINT32:
        case FieldDescriptor::TYPE_SINT64:
            return true;
        default:
            return false;
    }
}

bool IsUnsignedIntegralType(FieldDescriptor::Type type)
{
    switch (type) {
        case FieldDescriptor::TYPE_UINT32:
        case FieldDescriptor::TYPE_UINT64:
        case FieldDescriptor::TYPE_FIXED64:
        case FieldDescriptor::TYPE_FIXED32:
            return true;
        default:
            return false;
    }
}

bool IsStringType(FieldDescriptor::Type type)
{
    switch (type) {
        case FieldDescriptor::TYPE_BYTES:
        case FieldDescriptor::TYPE_STRING:
            return true;
        default:
            return false;
    }
}

bool IsMapKeyType(FieldDescriptor::Type type)
{
    return
        IsStringType(type) ||
        IsSignedIntegralType(type) ||
        IsUnsignedIntegralType(type);
}

TString ToUnderscoreCase(const TString& protobufName)
{
    TStringBuilder builder;
    for (auto ch : protobufName) {
        if (isupper(ch)) {
            if (builder.GetLength() > 0 && builder.GetBuffer()[builder.GetLength() - 1] != '_') {
                builder.AppendChar('_');
            }
            builder.AppendChar(tolower(ch));
        } else {
            builder.AppendChar(ch);
        }
    }
    return builder.Flush();
}

TString DeriveYsonName(const TString& protobufName, const google::protobuf::FileDescriptor* fileDescriptor)
{
    if (fileDescriptor->options().GetExtension(NYT::NYson::NProto::derive_underscore_case_names)) {
        return ToUnderscoreCase(protobufName);
    } else {
        return protobufName;
    }
}

////////////////////////////////////////////////////////////////////////////////

struct TProtobufInteropConfigSingleton
{
    TAtomicIntrusivePtr<TProtobufInteropConfig> Config{New<TProtobufInteropConfig>()};
};

TProtobufInteropConfigSingleton* GlobalProtobufInteropConfig()
{
    return LeakySingleton<TProtobufInteropConfigSingleton>();
}

TProtobufInteropConfigPtr GetProtobufInteropConfig()
{
    return GlobalProtobufInteropConfig()->Config.Acquire();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

void SetProtobufInteropConfig(TProtobufInteropConfigPtr config)
{
    GlobalProtobufInteropConfig()->Config.Store(std::move(config));
}

void WriteSchema(const TProtobufEnumType* enumType, IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

class TProtobufTypeRegistry
{
public:
    //! This method is called while reflecting types.
    TStringBuf GetYsonName(const FieldDescriptor* descriptor)
    {
        VERIFY_SPINLOCK_AFFINITY(Lock_);

        return GetYsonNameFromDescriptor(
            descriptor,
            descriptor->options().GetExtension(NYT::NYson::NProto::field_name));
    }

    //! This method is called while reflecting types.
    std::vector<TStringBuf> GetYsonNameAliases(const FieldDescriptor* descriptor)
    {
        VERIFY_SPINLOCK_AFFINITY(Lock_);

        std::vector<TStringBuf> aliases;
        auto extensions = descriptor->options().GetRepeatedExtension(NYT::NYson::NProto::field_name_alias);
        for (const auto& alias : extensions) {
            aliases.push_back(InternString(alias));
        }
        return aliases;
    }

    //! This method is called while reflecting types.
    TStringBuf GetYsonLiteral(const EnumValueDescriptor* descriptor)
    {
        VERIFY_SPINLOCK_AFFINITY(Lock_);

        return GetYsonNameFromDescriptor(
            descriptor,
            descriptor->options().GetExtension(NYT::NYson::NProto::enum_value_name));
    }

    const TProtobufMessageType* ReflectMessageType(const Descriptor* descriptor)
    {
        if (auto* result = MessageTypeSyncMap_.Find(descriptor)) {
            return *result;
        }

        auto guard = Guard(Lock_);
        Initialize();
        return ReflectMessageTypeInternal(descriptor);
    }

    const TProtobufEnumType* ReflectEnumType(const EnumDescriptor* descriptor)
    {
        if (auto* result = EnumTypeSyncMap_.Find(descriptor)) {
            return *result;
        }

        auto guard = Guard(Lock_);
        Initialize();
        return ReflectEnumTypeInternal(descriptor);
    }

    static TProtobufTypeRegistry* Get()
    {
        return Singleton<TProtobufTypeRegistry>();
    }

    using TRegisterAction = std::function<void()>;

    //! This method is called during static initialization and is not expected to be called during runtime.
    //! That is why there is no synchronization within.
    /*!
     *  Be cautious trying to acquire fork-aware spin lock during static initialization:
     *  fork-awareness is provided by the static fork-lock, acquiring may cause dependency within static initialization.
     */
    void AddRegisterAction(TRegisterAction action)
    {
        RegisterActions_.push_back(std::move(action));
    }

    //! This method is called during static initialization and is not expected to be called during runtime.
    void RegisterMessageTypeConverter(
        const Descriptor* descriptor,
        const TProtobufMessageConverter& converter)
    {
        EmplaceOrCrash(MessageTypeConverterMap_, descriptor, converter);
    }

    //! This method is called during static initialization and is not expected to be called during runtime.
    void RegisterMessageBytesFieldConverter(
        const Descriptor* descriptor,
        int fieldNumber,
        const TProtobufMessageBytesFieldConverter& converter)
    {
        EmplaceOrCrash(MessageFieldConverterMap_, std::pair(descriptor, fieldNumber), converter);
    }

    //! This method is called while reflecting types.
    std::optional<TProtobufMessageConverter> FindMessageTypeConverter(
        const Descriptor* descriptor) const
    {
        // No need to call Initialize: it has been already called within Reflect*Type higher up the stack.
        VERIFY_SPINLOCK_AFFINITY(Lock_);

        auto it = MessageTypeConverterMap_.find(descriptor);
        if (it == MessageTypeConverterMap_.end()) {
            return std::nullopt;
        } else {
            return it->second;
        }
    }

    //! This method is called while reflecting types.
    std::optional<TProtobufMessageBytesFieldConverter> FindMessageBytesFieldConverter(
        const Descriptor* descriptor,
        int fieldIndex) const
    {
        // No need to call Initialize: it has been already called within Reflect*Type higher up the stack.
        VERIFY_SPINLOCK_AFFINITY(Lock_);

        auto fieldNumber = descriptor->field(fieldIndex)->number();
        auto it = MessageFieldConverterMap_.find(std::pair(descriptor, fieldNumber));
        if (it == MessageFieldConverterMap_.end()) {
            return std::nullopt;
        } else {
            return it->second;
        }
    }

    // These are called while reflecting the types recursively;
    // the caller must be holding Lock_.
    const TProtobufMessageType* ReflectMessageTypeInternal(const Descriptor* descriptor);
    const TProtobufEnumType* ReflectEnumTypeInternal(const EnumDescriptor* descriptor);

private:
    Y_DECLARE_SINGLETON_FRIEND()
    TProtobufTypeRegistry() = default;

    void Initialize() const
    {
        if (!RegisterActions_.empty()) {
            for (const auto& action : RegisterActions_) {
                action();
            }
            RegisterActions_.clear();
        }
    }

    template <class TDescriptor>
    TStringBuf GetYsonNameFromDescriptor(const TDescriptor* descriptor, const TString& annotatedName)
    {
        auto ysonName = annotatedName ? annotatedName : DeriveYsonName(descriptor->name(), descriptor->file());
        return InternString(ysonName);
    }

    TStringBuf InternString(const TString& str)
    {
        VERIFY_SPINLOCK_AFFINITY(Lock_);

        return *InternedStrings_.emplace(str).first;
    }

private:
    template <class TKey, class TValue>
    using TForkAwareSyncMap = TSyncMap<
        TKey,
        TValue,
        THash<TKey>,
        TEqualTo<TKey>,
        TForkAwareSpinLock
    >;

    YT_DECLARE_SPIN_LOCK(TForkAwareSpinLock, Lock_);
    THashMap<const Descriptor*, std::unique_ptr<TProtobufMessageType>> MessageTypeMap_;
    TForkAwareSyncMap<const Descriptor*, const TProtobufMessageType*> MessageTypeSyncMap_;
    THashMap<const EnumDescriptor*, std::unique_ptr<TProtobufEnumType>> EnumTypeMap_;
    TForkAwareSyncMap<const EnumDescriptor*, const TProtobufEnumType*> EnumTypeSyncMap_;

    THashMap<const Descriptor*, TProtobufMessageConverter> MessageTypeConverterMap_;
    THashMap<std::pair<const Descriptor*, int>, TProtobufMessageBytesFieldConverter> MessageFieldConverterMap_;

    THashSet<TString> InternedStrings_;

    mutable std::vector<TRegisterAction> RegisterActions_;
};

////////////////////////////////////////////////////////////////////////////////

class TProtobufField
{
public:
    TProtobufField(TProtobufTypeRegistry* registry, const FieldDescriptor* descriptor)
        : Underlying_(descriptor)
        , YsonName_(registry->GetYsonName(descriptor))
        , YsonNameAliases_(registry->GetYsonNameAliases(descriptor))
        , MessageType_(descriptor->type() == FieldDescriptor::TYPE_MESSAGE ? registry->ReflectMessageTypeInternal(
            descriptor->message_type()) : nullptr)
        , EnumType_(descriptor->type() == FieldDescriptor::TYPE_ENUM ? registry->ReflectEnumTypeInternal(
            descriptor->enum_type()) : nullptr)
        , YsonString_(descriptor->options().GetExtension(NYT::NYson::NProto::yson_string))
        , YsonMap_(descriptor->options().GetExtension(NYT::NYson::NProto::yson_map))
        , Required_(descriptor->options().GetExtension(NYT::NYson::NProto::required))
        , Converter_(registry->FindMessageBytesFieldConverter(descriptor->containing_type(), descriptor->index()))
        , EnumYsonStorageType_(descriptor->options().HasExtension(NYT::NYson::NProto::enum_yson_storage_type) ?
            std::optional(descriptor->options().GetExtension(NYT::NYson::NProto::enum_yson_storage_type)) :
            std::nullopt)
    {
        if (YsonMap_ && !descriptor->is_map()) {
            THROW_ERROR_EXCEPTION("Field %v is not a map and cannot be annotated with \"yson_map\" option",
                GetFullName());
        }

        if (YsonMap_) {
            const auto* keyField = descriptor->message_type()->FindFieldByNumber(ProtobufMapKeyFieldNumber);
            if (!IsMapKeyType(keyField->type())) {
                THROW_ERROR_EXCEPTION("Map field %v has invalid key type",
                    GetFullName());
            }
        }

        if (Converter_ && GetType() != FieldDescriptor::Type::TYPE_BYTES) {
            THROW_ERROR_EXCEPTION("Field %v with custom converter has invalid type, only bytes fields are allowed",
                GetFullName());
        }
    }

    ui32 GetTag() const
    {
        return google::protobuf::internal::WireFormat::MakeTag(Underlying_);
    }

    const TString& GetFullName() const
    {
        return Underlying_->full_name();
    }

    TStringBuf GetYsonName() const
    {
        return YsonName_;
    }

    const std::vector<TStringBuf>& GetYsonNameAliases() const
    {
        return YsonNameAliases_;
    }

    int GetNumber() const
    {
        return Underlying_->number();
    }

    FieldDescriptor::Type GetType() const
    {
        return Underlying_->type();
    }

    const char* GetTypeName() const
    {
        return Underlying_->type_name();
    }

    bool IsRepeated() const
    {
        return Underlying_->is_repeated() && !IsYsonMap();
    }

    bool IsPacked() const
    {
        return Underlying_->is_packed() && !IsYsonMap();
    }

    bool IsRequired() const
    {
        return Underlying_->is_required() || Required_;
    }

    bool IsOptional() const
    {
        return Underlying_->is_optional() && !Required_;
    }

    bool IsMessage() const
    {
        return MessageType_ != nullptr;
    }

    bool IsYsonString() const
    {
        return YsonString_;
    }

    bool IsYsonMap() const
    {
        return YsonMap_;
    }

    const TProtobufField* GetYsonMapKeyField() const;
    const TProtobufField* GetYsonMapValueField() const;

    const TProtobufMessageType* GetMessageType() const
    {
        return MessageType_;
    }

    const TProtobufEnumType* GetEnumType() const
    {
        return EnumType_;
    }

    TProtobufElement GetElement(bool insideRepeated) const;

    const std::optional<TProtobufMessageBytesFieldConverter>& GetBytesFieldConverter() const
    {
        return Converter_;
    }

    EEnumYsonStorageType GetEnumYsonStorageType() const
    {
        if (EnumYsonStorageType_) {
            switch (*EnumYsonStorageType_) {
                case NYT::NYson::NProto::EEnumYsonStorageType::EYST_STRING:
                    return EEnumYsonStorageType::String;
                case NYT::NYson::NProto::EEnumYsonStorageType::EYST_INT:
                    return EEnumYsonStorageType::Int;
            }
        }

        auto config = GetProtobufInteropConfig();
        return config->DefaultEnumYsonStorageType;
    }

    void WriteSchema(IYsonConsumer* consumer) const
    {
        if (IsYsonMap()) {
            BuildYsonFluently(consumer)
                .BeginMap()
                    .Item("type_name").Value("dict")
                    .Item("key").Do([&] (auto fluent) {
                        GetYsonMapKeyField()->WriteSchema(fluent.GetConsumer());
                    })
                    .Item("value").Do([&] (auto fluent) {
                        GetYsonMapValueField()->WriteSchema(fluent.GetConsumer());
                    })
                .EndMap();

            return;
        }
        if (IsRepeated()) {
            consumer->OnBeginMap();
            consumer->OnKeyedItem("type_name");
            consumer->OnStringScalar("list");
            consumer->OnKeyedItem("item");
        }

        switch (GetType()) {
            case FieldDescriptor::TYPE_INT32:
            case FieldDescriptor::TYPE_FIXED32:
            case FieldDescriptor::TYPE_UINT32:
                consumer->OnStringScalar("uint32");
                break;
            case FieldDescriptor::TYPE_INT64:
            case FieldDescriptor::TYPE_FIXED64:
            case FieldDescriptor::TYPE_UINT64:
                consumer->OnStringScalar("uint64");
                break;
            case FieldDescriptor::TYPE_SINT32:
            case FieldDescriptor::TYPE_SFIXED32:
                consumer->OnStringScalar("int32");
                break;
            case FieldDescriptor::TYPE_SINT64:
            case FieldDescriptor::TYPE_SFIXED64:
                consumer->OnStringScalar("int64");
                break;
            case FieldDescriptor::TYPE_BOOL:
                consumer->OnStringScalar("bool");
                break;
            case FieldDescriptor::TYPE_FLOAT:
                consumer->OnStringScalar("float");
                break;
            case FieldDescriptor::TYPE_DOUBLE:
                consumer->OnStringScalar("double");
                break;
            case FieldDescriptor::TYPE_STRING:
                consumer->OnStringScalar("utf8");
                break;
            case FieldDescriptor::TYPE_BYTES:
                consumer->OnStringScalar("string");
                break;
            case FieldDescriptor::TYPE_ENUM:
                NYson::WriteSchema(GetEnumType(), consumer);
                break;
            case FieldDescriptor::TYPE_MESSAGE:
                NYson::WriteSchema(GetMessageType(), consumer);
                break;
            default:
                break;
        }
        if (IsRepeated()) {
            consumer->OnEndMap();
        }
    }

private:
    const FieldDescriptor* const Underlying_;
    const TStringBuf YsonName_;
    const std::vector<TStringBuf> YsonNameAliases_;
    const TProtobufMessageType* MessageType_;
    const TProtobufEnumType* EnumType_;
    const bool YsonString_;
    const bool YsonMap_;
    const bool Required_;
    const std::optional<TProtobufMessageBytesFieldConverter> Converter_;
    const std::optional<NYT::NYson::NProto::EEnumYsonStorageType> EnumYsonStorageType_;
};

////////////////////////////////////////////////////////////////////////////////

class TProtobufMessageType
{
public:
    TProtobufMessageType(TProtobufTypeRegistry* registry, const Descriptor* descriptor)
        : Registry_(registry)
        , Underlying_(descriptor)
        , AttributeDictionary_(descriptor->options().GetExtension(NYT::NYson::NProto::attribute_dictionary))
        , Converter_(registry->FindMessageTypeConverter(descriptor))
    { }

    void Build()
    {
        for (int index = 0; index < Underlying_->field_count(); ++index) {
            const auto* fieldDescriptor = Underlying_->field(index);
            RegisterField(fieldDescriptor);
        }

        auto descriptorPool = Underlying_->file()->pool();
        std::vector<const FieldDescriptor*> extensionFieldDescriptors;
        descriptorPool->FindAllExtensions(Underlying_, &extensionFieldDescriptors);
        for (const auto* extensionFieldDescriptor : extensionFieldDescriptors) {
            RegisterField(extensionFieldDescriptor);
        }

        for (int index = 0; index < Underlying_->reserved_name_count(); ++index) {
            ReservedFieldNames_.insert(Underlying_->reserved_name(index));
        }
    }

    const Descriptor* GetUnderlying() const
    {
        return Underlying_;
    }

    bool IsAttributeDictionary() const
    {
        return AttributeDictionary_;
    }

    const TString& GetFullName() const
    {
        return Underlying_->full_name();
    }

    const std::vector<int>& GetRequiredFieldNumbers() const
    {
        return RequiredFieldNumbers_;
    }

    const std::optional<TProtobufMessageConverter>& GetConverter() const
    {
        return Converter_;
    }

    bool IsReservedFieldName(TStringBuf name) const
    {
        return ReservedFieldNames_.contains(name);
    }

    bool IsReservedFieldNumber(int number) const
    {
        for (int index = 0; index < Underlying_->reserved_range_count(); ++index) {
            if (number >= Underlying_->reserved_range(index)->start &&
                number <= Underlying_->reserved_range(index)->end)
            {
                return true;
            }
        }
        return false;
    }


    const TProtobufField* FindFieldByName(TStringBuf name) const
    {
        auto it = NameToField_.find(name);
        return it == NameToField_.end() ? nullptr : it->second;
    }

    const TProtobufField* FindFieldByNumber(int number) const
    {
        auto it = NumberToField_.find(number);
        return it == NumberToField_.end() ? nullptr : it->second;
    }

    const TProtobufField* GetFieldByNumber(int number) const
    {
        const auto* field = FindFieldByNumber(number);
        YT_VERIFY(field);
        return field;
    }

    TProtobufElement GetElement() const
    {
        if (IsAttributeDictionary()) {
            return std::make_unique<TProtobufAttributeDictionaryElement>(TProtobufAttributeDictionaryElement{
                this
            });
        } else {
            return std::make_unique<TProtobufMessageElement>(TProtobufMessageElement{
                this
            });
        }
    }

    void WriteSchema(IYsonConsumer* consumer) const
    {
        BuildYsonFluently(consumer).BeginMap()
            .Item("type_name").Value("struct")
            .Item("members").DoListFor(0, Underlying_->field_count(), [&] (auto fluent, int index) {
                auto* field = GetFieldByNumber(Underlying_->field(index)->number());
                fluent.Item()
                    .BeginMap()
                        .Item("name").Value(field->GetYsonName())
                        .Item("type").Do([&] (auto fluent) {
                            field->WriteSchema(fluent.GetConsumer());
                        })
                        .DoIf(!field->IsYsonMap() && !field->IsRepeated() && !field->IsOptional(), [] (auto fluent) {
                            fluent.Item("required").Value(true);
                        })
                    .EndMap();
            })
            .EndMap();
    }

private:
    TProtobufTypeRegistry* const Registry_;
    const Descriptor* const Underlying_;
    const bool AttributeDictionary_;

    std::vector<std::unique_ptr<TProtobufField>> Fields_;
    std::vector<int> RequiredFieldNumbers_;
    THashMap<TStringBuf, const TProtobufField*> NameToField_;
    THashMap<int, const TProtobufField*> NumberToField_;
    THashSet<TString> ReservedFieldNames_;
    std::optional<TProtobufMessageConverter> Converter_;

    void RegisterField(const FieldDescriptor* fieldDescriptor)
    {
        auto fieldHolder = std::make_unique<TProtobufField>(Registry_, fieldDescriptor);
        auto* field = fieldHolder.get();
        if (field->IsRequired()) {
            RequiredFieldNumbers_.push_back(field->GetNumber());
        }
        YT_VERIFY(NameToField_.emplace(field->GetYsonName(), field).second);
        for (auto name : field->GetYsonNameAliases()) {
            YT_VERIFY(NameToField_.emplace(name, field).second);
        }
        YT_VERIFY(NumberToField_.emplace(field->GetNumber(), field).second);
        Fields_.push_back(std::move(fieldHolder));
    }
};

////////////////////////////////////////////////////////////////////////////////

const TProtobufField* TProtobufField::GetYsonMapKeyField() const
{
    return MessageType_->GetFieldByNumber(ProtobufMapKeyFieldNumber);
}

const TProtobufField* TProtobufField::GetYsonMapValueField() const
{
    return MessageType_->GetFieldByNumber(ProtobufMapValueFieldNumber);
}

TProtobufElement TProtobufField::GetElement(bool insideRepeated) const
{
    if (IsRepeated() && !insideRepeated) {
        return std::make_unique<TProtobufRepeatedElement>(TProtobufRepeatedElement{
            GetElement(/*insideRepeated*/ true)
        });
    } else if (IsYsonMap()) {
        auto element = GetYsonMapKeyField()->GetElement(/*insideRepeated*/ false);
        auto* keyElement = std::get_if<std::unique_ptr<TProtobufScalarElement>>(&element);
        YT_VERIFY(keyElement);
        return std::make_unique<TProtobufMapElement>(TProtobufMapElement{
            .KeyElement = std::move(**keyElement),
            .Element = GetYsonMapValueField()->GetElement(/*insideRepeated*/ false)
        });
    } else if (IsYsonString()) {
        return std::make_unique<TProtobufAnyElement>();
    } else if (IsMessage()) {
        return std::make_unique<TProtobufMessageElement>(TProtobufMessageElement{
            MessageType_
        });
    } else {
        return std::make_unique<TProtobufScalarElement>(TProtobufScalarElement{
            static_cast<TProtobufScalarElement::TType>(GetType()),
            GetEnumYsonStorageType()
        });
    }
}

////////////////////////////////////////////////////////////////////////////////

class TProtobufEnumType
{
public:
    TProtobufEnumType(TProtobufTypeRegistry* registry, const EnumDescriptor* descriptor)
        : Registry_(registry)
        , Underlying_(descriptor)
    { }

    void Build()
    {
        for (int index = 0; index < Underlying_->value_count(); ++index) {
            const auto* valueDescriptor = Underlying_->value(index);
            auto literal = Registry_->GetYsonLiteral(valueDescriptor);
            int number = valueDescriptor->number();
            // Allow aliases, i.e. different literals for the same tag or same literal for different tags.
            // The first literal is selected as canonical for each tag.
            YT_VERIFY(LiteralToValue_.try_emplace(literal, number).first->second == number);
            ValueToLiteral_.try_emplace(number, literal);
        }
    }

    const EnumDescriptor* GetUnderlying() const
    {
        return Underlying_;
    }

    const TString& GetFullName() const
    {
        return Underlying_->full_name();
    }

    std::optional<int> FindValueByLiteral(TStringBuf literal) const
    {
        auto it = LiteralToValue_.find(literal);
        return it == LiteralToValue_.end() ? std::nullopt : std::make_optional(it->second);
    }

    TStringBuf FindLiteralByValue(int value) const
    {
        auto it = ValueToLiteral_.find(value);
        return it == ValueToLiteral_.end() ? TStringBuf() : it->second;
    }

    void WriteSchema(IYsonConsumer* consumer) const
    {
        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("type_name").Value("enum")
                .Item("enum_name").Value(Underlying_->name())
                .Item("values").DoListFor(0, Underlying_->value_count(), [&] (auto fluent, int index) {
                    fluent.Item().Value(FindLiteralByValue(Underlying_->value(index)->number()));
                })
            .EndMap();
    }

private:
    TProtobufTypeRegistry* const Registry_;
    const EnumDescriptor* const Underlying_;

    THashMap<TStringBuf, int> LiteralToValue_;
    THashMap<int, TStringBuf> ValueToLiteral_;
};

////////////////////////////////////////////////////////////////////////////////

const TProtobufMessageType* TProtobufTypeRegistry::ReflectMessageTypeInternal(const Descriptor* descriptor)
{
    VERIFY_SPINLOCK_AFFINITY(Lock_);

    TProtobufMessageType* type;
    auto it = MessageTypeMap_.find(descriptor);
    if (it == MessageTypeMap_.end()) {
        auto typeHolder = std::make_unique<TProtobufMessageType>(this, descriptor);
        type = typeHolder.get();
        it = MessageTypeMap_.emplace(descriptor, std::move(typeHolder)).first;
        type->Build();
    } else {
        type = it->second.get();
    }

    YT_VERIFY(*MessageTypeSyncMap_.FindOrInsert(descriptor, [&] { return type; }).first == type);

    return type;
}

const TProtobufEnumType* TProtobufTypeRegistry::ReflectEnumTypeInternal(const EnumDescriptor* descriptor)
{
    VERIFY_SPINLOCK_AFFINITY(Lock_);

    TProtobufEnumType* type;
    auto it = EnumTypeMap_.find(descriptor);
    if (it == EnumTypeMap_.end()) {
        auto typeHolder = std::make_unique<TProtobufEnumType>(this, descriptor);
        type = typeHolder.get();
        it = EnumTypeMap_.emplace(descriptor, std::move(typeHolder)).first;
        type->Build();
    } else {
        type = it->second.get();
    }

    YT_VERIFY(*EnumTypeSyncMap_.FindOrInsert(descriptor, [&] { return type; }).first == type);

    return type;
}

////////////////////////////////////////////////////////////////////////////////

const TProtobufMessageType* ReflectProtobufMessageType(const Descriptor* descriptor)
{
    return TProtobufTypeRegistry::Get()->ReflectMessageType(descriptor);
}

const TProtobufEnumType* ReflectProtobufEnumType(const EnumDescriptor* descriptor)
{
    return TProtobufTypeRegistry::Get()->ReflectEnumType(descriptor);
}

const ::google::protobuf::Descriptor* UnreflectProtobufMessageType(const TProtobufMessageType* type)
{
    return type->GetUnderlying();
}

const ::google::protobuf::EnumDescriptor* UnreflectProtobufEnumType(const TProtobufEnumType* type)
{
    return type->GetUnderlying();
}

std::optional<int> FindProtobufEnumValueByLiteralUntyped(
    const TProtobufEnumType* type,
    TStringBuf literal)
{
    return type->FindValueByLiteral(literal);
}

TStringBuf FindProtobufEnumLiteralByValueUntyped(
    const TProtobufEnumType* type,
    int value)
{
    return type->FindLiteralByValue(value);
}

int ConvertToProtobufEnumValueUntyped(
    const TProtobufEnumType* type,
    const NYTree::INodePtr& node)
{
    switch (node->GetType()) {
        case NYTree::ENodeType::Int64:
        case NYTree::ENodeType::Uint64: {
            int value = NYTree::ConvertTo<int>(node);
            THROW_ERROR_EXCEPTION_UNLESS(type->FindLiteralByValue(value),
                "Unknown value %v of enum %Qv",
                value,
                type->GetUnderlying()->name());
            return value;
        }
        case NYTree::ENodeType::String: {
            const TString& literal = node->AsString()->GetValue();
            auto value = type->FindValueByLiteral(literal);
            THROW_ERROR_EXCEPTION_UNLESS(value,
                "Unknown value %Qv of enum %Qv",
                literal,
                type->GetUnderlying()->name());
            return *value;
        }
        default:
            THROW_ERROR_EXCEPTION("Expected integral or string, got %v",
                node->GetType());
    }
}

////////////////////////////////////////////////////////////////////////////////

class TProtobufTranscoderBase
{
protected:
    TYPathStack YPathStack_;


    void SortFields(TFieldNumberList* numbers)
    {
        std::sort(numbers->begin(), numbers->end());
    }

    void ValidateRequiredFieldsPresent(const TProtobufMessageType* type, const TFieldNumberList& numbers)
    {
        if (numbers.size() == type->GetRequiredFieldNumbers().size()) {
            return;
        }

        for (auto number : type->GetRequiredFieldNumbers()) {
            if (!std::binary_search(numbers.begin(), numbers.end(), number)) {
                const auto* field = type->FindFieldByNumber(number);
                YT_VERIFY(field);
                YPathStack_.Push(TString{field->GetYsonName()});
                THROW_ERROR_EXCEPTION("Missing required field %v",
                    YPathStack_.GetHumanReadablePath())
                    << TErrorAttribute("ypath", YPathStack_.GetPath())
                    << TErrorAttribute("proto_type", type->GetFullName())
                    << TErrorAttribute("proto_field", field->GetFullName());
            }
        }

        YT_ABORT();
    }

    void ValidateNoFieldDuplicates(const TProtobufMessageType* type, const TFieldNumberList& numbers)
    {
        for (auto index = 0; index + 1 < std::ssize(numbers); ++index) {
            if (numbers[index] == numbers[index + 1]) {
                const auto* field = type->GetFieldByNumber(numbers[index]);
                YPathStack_.Push(TString{field->GetYsonName()});
                THROW_ERROR_EXCEPTION("Duplicate field %v",
                    YPathStack_.GetHumanReadablePath())
                    << TErrorAttribute("ypath", YPathStack_.GetPath())
                    << TErrorAttribute("proto_type", type->GetFullName());
            }
        }
    }

    void ValidateString(TStringBuf data, TStringBuf fieldFullName)
    {
        auto config = GetProtobufInteropConfig();
        if (config->Utf8Check == EUtf8Check::Disable || IsUtf(data)) {
            return;
        }
        switch (config->Utf8Check) {
            case EUtf8Check::Disable:
                return;
            case EUtf8Check::LogOnFail:
                YT_LOG_WARNING("String field got non UTF-8 value (Path: %v, Value: %v)",
                    YPathStack_.GetHumanReadablePath(),
                    data);
                return;
            case EUtf8Check::ThrowOnFail:
                THROW_ERROR_EXCEPTION("Non UTF-8 value in string field %v",
                    YPathStack_.GetHumanReadablePath())
                    << TErrorAttribute("non_utf8_string", data)
                    << TErrorAttribute("ypath", YPathStack_.GetPath())
                    << TErrorAttribute("proto_field", fieldFullName);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TProtobufWriter
    : public TProtobufTranscoderBase
    , public TForwardingYsonConsumer
{
public:
    TProtobufWriter(
        ZeroCopyOutputStream* outputStream,
        const TProtobufMessageType* rootType,
        TProtobufWriterOptions options)
        : OutputStream_(outputStream)
        , RootType_(rootType)
        , Options_(std::move(options))
        , BodyOutputStream_(&BodyString_)
        , BodyCodedStream_(&BodyOutputStream_)
        , AttributeValueStream_(AttributeValue_)
        , AttributeValueWriter_(&AttributeValueStream_)
        , YsonStringStream_(YsonString_)
        , YsonStringWriter_(&YsonStringStream_)
        , UnknownYsonFieldValueStringStream_(UnknownYsonFieldValueString_)
        , UnknownYsonFieldValueStringWriter_(&UnknownYsonFieldValueStringStream_)
        , ForwardingUnknownYsonFieldValueWriter_(UnknownYsonFieldValueStringWriter_, Options_.UnknownYsonFieldModeResolver)
        , TreeBuilder_(CreateBuilderFromFactory(GetEphemeralNodeFactory()))
    { }

private:
    ZeroCopyOutputStream* const OutputStream_;
    const TProtobufMessageType* const RootType_;
    const TProtobufWriterOptions Options_;

    TString BodyString_;
    google::protobuf::io::StringOutputStream BodyOutputStream_;
    google::protobuf::io::CodedOutputStream BodyCodedStream_;

    struct TTypeEntry
    {
        explicit TTypeEntry(const TProtobufMessageType* type)
            : Type(type)
        { }

        const TProtobufMessageType* Type;
        TFieldNumberList RequiredFieldNumbers;
        TFieldNumberList NonRequiredFieldNumbers;
        int CurrentMapIndex = 0;
    };
    std::vector<TTypeEntry> TypeStack_;

    std::vector<int> NestedIndexStack_;

    struct TFieldEntry
    {
        explicit TFieldEntry(const TProtobufField* field)
            : Field(field)
        { }

        const TProtobufField* Field;
        int CurrentListIndex = 0;
        bool ParsingList = false;
        bool ParsingYsonMapFromList = false;
    };
    std::vector<TFieldEntry> FieldStack_;

    struct TNestedMessageEntry
    {
        TNestedMessageEntry(int lo, int hi)
            : Lo(lo)
            , Hi(hi)
        { }

        int Lo;
        int Hi;
        int ByteSize = -1;
    };
    std::vector<TNestedMessageEntry> NestedMessages_;

    TString AttributeKey_;
    TString AttributeValue_;
    TStringOutput AttributeValueStream_;
    TBufferedBinaryYsonWriter AttributeValueWriter_;

    TString YsonString_;
    TStringOutput YsonStringStream_;
    TBufferedBinaryYsonWriter YsonStringWriter_;

    TString SerializedMessage_;
    TString BytesString_;

    TString UnknownYsonFieldKey_;
    TString UnknownYsonFieldValueString_;
    TStringOutput UnknownYsonFieldValueStringStream_;
    TBufferedBinaryYsonWriter UnknownYsonFieldValueStringWriter_;
    TForwardingUnknownYsonFieldValueWriter ForwardingUnknownYsonFieldValueWriter_;

    std::unique_ptr<ITreeBuilder> TreeBuilder_;

    void OnMyStringScalar(TStringBuf value) override
    {
        WriteScalar([&] {
            const auto* field = FieldStack_.back().Field;
            switch (field->GetType()) {
                case FieldDescriptor::TYPE_STRING:
                    ValidateString(value, field->GetFullName());
                case FieldDescriptor::TYPE_BYTES:
                    BodyCodedStream_.WriteVarint64(value.length());
                    BodyCodedStream_.WriteRaw(value.begin(), static_cast<int>(value.length()));
                    break;

                case FieldDescriptor::TYPE_ENUM: {
                    const auto* enumType = field->GetEnumType();
                    auto optionalValue = enumType->FindValueByLiteral(value);
                    if (!optionalValue) {
                        THROW_ERROR_EXCEPTION("Field %v cannot have value %Qv",
                            YPathStack_.GetHumanReadablePath(),
                            value)
                            << TErrorAttribute("ypath", YPathStack_.GetPath())
                            << TErrorAttribute("proto_type", enumType->GetFullName());
                    }
                    if (field->IsPacked()) {
                        BodyCodedStream_.WriteVarint64(BodyCodedStream_.VarintSize32SignExtended(*optionalValue));
                    }
                    BodyCodedStream_.WriteVarint32SignExtended(*optionalValue);
                    break;
                }

                default:
                    THROW_ERROR_EXCEPTION("Field %v cannot be parsed from \"string\" values",
                        YPathStack_.GetHumanReadablePath())
                        << TErrorAttribute("ypath", YPathStack_.GetPath())
                        << TErrorAttribute("proto_field", field->GetFullName());
            }
        });
    }

    void OnMyInt64Scalar(i64 value) override
    {
        OnIntegerScalar(value);
    }

    void OnMyUint64Scalar(ui64 value) override
    {
        OnIntegerScalar(value);
    }

    void OnMyDoubleScalar(double value) override
    {
        WriteScalar([&] {
            const auto* field = FieldStack_.back().Field;
            switch (field->GetType()) {
                case FieldDescriptor::TYPE_DOUBLE: {
                    auto encodedValue = WireFormatLite::EncodeDouble(value);
                    if (field->IsPacked()) {
                        BodyCodedStream_.WriteVarint64(sizeof(encodedValue));
                    }
                    BodyCodedStream_.WriteRaw(&encodedValue, sizeof(encodedValue));
                    break;
                }

                case FieldDescriptor::TYPE_FLOAT: {
                    auto encodedValue = WireFormatLite::EncodeFloat(value);
                    if (field->IsPacked()) {
                        BodyCodedStream_.WriteVarint64(sizeof(encodedValue));
                    }
                    BodyCodedStream_.WriteRaw(&encodedValue, sizeof(encodedValue));
                    break;
                }

                default:
                    THROW_ERROR_EXCEPTION("Field %v cannot be parsed from \"double\" values",
                        YPathStack_.GetHumanReadablePath())
                        << TErrorAttribute("ypath", YPathStack_.GetPath())
                        << TErrorAttribute("proto_field", field->GetFullName());
            }
        });
    }

    void OnMyBooleanScalar(bool value) override
    {
        WriteScalar([&] {
            const auto* field = FieldStack_.back().Field;
            auto type = field->GetType();
            if (type != FieldDescriptor::TYPE_BOOL) {
                THROW_ERROR_EXCEPTION("Field %v cannot be parsed from \"boolean\" values",
                    YPathStack_.GetHumanReadablePath())
                    << TErrorAttribute("ypath", YPathStack_.GetPath())
                    << TErrorAttribute("proto_field", field->GetFullName());
            }
            if (field->IsPacked()) {
                BodyCodedStream_.WriteVarint64(1);
            }
            BodyCodedStream_.WriteVarint32(value ? 1 : 0);
        });
    }

    void OnMyEntity() override
    {
        if (FieldStack_.empty()) {
            // This is the root.
            return;
        }
        FieldStack_.pop_back();
        YPathStack_.Pop();
    }

    void OnMyBeginList() override
    {
        ValidateNotRoot();

        const auto* field = FieldStack_.back().Field;
        if (field->IsYsonMap()) {
            // We do allow parsing map from lists to ease migration; cf. YT-11055.
            FieldStack_.back().ParsingYsonMapFromList = true;
        } else {
            ValidateRepeated();
        }
    }

    void OnMyListItem() override
    {
        YT_ASSERT(!TypeStack_.empty());
        int index = FieldStack_.back().CurrentListIndex++;
        FieldStack_.push_back(FieldStack_.back());
        FieldStack_.back().ParsingList = true;
        YPathStack_.Push(index);
        TryWriteCustomlyConvertibleType();
    }

    void OnMyEndList() override
    {
        YT_ASSERT(!TypeStack_.empty());
        FieldStack_.pop_back();
        YPathStack_.Pop();
    }

    void OnMyBeginMap() override
    {
        if (TypeStack_.empty()) {
            TypeStack_.emplace_back(RootType_);
            FieldStack_.emplace_back(nullptr);
            return;
        }

        const auto* field = FieldStack_.back().Field;
        TypeStack_.emplace_back(field->GetMessageType());

        if (!field->IsYsonMap() || FieldStack_.back().ParsingYsonMapFromList) {
            if (field->GetType() != FieldDescriptor::TYPE_MESSAGE) {
                THROW_ERROR_EXCEPTION("Field %v cannot be parsed from \"map\" values",
                    YPathStack_.GetHumanReadablePath())
                    << TErrorAttribute("ypath", YPathStack_.GetPath())
                    << TErrorAttribute("proto_field", field->GetFullName());
            }

            ValidateNotRepeated();
            WriteTag();
            BeginNestedMessage();
        }
    }

    void OnMyKeyedItem(TStringBuf key) override
    {
        TString keyData;
        if (Options_.ConvertSnakeToCamelCase) {
            keyData = UnderscoreCaseToCamelCase(key);
            key = keyData;
        }
        const auto* field = FieldStack_.back().Field;
        if (field && field->IsYsonMap() && !FieldStack_.back().ParsingYsonMapFromList) {
            OnMyKeyedItemYsonMap(field, key);
        } else {
            YT_ASSERT(!TypeStack_.empty());
            const auto* type = TypeStack_.back().Type;
            if (type->IsAttributeDictionary()) {
                OnMyKeyedItemAttributeDictionary(key);
            } else {
                OnMyKeyedItemRegular(key);
            }
        }
    }

    void OnMyKeyedItemYsonMap(const TProtobufField* field, TStringBuf key)
    {
        auto& typeEntry = TypeStack_.back();
        if (typeEntry.CurrentMapIndex > 0) {
            EndNestedMessage();
        }
        ++typeEntry.CurrentMapIndex;

        WriteTag();
        BeginNestedMessage();

        const auto* keyField = field->GetYsonMapKeyField();
        auto keyType = keyField->GetType();
        BodyCodedStream_.WriteTag(google::protobuf::internal::WireFormatLite::MakeTag(
            ProtobufMapKeyFieldNumber,
            WireFormatLite::WireTypeForFieldType(static_cast<WireFormatLite::FieldType>(keyType))));

        switch (keyType) {
            case FieldDescriptor::TYPE_SFIXED32:
            case FieldDescriptor::TYPE_SFIXED64:
            case FieldDescriptor::TYPE_SINT32:
            case FieldDescriptor::TYPE_SINT64:
            case FieldDescriptor::TYPE_INT32:
            case FieldDescriptor::TYPE_INT64: {
                i64 keyValue; // the widest singed integral type
                if (!TryFromString(key, keyValue)) {
                    THROW_ERROR_EXCEPTION("Cannot parse a signed integral key of map %v from %Qv",
                        YPathStack_.GetHumanReadablePath(),
                        key)
                        << TErrorAttribute("ypath", YPathStack_.GetPath())
                        << TErrorAttribute("proto_field", field->GetFullName());
                }
                WriteIntegerScalar(keyField, keyValue);
                break;
            }

            case FieldDescriptor::TYPE_UINT32:
            case FieldDescriptor::TYPE_UINT64:
            case FieldDescriptor::TYPE_FIXED64:
            case FieldDescriptor::TYPE_FIXED32: {
                ui64 keyValue; // the widest unsigned integral type
                if (!TryFromString(key, keyValue)) {
                    THROW_ERROR_EXCEPTION("Cannot parse an unsigned integral key of map %v from %Qv",
                        YPathStack_.GetHumanReadablePath(),
                        key)
                        << TErrorAttribute("ypath", YPathStack_.GetPath())
                        << TErrorAttribute("proto_field", field->GetFullName());
                }
                WriteIntegerScalar(keyField, keyValue);
                break;
            }

            case FieldDescriptor::TYPE_STRING:
            case FieldDescriptor::TYPE_BYTES:
                BodyCodedStream_.WriteVarint64(key.length());
                BodyCodedStream_.WriteRaw(key.data(), static_cast<int>(key.length()));
                break;

            default:
                YT_ABORT();
        }

        const auto* valueField = field->GetYsonMapValueField();
        FieldStack_.emplace_back(valueField);
        YPathStack_.Push(TString(key));
        TryWriteCustomlyConvertibleType();
    }

    void OnMyKeyedItemRegular(TStringBuf key)
    {
        auto& typeEntry = TypeStack_.back();
        const auto* type = TypeStack_.back().Type;
        const auto* field = type->FindFieldByName(key);

        if (!field) {
            auto path = NYPath::YPathJoin(YPathStack_.GetPath(), key);
            auto unknownYsonFieldsMode = Options_.UnknownYsonFieldModeResolver(path);
            auto onFinishForwarding = [this] (auto& writer) {
                writer.Flush();
                BodyCodedStream_.WriteTag(google::protobuf::internal::WireFormatLite::MakeTag(UnknownYsonFieldNumber, WireFormatLite::WIRETYPE_LENGTH_DELIMITED));
                WriteKeyValuePair(UnknownYsonFieldKey_, UnknownYsonFieldValueString_);
            };
            if (unknownYsonFieldsMode == EUnknownYsonFieldsMode::Keep) {
                UnknownYsonFieldKey_ = TString(key);
                UnknownYsonFieldValueString_.clear();
                Forward(
                    &UnknownYsonFieldValueStringWriter_,
                    [this, onFinishForwarding] {
                        onFinishForwarding(UnknownYsonFieldValueStringWriter_);
                    });
                return;
            }

            if (unknownYsonFieldsMode == EUnknownYsonFieldsMode::Forward) {
                ForwardingUnknownYsonFieldValueWriter_.YPathStack() = YPathStack_;
                ForwardingUnknownYsonFieldValueWriter_.YPathStack().Push(TString(key));
                ForwardingUnknownYsonFieldValueWriter_.ResetMode();
                UnknownYsonFieldKey_ = TString(key);
                UnknownYsonFieldValueString_.clear();
                Forward(
                    &ForwardingUnknownYsonFieldValueWriter_,
                    [this, onFinishForwarding] {
                        onFinishForwarding(ForwardingUnknownYsonFieldValueWriter_);
                    });
                return;
            }

            if (unknownYsonFieldsMode == EUnknownYsonFieldsMode::Skip || type->IsReservedFieldName(key)) {
                Forward(GetNullYsonConsumer(), [] {});
                return;
            }

            THROW_ERROR_EXCEPTION("Unknown field %Qv at %v",
                key,
                YPathStack_.GetHumanReadablePath())
                << TErrorAttribute("ypath", YPathStack_.GetPath())
                << TErrorAttribute("proto_type", type->GetFullName());
        }

        auto number = field->GetNumber();
        ++typeEntry.CurrentMapIndex;
        if (field->IsRequired()) {
            typeEntry.RequiredFieldNumbers.push_back(number);
        } else {
            typeEntry.NonRequiredFieldNumbers.push_back(number);
        }
        FieldStack_.emplace_back(field);
        YPathStack_.Push(TString{field->GetYsonName()});

        if (field->IsYsonString()) {
            YsonString_.clear();
            Forward(&YsonStringWriter_, [this] {
                YsonStringWriter_.Flush();

                WriteScalar([this] {
                    BodyCodedStream_.WriteVarint64(YsonString_.length());
                    BodyCodedStream_.WriteRaw(YsonString_.begin(), static_cast<int>(YsonString_.length()));
                });
            });
        } else {
            TryWriteCustomlyConvertibleType();
        }
    }

    void OnMyKeyedItemAttributeDictionary(TStringBuf key)
    {
        AttributeKey_ = key;
        AttributeValue_.clear();
        Forward(&AttributeValueWriter_, [this] {
            AttributeValueWriter_.Flush();
            BodyCodedStream_.WriteTag(google::protobuf::internal::WireFormatLite::MakeTag(AttributeDictionaryAttributeFieldNumber, WireFormatLite::WIRETYPE_LENGTH_DELIMITED));
            WriteKeyValuePair(AttributeKey_, AttributeValue_);
        });
    }

    void OnMyEndMap() override
    {
        auto& typeEntry = TypeStack_.back();
        const auto* type = typeEntry.Type;

        const auto* field = FieldStack_.back().Field;
        if (field && field->IsYsonMap()  && !FieldStack_.back().ParsingYsonMapFromList) {
            if (typeEntry.CurrentMapIndex > 0) {
                EndNestedMessage();
            }

            TypeStack_.pop_back();
        } else {
            SortFields(&typeEntry.NonRequiredFieldNumbers);
            ValidateNoFieldDuplicates(type, typeEntry.NonRequiredFieldNumbers);

            SortFields(&typeEntry.RequiredFieldNumbers);
            ValidateNoFieldDuplicates(type, typeEntry.RequiredFieldNumbers);

            if (!Options_.SkipRequiredFields) {
                ValidateRequiredFieldsPresent(type, typeEntry.RequiredFieldNumbers);
            }

            TypeStack_.pop_back();
            if (TypeStack_.empty()) {
                Finish();
                return;
            }

            EndNestedMessage();
        }

        FieldStack_.pop_back();
        YPathStack_.Pop();
    }

    void ThrowAttributesNotSupported()
    {
        THROW_ERROR_EXCEPTION("Attributes are not supported")
            << TErrorAttribute("ypath", YPathStack_.GetPath());
    }

    void OnMyBeginAttributes() override
    {
        ThrowAttributesNotSupported();
    }

    void OnMyEndAttributes() override
    {
        ThrowAttributesNotSupported();
    }


    void BeginNestedMessage()
    {
        auto index =  static_cast<int>(NestedMessages_.size());
        NestedMessages_.emplace_back(BodyCodedStream_.ByteCount(), -1);
        NestedIndexStack_.push_back(index);
    }

    void EndNestedMessage()
    {
        int index = NestedIndexStack_.back();
        NestedIndexStack_.pop_back();
        YT_ASSERT(NestedMessages_[index].Hi == -1);
        NestedMessages_[index].Hi = BodyCodedStream_.ByteCount();
    }

    void Finish()
    {
        YT_VERIFY(YPathStack_.IsEmpty());
        YT_VERIFY(!FieldStack_.back().Field);

        BodyCodedStream_.Trim();

        int bodyLength = static_cast<int>(BodyString_.length());
        NestedMessages_.emplace_back(bodyLength, std::numeric_limits<int>::max());

        {
            int nestedIndex = 0;
            std::function<int(int, int)> computeByteSize = [&] (int lo, int hi) {
                auto position = lo;
                int result = 0;
                while (true) {
                    auto& nestedMessage = NestedMessages_[nestedIndex];

                    {
                        auto threshold = std::min(hi, nestedMessage.Lo);
                        result += (threshold - position);
                        position = threshold;
                    }

                    if (nestedMessage.Lo == position && nestedMessage.Hi < std::numeric_limits<int>::max()) {
                        ++nestedIndex;
                        int nestedResult = computeByteSize(nestedMessage.Lo, nestedMessage.Hi);
                        nestedMessage.ByteSize = nestedResult;
                        result += BodyCodedStream_.VarintSize32(static_cast<ui32>(nestedResult));
                        result += nestedResult;
                        position = nestedMessage.Hi;
                    } else {
                        break;
                    }
                }
                return result;
            };
            computeByteSize(0, bodyLength);
        }

        {
            int nestedIndex = 0;
            std::function<void(int, int)> write = [&] (int lo, int hi) {
                auto position = lo;
                while (true) {
                    const auto& nestedMessage = NestedMessages_[nestedIndex];

                    {
                        auto threshold = std::min(hi, nestedMessage.Lo);
                        if (threshold > position) {
                            WriteRaw(BodyString_.data() + position, threshold - position);
                        }
                        position = threshold;
                    }

                    if (nestedMessage.Lo == position && nestedMessage.Hi < std::numeric_limits<int>::max()) {
                        ++nestedIndex;
                        char buf[16];
                        auto length = WriteVarUint64(buf, nestedMessage.ByteSize);
                        WriteRaw(buf, length);
                        write(nestedMessage.Lo, nestedMessage.Hi);
                        position = nestedMessage.Hi;
                    } else {
                        break;
                    }
                }
            };
            write(0, bodyLength);
        }
    }

    void WriteRaw(const char* data, int size)
    {
        while (true) {
            void* chunkData;
            int chunkSize;
            if (!OutputStream_->Next(&chunkData, &chunkSize)) {
                THROW_ERROR_EXCEPTION("Error writing to output stream");
            }
            auto bytesToWrite = std::min(chunkSize, size);
            ::memcpy(chunkData, data, bytesToWrite);
            if (bytesToWrite == size) {
                OutputStream_->BackUp(chunkSize - size);
                break;
            }
            data += bytesToWrite;
            size -= bytesToWrite;
        }
    }


    void ValidateNotRoot()
    {
        if (FieldStack_.empty()) {
            THROW_ERROR_EXCEPTION("Protobuf message can only be parsed from \"map\" values")
                << TErrorAttribute("ypath", YPathStack_.GetPath())
                << TErrorAttribute("proto_type", RootType_->GetFullName());
        }
    }

    void ValidateNotRepeated()
    {
        if (FieldStack_.back().ParsingList) {
            return;
        }
        const auto* field = FieldStack_.back().Field;
        if (field->IsYsonMap()) {
            THROW_ERROR_EXCEPTION("Map %v cannot be parsed from scalar values",
                YPathStack_.GetHumanReadablePath())
                << TErrorAttribute("ypath", YPathStack_.GetPath())
                << TErrorAttribute("proto_field", field->GetFullName());
        }
        if (field->IsRepeated()) {
            THROW_ERROR_EXCEPTION("Field %v is repeated and cannot be parsed from scalar values",
                YPathStack_.GetHumanReadablePath())
                << TErrorAttribute("ypath", YPathStack_.GetPath())
                << TErrorAttribute("proto_field", field->GetFullName());
        }
    }

    void ValidateRepeated()
    {
        if (FieldStack_.back().ParsingList) {
            THROW_ERROR_EXCEPTION("Items of list %v cannot be lists themselves",
                YPathStack_.GetHumanReadablePath())
                << TErrorAttribute("ypath", YPathStack_.GetPath());
        }

        const auto* field = FieldStack_.back().Field;
        if (!field->IsRepeated()) {
            THROW_ERROR_EXCEPTION("Field %v is not repeated and cannot be parsed from \"list\" values",
                YPathStack_.GetHumanReadablePath())
                << TErrorAttribute("ypath", YPathStack_.GetPath())
                << TErrorAttribute("proto_field", field->GetFullName());
        }
    }

    void WriteTag()
    {
        YT_ASSERT(!FieldStack_.empty());
        const auto* field = FieldStack_.back().Field;
        BodyCodedStream_.WriteTag(field->GetTag());
    }

    template <class F>
    void WriteScalar(F func)
    {
        ValidateNotRoot();
        ValidateNotRepeated();
        WriteTag();
        func();
        FieldStack_.pop_back();
        YPathStack_.Pop();
    }

    void WriteKeyValuePair(const TString& key, const TString& value)
    {
        BodyCodedStream_.WriteVarint64(
            1 +
            CodedOutputStream::VarintSize64(key.length()) +
            key.length() +
            1 +
            CodedOutputStream::VarintSize64(value.length()) +
            value.length());

        BodyCodedStream_.WriteTag(google::protobuf::internal::WireFormatLite::MakeTag(ProtobufMapKeyFieldNumber, WireFormatLite::WIRETYPE_LENGTH_DELIMITED));
        BodyCodedStream_.WriteVarint64(key.length());
        BodyCodedStream_.WriteRaw(key.data(), static_cast<int>(key.length()));

        BodyCodedStream_.WriteTag(google::protobuf::internal::WireFormatLite::MakeTag(ProtobufMapValueFieldNumber, WireFormatLite::WIRETYPE_LENGTH_DELIMITED));
        BodyCodedStream_.WriteVarint64(value.length());
        BodyCodedStream_.WriteRaw(value.data(), static_cast<int>(value.length()));
    }


    template <class T>
    void OnIntegerScalar(T value)
    {
        WriteScalar([&] {
            const auto* field = FieldStack_.back().Field;
            WriteIntegerScalar(field, value);
        });
    }

    template <class T>
    void WriteIntegerScalar(const TProtobufField* field, T value)
    {
        switch (field->GetType()) {
            case FieldDescriptor::TYPE_INT32: {
                auto i32Value = CheckedCastField<i32>(value, TStringBuf("i32"), field);
                if (field->IsPacked()) {
                    BodyCodedStream_.WriteVarint64(BodyCodedStream_.VarintSize32SignExtended(i32Value));
                }
                BodyCodedStream_.WriteVarint32SignExtended(i32Value);
                break;
            }

            case FieldDescriptor::TYPE_INT64: {
                auto i64Value = CheckedCastField<i64>(value, TStringBuf("i64"), field);
                if (field->IsPacked()) {
                    BodyCodedStream_.WriteVarint64(BodyCodedStream_.VarintSize64(i64Value));
                }
                BodyCodedStream_.WriteVarint64(static_cast<ui64>(i64Value));
                break;
            }

            case FieldDescriptor::TYPE_SINT32: {
                auto i32Value = CheckedCastField<i32>(value, TStringBuf("i32"), field);
                auto encodedValue = ZigZagEncode64(i32Value);
                if (field->IsPacked()) {
                    BodyCodedStream_.WriteVarint64(BodyCodedStream_.VarintSize64(encodedValue));
                }
                BodyCodedStream_.WriteVarint64(encodedValue);
                break;
            }

            case FieldDescriptor::TYPE_SINT64: {
                auto i64Value = CheckedCastField<i64>(value, TStringBuf("i64"), field);
                auto encodedValue = ZigZagEncode64(i64Value);
                if (field->IsPacked()) {
                    BodyCodedStream_.WriteVarint64(BodyCodedStream_.VarintSize64(encodedValue));
                }
                BodyCodedStream_.WriteVarint64(encodedValue);
                break;
            }

            case FieldDescriptor::TYPE_UINT32: {
                auto ui32Value = CheckedCastField<ui32>(value, TStringBuf("ui32"), field);
                if (field->IsPacked()) {
                    BodyCodedStream_.WriteVarint64(BodyCodedStream_.VarintSize32(ui32Value));
                }
                BodyCodedStream_.WriteVarint32(ui32Value);
                break;
            }

            case FieldDescriptor::TYPE_UINT64: {
                auto ui64Value = CheckedCastField<ui64>(value, TStringBuf("ui64"), field);
                if (field->IsPacked()) {
                    BodyCodedStream_.WriteVarint64(BodyCodedStream_.VarintSize64(ui64Value));
                }
                BodyCodedStream_.WriteVarint64(ui64Value);
                break;
            }

            case FieldDescriptor::TYPE_FIXED32: {
                auto ui32Value = CheckedCastField<ui32>(value, TStringBuf("ui32"), field);
                if (field->IsPacked()) {
                    BodyCodedStream_.WriteVarint64(sizeof(ui32Value));
                }
                BodyCodedStream_.WriteRaw(&ui32Value, sizeof(ui32Value));
                break;
            }

            case FieldDescriptor::TYPE_FIXED64: {
                auto ui64Value = CheckedCastField<ui64>(value, TStringBuf("ui64"), field);
                if (field->IsPacked()) {
                    BodyCodedStream_.WriteVarint64(sizeof(ui64Value));
                }
                BodyCodedStream_.WriteRaw(&ui64Value, sizeof(ui64Value));
                break;
            }

            case FieldDescriptor::TYPE_SFIXED32: {
                auto i32Value = CheckedCastField<i32>(value, TStringBuf("i32"), field);
                if (field->IsPacked()) {
                    BodyCodedStream_.WriteVarint64(sizeof(i32Value));
                }
                BodyCodedStream_.WriteRaw(&i32Value, sizeof(i32Value));
                break;
            }

            case FieldDescriptor::TYPE_SFIXED64: {
                auto i64Value = CheckedCastField<i64>(value, TStringBuf("i64"), field);
                if (field->IsPacked()) {
                    BodyCodedStream_.WriteVarint64(sizeof(i64Value));
                }
                BodyCodedStream_.WriteRaw(&i64Value, sizeof(i64Value));
                break;
            }

            case FieldDescriptor::TYPE_ENUM: {
                auto i32Value = CheckedCastField<i32>(value, TStringBuf("i32"), field);
                const auto* enumType = field->GetEnumType();
                auto literal = enumType->FindLiteralByValue(i32Value);
                if (!literal) {
                    THROW_ERROR_EXCEPTION("Unknown value %v for field %v",
                        i32Value,
                        YPathStack_.GetHumanReadablePath())
                        << TErrorAttribute("ypath", YPathStack_.GetPath())
                        << TErrorAttribute("proto_field", field->GetFullName());
                }
                if (field->IsPacked()) {
                    BodyCodedStream_.WriteVarint64(BodyCodedStream_.VarintSize32SignExtended(i32Value));
                }
                BodyCodedStream_.WriteVarint32SignExtended(i32Value);
                break;
            }

            case FieldDescriptor::TYPE_DOUBLE: {
                auto encodedValue = WireFormatLite::EncodeDouble(static_cast<double>(value));
                if (field->IsPacked()) {
                    BodyCodedStream_.WriteVarint64(sizeof(encodedValue));
                }
                BodyCodedStream_.WriteRaw(&encodedValue, sizeof(encodedValue));
                break;
            }

            case FieldDescriptor::TYPE_FLOAT: {
                auto encodedValue = WireFormatLite::EncodeFloat(static_cast<double>(value));
                if (field->IsPacked()) {
                    BodyCodedStream_.WriteVarint64(sizeof(encodedValue));
                }
                BodyCodedStream_.WriteRaw(&encodedValue, sizeof(encodedValue));
                break;
            }

            default:
                THROW_ERROR_EXCEPTION("Field %v cannot be parsed from integer values",
                    YPathStack_.GetHumanReadablePath())
                    << TErrorAttribute("ypath", YPathStack_.GetPath())
                    << TErrorAttribute("proto_field", field->GetFullName());
        }
    }

    template <class TTo, class TFrom>
    TTo CheckedCastField(TFrom value, TStringBuf toTypeName, const TProtobufField* field)
    {
        TTo result;
        if (!TryIntegralCast<TTo>(value, &result)) {
            THROW_ERROR_EXCEPTION("Value %v of field %v cannot fit into %Qv",
                value,
                YPathStack_.GetHumanReadablePath(),
                toTypeName)
                << TErrorAttribute("ypath", YPathStack_.GetPath())
                << TErrorAttribute("proto_field", field->GetFullName());
        }
        return result;
    }

    void TryWriteCustomlyConvertibleType()
    {
        if (FieldStack_.empty()) {
            return;
        }

        const auto* field = FieldStack_.back().Field;
        if (field->GetBytesFieldConverter()) {
            if (field->IsRepeated() && !FieldStack_.back().ParsingList) {
                return;
            }
            const auto& converter = *field->GetBytesFieldConverter();
            TreeBuilder_->BeginTree();
            Forward(TreeBuilder_.get(), [this, converter] {
                auto node = TreeBuilder_->EndTree();
                BytesString_.clear();
                converter.Deserializer(&BytesString_, node);
                OnMyStringScalar(BytesString_);
            });
        } else if (field->GetType() == FieldDescriptor::TYPE_MESSAGE && field->GetMessageType()->GetConverter()) {
            if (field->IsRepeated() && !FieldStack_.back().ParsingList) {
                return;
            }
            const auto* messageType = field->GetMessageType();
            const auto& converter = *messageType->GetConverter();
            TreeBuilder_->BeginTree();
            Forward(TreeBuilder_.get(), [this, converter, messageType] {
                auto node = TreeBuilder_->EndTree();
                std::unique_ptr<Message> message(MessageFactory::generated_factory()->GetPrototype(messageType->GetUnderlying())->New());
                converter.Deserializer(message.get(), node);
                SerializedMessage_.clear();
                Y_PROTOBUF_SUPPRESS_NODISCARD message->SerializeToString(&SerializedMessage_);
                WriteTag();
                BodyCodedStream_.WriteVarint64(SerializedMessage_.length());
                BodyCodedStream_.WriteRaw(SerializedMessage_.begin(), static_cast<int>(SerializedMessage_.length()));
                FieldStack_.pop_back();
                YPathStack_.Pop();
            });
        }
    }
};

std::unique_ptr<IYsonConsumer> CreateProtobufWriter(
    ZeroCopyOutputStream* outputStream,
    const TProtobufMessageType* rootType,
    TProtobufWriterOptions options)
{
    return std::make_unique<TProtobufWriter>(outputStream, rootType, options);
}

////////////////////////////////////////////////////////////////////////////////

class TProtobufParser
    : public TProtobufTranscoderBase
{
public:
    TProtobufParser(
        IYsonConsumer* consumer,
        ZeroCopyInputStream* inputStream,
        const TProtobufMessageType* rootType,
        const TProtobufParserOptions& options)
        : Consumer_(consumer)
        , RootType_(rootType)
        , Options_(options)
        , InputStream_(inputStream)
        , CodedStream_(InputStream_)
    { }

    void Parse()
    {
        TypeStack_.emplace_back(RootType_);
        Consumer_->OnBeginMap();

        while (true) {
            auto& typeEntry = TypeStack_.back();
            const auto* type = typeEntry.Type;

            bool flag;
            if (type->IsAttributeDictionary()) {
                flag = ParseAttributeDictionary();
            } else if (IsYsonMapEntry()) {
                flag = ParseMapEntry();
            } else {
                flag = ParseRegular();
            }

            if (!flag) {
                if (typeEntry.RepeatedField) {
                    if (typeEntry.RepeatedField->IsYsonMap()) {
                        OnEndMap();
                    } else {
                        OnEndList();
                    }
                }

                SortFields(&typeEntry.OptionalFieldNumbers);
                ValidateNoFieldDuplicates(type, typeEntry.OptionalFieldNumbers);

                SortFields(&typeEntry.RequiredFieldNumbers);
                ValidateNoFieldDuplicates(type, typeEntry.RequiredFieldNumbers);

                if (!Options_.SkipRequiredFields && !IsYsonMapEntry()) {
                    ValidateRequiredFieldsPresent(type, typeEntry.RequiredFieldNumbers);
                }

                if (TypeStack_.size() == 1) {
                    break;
                }

                if (IsYsonMapEntry()) {
                    if (typeEntry.RequiredFieldNumbers.size() != 2) {
                        THROW_ERROR_EXCEPTION("Incomplete entry in protobuf map")
                            << TErrorAttribute("ypath", YPathStack_.GetPath());
                    }
                } else {
                    OnEndMap();
                }
                TypeStack_.pop_back();

                CodedStream_.PopLimit(LimitStack_.back());
                LimitStack_.pop_back();
                continue;
            }
        }

        Consumer_->OnEndMap();
        TypeStack_.pop_back();

        YT_VERIFY(TypeStack_.empty());
        YT_VERIFY(YPathStack_.IsEmpty());
        YT_VERIFY(LimitStack_.empty());
    }

private:
    IYsonConsumer* const Consumer_;
    const TProtobufMessageType* const RootType_;
    const TProtobufParserOptions Options_;
    ZeroCopyInputStream* const InputStream_;

    CodedInputStream CodedStream_;

    struct TTypeEntry
    {
        explicit TTypeEntry(const TProtobufMessageType* type)
            : Type(type)
        { }

        const TProtobufMessageType* Type;
        TFieldNumberList RequiredFieldNumbers;
        TFieldNumberList OptionalFieldNumbers;
        const TProtobufField* RepeatedField = nullptr;
        int RepeatedIndex = -1;

        void BeginRepeated(const TProtobufField* field)
        {
            YT_ASSERT(!RepeatedField);
            YT_ASSERT(RepeatedIndex == -1);
            RepeatedField = field;
            RepeatedIndex = 0;
        }

        void ResetRepeated()
        {
            RepeatedField = nullptr;
            RepeatedIndex = -1;
        }

        int GenerateNextListIndex()
        {
            YT_ASSERT(RepeatedField);
            return ++RepeatedIndex;
        }
    };
    std::vector<TTypeEntry> TypeStack_;

    std::vector<CodedInputStream::Limit> LimitStack_;

    std::vector<char> PooledString_;
    std::vector<char> PooledKey_;
    std::vector<char> PooledValue_;


    void OnBeginMap()
    {
        Consumer_->OnBeginMap();
    }

    void OnKeyedItem(const TProtobufField* field)
    {
        Consumer_->OnKeyedItem(field->GetYsonName());
        YPathStack_.Push(TString{field->GetYsonName()});
    }

    void OnKeyedItem(TString key)
    {
        Consumer_->OnKeyedItem(key);
        YPathStack_.Push(std::move(key));
    }

    void OnEndMap()
    {
        Consumer_->OnEndMap();
        YPathStack_.Pop();
    }


    void OnBeginList()
    {
        Consumer_->OnBeginList();
    }

    void OnListItem(int index)
    {
        Consumer_->OnListItem();
        YPathStack_.Push(index);
    }

    void OnEndList()
    {
        Consumer_->OnEndList();
        YPathStack_.Pop();
    }


    bool IsYsonMapEntry()
    {
        if (TypeStack_.size() < 2) {
            return false;
        }
        auto& typeEntry = TypeStack_[TypeStack_.size() - 2];
        if (!typeEntry.RepeatedField) {
            return false;
        }
        if (!typeEntry.RepeatedField->IsYsonMap()) {
            return false;
        }
        return true;
    }

    template <class T>
    void FillPooledStringWithInteger(T value)
    {
        PooledString_.resize(64); // enough for any value
        auto length = ToString(value, PooledString_.data(), PooledString_.size());
        PooledString_.resize(length);
    }

    bool ParseMapEntry()
    {
        auto& typeEntry = TypeStack_.back();
        const auto* type = typeEntry.Type;

        auto tag = CodedStream_.ReadTag();
        if (tag == 0) {
            return false;
        }

        auto wireType = WireFormatLite::GetTagWireType(tag);
        auto fieldNumber = WireFormatLite::GetTagFieldNumber(tag);
        typeEntry.RequiredFieldNumbers.push_back(fieldNumber);

        switch (fieldNumber) {
            case ProtobufMapKeyFieldNumber: {
                if (typeEntry.RequiredFieldNumbers.size() != 1) {
                    THROW_ERROR_EXCEPTION("Out-of-order protobuf map key")
                        << TErrorAttribute("ypath", YPathStack_.GetPath());
                }

                const auto* field = type->GetFieldByNumber(fieldNumber);
                switch (wireType) {
                    case WireFormatLite::WIRETYPE_VARINT: {
                        ui64 keyValue;
                        if (!CodedStream_.ReadVarint64(&keyValue)) {
                            THROW_ERROR_EXCEPTION("Error reading \"varint\" value for protobuf map key")
                                << TErrorAttribute("ypath", YPathStack_.GetPath())
                                << TErrorAttribute("proto_field", field->GetFullName());
                        }

                        switch (field->GetType()) {
                            case FieldDescriptor::TYPE_INT64:
                                FillPooledStringWithInteger(static_cast<i64>(keyValue));
                                break;

                            case FieldDescriptor::TYPE_UINT64:
                                FillPooledStringWithInteger(keyValue);
                                break;

                            case FieldDescriptor::TYPE_INT32:
                                FillPooledStringWithInteger(static_cast<i32>(keyValue));
                                break;

                            case FieldDescriptor::TYPE_SINT32:
                                FillPooledStringWithInteger(ZigZagDecode32(static_cast<ui32>(keyValue)));
                                break;

                            case FieldDescriptor::TYPE_SINT64:
                                FillPooledStringWithInteger(ZigZagDecode64(keyValue));
                                break;

                            default:
                                YT_ABORT();
                        }
                        break;
                    }

                    case WireFormatLite::WIRETYPE_FIXED32: {
                        ui32 keyValue;
                        if (!CodedStream_.ReadRaw(&keyValue, sizeof(keyValue))) {
                            THROW_ERROR_EXCEPTION("Error reading \"fixed32\" value for protobuf map key")
                                << TErrorAttribute("ypath", YPathStack_.GetPath())
                                << TErrorAttribute("proto_field", field->GetFullName());
                        }

                        if (IsSignedIntegralType(field->GetType())) {
                            FillPooledStringWithInteger(static_cast<i32>(keyValue));
                        } else {
                            FillPooledStringWithInteger(keyValue);
                        }
                        break;
                    }

                    case WireFormatLite::WIRETYPE_FIXED64:  {
                        ui64 keyValue;
                        if (!CodedStream_.ReadRaw(&keyValue, sizeof(keyValue))) {
                            THROW_ERROR_EXCEPTION("Error reading \"fixed64\" value for protobuf map key")
                                << TErrorAttribute("ypath", YPathStack_.GetPath())
                                << TErrorAttribute("proto_field", field->GetFullName());
                        }

                        if (IsSignedIntegralType(field->GetType())) {
                            FillPooledStringWithInteger(static_cast<i64>(keyValue));
                        } else {
                            FillPooledStringWithInteger(keyValue);
                        }
                        break;
                    }

                    case WireFormatLite::WIRETYPE_LENGTH_DELIMITED: {
                        ui64 keyLength;
                        if (!CodedStream_.ReadVarint64(&keyLength)) {
                            THROW_ERROR_EXCEPTION("Error reading \"varint\" value for protobuf map key length")
                                << TErrorAttribute("ypath", YPathStack_.GetPath())
                                << TErrorAttribute("proto_field", field->GetFullName());
                        }

                        PooledString_.resize(keyLength);
                        if (!CodedStream_.ReadRaw(PooledString_.data(), keyLength)) {
                            THROW_ERROR_EXCEPTION("Error reading \"string\" value for protobuf map key")
                                << TErrorAttribute("ypath", YPathStack_.GetPath())
                                << TErrorAttribute("proto_field", field->GetFullName());
                        }
                        break;
                    }

                    default:
                        THROW_ERROR_EXCEPTION("Unexpected wire type tag %x for protobuf map key",
                            tag)
                            << TErrorAttribute("ypath", YPathStack_.GetPath())
                            << TErrorAttribute("proto_field", field->GetFullName());
                }

                OnKeyedItem(TString(PooledString_.data(), PooledString_.size()));
                break;
            }

            case ProtobufMapValueFieldNumber: {
                if (typeEntry.RequiredFieldNumbers.size() != 2) {
                    THROW_ERROR_EXCEPTION("Out-of-order protobuf map value")
                        << TErrorAttribute("ypath", YPathStack_.GetPath());
                }

                const auto* field = type->GetFieldByNumber(fieldNumber);
                ParseFieldValue(field, tag, wireType);
                break;
            }

            default:
                THROW_ERROR_EXCEPTION("Unexpected field number %v in protobuf map",
                    fieldNumber)
                    << TErrorAttribute("ypath", YPathStack_.GetPath());
        }

        return true;
    }

    bool ParseRegular()
    {
        auto& typeEntry = TypeStack_.back();
        const auto* type = typeEntry.Type;

        auto tag = CodedStream_.ReadTag();
        if (tag == 0) {
            return false;
        }

        auto handleRepeated = [&] {
            if (typeEntry.RepeatedField) {
                if (typeEntry.RepeatedField->IsYsonMap()) {
                    Consumer_->OnEndMap();
                } else {
                    Consumer_->OnEndList();
                }
                YPathStack_.Pop();
            }
            typeEntry.ResetRepeated();
        };

        auto wireType = WireFormatLite::GetTagWireType(tag);
        auto fieldNumber = WireFormatLite::GetTagFieldNumber(tag);
        if (fieldNumber == UnknownYsonFieldNumber) {
            if (wireType != WireFormatLite::WIRETYPE_LENGTH_DELIMITED) {
                THROW_ERROR_EXCEPTION("Invalid wire type %v while parsing unknown field at %v",
                    static_cast<int>(wireType),
                    YPathStack_.GetHumanReadablePath())
                    << TErrorAttribute("ypath", YPathStack_.GetPath());
            }

            handleRepeated();
            ParseKeyValuePair();
            return true;
        }

        const auto* field = type->FindFieldByNumber(fieldNumber);
        if (!field) {
            if (Options_.SkipUnknownFields || type->IsReservedFieldNumber(fieldNumber)) {
                switch (wireType) {
                    case WireFormatLite::WIRETYPE_VARINT: {
                        ui64 unsignedValue;
                        if (!CodedStream_.ReadVarint64(&unsignedValue)) {
                            THROW_ERROR_EXCEPTION("Error reading \"varint\" value for unknown field %v",
                                fieldNumber)
                                << TErrorAttribute("ypath", YPathStack_.GetPath());
                        }
                        break;
                    }

                    case WireFormatLite::WIRETYPE_FIXED32: {
                        ui32 unsignedValue;
                        if (!CodedStream_.ReadLittleEndian32(&unsignedValue)) {
                            THROW_ERROR_EXCEPTION("Error reading \"fixed32\" value for unknown field %v",
                                fieldNumber)
                                << TErrorAttribute("ypath", YPathStack_.GetPath());
                        }
                        break;
                    }

                    case WireFormatLite::WIRETYPE_FIXED64: {
                        ui64 unsignedValue;
                        if (!CodedStream_.ReadLittleEndian64(&unsignedValue)) {
                            THROW_ERROR_EXCEPTION("Error reading \"fixed64\" value for unknown field %v",
                                fieldNumber)
                                << TErrorAttribute("ypath", YPathStack_.GetPath());
                        }
                        break;
                    }

                    case WireFormatLite::WIRETYPE_LENGTH_DELIMITED: {
                        ui64 length;
                        if (!CodedStream_.ReadVarint64(&length)) {
                            THROW_ERROR_EXCEPTION("Error reading \"varint\" value for unknown field %v",
                                fieldNumber)
                                << TErrorAttribute("ypath", YPathStack_.GetPath());
                        }
                        if (length > std::numeric_limits<int>::max()) {
                            THROW_ERROR_EXCEPTION("Invalid length %v for unknown field %v",
                                length,
                                fieldNumber)
                                << TErrorAttribute("ypath", YPathStack_.GetPath());
                        }
                        if (!CodedStream_.Skip(static_cast<int>(length))) {
                            THROW_ERROR_EXCEPTION("Error skipping unknown length-delimited field %v",
                                fieldNumber)
                                << TErrorAttribute("ypath", YPathStack_.GetPath());
                        }
                        break;
                    }

                    default:
                        THROW_ERROR_EXCEPTION("Unexpected wire type tag %x for unknown field %v",
                            tag,
                            fieldNumber)
                            << TErrorAttribute("ypath", YPathStack_.GetPath());
                }
                return true;
            }
            THROW_ERROR_EXCEPTION("Unknown field number %v at %v",
                fieldNumber,
                YPathStack_.GetHumanReadablePath())
                << TErrorAttribute("ypath", YPathStack_.GetPath())
                << TErrorAttribute("proto_type", type->GetFullName());
        }

        if (typeEntry.RepeatedField == field) {
            if (!field->IsYsonMap()) {
                YT_ASSERT(field->IsRepeated());
                OnListItem(typeEntry.GenerateNextListIndex());
            }
        } else {
            handleRepeated();

            OnKeyedItem(field);

            if (field->IsYsonMap()) {
                typeEntry.BeginRepeated(field);
                OnBeginMap();
            } else if (field->IsRepeated()) {
                typeEntry.BeginRepeated(field);
                OnBeginList();
                OnListItem(0);
            }
        }

        if (field->IsRequired()) {
            typeEntry.RequiredFieldNumbers.push_back(fieldNumber);
        } else if (field->IsOptional()) {
            typeEntry.OptionalFieldNumbers.push_back(fieldNumber);
        }

        ParseFieldValue(field, tag, wireType);

        return true;
    }

    template <class T>
    void ParseFixedPacked(ui64 length, const TProtobufField* field, auto&& func)
    {
        YT_ASSERT(length % sizeof(T) == 0);
        for (auto index = 0u; index < length / sizeof(T); ++index) {
            T unsignedValue;
            auto readResult = false;
            if constexpr (std::is_same_v<T, ui64>) {
                readResult = CodedStream_.ReadLittleEndian64(&unsignedValue);
            } else {
                readResult = CodedStream_.ReadLittleEndian32(&unsignedValue);
            }
            if (!readResult) {
                THROW_ERROR_EXCEPTION("Error reading %Qv value from field %v",
                    field->GetTypeName(),
                    YPathStack_.GetHumanReadablePath())
                    << TErrorAttribute("ypath", YPathStack_.GetPath())
                    << TErrorAttribute("proto_field", field->GetFullName());
            }
            if (index > 0) {
                YT_ASSERT(field->IsRepeated());
                OnListItem(TypeStack_.back().GenerateNextListIndex());
            }
            ParseScalar([&] {func(unsignedValue);});
        }
    }

    template <class T>
    void ParseVarintPacked(ui64 length, const TProtobufField* field, auto&& func)
    {
        const void* data = nullptr;
        int size = 0;
        CodedStream_.GetDirectBufferPointer(&data, &size);
        YT_ASSERT(length <= static_cast<ui64>(size));
        ArrayInputStream array(data, length);
        CodedInputStream in(&array);
        size_t index = 0;
        while (static_cast<ui64>(in.CurrentPosition()) < length) {
            T unsignedValue;
            auto read = false;
            if constexpr (std::is_same_v<T, ui64>) {
                read = in.ReadVarint64(&unsignedValue);
            } else {
                read = in.ReadVarint32(&unsignedValue);
            }
            if (!read) {
                THROW_ERROR_EXCEPTION("Error reading \"%v\" value for field %v",
                    field->GetTypeName(),
                    YPathStack_.GetHumanReadablePath())
                    << TErrorAttribute("ypath", YPathStack_.GetPath())
                    << TErrorAttribute("proto_field", field->GetFullName());
            }
            if (index > 0) {
                YT_ASSERT(field->IsRepeated());
                OnListItem(TypeStack_.back().GenerateNextListIndex());
            }
            ++index;
            ParseScalar([&] {func(unsignedValue);});
        }
        CodedStream_.Skip(length);
    }

    void ParseFieldValue(
        const TProtobufField* field,
        int tag,
        WireFormatLite::WireType wireType)
    {
        auto storeEnumAsInt = [this, field] (auto value) {
            const auto* enumType = field->GetEnumType();
            if (!enumType->FindLiteralByValue(value)) {
                THROW_ERROR_EXCEPTION("Unknown value %v for field %v",
                    value,
                    YPathStack_.GetHumanReadablePath())
                    << TErrorAttribute("ypath", YPathStack_.GetPath())
                    << TErrorAttribute("proto_field", field->GetFullName());
            }
            Consumer_->OnInt64Scalar(value);
        };
        auto storeEnumAsString = [this, field] (auto value) {
            auto signedValue = static_cast<int>(value);
            const auto* enumType = field->GetEnumType();
            auto literal = enumType->FindLiteralByValue(signedValue);
            if (!literal) {
                THROW_ERROR_EXCEPTION("Unknown value %v for field %v",
                    signedValue,
                    YPathStack_.GetHumanReadablePath())
                    << TErrorAttribute("ypath", YPathStack_.GetPath())
                    << TErrorAttribute("proto_field", field->GetFullName());
            }
            Consumer_->OnStringScalar(literal);
        };

        switch (wireType) {
            case WireFormatLite::WIRETYPE_VARINT: {
                ui64 unsignedValue;
                if (!CodedStream_.ReadVarint64(&unsignedValue)) {
                    THROW_ERROR_EXCEPTION("Error reading \"varint\" value for field %v",
                        YPathStack_.GetHumanReadablePath())
                        << TErrorAttribute("ypath", YPathStack_.GetPath())
                        << TErrorAttribute("proto_field", field->GetFullName());
                }

                switch (field->GetType()) {
                    case FieldDescriptor::TYPE_BOOL:
                        ParseScalar([&] {
                            Consumer_->OnBooleanScalar(unsignedValue != 0);
                        });
                        break;

                    case FieldDescriptor::TYPE_ENUM: {
                        ParseScalar([&] {
                            switch (field->GetEnumYsonStorageType()) {
                                case EEnumYsonStorageType::String:
                                    storeEnumAsString(unsignedValue);
                                    break;
                                case EEnumYsonStorageType::Int:
                                    storeEnumAsInt(unsignedValue);
                                    break;
                            }
                        });
                        break;
                    }

                    case FieldDescriptor::TYPE_INT32:
                    case FieldDescriptor::TYPE_INT64:
                        ParseScalar([&] {
                            auto signedValue = static_cast<i64>(unsignedValue);
                            Consumer_->OnInt64Scalar(signedValue);
                        });
                        break;

                    case FieldDescriptor::TYPE_UINT32:
                    case FieldDescriptor::TYPE_UINT64:
                        ParseScalar([&] {
                            Consumer_->OnUint64Scalar(unsignedValue);
                        });
                        break;

                    case FieldDescriptor::TYPE_SINT64:
                    case FieldDescriptor::TYPE_SINT32:
                        ParseScalar([&] {
                            auto signedValue = ZigZagDecode64(unsignedValue);
                            Consumer_->OnInt64Scalar(signedValue);
                        });
                        break;

                    default:
                        THROW_ERROR_EXCEPTION("Unexpected \"varint\" value for field %v",
                            YPathStack_.GetHumanReadablePath())
                            << TErrorAttribute("ypath", YPathStack_.GetPath())
                            << TErrorAttribute("proto_field", field->GetFullName());
                }
                break;
            }

            case WireFormatLite::WIRETYPE_FIXED32: {
                ui32 unsignedValue;
                if (!CodedStream_.ReadLittleEndian32(&unsignedValue)) {
                    THROW_ERROR_EXCEPTION("Error reading \"fixed32\" value for field %v",
                        YPathStack_.GetHumanReadablePath())
                        << TErrorAttribute("ypath", YPathStack_.GetPath())
                        << TErrorAttribute("proto_field", field->GetFullName());
                }

                switch (field->GetType()) {
                    case FieldDescriptor::TYPE_FIXED32:
                        ParseScalar([&] {
                            Consumer_->OnUint64Scalar(unsignedValue);
                        });
                        break;

                    case FieldDescriptor::TYPE_SFIXED32: {
                        ParseScalar([&] {
                            auto signedValue = static_cast<i32>(unsignedValue);
                            Consumer_->OnInt64Scalar(signedValue);
                        });
                        break;
                    }

                    case FieldDescriptor::TYPE_FLOAT: {
                        ParseScalar([&] {
                            auto floatValue = WireFormatLite::DecodeFloat(unsignedValue);
                            Consumer_->OnDoubleScalar(floatValue);
                        });
                        break;
                    }

                    default:
                        THROW_ERROR_EXCEPTION("Unexpected \"fixed32\" value for field %v",
                            YPathStack_.GetHumanReadablePath())
                            << TErrorAttribute("ypath", YPathStack_.GetPath())
                            << TErrorAttribute("proto_field", field->GetFullName());
                }
                break;
            }

            case WireFormatLite::WIRETYPE_FIXED64: {
                ui64 unsignedValue;
                if (!CodedStream_.ReadLittleEndian64(&unsignedValue)) {
                    THROW_ERROR_EXCEPTION("Error reading \"fixed64\" value for field %v",
                        YPathStack_.GetHumanReadablePath())
                        << TErrorAttribute("ypath", YPathStack_.GetPath())
                        << TErrorAttribute("proto_field", field->GetFullName());
                }

                switch (field->GetType()) {
                    case FieldDescriptor::TYPE_FIXED64:
                        ParseScalar([&] {
                            Consumer_->OnUint64Scalar(unsignedValue);
                        });
                        break;

                    case FieldDescriptor::TYPE_SFIXED64: {
                        ParseScalar([&] {
                            auto signedValue = static_cast<i64>(unsignedValue);
                            Consumer_->OnInt64Scalar(signedValue);
                        });
                        break;
                    }

                    case FieldDescriptor::TYPE_DOUBLE: {
                        ParseScalar([&] {
                            auto doubleValue = WireFormatLite::DecodeDouble(unsignedValue);
                            Consumer_->OnDoubleScalar(doubleValue);
                        });
                        break;
                    }

                    default:
                        THROW_ERROR_EXCEPTION("Unexpected \"fixed64\" value for field %v",
                            YPathStack_.GetHumanReadablePath())
                            << TErrorAttribute("ypath", YPathStack_.GetPath())
                            << TErrorAttribute("proto_field", field->GetFullName());
                }
                break;
            }

            case WireFormatLite::WIRETYPE_LENGTH_DELIMITED: {
                ui64 length;
                if (!CodedStream_.ReadVarint64(&length)) {
                    THROW_ERROR_EXCEPTION("Error reading \"varint\" value for field %v",
                        YPathStack_.GetHumanReadablePath())
                        << TErrorAttribute("ypath", YPathStack_.GetPath())
                        << TErrorAttribute("proto_field", field->GetFullName());
                }

                switch (field->GetType()) {
                    case FieldDescriptor::TYPE_BYTES:
                    case FieldDescriptor::TYPE_STRING: {
                        PooledString_.resize(length);
                        if (!CodedStream_.ReadRaw(PooledString_.data(), length)) {
                            THROW_ERROR_EXCEPTION("Error reading \"string\" value for field %v",
                                YPathStack_.GetHumanReadablePath())
                                << TErrorAttribute("ypath", YPathStack_.GetPath())
                                << TErrorAttribute("proto_field", field->GetFullName());
                        }
                        TStringBuf data(PooledString_.data(), length);
                        if (field->GetType() == FieldDescriptor::TYPE_STRING) {
                            ValidateString(data, field->GetFullName());
                        }
                        ParseScalar([&] {
                            if (field->GetBytesFieldConverter()) {
                                const auto& converter = *field->GetBytesFieldConverter();
                                converter.Serializer(Consumer_, data);
                            } else if (field->IsYsonString()) {
                                Consumer_->OnRaw(data, NYson::EYsonType::Node);
                            } else {
                                Consumer_->OnStringScalar(data);
                            }
                        });
                        break;
                    }

                    case FieldDescriptor::TYPE_MESSAGE: {
                        const auto* messageType = field->GetMessageType();
                        if (messageType->GetConverter()) {
                            const auto& converter = *messageType->GetConverter();
                            std::unique_ptr<Message> message(MessageFactory::generated_factory()->GetPrototype(messageType->GetUnderlying())->New());
                            PooledString_.resize(length);
                            CodedStream_.ReadRaw(PooledString_.data(), PooledString_.size());
                            Y_PROTOBUF_SUPPRESS_NODISCARD message->ParseFromArray(PooledString_.data(), PooledString_.size());
                            converter.Serializer(Consumer_, message.get());
                            YPathStack_.Pop();
                        } else {
                            LimitStack_.push_back(CodedStream_.PushLimit(static_cast<int>(length)));
                            TypeStack_.emplace_back(field->GetMessageType());
                            if (!IsYsonMapEntry()) {
                                OnBeginMap();
                            }
                        }
                        break;
                    }

                    case FieldDescriptor::TYPE_FIXED32: {
                        ParseFixedPacked<ui32>(length, field, [&] (auto value) {Consumer_->OnUint64Scalar(value);});
                        break;
                    }

                    case FieldDescriptor::TYPE_FIXED64: {
                        ParseFixedPacked<ui64>(length, field, [&] (auto value) {Consumer_->OnUint64Scalar(value);});
                        break;
                    }

                    case FieldDescriptor::TYPE_SFIXED32: {
                        ParseFixedPacked<ui32>(length, field, [&] (auto value) {Consumer_->OnInt64Scalar(static_cast<i32>(value));});
                        break;
                    }

                    case FieldDescriptor::TYPE_SFIXED64: {
                        ParseFixedPacked<ui64>(length, field, [&] (auto value) {Consumer_->OnInt64Scalar(static_cast<i64>(value));});
                        break;
                    }

                    case FieldDescriptor::TYPE_FLOAT: {
                        ParseFixedPacked<ui32>(length, field,
                            [&] (auto value) {
                                auto floatValue = WireFormatLite::DecodeFloat(value);
                                Consumer_->OnDoubleScalar(floatValue);
                            });
                        break;
                    }

                    case FieldDescriptor::TYPE_DOUBLE: {
                        ParseFixedPacked<ui64>(length, field,
                            [&] (auto value) {
                                auto doubleValue = WireFormatLite::DecodeDouble(value);
                                Consumer_->OnDoubleScalar(doubleValue);
                            });
                        break;
                    }

                    case FieldDescriptor::TYPE_INT32: {
                        ParseVarintPacked<ui32>(length, field, [&] (auto value) {Consumer_->OnInt64Scalar(static_cast<i32>(value));});
                        break;
                    }

                    case FieldDescriptor::TYPE_INT64: {
                        ParseVarintPacked<ui64>(length, field, [&] (auto value) {Consumer_->OnInt64Scalar(static_cast<i64>(value));});
                        break;
                    }

                    case FieldDescriptor::TYPE_UINT32: {
                        ParseVarintPacked<ui32>(length, field, [&] (auto value) {Consumer_->OnUint64Scalar(value);});
                        break;
                    }

                    case FieldDescriptor::TYPE_UINT64: {
                        ParseVarintPacked<ui64>(length, field, [&] (auto value) {Consumer_->OnUint64Scalar(value);});
                        break;
                    }

                    case FieldDescriptor::TYPE_ENUM: {
                        ParseVarintPacked<ui32>(length, field, [&] (auto value) {
                            switch (field->GetEnumYsonStorageType()) {
                                case EEnumYsonStorageType::String:
                                    storeEnumAsString(value);
                                    break;
                                case EEnumYsonStorageType::Int:
                                    storeEnumAsInt(value);
                                    break;
                            }
                        });
                        break;
                    }

                    default:
                        THROW_ERROR_EXCEPTION("Unexpected \"length-delimited\" value for field %v",
                            YPathStack_.GetHumanReadablePath())
                            << TErrorAttribute("ypath", YPathStack_.GetPath())
                            << TErrorAttribute("proto_field", field->GetFullName());
                }
                break;
            }

            default:
                THROW_ERROR_EXCEPTION("Unexpected wire type tag %x",
                    tag)
                    << TErrorAttribute("ypath", YPathStack_.GetPath())
                    << TErrorAttribute("proto_field", field->GetFullName());
        }
    }

    bool ParseAttributeDictionary()
    {
        auto throwUnexpectedWireType = [&] (WireFormatLite::WireType actualWireType) {
            THROW_ERROR_EXCEPTION("Invalid wire type %v while parsing attribute dictionary %v",
                static_cast<int>(actualWireType),
                YPathStack_.GetHumanReadablePath())
                << TErrorAttribute("ypath", YPathStack_.GetPath());
        };

        auto expectWireType = [&] (WireFormatLite::WireType actualWireType, WireFormatLite::WireType expectedWireType) {
            if (actualWireType != expectedWireType) {
                throwUnexpectedWireType(actualWireType);
            }
        };

        auto throwUnexpectedFieldNumber = [&] (int actualFieldNumber) {
            THROW_ERROR_EXCEPTION("Invalid field number %v while parsing attribute dictionary %v",
                actualFieldNumber,
                YPathStack_.GetHumanReadablePath())
                << TErrorAttribute("ypath", YPathStack_.GetPath());
        };

        auto expectFieldNumber = [&] (int actualFieldNumber, int expectedFieldNumber) {
            if (actualFieldNumber != expectedFieldNumber) {
                throwUnexpectedFieldNumber(actualFieldNumber);
            }
        };

        while (true) {
            auto tag = CodedStream_.ReadTag();
            if (tag == 0) {
                return false;
            }

            expectWireType(WireFormatLite::GetTagWireType(tag), WireFormatLite::WIRETYPE_LENGTH_DELIMITED);
            expectFieldNumber(WireFormatLite::GetTagFieldNumber(tag), AttributeDictionaryAttributeFieldNumber);

            ParseKeyValuePair();
        }
    }

    void ParseKeyValuePair()
    {
        auto throwUnexpectedWireType = [&] (WireFormatLite::WireType actualWireType) {
            THROW_ERROR_EXCEPTION("Invalid wire type %v while parsing key-value pair at %v",
                static_cast<int>(actualWireType),
                YPathStack_.GetHumanReadablePath())
                << TErrorAttribute("ypath", YPathStack_.GetPath());
        };

        auto expectWireType = [&] (WireFormatLite::WireType actualWireType, WireFormatLite::WireType expectedWireType) {
            if (actualWireType != expectedWireType) {
                throwUnexpectedWireType(actualWireType);
            }
        };

        auto throwUnexpectedFieldNumber = [&] (int actualFieldNumber) {
            THROW_ERROR_EXCEPTION("Invalid field number %v while parsing key-value pair at %v",
                actualFieldNumber,
                YPathStack_.GetHumanReadablePath())
                << TErrorAttribute("ypath", YPathStack_.GetPath());
        };

        auto readVarint64 = [&] {
            ui64 value;
            if (!CodedStream_.ReadVarint64(&value)) {
                THROW_ERROR_EXCEPTION("Error reading \"varint\" value while parsing key-value pair at %v",
                    YPathStack_.GetHumanReadablePath())
                    << TErrorAttribute("ypath", YPathStack_.GetPath());
            }
            return value;
        };

        auto readString = [&] (auto* pool) -> TStringBuf {
            auto length = readVarint64();
            pool->resize(length);
            if (!CodedStream_.ReadRaw(pool->data(), length)) {
                THROW_ERROR_EXCEPTION("Error reading \"string\" value while parsing key-value pair at %v",
                    YPathStack_.GetHumanReadablePath())
                    << TErrorAttribute("ypath", YPathStack_.GetPath());
            }
            return TStringBuf(pool->data(), length);
        };

        auto entryLength = readVarint64();
        LimitStack_.push_back(CodedStream_.PushLimit(static_cast<int>(entryLength)));

        std::optional<TStringBuf> key;
        std::optional<TStringBuf> value;
        while (true) {
            auto tag = CodedStream_.ReadTag();
            if (tag == 0) {
                break;
            }

            auto fieldNumber = WireFormatLite::GetTagFieldNumber(tag);
            switch (fieldNumber) {
                case ProtobufMapKeyFieldNumber: {
                    expectWireType(WireFormatLite::GetTagWireType(tag), WireFormatLite::WIRETYPE_LENGTH_DELIMITED);
                    if (key) {
                        THROW_ERROR_EXCEPTION("Duplicate key found while parsing key-value pair at%v",
                            YPathStack_.GetHumanReadablePath())
                            << TErrorAttribute("ypath", YPathStack_.GetPath());
                    }
                    key = readString(&PooledKey_);
                    break;
                }

                case ProtobufMapValueFieldNumber: {
                    expectWireType(WireFormatLite::GetTagWireType(tag), WireFormatLite::WIRETYPE_LENGTH_DELIMITED);
                    if (value) {
                        THROW_ERROR_EXCEPTION("Duplicate value found while parsing key-value pair at %v",
                            YPathStack_.GetHumanReadablePath())
                            << TErrorAttribute("ypath", YPathStack_.GetPath());
                    }
                    value = readString(&PooledValue_);
                    break;
                }

                default:
                    throwUnexpectedFieldNumber(fieldNumber);
                    break;
            }
        }

        if (!key) {
            THROW_ERROR_EXCEPTION("Missing key while parsing key-value pair at %v",
                YPathStack_.GetHumanReadablePath())
                << TErrorAttribute("ypath", YPathStack_.GetPath());
        }
        if (!value) {
            THROW_ERROR_EXCEPTION("Missing value while parsing key-value pair %v",
                YPathStack_.GetHumanReadablePath())
                << TErrorAttribute("ypath", YPathStack_.GetPath());
        }

        Consumer_->OnKeyedItem(*key);
        Consumer_->OnRaw(*value, NYson::EYsonType::Node);

        CodedStream_.PopLimit(LimitStack_.back());
        LimitStack_.pop_back();
    }

    template <class F>
    void ParseScalar(F func)
    {
        func();
        YPathStack_.Pop();
    }
};

void ParseProtobuf(
    IYsonConsumer* consumer,
    ZeroCopyInputStream* inputStream,
    const TProtobufMessageType* rootType,
    const TProtobufParserOptions& options)
{
    TProtobufParser parser(consumer, inputStream, rootType, options);
    parser.Parse();
}

void WriteProtobufMessage(
    IYsonConsumer* consumer,
    const ::google::protobuf::Message& message,
    const TProtobufParserOptions& options)
{
    auto data = SerializeProtoToRef(message);
    ArrayInputStream stream(data.Begin(), data.Size());
    const auto* type = ReflectProtobufMessageType(message.GetDescriptor());
    ParseProtobuf(consumer, &stream, type, options);
}

////////////////////////////////////////////////////////////////////////////////

namespace {

TStringBuf FormatYPath(TStringBuf ypath)
{
    return ypath.empty() ? TStringBuf("/") : ypath;
}

TProtobufElementResolveResult GetProtobufElementFromField(
    const TProtobufField* field,
    bool insideRepeated,
    const NYPath::TTokenizer& tokenizer)
{
    auto element = field->GetElement(insideRepeated);
    if (std::holds_alternative<std::unique_ptr<TProtobufScalarElement>>(element) && !tokenizer.GetSuffix().empty()) {
        THROW_ERROR_EXCEPTION("Field %v is scalar and does not support nested access",
            FormatYPath(tokenizer.GetPrefixPlusToken()))
            << TErrorAttribute("ypath", tokenizer.GetPrefixPlusToken())
            << TErrorAttribute("proto_field", field->GetFullName());
    }
    return TProtobufElementResolveResult{
        std::move(element),
        tokenizer.GetPrefixPlusToken(),
        tokenizer.GetSuffix()
    };
}

} // namespace

TProtobufElementResolveResult ResolveProtobufElementByYPath(
    const TProtobufMessageType* rootType,
    const NYPath::TYPathBuf path,
    const TResolveProtobufElementByYPathOptions& options)
{
    NYPath::TTokenizer tokenizer(path);

    auto makeResult = [&] (TProtobufElement element) {
        return TProtobufElementResolveResult{
            std::move(element),
            tokenizer.GetPrefixPlusToken(),
            tokenizer.GetSuffix()
        };
    };

    const auto* currentType = rootType;
    while (true) {
        YT_VERIFY(currentType);

        tokenizer.Advance();
        if (tokenizer.GetType() == NYPath::ETokenType::EndOfStream) {
            break;
        }

        tokenizer.Expect(NYPath::ETokenType::Slash);

        if (currentType->IsAttributeDictionary()) {
            tokenizer.Advance();
            tokenizer.Expect(NYPath::ETokenType::Literal);
            return makeResult(std::make_unique<TProtobufAnyElement>());
        }

        tokenizer.Advance();
        tokenizer.Expect(NYPath::ETokenType::Literal);

        const auto& fieldName = tokenizer.GetLiteralValue();
        const auto* field = currentType->FindFieldByName(fieldName);
        if (!field) {
            if (options.AllowUnknownYsonFields) {
                return makeResult(std::make_unique<TProtobufAnyElement>());
            }
            THROW_ERROR_EXCEPTION("No such field %v",
                FormatYPath(tokenizer.GetPrefixPlusToken()))
                << TErrorAttribute("ypath", tokenizer.GetPrefixPlusToken())
                << TErrorAttribute("message_type", currentType->GetFullName());
        }

        if (!field->IsMessage()) {
            if (field->IsRepeated()) {
                tokenizer.Advance();
                if (tokenizer.GetType() == NYPath::ETokenType::EndOfStream) {
                    return makeResult(field->GetElement(false));
                }

                tokenizer.Expect(NYPath::ETokenType::Slash);
                tokenizer.Advance();
                tokenizer.ExpectListIndex();

                return GetProtobufElementFromField(
                    field,
                    true,
                    tokenizer);
            } else {
                return GetProtobufElementFromField(
                    field,
                    false,
                    tokenizer);
            }
        }

        if (field->IsYsonMap()) {
            tokenizer.Advance();
            if (tokenizer.GetType() == NYPath::ETokenType::EndOfStream) {
                return makeResult(field->GetElement(false));
            }

            tokenizer.Expect(NYPath::ETokenType::Slash);
            tokenizer.Advance();
            tokenizer.Expect(NYPath::ETokenType::Literal);

            const auto* valueField = field->GetYsonMapValueField();
            if (!valueField->IsMessage()) {
                return GetProtobufElementFromField(
                    valueField,
                    false,
                    tokenizer);
            }

            currentType = valueField->GetMessageType();
        } else if (field->IsRepeated()) {
            tokenizer.Advance();
            if (tokenizer.GetType() == NYPath::ETokenType::EndOfStream) {
                return makeResult(field->GetElement(false));
            }

            tokenizer.Expect(NYPath::ETokenType::Slash);
            tokenizer.Advance();
            tokenizer.ExpectListIndex();

            if (!field->IsMessage()) {
                return GetProtobufElementFromField(
                    field,
                    true,
                    tokenizer);
            }

            currentType = field->GetMessageType();
        } else {
            currentType = field->GetMessageType();
        }
    }
    return makeResult(currentType->GetElement());
}

////////////////////////////////////////////////////////////////////////////////

void AddProtobufConverterRegisterAction(std::function<void()> action)
{
    TProtobufTypeRegistry::Get()->AddRegisterAction(std::move(action));
}

////////////////////////////////////////////////////////////////////////////////

void RegisterCustomProtobufConverter(
    const Descriptor* descriptor,
    const TProtobufMessageConverter& converter)
{
    TProtobufTypeRegistry::Get()->RegisterMessageTypeConverter(descriptor, converter);
}

////////////////////////////////////////////////////////////////////////////////

void RegisterCustomProtobufBytesFieldConverter(
    const Descriptor* descriptor,
    int fieldNumber,
    const TProtobufMessageBytesFieldConverter& converter)
{
    // NB: Protobuf internal singletons might not be ready, so we can't get field descriptor here.
    TProtobufTypeRegistry::Get()->RegisterMessageBytesFieldConverter(descriptor, fieldNumber, converter);
}

////////////////////////////////////////////////////////////////////////////////

TString YsonStringToProto(
    const TYsonString& ysonString,
    const TProtobufMessageType* payloadType,
    EUnknownYsonFieldsMode unknownFieldsMode)
{
    TProtobufWriterOptions protobufWriterOptions;
    protobufWriterOptions.UnknownYsonFieldModeResolver =
        TProtobufWriterOptions::CreateConstantUnknownYsonFieldModeResolver(
            unknownFieldsMode);
    return YsonStringToProto(ysonString, payloadType, std::move(protobufWriterOptions));
}

TString YsonStringToProto(
    const TYsonString& ysonString,
    const TProtobufMessageType* payloadType,
    TProtobufWriterOptions options)
{
    TString serializedProto;
    google::protobuf::io::StringOutputStream protobufStream(&serializedProto);
    auto protobufWriter = CreateProtobufWriter(&protobufStream, payloadType, std::move(options));
    ParseYsonStringBuffer(ysonString.AsStringBuf(), EYsonType::Node, protobufWriter.get());
    return serializedProto;
}

void WriteSchema(const TProtobufEnumType* type, IYsonConsumer* consumer)
{
    type->WriteSchema(consumer);
}

void WriteSchema(const TProtobufMessageType* type, IYsonConsumer* consumer)
{
    type->WriteSchema(consumer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson

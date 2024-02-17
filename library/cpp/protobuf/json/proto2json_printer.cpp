#include "proto2json_printer.h"
#include "config.h"
#include "util.h"

#include <google/protobuf/any.pb.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/util/time_util.h>

#include <library/cpp/protobuf/json/proto/enum_options.pb.h>

#include <util/generic/yexception.h>
#include <util/string/ascii.h>
#include <util/string/cast.h>
#include <util/system/win_undef.h>


namespace NProtobufJson {
    using namespace NProtoBuf;

    class TJsonKeyBuilder {
    public:
        TJsonKeyBuilder(const FieldDescriptor& field, const TProto2JsonConfig& config, TString& tmpBuf)
            : NewKeyStr(tmpBuf)
        {
            if (config.NameGenerator) {
                NewKeyStr = config.NameGenerator(field);
                NewKeyBuf = NewKeyStr;
                return;
            }

            if (config.UseJsonName) {
                Y_ASSERT(!field.json_name().empty());
                NewKeyStr = field.json_name();
                if (!field.has_json_name() && !NewKeyStr.empty()) {
                    // FIXME: https://st.yandex-team.ru/CONTRIB-139
                    NewKeyStr[0] = AsciiToLower(NewKeyStr[0]);
                }
                NewKeyBuf = NewKeyStr;
                return;
            }

            switch (config.FieldNameMode) {
                case TProto2JsonConfig::FieldNameOriginalCase: {
                    NewKeyBuf = field.name();
                    break;
                }

                case TProto2JsonConfig::FieldNameLowerCase: {
                    NewKeyStr = field.name();
                    NewKeyStr.to_lower();
                    NewKeyBuf = NewKeyStr;
                    break;
                }

                case TProto2JsonConfig::FieldNameUpperCase: {
                    NewKeyStr = field.name();
                    NewKeyStr.to_upper();
                    NewKeyBuf = NewKeyStr;
                    break;
                }

                case TProto2JsonConfig::FieldNameCamelCase: {
                    NewKeyStr = field.name();
                    if (!NewKeyStr.empty()) {
                        NewKeyStr[0] = AsciiToLower(NewKeyStr[0]);
                    }
                    NewKeyBuf = NewKeyStr;
                    break;
                }

                case TProto2JsonConfig::FieldNameSnakeCase: {
                    NewKeyStr = field.name();
                    ToSnakeCase(&NewKeyStr);
                    NewKeyBuf = NewKeyStr;
                    break;
                }

                case TProto2JsonConfig::FieldNameSnakeCaseDense: {
                    NewKeyStr = field.name();
                    ToSnakeCaseDense(&NewKeyStr);
                    NewKeyBuf = NewKeyStr;
                    break;
                }

                default:
                    Y_DEBUG_ABORT_UNLESS(false, "Unknown FieldNameMode.");
            }
        }

        const TStringBuf& GetKey() const {
            return NewKeyBuf;
        }

    private:
        TStringBuf NewKeyBuf;
        TString& NewKeyStr;
    };

    TProto2JsonPrinter::TProto2JsonPrinter(const TProto2JsonConfig& cfg)
        : Config(cfg)
    {
    }

    TProto2JsonPrinter::~TProto2JsonPrinter() {
    }

    TStringBuf TProto2JsonPrinter::MakeKey(const FieldDescriptor& field) {
        return TJsonKeyBuilder(field, GetConfig(), TmpBuf).GetKey();
    }

    template <bool InMapContext, typename T>
    std::enable_if_t<InMapContext, void> WriteWithMaybeEmptyKey(IJsonOutput& json, const TStringBuf& key, const T& value) {
        json.WriteKey(key).Write(value);
    }

    template <bool InMapContext, typename T>
    std::enable_if_t<!InMapContext, void> WriteWithMaybeEmptyKey(IJsonOutput& array, const TStringBuf& key, const T& value) {
        Y_ASSERT(!key);
        array.Write(value);
    }

    template <bool InMapContext>
    Y_NO_INLINE void TProto2JsonPrinter::PrintStringValue(const FieldDescriptor& field,
                                              const TStringBuf& key, const TString& value,
                                              IJsonOutput& json) {
        if (!GetConfig().StringTransforms.empty()) {
            TString tmpBuf = value;
            for (const TStringTransformPtr& stringTransform : GetConfig().StringTransforms) {
                Y_ASSERT(stringTransform);
                if (stringTransform) {
                    if (field.type() == FieldDescriptor::TYPE_BYTES)
                        stringTransform->TransformBytes(tmpBuf);
                    else
                        stringTransform->Transform(tmpBuf);
                }
            }
            WriteWithMaybeEmptyKey<InMapContext>(json, key, tmpBuf);
        } else {
            WriteWithMaybeEmptyKey<InMapContext>(json, key, value);
        }
    }

    template <bool InMapContext>
    void TProto2JsonPrinter::PrintEnumValue(const TStringBuf& key,
                                            const EnumValueDescriptor* value,
                                            IJsonOutput& json) {
        if (Config.EnumValueGenerator) {
            WriteWithMaybeEmptyKey<InMapContext>(json, key, Config.EnumValueGenerator(*value));
            return;
        }

        if (Config.UseJsonEnumValue) {
            auto jsonEnumValue = value->options().GetExtension(json_enum_value);
            if (!jsonEnumValue) {
                ythrow yexception() << "Trying to using json enum value for field " << value->name() << " which is not set.";
            }
            WriteWithMaybeEmptyKey<InMapContext>(json, key, jsonEnumValue);
            return;
        }

        switch (GetConfig().EnumMode) {
            case TProto2JsonConfig::EnumNumber: {
                WriteWithMaybeEmptyKey<InMapContext>(json, key, value->number());
                break;
            }

            case TProto2JsonConfig::EnumName: {
                WriteWithMaybeEmptyKey<InMapContext>(json, key, value->name());
                break;
            }

            case TProto2JsonConfig::EnumFullName: {
                WriteWithMaybeEmptyKey<InMapContext>(json, key, value->full_name());
                break;
            }

            case TProto2JsonConfig::EnumNameLowerCase: {
                TString newName = value->name();
                newName.to_lower();
                WriteWithMaybeEmptyKey<InMapContext>(json, key, newName);
                break;
            }

            case TProto2JsonConfig::EnumFullNameLowerCase: {
                TString newName = value->full_name();
                newName.to_lower();
                WriteWithMaybeEmptyKey<InMapContext>(json, key, newName);
                break;
            }

            default:
                Y_DEBUG_ABORT_UNLESS(false, "Unknown EnumMode.");
        }
    }

    bool HandleTimeConversion(const Message& proto, IJsonOutput& json) {
        using namespace google::protobuf;
        auto type = proto.GetDescriptor()->well_known_type();

        // XXX static_cast will cause UB if used with dynamic messages
        // (can be created by a DynamicMessageFactory with SetDelegateToGeneratedFactory(false). Unlikely, but still possible).
        // See workaround with CopyFrom in JsonString2Duration, JsonString2Timestamp (json2proto.cpp)
        if (type == Descriptor::WellKnownType::WELLKNOWNTYPE_DURATION) {
            const auto& duration = static_cast<const Duration&>(proto);
            json.Write(util::TimeUtil::ToString(duration));
            return true;
        } else if (type == Descriptor::WellKnownType::WELLKNOWNTYPE_TIMESTAMP) {
            const auto& timestamp = static_cast<const Timestamp&>(proto);
            json.Write(util::TimeUtil::ToString(timestamp));
            return true;
        }
        return false;
    }

    bool TProto2JsonPrinter::TryPrintAny(const Message& proto, IJsonOutput& json) {
        using namespace google::protobuf;

        const FieldDescriptor* typeUrlField;
        const FieldDescriptor* valueField;
        if (!Any::GetAnyFieldDescriptors(proto, &typeUrlField, &valueField)) {
            return false;
        }
        const Reflection* const reflection = proto.GetReflection();
        const TString& typeUrl = reflection->GetString(proto, typeUrlField);
        TString fullTypeName;
        if (!Any::ParseAnyTypeUrl(typeUrl, &fullTypeName)) {
            return false;
        }
        const Descriptor* const valueDesc = proto.GetDescriptor()->file()->pool()->FindMessageTypeByName(fullTypeName);
        if (!valueDesc) {
            return false;
        }
        DynamicMessageFactory factory;
        const THolder<Message> valueMessage{factory.GetPrototype(valueDesc)->New()};
        const TString& serializedValue = reflection->GetString(proto, valueField);
        if (!valueMessage->ParseFromString(serializedValue)) {
            return false;
        }

        json.BeginObject();
        json.WriteKey("@type").Write(typeUrl);
        PrintFields(*valueMessage, json);
        json.EndObject();

        return true;
    }

    void TProto2JsonPrinter::PrintSingleField(const Message& proto,
                                              const FieldDescriptor& field,
                                              IJsonOutput& json,
                                              TStringBuf key,
                                              bool inProtoMap) {
        Y_ABORT_UNLESS(!field.is_repeated(), "field is repeated.");

        if (!key) {
            key = MakeKey(field);
        }

#define FIELD_TO_JSON(EProtoCppType, ProtoGet)                         \
    case FieldDescriptor::EProtoCppType: {                             \
        json.WriteKey(key).Write(reflection->ProtoGet(proto, &field)); \
        break;                                                         \
    }

#define INT_FIELD_TO_JSON(EProtoCppType, ProtoGet)              \
    case FieldDescriptor::EProtoCppType: {                      \
        const auto value = reflection->ProtoGet(proto, &field); \
        if (NeedStringifyNumber(value)) {                       \
            json.WriteKey(key).Write(ToString(value));          \
        } else {                                                \
            json.WriteKey(key).Write(value);                    \
        }                                                       \
        break;                                                  \
    }

        const Reflection* reflection = proto.GetReflection();

        bool shouldPrintField = inProtoMap || reflection->HasField(proto, &field);
        if (!shouldPrintField && GetConfig().MissingSingleKeyMode == TProto2JsonConfig::MissingKeyExplicitDefaultThrowRequired) {
            if (field.has_default_value()) {
                shouldPrintField = true;
            } else if (field.is_required()) {
                ythrow yexception() << "Empty required protobuf field: "
                                    << field.full_name() << ".";
            }
        }
        shouldPrintField = shouldPrintField ||
            (GetConfig().MissingSingleKeyMode == TProto2JsonConfig::MissingKeyDefault && !field.containing_oneof());

        if (shouldPrintField) {
            switch (field.cpp_type()) {
                INT_FIELD_TO_JSON(CPPTYPE_INT32, GetInt32);
                INT_FIELD_TO_JSON(CPPTYPE_INT64, GetInt64);
                INT_FIELD_TO_JSON(CPPTYPE_UINT32, GetUInt32);
                INT_FIELD_TO_JSON(CPPTYPE_UINT64, GetUInt64);
                FIELD_TO_JSON(CPPTYPE_DOUBLE, GetDouble);
                FIELD_TO_JSON(CPPTYPE_FLOAT, GetFloat);
                FIELD_TO_JSON(CPPTYPE_BOOL, GetBool);

                case FieldDescriptor::CPPTYPE_MESSAGE: {
                    json.WriteKey(key);
                    if (Config.ConvertTimeAsString && HandleTimeConversion(reflection->GetMessage(proto, &field), json)) {
                        break;
                    }
                    const Message& msg = reflection->GetMessage(proto, &field);
                    if (Config.ConvertAny && TryPrintAny(msg, json)) {
                        break;
                    }
                    Print(msg, json);
                    break;
                }

                case FieldDescriptor::CPPTYPE_ENUM: {
                    PrintEnumValue<true>(key, reflection->GetEnum(proto, &field), json);
                    break;
                }

                case FieldDescriptor::CPPTYPE_STRING: {
                    TString scratch;
                    const TString& value = reflection->GetStringReference(proto, &field, &scratch);
                    PrintStringValue<true>(field, key, value, json);
                    break;
                }

                default:
                    ythrow yexception() << "Unknown protobuf field type: "
                                        << static_cast<int>(field.cpp_type()) << ".";
            }
        } else {
            switch (GetConfig().MissingSingleKeyMode) {
                case TProto2JsonConfig::MissingKeyNull: {
                    json.WriteKey(key).WriteNull();
                    break;
                }

                case TProto2JsonConfig::MissingKeySkip:
                case TProto2JsonConfig::MissingKeyExplicitDefaultThrowRequired:
                default:
                    break;
            }
        }
#undef FIELD_TO_JSON
    }

    void TProto2JsonPrinter::PrintRepeatedField(const Message& proto,
                                                const FieldDescriptor& field,
                                                IJsonOutput& json,
                                                TStringBuf key) {
        Y_ABORT_UNLESS(field.is_repeated(), "field isn't repeated.");

        const bool isMap = field.is_map() && GetConfig().MapAsObject;
        if (!key) {
            key = MakeKey(field);
        }

#define REPEATED_FIELD_TO_JSON(EProtoCppType, ProtoGet)                                \
    case FieldDescriptor::EProtoCppType: {                                             \
        for (size_t i = 0, endI = reflection->FieldSize(proto, &field); i < endI; ++i) \
            json.Write(reflection->ProtoGet(proto, &field, i));                        \
        break;                                                                         \
    }

        const Reflection* reflection = proto.GetReflection();

        if (reflection->FieldSize(proto, &field) > 0) {
            json.WriteKey(key);
            if (isMap) {
                json.BeginObject();
            } else {
                json.BeginList();
            }

            switch (field.cpp_type()) {
                REPEATED_FIELD_TO_JSON(CPPTYPE_INT32, GetRepeatedInt32);
                REPEATED_FIELD_TO_JSON(CPPTYPE_INT64, GetRepeatedInt64);
                REPEATED_FIELD_TO_JSON(CPPTYPE_UINT32, GetRepeatedUInt32);
                REPEATED_FIELD_TO_JSON(CPPTYPE_UINT64, GetRepeatedUInt64);
                REPEATED_FIELD_TO_JSON(CPPTYPE_DOUBLE, GetRepeatedDouble);
                REPEATED_FIELD_TO_JSON(CPPTYPE_FLOAT, GetRepeatedFloat);
                REPEATED_FIELD_TO_JSON(CPPTYPE_BOOL, GetRepeatedBool);

                case FieldDescriptor::CPPTYPE_MESSAGE: {
                    if (isMap) {
                        for (size_t i = 0, endI = reflection->FieldSize(proto, &field); i < endI; ++i) {
                            PrintKeyValue(reflection->GetRepeatedMessage(proto, &field, i), json);
                        }
                    } else {
                        for (size_t i = 0, endI = reflection->FieldSize(proto, &field); i < endI; ++i) {
                            Print(reflection->GetRepeatedMessage(proto, &field, i), json);
                        }
                    }
                    break;
                }

                case FieldDescriptor::CPPTYPE_ENUM: {
                    for (int i = 0, endI = reflection->FieldSize(proto, &field); i < endI; ++i)
                        PrintEnumValue<false>(TStringBuf(), reflection->GetRepeatedEnum(proto, &field, i), json);
                    break;
                }

                case FieldDescriptor::CPPTYPE_STRING: {
                    TString scratch;
                    for (int i = 0, endI = reflection->FieldSize(proto, &field); i < endI; ++i) {
                        const TString& value =
                            reflection->GetRepeatedStringReference(proto, &field, i, &scratch);
                        PrintStringValue<false>(field, TStringBuf(), value, json);
                    }
                    break;
                }

                default:
                    ythrow yexception() << "Unknown protobuf field type: "
                                        << static_cast<int>(field.cpp_type()) << ".";
            }

            if (isMap) {
                json.EndObject();
            } else {
                json.EndList();
            }
        } else {
            switch (GetConfig().MissingRepeatedKeyMode) {
                case TProto2JsonConfig::MissingKeyNull: {
                    json.WriteKey(key).WriteNull();
                    break;
                }

                case TProto2JsonConfig::MissingKeyDefault: {
                    json.WriteKey(key);
                    if (isMap) {
                        json.BeginObject().EndObject();
                    } else {
                        json.BeginList().EndList();
                    }
                    break;
                }

                case TProto2JsonConfig::MissingKeySkip:
                case TProto2JsonConfig::MissingKeyExplicitDefaultThrowRequired:
                default:
                    break;
            }
        }

#undef REPEATED_FIELD_TO_JSON
    }

    void TProto2JsonPrinter::PrintKeyValue(const NProtoBuf::Message& proto,
                                           IJsonOutput& json) {
        const FieldDescriptor* keyField = proto.GetDescriptor()->map_key();
        Y_ABORT_UNLESS(keyField, "Map entry key field not found.");
        TString key = MakeKey(proto, *keyField);
        const FieldDescriptor* valueField = proto.GetDescriptor()->map_value();
        Y_ABORT_UNLESS(valueField, "Map entry value field not found.");
        PrintSingleField(proto, *valueField, json, key, true);
    }

    TString TProto2JsonPrinter::MakeKey(const NProtoBuf::Message& proto,
                                        const NProtoBuf::FieldDescriptor& field) {
        const Reflection* reflection = proto.GetReflection();
        TString result;
        switch (field.cpp_type()) {
            case FieldDescriptor::CPPTYPE_INT32:
                result = ToString(reflection->GetInt32(proto, &field));
                break;
            case FieldDescriptor::CPPTYPE_INT64:
                result = ToString(reflection->GetInt64(proto, &field));
                break;
            case FieldDescriptor::CPPTYPE_UINT32:
                result = ToString(reflection->GetUInt32(proto, &field));
                break;
            case FieldDescriptor::CPPTYPE_UINT64:
                result = ToString(reflection->GetUInt64(proto, &field));
                break;
            case FieldDescriptor::CPPTYPE_DOUBLE:
                result = ToString(reflection->GetDouble(proto, &field));
                break;
            case FieldDescriptor::CPPTYPE_FLOAT:
                result = ToString(reflection->GetFloat(proto, &field));
                break;
            case FieldDescriptor::CPPTYPE_BOOL:
                result = ToString(reflection->GetBool(proto, &field));
                break;
            case FieldDescriptor::CPPTYPE_ENUM: {
                const EnumValueDescriptor* value = reflection->GetEnum(proto, &field);
                switch (GetConfig().EnumMode) {
                    case TProto2JsonConfig::EnumNumber:
                        result = ToString(value->number());
                        break;
                    case TProto2JsonConfig::EnumName:
                        result = value->name();
                        break;
                    case TProto2JsonConfig::EnumFullName:
                        result = value->full_name();
                        break;
                    case TProto2JsonConfig::EnumNameLowerCase:
                        result = value->name();
                        result.to_lower();
                        break;
                    case TProto2JsonConfig::EnumFullNameLowerCase:
                        result = value->full_name();
                        result.to_lower();
                        break;
                    default:
                        ythrow yexception() << "Unsupported enum mode.";
                }
                break;
            }
            case FieldDescriptor::CPPTYPE_STRING:
                result = reflection->GetString(proto, &field);
                break;
            default:
                ythrow yexception() << "Unsupported key type.";
        }

        return result;
    }

    void TProto2JsonPrinter::PrintField(const Message& proto,
                                        const FieldDescriptor& field,
                                        IJsonOutput& json,
                                        const TStringBuf key) {
        if (field.is_repeated())
            PrintRepeatedField(proto, field, json, key);
        else
            PrintSingleField(proto, field, json, key);
    }

    void TProto2JsonPrinter::PrintFields(const Message& proto, IJsonOutput& json) {
        const Descriptor* descriptor = proto.GetDescriptor();
        Y_ASSERT(descriptor);

        // Iterate over all non-extension fields
        for (int f = 0, endF = descriptor->field_count(); f < endF; ++f) {
            const FieldDescriptor* field = descriptor->field(f);
            Y_ASSERT(field);
            PrintField(proto, *field, json);
        }

        // Check extensions via ListFields
        std::vector<const FieldDescriptor*> fields;
        auto* ref = proto.GetReflection();
        ref->ListFields(proto, &fields);

        for (const FieldDescriptor* field : fields) {
            Y_ASSERT(field);
            if (field->is_extension()) {
                switch (GetConfig().ExtensionFieldNameMode) {
                    case TProto2JsonConfig::ExtFldNameFull:
                        PrintField(proto, *field, json, field->full_name());
                        break;
                    case TProto2JsonConfig::ExtFldNameShort:
                        PrintField(proto, *field, json);
                        break;
                }
            }
        }
    }

    void TProto2JsonPrinter::Print(const Message& proto, IJsonOutput& json, bool closeMap) {
        json.BeginObject();

        PrintFields(proto, json);

        if (closeMap) {
            json.EndObject();
        }
    }

    template <class T, class U>
    std::enable_if_t<!std::is_unsigned<T>::value, bool> ValueInRange(T value, U range) {
        return value > -range && value < range;
    }

    template <class T, class U>
    std::enable_if_t<std::is_unsigned<T>::value, bool> ValueInRange(T value, U range) {
        return value < (std::make_unsigned_t<U>)(range);
    }

    template <class T>
    bool TProto2JsonPrinter::NeedStringifyNumber(T value) const {
        constexpr long SAFE_INTEGER_RANGE_FLOAT = 1L << 24;
        constexpr long long SAFE_INTEGER_RANGE_DOUBLE = 1LL << 53;

        switch (GetConfig().StringifyNumbers) {
            case TProto2JsonConfig::StringifyLongNumbersNever:
                return false;
            case TProto2JsonConfig::StringifyLongNumbersForFloat:
                return !ValueInRange(value, SAFE_INTEGER_RANGE_FLOAT);
            case TProto2JsonConfig::StringifyLongNumbersForDouble:
                return !ValueInRange(value, SAFE_INTEGER_RANGE_DOUBLE);
            case TProto2JsonConfig::StringifyInt64Always:
                return std::is_same_v<T, i64> || std::is_same_v<T, ui64>;
        }

        return false;
    }

}

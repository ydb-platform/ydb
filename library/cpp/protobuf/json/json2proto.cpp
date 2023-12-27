#include "json2proto.h"
#include "util.h"

#include <library/cpp/json/json_value.h>
#include <library/cpp/protobuf/json/proto/enum_options.pb.h>

#include <google/protobuf/util/time_util.h>
#include <google/protobuf/message.h>
#include <google/protobuf/descriptor.h>

#include <util/generic/hash.h>
#include <util/generic/maybe.h>
#include <util/string/ascii.h>
#include <util/string/cast.h>

#define JSON_TO_FIELD(EProtoCppType, name, json, JsonCheckType, ProtoSet, JsonGet)        \
    case FieldDescriptor::EProtoCppType: {                                          \
        if (config.CastRobust) {                                                    \
            reflection->ProtoSet(&proto, &field, json.JsonGet##Robust());           \
            break;                                                                  \
        }                                                                           \
        if (!json.JsonCheckType()) {                                                \
            if (config.CastFromString && json.IsString()) {                         \
                if (config.DoNotCastEmptyStrings && json.GetString().empty()) {     \
                    /* Empty string is same as "no value" for scalar types.*/   \
                    break;                                                          \
                }                                                                   \
                reflection->ProtoSet(&proto, &field, FromString(json.GetString())); \
                break;                                                              \
            }                                                                       \
            ythrow yexception() << "Invalid type of JSON field " << name << ": "    \
                                << #JsonCheckType << "() failed while "             \
                                << #EProtoCppType << " is expected.";               \
        }                                                                           \
        reflection->ProtoSet(&proto, &field, json.JsonGet());                       \
        break;                                                                      \
    }

static TString GetFieldName(const google::protobuf::FieldDescriptor& field,
                            const NProtobufJson::TJson2ProtoConfig& config) {
    if (config.NameGenerator) {
        return config.NameGenerator(field);
    }

    if (config.UseJsonName) {
        Y_ASSERT(!field.json_name().empty());
        TString name = field.json_name();
        if (!field.has_json_name() && !name.empty()) {
            // FIXME: https://st.yandex-team.ru/CONTRIB-139
            name[0] = AsciiToLower(name[0]);
        }
        return name;
    }

    TString name = field.name();
    switch (config.FieldNameMode) {
        case NProtobufJson::TJson2ProtoConfig::FieldNameOriginalCase:
            break;
        case NProtobufJson::TJson2ProtoConfig::FieldNameLowerCase:
            name.to_lower();
            break;
        case NProtobufJson::TJson2ProtoConfig::FieldNameUpperCase:
            name.to_upper();
            break;
        case NProtobufJson::TJson2ProtoConfig::FieldNameCamelCase:
            if (!name.empty()) {
                name[0] = AsciiToLower(name[0]);
            }
            break;
        case NProtobufJson::TJson2ProtoConfig::FieldNameSnakeCase:
            NProtobufJson::ToSnakeCase(&name);
            break;
        case NProtobufJson::TJson2ProtoConfig::FieldNameSnakeCaseDense:
            NProtobufJson::ToSnakeCaseDense(&name);
            break;
        default:
            Y_DEBUG_ABORT_UNLESS(false, "Unknown FieldNameMode.");
    }
    return name;
}


static void JsonString2Duration(const NJson::TJsonValue& json,
                 google::protobuf::Message& proto,
                 const google::protobuf::FieldDescriptor& field,
                 const NProtobufJson::TJson2ProtoConfig& config) {
    using namespace google::protobuf;
    if (!json.GetString() && !config.CastRobust) {
        ythrow yexception() << "Invalid type of JSON field '" << field.name() << "': "
                            << "IsString() failed while "
                            << "CPPTYPE_STRING is expected.";
    }
    TString jsonString = json.GetStringRobust();

    Duration durationFromString;

    if (!util::TimeUtil::FromString(jsonString, &durationFromString)) {
        ythrow yexception() << "error while parsing google.protobuf.Duration from string on field '" <<
                                                                                    field.name() << "'";
    }

    proto.CopyFrom(durationFromString);

}

static void JsonString2Timestamp(const NJson::TJsonValue& json,
                 google::protobuf::Message& proto,
                 const google::protobuf::FieldDescriptor& field,
                 const NProtobufJson::TJson2ProtoConfig& config) {
    using namespace google::protobuf;
    if (!json.GetString() && !config.CastRobust) {
        ythrow yexception() << "Invalid type of JSON field '" << field.name() << "': "
                            << "IsString() failed while "
                            << "CPPTYPE_STRING is expected.";
    }
    TString jsonString = json.GetStringRobust();

    Timestamp timestampFromString;

    if (!util::TimeUtil::FromString(jsonString, &timestampFromString)) {
        ythrow yexception() << "error while parsing google.protobuf.Timestamp from string on field '" <<
                                                                                    field.name() << "'";
    }

    proto.CopyFrom(timestampFromString);
}

static void
JsonString2Field(const NJson::TJsonValue& json,
                 google::protobuf::Message& proto,
                 const google::protobuf::FieldDescriptor& field,
                 const NProtobufJson::TJson2ProtoConfig& config) {
    using namespace google::protobuf;

    const Reflection* reflection = proto.GetReflection();
    Y_ASSERT(!!reflection);

    if (!json.IsString() && !config.CastRobust) {
        ythrow yexception() << "Invalid type of JSON field '" << field.name() << "': "
                            << "IsString() failed while "
                            << "CPPTYPE_STRING is expected.";
    }
    TString value = json.GetStringRobust();
    for (size_t i = 0, endI = config.StringTransforms.size(); i < endI; ++i) {
        Y_ASSERT(!!config.StringTransforms[i]);
        if (!!config.StringTransforms[i]) {
            if (field.type() == google::protobuf::FieldDescriptor::TYPE_BYTES) {
                config.StringTransforms[i]->TransformBytes(value);
            } else {
                config.StringTransforms[i]->Transform(value);
            }
        }
    }

    if (field.is_repeated())
        reflection->AddString(&proto, &field, std::move(value));
    else
        reflection->SetString(&proto, &field, std::move(value));
}

static bool
HandleString2TimeConversion(const NJson::TJsonValue& json,
                 google::protobuf::Message& proto,
                 const google::protobuf::FieldDescriptor& field,
                 const NProtobufJson::TJson2ProtoConfig& config) {
    using namespace google::protobuf;
    auto type = proto.GetDescriptor()->well_known_type();

    if (type == Descriptor::WellKnownType::WELLKNOWNTYPE_DURATION) {
        JsonString2Duration(json, proto, field, config);
        return true;
    } else if (type == Descriptor::WellKnownType::WELLKNOWNTYPE_TIMESTAMP) {
        JsonString2Timestamp(json, proto, field, config);
        return true;
    }
    return false;
}

static const NProtoBuf::EnumValueDescriptor*
FindEnumValue(const NProtoBuf::EnumDescriptor* enumField,
              TStringBuf target, bool (*equals)(TStringBuf, TStringBuf)) {
    for (int i = 0; i < enumField->value_count(); i++) {
        auto* valueDescriptor = enumField->value(i);
        if (equals(valueDescriptor->name(), target)) {
            return valueDescriptor;
        }
    }
    return nullptr;
}

static const NProtoBuf::EnumValueDescriptor*
FindEnumValueByJsonName(const NProtoBuf::EnumDescriptor* enumField, TStringBuf target) {
    for (int i = 0; i < enumField->value_count(); i++) {
        auto* valueDescriptor = enumField->value(i);
        if (valueDescriptor->options().GetExtension(json_enum_value) == target) {
            return valueDescriptor;
        }
    }
    return nullptr;
}


static void
JsonEnum2Field(const NJson::TJsonValue& json,
               google::protobuf::Message& proto,
               const google::protobuf::FieldDescriptor& field,
               const NProtobufJson::TJson2ProtoConfig& config) {
    using namespace google::protobuf;

    const Reflection* reflection = proto.GetReflection();
    Y_ASSERT(!!reflection);

    const EnumDescriptor* enumField = field.enum_type();
    Y_ASSERT(!!enumField);

    /// @todo configure name/numerical value
    const EnumValueDescriptor* enumFieldValue = nullptr;

    if (json.IsInteger()) {
        const auto value = json.GetInteger();
        enumFieldValue = enumField->FindValueByNumber(value);
        if (!enumFieldValue) {
            ythrow yexception() << "Invalid integer value of JSON enum field: " << value << ".";
        }
    } else if (json.IsString()) {
        const auto& value = json.GetString();
        if (config.UseJsonEnumValue) {
            enumFieldValue = FindEnumValueByJsonName(enumField, value);
        } else {
            if (config.EnumValueMode == NProtobufJson::TJson2ProtoConfig::EnumCaseInsensetive) {
                enumFieldValue = FindEnumValue(enumField, value, AsciiEqualsIgnoreCase);
            } else if (config.EnumValueMode == NProtobufJson::TJson2ProtoConfig::EnumSnakeCaseInsensitive) {
                enumFieldValue = FindEnumValue(enumField, value, NProtobufJson::EqualsIgnoringCaseAndUnderscores);
            } else {
                enumFieldValue = enumField->FindValueByName(value);
            }
        }
        if (!enumFieldValue) {
            ythrow yexception() << "Invalid string value of JSON enum field: " << TStringBuf(value).Head(100) << ".";
        }
    } else {
        ythrow yexception() << "Invalid type of JSON enum field: not an integer/string.";
    }

    if (field.is_repeated()) {
        reflection->AddEnum(&proto, &field, enumFieldValue);
    } else {
        reflection->SetEnum(&proto, &field, enumFieldValue);
    }
}

static void
Json2SingleField(const NJson::TJsonValue& json,
                 google::protobuf::Message& proto,
                 const google::protobuf::FieldDescriptor& field,
                 const NProtobufJson::TJson2ProtoConfig& config,
                 bool isMapValue = false) {
    using namespace google::protobuf;

    const Reflection* reflection = proto.GetReflection();
    Y_ASSERT(!!reflection);

    const NJson::TJsonValue* fieldJsonPtr = &json;
    TString nameHolder;
    TStringBuf name;
    if (!isMapValue) {
        nameHolder = GetFieldName(field, config);
        name = nameHolder;
        const NJson::TJsonValue& fieldJson = json[name];
        if (auto fieldJsonType = fieldJson.GetType(); fieldJsonType == NJson::JSON_UNDEFINED || fieldJsonType == NJson::JSON_NULL) {
            if (field.is_required() && !field.has_default_value() && !reflection->HasField(proto, &field) && config.CheckRequiredFields) {
                ythrow yexception() << "JSON has no field for required field "
                                    << name << ".";
            }
            return;
        }
        if (name) {  // For compatibility with previous implementation. Not sure if GetFieldName is allowed to return empty strings,
            fieldJsonPtr = &fieldJson;
        }
    }

    const NJson::TJsonValue& fieldJson = *fieldJsonPtr;

    if (name && config.UnknownFieldsCollector) {
        config.UnknownFieldsCollector->OnEnterMapItem(nameHolder);
    }

    switch (field.cpp_type()) {
        JSON_TO_FIELD(CPPTYPE_INT32, field.name(), fieldJson, IsInteger, SetInt32, GetInteger);
        JSON_TO_FIELD(CPPTYPE_INT64, field.name(), fieldJson, IsInteger, SetInt64, GetInteger);
        JSON_TO_FIELD(CPPTYPE_UINT32, field.name(), fieldJson, IsInteger, SetUInt32, GetInteger);
        JSON_TO_FIELD(CPPTYPE_UINT64, field.name(), fieldJson, IsUInteger, SetUInt64, GetUInteger);
        JSON_TO_FIELD(CPPTYPE_DOUBLE, field.name(), fieldJson, IsDouble, SetDouble, GetDouble);
        JSON_TO_FIELD(CPPTYPE_FLOAT, field.name(), fieldJson, IsDouble, SetFloat, GetDouble);
        JSON_TO_FIELD(CPPTYPE_BOOL, field.name(), fieldJson, IsBoolean, SetBool, GetBoolean);

        case FieldDescriptor::CPPTYPE_STRING: {
            JsonString2Field(fieldJson, proto, field, config);
            break;
        }

        case FieldDescriptor::CPPTYPE_ENUM: {
            JsonEnum2Field(fieldJson, proto, field, config);
            break;
        }

        case FieldDescriptor::CPPTYPE_MESSAGE: {
            Message* innerProto = reflection->MutableMessage(&proto, &field);
            Y_ASSERT(!!innerProto);

            if (config.AllowString2TimeConversion &&
                                        HandleString2TimeConversion(fieldJson, *innerProto, field, config)) {
                break;
            }

            NProtobufJson::MergeJson2Proto(fieldJson, *innerProto, config);

            break;
        }

        default:
            ythrow yexception() << "Unknown protobuf field type: "
                                << static_cast<int>(field.cpp_type()) << ".";
    }

    if (name && config.UnknownFieldsCollector) {
        config.UnknownFieldsCollector->OnLeaveMapItem();
    }
}

static void
SetKey(NProtoBuf::Message& proto,
       const NProtoBuf::FieldDescriptor& field,
       const TString& key) {
    using namespace google::protobuf;
    using namespace NProtobufJson;

    const Reflection* reflection = proto.GetReflection();
    TString result;
    switch (field.cpp_type()) {
        case FieldDescriptor::CPPTYPE_INT32:
            reflection->SetInt32(&proto, &field, FromString<int32>(key));
            break;
        case FieldDescriptor::CPPTYPE_INT64:
            reflection->SetInt64(&proto, &field, FromString<int64>(key));
            break;
        case FieldDescriptor::CPPTYPE_UINT32:
            reflection->SetUInt32(&proto, &field, FromString<uint32>(key));
            break;
        case FieldDescriptor::CPPTYPE_UINT64:
            reflection->SetUInt64(&proto, &field, FromString<uint64>(key));
            break;
        case FieldDescriptor::CPPTYPE_BOOL:
            reflection->SetBool(&proto, &field, FromString<bool>(key));
            break;
        case FieldDescriptor::CPPTYPE_STRING:
            reflection->SetString(&proto, &field, key);
            break;
        default:
            ythrow yexception() << "Unsupported key type.";
    }
}

static void
Json2RepeatedFieldValue(const NJson::TJsonValue& jsonValue,
                        google::protobuf::Message& proto,
                        const google::protobuf::FieldDescriptor& field,
                        const NProtobufJson::TJson2ProtoConfig& config,
                        const google::protobuf::Reflection* reflection,
                        const TString* key = nullptr) {
    using namespace google::protobuf;

    switch (field.cpp_type()) {
        JSON_TO_FIELD(CPPTYPE_INT32, field.name(), jsonValue, IsInteger, AddInt32, GetInteger);
        JSON_TO_FIELD(CPPTYPE_INT64, field.name(), jsonValue, IsInteger, AddInt64, GetInteger);
        JSON_TO_FIELD(CPPTYPE_UINT32, field.name(), jsonValue, IsInteger, AddUInt32, GetInteger);
        JSON_TO_FIELD(CPPTYPE_UINT64, field.name(), jsonValue, IsUInteger, AddUInt64, GetUInteger);
        JSON_TO_FIELD(CPPTYPE_DOUBLE, field.name(), jsonValue, IsDouble, AddDouble, GetDouble);
        JSON_TO_FIELD(CPPTYPE_FLOAT, field.name(), jsonValue, IsDouble, AddFloat, GetDouble);
        JSON_TO_FIELD(CPPTYPE_BOOL, field.name(), jsonValue, IsBoolean, AddBool, GetBoolean);

        case FieldDescriptor::CPPTYPE_STRING: {
            JsonString2Field(jsonValue, proto, field, config);
            break;
        }

        case FieldDescriptor::CPPTYPE_ENUM: {
            JsonEnum2Field(jsonValue, proto, field, config);
            break;
        }

        case FieldDescriptor::CPPTYPE_MESSAGE: {
            Message* innerProto = reflection->AddMessage(&proto, &field);
            Y_ASSERT(!!innerProto);
            if (key) {
                const FieldDescriptor* keyField = innerProto->GetDescriptor()->map_key();
                Y_ENSURE(keyField, "Map entry key field not found: " << field.name());
                SetKey(*innerProto, *keyField, *key);

                const FieldDescriptor* valueField = innerProto->GetDescriptor()->map_value();
                Y_ENSURE(valueField, "Map entry value field not found.");
                Json2SingleField(jsonValue, *innerProto, *valueField, config, /*isMapValue=*/true);
            } else {
                NProtobufJson::MergeJson2Proto(jsonValue, *innerProto, config);
            }

            break;
        }

        default:
            ythrow yexception() << "Unknown protobuf field type: "
                                << static_cast<int>(field.cpp_type()) << ".";
    }
}

static void
Json2RepeatedField(const NJson::TJsonValue& json,
                   google::protobuf::Message& proto,
                   const google::protobuf::FieldDescriptor& field,
                   const NProtobufJson::TJson2ProtoConfig& config) {
    using namespace google::protobuf;

    TString name = GetFieldName(field, config);

    const NJson::TJsonValue& fieldJson = json[name];
    if (fieldJson.GetType() == NJson::JSON_UNDEFINED || fieldJson.GetType() == NJson::JSON_NULL)
        return;

    if (config.UnknownFieldsCollector) {
        config.UnknownFieldsCollector->OnEnterMapItem(name);
    }

    bool isMap = fieldJson.GetType() == NJson::JSON_MAP;
    if (isMap) {
        if (!config.MapAsObject) {
            ythrow yexception() << "Map as object representation is not allowed, field: " << field.name();
        } else if (!field.is_map() && !fieldJson.GetMap().empty()) {
            ythrow yexception() << "Field " << field.name() << " is not a map.";
        }
    }

    if (fieldJson.GetType() != NJson::JSON_ARRAY && !config.MapAsObject && !config.VectorizeScalars && !config.ValueVectorizer) {
        ythrow yexception() << "JSON field doesn't represent an array for "
                            << name
                            << "(actual type is "
                            << static_cast<int>(fieldJson.GetType()) << ").";
    }

    const Reflection* reflection = proto.GetReflection();
    Y_ASSERT(!!reflection);

    if (isMap) {
        const THashMap<TString, NJson::TJsonValue>& jsonMap = fieldJson.GetMap();
        for (const auto& x : jsonMap) {
            const TString& key = x.first;
            const NJson::TJsonValue& jsonValue = x.second;
            if (config.UnknownFieldsCollector) {
                config.UnknownFieldsCollector->OnEnterMapItem(key);
            }
            Json2RepeatedFieldValue(jsonValue, proto, field, config, reflection, &key);
            if (config.UnknownFieldsCollector) {
                config.UnknownFieldsCollector->OnLeaveMapItem();
            }
        }
    } else {
        if (config.ReplaceRepeatedFields) {
            reflection->ClearField(&proto, &field);
        }
        if (fieldJson.GetType() == NJson::JSON_ARRAY) {
            const NJson::TJsonValue::TArray& jsonArray = fieldJson.GetArray();
            ui64 id = 0;
            for (const NJson::TJsonValue& jsonValue : jsonArray) {
                if (config.UnknownFieldsCollector) {
                    config.UnknownFieldsCollector->OnEnterArrayItem(id);
                }
                Json2RepeatedFieldValue(jsonValue, proto, field, config, reflection);
                if (config.UnknownFieldsCollector) {
                    config.UnknownFieldsCollector->OnLeaveArrayItem();
                }
                ++id;
            }
        } else if (config.ValueVectorizer) {
            ui64 id = 0;
            for (const NJson::TJsonValue& jsonValue : config.ValueVectorizer(fieldJson)) {
                if (config.UnknownFieldsCollector) {
                    config.UnknownFieldsCollector->OnEnterArrayItem(id);
                }
                Json2RepeatedFieldValue(jsonValue, proto, field, config, reflection);
                if (config.UnknownFieldsCollector) {
                    config.UnknownFieldsCollector->OnLeaveArrayItem();
                }
                ++id;
            }
        } else if (config.VectorizeScalars) {
            Json2RepeatedFieldValue(fieldJson, proto, field, config, reflection);
        }
    }

    if (config.UnknownFieldsCollector) {
        config.UnknownFieldsCollector->OnLeaveMapItem();
    }
}

namespace NProtobufJson {
    void MergeJson2Proto(const NJson::TJsonValue& json, google::protobuf::Message& proto, const TJson2ProtoConfig& config) {
        if (json.IsNull()) {
            return;
        }

        const google::protobuf::Descriptor* descriptor = proto.GetDescriptor();
        Y_ASSERT(!!descriptor);
        Y_ENSURE(json.IsMap(), "Failed to merge json to proto for message: " << descriptor->full_name() << ", expected json map.");

        for (int f = 0, endF = descriptor->field_count(); f < endF; ++f) {
            const google::protobuf::FieldDescriptor* field = descriptor->field(f);
            Y_ASSERT(!!field);

            if (field->is_repeated()) {
                Json2RepeatedField(json, proto, *field, config);
            } else {
                Json2SingleField(json, proto, *field, config);
            }
        }

        if (!config.AllowUnknownFields || config.UnknownFieldsCollector) {
            THashMap<TString, bool> knownFields;
            for (int f = 0, endF = descriptor->field_count(); f < endF; ++f) {
                const google::protobuf::FieldDescriptor* field = descriptor->field(f);
                knownFields[GetFieldName(*field, config)] = 1;
            }
            for (const auto& f : json.GetMap()) {
                const bool isFieldKnown = knownFields.contains(f.first);
                Y_ENSURE(config.AllowUnknownFields || isFieldKnown, "unknown field \"" << f.first << "\" for \"" << descriptor->full_name() << "\"");
                if (!isFieldKnown) {
                    config.UnknownFieldsCollector->OnUnknownField(f.first, *descriptor);
                }
            }
        }
    }

    void MergeJson2Proto(const TStringBuf& json, google::protobuf::Message& proto, const TJson2ProtoConfig& config) {
        NJson::TJsonReaderConfig jsonCfg;
        jsonCfg.DontValidateUtf8 = true;
        jsonCfg.AllowComments = config.AllowComments;

        NJson::TJsonValue jsonValue;
        ReadJsonTree(json, &jsonCfg, &jsonValue, /* throwOnError = */ true);

        MergeJson2Proto(jsonValue, proto, config);
    }

    void Json2Proto(const NJson::TJsonValue& json, google::protobuf::Message& proto, const TJson2ProtoConfig& config) {
        proto.Clear();
        MergeJson2Proto(json, proto, config);
    }

    void Json2Proto(const TStringBuf& json, google::protobuf::Message& proto, const TJson2ProtoConfig& config) {
        proto.Clear();
        MergeJson2Proto(json, proto, config);
    }
}

#pragma once

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/protobuf/json/json_output_create.h>
#include <library/cpp/protobuf/json/proto2json.h>
#include <library/cpp/protobuf/json/proto2json_printer.h>
#include <library/cpp/string_utils/base64/base64.h>
#include <ydb/library/naming_conventions/naming_conventions.h>
#include <ydb/public/sdk/cpp/client/ydb_datastreams/datastreams.h>
#include <ydb/library/http_proxy/error/error.h>
#include <contrib/libs/protobuf/src/google/protobuf/message.h>
#include <contrib/libs/protobuf/src/google/protobuf/reflection.h>

#include <nlohmann/json.hpp>


namespace NKikimr::NHttpProxy {

inline TString ProxyFieldNameConverter(const google::protobuf::FieldDescriptor& descriptor) {
    return NNaming::SnakeToCamelCase(descriptor.name());
}

class TYdsProtoToJsonPrinter : public NProtobufJson::TProto2JsonPrinter {
public:
    TYdsProtoToJsonPrinter(const google::protobuf::Reflection* reflection,
                           const NProtobufJson::TProto2JsonConfig& config,
                           bool skipBase64Encode)
    : NProtobufJson::TProto2JsonPrinter(config)
    , ProtoReflection(reflection)
    , SkipBase64Encode(skipBase64Encode)
    {}

protected:
    template <bool InMapContext>
    void PrintDoubleValue(const TStringBuf& key, double value,
                          NProtobufJson::IJsonOutput& json) {
        if constexpr(InMapContext) {
            json.WriteKey(key).Write(value);
        } else {
            json.Write(value);
        }
    }

    void PrintField(const NProtoBuf::Message& proto, const NProtoBuf::FieldDescriptor& field,
                    NProtobufJson::IJsonOutput& json, TStringBuf key = {}) override {
        if (field.options().HasExtension(Ydb::FieldTransformation::FieldTransformer)) {
            if (field.options().GetExtension(Ydb::FieldTransformation::FieldTransformer) ==
                Ydb::FieldTransformation::TRANSFORM_BASE64) {
                Y_ENSURE(field.cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_STRING,
                         "Base64 is only supported for strings");
                if (!key) {
                    key = MakeKey(field);
                }

                auto maybeBase64Encode = [skipBase64Encode = this->SkipBase64Encode, &key](const TString& str) {
                    if (key == "Data" && skipBase64Encode) {
                        return str;
                    }

                    return Base64Encode(str);
                };

                if (field.is_repeated()) {
                    for (int i = 0, endI = ProtoReflection->FieldSize(proto, &field); i < endI; ++i) {
                        PrintStringValue<false>(field, TStringBuf(),
                            maybeBase64Encode(proto.GetReflection()->GetRepeatedString(proto, &field, i)), json);
                    }
                } else {
                    PrintStringValue<true>(field, key,
                        maybeBase64Encode(proto.GetReflection()->GetString(proto, &field)), json);
                }
                return;
            }

            if (field.options().GetExtension(Ydb::FieldTransformation::FieldTransformer) ==
                Ydb::FieldTransformation::TRANSFORM_DOUBLE_S_TO_INT_MS) {
                Y_ENSURE(field.cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_INT64,
                         "Double S to Int MS is only supported for int64 timestamps");

                if (!key) {
                    key = MakeKey(field);
                }

                if (field.is_repeated()) {
                    for (int i = 0, endI = ProtoReflection->FieldSize(proto, &field); i < endI; ++i) {
                        double value = proto.GetReflection()->GetRepeatedInt64(proto, &field, i) / 1000.0;
                        PrintDoubleValue<false>(TStringBuf(), value, json);
                    }
                } else {
                    double value = proto.GetReflection()->GetInt64(proto, &field) / 1000.0;
                    PrintDoubleValue<true>(key, value, json);
                }
                return;
            }

            if (field.options().GetExtension(Ydb::FieldTransformation::FieldTransformer) ==
                Ydb::FieldTransformation::TRANSFORM_EMPTY_TO_NOTHING) {
                Y_ENSURE(field.cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_STRING,
                         "Empty to nothing is only supported for strings");

                if (!key) {
                    key = MakeKey(field);
                }

                if (field.is_repeated()) {
                    for (int i = 0, endI = ProtoReflection->FieldSize(proto, &field); i < endI; ++i) {
                        auto value = proto.GetReflection()->GetRepeatedString(proto, &field, i);
                        if (!value.empty()) {
                            PrintStringValue<false>(field, TStringBuf(),
                                proto.GetReflection()->GetRepeatedString(proto, &field, i), json);
                        }
                    }
                } else {
                    auto value = proto.GetReflection()->GetString(proto, &field);
                    if (!value.empty()) {
                        PrintStringValue<true>(field, key,
                            proto.GetReflection()->GetString(proto, &field), json);
                    }
                }
                return;
            }
        } else {
            return NProtobufJson::TProto2JsonPrinter::PrintField(proto, field, json, key);
        }
    }

private:
    const google::protobuf::Reflection* ProtoReflection = nullptr;
    bool SkipBase64Encode;
};

inline void ProtoToJson(const NProtoBuf::Message& resp, NJson::TJsonValue& value, bool skipBase64Encode) {
    auto config = NProtobufJson::TProto2JsonConfig()
                  .SetFormatOutput(false)
                  .SetMissingSingleKeyMode(NProtobufJson::TProto2JsonConfig::MissingKeyDefault)
                  .SetNameGenerator(ProxyFieldNameConverter)
                  .SetMapAsObject(true)
                  .SetEnumMode(NProtobufJson::TProto2JsonConfig::EnumName);
    TYdsProtoToJsonPrinter printer(resp.GetReflection(), config, skipBase64Encode);
    printer.Print(resp, *NProtobufJson::CreateJsonMapOutput(value));
}

template<typename JSON, typename MAP>
inline void AddJsonObjectToProtoAsMap(
        const google::protobuf::FieldDescriptor* fieldDescriptor,
        const google::protobuf::Reflection* reflection,
        grpc::protobuf::Message* message,
        const JSON& jsonObject,
        std::function<const MAP(const JSON&)> extractMap,
        std::function<const TString(const JSON&)> valueToString
) {
    const auto& protoMap = reflection->GetMutableRepeatedFieldRef<google::protobuf::Message>(message, fieldDescriptor);
    for (const auto& [key, value] : extractMap(jsonObject)) {
        std::unique_ptr<google::protobuf::Message> stringStringEntry(
            google::protobuf::MessageFactory::generated_factory()
                ->GetPrototype(fieldDescriptor->message_type())
                ->New(message->GetArena())
        );
        stringStringEntry
            ->GetReflection()
            ->SetString(stringStringEntry.get(), fieldDescriptor->message_type()->field(0), key);
        stringStringEntry
            ->GetReflection()
            ->SetString(stringStringEntry.get(), fieldDescriptor->message_type()->field(1), valueToString(value));
        protoMap.Add(*stringStringEntry);
    }
}

inline void AddJsonObjectToProtoAsMap(
        const google::protobuf::FieldDescriptor* fieldDescriptor,
        const google::protobuf::Reflection* reflection,
        grpc::protobuf::Message* message,
        const NJson::TJsonValue& jsonObject
) {
    AddJsonObjectToProtoAsMap<NJson::TJsonValue, NJson::TJsonValue::TMapType>(
        fieldDescriptor,
        reflection,
        message,
        jsonObject,
        [](auto& json) { return json.GetMap(); },
        [](auto& value) -> const TString { return value.GetString(); }
    );
}

inline void AddJsonObjectToProtoAsMap(
    const google::protobuf::FieldDescriptor* fieldDescriptor,
    const google::protobuf::Reflection* reflection,
    grpc::protobuf::Message* message,
    const nlohmann::basic_json<>& jsonObject
) {
    AddJsonObjectToProtoAsMap<nlohmann::basic_json<>, std::map<TString, nlohmann::basic_json<>>>(
        fieldDescriptor,
        reflection,
        message,
        jsonObject,
        [](auto& json) { return json.template get<std::map<TString, nlohmann::basic_json<>>>(); },
        [](auto& value) -> const TString { return value.template get<TString>(); }
    );
}

inline void JsonToProto(const NJson::TJsonValue& jsonValue, NProtoBuf::Message* message, ui32 depth = 0) {
    Y_ENSURE(depth < 101, "Json depth is > 100");
    Y_ENSURE_EX(
        !jsonValue.IsNull(),
        NKikimr::NSQS::TSQSException(NKikimr::NSQS::NErrors::MISSING_PARAMETER) <<
            "Top level of json value is not a map"
    );
    auto* desc = message->GetDescriptor();
    auto* reflection = message->GetReflection();
    for (const auto& [key, value] : jsonValue.GetMap()) {
        auto* fieldDescriptor = desc->FindFieldByName(NNaming::CamelToSnakeCase(key));
        Y_ENSURE_EX(
            fieldDescriptor,
            NKikimr::NSQS::TSQSException(NKikimr::NSQS::NErrors::INVALID_QUERY_PARAMETER) <<
            "Unexpected json key: " << key
        );
        Y_ENSURE(fieldDescriptor, "Unexpected json key: " + key);
        auto transformer = Ydb::FieldTransformation::TRANSFORM_NONE;
        if (fieldDescriptor->options().HasExtension(Ydb::FieldTransformation::FieldTransformer)) {
            transformer = fieldDescriptor->options().GetExtension(Ydb::FieldTransformation::FieldTransformer);
        }

        if (value.IsArray()) {
            Y_ENSURE(fieldDescriptor->is_repeated());
            for (auto& elem : value.GetArray()) {
                switch (transformer) {
                    case Ydb::FieldTransformation::TRANSFORM_BASE64: {
                        Y_ENSURE(fieldDescriptor->cpp_type() ==
                                 google::protobuf::FieldDescriptor::CPPTYPE_STRING,
                                 "Base64 transformer is only applicable to strings");
                        reflection->AddString(message, fieldDescriptor, Base64Decode(elem.GetString()));
                        break;
                    }
                    case Ydb::FieldTransformation::TRANSFORM_DOUBLE_S_TO_INT_MS: {
                        reflection->AddInt64(message, fieldDescriptor, elem.GetDouble() * 1000);
                        break;
                    }
                    case Ydb::FieldTransformation::TRANSFORM_EMPTY_TO_NOTHING:
                    case Ydb::FieldTransformation::TRANSFORM_NONE: {
                        switch (fieldDescriptor->cpp_type()) {
                            case google::protobuf::FieldDescriptor::CPPTYPE_INT32:
                                reflection->AddInt32(message, fieldDescriptor, elem.GetInteger());
                                break;
                            case google::protobuf::FieldDescriptor::CPPTYPE_INT64:
                                reflection->AddInt64(message, fieldDescriptor, elem.GetInteger());
                                break;
                            case google::protobuf::FieldDescriptor::CPPTYPE_UINT32:
                                reflection->AddUInt32(message, fieldDescriptor, elem.GetUInteger());
                                break;
                            case google::protobuf::FieldDescriptor::CPPTYPE_UINT64:
                                reflection->AddUInt64(message, fieldDescriptor, elem.GetUInteger());
                                break;
                            case google::protobuf::FieldDescriptor::CPPTYPE_DOUBLE:
                                reflection->AddDouble(message, fieldDescriptor, elem.GetDouble());
                                break;
                            case google::protobuf::FieldDescriptor::CPPTYPE_FLOAT:
                                reflection->AddFloat(message, fieldDescriptor, elem.GetDouble());
                                break;
                            case google::protobuf::FieldDescriptor::CPPTYPE_BOOL:
                                reflection->AddBool(message, fieldDescriptor, elem.GetBoolean());
                                break;
                            case google::protobuf::FieldDescriptor::CPPTYPE_ENUM:
                                {
                                    const NProtoBuf::EnumValueDescriptor* enumValueDescriptor =
                                            fieldDescriptor->enum_type()->FindValueByName(elem.GetString());
                                    i32 number{0};
                                    if (enumValueDescriptor == nullptr &&
                                        TryFromString(elem.GetString(), number)) {
                                        enumValueDescriptor =
                                                fieldDescriptor->enum_type()->FindValueByNumber(number);
                                    }
                                    if (enumValueDescriptor != nullptr) {
                                        reflection->AddEnum(message, fieldDescriptor, enumValueDescriptor);
                                    }
                                }
                                break;
                            case google::protobuf::FieldDescriptor::CPPTYPE_STRING:
                                reflection->AddString(message, fieldDescriptor, elem.GetString());
                                break;
                            case google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE: {
                                NProtoBuf::Message *msg = reflection->AddMessage(message, fieldDescriptor);
                                JsonToProto(elem, msg, depth + 1);
                                break;
                            }
                            default:
                                Y_ENSURE(false, "Unexpected type");
                        }
                        break;
                    }
                    default:
                        Y_ENSURE(false, "Unknown transformer type");
                }
            }
        } else {
            switch (transformer) {
                case Ydb::FieldTransformation::TRANSFORM_BASE64: {
                    Y_ENSURE(fieldDescriptor->cpp_type() ==
                             google::protobuf::FieldDescriptor::CPPTYPE_STRING,
                             "Base64 transformer is applicable only to strings");
                    reflection->SetString(message, fieldDescriptor, Base64Decode(value.GetString()));
                    break;
                }
                case Ydb::FieldTransformation::TRANSFORM_DOUBLE_S_TO_INT_MS: {
                    reflection->SetInt64(message, fieldDescriptor, value.GetDouble() * 1000);
                    break;
                }
                case Ydb::FieldTransformation::TRANSFORM_EMPTY_TO_NOTHING:
                case Ydb::FieldTransformation::TRANSFORM_NONE: {
                    switch (fieldDescriptor->cpp_type()) {
                        case google::protobuf::FieldDescriptor::CPPTYPE_INT32:
                            reflection->SetInt32(message, fieldDescriptor, value.GetInteger());
                            break;
                        case google::protobuf::FieldDescriptor::CPPTYPE_INT64:
                            reflection->SetInt64(message, fieldDescriptor, value.GetInteger());
                            break;
                        case google::protobuf::FieldDescriptor::CPPTYPE_UINT32:
                            reflection->SetUInt32(message, fieldDescriptor, value.GetUInteger());
                            break;
                        case google::protobuf::FieldDescriptor::CPPTYPE_UINT64:
                            reflection->SetUInt64(message, fieldDescriptor, value.GetUInteger());
                            break;
                        case google::protobuf::FieldDescriptor::CPPTYPE_DOUBLE:
                            reflection->SetDouble(message, fieldDescriptor, value.GetDouble());
                            break;
                        case google::protobuf::FieldDescriptor::CPPTYPE_FLOAT:
                            reflection->SetFloat(message, fieldDescriptor, value.GetDouble());
                            break;
                        case google::protobuf::FieldDescriptor::CPPTYPE_BOOL:
                            reflection->SetBool(message, fieldDescriptor, value.GetBoolean());
                            break;
                        case google::protobuf::FieldDescriptor::CPPTYPE_ENUM: {
                            const NProtoBuf::EnumValueDescriptor* enumValueDescriptor =
                                    fieldDescriptor->enum_type()->FindValueByName(value.GetString());
                            i32 number{0};
                            if (enumValueDescriptor == nullptr &&
                                TryFromString(value.GetString(), number)) {
                                enumValueDescriptor =
                                        fieldDescriptor->enum_type()->FindValueByNumber(number);
                            }
                            if (enumValueDescriptor != nullptr) {
                                reflection->SetEnum(message, fieldDescriptor, enumValueDescriptor);
                            }
                            break;
                        }
                        case google::protobuf::FieldDescriptor::CPPTYPE_STRING:
                            reflection->SetString(message, fieldDescriptor, value.GetString());
                            break;
                        case google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE: {
                            if (fieldDescriptor->is_map()) {
                                AddJsonObjectToProtoAsMap(fieldDescriptor, reflection, message, value);
                            } else {
                                auto *msg = reflection->MutableMessage(message, fieldDescriptor);
                                JsonToProto(value, msg, depth + 1);
                            }
                            break;
                        }
                        default:
                            Y_ENSURE(false, "Unexpected type");
                    }
                    break;
                }
                default: Y_ENSURE(false, "Unexpected transformer");
            }
        }
    }
}

inline void NlohmannJsonToProto(const nlohmann::json& jsonValue, NProtoBuf::Message* message, ui32 depth = 0) {
    Y_ENSURE(depth < 101, "Json depth is > 100");
    Y_ENSURE_EX(
        !jsonValue.is_null(),
        NKikimr::NSQS::TSQSException(NKikimr::NSQS::NErrors::MISSING_PARAMETER) <<
            "Top level of json value is not a map"
    );
    auto* desc = message->GetDescriptor();
    auto* reflection = message->GetReflection();
    for (const auto& [key, value] : jsonValue.get<std::unordered_map<std::string, nlohmann::json>>()) {
        auto* fieldDescriptor = desc->FindFieldByName(NNaming::CamelToSnakeCase(key.c_str()));
        Y_ENSURE(fieldDescriptor, "Unexpected json key: " + key);
        auto transformer = Ydb::FieldTransformation::TRANSFORM_NONE;
        if (fieldDescriptor->options().HasExtension(Ydb::FieldTransformation::FieldTransformer)) {
            transformer = fieldDescriptor->options().GetExtension(Ydb::FieldTransformation::FieldTransformer);
        }

        if (value.is_array()) {
            Y_ENSURE(fieldDescriptor->is_repeated());
            for (auto& elem : value) {
                switch (transformer) {
                    case Ydb::FieldTransformation::TRANSFORM_BASE64: {
                        Y_ENSURE(fieldDescriptor->cpp_type() ==
                                 google::protobuf::FieldDescriptor::CPPTYPE_STRING,
                                 "Base64 transformer is only applicable to strings");
                        if (elem.is_binary()) {
                            reflection->AddString(message, fieldDescriptor, std::string(elem.get_binary().begin(), elem.get_binary().end()));
                        } else {
                            reflection->AddString(message, fieldDescriptor, Base64Decode(elem.get<std::string>()));
                        }
                        break;
                    }
                    case Ydb::FieldTransformation::TRANSFORM_DOUBLE_S_TO_INT_MS: {
                        reflection->AddInt64(message, fieldDescriptor, elem.get<double>() * 1000);
                        break;
                    }
                    case Ydb::FieldTransformation::TRANSFORM_EMPTY_TO_NOTHING:
                    case Ydb::FieldTransformation::TRANSFORM_NONE: {
                        switch (fieldDescriptor->cpp_type()) {
                            case google::protobuf::FieldDescriptor::CPPTYPE_INT32:
                                reflection->AddInt32(message, fieldDescriptor, elem.get<i32>());
                                break;
                            case google::protobuf::FieldDescriptor::CPPTYPE_INT64:
                                reflection->AddInt64(message, fieldDescriptor, elem.get<i32>());
                                break;
                            case google::protobuf::FieldDescriptor::CPPTYPE_UINT32:
                                reflection->AddUInt32(message, fieldDescriptor, elem.get<ui32>());
                                break;
                            case google::protobuf::FieldDescriptor::CPPTYPE_UINT64:
                                reflection->AddUInt64(message, fieldDescriptor, elem.get<ui32>());
                                break;
                            case google::protobuf::FieldDescriptor::CPPTYPE_DOUBLE:
                                reflection->AddDouble(message, fieldDescriptor, elem.get<double>());
                                break;
                            case google::protobuf::FieldDescriptor::CPPTYPE_FLOAT:
                                reflection->AddFloat(message, fieldDescriptor, elem.get<double>());
                                break;
                            case google::protobuf::FieldDescriptor::CPPTYPE_BOOL:
                                reflection->AddBool(message, fieldDescriptor, elem.get<bool>());
                                break;
                            case google::protobuf::FieldDescriptor::CPPTYPE_ENUM:
                                {
                                    const NProtoBuf::EnumValueDescriptor* enumValueDescriptor =
                                            fieldDescriptor->enum_type()->FindValueByName(elem.get<std::string>());
                                    i32 number{0};
                                    if (enumValueDescriptor == nullptr &&
                                        TryFromString(elem.get<std::string>(), number)) {
                                        enumValueDescriptor =
                                                fieldDescriptor->enum_type()->FindValueByNumber(number);
                                    }
                                    if (enumValueDescriptor != nullptr) {
                                        reflection->AddEnum(message, fieldDescriptor, enumValueDescriptor);
                                    }
                                }
                                break;
                            case google::protobuf::FieldDescriptor::CPPTYPE_STRING:
                                reflection->AddString(message, fieldDescriptor, elem.get<std::string>());
                                break;
                            case google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE: {
                                NProtoBuf::Message *msg = reflection->AddMessage(message, fieldDescriptor);
                                NlohmannJsonToProto(elem, msg, depth + 1);
                                break;
                            }
                            default:
                                Y_ENSURE(false, "Unexpected type");
                        }
                        break;
                    }
                    default:
                        Y_ENSURE(false, "Unknown transformer type");
                }
            }
        } else {
            switch (transformer) {
                case Ydb::FieldTransformation::TRANSFORM_BASE64: {
                    Y_ENSURE(fieldDescriptor->cpp_type() ==
                             google::protobuf::FieldDescriptor::CPPTYPE_STRING,
                             "Base64 transformer is applicable only to strings");
                    if (value.is_binary()) {
                        reflection->SetString(message, fieldDescriptor, std::string(value.get_binary().begin(), value.get_binary().end()));
                    } else {
                        reflection->SetString(message, fieldDescriptor, Base64Decode(value.get<std::string>()));
                    }
                    break;
                }
                case Ydb::FieldTransformation::TRANSFORM_DOUBLE_S_TO_INT_MS: {
                    reflection->SetInt64(message, fieldDescriptor, value.get<double>() * 1000);
                    break;
                }
                case Ydb::FieldTransformation::TRANSFORM_EMPTY_TO_NOTHING:
                case Ydb::FieldTransformation::TRANSFORM_NONE: {
                    switch (fieldDescriptor->cpp_type()) {
                        case google::protobuf::FieldDescriptor::CPPTYPE_INT32:
                            reflection->SetInt32(message, fieldDescriptor, value.get<i32>());
                            break;
                        case google::protobuf::FieldDescriptor::CPPTYPE_INT64:
                            reflection->SetInt64(message, fieldDescriptor, value.get<i32>());
                            break;
                        case google::protobuf::FieldDescriptor::CPPTYPE_UINT32:
                            reflection->SetUInt32(message, fieldDescriptor, value.get<ui32>());
                            break;
                        case google::protobuf::FieldDescriptor::CPPTYPE_UINT64:
                            reflection->SetUInt64(message, fieldDescriptor, value.get<ui32>());
                            break;
                        case google::protobuf::FieldDescriptor::CPPTYPE_DOUBLE:
                            reflection->SetDouble(message, fieldDescriptor, value.get<double>());
                            break;
                        case google::protobuf::FieldDescriptor::CPPTYPE_FLOAT:
                            reflection->SetFloat(message, fieldDescriptor, value.get<double>());
                            break;
                        case google::protobuf::FieldDescriptor::CPPTYPE_BOOL:
                            reflection->SetBool(message, fieldDescriptor, value.get<bool>());
                            break;
                        case google::protobuf::FieldDescriptor::CPPTYPE_ENUM: {
                            const NProtoBuf::EnumValueDescriptor* enumValueDescriptor =
                                    fieldDescriptor->enum_type()->FindValueByName(value.get<std::string>());
                            i32 number{0};
                            if (enumValueDescriptor == nullptr &&
                                TryFromString(value.get<std::string>(), number)) {
                                enumValueDescriptor =
                                        fieldDescriptor->enum_type()->FindValueByNumber(number);
                            }
                            if (enumValueDescriptor != nullptr) {
                                reflection->SetEnum(message, fieldDescriptor, enumValueDescriptor);
                            }
                            break;
                        }
                        case google::protobuf::FieldDescriptor::CPPTYPE_STRING:
                            reflection->SetString(message, fieldDescriptor, value.get<std::string>());
                            break;
                        case google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE: {
                            if (fieldDescriptor->is_map()) {
                                AddJsonObjectToProtoAsMap(fieldDescriptor, reflection, message, value);
                            } else {
                                auto *msg = reflection->MutableMessage(message, fieldDescriptor);
                                NlohmannJsonToProto(value, msg, depth + 1);
                            }
                            break;
                        }
                        default:
                            Y_ENSURE(false, "Unexpected type");
                    }
                    break;
                }
                default: Y_ENSURE(false, "Unexpected transformer");
            }
        }
    }
}

} // namespace NKikimr::NHttpProxy

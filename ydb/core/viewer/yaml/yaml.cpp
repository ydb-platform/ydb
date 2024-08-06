#include <unordered_set>
#include <util/stream/str.h>
#include <google/protobuf/duration.pb.h>
#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/wrappers.pb.h>
#include <google/protobuf/util/time_util.h>
#include "yaml.h"

#ifdef GetMessage
#undef GetMessage
#endif

void TProtoToYaml::FillEnum(YAML::Node property, const ::google::protobuf::EnumDescriptor* enumDescriptor, const TEnumSettings& enumSettings) {
    auto enm = property["enum"];
    auto valueCount = enumDescriptor->value_count();
    TString defaultValue;
    for (int i = 0; i < valueCount; ++i) {
        auto enumValueDescriptor = enumDescriptor->value(i);
        auto enumName = enumValueDescriptor->name();
        if (enumSettings.ConvertToLowerCase) {
            enumName = to_lower(enumName);
        }
        if (!defaultValue) {
            defaultValue = enumName;
            if (enumSettings.SkipDefaultValue) {
                continue;
            }
        }
        enm.push_back(enumName);
    }
    if (defaultValue && !enumSettings.SkipDefaultValue) {
        property["default"] = defaultValue;
    }
}

YAML::Node TProtoToYaml::ProtoToYamlSchema(const ::google::protobuf::Descriptor* descriptor, std::unordered_set<const ::google::protobuf::Descriptor*>& descriptors) {
    using namespace ::google::protobuf;
    if (descriptor == nullptr) {
        return {};
    }
    YAML::Node to;
    to["type"] = "object";
    to["title"] = descriptor->name();
    int fields = descriptor->field_count();
    if (fields > 0) {
        auto properties = to["properties"];
        int oneofFields = descriptor->oneof_decl_count();
        for (int idx = 0; idx < oneofFields; ++idx) {
            const OneofDescriptor* fieldDescriptor = descriptor->oneof_decl(idx);
            if (fieldDescriptor->name().StartsWith("_")) {
                continue;
            }
            properties[fieldDescriptor->name()]["type"] = "oneOf";
        }
        for (int idx = 0; idx < fields; ++idx) {
            const FieldDescriptor* fieldDescriptor = descriptor->field(idx);
            auto property = properties[fieldDescriptor->name()];
            if (fieldDescriptor->is_repeated()) {
                property["type"] = "array";
                property.reset(property["items"]);
            }
            if (fieldDescriptor->cpp_type() == FieldDescriptor::CPPTYPE_MESSAGE) {
                if (fieldDescriptor->message_type()->full_name() == google::protobuf::Timestamp::descriptor()->full_name()) {
                    property["type"] = "string";
                    property["format"] = "date-time";
                } else if (fieldDescriptor->message_type()->full_name() == google::protobuf::Duration::descriptor()->full_name()) {
                    property["type"] = "string";
                    property["example"] = "3600s";
                } else if (fieldDescriptor->message_type()->full_name() == google::protobuf::BoolValue::descriptor()->full_name()) {
                    property["type"] = "boolean";
                } else if (fieldDescriptor->message_type()->full_name() == google::protobuf::StringValue::descriptor()->full_name()) {
                    property["type"] = "string";
                } else if (fieldDescriptor->message_type()->full_name() == google::protobuf::Int64Value::descriptor()->full_name()) {
                    property["type"] = "integer";
                    property["format"] = "int64";
                } else if (descriptors.insert(descriptor).second) {
                    property = ProtoToYamlSchema(fieldDescriptor->message_type(), descriptors);
                    descriptors.erase(descriptor);
                }
            } else {
                switch (fieldDescriptor->cpp_type()) {
                case FieldDescriptor::CPPTYPE_INT32:
                    property["type"] = "integer";
                    property["format"] = "int32";
                    break;
                case FieldDescriptor::CPPTYPE_UINT32:
                    property["type"] = "integer";
                    property["format"] = "uint32";
                    break;
                case FieldDescriptor::CPPTYPE_INT64:
                    property["type"] = "string"; // because of JS compatibility (JavaScript could not handle large numbers (bigger than 2^53))
                    property["format"] = "int64";
                    break;
                case FieldDescriptor::CPPTYPE_UINT64:
                    property["type"] = "string"; // because of JS compatibility (JavaScript could not handle large numbers (bigger than 2^53))
                    property["format"] = "uint64";
                    break;
                case FieldDescriptor::CPPTYPE_STRING:
                case FieldDescriptor::CPPTYPE_ENUM:
                    property["type"] = "string";
                    break;
                case FieldDescriptor::CPPTYPE_FLOAT:
                    property["type"] = "number";
                    property["format"] = "float";
                    break;
                case FieldDescriptor::CPPTYPE_DOUBLE:
                    property["type"] = "number";
                    property["format"] = "double";
                    break;
                case FieldDescriptor::CPPTYPE_BOOL:
                    property["type"] = "boolean";
                    break;
                case FieldDescriptor::CPPTYPE_MESSAGE:
                    property["type"] = "object";
                    break;
                }

                if (fieldDescriptor->cpp_type() == FieldDescriptor::CPPTYPE_ENUM) {
                    FillEnum(property, fieldDescriptor->enum_type());
                }
            }
        }
    }
    return to;
}

YAML::Node TProtoToYaml::ProtoToYamlSchema(const ::google::protobuf::Descriptor* descriptor) {
    std::unordered_set<const ::google::protobuf::Descriptor*> descriptors;
    return ProtoToYamlSchema(descriptor, descriptors);
}

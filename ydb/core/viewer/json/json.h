#pragma once
#include <unordered_map>
#include <unordered_set>
#include <functional>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>
#include <util/stream/output.h>

struct TJsonSettings {
    using TMapperKey = const ::google::protobuf::FieldDescriptor*;
    using TMapperValue = std::function<void(IOutputStream&, const ::google::protobuf::Message&, const TJsonSettings&)>;
    using TNameGenerator = std::function<TString(const google::protobuf::FieldDescriptor&)>;
    using TEnumValueFilter = std::function<bool(const TString& value)>;
    bool UI64AsString = true; // JavaScript could not handle large numbers (bigger than 2^53)
    bool EnumAsNumbers = true;
    bool EmptyRepeated = false;
    TString NaN = "null";
    std::unordered_map<TMapperKey, TMapperValue> FieldRemapper;
    TNameGenerator NameGenerator;
    TEnumValueFilter EnumValueFilter;
};

class TProtoToJson {
public:
    static void EscapeJsonString(IOutputStream& os, const TString& s);
    static TString EscapeJsonString(const TString& s);
    static void ProtoToJson(IOutputStream& to, const ::google::protobuf::Message& protoFrom, const TJsonSettings& jsonSettings = TJsonSettings());
    static void ProtoToJsonInline(IOutputStream& to, const ::google::protobuf::Message& protoFrom, const TJsonSettings& jsonSettings = TJsonSettings());
    static void ProtoToJsonSchema(IOutputStream& to, const TJsonSettings& jsonSettings, const ::google::protobuf::Descriptor* descriptor);

    static void ProtoToJson(IOutputStream& to, const ::google::protobuf::EnumValueDescriptor* descriptor, const TJsonSettings& jsonSettings = TJsonSettings());

    template <typename ProtoType>
    static void ProtoToJsonSchema(IOutputStream& to, const TJsonSettings& jsonSettings = TJsonSettings()) {
        ProtoToJsonSchema(to, jsonSettings, ProtoType::descriptor());
    }

protected:
    static void ProtoToJsonSchema(IOutputStream& to, const TJsonSettings& jsonSettings, const ::google::protobuf::Descriptor* descriptor, std::unordered_set<const ::google::protobuf::Descriptor*>& descriptors);
};

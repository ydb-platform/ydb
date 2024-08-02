#pragma once
#include <google/protobuf/message.h>
#include <library/cpp/yaml/as/tstring.h>
#include <unordered_map>
#include <unordered_set>
#include <util/stream/output.h>

struct TEnumSettings {
    bool ConvertToLowerCase = false;
    bool SkipDefaultValue = false;
};

class TProtoToYaml {
public:
    static YAML::Node ProtoToYamlSchema(const ::google::protobuf::Descriptor* descriptor);

    static void FillEnum(YAML::Node property, const ::google::protobuf::EnumDescriptor* enumDescriptor, const TEnumSettings& enumSettings = TEnumSettings());

    template <typename ProtoType>
    static YAML::Node ProtoToYamlSchema() {
        return ProtoToYamlSchema(ProtoType::descriptor());
    }

protected:
    static YAML::Node ProtoToYamlSchema(const ::google::protobuf::Descriptor* descriptor, std::unordered_set<const ::google::protobuf::Descriptor*>& descriptors);
};

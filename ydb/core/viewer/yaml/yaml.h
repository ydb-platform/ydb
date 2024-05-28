#pragma once
#include <unordered_map>
#include <unordered_set>
#include <google/protobuf/message.h>
#include <util/stream/output.h>
#include <library/cpp/yaml/as/tstring.h>

class TProtoToYaml {
public:
    static YAML::Node ProtoToYamlSchema(const ::google::protobuf::Descriptor* descriptor);

    template <typename ProtoType>
    static YAML::Node ProtoToYamlSchema() {
        return ProtoToYamlSchema(ProtoType::descriptor());
    }

protected:
    static YAML::Node ProtoToYamlSchema(const ::google::protobuf::Descriptor* descriptor, std::unordered_set<const ::google::protobuf::Descriptor*>& descriptors);
};

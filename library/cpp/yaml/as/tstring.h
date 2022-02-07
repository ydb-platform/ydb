#pragma once

#include <contrib/libs/yaml-cpp/include/yaml-cpp/yaml.h>

#include <util/generic/string.h>

namespace YAML {
    template <>
    inline TString Node::as<TString>() const {
        const auto& converted = as<std::string>();
        return TString(converted.c_str(), converted.size());
    }

    template <>
    struct convert<TString> {
        static Node encode(const TString& rhs) {
            return Node(std::string(rhs));
        }

        static bool decode(const Node& node, TString& rhs) {
            if (!node.IsScalar())
                return false;
            rhs = node.Scalar();
            return true;
        }
    };
}

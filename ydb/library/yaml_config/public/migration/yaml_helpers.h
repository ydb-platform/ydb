#pragma once

#include <ydb/library/yaml_config/public/yaml_config.h>

#include <optional>

namespace NKikimr::NYamlConfig::NMigrationDetail {

inline std::optional<NFyaml::TMapping> AsMap(const NFyaml::TNodeRef& node) {
    return node.Type() == NFyaml::ENodeType::Mapping ? std::make_optional(node.Map()) : std::nullopt;
}

inline std::optional<NFyaml::TSequence> AsSequence(const NFyaml::TNodeRef& node) {
    return node.Type() == NFyaml::ENodeType::Sequence ? std::make_optional(node.Sequence()) : std::nullopt;
}

inline std::optional<NFyaml::TMapping> FindMap(const NFyaml::TMapping& map, TStringBuf key) {
    const TString name(key);
    return map.Has(name) ? AsMap(map.at(name)) : std::nullopt;
}

inline std::optional<NFyaml::TSequence> FindSequence(const NFyaml::TMapping& map, TStringBuf key) {
    const TString name(key);
    return map.Has(name) ? AsSequence(map.at(name)) : std::nullopt;
}

inline std::optional<TString> FindScalar(const NFyaml::TMapping& map, TStringBuf key) {
    const TString name(key);
    if (!map.Has(name)) {
        return std::nullopt;
    }

    const auto node = map.at(name);
    return node.Type() == NFyaml::ENodeType::Scalar ? std::make_optional(node.Scalar()) : std::nullopt;
}

} // namespace NKikimr::NYamlConfig::NMigrationDetail

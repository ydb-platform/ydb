#include "yaml_config.h"
#include "yaml_config_impl.h"

#include <util/digest/sequence.h>

template <>
struct THash<NKikimr::NYamlConfig::TLabel> {
    inline size_t operator()(const NKikimr::NYamlConfig::TLabel& value) const {
        return CombineHashes(THash<TString>{}(value.Value), (size_t)value.Type);
    }
};

template <>
struct THash<TVector<TString>> {
    inline size_t operator()(const TVector<TString>& value) const {
        size_t result = 0;
        for (auto& str : value) {
            result = CombineHashes(result, THash<TString>{}(str));
        }
        return result;
    }
};

template <>
struct THash<TVector<NKikimr::NYamlConfig::TLabel>> : public TSimpleRangeHash {};

namespace NKikimr::NYamlConfig {

inline const TMap<TString, EYamlConfigLabelTypeClass> ClassMapping{
    std::pair{TString("enum"), EYamlConfigLabelTypeClass::Closed},
    std::pair{TString("string"), EYamlConfigLabelTypeClass::Open},
};

inline const TStringBuf inheritMapTag{"!inherit"};
inline const TStringBuf inheritSeqTag{"!inherit:"};
inline const TStringBuf inheritMapInSeqTag{"!inherit"};
inline const TStringBuf removeTag{"!remove"};
inline const TStringBuf appendTag{"!append"};

TString GetKey(const NFyaml::TNodeRef& node, TString key) {
    auto map = node.Map();
    auto k = map.at(key).Scalar();
    return k;
}

bool Fit(const TSelector& selector, const TSet<TNamedLabel>& labels) {
    bool result = true;
    size_t matched = 0;
    for (auto& label : labels) {
        if (auto it = selector.NotIn.find(label.Name); it != selector.NotIn.end()
            && it->second.Values.contains(label.Value)) {

            return false;
        }

        if (auto it = selector.In.find(label.Name); it != selector.In.end()) {
            if (!it->second.Values.contains(label.Value)) {
                result = false;
            } else {
                ++matched;
            }
        }
    }
    return (matched == selector.In.size()) && result;
}

TSelector ParseSelector(const NFyaml::TNodeRef& selectors) {
    TSelector result;
    if (selectors) {
        auto selectorsMap = selectors.Map();

        for (auto it = selectorsMap.begin(); it != selectorsMap.end(); ++it) {
            switch (it->Value().Type()) {
                case NFyaml::ENodeType::Scalar:
                {
                    auto [label, _] = result.In.try_emplace(
                        it->Key().Scalar(),
                        TLabelValueSet{});
                    label->second.Values.insert(it->Value().Scalar());
                }
                break;
                case NFyaml::ENodeType::Mapping:
                {
                    auto in = it->Value().Map()["in"];
                    auto notIn = it->Value().Map()["not_in"];
                    if (in && notIn) {
                        ythrow TYamlConfigEx() << "Using both in and not_in for same label: "
                            << it->Value().Path();
                    }

                    if (in) {
                        auto inSeq = in.Sequence();
                        auto [label, _] = result.In.try_emplace(
                            it->Key().Scalar(),
                            TLabelValueSet{});
                        for(auto it3 = inSeq.begin(); it3 != inSeq.end(); ++it3) {
                            label->second.Values.insert(it3->Scalar());
                        }
                    }

                    if (notIn) {
                        auto notInSeq = notIn.Sequence();
                        auto [label, _] = result.NotIn.try_emplace(
                            it->Key().Scalar(),
                            TLabelValueSet{});
                        for(auto it3 = notInSeq.begin(); it3 != notInSeq.end(); ++it3) {
                            label->second.Values.insert(it3->Scalar());
                        }
                    }
                }
                break;
                default:
                {
                    ythrow TYamlConfigEx() << "Selector should be scalar, \"in\" or \"not_in\": "
                        << it->Value().Path();
                }
                break;
            }
        }
    } else {
        ythrow TYamlConfigEx() << "Selector shouldn't be empty";
    }
    return result;
}

TYamlConfigModel ParseConfig(NFyaml::TDocument& doc) {
    TYamlConfigModel res{.Doc = doc};
    auto root = doc.Root().Map();
    res.Config = root.at("config");

    auto allowedLabels = root.at("allowed_labels").Map();

    for (auto it = allowedLabels.begin(); it != allowedLabels.end(); ++it) {
        auto type = it->Value().Map().at("type");
        if (!type || type.Type() != NFyaml::ENodeType::Scalar) {
            ythrow TYamlConfigEx() << "Label type should be Scalar";
        }

        EYamlConfigLabelTypeClass classType;

        if (auto classIt = ClassMapping.find(type.Scalar()); classIt != ClassMapping.end()) {
            classType = classIt->second;
        } else {
            ythrow TYamlConfigEx() << "Unsupported label type: " << type.Scalar();
        }

        auto label = res.AllowedLabels.try_emplace(
            it->Key().Scalar(),
            TLabelType{classType, TSet<TString>{""}});

        if (auto labelDesc = it->Value().Map()["values"]; labelDesc) {
            auto values = labelDesc.Map();
            for(auto it2 = values.begin(); it2 != values.end(); ++it2) {
                label.first->second.Values.insert(it2->Key().Scalar());
            }
        }
    }

    auto selectorConfig = root.at("selector_config").Sequence();

    for (auto it = selectorConfig.begin(); it != selectorConfig.end(); ++it) {
        TYamlConfigModel::TSelectorModel selector;

        auto selectorRoot = it->Map();
        selector.Description = selectorRoot.at("description").Scalar();
        selector.Config = selectorRoot.at("config");
        selector.Selector = ParseSelector(selectorRoot.at("selector"));

        res.Selectors.push_back(selector);
    }

    return res;
}

TMap<TString, TLabelType> CollectLabels(NFyaml::TDocument& doc) {

    auto config = ParseConfig(doc);

    TMap<TString, TLabelType> result = config.AllowedLabels;

    for (auto& selector : config.Selectors) {
        for (auto& [name, valueSet] : selector.Selector.In) {
            result[name].Values.insert(valueSet.Values.begin(), valueSet.Values.end());
        }

        for (auto& [name, valueSet] : selector.Selector.NotIn) {
            result[name].Values.insert(valueSet.Values.begin(), valueSet.Values.end());
        }
    }

    return result;
}

bool IsMapInherit(const NFyaml::TNodeRef& node) {
    if (auto tag = node.Tag(); tag) {
        switch (node.Type()) {
            case NFyaml::ENodeType::Mapping:
                return *tag == inheritMapTag;
            case NFyaml::ENodeType::Sequence:
                return tag->StartsWith(inheritSeqTag);
            case NFyaml::ENodeType::Scalar:
                return false;
        }
    }
    return false;
}

bool IsSeqInherit(const NFyaml::TNodeRef& node) {
    if (auto tag = node.Tag(); tag) {
        switch (node.Type()) {
            case NFyaml::ENodeType::Mapping:
                return *tag == inheritMapInSeqTag;
            case NFyaml::ENodeType::Sequence:
                return false;
            case NFyaml::ENodeType::Scalar:
                return false;
        }
    }
    return false;
}

void Append(NFyaml::TNodeRef& to, const NFyaml::TNodeRef& from);

bool IsSeqAppend(const NFyaml::TNodeRef& node) {
    if (auto tag = node.Tag(); tag) {
        switch (node.Type()) {
            case NFyaml::ENodeType::Mapping:
                return false;
            case NFyaml::ENodeType::Sequence:
                return *tag == appendTag;
            case NFyaml::ENodeType::Scalar:
                return false;
        }
    }
    return false;
}

bool IsRemove(const NFyaml::TNodeRef& node) {
    if (auto tag = node.Tag(); tag) {
        return *tag == removeTag;
    }
    return false;
}

void Inherit(NFyaml::TMapping& toMap, const NFyaml::TMapping& fromMap) {
    for (auto it = fromMap.begin(); it != fromMap.end(); ++it) {
        if (auto toEntry = toMap.pair_at_opt(it->Key().Scalar()); toEntry) {
            auto fromNode = it->Value();
            auto toNode = toEntry.Value();

            if (IsMapInherit(fromNode)) {
                Apply(toNode, fromNode);
            } else if (IsSeqAppend(fromNode)) {
                Append(toNode, fromNode);
            } else {
                toMap.Remove(toEntry.Key());
                toMap.Append(it->Key().Copy().Ref(), it->Value().Copy().Ref());
            }
        } else {
            toMap.Append(it->Key().Copy().Ref(), it->Value().Copy().Ref());
        }
    }
}

void Inherit(NFyaml::TSequence& toSeq, const NFyaml::TSequence& fromSeq, const TString& key) {
    TMap<TString, NFyaml::TNodeRef> nodes;

    for (auto it = toSeq.begin(); it != toSeq.end(); ++it) {
        nodes[GetKey(*it, key)] = *it;
    }

    for (auto it = fromSeq.begin(); it != fromSeq.end(); ++it) {
        auto fromKey = GetKey(*it, key);

        if (nodes.contains(fromKey)) {
            if (IsSeqInherit(*it)) {
                Apply(nodes[fromKey], *it);
            } else if (IsSeqAppend(*it)) {
                Append(nodes[fromKey], *it);
            } else if (IsRemove(*it)) {
                toSeq.Remove(nodes[fromKey]);
                nodes.erase(fromKey);
            } else {
                auto newNode = it->Copy();
                toSeq.InsertAfter(nodes[fromKey], newNode.Ref());
                toSeq.Remove(nodes[fromKey]);
                nodes[fromKey] = newNode.Ref();
            }
        } else {
            auto newNode = it->Copy();
            toSeq.Append(newNode.Ref());
            nodes[fromKey] = newNode.Ref();
        }
    }
}

void Append(NFyaml::TNodeRef& to, const NFyaml::TNodeRef& from) {
    Y_ENSURE_EX(to, TYamlConfigEx() << "Appending to empty value: "
                << to.Path() << " <- " << from.Path());
    Y_ENSURE_EX(to.Type() == NFyaml::ENodeType::Sequence && from.Type() == NFyaml::ENodeType::Sequence, TYamlConfigEx() << "Appending to wrong type"
                << to.Path() << " <- " << from.Path());

    auto fromSeq = from.Sequence();
    auto toSeq = to.Sequence();

    for (auto it = fromSeq.begin(); it != fromSeq.end(); ++it) {
        auto newNode = it->Copy();
        toSeq.Append(newNode.Ref());
    }
}

void Apply(NFyaml::TNodeRef& to, const NFyaml::TNodeRef& from) {
    Y_ENSURE_EX(to, TYamlConfigEx() << "Overriding empty value: "
                << to.Path() << " <- " << from.Path());
    Y_ENSURE_EX(to.Type() == from.Type(), TYamlConfigEx() << "Overriding value with different types: "
                << to.Path() << " <- " << from.Path());

    switch (from.Type()) {
        case NFyaml::ENodeType::Mapping:
        {
            auto toMap = to.Map();
            auto fromMap = from.Map();
            Inherit(toMap, fromMap);
        }
        break;
        case NFyaml::ENodeType::Sequence:
        {
            auto tag = from.Tag();
            auto key = tag->substr(inheritSeqTag.length());
            auto toSeq = to.Sequence();
            auto fromSeq = from.Sequence();
            Inherit(toSeq, fromSeq, key);
        }
        break;
        case NFyaml::ENodeType::Scalar:
        {
            ythrow TYamlConfigEx() << "Override with scalar: "
                << to.Path() << " <- " << from.Path();
        }
        break;
    }
}

void RemoveTags(NFyaml::TDocument& doc) {
    for (auto it = doc.begin(); it != doc.end(); ++it) {
        it->RemoveTag();
    }
}

TDocumentConfig Resolve(
    const NFyaml::TDocument& doc,
    const TSet<TNamedLabel>& labels)
{
    TDocumentConfig res{doc.Clone(), NFyaml::TNodeRef{}};
    res.first.Resolve();

    auto rootMap = res.first.Root().Map();
    auto config = rootMap.at("config");
    auto selectorConfig = rootMap.at("selector_config").Sequence();

    for (auto it = selectorConfig.begin(); it != selectorConfig.end(); ++it) {
        auto selectorMap = it->Map();
        auto desc = selectorMap.at("description").Scalar();
        auto selectorNode = selectorMap.at("selector");
        auto selector = ParseSelector(selectorNode);
        if (Fit(selector, labels)) {
            Apply(config, selectorMap.at("config"));
        }
    }

    RemoveTags(res.first);

    res.second = config;

    return res;
}

void Combine(
    TVector<TVector<TLabel>>& labelCombinations,
    TVector<TLabel>& combination,
    const TVector<std::pair<TString, TSet<TLabel>>>& labels,
    size_t offset)
{
    if (offset == labels.size()) {
        labelCombinations.push_back(combination);
        return;
    }

    for (auto& label : labels[offset].second) {
        combination[offset] = label;
        Combine(labelCombinations, combination, labels, offset + 1);
    }
}

bool Fit(
    const TSelector& selector,
    const TVector<TLabel>& labels,
    const TVector<std::pair<TString, TSet<TLabel>>>& names)
{
    for (size_t i = 0; i < labels.size(); ++ i) {
        auto& label = labels[i];
        auto& name = names[i].first;
        switch(label.Type) {
            case TLabel::EType::Negative:
                if (selector.In.contains(name)) {
                    return false;
                }
                break;
            case TLabel::EType::Empty: [[fallthrough]];
            case TLabel::EType::Common:
                if (auto it = selector.In.find(name); it != selector.In.end()
                    && !it->second.Values.contains(label.Value)) {

                    return false;
                }
                if (auto it = selector.NotIn.find(name); it != selector.NotIn.end()
                    && it->second.Values.contains(label.Value)) {

                    return false;
                }
                break;
        }
    }

    return true;
}

TResolvedConfig ResolveAll(NFyaml::TDocument& doc)
{
    TVector<TString> labelNames;
    TVector<std::pair<TString, TSet<TLabel>>> labels;

    auto config = ParseConfig(doc);
    auto namedLabels = CollectLabels(doc);

    for (auto& [name, values]: namedLabels) {
        TSet<TLabel> set;
        if (values.Class == EYamlConfigLabelTypeClass::Open) {
            set.insert(TLabel{TLabel::EType::Negative, {}});
        }
        for (auto& value: values.Values) {
            if (value != "") {
                set.insert(TLabel{TLabel::EType::Common, value});
            } else {
                set.insert(TLabel{TLabel::EType::Empty, {}});
            }
        }
        labels.push_back({name, set});
        labelNames.push_back(name);
    }

    TVector<TVector<TLabel>> labelCombinations;

    TVector<TLabel> combination;
    combination.resize(labels.size());

    Combine(labelCombinations, combination, labels, 0);

    auto cmp = [](const TVector<int>& lhs, const TVector<int>& rhs) {
        auto lhsIt = lhs.begin();
        auto rhsIt = rhs.begin();

        while (lhsIt != lhs.end() && rhsIt != rhs.end() && (*lhsIt == *rhsIt)) {
            lhsIt++;
            rhsIt++;
        }

        if (lhsIt == lhs.end()) {
            return false;
        } else if (rhsIt == rhs.end()) {
            return true;
        }

        return *lhsIt < *rhsIt;
    };

    using TTriePath = TVector<int>;

    struct TTrieNode {
        TSimpleSharedPtr<TDocumentConfig> ResolvedConfig;
        TVector<TVector<TLabel>> LabelCombinations;
    };

    std::map<TTriePath, TSimpleSharedPtr<TDocumentConfig>, decltype(cmp)> selectorsTrie(cmp);
    std::map<TTriePath, TTrieNode, decltype(cmp)> appliedSelectors(cmp);

    auto rootConfig = TTrieNode {
        MakeSimpleShared<TDocumentConfig>(std::move(doc), config.Config),
        {},
    };

    selectorsTrie[{0}] = rootConfig.ResolvedConfig;

    for (size_t j = 0; j < labelCombinations.size(); ++j) {
        TSimpleSharedPtr<TDocumentConfig> cur = rootConfig.ResolvedConfig;
        TTriePath triePath({0});

        for (size_t i = 0; i < config.Selectors.size(); ++i) {
            if (Fit(config.Selectors[i].Selector, labelCombinations[j], labels)) {
                triePath.push_back(i + 1);
                if (auto it = selectorsTrie.find(triePath); it != selectorsTrie.end()) {
                    cur = it->second;
                } else {
                    auto clone = cur->first.Clone();
                    auto cloneConfig = ParseConfig(clone);

                    Apply(cloneConfig.Config, cloneConfig.Selectors[i].Config);

                    cur = MakeSimpleShared<std::pair<NFyaml::TDocument, NFyaml::TNodeRef>>(
                        std::move(clone),
                        cloneConfig.Config),
                    selectorsTrie[triePath] = cur;
                }
            }
        }

        if (auto it = appliedSelectors.find(triePath); it != appliedSelectors.end()) {
            it->second.LabelCombinations.push_back(labelCombinations[j]);
        } else {
            appliedSelectors.try_emplace(triePath, TTrieNode{
                    cur,
                    {labelCombinations[j]}
                });
        }
    }

    selectorsTrie.clear();

    TMap<TSet<TVector<TLabel>>, TDocumentConfig> configs;

    for (auto& [_, value]: appliedSelectors) {
        configs.try_emplace(
            TSet<TVector<TLabel>>(
                value.LabelCombinations.begin(),
                value.LabelCombinations.end()),
            std::make_pair(std::move(value.ResolvedConfig->first), value.ResolvedConfig->second));
    }

    return {labelNames, std::move(configs)};
}

size_t Hash(const NFyaml::TNodeRef& resolved) {
    TStringStream ss;
    ss << resolved;
    TString s = ss.Str();
    return THash<TString>{}(s);
}

size_t Hash(const TResolvedConfig& config)
{
    size_t configsHash = 0;
    for (auto& [labelSet, docConfig] : config.Configs) {
        for (auto labels : labelSet) {
            auto labelsHash = THash<TVector<TLabel>>{}(labels);
            configsHash = CombineHashes(labelsHash, configsHash);
        }
        configsHash = CombineHashes(Hash(docConfig.second), configsHash);
    }

    return CombineHashes(THash<TVector<TString>>{}(config.Labels), configsHash);
}

void ValidateVolatileConfig(NFyaml::TDocument& doc) {
    auto root = doc.Root();
    auto seq = root.Map().at("selector_config").Sequence();
    if (seq.size() == 0) {
        ythrow yexception() << "Empty volatile config";
    }
    for (auto& elem : seq) {
        auto map = elem.Map();
        if (map.size() != 3) {
            ythrow yexception() << "Invalid volatile config element: " << elem.Path();
        }
        for (auto& mapElem : map) {
            auto key = mapElem.Key().Scalar();
            if (key == "description") {
                mapElem.Value().Scalar();
            } else if (key == "selector") {
                mapElem.Value().Map();
            } else if (key == "config") {
                mapElem.Value().Map();
            } else {
                ythrow yexception() << "Unknown element in volatile config: " << elem.Path();
            }
        }
    }
}

void AppendVolatileConfigs(NFyaml::TDocument& config, NFyaml::TDocument& volatileConfig) {
    auto configRoot = config.Root();
    auto volatileConfigRoot = volatileConfig.Root();

    auto seq = volatileConfigRoot.Sequence();
    auto selectors = configRoot.Map().at("selector_config").Sequence();
    for (auto& elem : seq) {
        auto node = elem.Copy(config);
        selectors.Append(node.Ref());
    }
}

void AppendVolatileConfigs(NFyaml::TDocument& config, NFyaml::TNodeRef& volatileConfig) {
    auto configRoot = config.Root();

    auto seq = volatileConfig.Sequence();
    auto selectors = configRoot.Map().at("selector_config").Sequence();
    for (auto& elem : seq) {
        auto node = elem.Copy(config);
        selectors.Append(node.Ref());
    }
}

ui64 GetVersion(const TString& config) {
    auto metadata = GetMetadata(config);
    return metadata.Version.value_or(0);
}

TMetadata GetMetadata(const TString& config) {
    if (config.empty()) {
        return {};
    }

    auto doc = NFyaml::TDocument::Parse(config);

    if (auto node = doc.Root().Map()["metadata"]; node) {
        auto versionNode = node.Map()["version"];
        auto clusterNode = node.Map()["cluster"];
        return TMetadata{
            .Version = versionNode ? std::optional{FromString<ui64>(versionNode.Scalar())} : std::nullopt,
            .Cluster = clusterNode ? std::optional{clusterNode.Scalar()} : std::nullopt,
        };
    }

    return {};
}

TVolatileMetadata GetVolatileMetadata(const TString& config) {
    if (config.empty()) {
        return {};
    }

    auto doc = NFyaml::TDocument::Parse(config);

    if (auto node = doc.Root().Map().at("metadata"); node) {
        auto versionNode = node.Map().at("version");
        auto clusterNode = node.Map().at("cluster");
        auto idNode = node.Map().at("id");
        return TVolatileMetadata{
            .Version = versionNode ? std::make_optional(FromString<ui64>(versionNode.Scalar())) : std::nullopt,
            .Cluster = clusterNode ? std::make_optional(clusterNode.Scalar()) : std::nullopt,
            .Id = idNode ? std::make_optional(FromString<ui64>(idNode.Scalar())) : std::nullopt,
        };
    }

    return {};
}

TString ReplaceMetadata(const TString& config, const std::function<void(TStringStream&)>& serializeMetadata) {
    TStringStream sstr;
    auto doc = NFyaml::TDocument::Parse(config);
    if (doc.Root().Style() == NFyaml::ENodeStyle::Flow) {
        if (auto pair = doc.Root().Map().pair_at_opt("metadata"); pair) {
            doc.Root().Map().Remove(pair);
        }
        serializeMetadata(sstr);
        sstr << "\n" << doc;
    } else {
        if (auto pair = doc.Root().Map().pair_at_opt("metadata"); pair) {
            auto begin = pair.Key().BeginMark().InputPos;
            auto end = pair.Value().EndMark().InputPos;
            sstr << config.substr(0, begin);
            serializeMetadata(sstr);
            if (end < config.length() && config[end] == ':') {
                end = end + 1;
            }
            sstr << config.substr(end, TString::npos);
        } else {
            if (doc.HasExplicitDocumentStart()) {
                auto docStart = doc.BeginMark().InputPos + 4;
                sstr << config.substr(0, docStart);
                serializeMetadata(sstr);
                sstr << "\n" << config.substr(docStart, TString::npos);
            } else {
                serializeMetadata(sstr);
                sstr << "\n" << config;
            }
        }
    }
    return sstr.Str();

}

TString ReplaceMetadata(const TString& config, const TMetadata& metadata) {
    auto serializeMetadata = [&](TStringStream& sstr) {
        sstr
          << "metadata:"
          << "\n  kind: MainConfig"
          << "\n  cluster: \"" << *metadata.Cluster << "\""
          << "\n  version: " << *metadata.Version;
    };
    return ReplaceMetadata(config, serializeMetadata);
}

TString ReplaceMetadata(const TString& config, const TVolatileMetadata& metadata) {
    auto serializeMetadata = [&](TStringStream& sstr) {
        sstr
          << "metadata:"
          << "\n  kind: VolatileConfig"
          << "\n  cluster: \"" << *metadata.Cluster << "\""
          << "\n  version: " << *metadata.Version
          << "\n  id: "  << *metadata.Id;
    };
    return ReplaceMetadata(config, serializeMetadata);
}

bool IsConfigKindEquals(const TString& config, const TString& kind) {
    try {
        auto doc = NFyaml::TDocument::Parse(config);
        return doc.Root().Map().at("metadata").Map().at("kind").Scalar() == kind;
    } catch (yexception& e) {
        return false;
    }
}

bool IsVolatileConfig(const TString& config) {
    return IsConfigKindEquals(config, "VolatileConfig");
}

bool IsMainConfig(const TString& config) {
    return IsConfigKindEquals(config, "MainConfig");
}

TString StripMetadata(const TString& config) {
    auto doc = NFyaml::TDocument::Parse(config);

    TStringStream sstr;
    if (auto pair = doc.Root().Map().pair_at_opt("metadata"); pair) {
        auto begin = pair.Key().BeginMark().InputPos;
        sstr << config.substr(0, begin);
        auto end = pair.Value().EndMark().InputPos;
        sstr << config.substr(end, TString::npos);
    } else {
        if (doc.HasExplicitDocumentStart()) {
            auto docStart = doc.BeginMark().InputPos + 4;
            sstr << config.substr(0, docStart);
            sstr << "\n" << config.substr(docStart, TString::npos);
        } else {
            sstr << config;
        }
    }

    return sstr.Str();
}

} // namespace NKikimr::NYamlConfig

template <>
void Out<NKikimr::NYamlConfig::TLabel>(IOutputStream& out, const NKikimr::NYamlConfig::TLabel& value) {
    out << (int)value.Type << ":" << value.Value;
}

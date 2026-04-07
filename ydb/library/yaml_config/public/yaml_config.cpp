#include "yaml_config.h"
#include "yaml_config_impl.h"

#include <util/digest/sequence.h>
#include <functional>

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

template <>
struct THash<TVector<int>> : public TSimpleRangeHash {};

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

size_t Hash(const NFyaml::TNodeRef& resolved) {
    TStringStream ss;
    ss << resolved;
    TString s = ss.Str();
    return THash<TString>{}(s);
}

TString GetKey(const NFyaml::TNodeRef& node, TString key) {
    auto map = node.Map();
    auto k = map.at(key).Scalar();
    return k;
}

using TTriePath = TVector<int>;

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

    if (root.Has("allowed_labels")) {
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
    }

    if (root.Has("selector_config")) {
        auto selectorConfig = root.at("selector_config").Sequence();

        for (auto it = selectorConfig.begin(); it != selectorConfig.end(); ++it) {
            TYamlConfigModel::TSelectorModel selector;

            auto selectorRoot = it->Map();
            selector.Description = selectorRoot.at("description").Scalar();
            selector.Config = selectorRoot.at("config");
            selector.Selector = ParseSelector(selectorRoot.at("selector"));

            res.Selectors.push_back(selector);
        }
    }

    res.IncompatibilityRules = TIncompatibilityRules::GetDefaultRules();
    auto userRules = ParseIncompatibilityRules(root);
    res.IncompatibilityRules.MergeWith(userRules);

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
    auto config = res.first.Root();

    if (rootMap.Has("config")) {
        config = rootMap.at("config");
        if (rootMap.Has("selector_config")) {
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
        }
    }

    RemoveTags(res.first);

    res.second = config;

    return res;
}

bool TIncompatibilityRule::TLabelPattern::Matches(
    const TLabel& label,
    const TString& actualLabelName) const
{
    if (Name != actualLabelName) {
        return false;
    }
    
    bool baseMatch = false;
    
    // If Values is monostate, match any value
    if (std::holds_alternative<std::monostate>(Values)) {
        baseMatch = true;
    } else {
        const auto& valueSet = std::get<THashSet<TString>>(Values);
        
        if (label.Type == TLabel::EType::Empty) {
            // Empty label matches either empty string or $unset marker in pattern
            baseMatch = valueSet.contains("") || valueSet.contains(TIncompatibilityRules::UNSET_LABEL_MARKER);
        } else if (label.Type == TLabel::EType::Common) {
            // Check if the actual value is in the set
            baseMatch = valueSet.contains(label.Value);
        }
    }
    
    // Apply negation if needed
    return Negated ? !baseMatch : baseMatch;
}

TIncompatibilityRules TIncompatibilityRules::GetDefaultRules() {
    TIncompatibilityRules rules;
    
    auto addRequiredLabelRule = [&](const TString& labelName) {
        TIncompatibilityRule rule;
        rule.RuleName = TString("builtin_") + labelName + "_must_have_value";
        rule.Source = TIncompatibilityRule::ESource::BuiltIn;
        
        TIncompatibilityRule::TLabelPattern pattern;
        pattern.Name = labelName;
        THashSet<TString> valueSet;
        valueSet.insert(UNSET_LABEL_MARKER);
        valueSet.insert("");
        pattern.Values = std::move(valueSet);
        pattern.Negated = false;
        
        rule.Patterns.push_back(std::move(pattern));
        rules.AddRule(std::move(rule));
    };
    
    auto addMustBeDefinedRule = [&](const TString& labelName) {
        TIncompatibilityRule rule;
        rule.RuleName = TString("builtin_") + labelName + "_must_be_defined";
        rule.Source = TIncompatibilityRule::ESource::BuiltIn;
        
        TIncompatibilityRule::TLabelPattern pattern;
        pattern.Name = labelName;
        THashSet<TString> valueSet;
        valueSet.insert(UNSET_LABEL_MARKER);
        pattern.Values = std::move(valueSet);
        pattern.Negated = false;
        
        rule.Patterns.push_back(std::move(pattern));
        rules.AddRule(std::move(rule));
    };
    addRequiredLabelRule("branch");
    addRequiredLabelRule("configuration_version");
    addRequiredLabelRule("dynamic");
    addRequiredLabelRule("node_host");
    addRequiredLabelRule("node_id");
    addRequiredLabelRule("rev");
    
    addMustBeDefinedRule("node_type");
    addMustBeDefinedRule("tenant");
    
    {
        TIncompatibilityRule rule;
        rule.RuleName = "builtin_static_with_nonempty_tenant";
        rule.Source = TIncompatibilityRule::ESource::BuiltIn;
        
        TIncompatibilityRule::TLabelPattern pattern1;
        pattern1.Name = "dynamic";
        THashSet<TString> valueSet1;
        valueSet1.insert("false");
        pattern1.Values = std::move(valueSet1);
        pattern1.Negated = false;
        
        TIncompatibilityRule::TLabelPattern pattern2;
        pattern2.Name = "tenant";
        THashSet<TString> valueSet2;
        valueSet2.insert("");
        pattern2.Values = std::move(valueSet2);
        pattern2.Negated = true;
        
        rule.Patterns.push_back(std::move(pattern1));
        rule.Patterns.push_back(std::move(pattern2));
        rules.AddRule(std::move(rule));
    }
    
    {
        TIncompatibilityRule rule;
        rule.RuleName = "builtin_static_with_nonempty_node_type";
        rule.Source = TIncompatibilityRule::ESource::BuiltIn;
        
        TIncompatibilityRule::TLabelPattern pattern1;
        pattern1.Name = "dynamic";
        THashSet<TString> valueSet1;
        valueSet1.insert("false");
        pattern1.Values = std::move(valueSet1);
        pattern1.Negated = false;
        
        TIncompatibilityRule::TLabelPattern pattern2;
        pattern2.Name = "node_type";
        THashSet<TString> valueSet2;
        valueSet2.insert("");
        pattern2.Values = std::move(valueSet2);
        pattern2.Negated = true;
        
        rule.Patterns.push_back(std::move(pattern1));
        rule.Patterns.push_back(std::move(pattern2));
        rules.AddRule(std::move(rule));
    }
    
    auto addStaticNodeCloudLabelRule = [&](const TString& labelName) {
        TIncompatibilityRule rule;
        rule.RuleName = TString("builtin_static_with_") + labelName;
        rule.Source = TIncompatibilityRule::ESource::BuiltIn;
        
        TIncompatibilityRule::TLabelPattern pattern1;
        pattern1.Name = "dynamic";
        THashSet<TString> valueSet1;
        valueSet1.insert("false");
        pattern1.Values = std::move(valueSet1);
        pattern1.Negated = false;
        
        TIncompatibilityRule::TLabelPattern pattern2;
        pattern2.Name = labelName;
        THashSet<TString> valueSet2;
        valueSet2.insert(UNSET_LABEL_MARKER);
        pattern2.Values = std::move(valueSet2);
        pattern2.Negated = true;
        
        rule.Patterns.push_back(std::move(pattern1));
        rule.Patterns.push_back(std::move(pattern2));
        rules.AddRule(std::move(rule));
    };
    
    addStaticNodeCloudLabelRule("enable_auth");
    addStaticNodeCloudLabelRule("flavour");
    addStaticNodeCloudLabelRule("node_name");
    addStaticNodeCloudLabelRule("shared");
    addStaticNodeCloudLabelRule("ydb_postgres");
    addStaticNodeCloudLabelRule("ydbcp");
    
    {
        TIncompatibilityRule rule;
        rule.RuleName = "builtin_dynamic_without_valid_tenant";
        rule.Source = TIncompatibilityRule::ESource::BuiltIn;
        
        TIncompatibilityRule::TLabelPattern pattern1;
        pattern1.Name = "dynamic";
        THashSet<TString> valueSet1;
        valueSet1.insert("true");
        pattern1.Values = std::move(valueSet1);
        pattern1.Negated = false;
        
        TIncompatibilityRule::TLabelPattern pattern2;
        pattern2.Name = "tenant";
        THashSet<TString> valueSet2;
        valueSet2.insert(UNSET_LABEL_MARKER);
        valueSet2.insert("");
        pattern2.Values = std::move(valueSet2);
        pattern2.Negated = false;
        
        rule.Patterns.push_back(std::move(pattern1));
        rule.Patterns.push_back(std::move(pattern2));
        rules.AddRule(std::move(rule));
    }
    
    {
        TIncompatibilityRule rule;
        rule.RuleName = "builtin_dynamic_without_flavour";
        rule.Source = TIncompatibilityRule::ESource::BuiltIn;
        
        TIncompatibilityRule::TLabelPattern pattern1;
        pattern1.Name = "dynamic";
        THashSet<TString> valueSet1;
        valueSet1.insert("true");
        pattern1.Values = std::move(valueSet1);
        pattern1.Negated = false;
        
        TIncompatibilityRule::TLabelPattern pattern2;
        pattern2.Name = "flavour";
        THashSet<TString> valueSet2;
        valueSet2.insert(UNSET_LABEL_MARKER);
        pattern2.Values = std::move(valueSet2);
        pattern2.Negated = false;
        
        rule.Patterns.push_back(std::move(pattern1));
        rule.Patterns.push_back(std::move(pattern2));
        rules.AddRule(std::move(rule));
    }
    
    {
        TIncompatibilityRule rule;
        rule.RuleName = "builtin_dynamic_without_ydbcp";
        rule.Source = TIncompatibilityRule::ESource::BuiltIn;
        
        TIncompatibilityRule::TLabelPattern pattern1;
        pattern1.Name = "dynamic";
        THashSet<TString> valueSet1;
        valueSet1.insert("true");
        pattern1.Values = std::move(valueSet1);
        pattern1.Negated = false;
        
        TIncompatibilityRule::TLabelPattern pattern2;
        pattern2.Name = "ydbcp";
        THashSet<TString> valueSet2;
        valueSet2.insert(UNSET_LABEL_MARKER);
        pattern2.Values = std::move(valueSet2);
        pattern2.Negated = false;
        
        rule.Patterns.push_back(std::move(pattern1));
        rule.Patterns.push_back(std::move(pattern2));
        rules.AddRule(std::move(rule));
    }
    
    {
        TIncompatibilityRule rule;
        rule.RuleName = "builtin_cloud_node_without_enable_auth";
        rule.Source = TIncompatibilityRule::ESource::BuiltIn;
        
        TIncompatibilityRule::TLabelPattern pattern1;
        pattern1.Name = "dynamic";
        THashSet<TString> valueSet1;
        valueSet1.insert("true");
        pattern1.Values = std::move(valueSet1);
        pattern1.Negated = false;
        
        TIncompatibilityRule::TLabelPattern pattern2;
        pattern2.Name = "node_type";
        THashSet<TString> valueSet2;
        valueSet2.insert("");
        pattern2.Values = std::move(valueSet2);
        pattern2.Negated = false;
        
        TIncompatibilityRule::TLabelPattern pattern3;
        pattern3.Name = "enable_auth";
        THashSet<TString> valueSet3;
        valueSet3.insert(UNSET_LABEL_MARKER);
        pattern3.Values = std::move(valueSet3);
        pattern3.Negated = false;
        
        rule.Patterns.push_back(std::move(pattern1));
        rule.Patterns.push_back(std::move(pattern2));
        rule.Patterns.push_back(std::move(pattern3));
        rules.AddRule(std::move(rule));
    }
    
    {
        TIncompatibilityRule rule;
        rule.RuleName = "builtin_slot_type_static";
        rule.Source = TIncompatibilityRule::ESource::BuiltIn;
        
        TIncompatibilityRule::TLabelPattern pattern1;
        pattern1.Name = "node_type";
        THashSet<TString> valueSet1;
        valueSet1.insert("slot");
        pattern1.Values = std::move(valueSet1);
        pattern1.Negated = false;
        
        TIncompatibilityRule::TLabelPattern pattern2;
        pattern2.Name = "dynamic";
        THashSet<TString> valueSet2;
        valueSet2.insert("false");
        pattern2.Values = std::move(valueSet2);
        pattern2.Negated = false;
        
        rule.Patterns.push_back(std::move(pattern1));
        rule.Patterns.push_back(std::move(pattern2));
        rules.AddRule(std::move(rule));
    }
    
    auto addSlotNodeCloudLabelRule = [&](const TString& labelName) {
        TIncompatibilityRule rule;
        rule.RuleName = TString("builtin_slot_with_") + labelName;
        rule.Source = TIncompatibilityRule::ESource::BuiltIn;
        
        TIncompatibilityRule::TLabelPattern pattern1;
        pattern1.Name = "node_type";
        THashSet<TString> valueSet1;
        valueSet1.insert("slot");
        pattern1.Values = std::move(valueSet1);
        pattern1.Negated = false;
        
        TIncompatibilityRule::TLabelPattern pattern2;
        pattern2.Name = labelName;
        THashSet<TString> valueSet2;
        valueSet2.insert(UNSET_LABEL_MARKER);
        pattern2.Values = std::move(valueSet2);
        pattern2.Negated = true;  // IS set (NOT unset)
        
        rule.Patterns.push_back(std::move(pattern1));
        rule.Patterns.push_back(std::move(pattern2));
        rules.AddRule(std::move(rule));
    };
    
    addSlotNodeCloudLabelRule("enable_auth");
    addSlotNodeCloudLabelRule("node_name");
    addSlotNodeCloudLabelRule("shared");
    addSlotNodeCloudLabelRule("ydb_postgres");

    
    return rules;
}

void TIncompatibilityRules::AddRule(TIncompatibilityRule rule) {
    RulesByName[rule.RuleName] = std::move(rule);
}

void TIncompatibilityRules::RemoveRule(const TString& ruleName) {
    RulesByName.erase(ruleName);
}

bool TIncompatibilityRules::IsCompatible(
    const TVector<TLabel>& combination,
    const TVector<std::pair<TString, TSet<TLabel>>>& labelNames) const
{
    for (const auto& [ruleName, rule] : RulesByName) {
        if (DisabledRules.contains(ruleName)) {
            continue;
        }
        
        bool allPatternsMatch = true;
        for (const auto& pattern : rule.Patterns) {
            bool found = false;
            
            for (size_t i = 0; i < combination.size(); ++i) {
                if (pattern.Matches(combination[i], labelNames[i].first)) {
                    found = true;
                    break;
                }
            }
            
            if (!found) {
                allPatternsMatch = false;
                break;
            }
        }
        
        if (allPatternsMatch) {
            return false;
        }
    }
    
    return true;
}

bool TIncompatibilityRules::IsCompatible(const TMap<TString, TString>& labels) const {
    for (const auto& [ruleName, rule] : RulesByName) {
        if (DisabledRules.contains(ruleName)) {
            continue;
        }
        
        bool allPatternsMatch = true;
        for (const auto& pattern : rule.Patterns) {
            auto it = labels.find(pattern.Name);
            
            bool baseMatch = false;
            
            // If Values is monostate, match any value
            if (std::holds_alternative<std::monostate>(pattern.Values)) {
            baseMatch = (it != labels.end());  // Only match if label exists
        } else {
            const auto& valueSet = std::get<THashSet<TString>>(pattern.Values);
            
            if (it == labels.end()) {
                // Label is completely missing - matches $unset marker only
                baseMatch = valueSet.contains(TIncompatibilityRules::UNSET_LABEL_MARKER);
            } else if (it->second == TString("")) {
                // Label exists but is empty - matches empty string "" only, NOT $unset
                baseMatch = valueSet.contains(TString(""));
            } else {
                // Label has a non-empty value
                baseMatch = valueSet.contains(it->second);
            }
        }
        
        bool matches = pattern.Negated ? !baseMatch : baseMatch;
        
        if (!matches) {
            allPatternsMatch = false;
            break;
        }
    }
    
        if (allPatternsMatch) {
            return false;
        }
    }
    
    return true;
}

void TIncompatibilityRules::MergeWith(const TIncompatibilityRules& userRules) {
    for (const auto& ruleName : userRules.DisabledRules) {
        DisabledRules.insert(ruleName);
    }
    
    for (const auto& [name, rule] : userRules.RulesByName) {
        AddRule(rule);
    }
}

TIncompatibilityRules ParseIncompatibilityRules(const NFyaml::TNodeRef& root) {
    TIncompatibilityRules userRules;
    
    if (!root.Map().Has("incompatibility_overrides")) {
        return userRules;
    }
    
    auto overrides = root.Map().at("incompatibility_overrides");
    
    if (overrides.Map().Has("disable_all_builtin_rules")) {
        auto value = overrides.Map().at("disable_all_builtin_rules").Scalar();
        if (value == "true") {
            auto builtinRules = TIncompatibilityRules::GetDefaultRules();
            for (const auto& [ruleName, rule] : builtinRules.RulesByName) {
                userRules.DisabledRules.insert(ruleName);
            }
        }
    }
    
    if (overrides.Map().Has("disable_rules")) {
        auto disableRules = overrides.Map().at("disable_rules");
        if (disableRules.Type() == NFyaml::ENodeType::Sequence) {
            for (auto it = disableRules.Sequence().begin(); it != disableRules.Sequence().end(); ++it) {
                userRules.DisabledRules.insert(it->Scalar());
            }
        }
    }
    
    if (overrides.Map().Has("custom_rules")) {
        auto customRules = overrides.Map().at("custom_rules");
        if (customRules.Type() == NFyaml::ENodeType::Sequence) {
            for (auto ruleIt = customRules.Sequence().begin(); ruleIt != customRules.Sequence().end(); ++ruleIt) {
                TIncompatibilityRule rule;
                auto ruleMap = ruleIt->Map();
                
                rule.RuleName = ruleMap.at("name").Scalar();
                rule.Source = TIncompatibilityRule::ESource::UserDefined;
                
                if (ruleMap.Has("patterns")) {
                    auto patterns = ruleMap.at("patterns");
                    if (patterns.Type() == NFyaml::ENodeType::Sequence) {
                        for (auto patternIt = patterns.Sequence().begin(); patternIt != patterns.Sequence().end(); ++patternIt) {
                            TIncompatibilityRule::TLabelPattern pattern;
                            auto patternMap = patternIt->Map();
                            
                            pattern.Name = patternMap.at("label").Scalar();
                            
                            // Check for negation
                            if (patternMap.Has("negated")) {
                                TString negatedStr = patternMap.at("negated").Scalar();
                                pattern.Negated = (negatedStr == "true");
                            }
                            
                            // Helper to process value strings
                            // Keep $unset as-is, map $empty to empty string
                            auto processValue = [](const TString& valueStr) -> TString {
                                if (valueStr == TIncompatibilityRules::EMPTY_LABEL_MARKER) {
                                    return "";
                                } else {
                                    return valueStr;  // Includes $unset as-is
                                }
                            };
                            
                            // Parse value or value_in
                            if (patternMap.Has("value_in")) {
                                // Multiple values
                                THashSet<TString> valueSet;
                                auto valueArray = patternMap.at("value_in");
                                if (valueArray.Type() == NFyaml::ENodeType::Sequence) {
                                    for (auto v = valueArray.Sequence().begin(); v != valueArray.Sequence().end(); ++v) {
                                        valueSet.insert(processValue(v->Scalar()));
                                    }
                                }
                                pattern.Values = std::move(valueSet);
                            } else if (patternMap.Has("value")) {
                                // Single value - use HashSet
                                THashSet<TString> valueSet;
                                valueSet.insert(processValue(patternMap.at("value").Scalar()));
                                pattern.Values = std::move(valueSet);
                            }
                            // else: neither value nor value_in specified
                            // pattern.Values remains std::monostate (default-constructed)
                            
                            rule.Patterns.push_back(std::move(pattern));
                        }
                    }
                }
                
                userRules.AddRule(std::move(rule));
            }
        }
    }
    
    return userRules;
}

void CombineForEach(
    TVector<TLabel>& combination,
    const TVector<std::pair<TString, TSet<TLabel>>>& labels,
    size_t offset,
    const std::function<void(const TVector<TLabel>&)>& process)
{
    if (offset == labels.size()) {
        process(combination);
        return;
    }

    for (auto& label : labels[offset].second) {
        combination[offset] = label;
        CombineForEach(combination, labels, offset + 1, process);
    }
}

bool Fit(
    const TVector<std::pair<int, const TLabelValueSet*>>& in,
    const TVector<std::pair<int, const TLabelValueSet*>>& notIn,
    const TVector<TLabel>& labels)
{
    for (size_t i = 0; i < notIn.size(); ++i) {
        int idx = notIn[i].first;
        const auto& label = labels[idx];
        if (label.Type != TLabel::EType::Negative && notIn[i].second->Values.contains(label.Value)) {
            return false;
        }
    }
    for (size_t i = 0; i < in.size(); ++i) {
        int idx = in[i].first;
        const auto& label = labels[idx];
        if (label.Type == TLabel::EType::Negative) {
            return false;
        }
        if (!in[i].second->Values.contains(label.Value)) {
            return false;
        }
    }

    return true;
}

struct TCompiledSelector {
    TVector<std::pair<int, const TLabelValueSet*>> In;
    TVector<std::pair<int, const TLabelValueSet*>> NotIn;
};

THashSet<TString> ComputeUsedNames(const TYamlConfigModel& model) {
    THashSet<TString> usedNames;
    usedNames.reserve(model.Selectors.size() * 2);
    for (const auto& selectorModel : model.Selectors) {
        for (const auto& [label, _] : selectorModel.Selector.In) {
            usedNames.insert(label);
        }
        for (const auto& [label, _] : selectorModel.Selector.NotIn) {
            usedNames.insert(label);
        }
    }
    return usedNames;
}

void BuildLabelDomain(
    NFyaml::TDocument& doc,
    const THashSet<TString>& usedNames,
    TVector<TString>& labelNames,
    TVector<std::pair<TString, TSet<TLabel>>>& labels)
{
    auto namedLabels = CollectLabels(doc);

    for (auto& [name, values]: namedLabels) {
        if (!usedNames.contains(name)) {
            continue;
        }
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
}

THashMap<TString, int> BuildNameToIndex(const TVector<TString>& labelNames) {
    THashMap<TString, int> nameToIndex;
    nameToIndex.reserve(labelNames.size());
    for (size_t i = 0; i < labelNames.size(); ++i) {
        nameToIndex[labelNames[i]] = static_cast<int>(i);
    }
    return nameToIndex;
}

TVector<TCompiledSelector> CompileSelectors(
    const TYamlConfigModel& model,
    const THashMap<TString, int>& nameToIndex)
{
    TVector<TCompiledSelector> compiled;
    compiled.reserve(model.Selectors.size());

    for (const auto& selectorModel : model.Selectors) {
        TCompiledSelector cs;
        cs.In.reserve(selectorModel.Selector.In.size());
        cs.NotIn.reserve(selectorModel.Selector.NotIn.size());

        for (const auto& kv : selectorModel.Selector.In) {
            if (auto it = nameToIndex.find(kv.first); it != nameToIndex.end()) {
                cs.In.push_back({it->second, &kv.second});
            }
        }

        for (const auto& kv : selectorModel.Selector.NotIn) {
            if (auto it = nameToIndex.find(kv.first); it != nameToIndex.end()) {
                cs.NotIn.push_back({it->second, &kv.second});
            }
        }

        compiled.push_back(std::move(cs));
    }

    return compiled;
}

struct TResolveContext {
    TYamlConfigModel Model;
    TVector<TString> LabelNames;
    TVector<std::pair<TString, TSet<TLabel>>> Labels;
    TVector<TCompiledSelector> Compiled;
};

static TResolveContext PrepareResolveContext(NFyaml::TDocument& doc) {
    TResolveContext ctx{
        .Model = ParseConfig(doc),
    };
    auto usedNames = ComputeUsedNames(ctx.Model);
    BuildLabelDomain(doc, usedNames, ctx.LabelNames, ctx.Labels);
    auto nameToIndex = BuildNameToIndex(ctx.LabelNames);
    ctx.Compiled = CompileSelectors(ctx.Model, nameToIndex);
    return ctx;
}

TResolvedConfig ResolveAll(NFyaml::TDocument& doc)
{
    auto ctx = PrepareResolveContext(doc);

    struct TTrieNode {
        TSimpleSharedPtr<TDocumentConfig> ResolvedConfig;
        TVector<TVector<TLabel>> LabelCombinations;
    };

    THashMap<TTriePath, TTrieNode> appliedSelectors;
    THashMap<TTriePath, TSimpleSharedPtr<TDocumentConfig>> selectorsTrie;

    auto rootPtr = MakeSimpleShared<TDocumentConfig>(std::move(doc), ctx.Model.Config);
    selectorsTrie[TTriePath{0}] = rootPtr;

    TVector<TLabel> combination(ctx.Labels.size());

    const bool checkRules = ctx.Model.IncompatibilityRules.HasRules();

    CombineForEach(
        combination,
        ctx.Labels,
        0,
        [&](const TVector<TLabel>& current) {
            if (checkRules && !ctx.Model.IncompatibilityRules.IsCompatible(current, ctx.Labels)) {
                return;
            }
            TSimpleSharedPtr<TDocumentConfig> cur = rootPtr;
            TTriePath path({0});

            for (size_t i = 0; i < ctx.Model.Selectors.size(); ++i) {
                if (Fit(ctx.Compiled[i].In, ctx.Compiled[i].NotIn, current)) {
                    path.push_back(i + 1);
                    if (auto it = selectorsTrie.find(path); it != selectorsTrie.end()) {
                        cur = it->second;
                    } else {
                        auto clone = cur->first.Clone();
                        auto cloneConfig = ParseConfig(clone);

                        Apply(cloneConfig.Config, cloneConfig.Selectors[i].Config);

                        cur = MakeSimpleShared<TDocumentConfig>(std::move(clone), cloneConfig.Config);
                        selectorsTrie[path] = cur;
                    }
                }
            }

            auto& node = appliedSelectors[path];
            if (!node.ResolvedConfig) {
                node.ResolvedConfig = cur;
            }
            node.LabelCombinations.push_back(current);
        });

    TMap<TSet<TVector<TLabel>>, TDocumentConfig> configs;

    for (auto& [_, value]: appliedSelectors) {
        configs.try_emplace(
            TSet<TVector<TLabel>>(
                value.LabelCombinations.begin(),
                value.LabelCombinations.end()),
            std::make_pair(std::move(value.ResolvedConfig->first), value.ResolvedConfig->second));
    }

    return {ctx.LabelNames, std::move(configs)};
}

void ResolveUniqueDocs(
    NFyaml::TDocument& doc,
    const std::function<void(TDocumentConfig&&)>& onDocument)
{
    auto ctx = PrepareResolveContext(doc);

    struct TState {
        NFyaml::TDocument Doc;
        NFyaml::TNodeRef Config;
    };

    TVector<TState> states;
    states.reserve(ctx.Compiled.size() + 1);

    {
        auto baseDoc = doc.Clone();
        auto model = ParseConfig(baseDoc);
        states.push_back(TState{std::move(baseDoc), model.Config});
    }

    TVector<int> lastPath;
    lastPath.reserve(ctx.Compiled.size());

    TVector<TLabel> combination(ctx.Labels.size());
    TVector<int> currentPath;
    currentPath.reserve(ctx.Compiled.size());

    THashSet<size_t> seenHashes;
    THashSet<TTriePath> seenPaths;

    const bool checkRules = ctx.Model.IncompatibilityRules.HasRules();

    // for each combination of labels, we build a path of selectors
    CombineForEach(
        combination,
        ctx.Labels,
        0,
        [&](const TVector<TLabel>& current) {
            if (checkRules && !ctx.Model.IncompatibilityRules.IsCompatible(current, ctx.Labels)) {
                return;
            }
            currentPath.clear();
            // for each selector, we check if it fits the combination of labels
            // if it does, we add the selector index to the current path
            for (size_t selectorIdx = 0; selectorIdx < ctx.Compiled.size(); ++selectorIdx) {
                if (Fit(ctx.Compiled[selectorIdx].In, ctx.Compiled[selectorIdx].NotIn, current)) {
                    currentPath.push_back(static_cast<int>(selectorIdx));
                }
            }

            if (seenPaths.contains(currentPath)) {
                return;
            }

            // we find the common prefix of the current path and the last path
            // this allows us to reuse one of the previous documents for the common prefix
            size_t commonPrefix = 0;
            while (commonPrefix < currentPath.size() && commonPrefix < lastPath.size()
                   && currentPath[commonPrefix] == lastPath[commonPrefix]) {
                ++commonPrefix;
            }

            // cut the last path to the common prefix
            while (lastPath.size() > commonPrefix) {
                lastPath.pop_back();
                states.pop_back();
            }

            // for the selectors that are not in the common prefix, we applying them to the previous document
            for (size_t idx = commonPrefix; idx < currentPath.size(); ++idx) {
                int selectorIndex = currentPath[idx];
                auto nextDoc = states.back().Doc.Clone();

                // apply without re-parsing selectors: copy "config" from original doc's selector into nextDoc
                auto toConfig = nextDoc.Root().Map().at("config");
                auto fromConfigOriginal = ctx.Model.Selectors[selectorIndex].Config;
                auto fromConfigInNext = fromConfigOriginal.Copy(nextDoc);
                Apply(toConfig, fromConfigInNext);

                states.push_back(TState{std::move(nextDoc), toConfig});
                lastPath.push_back(selectorIndex);
            }

            auto resolvedDoc = states.back().Doc.Clone();
            RemoveTags(resolvedDoc);
            auto resolvedConfig = resolvedDoc.Root().Map().at("config");
            
            size_t h = Hash(resolvedConfig);
            if (seenHashes.emplace(h).second) {
                onDocument(TDocumentConfig{std::move(resolvedDoc), resolvedConfig});
            }

            seenPaths.insert(currentPath);
        });
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
    if (!configRoot.Map().Has("selector_config")) {
        configRoot.Map().Append(config.Buildf("selector_config"), config.Buildf("[]"));
    }

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
    if (!configRoot.Map().Has("selector_config")) {
        configRoot.Map().Append(config.Buildf("selector_config"), config.Buildf("[]"));
    }

    auto seq = volatileConfig.Sequence();
    auto selectors = configRoot.Map().at("selector_config").Sequence();
    for (auto& elem : seq) {
        auto node = elem.Copy(config);
        selectors.Append(node.Ref());
    }
}

void AppendDatabaseConfig(NFyaml::TDocument& config, NFyaml::TDocument& databaseConfig) {
    auto configRoot = config.Root();
    if (!configRoot.Map().Has("selector_config")) {
        configRoot.Map().Append(config.Buildf("selector_config"), config.Buildf("[]"));
    }

    auto databaseConfigRoot = databaseConfig.Root();

    auto selectors = configRoot.Map().at("selector_config").Sequence();
    selectors.Append(config.Buildf(R"(
description: Implicit DatabaseConfig node
selector: {}
)"));
    auto node = databaseConfigRoot.Map()["config"].Copy(config);
    selectors.at(0).Map().Append(config.Buildf("config"), node);
}

// Storage-only keys that should not be included in dynamic node configs
// See ydb/tools/ydbd_slice/yaml_configurator.py STORAGE_ONLY_KEYS
static const THashSet<TString> StorageOnlyKeys = {
    "static_erasure",
    "host_configs",
    "nameservice_config",
    "blob_storage_config",
    "hosts"
};

NFyaml::TDocument FuseConfigs(const TString& baseConfig, const TString& consoleConfig) {
    auto consoleDoc = NFyaml::TDocument::Parse(consoleConfig);

    auto consoleRoot = consoleDoc.Root().Map();
    Y_ENSURE_EX(consoleRoot.Has("config"), TYamlConfigEx() << "Console config must have 'config' section");

    // Create output document
    auto outputDoc = NFyaml::TDocument::Parse("{}");
    auto outRoot = outputDoc.Root().Map();

    // Copy metadata from console
    if (consoleRoot.Has("metadata")) {
        outRoot.Append(outputDoc.Buildf("metadata"), consoleRoot.at("metadata").Copy(outputDoc).Ref());
    }

    // Start with console config as the target (console wins)
    auto mergedConfig = consoleRoot.at("config").Copy(outputDoc);
    auto mergedMap = mergedConfig.Ref().Map();

    // Add base config keys that are NOT in console (emplace behavior)
    // Like init.cpp:800-802 but with console as target
    // Skip storage-only keys that shouldn't be in dynamic node configs
    if (!baseConfig.empty()) {
        auto baseDoc = NFyaml::TDocument::Parse(baseConfig);
        auto baseRoot = baseDoc.Root();
        if (baseRoot.Type() == NFyaml::ENodeType::Mapping) {
            auto baseMap = baseRoot.Map();
            for (auto it = baseMap.begin(); it != baseMap.end(); ++it) {
                auto key = it->Key().Scalar();
                // Skip storage-only keys
                if (StorageOnlyKeys.contains(key)) {
                    continue;
                }
                // Only add if key doesn't exist in console (emplace)
                if (!mergedMap.Has(key)) {
                    mergedMap.Append(it->Key().Copy(outputDoc).Ref(), it->Value().Copy(outputDoc).Ref());
                }
            }
        }
    }

    outRoot.Append(outputDoc.Buildf("config"), mergedConfig.Ref());

    // Copy allowed_labels from console
    if (consoleRoot.Has("allowed_labels")) {
        outRoot.Append(outputDoc.Buildf("allowed_labels"), consoleRoot.at("allowed_labels").Copy(outputDoc).Ref());
    }

    // Copy selector_config from console (preserved intact)
    if (consoleRoot.Has("selector_config")) {
        outRoot.Append(outputDoc.Buildf("selector_config"), consoleRoot.at("selector_config").Copy(outputDoc).Ref());
    }

    return outputDoc;
}

ui64 GetVersion(const TString& config) {
    auto metadata = GetMainMetadata(config);
    return metadata.Version.value_or(0);
}

struct TMetadataDocument {
    NFyaml::TDocument Doc;
    NFyaml::TMapping Node;

    TMetadataDocument(const TString& yaml)
        : Doc(NFyaml::TDocument::Parse(yaml))
        , Node(Doc.Root().Map().at("metadata").Map())
    {}
};

std::optional<TMetadataDocument> GetMetadataDoc(const TString& config) {
    if (config.empty()) {
        return {};
    }

    try {
        return TMetadataDocument(config);
    } catch(const NFyaml::TFyamlEx&) {
        return {};
    }
}

TMainMetadata GetMainMetadata(const TString& config) {
    if (auto doc = GetMetadataDoc(config); doc) {
        auto versionNode = doc->Node["version"];
        auto clusterNode = doc->Node["cluster"];
        return TMainMetadata{
            .Version = versionNode ? std::optional{FromString<ui64>(versionNode.Scalar())} : std::nullopt,
            .Cluster = clusterNode ? std::optional{clusterNode.Scalar()} : std::nullopt,
        };
    }

    return {};
}

TDatabaseMetadata GetDatabaseMetadata(const TString& config) {
    if (auto doc = GetMetadataDoc(config); doc) {
        auto databaseNode = doc->Node["database"];
        auto versionNode = doc->Node["version"];
        return TDatabaseMetadata{
            .Version = versionNode ? std::optional{FromString<ui64>(versionNode.Scalar())} : std::nullopt,
            .Database = databaseNode ? std::optional{databaseNode.Scalar()} : std::nullopt,
        };
    }

    return {};
}

TStorageMetadata GetStorageMetadata(const TString& config) {
    if (auto doc = GetMetadataDoc(config); doc) {
        auto versionNode = doc->Node["version"];
        auto clusterNode = doc->Node["cluster"];
        return TStorageMetadata{
            .Version = versionNode ? std::optional{FromString<ui64>(versionNode.Scalar())} : std::nullopt,
            .Cluster = clusterNode ? std::optional{clusterNode.Scalar()} : std::nullopt,
        };
    }

    return {};
}

TVolatileMetadata GetVolatileMetadata(const TString& config) {
    if (auto doc = GetMetadataDoc(config); doc) {
        auto versionNode = doc->Node.at("version");
        auto clusterNode = doc->Node.at("cluster");
        auto idNode = doc->Node.at("id");
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
            if (end < config.length() && (config[end] == ':' || config[end] == '"')) {
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

TString ReplaceMetadata(const TString& config, const TMainMetadata& metadata) {
    auto serializeMetadata = [&](TStringStream& sstr) {
        sstr
          << "metadata:"
          << "\n  kind: MainConfig"
          << "\n  cluster: \"" << *metadata.Cluster << "\""
          << "\n  version: " << *metadata.Version;
    };
    return ReplaceMetadata(config, serializeMetadata);
}

TString ReplaceMetadata(const TString& config, const TDatabaseMetadata& metadata) {
    auto serializeMetadata = [&](TStringStream& sstr) {
        sstr
          << "metadata:"
          << "\n  kind: DatabaseConfig"
          << "\n  database: \"" << *metadata.Database << "\""
          << "\n  version: " << *metadata.Version;
    };
    return ReplaceMetadata(config, serializeMetadata);
}

TString ReplaceMetadata(const TString& config, const TStorageMetadata& metadata) {
    auto serializeMetadata = [&](TStringStream& sstr) {
        sstr
          << "metadata:"
          << "\n  kind: StorageConfig"
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

bool IsStorageConfig(const TString& config) {
    return IsConfigKindEquals(config, "StorageConfig");
}

bool IsDatabaseConfig(const TString& config) {
    return IsConfigKindEquals(config, "DatabaseConfig");
}

bool IsStaticConfig(const TString& config) {
    auto doc = NFyaml::TDocument::Parse(config);
    return !doc.Root().Map().Has("metadata");
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

std::variant<TMainMetadata, TDatabaseMetadata, TError> GetGenericMetadata(const TString& config) {
    try {
        auto doc = NFyaml::TDocument::Parse(config);
        auto metadata = doc.Root().Map().at("metadata").Map();
        // if we have metadata, but do not have kind
        // we suppose it is MainConfig
        // later we will remove this behaviour
        // but we need it for compatibility for now
        if (!metadata.Has("kind")) {
            return GetMainMetadata(config);
        }

        auto kind = metadata.at("kind").Scalar();
        if (kind == "MainConfig") {
            return GetMainMetadata(config);
        } else if (kind == "DatabaseConfig") {
            return GetDatabaseMetadata(config);
        } else {
            return TError {
                .Error = TString("Unknown kind: ") + kind,
            };
        }
    } catch (yexception& e) {
        return TError {
            .Error = e.what(),
        };
    }
}

TString UpgradeMainConfigVersion(const TString& config) {
    auto metadata = GetMainMetadata(config);
    Y_ENSURE(metadata.Version);
    *metadata.Version = *metadata.Version + 1;
    return ReplaceMetadata(config, metadata);
}

TString UpgradeStorageConfigVersion(const TString& config) {
    auto metadata = GetStorageMetadata(config);
    Y_ENSURE(metadata.Version);
    *metadata.Version = *metadata.Version + 1;
    return ReplaceMetadata(config, metadata);
}

} // namespace NKikimr::NYamlConfig

template <>
void Out<NKikimr::NYamlConfig::TLabel>(IOutputStream& out, const NKikimr::NYamlConfig::TLabel& value) {
    out << (int)value.Type << ":" << value.Value;
}

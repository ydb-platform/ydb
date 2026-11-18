#include "console_dumper.h"

#include "util.h"
#include "yaml_config.h"

#include <ydb/core/cms/console/util/config_index.h>
#include <ydb/library/fyamlcpp/fyamlcpp.h>

#include <regex>

namespace NKikimr::NYamlConfig {

using TUsageScope = NConsole::TUsageScope;

using TId = ui64;
using TGeneration = ui64;
using TDomainKey = std::tuple<ui32, TId, TGeneration>;
using TDomainItemsContainer = TMap<TDomainKey, NKikimrConsole::TConfigItem>;

// Use config hash to clue items with same body
using TCookieHash = ui64;
using TConfigHash = ui64;
using TSelectorKey = std::tuple<TUsageScope, TCookieHash, TConfigHash>;
using TSelectorItemsContainer = TMap<TSelectorKey, TVector<NKikimrConsole::TConfigItem>>;

struct TSelectorData {
    ui32 MergeStrategy;
    NYamlConfig::TSelector Rules;
    NKikimrConfig::TAppConfig Config;
    TString Description;
};

void MarkYamlForMergeOverwriteRepeated(NFyaml::TNodeRef &node) {
    auto rootMap = node.Map();
    for (auto &child : rootMap) {
        auto value = child.Value();
        if (value.Type() == NFyaml::ENodeType::Mapping) {
            value.SetTag("!inherit");
            MarkYamlForMergeOverwriteRepeated(value);
        }
    }
}

void MarkYamlForMerge(NFyaml::TNodeRef &node) {
    auto rootMap = node.Map();
    for (auto &child : rootMap) {
        auto value = child.Value();
        if (value.Type() == NFyaml::ENodeType::Mapping) {
            value.SetTag("!inherit");
            MarkYamlForMerge(value);
        } else if (value.Type() == NFyaml::ENodeType::Sequence) {
            value.SetTag("!append");
        }
    }
}

void Beautify(NFyaml::TDocument &doc) {
    for (auto &node : doc) {
        if (node.Style() == NFyaml::ENodeStyle::DoubleQuoted) {
            node.SetStyle(NFyaml::ENodeStyle::Any);
        }
    }
}

void ClearOverwrittenRepeated(::google::protobuf::Message &to,
                              const ::google::protobuf::Message &from) {
    auto *desc = to.GetDescriptor();
    auto *reflection = to.GetReflection();
    for (int i = 0; i < desc->field_count(); ++i) {
        auto *field = desc->field(i);
        if (field->is_repeated()) {
            if (reflection->FieldSize(from, field)) {
                reflection->ClearField(&to, field);
            }
        } else if (field->type() == ::google::protobuf::FieldDescriptor::TYPE_MESSAGE) {
            if (reflection->HasField(to, field) && reflection->HasField(from, field)) {
                ClearOverwrittenRepeated(*reflection->MutableMessage(&to, field),
                                         reflection->GetMessage(from, field));
            }
        }
    }
}

void MergeMessageOverwriteRepeated(::google::protobuf::Message &to,
                                   const ::google::protobuf::Message &from,
                                   ui32 kind) {
    auto *desc = to.GetDescriptor();
    auto *reflection = to.GetReflection();
    for (int i = 0; i < desc->field_count(); ++i) {
        auto *field = desc->field(i);
        auto tag = field->number();

        if (tag != (int)kind) {
            continue;
        }

        auto &toMsg = *reflection->MutableMessage(&to, field);
        auto &fromMsg = reflection->GetMessage(from, field);


        ClearOverwrittenRepeated(toMsg, fromMsg);
        toMsg.MergeFrom(fromMsg);
    }
}

void CopyFrom(::google::protobuf::Message &to,
              const ::google::protobuf::Message &from,
              ui32 kind) {
    auto *desc = to.GetDescriptor();
    auto *reflection = to.GetReflection();
    for (int i = 0; i < desc->field_count(); ++i) {
        auto *field = desc->field(i);
        auto tag = field->number();

        if (tag != (int)kind) {
            continue;
        }

        reflection->MutableMessage(&to, field)->CopyFrom(reflection->GetMessage(from, field));
    }
}

ui32 DetectConfigItemKind(const NKikimrConsole::TConfigItem &item)
{
    std::vector<const ::google::protobuf::FieldDescriptor*> fields;
    auto *reflection = item.GetConfig().GetReflection();
    reflection->ListFields(item.GetConfig(), &fields);

    Y_ABORT_UNLESS(fields.size() == 1, "Can't detect config item kind");

    return fields[0]->number();
}

std::pair<TDomainItemsContainer, TSelectorItemsContainer> ExtractSuitableItems(
    const ::google::protobuf::RepeatedPtrField<NKikimrConsole::TConfigItem> &configItems) {

    TDomainItemsContainer domainItemsByOrder;
    TSelectorItemsContainer selectorItemsByOrder;

    for (auto &item : configItems) {
        ui32 kind = item.GetKind();

        if (kind == 0) {
            kind = DetectConfigItemKind(item);
        }


        if (kind == NKikimrConsole::TConfigItem::NetClassifierDistributableConfigItem ||
            kind == NKikimrConsole::TConfigItem::NamedConfigsItem ||
            item.GetCookie().StartsWith("ydbcp")) {
            continue;
        }

        if (item.GetUsageScope().GetFilterCase() == NKikimrConsole::TUsageScope::FILTER_NOT_SET) {
            domainItemsByOrder.emplace(
                std::tuple<ui32, ui64, ui64>{item.GetOrder(), item.GetId().GetId(), item.GetId().GetGeneration()},
                item);
        } else {
            TUsageScope scope(item.GetUsageScope(), item.GetOrder());
            TSelectorKey key{scope, THash<TString>{}(item.GetCookie()), THash<TString>{}(item.GetConfig().ShortDebugString())};
            if (auto it = selectorItemsByOrder.find(key); it != selectorItemsByOrder.end()) {
                Y_ABORT_UNLESS(it->second.back().GetMergeStrategy() == item.GetMergeStrategy());
                it->second.emplace_back(item);
            } else {
                selectorItemsByOrder.emplace(key, TVector<NKikimrConsole::TConfigItem>{item});
            }
        }
    }

    return {domainItemsByOrder, selectorItemsByOrder};
}

NKikimrConfig::TAppConfig BundleDomainConfig(const TDomainItemsContainer &items) {
    NKikimrConfig::TAppConfig config;

    for (auto &[_, item] : items) {
        ui32 kind = item.GetKind();

        if (kind == 0) {
            kind = DetectConfigItemKind(item);
        }

        if (item.GetMergeStrategy() == NKikimrConsole::TConfigItem::MERGE) {
            config.MergeFrom(item.GetConfig());
        } else if (item.GetMergeStrategy() == NKikimrConsole::TConfigItem::OVERWRITE) {
            CopyFrom(config, item.GetConfig(), kind);
        } else if (item.GetMergeStrategy() == NKikimrConsole::TConfigItem::MERGE_OVERWRITE_REPEATED) {
            MergeMessageOverwriteRepeated(config, item.GetConfig(), kind);
        }
    }

    return config;
}

TVector<TSelectorData> FillSelectorsData(const TSelectorItemsContainer &sItems) {
    TVector<TSelectorData> selectors;

    for (auto &[_, items] : sItems) {
        auto &item = items.back();

        NYamlConfig::TSelector rules;

        auto &scope = item.GetUsageScope();
        switch (scope.GetFilterCase()) {
            case NKikimrConsole::TUsageScope::kNodeFilter:
                {
                    TSet<TString> nodeIds;
                    for (auto &it : items) {
                        for (auto &id : it.GetUsageScope().GetNodeFilter().GetNodes()) {
                            nodeIds.insert(ToString(id));
                        }
                    }

                    rules.In.emplace("node_id", NYamlConfig::TLabelValueSet{nodeIds});
                }
                break;
            case NKikimrConsole::TUsageScope::kHostFilter:
                {
                    TSet<TString> hosts;
                    for (auto &it : items) {
                        for (auto &host : it.GetUsageScope().GetHostFilter().GetHosts()) {
                            hosts.insert(host);
                        }
                    }

                    rules.In.emplace("host", NYamlConfig::TLabelValueSet{hosts});
                }
                break;
            case NKikimrConsole::TUsageScope::kTenantAndNodeTypeFilter:
                if (!scope.GetTenantAndNodeTypeFilter().GetTenant().empty() &&
                    !scope.GetTenantAndNodeTypeFilter().GetNodeType().empty()) {
                    for (auto &it : items) {
                        rules.In.emplace(
                            "tenant",
                            NYamlConfig::TLabelValueSet{
                                TSet<TString>{it.GetUsageScope().GetTenantAndNodeTypeFilter().GetTenant()}});
                        rules.In.emplace(
                            "node_type",
                            NYamlConfig::TLabelValueSet{
                                TSet<TString>{it.GetUsageScope().GetTenantAndNodeTypeFilter().GetNodeType()}});

                        TStringStream desc;
                        desc << "cookie=" << item.GetCookie()
                             << " merge_strategy=" << NConsole::TConfigItem::MergeStrategyName(item.GetMergeStrategy());
                        if (it.GetId().GetId() != 0) {
                            desc << " id=" << it.GetId().GetId() << "." << it.GetId().GetGeneration();;
                        }

                        selectors.emplace_back(TSelectorData{
                            item.GetMergeStrategy(),
                            rules,
                            item.GetConfig(),
                            desc.Str()});
                        rules.In.clear();
                    }

                    break;
                }
                if (!scope.GetTenantAndNodeTypeFilter().GetTenant().empty()) {
                    TSet<TString> tenants;
                    for (auto &it : items) {
                        tenants.insert(it.GetUsageScope().GetTenantAndNodeTypeFilter().GetTenant());
                    }

                    rules.In.emplace("tenant", NYamlConfig::TLabelValueSet{tenants});
                }
                if (!scope.GetTenantAndNodeTypeFilter().GetNodeType().empty()) {
                    TSet<TString> nodeTypes;
                    for (auto &it : items) {
                        nodeTypes.insert(it.GetUsageScope().GetTenantAndNodeTypeFilter().GetNodeType());
                    }

                    rules.In.emplace("node_type", NYamlConfig::TLabelValueSet{nodeTypes});
                }
                break;
            default: break;
        }
        if (!rules.In.empty()) {
            TStringStream desc;
            desc << "cookie=" << item.GetCookie()
                 << " merge_strategy=" << NConsole::TConfigItem::MergeStrategyName(item.GetMergeStrategy());

            if (items[0].GetId().GetId() != 0) {
                desc << " id=";
                bool first = true;
                for (auto &it : items) {
                    desc << (first ? "" : ",") << it.GetId().GetId() << "." << it.GetId().GetGeneration();
                    first = false;
                }
            }

            selectors.emplace_back(TSelectorData{
                item.GetMergeStrategy(),
                rules,
                item.GetConfig(),
                desc.Str()});
        }
    }

    return selectors;
}

void SerializeSelectorsToYaml(
    const TVector<TSelectorData> &selectors,
    NFyaml::TDocument &doc,
    NFyaml::TSequence &seq) {

    const TString selectorTemplate = R"(
description: ""
selector: {}
config: {}
)";

    auto selectorTemplateYaml = NFyaml::TDocument::Parse(selectorTemplate);
    auto selectorConfigRoot = selectorTemplateYaml.Root();

    for (auto &selector : selectors) {
        auto config = NFyaml::TDocument::Parse(NProtobufJson::Proto2Json(selector.Config, GetProto2JsonConfig()));
        auto configNode = config.Root().Copy(doc);
        auto configNodeRef = configNode.Ref();

        switch (selector.MergeStrategy) {
            case NKikimrConsole::TConfigItem::MERGE_OVERWRITE_REPEATED:
                MarkYamlForMergeOverwriteRepeated(configNodeRef);
            break;
            case NKikimrConsole::TConfigItem::MERGE:
                MarkYamlForMerge(configNodeRef);
            break;
            default: break;
        }

        auto node = selectorConfigRoot.Copy(doc);
        seq.Append(node.Ref());
        node.Ref().Map().pair_at("config").SetValue(configNode.Ref());

        node.Ref().Map().pair_at("description").SetValue(doc.Buildf("%s", selector.Description.c_str()));

        auto selectorNode = node.Ref().Map().at("selector").Map();
        for (auto &[label, values] : selector.Rules.In) {
            if (values.Values.size() == 1) {
                auto labelNode = doc.Buildf("%s", label.c_str());
                auto valueNode = doc.Buildf("%s", values.Values.begin()->c_str());
                selectorNode.Append(labelNode, valueNode);
            } else {
                auto labelNode = doc.Buildf("%s", label.c_str());
                auto inNode = doc.Buildf("{ in: [] }");
                auto inSeq = inNode.Map().at("in").Sequence();
                for (auto &value : values.Values) {
                    auto valueNode = doc.Buildf("%s", value.c_str());
                    inSeq.Append(valueNode);
                }
                selectorNode.Append(labelNode, inNode);
            }
        }
    }
}

TString DumpConsoleConfigs(const ::google::protobuf::RepeatedPtrField<NKikimrConsole::TConfigItem> &configItems) {
    const auto [domainItemsByOrder, selectorItemsByOrder] = ExtractSuitableItems(configItems);

    const NKikimrConfig::TAppConfig configProto = BundleDomainConfig(domainItemsByOrder);
    auto mainConfigYaml = NFyaml::TDocument::Parse(
        NProtobufJson::Proto2Json(configProto, GetProto2JsonConfig()));

    const TString configTemplate = R"(
config: {}

allowed_labels:
  node_id: {type: string}
  host: {type: string}
  tenant: {type: string}

selector_config: []
)";

    auto outDoc = NFyaml::TDocument::Parse(configTemplate);

    auto configTemplateConfig = outDoc.Root().Map().pair_at("config");
    auto mainConfigRoot = mainConfigYaml.Root().Copy(outDoc);
    configTemplateConfig.SetValue(mainConfigRoot.Ref());

    const auto selectors = FillSelectorsData(selectorItemsByOrder);
    auto selectorsSeq = outDoc.Root().Map().at("selector_config").Sequence();
    SerializeSelectorsToYaml(selectors, outDoc, selectorsSeq);

    Beautify(outDoc);

    TStringStream res;
    res << outDoc;

    return res.Str();
}

TDumpConsoleConfigItemResult DumpConsoleConfigItem(const NKikimrConsole::TConfigItem &item) {
    google::protobuf::RepeatedPtrField<NKikimrConsole::TConfigItem> configItems;
    auto* newItem = configItems.Add();
    newItem->CopyFrom(item);
    const auto [domainItemsByOrder, selectorItemsByOrder] = ExtractSuitableItems(configItems);

    const TString configTemplate = R"(
[]
)";
    auto outDoc = NFyaml::TDocument::Parse(configTemplate);
    auto selectorsSeq = outDoc.Root().Sequence();
    if (!domainItemsByOrder.empty()) {
        const NKikimrConfig::TAppConfig configProto = BundleDomainConfig(domainItemsByOrder);
        auto mainConfigYaml = NFyaml::TDocument::Parse(
            NProtobufJson::Proto2Json(configProto, GetProto2JsonConfig()));

        auto configNodeRef = mainConfigYaml.Root();

        switch (item.GetMergeStrategy()) {
            case NKikimrConsole::TConfigItem::MERGE_OVERWRITE_REPEATED:
                MarkYamlForMergeOverwriteRepeated(configNodeRef);
            break;
            case NKikimrConsole::TConfigItem::MERGE:
                MarkYamlForMerge(configNodeRef);
            break;
            default: break;
        }

        const TString selectorTemplate = R"(
description: ""
selector: {}
config: {}
)";

        auto selectorTemplateYaml = NFyaml::TDocument::Parse(selectorTemplate);
        auto selectorConfigRoot = selectorTemplateYaml.Root();

        auto mainConfigRoot = mainConfigYaml.Root().Copy(selectorTemplateYaml);
        selectorConfigRoot.Map().pair_at("config").SetValue(mainConfigRoot.Ref());

        TStringStream desc;

        desc << "cookie=" << item.GetCookie()
             << " merge_strategy=" << NConsole::TConfigItem::MergeStrategyName(item.GetMergeStrategy());

        selectorConfigRoot.Map().pair_at("description").SetValue(selectorTemplateYaml.Buildf("%s", desc.Str().c_str()));

        auto selector = selectorConfigRoot.Copy(outDoc);
        outDoc.Root().Sequence().Append(selector.Ref());
    } else {
        const auto selectors = FillSelectorsData(selectorItemsByOrder);
        SerializeSelectorsToYaml(selectors, outDoc, selectorsSeq);
    }

    Beautify(outDoc);

    TStringStream res;
    res << outDoc;

    return {!domainItemsByOrder.empty(), res.Str()};
}

bool CheckYamlMarkedForMergeOverwriteRepeated(NFyaml::TNodeRef &node) {
    auto rootMap = node.Map();
    for (auto &child : rootMap) {
        auto value = child.Value();
        if (value.Type() == NFyaml::ENodeType::Mapping) {
            if (value.Tag() != "!inherit") {
                return false;
            }
            if (!CheckYamlMarkedForMergeOverwriteRepeated(value)) {
                return false;
            }
        } else if (value.Type() == NFyaml::ENodeType::Sequence) {
            if (value.Tag() && value.Tag() != "") {
                return false;
            }
        }
    }
    return true;
}

bool CheckYamlMarkedForMerge(NFyaml::TNodeRef &node) {
    auto rootMap = node.Map();
    for (auto &child : rootMap) {
        auto value = child.Value();
        if (value.Type() == NFyaml::ENodeType::Mapping) {
            if (value.Tag() != "!inherit") {
                return false;
            }
            if (!CheckYamlMarkedForMerge(value)) {
                return false;
            }
        } else if (value.Type() == NFyaml::ENodeType::Sequence) {
            if (value.Tag() != "!append") {
                return false;
            }
        }
    }
    return true;
}

bool CheckYamlMarkedForOverwrite(NFyaml::TNodeRef &node) {
    auto rootMap = node.Map();
    for (auto &child : rootMap) {
        auto value = child.Value();
        if (value.Type() == NFyaml::ENodeType::Mapping) {
            if (value.Tag() && value.Tag() != "") {
                return false;
            }
            if (!CheckYamlMarkedForOverwrite(value)) {
                return false;
            }
        } else if (value.Type() == NFyaml::ENodeType::Sequence) {
            if (value.Tag() && value.Tag() != "") {
                return false;
            }
        }
    }
    return true;
}

NKikimrConsole::TConfigureRequest DumpYamlConfigRequest(const TString &cItem, const TString &domain) {
    NKikimrConsole::TConfigureRequest result;
    auto doc = NFyaml::TDocument::Parse(cItem);
    auto root = doc.Root();
    Y_ABORT_UNLESS(root.Type() == NFyaml::ENodeType::Sequence, "Root has to be sequence");
    auto rootSeq = root.Sequence();

    Y_ABORT_UNLESS(rootSeq.size() == 1, "Only single-element configs are supported");
    auto selectorNode = rootSeq.at(0);

    Y_ABORT_UNLESS(selectorNode.Type() == NFyaml::ENodeType::Mapping, "Selector has to be mapping");
    auto configMap = selectorNode.Map();

    auto descNode = configMap["description"];
    auto selectorsNode = configMap["selector"];
    auto configNode = configMap["config"];
    Y_ABORT_UNLESS(descNode && selectorsNode && configNode, "Selector should have description, selector and config fields");

    Y_ABORT_UNLESS(descNode.Type() == NFyaml::ENodeType::Scalar, "Description has to be scalar");
    auto desc = descNode.Scalar();

    std::regex cookieRegex("cookie=(\\S+)");
    auto cookieBegin = std::sregex_iterator(desc.begin(), desc.end(), cookieRegex);
    TString cookie = desc;
    if (std::distance(cookieBegin, std::sregex_iterator()) == 1) {
        cookie = cookieBegin->str().substr(strlen("cookie="), std::string::npos);
    }

    Y_ABORT_UNLESS(selectorsNode.Type() == NFyaml::ENodeType::Mapping, "Selectors field has to be mapping");
    auto selectorsMap = selectorsNode.Map();
    Y_ABORT_UNLESS(selectorsMap.size() <= 1, "Selectors should have zero or exactly one selectors");
    TVector<TString> tenants;
    if (selectorsMap.size() == 1) {
        Y_ABORT_UNLESS(selectorsMap["tenant"], "The only supported selector is tenant");
        switch (selectorsMap["tenant"].Type()) {
            case NFyaml::ENodeType::Scalar:
                {
                    auto tenant = selectorsMap["tenant"].Scalar();
                    Y_ABORT_UNLESS(tenant.StartsWith(domain + "/"), "Tenant should be in domain");
                    tenants.push_back(tenant);
                }
                break;
            case NFyaml::ENodeType::Mapping:
                {
                    Y_ABORT_UNLESS(selectorsMap["tenant"].Map()["in"] && selectorsMap["tenant"].Map()["in"].Type() == NFyaml::ENodeType::Sequence, "Invalid tenant list");
                    for (auto& node : selectorsMap["tenant"].Map()["in"].Sequence()) {
                        Y_ABORT_UNLESS(node.Type() == NFyaml::ENodeType::Scalar, "Invalid tenant");
                        Y_ABORT_UNLESS(node.Scalar().StartsWith(domain + "/"), "Tenant should be in domain");
                        tenants.push_back(node.Scalar());
                    }
                }
                break;
            default:
                Y_ABORT_UNLESS(false, "Invalid tenant");
        }
    }

    std::regex msRegex("merge_strategy=(\\S+)");
    auto msBegin = std::sregex_iterator(desc.begin(), desc.end(), msRegex);
    auto nStrategies = std::distance(msBegin, std::sregex_iterator());
    Y_ABORT_UNLESS(nStrategies <= 1, "Description should have exactly one merge_strategy");
    auto mergeStrategy = NKikimrConsole::TConfigItem::MERGE;
    if (nStrategies) {
        auto ms = msBegin->str().substr(strlen("merge_strategy="), std::string::npos);
        if (ms == "MERGE") {
            mergeStrategy = NKikimrConsole::TConfigItem::MERGE;
        } else if (ms == "MERGE_OVERWRITE_REPEATED") {
            mergeStrategy = NKikimrConsole::TConfigItem::MERGE_OVERWRITE_REPEATED;
        } else if (ms == "OVERWRITE") {
            mergeStrategy = NKikimrConsole::TConfigItem::OVERWRITE;
        } else {
            Y_ABORT_UNLESS(false, "Incorrect merge_strategy in description");
        }
    }

    auto prepareActions = [&](auto& result, auto configItemCb) {
        Y_ABORT_UNLESS(configNode.Type() == NFyaml::ENodeType::Mapping, "Config has to be mapping");
        switch (mergeStrategy) {
            case NKikimrConsole::TConfigItem::MERGE_OVERWRITE_REPEATED:
                Y_ABORT_UNLESS(CheckYamlMarkedForMergeOverwriteRepeated(configNode), "Inheritance tags doesn't match choosen merge strategy");
            break;
            case NKikimrConsole::TConfigItem::MERGE:
                Y_ABORT_UNLESS(CheckYamlMarkedForMerge(configNode), "Inheritance tags doesn't match choosen merge strategy");
            break;
            default:
                Y_ABORT_UNLESS(CheckYamlMarkedForOverwrite(configNode), "Inheritance tags doesn't match choosen merge strategy");
        }

        for (auto& item : configNode.Map()) {
            auto separateConfig = NFyaml::TDocument::Parse("{}");
            auto key = item.Key().Copy(separateConfig);
            auto value = item.Value().Copy(separateConfig);
            separateConfig.Root().Map().Append(key, value);
            auto config = YamlToProto(separateConfig.Root(), true, false);
            NKikimrConsole::TConfigItem& configItem = *result.AddActions()->MutableAddConfigItem()->MutableConfigItem();
            configItemCb(configItem);
            configItem.MutableConfig()->CopyFrom(config);
            configItem.SetCookie(cookie.c_str());
            configItem.SetMergeStrategy(mergeStrategy);
        }
    };

    // for domain
    if (tenants.empty()) {
        prepareActions(result, [](auto&){});
    }

    for (auto& tenant : tenants) {
        prepareActions(result, [&tenant](auto& configItem) {
            configItem.MutableUsageScope()->MutableTenantAndNodeTypeFilter()->SetTenant(tenant);
        });
    }

    return result;
}

} // namespace NKikimr::NYamlConfig

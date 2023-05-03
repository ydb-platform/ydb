#include "console_dumper.h"

#include "util.h"

#include <ydb/core/cms/console/yaml_config/yaml_config.h>
#include <ydb/core/cms/console/util/config_index.h>
#include <library/cpp/yaml/fyamlcpp/fyamlcpp.h>

namespace NYamlConfig {

using namespace NKikimr;

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

std::pair<TDomainItemsContainer, TSelectorItemsContainer> ExtractSuitableItems(
    const ::google::protobuf::RepeatedPtrField<NKikimrConsole::TConfigItem> &configItems) {

    TDomainItemsContainer domainItemsByOrder;
    TSelectorItemsContainer selectorItemsByOrder;

    for (auto &item : configItems) {
        if (item.GetKind() == NKikimrConsole::TConfigItem::NameserviceConfigItem ||
            item.GetKind() == NKikimrConsole::TConfigItem::NetClassifierDistributableConfigItem ||
            item.GetKind() == NKikimrConsole::TConfigItem::NamedConfigsItem ||
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
                Y_VERIFY(it->second.back().GetMergeStrategy() == item.GetMergeStrategy());
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
        Y_VERIFY(item.GetKind() != 0, "Tool doesn't support items with kind Auto");
        if (item.GetMergeStrategy() == NKikimrConsole::TConfigItem::MERGE) {
            config.MergeFrom(item.GetConfig());
        } else if (item.GetMergeStrategy() == NKikimrConsole::TConfigItem::OVERWRITE) {
            CopyFrom(config, item.GetConfig(), item.GetKind());
        } else if (item.GetMergeStrategy() == NKikimrConsole::TConfigItem::MERGE_OVERWRITE_REPEATED) {
            MergeMessageOverwriteRepeated(config, item.GetConfig(), item.GetKind());
        }
    }

    return config;
}

TVector<TSelectorData> FillSelectorsData(const TSelectorItemsContainer &items) {
    TVector<TSelectorData> selectors;

    for (auto &[tuple, items] : items) {
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
                             << " merge_strategy=" << NConsole::TConfigItem::MergeStrategyName(item.GetMergeStrategy())
                             << " id=" << it.GetId().GetId() << "." << it.GetId().GetGeneration();;

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
                 << " merge_strategy=" << NConsole::TConfigItem::MergeStrategyName(item.GetMergeStrategy())
                 <<  " id=";
            bool first = true;
            for (auto &it : items) {
                desc << (first ? "" : ",") << it.GetId().GetId() << "." << it.GetId().GetGeneration();
                first = false;
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

} // namespace NYamlConfig

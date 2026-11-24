#pragma once

#include "console_configs_provider.h"
#include "validators/validator_nameservice.h"

#include <ydb/core/testlib/tenant_runtime.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NConsole::NTests {

class TCountingValidator : public IConfigValidator {
public:
    mutable ui64 Checks = 0;
    mutable bool ToggleDuringCheck = false;

    TCountingValidator()
        : IConfigValidator("counting", THashSet<ui32>{})
    {}

    bool CheckConfig(const NKikimrConfig::TAppConfig&, const NKikimrConfig::TAppConfig& config,
            TVector<Ydb::Issue::IssueMessage>& issues) const override {
        ++Checks;
        if (ToggleDuringCheck) {
            ToggleDuringCheck = false;
            auto* self = const_cast<TCountingValidator*>(this);
            self->Disable();
            self->Enable();
        }
        if (config.GetLogConfig().GetClusterName() == "reject") {
            AddError(issues, "rejected config");
            return false;
        }
        return true;
    }
};

class TCountingNameserviceValidator : public TNameserviceConfigValidator {
public:
    mutable ui64 Checks = 0;

    bool CheckConfig(const NKikimrConfig::TAppConfig& oldConfig, const NKikimrConfig::TAppConfig& config,
            TVector<Ydb::Issue::IssueMessage>& issues) const override {
        ++Checks;
        return TNameserviceConfigValidator::CheckConfig(oldConfig, config, issues);
    }
};

inline NKikimrConsole::TNodeAttributes Node(ui32 id, TString host = {}, TString tenant = {}, TString type = {}) {
    NKikimrConsole::TNodeAttributes node;
    node.SetNodeId(id);
    node.SetHost(host);
    node.SetTenant(tenant);
    node.SetNodeType(type);
    return node;
}

inline TConfigItem::TPtr LogItem(ui64 id, TString value, TVector<ui32> nodes = {}, TVector<TString> hosts = {},
        TString tenant = {}, TString type = {}, ui32 order = 0) {
    NKikimrConsole::TConfigItem item;
    item.MutableId()->SetId(id);
    item.MutableId()->SetGeneration(1);
    item.SetKind(NKikimrConsole::TConfigItem::LogConfigItem);
    item.SetOrder(order);
    item.SetMergeStrategy(NKikimrConsole::TConfigItem::MERGE);
    item.MutableConfig()->MutableLogConfig()->SetClusterName(value);
    if (!nodes.empty()) {
        for (auto node : nodes) {
            item.MutableUsageScope()->MutableNodeFilter()->AddNodes(node);
        }
    } else if (!hosts.empty()) {
        for (const auto& host : hosts) {
            item.MutableUsageScope()->MutableHostFilter()->AddHosts(host);
        }
    } else if (tenant || type) {
        item.MutableUsageScope()->MutableTenantAndNodeTypeFilter()->SetTenant(tenant);
        item.MutableUsageScope()->MutableTenantAndNodeTypeFilter()->SetNodeType(type);
    }
    return new TConfigItem(item);
}

inline TConfigItem::TPtr NameserviceItem(ui32 nodes) {
    auto item = LogItem(1, {});
    item->Kind = NKikimrConsole::TConfigItem::NameserviceConfigItem;
    item->Config.Clear();
    for (ui32 id = 1; id <= nodes; ++id) {
        auto* node = item->Config.MutableNameserviceConfig()->AddNode();
        node->SetNodeId(id);
        node->SetHost(TStringBuilder() << "host" << id);
        node->SetInterconnectHost(TStringBuilder() << "host" << id);
        node->SetAddress(TStringBuilder() << "address" << id);
        node->SetPort(19001);
    }
    return item;
}

inline TString MainYaml(ui64 version) {
    return TStringBuilder() << "metadata:\n  kind: MainConfig\n  cluster: \"\"\n  version: " << version << "\nconfig: {}\n";
}

class TConfigsProviderFixture {
public:
    TTenantTestRuntime Runtime{DefaultTenantTestConfig};
    NMonitoring::TDynamicCounterPtr Counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
    TActorId Provider;
    TActorId Sender;

    explicit TConfigsProviderFixture(IConfigValidator::TPtr validator) {
        TValidatorsRegistry::DropInstance();
        auto registry = TValidatorsRegistry::Instance();
        UNIT_ASSERT(registry->AddValidator(validator));
        registry->LockValidators();
        Runtime.SetLogPriority(NKikimrServices::CMS_CONFIGS, NLog::PRI_ERROR);
        Sender = Runtime.AllocateEdgeActor();
        Provider = Runtime.Register(new TConfigsProvider(Sender, Counters));
    }

    void Send(IEventBase* event) {
        Runtime.Send(new IEventHandle(Provider, Sender, event), 0, true);
    }

    void Sync() {
        Send(new TConfigsProvider::TEvPrivate::TEvUpdateConfigs({},
            new IEventHandle(Sender, Provider, new TEvents::TEvWakeup())));
        Runtime.GrabEdgeEventRethrow<TEvents::TEvWakeup>(Sender);
    }

    void SetItems(const TVector<TConfigItem::TPtr>& items) {
        THashMap<ui64, TConfigItem::TPtr> map;
        for (auto& item : items) {
            map.emplace(item->Id, item);
        }
        Send(new TConfigsProvider::TEvPrivate::TEvSetConfigs(map));
        Sync();
    }

    void Update(const TConfigModifications& modifications) {
        Send(new TConfigsProvider::TEvPrivate::TEvUpdateConfigs(modifications, {}));
        Sync();
    }

    void SetValidation(bool enabled) {
        TConfigsConfig config;
        config.EnableValidationOnNodeConfigRequest = enabled;
        Send(new TConfigsProvider::TEvPrivate::TEvSetConfig(config));
        Sync();
    }

    TEvConsole::TEvGetNodeConfigResponse::TPtr Request(const NKikimrConsole::TNodeAttributes& node,
            const TVector<ui32>& kinds = {}, bool serveYaml = false, std::optional<ui32> apiVersion = {}, ui64 cookie = 0) {
        auto event = MakeHolder<TEvConsole::TEvGetNodeConfigRequest>();
        event->Record.MutableNode()->CopyFrom(node);
        for (auto kind : kinds) {
            event->Record.AddItemKinds(kind);
        }
        event->Record.SetServeYaml(serveYaml);
        if (apiVersion) {
            event->Record.SetYamlApiVersion(*apiVersion);
        }
        Runtime.Send(new IEventHandle(Provider, Sender, event.Release(), 0, cookie), 0, true);
        auto response = Runtime.GrabEdgeEventRethrow<TEvConsole::TEvGetNodeConfigResponse>(Sender);
        UNIT_ASSERT_VALUES_EQUAL(response->Sender, Provider);
        UNIT_ASSERT_VALUES_EQUAL(response->Cookie, cookie);
        return response;
    }

    ui64 Counter(const TString& name, bool derivative = false) const {
        return Counters->GetCounter(name, derivative)->Val();
    }
};

} // namespace NKikimr::NConsole::NTests

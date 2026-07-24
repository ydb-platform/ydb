#include <ydb/core/cms/console/util/config_index.h>
#include <ydb/core/protos/tenant_pool.pb.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <array>

using namespace NKikimr;
using namespace NKikimr::NConsole;

namespace {

constexpr std::array<ui32, 3> Kinds = {
    NKikimrConsole::TConfigItem::LogConfigItem,
    NKikimrConsole::TConfigItem::TenantPoolConfigItem,
    NKikimrConsole::TConfigItem::YamlConfigEnabledItem,
};

constexpr std::array<TStringBuf, 3> Hosts = {
    "host-a",
    "host-b",
    "host-c",
};

constexpr std::array<TStringBuf, 3> Tenants = {
    "/Root",
    "/Root/tenant-a",
    "/Root/tenant-b",
};

constexpr std::array<TStringBuf, 3> NodeTypes = {
    "storage",
    "compute",
    "hybrid",
};

ui32 PickKind(FuzzedDataProvider& fdp) {
    return Kinds[fdp.ConsumeIntegralInRange<size_t>(0, Kinds.size() - 1)];
}

TString PickString(FuzzedDataProvider& fdp, const std::array<TStringBuf, 3>& values) {
    return TString(values[fdp.ConsumeIntegralInRange<size_t>(0, values.size() - 1)]);
}

THashSet<ui32> PickKinds(FuzzedDataProvider& fdp) {
    THashSet<ui32> result;
    for (ui32 kind : Kinds) {
        if (fdp.ConsumeBool()) {
            result.insert(kind);
        }
    }
    if (result.empty()) {
        result.insert(PickKind(fdp));
    }
    return result;
}

TUsageScope MakeScope(FuzzedDataProvider& fdp, ui32 order) {
    TUsageScope scope;
    scope.Order = order;

    switch (fdp.ConsumeIntegralInRange<ui8>(0, 4)) {
        case 0:
            scope.NodeIds.insert(fdp.ConsumeIntegralInRange<ui32>(1, 4));
            if (fdp.ConsumeBool()) {
                scope.NodeIds.insert(fdp.ConsumeIntegralInRange<ui32>(1, 4));
            }
            break;
        case 1:
            scope.Hosts.insert(PickString(fdp, Hosts));
            if (fdp.ConsumeBool()) {
                scope.Hosts.insert(PickString(fdp, Hosts));
            }
            break;
        case 2:
            scope.Tenant = PickString(fdp, Tenants);
            scope.NodeType = PickString(fdp, NodeTypes);
            break;
        case 3:
            scope.Tenant = PickString(fdp, Tenants);
            break;
        case 4:
            scope.NodeType = PickString(fdp, NodeTypes);
            break;
    }

    NKikimrConsole::TUsageScope proto;
    scope.Serialize(proto);
    Y_ABORT_UNLESS(scope == TUsageScope(proto, order));

    return scope;
}

NKikimrConfig::TAppConfig MakeConfig(FuzzedDataProvider& fdp, ui32 kind, ui64 id) {
    NKikimrConfig::TAppConfig config;
    switch (kind) {
        case NKikimrConsole::TConfigItem::LogConfigItem: {
            auto* log = config.MutableLogConfig();
            log->SetClusterName("cluster-" + ToString(id % 5));
            log->SetDefaultLevel(fdp.ConsumeIntegralInRange<ui32>(0, 8));
            if (fdp.ConsumeBool()) {
                auto* entry = log->AddEntry();
                entry->SetComponent("component-" + ToString(fdp.ConsumeIntegralInRange<ui32>(0, 4)));
                entry->SetLevel(fdp.ConsumeIntegralInRange<ui32>(0, 8));
            }
            break;
        }
        case NKikimrConsole::TConfigItem::TenantPoolConfigItem: {
            auto* tenantPool = config.MutableTenantPoolConfig();
            tenantPool->SetIsEnabled(fdp.ConsumeBool());
            tenantPool->SetNodeType(PickString(fdp, NodeTypes));
            if (fdp.ConsumeBool()) {
                auto* slot = tenantPool->AddSlots();
                slot->SetId("slot-" + ToString(id % 7));
                slot->SetTenantName(PickString(fdp, Tenants));
                slot->SetType(PickString(fdp, NodeTypes));
            }
            break;
        }
        default:
            config.SetYamlConfigEnabled(fdp.ConsumeBool());
            break;
    }
    return config;
}

ui32 PickMergeStrategy(FuzzedDataProvider& fdp) {
    switch (fdp.ConsumeIntegralInRange<ui8>(0, 2)) {
        case 0:
            return NKikimrConsole::TConfigItem::OVERWRITE;
        case 1:
            return NKikimrConsole::TConfigItem::MERGE;
        default:
            return NKikimrConsole::TConfigItem::MERGE_OVERWRITE_REPEATED;
    }
}

TConfigItem::TPtr MakeItem(FuzzedDataProvider& fdp, ui64 id) {
    TConfigItem::TPtr item = new TConfigItem;
    item->Id = id;
    item->Generation = fdp.ConsumeIntegralInRange<ui64>(1, 4);
    item->Kind = PickKind(fdp);
    item->UsageScope = MakeScope(fdp, id & 0xffff);
    item->MergeStrategy = PickMergeStrategy(fdp);
    item->Config = MakeConfig(fdp, item->Kind, id);
    item->Cookie = "cookie-" + ToString(fdp.ConsumeIntegralInRange<ui32>(0, 5));

    NKikimrConsole::TConfigItem proto;
    item->Serialize(proto);
    TConfigItem roundtrip(proto);
    Y_ABORT_UNLESS(roundtrip.Id == item->Id);
    Y_ABORT_UNLESS(roundtrip.Generation == item->Generation);
    Y_ABORT_UNLESS(roundtrip.Kind == item->Kind);
    Y_ABORT_UNLESS(roundtrip.UsageScope == item->UsageScope);
    Y_ABORT_UNLESS(roundtrip.MergeStrategy == item->MergeStrategy);
    Y_ABORT_UNLESS(roundtrip.Config.ShortDebugString() == item->Config.ShortDebugString());
    Y_ABORT_UNLESS(roundtrip.Cookie == item->Cookie);

    return item;
}

NKikimrConsole::TNodeAttributes MakeNode(FuzzedDataProvider& fdp) {
    NKikimrConsole::TNodeAttributes attrs;
    attrs.SetNodeId(fdp.ConsumeIntegralInRange<ui32>(1, 4));
    attrs.SetHost(PickString(fdp, Hosts));
    attrs.SetTenant(PickString(fdp, Tenants));
    attrs.SetNodeType(PickString(fdp, NodeTypes));
    return attrs;
}

TSubscription::TPtr MakeSubscription(FuzzedDataProvider& fdp, ui64 id) {
    TSubscription::TPtr subscription = new TSubscription;
    subscription->Id = id;
    if (fdp.ConsumeBool()) {
        subscription->Subscriber.TabletId = fdp.ConsumeIntegralInRange<ui64>(1, 8);
    } else {
        subscription->Subscriber.ServiceId = TActorId(fdp.ConsumeIntegralInRange<ui32>(1, 4), "cfgsvc");
    }
    subscription->NodeId = fdp.ConsumeIntegralInRange<ui32>(1, 4);
    subscription->Host = PickString(fdp, Hosts);
    subscription->Tenant = PickString(fdp, Tenants);
    subscription->NodeType = PickString(fdp, NodeTypes);
    subscription->ItemKinds = PickKinds(fdp);
    subscription->Cookie = fdp.ConsumeIntegral<ui64>();

    NKikimrConsole::TSubscription proto;
    subscription->Serialize(proto);
    TSubscription roundtrip(proto);
    Y_ABORT_UNLESS(subscription->IsEqual(roundtrip));

    return subscription;
}

template <typename TMap>
TVector<ui64> Keys(const TMap& map) {
    TVector<ui64> result;
    result.reserve(map.size());
    for (const auto& [id, _] : map) {
        result.push_back(id);
    }
    return result;
}

void CheckIndex(TConfigIndex& configs, TSubscriptionIndex& subscriptions, FuzzedDataProvider& fdp) {
    const auto kinds = PickKinds(fdp);
    const auto attrs = MakeNode(fdp);

    TConfigItems nodeItems;
    configs.CollectItemsForNode(attrs, kinds, nodeItems);

    auto scoped = configs.GetNodeConfig(attrs, kinds);
    NKikimrConfig::TAppConfig computed;
    scoped->ComputeConfig(computed, true);
    Y_ABORT_UNLESS(static_cast<size_t>(computed.GetVersion().ItemsSize()) <= configs.GetConfigItems().size());

    TSubscriptionSet affected;
    TUsageScope scope = MakeScope(fdp, fdp.ConsumeIntegralInRange<ui32>(0, 32));
    subscriptions.CollectAffectedSubscriptions(scope, PickKind(fdp), affected);
    Y_ABORT_UNLESS(affected.size() <= subscriptions.GetSubscriptions().size());
}

void FuzzConsole(FuzzedDataProvider& fdp) {
    TConfigIndex configs;
    TSubscriptionIndex subscriptions;

    ui64 nextConfigId = 1;
    ui64 nextSubscriptionId = 1;

    const ui32 steps = fdp.ConsumeIntegralInRange<ui32>(1, 96);
    for (ui32 step = 0; step < steps && fdp.remaining_bytes(); ++step) {
        switch (fdp.ConsumeIntegralInRange<ui8>(0, 7)) {
            case 0: {
                configs.AddItem(MakeItem(fdp, nextConfigId++));
                break;
            }
            case 1: {
                auto ids = Keys(configs.GetConfigItems());
                if (!ids.empty()) {
                    const ui64 id = ids[fdp.ConsumeIntegralInRange<size_t>(0, ids.size() - 1)];
                    TConfigModifications modifications;
                    auto item = MakeItem(fdp, id);
                    item->Generation = configs.GetItem(id)->Generation + 1;
                    modifications.ModifiedItems.emplace(id, item);
                    modifications.ApplyTo(configs);
                }
                break;
            }
            case 2: {
                auto ids = Keys(configs.GetConfigItems());
                if (!ids.empty()) {
                    const ui64 id = ids[fdp.ConsumeIntegralInRange<size_t>(0, ids.size() - 1)];
                    TConfigModifications modifications;
                    modifications.RemovedItems.emplace(id, configs.GetItem(id));
                    modifications.ApplyTo(configs);
                }
                break;
            }
            case 3: {
                subscriptions.AddSubscription(MakeSubscription(fdp, nextSubscriptionId++));
                break;
            }
            case 4: {
                const auto& all = subscriptions.GetSubscriptions();
                auto ids = Keys(all);
                if (!ids.empty()) {
                    subscriptions.RemoveSubscription(ids[fdp.ConsumeIntegralInRange<size_t>(0, ids.size() - 1)]);
                }
                break;
            }
            case 5: {
                TConfigItems items;
                configs.CollectItemsByScope(MakeScope(fdp, fdp.ConsumeIntegralInRange<ui32>(0, 32)), PickKinds(fdp), items);
                Y_ABORT_UNLESS(items.size() <= configs.GetConfigItems().size());
                break;
            }
            case 6: {
                TConfigItems conflicts;
                configs.CollectItemsByConflictingScope(
                    MakeScope(fdp, fdp.ConsumeIntegralInRange<ui32>(0, 32)),
                    PickKinds(fdp),
                    fdp.ConsumeBool(),
                    conflicts);
                Y_ABORT_UNLESS(conflicts.size() <= configs.GetConfigItems().size());
                break;
            }
            default:
                CheckIndex(configs, subscriptions, fdp);
                break;
        }

        if ((step & 7) == 7) {
            CheckIndex(configs, subscriptions, fdp);
        }
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size > 16 * 1024) {
        return 0;
    }

    try {
        FuzzedDataProvider fdp(data, size);
        FuzzConsole(fdp);
    } catch (...) {
    }

    return 0;
}

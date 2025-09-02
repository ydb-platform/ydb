#include "config_index.h"

#include <util/string/builder.h>
#include <util/string/join.h>

namespace NKikimr::NConsole {

TUsageScope::TUsageScope()
    : Order(0)
{
}

TUsageScope::TUsageScope(const NKikimrConsole::TUsageScope &scope,
                         ui32 order)
    : Order(order)
{
    switch (scope.GetFilterCase()) {
    case NKikimrConsole::TUsageScope::kNodeFilter:
        for (auto &id : scope.GetNodeFilter().GetNodes())
            NodeIds.insert(id);
        break;
    case NKikimrConsole::TUsageScope::kHostFilter:
        for (auto &host : scope.GetHostFilter().GetHosts())
            Hosts.insert(host);
        break;
    case NKikimrConsole::TUsageScope::kTenantAndNodeTypeFilter:
        Tenant = scope.GetTenantAndNodeTypeFilter().GetTenant();
        NodeType = scope.GetTenantAndNodeTypeFilter().GetNodeType();
        break;
    case NKikimrConsole::TUsageScope::FILTER_NOT_SET:
        break;
    default:
        Y_ABORT("unexpected usage scope filter");
    }
}

int TUsageScope::GetPriority() const
{
    if (!NodeIds.empty())
        return 1;
    if (!Hosts.empty())
        return 2;
    if (Tenant && NodeType)
        return 3;
    if (Tenant)
        return 4;
    if (NodeType)
        return 5;
    return 6;
}

void TUsageScope::Serialize(NKikimrConsole::TUsageScope &scope) const
{
    for (auto id : NodeIds)
        scope.MutableNodeFilter()->AddNodes(id);
    for (auto &host : Hosts)
        scope.MutableHostFilter()->AddHosts(host);
    if (Tenant || NodeType) {
        scope.MutableTenantAndNodeTypeFilter()->SetTenant(Tenant);
        scope.MutableTenantAndNodeTypeFilter()->SetNodeType(NodeType);
    }
}

bool TUsageScope::IsSameScope(const NKikimrConsole::TUsageScope &scope) const
{
    return (*this == TUsageScope(scope, Order));
}

bool TUsageScope::HasConflict(const TUsageScope &other,
                              bool ignoreOrder) const
{
    if (GetPriority() != other.GetPriority())
        return false;

    if (!ignoreOrder && Order != other.Order)
        return false;

    if (!NodeIds.empty()) {
        for (auto id : NodeIds)
            if (other.NodeIds.contains(id))
                return true;
    } else if (!Hosts.empty()) {
        for (auto &host : Hosts)
            if (other.Hosts.contains(host))
                return true;
    } else if (Tenant == other.Tenant && NodeType == other.NodeType)
        return true;

    return false;
}

TString TUsageScope::ToString() const
{
    TStringBuilder str;
    if (!NodeIds.empty()) {
        str << "node(s)";
        for (auto id : NodeIds)
            str << " #" << id;
        str << " ";
    }
    else if (!Hosts.empty()) {
        str << "host(s)";
        for (auto &host : Hosts)
            str << " '" << host << "'";
        str << " ";
    } else {
        if (Tenant)
            str << "tenant " << Tenant << " ";
        if (NodeType)
            str << "node type " << NodeType << " ";
        if (!Tenant && !NodeType)
            str << "domain ";
    }
    str << "order " << Order;
    return str;
}

int TUsageScope::ComparePriority(const TUsageScope &lhs,
                                 const TUsageScope &rhs)
{
    int lhp = lhs.GetPriority();
    int rhp = rhs.GetPriority();
    if (lhp < rhp)
        return 1;
    if (lhp > rhp)
        return -1;
    if (lhs.Order > rhs.Order)
        return 1;
    if (lhs.Order < rhs.Order)
        return -1;
    return 0;
}

TConfigItem::TConfigItem()
    : Kind(0)
{
}

TConfigItem::TConfigItem(const NKikimrConsole::TConfigItem &item)
    : Id(item.GetId().GetId())
    , Generation(item.GetId().GetGeneration())
    , Kind(item.GetKind())
    , UsageScope(item.GetUsageScope(), item.GetOrder())
    , MergeStrategy(item.GetMergeStrategy())
    , Config(item.GetConfig())
    , Cookie(item.GetCookie())
{
}

void TConfigItem::Serialize(NKikimrConsole::TConfigItem &item) const
{
    item.MutableId()->SetId(Id);
    item.MutableId()->SetGeneration(Generation);
    item.SetKind(Kind);
    item.MutableConfig()->CopyFrom(Config);
    UsageScope.Serialize(*item.MutableUsageScope());
    item.SetOrder(UsageScope.Order);
    item.SetMergeStrategy(MergeStrategy);
    item.SetCookie(Cookie);
}

TString TConfigItem::KindName(ui32 kind)
{
    if (NKikimrConsole::TConfigItem::EKind_IsValid(static_cast<int>(kind)))
        return NKikimrConsole::TConfigItem::EKind_Name(static_cast<NKikimrConsole::TConfigItem::EKind>(kind));
    return Sprintf("<unknown item kind (%" PRIu32 ")>", kind);
}

TString TConfigItem::KindName() const
{
    return KindName(Kind);
}

TString TConfigItem::MergeStrategyName(ui32 merge)
{
    if (NKikimrConsole::TConfigItem::EMergeStrategy_IsValid(static_cast<int>(merge)))
        return NKikimrConsole::TConfigItem::EMergeStrategy_Name(static_cast<NKikimrConsole::TConfigItem::EMergeStrategy>(merge));
    return Sprintf("<unknown merge strategy (%" PRIu32 ")>", merge);
}

TString TConfigItem::MergeStrategyName() const
{
    return MergeStrategyName(MergeStrategy);
}

TString TConfigItem::ToString() const
{
    TStringBuilder str;
    str << "item #" << Id << " kind=" << KindName()
        << " scope='" << UsageScope.ToString() << "'"
        << " merge=" << MergeStrategyName()
        << " config=" << Config.ShortDebugString()
        << " cookie=" << Cookie;

    return str;
}

void ClearOverwrittenRepeated(::google::protobuf::Message &to,
                              ::google::protobuf::Message &from)
{
    auto *desc = to.GetDescriptor();
    auto *reflection = to.GetReflection();
    for (int i = 0; i < desc->field_count(); ++i) {
        auto *field = desc->field(i);
        if (field->is_repeated()) {
            if (reflection->FieldSize(from, field))
                reflection->ClearField(&to, field);
        } else if (field->type() == ::google::protobuf::FieldDescriptor::TYPE_MESSAGE) {
            if (reflection->HasField(to, field) && reflection->HasField(from, field))
                ClearOverwrittenRepeated(*reflection->MutableMessage(&to, field),
                                         *reflection->MutableMessage(&from, field));
        }
    }
}

void MergeMessageOverwriteRepeated(::google::protobuf::Message &to,
                                   ::google::protobuf::Message &from)
{
    ClearOverwrittenRepeated(to, from);
    to.MergeFrom(from);
}

void TScopedConfig::ComputeConfig(NKikimrConfig::TAppConfig &config, bool addVersion) const
{
    for (auto &pr : ConfigItems) {
        NKikimrConfig::TAppConfig mergeResult;
        MergeItems(pr.second, mergeResult, addVersion);
        config.MergeFrom(mergeResult);
    }
}

void TScopedConfig::ComputeConfig(const THashSet<ui32> &kinds, NKikimrConfig::TAppConfig &config, bool addVersion) const
{
    for (auto &pr : ConfigItems) {
        if (kinds.contains(pr.first)) {
            NKikimrConfig::TAppConfig mergeResult;
            MergeItems(pr.second, mergeResult, addVersion);
            config.MergeFrom(mergeResult);
        }
    }
}

void TScopedConfig::MergeItems(const TOrderedConfigItems &items,
                               NKikimrConfig::TAppConfig &config, bool addVersion) const
{
    for (auto &item : items)
        MergeItem(item, config, addVersion);
}

void TScopedConfig::MergeItem(TConfigItem::TPtr item,
                              NKikimrConfig::TAppConfig &config, bool addVersion) const
{
    if (item->MergeStrategy == NKikimrConsole::TConfigItem::OVERWRITE)
        config.CopyFrom(item->Config);
    else if (item->MergeStrategy == NKikimrConsole::TConfigItem::MERGE)
        config.MergeFrom(item->Config);
    else if (item->MergeStrategy == NKikimrConsole::TConfigItem::MERGE_OVERWRITE_REPEATED)
        MergeMessageOverwriteRepeated(config, item->Config);
    else
        Y_ABORT("unexpected merge strategy %d", static_cast<int>(item->MergeStrategy));

    if (addVersion) {
        auto vItem = config.MutableVersion()->AddItems();
        vItem->SetKind(item->Kind);
        vItem->SetId(item->Id);
        vItem->SetGeneration(item->Generation);
    }
}

TConfigItem::TPtr TConfigIndex::GetItem(ui64 id) const
{
    auto it = ConfigItems.find(id);
    if (it != ConfigItems.end())
        return it->second;
    return nullptr;
}

void TConfigIndex::AddItem(TConfigItem::TPtr item)
{
    Y_ABORT_UNLESS(!ConfigItems.contains(item->Id));
    ConfigItems.emplace(item->Id, item);

    if (!item->UsageScope.NodeIds.empty()) {
        for (auto nodeId : item->UsageScope.NodeIds)
            AddToIndex(item, nodeId, ConfigItemsByNodeId);
    } else if (!item->UsageScope.Hosts.empty()) {
        for (auto &host : item->UsageScope.Hosts)
            AddToIndex(item, host, ConfigItemsByHost);
    } else {
        if (item->UsageScope.Tenant)
            AddToIndex(item, item->UsageScope.Tenant, ConfigItemsByTenant);
        if (item->UsageScope.NodeType)
            AddToIndex(item, item->UsageScope.NodeType, ConfigItemsByNodeType);
        AddToIndex(item, TTenantAndNodeType(item->UsageScope.Tenant, item->UsageScope.NodeType),
                   ConfigItemsByTenantAndNodeType);
    }
    AddToIndex(item, item->Cookie, ConfigItemsByCookie);
}

void TConfigIndex::RemoveItem(ui64 id)
{
    auto item = GetItem(id);
    Y_ABORT_UNLESS(item);

    if (!item->UsageScope.NodeIds.empty()) {
        for (auto nodeId : item->UsageScope.NodeIds)
            RemoveFromIndex(item, nodeId, ConfigItemsByNodeId);
    } else if (!item->UsageScope.Hosts.empty()) {
        for (auto &host : item->UsageScope.Hosts)
            RemoveFromIndex(item, host, ConfigItemsByHost);
    } else {
        if (item->UsageScope.Tenant)
            RemoveFromIndex(item, item->UsageScope.Tenant, ConfigItemsByTenant);
        if (item->UsageScope.NodeType)
            RemoveFromIndex(item, item->UsageScope.NodeType, ConfigItemsByNodeType);
        RemoveFromIndex(item, TTenantAndNodeType(item->UsageScope.Tenant, item->UsageScope.NodeType),
                        ConfigItemsByTenantAndNodeType);
    }
    RemoveFromIndex(item, item->Cookie, ConfigItemsByCookie);

    ConfigItems.erase(id);
}

void TConfigIndex::Clear()
{
    ConfigItems.clear();
    ConfigItemsByNodeId.clear();
    ConfigItemsByHost.clear();
    ConfigItemsByTenant.clear();
    ConfigItemsByNodeType.clear();
    ConfigItemsByTenantAndNodeType.clear();
    ConfigItemsByCookie.clear();
}

void TConfigIndex::CollectItemsByScope(const NKikimrConsole::TUsageScope &scope,
                                       const THashSet<ui32> &kinds,
                                       TConfigItems &items) const
{
    TConfigItems candidates;

    switch (scope.GetFilterCase()) {
    case NKikimrConsole::TUsageScope::kNodeFilter:
        {
            auto &filter = scope.GetNodeFilter();
            if (filter.NodesSize())
                CollectItemsByNodeId(filter.GetNodes(0), kinds, candidates);
        }
        break;
    case NKikimrConsole::TUsageScope::kHostFilter:
        {
            auto &filter = scope.GetHostFilter();
            if (filter.HostsSize())
                CollectItemsByHost(filter.GetHosts(0), kinds, candidates);
        }
        break;
    case NKikimrConsole::TUsageScope::kTenantAndNodeTypeFilter:
        {
            auto &filter = scope.GetTenantAndNodeTypeFilter();
            CollectItemsByTenantAndNodeType(filter.GetTenant(), filter.GetNodeType(), kinds, candidates);
        }
        break;
    case NKikimrConsole::TUsageScope::FILTER_NOT_SET:
            CollectItemsByTenantAndNodeType("", "", kinds, candidates);
        break;
    default:
        Y_ABORT("unsupported filter case");
    }

    for (auto &item : candidates)
        if (item->UsageScope.IsSameScope(scope))
            items.insert(item);
}

void TConfigIndex::CollectItemsByScope(const TUsageScope &scope,
                                       const THashSet<ui32> &kinds,
                                       TConfigItems &items) const
{
    TConfigItems candidates;

    if (!scope.NodeIds.empty())
        CollectItemsByNodeId(*scope.NodeIds.begin(), kinds, candidates);
    else if (!scope.Hosts.empty())
        CollectItemsByHost(*scope.Hosts.begin(), kinds, candidates);
    else
        CollectItemsByTenantAndNodeType(scope.Tenant, scope.NodeType, kinds, candidates);

    for (auto &item : candidates)
        if (item->UsageScope == scope)
            items.insert(item);
}

void TConfigIndex::CollectItemsByConflictingScope(const TUsageScope &scope,
                                                  const THashSet<ui32> &kinds,
                                                  bool ignoreOrder,
                                                  TConfigItems &items) const
{
    TConfigItems candidates;

    if (!scope.NodeIds.empty()) {
        for (auto id : scope.NodeIds)
            CollectItemsByNodeId(id, kinds, candidates);
    } else if (!scope.Hosts.empty()) {
        for (auto &host : scope.Hosts)
            CollectItemsByHost(host, kinds, candidates);
    } else
        CollectItemsByTenantAndNodeType(scope.Tenant, scope.NodeType, kinds, candidates);

    if (ignoreOrder)
        items.insert(candidates.begin(), candidates.end());
    else {
        for (auto &item : candidates) {
            if (item->UsageScope.Order == scope.Order)
                items.insert(item);
        }
    }
}

void TConfigIndex::CollectItemsForNode(const NKikimrConsole::TNodeAttributes &attrs,
                                       const THashSet<ui32> &kinds,
                                       TConfigItems &items) const
{
    CollectItemsByNodeId(attrs.GetNodeId(), kinds, items);
    CollectItemsByHost(attrs.GetHost(), kinds, items);
    CollectItemsByTenantAndNodeType(attrs.GetTenant(), attrs.GetNodeType(), kinds, items);
    if (attrs.GetTenant())
        CollectItemsByTenantAndNodeType("", attrs.GetNodeType(), kinds, items);
    if (attrs.GetNodeType())
        CollectItemsByTenantAndNodeType(attrs.GetTenant(), "", kinds, items);
    if (attrs.GetTenant() && attrs.GetNodeType())
        CollectItemsByTenantAndNodeType("", "", kinds, items);
}

void TConfigIndex::CollectItemsFromMap(const TConfigItemsMap &map,
                                       const THashSet<ui32> &kinds,
                                       TConfigItems &items) const
{
    if (kinds.empty()) {
        for (auto &pr : map)
            items.insert(pr.second.begin(), pr.second.end());
    } else {
        for (auto kind : kinds) {
            auto it = map.find(kind);
            if (it != map.end())
                items.insert(it->second.begin(), it->second.end());
        }
    }
}

void TConfigIndex::CollectItemsFromMap(const TConfigItemsMap &map,
                                       const THashSet<ui32> &kinds,
                                       TOrderedConfigItemsMap &items) const
{
    if (kinds.empty()) {
        for (auto &pr : map)
            items[pr.first].insert(pr.second.begin(), pr.second.end());
    } else {
        for (auto kind : kinds) {
            auto it = map.find(kind);
            if (it != map.end())
                items[kind].insert(it->second.begin(), it->second.end());
        }
    }
}

void TConfigIndex::CollectItemsFromMap(const TConfigItemsMap &map,
                                       const TDynBitMap &kinds,
                                       TConfigItems &items) const
{
    if (kinds.Empty()) {
        for (auto &pr : map)
            items.insert(pr.second.begin(), pr.second.end());
    } else {
        Y_FOR_EACH_BIT(kind, kinds) {
            auto it = map.find(static_cast<ui32>(kind));
            if (it != map.end())
                items.insert(it->second.begin(), it->second.end());
        }
    }
}

void TConfigIndex::CollectItemsFromMap(const TConfigItemsMap &map,
                                       const TDynBitMap &kinds,
                                       TOrderedConfigItemsMap &items) const
{
    if (kinds.Empty()) {
        for (auto &pr : map)
            items[pr.first].insert(pr.second.begin(), pr.second.end());
    } else {
        Y_FOR_EACH_BIT(kind, kinds) {
            auto it = map.find(static_cast<ui32>(kind));
            if (it != map.end())
                items[kind].insert(it->second.begin(), it->second.end());
        }
    }
}

TDynBitMap TConfigIndex::ItemKindsWithDomainScope() const
{
    TDynBitMap result;
    auto it = ConfigItemsByTenantAndNodeType.find(TTenantAndNodeType(TString(), TString()));
    if (it != ConfigItemsByTenantAndNodeType.end()) {
        for (auto &pr : it->second)
            result.Set(pr.first);
    }
    return result;
}

TDynBitMap TConfigIndex::ItemKindsWithTenantScope() const
{
    TDynBitMap result;
    for (auto &pr1 : ConfigItemsByTenant) {
        for (auto &pr2 : pr1.second)
            result.Set(pr2.first);
    }
    return result;
}

TDynBitMap TConfigIndex::ItemKindsWithNodeTypeScope() const
{
    TDynBitMap result;
    for (auto &pr1 : ConfigItemsByNodeType) {
        for (auto &pr2 : pr1.second)
            result.Set(pr2.first);
    }
    return result;
}

void TConfigIndex::CollectTenantAndNodeTypeUsageScopes(const TDynBitMap &kinds,
                                                       THashSet<TString> &tenants,
                                                       THashSet<TString> &types,
                                                       THashSet<TTenantAndNodeType> &tenantAndNodeTypes) const
{
    for (auto &pr : ConfigItemsByTenantAndNodeType) {
        if (!kinds.Empty()) {
            bool found = false;
            Y_FOR_EACH_BIT(kind, kinds) {
                if (pr.second.contains(kind)) {
                    found = true;
                    break;
                }
            }
            if (!found)
                continue;
        }

        if (pr.first.Tenant && pr.first.NodeType)
            tenantAndNodeTypes.insert(pr.first);
        else if (pr.first.Tenant)
            tenants.insert(pr.first.Tenant);
        else if (pr.first.NodeType)
            types.insert(pr.first.NodeType);
    }
}

TScopedConfig::TPtr TConfigIndex::GetNodeConfig(const NKikimrConsole::TNodeAttributes &attrs,
                                                const THashSet<ui32> &kinds) const
{
    return BuildConfig(attrs.GetNodeId(), attrs.GetHost(), attrs.GetTenant(),
                       attrs.GetNodeType(), kinds);
}

void TConfigModifications::Clear()
{
    AddedItems.clear();
    ModifiedItems.clear();
    RemovedItems.clear();
}

bool TConfigModifications::IsEmpty() const
{
    return AddedItems.empty() && ModifiedItems.empty() && RemovedItems.empty();
}

void TConfigModifications::DeepCopyFrom(const TConfigModifications &other)
{
    for (auto item : other.AddedItems)
        AddedItems.push_back(new TConfigItem(*item));
    for (auto &pr : other.ModifiedItems)
        ModifiedItems.emplace(pr.first, new TConfigItem(*pr.second));
    RemovedItems = other.RemovedItems;
}

void TConfigModifications::ApplyTo(TConfigIndex &index) const
{
    for (auto &[id, _] : RemovedItems)
        index.RemoveItem(id);
    for (auto &pr : ModifiedItems)
        index.RemoveItem(pr.first);
    for (auto &pr : ModifiedItems)
        index.AddItem(pr.second);
    for (auto item : AddedItems)
        index.AddItem(item);
}

void TSubscription::Load(const NKikimrConsole::TSubscription &subscription)
{
    Id = subscription.GetId();
    if (subscription.GetSubscriber().HasTabletId())
        Subscriber.TabletId = subscription.GetSubscriber().GetTabletId();
    else if (subscription.GetSubscriber().HasServiceId())
        Subscriber.ServiceId = ActorIdFromProto(subscription.GetSubscriber().GetServiceId());
    NodeId = subscription.GetOptions().GetNodeId();
    Host = subscription.GetOptions().GetHost();
    Tenant = subscription.GetOptions().GetTenant();
    NodeType = subscription.GetOptions().GetNodeType();
    for (auto &kind : subscription.GetConfigItemKinds())
        ItemKinds.insert(kind);
}

void TSubscription::Serialize(NKikimrConsole::TSubscription &subscription)
{
    subscription.SetId(Id);
    Subscriber.Serialize(*subscription.MutableSubscriber());
    subscription.MutableOptions()->SetNodeId(NodeId);
    subscription.MutableOptions()->SetHost(Host);
    subscription.MutableOptions()->SetTenant(Tenant);
    subscription.MutableOptions()->SetNodeType(NodeType);
    for (auto &kind : ItemKinds)
        subscription.AddConfigItemKinds(kind);
}

bool TSubscription::IsEqual(const TSubscription &other) const
{
    return (Subscriber == other.Subscriber
            && NodeId == other.NodeId
            && Host == other.Host
            && Tenant == other.Tenant
            && NodeType == other.NodeType
            && ItemKinds == other.ItemKinds);
}

TString TSubscription::ToString() const
{
    TStringStream ss;
    ss << "id=" << Id
       << " tabletid=" << Subscriber.TabletId
       << " serviceid=" << Subscriber.ServiceId
       << " nodeid=" << NodeId
       << " host=" << Host
       << " tenant=" << Tenant
       << " nodetype=" << NodeType
       << " kinds=" << JoinSeq(",", ItemKinds)
       << " lastprovidedconfig=" << LastProvidedConfig.ToString();
    return ss.Str();
}

TSubscription::TPtr TSubscriptionIndex::GetSubscription(ui64 id) const
{
    auto it = Subscriptions.find(id);
    if (it != Subscriptions.end())
        return it->second;
    return nullptr;
}

const TSubscriptionSet &TSubscriptionIndex::GetSubscriptions(TSubscriberId id) const
{
    auto it = SubscriptionsBySubscriber.find(id);
    if (it != SubscriptionsBySubscriber.end())
        return it->second;
    return EmptySubscriptionsSet;
}

const THashMap<ui64, TSubscription::TPtr> &TSubscriptionIndex::GetSubscriptions() const
{
    return Subscriptions;
}

void TSubscriptionIndex::AddSubscription(TSubscription::TPtr subscription)
{
    Y_ABORT_UNLESS(!Subscriptions.contains(subscription->Id));
    Subscriptions.emplace(subscription->Id, subscription);
    SubscriptionsBySubscriber[subscription->Subscriber].insert(subscription);
    SubscriptionsByNodeId[subscription->NodeId].insert(subscription);
    SubscriptionsByHost[subscription->Host].insert(subscription);
    SubscriptionsByTenant[subscription->Tenant].insert(subscription);
    SubscriptionsByNodeType[subscription->NodeType].insert(subscription);
}

void TSubscriptionIndex::RemoveSubscription(ui64 id)
{
    auto subscription = GetSubscription(id);
    Y_ABORT_UNLESS(subscription);
    RemoveFromIndex(subscription, subscription->Subscriber, SubscriptionsBySubscriber);
    RemoveFromIndex(subscription, subscription->NodeId, SubscriptionsByNodeId);
    RemoveFromIndex(subscription, subscription->Host, SubscriptionsByHost);
    RemoveFromIndex(subscription, subscription->Tenant, SubscriptionsByTenant);
    RemoveFromIndex(subscription, subscription->NodeType, SubscriptionsByNodeType);
    Subscriptions.erase(id);
}

TInMemorySubscription::TPtr TInMemorySubscriptionIndex::GetSubscription(const TActorId &subscriber)
{
    auto it = Subscriptions.find(subscriber);
    if (it == Subscriptions.end())
        return nullptr;
    return it->second;
}

const THashMap<TActorId, TInMemorySubscription::TPtr> &TInMemorySubscriptionIndex::GetSubscriptions() const
{
    return Subscriptions;
}

const TInMemorySubscriptionSet *TInMemorySubscriptionIndex::GetSubscriptions(const TString& path) const
{
    if (auto it = SubscriptionsByTenant.find(path); it != SubscriptionsByTenant.end()) {
        return &(it->second);
    }

    return nullptr;
}

void TInMemorySubscriptionIndex::AddSubscription(TInMemorySubscription::TPtr subscription)
{
    Y_ABORT_UNLESS(!Subscriptions.contains(subscription->Subscriber));

    Subscriptions.emplace(subscription->Subscriber, subscription);

    SubscriptionsByNodeId[subscription->NodeId].insert(subscription);
    SubscriptionsByHost[subscription->Host].insert(subscription);
    SubscriptionsByTenant[subscription->Tenant].insert(subscription);
    SubscriptionsByNodeType[subscription->NodeType].insert(subscription);
}

void TInMemorySubscriptionIndex::RemoveSubscription(const TActorId &subscriber)
{
    auto it = Subscriptions.find(subscriber);
    Y_ABORT_UNLESS(it != Subscriptions.end());
    auto subscription = it->second;

    RemoveFromIndex(subscription, subscription->NodeId, SubscriptionsByNodeId);
    RemoveFromIndex(subscription, subscription->Host, SubscriptionsByHost);
    RemoveFromIndex(subscription, subscription->Tenant, SubscriptionsByTenant);
    RemoveFromIndex(subscription, subscription->NodeType, SubscriptionsByNodeType);

    Subscriptions.erase(it);
}

void TInMemorySubscriptionIndex::CollectAffectedSubscriptions(const TUsageScope &scope,
                                                      ui32 kind,
                                                      TInMemorySubscriptionSet &subscriptions) const
{
    if (subscriptions.size() == Subscriptions.size())
        return;

    if (scope.NodeIds.empty() && scope.Hosts.empty() && !scope.Tenant && !scope.NodeType) {
        for (auto &it : Subscriptions) {
            if (it.second->ItemKinds.contains(kind))
                subscriptions.insert(it.second);
        }

        return;
    }

    for (ui32 nodeId : scope.NodeIds) {
        auto it = SubscriptionsByNodeId.find(nodeId);
        if (it != SubscriptionsByNodeId.end()) {
            for (auto & subscription : it->second)
                if (subscription->ItemKinds.contains(kind))
                    subscriptions.insert(subscription);
        }
    }

    for (auto &host : scope.Hosts) {
        auto it = SubscriptionsByHost.find(host);
        if (it != SubscriptionsByHost.end()) {
            for (auto & subscription : it->second)
                if (subscription->ItemKinds.contains(kind))
                    subscriptions.insert(subscription);
        }
    }

    if (scope.Tenant) {
        auto it = SubscriptionsByTenant.find(scope.Tenant);
        if (it != SubscriptionsByTenant.end()) {
            for (auto & subscription : it->second)
                if (subscription->ItemKinds.contains(kind))
                    subscriptions.insert(subscription);
        }
    }

    if (scope.NodeType) {
        auto it = SubscriptionsByNodeType.find(scope.NodeType);
        if (it != SubscriptionsByNodeType.end()) {
            for (auto & subscription : it->second)
                if (subscription->ItemKinds.contains(kind))
                    subscriptions.insert(subscription);
        }
    }
}

void TSubscriptionIndex::CollectAffectedSubscriptions(const TUsageScope &scope,
                                                      ui32 kind,
                                                      TSubscriptionSet &subscriptions) const
{
    if (subscriptions.size() == Subscriptions.size())
        return;

    bool hasFilter = false;

    for (ui32 nodeId : scope.NodeIds) {
        auto it = SubscriptionsByNodeId.find(nodeId);
        if (it != SubscriptionsByNodeId.end()) {
            for (auto & subscription : it->second)
                if (subscription->ItemKinds.contains(kind))
                    subscriptions.insert(subscription);
        }
        hasFilter = true;
    }

    for (auto &host : scope.Hosts) {
        auto it = SubscriptionsByHost.find(host);
        if (it != SubscriptionsByHost.end()) {
            for (auto & subscription : it->second)
                if (subscription->ItemKinds.contains(kind))
                    subscriptions.insert(subscription);
        }
        hasFilter = true;
    }

    if (scope.Tenant) {
        auto it = SubscriptionsByTenant.find(scope.Tenant);
        if (it != SubscriptionsByTenant.end()) {
            for (auto & subscription : it->second)
                if (subscription->ItemKinds.contains(kind))
                    subscriptions.insert(subscription);
        }
        hasFilter = true;
    }

    if (scope.NodeType) {
        auto it = SubscriptionsByNodeType.find(scope.NodeType);
        if (it != SubscriptionsByNodeType.end()) {
            for (auto & subscription : it->second)
                if (subscription->ItemKinds.contains(kind))
                    subscriptions.insert(subscription);
        }
        hasFilter = true;
    }

    if (!hasFilter) {
        for (auto &pr : Subscriptions)
            subscriptions.insert(pr.second);
    }
}

void TSubscriptionIndex::Clear()
{
    Subscriptions.clear();
    SubscriptionsBySubscriber.clear();
}

void TSubscriptionModifications::Clear()
{
    RemovedSubscriptions.clear();
    AddedSubscriptions.clear();
    ModifiedLastProvided.clear();
    ModifiedCookies.clear();
}

bool TSubscriptionModifications::IsEmpty() const
{
    return (RemovedSubscriptions.empty()
            && AddedSubscriptions.empty()
            && ModifiedLastProvided.empty()
            && ModifiedCookies.empty());
}

void TSubscriptionModifications::DeepCopyFrom(const TSubscriptionModifications &other)
{
    RemovedSubscriptions = other.RemovedSubscriptions;
    for (auto &subscription : other.AddedSubscriptions)
        AddedSubscriptions.push_back(new TSubscription(*subscription));
    ModifiedLastProvided = other.ModifiedLastProvided;
    ModifiedCookies = other.ModifiedCookies;
}

} // namespace NKikimr::NConsole

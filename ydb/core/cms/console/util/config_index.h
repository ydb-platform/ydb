#pragma once

#include "defs.h"

#include <ydb/core/protos/console_config.pb.h>
#include <ydb/core/protos/config.pb.h>

#include <util/generic/bitmap.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/ptr.h>
#include <util/generic/set.h>
#include <util/stream/str.h>

namespace NKikimr::NConsole {

struct TTenantAndNodeType {
    TTenantAndNodeType() = default;
    TTenantAndNodeType(const TTenantAndNodeType &other) = default;
    TTenantAndNodeType(TTenantAndNodeType &&other) = default;

    TTenantAndNodeType(const TString &tenant,
                       const TString nodeType)
        : Tenant(tenant)
        , NodeType(nodeType)
    {
    }

    TTenantAndNodeType &operator=(const TTenantAndNodeType &other) = default;
    TTenantAndNodeType &operator=(TTenantAndNodeType &&other) = default;

    bool operator==(const NKikimr::NConsole::TTenantAndNodeType &other) const
    {
        return (Tenant == other.Tenant
                && NodeType == other.NodeType);
    };

    bool operator!=(const NKikimr::NConsole::TTenantAndNodeType &other) const
    {
        return !(*this == other);
    };

    TString Tenant;
    TString NodeType;
};

/**
 * Structure to describe configs subscriber which is
 * either tablet or service.
 */
struct TSubscriberId {
    TSubscriberId()
        : TabletId(0)
    {
    }

    TSubscriberId(const NKikimrConsole::TSubscriber &proto)
        : TabletId(0)
    {
        if (proto.HasTabletId())
            TabletId = proto.GetTabletId();
        else if (proto.HasServiceId())
            ServiceId = ActorIdFromProto(proto.GetServiceId());
    }

    void Serialize(NKikimrConsole::TSubscriber &proto) const
    {
        if (TabletId)
            proto.SetTabletId(TabletId);
        else
            ActorIdToProto(ServiceId, proto.MutableServiceId());
    }

    bool operator==(const TSubscriberId &other) const
    {
        return (TabletId == other.TabletId
                && ServiceId == other.ServiceId);
    }

    ui64 TabletId = 0;
    TActorId ServiceId;
};

} // namespace NKikimr::NConsole

template<>
struct THash<NKikimr::NConsole::TSubscriberId> {
    inline size_t operator()(const NKikimr::NConsole::TSubscriberId &id) const {
        auto pair = std::make_pair(id.TabletId, id.ServiceId);
        return THash<decltype(pair)>()(pair);
    }
};
/*
bool operator ==(const NKikimr::NConsole::TTenantAndNodeType &lhs,
                 const NKikimr::NConsole::TTenantAndNodeType &rhs)
{
    return (lhs.Tenant == rhs.Tenant
            && lhs.NodeType == rhs.NodeType);
};
*/
template<>
struct THash<NKikimr::NConsole::TTenantAndNodeType> {
    inline size_t operator()(const NKikimr::NConsole::TTenantAndNodeType &val) const {
        auto pair = std::make_pair(val.Tenant, val.NodeType);
        return THash<decltype(pair)>()(pair);
    }
};

namespace NKikimr::NConsole {

/**
 * Structre to hold config usage scope. For config items it is used similar
 * to NKikimrConsole::TUsageScope but also includes item's order.
 */
struct TUsageScope {
    THashSet<ui32> NodeIds;
    THashSet<TString> Hosts;
    TString Tenant;
    TString NodeType;
    ui32 Order;

    TUsageScope();
    TUsageScope(const TUsageScope &other) = default;
    TUsageScope& operator=(const TUsageScope &other) = default;
    TUsageScope(const NKikimrConsole::TUsageScope &scope, ui32 order = 0);

    bool operator==(const TUsageScope &rhs) const {
        return (NodeIds == rhs.NodeIds
                && Hosts == rhs.Hosts
                && Tenant == rhs.Tenant
                && NodeType == rhs.NodeType
                && Order == rhs.Order);
    }

    bool operator!=(const TUsageScope &rhs) const {
        return !(*this == rhs);
    }

    /**
     * Get scope priority basing on node filters (i.e. ignoring Order).
     * Lesser value means mor prioritized scope.
     */
    int GetPriority() const;

    void Serialize(NKikimrConsole::TUsageScope &scope) const;

    bool IsSameScope(const NKikimrConsole::TUsageScope &scope) const;

    bool HasConflict(const TUsageScope &other,
                     bool ignoreOrder = false) const;

    TString ToString() const;

    // Compare scope priorities.
    //   -1 - lhs is less prioritized
    //    0 - scopes has equal priorities
    //    1 - lhs is more prioritized
    static int ComparePriority(const TUsageScope &lhs,
                               const TUsageScope &rhs);

    bool operator<(const TUsageScope& other) const { return ComparePriority(*this, other) < 0; };
};

/**
 * Structure used to identify config. It holds ids and generations
 * of all config items used to build config.
 */
struct TConfigId {
    TConfigId()
    {
    }
    TConfigId(const NKikimrConsole::TConfigId &id)
    {
        Load(id);
    }
    TConfigId(const TConfigId &other) = default;
    TConfigId(TConfigId &&other) = default;

    TConfigId &operator=(const TConfigId &other) = default;
    TConfigId &operator=(TConfigId &&other) = default;

    void Load(const NKikimrConsole::TConfigId &id)
    {
        Clear();
        for (auto &item : id.GetItemIds())
            ItemIds.push_back(std::make_pair(item.GetId(), item.GetGeneration()));
    }

    void Serialize(NKikimrConsole::TConfigId &id) const
    {
        for (auto &pr : ItemIds) {
            auto &item = *id.AddItemIds();
            item.SetId(pr.first);
            item.SetGeneration(pr.second);
        }
    }

    void Clear()
    {
        ItemIds.clear();
    }

    bool operator==(const TConfigId &other)
    {
        return ItemIds == other.ItemIds;
    }

    bool operator!=(const TConfigId &other)
    {
        return !(*this == other);
    }

    TString ToString() const
    {
        TStringStream ss;
        for (size_t i = 0; i < ItemIds.size(); ++i) {
            if (i)
                ss << ", ";
            ss << ItemIds[i].first << "." << ItemIds[i].second;
        }
        return ss.Str();
    }

    TVector<std::pair<ui64, ui64>> ItemIds;
};

/**
 * Cluster config atom wich holds a part of config. Multiple
 * config items are merged to build config.
 */
struct TConfigItem : public TThrRefBase {
    using TPtr = TIntrusivePtr<TConfigItem>;

    TConfigItem();
    TConfigItem(const TConfigItem &other) = default;
    TConfigItem(const NKikimrConsole::TConfigItem &item);

    void Serialize(NKikimrConsole::TConfigItem &item) const;

    static TString KindName(ui32 kind);
    TString KindName() const;

    static TString MergeStrategyName(ui32 merge);
    TString MergeStrategyName() const;

    TString ToString() const;

    ui64 Id;
    ui64 Generation;
    ui32 Kind;
    TUsageScope UsageScope;
    ui32 MergeStrategy;
    NKikimrConfig::TAppConfig Config;
    TString Cookie;
};

struct TConfigItemPriorityLess {
    bool operator()(const TConfigItem::TPtr &lhs,
                    const TConfigItem::TPtr &rhs) const
    {
        return TUsageScope::ComparePriority(lhs->UsageScope, rhs->UsageScope) < 0;
    }
};

void ClearOverwrittenRepeated(::google::protobuf::Message &to,
                              ::google::protobuf::Message &from);
void MergeMessageOverwriteRepeated(::google::protobuf::Message &to,
                                   ::google::protobuf::Message &from);

// Generic set of config items.
using TConfigItems = THashSet<TConfigItem::TPtr, TPtrHash>;
// Config items grouped by kind.
using TConfigItemsMap = THashMap<ui32, TConfigItems>;
// Used to store config items of the same kind ordered by priority.
using TOrderedConfigItems = TSet<TConfigItem::TPtr, TConfigItemPriorityLess>;
// Ordered config items grouped by kind.
using TOrderedConfigItemsMap = THashMap<ui32, TOrderedConfigItems>;

/**
 * Collection of items with the same or intersecting scopes.
 * This is built to compute config for particular node.
 */
class TScopedConfig : public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<TScopedConfig>;

    // Merge all config items into resulting config.
    void ComputeConfig(NKikimrConfig::TAppConfig &config, bool addVersion = false) const;

    // Merge config items with given kinds into resulting config.
    void ComputeConfig(const THashSet<ui32> &kinds, NKikimrConfig::TAppConfig &config, bool addVersion = false) const;

private:
    void MergeItems(const TOrderedConfigItems &items,
                    NKikimrConfig::TAppConfig &config, bool addVersion) const;
    void MergeItem(TConfigItem::TPtr item,
                   NKikimrConfig::TAppConfig &config, bool addVersion) const;

public:
    // Here all config items included into the scope are listed.
    // Sorted by priority in ascending order.
    TOrderedConfigItemsMap ConfigItems;
};

class TConfigIndex {
public:
    TConfigItem::TPtr GetItem(ui64 id) const;
    void AddItem(TConfigItem::TPtr item);
    void RemoveItem(ui64 id);

    const THashMap<ui64, TConfigItem::TPtr> &GetConfigItems() const
    {
        return ConfigItems;
    }

    void Clear();

    template <typename TIndexMap, typename TKey, typename TItemsStorage, typename TKinds>
    void CollectItemsByIndex(const TKey &key,
                             const TIndexMap &index,
                             const TKinds &kinds,
                             TItemsStorage &items) const
    {
        auto it = index.find(key);
        if (it != index.end())
            CollectItemsFromMap(it->second, kinds, items);
    }

    template <typename TItemsStorage, typename TKinds>
    void CollectItemsByNodeId(ui32 nodeId,
                              const TKinds &kinds,
                              TItemsStorage &items) const
    {
        CollectItemsByIndex(nodeId, ConfigItemsByNodeId, kinds, items);
    }

    template <typename TItemsStorage, typename TKinds>
    void CollectItemsByHost(const TString &host,
                            const TKinds &kinds,
                            TItemsStorage &items) const
    {
        CollectItemsByIndex(host, ConfigItemsByHost, kinds, items);
    }

    template <typename TItemsStorage, typename TKinds>
    void CollectItemsByTenant(const TString &tenant,
                              const TKinds &kinds,
                              TItemsStorage &items) const
    {
        CollectItemsByIndex(tenant, ConfigItemsByTenant, kinds, items);
    }

    template <typename TItemsStorage, typename TKinds>
    void CollectItemsByNodeType(const TString &type,
                                const TKinds &kinds,
                                TItemsStorage &items) const
    {
        CollectItemsByIndex(type, ConfigItemsByNodeType, kinds, items);
    }

    template <typename TItemsStorage, typename TKinds>
    void CollectItemsByTenantAndNodeType(const TString &tenant,
                                         const TString &type,
                                         const TKinds &kinds,
                                         TItemsStorage &items) const
    {
        CollectItemsByIndex(TTenantAndNodeType(tenant, type), ConfigItemsByTenantAndNodeType, kinds, items);
    }

    template <typename TItemsStorage>
    void CollectItemsByCookie(const TString &cookie,
                              const THashSet<ui32> &kinds,
                              TItemsStorage &items) const
    {
        CollectItemsByIndex(cookie, ConfigItemsByCookie, kinds, items);
    }

    template <typename TItemsStorage>
    void CollectItemsWithNodeIdScope(const THashSet<ui32> &kinds,
                                     TItemsStorage &items) const
    {
        for (auto &pr : ConfigItemsByNodeId)
            CollectItemsFromMap(pr.second, kinds, items);
    }

    template <typename TItemsStorage>
    void CollectItemsWithHostScope(const THashSet<ui32> &kinds,
                                   TItemsStorage &items) const
    {
        for (auto &pr : ConfigItemsByHost)
            CollectItemsFromMap(pr.second, kinds, items);
    }

    template <typename TItemsStorage>
    void CollectItemsWithTenantScope(const THashSet<ui32> &kinds,
                                     TItemsStorage &items) const
    {
        for (auto &pr : ConfigItemsByTenant)
            CollectItemsFromMap(pr.second, kinds, items);
    }

    template <typename TItemsStorage>
    void CollectItemsWithPureTenantScope(const THashSet<ui32> &kinds,
                                         TItemsStorage &items) const
    {
        for (auto &pr : ConfigItemsByTenant)
            CollectItemsByTenantAndNodeType(pr.first, "", kinds, items);
    }

    template <typename TItemsStorage>
    void CollectItemsWithNodeTypeScope(const THashSet<ui32> &kinds,
                                       TItemsStorage &items) const
    {
        for (auto &pr : ConfigItemsByNodeType)
            CollectItemsFromMap(pr.second, kinds, items);
    }

    template <typename TItemsStorage>
    void CollectItemsWithTenantAndNodeTypeScope(const THashSet<ui32> &kinds,
                                                TItemsStorage &items) const
    {
        for (auto &pr : ConfigItemsByTenantAndNodeType)
            if (pr.first.Tenant && pr.first.NodeType)
                CollectItemsFromMap(pr.second, kinds, items);
    }

    void CollectItemsByScope(const NKikimrConsole::TUsageScope &scope,
                             const THashSet<ui32> &kinds,
                             TConfigItems &items) const;
    void CollectItemsByScope(const TUsageScope &scope,
                             const THashSet<ui32> &kinds,
                             TConfigItems &items) const;
    void CollectItemsByConflictingScope(const TUsageScope &scope,
                                        const THashSet<ui32> &kinds,
                                        bool ignoreOrder,
                                        TConfigItems &items) const;
    void CollectItemsForNode(const NKikimrConsole::TNodeAttributes &attrs,
                             const THashSet<ui32> &kinds,
                             TConfigItems &items) const;

    TScopedConfig::TPtr GetNodeConfig(const NKikimrConsole::TNodeAttributes &attrs,
                                      const THashSet<ui32> &kinds) const;

    template <typename TKinds>
    TScopedConfig::TPtr BuildConfig(ui32 nodeId,
                                    const TString &host,
                                    const TString &tenant,
                                    const TString &nodeType,
                                    const TKinds &kinds) const
    {
        TScopedConfig::TPtr config = new TScopedConfig;
        CollectItemsByNodeId(nodeId, kinds, config->ConfigItems);
        CollectItemsByHost(host, kinds, config->ConfigItems);
        CollectItemsByTenantAndNodeType(tenant, nodeType, kinds, config->ConfigItems);
        if (tenant)
            CollectItemsByTenantAndNodeType("", nodeType, kinds, config->ConfigItems);
        if (nodeType)
            CollectItemsByTenantAndNodeType(tenant, "", kinds, config->ConfigItems);
        if (tenant && nodeType)
            CollectItemsByTenantAndNodeType("", "", kinds, config->ConfigItems);
        return config;
    }

    void CollectItemsFromMap(const TConfigItemsMap &map,
                             const THashSet<ui32> &kinds,
                             TConfigItems &items) const;

    void CollectItemsFromMap(const TConfigItemsMap &map,
                             const THashSet<ui32> &kinds,
                             TOrderedConfigItemsMap &items) const;

    void CollectItemsFromMap(const TConfigItemsMap &map,
                             const TDynBitMap &kinds,
                             TConfigItems &items) const;

    void CollectItemsFromMap(const TConfigItemsMap &map,
                             const TDynBitMap &kinds,
                             TOrderedConfigItemsMap &items) const;

    TDynBitMap ItemKindsWithDomainScope() const;
    TDynBitMap ItemKindsWithTenantScope() const;
    TDynBitMap ItemKindsWithNodeTypeScope() const;

    void CollectTenantAndNodeTypeUsageScopes(const TDynBitMap &kinds,
                                             THashSet<TString> &tenants,
                                             THashSet<TString> &types,
                                             THashSet<TTenantAndNodeType> &tenantAndNodeTypes) const;

    bool IsEmpty() const
    {
        return ConfigItems.empty();
    }

private:
    template <typename TIndexMap, typename TKey, bool forced = true>
    void AddToIndex(TConfigItem::TPtr item,
                    const TKey &key,
                    TIndexMap &map)
    {
        auto &set = map[key][item->Kind];
        Y_ABORT_UNLESS(!forced || !set.contains(item));
        set.insert(item);
    }

    template <typename TIndexMap, typename TKey, bool forced = true>
    void RemoveFromIndex(TConfigItem::TPtr item,
                         const TKey &key,
                         TIndexMap &map)
    {
        auto mapIt = map.find(key);
        if (mapIt == map.end()) {
            Y_ABORT_UNLESS(!forced);
            return;
        }
        auto kindIt = mapIt->second.find(item->Kind);
        if (kindIt == mapIt->second.end()) {
            Y_ABORT_UNLESS(!forced);
            return;
        }
        auto itemIt = kindIt->second.find(item);
        if (itemIt == kindIt->second.end()) {
            Y_ABORT_UNLESS(!forced);
            return;
        }
        kindIt->second.erase(itemIt);
        if (kindIt->second.empty()) {
            mapIt->second.erase(kindIt);
            if (mapIt->second.empty())
                map.erase(mapIt);
        }
    }

    THashMap<ui64, TConfigItem::TPtr> ConfigItems;
    THashMap<ui32, TConfigItemsMap> ConfigItemsByNodeId;
    THashMap<TString, TConfigItemsMap> ConfigItemsByHost;
    THashMap<TString, TConfigItemsMap> ConfigItemsByTenant;
    THashMap<TString, TConfigItemsMap> ConfigItemsByNodeType;
    THashMap<TTenantAndNodeType, TConfigItemsMap> ConfigItemsByTenantAndNodeType;
    THashMap<TString, TConfigItemsMap> ConfigItemsByCookie;
};

struct TConfigModifications {
    void Clear();
    bool IsEmpty() const;
    void DeepCopyFrom(const TConfigModifications &other);

    void ApplyTo(TConfigIndex &index) const;

    TVector<TConfigItem::TPtr> AddedItems;
    THashMap<ui64, TConfigItem::TPtr> ModifiedItems;
    THashMap<ui64, TConfigItem::TPtr> RemovedItems;
};

struct TSubscription : public TThrRefBase {
    using TPtr = TIntrusivePtr<TSubscription>;

    TSubscription()
        : Id(0)
        , NodeId(0)
        , Cookie(0)
    {
    }

    TSubscription(const NKikimrConsole::TSubscription &subscription)
        : Cookie(0)
    {
        Load(subscription);
    }

    TSubscription(const TSubscription &other) = default;
    TSubscription(TSubscription &&other) = default;

    TSubscription &operator=(const TSubscription &other) = default;
    TSubscription &operator=(TSubscription &&other) = default;

    void Load(const NKikimrConsole::TSubscription &subscription);
    void Serialize(NKikimrConsole::TSubscription &subscription);

    // LastProvidedConfig and Id are ignored in comparison.
    bool IsEqual(const TSubscription &other) const;

    TString ToString() const;

    ui64 Id;
    TSubscriberId Subscriber;
    ui32 NodeId;
    TString Host;
    TString Tenant;
    TString NodeType;
    THashSet<ui32> ItemKinds;
    TConfigId LastProvidedConfig;
    TConfigId CurrentConfigId;
    NKikimrConfig::TAppConfig CurrentConfig;
    TActorId Worker;
    // Cookie allows to identify whether TEvConfigNotificationResponse
    // still holds actual data. If Cookie doesn't match the one carried
    // in an event then this event is ignored. Cookie is defined when
    // subscription is created/loaded and modified only for service
    // clients which renew subscription with TEvReplaceSubscriptionRequest.
    // Thusly TEvConfigNotificationResponse is ignored for died service
    // but not ignored for died tablets which are assumed to preserve their
    // configs.
    ui64 Cookie;

    TString YamlConfig;
    TMap<ui64, TString> VolatileYamlConfigs;
};

using TSubscriptionSet = THashSet<TSubscription::TPtr, TPtrHash>;

class TSubscriptionIndex {
public:
    TSubscription::TPtr GetSubscription(ui64 id) const;
    const TSubscriptionSet &GetSubscriptions(TSubscriberId id) const;
    const THashMap<ui64, TSubscription::TPtr> &GetSubscriptions() const;

    void AddSubscription(TSubscription::TPtr subscription);
    void RemoveSubscription(ui64 id);

    void CollectAffectedSubscriptions(const TUsageScope &scope,
                                      ui32 kind,
                                      TSubscriptionSet &subscriptions) const;

    bool IsEmpty() const
    {
        return Subscriptions.empty();
    }

    void Clear();

private:
    template <typename TKey>
    void RemoveFromIndex(TSubscription::TPtr subscription,
                         const TKey &key,
                         THashMap<TKey, TSubscriptionSet> &index)
    {
        auto it = index.find(key);
        Y_ABORT_UNLESS(it != index.end());
        Y_ABORT_UNLESS(it->second.contains(subscription));
        it->second.erase(subscription);
        if (it->second.empty())
            index.erase(it);
    }

    THashMap<ui64, TSubscription::TPtr> Subscriptions;
    THashMap<TSubscriberId, TSubscriptionSet> SubscriptionsBySubscriber;
    THashMap<ui32, TSubscriptionSet> SubscriptionsByNodeId;
    THashMap<TString, TSubscriptionSet> SubscriptionsByHost;
    THashMap<TString, TSubscriptionSet> SubscriptionsByTenant;
    THashMap<TString, TSubscriptionSet> SubscriptionsByNodeType;
    TSubscriptionSet EmptySubscriptionsSet;
};

struct TInMemorySubscription : public TThrRefBase {
    using TPtr = TIntrusivePtr<TInMemorySubscription>;

    TActorId Subscriber;
    ui64 Generation;

    ui32 NodeId;
    TString Host;
    TString Tenant;
    TString NodeType;
    THashSet<ui32> ItemKinds;

    NKikimrConfig::TConfigVersion LastProvided;

    bool ServeYaml = false;
    ui64 YamlApiVersion = 0;
    ui64 YamlConfigVersion = 0;
    TMap<ui64, ui64> VolatileYamlConfigHashes;

    bool FirstUpdateSent = false;

    TActorId Worker;
};

using TInMemorySubscriptionSet = THashSet<TInMemorySubscription::TPtr, TPtrHash>;

class TInMemorySubscriptionIndex {
public:
    void CollectAffectedSubscriptions(const TUsageScope &scope, ui32 kind, TInMemorySubscriptionSet &out) const;

    TInMemorySubscription::TPtr GetSubscription(const TActorId &subscriber);

    const THashMap<TActorId, TInMemorySubscription::TPtr> &GetSubscriptions() const;

    void AddSubscription(TInMemorySubscription::TPtr subscription);

    void RemoveSubscription(const TActorId &subscriber);

private:
    template <typename TKey>
    void RemoveFromIndex(TInMemorySubscription::TPtr subscription,
                         const TKey &key,
                         THashMap<TKey, TInMemorySubscriptionSet> &index)
    {
        auto it = index.find(key);
        Y_ABORT_UNLESS(it != index.end());
        Y_ABORT_UNLESS(it->second.contains(subscription));
        it->second.erase(subscription);
        if (it->second.empty())
            index.erase(it);
    }

    THashMap<TActorId, TInMemorySubscription::TPtr> Subscriptions;

    THashMap<ui32, TInMemorySubscriptionSet> SubscriptionsByNodeId;
    THashMap<TString, TInMemorySubscriptionSet> SubscriptionsByHost;
    THashMap<TString, TInMemorySubscriptionSet> SubscriptionsByTenant;
    THashMap<TString, TInMemorySubscriptionSet> SubscriptionsByNodeType;
};

struct TSubscriptionModifications {
    TSubscriptionModifications() = default;
    TSubscriptionModifications(const TSubscriptionModifications &other) = default;

    void Clear();
    bool IsEmpty() const;
    void DeepCopyFrom(const TSubscriptionModifications &other);

    THashSet<ui64> RemovedSubscriptions;
    TVector<TSubscription::TPtr> AddedSubscriptions;
    THashMap<ui64, TConfigId> ModifiedLastProvided;
    THashMap<ui64, ui64> ModifiedCookies;
};

} // namespace NKikimr::NConsole

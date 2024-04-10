#include "ut_helpers.h"
#include "console_configs_manager.h"
#include "console_configs_subscriber.h"

#include <ydb/core/base/counters.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/base/path.h>
#include <ydb/core/cms/console/validators/registry.h>
#include <ydb/core/tablet/bootstrapper.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/testlib/tablet_helpers.h>

#include <ydb/library/actors/interconnect/interconnect_impl.h>

#include <ydb/core/testlib/tenant_runtime.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/system/hostname.h>

namespace NKikimr {

using namespace NConsole;
using namespace NConsole::NUT;

namespace {

TTenantTestConfig::TTenantPoolConfig DefaultTenantPoolConfig()
{
    TTenantTestConfig::TTenantPoolConfig res = {
        // Static slots {tenant, {cpu, memory, network}}
        {{ {DOMAIN1_NAME, {1, 1, 1}} }},
        // NodeType
        "type1"
    };
    return res;
}

TTenantTestConfig::TTenantPoolConfig TenantTenantPoolConfig()
{
    TTenantTestConfig::TTenantPoolConfig res = {
        // Static slots {tenant, {cpu, memory, network}}
        {{ {TENANT1_1_NAME, {1, 1, 1}} }},
        // NodeType
        "type1"
    };
    return res;
}

TTenantTestConfig::TTenantPoolConfig MultipleTenantsTenantPoolConfig()
{
    TTenantTestConfig::TTenantPoolConfig res = {
        // Static slots {tenant, {cpu, memory, network}}
        {{ {TENANT1_1_NAME, {1, 1, 1}},
           {TENANT1_2_NAME, {1, 1, 1}} }},
        // NodeType
        "type1"
    };
    return res;
}

TTenantTestConfig DefaultConsoleTestConfig()
{
    TTenantTestConfig res = {
        // Domains {name, schemeshard {{ subdomain_names }}}
        {{ {DOMAIN1_NAME, SCHEME_SHARD1_ID, TVector<TString>()} }},
        // HiveId
        HIVE_ID,
        // FakeTenantSlotBroker
        true,
        // FakeSchemeShard
        false,
        // CreateConsole
        true,
        // Nodes {tenant_pool_config, data_center}
        {{
                {DefaultTenantPoolConfig()},
        }},
        // DataCenterCount
        1
    };
    return res;
}

TTenantTestConfig MultipleNodesConsoleTestConfig()
{
    TTenantTestConfig res = {
        // Domains {name, schemeshard {{ subdomain_names }}}
        {{ {DOMAIN1_NAME, SCHEME_SHARD1_ID, TVector<TString>()} }},
        // HiveId
        HIVE_ID,
        // FakeTenantSlotBroker
        true,
        // FakeSchemeShard
        false,
        // CreateConsole
        true,
        // Nodes {tenant_pool_config, data_center}
        {{
            {DefaultTenantPoolConfig()},
        },{
            {DefaultTenantPoolConfig()},
        }},
        // DataCenterCount
        1
    };
    return res;
}

TTenantTestConfig TenantConsoleTestConfig()
{
    TTenantTestConfig res = {
        // Domains {name, schemeshard {{ subdomain_names }}}
        {{ {DOMAIN1_NAME, SCHEME_SHARD1_ID, {{ TENANT1_1_NAME }}} }},
        // HiveId
        HIVE_ID,
        // FakeTenantSlotBroker
        true,
        // FakeSchemeShard
        true,
        // CreateConsole
        true,
        // Nodes {tenant_pool_config, data_center}
        {{
                {TenantTenantPoolConfig()},
        }},
        // DataCenterCount
        1
    };
    return res;
}

TTenantTestConfig MultipleTenantsConsoleTestConfig()
{
    TTenantTestConfig res = {
        // Domains {name, schemeshard {{ subdomain_names }}}
        {{ {DOMAIN1_NAME, SCHEME_SHARD1_ID, {{ TENANT1_1_NAME, TENANT1_2_NAME }}} }},
        // HiveId
        HIVE_ID,
        // FakeTenantSlotBroker
        true,
        // FakeSchemeShard
        true,
        // CreateConsole
        true,
        // Nodes {tenant_pool_config, data_center}
        {{
                {MultipleTenantsTenantPoolConfig()},
        }},
        // DataCenterCount
        1
    };
    return res;
}

template <typename TRequestEvent, typename TResponseEvent>
void CheckGetItems(TTenantTestRuntime &runtime,
                   THashMap<ui64, TConfigItem::TPtr> items,
                   const typename TRequestEvent::ProtoRecordType &request)
{
    auto *event = new TRequestEvent;
    event->Record.CopyFrom(request);

    TAutoPtr<IEventHandle> handle;
    runtime.SendToConsole(event);
    auto reply = runtime.GrabEdgeEventRethrow<TResponseEvent>(handle);
    UNIT_ASSERT_VALUES_EQUAL(reply->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);

    for (auto &item : reply->Record.GetConfigItems()) {
        auto it = items.find(item.GetId().GetId());
        UNIT_ASSERT(it != items.end());
        UNIT_ASSERT_VALUES_EQUAL(item.GetId().GetGeneration(), it->second->Generation);
        UNIT_ASSERT_VALUES_EQUAL(item.GetKind(), it->second->Kind);
        UNIT_ASSERT(TUsageScope(item.GetUsageScope(), item.GetOrder()) == it->second->UsageScope);
        UNIT_ASSERT_VALUES_EQUAL(item.GetMergeStrategy(), it->second->MergeStrategy);
        UNIT_ASSERT_VALUES_EQUAL(item.GetConfig().ShortDebugString(), it->second->Config.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(item.GetCookie(), it->second->Cookie);
        items.erase(it);
    }
    UNIT_ASSERT(items.empty());
}

void CheckGetItems(TTenantTestRuntime &runtime,
                   THashMap<ui64, TConfigItem::TPtr> items,
                   const NKikimrConsole::TGetConfigItemsRequest &request)
{
    CheckGetItems<TEvConsole::TEvGetConfigItemsRequest, TEvConsole::TEvGetConfigItemsResponse>
        (runtime, items, request);
}

void CheckGetItems(TTenantTestRuntime &runtime,
                   THashMap<ui64, TConfigItem::TPtr> items,
                   const NKikimrConsole::TGetNodeConfigItemsRequest &request)
{
    CheckGetItems<TEvConsole::TEvGetNodeConfigItemsRequest, TEvConsole::TEvGetNodeConfigItemsResponse>
        (runtime, items, request);
}

void CollectItems(THashMap<ui64, TConfigItem::TPtr> &)
{
}

void CollectItems(THashMap<ui64, TConfigItem::TPtr> &items,
                  const NKikimrConsole::TConfigItem &item)
{
    Y_ABORT_UNLESS(!items.contains(item.GetId().GetId()));
    items.emplace(item.GetId().GetId(), new TConfigItem(item));
}

template <typename ...Ts>
void CollectItems(THashMap<ui64, TConfigItem::TPtr> &items,
                  const NKikimrConsole::TConfigItem &item, Ts... args)
{
    CollectItems(items, item);
    CollectItems(items, args...);
}

void CollectKinds(NKikimrConsole::TGetConfigItemsRequest &request, const TVector<ui32> &kinds)
{
    for (auto &kind : kinds)
        request.AddItemKinds(kind);
}

template <typename ...Ts>
void CheckGetItemsById(TTenantTestRuntime &runtime, const TVector<ui64> &ids, const TVector<ui32> &kinds, Ts... args)
{
    THashMap<ui64, TConfigItem::TPtr> items;
    CollectItems(items, args...);


    NKikimrConsole::TGetConfigItemsRequest request;
    for (auto id : ids)
        request.AddItemIds(id);
    CollectKinds(request, kinds);
    CheckGetItems(runtime, items, request);
}

template <typename ...Ts>
void CheckGetItemsByNodeId(TTenantTestRuntime &runtime, const TVector<ui32> &nodes, const TVector<ui32> &kinds, Ts... args)
{
    THashMap<ui64, TConfigItem::TPtr> items;
    CollectItems(items, args...);

    NKikimrConsole::TGetConfigItemsRequest request;
    request.MutableNodeFilter();
    for (auto node : nodes)
        request.MutableNodeFilter()->AddNodes(node);
    CollectKinds(request, kinds);
    CheckGetItems(runtime, items, request);
}

template <typename ...Ts>
void CheckGetItemsByHost(TTenantTestRuntime &runtime, const TVector<TString> &hosts, const TVector<ui32> &kinds, Ts... args)
{
    THashMap<ui64, TConfigItem::TPtr> items;
    CollectItems(items, args...);

    NKikimrConsole::TGetConfigItemsRequest request;
    request.MutableHostFilter();
    for (auto &host : hosts)
        request.MutableHostFilter()->AddHosts(host);
    CollectKinds(request, kinds);
    CheckGetItems(runtime, items, request);
}

template <typename ...Ts>
void CheckGetItemsByTenant(TTenantTestRuntime &runtime, const TVector<TString> &tenants, const TVector<ui32> &kinds, Ts... args)
{
    THashMap<ui64, TConfigItem::TPtr> items;
    CollectItems(items, args...);

    NKikimrConsole::TGetConfigItemsRequest request;
    request.MutableTenantFilter();
    for (auto &tenant : tenants)
        request.MutableTenantFilter()->AddTenants(tenant);
    CollectKinds(request, kinds);
    CheckGetItems(runtime, items, request);
}

template <typename ...Ts>
void CheckGetItemsByNodeType(TTenantTestRuntime &runtime, const TVector<TString> &types, const TVector<ui32> &kinds, Ts... args)
{
    THashMap<ui64, TConfigItem::TPtr> items;
    CollectItems(items, args...);

    NKikimrConsole::TGetConfigItemsRequest request;
    request.MutableNodeTypeFilter();
    for (auto &type : types)
        request.MutableNodeTypeFilter()->AddNodeTypes(type);
    CollectKinds(request, kinds);
    CheckGetItems(runtime, items, request);
}

template <typename ...Ts>
void CheckGetItemsByTenantAndNodeType(TTenantTestRuntime &runtime, const TVector<std::pair<TString, TString>> &filters,
                                      const TVector<ui32> &kinds, Ts... args)
{
    THashMap<ui64, TConfigItem::TPtr> items;
    CollectItems(items, args...);

    NKikimrConsole::TGetConfigItemsRequest request;
    request.MutableTenantAndNodeTypeFilter();
    for (auto &pr : filters) {
        auto &rec = *request.MutableTenantAndNodeTypeFilter()->AddTenantAndNodeTypes();
        rec.SetTenant(pr.first);
        rec.SetNodeType(pr.second);
    }
    CollectKinds(request, kinds);
    CheckGetItems(runtime, items, request);
}

template <typename ...Ts>
void CheckGetItemsByUsageScope(TTenantTestRuntime &runtime, const TVector<NKikimrConsole::TUsageScope> &scopes,
                               const TVector<ui32> &kinds, Ts... args)
{
    THashMap<ui64, TConfigItem::TPtr> items;
    CollectItems(items, args...);

    NKikimrConsole::TGetConfigItemsRequest request;
    for (auto &scope : scopes)
        request.AddUsageScopes()->CopyFrom(scope);
    CollectKinds(request, kinds);
    CheckGetItems(runtime, items, request);
}

template <typename ...Ts>
void CheckGetItemsByUsageScope(TTenantTestRuntime &runtime, const NKikimrConsole::TUsageScope &scope,
                               const TVector<ui32> &kinds, Ts... args)
{
    CheckGetItemsByUsageScope(runtime, TVector<NKikimrConsole::TUsageScope>({scope}), kinds, args...);
}

template <typename ...Ts>
void CheckGetItemsByCookie(TTenantTestRuntime &runtime, const TVector<TString> &cookies, const TVector<ui32> &kinds, Ts... args)
{
    THashMap<ui64, TConfigItem::TPtr> items;
    CollectItems(items, args...);

    NKikimrConsole::TGetConfigItemsRequest request;
    request.MutableCookieFilter();
    for (auto &cookie : cookies)
        request.MutableCookieFilter()->AddCookies(cookie);
    CollectKinds(request, kinds);
    CheckGetItems(runtime, items, request);
}

template <typename ...Ts>
void CheckGetItems(TTenantTestRuntime &runtime, const TVector<ui32> &kinds, Ts... args)
{
    THashMap<ui64, TConfigItem::TPtr> items;
    CollectItems(items, args...);

    NKikimrConsole::TGetConfigItemsRequest request;
    CollectKinds(request, kinds);
    CheckGetItems(runtime, items, request);
}

template <typename ...Ts>
void CheckGetNodeItems(TTenantTestRuntime &runtime, ui32 nodeId, const TString &host,
                       const TString &tenant, const TString &type, const TVector<ui32> &kinds, Ts... args)
{
    THashMap<ui64, TConfigItem::TPtr> items;
    CollectItems(items, args...);

    NKikimrConsole::TGetNodeConfigItemsRequest request;
    request.MutableNode()->SetNodeId(nodeId);
    request.MutableNode()->SetHost(host);
    request.MutableNode()->SetTenant(tenant);
    request.MutableNode()->SetNodeType(type);
    for (auto &kind : kinds)
        request.AddItemKinds(kind);
    CheckGetItems(runtime, items, request);
}

void CheckGetNodeConfig(TTenantTestRuntime &runtime, ui32 nodeId, const TString &host,
                        const TString &tenant, const TString &type,
                        const NKikimrConfig::TAppConfig &config,
                        const TVector<ui32> &kinds = TVector<ui32>())
{
    auto *event = new TEvConsole::TEvGetNodeConfigRequest;
    event->Record.MutableNode()->SetNodeId(nodeId);
    event->Record.MutableNode()->SetHost(host);
    event->Record.MutableNode()->SetTenant(tenant);
    event->Record.MutableNode()->SetNodeType(type);
    for (auto &kind : kinds)
        event->Record.AddItemKinds(kind);

    TAutoPtr<IEventHandle> handle;
    runtime.SendToConsole(event);
    auto reply = runtime.GrabEdgeEventRethrow<TEvConsole::TEvGetNodeConfigResponse>(handle);
    UNIT_ASSERT_VALUES_EQUAL(reply->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
    CheckEqualsIgnoringVersion(reply->Record.GetConfig(), config);
}

void CheckGetNodeConfig(TTenantTestRuntime &runtime,
                        ui32 nodeId,
                        const TString &host,
                        const TString &tenant,
                        const TString &type,
                        Ydb::StatusIds::StatusCode code)
{
    auto *event = new TEvConsole::TEvGetNodeConfigRequest;
    event->Record.MutableNode()->SetNodeId(nodeId);
    event->Record.MutableNode()->SetHost(host);
    event->Record.MutableNode()->SetTenant(tenant);
    event->Record.MutableNode()->SetNodeType(type);

    TAutoPtr<IEventHandle> handle;
    runtime.SendToConsole(event);
    auto reply = runtime.GrabEdgeEventRethrow<TEvConsole::TEvGetNodeConfigResponse>(handle);
    UNIT_ASSERT_VALUES_EQUAL(reply->Record.GetStatus().GetCode(), code);
}

void IncGeneration(NKikimrConsole::TConfigItem &item)
{
    item.MutableId()->SetGeneration(item.GetId().GetGeneration() + 1);
}

template <typename ...Ts>
void IncGeneration(NKikimrConsole::TConfigItem &item, Ts&... args)
{
    IncGeneration(item);
    IncGeneration(args...);
}

ui64 CheckAddConfigSubscription(TTenantTestRuntime &runtime, Ydb::StatusIds::StatusCode code,
                                ui32 nodeId, const TString &host, const TString &tenant,
                                const TString &nodeType, ui64 tabletId, TActorId serviceId,
                                TVector<ui32> kinds, ui64 id = 0)
{
    auto *event = new TEvConsole::TEvAddConfigSubscriptionRequest;
    event->Record.MutableSubscription()->SetId(id);
    if (tabletId)
        event->Record.MutableSubscription()->MutableSubscriber()->SetTabletId(tabletId);
    else if (serviceId)
        ActorIdToProto(serviceId, event->Record.MutableSubscription()->MutableSubscriber()->MutableServiceId());
    event->Record.MutableSubscription()->MutableOptions()->SetNodeId(nodeId);
    event->Record.MutableSubscription()->MutableOptions()->SetHost(host);
    event->Record.MutableSubscription()->MutableOptions()->SetTenant(tenant);
    event->Record.MutableSubscription()->MutableOptions()->SetNodeType(nodeType);
    for (auto &kind : kinds)
        event->Record.MutableSubscription()->AddConfigItemKinds(kind);

    TAutoPtr<IEventHandle> handle;
    runtime.SendToConsole(event);
    auto reply = runtime.GrabEdgeEventRethrow<TEvConsole::TEvAddConfigSubscriptionResponse>(handle);
    UNIT_ASSERT_VALUES_EQUAL(reply->Record.GetStatus().GetCode(), code);

    ui64 res = reply->Record.GetSubscriptionId();
    UNIT_ASSERT(code != Ydb::StatusIds::SUCCESS || res);
    return res;
}

ui64 CheckReplaceConfigSubscriptions(TTenantTestRuntime &runtime, Ydb::StatusIds::StatusCode code,
                                     ui32 nodeId, const TString &host, const TString &tenant,
                                     const TString &nodeType, ui64 tabletId, TActorId serviceId,
                                     TVector<ui32> kinds, ui64 id = 0)
{
    auto *event = new TEvConsole::TEvReplaceConfigSubscriptionsRequest;
    event->Record.MutableSubscription()->SetId(id);
    if (tabletId)
        event->Record.MutableSubscription()->MutableSubscriber()->SetTabletId(tabletId);
    else if (serviceId)
        ActorIdToProto(serviceId, event->Record.MutableSubscription()->MutableSubscriber()->MutableServiceId());
    event->Record.MutableSubscription()->MutableOptions()->SetNodeId(nodeId);
    event->Record.MutableSubscription()->MutableOptions()->SetHost(host);
    event->Record.MutableSubscription()->MutableOptions()->SetTenant(tenant);
    event->Record.MutableSubscription()->MutableOptions()->SetNodeType(nodeType);
    for (auto &kind : kinds)
        event->Record.MutableSubscription()->AddConfigItemKinds(kind);

    TAutoPtr<IEventHandle> handle;
    runtime.SendToConsole(event);
    auto reply = runtime.GrabEdgeEventRethrow<TEvConsole::TEvReplaceConfigSubscriptionsResponse>(handle);
    UNIT_ASSERT_VALUES_EQUAL(reply->Record.GetStatus().GetCode(), code);

    ui64 res = reply->Record.GetSubscriptionId();
    UNIT_ASSERT(code != Ydb::StatusIds::SUCCESS || res);
    return res;
}

void CheckGetConfigSubscription(TTenantTestRuntime &runtime, Ydb::StatusIds::StatusCode code,
                                ui64 id, ui32 nodeId = 0, const TString &host = "", const TString &tenant = "",
                                const TString &nodeType = "", ui64 tabletId = 0, TActorId serviceId = {},
                                TVector<ui32> kinds = {})
{
    auto *event = new TEvConsole::TEvGetConfigSubscriptionRequest;
    event->Record.SetSubscriptionId(id);

    TAutoPtr<IEventHandle> handle;
    runtime.SendToConsole(event);
    auto reply = runtime.GrabEdgeEventRethrow<TEvConsole::TEvGetConfigSubscriptionResponse>(handle);
    UNIT_ASSERT_VALUES_EQUAL(reply->Record.GetStatus().GetCode(), code);

    if (code == Ydb::StatusIds::SUCCESS) {
        THashSet<ui32> k(kinds.begin(), kinds.end());
        UNIT_ASSERT_VALUES_EQUAL(reply->Record.GetSubscription().GetId(), id);
        UNIT_ASSERT_VALUES_EQUAL(reply->Record.GetSubscription().GetSubscriber().GetTabletId(), tabletId);
        UNIT_ASSERT_VALUES_EQUAL(ActorIdFromProto(reply->Record.GetSubscription().GetSubscriber().GetServiceId()), serviceId);
        UNIT_ASSERT_VALUES_EQUAL(reply->Record.GetSubscription().GetOptions().GetNodeId(), nodeId);
        UNIT_ASSERT_VALUES_EQUAL(reply->Record.GetSubscription().GetOptions().GetHost(), host);
        UNIT_ASSERT_VALUES_EQUAL(reply->Record.GetSubscription().GetOptions().GetTenant(), tenant);
        UNIT_ASSERT_VALUES_EQUAL(reply->Record.GetSubscription().GetOptions().GetNodeType(), nodeType);
        for (auto &kind : reply->Record.GetSubscription().GetConfigItemKinds()) {
            UNIT_ASSERT(k.contains(kind));
            k.erase(kind);
        }
        UNIT_ASSERT(k.empty());
    }
}

void CheckRemoveConfigSubscription(TTenantTestRuntime &runtime, Ydb::StatusIds::StatusCode code, ui64 id)
{
    auto *event = new TEvConsole::TEvRemoveConfigSubscriptionRequest;
    event->Record.SetSubscriptionId(id);

    TAutoPtr<IEventHandle> handle;
    runtime.SendToConsole(event);
    auto reply = runtime.GrabEdgeEventRethrow<TEvConsole::TEvRemoveConfigSubscriptionResponse>(handle);
    UNIT_ASSERT_VALUES_EQUAL(reply->Record.GetStatus().GetCode(), code);
}

void CheckRemoveConfigSubscriptions(TTenantTestRuntime &runtime, Ydb::StatusIds::StatusCode code,
                                    ui64 tabletId, TActorId serviceId)
{
    auto *event = new TEvConsole::TEvRemoveConfigSubscriptionsRequest;
    if (tabletId)
        event->Record.MutableSubscriber()->SetTabletId(tabletId);
    else if (serviceId)
        ActorIdToProto(serviceId, event->Record.MutableSubscriber()->MutableServiceId());

    TAutoPtr<IEventHandle> handle;
    runtime.SendToConsole(event);
    auto reply = runtime.GrabEdgeEventRethrow<TEvConsole::TEvRemoveConfigSubscriptionsResponse>(handle);
    UNIT_ASSERT_VALUES_EQUAL(reply->Record.GetStatus().GetCode(), code);
}

/*void CollectConfigItemIds(NConsole::TConfigId &)
{
}
*/
void CollectConfigItemIds(NConsole::TConfigId &id, const NKikimrConsole::TConfigItem &item)
{
    id.ItemIds.push_back(std::make_pair(item.GetId().GetId(), item.GetId().GetGeneration()));
}

template <typename ...Ts>
void CollectConfigItemIds(NConsole::TConfigId &id, const NKikimrConsole::TConfigItem &item, Ts ...args)
{
    CollectConfigItemIds(id, item);
    CollectConfigItemIds(id, args...);
}

template <typename ...Ts>
void CheckConfigId(const NKikimrConsole::TConfigId &configId, Ts ...args)
{
    NConsole::TConfigId lhs(configId);
    NConsole::TConfigId rhs;
    CollectConfigItemIds(rhs, args...);
    UNIT_ASSERT_VALUES_EQUAL(lhs.ToString(), rhs.ToString());
}

void AcceptConfig(TTenantTestRuntime &runtime, TAutoPtr<IEventHandle>& handle)
{
    auto &rec = handle->Get<TEvConsole::TEvConfigNotificationRequest>()->Record;
    auto *response = new TEvConsole::TEvConfigNotificationResponse;
    response->Record.SetSubscriptionId(rec.GetSubscriptionId());
    response->Record.MutableConfigId()->CopyFrom(rec.GetConfigId());
    runtime.Send(new IEventHandle(handle->Sender, runtime.Sender, response, 0, handle->Cookie));
}

NKikimrConsole::TConfig GetCurrentConfig(TTenantTestRuntime &runtime)
{
    runtime.SendToConsole(new TEvConsole::TEvGetConfigRequest);
    TAutoPtr<IEventHandle> handle;
    auto reply = runtime.GrabEdgeEventRethrow<TEvConsole::TEvGetConfigResponse>(handle);
    return reply->Record.GetConfig();
}

void CheckSetConfig(TTenantTestRuntime &runtime, const NKikimrConsole::TConfig &config, Ydb::StatusIds::StatusCode code)
{
    runtime.SendToConsole(new TEvConsole::TEvSetConfigRequest(config));
    TAutoPtr<IEventHandle> handle;
    auto reply = runtime.GrabEdgeEventRethrow<TEvConsole::TEvSetConfigResponse>(handle);
    UNIT_ASSERT_VALUES_EQUAL(reply->Record.GetStatus().GetCode(), code);
}

void CheckCheckConfigUpdates(TTenantTestRuntime &runtime,
                             TVector<NKikimrConsole::TConfigItem> baseItems,
                             THashSet<std::pair<ui64, ui64>> addedItems,
                             THashSet<std::pair<ui64, ui64>> removedItems,
                             THashSet<std::pair<ui64, ui64>> updatedItems)
{
    auto *request = new TEvConsole::TEvCheckConfigUpdatesRequest;
    for (auto &item : baseItems)
        request->Record.AddBaseItemIds()->CopyFrom(item.GetId());
    runtime.SendToConsole(request);

    TAutoPtr<IEventHandle> handle;
    auto reply = runtime.GrabEdgeEventRethrow<TEvConsole::TEvCheckConfigUpdatesResponse>(handle);
    UNIT_ASSERT_VALUES_EQUAL(reply->Record.AddedItemsSize(), addedItems.size());
    for (auto &item : reply->Record.GetAddedItems()) {
        auto id = std::make_pair(item.GetId(), item.GetGeneration());
        UNIT_ASSERT(addedItems.contains(id));
        addedItems.erase(id);
    }
    UNIT_ASSERT_VALUES_EQUAL(reply->Record.RemovedItemsSize(), removedItems.size());
    for (auto &item : reply->Record.GetRemovedItems()) {
        auto id = std::make_pair(item.GetId(), item.GetGeneration());
        UNIT_ASSERT(removedItems.contains(id));
        removedItems.erase(id);
    }
    UNIT_ASSERT_VALUES_EQUAL(reply->Record.UpdatedItemsSize(), updatedItems.size());
    for (auto &item : reply->Record.GetUpdatedItems()) {
        auto id = std::make_pair(item.GetId(), item.GetGeneration());
        UNIT_ASSERT(updatedItems.contains(id));
        updatedItems.erase(id);
    }
}

void RestartConsole(TTenantTestRuntime &runtime)
{
    runtime.Register(CreateTabletKiller(MakeConsoleID()));
    TDispatchOptions options;
    options.FinalEvents.emplace_back(&IsTabletActiveEvent, 1);
    runtime.DispatchEvents(options);
}

NKikimrConsole::TConfigItem ITEM_DOMAIN_LOG_1;
NKikimrConsole::TConfigItem ITEM_DOMAIN_LOG_2;
NKikimrConsole::TConfigItem ITEM_NODE12_LOG_1;
NKikimrConsole::TConfigItem ITEM_NODE23_LOG_1;
NKikimrConsole::TConfigItem ITEM_NODE34_LOG_1;
NKikimrConsole::TConfigItem ITEM_HOST12_LOG_1;
NKikimrConsole::TConfigItem ITEM_HOST23_LOG_1;
NKikimrConsole::TConfigItem ITEM_HOST34_LOG_1;
NKikimrConsole::TConfigItem ITEM_TENANT1_LOG_1;
NKikimrConsole::TConfigItem ITEM_TENANT1_LOG_2;
NKikimrConsole::TConfigItem ITEM_TENANT2_LOG_1;
NKikimrConsole::TConfigItem ITEM_TENANT2_LOG_2;
NKikimrConsole::TConfigItem ITEM_TYPE1_LOG_1;
NKikimrConsole::TConfigItem ITEM_TYPE1_LOG_2;
NKikimrConsole::TConfigItem ITEM_TYPE2_LOG_1;
NKikimrConsole::TConfigItem ITEM_TYPE2_LOG_2;
NKikimrConsole::TConfigItem ITEM_TENANT1_TYPE1_LOG_1;
NKikimrConsole::TConfigItem ITEM_TENANT1_TYPE1_LOG_2;
NKikimrConsole::TConfigItem ITEM_TENANT1_TYPE2_LOG_1;
NKikimrConsole::TConfigItem ITEM_TENANT1_TYPE2_LOG_2;
NKikimrConsole::TConfigItem ITEM_TENANT2_TYPE1_LOG_1;
NKikimrConsole::TConfigItem ITEM_TENANT2_TYPE1_LOG_2;
NKikimrConsole::TConfigItem ITEM_TENANT2_TYPE2_LOG_1;
NKikimrConsole::TConfigItem ITEM_TENANT2_TYPE2_LOG_2;
NKikimrConsole::TConfigItem ITEM_DOMAIN_TENANT_POOL_1;
NKikimrConsole::TConfigItem ITEM_DOMAIN_TENANT_POOL_2;

const TString YAML_CONFIG_1 = R"(
---
metadata:
  cluster: ""
  version: 0
config:
  log_config:
    cluster_name: cluster1
allowed_labels:
  test:
    type: enum
    values:
      ? true

selector_config: []
)";

const TString YAML_CONFIG_1_UPDATED = R"(
---
metadata:
  kind: MainConfig
  cluster: ""
  version: 1
config:
  log_config:
    cluster_name: cluster1
allowed_labels:
  test:
    type: enum
    values:
      ? true

selector_config: []
)";

const TString YAML_CONFIG_2 = R"(
---
metadata:
  cluster: ""
  version: 1
config:
  log_config:
    cluster_name: cluster2
allowed_labels:
  test:
    type: enum
    values:
      ? true

selector_config: []
)";

const TString YAML_CONFIG_2_UPDATED = R"(
---
metadata:
  kind: MainConfig
  cluster: ""
  version: 2
config:
  log_config:
    cluster_name: cluster2
allowed_labels:
  test:
    type: enum
    values:
      ? true

selector_config: []
)";

const TString VOLATILE_YAML_CONFIG_1_1 = R"(
- description: test 4
  selector:
    tenant: /slice
  config:
    yaml_config_enabled: true
    cms_config:
      sentinel_config:
        enable: false
)";

const TString EXTENDED_VOLATILE_YAML_CONFIG_1_1 = R"(metadata:
  kind: VolatileConfig
  cluster: ""
  version: 1
  id: 0
selector_config:

- description: test 4
  selector:
    tenant: /slice
  config:
    yaml_config_enabled: true
    cms_config:
      sentinel_config:
        enable: false
)";

const size_t VOLATILE_YAML_CONFIG_1_1_HASH = THash<TString>{}(VOLATILE_YAML_CONFIG_1_1);

const TString VOLATILE_YAML_CONFIG_1_2 = R"(
- description: test 5
  selector:
    tenant: /slice/test
  config:
    yaml_config_enabled: false
    cms_config:
      sentinel_config:
        enable: true
)";

const size_t VOLATILE_YAML_CONFIG_1_2_HASH = THash<TString>{}(VOLATILE_YAML_CONFIG_1_2);

void InitializeTestConfigItems()
{
    ITEM_DOMAIN_LOG_1
        = MakeConfigItem(NKikimrConsole::TConfigItem::LogConfigItem,
                         NKikimrConfig::TAppConfig(), {}, {}, "", "", 1,
                         NKikimrConsole::TConfigItem::MERGE, "");
    ITEM_DOMAIN_LOG_2
        = MakeConfigItem(NKikimrConsole::TConfigItem::LogConfigItem,
                         NKikimrConfig::TAppConfig(), {}, {}, "", "", 2,
                         NKikimrConsole::TConfigItem::MERGE, "");
    ITEM_NODE12_LOG_1
        = MakeConfigItem(NKikimrConsole::TConfigItem::LogConfigItem,
                         NKikimrConfig::TAppConfig(), {1, 2}, {}, "", "", 1,
                         NKikimrConsole::TConfigItem::MERGE, "cookie1");
    ITEM_NODE23_LOG_1
        = MakeConfigItem(NKikimrConsole::TConfigItem::LogConfigItem,
                         NKikimrConfig::TAppConfig(), {2, 3}, {}, "", "", 1,
                         NKikimrConsole::TConfigItem::MERGE, "cookie1");
    ITEM_NODE34_LOG_1
        = MakeConfigItem(NKikimrConsole::TConfigItem::LogConfigItem,
                         NKikimrConfig::TAppConfig(), {3, 4}, {}, "", "", 1,
                         NKikimrConsole::TConfigItem::MERGE, "cookie1");
    ITEM_HOST12_LOG_1
        = MakeConfigItem(NKikimrConsole::TConfigItem::LogConfigItem,
                         NKikimrConfig::TAppConfig(), {}, {"host1", "host2"}, "", "", 1,
                         NKikimrConsole::TConfigItem::MERGE, "cookie1");
    ITEM_HOST23_LOG_1
        = MakeConfigItem(NKikimrConsole::TConfigItem::LogConfigItem,
                         NKikimrConfig::TAppConfig(), {}, {"host2", "host3"}, "", "", 1,
                         NKikimrConsole::TConfigItem::MERGE, "cookie1");
    ITEM_HOST34_LOG_1
        = MakeConfigItem(NKikimrConsole::TConfigItem::LogConfigItem,
                         NKikimrConfig::TAppConfig(), {}, {"host3", "host4"}, "", "", 1,
                         NKikimrConsole::TConfigItem::MERGE, "cookie1");
    ITEM_TENANT1_LOG_1
        = MakeConfigItem(NKikimrConsole::TConfigItem::LogConfigItem,
                         NKikimrConfig::TAppConfig(), {}, {}, "tenant1", "", 1,
                         NKikimrConsole::TConfigItem::MERGE, "cookie1");
    ITEM_TENANT1_LOG_2
        = MakeConfigItem(NKikimrConsole::TConfigItem::LogConfigItem,
                         NKikimrConfig::TAppConfig(), {}, {}, "tenant1", "", 2,
                         NKikimrConsole::TConfigItem::MERGE, "cookie2");
    ITEM_TENANT2_LOG_1
        = MakeConfigItem(NKikimrConsole::TConfigItem::LogConfigItem,
                         NKikimrConfig::TAppConfig(), {}, {}, "tenant2", "", 1,
                         NKikimrConsole::TConfigItem::MERGE, "cookie1");
    ITEM_TENANT2_LOG_2
        = MakeConfigItem(NKikimrConsole::TConfigItem::LogConfigItem,
                         NKikimrConfig::TAppConfig(), {}, {}, "tenant2", "", 2,
                         NKikimrConsole::TConfigItem::MERGE, "cookie2");
    ITEM_TYPE1_LOG_1
        = MakeConfigItem(NKikimrConsole::TConfigItem::LogConfigItem,
                         NKikimrConfig::TAppConfig(), {}, {}, "", "type1", 1,
                         NKikimrConsole::TConfigItem::MERGE, "cookie1");
    ITEM_TYPE1_LOG_2
        = MakeConfigItem(NKikimrConsole::TConfigItem::LogConfigItem,
                         NKikimrConfig::TAppConfig(), {}, {}, "", "type1", 2,
                         NKikimrConsole::TConfigItem::MERGE, "cookie2");
    ITEM_TYPE2_LOG_1
        = MakeConfigItem(NKikimrConsole::TConfigItem::LogConfigItem,
                         NKikimrConfig::TAppConfig(), {}, {}, "", "type2", 1,
                         NKikimrConsole::TConfigItem::MERGE, "cookie1");
    ITEM_TYPE2_LOG_2
        = MakeConfigItem(NKikimrConsole::TConfigItem::LogConfigItem,
                         NKikimrConfig::TAppConfig(), {}, {}, "", "type2", 2,
                         NKikimrConsole::TConfigItem::MERGE, "cookie2");
    ITEM_TENANT1_TYPE1_LOG_1
        = MakeConfigItem(NKikimrConsole::TConfigItem::LogConfigItem,
                         NKikimrConfig::TAppConfig(), {}, {}, "tenant1", "type1", 1,
                         NKikimrConsole::TConfigItem::MERGE, "cookie1");
    ITEM_TENANT1_TYPE1_LOG_2
        = MakeConfigItem(NKikimrConsole::TConfigItem::LogConfigItem,
                         NKikimrConfig::TAppConfig(), {}, {}, "tenant1", "type1", 2,
                         NKikimrConsole::TConfigItem::MERGE, "cookie2");
    ITEM_TENANT1_TYPE2_LOG_1
        = MakeConfigItem(NKikimrConsole::TConfigItem::LogConfigItem,
                         NKikimrConfig::TAppConfig(), {}, {}, "tenant1", "type2", 1,
                         NKikimrConsole::TConfigItem::MERGE, "cookie1");
    ITEM_TENANT1_TYPE2_LOG_2
        = MakeConfigItem(NKikimrConsole::TConfigItem::LogConfigItem,
                         NKikimrConfig::TAppConfig(), {}, {}, "tenant1", "type2", 2,
                         NKikimrConsole::TConfigItem::MERGE, "cookie2");
    ITEM_TENANT2_TYPE1_LOG_1
        = MakeConfigItem(NKikimrConsole::TConfigItem::LogConfigItem,
                         NKikimrConfig::TAppConfig(), {}, {}, "tenant2", "type1", 1,
                         NKikimrConsole::TConfigItem::MERGE, "cookie1");
    ITEM_TENANT2_TYPE1_LOG_2
        = MakeConfigItem(NKikimrConsole::TConfigItem::LogConfigItem,
                         NKikimrConfig::TAppConfig(), {}, {}, "tenant2", "type1", 2,
                         NKikimrConsole::TConfigItem::MERGE, "cookie2");
    ITEM_TENANT2_TYPE2_LOG_1
        = MakeConfigItem(NKikimrConsole::TConfigItem::LogConfigItem,
                         NKikimrConfig::TAppConfig(), {}, {}, "tenant2", "type2", 1,
                         NKikimrConsole::TConfigItem::MERGE, "cookie1");
    ITEM_TENANT2_TYPE2_LOG_2
        = MakeConfigItem(NKikimrConsole::TConfigItem::LogConfigItem,
                         NKikimrConfig::TAppConfig(), {}, {}, "tenant2", "type2", 2,
                         NKikimrConsole::TConfigItem::MERGE, "cookie2");
    ITEM_DOMAIN_TENANT_POOL_1
        = MakeConfigItem(NKikimrConsole::TConfigItem::TenantPoolConfigItem,
                         NKikimrConfig::TAppConfig(), {}, {}, "", "", 1,
                         NKikimrConsole::TConfigItem::MERGE, "cookie1");
    ITEM_DOMAIN_TENANT_POOL_2
        = MakeConfigItem(NKikimrConsole::TConfigItem::TenantPoolConfigItem,
                         NKikimrConfig::TAppConfig(), {}, {}, "", "", 2,
                         NKikimrConsole::TConfigItem::MERGE, "cookie2");
}

class TTestValidator : public IConfigValidator {
public:
    TTestValidator(ui32 maxCount,
                   const TString &name = "test")
        : IConfigValidator(name, NKikimrConsole::TConfigItem::LogConfigItem)
        , MaxCount(maxCount)
    {
    }

    TTestValidator(ui32 maxCount,
                   const TString &name,
                   const THashSet<ui32> &kinds)
        : IConfigValidator(name, kinds)
        , MaxCount(maxCount)
    {
    }

    bool CheckConfig(const NKikimrConfig::TAppConfig &oldConfig,
                     const NKikimrConfig::TAppConfig &newConfig,
                     TVector<Ydb::Issue::IssueMessage> &issues) const override
    {
        Y_UNUSED(oldConfig);
        if (newConfig.GetLogConfig().EntrySize() > MaxCount) {
            Ydb::Issue::IssueMessage issue;
            issue.set_message("too many entries");
            issue.set_severity(NYql::TSeverityIds::S_ERROR);
            issues.push_back(issue);
            return false;
        }
        return true;
    }

private:
    ui32 MaxCount;
};

struct TValidatorInfo {
    TString Name;
    TString Description;
    THashSet<ui32> CheckedKinds;
    bool Enabled = false;
};

void CheckListValidators(TTenantTestRuntime &runtime,
                         TVector<TValidatorInfo> validators)
{
    auto *request = new TEvConsole::TEvListConfigValidatorsRequest;
    runtime.SendToConsole(request);

    TAutoPtr<IEventHandle> handle;
    auto reply = runtime.GrabEdgeEventRethrow<TEvConsole::TEvListConfigValidatorsResponse>(handle);
    auto &rec = reply->Record;
    UNIT_ASSERT_VALUES_EQUAL(rec.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_VALUES_EQUAL(rec.ValidatorsSize(), validators.size());

    THashMap<TString, TValidatorInfo> infos;
    for (auto &validator : validators)
        infos[validator.Name] = validator;

    for (auto &entry : rec.GetValidators()) {
        UNIT_ASSERT(infos.contains(entry.GetName()));
        auto &info = infos.at(entry.GetName());
        UNIT_ASSERT_VALUES_EQUAL(info.Description, entry.GetDescription());
        UNIT_ASSERT_VALUES_EQUAL(info.CheckedKinds.size(), entry.CheckedItemKindsSize());
        for (auto kind : entry.GetCheckedItemKinds()) {
            UNIT_ASSERT(info.CheckedKinds.contains(kind));
            info.CheckedKinds.erase(kind);
        }
        UNIT_ASSERT_VALUES_EQUAL(info.Enabled, entry.GetEnabled());
        infos.erase(entry.GetName());
    }
}

void CheckToggleValidator(TTenantTestRuntime &runtime,
                          const TString &name,
                          bool disable,
                          Ydb::StatusIds::StatusCode code)
{
    auto *request = new TEvConsole::TEvToggleConfigValidatorRequest;
    request->Record.SetName(name);
    request->Record.SetDisable(disable);
    runtime.SendToConsole(request);

    TAutoPtr<IEventHandle> handle;
    auto reply = runtime.GrabEdgeEventRethrow<TEvConsole::TEvToggleConfigValidatorResponse>(handle);
    auto &rec = reply->Record;
    UNIT_ASSERT_VALUES_EQUAL(rec.GetStatus().GetCode(), code);
}

void CheckDisableValidator(TTenantTestRuntime &runtime,
                           const TString &name,
                           Ydb::StatusIds::StatusCode code)
{
    CheckToggleValidator(runtime, name, true, code);
}

void CheckEnableValidator(TTenantTestRuntime &runtime,
                          const TString &name,
                          Ydb::StatusIds::StatusCode code)
{
    CheckToggleValidator(runtime, name, false, code);
}

template <typename ...Ts>
TVector<ui64> CheckConfigureLogAffected(TTenantTestRuntime &runtime,
                                        Ydb::StatusIds::StatusCode code,
                                        THashMap<TTenantAndNodeType, std::pair<ui64, ui64>> affected,
                                        bool dryRun,
                                        bool fillAffected,
                                        Ts... args)
{
    auto *event = new TEvConsole::TEvConfigureRequest;
    event->Record.SetDryRun(dryRun);
    event->Record.SetFillAffectedConfigs(fillAffected);
    CollectActions(event->Record, args...);

    TAutoPtr<IEventHandle> handle;
    runtime.SendToConsole(event);
    auto reply = runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigureResponse>(handle);
    UNIT_ASSERT_VALUES_EQUAL(reply->Record.GetStatus().GetCode(), code);

    UNIT_ASSERT_VALUES_EQUAL(reply->Record.AffectedConfigsSize(), affected.size());
    for (auto &entry : reply->Record.GetAffectedConfigs()) {
        TTenantAndNodeType key(entry.GetTenant(), entry.GetNodeType());
        UNIT_ASSERT(affected.contains(key));
        UNIT_ASSERT_VALUES_EQUAL(affected.at(key).first, entry.GetOldConfig().GetLogConfig().EntrySize());
        UNIT_ASSERT_VALUES_EQUAL(affected.at(key).second, entry.GetNewConfig().GetLogConfig().EntrySize());
        affected.erase(key);
    }

    return {reply->Record.GetAddedItemIds().begin(), reply->Record.GetAddedItemIds().end()};
}



} // anonymous namespace

Y_UNIT_TEST_SUITE(TConsoleConfigTests) {
    Y_UNIT_TEST(TestAddConfigItem) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        InitializeTestConfigItems();

        auto tmp1 = ITEM_DOMAIN_TENANT_POOL_1;
        auto tmp2 = ITEM_DOMAIN_TENANT_POOL_2;
        tmp2.MutableConfig()->MutableLogConfig();

        // OK.
        auto id1 = CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                                  MakeAddAction(ITEM_DOMAIN_TENANT_POOL_1));
        AssignIds(id1, ITEM_DOMAIN_TENANT_POOL_1);
        // Error: action has Id specified.
        CheckConfigure(runtime, Ydb::StatusIds::BAD_REQUEST,
                       MakeAddAction(ITEM_DOMAIN_TENANT_POOL_1));
        // Error: config doesn't match kind.
        CheckConfigure(runtime, Ydb::StatusIds::BAD_REQUEST,
                       MakeAddAction(tmp2));
        // Error: order conflict with existing item.
        CheckConfigure(runtime, Ydb::StatusIds::BAD_REQUEST,
                       MakeAddAction(tmp1));
        // Error: order conflict between new items.
        CheckConfigure(runtime, Ydb::StatusIds::BAD_REQUEST,
                       MakeAddAction(ITEM_DOMAIN_LOG_1),
                       MakeAddAction(ITEM_DOMAIN_LOG_1));
        // OK.
        auto id2 = CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                                  MakeAddAction(ITEM_DOMAIN_TENANT_POOL_2));
        AssignIds(id2, ITEM_DOMAIN_TENANT_POOL_2);
        // Add several items. Order of ids should match items order.
        auto id3 = CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                                  MakeAddAction(ITEM_DOMAIN_LOG_1),
                                  MakeAddAction(ITEM_DOMAIN_LOG_2),
                                  MakeAddAction(ITEM_HOST12_LOG_1));
        AssignIds(id3, ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_LOG_2, ITEM_HOST12_LOG_1);

        // Error: ' ' symbol in host name.
        auto item = MakeConfigItem(NKikimrConsole::TConfigItem::LogConfigItem,
                                   NKikimrConfig::TAppConfig(), {}, {"host1 host2"}, "", "", 10,
                                   NKikimrConsole::TConfigItem::MERGE, "cookie1");
        CheckConfigure(runtime, Ydb::StatusIds::BAD_REQUEST,
                       MakeAddAction(tmp1));

        {
            runtime.Register(CreateTabletKiller(MakeConsoleID()));
            TDispatchOptions options;
            options.FinalEvents.emplace_back(&IsTabletActiveEvent, 1);
            runtime.DispatchEvents(options);
        }

        // Check all items.
        CheckGetItems(runtime, TVector<ui32>(),
                      ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_LOG_2, ITEM_DOMAIN_TENANT_POOL_1,
                      ITEM_DOMAIN_TENANT_POOL_2, ITEM_HOST12_LOG_1);
    }

    Y_UNIT_TEST(TestModifyConfigItem) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        InitializeTestConfigItems();

        auto id1 = CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                                  MakeAddAction(ITEM_DOMAIN_LOG_1),
                                  MakeAddAction(ITEM_DOMAIN_LOG_2),
                                  MakeAddAction(ITEM_DOMAIN_TENANT_POOL_1));
        AssignIds(id1, ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_LOG_2,
                  ITEM_DOMAIN_TENANT_POOL_1);
        // Error: kind mismatch.
        ITEM_DOMAIN_LOG_1.MutableConfig()->MutableTenantPoolConfig();
        CheckConfigure(runtime, Ydb::StatusIds::BAD_REQUEST,
                       MakeModifyAction(ITEM_DOMAIN_LOG_1));
        ITEM_DOMAIN_LOG_1.MutableConfig()->ClearTenantPoolConfig();
        // Error: generation mismatch
        ITEM_DOMAIN_LOG_1.MutableId()->SetGeneration(2);
        CheckConfigure(runtime, Ydb::StatusIds::BAD_REQUEST,
                       MakeModifyAction(ITEM_DOMAIN_LOG_1));
        ITEM_DOMAIN_LOG_1.MutableId()->SetGeneration(1);
        // Error: wrong id.
        ITEM_DOMAIN_LOG_1.MutableId()->SetId(987654321);
        CheckConfigure(runtime, Ydb::StatusIds::BAD_REQUEST,
                       MakeModifyAction(ITEM_DOMAIN_LOG_1));
        ITEM_DOMAIN_LOG_1.MutableId()->SetId(id1[0]);
        // Error: cannot change kind
        ITEM_DOMAIN_TENANT_POOL_2.MutableId()->CopyFrom(ITEM_DOMAIN_LOG_1.GetId());
        CheckConfigure(runtime, Ydb::StatusIds::BAD_REQUEST,
                       MakeModifyAction(ITEM_DOMAIN_TENANT_POOL_2));
        ITEM_DOMAIN_TENANT_POOL_2.ClearId();
        // Error: double modification of the same item.
        CheckConfigure(runtime, Ydb::StatusIds::BAD_REQUEST,
                       MakeModifyAction(ITEM_DOMAIN_LOG_1),
                       MakeModifyAction(ITEM_DOMAIN_LOG_1));
        // Error: order confilict with existing item.
        ITEM_DOMAIN_LOG_1.SetOrder(2);
        CheckConfigure(runtime, Ydb::StatusIds::BAD_REQUEST,
                       MakeModifyAction(ITEM_DOMAIN_LOG_1));
        // Error: order confilict with another modification.
        ITEM_DOMAIN_LOG_1.SetOrder(3);
        ITEM_DOMAIN_LOG_2.SetOrder(3);
        CheckConfigure(runtime, Ydb::StatusIds::BAD_REQUEST,
                       MakeModifyAction(ITEM_DOMAIN_LOG_1),
                       MakeModifyAction(ITEM_DOMAIN_LOG_2));
        // Error: order conflict with added item.
        ITEM_DOMAIN_TENANT_POOL_1.SetOrder(2);
        CheckConfigure(runtime, Ydb::StatusIds::BAD_REQUEST,
                       MakeModifyAction(ITEM_DOMAIN_TENANT_POOL_1),
                       MakeAddAction(ITEM_DOMAIN_TENANT_POOL_2));
        // OK to modify order.
        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                       MakeModifyAction(ITEM_DOMAIN_TENANT_POOL_1));
        IncGeneration(ITEM_DOMAIN_TENANT_POOL_1);
        // OK to modify order and add with the previous value.
        ITEM_DOMAIN_TENANT_POOL_1.SetOrder(3);
        auto id2 = CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                                  MakeModifyAction(ITEM_DOMAIN_TENANT_POOL_1),
                                  MakeAddAction(ITEM_DOMAIN_TENANT_POOL_2));
        AssignIds(id2, ITEM_DOMAIN_TENANT_POOL_2);
        IncGeneration(ITEM_DOMAIN_TENANT_POOL_1);
        // OK to switch orders.
        ITEM_DOMAIN_LOG_1.SetOrder(2);
        ITEM_DOMAIN_LOG_2.SetOrder(1);
        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                       MakeModifyAction(ITEM_DOMAIN_LOG_1),
                       MakeModifyAction(ITEM_DOMAIN_LOG_2));
        IncGeneration(ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_LOG_2);
        // OK to modify scope.
        ITEM_DOMAIN_LOG_1.MutableUsageScope()->MutableNodeFilter()->AddNodes(1);
        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                       MakeModifyAction(ITEM_DOMAIN_LOG_1));
        IncGeneration(ITEM_DOMAIN_LOG_1);
        // Error: order conflict.
        ITEM_DOMAIN_LOG_2.MutableUsageScope()->MutableNodeFilter()->AddNodes(1);
        ITEM_DOMAIN_LOG_2.MutableUsageScope()->MutableNodeFilter()->AddNodes(2);
        ITEM_DOMAIN_LOG_2.SetOrder(2);
        CheckConfigure(runtime, Ydb::StatusIds::BAD_REQUEST,
                       MakeModifyAction(ITEM_DOMAIN_LOG_2));
        // OK to switch order back because of different scope.
        ITEM_DOMAIN_LOG_2.ClearUsageScope();
        ITEM_DOMAIN_LOG_2.SetOrder(2);
        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                       MakeModifyAction(ITEM_DOMAIN_LOG_2));
        IncGeneration(ITEM_DOMAIN_LOG_2);
        // OK to use disjoint scopes.
        ITEM_DOMAIN_LOG_2.MutableUsageScope()->MutableNodeFilter()->AddNodes(2);
        ITEM_DOMAIN_LOG_2.SetOrder(2);
        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                       MakeModifyAction(ITEM_DOMAIN_LOG_2));
        IncGeneration(ITEM_DOMAIN_LOG_2);
        // Error: order conflicts in intersecting scopes.
        ITEM_DOMAIN_LOG_1.ClearUsageScope();
        ITEM_DOMAIN_LOG_1.MutableUsageScope()->MutableHostFilter()->AddHosts("host1");
        ITEM_DOMAIN_LOG_1.MutableUsageScope()->MutableHostFilter()->AddHosts("host2");
        ITEM_DOMAIN_LOG_2.ClearUsageScope();
        ITEM_DOMAIN_LOG_2.MutableUsageScope()->MutableHostFilter()->AddHosts("host2");
        ITEM_DOMAIN_LOG_2.MutableUsageScope()->MutableHostFilter()->AddHosts("host3");
        CheckConfigure(runtime, Ydb::StatusIds::BAD_REQUEST,
                       MakeModifyAction(ITEM_DOMAIN_LOG_1),
                       MakeModifyAction(ITEM_DOMAIN_LOG_2));
        // OK
        ITEM_DOMAIN_LOG_2.ClearUsageScope();
        ITEM_DOMAIN_LOG_2.MutableUsageScope()->MutableHostFilter()->AddHosts("host3");
        ITEM_DOMAIN_LOG_2.MutableUsageScope()->MutableHostFilter()->AddHosts("host4");
        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                       MakeModifyAction(ITEM_DOMAIN_LOG_1),
                       MakeModifyAction(ITEM_DOMAIN_LOG_2));
        IncGeneration(ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_LOG_2);

        // Check all items.
        CheckGetItems(runtime, TVector<ui32>(),
                      ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_LOG_2, ITEM_DOMAIN_TENANT_POOL_1,
                      ITEM_DOMAIN_TENANT_POOL_2);
    }

    Y_UNIT_TEST(TestRemoveConfigItem) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        InitializeTestConfigItems();

        auto id1 = CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                                  MakeAddAction(ITEM_DOMAIN_LOG_1),
                                  MakeAddAction(ITEM_DOMAIN_LOG_2),
                                  MakeAddAction(ITEM_DOMAIN_TENANT_POOL_1));
        AssignIds(id1, ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_LOG_2,
                  ITEM_DOMAIN_TENANT_POOL_1);
        // OK remove
        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                       MakeRemoveAction(ITEM_DOMAIN_LOG_1));
        // Error: wrong id
        CheckConfigure(runtime, Ydb::StatusIds::BAD_REQUEST,
                       MakeRemoveAction(ITEM_DOMAIN_LOG_1));
        // Error: wrong generation
        CheckConfigure(runtime, Ydb::StatusIds::BAD_REQUEST,
                       MakeRemoveAction(ITEM_DOMAIN_LOG_2.GetId().GetId(),
                                        ITEM_DOMAIN_LOG_2.GetId().GetGeneration() + 1));
        // OK to modify order
        ITEM_DOMAIN_LOG_2.SetOrder(1);
        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                       MakeModifyAction(ITEM_DOMAIN_LOG_2));
        IncGeneration(ITEM_DOMAIN_LOG_2);
        // Error: conflicting order.
        ITEM_DOMAIN_LOG_1.ClearId();
        CheckConfigure(runtime, Ydb::StatusIds::BAD_REQUEST,
                       MakeAddAction(ITEM_DOMAIN_LOG_1));
        // OK to add item with conflicting order if conflict is removed
        // at the same time.
        auto id2 = CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                                  MakeRemoveAction(ITEM_DOMAIN_LOG_2),
                                  MakeAddAction(ITEM_DOMAIN_LOG_1));
        AssignIds(id2, ITEM_DOMAIN_LOG_1);
        // Add item back.
        ITEM_DOMAIN_LOG_2.ClearId();
        ITEM_DOMAIN_LOG_2.SetOrder(2);
        auto id3 = CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                                  MakeAddAction(ITEM_DOMAIN_LOG_2));
        AssignIds(id3, ITEM_DOMAIN_LOG_2);
        // Error: conflicting order.
        ITEM_DOMAIN_LOG_2.SetOrder(1);
        CheckConfigure(runtime, Ydb::StatusIds::BAD_REQUEST,
                       MakeModifyAction(ITEM_DOMAIN_LOG_2));

        // OK to add item with conflicting order if conflict is removed
        // at the same time.
        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                       MakeRemoveAction(ITEM_DOMAIN_LOG_1),
                       MakeModifyAction(ITEM_DOMAIN_LOG_2));
        IncGeneration(ITEM_DOMAIN_LOG_2);

        // Check all items.
        CheckGetItems(runtime, TVector<ui32>(),
                      ITEM_DOMAIN_LOG_2, ITEM_DOMAIN_TENANT_POOL_1);
    }

    Y_UNIT_TEST(TestRemoveConfigItems) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        InitializeTestConfigItems();

        auto id1 = CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                                  MakeAddAction(ITEM_DOMAIN_LOG_1),
                                  MakeAddAction(ITEM_TYPE1_LOG_1),
                                  MakeAddAction(ITEM_TYPE1_LOG_2),
                                  MakeAddAction(ITEM_TYPE2_LOG_1),
                                  MakeAddAction(ITEM_TYPE2_LOG_2),
                                  MakeAddAction(ITEM_DOMAIN_TENANT_POOL_1));
        AssignIds(id1, ITEM_DOMAIN_LOG_1, ITEM_TYPE1_LOG_1, ITEM_TYPE1_LOG_2,
                  ITEM_TYPE2_LOG_1, ITEM_TYPE2_LOG_2, ITEM_DOMAIN_TENANT_POOL_1);
        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                       MakeRemoveByCookieAction("cookie1"));
        CheckGetItems(runtime, TVector<ui32>(),
                      ITEM_DOMAIN_LOG_1, ITEM_TYPE1_LOG_2,
                      ITEM_TYPE2_LOG_2);
        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                       MakeRemoveByCookieAction("cookie1"));
        CheckGetItems(runtime, TVector<ui32>(),
                      ITEM_DOMAIN_LOG_1, ITEM_TYPE1_LOG_2,
                      ITEM_TYPE2_LOG_2);
        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                       MakeRemoveByCookieAction("cookie2", ""));
        CheckGetItems(runtime, TVector<ui32>());
    }

    Y_UNIT_TEST(TestConfigureOrderConflicts) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        InitializeTestConfigItems();

        // NODE ITEMS
        // Error: order conflict.
        CheckConfigure(runtime, Ydb::StatusIds::BAD_REQUEST,
                       MakeAddAction(ITEM_NODE12_LOG_1),
                       MakeAddAction(ITEM_NODE23_LOG_1));
        CheckConfigure(runtime, Ydb::StatusIds::BAD_REQUEST,
                       MakeAddAction(ITEM_NODE23_LOG_1),
                       MakeAddAction(ITEM_NODE34_LOG_1));
        // OK
        auto id1 = CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                                  MakeAddAction(ITEM_NODE12_LOG_1),
                                  MakeAddAction(ITEM_NODE34_LOG_1));
        AssignIds(id1, ITEM_NODE12_LOG_1, ITEM_NODE34_LOG_1);
        // Error: order conflict.
        ITEM_NODE12_LOG_1.MutableUsageScope()->MutableNodeFilter()->AddNodes(3);
        CheckConfigure(runtime, Ydb::StatusIds::BAD_REQUEST,
                       MakeModifyAction(ITEM_NODE12_LOG_1));
        // OK
        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                       MakeRemoveAction(ITEM_NODE34_LOG_1),
                       MakeModifyAction(ITEM_NODE12_LOG_1));
        IncGeneration(ITEM_NODE12_LOG_1);

        // HOST ITEMS
        // Error: order conflict.
        CheckConfigure(runtime, Ydb::StatusIds::BAD_REQUEST,
                       MakeAddAction(ITEM_HOST12_LOG_1),
                       MakeAddAction(ITEM_HOST23_LOG_1));
        CheckConfigure(runtime, Ydb::StatusIds::BAD_REQUEST,
                       MakeAddAction(ITEM_HOST23_LOG_1),
                       MakeAddAction(ITEM_HOST34_LOG_1));
        // OK
        auto id2 = CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                                  MakeAddAction(ITEM_HOST12_LOG_1),
                                  MakeAddAction(ITEM_HOST34_LOG_1));
        AssignIds(id2, ITEM_HOST12_LOG_1, ITEM_HOST34_LOG_1);
        // Error: order conflict.
        ITEM_HOST12_LOG_1.MutableUsageScope()->MutableHostFilter()->AddHosts("host3");
        CheckConfigure(runtime, Ydb::StatusIds::BAD_REQUEST,
                       MakeModifyAction(ITEM_HOST12_LOG_1));
        // OK
        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                       MakeRemoveAction(ITEM_HOST34_LOG_1),
                       MakeModifyAction(ITEM_HOST12_LOG_1));
        IncGeneration(ITEM_HOST12_LOG_1);

        // TENANT ITEMS
        // Error: order conflict.
        CheckConfigure(runtime, Ydb::StatusIds::BAD_REQUEST,
                       MakeAddAction(ITEM_TENANT1_LOG_1),
                       MakeAddAction(ITEM_TENANT1_LOG_1));
        // OK
        auto id3 = CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                                  MakeAddAction(ITEM_TENANT1_LOG_1),
                                  MakeAddAction(ITEM_TENANT2_LOG_1));
        AssignIds(id3, ITEM_TENANT1_LOG_1, ITEM_TENANT2_LOG_1);
        // Error: order conflict.
        ITEM_TENANT2_LOG_1.MutableUsageScope()->MutableTenantAndNodeTypeFilter()->SetTenant("tenant1");
        CheckConfigure(runtime, Ydb::StatusIds::BAD_REQUEST,
                       MakeModifyAction(ITEM_TENANT2_LOG_1));
        // Error: order conflict.
        ITEM_TENANT2_LOG_1.MutableUsageScope()->MutableTenantAndNodeTypeFilter()->SetTenant("tenant1");
        CheckConfigure(runtime, Ydb::StatusIds::BAD_REQUEST,
                       MakeModifyAction(ITEM_TENANT2_LOG_1));
        // OK
        ITEM_TENANT1_LOG_1.SetOrder(2);
        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                       MakeModifyAction(ITEM_TENANT1_LOG_1),
                       MakeModifyAction(ITEM_TENANT2_LOG_1));
        IncGeneration(ITEM_TENANT1_LOG_1, ITEM_TENANT2_LOG_1);
        // OK
        ITEM_TENANT1_LOG_1.SetOrder(1);
        ITEM_TENANT2_LOG_1.MutableUsageScope()->MutableTenantAndNodeTypeFilter()->SetTenant("tenant2");
        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                       MakeModifyAction(ITEM_TENANT1_LOG_1),
                       MakeModifyAction(ITEM_TENANT2_LOG_1));
        IncGeneration(ITEM_TENANT1_LOG_1, ITEM_TENANT2_LOG_1);

        // NODE TYPE ITEMS
        // Error: order conflict.
        CheckConfigure(runtime, Ydb::StatusIds::BAD_REQUEST,
                       MakeAddAction(ITEM_TYPE1_LOG_1),
                       MakeAddAction(ITEM_TYPE1_LOG_1));
        // OK
        auto id4 = CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                                  MakeAddAction(ITEM_TYPE1_LOG_1),
                                  MakeAddAction(ITEM_TYPE2_LOG_1));
        AssignIds(id4, ITEM_TYPE1_LOG_1, ITEM_TYPE2_LOG_1);
        // Error: order conflict.
        ITEM_TYPE2_LOG_1.MutableUsageScope()->MutableTenantAndNodeTypeFilter()->SetNodeType("type1");
        CheckConfigure(runtime, Ydb::StatusIds::BAD_REQUEST,
                       MakeModifyAction(ITEM_TYPE2_LOG_1));
        // Error: order conflict.
        ITEM_TYPE2_LOG_1.MutableUsageScope()->MutableTenantAndNodeTypeFilter()->SetNodeType("type1");
        CheckConfigure(runtime, Ydb::StatusIds::BAD_REQUEST,
                       MakeModifyAction(ITEM_TYPE2_LOG_1));
        // OK
        ITEM_TYPE1_LOG_1.SetOrder(2);
        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                       MakeModifyAction(ITEM_TYPE1_LOG_1),
                       MakeModifyAction(ITEM_TYPE2_LOG_1));
        IncGeneration(ITEM_TYPE1_LOG_1, ITEM_TYPE2_LOG_1);
        // OK
        ITEM_TYPE1_LOG_1.SetOrder(1);
        ITEM_TYPE2_LOG_1.MutableUsageScope()->MutableTenantAndNodeTypeFilter()->SetNodeType("type2");
        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                       MakeModifyAction(ITEM_TYPE1_LOG_1),
                       MakeModifyAction(ITEM_TYPE2_LOG_1));
        IncGeneration(ITEM_TYPE1_LOG_1, ITEM_TYPE2_LOG_1);

        // TENANT AND NODE TYPE ITEMS
        // Error: order conflict.
        CheckConfigure(runtime, Ydb::StatusIds::BAD_REQUEST,
                       MakeAddAction(ITEM_TENANT1_TYPE1_LOG_1),
                       MakeAddAction(ITEM_TENANT1_TYPE1_LOG_1));
        // OK
        auto id5 = CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                                  MakeAddAction(ITEM_TENANT1_TYPE1_LOG_1),
                                  MakeAddAction(ITEM_TENANT1_TYPE2_LOG_1),
                                  MakeAddAction(ITEM_TENANT2_TYPE1_LOG_1),
                                  MakeAddAction(ITEM_TENANT2_TYPE2_LOG_1));
        AssignIds(id5, ITEM_TENANT1_TYPE1_LOG_1, ITEM_TENANT1_TYPE2_LOG_1,
                  ITEM_TENANT2_TYPE1_LOG_1, ITEM_TENANT2_TYPE2_LOG_1);
        // Error: order conflict
        ITEM_TENANT1_TYPE1_LOG_1.MutableUsageScope()->MutableTenantAndNodeTypeFilter()->SetNodeType("type2");
        CheckConfigure(runtime, Ydb::StatusIds::BAD_REQUEST,
                       MakeModifyAction(ITEM_TENANT1_TYPE1_LOG_1));
        // OK
        ITEM_TENANT1_TYPE2_LOG_1.SetOrder(2);
        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                       MakeModifyAction(ITEM_TENANT1_TYPE1_LOG_1),
                       MakeModifyAction(ITEM_TENANT1_TYPE2_LOG_1));
        IncGeneration(ITEM_TENANT1_TYPE1_LOG_1, ITEM_TENANT1_TYPE2_LOG_1);
        // OK
        ITEM_TENANT1_TYPE2_LOG_1.SetOrder(1);
        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                       MakeRemoveAction(ITEM_TENANT1_TYPE1_LOG_1),
                       MakeModifyAction(ITEM_TENANT1_TYPE2_LOG_1));
        IncGeneration(ITEM_TENANT1_TYPE2_LOG_1);

        // Check all items.
        CheckGetItems(runtime, TVector<ui32>(),
                      ITEM_NODE12_LOG_1, ITEM_HOST12_LOG_1,
                      ITEM_TENANT1_LOG_1, ITEM_TENANT2_LOG_1,
                      ITEM_TYPE1_LOG_1, ITEM_TYPE2_LOG_1,
                      ITEM_TENANT1_TYPE2_LOG_1,
                      ITEM_TENANT2_TYPE1_LOG_1, ITEM_TENANT2_TYPE2_LOG_1);
    }

    Y_UNIT_TEST(TestGetItems) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        InitializeTestConfigItems();

        auto id1 = CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                                  MakeAddAction(ITEM_DOMAIN_LOG_1),
                                  MakeAddAction(ITEM_DOMAIN_LOG_2),
                                  MakeAddAction(ITEM_DOMAIN_TENANT_POOL_1),
                                  MakeAddAction(ITEM_DOMAIN_TENANT_POOL_2));
        AssignIds(id1, ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_LOG_2,
                  ITEM_DOMAIN_TENANT_POOL_1, ITEM_DOMAIN_TENANT_POOL_2);

        CheckGetItems(runtime, TVector<ui32>(),
                      ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_LOG_2,
                      ITEM_DOMAIN_TENANT_POOL_1, ITEM_DOMAIN_TENANT_POOL_2);
        CheckGetItems(runtime, TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}),
                      ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_LOG_2);
        CheckGetItems(runtime, TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::TenantPoolConfigItem}),
                      ITEM_DOMAIN_TENANT_POOL_1, ITEM_DOMAIN_TENANT_POOL_2);
        CheckGetItems(runtime, TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem,
                                              (ui32)NKikimrConsole::TConfigItem::TenantPoolConfigItem}),
                      ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_LOG_2,
                      ITEM_DOMAIN_TENANT_POOL_1, ITEM_DOMAIN_TENANT_POOL_2);
        CheckGetItems(runtime, TVector<ui32>(NKikimrConsole::TConfigItem::ActorSystemConfigItem));

        ITEM_NODE23_LOG_1.SetOrder(2);
        ITEM_HOST23_LOG_1.SetOrder(2);
        auto id2 = CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                                  MakeAddAction(ITEM_NODE12_LOG_1), MakeAddAction(ITEM_NODE23_LOG_1),
                                  MakeAddAction(ITEM_NODE34_LOG_1), MakeAddAction(ITEM_HOST12_LOG_1),
                                  MakeAddAction(ITEM_HOST23_LOG_1), MakeAddAction(ITEM_HOST34_LOG_1),
                                  MakeAddAction(ITEM_TENANT1_LOG_1), MakeAddAction(ITEM_TENANT1_LOG_2),
                                  MakeAddAction(ITEM_TENANT2_LOG_1), MakeAddAction(ITEM_TENANT2_LOG_2),
                                  MakeAddAction(ITEM_TYPE1_LOG_1), MakeAddAction(ITEM_TYPE1_LOG_2),
                                  MakeAddAction(ITEM_TYPE2_LOG_1), MakeAddAction(ITEM_TYPE2_LOG_2),
                                  MakeAddAction(ITEM_TENANT1_TYPE1_LOG_1), MakeAddAction(ITEM_TENANT1_TYPE1_LOG_2),
                                  MakeAddAction(ITEM_TENANT1_TYPE2_LOG_1), MakeAddAction(ITEM_TENANT1_TYPE2_LOG_2),
                                  MakeAddAction(ITEM_TENANT2_TYPE1_LOG_1), MakeAddAction(ITEM_TENANT2_TYPE1_LOG_2),
                                  MakeAddAction(ITEM_TENANT2_TYPE2_LOG_1), MakeAddAction(ITEM_TENANT2_TYPE2_LOG_2));
        AssignIds(id2, ITEM_NODE12_LOG_1, ITEM_NODE23_LOG_1,
                  ITEM_NODE34_LOG_1, ITEM_HOST12_LOG_1,
                  ITEM_HOST23_LOG_1, ITEM_HOST34_LOG_1,
                  ITEM_TENANT1_LOG_1, ITEM_TENANT1_LOG_2,
                  ITEM_TENANT2_LOG_1, ITEM_TENANT2_LOG_2,
                  ITEM_TYPE1_LOG_1, ITEM_TYPE1_LOG_2,
                  ITEM_TYPE2_LOG_1, ITEM_TYPE2_LOG_2,
                  ITEM_TENANT1_TYPE1_LOG_1, ITEM_TENANT1_TYPE1_LOG_2,
                  ITEM_TENANT1_TYPE2_LOG_1, ITEM_TENANT1_TYPE2_LOG_2,
                  ITEM_TENANT2_TYPE1_LOG_1, ITEM_TENANT2_TYPE1_LOG_2,
                  ITEM_TENANT2_TYPE2_LOG_1, ITEM_TENANT2_TYPE2_LOG_2);

        CheckGetItemsById(runtime, TVector<ui64>({id2[0]}),
                          TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}),
                          ITEM_NODE12_LOG_1);
        CheckGetItemsById(runtime, TVector<ui64>({id2[1]}),
                          TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}),
                          ITEM_NODE23_LOG_1);
        CheckGetItemsById(runtime, TVector<ui64>({id2[0], id2[0]}),
                          TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}),
                          ITEM_NODE12_LOG_1);
        CheckGetItemsById(runtime, TVector<ui64>({id2[0], id2[1]}),
                          TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}),
                          ITEM_NODE12_LOG_1, ITEM_NODE23_LOG_1);
        CheckGetItemsById(runtime, TVector<ui64>({id2[1]}),
                          TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::ActorSystemConfigItem}));
        CheckGetItemsById(runtime, TVector<ui64>({987654321}),
                          TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}));

        CheckGetItemsByNodeId(runtime, TVector<ui32>({1}),
                              TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}),
                              ITEM_NODE12_LOG_1);
        CheckGetItemsByNodeId(runtime, TVector<ui32>({2}),
                              TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}),
                              ITEM_NODE12_LOG_1, ITEM_NODE23_LOG_1);
        CheckGetItemsByNodeId(runtime, TVector<ui32>({3}),
                              TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}),
                              ITEM_NODE23_LOG_1, ITEM_NODE34_LOG_1);
        CheckGetItemsByNodeId(runtime, TVector<ui32>({4}),
                              TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}),
                              ITEM_NODE34_LOG_1);
        CheckGetItemsByNodeId(runtime, TVector<ui32>({1, 1}),
                              TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}),
                              ITEM_NODE12_LOG_1);
        CheckGetItemsByNodeId(runtime, TVector<ui32>({1, 2}),
                              TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}),
                              ITEM_NODE12_LOG_1, ITEM_NODE23_LOG_1);
        CheckGetItemsByNodeId(runtime, TVector<ui32>({1, 2, 3}),
                              TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}),
                              ITEM_NODE12_LOG_1, ITEM_NODE23_LOG_1, ITEM_NODE34_LOG_1);
        CheckGetItemsByNodeId(runtime, TVector<ui32>({1, 2, 3, 4}),
                              TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}),
                              ITEM_NODE12_LOG_1, ITEM_NODE23_LOG_1, ITEM_NODE34_LOG_1);
        CheckGetItemsByNodeId(runtime, TVector<ui32>({2, 4, 5}),
                              TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}),
                              ITEM_NODE12_LOG_1, ITEM_NODE23_LOG_1, ITEM_NODE34_LOG_1);
        CheckGetItemsByNodeId(runtime, TVector<ui32>({}),
                              TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}),
                              ITEM_NODE12_LOG_1, ITEM_NODE23_LOG_1, ITEM_NODE34_LOG_1);
        CheckGetItemsByNodeId(runtime, TVector<ui32>({4}),
                              TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::ActorSystemConfigItem}));
        CheckGetItemsByNodeId(runtime, TVector<ui32>({}),
                              TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::ActorSystemConfigItem}));
        CheckGetItemsByNodeId(runtime, TVector<ui32>({5}),
                              TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}));

        CheckGetItemsByHost(runtime, TVector<TString>({"host1"}),
                            TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}),
                            ITEM_HOST12_LOG_1);
        CheckGetItemsByHost(runtime, TVector<TString>({"host2"}),
                            TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}),
                            ITEM_HOST12_LOG_1, ITEM_HOST23_LOG_1);
        CheckGetItemsByHost(runtime, TVector<TString>({"host3"}),
                            TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}),
                            ITEM_HOST23_LOG_1, ITEM_HOST34_LOG_1);
        CheckGetItemsByHost(runtime, TVector<TString>({"host4"}),
                            TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}),
                            ITEM_HOST34_LOG_1);
        CheckGetItemsByHost(runtime, TVector<TString>({"host1", "host1"}),
                            TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}),
                            ITEM_HOST12_LOG_1);
        CheckGetItemsByHost(runtime, TVector<TString>({"host1", "host2"}),
                            TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}),
                            ITEM_HOST12_LOG_1, ITEM_HOST23_LOG_1);
        CheckGetItemsByHost(runtime, TVector<TString>({"host1", "host2", "host3"}),
                            TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}),
                            ITEM_HOST12_LOG_1, ITEM_HOST23_LOG_1, ITEM_HOST34_LOG_1);
        CheckGetItemsByHost(runtime, TVector<TString>({"host1", "host2", "host3", "host4"}),
                            TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}),
                            ITEM_HOST12_LOG_1, ITEM_HOST23_LOG_1, ITEM_HOST34_LOG_1);
        CheckGetItemsByHost(runtime, TVector<TString>({"host2", "host3", "host5"}),
                            TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}),
                            ITEM_HOST12_LOG_1, ITEM_HOST23_LOG_1, ITEM_HOST34_LOG_1);
        CheckGetItemsByHost(runtime, TVector<TString>({}),
                            TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}),
                            ITEM_HOST12_LOG_1, ITEM_HOST23_LOG_1, ITEM_HOST34_LOG_1);
        CheckGetItemsByHost(runtime, TVector<TString>({"host4"}),
                            TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::ActorSystemConfigItem}));
        CheckGetItemsByHost(runtime, TVector<TString>({}),
                            TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::ActorSystemConfigItem}));
        CheckGetItemsByHost(runtime, TVector<TString>({"host5"}),
                            TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}));

        CheckGetItemsByTenant(runtime, TVector<TString>({"tenant1"}),
                              TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}),
                              ITEM_TENANT1_LOG_1, ITEM_TENANT1_LOG_2,
                              ITEM_TENANT1_TYPE1_LOG_1, ITEM_TENANT1_TYPE1_LOG_2,
                              ITEM_TENANT1_TYPE2_LOG_1, ITEM_TENANT1_TYPE2_LOG_2);
        CheckGetItemsByTenant(runtime, TVector<TString>({"tenant2"}),
                              TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}),
                              ITEM_TENANT2_LOG_1, ITEM_TENANT2_LOG_2,
                              ITEM_TENANT2_TYPE1_LOG_1, ITEM_TENANT2_TYPE1_LOG_2,
                              ITEM_TENANT2_TYPE2_LOG_1, ITEM_TENANT2_TYPE2_LOG_2);
        CheckGetItemsByTenant(runtime, TVector<TString>({"tenant1", "tenant1"}),
                              TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}),
                              ITEM_TENANT1_LOG_1, ITEM_TENANT1_LOG_2,
                              ITEM_TENANT1_TYPE1_LOG_1, ITEM_TENANT1_TYPE1_LOG_2,
                              ITEM_TENANT1_TYPE2_LOG_1, ITEM_TENANT1_TYPE2_LOG_2);
        CheckGetItemsByTenant(runtime, TVector<TString>({"tenant1", "tenant2"}),
                              TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}),
                              ITEM_TENANT1_LOG_1, ITEM_TENANT1_LOG_2,
                              ITEM_TENANT1_TYPE1_LOG_1, ITEM_TENANT1_TYPE1_LOG_2,
                              ITEM_TENANT1_TYPE2_LOG_1, ITEM_TENANT1_TYPE2_LOG_2,
                              ITEM_TENANT2_LOG_1, ITEM_TENANT2_LOG_2,
                              ITEM_TENANT2_TYPE1_LOG_1, ITEM_TENANT2_TYPE1_LOG_2,
                              ITEM_TENANT2_TYPE2_LOG_1, ITEM_TENANT2_TYPE2_LOG_2);
        CheckGetItemsByTenant(runtime, TVector<TString>({}),
                              TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}),
                              ITEM_TENANT1_LOG_1, ITEM_TENANT1_LOG_2,
                              ITEM_TENANT1_TYPE1_LOG_1, ITEM_TENANT1_TYPE1_LOG_2,
                              ITEM_TENANT1_TYPE2_LOG_1, ITEM_TENANT1_TYPE2_LOG_2,
                              ITEM_TENANT2_LOG_1, ITEM_TENANT2_LOG_2,
                              ITEM_TENANT2_TYPE1_LOG_1, ITEM_TENANT2_TYPE1_LOG_2,
                              ITEM_TENANT2_TYPE2_LOG_1, ITEM_TENANT2_TYPE2_LOG_2);
        CheckGetItemsByTenant(runtime, TVector<TString>({"tenant2"}),
                              TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::ActorSystemConfigItem}));
        CheckGetItemsByTenant(runtime, TVector<TString>({}),
                              TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::ActorSystemConfigItem}));
        CheckGetItemsByTenant(runtime, TVector<TString>({"tenant3"}),
                              TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}));

        CheckGetItemsByNodeType(runtime, TVector<TString>({"type1"}),
                                TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}),
                                ITEM_TYPE1_LOG_1, ITEM_TYPE1_LOG_2,
                                ITEM_TENANT1_TYPE1_LOG_1, ITEM_TENANT1_TYPE1_LOG_2,
                                ITEM_TENANT2_TYPE1_LOG_1, ITEM_TENANT2_TYPE1_LOG_2);
        CheckGetItemsByNodeType(runtime, TVector<TString>({"type2"}),
                                TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}),
                                ITEM_TYPE2_LOG_1, ITEM_TYPE2_LOG_2,
                                ITEM_TENANT1_TYPE2_LOG_1, ITEM_TENANT1_TYPE2_LOG_2,
                                ITEM_TENANT2_TYPE2_LOG_1, ITEM_TENANT2_TYPE2_LOG_2);
        CheckGetItemsByNodeType(runtime, TVector<TString>({"type1", "type1"}),
                                TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}),
                                ITEM_TYPE1_LOG_1, ITEM_TYPE1_LOG_2,
                                ITEM_TENANT1_TYPE1_LOG_1, ITEM_TENANT1_TYPE1_LOG_2,
                                ITEM_TENANT2_TYPE1_LOG_1, ITEM_TENANT2_TYPE1_LOG_2);
        CheckGetItemsByNodeType(runtime, TVector<TString>({"type1", "type2"}),
                                TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}),
                                ITEM_TYPE1_LOG_1, ITEM_TYPE1_LOG_2,
                                ITEM_TENANT1_TYPE1_LOG_1, ITEM_TENANT1_TYPE1_LOG_2,
                                ITEM_TENANT2_TYPE1_LOG_1, ITEM_TENANT2_TYPE1_LOG_2,
                                ITEM_TYPE2_LOG_1, ITEM_TYPE2_LOG_2,
                                ITEM_TENANT1_TYPE2_LOG_1, ITEM_TENANT1_TYPE2_LOG_2,
                                ITEM_TENANT2_TYPE2_LOG_1, ITEM_TENANT2_TYPE2_LOG_2);
        CheckGetItemsByNodeType(runtime, TVector<TString>({}),
                                TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}),
                                ITEM_TYPE1_LOG_1, ITEM_TYPE1_LOG_2,
                                ITEM_TENANT1_TYPE1_LOG_1, ITEM_TENANT1_TYPE1_LOG_2,
                                ITEM_TENANT2_TYPE1_LOG_1, ITEM_TENANT2_TYPE1_LOG_2,
                                ITEM_TYPE2_LOG_1, ITEM_TYPE2_LOG_2,
                                ITEM_TENANT1_TYPE2_LOG_1, ITEM_TENANT1_TYPE2_LOG_2,
                                ITEM_TENANT2_TYPE2_LOG_1, ITEM_TENANT2_TYPE2_LOG_2);
        CheckGetItemsByNodeType(runtime, TVector<TString>({"type2"}),
                                TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::ActorSystemConfigItem}));
        CheckGetItemsByNodeType(runtime, TVector<TString>({}),
                                TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::ActorSystemConfigItem}));
        CheckGetItemsByNodeType(runtime, TVector<TString>({"type3"}),
                                TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}));

        CheckGetItemsByTenantAndNodeType(runtime, TVector<std::pair<TString, TString>>({std::make_pair("tenant1", "type1")}),
                                         TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}),
                                         ITEM_TENANT1_TYPE1_LOG_1, ITEM_TENANT1_TYPE1_LOG_2);
        CheckGetItemsByTenantAndNodeType(runtime, TVector<std::pair<TString, TString>>({std::make_pair("tenant1", "type2")}),
                                         TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}),
                                         ITEM_TENANT1_TYPE2_LOG_1, ITEM_TENANT1_TYPE2_LOG_2);
        CheckGetItemsByTenantAndNodeType(runtime, TVector<std::pair<TString, TString>>({std::make_pair("tenant2", "type1")}),
                                         TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}),
                                         ITEM_TENANT2_TYPE1_LOG_1, ITEM_TENANT2_TYPE1_LOG_2);
        CheckGetItemsByTenantAndNodeType(runtime, TVector<std::pair<TString, TString>>({std::make_pair("tenant2", "type2")}),
                                         TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}),
                                         ITEM_TENANT2_TYPE2_LOG_1, ITEM_TENANT2_TYPE2_LOG_2);
        CheckGetItemsByTenantAndNodeType(runtime, TVector<std::pair<TString, TString>>({std::make_pair("tenant1", "type1"),
                                                                                        std::make_pair("tenant1", "type1")}),
                                         TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}),
                                         ITEM_TENANT1_TYPE1_LOG_1, ITEM_TENANT1_TYPE1_LOG_2);
        CheckGetItemsByTenantAndNodeType(runtime, TVector<std::pair<TString, TString>>({std::make_pair("tenant1", "type1"),
                                                                                        std::make_pair("tenant1", "type2")}),
                                         TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}),
                                         ITEM_TENANT1_TYPE1_LOG_1, ITEM_TENANT1_TYPE1_LOG_2,
                                         ITEM_TENANT1_TYPE2_LOG_1, ITEM_TENANT1_TYPE2_LOG_2);
        CheckGetItemsByTenantAndNodeType(runtime, TVector<std::pair<TString, TString>>({std::make_pair("tenant1", "type1"),
                                                                                        std::make_pair("tenant2", "type2")}),
                                         TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}),
                                         ITEM_TENANT1_TYPE1_LOG_1, ITEM_TENANT1_TYPE1_LOG_2,
                                         ITEM_TENANT2_TYPE2_LOG_1, ITEM_TENANT2_TYPE2_LOG_2);
        CheckGetItemsByTenantAndNodeType(runtime, TVector<std::pair<TString, TString>>({}),
                                         TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}),
                                         ITEM_TENANT1_TYPE1_LOG_1, ITEM_TENANT1_TYPE1_LOG_2,
                                         ITEM_TENANT1_TYPE2_LOG_1, ITEM_TENANT1_TYPE2_LOG_2,
                                         ITEM_TENANT2_TYPE1_LOG_1, ITEM_TENANT2_TYPE1_LOG_2,
                                         ITEM_TENANT2_TYPE2_LOG_1, ITEM_TENANT2_TYPE2_LOG_2);
        CheckGetItemsByTenantAndNodeType(runtime, TVector<std::pair<TString, TString>>({std::make_pair("tenant2", "type2")}),
                                         TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::ActorSystemConfigItem}));
        CheckGetItemsByTenantAndNodeType(runtime, TVector<std::pair<TString, TString>>({}),
                                         TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::ActorSystemConfigItem}));
        CheckGetItemsByTenantAndNodeType(runtime, TVector<std::pair<TString, TString>>({std::make_pair("tenant1", "type3")}),
                                         TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}));
        CheckGetItemsByTenantAndNodeType(runtime, TVector<std::pair<TString, TString>>({std::make_pair("tenant3", "type2")}),
                                         TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}));

        CheckGetItemsByUsageScope(runtime, MakeUsageScope(TVector<ui32>({1, 2})), TVector<ui32>(),
                                  ITEM_NODE12_LOG_1);
        CheckGetItemsByUsageScope(runtime, MakeUsageScope(TVector<ui32>({2, 3})), TVector<ui32>(),
                                  ITEM_NODE23_LOG_1);
        CheckGetItemsByUsageScope(runtime, MakeUsageScope(TVector<ui32>({3, 4})), TVector<ui32>(),
                                  ITEM_NODE34_LOG_1);
        CheckGetItemsByUsageScope(runtime, MakeUsageScope(TVector<ui32>({3, 4})),
                                  TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::ActorSystemConfigItem}));
        CheckGetItemsByUsageScope(runtime, MakeUsageScope(TVector<ui32>({})), TVector<ui32>());
        CheckGetItemsByUsageScope(runtime, MakeUsageScope(TVector<ui32>({1})), TVector<ui32>());
        CheckGetItemsByUsageScope(runtime, MakeUsageScope(TVector<ui32>({2})), TVector<ui32>());
        CheckGetItemsByUsageScope(runtime, MakeUsageScope(TVector<ui32>({3})), TVector<ui32>());
        CheckGetItemsByUsageScope(runtime, MakeUsageScope(TVector<ui32>({1, 2, 3})), TVector<ui32>());

        CheckGetItemsByUsageScope(runtime, MakeUsageScope(TVector<TString>({TString("host1"), TString("host2")})),
                                  TVector<ui32>(),
                                  ITEM_HOST12_LOG_1);
        CheckGetItemsByUsageScope(runtime, MakeUsageScope(TVector<TString>({TString("host2"), TString("host3")})),
                                  TVector<ui32>(),
                                  ITEM_HOST23_LOG_1);
        CheckGetItemsByUsageScope(runtime, MakeUsageScope(TVector<TString>({TString("host3"), TString("host4")})),
                                  TVector<ui32>(),
                                  ITEM_HOST34_LOG_1);
        CheckGetItemsByUsageScope(runtime, MakeUsageScope(TVector<TString>({TString("host3"), TString("host4")})),
                                  TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::ActorSystemConfigItem}));
        CheckGetItemsByUsageScope(runtime, MakeUsageScope(TVector<TString>()), TVector<ui32>());
        CheckGetItemsByUsageScope(runtime, MakeUsageScope(TVector<TString>({TString("host1")})), TVector<ui32>());
        CheckGetItemsByUsageScope(runtime, MakeUsageScope(TVector<TString>({TString("host2")})), TVector<ui32>());
        CheckGetItemsByUsageScope(runtime, MakeUsageScope(TVector<TString>({TString("host3")})), TVector<ui32>());
        CheckGetItemsByUsageScope(runtime, MakeUsageScope(TVector<TString>({TString("host1"), TString("host2"), TString("host3")})),
                                  TVector<ui32>());

        CheckGetItemsByUsageScope(runtime, MakeUsageScope("tenant1", ""), TVector<ui32>(),
                                  ITEM_TENANT1_LOG_1, ITEM_TENANT1_LOG_2);
        CheckGetItemsByUsageScope(runtime, MakeUsageScope("tenant2", ""), TVector<ui32>(),
                                  ITEM_TENANT2_LOG_1, ITEM_TENANT2_LOG_2);
        CheckGetItemsByUsageScope(runtime, MakeUsageScope("tenant2", ""),
                                  TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::ActorSystemConfigItem}));
        CheckGetItemsByUsageScope(runtime, MakeUsageScope("tenant3", ""), TVector<ui32>());
        CheckGetItemsByUsageScope(runtime, MakeUsageScope("", "type1"), TVector<ui32>(),
                                  ITEM_TYPE1_LOG_1, ITEM_TYPE1_LOG_2);
        CheckGetItemsByUsageScope(runtime, MakeUsageScope("", "type2"), TVector<ui32>(),
                                  ITEM_TYPE2_LOG_1, ITEM_TYPE2_LOG_2);
        CheckGetItemsByUsageScope(runtime, MakeUsageScope("", "type2"),
                                  TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::ActorSystemConfigItem}));
        CheckGetItemsByUsageScope(runtime, MakeUsageScope("", "type3"), TVector<ui32>());
        CheckGetItemsByUsageScope(runtime, MakeUsageScope("tenant1", "type1"), TVector<ui32>(),
                                  ITEM_TENANT1_TYPE1_LOG_1, ITEM_TENANT1_TYPE1_LOG_2);
        CheckGetItemsByUsageScope(runtime, MakeUsageScope("tenant1", "type2"), TVector<ui32>(),
                                  ITEM_TENANT1_TYPE2_LOG_1, ITEM_TENANT1_TYPE2_LOG_2);
        CheckGetItemsByUsageScope(runtime, MakeUsageScope("tenant2", "type1"), TVector<ui32>(),
                                  ITEM_TENANT2_TYPE1_LOG_1, ITEM_TENANT2_TYPE1_LOG_2);
        CheckGetItemsByUsageScope(runtime, MakeUsageScope("tenant2", "type2"), TVector<ui32>(),
                                  ITEM_TENANT2_TYPE2_LOG_1, ITEM_TENANT2_TYPE2_LOG_2);
        CheckGetItemsByUsageScope(runtime, MakeUsageScope("tenant2", "type2"),
                                  TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::ActorSystemConfigItem}));
        CheckGetItemsByUsageScope(runtime, MakeUsageScope("tenant3", "type1"), TVector<ui32>());
        CheckGetItemsByUsageScope(runtime, MakeUsageScope("tenant1", "type3"), TVector<ui32>());

        CheckGetItemsByUsageScope(runtime, MakeUsageScope("", ""), TVector<ui32>(),
                                  ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_LOG_2,
                                  ITEM_DOMAIN_TENANT_POOL_1, ITEM_DOMAIN_TENANT_POOL_2);
        CheckGetItemsByUsageScope(runtime, MakeUsageScope("", ""),
                                  TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}),
                                  ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_LOG_2);
        CheckGetItemsByUsageScope(runtime, MakeUsageScope("", ""),
                                  TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::ActorSystemConfigItem}));
        CheckGetItemsByUsageScope(runtime, NKikimrConsole::TUsageScope(), TVector<ui32>(),
                                  ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_LOG_2,
                                  ITEM_DOMAIN_TENANT_POOL_1, ITEM_DOMAIN_TENANT_POOL_2);
        CheckGetItemsByUsageScope(runtime, NKikimrConsole::TUsageScope(),
                                  TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}),
                                  ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_LOG_2);
        CheckGetItemsByUsageScope(runtime, NKikimrConsole::TUsageScope(),
                                  TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::ActorSystemConfigItem}));

        auto scope1 = MakeUsageScope(TVector<ui32>({1, 2}));
        auto scope2 = MakeUsageScope(TVector<TString>({TString("host1"), TString("host2")}));
        auto scope3 = MakeUsageScope("tenant1", "");
        auto scope4 = MakeUsageScope("", "type1");
        auto scope5 = MakeUsageScope("tenant1", "type1");
        auto scope6 = MakeUsageScope("", "");
        CheckGetItemsByUsageScope(runtime, TVector<NKikimrConsole::TUsageScope>({scope1, scope2, scope3, scope4, scope5, scope6}),
                                  TVector<ui32>(),
                                  ITEM_NODE12_LOG_1, ITEM_HOST12_LOG_1,
                                  ITEM_TENANT1_LOG_1, ITEM_TENANT1_LOG_2,
                                  ITEM_TYPE1_LOG_1, ITEM_TYPE1_LOG_2,
                                  ITEM_TENANT1_TYPE1_LOG_1, ITEM_TENANT1_TYPE1_LOG_2,
                                  ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_LOG_2,
                                  ITEM_DOMAIN_TENANT_POOL_1, ITEM_DOMAIN_TENANT_POOL_2);
        CheckGetItemsByUsageScope(runtime, TVector<NKikimrConsole::TUsageScope>({scope1, scope2, scope3, scope4, scope5, scope6}),
                                  TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}),
                                  ITEM_NODE12_LOG_1, ITEM_HOST12_LOG_1,
                                  ITEM_TENANT1_LOG_1, ITEM_TENANT1_LOG_2,
                                  ITEM_TYPE1_LOG_1, ITEM_TYPE1_LOG_2,
                                  ITEM_TENANT1_TYPE1_LOG_1, ITEM_TENANT1_TYPE1_LOG_2,
                                  ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_LOG_2);

        CheckGetItemsByCookie(runtime, TVector<TString>({"cookie1"}),
                              TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}),
                              ITEM_NODE12_LOG_1, ITEM_NODE23_LOG_1,
                              ITEM_NODE34_LOG_1, ITEM_HOST12_LOG_1,
                              ITEM_HOST23_LOG_1, ITEM_HOST34_LOG_1,
                              ITEM_TENANT1_LOG_1, ITEM_TENANT2_LOG_1,
                              ITEM_TYPE1_LOG_1, ITEM_TYPE2_LOG_1,
                              ITEM_TENANT1_TYPE1_LOG_1, ITEM_TENANT1_TYPE2_LOG_1,
                              ITEM_TENANT2_TYPE1_LOG_1, ITEM_TENANT2_TYPE2_LOG_1);
        CheckGetItemsByCookie(runtime, TVector<TString>({"cookie2"}),
                              TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}),
                              ITEM_TENANT1_LOG_2, ITEM_TENANT2_LOG_2,
                              ITEM_TYPE1_LOG_2, ITEM_TYPE2_LOG_2,
                              ITEM_TENANT1_TYPE1_LOG_2, ITEM_TENANT1_TYPE2_LOG_2,
                              ITEM_TENANT2_TYPE1_LOG_2, ITEM_TENANT2_TYPE2_LOG_2);
        CheckGetItemsByCookie(runtime, TVector<TString>({""}),
                              TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}),
                              ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_LOG_2);
        CheckGetItemsByCookie(runtime, TVector<TString>({"cookie3"}),
                              TVector<ui32>());

        CheckGetItems(runtime, TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}),
                      ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_LOG_2,
                      ITEM_NODE12_LOG_1, ITEM_NODE23_LOG_1,
                      ITEM_NODE34_LOG_1, ITEM_HOST12_LOG_1,
                      ITEM_HOST23_LOG_1, ITEM_HOST34_LOG_1,
                      ITEM_TENANT1_LOG_1, ITEM_TENANT1_LOG_2,
                      ITEM_TENANT2_LOG_1, ITEM_TENANT2_LOG_2,
                      ITEM_TYPE1_LOG_1, ITEM_TYPE1_LOG_2,
                      ITEM_TYPE2_LOG_1, ITEM_TYPE2_LOG_2,
                      ITEM_TENANT1_TYPE1_LOG_1, ITEM_TENANT1_TYPE1_LOG_2,
                      ITEM_TENANT1_TYPE2_LOG_1, ITEM_TENANT1_TYPE2_LOG_2,
                      ITEM_TENANT2_TYPE1_LOG_1, ITEM_TENANT2_TYPE1_LOG_2,
                      ITEM_TENANT2_TYPE2_LOG_1, ITEM_TENANT2_TYPE2_LOG_2);
        CheckGetItems(runtime, TVector<ui32>(),
                      ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_LOG_2,
                      ITEM_DOMAIN_TENANT_POOL_1, ITEM_DOMAIN_TENANT_POOL_2,
                      ITEM_NODE12_LOG_1, ITEM_NODE23_LOG_1,
                      ITEM_NODE34_LOG_1, ITEM_HOST12_LOG_1,
                      ITEM_HOST23_LOG_1, ITEM_HOST34_LOG_1,
                      ITEM_TENANT1_LOG_1, ITEM_TENANT1_LOG_2,
                      ITEM_TENANT2_LOG_1, ITEM_TENANT2_LOG_2,
                      ITEM_TYPE1_LOG_1, ITEM_TYPE1_LOG_2,
                      ITEM_TYPE2_LOG_1, ITEM_TYPE2_LOG_2,
                      ITEM_TENANT1_TYPE1_LOG_1, ITEM_TENANT1_TYPE1_LOG_2,
                      ITEM_TENANT1_TYPE2_LOG_1, ITEM_TENANT1_TYPE2_LOG_2,
                      ITEM_TENANT2_TYPE1_LOG_1, ITEM_TENANT2_TYPE1_LOG_2,
                      ITEM_TENANT2_TYPE2_LOG_1, ITEM_TENANT2_TYPE2_LOG_2);
    }

    Y_UNIT_TEST(TestGetNodeItems) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        InitializeTestConfigItems();

        ITEM_NODE23_LOG_1.SetOrder(2);
        ITEM_HOST23_LOG_1.SetOrder(2);
        auto id1 = CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                                  MakeAddAction(ITEM_DOMAIN_LOG_1), MakeAddAction(ITEM_DOMAIN_LOG_2),
                                  MakeAddAction(ITEM_NODE12_LOG_1), MakeAddAction(ITEM_NODE23_LOG_1),
                                  MakeAddAction(ITEM_NODE34_LOG_1), MakeAddAction(ITEM_HOST12_LOG_1),
                                  MakeAddAction(ITEM_HOST23_LOG_1), MakeAddAction(ITEM_HOST34_LOG_1),
                                  MakeAddAction(ITEM_TENANT1_LOG_1), MakeAddAction(ITEM_TENANT1_LOG_2),
                                  MakeAddAction(ITEM_TENANT2_LOG_1), MakeAddAction(ITEM_TENANT2_LOG_2),
                                  MakeAddAction(ITEM_TYPE1_LOG_1), MakeAddAction(ITEM_TYPE1_LOG_2),
                                  MakeAddAction(ITEM_TYPE2_LOG_1), MakeAddAction(ITEM_TYPE2_LOG_2),
                                  MakeAddAction(ITEM_TENANT1_TYPE1_LOG_1), MakeAddAction(ITEM_TENANT1_TYPE1_LOG_2),
                                  MakeAddAction(ITEM_TENANT1_TYPE2_LOG_1), MakeAddAction(ITEM_TENANT1_TYPE2_LOG_2),
                                  MakeAddAction(ITEM_TENANT2_TYPE1_LOG_1), MakeAddAction(ITEM_TENANT2_TYPE1_LOG_2),
                                  MakeAddAction(ITEM_TENANT2_TYPE2_LOG_1), MakeAddAction(ITEM_TENANT2_TYPE2_LOG_2));
        AssignIds(id1, ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_LOG_2,
                  ITEM_NODE12_LOG_1, ITEM_NODE23_LOG_1,
                  ITEM_NODE34_LOG_1, ITEM_HOST12_LOG_1,
                  ITEM_HOST23_LOG_1, ITEM_HOST34_LOG_1,
                  ITEM_TENANT1_LOG_1, ITEM_TENANT1_LOG_2,
                  ITEM_TENANT2_LOG_1, ITEM_TENANT2_LOG_2,
                  ITEM_TYPE1_LOG_1, ITEM_TYPE1_LOG_2,
                  ITEM_TYPE2_LOG_1, ITEM_TYPE2_LOG_2,
                  ITEM_TENANT1_TYPE1_LOG_1, ITEM_TENANT1_TYPE1_LOG_2,
                  ITEM_TENANT1_TYPE2_LOG_1, ITEM_TENANT1_TYPE2_LOG_2,
                  ITEM_TENANT2_TYPE1_LOG_1, ITEM_TENANT2_TYPE1_LOG_2,
                  ITEM_TENANT2_TYPE2_LOG_1, ITEM_TENANT2_TYPE2_LOG_2);

        CheckGetNodeItems(runtime, 1, "host1", "tenant1", "type1", TVector<ui32>(),
                          ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_LOG_2,
                          ITEM_NODE12_LOG_1, ITEM_HOST12_LOG_1,
                          ITEM_TENANT1_LOG_1, ITEM_TENANT1_LOG_2,
                          ITEM_TYPE1_LOG_1, ITEM_TYPE1_LOG_2,
                          ITEM_TENANT1_TYPE1_LOG_1, ITEM_TENANT1_TYPE1_LOG_2);
        CheckGetNodeItems(runtime, 1, "host1", "tenant1", "type1",
                          TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::ActorSystemConfigItem}));
        CheckGetNodeItems(runtime, 2, "host1", "tenant1", "type1", TVector<ui32>(),
                          ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_LOG_2,
                          ITEM_NODE12_LOG_1, ITEM_NODE23_LOG_1, ITEM_HOST12_LOG_1,
                          ITEM_TENANT1_LOG_1, ITEM_TENANT1_LOG_2,
                          ITEM_TYPE1_LOG_1, ITEM_TYPE1_LOG_2,
                          ITEM_TENANT1_TYPE1_LOG_1, ITEM_TENANT1_TYPE1_LOG_2);
        CheckGetNodeItems(runtime, 5, "host2", "tenant1", "type1", TVector<ui32>(),
                          ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_LOG_2,
                          ITEM_HOST12_LOG_1, ITEM_HOST23_LOG_1,
                          ITEM_TENANT1_LOG_1, ITEM_TENANT1_LOG_2,
                          ITEM_TYPE1_LOG_1, ITEM_TYPE1_LOG_2,
                          ITEM_TENANT1_TYPE1_LOG_1, ITEM_TENANT1_TYPE1_LOG_2);
        CheckGetNodeItems(runtime, 5, "host4", "tenant2", "", TVector<ui32>(),
                          ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_LOG_2,
                          ITEM_HOST34_LOG_1,
                          ITEM_TENANT2_LOG_1, ITEM_TENANT2_LOG_2);
        CheckGetNodeItems(runtime, 5, "host5", "tenant2", "type3", TVector<ui32>(),
                          ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_LOG_2,
                          ITEM_TENANT2_LOG_1, ITEM_TENANT2_LOG_2);
        CheckGetNodeItems(runtime, 5, "host5", "", "type2", TVector<ui32>(),
                          ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_LOG_2,
                          ITEM_TYPE2_LOG_1, ITEM_TYPE2_LOG_2);
        CheckGetNodeItems(runtime, 5, "host5", "", "type3", TVector<ui32>(),
                          ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_LOG_2);
    }

    Y_UNIT_TEST(TestGetNodeConfig) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        InitializeTestConfigItems();

        ITEM_DOMAIN_LOG_1.MutableConfig()->MutableLogConfig()->SetClusterName("cluster1");
        ITEM_DOMAIN_LOG_1.MutableConfig()->MutableLogConfig()->SetBackendFileName("file1");
        ITEM_DOMAIN_LOG_2.MutableConfig()->MutableLogConfig()->SetBackendFileName("file2");
        ITEM_TYPE1_LOG_1.MutableConfig()->MutableLogConfig()->SetBackendFileName("file3");
        ITEM_TENANT1_LOG_1.MutableConfig()->MutableLogConfig()->SetBackendFileName("file4");
        ITEM_TENANT1_TYPE1_LOG_1.MutableConfig()->MutableLogConfig()->SetBackendFileName("file5");
        ITEM_HOST12_LOG_1.MutableConfig()->MutableLogConfig()->SetBackendFileName("file6");
        ITEM_NODE12_LOG_1.MutableConfig()->MutableLogConfig()->SetBackendFileName("file7");

        ITEM_TENANT2_LOG_1.MutableConfig()->MutableLogConfig()->AddEntry()->SetLevel(1);
        ITEM_TENANT2_LOG_2.MutableConfig()->MutableLogConfig()->AddEntry()->SetLevel(2);

        ITEM_TENANT2_TYPE1_LOG_1.MutableConfig()->MutableLogConfig()->AddEntry()->SetLevel(3);
        ITEM_TENANT2_TYPE1_LOG_1.SetMergeStrategy(NKikimrConsole::TConfigItem::MERGE_OVERWRITE_REPEATED);

        ITEM_NODE23_LOG_1.SetOrder(2);

        ITEM_HOST23_LOG_1.SetOrder(2);
        ITEM_HOST23_LOG_1.SetMergeStrategy(NKikimrConsole::TConfigItem::OVERWRITE);
        ITEM_HOST23_LOG_1.MutableConfig()->MutableLogConfig();

        ITEM_DOMAIN_TENANT_POOL_1.MutableConfig()->MutableTenantPoolConfig();

        auto id1 = CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                                  MakeAddAction(ITEM_DOMAIN_LOG_1), MakeAddAction(ITEM_DOMAIN_LOG_2),
                                  MakeAddAction(ITEM_DOMAIN_TENANT_POOL_1), MakeAddAction(ITEM_DOMAIN_TENANT_POOL_2),
                                  MakeAddAction(ITEM_NODE12_LOG_1), MakeAddAction(ITEM_NODE23_LOG_1),
                                  MakeAddAction(ITEM_NODE34_LOG_1), MakeAddAction(ITEM_HOST12_LOG_1),
                                  MakeAddAction(ITEM_HOST23_LOG_1), MakeAddAction(ITEM_HOST34_LOG_1),
                                  MakeAddAction(ITEM_TENANT1_LOG_1), MakeAddAction(ITEM_TENANT1_LOG_2),
                                  MakeAddAction(ITEM_TENANT2_LOG_1), MakeAddAction(ITEM_TENANT2_LOG_2),
                                  MakeAddAction(ITEM_TYPE1_LOG_1), MakeAddAction(ITEM_TYPE1_LOG_2),
                                  MakeAddAction(ITEM_TYPE2_LOG_1), MakeAddAction(ITEM_TYPE2_LOG_2),
                                  MakeAddAction(ITEM_TENANT1_TYPE1_LOG_1), MakeAddAction(ITEM_TENANT1_TYPE1_LOG_2),
                                  MakeAddAction(ITEM_TENANT1_TYPE2_LOG_1), MakeAddAction(ITEM_TENANT1_TYPE2_LOG_2),
                                  MakeAddAction(ITEM_TENANT2_TYPE1_LOG_1), MakeAddAction(ITEM_TENANT2_TYPE1_LOG_2),
                                  MakeAddAction(ITEM_TENANT2_TYPE2_LOG_1), MakeAddAction(ITEM_TENANT2_TYPE2_LOG_2));
        AssignIds(id1, ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_LOG_2,
                  ITEM_DOMAIN_TENANT_POOL_1, ITEM_DOMAIN_TENANT_POOL_2,
                  ITEM_NODE12_LOG_1, ITEM_NODE23_LOG_1,
                  ITEM_NODE34_LOG_1, ITEM_HOST12_LOG_1,
                  ITEM_HOST23_LOG_1, ITEM_HOST34_LOG_1,
                  ITEM_TENANT1_LOG_1, ITEM_TENANT1_LOG_2,
                  ITEM_TENANT2_LOG_1, ITEM_TENANT2_LOG_2,
                  ITEM_TYPE1_LOG_1, ITEM_TYPE1_LOG_2,
                  ITEM_TYPE2_LOG_1, ITEM_TYPE2_LOG_2,
                  ITEM_TENANT1_TYPE1_LOG_1, ITEM_TENANT1_TYPE1_LOG_2,
                  ITEM_TENANT1_TYPE2_LOG_1, ITEM_TENANT1_TYPE2_LOG_2,
                  ITEM_TENANT2_TYPE1_LOG_1, ITEM_TENANT2_TYPE1_LOG_2,
                  ITEM_TENANT2_TYPE2_LOG_1, ITEM_TENANT2_TYPE2_LOG_2);

        NKikimrConfig::TAppConfig config1;
        config1.MutableTenantPoolConfig();
        config1.MutableLogConfig()->SetClusterName("cluster1");
        config1.MutableLogConfig()->SetBackendFileName("file2");
        CheckGetNodeConfig(runtime, 0, "", "", "", config1);
        config1.MutableLogConfig()->SetBackendFileName("file3");
        CheckGetNodeConfig(runtime, 0, "", "", "type1", config1);
        config1.MutableLogConfig()->SetBackendFileName("file4");
        CheckGetNodeConfig(runtime, 0, "", "tenant1", "", config1);
        config1.MutableLogConfig()->SetBackendFileName("file5");
        CheckGetNodeConfig(runtime, 0, "", "tenant1", "type1", config1);
        config1.MutableLogConfig()->SetBackendFileName("file6");
        CheckGetNodeConfig(runtime, 0, "host1", "tenant1", "type1", config1);
        config1.MutableLogConfig()->SetBackendFileName("file7");
        CheckGetNodeConfig(runtime, 1, "host1", "tenant1", "type1", config1);
        config1.MutableLogConfig()->ClearClusterName();
        CheckGetNodeConfig(runtime, 1, "host2", "tenant1", "type1", config1);
        config1.MutableLogConfig()->ClearBackendFileName();
        CheckGetNodeConfig(runtime, 0, "host2", "tenant1", "type1", config1);
        config1.MutableLogConfig()->SetClusterName("cluster1");
        config1.MutableLogConfig()->SetBackendFileName("file2");
        config1.MutableLogConfig()->AddEntry()->SetLevel(1);
        config1.MutableLogConfig()->AddEntry()->SetLevel(2);
        CheckGetNodeConfig(runtime, 0, "", "tenant2", "", config1);
        config1.MutableLogConfig()->SetBackendFileName("file3");
        config1.MutableLogConfig()->ClearEntry();
        config1.MutableLogConfig()->AddEntry()->SetLevel(3);
        CheckGetNodeConfig(runtime, 0, "", "tenant2", "type1", config1);

        config1.ClearTenantPoolConfig();
        CheckGetNodeConfig(runtime, 0, "", "tenant2", "type1", config1,
                           TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}));
    }

    Y_UNIT_TEST(TestAutoOrder) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        InitializeTestConfigItems();

        ITEM_DOMAIN_LOG_1.SetOrder(0);
        auto id1 = CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                                  MakeAddAction(ITEM_DOMAIN_LOG_1));
        AssignIds(id1, ITEM_DOMAIN_LOG_1);
        ITEM_DOMAIN_LOG_1.SetOrder(10);
        CheckGetItems(runtime, TVector<ui32>(),
                      ITEM_DOMAIN_LOG_1);

        ITEM_DOMAIN_LOG_2.SetOrder(0);
        auto id2 = CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                                  MakeAddAction(ITEM_DOMAIN_LOG_2));
        AssignIds(id2, ITEM_DOMAIN_LOG_2);
        ITEM_DOMAIN_LOG_2.SetOrder(20);
        CheckGetItems(runtime, TVector<ui32>(),
                      ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_LOG_2);

        ITEM_DOMAIN_LOG_1.SetOrder(0);
        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                       MakeModifyAction(ITEM_DOMAIN_LOG_1));
        ITEM_DOMAIN_LOG_1.SetOrder(30);
        IncGeneration(ITEM_DOMAIN_LOG_1);
        CheckGetItems(runtime, TVector<ui32>(),
                      ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_LOG_2);

        ITEM_DOMAIN_LOG_1.SetOrder(0);
        ITEM_DOMAIN_LOG_2.SetOrder(0);
        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                       MakeModifyAction(ITEM_DOMAIN_LOG_1), MakeModifyAction(ITEM_DOMAIN_LOG_2));
        ITEM_DOMAIN_LOG_1.SetOrder(10);
        ITEM_DOMAIN_LOG_2.SetOrder(20);
        IncGeneration(ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_LOG_2);
        CheckGetItems(runtime, TVector<ui32>(),
                      ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_LOG_2);

        ITEM_DOMAIN_LOG_2.SetOrder(0);
        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                       MakeRemoveAction(ITEM_DOMAIN_LOG_1), MakeModifyAction(ITEM_DOMAIN_LOG_2));
        ITEM_DOMAIN_LOG_2.SetOrder(10);
        IncGeneration(ITEM_DOMAIN_LOG_2);
        CheckGetItems(runtime, TVector<ui32>(),
                      ITEM_DOMAIN_LOG_2);

        ITEM_DOMAIN_LOG_1.SetOrder(0);
        ITEM_DOMAIN_LOG_1.ClearId();
        ITEM_DOMAIN_LOG_2.SetOrder(0);
        auto id3 = CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                                  MakeAddAction(ITEM_DOMAIN_LOG_1), MakeModifyAction(ITEM_DOMAIN_LOG_2));
        AssignIds(id3, ITEM_DOMAIN_LOG_1);
        ITEM_DOMAIN_LOG_1.SetOrder(10);
        ITEM_DOMAIN_LOG_2.SetOrder(20);
        IncGeneration(ITEM_DOMAIN_LOG_2);
        CheckGetItems(runtime, TVector<ui32>(),
                      ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_LOG_2);
    }

    Y_UNIT_TEST(TestAutoKind) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        InitializeTestConfigItems();

        ITEM_DOMAIN_LOG_1.ClearKind();
        ITEM_DOMAIN_LOG_1.MutableConfig()->MutableLogConfig();
        ITEM_DOMAIN_TENANT_POOL_1.ClearKind();
        ITEM_DOMAIN_TENANT_POOL_1.MutableConfig()->MutableTenantPoolConfig();
        auto id1 = CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                                  MakeAddAction(ITEM_DOMAIN_LOG_1), MakeAddAction(ITEM_DOMAIN_TENANT_POOL_1));
        AssignIds(id1, ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_TENANT_POOL_1);
        ITEM_DOMAIN_LOG_1.SetKind(NKikimrConsole::TConfigItem::LogConfigItem);
        ITEM_DOMAIN_TENANT_POOL_1.SetKind(NKikimrConsole::TConfigItem::TenantPoolConfigItem);
        CheckGetItems(runtime, TVector<ui32>(),
                      ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_TENANT_POOL_1);

        // Cannot determine kind.
        ITEM_DOMAIN_LOG_2.ClearKind();
        ITEM_DOMAIN_LOG_2.MutableConfig()->MutableLogConfig();
        ITEM_DOMAIN_LOG_2.MutableConfig()->MutableTenantPoolConfig();
        CheckConfigure(runtime, Ydb::StatusIds::BAD_REQUEST,
                       MakeAddAction(ITEM_DOMAIN_LOG_2));
    }

    Y_UNIT_TEST(TestAutoSplit) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        InitializeTestConfigItems();

        ITEM_DOMAIN_LOG_1.ClearKind();
        ITEM_DOMAIN_LOG_1.MutableConfig()->MutableLogConfig();
        ITEM_DOMAIN_LOG_1.MutableConfig()->MutableTenantPoolConfig();
        ITEM_DOMAIN_LOG_1.SetCookie("cookie3");
        auto id1 = CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                                  MakeAddAction(ITEM_DOMAIN_LOG_1, true));
        AssignIds(id1, ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_TENANT_POOL_1);
        ITEM_DOMAIN_LOG_1.SetKind(NKikimrConsole::TConfigItem::LogConfigItem);
        ITEM_DOMAIN_LOG_1.MutableConfig()->ClearTenantPoolConfig();
        ITEM_DOMAIN_TENANT_POOL_1.MutableConfig()->MutableTenantPoolConfig();
        ITEM_DOMAIN_TENANT_POOL_1.SetCookie(ITEM_DOMAIN_LOG_1.GetCookie());
        CheckGetItems(runtime, TVector<ui32>(),
                      ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_TENANT_POOL_1);

        ITEM_NODE12_LOG_1.ClearKind();
        ITEM_NODE12_LOG_1.MutableConfig()->MutableLogConfig();
        auto id2 = CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                                  MakeAddAction(ITEM_NODE12_LOG_1, true));
        AssignIds(id2, ITEM_NODE12_LOG_1);
        ITEM_NODE12_LOG_1.SetKind(NKikimrConsole::TConfigItem::LogConfigItem);
        CheckGetItems(runtime, TVector<ui32>(),
                      ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_TENANT_POOL_1,
                      ITEM_NODE12_LOG_1);

        // Cannot split empty config.
        ITEM_DOMAIN_LOG_2.ClearKind();
        ITEM_DOMAIN_LOG_2.ClearConfig();
        CheckConfigure(runtime, Ydb::StatusIds::BAD_REQUEST,
                       MakeAddAction(ITEM_DOMAIN_LOG_2, true));
    }

    Y_UNIT_TEST(TestAllowedScopes) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        InitializeTestConfigItems();

        NKikimrConsole::TConfig config = GetCurrentConfig(runtime);
        config.MutableConfigsConfig()->MutableUsageScopeRestrictions()->Clear();
        config.MutableConfigsConfig()->MutableUsageScopeRestrictions()
            ->AddDisallowedDomainUsageScopeKinds(NKikimrConsole::TConfigItem::LogConfigItem);
        CheckSetConfig(runtime, config, Ydb::StatusIds::SUCCESS);

        CheckConfigure(runtime, Ydb::StatusIds::BAD_REQUEST,
                       MakeAddAction(ITEM_DOMAIN_LOG_1, true));
        CheckConfigure(runtime, Ydb::StatusIds::BAD_REQUEST,
                       MakeAddAction(ITEM_NODE12_LOG_1, true));
        CheckConfigure(runtime, Ydb::StatusIds::BAD_REQUEST,
                       MakeAddAction(ITEM_HOST12_LOG_1, true));
        CheckConfigure(runtime, Ydb::StatusIds::BAD_REQUEST,
                       MakeAddAction(ITEM_TENANT1_LOG_1, true));
        CheckConfigure(runtime, Ydb::StatusIds::BAD_REQUEST,
                       MakeAddAction(ITEM_TYPE1_LOG_1, true));

        config.MutableConfigsConfig()->MutableUsageScopeRestrictions()
            ->AddAllowedNodeIdUsageScopeKinds(NKikimrConsole::TConfigItem::LogConfigItem);
        config.MutableConfigsConfig()->MutableUsageScopeRestrictions()
            ->AddAllowedHostUsageScopeKinds(NKikimrConsole::TConfigItem::LogConfigItem);
        config.MutableConfigsConfig()->MutableUsageScopeRestrictions()
            ->ClearDisallowedDomainUsageScopeKinds();
        CheckSetConfig(runtime, config, Ydb::StatusIds::SUCCESS);

        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                       MakeAddAction(ITEM_DOMAIN_LOG_1, true),
                       MakeAddAction(ITEM_NODE12_LOG_1, true),
                       MakeAddAction(ITEM_HOST12_LOG_1, true),
                       MakeAddAction(ITEM_TENANT1_LOG_1, true),
                       MakeAddAction(ITEM_TYPE1_LOG_1, true));

        config.MutableConfigsConfig()->MutableUsageScopeRestrictions()
            ->ClearAllowedNodeIdUsageScopeKinds();
        CheckSetConfig(runtime, config, Ydb::StatusIds::BAD_REQUEST);
        config.MutableConfigsConfig()->MutableUsageScopeRestrictions()
            ->AddAllowedNodeIdUsageScopeKinds(NKikimrConsole::TConfigItem::LogConfigItem);
        config.MutableConfigsConfig()->MutableUsageScopeRestrictions()
            ->ClearAllowedHostUsageScopeKinds();
        CheckSetConfig(runtime, config, Ydb::StatusIds::BAD_REQUEST);
        config.MutableConfigsConfig()->MutableUsageScopeRestrictions()
            ->AddAllowedHostUsageScopeKinds(NKikimrConsole::TConfigItem::LogConfigItem);
        CheckSetConfig(runtime, config, Ydb::StatusIds::SUCCESS);
        config.MutableConfigsConfig()->MutableUsageScopeRestrictions()
            ->AddDisallowedDomainUsageScopeKinds(NKikimrConsole::TConfigItem::LogConfigItem);
        CheckSetConfig(runtime, config, Ydb::StatusIds::BAD_REQUEST);
        config.MutableConfigsConfig()->MutableUsageScopeRestrictions()
            ->ClearDisallowedDomainUsageScopeKinds();
        CheckSetConfig(runtime, config, Ydb::StatusIds::SUCCESS);
    }

    Y_UNIT_TEST(TestValidation) {
        TValidatorsRegistry::DropInstance();
        RegisterValidator(new TTestValidator(3));

        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        InitializeTestConfigItems();
        ITEM_DOMAIN_LOG_1.MutableConfig()->MutableLogConfig()->AddEntry();
        ITEM_DOMAIN_LOG_2.MutableConfig()->MutableLogConfig()->AddEntry();
        ITEM_DOMAIN_LOG_2.MutableConfig()->MutableLogConfig()->AddEntry();
        ITEM_NODE12_LOG_1.MutableConfig()->MutableLogConfig()->AddEntry();
        ITEM_HOST12_LOG_1.MutableConfig()->MutableLogConfig()->AddEntry();
        ITEM_TENANT1_LOG_1.MutableConfig()->MutableLogConfig()->AddEntry();
        ITEM_TYPE1_LOG_1.MutableConfig()->MutableLogConfig()->AddEntry();
        ITEM_TENANT1_TYPE1_LOG_1.MutableConfig()->MutableLogConfig()->AddEntry();

        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                       MakeAddAction(ITEM_DOMAIN_LOG_1, true),
                       MakeAddAction(ITEM_NODE12_LOG_1, true),
                       MakeAddAction(ITEM_HOST12_LOG_1, true),
                       MakeAddAction(ITEM_TENANT1_LOG_1, true),
                       MakeAddAction(ITEM_TYPE1_LOG_1, true));

        // OK configs.
        CheckGetNodeConfig(runtime, 0, "", "tenant1", "type1", Ydb::StatusIds::SUCCESS);
        CheckGetNodeConfig(runtime, 1, "host1", "tenant2", "type2", Ydb::StatusIds::SUCCESS);
        CheckGetNodeConfig(runtime, 3, "host1", "tenant1", "type2", Ydb::StatusIds::SUCCESS);
        CheckGetNodeConfig(runtime, 1, "host3", "tenant2", "type1", Ydb::StatusIds::SUCCESS);
        // Bad configs.
        CheckGetNodeConfig(runtime, 1, "host1", "tenant1", "type2", Ydb::StatusIds::PRECONDITION_FAILED);
        CheckGetNodeConfig(runtime, 1, "host1", "tenant2", "type1", Ydb::StatusIds::PRECONDITION_FAILED);
        CheckGetNodeConfig(runtime, 3, "host2", "tenant1", "type1", Ydb::StatusIds::PRECONDITION_FAILED);

        // Validator should fail for <tenant1, type1> combination.
        CheckConfigure(runtime, Ydb::StatusIds::BAD_REQUEST,
                       MakeAddAction(ITEM_TENANT1_TYPE1_LOG_1, true));

        // Disable validation for types and try again.
        NKikimrConsole::TConfig config = GetCurrentConfig(runtime);
        config.MutableConfigsConfig()->MutableValidationOptions()
            ->SetValidationLevel(NKikimrConsole::VALIDATE_TENANTS);
        CheckSetConfig(runtime, config, Ydb::StatusIds::SUCCESS);
        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                       MakeAddAction(ITEM_TENANT1_TYPE1_LOG_1, true));

        // Validator should fail for tenant1.
        CheckConfigure(runtime, Ydb::StatusIds::BAD_REQUEST,
                       MakeAddAction(ITEM_DOMAIN_LOG_2, true));

        // Disable validation for tenants and try again.
        config.MutableConfigsConfig()->MutableValidationOptions()
            ->SetValidationLevel(NKikimrConsole::VALIDATE_DOMAIN);
        CheckSetConfig(runtime, config, Ydb::StatusIds::SUCCESS);
        auto ids = CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                                  MakeAddAction(ITEM_DOMAIN_LOG_2, true));
        AssignIds(ids, ITEM_DOMAIN_LOG_2);

        // Validator should fail for domain.
        ITEM_DOMAIN_LOG_2.MutableConfig()->MutableLogConfig()->AddEntry();
        CheckConfigure(runtime, Ydb::StatusIds::BAD_REQUEST,
                       MakeModifyAction(ITEM_DOMAIN_LOG_2));

        // Disable validation and try again.
        config.MutableConfigsConfig()->MutableValidationOptions()
            ->SetValidationLevel(NKikimrConsole::VALIDATE_NONE);
        CheckSetConfig(runtime, config, Ydb::StatusIds::SUCCESS);
        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                       MakeModifyAction(ITEM_DOMAIN_LOG_2));

        // Configs are still bad.
        CheckGetNodeConfig(runtime, 0, "", "", "", Ydb::StatusIds::PRECONDITION_FAILED);
        CheckGetNodeConfig(runtime, 1, "host1", "tenant1", "type1", Ydb::StatusIds::PRECONDITION_FAILED);

        // Disable node configs check and get configs.
        config.MutableConfigsConfig()->MutableValidationOptions()
            ->SetEnableValidationOnNodeConfigRequest(false);
        CheckSetConfig(runtime, config, Ydb::StatusIds::SUCCESS);
        CheckGetNodeConfig(runtime, 0, "", "", "", Ydb::StatusIds::SUCCESS);
        CheckGetNodeConfig(runtime, 1, "host1", "tenant1", "type1", Ydb::StatusIds::SUCCESS);
    }

    Y_UNIT_TEST(TestCheckConfigUpdates)
    {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        InitializeTestConfigItems();
        TAutoPtr<IEventHandle> handle;

        auto id1 = CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                                  MakeAddAction(ITEM_DOMAIN_LOG_1),
                                  MakeAddAction(ITEM_DOMAIN_LOG_2),
                                  MakeAddAction(ITEM_NODE12_LOG_1));
        AssignIds(id1, ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_LOG_2,
                  ITEM_NODE12_LOG_1);

        CheckCheckConfigUpdates(runtime,
                                { },
                                { std::make_pair(ITEM_DOMAIN_LOG_1.GetId().GetId(), ITEM_DOMAIN_LOG_1.GetId().GetGeneration()),
                                  std::make_pair(ITEM_DOMAIN_LOG_2.GetId().GetId(), ITEM_DOMAIN_LOG_2.GetId().GetGeneration()),
                                  std::make_pair(ITEM_NODE12_LOG_1.GetId().GetId(), ITEM_NODE12_LOG_1.GetId().GetGeneration()) },
                                { },
                                { });

        ITEM_DOMAIN_LOG_2.SetOrder(0);
        auto id2 = CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                                  MakeAddAction(ITEM_NODE34_LOG_1),
                                  MakeModifyAction(ITEM_DOMAIN_LOG_2),
                                  MakeRemoveAction(ITEM_NODE12_LOG_1));
        AssignIds(id2, ITEM_NODE34_LOG_1);

        CheckCheckConfigUpdates(runtime,
                                { ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_LOG_2, ITEM_NODE12_LOG_1 },
                                { std::make_pair(ITEM_NODE34_LOG_1.GetId().GetId(), ITEM_NODE34_LOG_1.GetId().GetGeneration()) },
                                { std::make_pair(ITEM_NODE12_LOG_1.GetId().GetId(), ITEM_NODE12_LOG_1.GetId().GetGeneration()) },
                                { std::make_pair(ITEM_DOMAIN_LOG_2.GetId().GetId(), ITEM_DOMAIN_LOG_2.GetId().GetGeneration() + 1) });
    }

    Y_UNIT_TEST(TestManageValidators)
    {
        TValidatorsRegistry::DropInstance();
        RegisterValidator(new TTestValidator(1, "name1", {1, 2, 3}));
        RegisterValidator(new TTestValidator(1, "name2", {4, 5, 6}));

        TTenantTestRuntime runtime(DefaultConsoleTestConfig());

        CheckListValidators(runtime, { { "name1", "name1 configs validator", {1, 2, 3}, true },
                                       { "name2", "name2 configs validator", {4, 5, 6}, true } });
        CheckDisableValidator(runtime, "name1", Ydb::StatusIds::SUCCESS);
        CheckListValidators(runtime, { { "name1", "name1 configs validator", {1, 2, 3}, false },
                                       { "name2", "name2 configs validator", {4, 5, 6}, true } });
        CheckDisableValidator(runtime, "name1", Ydb::StatusIds::SUCCESS);
        CheckListValidators(runtime, { { "name1", "name1 configs validator", {1, 2, 3}, false },
                                       { "name2", "name2 configs validator", {4, 5, 6}, true } });
        CheckDisableValidator(runtime, "name2", Ydb::StatusIds::SUCCESS);
        CheckListValidators(runtime, { { "name1", "name1 configs validator", {1, 2, 3}, false },
                                       { "name2", "name2 configs validator", {4, 5, 6}, false } });
        CheckEnableValidator(runtime, "name1", Ydb::StatusIds::SUCCESS);
        CheckEnableValidator(runtime, "name2", Ydb::StatusIds::SUCCESS);
        CheckListValidators(runtime, { { "name1", "name1 configs validator", {1, 2, 3}, true },
                                       { "name2", "name2 configs validator", {4, 5, 6}, true } });

        CheckEnableValidator(runtime, "name3", Ydb::StatusIds::NOT_FOUND);
        CheckDisableValidator(runtime, "name3", Ydb::StatusIds::NOT_FOUND);

        RestartConsole(runtime);

        CheckListValidators(runtime, { { "name1", "name1 configs validator", {1, 2, 3}, true },
                                       { "name2", "name2 configs validator", {4, 5, 6}, true } });

        auto registry = TValidatorsRegistry::Instance();
        registry->DisableValidators();

        CheckListValidators(runtime, { { "name1", "name1 configs validator", {1, 2, 3}, false },
                                       { "name2", "name2 configs validator", {4, 5, 6}, false } });

        RestartConsole(runtime);

        CheckListValidators(runtime, { { "name1", "name1 configs validator", {1, 2, 3}, true },
                                       { "name2", "name2 configs validator", {4, 5, 6}, true } });

        CheckDisableValidator(runtime, "name1", Ydb::StatusIds::SUCCESS);
        CheckDisableValidator(runtime, "name2", Ydb::StatusIds::SUCCESS);

        CheckListValidators(runtime, { { "name1", "name1 configs validator", {1, 2, 3}, false },
                                       { "name2", "name2 configs validator", {4, 5, 6}, false } });

        registry->EnableValidators();

        CheckListValidators(runtime, { { "name1", "name1 configs validator", {1, 2, 3}, true },
                                       { "name2", "name2 configs validator", {4, 5, 6}, true } });

        RestartConsole(runtime);

        CheckListValidators(runtime, { { "name1", "name1 configs validator", {1, 2, 3}, false },
                                       { "name2", "name2 configs validator", {4, 5, 6}, false } });

        CheckEnableValidator(runtime, "name1", Ydb::StatusIds::SUCCESS);

        registry->DisableValidator("name1");
        registry->EnableValidator("name2");

        CheckListValidators(runtime, { { "name1", "name1 configs validator", {1, 2, 3}, false },
                                       { "name2", "name2 configs validator", {4, 5, 6}, true } });

        RestartConsole(runtime);

        CheckListValidators(runtime, { { "name1", "name1 configs validator", {1, 2, 3}, true },
                                       { "name2", "name2 configs validator", {4, 5, 6}, false } });
    }

    Y_UNIT_TEST(TestDryRun)
    {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        InitializeTestConfigItems();

        auto id1 = CheckConfigure(runtime, Ydb::StatusIds::SUCCESS, true, false,
                                  MakeAddAction(ITEM_DOMAIN_LOG_1),
                                  MakeAddAction(ITEM_DOMAIN_LOG_2),
                                  MakeAddAction(ITEM_NODE12_LOG_1));
        UNIT_ASSERT(id1.empty());

        CheckCheckConfigUpdates(runtime,
                                { }, { }, { }, { });

        auto id2 = CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                                  MakeAddAction(ITEM_DOMAIN_LOG_1),
                                  MakeAddAction(ITEM_DOMAIN_LOG_2),
                                  MakeAddAction(ITEM_NODE12_LOG_1));
        AssignIds(id2, ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_LOG_2,
                  ITEM_NODE12_LOG_1);

        ITEM_DOMAIN_LOG_2.SetOrder(0);
        auto id3 = CheckConfigure(runtime, Ydb::StatusIds::SUCCESS, true, false,
                                  MakeAddAction(ITEM_NODE34_LOG_1),
                                  MakeModifyAction(ITEM_DOMAIN_LOG_2),
                                  MakeRemoveAction(ITEM_NODE12_LOG_1));
        UNIT_ASSERT(id3.empty());

        CheckCheckConfigUpdates(runtime,
                                { ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_LOG_2, ITEM_NODE12_LOG_1 },
                                { }, { }, { });
    }

    Y_UNIT_TEST(TestAffectedConfigs)
    {
        TValidatorsRegistry::Instance()->DropInstance();
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        InitializeTestConfigItems();

        ITEM_DOMAIN_LOG_1.MutableConfig()->MutableLogConfig()->AddEntry();
        ITEM_TENANT1_LOG_1.MutableConfig()->MutableLogConfig()->AddEntry();
        ITEM_TYPE1_LOG_1.MutableConfig()->MutableLogConfig()->AddEntry();
        ITEM_TENANT2_TYPE2_LOG_1.MutableConfig()->MutableLogConfig()->AddEntry();
        ITEM_NODE12_LOG_1.MutableConfig()->MutableLogConfig()->AddEntry();
        ITEM_HOST12_LOG_1.MutableConfig()->MutableLogConfig()->AddEntry();

        THashMap<TTenantAndNodeType, std::pair<ui64, ui64>> affected1;
        affected1[TTenantAndNodeType("", "")] = std::make_pair(0ULL, 1ULL);
        affected1[TTenantAndNodeType("tenant1", "")] = std::make_pair(0ULL, 2ULL);
        affected1[TTenantAndNodeType("", "type1")] = std::make_pair(0ULL, 2ULL);
        affected1[TTenantAndNodeType("tenant1", "type1")] = std::make_pair(0ULL, 3ULL);
        affected1[TTenantAndNodeType("tenant2", "type2")] = std::make_pair(0ULL, 2ULL);
        auto id1 = CheckConfigureLogAffected(runtime, Ydb::StatusIds::SUCCESS,
                                             affected1, false, true,
                                             MakeAddAction(ITEM_DOMAIN_LOG_1),
                                             MakeAddAction(ITEM_TENANT1_LOG_1),
                                             MakeAddAction(ITEM_TYPE1_LOG_1),
                                             MakeAddAction(ITEM_TENANT2_TYPE2_LOG_1),
                                             MakeAddAction(ITEM_NODE12_LOG_1),
                                             MakeAddAction(ITEM_HOST12_LOG_1));
        AssignIds(id1, ITEM_DOMAIN_LOG_1, ITEM_TENANT1_LOG_1, ITEM_TYPE1_LOG_1,
                  ITEM_TENANT2_TYPE2_LOG_1, ITEM_NODE12_LOG_1, ITEM_HOST12_LOG_1);

        THashMap<TTenantAndNodeType, std::pair<ui64, ui64>> affected2;
        affected2[TTenantAndNodeType("", "")] = std::make_pair(1ULL, 0ULL);
        affected2[TTenantAndNodeType("tenant1", "")] = std::make_pair(2ULL, 1ULL);
        affected2[TTenantAndNodeType("", "type1")] = std::make_pair(2ULL, 1ULL);
        affected2[TTenantAndNodeType("tenant1", "type1")] = std::make_pair(3ULL, 2ULL);
        affected2[TTenantAndNodeType("tenant2", "type2")] = std::make_pair(2ULL, 1ULL);
        CheckConfigureLogAffected(runtime, Ydb::StatusIds::SUCCESS,
                                  affected2, true, true,
                                  MakeRemoveAction(ITEM_DOMAIN_LOG_1));

        THashMap<TTenantAndNodeType, std::pair<ui64, ui64>> affected3;
        affected3[TTenantAndNodeType("tenant1", "")] = std::make_pair(2ULL, 1ULL);
        affected3[TTenantAndNodeType("tenant1", "type1")] = std::make_pair(3ULL, 2ULL);
        CheckConfigureLogAffected(runtime, Ydb::StatusIds::SUCCESS,
                                  affected3, true, true,
                                  MakeRemoveAction(ITEM_TENANT1_LOG_1));

        THashMap<TTenantAndNodeType, std::pair<ui64, ui64>> affected4;
        affected4[TTenantAndNodeType("", "type1")] = std::make_pair(2ULL, 1ULL);
        affected4[TTenantAndNodeType("tenant1", "type1")] = std::make_pair(3ULL, 2ULL);
        CheckConfigureLogAffected(runtime, Ydb::StatusIds::SUCCESS,
                                  affected4, true, true,
                                  MakeRemoveAction(ITEM_TYPE1_LOG_1));

        THashMap<TTenantAndNodeType, std::pair<ui64, ui64>> affected5;
        affected5[TTenantAndNodeType("tenant2", "type2")] = std::make_pair(2ULL, 1ULL);
        CheckConfigureLogAffected(runtime, Ydb::StatusIds::SUCCESS,
                                  affected5, true, true,
                                  MakeRemoveAction(ITEM_TENANT2_TYPE2_LOG_1));

        THashMap<TTenantAndNodeType, std::pair<ui64, ui64>> affected6;
        CheckConfigureLogAffected(runtime, Ydb::StatusIds::SUCCESS,
                                  affected6, true, true,
                                  MakeRemoveAction(ITEM_NODE12_LOG_1));
        CheckConfigureLogAffected(runtime, Ydb::StatusIds::SUCCESS,
                                  affected6, true, true,
                                  MakeRemoveAction(ITEM_HOST12_LOG_1));
    }
}

namespace {

class TConfigProxy : public TActor<TConfigProxy>, public TTabletExecutedFlat {
    void DefaultSignalTabletActive(const TActorContext &) override
    {
        // must be empty
    }

    void OnActivateExecutor(const TActorContext &ctx) override
    {
        Become(&TThis::StateWork);
        SignalTabletActive(ctx);
        ctx.Send(Sink, new TEvents::TEvWakeup);
    }

    void OnDetach(const TActorContext &ctx) override
    {
        Die(ctx);
    }

    void OnTabletDead(TEvTablet::TEvTabletDead::TPtr &, const TActorContext &ctx) override
    {
        Die(ctx);
    }

    void Handle(TEvConsole::TEvConfigNotificationRequest::TPtr &ev, const TActorContext &ctx)
    {
        ctx.Send(ev->Forward(Sink));
    }

public:
    TConfigProxy(const TActorId &tablet, TTabletStorageInfo *info, TActorId sink)
        : TActor(&TThis::StateInit)
        , TTabletExecutedFlat(info, tablet, nullptr)
        , Sink(sink)
    {
    }

    STFUNC(StateInit)
    {
        StateInitImpl(ev, SelfId());
    }

    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvConsole::TEvConfigNotificationRequest, Handle);
        }
    }
private:
    TActorId Sink;
};

class TConfigProxyService : public TActorBootstrapped<TConfigProxyService> {
public:
    TConfigProxyService(TActorId sink)
        : Sink(sink)
    {
    }

    void Bootstrap(const TActorContext &/*ctx*/)
    {
        Become(&TConfigProxyService::StateWork);
    }

    void Handle(TEvConsole::TEvConfigNotificationRequest::TPtr &ev, const TActorContext &ctx)
    {
        ctx.Send(ev->Forward(Sink));
    }

    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvConsole::TEvConfigNotificationRequest, Handle);
        }
    }

private:
    TActorId Sink;
};

const ui64 CONFIG_PROXY_TABLET_ID = 0x0000000000840103;

void StartConfigProxy(TTenantTestRuntime &runtime)
{
    auto info = CreateTestTabletInfo(CONFIG_PROXY_TABLET_ID, TTabletTypes::Dummy, TErasureType::ErasureNone);
    TActorId actorId = CreateTestBootstrapper(runtime, info, [&runtime](const TActorId &tablet, TTabletStorageInfo *info) -> IActor* {
        return new TConfigProxy(tablet, info, runtime.Sender);
    });
    runtime.EnableScheduleForActor(actorId, true);

    auto aid = runtime.Register(new TConfigProxyService(runtime.Sender));
    runtime.RegisterService(TActorId(runtime.GetNodeId(0), "configproxy"), aid, 0);

    TAutoPtr<IEventHandle> handle;
    runtime.GrabEdgeEventRethrow<TEvents::TEvWakeup>(handle);
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(TConsoleConfigSubscriptionTests) {
    Y_UNIT_TEST(TestAddConfigSubscription) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());

        // OK subscription for tablet
        ui32 nodeId = runtime.GetNodeId(0);
        ui64 id1 = CheckAddConfigSubscription(runtime, Ydb::StatusIds::SUCCESS,
                                              1, "host1", "tenant-1", "type1",
                                              1, TActorId(), {{1, 2, 3}});
        // OK subscription for service
        ui64 id2 = CheckAddConfigSubscription(runtime, Ydb::StatusIds::SUCCESS,
                                              2, "host2", "tenant-2", "type2",
                                              0, TActorId(nodeId, "service"), {{4, 5, 6}});

        runtime.Register(CreateTabletKiller(MakeConsoleID()));
        TDispatchOptions options;
        options.FinalEvents.emplace_back(&IsTabletActiveEvent, 1);
        runtime.DispatchEvents(options);

        // OK subscription for service
        ui64 id3 = CheckAddConfigSubscription(runtime, Ydb::StatusIds::SUCCESS,
                                              3, "host3", "tenant-3", "type3",
                                              0, TActorId(nodeId, "service"), {{1, 1, 2}});

        CheckGetConfigSubscription(runtime, Ydb::StatusIds::SUCCESS, id1,
                                   1, "host1", "tenant-1", "type1",
                                   1, TActorId(), {{1, 2, 3}});
        CheckGetConfigSubscription(runtime, Ydb::StatusIds::SUCCESS, id2,
                                   2, "host2", "tenant-2", "type2",
                                   0, TActorId(nodeId, "service"), {{4, 5, 6}});
        CheckGetConfigSubscription(runtime, Ydb::StatusIds::SUCCESS, id3,
                                   3, "host3", "tenant-3", "type3",
                                   0, TActorId(nodeId, "service"), {{1, 2}});

        // non-zero subscription id
        CheckAddConfigSubscription(runtime, Ydb::StatusIds::BAD_REQUEST,
                                   1, "host1", "tenant-1", "type1",
                                   1, TActorId(), {{1, 2, 3}}, 1);
        // no subscriber
        CheckAddConfigSubscription(runtime, Ydb::StatusIds::BAD_REQUEST,
                                   1, "host1", "tenant-1", "type1",
                                   0, TActorId(), {{1, 2, 3}});
        // wrong service id
        CheckAddConfigSubscription(runtime, Ydb::StatusIds::BAD_REQUEST,
                                   1, "host1", "tenant-1", "type1",
                                   0, TActorId(1, 0, 0, 0), {{1, 2, 3}});
        // no kinds
        CheckAddConfigSubscription(runtime, Ydb::StatusIds::BAD_REQUEST,
                                   1, "host1", "tenant-1", "type1",
                                   1, TActorId(), {});
    }

    Y_UNIT_TEST(TestRemoveConfigSubscription) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());

        // OK subscription for tablet
        ui32 nodeId = runtime.GetNodeId(0);
        ui64 id1 = CheckAddConfigSubscription(runtime, Ydb::StatusIds::SUCCESS,
                                              1, "host1", "tenant-1", "type1",
                                              1, TActorId(), {{1, 2, 3}});
        // OK subscription for tablet
        ui64 id2 = CheckAddConfigSubscription(runtime, Ydb::StatusIds::SUCCESS,
                                              2, "host1", "tenant-1", "type1",
                                              1, TActorId(), {{1, 2, 3}});
        // OK subscription for service
        ui64 id3 = CheckAddConfigSubscription(runtime, Ydb::StatusIds::SUCCESS,
                                              2, "host2", "tenant-2", "type2",
                                              0, TActorId(nodeId, "service"), {{4, 5, 6}});

        CheckRemoveConfigSubscription(runtime, Ydb::StatusIds::SUCCESS, id2);
        CheckRemoveConfigSubscription(runtime, Ydb::StatusIds::NOT_FOUND, id2);

        runtime.Register(CreateTabletKiller(MakeConsoleID()));
        TDispatchOptions options;
        options.FinalEvents.emplace_back(&IsTabletActiveEvent, 1);
        runtime.DispatchEvents(options);

        CheckGetConfigSubscription(runtime, Ydb::StatusIds::SUCCESS, id1,
                                   1, "host1", "tenant-1", "type1",
                                   1, TActorId(), {{1, 2, 3}});
        CheckGetConfigSubscription(runtime, Ydb::StatusIds::NOT_FOUND, id2);
        CheckGetConfigSubscription(runtime, Ydb::StatusIds::SUCCESS, id3,
                                   2, "host2", "tenant-2", "type2",
                                   0, TActorId(nodeId, "service"), {{4, 5, 6}});

        CheckRemoveConfigSubscription(runtime, Ydb::StatusIds::SUCCESS, id1);
        CheckGetConfigSubscription(runtime, Ydb::StatusIds::NOT_FOUND, id1);
    }

    Y_UNIT_TEST(TestRemoveConfigSubscriptions) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());

        // OK subscription for tablet
        ui32 nodeId = runtime.GetNodeId(0);
        ui64 id1 = CheckAddConfigSubscription(runtime, Ydb::StatusIds::SUCCESS,
                                              1, "host1", "tenant-1", "type1",
                                              1, TActorId(), {{1, 2, 3}});
        // OK subscription for tablet
        ui64 id2 = CheckAddConfigSubscription(runtime, Ydb::StatusIds::SUCCESS,
                                              2, "host1", "tenant-1", "type1",
                                              1, TActorId(), {{1, 2, 3}});
        // OK subscription for service
        ui64 id3 = CheckAddConfigSubscription(runtime, Ydb::StatusIds::SUCCESS,
                                              2, "host2", "tenant-2", "type2",
                                              0, TActorId(nodeId, "service"), {{4, 5, 6}});
        // OK subscription for service
        ui64 id4 = CheckAddConfigSubscription(runtime, Ydb::StatusIds::SUCCESS,
                                              3, "host2", "tenant-2", "type2",
                                              0, TActorId(nodeId, "service"), {{4, 5, 6}});

        CheckRemoveConfigSubscriptions(runtime, Ydb::StatusIds::SUCCESS,
                                       1, TActorId());
        CheckRemoveConfigSubscriptions(runtime, Ydb::StatusIds::SUCCESS,
                                       1, TActorId());
        CheckRemoveConfigSubscriptions(runtime, Ydb::StatusIds::SUCCESS,
                                       2, TActorId());
        CheckRemoveConfigSubscriptions(runtime, Ydb::StatusIds::SUCCESS,
                                       0, TActorId());

        runtime.Register(CreateTabletKiller(MakeConsoleID()));
        TDispatchOptions options;
        options.FinalEvents.emplace_back(&IsTabletActiveEvent, 1);
        runtime.DispatchEvents(options);

        CheckGetConfigSubscription(runtime, Ydb::StatusIds::NOT_FOUND, id1);
        CheckGetConfigSubscription(runtime, Ydb::StatusIds::NOT_FOUND, id2);
        CheckGetConfigSubscription(runtime, Ydb::StatusIds::SUCCESS, id3,
                                   2, "host2", "tenant-2", "type2",
                                   0, TActorId(nodeId, "service"), {{4, 5, 6}});
        CheckGetConfigSubscription(runtime, Ydb::StatusIds::SUCCESS, id4,
                                   3, "host2", "tenant-2", "type2",
                                   0, TActorId(nodeId, "service"), {{4, 5, 6}});

        CheckRemoveConfigSubscriptions(runtime, Ydb::StatusIds::SUCCESS,
                                       0, TActorId(nodeId, "service"));

        CheckGetConfigSubscription(runtime, Ydb::StatusIds::NOT_FOUND, id3);
        CheckGetConfigSubscription(runtime, Ydb::StatusIds::NOT_FOUND, id4);
    }

    Y_UNIT_TEST(TestListConfigSubscriptions) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());

        // OK subscription for tablet
        ui32 nodeId = runtime.GetNodeId(0);
        ui64 id1 = CheckAddConfigSubscription(runtime, Ydb::StatusIds::SUCCESS,
                                              1, "host1", "tenant-1", "type1",
                                              1, TActorId(), {{1, 2}});
        // OK subscription for tablet
        ui64 id2 = CheckAddConfigSubscription(runtime, Ydb::StatusIds::SUCCESS,
                                              1, "host1", "tenant-1", "type1",
                                              1, TActorId(), {{3, 4}});
        // OK subscription for service
        ui64 id3 = CheckAddConfigSubscription(runtime, Ydb::StatusIds::SUCCESS,
                                              2, "host2", "tenant-2", "type2",
                                              0, TActorId(nodeId, "service"), {{4, 5}});
        // OK subscription for service
        ui64 id4 = CheckAddConfigSubscription(runtime, Ydb::StatusIds::SUCCESS,
                                              2, "host2", "tenant-2", "type2",
                                              0, TActorId(nodeId, "service"), {{6, 7}});

        // List 1 2
        CheckListConfigSubscriptions(runtime, Ydb::StatusIds::SUCCESS, 1, TActorId(),
                                     id1, 1, "host1", "tenant-1", "type1",
                                     1, TActorId(), TVector<ui32>({1, 2}),
                                     id2, 1, "host1", "tenant-1", "type1",
                                     1, TActorId(), TVector<ui32>({3, 4}));
        // List 3 4
        CheckListConfigSubscriptions(runtime, Ydb::StatusIds::SUCCESS, 0, TActorId(nodeId, "service"),
                                     id3, 2, "host2", "tenant-2", "type2",
                                     0, TActorId(nodeId, "service"), TVector<ui32>({4, 5}),
                                     id4, 2, "host2", "tenant-2", "type2",
                                     0, TActorId(nodeId, "service"), TVector<ui32>({6, 7}));
        // List 1 2 3 4
        CheckListConfigSubscriptions(runtime, Ydb::StatusIds::SUCCESS, 0, TActorId(),
                                     id1, 1, "host1", "tenant-1", "type1",
                                     1, TActorId(), TVector<ui32>({1, 2}),
                                     id2, 1, "host1", "tenant-1", "type1",
                                     1, TActorId(), TVector<ui32>({3, 4}),
                                     id3, 2, "host2", "tenant-2", "type2",
                                     0, TActorId(nodeId, "service"), TVector<ui32>({4, 5}),
                                     id4, 2, "host2", "tenant-2", "type2",
                                     0, TActorId(nodeId, "service"), TVector<ui32>({6, 7}));
        // Empty list
        CheckListConfigSubscriptions(runtime, Ydb::StatusIds::SUCCESS, 2, TActorId());
        // Empty list
        CheckListConfigSubscriptions(runtime, Ydb::StatusIds::SUCCESS, 0, TActorId(nodeId, "service1"));
        // Empty list
        CheckListConfigSubscriptions(runtime, Ydb::StatusIds::SUCCESS, 0, TActorId(nodeId + 1, "service"));

        // Remove 1 2
        CheckRemoveConfigSubscriptions(runtime, Ydb::StatusIds::SUCCESS,
                                       1, TActorId());
        // Remove 3
        CheckRemoveConfigSubscription(runtime, Ydb::StatusIds::SUCCESS, id3);

        // Empty list
        CheckListConfigSubscriptions(runtime, Ydb::StatusIds::SUCCESS, 1, TActorId());
        // List 4
        CheckListConfigSubscriptions(runtime, Ydb::StatusIds::SUCCESS, 0, TActorId(nodeId, "service"),
                                     id4, 2, "host2", "tenant-2", "type2",
                                     0, TActorId(nodeId, "service"), TVector<ui32>({6, 7}));
        // List 4
        CheckListConfigSubscriptions(runtime, Ydb::StatusIds::SUCCESS, 0, TActorId(),
                                     id4, 2, "host2", "tenant-2", "type2",
                                     0, TActorId(nodeId, "service"), TVector<ui32>({6, 7}));
    }

    Y_UNIT_TEST(TestReplaceConfigSubscriptions) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());

        ui32 nodeId = runtime.GetNodeId(0);
        ui64 id1 = CheckReplaceConfigSubscriptions(runtime, Ydb::StatusIds::SUCCESS,
                                                   1, "host1", "tenant-1", "type1",
                                                   1, TActorId(), {{1, 2, 3}});
        ui64 id2 = CheckReplaceConfigSubscriptions(runtime, Ydb::StatusIds::SUCCESS,
                                                   2, "host2", "tenant-2", "type2",
                                                   0, TActorId(nodeId, "service"), {{4, 5, 6}});

        CheckListConfigSubscriptions(runtime, Ydb::StatusIds::SUCCESS, 1, TActorId(),
                                     id1, 1, "host1", "tenant-1", "type1",
                                     1, TActorId(), TVector<ui32>({1, 2, 3}));
        CheckListConfigSubscriptions(runtime, Ydb::StatusIds::SUCCESS, 0, TActorId(nodeId, "service"),
                                     id2, 2, "host2", "tenant-2", "type2",
                                     0, TActorId(nodeId, "service"), TVector<ui32>({4, 5, 6}));

        CheckAddConfigSubscription(runtime, Ydb::StatusIds::SUCCESS,
                                   1, "host1", "tenant-1", "type1",
                                   1, TActorId(), {{1, 2}});
        CheckAddConfigSubscription(runtime, Ydb::StatusIds::SUCCESS,
                                   2, "host2", "tenant-2", "type2",
                                   0, TActorId(nodeId, "service"), {{4, 5}});

        ui64 id5 = CheckReplaceConfigSubscriptions(runtime, Ydb::StatusIds::SUCCESS,
                                                   1, "host1", "tenant-1", "type1",
                                                   1, TActorId(), {{1, 2, 3}});
        UNIT_ASSERT_VALUES_EQUAL(id1, id5);
        ui64 id6 = CheckReplaceConfigSubscriptions(runtime, Ydb::StatusIds::SUCCESS,
                                                   2, "host2", "tenant-2", "type2",
                                                   0, TActorId(nodeId, "service"), {{4, 5, 6}});
        UNIT_ASSERT_VALUES_EQUAL(id2, id6);

        runtime.Register(CreateTabletKiller(MakeConsoleID()));
        TDispatchOptions options;
        options.FinalEvents.emplace_back(&IsTabletActiveEvent, 1);
        runtime.DispatchEvents(options);

        CheckListConfigSubscriptions(runtime, Ydb::StatusIds::SUCCESS, 1, TActorId(),
                                     id1, 1, "host1", "tenant-1", "type1",
                                     1, TActorId(), TVector<ui32>({1, 2, 3}));
        CheckListConfigSubscriptions(runtime, Ydb::StatusIds::SUCCESS, 0, TActorId(nodeId, "service"),
                                     id2, 2, "host2", "tenant-2", "type2",
                                     0, TActorId(nodeId, "service"), TVector<ui32>({4, 5, 6}));

        ui64 id7 = CheckReplaceConfigSubscriptions(runtime, Ydb::StatusIds::SUCCESS,
                                                   1, "host1", "tenant-1", "type1",
                                                   1, TActorId(), {{1, 2}});
        UNIT_ASSERT(id1 != id7);
        ui64 id8 = CheckReplaceConfigSubscriptions(runtime, Ydb::StatusIds::SUCCESS,
                                                   2, "host2", "tenant-2", "type2",
                                                   0, TActorId(nodeId, "service"), {{4, 5}});
        UNIT_ASSERT(id2 != id8);

        CheckListConfigSubscriptions(runtime, Ydb::StatusIds::SUCCESS, 1, TActorId(),
                                     id7, 1, "host1", "tenant-1", "type1",
                                     1, TActorId(), TVector<ui32>({1, 2}));
        CheckListConfigSubscriptions(runtime, Ydb::StatusIds::SUCCESS, 0, TActorId(nodeId, "service"),
                                     id8, 2, "host2", "tenant-2", "type2",
                                     0, TActorId(nodeId, "service"), TVector<ui32>({4, 5}));
    }

    void TestNotificationForNewSubscriptionBase(bool useService)
    {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        InitializeTestConfigItems();
        StartConfigProxy(runtime);
        TAutoPtr<IEventHandle> handle;
        TActorId serviceId;
        ui64 tabletId = 0;

        if (useService)
            serviceId = TActorId(runtime.GetNodeId(0), "configproxy");
        else
            tabletId = CONFIG_PROXY_TABLET_ID;

        auto id1 = CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                                  MakeAddAction(ITEM_DOMAIN_LOG_1),
                                  MakeAddAction(ITEM_DOMAIN_LOG_2));
        AssignIds(id1, ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_LOG_2);

        // New subscription should cause notification.
        CheckAddConfigSubscription(runtime, Ydb::StatusIds::SUCCESS,
                                   1, "host1", "tenant1", "type1",
                                   tabletId, serviceId,
                                   TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}));
        auto reply1 = runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigNotificationRequest>(handle);
        CheckConfigId(reply1->Record.GetConfigId(), ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_LOG_2);
    }

    Y_UNIT_TEST(TestNotificationForNewSubscription) {
        TestNotificationForNewSubscriptionBase(false);
        TestNotificationForNewSubscriptionBase(true);
    }

    void TestNotificationForNewConfigItemBase(bool useService)
    {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        InitializeTestConfigItems();
        StartConfigProxy(runtime);
        TAutoPtr<IEventHandle> handle;
        TActorId serviceId;
        ui64 tabletId = 0;

        if (useService)
            serviceId = TActorId(runtime.GetNodeId(0), "configproxy");
        else
            tabletId = CONFIG_PROXY_TABLET_ID;

        auto id1 = CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                                  MakeAddAction(ITEM_DOMAIN_LOG_1));
        AssignIds(id1, ITEM_DOMAIN_LOG_1);

        // New subscription should cause notification.
        CheckAddConfigSubscription(runtime, Ydb::StatusIds::SUCCESS,
                                   1, "host1", "tenant1", "type1",
                                   tabletId, serviceId,
                                   TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}));
        auto reply1 = runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigNotificationRequest>(handle);
        CheckConfigId(reply1->Record.GetConfigId(), ITEM_DOMAIN_LOG_1);
        AcceptConfig(runtime, handle);

        // New items with another kind/node/host/tenant/type shouldn't cause notification.
        auto id2 = CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                                  MakeAddAction(ITEM_DOMAIN_TENANT_POOL_1),
                                  MakeAddAction(ITEM_NODE34_LOG_1),
                                  MakeAddAction(ITEM_HOST23_LOG_1),
                                  MakeAddAction(ITEM_TENANT2_LOG_1),
                                  MakeAddAction(ITEM_TYPE2_LOG_1));
        AssignIds(id2, ITEM_DOMAIN_TENANT_POOL_1, ITEM_NODE34_LOG_1, ITEM_HOST23_LOG_1, ITEM_TENANT2_LOG_1, ITEM_TYPE2_LOG_1);
        auto reply2 = runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigNotificationRequest>(handle, TDuration::Seconds(1));
        UNIT_ASSERT(!reply2);

        // Add item with domain scope -> notification.
        auto id3 = CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                                  MakeAddAction(ITEM_DOMAIN_LOG_2));
        AssignIds(id3, ITEM_DOMAIN_LOG_2);
        auto reply3 = runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigNotificationRequest>(handle);
        CheckConfigId(reply3->Record.GetConfigId(),
                      ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_LOG_2);
        AcceptConfig(runtime, handle);

        // Add item with matching node scope -> notification.
        auto id4 = CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                                  MakeAddAction(ITEM_NODE12_LOG_1));
        AssignIds(id4, ITEM_NODE12_LOG_1);
        auto reply4 = runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigNotificationRequest>(handle);
        CheckConfigId(reply4->Record.GetConfigId(),
                      ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_LOG_2, ITEM_NODE12_LOG_1);
        AcceptConfig(runtime, handle);

        // Add item with matching host scope -> notification.
        ITEM_HOST12_LOG_1.SetOrder(0);
        auto id5 = CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                                  MakeAddAction(ITEM_HOST12_LOG_1));
        AssignIds(id5, ITEM_HOST12_LOG_1);
        auto reply5 = runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigNotificationRequest>(handle);
        CheckConfigId(reply5->Record.GetConfigId(),
                      ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_LOG_2, ITEM_HOST12_LOG_1,
                      ITEM_NODE12_LOG_1);
        AcceptConfig(runtime, handle);

        // Add item with matching tenant scope -> notification.
        auto id6 = CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                                  MakeAddAction(ITEM_TENANT1_LOG_1));
        AssignIds(id6, ITEM_TENANT1_LOG_1);
        auto reply6 = runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigNotificationRequest>(handle);
        CheckConfigId(reply6->Record.GetConfigId(),
                      ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_LOG_2, ITEM_TENANT1_LOG_1,
                      ITEM_HOST12_LOG_1, ITEM_NODE12_LOG_1);
        AcceptConfig(runtime, handle);

        // Add item with matching node type scope -> notification.
        auto id7 = CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                                  MakeAddAction(ITEM_TYPE1_LOG_1));
        AssignIds(id7, ITEM_TYPE1_LOG_1);
        auto reply7 = runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigNotificationRequest>(handle);
        CheckConfigId(reply7->Record.GetConfigId(),
                      ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_LOG_2, ITEM_TYPE1_LOG_1,
                      ITEM_TENANT1_LOG_1, ITEM_HOST12_LOG_1, ITEM_NODE12_LOG_1);
        AcceptConfig(runtime, handle);
    }

    Y_UNIT_TEST(TestNotificationForNewConfigItem) {
        TestNotificationForNewConfigItemBase(false);
        TestNotificationForNewConfigItemBase(true);
    }

    void TestNotificationForModifiedConfigItemBase(bool useService)
    {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        InitializeTestConfigItems();
        StartConfigProxy(runtime);
        TAutoPtr<IEventHandle> handle;
        TActorId serviceId;
        ui64 tabletId = 0;

        if (useService)
            serviceId = TActorId(runtime.GetNodeId(0), "configproxy");
        else
            tabletId = CONFIG_PROXY_TABLET_ID;

        auto id1 = CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                                  MakeAddAction(ITEM_DOMAIN_LOG_1),
                                  MakeAddAction(ITEM_DOMAIN_LOG_2),
                                  MakeAddAction(ITEM_NODE12_LOG_1),
                                  MakeAddAction(ITEM_NODE34_LOG_1),
                                  MakeAddAction(ITEM_HOST12_LOG_1),
                                  MakeAddAction(ITEM_HOST34_LOG_1),
                                  MakeAddAction(ITEM_TENANT1_LOG_1),
                                  MakeAddAction(ITEM_TENANT2_LOG_1),
                                  MakeAddAction(ITEM_TYPE1_LOG_1),
                                  MakeAddAction(ITEM_TYPE2_LOG_1),
                                  MakeAddAction(ITEM_DOMAIN_TENANT_POOL_1));
        AssignIds(id1, ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_LOG_2, ITEM_NODE12_LOG_1,
                  ITEM_NODE34_LOG_1, ITEM_HOST12_LOG_1, ITEM_HOST34_LOG_1,
                  ITEM_TENANT1_LOG_1, ITEM_TENANT2_LOG_1, ITEM_TYPE1_LOG_1,
                  ITEM_TYPE2_LOG_1, ITEM_DOMAIN_TENANT_POOL_1);

        // New subscription should cause notification.
        CheckAddConfigSubscription(runtime, Ydb::StatusIds::SUCCESS,
                                   1, "host1", "tenant1", "type1",
                                   tabletId, serviceId,
                                   TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}));
        auto reply1 = runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigNotificationRequest>(handle);
        CheckConfigId(reply1->Record.GetConfigId(),
                      ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_LOG_2, ITEM_TYPE1_LOG_1,
                      ITEM_TENANT1_LOG_1, ITEM_HOST12_LOG_1, ITEM_NODE12_LOG_1);
        AcceptConfig(runtime, handle);

        // Modify items with another kind/node/host/tenant/type shouldn't cause notification.
        ITEM_NODE34_LOG_1.MutableConfig()->MutableLogConfig()->SetClusterName("cluster1");
        ITEM_HOST34_LOG_1.MutableConfig()->MutableLogConfig()->SetClusterName("cluster1");
        ITEM_TENANT2_LOG_1.MutableConfig()->MutableLogConfig()->SetClusterName("cluster1");
        ITEM_TYPE2_LOG_1.MutableConfig()->MutableLogConfig()->SetClusterName("cluster1");
        ITEM_DOMAIN_TENANT_POOL_1.MutableConfig()->MutableTenantPoolConfig()->SetIsEnabled(false);
        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                       MakeModifyAction(ITEM_DOMAIN_TENANT_POOL_1),
                       MakeModifyAction(ITEM_NODE34_LOG_1),
                       MakeModifyAction(ITEM_HOST34_LOG_1),
                       MakeModifyAction(ITEM_TENANT2_LOG_1),
                       MakeModifyAction(ITEM_TYPE2_LOG_1));
        IncGeneration(ITEM_DOMAIN_TENANT_POOL_1, ITEM_NODE34_LOG_1, ITEM_HOST34_LOG_1, ITEM_TENANT2_LOG_1, ITEM_TYPE2_LOG_1);
        auto reply2 = runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigNotificationRequest>(handle, TDuration::Seconds(1));
        UNIT_ASSERT(!reply2);

        // Modify item with domain scope -> notification.
        ITEM_DOMAIN_LOG_2.MutableConfig()->MutableLogConfig()->SetClusterName("cluster1");
        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                       MakeModifyAction(ITEM_DOMAIN_LOG_2));
        IncGeneration(ITEM_DOMAIN_LOG_2);
        auto reply3 = runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigNotificationRequest>(handle);
        CheckConfigId(reply3->Record.GetConfigId(),
                      ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_LOG_2, ITEM_TYPE1_LOG_1,
                      ITEM_TENANT1_LOG_1, ITEM_HOST12_LOG_1, ITEM_NODE12_LOG_1);
        AcceptConfig(runtime, handle);

        // Modify item with matching node scope -> notification.
        ITEM_NODE12_LOG_1.MutableConfig()->MutableLogConfig()->SetClusterName("cluster1");
        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                       MakeModifyAction(ITEM_NODE12_LOG_1));
        IncGeneration(ITEM_NODE12_LOG_1);
        auto reply4 = runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigNotificationRequest>(handle);
        CheckConfigId(reply4->Record.GetConfigId(),
                      ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_LOG_2, ITEM_TYPE1_LOG_1,
                      ITEM_TENANT1_LOG_1, ITEM_HOST12_LOG_1, ITEM_NODE12_LOG_1);
        AcceptConfig(runtime, handle);

        // Modify item with matching host scope -> notification.
        ITEM_HOST12_LOG_1.MutableConfig()->MutableLogConfig()->SetClusterName("cluster1");
        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                       MakeModifyAction(ITEM_HOST12_LOG_1));
        IncGeneration(ITEM_HOST12_LOG_1);
        auto reply5 = runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigNotificationRequest>(handle);
        CheckConfigId(reply5->Record.GetConfigId(),
                      ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_LOG_2, ITEM_TYPE1_LOG_1,
                      ITEM_TENANT1_LOG_1, ITEM_HOST12_LOG_1, ITEM_NODE12_LOG_1);
        AcceptConfig(runtime, handle);

        // Modify item with matching tenant scope -> notification.
        ITEM_TENANT1_LOG_1.MutableConfig()->MutableLogConfig()->SetClusterName("cluster1");
        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                       MakeModifyAction(ITEM_TENANT1_LOG_1));
        IncGeneration(ITEM_TENANT1_LOG_1);
        auto reply6 = runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigNotificationRequest>(handle);
        CheckConfigId(reply6->Record.GetConfigId(),
                      ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_LOG_2, ITEM_TYPE1_LOG_1,
                      ITEM_TENANT1_LOG_1, ITEM_HOST12_LOG_1, ITEM_NODE12_LOG_1);
        AcceptConfig(runtime, handle);

        // Modify item with matching node type scope -> notification.
        ITEM_TYPE1_LOG_1.MutableConfig()->MutableLogConfig()->SetClusterName("cluster1");
        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                       MakeModifyAction(ITEM_TYPE1_LOG_1));
        IncGeneration(ITEM_TYPE1_LOG_1);
        auto reply7 = runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigNotificationRequest>(handle);
        CheckConfigId(reply7->Record.GetConfigId(),
                      ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_LOG_2, ITEM_TYPE1_LOG_1,
                      ITEM_TENANT1_LOG_1, ITEM_HOST12_LOG_1, ITEM_NODE12_LOG_1);
        AcceptConfig(runtime, handle);
    }

    Y_UNIT_TEST(TestNotificationForModifiedConfigItem) {
        TestNotificationForModifiedConfigItemBase(false);
        TestNotificationForModifiedConfigItemBase(true);
    }

    void TestNotificationForModifiedConfigItemScopeBase(bool useService)
    {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        InitializeTestConfigItems();
        StartConfigProxy(runtime);
        TAutoPtr<IEventHandle> handle;
        TActorId serviceId;
        ui64 tabletId = 0;

        if (useService)
            serviceId = TActorId(runtime.GetNodeId(0), "configproxy");
        else
            tabletId = CONFIG_PROXY_TABLET_ID;

        auto id1 = CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                                  MakeAddAction(ITEM_DOMAIN_LOG_1),
                                  MakeAddAction(ITEM_DOMAIN_LOG_2),
                                  MakeAddAction(ITEM_NODE12_LOG_1),
                                  MakeAddAction(ITEM_NODE34_LOG_1),
                                  MakeAddAction(ITEM_HOST12_LOG_1),
                                  MakeAddAction(ITEM_HOST34_LOG_1),
                                  MakeAddAction(ITEM_TENANT1_LOG_1),
                                  MakeAddAction(ITEM_TENANT2_LOG_1),
                                  MakeAddAction(ITEM_TYPE1_LOG_1),
                                  MakeAddAction(ITEM_TYPE2_LOG_1),
                                  MakeAddAction(ITEM_DOMAIN_TENANT_POOL_1));
        AssignIds(id1, ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_LOG_2, ITEM_NODE12_LOG_1,
                  ITEM_NODE34_LOG_1, ITEM_HOST12_LOG_1, ITEM_HOST34_LOG_1,
                  ITEM_TENANT1_LOG_1, ITEM_TENANT2_LOG_1, ITEM_TYPE1_LOG_1,
                  ITEM_TYPE2_LOG_1, ITEM_DOMAIN_TENANT_POOL_1);

        // New subscription should cause notification.
        CheckAddConfigSubscription(runtime, Ydb::StatusIds::SUCCESS,
                                   1, "host1", "tenant1", "type1",
                                   tabletId, serviceId,
                                   TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}));
        auto reply1 = runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigNotificationRequest>(handle);
        CheckConfigId(reply1->Record.GetConfigId(),
                      ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_LOG_2, ITEM_TYPE1_LOG_1,
                      ITEM_TENANT1_LOG_1, ITEM_HOST12_LOG_1, ITEM_NODE12_LOG_1);
        AcceptConfig(runtime, handle);

        // Modify item's scopes with another kind/node/host/tenant/type shouldn't cause notification.
        ITEM_NODE34_LOG_1.MutableUsageScope()->MutableNodeFilter()->AddNodes(5);
        ITEM_HOST34_LOG_1.MutableUsageScope()->MutableHostFilter()->AddHosts("host5");
        ITEM_TENANT2_LOG_1.MutableUsageScope()->MutableTenantAndNodeTypeFilter()->SetTenant("tenant3");
        ITEM_TYPE2_LOG_1.MutableUsageScope()->MutableTenantAndNodeTypeFilter()->SetNodeType("type3");
        ITEM_DOMAIN_TENANT_POOL_1.MutableUsageScope()->MutableTenantAndNodeTypeFilter()->SetTenant("tenant3");
        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                       MakeModifyAction(ITEM_DOMAIN_TENANT_POOL_1),
                       MakeModifyAction(ITEM_NODE34_LOG_1),
                       MakeModifyAction(ITEM_HOST34_LOG_1),
                       MakeModifyAction(ITEM_TENANT2_LOG_1),
                       MakeModifyAction(ITEM_TYPE2_LOG_1));
        IncGeneration(ITEM_DOMAIN_TENANT_POOL_1, ITEM_NODE34_LOG_1, ITEM_HOST34_LOG_1, ITEM_TENANT2_LOG_1, ITEM_TYPE2_LOG_1);
        auto reply2 = runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigNotificationRequest>(handle, TDuration::Seconds(1));
        UNIT_ASSERT(!reply2);

        // Modify item's node scope to match subscription -> notification.
        ITEM_NODE34_LOG_1.MutableUsageScope()->MutableNodeFilter()->AddNodes(1);
        ITEM_NODE34_LOG_1.SetOrder(0);
        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                       MakeModifyAction(ITEM_NODE34_LOG_1));
        IncGeneration(ITEM_NODE34_LOG_1);
        auto reply3 = runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigNotificationRequest>(handle);
        CheckConfigId(reply3->Record.GetConfigId(),
                      ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_LOG_2, ITEM_TYPE1_LOG_1,
                      ITEM_TENANT1_LOG_1, ITEM_HOST12_LOG_1, ITEM_NODE12_LOG_1,
                      ITEM_NODE34_LOG_1);
        AcceptConfig(runtime, handle);

        // Modify item's node scope to mismatch subscription -> notification.
        ITEM_NODE12_LOG_1.MutableUsageScope()->MutableNodeFilter()->ClearNodes();
        ITEM_NODE12_LOG_1.MutableUsageScope()->MutableNodeFilter()->AddNodes(2);
        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                       MakeModifyAction(ITEM_NODE12_LOG_1));
        IncGeneration(ITEM_NODE12_LOG_1);
        auto reply4 = runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigNotificationRequest>(handle);
        CheckConfigId(reply4->Record.GetConfigId(),
                      ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_LOG_2, ITEM_TYPE1_LOG_1,
                      ITEM_TENANT1_LOG_1, ITEM_HOST12_LOG_1, ITEM_NODE34_LOG_1);
        AcceptConfig(runtime, handle);

        // Modify item's host scope to match subscription -> notification.
        ITEM_HOST34_LOG_1.MutableUsageScope()->MutableHostFilter()->AddHosts("host1");
        ITEM_HOST34_LOG_1.SetOrder(0);
        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                       MakeModifyAction(ITEM_HOST34_LOG_1));
        IncGeneration(ITEM_HOST34_LOG_1);
        auto reply5 = runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigNotificationRequest>(handle);
        CheckConfigId(reply5->Record.GetConfigId(),
                      ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_LOG_2, ITEM_TYPE1_LOG_1,
                      ITEM_TENANT1_LOG_1, ITEM_HOST12_LOG_1, ITEM_HOST34_LOG_1,
                      ITEM_NODE34_LOG_1);
        AcceptConfig(runtime, handle);

        // Modify item's host scope to mismatch subscription -> notification.
        ITEM_HOST12_LOG_1.MutableUsageScope()->MutableHostFilter()->ClearHosts();
        ITEM_HOST12_LOG_1.MutableUsageScope()->MutableHostFilter()->AddHosts("host2");
        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                       MakeModifyAction(ITEM_HOST12_LOG_1));
        IncGeneration(ITEM_HOST12_LOG_1);
        auto reply6 = runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigNotificationRequest>(handle);
        CheckConfigId(reply6->Record.GetConfigId(),
                      ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_LOG_2, ITEM_TYPE1_LOG_1,
                      ITEM_TENANT1_LOG_1, ITEM_HOST34_LOG_1, ITEM_NODE34_LOG_1);
        AcceptConfig(runtime, handle);

        // Modify item's tenant scope to match subscription -> notification.
        ITEM_TENANT2_LOG_1.MutableUsageScope()->MutableTenantAndNodeTypeFilter()->SetTenant("tenant1");
        ITEM_TENANT2_LOG_1.SetOrder(0);
        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                       MakeModifyAction(ITEM_TENANT2_LOG_1));
        IncGeneration(ITEM_TENANT2_LOG_1);
        auto reply7 = runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigNotificationRequest>(handle);
        CheckConfigId(reply7->Record.GetConfigId(),
                      ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_LOG_2, ITEM_TYPE1_LOG_1,
                      ITEM_TENANT1_LOG_1, ITEM_TENANT2_LOG_1, ITEM_HOST34_LOG_1,
                      ITEM_NODE34_LOG_1);
        AcceptConfig(runtime, handle);

        // Modify item's tenant scope to mismatch subscription -> notification.
        ITEM_TENANT1_LOG_1.MutableUsageScope()->MutableTenantAndNodeTypeFilter()->SetTenant("tenant2");
        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                       MakeModifyAction(ITEM_TENANT1_LOG_1));
        IncGeneration(ITEM_TENANT1_LOG_1);
        auto reply8 = runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigNotificationRequest>(handle);
        CheckConfigId(reply8->Record.GetConfigId(),
                      ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_LOG_2, ITEM_TYPE1_LOG_1,
                      ITEM_TENANT2_LOG_1, ITEM_HOST34_LOG_1, ITEM_NODE34_LOG_1);
        AcceptConfig(runtime, handle);

        // Modify item's node type scope to match subscription -> notification.
        ITEM_TYPE2_LOG_1.MutableUsageScope()->MutableTenantAndNodeTypeFilter()->SetNodeType("type1");
        ITEM_TYPE2_LOG_1.SetOrder(0);
        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                       MakeModifyAction(ITEM_TYPE2_LOG_1));
        IncGeneration(ITEM_TYPE2_LOG_1);
        auto reply9 = runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigNotificationRequest>(handle);
        CheckConfigId(reply9->Record.GetConfigId(),
                      ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_LOG_2, ITEM_TYPE1_LOG_1,
                      ITEM_TYPE2_LOG_1, ITEM_TENANT2_LOG_1, ITEM_HOST34_LOG_1,
                      ITEM_NODE34_LOG_1);
        AcceptConfig(runtime, handle);

        // Modify item's node type scope to mismatch subscription -> notification.
        ITEM_TYPE1_LOG_1.MutableUsageScope()->MutableTenantAndNodeTypeFilter()->SetNodeType("type2");
        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                       MakeModifyAction(ITEM_TYPE1_LOG_1));
        IncGeneration(ITEM_TYPE1_LOG_1);
        auto reply10 = runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigNotificationRequest>(handle);
        CheckConfigId(reply10->Record.GetConfigId(),
                      ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_LOG_2, ITEM_TYPE2_LOG_1,
                      ITEM_TENANT2_LOG_1, ITEM_HOST34_LOG_1, ITEM_NODE34_LOG_1);
        AcceptConfig(runtime, handle);
    }

    Y_UNIT_TEST(TestNotificationForModifiedConfigItemScope) {
        TestNotificationForModifiedConfigItemScopeBase(false);
        TestNotificationForModifiedConfigItemScopeBase(true);
    }

    void TestNotificationForRemovedConfigItemBase(bool useService)
    {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        InitializeTestConfigItems();
        StartConfigProxy(runtime);
        TAutoPtr<IEventHandle> handle;
        TActorId serviceId;
        ui64 tabletId = 0;

        if (useService)
            serviceId = TActorId(runtime.GetNodeId(0), "configproxy");
        else
            tabletId = CONFIG_PROXY_TABLET_ID;

        auto id1 = CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                                  MakeAddAction(ITEM_DOMAIN_LOG_1),
                                  MakeAddAction(ITEM_DOMAIN_LOG_2),
                                  MakeAddAction(ITEM_NODE12_LOG_1),
                                  MakeAddAction(ITEM_NODE34_LOG_1),
                                  MakeAddAction(ITEM_HOST12_LOG_1),
                                  MakeAddAction(ITEM_HOST34_LOG_1),
                                  MakeAddAction(ITEM_TENANT1_LOG_1),
                                  MakeAddAction(ITEM_TENANT2_LOG_1),
                                  MakeAddAction(ITEM_TYPE1_LOG_1),
                                  MakeAddAction(ITEM_TYPE2_LOG_1),
                                  MakeAddAction(ITEM_DOMAIN_TENANT_POOL_1));
        AssignIds(id1, ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_LOG_2, ITEM_NODE12_LOG_1,
                  ITEM_NODE34_LOG_1, ITEM_HOST12_LOG_1, ITEM_HOST34_LOG_1,
                  ITEM_TENANT1_LOG_1, ITEM_TENANT2_LOG_1, ITEM_TYPE1_LOG_1,
                  ITEM_TYPE2_LOG_1, ITEM_DOMAIN_TENANT_POOL_1);

        // New subscription should cause notification.
        CheckAddConfigSubscription(runtime, Ydb::StatusIds::SUCCESS,
                                   1, "host1", "tenant1", "type1",
                                   tabletId, serviceId,
                                   TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}));
        auto reply1 = runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigNotificationRequest>(handle);
        CheckConfigId(reply1->Record.GetConfigId(),
                      ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_LOG_2, ITEM_TYPE1_LOG_1,
                      ITEM_TENANT1_LOG_1, ITEM_HOST12_LOG_1, ITEM_NODE12_LOG_1);
        AcceptConfig(runtime, handle);

        // Remove items with another kind/node/host/tenant/type shouldn't cause notification.
        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                       MakeRemoveAction(ITEM_DOMAIN_TENANT_POOL_1),
                       MakeRemoveAction(ITEM_NODE34_LOG_1),
                       MakeRemoveAction(ITEM_HOST34_LOG_1),
                       MakeRemoveAction(ITEM_TENANT2_LOG_1),
                       MakeRemoveAction(ITEM_TYPE2_LOG_1));
        auto reply2 = runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigNotificationRequest>(handle, TDuration::Seconds(1));
        UNIT_ASSERT(!reply2);

        // Remove item with domain scope -> notification.
        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                       MakeRemoveAction(ITEM_DOMAIN_LOG_2));
        auto reply3 = runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigNotificationRequest>(handle);
        CheckConfigId(reply3->Record.GetConfigId(),
                      ITEM_DOMAIN_LOG_1, ITEM_TYPE1_LOG_1, ITEM_TENANT1_LOG_1,
                      ITEM_HOST12_LOG_1, ITEM_NODE12_LOG_1);
        AcceptConfig(runtime, handle);

        // Remove item with matching node scope -> notification.
        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                       MakeRemoveAction(ITEM_NODE12_LOG_1));
        auto reply4 = runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigNotificationRequest>(handle);
        CheckConfigId(reply4->Record.GetConfigId(),
                      ITEM_DOMAIN_LOG_1, ITEM_TYPE1_LOG_1, ITEM_TENANT1_LOG_1,
                      ITEM_HOST12_LOG_1);
        AcceptConfig(runtime, handle);

        // Remove item with matching host scope -> notification.
        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                       MakeRemoveAction(ITEM_HOST12_LOG_1));
        auto reply5 = runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigNotificationRequest>(handle);
        CheckConfigId(reply5->Record.GetConfigId(),
                      ITEM_DOMAIN_LOG_1, ITEM_TYPE1_LOG_1, ITEM_TENANT1_LOG_1);
        AcceptConfig(runtime, handle);

        // Remove item with matching tenant scope -> notification.
        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                       MakeRemoveAction(ITEM_TENANT1_LOG_1));
        auto reply6 = runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigNotificationRequest>(handle);
        CheckConfigId(reply6->Record.GetConfigId(),
                      ITEM_DOMAIN_LOG_1, ITEM_TYPE1_LOG_1);
        AcceptConfig(runtime, handle);

        // Remove item with matching node type scope -> notification.
        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                       MakeRemoveAction(ITEM_TYPE1_LOG_1));
        auto reply7 = runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigNotificationRequest>(handle);
        CheckConfigId(reply7->Record.GetConfigId(), ITEM_DOMAIN_LOG_1);
        AcceptConfig(runtime, handle);
    }

    Y_UNIT_TEST(TestNotificationForRemovedConfigItem) {
        TestNotificationForRemovedConfigItemBase(false);
        TestNotificationForRemovedConfigItemBase(true);
    }

    void TestNotificationForRestartedClientBase(bool useService)
    {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        InitializeTestConfigItems();
        StartConfigProxy(runtime);
        TAutoPtr<IEventHandle> handle;
        TActorId serviceId;
        ui64 tabletId = 0;

        if (useService)
            serviceId = TActorId(runtime.GetNodeId(0), "configproxy");
        else
            tabletId = CONFIG_PROXY_TABLET_ID;

        auto id1 = CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                                  MakeAddAction(ITEM_DOMAIN_LOG_1));
        AssignIds(id1, ITEM_DOMAIN_LOG_1);

        // New subscription should cause notification.
        CheckAddConfigSubscription(runtime, Ydb::StatusIds::SUCCESS,
                                   1, "host1", "tenant1", "type1",
                                   tabletId, serviceId,
                                   TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}));
        auto reply1 = runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigNotificationRequest>(handle);
        CheckConfigId(reply1->Record.GetConfigId(), ITEM_DOMAIN_LOG_1);

        // Replace subscription (restart client) before accepting config update -> new notification.
        if (useService)
            CheckReplaceConfigSubscriptions(runtime, Ydb::StatusIds::SUCCESS,
                                            1, "host1", "tenant1", "type1",
                                            tabletId, serviceId,
                                            TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}));
        else
            runtime.Register(CreateTabletKiller(CONFIG_PROXY_TABLET_ID));
        auto reply2 = runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigNotificationRequest>(handle);
        CheckConfigId(reply2->Record.GetConfigId(), ITEM_DOMAIN_LOG_1);
        AcceptConfig(runtime, handle);

        // Replace subscription (restart client) after accepting config update -> new notification
        // only for service, not for tablet.
        if (useService) {
            CheckReplaceConfigSubscriptions(runtime, Ydb::StatusIds::SUCCESS,
                                            1, "host1", "tenant1", "type1",
                                            tabletId, serviceId,
                                            TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}));
            auto reply3 = runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigNotificationRequest>(handle);
            CheckConfigId(reply3->Record.GetConfigId(), ITEM_DOMAIN_LOG_1);
        } else {
            runtime.Register(CreateTabletKiller(CONFIG_PROXY_TABLET_ID));
            auto reply3 = runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigNotificationRequest>(handle, TDuration::Seconds(2));
            UNIT_ASSERT(!reply3);
        }
    }

    Y_UNIT_TEST(TestNotificationForRestartedClient) {
        TestNotificationForRestartedClientBase(false);
        TestNotificationForRestartedClientBase(true);
    }

    void TestNotificationForTimeoutedNotificationResponseBase(bool useService)
    {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        InitializeTestConfigItems();
        StartConfigProxy(runtime);
        TAutoPtr<IEventHandle> handle;
        TActorId serviceId;
        ui64 tabletId = 0;

        if (useService)
            serviceId = TActorId(runtime.GetNodeId(0), "configproxy");
        else
            tabletId = CONFIG_PROXY_TABLET_ID;

        auto id1 = CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                                  MakeAddAction(ITEM_DOMAIN_LOG_1));
        AssignIds(id1, ITEM_DOMAIN_LOG_1);

        // New subscription should cause notification.
        CheckAddConfigSubscription(runtime, Ydb::StatusIds::SUCCESS,
                                   1, "host1", "tenant1", "type1",
                                   tabletId, serviceId,
                                   TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}));
        auto reply1 = runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigNotificationRequest>(handle);
        CheckConfigId(reply1->Record.GetConfigId(), ITEM_DOMAIN_LOG_1);

        // Don't answer notification for too long and get another one.
        runtime.UpdateCurrentTime(runtime.GetCurrentTime() + TDuration::Minutes(20));
        auto reply2 = runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigNotificationRequest>(handle);
        CheckConfigId(reply2->Record.GetConfigId(), ITEM_DOMAIN_LOG_1);
    }

    Y_UNIT_TEST(TestNotificationForTimeoutedNotificationResponse) {
        TestNotificationForTimeoutedNotificationResponseBase(false);
        TestNotificationForTimeoutedNotificationResponseBase(true);
    }

    void TestNotificationForRestartedServerBase(bool useService)
    {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        InitializeTestConfigItems();
        StartConfigProxy(runtime);
        TAutoPtr<IEventHandle> handle;
        TActorId serviceId;
        ui64 tabletId = 0;

        if (useService)
            serviceId = TActorId(runtime.GetNodeId(0), "configproxy");
        else
            tabletId = CONFIG_PROXY_TABLET_ID;

        auto id1 = CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                                  MakeAddAction(ITEM_DOMAIN_LOG_1));
        AssignIds(id1, ITEM_DOMAIN_LOG_1);

        // New subscription should cause notification.
        CheckAddConfigSubscription(runtime, Ydb::StatusIds::SUCCESS,
                                   1, "host1", "tenant1", "type1",
                                   tabletId, serviceId,
                                   TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}));
        auto reply1 = runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigNotificationRequest>(handle);
        CheckConfigId(reply1->Record.GetConfigId(), ITEM_DOMAIN_LOG_1);

        // Restart server before notification response -> new notification.
        runtime.Register(CreateTabletKiller(MakeConsoleID()));
        auto reply2 = runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigNotificationRequest>(handle);
        CheckConfigId(reply2->Record.GetConfigId(), ITEM_DOMAIN_LOG_1);
        AcceptConfig(runtime, handle);
        // Wait for subscription update.
        TDispatchOptions options;
        options.FinalEvents.emplace_back(TConfigsProvider::TEvPrivate::EvUpdateSubscriptions, 1);
        runtime.DispatchEvents(options);

        // Restart server after notification response -> no new notification.
        runtime.Register(CreateTabletKiller(MakeConsoleID()));
        auto reply3 = runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigNotificationRequest>(handle, TDuration::Seconds(5));
        UNIT_ASSERT(!reply3);
    }

    Y_UNIT_TEST(TestNotificationForRestartedServer) {
        TestNotificationForRestartedServerBase(false);
        TestNotificationForRestartedServerBase(true);
    }

    void TestAddSubscriptionIdempotencyBase(bool useService)
    {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        InitializeTestConfigItems();
        StartConfigProxy(runtime);
        TAutoPtr<IEventHandle> handle;
        TActorId serviceId;
        ui64 tabletId = 0;

        if (useService)
            serviceId = TActorId(runtime.GetNodeId(0), "configproxy");
        else
            tabletId = CONFIG_PROXY_TABLET_ID;

        // Add subscriptions and check similar ones are not duplicated.
        ui64 id1 = CheckAddConfigSubscription(runtime, Ydb::StatusIds::SUCCESS,
                                              1, "host1", "tenant-1", "type1",
                                              tabletId, serviceId, {{1, 2, 3}});
        ui64 id2 = CheckAddConfigSubscription(runtime, Ydb::StatusIds::SUCCESS,
                                              1, "host1", "tenant-1", "type1",
                                              tabletId, serviceId, {{1, 2, 3}});
        ui64 id3 = CheckAddConfigSubscription(runtime, Ydb::StatusIds::SUCCESS,
                                              1, "host1", "tenant-1", "type1",
                                              tabletId, serviceId, {{1, 2}});
        ui64 id4 = CheckAddConfigSubscription(runtime, Ydb::StatusIds::SUCCESS,
                                              1, "host1", "tenant-1", "type1",
                                              tabletId, serviceId, {{1, 2}});
        ui64 id5 = CheckAddConfigSubscription(runtime, Ydb::StatusIds::SUCCESS,
                                              1, "host1", "tenant-1", "type2",
                                              tabletId, serviceId, {{1, 2, 3}});
        ui64 id6 = CheckAddConfigSubscription(runtime, Ydb::StatusIds::SUCCESS,
                                              1, "host1", "tenant-2", "type1",
                                              tabletId, serviceId, {{1, 2, 3}});
        ui64 id7 = CheckAddConfigSubscription(runtime, Ydb::StatusIds::SUCCESS,
                                              1, "host2", "tenant-1", "type1",
                                              tabletId, serviceId, {{1, 2, 3}});
        ui64 id8 = CheckAddConfigSubscription(runtime, Ydb::StatusIds::SUCCESS,
                                              2, "host1", "tenant-1", "type1",
                                              tabletId, serviceId, {{1, 2, 3}});
        UNIT_ASSERT_VALUES_EQUAL(id1, id2);
        UNIT_ASSERT_VALUES_EQUAL(id3, id4);
        UNIT_ASSERT_VALUES_UNEQUAL(id1, id4);
        UNIT_ASSERT_VALUES_UNEQUAL(id1, id5);
        UNIT_ASSERT_VALUES_UNEQUAL(id1, id6);
        UNIT_ASSERT_VALUES_UNEQUAL(id1, id7);
        UNIT_ASSERT_VALUES_UNEQUAL(id1, id8);
    }

    Y_UNIT_TEST(TestAddSubscriptionIdempotency) {
        TestAddSubscriptionIdempotencyBase(false);
        TestAddSubscriptionIdempotencyBase(true);
    }

    Y_UNIT_TEST(TestConfigNotificationRetries) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        InitializeTestConfigItems();
        //StartConfigProxy(runtime);
        //TAutoPtr<IEventHandle> handle;

        CheckAddConfigSubscription(runtime, Ydb::StatusIds::SUCCESS,
                                   100, "host100", "tenant-100", "type100",
                                   0, TActorId(100, "service"),
                                   TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}));

        ui32 undelivered = 0;
        bool attemptFinished = false;
        auto countRetries = [&](TAutoPtr<IEventHandle> &event) -> auto {
            if (event->GetTypeRewrite() == TEvents::TSystem::Undelivered) {
                if (!attemptFinished)
                    ++undelivered;
            }
            if (event->GetTypeRewrite() == TConfigsProvider::TEvPrivate::EvNotificationTimeout
                && dynamic_cast<TConfigsProvider::TEvPrivate::TEvNotificationTimeout*>(event->StaticCastAsLocal<IEventBase>())) {
                attemptFinished = true;
            }
            // Don't allow to cleanup config for missing node.
            if (event->GetTypeRewrite() == TConfigsManager::TEvPrivate::EvCleanupSubscriptions
                && dynamic_cast<TConfigsManager::TEvPrivate::TEvCleanupSubscriptions*>(event->StaticCastAsLocal<IEventBase>()))
                return TTestActorRuntime::EEventAction::DROP;
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        runtime.SetObserverFunc(countRetries);


        // When target node is down we retry with increasing
        // interval up to 1 min. One attempt lasts for 10
        // minutes. This regression test checks we don't have
        // excessive retries and don't re-create long timer
        // (loosing TConfigsProvider::TEvPrivate::TEvNotificationTimeout)
        auto *event = new TEvConsole::TEvConfigureRequest;
        CollectActions(event->Record, MakeAddAction(ITEM_DOMAIN_LOG_1));
        runtime.SendToConsole(event);

        TDispatchOptions options;
        options.FinalEvents.emplace_back([&](IEventHandle &) -> bool {
                return attemptFinished;
            });
        runtime.DispatchEvents(options);

        UNIT_ASSERT_VALUES_EQUAL(undelivered, 12);
    }

    Y_UNIT_TEST(TestConfigSubscriptionsCleanup) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        //InitializeTestConfigItems();
        //StartConfigProxy(runtime);
        //TAutoPtr<IEventHandle> handle;

        ui32 nodeId = runtime.GetNodeId(0);
        ui64 id1 = CheckAddConfigSubscription(runtime, Ydb::StatusIds::SUCCESS,
                                              nodeId, "host1", "tenant-1", "type1",
                                              0, TActorId(nodeId, "service"), TVector<ui32>({1}));
        ui64 id2 = CheckAddConfigSubscription(runtime, Ydb::StatusIds::SUCCESS,
                                              100, "host100", "tenant-100", "type100",
                                              0, TActorId(100, "service"), TVector<ui32>({1}));
        ui64 id3 = CheckAddConfigSubscription(runtime, Ydb::StatusIds::SUCCESS,
                                              100, "host100", "tenant-100", "type100",
                                              100, TActorId(), TVector<ui32>({1}));

        CheckListConfigSubscriptions(runtime, Ydb::StatusIds::SUCCESS, 0, TActorId(nodeId, "service"),
                                     id1, nodeId, "host1", "tenant-1", "type1",
                                     0, TActorId(nodeId, "service"), TVector<ui32>({1}));
        CheckListConfigSubscriptions(runtime, Ydb::StatusIds::SUCCESS, 0, TActorId(100, "service"),
                                     id2, 100, "host100", "tenant-100", "type100",
                                     0, TActorId(100, "service"), TVector<ui32>({1}));
        CheckListConfigSubscriptions(runtime, Ydb::StatusIds::SUCCESS, 100, TActorId(),
                                     id3, 100, "host100", "tenant-100", "type100",
                                     100, TActorId(), TVector<ui32>({1}));

        TDispatchOptions options;
        options.FinalEvents.emplace_back([&](IEventHandle &ev) -> bool {
                if (ev.GetTypeRewrite() == TConfigsManager::TEvPrivate::EvCleanupSubscriptions
                    && dynamic_cast<TConfigsManager::TEvPrivate::TEvCleanupSubscriptions*>(ev.StaticCastAsLocal<IEventBase>()))
                    return true;
                return false;
            }, 1);
        runtime.DispatchEvents(options);


        CheckAddConfigSubscription(runtime, Ydb::StatusIds::SUCCESS,
                                   nodeId + 1, "host2", "tenant-2", "type2",
                                   0, TActorId(nodeId + 1, "service"), TVector<ui32>({1}));
        CheckListConfigSubscriptions(runtime, Ydb::StatusIds::SUCCESS, 0, TActorId(nodeId, "service"),
                                     id1, nodeId, "host1", "tenant-1", "type1",
                                     0, TActorId(nodeId, "service"), TVector<ui32>({1}));
        CheckListConfigSubscriptions(runtime, Ydb::StatusIds::SUCCESS, 0, TActorId(100, "service"));

        CheckListConfigSubscriptions(runtime, Ydb::StatusIds::SUCCESS, 100, TActorId(),
                                     id3, 100, "host100", "tenant-100", "type100",
                                     100, TActorId(), TVector<ui32>({1}));
    }
}

Y_UNIT_TEST_SUITE(TConsoleInMemoryConfigSubscriptionTests) {
    Y_UNIT_TEST(TestSubscriptionCreate) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        InitializeTestConfigItems();

        ui32 nodeId = runtime.GetNodeId(0);
        ui32 generation = 1;
        TActorId edgeId = runtime.Sender;

        auto *event = new TEvConsole::TEvConfigSubscriptionRequest;
        event->Record.SetGeneration(generation);
        event->Record.MutableOptions()->SetNodeId(nodeId);
        event->Record.MutableOptions()->SetHost("host1");
        event->Record.MutableOptions()->SetTenant("tenant-1");
        event->Record.MutableOptions()->SetNodeType("type1");
        event->Record.AddConfigItemKinds(NKikimrConsole::TConfigItem::LogConfigItem);
        runtime.SendToPipe(MakeConsoleID(), edgeId, event, 0, GetPipeConfigWithRetries());

        runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigSubscriptionNotification>(edgeId); // initial update

        ITEM_DOMAIN_LOG_1.MutableConfig()->MutableLogConfig()->SetClusterName("cluster-1");

        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                       MakeAddAction(ITEM_DOMAIN_LOG_1));

        NKikimrConfig::TAppConfig config1;
        config1.MutableLogConfig()->SetClusterName("cluster-1");

        auto item = config1.MutableVersion()->AddItems();
        item->SetKind(NKikimrConsole::TConfigItem::LogConfigItem);
        item->SetId(1);
        item->SetGeneration(1);

        auto notification = runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigSubscriptionNotification>(edgeId);

        UNIT_ASSERT_VALUES_EQUAL(notification->Get()->Record.GetGeneration(), generation);
        UNIT_ASSERT_VALUES_EQUAL(notification->Get()->Record.GetConfig().ShortDebugString(), config1.ShortDebugString());
    }

    Y_UNIT_TEST(TestSubscriptionClient) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        InitializeTestConfigItems();

        TActorId edgeId = runtime.Sender;

        ITEM_DOMAIN_LOG_1.MutableConfig()->MutableLogConfig()->SetClusterName("cluster-1");

        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                       MakeAddAction(ITEM_DOMAIN_LOG_1));

        auto subscriber = NConsole::CreateConfigsSubscriber(edgeId, TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}), NKikimrConfig::TAppConfig());

        runtime.Register(subscriber);

        NKikimrConfig::TAppConfig config;
        config.MutableLogConfig()->SetClusterName("cluster-1");

        auto item = config.MutableVersion()->AddItems();
        item->SetKind(NKikimrConsole::TConfigItem::LogConfigItem);
        item->SetId(1);
        item->SetGeneration(1);

        auto notification = runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigSubscriptionNotification>(edgeId);

        UNIT_ASSERT_VALUES_EQUAL(notification->Get()->Record.GetGeneration(), 1);
        UNIT_ASSERT_VALUES_EQUAL(notification->Get()->Record.GetConfig().ShortDebugString(), config.ShortDebugString());
    }

    Y_UNIT_TEST(TestSubscriptionClientManyUpdates) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        InitializeTestConfigItems();

        TActorId edgeId = runtime.Sender;

        auto subscriber = NConsole::CreateConfigsSubscriber(edgeId, TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}), NKikimrConfig::TAppConfig());

        runtime.Register(subscriber);

        runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigSubscriptionNotification>(edgeId); // initial update

        for (int i = 0; i < 100; i++) {
            ITEM_DOMAIN_LOG_1.MutableConfig()->MutableLogConfig()->SetClusterName(TStringBuilder() << "cluster-" << i);

            if (i == 0) {
                CheckConfigure(runtime, Ydb::StatusIds::SUCCESS, MakeAddAction(ITEM_DOMAIN_LOG_1));
                ITEM_DOMAIN_LOG_1.MutableId()->SetId(1);
            } else {
                CheckConfigure(runtime, Ydb::StatusIds::SUCCESS, MakeModifyAction(ITEM_DOMAIN_LOG_1));
            }

            ITEM_DOMAIN_LOG_1.MutableId()->SetGeneration(i + 1);
        }

        for (int i = 0; i < 100; i++) {
            NKikimrConfig::TAppConfig config;
            config.MutableLogConfig()->SetClusterName(TStringBuilder() << "cluster-" << i);

            auto item = config.MutableVersion()->AddItems();
            item->SetKind(NKikimrConsole::TConfigItem::LogConfigItem);
            item->SetId(1);
            item->SetGeneration(i + 1);

            auto notification = runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigSubscriptionNotification>(edgeId);

            UNIT_ASSERT_VALUES_EQUAL(notification->Get()->Record.GetGeneration(), 1);
            UNIT_ASSERT_VALUES_EQUAL(notification->Get()->Record.GetConfig().ShortDebugString(), config.ShortDebugString());
        }
    }

    Y_UNIT_TEST(TestSubscriptionClientManyUpdatesAddRemove) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        TActorId edgeId = runtime.Sender;

        auto subscriber = NConsole::CreateConfigsSubscriber(edgeId, TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}), NKikimrConfig::TAppConfig());

        runtime.Register(subscriber);

        runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigSubscriptionNotification>(edgeId); // initial update

        ui64 id = 0;
        for (int i = 0; i < 100; i++) {
            if (i % 2 == 0) {
                auto logConfigItem = MakeConfigItem(NKikimrConsole::TConfigItem::LogConfigItem,
                                                    NKikimrConfig::TAppConfig(), {}, {}, "", "", 1,
                                                    NKikimrConsole::TConfigItem::MERGE, "");

                logConfigItem.MutableConfig()->MutableLogConfig()->SetClusterName(TStringBuilder() << "cluster-" << i);

                auto ids = CheckConfigure(runtime, Ydb::StatusIds::SUCCESS, MakeAddAction(logConfigItem));

                UNIT_ASSERT_VALUES_EQUAL(ids.size(), 1);

                id = ids[0];
            } else {
                CheckConfigure(runtime, Ydb::StatusIds::SUCCESS, MakeRemoveAction(id, 1));
            }
        }

        for (int i = 0; i < 100; i++) {
            NKikimrConfig::TAppConfig config;

            if (i % 2 == 0) {
                config.MutableLogConfig()->SetClusterName(TStringBuilder() << "cluster-" << i);

                auto item = config.MutableVersion()->AddItems();
                item->SetKind(NKikimrConsole::TConfigItem::LogConfigItem);
                item->SetId(i / 2 + 1);
                item->SetGeneration(1);
            }

            auto notification = runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigSubscriptionNotification>(edgeId);

            UNIT_ASSERT_VALUES_EQUAL(notification->Get()->Record.GetGeneration(), 1);
            UNIT_ASSERT_VALUES_EQUAL(notification->Get()->Record.GetConfig().ShortDebugString(), config.ShortDebugString());
        }
    }

    Y_UNIT_TEST(TestSubscriptionClientDeadCausesSubscriptionDeregistration) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        InitializeTestConfigItems();

        TActorId edgeId = runtime.Sender;

        auto subscriber = NConsole::CreateConfigsSubscriber(edgeId, TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}), NKikimrConfig::TAppConfig());
        const auto &clientId = runtime.Register(subscriber);

        TDispatchOptions options1;
        options1.FinalEvents.emplace_back(TEvConsole::EvConfigSubscriptionResponse, 1);
        runtime.DispatchEvents(options1);

        runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigSubscriptionNotification>(edgeId); // initial update

        runtime.Send(new IEventHandle(clientId, edgeId, new TEvents::TEvPoisonPill()), 0, true);

        TDispatchOptions options2;
        options2.FinalEvents.emplace_back(TEvConsole::EvConfigSubscriptionCanceled, 3);
        runtime.DispatchEvents(options2);
    }

    Y_UNIT_TEST(TestSubscriptionClientReconnectsOnConnectionLoose) {
        TTenantTestRuntime runtime(MultipleNodesConsoleTestConfig());
        InitializeTestConfigItems();

        TActorId edgeId = runtime.AllocateEdgeActor(1);

        ITEM_DOMAIN_LOG_1.MutableConfig()->MutableLogConfig()->SetClusterName("cluster-1");

        auto ids = CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                                  MakeAddAction(ITEM_DOMAIN_LOG_1));

        AssignIds(ids, ITEM_DOMAIN_LOG_1);

        auto subscriber = NConsole::CreateConfigsSubscriber(edgeId, TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}), NKikimrConfig::TAppConfig());
        runtime.Register(subscriber, 1);

        TDispatchOptions options1;
        options1.FinalEvents.emplace_back(TEvConsole::EvConfigSubscriptionResponse, 1);
        runtime.DispatchEvents(options1);

        NKikimrConfig::TAppConfig config1;
        config1.MutableLogConfig()->SetClusterName("cluster-1");
        {
            auto item = config1.MutableVersion()->AddItems();
            item->SetKind(NKikimrConsole::TConfigItem::LogConfigItem);
            item->SetId(1);
            item->SetGeneration(1);
        }

        auto notification1 = runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigSubscriptionNotification>(edgeId);

        UNIT_ASSERT_VALUES_EQUAL(notification1->Get()->Record.GetGeneration(), 1);
        UNIT_ASSERT_VALUES_EQUAL(notification1->Get()->Record.GetConfig().ShortDebugString(), config1.ShortDebugString());

        TAutoPtr<IEventHandle> handle =  new IEventHandle(
            runtime.GetInterconnectProxy(0, 1),
            runtime.Sender,
            new TEvInterconnect::TEvDisconnect());

        runtime.Send(handle.Release(), 0, true);

        TDispatchOptions options2;
        options2.FinalEvents.emplace_back(TEvConsole::EvConfigSubscriptionResponse, 1);
        runtime.DispatchEvents(options2);

        ITEM_DOMAIN_LOG_1.MutableConfig()->MutableLogConfig()->SetClusterName("cluster-2");

        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                       MakeModifyAction(ITEM_DOMAIN_LOG_1));

        NKikimrConfig::TAppConfig config2;
        config2.MutableLogConfig()->SetClusterName("cluster-2");
        {
            auto item = config2.MutableVersion()->AddItems();
            item->SetKind(NKikimrConsole::TConfigItem::LogConfigItem);
            item->SetId(1);
            item->SetGeneration(2);
        }

        auto notification2 = runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigSubscriptionNotification>(edgeId);

        UNIT_ASSERT_VALUES_EQUAL(notification2->Get()->Record.GetGeneration(), 2);
        UNIT_ASSERT_VALUES_EQUAL(notification2->Get()->Record.GetConfig().ShortDebugString(), config2.ShortDebugString());
    }

    Y_UNIT_TEST(TestNoYamlWithoutFlag) {
        TTenantTestRuntime runtime(MultipleNodesConsoleTestConfig());
        InitializeTestConfigItems();

        TActorId edgeId = runtime.AllocateEdgeActor(1);

        auto subscriber = NConsole::CreateConfigsSubscriber(
            edgeId,
            TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}),
            NKikimrConfig::TAppConfig(),
            0,
            false);
        runtime.Register(subscriber, 1);

        TDispatchOptions options1;
        options1.FinalEvents.emplace_back(TEvConsole::EvConfigSubscriptionResponse, 1);
        runtime.DispatchEvents(options1);

        runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigSubscriptionNotification>(edgeId); // initial update

        ITEM_DOMAIN_LOG_1.MutableConfig()->MutableLogConfig()->SetClusterName("cluster-1");

        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                       MakeAddAction(ITEM_DOMAIN_LOG_1));

        NKikimrConfig::TAppConfig config;
        config.MutableLogConfig()->SetClusterName("cluster-1");

        auto item = config.MutableVersion()->AddItems();
        item->SetKind(NKikimrConsole::TConfigItem::LogConfigItem);
        item->SetId(1);
        item->SetGeneration(1);

        auto notification = runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigSubscriptionNotification>(edgeId);

        UNIT_ASSERT_VALUES_EQUAL(notification->Get()->Record.GetGeneration(), 1);
        UNIT_ASSERT_VALUES_EQUAL(notification->Get()->Record.GetConfig().ShortDebugString(), config.ShortDebugString());

        CheckReplaceConfig(runtime, Ydb::StatusIds::SUCCESS, YAML_CONFIG_1);

        ITEM_DOMAIN_LOG_2.MutableConfig()->MutableLogConfig()->SetClusterName("cluster-2");

        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                       MakeAddAction(ITEM_DOMAIN_LOG_2));

        config.MutableLogConfig()->SetClusterName("cluster-2");
        item = config.MutableVersion()->AddItems();
        item->SetKind(NKikimrConsole::TConfigItem::LogConfigItem);
        item->SetId(2);
        item->SetGeneration(1);

        notification = runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigSubscriptionNotification>(edgeId);
        UNIT_ASSERT_VALUES_EQUAL(notification->Get()->Record.GetGeneration(), 1);
        UNIT_ASSERT_VALUES_EQUAL(notification->Get()->Record.GetConfig().ShortDebugString(), config.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(notification->Get()->Record.HasYamlConfig(), false);
    }

    Y_UNIT_TEST(TestConsoleRestart) {
        TTenantTestRuntime runtime(MultipleNodesConsoleTestConfig());
        InitializeTestConfigItems();

        TActorId edgeId = runtime.AllocateEdgeActor(1);

        ITEM_DOMAIN_LOG_1.MutableConfig()->MutableLogConfig()->SetClusterName("cluster-1");

        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                       MakeAddAction(ITEM_DOMAIN_LOG_1));

        CheckReplaceConfig(runtime, Ydb::StatusIds::SUCCESS, YAML_CONFIG_1);

        GracefulRestartTablet(runtime, MakeConsoleID(), runtime.AllocateEdgeActor(0));

        auto subscriber = NConsole::CreateConfigsSubscriber(
            edgeId,
            TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}),
            NKikimrConfig::TAppConfig(),
            0,
            true,
            1);
        runtime.Register(subscriber, 1);

        NKikimrConfig::TAppConfig config;
        config.MutableLogConfig()->SetClusterName("cluster-1");

        auto item = config.MutableVersion()->AddItems();
        item->SetKind(NKikimrConsole::TConfigItem::LogConfigItem);
        item->SetId(1);
        item->SetGeneration(1);

        auto notification = runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigSubscriptionNotification>(edgeId);
        UNIT_ASSERT_VALUES_EQUAL(notification->Get()->Record.GetGeneration(), 1);
        UNIT_ASSERT_VALUES_EQUAL(notification->Get()->Record.GetConfig().ShortDebugString(), config.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(notification->Get()->Record.GetYamlConfig(), YAML_CONFIG_1_UPDATED);
    }

    Y_UNIT_TEST(TestComplexYamlConfigChanges) {
        TTenantTestRuntime runtime(MultipleNodesConsoleTestConfig());
        InitializeTestConfigItems();

        TActorId edgeId = runtime.AllocateEdgeActor(1);

        ITEM_DOMAIN_LOG_1.MutableConfig()->MutableLogConfig()->SetClusterName("cluster-1");

        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                       MakeAddAction(ITEM_DOMAIN_LOG_1));

        auto subscriber = NConsole::CreateConfigsSubscriber(
            edgeId,
            TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::LogConfigItem}),
            NKikimrConfig::TAppConfig(),
            0,
            true,
            1);
        runtime.Register(subscriber, 1);

        TDispatchOptions options1;
        options1.FinalEvents.emplace_back(TEvConsole::EvConfigSubscriptionResponse, 1);
        runtime.DispatchEvents(options1);

        NKikimrConfig::TAppConfig config;
        config.MutableLogConfig()->SetClusterName("cluster-1");

        auto item = config.MutableVersion()->AddItems();
        item->SetKind(NKikimrConsole::TConfigItem::LogConfigItem);
        item->SetId(1);
        item->SetGeneration(1);

        auto notification = runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigSubscriptionNotification>(edgeId);

        UNIT_ASSERT_VALUES_EQUAL(notification->Get()->Record.GetGeneration(), 1);
        UNIT_ASSERT_VALUES_EQUAL(notification->Get()->Record.GetConfig().ShortDebugString(), config.ShortDebugString());

        CheckReplaceConfig(runtime, Ydb::StatusIds::SUCCESS, YAML_CONFIG_1);

        notification = runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigSubscriptionNotification>(edgeId);
        UNIT_ASSERT_VALUES_EQUAL(notification->Get()->Record.GetGeneration(), 1);
        UNIT_ASSERT_VALUES_EQUAL(notification->Get()->Record.GetConfig().ShortDebugString(), config.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(notification->Get()->Record.GetYamlConfig(), YAML_CONFIG_1_UPDATED);

        CheckAddVolatileConfig(runtime, Ydb::StatusIds::SUCCESS, "", 1, 0, VOLATILE_YAML_CONFIG_1_1);
        notification = runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigSubscriptionNotification>(edgeId);

        UNIT_ASSERT_VALUES_EQUAL(notification->Get()->Record.GetGeneration(), 1);
        UNIT_ASSERT_VALUES_EQUAL(notification->Get()->Record.GetConfig().ShortDebugString(), config.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(notification->Get()->Record.GetYamlConfig(), YAML_CONFIG_1_UPDATED);
        UNIT_ASSERT_VALUES_EQUAL(notification->Get()->Record.VolatileConfigsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(notification->Get()->Record.GetVolatileConfigs()[0].GetConfig(), VOLATILE_YAML_CONFIG_1_1);

        runtime.SendToConsole(new TEvConsole::TEvGetAllConfigsRequest());
        TAutoPtr<IEventHandle> handle;
        auto configs = runtime.GrabEdgeEventRethrow<TEvConsole::TEvGetAllConfigsResponse>(handle);
        UNIT_ASSERT_VALUES_EQUAL(configs->Record.GetResponse().identity().cluster(), "");
        UNIT_ASSERT_VALUES_EQUAL(configs->Record.GetResponse().identity().version(), 1);
        UNIT_ASSERT_VALUES_EQUAL(configs->Record.GetResponse().config(), YAML_CONFIG_1_UPDATED);
        UNIT_ASSERT_VALUES_EQUAL(configs->Record.GetResponse().volatile_configs_size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(configs->Record.GetResponse().volatile_configs(0).config(), EXTENDED_VOLATILE_YAML_CONFIG_1_1);

        CheckReplaceConfig(runtime, Ydb::StatusIds::SUCCESS, YAML_CONFIG_1);
        CheckAddVolatileConfig(runtime, Ydb::StatusIds::SUCCESS, "", 1, 0, VOLATILE_YAML_CONFIG_1_1);

        ITEM_DOMAIN_LOG_2.MutableConfig()->MutableLogConfig()->SetClusterName("cluster-2");

        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                       MakeAddAction(ITEM_DOMAIN_LOG_2));

        config.MutableLogConfig()->SetClusterName("cluster-2");
        item = config.MutableVersion()->AddItems();
        item->SetKind(NKikimrConsole::TConfigItem::LogConfigItem);
        item->SetId(2);
        item->SetGeneration(1);

        notification = runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigSubscriptionNotification>(edgeId);

        UNIT_ASSERT_VALUES_EQUAL(notification->Get()->Record.GetGeneration(), 1);
        UNIT_ASSERT_VALUES_EQUAL(notification->Get()->Record.GetConfig().ShortDebugString(), config.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(notification->Get()->Record.GetYamlConfig(), YAML_CONFIG_1_UPDATED);
        UNIT_ASSERT_VALUES_EQUAL(notification->Get()->Record.VolatileConfigsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(notification->Get()->Record.GetVolatileConfigs()[0].GetConfig(), VOLATILE_YAML_CONFIG_1_1);

        CheckAddVolatileConfig(runtime, Ydb::StatusIds::SUCCESS, "", 1, 1, VOLATILE_YAML_CONFIG_1_2);
        notification = runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigSubscriptionNotification>(edgeId);

        UNIT_ASSERT_VALUES_EQUAL(notification->Get()->Record.GetGeneration(), 1);
        UNIT_ASSERT_VALUES_EQUAL(notification->Get()->Record.GetConfig().ShortDebugString(), config.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(notification->Get()->Record.GetYamlConfig(), YAML_CONFIG_1_UPDATED);
        UNIT_ASSERT_VALUES_EQUAL(notification->Get()->Record.VolatileConfigsSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(notification->Get()->Record.GetVolatileConfigs()[0].GetConfig(), VOLATILE_YAML_CONFIG_1_1);
        UNIT_ASSERT_VALUES_EQUAL(notification->Get()->Record.GetVolatileConfigs()[1].GetConfig(), VOLATILE_YAML_CONFIG_1_2);

        CheckRemoveVolatileConfig(runtime, Ydb::StatusIds::SUCCESS, "", 1, 0);
        notification = runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigSubscriptionNotification>(edgeId);

        UNIT_ASSERT_VALUES_EQUAL(notification->Get()->Record.GetGeneration(), 1);
        UNIT_ASSERT_VALUES_EQUAL(notification->Get()->Record.GetConfig().ShortDebugString(), config.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(notification->Get()->Record.GetYamlConfig(), YAML_CONFIG_1_UPDATED);
        UNIT_ASSERT_VALUES_EQUAL(notification->Get()->Record.VolatileConfigsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(notification->Get()->Record.GetVolatileConfigs()[0].GetConfig(), VOLATILE_YAML_CONFIG_1_2);

        CheckReplaceConfig(runtime, Ydb::StatusIds::SUCCESS, YAML_CONFIG_2);
        notification = runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigSubscriptionNotification>(edgeId);

        UNIT_ASSERT_VALUES_EQUAL(notification->Get()->Record.GetGeneration(), 1);
        UNIT_ASSERT_VALUES_EQUAL(notification->Get()->Record.GetConfig().ShortDebugString(), config.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(notification->Get()->Record.GetYamlConfig(), YAML_CONFIG_2_UPDATED);
        UNIT_ASSERT_VALUES_EQUAL(notification->Get()->Record.VolatileConfigsSize(), 0);
    }

    Y_UNIT_TEST(TestNoYamlResend) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        InitializeTestConfigItems();

        ui32 nodeId = runtime.GetNodeId(0);
        ui32 generation = 1;
        TActorId edgeId = runtime.Sender;

        auto *event = new TEvConsole::TEvConfigSubscriptionRequest;
        event->Record.SetGeneration(generation);
        event->Record.MutableOptions()->SetNodeId(nodeId);
        event->Record.MutableOptions()->SetHost("host1");
        event->Record.MutableOptions()->SetTenant("tenant-1");
        event->Record.MutableOptions()->SetNodeType("type1");
        event->Record.SetServeYaml(true);
        event->Record.SetYamlApiVersion(1);
        event->Record.AddConfigItemKinds(NKikimrConsole::TConfigItem::LogConfigItem);
        runtime.SendToPipe(MakeConsoleID(), edgeId, event, 0, GetPipeConfigWithRetries());

        runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigSubscriptionNotification>(edgeId); // initial update

        CheckReplaceConfig(runtime, Ydb::StatusIds::SUCCESS, YAML_CONFIG_1);
        auto notification = runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigSubscriptionNotification>(edgeId);

        CheckAddVolatileConfig(runtime, Ydb::StatusIds::SUCCESS, "", 1, 0, VOLATILE_YAML_CONFIG_1_1);
        notification = runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigSubscriptionNotification>(edgeId);

        CheckAddVolatileConfig(runtime, Ydb::StatusIds::SUCCESS, "", 1, 1, VOLATILE_YAML_CONFIG_1_2);
        notification = runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigSubscriptionNotification>(edgeId);

        ITEM_DOMAIN_LOG_1.MutableConfig()->MutableLogConfig()->SetClusterName("cluster-1");

        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                       MakeAddAction(ITEM_DOMAIN_LOG_1));

        NKikimrConfig::TAppConfig config1;
        config1.MutableLogConfig()->SetClusterName("cluster-1");

        auto item = config1.MutableVersion()->AddItems();
        item->SetKind(NKikimrConsole::TConfigItem::LogConfigItem);
        item->SetId(1);
        item->SetGeneration(1);

        notification = runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigSubscriptionNotification>(edgeId);

        UNIT_ASSERT_VALUES_EQUAL(notification->Get()->Record.GetGeneration(), generation);
        UNIT_ASSERT_VALUES_EQUAL(notification->Get()->Record.GetConfig().ShortDebugString(), config1.ShortDebugString());
        UNIT_ASSERT(!notification->Get()->Record.HasYamlConfig());
        UNIT_ASSERT(notification->Get()->Record.GetYamlConfigNotChanged());
        for (auto &volatileConfig : notification->Get()->Record.GetVolatileConfigs()) {
            UNIT_ASSERT(volatileConfig.HasId());
            UNIT_ASSERT(!volatileConfig.HasConfig());
            UNIT_ASSERT(volatileConfig.GetNotChanged());
        }
    }

    Y_UNIT_TEST(TestSubscribeAfterConfigApply) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        InitializeTestConfigItems();

        ui32 nodeId = runtime.GetNodeId(0);
        ui32 generation = 1;
        TActorId edgeId = runtime.Sender;

        CheckReplaceConfig(runtime, Ydb::StatusIds::SUCCESS, YAML_CONFIG_1);

        CheckAddVolatileConfig(runtime, Ydb::StatusIds::SUCCESS, "", 1, 0, VOLATILE_YAML_CONFIG_1_1);

        CheckAddVolatileConfig(runtime, Ydb::StatusIds::SUCCESS, "", 1, 1, VOLATILE_YAML_CONFIG_1_2);

        ITEM_DOMAIN_LOG_1.MutableConfig()->MutableLogConfig()->SetClusterName("cluster-1");

        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                       MakeAddAction(ITEM_DOMAIN_LOG_1));

        NKikimrConfig::TAppConfig config1;
        config1.MutableLogConfig()->SetClusterName("cluster-1");

        auto item = config1.MutableVersion()->AddItems();
        item->SetKind(NKikimrConsole::TConfigItem::LogConfigItem);
        item->SetId(1);
        item->SetGeneration(1);

        auto *event = new TEvConsole::TEvConfigSubscriptionRequest;
        event->Record.SetGeneration(generation);
        event->Record.MutableOptions()->SetNodeId(nodeId);
        event->Record.MutableOptions()->SetHost("host1");
        event->Record.MutableOptions()->SetTenant("tenant-1");
        event->Record.MutableOptions()->SetNodeType("type1");
        event->Record.SetServeYaml(true);
        event->Record.SetYamlApiVersion(1);
        event->Record.AddConfigItemKinds(NKikimrConsole::TConfigItem::LogConfigItem);
        runtime.SendToPipe(MakeConsoleID(), edgeId, event, 0, GetPipeConfigWithRetries());

        auto notification = runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigSubscriptionNotification>(edgeId);

        UNIT_ASSERT_VALUES_EQUAL(notification->Get()->Record.GetGeneration(), generation);
        UNIT_ASSERT_VALUES_EQUAL(notification->Get()->Record.GetConfig().ShortDebugString(), config1.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(notification->Get()->Record.GetYamlConfig(), YAML_CONFIG_1_UPDATED);
        UNIT_ASSERT_VALUES_EQUAL(notification->Get()->Record.VolatileConfigsSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(notification->Get()->Record.GetVolatileConfigs()[0].GetConfig(), VOLATILE_YAML_CONFIG_1_1);
        UNIT_ASSERT_VALUES_EQUAL(notification->Get()->Record.GetVolatileConfigs()[1].GetConfig(), VOLATILE_YAML_CONFIG_1_2);
    }

    Y_UNIT_TEST(TestSubscribeAfterConfigApplyWithKnownConfig) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        InitializeTestConfigItems();

        ui32 nodeId = runtime.GetNodeId(0);
        ui32 generation = 1;
        TActorId edgeId = runtime.Sender;

        CheckReplaceConfig(runtime, Ydb::StatusIds::SUCCESS, YAML_CONFIG_1);

        CheckAddVolatileConfig(runtime, Ydb::StatusIds::SUCCESS, "", 1, 0, VOLATILE_YAML_CONFIG_1_1);

        CheckAddVolatileConfig(runtime, Ydb::StatusIds::SUCCESS, "", 1, 1, VOLATILE_YAML_CONFIG_1_2);

        ITEM_DOMAIN_LOG_1.MutableConfig()->MutableLogConfig()->SetClusterName("cluster-1");

        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                       MakeAddAction(ITEM_DOMAIN_LOG_1));

        NKikimrConfig::TAppConfig config1;
        config1.MutableLogConfig()->SetClusterName("cluster-1");

        auto item = config1.MutableVersion()->AddItems();
        item->SetKind(NKikimrConsole::TConfigItem::LogConfigItem);
        item->SetId(1);
        item->SetGeneration(1);

        auto prepareConfigSubscriptionRequest = [&](){
            auto *event = new TEvConsole::TEvConfigSubscriptionRequest;
            event->Record.SetGeneration(generation);
            event->Record.MutableOptions()->SetNodeId(nodeId);
            event->Record.MutableOptions()->SetHost("host1");
            event->Record.MutableOptions()->SetTenant("tenant-1");
            event->Record.MutableOptions()->SetNodeType("type1");
            event->Record.SetServeYaml(true);
            event->Record.SetYamlApiVersion(1);
            event->Record.SetYamlVersion(1);
            event->Record.MutableKnownVersion()->CopyFrom(config1.GetVersion());
            return event;
        };

        auto *event = prepareConfigSubscriptionRequest();
        auto *volatileYamlVersion = event->Record.AddVolatileYamlVersion();
        volatileYamlVersion->SetId(0);
        volatileYamlVersion->SetHash(VOLATILE_YAML_CONFIG_1_1_HASH);
        volatileYamlVersion = event->Record.AddVolatileYamlVersion();
        volatileYamlVersion->SetId(1);
        volatileYamlVersion->SetHash(VOLATILE_YAML_CONFIG_1_2_HASH);
        event->Record.AddConfigItemKinds(NKikimrConsole::TConfigItem::LogConfigItem);
        runtime.SendToPipe(MakeConsoleID(), edgeId, event, 0, GetPipeConfigWithRetries());

        runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigSubscriptionNotification>(edgeId); // initial update

        generation = 2;

        event = prepareConfigSubscriptionRequest();
        volatileYamlVersion = event->Record.AddVolatileYamlVersion();
        volatileYamlVersion->SetId(0);
        volatileYamlVersion->SetHash(VOLATILE_YAML_CONFIG_1_1_HASH);
        runtime.SendToPipe(MakeConsoleID(), edgeId, event, 0, GetPipeConfigWithRetries());

        auto notification = runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigSubscriptionNotification>(edgeId);

        UNIT_ASSERT_VALUES_EQUAL(notification->Get()->Record.GetGeneration(), generation);
        UNIT_ASSERT_VALUES_EQUAL(notification->Get()->Record.GetConfig().GetVersion().ShortDebugString(), config1.GetVersion().ShortDebugString());
        UNIT_ASSERT(!notification->Get()->Record.HasYamlConfig());
        UNIT_ASSERT(notification->Get()->Record.GetYamlConfigNotChanged());
        UNIT_ASSERT_VALUES_EQUAL(notification->Get()->Record.VolatileConfigsSize(), 2);
        UNIT_ASSERT(notification->Get()->Record.GetVolatileConfigs()[0].GetNotChanged());
        UNIT_ASSERT_VALUES_EQUAL(notification->Get()->Record.GetVolatileConfigs()[1].GetConfig(), VOLATILE_YAML_CONFIG_1_2);

        generation = 3;

        event = prepareConfigSubscriptionRequest();
        volatileYamlVersion = event->Record.AddVolatileYamlVersion();
        volatileYamlVersion->SetId(1);
        volatileYamlVersion->SetHash(VOLATILE_YAML_CONFIG_1_2_HASH);
        runtime.SendToPipe(MakeConsoleID(), edgeId, event, 0, GetPipeConfigWithRetries());

        notification = runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigSubscriptionNotification>(edgeId);

        UNIT_ASSERT_VALUES_EQUAL(notification->Get()->Record.GetGeneration(), generation);
        UNIT_ASSERT_VALUES_EQUAL(notification->Get()->Record.GetConfig().GetVersion().ShortDebugString(), config1.GetVersion().ShortDebugString());
        UNIT_ASSERT(!notification->Get()->Record.HasYamlConfig());
        UNIT_ASSERT(notification->Get()->Record.GetYamlConfigNotChanged());
        UNIT_ASSERT_VALUES_EQUAL(notification->Get()->Record.VolatileConfigsSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(notification->Get()->Record.GetVolatileConfigs()[0].GetConfig(), VOLATILE_YAML_CONFIG_1_1);
        UNIT_ASSERT(notification->Get()->Record.GetVolatileConfigs()[1].GetNotChanged());

        generation = 4;

        event = prepareConfigSubscriptionRequest();
        volatileYamlVersion = event->Record.AddVolatileYamlVersion();
        volatileYamlVersion->SetId(0);
        volatileYamlVersion->SetHash(VOLATILE_YAML_CONFIG_1_1_HASH);
        volatileYamlVersion = event->Record.AddVolatileYamlVersion();
        volatileYamlVersion->SetId(1);
        volatileYamlVersion->SetHash(VOLATILE_YAML_CONFIG_1_2_HASH);
        volatileYamlVersion = event->Record.AddVolatileYamlVersion();
        volatileYamlVersion->SetId(2);
        volatileYamlVersion->SetHash(VOLATILE_YAML_CONFIG_1_2_HASH);
        runtime.SendToPipe(MakeConsoleID(), edgeId, event, 0, GetPipeConfigWithRetries());

        notification = runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigSubscriptionNotification>(edgeId);

        UNIT_ASSERT_VALUES_EQUAL(notification->Get()->Record.GetGeneration(), generation);
        UNIT_ASSERT_VALUES_EQUAL(notification->Get()->Record.GetConfig().GetVersion().ShortDebugString(), config1.GetVersion().ShortDebugString());
        UNIT_ASSERT(!notification->Get()->Record.HasYamlConfig());
        UNIT_ASSERT(notification->Get()->Record.GetYamlConfigNotChanged());
        UNIT_ASSERT_VALUES_EQUAL(notification->Get()->Record.VolatileConfigsSize(), 2);
        UNIT_ASSERT(notification->Get()->Record.GetVolatileConfigs()[0].GetNotChanged());
        UNIT_ASSERT(notification->Get()->Record.GetVolatileConfigs()[1].GetNotChanged());
    }
}

Y_UNIT_TEST_SUITE(TConsoleConfigHelpersTests) {
    Y_UNIT_TEST(TestConfigCourier) {
        TTenantTestRuntime runtime(TenantConsoleTestConfig());
        InitializeTestConfigItems();
        StartConfigProxy(runtime);
        TAutoPtr<IEventHandle> handle;

        ITEM_TENANT1_LOG_1.MutableUsageScope()->MutableTenantAndNodeTypeFilter()->SetTenant(TENANT1_1_NAME);
        ITEM_TENANT1_LOG_1.MutableConfig()->MutableLogConfig()->SetClusterName("cluster1");
        ITEM_TENANT2_LOG_1.MutableUsageScope()->MutableTenantAndNodeTypeFilter()->SetTenant(TENANT1_2_NAME);
        ITEM_TENANT2_LOG_1.MutableConfig()->MutableLogConfig()->SetClusterName("cluster2");
        ITEM_DOMAIN_TENANT_POOL_1.MutableConfig()->MutableTenantPoolConfig();

        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                       MakeAddAction(ITEM_TENANT1_LOG_1),
                       MakeAddAction(ITEM_TENANT2_LOG_1),
                       MakeAddAction(ITEM_DOMAIN_TENANT_POOL_1));

        NKikimrConfig::TAppConfig config1;
        config1.MutableTenantPoolConfig();
        config1.MutableLogConfig()->SetClusterName("cluster1");
        auto *courier1 = CreateNodeConfigCourier(runtime.Sender, 123);
        runtime.Register(courier1, 0);
        auto reply1 = runtime.GrabEdgeEventRethrow<TEvConsole::TEvGetNodeConfigResponse>(handle);
        UNIT_ASSERT_VALUES_EQUAL(handle->Cookie, 123);
        CheckEqualsIgnoringVersion(reply1->Record.GetConfig(), config1);

        NKikimrConfig::TAppConfig config2;
        config2.MutableLogConfig()->SetClusterName("cluster2");
        auto *courier2 = CreateNodeConfigCourier((ui32)NKikimrConsole::TConfigItem::LogConfigItem,
                                                 TENANT1_2_NAME, runtime.Sender, 321);
        runtime.Register(courier2, 0);
        auto reply2 = runtime.GrabEdgeEventRethrow<TEvConsole::TEvGetNodeConfigResponse>(handle);
        UNIT_ASSERT_VALUES_EQUAL(handle->Cookie, 321);
        CheckEqualsIgnoringVersion(reply2->Record.GetConfig(), config2);
    }

    void TestConfigSubscriberBase(bool useService)
    {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        InitializeTestConfigItems();
        StartConfigProxy(runtime);
        TAutoPtr<IEventHandle> handle;
        TActorId serviceId;
        ui64 tabletId = 0;
        IActor *subscriber;

        if (useService)
            serviceId = TActorId(runtime.GetNodeId(0), "configproxy");
        else
            tabletId = CONFIG_PROXY_TABLET_ID;

        ui64 id1 = CheckAddConfigSubscription(runtime, Ydb::StatusIds::SUCCESS,
                                              1, "host1", "tenant-1", "type1",
                                              tabletId, serviceId, {{1, 2, 3}});

        if (useService)
            subscriber = CreateConfigSubscriber(serviceId,
                                                TVector<ui32>({1, 2}),
                                                "tenant1",
                                                runtime.Sender,
                                                true,
                                                123);
        else
            subscriber = CreateConfigSubscriber(tabletId,
                                                TVector<ui32>({1, 2}),
                                                "tenant1",
                                                runtime.Sender,
                                                true,
                                                123);
        runtime.Register(subscriber, 0);

        auto reply1 = runtime.GrabEdgeEventRethrow<TEvConsole::TEvReplaceConfigSubscriptionsResponse>(handle);
        UNIT_ASSERT_VALUES_EQUAL(reply1->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(handle->Cookie, 123);
        ui64 id2 = reply1->Record.GetSubscriptionId();

        // Subscriber replaced old configs with a new one.
        CheckGetConfigSubscription(runtime, Ydb::StatusIds::NOT_FOUND, id1);
        CheckGetConfigSubscription(runtime, Ydb::StatusIds::SUCCESS, id2,
                                   runtime.GetNodeId(0), FQDNHostName(), "tenant1", "type1",
                                   tabletId, serviceId, {{1, 2}});

        if (useService)
            subscriber = CreateConfigSubscriber(serviceId,
                                                TVector<ui32>({1, 2, 3}),
                                                "tenant1",
                                                runtime.Sender,
                                                false,
                                                321);
        else
            subscriber = CreateConfigSubscriber(tabletId,
                                                TVector<ui32>({1, 2, 3}),
                                                "tenant1",
                                                runtime.Sender,
                                                false,
                                                321);
        runtime.Register(subscriber, 0);

        auto reply2 = runtime.GrabEdgeEventRethrow<TEvConsole::TEvAddConfigSubscriptionResponse>(handle);
        UNIT_ASSERT_VALUES_EQUAL(reply2->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(handle->Cookie, 321);
        ui64 id3 = reply2->Record.GetSubscriptionId();

        // This time we asked to not remove existing configs.
        CheckGetConfigSubscription(runtime, Ydb::StatusIds::SUCCESS, id2,
                                   runtime.GetNodeId(0), FQDNHostName(), "tenant1", "type1",
                                   tabletId, serviceId, {{1, 2}});
        CheckGetConfigSubscription(runtime, Ydb::StatusIds::SUCCESS, id3,
                                   runtime.GetNodeId(0), FQDNHostName(), "tenant1", "type1",
                                   tabletId, serviceId, {{1, 2, 3}});
    }

    Y_UNIT_TEST(TestConfigSubscriber) {
        TestConfigSubscriberBase(false);
        TestConfigSubscriberBase(true);
    }

    void TestConfigSubscriberAutoTenantTenantBase(bool useService)
    {
        TTenantTestRuntime runtime(TenantConsoleTestConfig());
        InitializeTestConfigItems();
        StartConfigProxy(runtime);
        TAutoPtr<IEventHandle> handle;
        TActorId serviceId;
        ui64 tabletId = 0;
        IActor *subscriber;

        if (useService) {
            serviceId = TActorId(runtime.GetNodeId(0), "configproxy");
            subscriber = CreateConfigSubscriber(serviceId,
                                                TVector<ui32>({1, 2}),
                                                runtime.Sender);
        } else {
            tabletId = CONFIG_PROXY_TABLET_ID;
            subscriber = CreateConfigSubscriber(tabletId,
                                                TVector<ui32>({1, 2}),
                                                runtime.Sender);
        }
        runtime.Register(subscriber, 0);

        auto reply1 = runtime.GrabEdgeEventRethrow<TEvConsole::TEvReplaceConfigSubscriptionsResponse>(handle);
        UNIT_ASSERT_VALUES_EQUAL(reply1->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
        ui64 id1 = reply1->Record.GetSubscriptionId();

        CheckGetConfigSubscription(runtime, Ydb::StatusIds::SUCCESS, id1,
                                   runtime.GetNodeId(0), FQDNHostName(), TENANT1_1_NAME, "type1",
                                   tabletId, serviceId, {{1, 2}});
    }

    Y_UNIT_TEST(TestConfigSubscriberAutoTenantTenant) {
        TestConfigSubscriberAutoTenantTenantBase(false);
        TestConfigSubscriberAutoTenantTenantBase(true);
    }

    void TestConfigSubscriberAutoTenantMultipleTenantsBase(bool useService)
    {
        TTenantTestRuntime runtime(MultipleTenantsConsoleTestConfig());
        InitializeTestConfigItems();
        StartConfigProxy(runtime);
        TAutoPtr<IEventHandle> handle;
        TActorId serviceId;
        ui64 tabletId = 0;
        IActor *subscriber;

        if (useService) {
            serviceId = TActorId(runtime.GetNodeId(0), "configproxy");
            subscriber = CreateConfigSubscriber(serviceId,
                                                TVector<ui32>({1, 2}),
                                                runtime.Sender);
        } else {
            tabletId = CONFIG_PROXY_TABLET_ID;
            subscriber = CreateConfigSubscriber(tabletId,
                                                TVector<ui32>({1, 2}),
                                                runtime.Sender);
        }
        runtime.Register(subscriber, 0);

        auto reply1 = runtime.GrabEdgeEventRethrow<TEvConsole::TEvReplaceConfigSubscriptionsResponse>(handle);
        UNIT_ASSERT_VALUES_EQUAL(reply1->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
        ui64 id1 = reply1->Record.GetSubscriptionId();

        CheckGetConfigSubscription(runtime, Ydb::StatusIds::SUCCESS, id1,
                                   runtime.GetNodeId(0), FQDNHostName(), "<multiple>", "type1",
                                   tabletId, serviceId, {{1, 2}});
    }

    Y_UNIT_TEST(TestConfigSubscriberAutoTenantMultipleTenants) {
        TestConfigSubscriberAutoTenantMultipleTenantsBase(false);
        TestConfigSubscriberAutoTenantMultipleTenantsBase(true);
    }

    void TestConfigSubscriberAutoTenantDomainBase(bool useService)
    {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        InitializeTestConfigItems();
        StartConfigProxy(runtime);
        TAutoPtr<IEventHandle> handle;
        TActorId serviceId;
        ui64 tabletId = 0;
        IActor *subscriber;

        if (useService) {
            serviceId = TActorId(runtime.GetNodeId(0), "configproxy");
            subscriber = CreateConfigSubscriber(serviceId,
                                                TVector<ui32>({1, 2}),
                                                runtime.Sender);
        } else {
            tabletId = CONFIG_PROXY_TABLET_ID;
            subscriber = CreateConfigSubscriber(tabletId,
                                                TVector<ui32>({1, 2}),
                                                runtime.Sender);
        }
        runtime.Register(subscriber, 0);

        auto reply1 = runtime.GrabEdgeEventRethrow<TEvConsole::TEvReplaceConfigSubscriptionsResponse>(handle);
        UNIT_ASSERT_VALUES_EQUAL(reply1->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
        ui64 id1 = reply1->Record.GetSubscriptionId();

        CheckGetConfigSubscription(runtime, Ydb::StatusIds::SUCCESS, id1,
                                   runtime.GetNodeId(0), FQDNHostName(), CanonizePath(DOMAIN1_NAME), "type1",
                                   tabletId, serviceId, {{1, 2}});
    }

    Y_UNIT_TEST(TestConfigSubscriberAutoTenantDomain) {
        TestConfigSubscriberAutoTenantDomainBase(false);
        TestConfigSubscriberAutoTenantDomainBase(true);
    }

    Y_UNIT_TEST(TestConfigSubscriptionEraser) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        InitializeTestConfigItems();
        StartConfigProxy(runtime);
        TAutoPtr<IEventHandle> handle;

        ui64 id1 = CheckAddConfigSubscription(runtime, Ydb::StatusIds::SUCCESS,
                                              1, "host1", "tenant-1", "type1",
                                              CONFIG_PROXY_TABLET_ID, TActorId(), {{1, 2, 3}});

        auto *eraser = CreateSubscriptionEraser(id1, runtime.Sender, 123);
        runtime.Register(eraser, 0);
        auto reply = runtime.GrabEdgeEventRethrow<TEvConsole::TEvRemoveConfigSubscriptionResponse>(handle);
        UNIT_ASSERT_VALUES_EQUAL(reply->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(handle->Cookie, 123);
    }
}

} // namespace NKikimr

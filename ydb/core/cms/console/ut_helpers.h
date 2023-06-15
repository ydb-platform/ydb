#pragma once

#include "config_helpers.h"
#include "console_configs_provider.h"
#include "console_impl.h"
#include "console_tenants_manager.h"

#include <ydb/core/cms/console/util/config_index.h>
#include <ydb/core/testlib/tenant_runtime.h>
#include <ydb/core/testlib/tenant_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NConsole::NUT {

inline NKikimrConsole::TUsageScope MakeUsageScope(const TVector<ui32> &nodes)
{
    NKikimrConsole::TUsageScope res;
    auto &filter = *res.MutableNodeFilter();
    for (auto &host : nodes)
        filter.AddNodes(host);
    return res;
}

inline NKikimrConsole::TUsageScope MakeUsageScope(const TVector<TString> &hosts)
{
    NKikimrConsole::TUsageScope res;
    auto &filter = *res.MutableHostFilter();
    for (auto &host : hosts)
        filter.AddHosts(host);
    return res;
}

inline NKikimrConsole::TUsageScope MakeUsageScope(const TString &tenant, const TString &nodeType)
{
    NKikimrConsole::TUsageScope res;
    res.MutableTenantAndNodeTypeFilter()->SetTenant(tenant);
    res.MutableTenantAndNodeTypeFilter()->SetNodeType(nodeType);
    return res;
}

inline NKikimrConsole::TConfigItem MakeConfigItem(ui32 kind, const NKikimrConfig::TAppConfig &config,
                                                  TVector<ui32> nodes, TVector<TString> hosts,
                                                  const TString &tenant, const TString &nodeType,
                                                  ui32 order, ui32 merge, const TString &cookie = "")
{
    NKikimrConsole::TConfigItem item;
    item.SetKind(kind);
    item.MutableConfig()->CopyFrom(config);
    for (auto id : nodes)
        item.MutableUsageScope()->MutableNodeFilter()->AddNodes(id);
    for (auto host : hosts)
        item.MutableUsageScope()->MutableHostFilter()->AddHosts(host);
    if (nodes.empty() && hosts.empty()) {
        item.MutableUsageScope()->MutableTenantAndNodeTypeFilter()->SetTenant(tenant);
        item.MutableUsageScope()->MutableTenantAndNodeTypeFilter()->SetNodeType(nodeType);
    }
    item.SetOrder(order);
    item.SetMergeStrategy(merge);
    item.SetCookie(cookie);
    return item;
}

inline NKikimrConsole::TConfigureAction MakeAddAction(const NKikimrConsole::TConfigItem &item, bool split = false)
{
    NKikimrConsole::TConfigureAction res;
    res.MutableAddConfigItem()->MutableConfigItem()->CopyFrom(item);
    res.MutableAddConfigItem()->SetEnableAutoSplit(split);
    return res;
}

inline NKikimrConsole::TConfigureAction MakeModifyAction(const NKikimrConsole::TConfigItem &item)
{
    NKikimrConsole::TConfigureAction res;
    res.MutableModifyConfigItem()->MutableConfigItem()->CopyFrom(item);
    return res;
}

inline NKikimrConsole::TConfigureAction MakeRemoveAction(ui64 id, ui64 generation)
{
    NKikimrConsole::TConfigureAction res;
    res.MutableRemoveConfigItem()->MutableConfigItemId()->SetId(id);
    res.MutableRemoveConfigItem()->MutableConfigItemId()->SetGeneration(generation);
    return res;
}

inline NKikimrConsole::TConfigureAction MakeRemoveAction(const NKikimrConsole::TConfigItem &item)
{
    return MakeRemoveAction(item.GetId().GetId(), item.GetId().GetGeneration());
}

inline NKikimrConsole::TConfigureAction MakeRemoveByCookieAction(const TString &cookie)
{
    NKikimrConsole::TConfigureAction res;
    res.MutableRemoveConfigItems()->MutableCookieFilter()->AddCookies(cookie);
    return res;
}

inline NKikimrConsole::TConfigureAction MakeRemoveByCookieAction(const TString &cookie1,
                                                                 const TString &cookie2)
{
    NKikimrConsole::TConfigureAction res;
    res.MutableRemoveConfigItems()->MutableCookieFilter()->AddCookies(cookie1);
    res.MutableRemoveConfigItems()->MutableCookieFilter()->AddCookies(cookie2);
    return res;
}

inline void CollectActions(NKikimrConsole::TConfigureRequest &request,
                           const NKikimrConsole::TConfigureAction &action)
{
    request.AddActions()->CopyFrom(action);
}

template <typename ...Ts>
void CollectActions(NKikimrConsole::TConfigureRequest &request,
                    const NKikimrConsole::TConfigureAction &action, Ts... args)
{
    CollectActions(request, action);
    CollectActions(request, args...);
}

template <typename ...Ts>
TVector<ui64> CheckConfigure(TTenantTestRuntime &runtime,
                             Ydb::StatusIds::StatusCode code,
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
    return {reply->Record.GetAddedItemIds().begin(), reply->Record.GetAddedItemIds().end()};
}

template <typename ...Ts>
TVector<ui64> CheckConfigure(TTenantTestRuntime &runtime,
                             Ydb::StatusIds::StatusCode code,
                             Ts... args)
{
    return CheckConfigure(runtime, code, false, false, args...);
}

template <typename ...Ts>
void SendConfigure(TTenantTestRuntime &runtime, Ts... args)
{
    auto *event = new TEvConsole::TEvConfigureRequest;
    CollectActions(event->Record, args...);
    runtime.SendToConsole(event);
}

inline void CollectSubscriptions(THashMap<ui64, TSubscription> &)
{
}

inline void CollectSubscriptions(THashMap<ui64, TSubscription> &subscriptions,
                                 ui64 id, ui32 nodeId, const TString &host, const TString &tenant,
                                 const TString &nodeType, ui64 tabletId, TActorId serviceId,
                                 TVector<ui32> kinds)
{
    TSubscription subscription;
    subscription.Id = id;
    subscription.NodeId = nodeId;
    subscription.Host = host;
    subscription.Tenant = tenant;
    subscription.NodeType = nodeType;
    subscription.Subscriber.TabletId = tabletId;
    subscription.Subscriber.ServiceId = serviceId;
    for (auto &kind : kinds)
        subscription.ItemKinds.insert(kind);
    subscriptions[id] = subscription;
}

template <typename ...Ts>
void CollectSubscriptions(THashMap<ui64, TSubscription> &subscriptions,
                          ui64 id, ui32 nodeId, const TString &host, const TString &tenant,
                          const TString &nodeType, ui64 tabletId, TActorId serviceId,
                          TVector<ui32> kinds, Ts ...args)
{
    CollectSubscriptions(subscriptions, id, nodeId, host, tenant, nodeType, tabletId, serviceId, kinds);
    CollectSubscriptions(subscriptions, args...);
}

template <typename ...Ts>
void CheckListConfigSubscriptions(TTenantTestRuntime &runtime, Ydb::StatusIds::StatusCode code,
                                  ui64 tabletId, TActorId serviceId, Ts ...args)
{
    THashMap<ui64, TSubscription> subscriptions;
    CollectSubscriptions(subscriptions, args...);

    auto *event = new TEvConsole::TEvListConfigSubscriptionsRequest;
    if (tabletId)
        event->Record.MutableSubscriber()->SetTabletId(tabletId);
    else if (serviceId)
        ActorIdToProto(serviceId, event->Record.MutableSubscriber()->MutableServiceId());

    TAutoPtr<IEventHandle> handle;
    runtime.SendToConsole(event);
    auto reply = runtime.GrabEdgeEventRethrow<TEvConsole::TEvListConfigSubscriptionsResponse>(handle);
    UNIT_ASSERT_VALUES_EQUAL(reply->Record.GetStatus().GetCode(), code);

    for (auto &rec : reply->Record.GetSubscriptions()) {
        TSubscription subscription(rec);
        UNIT_ASSERT(subscriptions.contains(subscription.Id));
        UNIT_ASSERT(subscriptions.at(subscription.Id).IsEqual(subscription));
        subscriptions.erase(subscription.Id);
    }
    UNIT_ASSERT(subscriptions.empty());
}

inline void WaitForTenantStatus(TTenantTestRuntime &runtime,
                                const TString &path,
                                Ydb::StatusIds::StatusCode code)
{
   while (true) {
       auto *event = new TEvConsole::TEvGetTenantStatusRequest;
       event->Record.MutableRequest()->set_path(path);

       TAutoPtr<IEventHandle> handle;
       runtime.SendToConsole(event);
       auto reply = runtime.GrabEdgeEventRethrow<TEvConsole::TEvGetTenantStatusResponse>(handle);
       auto &operation = reply->Record.GetResponse().operation();
       if (operation.status() == code)
           return;

       TDispatchOptions options;
       runtime.DispatchEvents(options, TDuration::MilliSeconds(100));
   }
}

inline void ChangeTenant(TTenantTestRuntime &runtime,
                         const TString &tenant,
                         ui32 nodeIdx = 0,
                         bool wait = true)
{
    runtime.Send(new IEventHandle(MakeTenantPoolID(runtime.GetNodeId(nodeIdx)),
                                  runtime.Sender,
                                  new TEvTenantPool::TEvTakeOwnership));

    TAutoPtr<IEventHandle> handle;
    runtime.GrabEdgeEventRethrow<TEvTenantPool::TEvTenantPoolStatus>(handle);

    auto *request = new TEvTenantPool::TEvConfigureSlot;
    request->Record.SetSlotId("slot");
    request->Record.SetAssignedTenant(tenant);
    request->Record.SetLabel("slot-1");
    runtime.Send(new IEventHandle(MakeTenantPoolID(runtime.GetNodeId(nodeIdx)),
                                  runtime.Sender,
                                  request));

    if (wait) {
        auto reply = runtime.GrabEdgeEventRethrow<TEvTenantPool::TEvConfigureSlotResult>(handle);
        UNIT_ASSERT_VALUES_EQUAL(reply->Record.GetStatus(), NKikimrTenantPool::SUCCESS);
    }
}

inline int AssignIds(const TVector<ui64> &ids,
                     int no,
                     NKikimrConsole::TConfigItem &item)
{
    item.MutableId()->SetId(ids[no]);
    item.MutableId()->SetGeneration(1);
    return no + 1;
}

template <typename ...Ts>
int AssignIds(const TVector<ui64> &ids,
              int no,
              NKikimrConsole::TConfigItem &item, Ts&... args)
{
    AssignIds(ids, no, item);
    return AssignIds(ids, no + 1, args...);
}

template <typename ...Ts>
void AssignIds(const TVector<ui64> &ids,
               Ts&... args)
{
    UNIT_ASSERT_VALUES_EQUAL(ids.size(), AssignIds(ids, 0, args...));
}


inline void CheckEqualsIgnoringVersion(NKikimrConfig::TAppConfig config1, NKikimrConfig::TAppConfig config2)
{
    config1.ClearVersion();
    config2.ClearVersion();

    UNIT_ASSERT_VALUES_EQUAL(config1.ShortDebugString(), config2.ShortDebugString());
}

inline void CheckReplaceConfig(TTenantTestRuntime &runtime,
                             Ydb::StatusIds::StatusCode code,
                             TString yamlConfig)
{
        TAutoPtr<IEventHandle> handle;
        auto *event = new TEvConsole::TEvReplaceYamlConfigRequest;
        event->Record.MutableRequest()->set_config(yamlConfig);
        runtime.SendToConsole(event);

        runtime.GrabEdgeEventRethrow<TEvConsole::TEvReplaceYamlConfigResponse>(handle);
        Y_UNUSED(code);
}

inline void CheckDropConfig(TTenantTestRuntime &runtime,
                            Ydb::StatusIds::StatusCode code,
                            TString clusterName,
                            ui64 version)
{
        TAutoPtr<IEventHandle> handle;
        auto *event = new TEvConsole::TEvDropConfigRequest;
        event->Record.MutableRequest()->mutable_identity()->set_cluster(clusterName);
        event->Record.MutableRequest()->mutable_identity()->set_version(version);
        runtime.SendToConsole(event);

        runtime.GrabEdgeEventRethrow<TEvConsole::TEvDropConfigResponse>(handle);
        Y_UNUSED(code);
}

inline void CheckAddVolatileConfig(TTenantTestRuntime &runtime,
                                   Ydb::StatusIds::StatusCode code,
                                   TString clusterName,
                                   ui64 version,
                                   ui64 id,
                                   TString volatileYamlConfig)
{
        TString config = TString("metadata:\n  cluster: \"") + clusterName + "\"\n  version: " + ToString(version) + "\n  id: " + ToString(id) + "\nselector_config:\n" + volatileYamlConfig;

        TAutoPtr<IEventHandle> handle;
        auto *event = new TEvConsole::TEvAddVolatileConfigRequest;
        event->Record.MutableRequest()->set_config(config);
        runtime.SendToConsole(event);

        runtime.GrabEdgeEventRethrow<TEvConsole::TEvAddVolatileConfigResponse>(handle);
        Y_UNUSED(code);
}

inline void CheckRemoveVolatileConfig(TTenantTestRuntime &runtime,
                                      Ydb::StatusIds::StatusCode code,
                                      TString clusterName,
                                      ui64 version,
                                      ui64 id)
{
        TAutoPtr<IEventHandle> handle;
        auto *event = new TEvConsole::TEvRemoveVolatileConfigRequest;
        event->Record.MutableRequest()->mutable_identity()->set_cluster(clusterName);
        event->Record.MutableRequest()->mutable_identity()->set_version(version);
        event->Record.MutableRequest()->mutable_ids()->add_ids(id);
        runtime.SendToConsole(event);

        runtime.GrabEdgeEventRethrow<TEvConsole::TEvRemoveVolatileConfigResponse>(handle);
        Y_UNUSED(code);
}

} // namesapce NKikimr::NConsole::NUT

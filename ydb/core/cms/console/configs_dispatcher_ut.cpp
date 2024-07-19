#include "configs_dispatcher.h"
#include "ut_helpers.h"

#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/tablet/bootstrapper.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/testlib/tablet_helpers.h>

#include <util/system/hostname.h>

#include <ydb/core/protos/netclassifier.pb.h>

namespace NKikimr {

using namespace NConsole;
using namespace NConsole::NUT;

namespace {

TTenantTestConfig::TTenantPoolConfig TenantTenantPoolConfig()
{
    TTenantTestConfig::TTenantPoolConfig res = {
        // Static slots {tenant, {cpu, memory, network}}
        {
            {TENANT1_1_NAME, {1, 1, 1}}
        },
        // NodeType
        "type1"
    };
    return res;
}

TTenantTestConfig DefaultConsoleTestConfig()
{
    TTenantTestConfig res = {
        // Domains {name, schemeshard {{ subdomain_names }}}
        {{ {DOMAIN1_NAME, SCHEME_SHARD1_ID, { TENANT1_1_NAME }} }},
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
        1,
        // CreateConfigsDispatcher
        true
    };
    return res;
}

NKikimrConsole::TConfigItem ITEM_DOMAIN_LOG_1;
NKikimrConsole::TConfigItem ITEM_DOMAIN_LOG_2;
NKikimrConsole::TConfigItem ITEM_NET_CLASSIFIER_1;
NKikimrConsole::TConfigItem ITEM_NET_CLASSIFIER_2;
NKikimrConsole::TConfigItem ITEM_NET_CLASSIFIER_3;

TActorId InitConfigsDispatcher(TTenantTestRuntime &runtime)
{
    ITEM_DOMAIN_LOG_1
        = MakeConfigItem(NKikimrConsole::TConfigItem::LogConfigItem,
                         NKikimrConfig::TAppConfig(), {}, {}, "", "", 1,
                         NKikimrConsole::TConfigItem::MERGE, "");
    ITEM_DOMAIN_LOG_2
        = MakeConfigItem(NKikimrConsole::TConfigItem::LogConfigItem,
                         NKikimrConfig::TAppConfig(), {}, {}, "", "", 2,
                         NKikimrConsole::TConfigItem::MERGE, "");

    ITEM_NET_CLASSIFIER_1
        = MakeConfigItem(NKikimrConsole::TConfigItem::NetClassifierDistributableConfigItem,
                         NKikimrConfig::TAppConfig(), {}, {}, "", "", 3,
                         NKikimrConsole::TConfigItem::MERGE, "");
    ITEM_NET_CLASSIFIER_2
        = MakeConfigItem(NKikimrConsole::TConfigItem::NetClassifierDistributableConfigItem,
                         NKikimrConfig::TAppConfig(), {}, {}, "", "", 4,
                         NKikimrConsole::TConfigItem::MERGE, "");
    ITEM_NET_CLASSIFIER_3
        = MakeConfigItem(NKikimrConsole::TConfigItem::NetClassifierDistributableConfigItem,
                         NKikimrConfig::TAppConfig(), {}, {}, "", "", 5,
                         NKikimrConsole::TConfigItem::MERGE, "");

    return MakeConfigsDispatcherID(runtime.GetNodeId(0));
}

struct CatchReplaceConfigResult {
    CatchReplaceConfigResult(ui64 &id)
        : Id(id)
    {
    }

    bool operator()(IEventHandle &ev) {
        if (ev.GetTypeRewrite() == TEvConsole::EvReplaceConfigSubscriptionsResponse) {
            auto *x = ev.Get<TEvConsole::TEvReplaceConfigSubscriptionsResponse>();
            UNIT_ASSERT_VALUES_EQUAL(x->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
            Id = x->Record.GetSubscriptionId();
            return true;
        }
        return false;
    }

    ui64 &Id;
};

struct TEvPrivate {
    enum EEv {
        EvHoldResponse = EventSpaceBegin(NKikimr::TKikimrEvents::ES_PRIVATE),
        EvSetSubscription,
        EvGotNotification,
        EvComplete,
        EvEnd
    };

    struct TEvHoldResponse : public TEventLocal<TEvHoldResponse, EvHoldResponse> {
        TEvHoldResponse(bool hold)
            : Hold(hold)
        {}

        bool Hold;
    };

    struct TEvSetSubscription : public TEventLocal<TEvSetSubscription, EvSetSubscription> {
        TEvSetSubscription()
        {
        }

        TEvSetSubscription(ui32 kind)
            : ConfigItemKinds({kind})
        {
        }

        TEvSetSubscription(std::initializer_list<ui32> kinds)
            : ConfigItemKinds(kinds)
        {
        }

        TEvSetSubscription(TVector<ui32> kinds)
            : ConfigItemKinds(kinds)
        {
        }

        TVector<ui32> ConfigItemKinds;
    };

    struct TEvGotNotification : public TEventLocal<TEvGotNotification, EvGotNotification> {
        TConfigId ConfigId;
        NKikimrConfig::TAppConfig Config;
    };

    struct TEvComplete : public TEventLocal<TEvComplete, EvComplete> {};
};

class TTestSubscriber : public TActorBootstrapped<TTestSubscriber> {
public:
    TTestSubscriber(TActorId sink, TVector<ui32> kinds, bool hold)
        : Sink(sink)
        , Kinds(std::move(kinds))
        , HoldResponse(hold)
    {
    }

    void Bootstrap(const TActorContext &ctx)
    {
        if (!Kinds.empty())
            SendSubscriptionRequest(ctx);
        Become(&TTestSubscriber::StateWork);
    }

    void SendSubscriptionRequest(const TActorContext &ctx)
    {
        auto *req = new TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest;
        req->ConfigItemKinds = Kinds;
        ctx.Send(MakeConfigsDispatcherID(ctx.SelfID.NodeId()), req);
    }

    void EnqueueEvent(TAutoPtr<IEventHandle> ev)
    {
        EventsQueue.push_back(std::move(ev));
    }

    void ProcessEnqueuedEvents(const TActorContext &ctx)
    {
        while (!EventsQueue.empty()) {
            TAutoPtr<IEventHandle> &ev = EventsQueue.front();
            LOG_DEBUG_S(ctx, NKikimrServices::CONFIGS_DISPATCHER,
                        "Dequeue event type: " << ev->GetTypeRewrite());
            ctx.ExecutorThread.Send(ev.Release());
            EventsQueue.pop_front();
        }
    }

    void Handle(TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse::TPtr &ev, const TActorContext &ctx)
    {
        if (Sink)
            ctx.Send(ev->Forward(Sink));
    }

    void Handle(TEvConsole::TEvConfigNotificationRequest::TPtr &ev, const TActorContext &ctx)
    {
        auto &rec = ev->Get()->Record;
        if (Sink) {
            auto *event = new TEvPrivate::TEvGotNotification;
            event->ConfigId.Load(rec.GetConfigId());
            event->Config = rec.GetConfig();
            ctx.Send(Sink, event);
        }

        if (HoldResponse) {
            EnqueueEvent(ev.Release());
            return;
        }

        auto *resp = new TEvConsole::TEvConfigNotificationResponse;
        resp->Record.SetSubscriptionId(rec.GetSubscriptionId());
        resp->Record.MutableConfigId()->CopyFrom(rec.GetConfigId());
        ctx.Send(ev->Sender, resp, 0, ev->Cookie);
    }

    void Handle(TEvPrivate::TEvHoldResponse::TPtr &ev, const TActorContext &ctx)
    {
        HoldResponse = ev->Get()->Hold;
        if (!HoldResponse)
            ProcessEnqueuedEvents(ctx);
        ctx.Send(ev->Sender, new TEvPrivate::TEvComplete);
    }

    void Handle(TEvPrivate::TEvSetSubscription::TPtr &ev, const TActorContext &ctx)
    {
        if (ev->Get()->ConfigItemKinds == Kinds)
            return;

        Kinds = ev->Get()->ConfigItemKinds;
        SendSubscriptionRequest(ctx);
    }

    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse, Handle);
            HFunc(TEvConsole::TEvConfigNotificationRequest, Handle);
            HFunc(TEvPrivate::TEvHoldResponse, Handle);
            HFunc(TEvPrivate::TEvSetSubscription, Handle);
        }
    }

private:
    TActorId Sink;
    TVector<ui32> Kinds;
    bool HoldResponse;
    TDeque<TAutoPtr<IEventHandle>> EventsQueue;
};

TActorId AddSubscriber(TTenantTestRuntime &runtime, TVector<ui32> kinds, bool hold = false)
{
    auto aid = runtime.Register(new TTestSubscriber(runtime.Sender, std::move(kinds), hold));
    runtime.EnableScheduleForActor(aid, true);
    return aid;
}

void HoldSubscriber(TTenantTestRuntime &runtime, TActorId aid)
{
    TAutoPtr<IEventHandle> handle;
    runtime.Send(new IEventHandle(aid, runtime.Sender, new TEvPrivate::TEvHoldResponse(true)));
    runtime.GrabEdgeEventRethrow<TEvPrivate::TEvComplete>(handle);
}

void UnholdSubscriber(TTenantTestRuntime &runtime, TActorId aid)
{
    runtime.Send(new IEventHandle(aid, runtime.Sender, new TEvPrivate::TEvHoldResponse(false)));
}

void SetSubscriptions(TTenantTestRuntime &runtime, TActorId aid, TVector<ui32> kinds)
{
    runtime.Send(new IEventHandle(aid, runtime.Sender, new TEvPrivate::TEvSetSubscription(kinds)));
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(TConfigsDispatcherTests) {
    Y_UNIT_TEST(TestSubscriptionNotification) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        TAutoPtr<IEventHandle> handle;
        InitConfigsDispatcher(runtime);

        AddSubscriber(runtime, {(ui32)NKikimrConsole::TConfigItem::LogConfigItem});
        AddSubscriber(runtime, {(ui32)NKikimrConsole::TConfigItem::LogConfigItem});

        // Subscribers get notification.
        runtime.GrabEdgeEventRethrow<TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse>(handle);
        runtime.GrabEdgeEventRethrow<TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse>(handle);

        SendConfigure(runtime, MakeAddAction(ITEM_DOMAIN_LOG_1));

        // Expect two responses from subscribers and zero from dispatcher
        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvConsole::EvConfigNotificationResponse, 2);
        runtime.DispatchEvents(options);
    }

    Y_UNIT_TEST(TestSubscriptionNotificationForNewSubscriberAfterUpdate) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        TAutoPtr<IEventHandle> handle;
        InitConfigsDispatcher(runtime);

        auto s1 = AddSubscriber(runtime, {(ui32)NKikimrConsole::TConfigItem::LogConfigItem});
        runtime.GrabEdgeEventRethrow<TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse>(handle);
        HoldSubscriber(runtime, s1);

        SendConfigure(runtime, MakeAddAction(ITEM_DOMAIN_LOG_1));


        auto reply1 = runtime.GrabEdgeEventRethrow<TEvPrivate::TEvGotNotification>(handle);
        auto configId1 = reply1->ConfigId;

        UnholdSubscriber(runtime, s1);

        // Expect response from subscriber
        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvConsole::EvConfigNotificationResponse, 1);
        runtime.DispatchEvents(options);

        // New subscriber should get notification.
        AddSubscriber(runtime, {(ui32)NKikimrConsole::TConfigItem::LogConfigItem});

        auto reply2 = runtime.GrabEdgeEventRethrow<TEvPrivate::TEvGotNotification>(handle);
        auto configId2 = reply2->ConfigId;
        UNIT_ASSERT_VALUES_EQUAL(configId1.ToString(), configId2.ToString());
    }

    Y_UNIT_TEST(TestSubscriptionNotificationForNewSubscriberDuringUpdate) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        TAutoPtr<IEventHandle> handle;
        InitConfigsDispatcher(runtime);

        auto s1 = AddSubscriber(runtime, {(ui32)NKikimrConsole::TConfigItem::LogConfigItem});
        runtime.GrabEdgeEventRethrow<TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse>(handle);
        HoldSubscriber(runtime, s1);

        SendConfigure(runtime, MakeAddAction(ITEM_DOMAIN_LOG_1));

        auto reply1 = runtime.GrabEdgeEventRethrow<TEvPrivate::TEvGotNotification>(handle);
        auto configId1 = reply1->ConfigId;

        // New subscriber should get notification.
        AddSubscriber(runtime, {(ui32)NKikimrConsole::TConfigItem::LogConfigItem});

        auto reply2 = runtime.GrabEdgeEventRethrow<TEvPrivate::TEvGotNotification>(handle);
        auto configId2 = reply2->ConfigId;
        UNIT_ASSERT_VALUES_EQUAL(configId1.ToString(), configId2.ToString());

        UnholdSubscriber(runtime, s1);

        // Expect response from unhold subscriber
        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvConsole::EvConfigNotificationResponse, 1);
        runtime.DispatchEvents(options);
    }

    Y_UNIT_TEST(TestRemoveSubscription) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        TAutoPtr<IEventHandle> handle;
        InitConfigsDispatcher(runtime);

        auto s1 = AddSubscriber(runtime, {(ui32)NKikimrConsole::TConfigItem::LogConfigItem});
        runtime.GrabEdgeEventRethrow<TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse>(handle);

        // Subscriber removal should cause CMS subscription removal.
        SetSubscriptions(runtime, s1, {});

        TDispatchOptions options;
        runtime.DispatchEvents(options);

        runtime.GrabEdgeEventRethrow<TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse>(handle);
    }

    Y_UNIT_TEST(TestRemoveSubscriptionWhileUpdateInProcess) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        TAutoPtr<IEventHandle> handle;
        InitConfigsDispatcher(runtime);

        auto s1 = AddSubscriber(runtime, {(ui32)NKikimrConsole::TConfigItem::LogConfigItem});
        runtime.GrabEdgeEventRethrow<TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse>(handle);
        HoldSubscriber(runtime, s1);

        AddSubscriber(runtime, {(ui32)NKikimrConsole::TConfigItem::LogConfigItem});
        runtime.GrabEdgeEventRethrow<TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse>(handle);

        SendConfigure(runtime, MakeAddAction(ITEM_DOMAIN_LOG_1));

        TDispatchOptions options1;
        options1.FinalEvents.emplace_back(TEvConsole::EvConfigNotificationResponse, 1);
        runtime.DispatchEvents(options1);

        // We don't track acks from config dispatcher with InMemory subscriptions
        SetSubscriptions(runtime, s1, {});

        TDispatchOptions options2;
        runtime.DispatchEvents(options2);
    }

    Y_UNIT_TEST(TestEmptyChangeCausesNoNotification) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        TAutoPtr<IEventHandle> handle;
        InitConfigsDispatcher(runtime);

        ui64 notifications = 0;
        TActorId subscriber;
        auto observer = [&notifications,&subscriber,recipient=runtime.Sender](TAutoPtr<IEventHandle>& ev) -> TTenantTestRuntime::EEventAction {
            if (ev->Recipient == recipient && ev->Sender == subscriber) {
                switch (ev->GetTypeRewrite()) {
                case TEvPrivate::EvGotNotification:
                    ++notifications;
                    break;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };

        ITEM_DOMAIN_LOG_1.MutableConfig()->MutableLogConfig()->SetClusterName("cluster1");
        ITEM_DOMAIN_LOG_2.MutableConfig()->MutableLogConfig()->SetClusterName("cluster1");

        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                       MakeAddAction(ITEM_DOMAIN_LOG_1));

        runtime.SetObserverFunc(observer);

        // Add subscriber and get config via notification.
        subscriber = AddSubscriber(runtime, {(ui32)NKikimrConsole::TConfigItem::LogConfigItem});
        runtime.GrabEdgeEventRethrow<TEvPrivate::TEvGotNotification>(handle);
        UNIT_ASSERT(notifications > 0);

        // Now add another element which doesn't change config body.
        // It should cause notification to dispatcher but not test subscriber.
        SendConfigure(runtime, MakeAddAction(ITEM_DOMAIN_LOG_2));
        notifications = 0;
        TDispatchOptions options1;
        runtime.DispatchEvents(options1);
        UNIT_ASSERT_VALUES_EQUAL(notifications, 0);
    }

    Y_UNIT_TEST(TestYamlAndNonYamlCoexist) {
        NKikimrConfig::TAppConfig config;
        auto *label = config.AddLabels();
        label->SetName("test");
        label->SetValue("true");

        TTenantTestRuntime runtime(DefaultConsoleTestConfig(), config);
        TAutoPtr<IEventHandle> handle;
        InitConfigsDispatcher(runtime);

        ui64 notifications = 0;
        TActorId subscriber;
        auto observer = [&notifications, &subscriber, recipient = runtime.Sender](
            TAutoPtr<IEventHandle> &ev) -> TTenantTestRuntime::EEventAction {
            if (ev->Recipient == recipient && ev->Sender == subscriber) {
                switch (ev->GetTypeRewrite()) {
                case TEvPrivate::EvGotNotification:
                    ++notifications;
                    break;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        runtime.SetObserverFunc(observer);

        ITEM_DOMAIN_LOG_1.MutableConfig()->MutableLogConfig()->SetClusterName("cluster1");
        ITEM_NET_CLASSIFIER_1.MutableConfig()->MutableNetClassifierDistributableConfig()->SetLastUpdateTimestamp(1);

        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                       MakeAddAction(ITEM_DOMAIN_LOG_1),
                       MakeAddAction(ITEM_NET_CLASSIFIER_1));

        subscriber = AddSubscriber(runtime, {(ui32)NKikimrConsole::TConfigItem::NetClassifierDistributableConfigItem});

        auto reply = runtime.GrabEdgeEventRethrow<TEvPrivate::TEvGotNotification>(handle);
        NKikimrConfig::TAppConfig expectedConfig;
        label = expectedConfig.AddLabels();
        label->SetName("test");
        label->SetValue("true");
        auto *ncdConfig = expectedConfig.MutableNetClassifierDistributableConfig();
        ncdConfig->SetLastUpdateTimestamp(1);
        UNIT_ASSERT(notifications > 0);
        UNIT_ASSERT_VALUES_EQUAL(expectedConfig.ShortDebugString(), reply->Config.ShortDebugString());
        notifications = 0;

        TString yamlConfig1 = R"(
---
metadata:
  cluster: ""
  version: 0

config:
  log_config:
    cluster_name: cluster2
  net_classifier_distributable_config:
    last_update_timestamp: 3
  yaml_config_enabled: true

allowed_labels:
  test:
    type: enum
    values:
      ? true

selector_config: []
)";

        CheckReplaceConfig(runtime, Ydb::StatusIds::SUCCESS, yamlConfig1);

        UNIT_ASSERT(notifications == 0);

        ITEM_DOMAIN_LOG_2.MutableConfig()->MutableLogConfig()->SetClusterName("cluster3");

        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                       MakeAddAction(ITEM_DOMAIN_LOG_2));

        UNIT_ASSERT(notifications == 0);

        ITEM_NET_CLASSIFIER_2.MutableConfig()->MutableNetClassifierDistributableConfig()->SetLastUpdateTimestamp(3);

        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                       MakeAddAction(ITEM_NET_CLASSIFIER_2));

        reply = runtime.GrabEdgeEventRethrow<TEvPrivate::TEvGotNotification>(handle);
        ncdConfig->SetLastUpdateTimestamp(3);
        UNIT_ASSERT(notifications > 0);
        UNIT_ASSERT_VALUES_EQUAL(expectedConfig.ShortDebugString(), reply->Config.ShortDebugString());
        notifications = 0;

        TString yamlConfig2 = R"(
---
metadata:
  cluster: ""
  version: 1

config: {yaml_config_enabled: false}
allowed_labels: {}
selector_config: []
)";
        CheckReplaceConfig(runtime, Ydb::StatusIds::SUCCESS, yamlConfig2);

        UNIT_ASSERT(notifications == 0);

        ITEM_NET_CLASSIFIER_3.MutableConfig()->MutableNetClassifierDistributableConfig()->SetLastUpdateTimestamp(5);

        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                       MakeAddAction(ITEM_NET_CLASSIFIER_3));

        reply = runtime.GrabEdgeEventRethrow<TEvPrivate::TEvGotNotification>(handle);
        ncdConfig->SetLastUpdateTimestamp(5);
        UNIT_ASSERT(notifications > 0);
        UNIT_ASSERT_VALUES_EQUAL(expectedConfig.ShortDebugString(), reply->Config.ShortDebugString());
    }

    Y_UNIT_TEST(TestYamlEndToEnd) {
        NKikimrConfig::TAppConfig config;
        auto *label = config.AddLabels();
        label->SetName("test");
        label->SetValue("true");

        TTenantTestRuntime runtime(DefaultConsoleTestConfig(), config);
        TAutoPtr<IEventHandle> handle;
        InitConfigsDispatcher(runtime);

        ui64 notifications = 0;
        TActorId subscriber;
        auto observer = [&notifications, &subscriber, recipient = runtime.Sender](
            TAutoPtr<IEventHandle> &ev) -> TTenantTestRuntime::EEventAction {
            if (ev->Recipient == recipient && ev->Sender == subscriber) {
                switch (ev->GetTypeRewrite()) {
                case TEvPrivate::EvGotNotification:
                    ++notifications;
                    break;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        runtime.SetObserverFunc(observer);

        ITEM_DOMAIN_LOG_1.MutableConfig()->MutableLogConfig()->SetClusterName("cluster1");
        ITEM_DOMAIN_LOG_1.MutableConfig()->MutableLogConfig()->SetDefaultLevel(5);

        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                       MakeAddAction(ITEM_DOMAIN_LOG_1));

        subscriber = AddSubscriber(runtime, {(ui32)NKikimrConsole::TConfigItem::LogConfigItem});
        auto reply = runtime.GrabEdgeEventRethrow<TEvPrivate::TEvGotNotification>(handle);
        NKikimrConfig::TAppConfig expectedConfig;
        label = expectedConfig.AddLabels();
        label->SetName("test");
        label->SetValue("true");
        auto *logConfig = expectedConfig.MutableLogConfig();
        logConfig->SetClusterName("cluster1");
        logConfig->SetDefaultLevel(5);
        UNIT_ASSERT(notifications > 0);
        UNIT_ASSERT_VALUES_EQUAL(expectedConfig.ShortDebugString(), reply->Config.ShortDebugString());
        notifications = 0;

        TString yamlConfig1 = R"(
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
        CheckReplaceConfig(runtime, Ydb::StatusIds::SUCCESS, yamlConfig1);
        UNIT_ASSERT(notifications == 0);

        TString yamlConfig2 = R"(
---
metadata:
  cluster: ""
  version: 1

config:
  log_config:
    cluster_name: cluster1
  yaml_config_enabled: true

allowed_labels:
  test:
    type: enum
    values:
      ? true

selector_config: []
)";

        CheckReplaceConfig(runtime, Ydb::StatusIds::SUCCESS, yamlConfig2);

        ITEM_DOMAIN_LOG_2.MutableConfig()->MutableLogConfig()->SetClusterName("cluster2");
        ITEM_DOMAIN_LOG_2.MutableConfig()->MutableLogConfig()->SetDefaultLevel(5);

        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                       MakeAddAction(ITEM_DOMAIN_LOG_2));

        TString yamlConfig3 = R"(
---
metadata:
  cluster: ""
  version: 2

config:
  log_config:
    cluster_name: cluster3
  yaml_config_enabled: true

allowed_labels:
  test:
    type: enum
    values:
      ? true

selector_config: []
)";
        CheckReplaceConfig(runtime, Ydb::StatusIds::SUCCESS, yamlConfig3);

        reply = runtime.GrabEdgeEventRethrow<TEvPrivate::TEvGotNotification>(handle);
        expectedConfig = {};
        label = expectedConfig.AddLabels();
        label->SetName("test");
        label->SetValue("true");
        logConfig = expectedConfig.MutableLogConfig();
        logConfig->SetClusterName("cluster3");
        logConfig->SetDefaultLevel(5);
        UNIT_ASSERT(notifications > 0);
        UNIT_ASSERT_VALUES_EQUAL(expectedConfig.ShortDebugString(), reply->Config.ShortDebugString());
        notifications = 0;

        TString yamlConfig4 = R"(
---
metadata:
  cluster: ""
  version: 3

config:
  log_config:
    cluster_name: cluster3
  cms_config:
    sentinel_config:
      enable: true
  yaml_config_enabled: true

allowed_labels:
  test:
    type: enum
    values:
      ? true

selector_config: []
)";
        CheckReplaceConfig(runtime, Ydb::StatusIds::SUCCESS, yamlConfig4);
        CheckReplaceConfig(runtime, Ydb::StatusIds::SUCCESS, yamlConfig4);
        UNIT_ASSERT(notifications == 0);

        TString yamlConfig5 = R"(
---
metadata:
  cluster: ""
  version: 4

config:
  log_config:
    cluster_name: cluster3
  cms_config:
    sentinel_config:
      enable: true
  yaml_config_enabled: true

allowed_labels:
  test:
    type: enum
    values:
      ? true

selector_config:
- description: Test
  selector:
    test: true
  config:
    log_config: !inherit
      entry:
      - component: AUDIT_LOG_WRITER
        level: 7
)";
        CheckReplaceConfig(runtime, Ydb::StatusIds::SUCCESS, yamlConfig5);

        reply = runtime.GrabEdgeEventRethrow<TEvPrivate::TEvGotNotification>(handle);
        expectedConfig = {};
        label = expectedConfig.AddLabels();
        label->SetName("test");
        label->SetValue("true");
        logConfig = expectedConfig.MutableLogConfig();
        logConfig->SetClusterName("cluster3");
        logConfig->SetDefaultLevel(5);
        auto *entry = logConfig->AddEntry();
        entry->SetComponent("AUDIT_LOG_WRITER");
        entry->SetLevel(7);
        UNIT_ASSERT(notifications > 0);
        UNIT_ASSERT_VALUES_EQUAL(expectedConfig.ShortDebugString(), reply->Config.ShortDebugString());
        notifications = 0;

        TString yamlConfig6 = R"(
---
metadata:
  cluster: ""
  version: 5

config:
  log_config:
    cluster_name: cluster3
  cms_config:
    sentinel_config:
      enable: true
  yaml_config_enabled: true

allowed_labels:
  test:
    type: enum
    values:
      ? true

selector_config:
- description: Test
  selector:
    test: true
  config:
    log_config: !inherit
      entry:
      - component: AUDIT_LOG_WRITER
        level: 6
)";
        CheckReplaceConfig(runtime, Ydb::StatusIds::SUCCESS, yamlConfig6);

        reply = runtime.GrabEdgeEventRethrow<TEvPrivate::TEvGotNotification>(handle);
        expectedConfig = {};
        label = expectedConfig.AddLabels();
        label->SetName("test");
        label->SetValue("true");
        logConfig = expectedConfig.MutableLogConfig();
        logConfig->SetClusterName("cluster3");
        logConfig->SetDefaultLevel(5);
        entry = logConfig->AddEntry();
        entry->SetComponent("AUDIT_LOG_WRITER");
        entry->SetLevel(6);
        UNIT_ASSERT(notifications > 0);
        UNIT_ASSERT_VALUES_EQUAL(expectedConfig.ShortDebugString(), reply->Config.ShortDebugString());
        notifications = 0;

        TString yamlConfig7 = R"(
---
metadata:
  cluster: ""
  version: 6
config:
  log_config:
    cluster_name: cluster3
  cms_config:
    sentinel_config:
      enable: true
  yaml_config_enabled: true

allowed_labels:
  test:
    type: enum
    values:
      ? true

selector_config:
- description: Test
  selector:
    test:
      not_in:
      - true
  config:
    log_config: !inherit
      entry:
      - component: AUDIT_LOG_WRITER
        level: 7
)";
        CheckReplaceConfig(runtime, Ydb::StatusIds::SUCCESS, yamlConfig7);

        reply = runtime.GrabEdgeEventRethrow<TEvPrivate::TEvGotNotification>(handle);
        expectedConfig = {};
        label = expectedConfig.AddLabels();
        label->SetName("test");
        label->SetValue("true");
        logConfig = expectedConfig.MutableLogConfig();
        logConfig->SetClusterName("cluster3");
        logConfig->SetDefaultLevel(5);
        UNIT_ASSERT(notifications > 0);
        UNIT_ASSERT_VALUES_EQUAL(expectedConfig.ShortDebugString(), reply->Config.ShortDebugString());
        notifications = 0;

        TString yamlConfig8 = R"(
---
metadata:
  cluster: ""
  version: 7

config:
  log_config:
    cluster_name: cluster3
  yaml_config_enabled: true

allowed_labels:
  test:
    type: enum
    values:
      ? true

selector_config:
- description: Test
  selector:
    test: true
  config:
    yaml_config_enabled: false
)";
        CheckReplaceConfig(runtime, Ydb::StatusIds::SUCCESS, yamlConfig8);

        reply = runtime.GrabEdgeEventRethrow<TEvPrivate::TEvGotNotification>(handle);
        expectedConfig = {};
        label = expectedConfig.AddLabels();
        label->SetName("test");
        label->SetValue("true");
        logConfig = expectedConfig.MutableLogConfig();
        logConfig->SetClusterName("cluster2");
        logConfig->SetDefaultLevel(5);
        UNIT_ASSERT(notifications > 0);
        UNIT_ASSERT_VALUES_EQUAL(expectedConfig.ShortDebugString(), reply->Config.ShortDebugString());
    }
}

} // namespace NKikimr

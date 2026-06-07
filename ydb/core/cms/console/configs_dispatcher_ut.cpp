#include "configs_dispatcher.h"
#include "ut_helpers.h"

#include <ydb/core/config/init/mock.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/tablet/bootstrapper.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/testlib/tablet_helpers.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/monlib/service/mon_service_http_request.h>

#include <util/system/hostname.h>

#include <ydb/core/protos/netclassifier.pb.h>

namespace NKikimr {

using namespace NConsole;
using namespace NConsole::NUT;

namespace {

struct THttpRequest : NMonitoring::IHttpRequest {
    HTTP_METHOD Method;
    TCgiParameters CgiParameters;
    THttpHeaders HttpHeaders;

    explicit THttpRequest(HTTP_METHOD method)
        : Method(method)
    {
    }

    const char* GetURI() const override {
        return "";
    }

    const char* GetPath() const override {
        return "";
    }

    const TCgiParameters& GetParams() const override {
        return CgiParameters;
    }

    const TCgiParameters& GetPostParams() const override {
        return CgiParameters;
    }

    TStringBuf GetPostContent() const override {
        return TStringBuf();
    }

    HTTP_METHOD GetMethod() const override {
        return Method;
    }

    const THttpHeaders& GetHeaders() const override {
        return HttpHeaders;
    }

    TString GetRemoteAddr() const override {
        return TString();
    }
};

NJson::TJsonValue ReadJsonFromString(TStringBuf json) {
    NJson::TJsonValue value;
    UNIT_ASSERT_C(NJson::ReadJsonTree(json, &value), TString(json));
    return value;
}

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
            ctx.Send(ev.Release());
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

config:
  yaml_config_enabled: false
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

    Y_UNIT_TEST(TestYamlConfigAndIcb) {
        NKikimrConfig::TAppConfig config;
        auto *label = config.AddLabels();
        label->SetName("test");
        label->SetValue("true");

        TTenantTestRuntime runtime(DefaultConsoleTestConfig(), config);
        InitConfigsDispatcher(runtime);

        TString yamlConfig1 = R"(
---
metadata:
  cluster: ""
  version: 0
  kind: MainConfig
config:
  log_config:
    cluster_name: cluster3
  yaml_config_enabled: true
  immediate_controls_config:
    data_shard_controls:
      enable_leader_leases: 1
      enable_locked_writes: 1
)";
        CheckReplaceConfig(runtime, Ydb::StatusIds::SUCCESS, yamlConfig1);
        auto& icb = *runtime.GetAppData().Icb;
        TAtomic controlValue;
        UNIT_ASSERT(!!icb.DataShardControls.EnableLeaderLeases.AtomicLoad());
        controlValue = icb.DataShardControls.EnableLeaderLeases.AtomicLoad()->Get();
        UNIT_ASSERT_VALUES_EQUAL(controlValue, 1);
        UNIT_ASSERT(!!icb.DataShardControls.EnableLockedWrites.AtomicLoad());
        controlValue = icb.DataShardControls.EnableLockedWrites.AtomicLoad()->Get();
        UNIT_ASSERT_VALUES_EQUAL(controlValue, 1);
    }

    // Tests for the "skip unchanged YAML" optimization
    // (PR #41360: continue when !isYamlChanged && !yamlConfigTurnedOff && not-StateInit)

    // Test 1: A YAML subscriber must NOT be notified when a non-YAML proto-config
    // item changes and the YAML content itself is unchanged.
    //
    // Scenario: yaml_config_enabled is on; a LogConfig subscriber (yaml kind) drains
    // its initial notification; then a NetClassifier proto-item is added (non-yaml kind,
    // not subscribed by the yaml subscriber).  The TEvConfigSubscriptionNotification that
    // arrives has isYamlChanged=false, so the new `continue` must fire and the yaml
    // subscriber receives zero new notifications.
    Y_UNIT_TEST(TestYamlSubscriberSkipsUnchangedYamlOnNonYamlEvent) {
        NKikimrConfig::TAppConfig config;
        auto *label = config.AddLabels();
        label->SetName("test");
        label->SetValue("true");

        TTenantTestRuntime runtime(DefaultConsoleTestConfig(), config);
        TAutoPtr<IEventHandle> handle;
        InitConfigsDispatcher(runtime);

        // Install yaml config with yaml_config_enabled: true (LogConfig kind is yaml kind)
        TString yamlConfig = R"(
---
metadata:
  cluster: ""
  version: 0

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
        CheckReplaceConfig(runtime, Ydb::StatusIds::SUCCESS, yamlConfig);

        ui64 notifications = 0;
        TActorId subscriber;
        auto observer = [&notifications, &subscriber, recipient = runtime.Sender](
            TAutoPtr<IEventHandle> &ev) -> TTenantTestRuntime::EEventAction {
            if (ev->Recipient == recipient && ev->Sender == subscriber) {
                if (ev->GetTypeRewrite() == TEvPrivate::EvGotNotification) {
                    ++notifications;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };

        // Add a yaml subscriber (LogConfigItem is NOT in NON_YAML_KINDS -> Yaml=true)
        subscriber = AddSubscriber(runtime, {(ui32)NKikimrConsole::TConfigItem::LogConfigItem});

        // Drain the initial notification that every new subscriber receives
        runtime.GrabEdgeEventRethrow<TEvPrivate::TEvGotNotification>(handle);
        UNIT_ASSERT(notifications >= 0); // initial notification counted

        // Now start counting fresh
        runtime.SetObserverFunc(observer);
        notifications = 0;

        // Trigger a proto-config change for a NON-yaml kind (NetClassifier) that the
        // yaml subscriber does not subscribe to.  This produces a
        // TEvConfigSubscriptionNotification with isYamlChanged=false.
        ITEM_NET_CLASSIFIER_1.MutableConfig()->MutableNetClassifierDistributableConfig()->SetLastUpdateTimestamp(42);
        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS, MakeAddAction(ITEM_NET_CLASSIFIER_1));

        // CheckConfigure synchronously drains through the dispatcher's Handle.
        // The yaml subscriber must receive zero notifications: the optimization skipped it.
        UNIT_ASSERT_VALUES_EQUAL_C(notifications, 0,
            "yaml subscriber should not be notified when yaml content is unchanged");
    }

    // Test 2: A YAML subscriber MUST still be notified when the actual YAML body changes.
    // Regression guard: the `continue` must NOT fire when isYamlChanged=true.
    Y_UNIT_TEST(TestYamlSubscriberStillNotifiedOnActualYamlChange) {
        NKikimrConfig::TAppConfig config;
        auto *label = config.AddLabels();
        label->SetName("test");
        label->SetValue("true");

        TTenantTestRuntime runtime(DefaultConsoleTestConfig(), config);
        TAutoPtr<IEventHandle> handle;
        InitConfigsDispatcher(runtime);

        TString yamlConfig1 = R"(
---
metadata:
  cluster: ""
  version: 0

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
        CheckReplaceConfig(runtime, Ydb::StatusIds::SUCCESS, yamlConfig1);

        ui64 notifications = 0;
        TActorId subscriber;
        auto observer = [&notifications, &subscriber, recipient = runtime.Sender](
            TAutoPtr<IEventHandle> &ev) -> TTenantTestRuntime::EEventAction {
            if (ev->Recipient == recipient && ev->Sender == subscriber) {
                if (ev->GetTypeRewrite() == TEvPrivate::EvGotNotification) {
                    ++notifications;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };

        subscriber = AddSubscriber(runtime, {(ui32)NKikimrConsole::TConfigItem::LogConfigItem});

        // Drain initial notification
        auto reply = runtime.GrabEdgeEventRethrow<TEvPrivate::TEvGotNotification>(handle);

        runtime.SetObserverFunc(observer);
        notifications = 0;

        // Replace yaml with different cluster_name -> isYamlChanged=true
        TString yamlConfig2 = R"(
---
metadata:
  cluster: ""
  version: 1

config:
  log_config:
    cluster_name: cluster2
  yaml_config_enabled: true

allowed_labels:
  test:
    type: enum
    values:
      ? true

selector_config: []
)";
        CheckReplaceConfig(runtime, Ydb::StatusIds::SUCCESS, yamlConfig2);

        // Subscriber must receive a notification with the updated log config
        reply = runtime.GrabEdgeEventRethrow<TEvPrivate::TEvGotNotification>(handle);
        UNIT_ASSERT_C(notifications > 0,
            "yaml subscriber must be notified when yaml content actually changes");

        // Only assert on ClusterName — DefaultLevel may be inherited from shared global
        // state (ITEM_DOMAIN_LOG_1 is modified by other tests in the suite).
        UNIT_ASSERT_VALUES_EQUAL_C(reply->Config.GetLogConfig().GetClusterName(), "cluster2",
            "yaml subscriber must receive updated ClusterName from new yaml config");
    }

    // Test 3: A YAML subscriber MUST be notified when yaml_config_enabled is turned off
    // (yamlConfigTurnedOff=true), even though the yaml text may not have changed otherwise.
    // This verifies the `!yamlConfigTurnedOff` branch of the optimization guard.
    Y_UNIT_TEST(TestYamlSubscriberNotifiedOnYamlTurnedOff) {
        NKikimrConfig::TAppConfig config;
        auto *label = config.AddLabels();
        label->SetName("test");
        label->SetValue("true");

        TTenantTestRuntime runtime(DefaultConsoleTestConfig(), config);
        TAutoPtr<IEventHandle> handle;
        InitConfigsDispatcher(runtime);

        // Set up initial proto-config that will serve as fallback when yaml is disabled
        ITEM_DOMAIN_LOG_1.MutableConfig()->MutableLogConfig()->SetClusterName("proto_cluster");
        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS, MakeAddAction(ITEM_DOMAIN_LOG_1));

        TString yamlConfigOn = R"(
---
metadata:
  cluster: ""
  version: 0

config:
  log_config:
    cluster_name: yaml_cluster
  yaml_config_enabled: true

allowed_labels:
  test:
    type: enum
    values:
      ? true

selector_config: []
)";
        CheckReplaceConfig(runtime, Ydb::StatusIds::SUCCESS, yamlConfigOn);

        ui64 notifications = 0;
        TActorId subscriber;
        auto observer = [&notifications, &subscriber, recipient = runtime.Sender](
            TAutoPtr<IEventHandle> &ev) -> TTenantTestRuntime::EEventAction {
            if (ev->Recipient == recipient && ev->Sender == subscriber) {
                if (ev->GetTypeRewrite() == TEvPrivate::EvGotNotification) {
                    ++notifications;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };

        subscriber = AddSubscriber(runtime, {(ui32)NKikimrConsole::TConfigItem::LogConfigItem});

        // Drain initial notification (subscriber sees yaml_cluster)
        auto reply = runtime.GrabEdgeEventRethrow<TEvPrivate::TEvGotNotification>(handle);
        UNIT_ASSERT(reply->Config.GetLogConfig().GetClusterName() == "yaml_cluster");

        runtime.SetObserverFunc(observer);
        notifications = 0;

        // Now turn off yaml_config_enabled: yamlConfigTurnedOff becomes true
        TString yamlConfigOff = R"(
---
metadata:
  cluster: ""
  version: 1

config:
  log_config:
    cluster_name: yaml_cluster
  yaml_config_enabled: false

allowed_labels:
  test:
    type: enum
    values:
      ? true

selector_config: []
)";
        CheckReplaceConfig(runtime, Ydb::StatusIds::SUCCESS, yamlConfigOff);

        // Subscriber must be notified because yamlConfigTurnedOff=true bypasses the guard
        reply = runtime.GrabEdgeEventRethrow<TEvPrivate::TEvGotNotification>(handle);
        UNIT_ASSERT_C(notifications > 0,
            "yaml subscriber must be notified when yaml_config_enabled is turned off");
    }

    // Test 4: A YAML subscriber must NOT receive a redundant notification when the
    // same yaml content is replaced again (isYamlChanged=false because the yaml text
    // is byte-identical).  This makes explicit the invariant already implicit in
    // TestYamlEndToEnd (the double-replace at line 762).
    Y_UNIT_TEST(TestYamlSubscriberNoRedundantNotificationOnIdenticalYamlReplace) {
        NKikimrConfig::TAppConfig config;
        auto *label = config.AddLabels();
        label->SetName("test");
        label->SetValue("true");

        TTenantTestRuntime runtime(DefaultConsoleTestConfig(), config);
        TAutoPtr<IEventHandle> handle;
        InitConfigsDispatcher(runtime);

        // Use a fixed yaml string; re-applying the exact same bytes yields isYamlChanged=false
        TString yamlConfig = R"(
---
metadata:
  cluster: ""
  version: 0

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
        CheckReplaceConfig(runtime, Ydb::StatusIds::SUCCESS, yamlConfig);

        ui64 notifications = 0;
        TActorId subscriber;
        auto observer = [&notifications, &subscriber, recipient = runtime.Sender](
            TAutoPtr<IEventHandle> &ev) -> TTenantTestRuntime::EEventAction {
            if (ev->Recipient == recipient && ev->Sender == subscriber) {
                if (ev->GetTypeRewrite() == TEvPrivate::EvGotNotification) {
                    ++notifications;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };

        subscriber = AddSubscriber(runtime, {(ui32)NKikimrConsole::TConfigItem::LogConfigItem});

        // Drain the mandatory initial notification
        runtime.GrabEdgeEventRethrow<TEvPrivate::TEvGotNotification>(handle);

        runtime.SetObserverFunc(observer);
        notifications = 0;

        // Replace with the byte-identical yaml: dispatcher sees newYamlConfig == MainYamlConfig
        // so isYamlChanged=false, and the new `continue` must fire.
        // CheckReplaceConfig synchronously drives the dispatcher's Handle.
        CheckReplaceConfig(runtime, Ydb::StatusIds::SUCCESS, yamlConfig);

        UNIT_ASSERT_VALUES_EQUAL_C(notifications, 0,
            "yaml subscriber must not receive a redundant notification when yaml is byte-identical");
    }

    // Regression for the pending-update-loss corner case (raised by Copilot review on PR #41360):
    // 1. yaml V1->V2, subscriber is held -> dispatcher sets UpdateInProcess and sends V2.
    // 2. A non-yaml event arrives while the subscriber is still held. With the bare optimization
    //    the loop at configs_dispatcher.cpp:1160-1163 clears UpdateInProcess, then `continue`
    //    skips re-creating it, so the subscriber's eventual ACK is dropped (cookie/no-update path).
    // 3. After unhold + ACK, subscription->CurrentConfig stays at V1 while the subscriber has V2.
    // 4. A subsequent yaml V2->V3 whose filtered content equals V1 then makes CompareConfigs
    //    return equal, so no notification is sent and the subscriber is stuck at V2.
    //
    // The fix is to defer clearing UpdateInProcess until after the skip decision: when we are
    // going to `continue`, leave the in-flight update intact so the held ACK can complete.
    Y_UNIT_TEST(TestYamlSubscriberPendingUpdateNotLostOnNonYamlEvent) {
        NKikimrConfig::TAppConfig config;
        auto *label = config.AddLabels();
        label->SetName("test");
        label->SetValue("true");

        TTenantTestRuntime runtime(DefaultConsoleTestConfig(), config);
        TAutoPtr<IEventHandle> handle;
        InitConfigsDispatcher(runtime);

        TString yamlV1 = R"(
---
metadata:
  cluster: ""
  version: 0

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
        CheckReplaceConfig(runtime, Ydb::StatusIds::SUCCESS, yamlV1);

        TActorId subscriber = AddSubscriber(runtime, {(ui32)NKikimrConsole::TConfigItem::LogConfigItem});
        // Drain initial notification at V1
        auto reply = runtime.GrabEdgeEventRethrow<TEvPrivate::TEvGotNotification>(handle);
        UNIT_ASSERT_VALUES_EQUAL(reply->Config.GetLogConfig().GetClusterName(), "cluster1");

        // Hold subscriber so its ACK to the next notification is deferred
        HoldSubscriber(runtime, subscriber);

        // yaml V1 -> V2: held subscriber receives the notification but cannot ACK yet
        TString yamlV2 = R"(
---
metadata:
  cluster: ""
  version: 1

config:
  log_config:
    cluster_name: cluster2
  yaml_config_enabled: true

allowed_labels:
  test:
    type: enum
    values:
      ? true

selector_config: []
)";
        CheckReplaceConfig(runtime, Ydb::StatusIds::SUCCESS, yamlV2);

        reply = runtime.GrabEdgeEventRethrow<TEvPrivate::TEvGotNotification>(handle);
        UNIT_ASSERT_VALUES_EQUAL(reply->Config.GetLogConfig().GetClusterName(), "cluster2");

        // Non-yaml event while still held. Without the fix, this clears UpdateInProcess and
        // the optimization's `continue` prevents re-creating it.
        ITEM_NET_CLASSIFIER_1.MutableConfig()->MutableNetClassifierDistributableConfig()->SetLastUpdateTimestamp(99);
        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS, MakeAddAction(ITEM_NET_CLASSIFIER_1));

        // Unhold: subscriber drains its queued notification and sends the ACK for V2.
        // The dequeue-replay also emits a duplicate EvGotNotification(cluster2) to the sink,
        // so drain it explicitly before issuing the next yaml change.
        UnholdSubscriber(runtime, subscriber);
        reply = runtime.GrabEdgeEventRethrow<TEvPrivate::TEvGotNotification>(handle);
        UNIT_ASSERT_VALUES_EQUAL(reply->Config.GetLogConfig().GetClusterName(), "cluster2");

        // yaml V2 -> V3 whose filtered LogConfig content equals V1 (cluster1).  With a correct
        // CurrentConfig (=V2) the dispatcher must send V3 because V2 != V1.  With the bug,
        // CurrentConfig is the stale V1, so CompareConfigs(V1, V3==V1) returns equal and the
        // subscriber would silently stay at V2.
        TString yamlV3 = R"(
---
metadata:
  cluster: ""
  version: 2

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
        CheckReplaceConfig(runtime, Ydb::StatusIds::SUCCESS, yamlV3);

        reply = runtime.GrabEdgeEventRethrow<TEvPrivate::TEvGotNotification>(handle);
        UNIT_ASSERT_VALUES_EQUAL_C(reply->Config.GetLogConfig().GetClusterName(), "cluster1",
            "yaml subscriber must converge to the latest yaml content even after a non-yaml event "
            "interleaves with a held update");
    }

}

Y_UNIT_TEST_SUITE(TConfigsDispatcherObservabilityTests) {
    
    TActorId GetRuntimeDispatcherId(TTenantTestRuntime& runtime) {
        return MakeConfigsDispatcherID(runtime.GetNodeId(0));
    }
    
    TConfigsDispatcherState QueryState(TTenantTestRuntime& runtime, TActorId dispatcherId) {
        runtime.Send(new IEventHandle(dispatcherId, runtime.Sender, new TEvConfigsDispatcher::TEvGetStateRequest()));
        TAutoPtr<IEventHandle> handle;
        auto response = runtime.GrabEdgeEventRethrow<TEvConfigsDispatcher::TEvGetStateResponse>(handle);
        return response->State;
    }
    
    TString QueryStorageYaml(TTenantTestRuntime& runtime, TActorId dispatcherId) {
        runtime.Send(new IEventHandle(dispatcherId, runtime.Sender, new TEvConfigsDispatcher::TEvGetStorageYamlRequest()));
        TAutoPtr<IEventHandle> handle;
        auto response = runtime.GrabEdgeEventRethrow<TEvConfigsDispatcher::TEvGetStorageYamlResponse>(handle);
        return response->StorageYaml;
    }
    
    TTenantTestConfig ConfigWithoutDispatcher() {
        TTenantTestConfig cfg = DefaultConsoleTestConfig();
        cfg.CreateConfigsDispatcher = false;
        return cfg;
    }
    
    Y_UNIT_TEST(TestGetStateRequestResponse) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        InitConfigsDispatcher(runtime);
        
        TActorId dispatcherId = GetRuntimeDispatcherId(runtime);
        auto state = QueryState(runtime, dispatcherId);
        
        UNIT_ASSERT(!state.ConfigSourceLabel.empty() || state.ConfigSource != EConfigSource::Unknown);
        UNIT_ASSERT(state.SubscriptionsCount >= 0);
    }
    
    Y_UNIT_TEST(TestGetStorageYamlRequestResponse) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        InitConfigsDispatcher(runtime);
        
        TActorId dispatcherId = GetRuntimeDispatcherId(runtime);
        TString storageYaml = QueryStorageYaml(runtime, dispatcherId);
        
        UNIT_ASSERT(storageYaml.empty());
    }
    
    Y_UNIT_TEST(TestSeedNodesInitialization) {
        NKikimrConfig::TAppConfig config;
        TString storageYaml = "storage:\n  nodes:\n  - node1:2135\n  - node2:2135\n";
        config.SetStartupStorageYaml(storageYaml);
        
        NConfig::TConfigsDispatcherInitInfo initInfo;
        initInfo.InitialConfig = config;
        initInfo.StartupConfigYaml = "config:\n  log_config:\n    cluster_name: test\n";
        initInfo.StartupStorageYaml = storageYaml;
        initInfo.Labels["config_source"] = "seed_nodes";
        initInfo.Labels["configuration_version"] = "v2";
        initInfo.DebugInfo = NConfig::TDebugInfo{};
        
        TTenantTestRuntime runtime(ConfigWithoutDispatcher(), config);
        
        auto* dispatcher = NConsole::CreateConfigsDispatcher(initInfo);
        TActorId dispatcherId = runtime.Register(dispatcher);
        runtime.EnableScheduleForActor(dispatcherId, true);
        
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TDispatchOptions::TFinalEventCondition(TEvConsole::EvConfigSubscriptionNotification));
            runtime.DispatchEvents(options);
        }
        
        auto state = QueryState(runtime, dispatcherId);
        
        UNIT_ASSERT_EQUAL(state.ConfigSource, EConfigSource::SeedNodes);
        UNIT_ASSERT_VALUES_EQUAL(state.ConfigSourceLabel, "seed_nodes");
        UNIT_ASSERT_VALUES_EQUAL(state.ConfigurationVersion, "v2");
        UNIT_ASSERT(state.HasStorageYaml);
        UNIT_ASSERT(state.StorageYamlSize > 0);
        
        TString retrievedStorageYaml = QueryStorageYaml(runtime, dispatcherId);
        UNIT_ASSERT_VALUES_EQUAL(retrievedStorageYaml, storageYaml);
    }
    
    Y_UNIT_TEST(TestDynamicConfigInitialization) {
        
        NKikimrConfig::TAppConfig config;
        
        NConfig::TConfigsDispatcherInitInfo initInfo;
        initInfo.InitialConfig = config;
        initInfo.StartupConfigYaml = "config:\n  log_config:\n    cluster_name: test\n";
        initInfo.Labels["config_source"] = "dynamic";
        initInfo.Labels["configuration_version"] = "v1";
        initInfo.DebugInfo = NConfig::TDebugInfo{};
        
        TTenantTestRuntime runtime(ConfigWithoutDispatcher(), config);
        
        auto* dispatcher = NConsole::CreateConfigsDispatcher(initInfo);
        TActorId dispatcherId = runtime.Register(dispatcher);
        runtime.EnableScheduleForActor(dispatcherId, true);
        
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TDispatchOptions::TFinalEventCondition(TEvConsole::EvConfigSubscriptionNotification));
            runtime.DispatchEvents(options);
        }
        
        auto state = QueryState(runtime, dispatcherId);
        
        UNIT_ASSERT_EQUAL(state.ConfigSource, EConfigSource::DynamicConfig);
        UNIT_ASSERT_VALUES_EQUAL(state.ConfigSourceLabel, "dynamic");
        UNIT_ASSERT_VALUES_EQUAL(state.ConfigurationVersion, "v1");
        UNIT_ASSERT(!state.HasStorageYaml);
        UNIT_ASSERT_VALUES_EQUAL(state.StorageYamlSize, 0);
        
        TString retrievedStorageYaml = QueryStorageYaml(runtime, dispatcherId);
        UNIT_ASSERT(retrievedStorageYaml.empty());
    }
    
    Y_UNIT_TEST(TestUnknownConfigSource) {
        NKikimrConfig::TAppConfig config;
        
        NConfig::TConfigsDispatcherInitInfo initInfo;
        initInfo.InitialConfig = config;
        initInfo.StartupConfigYaml = "config: {}\n";
        initInfo.DebugInfo = NConfig::TDebugInfo{};
        
        TTenantTestRuntime runtime(ConfigWithoutDispatcher(), config);
        
        auto* dispatcher = NConsole::CreateConfigsDispatcher(initInfo);
        TActorId dispatcherId = runtime.Register(dispatcher);
        runtime.EnableScheduleForActor(dispatcherId, true);
        
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TDispatchOptions::TFinalEventCondition(TEvConsole::EvConfigSubscriptionNotification));
            runtime.DispatchEvents(options);
        }
        
        auto state = QueryState(runtime, dispatcherId);
        
        UNIT_ASSERT_EQUAL(state.ConfigSource, EConfigSource::DynamicConfig);
        UNIT_ASSERT(state.ConfigSourceLabel.empty());
        UNIT_ASSERT(!state.HasStorageYaml);
    }

    Y_UNIT_TEST(TestMonJsonMasksSensitiveFieldsInDebugInfo) {
        NKikimrConfig::TAppConfig config;

        NConfig::TConfigsDispatcherInitInfo initInfo;
        initInfo.InitialConfig = config;
        initInfo.StartupConfigYaml = "config: {}\n";
        initInfo.DebugInfo = NConfig::TDebugInfo{};
        initInfo.DebugInfo->StaticConfig.MutableGRpcConfig()->SetKey("static_secret");
        initInfo.DebugInfo->OldDynConfig.MutableGRpcConfig()->SetKey("old_dyn_secret");
        initInfo.DebugInfo->NewDynConfig.MutableGRpcConfig()->SetKey("new_dyn_secret");

        TTenantTestRuntime runtime(ConfigWithoutDispatcher(), config);
        auto* dispatcher = NConsole::CreateConfigsDispatcher(initInfo);
        const TActorId dispatcherId = runtime.Register(dispatcher);
        runtime.EnableScheduleForActor(dispatcherId, true);

        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TDispatchOptions::TFinalEventCondition(TEvConsole::EvConfigSubscriptionNotification));
            runtime.DispatchEvents(options);
        }

        const TActorId edge = runtime.AllocateEdgeActor();
        auto request = MakeHolder<THttpRequest>(HTTP_METHOD_GET);
        request->HttpHeaders.AddHeader("Content-Type", "application/json");
        NMonitoring::TMonService2HttpRequest monReq(nullptr, request.Get(), nullptr, nullptr, "", nullptr);
        runtime.Send(new IEventHandle(dispatcherId, edge, new NMon::TEvHttpInfo(monReq)));

        TAutoPtr<IEventHandle> handle;
        const auto* response = runtime.GrabEdgeEventRethrow<NMon::TEvHttpInfoRes>(handle);
        const TString& answer = response->Answer;

        const size_t jsonBegin = answer.find('{');
        UNIT_ASSERT_UNEQUAL(jsonBegin, TString::npos);
        const TString jsonBody = answer.substr(jsonBegin);
        const NJson::TJsonValue actual = ReadJsonFromString(jsonBody);
        const NJson::TJsonValue expected = ReadJsonFromString(R"json({
            "last_replay_seed_nodes": false,
            "initial_cms_json_config": {
                "grpc_config": {
                    "key": "***"
                }
            },
            "last_replay_dynamic_config": false,
            "resolved_json_config": null,
            "initial_cms_yaml_json_config": {
                "grpc_config": {
                    "key": "***"
                }
            },
            "current_json_config": {},
            "has_storage_yaml": false,
            "yaml_config": "",
            "initial_json_config": {
                "grpc_config": {
                    "key": "***"
                }
            },
            "labels": []
        })json");
        UNIT_ASSERT_C(expected == actual, jsonBody);
    }
}

} // namespace NKikimr

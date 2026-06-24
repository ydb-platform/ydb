#include "console_configs_manager.h"
#include "ut_helpers.h"

#include <ydb/core/config/init/mock.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/tablet/bootstrapper.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/cms/console/ut_configs_dispatcher/ut_private_database_config.pb.h>
#include <ydb/library/fyamlcpp/fyamlcpp.h>
#include <ydb/library/yaml_config/yaml_config_parser.h>
#include <ydb/library/yaml_config/yaml_config_helpers.h>
#include <library/cpp/protobuf/json/json2proto.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/monlib/service/mon_service_http_request.h>

#include <util/system/hostname.h>
#include <util/string/subst.h>

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
        EvParsedPrivateDatabaseConfig,
        EvResetPrivateDatabaseConfig,
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


    // Carries the secret_port the end-node subscriber read from the dispatcher-
    // parsed TUtPrivateDatabaseConfig (OpaqueConfigs), proving the dispatcher did the parse.
    struct TEvParsedPrivateDatabaseConfig : public TEventLocal<TEvParsedPrivateDatabaseConfig, EvParsedPrivateDatabaseConfig> {
        ui32 SecretPort = 0;
    };

    // Emitted when the subscriber received a notification for its subscribed
    // opaque kind but OpaqueConfigs carried no parsed payload — i.e. the
    // dispatcher signaled "section absent / cleared".
    struct TEvResetPrivateDatabaseConfig : public TEventLocal<TEvResetPrivateDatabaseConfig, EvResetPrivateDatabaseConfig> {
    };
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

// End-node subscriber: subscribes to the opaque kind PrivateDatabaseConfigItem
// and reads the ALREADY-PARSED TUtPrivateDatabaseConfig from the notification's
// OpaqueConfigs - it does not touch the opaque string itself; the dispatcher parsed it.
class TPrivateDatabaseConfigSubscriber : public TActorBootstrapped<TPrivateDatabaseConfigSubscriber>
{
public:
    // No trust to the runtime.SetObserverFunc() - fires multiple times per delivery
    // for ES_PRIVATE events that share a numeric id with other unrelated private events;
    // ev->Sender + ev->Recipient filter not help.
    std::atomic<int> parsedCnt{0};
    std::atomic<int> resetCnt{0};

    TPrivateDatabaseConfigSubscriber(TActorId sink)
        : Sink(sink)
    {}

    void Bootstrap(const TActorContext &ctx) {
        Become(&TThis::StateWork);
        ctx.Send(MakeConfigsDispatcherID(ctx.SelfID.NodeId()),
            new TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest(
                (ui32)NKikimrConsole::TConfigItem::PrivateDatabaseConfigItem, ctx.SelfID));
    }

    void Handle(TEvConsole::TEvConfigNotificationRequest::TPtr &ev, const TActorContext &ctx) {
        auto &rec = ev->Get()->Record;
        const auto &opaqueConfigs = ev->Get()->OpaqueConfigs;

        if (auto it = opaqueConfigs.find((ui32)NKikimrConsole::TConfigItem::PrivateDatabaseConfigItem);
            it != opaqueConfigs.end() && it->second)
        {
            NKikimrOpaqueConfigUt::TUtPrivateDatabaseConfig impl;
            impl.CopyFrom(*it->second);                 // descriptor-checked, no RTTI
            auto *event = new TEvPrivate::TEvParsedPrivateDatabaseConfig;
            event->SecretPort = impl.GetSecretPort();
            parsedCnt++;
            ctx.Send(Sink, event);
        } else {
            // Notification arrived for our subscribed kind but no parsed
            // opaque payload — section is absent / was cleared. Forward
            // it as a distinguishable event so tests can assert it.
            resetCnt++;
            ctx.Send(Sink, new TEvPrivate::TEvResetPrivateDatabaseConfig);
        }
        ctx.Send(ev->Sender, new TEvConsole::TEvConfigNotificationResponse(rec), 0, ev->Cookie);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvConsole::TEvConfigNotificationRequest, Handle);
            IgnoreFunc(TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse);
        }
    }
private:
    TActorId Sink;
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

    // The dispatcher computes unknown/deprecated fields for the *resolved* config it
    // receives from the console and exposes them in its mon JSON ("unknown_fields").
    Y_UNIT_TEST(TestMonJsonReportsResolvedUnknownFields) {
        // The dispatcher (re)computes unknown/deprecated fields in ParseYamlProtoConfig,
        // which only runs when it receives a config-subscription notification carrying a
        // changed YAML body. That notification is only produced once the dispatcher has a
        // subscriber, so we must: replace the YAML on the console (allowing the unknown
        // field), then add a subscriber and wait for it to be notified -- by which point
        // the dispatcher has resolved the config and filled ResolvedConfigUnknownFields.
        NKikimrConfig::TAppConfig bootstrapConfig;
        auto *label = bootstrapConfig.AddLabels();
        label->SetName("test");
        label->SetValue("true");

        const TString yamlWithUnknown = R"(
---
metadata:
  cluster: ""
  version: 0

config:
  log_config:
    cluster_name: cluster1
  yaml_config_enabled: true
  unknown_field_for_test: 42

allowed_labels:
  test:
    type: enum
    values:
      ? true

selector_config: []
)";

        TTenantTestRuntime runtime(DefaultConsoleTestConfig(), bootstrapConfig);
        TAutoPtr<IEventHandle> handle;
        const TActorId dispatcherId = InitConfigsDispatcher(runtime);

        {
            auto *event = new TEvConsole::TEvReplaceYamlConfigRequest;
            event->Record.MutableRequest()->set_config(yamlWithUnknown);
            event->Record.MutableRequest()->set_allow_unknown_fields(true);
            runtime.SendToConsole(event);

            runtime.GrabEdgeEventRethrow<TEvConsole::TEvReplaceYamlConfigResponse>(handle);
        }

        // Subscribing drives the dispatcher to fetch & resolve the YAML config; grabbing the
        // subscriber's notification guarantees ParseYamlProtoConfig has already run.
        AddSubscriber(runtime, {(ui32)NKikimrConsole::TConfigItem::LogConfigItem});
        runtime.GrabEdgeEventRethrow<TEvPrivate::TEvGotNotification>(handle);

        const TActorId edge = runtime.AllocateEdgeActor();
        auto request = MakeHolder<THttpRequest>(HTTP_METHOD_GET);
        request->HttpHeaders.AddHeader("Content-Type", "application/json");
        NMonitoring::TMonService2HttpRequest monReq(nullptr, request.Get(), nullptr, nullptr, "", nullptr);
        runtime.Send(new IEventHandle(dispatcherId, edge, new NMon::TEvHttpInfo(monReq)));

        const auto* response = runtime.GrabEdgeEventRethrow<NMon::TEvHttpInfoRes>(handle);
        const TString& answer = response->Answer;

        const size_t jsonBegin = answer.find('{');
        UNIT_ASSERT_UNEQUAL(jsonBegin, TString::npos);
        const NJson::TJsonValue json = ReadJsonFromString(answer.substr(jsonBegin));

        UNIT_ASSERT_C(json.Has("unknown_fields"), "no unknown_fields in mon json: " << answer);
        bool found = false;
        for (const auto& f : json["unknown_fields"].GetArray()) {
            if (f["name"].GetString() == "unknown_field_for_test") {
                found = true;
                UNIT_ASSERT_VALUES_EQUAL(f["deprecated"].GetBoolean(), false);
            }
        }
        UNIT_ASSERT_C(found, "unknown_field_for_test not reported: " << answer);
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
            "configuration_version": "v1",
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
            "unknown_fields": [],
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

Y_UNIT_TEST_SUITE(TConfigsDispatcherOpaqueConfigTests) {

    TTenantTestConfig TenantTestConfig()
    {
        // need real SchemeShard for CreateTenant; tenant is created dynamically,
        // so remove the pre-existing subdomain and static slot that would otherwise
        // be started before the path exists and cause a VERIFY failure.
        TTenantTestConfig res = DefaultConsoleTestConfig();
        res.FakeSchemeShard = false;
        res.Domains[0].Subdomains.clear();
        res.Nodes[0].TenantPoolConfig.StaticSlots.clear();
        return res;
    }

    void DoTestDispatcherParseAndSendOpaquePrivateDatabaseConfig(bool withParser) {
        const TString mainConfig0 = R"(
---
metadata:
  kind: MainConfig
  cluster: ""
  version: 0
config:
  yaml_config_enabled: true
  feature_flags:
    database_yaml_config_allowed: true
)";
        const TString mainConfig1 = R"(
---
metadata:
  kind: MainConfig
  cluster: ""
  version: 1
config:
  yaml_config_enabled: true
  feature_flags:
    database_yaml_config_allowed: true
  private_database_config:
    secret_port: 666
)";
        const TString dbConfigTemplate = R"(
---
metadata:
  kind: DatabaseConfig
  database: "/dc-1/users/tenant-1"
  version: VERSION
config:
  private_database_config: INHERIT
    secret_port: VALUE
)";

        // Set the end-node parser for the opaque kind into the dispatcher.
        NKikimrConfig::TAppConfig appcfg;
        auto *label = appcfg.AddLabels();
        label->SetName("tenant");
        label->SetValue(TENANT1_1_NAME);
        appcfg.MutableFeatureFlags()->SetDatabaseYamlConfigAllowed(true);

        TTenantTestConfig testConfig = TenantTestConfig();

        if (withParser) {
            auto parser = std::bind(NKikimr::NYaml::DefaultOpaqueConfigParser<NKikimrOpaqueConfigUt::TUtPrivateDatabaseConfig>, std::placeholders::_1, true);
            testConfig.OpaqueConfigParsers[(ui32)NKikimrConsole::TConfigItem::PrivateDatabaseConfigItem] = parser;
        }

        TTenantTestRuntime runtime(testConfig, appcfg);

        CheckCreateTenant(runtime, TENANT1_1_NAME, Ydb::StatusIds::SUCCESS, {{"hdd", 1}});

        // The end-node consumer subscribes to the private kind.
        auto subscriber = new TPrivateDatabaseConfigSubscriber(runtime.Sender);
        TActorId subscriberId = runtime.Register(subscriber);
        runtime.EnableScheduleForActor(subscriberId, true);

        // Init cluster with main config without private_database_config
        CheckReplaceConfig(runtime, Ydb::StatusIds::SUCCESS, mainConfig0);

        // No private_database_config yet.
        // The subscriber must report nothing.
        DrainAllEvents<TEvPrivate::TEvParsedPrivateDatabaseConfig>(runtime);
        UNIT_ASSERT_VALUES_EQUAL(subscriber->parsedCnt.load(), 0);

        // The OpaqueConfig marker makes the cluster accept config.private_database_config
        // WITHOUT allow_unknown_fields.
        // The dispatcher resolves it and pulls config.private_database_config from the resolved
        // YAML for the injected parser.

        // Set the opaque section in the main config.
        //  withParser: The subscriber reports secret_port read from the dispatcher-parsed
        //              main config.
        // !withParser: The subscriber must report nothing.
        {
            subscriber->parsedCnt = 0;
            CheckReplaceConfig(runtime, Ydb::StatusIds::SUCCESS, mainConfig1);
            TAutoPtr<IEventHandle> handle;
            if (withParser) {
                auto ev = runtime.GrabEdgeEventRethrow<TEvPrivate::TEvParsedPrivateDatabaseConfig>(handle, TDuration::MilliSeconds(100));
                UNIT_ASSERT_C(ev, "No PrivateDatabaseConfig received and processed by subscriber");
                UNIT_ASSERT_VALUES_EQUAL(ev->SecretPort, 666);
                UNIT_ASSERT_VALUES_EQUAL(subscriber->parsedCnt.load(), 1);
            }
            else {
                DrainAllEvents<TEvPrivate::TEvParsedPrivateDatabaseConfig>(runtime);
                UNIT_ASSERT_VALUES_EQUAL(subscriber->parsedCnt.load(), 0);
            }
        }

        // Checks both without and with !inherit marker
        int dbVersion = 0;
        TString inherit = "";
        for (int x = 0; x < 2; x++)
        {
            // Set the opaque section in the database config.
            //  withParser: The subscriber reports secret_port read from the dispatcher-parsed
            //              database config.
            // !withParser: The subscriber must report nothing.
            // Run several times to ensure opaque-only change is passed to subscriber.
            subscriber->parsedCnt = 0;
            for (int i = 0; i < 5; i++ )
            {
                Cerr << Endl << ">>>>> "
                     << (inherit ? "WITH" : "WITHOUT") << " !inherit" << Endl;

                auto dbConfig = dbConfigTemplate;

                SubstGlobal(dbConfig, "INHERIT", inherit);
                SubstGlobal(dbConfig, "VERSION", ToString(dbVersion++));
                SubstGlobal(dbConfig, "VALUE", ToString(dbVersion));

                CheckReplaceDatabaseConfig(runtime, Ydb::StatusIds::SUCCESS, dbConfig);
                {
                    TAutoPtr<IEventHandle> handle;
                    if (withParser) {
                        auto ev = runtime.GrabEdgeEventRethrow<TEvPrivate::TEvParsedPrivateDatabaseConfig>(handle, TDuration::MilliSeconds(100));
                        UNIT_ASSERT_C(ev, "No PrivateDatabaseConfig received and processed by subscriber");
                        UNIT_ASSERT_VALUES_EQUAL(subscriber->parsedCnt.load(), i+1);
                        UNIT_ASSERT_VALUES_EQUAL(ev->SecretPort, dbVersion);
                    }
                    else {
                        DrainAllEvents<TEvPrivate::TEvParsedPrivateDatabaseConfig>(runtime);
                        UNIT_ASSERT_VALUES_EQUAL(subscriber->parsedCnt.load(), 0);
                    }
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(subscriber->parsedCnt.load(), withParser ? 5 : 0);

            inherit = "!inherit";
        }
    }

    Y_UNIT_TEST(TestDispatcherOmitsOpaquePrivateDatabaseConfigWithoutParser) {
        DoTestDispatcherParseAndSendOpaquePrivateDatabaseConfig(/*withParser=*/false);
    }

    Y_UNIT_TEST(TestDispatcherParseAndSendOpaquePrivateDatabaseConfigWithParser) {
        DoTestDispatcherParseAndSendOpaquePrivateDatabaseConfig(/*withParser=*/true);
    }

    // Test every transition of the opaque configs changes Edge cases:
    //   absent -> absent             (init, no dispatch)
    //   absent -> non-empty          (added - dispatch, parsed payload)
    //   non-empty -> non-empty       (same value - no dispatch)
    //   non-empty(A) -> non-empty(B) (mutated - dispatch, parsed payload)
    //   non-empty -> absent          (removed - dispatch, reset / no payload)
    //   absent -> non-empty          (re-added via database yaml - dispatch, parsed)
    //   non-empty -> non-empty       (mutated via database yaml - dispatch, parsed)
    Y_UNIT_TEST(TestOpaquePrivateDatabaseConfigEdgeCases) {
        // No private_database_config — opaque section absent.
        const TString mainConfigAbsent = R"(
---
metadata:
  kind: MainConfig
  cluster: ""
  version: VERSION
config:
  yaml_config_enabled: true
  feature_flags:
    database_yaml_config_allowed: true
)";
        // With private_database_config.secret_port = VALUE.
        const TString mainConfigWithOpaque = R"(
---
metadata:
  kind: MainConfig
  cluster: ""
  version: VERSION
config:
  yaml_config_enabled: true
  feature_flags:
    database_yaml_config_allowed: true
  private_database_config:
    secret_port: VALUE
)";
        const TString dbConfigTemplate = R"(
---
metadata:
  kind: DatabaseConfig
  database: "/dc-1/users/tenant-1"
  version: VERSION
config:
  private_database_config:
    secret_port: VALUE
)";
        auto subst = [](TString tmpl, ui64 version, ui64 value) {
            SubstGlobal(tmpl, "VERSION", ToString(version));
            SubstGlobal(tmpl, "VALUE", ToString(value));
            return tmpl;
        };

        NKikimrConfig::TAppConfig appcfg;
        auto *label = appcfg.AddLabels();
        label->SetName("tenant");
        label->SetValue(TENANT1_1_NAME);
        appcfg.MutableFeatureFlags()->SetDatabaseYamlConfigAllowed(true);

        TTenantTestConfig testConfig = DefaultConsoleTestConfig();
        auto parser = std::bind(NKikimr::NYaml::DefaultOpaqueConfigParser<NKikimrOpaqueConfigUt::TUtPrivateDatabaseConfig>, std::placeholders::_1, true);
        testConfig.OpaqueConfigParsers[(ui32)NKikimrConsole::TConfigItem::PrivateDatabaseConfigItem] = parser;

        TTenantTestRuntime runtime(testConfig, appcfg);

        auto subscriber = new TPrivateDatabaseConfigSubscriber(runtime.Sender);
        TActorId subscriberId = runtime.Register(subscriber);
        runtime.EnableScheduleForActor(subscriberId, true);

        // No trust to the runtime.SetObserverFunc() - fires multiple times per delivery
        // for ES_PRIVATE events that share a numeric id with other unrelated private events;
        // ev->Sender + ev->Recipient filter not help.
        auto pollOneOfEither = [&](TDuration timeout) -> ui32 {
            TAutoPtr<IEventHandle> handle;
            auto [parsed, reset] = runtime.GrabEdgeEventsRethrow<
                TEvPrivate::TEvParsedPrivateDatabaseConfig,
                TEvPrivate::TEvResetPrivateDatabaseConfig>(handle, timeout);
            if (parsed) {
                return TEvPrivate::EvParsedPrivateDatabaseConfig;
            }
            if (reset) {
                return TEvPrivate::EvResetPrivateDatabaseConfig;
            }
            return 0;
        };

        ui64 mainVersion = 0;

        // Case 1: absent -> absent (initial state). No opaque dispatch
        // expected. There may be an initial subscription notification that
        // arrives empty — observed via subscriber->resetQuan.
        subscriber->parsedCnt = 0;
        subscriber->resetCnt = 0;
        CheckReplaceConfig(runtime, Ydb::StatusIds::SUCCESS, subst(mainConfigAbsent, mainVersion++, 0));
        {
            TAutoPtr<IEventHandle> handle;
            auto ev = runtime.GrabEdgeEventRethrow<TEvPrivate::TEvResetPrivateDatabaseConfig>(handle, TDuration::MilliSeconds(100));
            UNIT_ASSERT_C(ev, "absent -> absent must produce the initial reset notification");
        }
        UNIT_ASSERT_VALUES_EQUAL(subscriber->parsedCnt.load(), 0);
        // No subscriber->resetQuan.load() check - the subscriber-init notification always
        // dispatches once on the dispatcher side, so subscriber->resetQuan may have grown.

        // Case 2: absent -> non-empty (added). Expect one parsed event.
        subscriber->parsedCnt = 0;
        subscriber->resetCnt = 0;
        CheckReplaceConfig(runtime, Ydb::StatusIds::SUCCESS, subst(mainConfigWithOpaque, mainVersion++, 666));
        {
            TAutoPtr<IEventHandle> handle;
            auto ev = runtime.GrabEdgeEventRethrow<TEvPrivate::TEvParsedPrivateDatabaseConfig>(handle, TDuration::MilliSeconds(100));
            UNIT_ASSERT_C(ev, "absent -> non-empty must dispatch a parsed opaque config");
            UNIT_ASSERT_VALUES_EQUAL(ev->SecretPort, 666);
        }
        UNIT_ASSERT_VALUES_EQUAL(subscriber->parsedCnt.load(), 1);
        UNIT_ASSERT_VALUES_EQUAL(subscriber->resetCnt.load(), 0);

        // Case 3: non-empty -> non-empty (same value, version bumped).
        // The opaque RawJson is byte-identical so no opaque-driven dispatch;
        // and the proto truncation for this kind is unchanged so the regular
        // path also does not fire. Expect zero new events.
        subscriber->parsedCnt = 0;
        subscriber->resetCnt = 0;
        CheckReplaceConfig(runtime, Ydb::StatusIds::SUCCESS, subst(mainConfigWithOpaque, mainVersion++, 666));
        {
            UNIT_ASSERT_VALUES_EQUAL_C(pollOneOfEither(TDuration::MilliSeconds(100)), 0u,
                                      "identical opaque must not redispatch");
        }
        UNIT_ASSERT_VALUES_EQUAL_C(subscriber->parsedCnt.load(), 0, "identical opaque must not redispatch");
        UNIT_ASSERT_VALUES_EQUAL_C(subscriber->resetCnt.load(), 0, "identical opaque must not redispatch");

        // Case 4: non-empty(666) -> non-empty(777). RawJson differs:
        // expect exactly one parsed event carrying the new value.
        subscriber->parsedCnt = 0;
        subscriber->resetCnt = 0;
        CheckReplaceConfig(runtime, Ydb::StatusIds::SUCCESS, subst(mainConfigWithOpaque, mainVersion++, 777));
        {
            TAutoPtr<IEventHandle> handle;
            auto ev = runtime.GrabEdgeEventRethrow<TEvPrivate::TEvParsedPrivateDatabaseConfig>(handle, TDuration::MilliSeconds(100));
            UNIT_ASSERT_C(ev, "non-empty A -> non-empty B must dispatch a parsed opaque config");
            UNIT_ASSERT_VALUES_EQUAL(ev->SecretPort, 777);
        }
        UNIT_ASSERT_VALUES_EQUAL(subscriber->parsedCnt.load(), 1);
        UNIT_ASSERT_VALUES_EQUAL(subscriber->resetCnt.load(), 0);

        // Case 5: non-empty -> absent (removed). Expect one reset event
        // and no parsed event.
        subscriber->parsedCnt = 0;
        subscriber->resetCnt = 0;
        CheckReplaceConfig(runtime, Ydb::StatusIds::SUCCESS, subst(mainConfigAbsent, mainVersion++, 0));
        {
            TAutoPtr<IEventHandle> handle;
            auto ev = runtime.GrabEdgeEventRethrow<TEvPrivate::TEvResetPrivateDatabaseConfig>(handle, TDuration::MilliSeconds(100));
            UNIT_ASSERT_C(ev, "non-empty -> absent must dispatch a reset notification");
        }
        UNIT_ASSERT_VALUES_EQUAL(subscriber->parsedCnt.load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(subscriber->resetCnt.load(), 1);

        // Case 6: absent -> non-empty via database yaml. The opaque
        // section reappears with a different value.
        subscriber->parsedCnt = 0;
        subscriber->resetCnt = 0;
        CheckReplaceDatabaseConfig(runtime, Ydb::StatusIds::SUCCESS, subst(dbConfigTemplate, 0, 888));
        {
            TAutoPtr<IEventHandle> handle;
            auto ev = runtime.GrabEdgeEventRethrow<TEvPrivate::TEvParsedPrivateDatabaseConfig>(handle, TDuration::MilliSeconds(100));
            UNIT_ASSERT_C(ev, "absent -> non-empty (via db yaml) must dispatch parsed opaque config");
            UNIT_ASSERT_VALUES_EQUAL(ev->SecretPort, 888);
        }
        UNIT_ASSERT_VALUES_EQUAL(subscriber->parsedCnt.load(), 1);
        UNIT_ASSERT_VALUES_EQUAL(subscriber->resetCnt.load(), 0);

        // Case 7: non-empty(888) -> non-empty(999) via database yaml.
        subscriber->parsedCnt = 0;
        subscriber->resetCnt = 0;
        CheckReplaceDatabaseConfig(runtime, Ydb::StatusIds::SUCCESS, subst(dbConfigTemplate, 1, 999));
        {
            TAutoPtr<IEventHandle> handle;
            auto ev = runtime.GrabEdgeEventRethrow<TEvPrivate::TEvParsedPrivateDatabaseConfig>(handle, TDuration::MilliSeconds(100));
            UNIT_ASSERT_C(ev, "non-empty A -> non-empty B (via db yaml) must dispatch parsed opaque config");
            UNIT_ASSERT_VALUES_EQUAL(ev->SecretPort, 999);
        }
        UNIT_ASSERT_VALUES_EQUAL(subscriber->parsedCnt.load(), 1);
        UNIT_ASSERT_VALUES_EQUAL(subscriber->resetCnt.load(), 0);
    }
}

Y_UNIT_TEST_SUITE(TConfigsDispatcherDatabaseConfigSelectorsTests) {

    TTenantTestConfig DatabaseSelectorsTestConfig()
    {
        // need real SchemeShard for CreateTenant; tenant is created dynamically,
        // so remove the pre-existing subdomain and static slot that would otherwise
        // be started before the path exists and cause a VERIFY failure.
        TTenantTestConfig res = DefaultConsoleTestConfig();
        res.FakeSchemeShard = false;
        res.Domains[0].Subdomains.clear();
        res.Nodes[0].TenantPoolConfig.StaticSlots.clear();
        return res;
    }

    const TString PermissiveTenantUserAttribute = NConsole::TConfigsManager::GetPermissiveDatabaseConfigSelectorsTenantAttributeName();

    Y_UNIT_TEST(TestEndToEndSelectorsResolution) {
        const TString mainConfig = R"(
---
metadata:
  kind: MainConfig
  cluster: ""
  version: 0
config:
  yaml_config_enabled: true
  feature_flags:
    database_yaml_config_allowed: true
  private_database_config:
    secret_port: 0
)";

        const TString dbConfigTemplate = R"(
---
metadata:
  kind: DatabaseConfig
  database: "/dc-1/users/tenant-1"
  version: 0

config:
  feature_flags: INHERIT
    enable_external_hive: false
  private_database_config: INHERIT
    secret_port: 1

allowed_labels:
  node_type:
    type: string

selector_config:

- description: node type a
  selector:
    node_type: a
  config:
    feature_flags: INHERIT
      enable_external_hive: true
    private_database_config: INHERIT
      secret_port: 2

- description: node type b
  selector:
    node_type: b
  config:
    feature_flags: INHERIT
      enable_external_hive: true
    private_database_config: INHERIT
      secret_port: 3
)";
        // Checks both without and with !inherit marker
        TString inherit = "";
        const TMap<TString, std::pair<int, bool>> expectedLabelValue = {
            // { node_type, { secret_port, enable_external_hive } }
            // node_type "-" - no label at all
            {"-", {1, false}},
            {"" , {1, false}},
            {"a", {2, true }},
            {"b", {3, true }},
            {"x", {1, false}},
        };
        for (int x = 0; x < 2; x++)
        {
            for ( const auto& lv: expectedLabelValue )
            {
                Cerr << Endl << ">>>>> "
                    << (inherit ? "WITH" : "WITHOUT") << " !inherit"
                    << ", node_type = '" << lv.first << "'" << Endl;

                auto dbConfig = dbConfigTemplate;
                SubstGlobal(dbConfig, "INHERIT", inherit);

                NKikimrConfig::TAppConfig appcfg;
                auto *label = appcfg.AddLabels();
                label->SetName("tenant");
                label->SetValue(TENANT1_1_NAME);
                if (lv.first != "-") {
                  label = appcfg.AddLabels();
                  label->SetName("node_type");
                  label->SetValue(lv.first);
                }
                appcfg.MutableFeatureFlags()->SetDatabaseYamlConfigAllowed(true);

                TTenantTestConfig testConfig = DatabaseSelectorsTestConfig();

                auto parser = std::bind(
                    NKikimr::NYaml::DefaultOpaqueConfigParser<NKikimrOpaqueConfigUt::TUtPrivateDatabaseConfig>,
                    std::placeholders::_1, true);
                testConfig.OpaqueConfigParsers[(ui32)NKikimrConsole::TConfigItem::PrivateDatabaseConfigItem] = parser;

                TTenantTestRuntime runtime(testConfig, appcfg);
                CheckCreateTenant(runtime, TENANT1_1_NAME, Ydb::StatusIds::SUCCESS, {{"hdd", 1}});

                auto flagsSubscriber = new TTestSubscriber(runtime.Sender, {(ui32)NKikimrConsole::TConfigItem::FeatureFlagsItem}, false);
                TActorId flagsSubscriberId = runtime.Register(flagsSubscriber);
                runtime.EnableScheduleForActor(flagsSubscriberId, true);

                auto opaqueSubscriber = new TPrivateDatabaseConfigSubscriber(runtime.Sender);
                TActorId opaqueSubscriberId = runtime.Register(opaqueSubscriber);
                runtime.EnableScheduleForActor(opaqueSubscriberId, true);

                CheckReplaceConfig(runtime, Ydb::StatusIds::SUCCESS, mainConfig);
                // Let subscribers receive initial config
                DrainAllEvents<TEvPrivate::TEvGotNotification,
                              TEvPrivate::TEvParsedPrivateDatabaseConfig>(runtime);

                CheckSetTenantAttribute(runtime, TENANT1_1_NAME, Ydb::StatusIds::SUCCESS,
                    PermissiveTenantUserAttribute, "true");
                opaqueSubscriber->parsedCnt = 0;
                opaqueSubscriber->resetCnt = 0;
                CheckReplaceDatabaseConfig(runtime, Ydb::StatusIds::SUCCESS, dbConfig);
                {
                    TAutoPtr<IEventHandle> handleFlags;
                    TAutoPtr<IEventHandle> handleOpaque;

                    auto evFlags  = runtime.GrabEdgeEventRethrow<TEvPrivate::TEvGotNotification>(
                        handleFlags, TDuration::MilliSeconds(100));
                    auto evOpaque = runtime.GrabEdgeEventRethrow<TEvPrivate::TEvParsedPrivateDatabaseConfig>(
                        handleOpaque, TDuration::MilliSeconds(100));

                    UNIT_ASSERT_C(evFlags, "Database config with selectors must be dispatched for 'feature_flags'");
                    UNIT_ASSERT(evFlags->Config.HasFeatureFlags());
                    UNIT_ASSERT(evFlags->Config.GetFeatureFlags().HasEnableExternalHive());
                    UNIT_ASSERT_VALUES_EQUAL(evFlags->Config.GetFeatureFlags().GetEnableExternalHive(), lv.second.second);

                    UNIT_ASSERT_C(evOpaque, "Database config with selectors must be dispatched for 'private_database_config");
                    UNIT_ASSERT_VALUES_EQUAL(evOpaque->SecretPort, lv.second.first);
                }
                UNIT_ASSERT_VALUES_EQUAL(opaqueSubscriber->parsedCnt.load(), 1);
                UNIT_ASSERT_VALUES_EQUAL(opaqueSubscriber->resetCnt.load(), 0);
            }

            inherit = "!inherit";
        }
    }

    Y_UNIT_TEST(TestSelectorsKeepWorkingForCurrentConfigAfterDisabled) {

        const TString mainConfig = R"(
---
metadata:
  kind: MainConfig
  cluster: ""
  version: 0
config:
  yaml_config_enabled: true
  feature_flags:
    database_yaml_config_allowed: true
    enable_external_hive: false
  private_database_config:
    secret_port: 1
)";

        NKikimrConfig::TAppConfig appcfg;
        auto *label = appcfg.AddLabels();
        label->SetName("tenant");
        label->SetValue(TENANT1_1_NAME);
        label = appcfg.AddLabels();
        label->SetName("node_type");
        label->SetValue("a");
        appcfg.MutableFeatureFlags()->SetDatabaseYamlConfigAllowed(true);

        TTenantTestConfig testConfig = DatabaseSelectorsTestConfig();

        auto parser = std::bind(
            NKikimr::NYaml::DefaultOpaqueConfigParser<NKikimrOpaqueConfigUt::TUtPrivateDatabaseConfig>,
            std::placeholders::_1, true);
        testConfig.OpaqueConfigParsers[(ui32)NKikimrConsole::TConfigItem::PrivateDatabaseConfigItem] = parser;

        TTenantTestRuntime runtime(testConfig, appcfg);
        CheckCreateTenant(runtime, TENANT1_1_NAME, Ydb::StatusIds::SUCCESS, {{"hdd", 1}});

        auto flagsSubscriber = new TTestSubscriber(runtime.Sender, {(ui32)NKikimrConsole::TConfigItem::FeatureFlagsItem}, false);
        TActorId flagsSubscriberId = runtime.Register(flagsSubscriber);
        runtime.EnableScheduleForActor(flagsSubscriberId, true);

        auto opaqueSubscriber = new TPrivateDatabaseConfigSubscriber(runtime.Sender);
        TActorId opaqueSubscriberId = runtime.Register(opaqueSubscriber);
        runtime.EnableScheduleForActor(opaqueSubscriberId, true);

        Cerr << ">>>>> LINE " << __LINE__ << Endl;
        CheckReplaceConfig(runtime, Ydb::StatusIds::SUCCESS, mainConfig);
        Cerr << ">>>>> LINE " << __LINE__ << Endl;
        // Let subscribers receive initial config
        DrainAllEvents<TEvPrivate::TEvGotNotification,
                       TEvPrivate::TEvParsedPrivateDatabaseConfig>(runtime);
        Cerr << ">>>>> LINE " << __LINE__ << Endl;

        const TString dbConfigTemplate = R"(
---
metadata:
  kind: DatabaseConfig
  database: "/dc-1/users/tenant-1"
  version: 0

config:
  feature_flags: !inherit
      enable_external_hive: true
  private_database_config: !inherit
    secret_port: 2

selector_config:

- description:
  selector: {}
  config:
    private_database_config: !inherit
      secret_port: 666

- description:
  selector:
    node_type: a
  config:
    private_database_config: !inherit
      secret_port: VALUE
)";
        int dbVersion = 0;
        TString dbConfigWithSelectors, dbConfig;

        /**
         * Enable selectors and set database config with selectors
         */
        dbConfigWithSelectors = dbConfigTemplate;
        SubstGlobal(dbConfigWithSelectors, "VERSION", ToString(dbVersion++));
        SubstGlobal(dbConfigWithSelectors, "VALUE", "3");
        CheckSetTenantAttribute(runtime, TENANT1_1_NAME, Ydb::StatusIds::SUCCESS,
            PermissiveTenantUserAttribute, "true");

        Cerr << ">>>>> LINE " << __LINE__ << Endl;

        opaqueSubscriber->parsedCnt = 0;
        opaqueSubscriber->resetCnt = 0;
        CheckReplaceDatabaseConfig(runtime, Ydb::StatusIds::SUCCESS, dbConfigWithSelectors);
        CheckDatabaseConfigReplacedWith(runtime, TENANT1_1_NAME, dbConfigWithSelectors);
        {
            TAutoPtr<IEventHandle> handleFlags;
            TAutoPtr<IEventHandle> handleOpaque;

            auto evFlags  = runtime.GrabEdgeEventRethrow<TEvPrivate::TEvGotNotification>(
                handleFlags, TDuration::MilliSeconds(100));
            auto evOpaque = runtime.GrabEdgeEventRethrow<TEvPrivate::TEvParsedPrivateDatabaseConfig>(
                handleOpaque, TDuration::MilliSeconds(100));

            UNIT_ASSERT_C(evFlags, "Database config with selectors must be dispatched for 'feature_flags'");
            UNIT_ASSERT(evFlags->Config.HasFeatureFlags());
            UNIT_ASSERT(evFlags->Config.GetFeatureFlags().HasEnableExternalHive());
            UNIT_ASSERT_VALUES_EQUAL(evFlags->Config.GetFeatureFlags().GetEnableExternalHive(), true);

            UNIT_ASSERT_C(evOpaque, "Database config with selectors must be dispatched for 'private_database_config'");
            UNIT_ASSERT_VALUES_EQUAL(evOpaque->SecretPort, 3);
        }
        UNIT_ASSERT_VALUES_EQUAL(opaqueSubscriber->parsedCnt.load(), 1);
        UNIT_ASSERT_VALUES_EQUAL(opaqueSubscriber->resetCnt.load(), 0);

        /**
         * Disable selectors and try to set database config with selectors.
         * No config must be applied and dispatched
         */
        dbConfig = dbConfigTemplate;
        SubstGlobal(dbConfig, "VERSION", ToString(dbVersion));
        SubstGlobal(dbConfig, "VALUE", "4");
        CheckSetTenantAttribute(runtime, TENANT1_1_NAME, Ydb::StatusIds::SUCCESS,
            PermissiveTenantUserAttribute, "");

        opaqueSubscriber->parsedCnt = 0;
        opaqueSubscriber->resetCnt = 0;
        CheckReplaceDatabaseConfig(runtime, Ydb::StatusIds::BAD_REQUEST, dbConfig);
        {
            TAutoPtr<IEventHandle> handleFlags;
            TAutoPtr<IEventHandle> handleOpaque;

            auto evFlags  = runtime.GrabEdgeEventRethrow<TEvPrivate::TEvGotNotification>(
                handleFlags, TDuration::MilliSeconds(100));
            auto evOpaque = runtime.GrabEdgeEventRethrow<TEvPrivate::TEvParsedPrivateDatabaseConfig>(
                handleOpaque, TDuration::MilliSeconds(100));

            UNIT_ASSERT_C(!evFlags, "No config must be dispatched for 'feature_flags'");
            UNIT_ASSERT_C(!evOpaque, "Database config with selectors must be dispatched for 'private_database_config'");
        }
        UNIT_ASSERT_VALUES_EQUAL(opaqueSubscriber->parsedCnt.load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(opaqueSubscriber->resetCnt.load(), 0);

        /**
         * Ensure database config with selectors survives system restart
         * after selectors has been disabled
         */

        // Restart Console
        GracefulRestartTablet(runtime, MakeConsoleID(), runtime.AllocateEdgeActor(0));

        // Ensure stored config is the same as before restart (with selectors)
        CheckDatabaseConfigReplacedWith(runtime, TENANT1_1_NAME, dbConfigWithSelectors);

        // Ensure incoming config is the same as before restart (with selectors):
        //  - re-create subscribers so they re-subscribe to the ConfigsDispatcher
        //    and receive unchanged config
        runtime.Send(new IEventHandle(flagsSubscriberId, TActorId(), new TEvents::TEvPoisonPill));
        runtime.Send(new IEventHandle(opaqueSubscriberId, TActorId(), new TEvents::TEvPoisonPill));
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(100));

        flagsSubscriber = new TTestSubscriber(runtime.Sender, {(ui32)NKikimrConsole::TConfigItem::FeatureFlagsItem}, false);
        flagsSubscriberId = runtime.Register(flagsSubscriber);
        runtime.EnableScheduleForActor(flagsSubscriberId, true);

        opaqueSubscriber = new TPrivateDatabaseConfigSubscriber(runtime.Sender);
        opaqueSubscriberId = runtime.Register(opaqueSubscriber);
        runtime.EnableScheduleForActor(opaqueSubscriberId, true);

        opaqueSubscriber->parsedCnt = 0;
        opaqueSubscriber->resetCnt = 0;
        {
            TAutoPtr<IEventHandle> handleFlags;
            TAutoPtr<IEventHandle> handleOpaque;

            auto evFlags  = runtime.GrabEdgeEventRethrow<TEvPrivate::TEvGotNotification>(
                handleFlags, TDuration::MilliSeconds(100));
            auto evOpaque = runtime.GrabEdgeEventRethrow<TEvPrivate::TEvParsedPrivateDatabaseConfig>(
                handleOpaque, TDuration::MilliSeconds(100));

            UNIT_ASSERT_C(evFlags, "Database config with selectors must be dispatched for 'feature_flags'");
            UNIT_ASSERT(evFlags->Config.HasFeatureFlags());
            UNIT_ASSERT(evFlags->Config.GetFeatureFlags().HasEnableExternalHive());
            UNIT_ASSERT_VALUES_EQUAL(evFlags->Config.GetFeatureFlags().GetEnableExternalHive(), true);

            UNIT_ASSERT_C(evOpaque, "Database config with selectors must be dispatched for 'private_database_config'");
            UNIT_ASSERT_VALUES_EQUAL(evOpaque->SecretPort, 3);
        }
        UNIT_ASSERT_VALUES_EQUAL(opaqueSubscriber->parsedCnt.load(), 1);
        UNIT_ASSERT_VALUES_EQUAL(opaqueSubscriber->resetCnt.load(), 0);
    }
}

} // namespace NKikimr

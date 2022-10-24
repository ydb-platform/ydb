#include "configs_dispatcher.h"
#include "ut_helpers.h"

#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/tablet/bootstrapper.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/testlib/tablet_helpers.h>

#include <util/system/hostname.h>

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

NKikimrConfig::TAppConfig GetConfig(TTenantTestRuntime &runtime, TVector<ui32> kinds, bool cache = true)
{
    TAutoPtr<IEventHandle> handle;
    runtime.Send(new IEventHandle(MakeConfigsDispatcherID(runtime.GetNodeId(0)),
                                  runtime.Sender,
                                  new TEvConfigsDispatcher::TEvGetConfigRequest(kinds, cache)));
    return *runtime.GrabEdgeEventRethrow<TEvConfigsDispatcher::TEvGetConfigResponse>(handle)->Config;
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
    Y_UNIT_TEST(TestSelfSubscription) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        auto serviceId = InitConfigsDispatcher(runtime);

        ui64 id;
        TDispatchOptions options;
        options.FinalEvents.emplace_back(CatchReplaceConfigResult(id), 1);
        runtime.DispatchEvents(options);

        CheckListConfigSubscriptions(runtime, Ydb::StatusIds::SUCCESS, 0, serviceId,
                                     id, runtime.GetNodeId(0), FQDNHostName(), TENANT1_1_NAME, "type1",
                                     0, serviceId,
                                     TVector<ui32>({(ui32)NKikimrConsole::TConfigItem::ConfigsDispatcherConfigItem}));
    }

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

        // Expect two responses from subscribers and one from dispatcher.
        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvConsole::EvConfigNotificationResponse, 3);
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

        // Expect response from subscriber and from dispatcher.
        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvConsole::EvConfigNotificationResponse, 2);
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

        // Expect response from unhold subscriber and from dispatcher.
        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvConsole::EvConfigNotificationResponse, 2);
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
        options.FinalEvents.emplace_back(TEvConsole::EvRemoveConfigSubscriptionResponse, 1);
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

        // Subscriber removal should cause config notification response.
        SetSubscriptions(runtime, s1, {});

        TDispatchOptions options2;
        options2.FinalEvents.emplace_back(TEvConsole::EvConfigNotificationResponse, 1);
        runtime.DispatchEvents(options2);
    }

    Y_UNIT_TEST(TestGetCachedConfig) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        TAutoPtr<IEventHandle> handle;
        InitConfigsDispatcher(runtime);

        ui64 nodeConfigRequests = 0;
        auto observer = [&nodeConfigRequests](TTestActorRuntimeBase&, TAutoPtr<IEventHandle> &ev) -> TTenantTestRuntime::EEventAction {
            switch (ev->GetTypeRewrite()) {
            case TEvConsole::EvGetNodeConfigRequest:
                ++nodeConfigRequests;
                break;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };

        ITEM_DOMAIN_LOG_1.MutableConfig()->MutableLogConfig()->SetClusterName("cluster1");
        ITEM_DOMAIN_LOG_2.MutableConfig()->MutableLogConfig()->SetClusterName("cluster2");

        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                       MakeAddAction(ITEM_DOMAIN_LOG_1));

        NKikimrConfig::TAppConfig config;
        config.MutableLogConfig()->SetClusterName("cluster1");

        runtime.SetObserverFunc(observer);

        // Config should be requested from CMS.
        auto config1 = GetConfig(runtime, {(ui32)NKikimrConsole::TConfigItem::LogConfigItem}, false);
        CheckEqualsIgnoringVersion(config, config1);
        UNIT_ASSERT(nodeConfigRequests > 0);

        // We didn't ask to cache, so config should still be requested from CMS.
        // This time ask to cache config.
        nodeConfigRequests = 0;
        auto config2 = GetConfig(runtime, {(ui32)NKikimrConsole::TConfigItem::LogConfigItem}, true);
        CheckEqualsIgnoringVersion(config, config2);
        UNIT_ASSERT(nodeConfigRequests > 0);

        // Make sure subscription is online by using it with another subscriber.
        AddSubscriber(runtime, {(ui32)NKikimrConsole::TConfigItem::LogConfigItem});
        runtime.GrabEdgeEventRethrow<TEvPrivate::TEvGotNotification>(handle);

        // This time we should get config with no requests to CMS.
        nodeConfigRequests = 0;
        auto config3 = GetConfig(runtime, {(ui32)NKikimrConsole::TConfigItem::LogConfigItem}, true);
        CheckEqualsIgnoringVersion(config, config3);
        UNIT_ASSERT_VALUES_EQUAL(nodeConfigRequests, 0);

        // Change config and expect dispatcher to process notification.
        SendConfigure(runtime, MakeAddAction(ITEM_DOMAIN_LOG_2));
        runtime.GrabEdgeEventRethrow<TEvPrivate::TEvGotNotification>(handle);

        // Now we should get new config with no requests to CMS.
        config.MutableLogConfig()->SetClusterName("cluster2");
        auto config4 = GetConfig(runtime, {(ui32)NKikimrConsole::TConfigItem::LogConfigItem}, true);
        CheckEqualsIgnoringVersion(config, config4);
        UNIT_ASSERT_VALUES_EQUAL(nodeConfigRequests, 0);
    }

    Y_UNIT_TEST(TestEmptyChangeCausesNoNotification) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        TAutoPtr<IEventHandle> handle;
        InitConfigsDispatcher(runtime);

        ui64 notifications = 0;
        TActorId subscriber;
        auto observer = [&notifications,&subscriber,recipient=runtime.Sender](TTestActorRuntimeBase&, TAutoPtr<IEventHandle> &ev) -> TTenantTestRuntime::EEventAction {
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
        options1.FinalEvents.emplace_back(TEvConsole::EvConfigNotificationResponse, 1);
        runtime.DispatchEvents(options1);
        UNIT_ASSERT_VALUES_EQUAL(notifications, 0);
    }
}

} // namespace NKikimr

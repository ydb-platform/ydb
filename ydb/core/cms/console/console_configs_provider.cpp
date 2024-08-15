#include "console_configs_provider.h"
#include "util.h"
#include "http.h"

#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/cms/console/validators/registry.h>
#include <ydb/core/mon/mon.h>

#include <ydb/library/actors/core/interconnect.h>

namespace NKikimr::NConsole {

namespace {

class TTabletConfigSender : public TActorBootstrapped<TTabletConfigSender> {
private:
    using TBase = TActorBootstrapped<TTabletConfigSender>;

    TSubscription::TPtr Subscription;
    TActorId OwnerId;
    TActorId Pipe;
    TSchedulerCookieHolder TimeoutTimerCookieHolder;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType()
    {
        return NKikimrServices::TActivity::CMS_CONFIGS_PROVIDER;
    }

    TTabletConfigSender(TSubscription::TPtr subscription, TActorId ownerId)
        : Subscription(subscription)
        , OwnerId(ownerId)
    {
    }

    void Die(const TActorContext &ctx) override
    {
        LOG_TRACE_S(ctx, NKikimrServices::CMS_CONFIGS,
                    "TTabletConfigSender(" << Subscription->Id << ") Die");

        if (Pipe)
            NTabletPipe::CloseClient(ctx, Pipe);
        TBase::Die(ctx);
    }

    void OnPipeDestroyed(const TActorContext &ctx)
    {
        if (Pipe) {
            NTabletPipe::CloseClient(ctx, Pipe);
            Pipe = TActorId();
        }

        SendNotifyRequest(ctx);
    }

    void OpenPipe(const TActorContext &ctx)
    {
        Y_ABORT_UNLESS(Subscription->Subscriber.TabletId);
        NTabletPipe::TClientConfig pipeConfig;
        pipeConfig.RetryPolicy = FastConnectRetryPolicy();
        auto pipe = NTabletPipe::CreateClient(ctx.SelfID, Subscription->Subscriber.TabletId, pipeConfig);
        Pipe = ctx.ExecutorThread.RegisterActor(pipe);
    }

    void SendNotifyRequest(const TActorContext &ctx)
    {
        if (!Pipe)
            OpenPipe(ctx);

        auto request = MakeHolder<TEvConsole::TEvConfigNotificationRequest>();
        request->Record.SetSubscriptionId(Subscription->Id);
        Subscription->CurrentConfigId.Serialize(*request->Record.MutableConfigId());
        request->Record.MutableConfig()->CopyFrom(Subscription->CurrentConfig);

        LOG_TRACE_S(ctx, NKikimrServices::CMS_CONFIGS,
                    "TTabletConfigSender(" << Subscription->Id << ") send TEvConfigNotificationRequest: "
                    << request->Record.ShortDebugString());

        NTabletPipe::SendData(ctx, Pipe, request.Release(), Subscription->Cookie);
    }

    void Bootstrap(const TActorContext &ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::CMS_CONFIGS,
                    "TTabletConfigSender(" << Subscription->Id << ") Bootstrap");
        Become(&TThis::StateWork);

        SendNotifyRequest(ctx);

        TimeoutTimerCookieHolder.Reset(ISchedulerCookie::Make2Way());
        auto event = new TConfigsProvider::TEvPrivate::TEvNotificationTimeout(Subscription);
        CreateLongTimer(ctx, TDuration::Minutes(10),
                        new IEventHandle(SelfId(), SelfId(), event),
                        AppData(ctx)->SystemPoolId,
                        TimeoutTimerCookieHolder.Get());
    }

    void Handle(TEvConsole::TEvConfigNotificationResponse::TPtr& ev, const TActorContext& ctx) {
        ctx.Send(ev->Forward(OwnerId));
        Die(ctx);
    }

    void Handle(TEvents::TEvPoisonPill::TPtr &/*ev*/, const TActorContext &ctx)
    {
        LOG_DEBUG_S(ctx, NKikimrServices::CMS_CONFIGS,
                    "TTabletConfigSender(" << Subscription->Id << ") die to poison pill");
        ctx.Send(OwnerId, new TConfigsProvider::TEvPrivate::TEvSenderDied(Subscription));
        Die(ctx);
    }

    void Handle(TConfigsProvider::TEvPrivate::TEvNotificationTimeout::TPtr &/*ev*/, const TActorContext &ctx)
    {
        LOG_DEBUG_S(ctx, NKikimrServices::CMS_CONFIGS,
                    "TTabletConfigSender(" << Subscription->Id << ") die to timeout");
        ctx.Send(OwnerId, new TConfigsProvider::TEvPrivate::TEvNotificationTimeout(Subscription));
        Die(ctx);
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::CMS_CONFIGS,
                    "TTabletConfigSender(" << Subscription->Id << ") connection "
                    << ((ev->Get()->Status == NKikimrProto::OK) ? "established" : "failed"));

        if (ev->Get()->Status != NKikimrProto::OK) {
            OnPipeDestroyed(ctx);
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& /*ev*/, const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::CMS_CONFIGS,
                    "TTabletConfigSender(" << Subscription->Id << ") TEvTabletPipe::TEvClientDestroyed");

        OnPipeDestroyed(ctx);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvConsole::TEvConfigNotificationResponse, Handle);
            HFunc(TEvents::TEvPoisonPill, Handle);
            HFunc(TConfigsProvider::TEvPrivate::TEvNotificationTimeout, Handle);
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);

        default:
            Y_ABORT("unexpected event type: %" PRIx32 " event: %s",
                   ev->GetTypeRewrite(), ev->ToString().data());
            break;
        }
    }
};

class TServiceConfigSender : public TActorBootstrapped<TServiceConfigSender> {
private:
    using TBase = TActorBootstrapped<TServiceConfigSender>;

    TSubscription::TPtr Subscription;
    TActorId OwnerId;
    TDuration RetryInterval;
    TSchedulerCookieHolder TimeoutTimerCookieHolder;
    bool ScheduledRetry;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType()
    {
        return NKikimrServices::TActivity::CMS_CONFIGS_PROVIDER;
    }

    TServiceConfigSender(TSubscription::TPtr subscription, TActorId ownerId)
        : Subscription(subscription)
        , OwnerId(ownerId)
        , RetryInterval(TDuration::Seconds(5))
        , ScheduledRetry(false)
    {
    }

    void Die(const TActorContext &ctx) override
    {
        LOG_TRACE_S(ctx, NKikimrServices::CMS_CONFIGS,
                    "TServiceConfigSender(" << Subscription->Id << ") Die");

        auto nodeId = Subscription->Subscriber.ServiceId.NodeId();
        ctx.Send(TActivationContext::InterconnectProxy(nodeId),
                 new TEvents::TEvUnsubscribe);
        TBase::Die(ctx);
    }

    void SendNotifyRequest(const TActorContext &ctx)
    {
        auto request = MakeHolder<TEvConsole::TEvConfigNotificationRequest>();
        request->Record.SetSubscriptionId(Subscription->Id);
        Subscription->CurrentConfigId.Serialize(*request->Record.MutableConfigId());
        request->Record.MutableConfig()->CopyFrom(Subscription->CurrentConfig);

        LOG_TRACE_S(ctx, NKikimrServices::CMS_CONFIGS,
                    "TServiceConfigSender(" << Subscription->Id << ") send TEvConfigNotificationRequest: "
                    << request->Record.ShortDebugString());

        ctx.Send(Subscription->Subscriber.ServiceId, request.Release(),
                 IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession,
                 Subscription->Cookie);

        ScheduledRetry = false;
    }

    void Bootstrap(const TActorContext &ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::CMS_CONFIGS,
                    "TServiceConfigSender(" << Subscription->Id << ") Bootstrap");
        Become(&TThis::StateWork);

        SendNotifyRequest(ctx);

        TimeoutTimerCookieHolder.Reset(ISchedulerCookie::Make2Way());
        auto event = new TConfigsProvider::TEvPrivate::TEvNotificationTimeout(Subscription);
        CreateLongTimer(ctx, TDuration::Minutes(10),
                        new IEventHandle(SelfId(), SelfId(), event),
                        AppData(ctx)->SystemPoolId,
                        TimeoutTimerCookieHolder.Get());
    }

    void Handle(TEvConsole::TEvConfigNotificationResponse::TPtr& ev, const TActorContext& ctx) {
        ctx.Send(ev->Forward(OwnerId));
        Die(ctx);
    }

    void Handle(TEvents::TEvPoisonPill::TPtr &/*ev*/, const TActorContext &ctx)
    {
        LOG_DEBUG_S(ctx, NKikimrServices::CMS_CONFIGS,
                    "TServiceConfigSender(" << Subscription->Id
                    << ") die to poison pill");
        ctx.Send(OwnerId, new TConfigsProvider::TEvPrivate::TEvSenderDied(Subscription));
        Die(ctx);
    }

    void Handle(TEvents::TEvUndelivered::TPtr &/*ev*/, const TActorContext &ctx)
    {
        RetryInterval += RetryInterval;
        RetryInterval = Min(RetryInterval, TDuration::Minutes(1));

        LOG_DEBUG_S(ctx, NKikimrServices::CMS_CONFIGS,
                    "TServiceConfigSender(" << Subscription->Id
                    << ") undelivered notification (retry in "
                    << RetryInterval.Seconds() << " seconds)");

        if (!ScheduledRetry) {
            ctx.Schedule(RetryInterval, new TEvents::TEvWakeup);
            ScheduledRetry = true;
        }
    }

    void Handle(TEvInterconnect::TEvNodeDisconnected::TPtr &/*ev*/, const TActorContext &ctx)
    {
        RetryInterval += RetryInterval;
        RetryInterval = Min(RetryInterval, TDuration::Minutes(1));

        LOG_DEBUG_S(ctx, NKikimrServices::CMS_CONFIGS,
                    "TServiceConfigSender(" << Subscription->Id
                    << ") disconnected (retry in "
                    << RetryInterval.Seconds() << " seconds)");

        if (!ScheduledRetry) {
            ctx.Schedule(RetryInterval, new TEvents::TEvWakeup);
            ScheduledRetry = true;
        }
    }

    void Handle(TConfigsProvider::TEvPrivate::TEvNotificationTimeout::TPtr &/*ev*/, const TActorContext &ctx)
    {
        LOG_DEBUG_S(ctx, NKikimrServices::CMS_CONFIGS,
                    "TServiceConfigSender(" << Subscription->Id << ") die to timeout");
        ctx.Send(OwnerId, new TConfigsProvider::TEvPrivate::TEvNotificationTimeout(Subscription));
        Die(ctx);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvConsole::TEvConfigNotificationResponse, Handle);
            HFunc(TEvents::TEvPoisonPill, Handle);
            HFunc(TEvents::TEvUndelivered, Handle);
            CFunc(TEvents::TSystem::Wakeup, SendNotifyRequest);
            HFunc(TEvInterconnect::TEvNodeDisconnected, Handle);
            HFunc(TConfigsProvider::TEvPrivate::TEvNotificationTimeout, Handle);
            IgnoreFunc(TEvInterconnect::TEvNodeConnected);

        default:
            Y_ABORT("unexpected event type: %" PRIx32 " event: %s",
                   ev->GetTypeRewrite(), ev->ToString().data());
            break;
        }
    }
};

class TSubscriptionClientSender : public TActorBootstrapped<TSubscriptionClientSender> {
    using TBase = TActorBootstrapped<TSubscriptionClientSender>;

private:
    TInMemorySubscription::TPtr Subscription;
    TActorId OwnerId;

    ui64 NextOrder;

public:
    TSubscriptionClientSender(TInMemorySubscription::TPtr subscription, const TActorId &ownerId)
        : Subscription(subscription)
        , OwnerId(ownerId)
        , NextOrder(1)
    {
    }

    void Bootstrap(const TActorContext &ctx) {
        LOG_TRACE_S(ctx, NKikimrServices::CMS_CONFIGS,
                    "TSubscriptionClientSender(" << Subscription->Subscriber.ToString() << ") send TEvConfigSubscriptionResponse");

        Send(Subscription->Subscriber, new TEvConsole::TEvConfigSubscriptionResponse(Subscription->Generation, Ydb::StatusIds::SUCCESS),
             IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession);
        Become(&TThis::StateWork);
    }

    STFUNC(StateWork) {
        TRACE_EVENT(NKikimrServices::CMS_CONFIGS);

        switch (ev->GetTypeRewrite()) {
            HFuncTraced(TEvConsole::TEvConfigSubscriptionNotification, Handle);
            HFuncTraced(TEvents::TEvPoisonPill, Handle);
            HFuncTraced(TEvents::TEvUndelivered, Handle);
            HFuncTraced(TEvInterconnect::TEvNodeDisconnected, Handle);
            IgnoreFunc(TEvInterconnect::TEvNodeConnected);

            default:
                Y_ABORT("unexpected event type: %" PRIx32 " event: %s",
                       ev->GetTypeRewrite(), ev->ToString().data());
                break;
        }
    }

    void Handle(TEvents::TEvPoisonPill::TPtr &/*ev*/, const TActorContext &ctx)
    {
        LOG_DEBUG_S(ctx, NKikimrServices::CMS_CONFIGS,
                    "TSubscriptionClientSender(" << Subscription->Subscriber.ToString() << ") received poison pill, "
                                                 << "will die.");
        Die(ctx);
    }

    void Handle(TEvents::TEvUndelivered::TPtr &/*ev*/, const TActorContext &ctx)
    {
        LOG_DEBUG_S(ctx, NKikimrServices::CMS_CONFIGS,
                    "TSubscriptionClientSender(" << Subscription->Subscriber.ToString() << ") received undelivered notification, "
                                            << "will disconnect.");

        Send(OwnerId, new TConfigsProvider::TEvPrivate::TEvWorkerDisconnected(Subscription));
        Die(ctx);
    }

    void Handle(TEvInterconnect::TEvNodeDisconnected::TPtr &/*ev*/, const TActorContext &ctx)
    {
        LOG_DEBUG_S(ctx, NKikimrServices::CMS_CONFIGS,
                    "TSubscriptionClientSender(" << Subscription->Subscriber.ToString() << ") received node disconnected notification, "
                                                 << "will disconnect.");

        Send(OwnerId, new TConfigsProvider::TEvPrivate::TEvWorkerDisconnected(Subscription));
        Die(ctx);
    }

    void Handle(NConsole::TEvConsole::TEvConfigSubscriptionNotification::TPtr &ev, const TActorContext& ctx)
    {
        TAutoPtr<NConsole::TEvConsole::TEvConfigSubscriptionNotification> notification = ev->Release();
        notification.Get()->Record.SetOrder(NextOrder++);

        LOG_TRACE_S(ctx, NKikimrServices::CMS_CONFIGS,
                    "TSubscriptionClientSender(" << Subscription->Subscriber.ToString() << ") send TEvConfigSubscriptionNotificationRequest: "
                                                 << notification.Get()->Record.ShortDebugString());

        Send(Subscription->Subscriber, notification.Release(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession);
    }

protected:
    void Die(const TActorContext &ctx) override {
        Send(TActivationContext::InterconnectProxy(Subscription->Subscriber.NodeId()), new TEvents::TEvUnsubscribe);

        TBase::Die(ctx);
    }
};

} // anonymous namespace

void TConfigsProvider::Bootstrap(const TActorContext &ctx)
{
    LOG_DEBUG(ctx, NKikimrServices::CMS_CONFIGS, "TConfigsProvider::Bootstrap");

    NActors::TMon *mon = AppData()->Mon;
    if (mon) {
        NMonitoring::TIndexMonPage *actorsMonPage = mon->RegisterIndexPage("actors", "Actors");
        mon->RegisterActorPage(actorsMonPage, "console_configs_provider", "Console Configs Provider", false, TlsActivationContext->ExecutorThread.ActorSystem, SelfId());
    }

    Become(&TThis::StateWork);
}

void TConfigsProvider::Die(const TActorContext &ctx)
{
    for (auto &it : InMemoryIndex.GetSubscriptions()) {
        Send(it.second->Subscriber, new TEvConsole::TEvConfigSubscriptionCanceled(it.second->Generation));
        Send(it.second->Worker, new TEvents::TEvPoisonPill());
    }

    TBase::Die(ctx);
}

void TConfigsProvider::ClearState()
{
    ConfigIndex.Clear();
    SubscriptionIndex.Clear();
}

void TConfigsProvider::ApplyConfigModifications(const TConfigModifications &modifications,
                                                const TActorContext &ctx)
{
    LOG_TRACE(ctx, NKikimrServices::CMS_CONFIGS, "TConfigsProvider: applying config midifications");

    TSubscriptionSet subscriptions;
    TInMemorySubscriptionSet inMemorySubscriptions;

    for (auto &[id, _] : modifications.RemovedItems) {
        auto item = ConfigIndex.GetItem(id);
        LOG_TRACE_S(ctx, NKikimrServices::CMS_CONFIGS, "TConfigsProvider: remove " << item->ToString());
        ConfigIndex.RemoveItem(id);
        SubscriptionIndex.CollectAffectedSubscriptions(item->UsageScope, item->Kind, subscriptions);
        InMemoryIndex.CollectAffectedSubscriptions(item->UsageScope, item->Kind, inMemorySubscriptions);
    }
    for (auto &pr : modifications.ModifiedItems) {
        auto item = ConfigIndex.GetItem(pr.first);

        LOG_TRACE_S(ctx, NKikimrServices::CMS_CONFIGS, "TConfigsProvider: remove modified " << item->ToString());
        ConfigIndex.RemoveItem(pr.first);

        LOG_TRACE_S(ctx, NKikimrServices::CMS_CONFIGS, "TConfigsProvider: add modified " << pr.second->ToString());
        ConfigIndex.AddItem(pr.second);

        SubscriptionIndex.CollectAffectedSubscriptions(item->UsageScope, item->Kind, subscriptions);
        InMemoryIndex.CollectAffectedSubscriptions(item->UsageScope, item->Kind, inMemorySubscriptions);
        if (item->UsageScope != pr.second->UsageScope) {
            SubscriptionIndex.CollectAffectedSubscriptions(pr.second->UsageScope, item->Kind, subscriptions);
            InMemoryIndex.CollectAffectedSubscriptions(pr.second->UsageScope, item->Kind, inMemorySubscriptions);
        }
    }
    for (auto item : modifications.AddedItems) {
        LOG_TRACE_S(ctx, NKikimrServices::CMS_CONFIGS, "TConfigsProvider: add new " << item->ToString());
        ConfigIndex.AddItem(item);
        SubscriptionIndex.CollectAffectedSubscriptions(item->UsageScope, item->Kind, subscriptions);
        InMemoryIndex.CollectAffectedSubscriptions(item->UsageScope, item->Kind, inMemorySubscriptions);
    }

    CheckSubscriptions(subscriptions, ctx);
    CheckSubscriptions(inMemorySubscriptions, ctx);
}

void TConfigsProvider::ApplySubscriptionModifications(const TSubscriptionModifications &modifications,
                                                      const TActorContext &ctx)
{
    LOG_TRACE(ctx, NKikimrServices::CMS_CONFIGS, "TConfigsProvider: applying subscription midifications");

    TSubscriptionSet subscriptions;

    for (auto &id : modifications.RemovedSubscriptions) {
        auto subscription = SubscriptionIndex.GetSubscription(id);
        LOG_TRACE_S(ctx, NKikimrServices::CMS_CONFIGS,
                    "TConfigsProvider: remove subscription " << subscription->ToString());
        if (subscription->Worker) {
            ctx.Send(subscription->Worker, new TEvents::TEvPoisonPill);
            subscription->Worker = TActorId();
        }
        SubscriptionIndex.RemoveSubscription(id);
    }
    for (auto &subscription : modifications.AddedSubscriptions) {
        LOG_TRACE_S(ctx, NKikimrServices::CMS_CONFIGS,
                    "TConfigsProvider: add subscription " << subscription->ToString());
        SubscriptionIndex.AddSubscription(subscription);
        subscriptions.insert(subscription);
    }
    for (auto &pr : modifications.ModifiedLastProvided) {
        LOG_TRACE_S(ctx, NKikimrServices::CMS_CONFIGS,
                    "TConfigsProvider: update last provided config for subscription"
                    << " id=" << pr.first
                    << " lastprovidedconfig=" << pr.second.ToString());
        auto subscription = SubscriptionIndex.GetSubscription(pr.first);
        subscription->LastProvidedConfig = pr.second;
        subscriptions.insert(subscription);
    }
    for (auto &pr : modifications.ModifiedCookies) {
        LOG_TRACE_S(ctx, NKikimrServices::CMS_CONFIGS,
                    "TConfigsProvider: update cookie for subscription"
                    << " id=" << pr.first
                    << " cookie=" << pr.second);
        auto subscription = SubscriptionIndex.GetSubscription(pr.first);
        subscription->Cookie = pr.second;
        if (subscription->Worker) {
            ctx.Send(subscription->Worker, new TEvents::TEvPoisonPill);
            subscription->Worker = TActorId();
        }
        subscriptions.insert(subscription);
    }

    CheckSubscriptions(subscriptions, ctx);
}

void TConfigsProvider::CheckAllSubscriptions(const TActorContext &ctx)
{
    TSubscriptionSet subscriptions;
    for (auto &pr : SubscriptionIndex.GetSubscriptions())
        subscriptions.insert(pr.second);
    CheckSubscriptions(subscriptions, ctx);
}

void TConfigsProvider::CheckSubscriptions(const TSubscriptionSet &subscriptions,
                                          const TActorContext &ctx)
{
    for (auto &subscription : subscriptions)
        CheckSubscription(subscription, ctx);
}

void TConfigsProvider::CheckSubscriptions(const TInMemorySubscriptionSet &subscriptions,
                                          const TActorContext &ctx)
{
    for (auto &subscription : subscriptions)
        CheckSubscription(subscription, ctx);
}

void TConfigsProvider::CheckSubscription(TSubscription::TPtr subscription,
                                         const TActorContext &ctx)
{
    LOG_TRACE_S(ctx, NKikimrServices::CMS_CONFIGS,
                "TConfigsProvider: check if update is required for subscription"
                << " id=" << subscription->Id);

    auto config = ConfigIndex.BuildConfig(subscription->NodeId, subscription->Host,
                                          subscription->Tenant, subscription->NodeType,
                                          subscription->ItemKinds);
    TConfigId configId;
    for (auto kind : subscription->ItemKinds) {
        auto it = config->ConfigItems.find(kind);
        if (it == config->ConfigItems.end())
            continue;

        for (auto &item : it->second)
            configId.ItemIds.push_back(std::make_pair(item->Id, item->Generation));
    }

    if (subscription->LastProvidedConfig == configId) {
        LOG_TRACE_S(ctx, NKikimrServices::CMS_CONFIGS,
                    "TConfigsProvider: no changes found for subscription"
                    << " id=" << subscription->Id);
        return;
    }

    if (configId != subscription->CurrentConfigId) {
        if (subscription->Worker) {
            LOG_TRACE_S(ctx, NKikimrServices::CMS_CONFIGS,
                        "TConfigsProvider: killing outdated worker for subscription"
                        << " id=" << subscription->Id);
            ctx.Send(subscription->Worker, new TEvents::TEvPoisonPill);
            return;
        }

        LOG_TRACE_S(ctx, NKikimrServices::CMS_CONFIGS,
                    "TConfigsProvider: new config found for subscription"
                    << " id=" << subscription->Id
                    << " configid=" << configId.ToString());

        subscription->CurrentConfigId = std::move(configId);
        subscription->CurrentConfig.Clear();
        config->ComputeConfig(subscription->CurrentConfig);
    }

    LOG_TRACE_S(ctx, NKikimrServices::CMS_CONFIGS,
                "TConfigsProvider: create new worker to update config for subscription"
                << " id=" << subscription->Id);

    IActor *worker;
    if (subscription->Subscriber.ServiceId)
        worker = new TServiceConfigSender(subscription, ctx.SelfID);
    else
        worker = new TTabletConfigSender(subscription, ctx.SelfID);
    subscription->Worker = ctx.RegisterWithSameMailbox(worker);
}

void TConfigsProvider::CheckSubscription(TInMemorySubscription::TPtr subscription,
                                         const TActorContext &ctx)
{
    LOG_TRACE_S(ctx, NKikimrServices::CMS_CONFIGS,
                "TConfigsProvider: check if update is required for volatile subscription"
                    << " " << subscription->Subscriber.ToString() << ":" << subscription->Generation);

    auto config = ConfigIndex.BuildConfig(subscription->NodeId, subscription->Host,
                                          subscription->Tenant, subscription->NodeType,
                                          subscription->ItemKinds);

    THashMap<ui32, TConfigId> currKindIdMap;
    THashMap<ui32, TConfigId> prevKindIdMap;

    for (auto kind : subscription->ItemKinds) {
        auto it = config->ConfigItems.find(kind);
        if (it != config->ConfigItems.end()) {
            auto &id = currKindIdMap[kind];
            for (auto &item : it->second) {
                if (item->MergeStrategy == NKikimrConsole::TConfigItem::OVERWRITE)
                    id.ItemIds.clear();
                id.ItemIds.push_back(std::make_pair(item->Id, item->Generation));
            }
        }
    }
    for (auto &item : subscription->LastProvided.GetItems()) {
        auto &id = prevKindIdMap[item.kind()];
        id.ItemIds.push_back(std::make_pair(item.GetId(), item.GetGeneration()));
    }

    THashSet<ui32> affectedKinds;
    NKikimrConfig::TConfigVersion version;

    for (auto &curr : currKindIdMap) {
        auto prev = prevKindIdMap.find(curr.first);

        if (prev == prevKindIdMap.end() || curr.second != prev->second) {
            // There are changes for the kind
            affectedKinds.insert(curr.first);
            for (auto &pr : curr.second.ItemIds) {
                auto itemId = version.AddItems();
                itemId->SetKind(curr.first);
                itemId->SetId(pr.first);
                itemId->SetGeneration(pr.second);
            }
        } else if (prev != prevKindIdMap.end()) {
            // There are no changes for the kind, lets just fill in current config ids
            for (auto &pr : curr.second.ItemIds) {
                auto itemId = version.AddItems();
                itemId->SetKind(curr.first);
                itemId->SetId(pr.first);
                itemId->SetGeneration(pr.second);
            }
        }

        if (prev != prevKindIdMap.end())
            prevKindIdMap.erase(prev);
    }

    for (auto &prev : prevKindIdMap) {
        // We already deleted all currently presented config kinds, so this one is definitely deleted
        affectedKinds.insert(prev.first);
    }

    bool yamlChanged = false;

    if (subscription->YamlConfigVersion != YamlConfigVersion) {
        yamlChanged = true;
    }

    yamlChanged |= VolatileYamlConfigHashes.size() != subscription->VolatileYamlConfigHashes.size();

    for (auto &[id, hash] : VolatileYamlConfigHashes) {
        if (auto it = subscription->VolatileYamlConfigHashes.find(id); it == subscription->VolatileYamlConfigHashes.end() || it->second != hash) {
            yamlChanged = true;
        }
    }

    if (affectedKinds.empty() && !yamlChanged && subscription->FirstUpdateSent) {
        LOG_TRACE_S(ctx, NKikimrServices::CMS_CONFIGS,
                    "TConfigsProvider: no changes found for subscription"
                        << " " << subscription->Subscriber.ToString() << ":" << subscription->Generation);
        return;
    }

    LOG_TRACE_S(ctx, NKikimrServices::CMS_CONFIGS,
                "TConfigsProvider: new config found for subscription"
                    << " " << subscription->Subscriber.ToString() << ":" << subscription->Generation
                    << " version=" << version.ShortDebugString());

    subscription->LastProvided.Swap(&version);

    NKikimrConfig::TAppConfig appConfig;
    config->ComputeConfig(affectedKinds, appConfig, true);

    auto request = MakeHolder<TEvConsole::TEvConfigSubscriptionNotification>(
            subscription->Generation,
            std::move(appConfig),
            affectedKinds);

    if (subscription->YamlConfigVersion != YamlConfigVersion) {
        subscription->YamlConfigVersion = YamlConfigVersion;
        request->Record.SetYamlConfig(YamlConfig);
    } else {
        request->Record.SetYamlConfigNotChanged(true);
    }

    for (auto &[id, hash] : VolatileYamlConfigHashes) {
        auto *volatileConfig = request->Record.AddVolatileConfigs();
        volatileConfig->SetId(id);
        auto hashes = subscription->VolatileYamlConfigHashes.size();
        Y_UNUSED(hashes);
        auto itt = subscription->VolatileYamlConfigHashes.find(id);
        if (itt != subscription->VolatileYamlConfigHashes.end()) {
            auto tmp = itt->second;
            Y_UNUSED(tmp);
        }
        if (auto it = subscription->VolatileYamlConfigHashes.find(id); it != subscription->VolatileYamlConfigHashes.end() && it->second == hash) {
            volatileConfig->SetNotChanged(true);
        } else {
            volatileConfig->SetConfig(VolatileYamlConfigs[id]);
        }
    }

    subscription->VolatileYamlConfigHashes = VolatileYamlConfigHashes;

    ctx.Send(subscription->Worker, request.Release());

    subscription->FirstUpdateSent = true;
}

void TConfigsProvider::DumpStateHTML(IOutputStream &os) const {
    HTML(os) {
        COLLAPSED_REF_CONTENT("yaml-config", "Yaml Config") {
            DIV_CLASS("tab-left") {
                TAG(TH5) {
                    os << "Presistent Config" << Endl;
                }
                PRE() {
                    os << YamlConfig;
                }
                for (auto &[id, config] : VolatileYamlConfigs) {
                    TAG(TH5) {
                        os << "Volatile Config #" << id << Endl;
                    }
                    PRE() {
                        os << config;
                    }
                }
            }
        }
        os << "<br/>" << Endl;
        COLLAPSED_REF_CONTENT("in-memory-subscription", "InMemorySubscriptions") {
            DIV_CLASS("tab-left") {
                PRE() {
                    for (auto &[_, s] : InMemoryIndex.GetSubscriptions()) {
                        os << "- Subscriber: " << s->Subscriber << Endl
                           << "  Generation: " << s->Generation << Endl
                           << "  NodeId: " << s->NodeId << Endl
                           << "  Host: " << s->Host << Endl
                           << "  Tenant: " << s->Tenant << Endl
                           << "  NodeType: " << s->NodeType << Endl
                           << "  ItemKinds: " << KindsToString(s->ItemKinds) << Endl
                           << "  ConfigVersions: " << s->LastProvided.ShortDebugString() << Endl
                           << "  ServeYaml: " << s->ServeYaml << Endl
                           << "  YamlApiVersion: " << s->YamlApiVersion << Endl
                           << "  YamlVersion: " << s->YamlConfigVersion << ".[";
                        bool first = true;
                        for (auto &[id, hash] : s->VolatileYamlConfigHashes) {
                            os << (first ? "" : ",") << id << "." << hash;
                            first = false;
                        }
                        os << "]" << Endl;
                    }
                }
            }
        }
    }
}

void TConfigsProvider::Handle(NMon::TEvHttpInfo::TPtr &ev)
{
    TStringStream str;
    str << NMonitoring::HTTPOKHTML;
    HTML(str) {
        NHttp::OutputStaticPart(str);
        DumpStateHTML(str);
    }

    Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str(), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
}

void TConfigsProvider::Handle(TEvConsole::TEvConfigSubscriptionRequest::TPtr &ev, const TActorContext &ctx)
{
    auto subscriber = ev->Sender;
    auto &rec = ev->Get()->Record;

    auto existing = InMemoryIndex.GetSubscription(subscriber);
    if (existing) {
        if (existing->Generation >= rec.GetGeneration()) {
            LOG_DEBUG_S(ctx, NKikimrServices::CMS_CONFIGS,
                        "TConfigsProvider received stale subscription request "
                        << subscriber.ToString() << ":" << rec.GetGeneration());
            return;
        }

        InMemoryIndex.RemoveSubscription(subscriber);
        Send(existing->Worker, new TEvents::TEvPoisonPill());
    }

    TInMemorySubscription::TPtr subscription = new TInMemorySubscription();

    subscription->Subscriber = subscriber;
    subscription->Generation = rec.GetGeneration();

    subscription->NodeId = rec.GetOptions().GetNodeId();
    subscription->Host = rec.GetOptions().GetHost();
    subscription->Tenant = rec.GetOptions().GetTenant();
    subscription->NodeType = rec.GetOptions().GetNodeType();

    subscription->ItemKinds.insert(rec.GetConfigItemKinds().begin(), rec.GetConfigItemKinds().end());
    subscription->LastProvided.CopyFrom(rec.GetKnownVersion());

    if (rec.HasServeYaml() && rec.GetServeYaml() && rec.HasYamlApiVersion() && rec.GetYamlApiVersion() == 1) {
        subscription->ServeYaml = rec.GetServeYaml();
        subscription->YamlApiVersion = rec.GetYamlApiVersion();
        subscription->YamlConfigVersion = rec.GetYamlVersion();
        for (auto &volatileConfigVersion : rec.GetVolatileYamlVersion()) {
            subscription->VolatileYamlConfigHashes[volatileConfigVersion.GetId()] = volatileConfigVersion.GetHash();
        }
    }

    InMemoryIndex.AddSubscription(subscription);

    LOG_DEBUG_S(ctx, NKikimrServices::CMS_CONFIGS,
                "TConfigsProvider registered new subscription "
                << subscriber.ToString() << ":" << rec.GetGeneration());

    subscription->Worker = RegisterWithSameMailbox(new TSubscriptionClientSender(subscription, SelfId()));

    CheckSubscription(subscription, ctx);
}

void TConfigsProvider::Handle(TEvConsole::TEvConfigSubscriptionCanceled::TPtr &ev, const TActorContext &ctx)
{
    auto subscriber = ev->Sender;
    auto &rec = ev->Get()->Record;

    auto subscription = InMemoryIndex.GetSubscription(subscriber);
    if (!subscription || subscription->Generation > rec.GetGeneration()) {
        LOG_DEBUG_S(ctx, NKikimrServices::CMS_CONFIGS,
                    "TConfigsProvider received stale subscription canceled request "
                    << subscriber.ToString() << ":" << rec.GetGeneration());
        return;
    }

    Y_ABORT_UNLESS(subscription->Worker);

    InMemoryIndex.RemoveSubscription(subscriber);
    Send(subscription->Worker, new TEvents::TEvPoisonPill());
}

void TConfigsProvider::Handle(TEvPrivate::TEvWorkerDisconnected::TPtr &ev, const TActorContext &ctx)
{
    auto subscription = ev->Get()->Subscription;
    auto existing = InMemoryIndex.GetSubscription(subscription->Subscriber);
    if (existing == subscription) {
        InMemoryIndex.RemoveSubscription(subscription->Subscriber);
        Send(subscription->Subscriber, new TEvConsole::TEvConfigSubscriptionCanceled(subscription->Generation));

        LOG_DEBUG_S(ctx, NKikimrServices::CMS_CONFIGS, "TConfigsProvider removed subscription "
                    << subscription->Subscriber<< ":" << subscription->Generation << " (subscription worker died)");
    }
}

void TConfigsProvider::Handle(TEvConsole::TEvCheckConfigUpdatesRequest::TPtr &ev, const TActorContext &ctx)
{
    auto &rec = ev->Get()->Record;

    auto response = MakeHolder<TEvConsole::TEvCheckConfigUpdatesResponse>();
    response->Record.MutableStatus()->SetCode(Ydb::StatusIds::SUCCESS);

    THashSet<ui64> base;
    for (auto &baseItem : rec.GetBaseItemIds()) {
        auto item = ConfigIndex.GetItem(baseItem.GetId());
        if (item && item->Generation != baseItem.GetGeneration()) {
            auto &entry = *response->Record.AddUpdatedItems();
            entry.SetId(item->Id);
            entry.SetGeneration(item->Generation);
        } else if (!item) {
            response->Record.AddRemovedItems()->CopyFrom(baseItem);
        }

        base.insert(baseItem.GetId());
    }

    for (auto &pr : ConfigIndex.GetConfigItems()) {
        if (!base.contains(pr.first)) {
            auto &entry = *response->Record.AddAddedItems();
            entry.SetId(pr.first);
            entry.SetGeneration(pr.second->Generation);
        }
    }

    LOG_TRACE_S(ctx, NKikimrServices::CMS_CONFIGS,
                "Send TEvCheckConfigUpdatesResponse: " << response->Record.ShortDebugString());

    ctx.Send(ev->Sender, response.Release(), 0, ev->Cookie);
}

void TConfigsProvider::Handle(TEvConsole::TEvGetConfigItemsRequest::TPtr &ev, const TActorContext &ctx)
{
    auto &rec = ev->Get()->Record;

    auto response = MakeHolder<TEvConsole::TEvGetConfigItemsResponse>();
    response->Record.MutableStatus()->SetCode(Ydb::StatusIds::SUCCESS);

    THashSet<ui32> kinds;
    for (auto &kind : rec.GetItemKinds())
        kinds.insert(kind);

    TConfigItems items;
    bool hasFilter = false;
    if (rec.ItemIdsSize()) {
        for (ui64 id : rec.GetItemIds()) {
            auto item = ConfigIndex.GetItem(id);
            if (item && (kinds.empty() || kinds.contains(item->Kind)))
                items.insert(item);
        }
        hasFilter = true;
    }
    if (rec.HasNodeFilter()) {
        if (rec.GetNodeFilter().NodesSize()) {
            for (ui32 nodeId : rec.GetNodeFilter().GetNodes())
                ConfigIndex.CollectItemsByNodeId(nodeId, kinds, items);
        } else {
            ConfigIndex.CollectItemsWithNodeIdScope(kinds, items);
        }
        hasFilter = true;
    }
    if (rec.HasHostFilter()) {
        if (rec.GetHostFilter().HostsSize()) {
            for (const TString &host : rec.GetHostFilter().GetHosts())
                ConfigIndex.CollectItemsByHost(host, kinds, items);
        } else {
            ConfigIndex.CollectItemsWithHostScope(kinds, items);
        }
        hasFilter = true;
    }
    if (rec.HasTenantFilter()) {
        if (rec.GetTenantFilter().TenantsSize()) {
            for (const TString &tenant : rec.GetTenantFilter().GetTenants())
                ConfigIndex.CollectItemsByTenant(tenant, kinds, items);
        } else {
            ConfigIndex.CollectItemsWithTenantScope(kinds, items);
        }
        hasFilter = true;
    }
    if (rec.HasNodeTypeFilter()) {
        if (rec.GetNodeTypeFilter().NodeTypesSize()) {
            for (const TString &nodeType : rec.GetNodeTypeFilter().GetNodeTypes())
                ConfigIndex.CollectItemsByNodeType(nodeType, kinds, items);
        } else {
            ConfigIndex.CollectItemsWithNodeTypeScope(kinds, items);
        }
        hasFilter = true;
    }
    if (rec.HasTenantAndNodeTypeFilter()) {
        if (rec.GetTenantAndNodeTypeFilter().TenantAndNodeTypesSize()) {
            for (auto &filter : rec.GetTenantAndNodeTypeFilter().GetTenantAndNodeTypes())
                ConfigIndex.CollectItemsByTenantAndNodeType(filter.GetTenant(),
                                                            filter.GetNodeType(),
                                                            kinds, items);
        } else {
            ConfigIndex.CollectItemsWithTenantAndNodeTypeScope(kinds, items);
        }
        hasFilter = true;
    }
    if (rec.UsageScopesSize()) {
        for (auto &usageScope : rec.GetUsageScopes())
            ConfigIndex.CollectItemsByScope(usageScope, kinds, items);
        hasFilter = true;
    }
    if (rec.HasCookieFilter()) {
        for (auto &cookie : rec.GetCookieFilter().GetCookies())
            ConfigIndex.CollectItemsByCookie(cookie, kinds, items);
        hasFilter = true;
    }
    if (!hasFilter) {
        for (auto &pr : ConfigIndex.GetConfigItems())
            if (kinds.empty() || kinds.contains(pr.second->Kind))
                items.insert(pr.second);
    }

    for (auto &item : items)
        item->Serialize(*response->Record.AddConfigItems());

    LOG_TRACE_S(ctx, NKikimrServices::CMS_CONFIGS,
                "Send TEvGetConfigItemsResponse: " << response->Record.ShortDebugString());

    ctx.Send(ev->Sender, response.Release(), 0, ev->Cookie);
}

void TConfigsProvider::Handle(TEvConsole::TEvConfigNotificationResponse::TPtr &ev, const TActorContext &ctx)
{
    auto &rec = ev->Get()->Record;
    auto subscription = SubscriptionIndex.GetSubscription(rec.GetSubscriptionId());
    // Subscription was removed
    if (!subscription) {
        LOG_DEBUG_S(ctx, NKikimrServices::CMS_CONFIGS,
                    "Config notification response for missing subscription id="
                    << rec.GetSubscriptionId());
        return;
    }
    // Service was restarted.
    if (ev->Cookie != subscription->Cookie) {
        LOG_DEBUG_S(ctx, NKikimrServices::CMS_CONFIGS,
                    "Config notification response cookie mismatch for"
                    << " subscription id=" << rec.GetSubscriptionId());
        Y_ABORT_UNLESS(subscription->Subscriber.ServiceId);
        return;
    }
    // Actually it's possible cookie was changed in configs manager
    // and update is on its way. We ignore it here and update last
    // provided config anyway because cookie update always come with
    // last provided update.
    subscription->LastProvidedConfig.Load(rec.GetConfigId());
    subscription->Worker = TActorId();

    ctx.Send(ev->Forward(ConfigsManager));
}

void TConfigsProvider::Handle(TEvConsole::TEvGetConfigSubscriptionRequest::TPtr &ev, const TActorContext &ctx)
{
    ui64 id = ev->Get()->Record.GetSubscriptionId();
    auto resp = MakeHolder<TEvConsole::TEvGetConfigSubscriptionResponse>();
    auto subscription = SubscriptionIndex.GetSubscription(id);
    if (subscription) {
        resp->Record.MutableStatus()->SetCode(Ydb::StatusIds::SUCCESS);
        subscription->Serialize(*resp->Record.MutableSubscription());
    } else {
        resp->Record.MutableStatus()->SetCode(Ydb::StatusIds::NOT_FOUND);
        resp->Record.MutableSubscription()->SetId(id);
    }

    LOG_TRACE_S(ctx, NKikimrServices::CMS_CONFIGS,
                "Send TEvGetConfigSubscriptionResponse: " << resp->Record.ShortDebugString());

    ctx.Send(ev->Sender, resp.Release(), 0, ev->Cookie);
}

void TConfigsProvider::Handle(TEvConsole::TEvGetNodeConfigItemsRequest::TPtr &ev, const TActorContext &ctx)
{
    auto &rec = ev->Get()->Record;

    auto response = MakeHolder<TEvConsole::TEvGetNodeConfigItemsResponse>();
    response->Record.MutableStatus()->SetCode(Ydb::StatusIds::SUCCESS);

    THashSet<ui32> kinds;
    for (auto &kind : rec.GetItemKinds())
        kinds.insert(kind);

    TConfigItems items;
    ConfigIndex.CollectItemsForNode(rec.GetNode(), kinds, items);

    for (auto &item : items)
        item->Serialize(*response->Record.AddConfigItems());

    LOG_TRACE_S(ctx, NKikimrServices::CMS_CONFIGS,
                "Send TEvGetNodeConfigItemsResponse: " << response->Record.ShortDebugString());

    ctx.Send(ev->Sender, response.Release(), 0, ev->Cookie);
}

void TConfigsProvider::Handle(TEvConsole::TEvGetNodeConfigRequest::TPtr &ev, const TActorContext &ctx)
{
    auto &rec = ev->Get()->Record;

    auto response = MakeHolder<TEvConsole::TEvGetNodeConfigResponse>();
    response->Record.MutableStatus()->SetCode(Ydb::StatusIds::SUCCESS);

    THashSet<ui32> kinds;
    for (auto &kind : rec.GetItemKinds())
        kinds.insert(kind);

    auto config = ConfigIndex.GetNodeConfig(rec.GetNode(), kinds);
    config->ComputeConfig(*response->Record.MutableConfig(), true);

    if (Config.EnableValidationOnNodeConfigRequest) {
        auto registry = TValidatorsRegistry::Instance();
        TVector<Ydb::Issue::IssueMessage> issues;
        if (!registry->CheckConfig({}, response->Record.GetConfig(), issues)) {
            response->Record.ClearConfig();
            response->Record.MutableStatus()->SetCode(Ydb::StatusIds::PRECONDITION_FAILED);
            for (auto &issue : issues) {
                if (issue.severity() == NYql::TSeverityIds::S_ERROR
                    || issue.severity() == NYql::TSeverityIds::S_FATAL) {
                    response->Record.MutableStatus()
                        ->SetReason("Config validation failure: " + issue.message());
                    break;
                }
            }
        }
    }

    LOG_TRACE_S(ctx, NKikimrServices::CMS_CONFIGS,
                "Send TEvGetNodeConfigResponse: " << response->Record.ShortDebugString());

    if (rec.HasServeYaml() && rec.GetServeYaml() && rec.HasYamlApiVersion() && rec.GetYamlApiVersion() == 1) {
        response->Record.SetYamlConfig(YamlConfig);

        for (auto &[id, config] : VolatileYamlConfigs) {
            auto &item = *response->Record.AddVolatileConfigs();
            item.SetId(id);
            item.SetConfig(config);
        }
    }

    ctx.Send(ev->Sender, response.Release(), 0, ev->Cookie);
}

void TConfigsProvider::Handle(TEvConsole::TEvListConfigSubscriptionsRequest::TPtr &ev, const TActorContext &ctx)
{
    auto &rec = ev->Get()->Record;

    auto response = MakeHolder<TEvConsole::TEvListConfigSubscriptionsResponse>();
    response->Record.MutableStatus()->SetCode(Ydb::StatusIds::SUCCESS);

    if (rec.HasSubscriber()) {
        TSubscriberId subscriber(rec.GetSubscriber());
        auto &subscriptions = SubscriptionIndex.GetSubscriptions(subscriber);
        for (auto subscription : subscriptions)
                subscription->Serialize(*response->Record.AddSubscriptions());
    } else {
        for (auto &pr : SubscriptionIndex.GetSubscriptions())
            pr.second->Serialize(*response->Record.AddSubscriptions());
    }

    LOG_TRACE_S(ctx, NKikimrServices::CMS_CONFIGS,
                "Send TEvListConfigSubscriptionsResponse: " << response->Record.ShortDebugString());

    ctx.Send(ev->Sender, response.Release(), 0, ev->Cookie);
}

void TConfigsProvider::Handle(TEvPrivate::TEvNotificationTimeout::TPtr &ev, const TActorContext &ctx)
{
    auto subscription = ev->Get()->Subscription;

    LOG_ERROR_S(ctx, NKikimrServices::CMS_CONFIGS,
                "Couldn't deliver config notification for subscription "
                << subscription->ToString() );

    // Subscription was removed
    if (!SubscriptionIndex.GetSubscription(subscription->Id)) {
        LOG_DEBUG_S(ctx, NKikimrServices::CMS_CONFIGS,
                    "Config notification timeout for missing subscription id="
                    << subscription->Id);
        return;
    }
    // Worker has changed.
    if (ev->Sender != subscription->Worker) {
        LOG_ERROR_S(ctx, NKikimrServices::CMS_CONFIGS,
                    "Config notification timeout from unexpected worker for"
                    << " subscription id=" << subscription->Id);
        return;
    }
    subscription->Worker = TActorId();
    CheckSubscription(subscription, ctx);
}

void TConfigsProvider::Handle(TEvPrivate::TEvSenderDied::TPtr &ev, const TActorContext &ctx)
{
    auto subscription = ev->Get()->Subscription;
    // Subscription was removed
    if (!SubscriptionIndex.GetSubscription(subscription->Id)) {
        LOG_DEBUG_S(ctx, NKikimrServices::CMS_CONFIGS,
                    "Config sender died for missing subscription id="
                    << subscription->Id);
        return;
    }
    // Worker has changed.
    if (ev->Sender != subscription->Worker) {
        LOG_ERROR_S(ctx, NKikimrServices::CMS_CONFIGS,
                    "Unexpected config sender died for"
                    << " subscription id=" << subscription->Id);
        return;
    }
    subscription->Worker = TActorId();
    CheckSubscription(subscription, ctx);
}

void TConfigsProvider::Handle(TEvPrivate::TEvSetConfig::TPtr &ev, const TActorContext &ctx)
{
    Y_UNUSED(ctx);
    Config = std::move(ev->Get()->Config);
}

void TConfigsProvider::Handle(TEvPrivate::TEvSetConfigs::TPtr &ev, const TActorContext &ctx)
{
    Y_UNUSED(ctx);
    Y_ABORT_UNLESS(ConfigIndex.IsEmpty());
    for (auto &pr : ev->Get()->ConfigItems)
        ConfigIndex.AddItem(pr.second);
    CheckAllSubscriptions(ctx);
}

void TConfigsProvider::Handle(TEvPrivate::TEvSetSubscriptions::TPtr &ev, const TActorContext &ctx)
{
    Y_UNUSED(ctx);
    Y_ABORT_UNLESS(SubscriptionIndex.IsEmpty());
    for (auto &pr : ev->Get()->Subscriptions)
        SubscriptionIndex.AddSubscription(pr.second);
    CheckAllSubscriptions(ctx);
}

void TConfigsProvider::Handle(TEvPrivate::TEvUpdateConfigs::TPtr &ev, const TActorContext &ctx)
{
    auto &event = ev->Get()->Event;
    if (event) {
        LOG_TRACE_S(ctx, NKikimrServices::CMS_CONFIGS,
                    "TConfigsProvider send: " << ev->ToString());
        ctx.Send(event.Release());
    }

    ApplyConfigModifications(ev->Get()->Modifications, ctx);
}

void TConfigsProvider::Handle(TEvPrivate::TEvUpdateSubscriptions::TPtr &ev, const TActorContext &ctx)
{
    auto &event = ev->Get()->Event;
    if (event) {
        LOG_TRACE_S(ctx, NKikimrServices::CMS_CONFIGS,
                    "TConfigsProvider send: " << ev->ToString());
        ctx.Send(event.Release());
    }

    ApplySubscriptionModifications(ev->Get()->Modifications, ctx);
}

void TConfigsProvider::Handle(TEvPrivate::TEvUpdateYamlConfig::TPtr &ev, const TActorContext &ctx) {
    YamlConfig = ev->Get()->YamlConfig;
    VolatileYamlConfigs.clear();

    YamlConfigVersion = NYamlConfig::GetVersion(YamlConfig);
    VolatileYamlConfigHashes.clear();
    for (auto& [id, config] : ev->Get()->VolatileYamlConfigs) {
        auto doc = NFyaml::TDocument::Parse(config);
        // we strip it to provide old format for config dispatcher
        auto node = doc.Root().Map().at("selector_config");
        TString strippedConfig = "\n" + config.substr(node.BeginMark().InputPos, node.EndMark().InputPos - node.BeginMark().InputPos) + "\n";
        VolatileYamlConfigs[id] = strippedConfig;
        VolatileYamlConfigHashes[id] = THash<TString>()(strippedConfig);
    }

    for (auto &[_, subscription] : InMemoryIndex.GetSubscriptions()) {
        if (subscription->ServeYaml) {
            auto request = MakeHolder<TEvConsole::TEvConfigSubscriptionNotification>(
                    subscription->Generation,
                    NKikimrConfig::TAppConfig{},
                    THashSet<ui32>{});

            if (subscription->YamlConfigVersion != YamlConfigVersion) {
                subscription->YamlConfigVersion = YamlConfigVersion;
                request->Record.SetYamlConfig(YamlConfig);
            } else {
                request->Record.SetYamlConfigNotChanged(true);
            }

            for (auto &[id, hash] : VolatileYamlConfigHashes) {
                auto *volatileConfig = request->Record.AddVolatileConfigs();
                volatileConfig->SetId(id);
                if (auto it = subscription->VolatileYamlConfigHashes.find(id); it != subscription->VolatileYamlConfigHashes.end() && it->second == hash) {
                    volatileConfig->SetNotChanged(true);
                } else {
                    volatileConfig->SetConfig(VolatileYamlConfigs[id]);
                }
            }

            subscription->VolatileYamlConfigHashes = VolatileYamlConfigHashes;

            ctx.Send(subscription->Worker, request.Release());
        }
    }
}

} // namespace NKikimr::NConsole

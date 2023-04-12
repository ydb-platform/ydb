#include "config_helpers.h"
#include "config_index.h"
#include "configs_dispatcher.h"
#include "console.h"
#include "http.h"

#include <ydb/core/mind/tenant_pool.h>
#include <ydb/core/mon/mon.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/mon.h>

#include <util/generic/bitmap.h>
#include <util/generic/ptr.h>
#include <util/string/join.h>

#if defined BLOG_D || defined BLOG_I || defined BLOG_ERROR || defined BLOG_TRACE
#error log macro definition clash
#endif

#define BLOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::CONFIGS_DISPATCHER, stream)
#define BLOG_N(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::CONFIGS_DISPATCHER, stream)
#define BLOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::CONFIGS_DISPATCHER, stream)
#define BLOG_W(stream) LOG_WARN_S(*TlsActivationContext, NKikimrServices::CONFIGS_DISPATCHER, stream)
#define BLOG_ERROR(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::CONFIGS_DISPATCHER, stream)
#define BLOG_TRACE(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::CONFIGS_DISPATCHER, stream)

namespace NKikimr::NConsole {

namespace {

class TConfigsDispatcher : public TActorBootstrapped<TConfigsDispatcher> {
private:
    using TBase = TActorBootstrapped<TConfigsDispatcher>;

    struct TConfig {
        TConfigId ConfigId;
        NKikimrConfig::TAppConfig Config;
    };

    /**
     * Structure to describe configs subscription shared by multiple
     * dispatcher subscribers.
     */
    struct TSubscription : public TThrRefBase {
        using TPtr = TIntrusivePtr<TSubscription>;

        // ID of corresponding subscription in CMS. Zero value means
        // we haven't received subscription confirmation from CMS yet.
        ui64 SubscriptionId = 0;
        TDynBitMap Kinds;
        THashSet<TActorId> Subscribers;
        // Set to true if there were no config update notifications
        // processed for this subscription.
        bool FirstUpdate = true;
        // Last config which was delivered to all subscribers.
        TConfig CurrentConfig;
        // Config update which is currently delivered to subscribers.
        TEvConsole::TEvConfigNotificationRequest::TPtr UpdateInProcess;
        // Subscribers who didn't respond yet to the latest config update.
        THashSet<TActorId> SubscribersToUpdate;
    };

    /**
     * Structure to describe subscribers. Subscriber can have multiple
     * subscriptions but we allow only single subscription per external
     * client. The only client who can have multiple subscriptions is
     * dispatcher itself.
     */
    struct TSubscriber : public TThrRefBase {
        using TPtr = TIntrusivePtr<TSubscriber>;

        TActorId Subscriber;
        THashSet<TSubscription::TPtr> Subscriptions;
        TConfigId CurrentConfigId;
    };

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType()
    {
        return NKikimrServices::TActivity::CONFIGS_DISPATCHER_ACTOR;
    }

    TConfigsDispatcher(const NKikimrConfig::TAppConfig &config, const TMap<TString, TString> &labels);

    void Bootstrap();

    void EnqueueEvent(TAutoPtr<IEventHandle> &ev);
    void ProcessEnqueuedEvents();

    TDynBitMap KindsToBitMap(const TVector<ui32> &kinds) const;
    TString KindsToString(const TDynBitMap &kinds) const;
    TVector<ui32> KindsToVector(const TDynBitMap &kinds) const;

    /**
     * Overwrite specified protobuf fields with values from
     * another protobuf. It's assumed that source config doesn't
     * have fields not listed in kinds.
     */
    void ReplaceConfigItems(const NKikimrConfig::TAppConfig &from, NKikimrConfig::TAppConfig &to, const TDynBitMap &kinds) const;
    bool CompareConfigs(const NKikimrConfig::TAppConfig &lhs, const NKikimrConfig::TAppConfig &rhs);

    TSubscription::TPtr FindSubscription(ui64 id);
    TSubscription::TPtr FindSubscription(const TDynBitMap &kinds);

    TSubscriber::TPtr FindSubscriber(TActorId aid);

    void SendNotificationResponse(TEvConsole::TEvConfigNotificationRequest::TPtr &ev);

    void MaybeSendNotificationResponse(TSubscription::TPtr subscription);

    void CreateSubscriberActor(ui32 kind, bool replace);
    void CreateSubscriberActor(const TDynBitMap &kinds, bool replace);
    /**
     * Send config notification to a subscriber. Called for subscriptions
     * having config update being processed.
     */

    void SendUpdateToSubscriber(TSubscription::TPtr subscription, TActorId subscriber);
    /**
     * Remove subscriber and all his subscriptions.
     */
    void RemoveSubscriber(TSubscriber::TPtr subscriber);

    /**
     * Remove subscription from own data and CMS. It should be called
     * for confirmed CMS subscriptions which have no more local
     * subscribers.
     */
    void RemoveSubscription(TSubscription::TPtr subscription);

    /**
     * Create subscription for subscriber. If subscription with similar
     * config kinds already exists then just re-use it. Otherwise
     * create a new one. If existing subscription has some config received
     * then deliver it to the new subscriber.
     */
    void AddSubscription(TActorId subscriber, const TDynBitMap &kinds, bool replace);

    /**
     * This is called on start and on tenant change to clean up old config
     * subscriptions. It also adds subscription for own config.
     */
    void CleanUpSubscriptions();
    /**
     * Process successfull subscription registration in CMS. Send
     * corresponsing notifications to subscribers. If no more subscribers
     * left for this subscription then remove it.
     */

    void ProcessAddedSubscription(TSubscription::TPtr subscription, ui64 id);

    /**
     * This method is used to process notifications sent to self.
     */
    void ProcessLocalCacheUpdate(TEvConsole::TEvConfigNotificationRequest::TPtr &ev);

    void Handle(NMon::TEvHttpInfo::TPtr &ev);
    void Handle(TEvConfigsDispatcher::TEvGetConfigRequest::TPtr &ev);
    void Handle(TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest::TPtr &ev);
    void Handle(TEvConsole::TEvAddConfigSubscriptionResponse::TPtr &ev);
    void Handle(TEvConsole::TEvConfigNotificationResponse::TPtr &ev);
    void Handle(TEvConsole::TEvConfigNotificationRequest::TPtr &ev);
    void Handle(TEvConsole::TEvGetNodeConfigResponse::TPtr &ev);
    void Handle(TEvConsole::TEvReplaceConfigSubscriptionsResponse::TPtr &ev);
    void Handle(TEvTenantPool::TEvTenantPoolStatus::TPtr &ev);

    /**
     * Initial state when we just get status from tenant pool to collect assigned
     * tenants. It's possible we start before tenant pool and therefore might have
     * to retry request.
     */
    STATEFN(StateInit) {
        TRACE_EVENT(NKikimrServices::CONFIGS_DISPATCHER);
        switch (ev->GetTypeRewrite()) {
            hFuncTraced(NMon::TEvHttpInfo, Handle);
            hFuncTraced(TEvTenantPool::TEvTenantPoolStatus, Handle);

        default:
            EnqueueEvent(ev);
            break;
        }
    }

    /**
     * In this state we remove all old service subscriptions and install a new
     * one for own config.
     */
    STATEFN(StateConfigure) {
        TRACE_EVENT(NKikimrServices::CONFIGS_DISPATCHER);
        switch (ev->GetTypeRewrite()) {
            hFuncTraced(NMon::TEvHttpInfo, Handle);
            hFuncTraced(TEvConsole::TEvReplaceConfigSubscriptionsResponse, Handle);

        default:
            EnqueueEvent(ev);
            break;
        }
    }

    /**
     * Primary state for subscriptions and notifications processing.
     */
    STATEFN(StateWork) {
        TRACE_EVENT(NKikimrServices::CONFIGS_DISPATCHER);
        switch (ev->GetTypeRewrite()) {
            hFuncTraced(NMon::TEvHttpInfo, Handle);
            hFuncTraced(TEvConfigsDispatcher::TEvGetConfigRequest, Handle);
            hFuncTraced(TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest, Handle);
            hFuncTraced(TEvConsole::TEvAddConfigSubscriptionResponse, Handle);
            hFuncTraced(TEvConsole::TEvConfigNotificationResponse, Handle);
            hFuncTraced(TEvConsole::TEvConfigNotificationRequest, Handle);
            hFuncTraced(TEvConsole::TEvGetNodeConfigResponse, Handle);
            hFuncTraced(TEvTenantPool::TEvTenantPoolStatus, Handle);
            IgnoreFunc(TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse);

        default:
            Y_FAIL("unexpected event type: %" PRIx32 " event: %s",
                   ev->GetTypeRewrite(), ev->ToString().data());
            break;
        }
    }

private:
    TMap<TString, TString> Labels;
    TDeque<TAutoPtr<IEventHandle>> EventsQueue;
    NKikimrConfig::TAppConfig InitialConfig;
    NKikimrConfig::TAppConfig CurrentConfig;
    THashSet<TSubscription::TPtr> Subscriptions;
    THashMap<ui64, TSubscription::TPtr> SubscriptionsById;
    THashMap<TDynBitMap, TSubscription::TPtr> SubscriptionsByKinds;
    THashMap<TActorId, TSubscriber::TPtr> Subscribers;

    // Messages that had an unknown subscription id at the time they are received
    THashMap<ui64, TEvConsole::TEvConfigNotificationRequest::TPtr> OutOfOrderConfigNotifications;

    // Cookies are used to tie CMS requests to kinds they were generated for.
    THashMap<ui64, TDynBitMap> RequestCookies;
    ui64 NextRequestCookie;
    THashSet<TString> CurrentTenants;

    // Structures to process config requests.
    THashMap<ui64, THolder<IEventHandle>> ConfigRequests;
    THashMap<TDynBitMap, std::shared_ptr<NKikimrConfig::TAppConfig>> ConfigsCache;
};

TConfigsDispatcher::TConfigsDispatcher(const NKikimrConfig::TAppConfig &config, const TMap<TString, TString> &labels)
    : Labels(labels)
    , InitialConfig(config)
    , CurrentConfig(config)
    , NextRequestCookie(Now().GetValue())
{
}

void TConfigsDispatcher::Bootstrap()
{
    BLOG_D("TConfigsDispatcher Bootstrap");

    NActors::TMon *mon = AppData()->Mon;
    if (mon) {
        NMonitoring::TIndexMonPage *actorsMonPage = mon->RegisterIndexPage("actors", "Actors");
        mon->RegisterActorPage(actorsMonPage, "configs_dispatcher", "Configs Dispatcher", false, TlsActivationContext->ExecutorThread.ActorSystem, SelfId());
    }

    Become(&TThis::StateInit);
    Send(MakeTenantPoolRootID(), new TEvents::TEvSubscribe);
}

void TConfigsDispatcher::EnqueueEvent(TAutoPtr<IEventHandle> &ev)
{
    BLOG_D("Enqueue event type: " << ev->GetTypeRewrite());
    EventsQueue.push_back(ev);
}

void TConfigsDispatcher::ProcessEnqueuedEvents()
{
    while (!EventsQueue.empty()) {
        TAutoPtr<IEventHandle> &ev = EventsQueue.front();
        BLOG_D("Dequeue event type: " << ev->GetTypeRewrite());
        TlsActivationContext->Send(ev.Release());
        EventsQueue.pop_front();
    }
}

TDynBitMap TConfigsDispatcher::KindsToBitMap(const TVector<ui32> &kinds) const
{
    TDynBitMap result;
    for (auto &kind : kinds)
        result.Set(kind);
    return result;
}

TString TConfigsDispatcher::KindsToString(const TDynBitMap &kinds) const
{
    TStringStream ss;
    bool first = true;
    Y_FOR_EACH_BIT(kind, kinds) {
        ss << (first ? "" : ", ") << static_cast<NKikimrConsole::TConfigItem::EKind>(kind);
        first = false;
    }
    return ss.Str();
}

TVector<ui32> TConfigsDispatcher::KindsToVector(const TDynBitMap &kinds) const
{
    TVector<ui32> res;
    Y_FOR_EACH_BIT(kind, kinds) {
        res.push_back(kind);
    }
    return res;
}

void TConfigsDispatcher::ReplaceConfigItems(const NKikimrConfig::TAppConfig &from,
                                            NKikimrConfig::TAppConfig &to,
                                            const TDynBitMap &kinds) const
{
    auto *desc = to.GetDescriptor();
    auto *reflection = to.GetReflection();

    Y_FOR_EACH_BIT(kind, kinds) {
        auto *field = desc->FindFieldByNumber(kind);
        if (field && reflection->HasField(to, field))
            reflection->ClearField(&to, field);
    }

    to.MergeFrom(from);
}

bool TConfigsDispatcher::CompareConfigs(const NKikimrConfig::TAppConfig &lhs, const NKikimrConfig::TAppConfig &rhs)
{
    TString str1, str2;
    Y_PROTOBUF_SUPPRESS_NODISCARD lhs.SerializeToString(&str1);
    Y_PROTOBUF_SUPPRESS_NODISCARD rhs.SerializeToString(&str2);
    return (str1 == str2);
}

TConfigsDispatcher::TSubscription::TPtr TConfigsDispatcher::FindSubscription(ui64 id)
{
    auto it = SubscriptionsById.find(id);
    if (it == SubscriptionsById.end())
        return nullptr;
    return it->second;
}

TConfigsDispatcher::TSubscription::TPtr TConfigsDispatcher::FindSubscription(const TDynBitMap &kinds)
{
    auto it = SubscriptionsByKinds.find(kinds);
    if (it == SubscriptionsByKinds.end())
        return nullptr;
    return it->second;
}

TConfigsDispatcher::TSubscriber::TPtr TConfigsDispatcher::FindSubscriber(TActorId aid)
{
    auto it = Subscribers.find(aid);
    if (it == Subscribers.end())
        return nullptr;
    return it->second;
}

void TConfigsDispatcher::SendNotificationResponse(TEvConsole::TEvConfigNotificationRequest::TPtr &ev)
{
    const auto &rec = ev->Get()->Record;
    auto resp = MakeHolder<TEvConsole::TEvConfigNotificationResponse>(rec);

    BLOG_TRACE("Send TEvConfigNotificationResponse: " << resp->Record.ShortDebugString());

    Send(ev->Sender, resp.Release(), 0, ev->Cookie);
}

void TConfigsDispatcher::MaybeSendNotificationResponse(TSubscription::TPtr subscription)
{
    if (!subscription->UpdateInProcess || !subscription->SubscribersToUpdate.empty())
        return;

    auto &rec = subscription->UpdateInProcess->Get()->Record;
    subscription->CurrentConfig.Config.Swap(rec.MutableConfig());
    subscription->CurrentConfig.ConfigId.Load(rec.GetConfigId());

    ReplaceConfigItems(subscription->CurrentConfig.Config, CurrentConfig, subscription->Kinds);

    BLOG_D("Got all confirmations for config update"
                << " subscriptionid=" << subscription->SubscriptionId
                << " configid=" << TConfigId(rec.GetConfigId()).ToString());

    SendNotificationResponse(subscription->UpdateInProcess);

    subscription->UpdateInProcess = nullptr;
}

void TConfigsDispatcher::CreateSubscriberActor(ui32 kind, bool replace)
{
    TDynBitMap kinds;
    kinds.Set(kind);
    CreateSubscriberActor(kinds, replace);
}

void TConfigsDispatcher::CreateSubscriberActor(const TDynBitMap &kinds, bool replace)
{
    BLOG_D("Create new subscriber kinds=" << KindsToString(kinds));

    auto *subscriber = CreateConfigSubscriber(MakeConfigsDispatcherID(SelfId().NodeId()),
                                              KindsToVector(kinds),
                                              SelfId(),
                                              replace,
                                              NextRequestCookie);
    Register(subscriber);
    RequestCookies[NextRequestCookie] = kinds;
    ++NextRequestCookie;
}

void TConfigsDispatcher::SendUpdateToSubscriber(TSubscription::TPtr subscription, TActorId subscriber)
{
    Y_VERIFY(subscription->SubscriptionId);
    Y_VERIFY(subscription->UpdateInProcess);

    subscription->SubscribersToUpdate.insert(subscriber);

    auto notification = MakeHolder<TEvConsole::TEvConfigNotificationRequest>();
    notification->Record.CopyFrom(subscription->UpdateInProcess->Get()->Record);

    BLOG_TRACE("Send TEvConsole::TEvConfigNotificationRequest to " << subscriber
                << ": " << notification->Record.ShortDebugString());

    Send(subscriber, notification.Release(), 0, subscription->UpdateInProcess->Cookie);
}

void TConfigsDispatcher::RemoveSubscriber(TSubscriber::TPtr subscriber)
{
    BLOG_D("Remove subscriber " << subscriber->Subscriber);

    for (auto subscription : subscriber->Subscriptions) {
        Y_VERIFY(subscription->Subscribers.contains(subscriber->Subscriber));
        subscription->Subscribers.erase(subscriber->Subscriber);

        if (subscription->UpdateInProcess) {
            subscription->SubscribersToUpdate.erase(subscriber->Subscriber);
            MaybeSendNotificationResponse(subscription);
        }

        // If there are no more subscribers using this subscription then
        // it can be removed. Don't remove subscriptions which are not
        // yet confirmed by CMS.
        if (subscription->Subscribers.empty() && subscription->SubscriptionId)
            RemoveSubscription(subscription);
    }

    Subscribers.erase(subscriber->Subscriber);
}

void TConfigsDispatcher::RemoveSubscription(TSubscription::TPtr subscription)
{
    Subscriptions.erase(subscription);
    SubscriptionsById.erase(subscription->SubscriptionId);
    SubscriptionsByKinds.erase(subscription->Kinds);

    BLOG_D("Remove subscription id=" << subscription->SubscriptionId
                << " kinds=" << KindsToString(subscription->Kinds));

    Register(CreateSubscriptionEraser(subscription->SubscriptionId));
}

void TConfigsDispatcher::AddSubscription(TActorId subscriber,
                                         const TDynBitMap &kinds,
                                         bool replace)
{
    BLOG_D("Add subscription for " << subscriber << " kinds=" << KindsToString(kinds));

    // If there is a subscription for required config kinds then
    // re-use it for new subscriber. Otherwise create a new one.
    auto subscription = FindSubscription(kinds);
    if (!subscription) {
        subscription = new TSubscription;
        subscription->Kinds = kinds;

        Subscriptions.insert(subscription);
        SubscriptionsByKinds.emplace(kinds, subscription);

        CreateSubscriberActor(kinds, replace);
    }
    subscription->Subscribers.insert(subscriber);

    auto s = FindSubscriber(subscriber);
    if (!s) {
        s = new TSubscriber;
        s->Subscriber = subscriber;
        Subscribers.emplace(subscriber, s);
    }
    s->Subscriptions.insert(subscription);

    // Non-zero subscription ID means there is an active CMS
    // subscription and therefore we can respond to the subscriber
    // immediately.  Otherwise we should wait until CMS
    // subscription request is complete.
    if (subscription->SubscriptionId) {
        Y_VERIFY(!replace);
        auto resp = MakeHolder<TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse>();

        BLOG_TRACE("Send TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse to "
                    << subscriber);

        Send(subscriber, resp.Release());
    }

    // If there is an ongoing config update then include new subscriber into
    // the process.
    if (subscription->UpdateInProcess) {
        Y_VERIFY(!replace);
        SendUpdateToSubscriber(subscription, subscriber);
    } else if (!subscription->FirstUpdate) {
        // If subscription already had an update notification then send corresponding
        // notification to the subscriber using current config.
        Y_VERIFY(!replace);
        Y_VERIFY(subscription->SubscriptionId);
        auto notification = MakeHolder<TEvConsole::TEvConfigNotificationRequest>();
        notification->Record.SetSubscriptionId(subscription->SubscriptionId);
        subscription->CurrentConfig.ConfigId.Serialize(*notification->Record.MutableConfigId());
        notification->Record.MutableConfig()->CopyFrom(subscription->CurrentConfig.Config);

        BLOG_TRACE("Send TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse to "<< subscriber);

        Send(subscriber, notification.Release());
    }
}

void TConfigsDispatcher::CleanUpSubscriptions()
{
    BLOG_N("Cleaning up all current subscriptions");

    // If there are active subscriptions then we should
    // mark them as removed by reseting their IDs
    // and configs.
    for (auto &subscription : Subscriptions) {
        subscription->SubscriptionId = 0;
        subscription->SubscribersToUpdate.clear();
        subscription->UpdateInProcess = nullptr;
        subscription->FirstUpdate = true;
    }
    SubscriptionsById.clear();
    RequestCookies.clear();
    OutOfOrderConfigNotifications.clear();

    // We should invalidate configs cache to avoid its usage until
    // updated configs are received.
    ConfigsCache.clear();

    TDynBitMap kinds;
    kinds.Set(NKikimrConsole::TConfigItem::ConfigsDispatcherConfigItem);
    auto subscription = FindSubscription(kinds);
    if (subscription) {
        CreateSubscriberActor(kinds, true);
    } else {
        AddSubscription(SelfId(), kinds, true);
    }

    Become(&TThis::StateConfigure);
}

void TConfigsDispatcher::ProcessAddedSubscription(TSubscription::TPtr subscription, ui64 id)
{
    BLOG_N("Confirmed CMS subscription"
                << " kinds=" << KindsToString(subscription->Kinds)
                << " id=" << id);

    Y_VERIFY(!subscription->SubscriptionId);
    subscription->SubscriptionId = id;
    SubscriptionsById[id] = subscription;

    for (auto &subscriber : subscription->Subscribers) {
        BLOG_TRACE("Send TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse to " << subscriber);

        Send(subscriber, new TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse);
    }

    auto it = OutOfOrderConfigNotifications.find(id);
    if (it != OutOfOrderConfigNotifications.end()) {
        auto ev = std::move(it->second);
        OutOfOrderConfigNotifications.erase(it);
        Handle(ev);
    }

    while (!(SubscriptionsById.size() < Subscriptions.size()) && !OutOfOrderConfigNotifications.empty()) {
        auto it = OutOfOrderConfigNotifications.begin();
        auto ev = std::move(it->second);
        OutOfOrderConfigNotifications.erase(it);
        SendNotificationResponse(ev);
    }

    // Probably there are no more subscribers for this subscription.
    // In that case it should be removed.
    if (subscription->Subscribers.empty())
        RemoveSubscription(subscription);
}

void TConfigsDispatcher::ProcessLocalCacheUpdate(TEvConsole::TEvConfigNotificationRequest::TPtr &ev)
{
    auto &rec = ev->Get()->Record;
    BLOG_D("Got new config: " << rec.ShortDebugString());

    auto subscription = FindSubscription(rec.GetSubscriptionId());
    if (!subscription) {
        BLOG_ERROR("Cannot find subscription for configs cache update subscriptionid=" << rec.GetSubscriptionId());
        return;
    }

    BLOG_D("Update local cache for kinds=" << KindsToString(subscription->Kinds)
                << " config='" << rec.GetConfig().ShortDebugString() << "'");

    ConfigsCache[subscription->Kinds].reset(new NKikimrConfig::TAppConfig(rec.GetConfig()));

    auto resp = MakeHolder<TEvConsole::TEvConfigNotificationResponse>(rec);

    BLOG_TRACE("Send TEvConsole::TEvConfigNotificationResponse to self: " << resp->Record.ShortDebugString());

    Send(ev->Sender, resp.Release(), 0, ev->Cookie);
}

void TConfigsDispatcher::Handle(NMon::TEvHttpInfo::TPtr &ev)
{
    TStringStream str;
    str << NMonitoring::HTTPOKHTML;
    HTML(str) {
        NHttp::OutputStaticPart(str);
        PRE() {
            str << "Maintained tenant: " << JoinSeq(", ", CurrentTenants);
        }
        DIV_CLASS("tab-left") {
            COLLAPSED_REF_CONTENT("node-labels", "Node labels") {
                PRE() {
                    for (auto& [key, value] : Labels) {
                        str << key << " = " << value << Endl;
                    }
                }
            }
            str << "<br />" << Endl;
            COLLAPSED_REF_CONTENT("current-config", "Current config") {
                NHttp::OutputConfigHTML(str, CurrentConfig);
            }
            str << "<br />" << Endl;
            COLLAPSED_REF_CONTENT("initial-config", "Initial config") {
                NHttp::OutputConfigHTML(str, InitialConfig);
            }
            str << "<br />" << Endl;
            COLLAPSED_REF_CONTENT("subscriptions", "Subscriptions") {
                PRE() {
                    for (auto subscription : Subscriptions) {
                        str << " - ID: " << subscription->SubscriptionId << Endl
                            << "   Kinds: " << KindsToString(subscription->Kinds) << Endl
                            << "   Subscribers:";
                        for (auto &id : subscription->Subscribers)
                            str << " " << id;
                        str << Endl
                            << "   FirstUpdate: " << subscription->FirstUpdate << Endl
                            << "   CurrentConfigId: " << subscription->CurrentConfig.ConfigId.ToString() << Endl
                            << "   CurrentConfig: " << subscription->CurrentConfig.Config.ShortDebugString() << Endl;
                        if (subscription->UpdateInProcess) {
                            str << "   UpdateInProcess: " << subscription->UpdateInProcess->Get()->Record.ShortDebugString() << Endl
                                << "   SubscribersToUpdate:";
                            for (auto &id : subscription->SubscribersToUpdate)
                                str << " " << id;
                            str << Endl;
                        }
                    }
                }
            }
            str << "<br />" << Endl;
            COLLAPSED_REF_CONTENT("subscribers", "Subscribers") {
                PRE() {
                    for (auto &pr : Subscribers) {
                        str << " - Subscriber: " << pr.second->Subscriber << Endl
                            << "   Subscriptions:";
                        for (auto subscription : pr.second->Subscriptions)
                            str << " " << subscription->SubscriptionId;
                        str << Endl
                            << "   CurrentConfigId: " << pr.second->CurrentConfigId.ToString() << Endl;
                    }
                }
            }
            str << "<br />" << Endl;
            COLLAPSED_REF_CONTENT("cache", "Configs cache") {
                DIV_CLASS("tab-left") {
                    ui32 id = 1;
                    for (auto &pr : ConfigsCache) {
                        TString kinds = KindsToString(pr.first);
                        str << "<br />" << Endl;
                        COLLAPSED_REF_CONTENT("cache-" + ToString(id++), kinds) {
                            DIV_CLASS("tab-left") {
                                PRE() {
                                    str << pr.second->DebugString() << Endl;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str(), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
}

void TConfigsDispatcher::Handle(TEvConfigsDispatcher::TEvGetConfigRequest::TPtr &ev)
{
    auto kinds = KindsToBitMap(ev->Get()->ConfigItemKinds);

    if (ev->Get()->Cache && !kinds.Empty()) {
        auto subscription = FindSubscription(kinds);
        if (!subscription || !subscription->Subscribers.contains(SelfId())) {
            BLOG_D("Add subscription for local cache kinds=" << KindsToString(kinds));
            AddSubscription(SelfId(), kinds, false);
        }
    }

    if (ConfigsCache.contains(kinds)) {
        auto resp = MakeHolder<TEvConfigsDispatcher::TEvGetConfigResponse>();
        resp->Config = ConfigsCache.at(kinds);

        BLOG_TRACE("Send TEvConfigsDispatcher::TEvGetConfigResponse"
            " to " << ev->Sender << ": " << resp->Config->ShortDebugString());

        Send(ev->Sender, std::move(resp), 0, ev->Cookie);
    } else {
        Register(CreateNodeConfigCourier(ev->Get()->ConfigItemKinds, SelfId(), NextRequestCookie));
        ConfigRequests[NextRequestCookie++] = THolder<IEventHandle>(ev.Release());
    }
}

void TConfigsDispatcher::Handle(TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest::TPtr &ev)
{
    auto kinds = KindsToBitMap(ev->Get()->ConfigItemKinds);
    auto subscriber = FindSubscriber(ev->Sender);

    if (subscriber) {
        Y_VERIFY(subscriber->Subscriptions.size() == 1);
        auto subscription = *subscriber->Subscriptions.begin();

        if (subscription->Kinds == kinds) {
            BLOG_D("Nothing to change for " << subscriber->Subscriber);
            BLOG_TRACE("Send TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse to " << subscriber->Subscriber);

            Send(subscriber->Subscriber, new TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse);
            return;
        }

        // something changed so refresh subscription
        RemoveSubscriber(subscriber);
    }

    if (!kinds.Empty()) {
        AddSubscription(ev->Sender, kinds, false);
    } else {
        BLOG_TRACE("Send TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse to " << ev->Sender);

        Send(ev->Sender, new TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse);
    }
}

void TConfigsDispatcher::Handle(TEvConsole::TEvAddConfigSubscriptionResponse::TPtr &ev)
{
    auto it = RequestCookies.find(ev->Cookie);
    if (it == RequestCookies.end()) {
        BLOG_I("Cookie mismatch for TEvAddConfigSubscriptionResponse");
        return;
    }
    auto kinds = it->second;
    RequestCookies.erase(it);

    auto &rec = ev->Get()->Record;
    if (rec.GetStatus().GetCode() != Ydb::StatusIds::SUCCESS) {
        LOG_CRIT_S(*TlsActivationContext, NKikimrServices::CONFIGS_DISPATCHER,
                   "Cannot get config subscription for " << KindsToString(kinds)
                   << " code=" << rec.GetStatus().GetCode()
                   << " reason= " << rec.GetStatus().GetReason());
        CreateSubscriberActor(kinds, false);
        return;
    }

    auto subscription = FindSubscription(kinds);
    Y_VERIFY(subscription);

    ProcessAddedSubscription(subscription, rec.GetSubscriptionId());
}

void TConfigsDispatcher::Handle(TEvConsole::TEvConfigNotificationResponse::TPtr &ev)
{
    auto rec = ev->Get()->Record;
    auto subscription = FindSubscription(rec.GetSubscriptionId());

    // Probably subscription was cleared up due to tenant's change.
    if (!subscription) {
        BLOG_I("Got notification response for unknown subscription " << rec.GetSubscriptionId());
        return;
    }

    if (!subscription->UpdateInProcess) {
        BLOG_D("Notification was ignored for subscription "
                    << rec.GetSubscriptionId());
        return;
    }

    if (ev->Cookie != subscription->UpdateInProcess->Cookie) {
        BLOG_ERROR("Notification cookie mismatch for subscription " << rec.GetSubscriptionId());
        return;
    }

    TConfigId id1(subscription->UpdateInProcess->Get()->Record.GetConfigId());
    TConfigId id2(rec.GetConfigId());
    // This might be outdated notification response.
    if (id1 != id2) {
        BLOG_I("Config id mismatch in notification response for subscription " << rec.GetSubscriptionId());
        return;
    }

    if (!subscription->SubscribersToUpdate.contains(ev->Sender)) {
        BLOG_ERROR("Notification from unexpected subscriber for subscription " << rec.GetSubscriptionId());
        return;
    }

    Subscribers.at(ev->Sender)->CurrentConfigId = id1;

    // If all subscribers responded then send response to CMS.
    subscription->SubscribersToUpdate.erase(ev->Sender);
    MaybeSendNotificationResponse(subscription);
}

void TConfigsDispatcher::Handle(TEvConsole::TEvConfigNotificationRequest::TPtr &ev)
{
    // Process local update sent by own local subscription.
    if (ev->Sender == SelfId()) {
        ProcessLocalCacheUpdate(ev);
        return;
    }

    auto &rec = ev->Get()->Record;
    auto subscription = FindSubscription(rec.GetSubscriptionId());
    if (!subscription) {
        BLOG_W("Got notification for unknown subscription id=" << rec.GetSubscriptionId());

        if (SubscriptionsById.size() < Subscriptions.size()) {
            // There are subscriptions that don't have an id yet
            // Delay processing until we know ids of all subscriptions
            auto &prev = OutOfOrderConfigNotifications[rec.GetSubscriptionId()];
            if (prev) {
                SendNotificationResponse(prev);
            }
            prev = ev;
            return;
        }

        SendNotificationResponse(ev);
        return;
    }

    if (subscription->UpdateInProcess) {
        BLOG_D("Drop previous unfinished notification for subscription id="
                    << subscription->SubscriptionId);
        subscription->UpdateInProcess = nullptr;
        subscription->SubscribersToUpdate.clear();
    }

    subscription->UpdateInProcess = std::move(ev);

    /**
     * Avoid notifications in case only config id changed and
     * config body is equal to currently used one.
     */
    if (subscription->FirstUpdate || !CompareConfigs(subscription->CurrentConfig.Config, rec.GetConfig())) {
        for (auto &subscriber : subscription->Subscribers)
            SendUpdateToSubscriber(subscription, subscriber);
    } else {
        MaybeSendNotificationResponse(subscription);
    }

    subscription->FirstUpdate = false;
}

void TConfigsDispatcher::Handle(TEvConsole::TEvGetNodeConfigResponse::TPtr &ev)
{
    auto it = ConfigRequests.find(ev->Cookie);

    if (it == ConfigRequests.end()) {
        BLOG_ERROR("Node config response for unknown request cookie=" << ev->Cookie);
        return;
    }

    auto resp = MakeHolder<TEvConfigsDispatcher::TEvGetConfigResponse>();
    resp->Config.reset(new NKikimrConfig::TAppConfig(ev->Get()->Record.GetConfig()));

    BLOG_TRACE("Send TEvConfigsDispatcher::TEvGetConfigResponse"
            " to " << ev->Sender
            << ": " << resp->Config->ShortDebugString());

    Send(it->second->Sender, resp.Release(), 0, it->second->Cookie);

    ConfigRequests.erase(it);
}

void TConfigsDispatcher::Handle(TEvConsole::TEvReplaceConfigSubscriptionsResponse::TPtr &ev)
{
    auto it = RequestCookies.find(ev->Cookie);
    if (it == RequestCookies.end()) {
        BLOG_ERROR("Cookie mismatch for TEvReplaceConfigSubscriptionsResponse");
        return;
    }

    auto &rec = ev->Get()->Record;
    if (rec.GetStatus().GetCode() != Ydb::StatusIds::SUCCESS) {
        LOG_CRIT_S(*TlsActivationContext, NKikimrServices::CONFIGS_DISPATCHER,
                   "Cannot initialize subscription: " << rec.GetStatus().GetReason());
        CleanUpSubscriptions();
        return;
    }

    auto subscription = FindSubscription(it->second);
    Y_VERIFY(subscription);
    ProcessAddedSubscription(subscription, rec.GetSubscriptionId());

    // Register other subscriptions in CMS.
    for (auto subscription : Subscriptions)
        if (!subscription->SubscriptionId)
            CreateSubscriberActor(subscription->Kinds, false);

    Become(&TThis::StateWork);
    ProcessEnqueuedEvents();
}

void TConfigsDispatcher::Handle(TEvTenantPool::TEvTenantPoolStatus::TPtr &ev)
{
    auto &rec = ev->Get()->Record;

    THashSet<TString> tenants;
    for (auto &slot : rec.GetSlots())
        tenants.insert(slot.GetAssignedTenant());

    // If we are in initial state then subscriptions set is empty
    // and we should start initialization. Otherwise we should
    // re-initialize if tenants set changed.
    if (CurrentTenants != tenants || Subscriptions.empty()) {
        CurrentTenants = tenants;

        BLOG_N("Update list of assigned tenants: " << JoinSeq(", ", CurrentTenants));

        CleanUpSubscriptions();
    }
}

} // anonymous namespace

IActor *CreateConfigsDispatcher(const NKikimrConfig::TAppConfig &config, const TMap<TString, TString> &labels)
{
    return new TConfigsDispatcher(config, labels);
}

} // namespace NKikimr::NConsole

#pragma once

#include "defs.h"

#include "configs_config.h"
#include "console.h"

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/cms/console/util/config_index.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>

#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr::NConsole {

class TConfigsProvider : public TActorBootstrapped<TConfigsProvider> {
public:
    struct TEvPrivate {
        enum EEv {
            EvNotificationTimeout = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
            EvSenderDied,
            EvSetConfig,
            EvSetConfigs,
            EvSetSubscriptions,
            EvUpdateConfigs,
            EvUpdateYamlConfig,
            EvUpdateSubscriptions,
            EvWorkerDisconnected,

            EvEnd
        };

        static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_PRIVATE)");

        struct TEvNotificationTimeout : public TEventLocal<TEvNotificationTimeout, EvNotificationTimeout> {
            TEvNotificationTimeout(TSubscription::TPtr subscription)
                : Subscription(subscription)
            {
            }

            TSubscription::TPtr Subscription;
        };

        struct TEvSenderDied : public TEventLocal<TEvSenderDied, EvSenderDied> {
            TEvSenderDied(TSubscription::TPtr subscription)
                : Subscription(subscription)
            {
            }

            TSubscription::TPtr Subscription;
        };

        struct TEvWorkerDisconnected : public TEventLocal<TEvWorkerDisconnected, EvWorkerDisconnected> {
            explicit TEvWorkerDisconnected(TInMemorySubscription::TPtr subscription)
                : Subscription(subscription)
            {
            }

            TInMemorySubscription::TPtr Subscription;
        };

        struct TEvSetConfig : public TEventLocal<TEvSetConfig, EvSetConfig> {
            TEvSetConfig(const TConfigsConfig &config)
                : Config(config)
            {
            }

            TConfigsConfig Config;
        };

        struct TEvSetConfigs : public TEventLocal<TEvSetConfigs, EvSetConfigs> {
            TEvSetConfigs(const THashMap<ui64, TConfigItem::TPtr> &items)
            {
                for (auto &pr : items)
                    ConfigItems.emplace(pr.first, new TConfigItem(*pr.second));
            }

            THashMap<ui64, TConfigItem::TPtr> ConfigItems;
        };

        struct TEvSetSubscriptions : public TEventLocal<TEvSetSubscriptions, EvSetSubscriptions> {
            TEvSetSubscriptions(const THashMap<ui64, TSubscription::TPtr> &subscriptions)
            {
                for (auto &pr : subscriptions)
                    Subscriptions.emplace(pr.first, new TSubscription(*pr.second));
            }

            THashMap<ui64, TSubscription::TPtr> Subscriptions;
        };

        struct TEvUpdateConfigs : public TEventLocal<TEvUpdateConfigs, EvUpdateConfigs> {
            TEvUpdateConfigs(const TConfigModifications &mod, TAutoPtr<IEventHandle> ev)
                : Event(ev)
            {
                Modifications.DeepCopyFrom(mod);
            }

            TConfigModifications Modifications;
            TAutoPtr<IEventHandle> Event;
        };

        struct TEvUpdateYamlConfig : public TEventLocal<TEvUpdateYamlConfig, EvUpdateYamlConfig> {
            TEvUpdateYamlConfig(const TString &yamlConfig, const TMap<ui64, TString> &volatileYamlConfigs = {})
                : YamlConfig(yamlConfig)
                , VolatileYamlConfigs(volatileYamlConfigs)
            {
            }

            TString YamlConfig;
            TMap<ui64, TString> VolatileYamlConfigs;
        };

        struct TEvUpdateSubscriptions : public TEventLocal<TEvUpdateSubscriptions, EvUpdateSubscriptions> {
            TEvUpdateSubscriptions(const TSubscriptionModifications &mod, TAutoPtr<IEventHandle> ev)
                : Event(ev)
            {
                Modifications.DeepCopyFrom(mod);
            }

            TSubscriptionModifications Modifications;
            TAutoPtr<IEventHandle> Event;
        };
    };

private:
    using TBase = TActorBootstrapped<TConfigsProvider>;

    void ClearState();

    void ApplyConfigModifications(const TConfigModifications &modifications,
                                  const TActorContext &ctx);
    void ApplySubscriptionModifications(const TSubscriptionModifications &modifications,
                                        const TActorContext &ctx);

    void CheckAllSubscriptions(const TActorContext &ctx);
    void CheckSubscriptions(const TSubscriptionSet &subscriptions,
                            const TActorContext &ctx);
    void CheckSubscriptions(const TInMemorySubscriptionSet &subscriptions,
                            const TActorContext &ctx);
    void CheckSubscription(TSubscription::TPtr subscriptions,
                           const TActorContext &ctx);
    void CheckSubscription(TInMemorySubscription::TPtr subscriptions,
                           const TActorContext &ctx);

    void Handle(NMon::TEvHttpInfo::TPtr &ev);
    void Handle(NEvConsole::TEvConfigSubscriptionRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(NEvConsole::TEvConfigSubscriptionCanceled::TPtr &ev, const TActorContext &ctx);
    void Handle(NEvConsole::TEvCheckConfigUpdatesRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(NEvConsole::TEvConfigNotificationResponse::TPtr &ev, const TActorContext &ctx);
    void Handle(NEvConsole::TEvGetConfigItemsRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(NEvConsole::TEvGetConfigSubscriptionRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(NEvConsole::TEvGetNodeConfigItemsRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(NEvConsole::TEvGetNodeConfigRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(NEvConsole::TEvListConfigSubscriptionsRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvPrivate::TEvWorkerDisconnected::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvPrivate::TEvNotificationTimeout::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvPrivate::TEvSenderDied::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvPrivate::TEvSetConfig::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvPrivate::TEvSetConfigs::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvPrivate::TEvSetSubscriptions::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvPrivate::TEvUpdateConfigs::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvPrivate::TEvUpdateYamlConfig::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvPrivate::TEvUpdateSubscriptions::TPtr &ev, const TActorContext &ctx);

    void HandlePoison(const TActorContext &ctx)
    {
        LOG_DEBUG(ctx, NKikimrServices::CMS_CONFIGS, "TConfigsProvider::HandlePoison");

        Die(ctx);
    }

    STFUNC(StateWork) {
        TRACE_EVENT(NKikimrServices::CMS_CONFIGS);
        switch (ev->GetTypeRewrite()) {
            hFuncTraced(NMon::TEvHttpInfo, Handle);
            HFuncTraced(NEvConsole::TEvConfigSubscriptionRequest, Handle);
            HFuncTraced(NEvConsole::TEvConfigSubscriptionCanceled, Handle);
            HFuncTraced(NEvConsole::TEvCheckConfigUpdatesRequest, Handle);
            HFuncTraced(NEvConsole::TEvConfigNotificationResponse, Handle);
            HFuncTraced(NEvConsole::TEvGetConfigItemsRequest, Handle);
            HFuncTraced(NEvConsole::TEvGetConfigSubscriptionRequest, Handle);
            HFuncTraced(NEvConsole::TEvGetNodeConfigItemsRequest, Handle);
            HFuncTraced(NEvConsole::TEvGetNodeConfigRequest, Handle);
            HFuncTraced(NEvConsole::TEvListConfigSubscriptionsRequest, Handle);
            HFuncTraced(TEvPrivate::TEvWorkerDisconnected, Handle);
            HFuncTraced(TEvPrivate::TEvNotificationTimeout, Handle);
            HFuncTraced(TEvPrivate::TEvSenderDied, Handle);
            HFuncTraced(TEvPrivate::TEvSetConfig, Handle);
            HFuncTraced(TEvPrivate::TEvSetConfigs, Handle);
            HFuncTraced(TEvPrivate::TEvSetSubscriptions, Handle);
            HFuncTraced(TEvPrivate::TEvUpdateConfigs, Handle);
            HFuncTraced(TEvPrivate::TEvUpdateYamlConfig, Handle);
            HFuncTraced(TEvPrivate::TEvUpdateSubscriptions, Handle);
            CFunc(TEvents::TSystem::PoisonPill, HandlePoison);

        default:
            Y_ABORT("unexpected event type: %" PRIx32 " event: %s",
                   ev->GetTypeRewrite(), ev->ToString().data());
            break;
        }
    }

public:
    TConfigsProvider(TActorId ownerId)
        : ConfigsManager(ownerId)
    {
    }

    ~TConfigsProvider()
    {
        ClearState();
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType()
    {
        return NKikimrServices::TActivity::CMS_CONFIGS_PROVIDER;
    }

    void Bootstrap(const TActorContext &ctx);
    void DumpStateHTML(IOutputStream &os) const;
    void Die(const TActorContext &ctx) override;

private:
    TActorId ConfigsManager;
    TConfigsConfig Config;
    TConfigIndex ConfigIndex;
    TSubscriptionIndex SubscriptionIndex;
    TInMemorySubscriptionIndex InMemoryIndex;

    TString YamlConfig;
    TMap<ui64, TString> VolatileYamlConfigs;
    ui64 YamlConfigVersion = 0;
    TMap<ui64, ui64> VolatileYamlConfigHashes;
};

} // namespace NKikimr::NConsole

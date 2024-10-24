#include "feature_flags_configurator.h"
#include "configs_dispatcher.h"
#include "console.h"
#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/base/feature_flags.h>
#include <ydb/core/base/feature_flags_service.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NConsole {

    class TFeatureFlagsConfigurator : public TActorBootstrapped<TFeatureFlagsConfigurator> {
    public:
        static constexpr auto ActorActivityType() {
            return NKikimrServices::TActivity::FEATURE_FLAGS_CONFIGURATOR;
        }

        TFeatureFlagsConfigurator() = default;

        void Bootstrap() {
            LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::CMS_CONFIGS,
                "TFeatureFlagsConfigurator Bootstrap");

            Become(&TThis::StateWork);

            ui32 item = (ui32)NKikimrConsole::TConfigItem::FeatureFlagsItem;
            Send(MakeConfigsDispatcherID(SelfId().NodeId()),
                    new NEvConfigsDispatcher::TEvSetConfigSubscriptionRequest(item));
        }

        void Handle(NEvConsole::TEvConfigNotificationRequest::TPtr& ev) {
            const auto& record = ev->Get()->Record;
            LOG_INFO_S(*TlsActivationContext, NKikimrServices::CMS_CONFIGS,
                "TFeatureFlagsConfigurator: new config: " << record.GetConfig().ShortDebugString());

            // Atomically replace runtime feature flags with the new config
            AppDataVerified().UpdateRuntimeFlags(record.GetConfig().GetFeatureFlags());
            Updated = true;

            Send(ev->Sender, new NEvConsole::TEvConfigNotificationResponse(record), 0, ev->Cookie);

            // Notify all subscribers about feature flags potentially changing
            for (auto& pr : Subscribers) {
                // Since feature flags are relaxed this event also establishes
                // release-acquire ordering on repeat accesses.
                Send(pr.first, new TEvFeatureFlags::TEvChanged, 0, pr.second);
            }
        }

        void Handle(TEvFeatureFlags::TEvSubscribe::TPtr& ev) {
            Subscribers[ev->Sender] = ev->Cookie;

            // Since feature flags are relaxed client may have observed outdated
            // values just before the subscription, which may have actually
            // changed, including the time the event was inflight. We always
            // send the first (potentially spurious) notification.
            if (Updated) {
                Send(ev->Sender, new TEvFeatureFlags::TEvChanged, 0, ev->Cookie);
            }
        }

        void Handle(TEvFeatureFlags::TEvUnsubscribe::TPtr& ev) {
            Subscribers.erase(ev->Sender);
        }

    private:
        STFUNC(StateWork) {
            switch (ev->GetTypeRewrite()) {
                hFunc(NEvConsole::TEvConfigNotificationRequest, Handle);
                IgnoreFunc(NEvConfigsDispatcher::TEvSetConfigSubscriptionResponse);
                hFunc(TEvFeatureFlags::TEvSubscribe, Handle);
                hFunc(TEvFeatureFlags::TEvUnsubscribe, Handle);
            }
        }

    private:
        // ActorId -> Cookie
        THashMap<TActorId, ui64> Subscribers;
        // Tracks when we updated feature flags at least once
        bool Updated = false;
    };

    IActor* CreateFeatureFlagsConfigurator() {
        return new TFeatureFlagsConfigurator();
    }

} // namespace NKikimr::NConsole

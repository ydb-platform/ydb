#include "interconnect_configurator.h"
#include "configs_dispatcher.h"
#include "console.h"

#include <ydb/core/protos/config.pb.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::CMS_CONFIGS

namespace NKikimr::NConsole {

    class TInterconnectConfigurator : public TActorBootstrapped<TInterconnectConfigurator> {
    public:
        static constexpr auto ActorActivityType() {
            return NKikimrServices::TActivity::INTERCONNECT_CONFIGURATOR;
        }

        TInterconnectConfigurator(TIntrusivePtr<NActors::TInterconnectProxyCommon> common)
            : Common(std::move(common))
        {}

        void Bootstrap() {
            YDB_LOG_DEBUG("TInterconnectConfigurator Bootstrap");

            Become(&TThis::StateWork);

            ui32 item = (ui32)NKikimrConsole::TConfigItem::InterconnectConfigItem;
            Send(MakeConfigsDispatcherID(SelfId().NodeId()),
                    new TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest(item));
        }

        void Handle(TEvConsole::TEvConfigNotificationRequest::TPtr& ev) {
            const auto& record = ev->Get()->Record;

            ApplyConfig(record.GetConfig().GetInterconnectConfig());

            Send(ev->Sender, new TEvConsole::TEvConfigNotificationResponse(record), 0, ev->Cookie);
        }

    private:
        void ApplyConfig(const NKikimrConfig::TInterconnectConfig& config) {
            // The dispatcher hands us the effective config for the item we subscribed to -- the cluster
            // config resolved over this node's startup config -- so a missing V2Config here genuinely
            // means v2 is not enabled anywhere, not merely that this update did not mention it.
            const bool enable = config.GetV2Config().GetEnable();
            const bool prev = Common->Settings.V2.Enable;
            if (prev == enable && Applied) {
                return;
            }
            Applied = true;

            // Read by handshake actors on interconnect threads; this actor is the only writer.
            Common->Settings.V2.Enable = enable;

            if (prev != enable) {
                YDB_LOG_NOTICE("TInterconnectConfigurator: interconnect v2 negotiation switched",
                    {"enabled", enable},
                    {"engine", bool(Common->UringEngineV2)});
            }

            // Enabling v2 on a node that was not started with V2Config.Threads set does nothing: the
            // engine is only ever created at startup, so such a node keeps negotiating v1 until it is
            // restarted. Say so loudly -- from the cluster config alone this looks like it worked.
            if (enable && !Common->UringEngineV2) {
                YDB_LOG_WARN("TInterconnectConfigurator: interconnect v2 is enabled in config, but this "
                    "node has no v2 engine and will keep using v1; restart it with a non-zero "
                    "InterconnectConfig.V2Config.Threads (and io_uring available) to run v2",
                    {"threads", Common->Settings.V2.Threads});
            }
        }

        STFUNC(StateWork) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvConsole::TEvConfigNotificationRequest, Handle);
                IgnoreFunc(TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse);
            }
        }

    private:
        TIntrusivePtr<NActors::TInterconnectProxyCommon> Common;
        // The first notification is applied even when it matches what we already have, so that the
        // "enabled but no engine" warning is emitted on a node that starts up in that state.
        bool Applied = false;
    };

    IActor *CreateInterconnectConfigurator(TIntrusivePtr<NActors::TInterconnectProxyCommon> common) {
        return new TInterconnectConfigurator(std::move(common));
    }

} // namespace NKikimr::NConsole

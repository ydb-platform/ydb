#include "feature_flags_configurator.h"
#include "configs_dispatcher.h"
#include "console.h"
#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/base/feature_flags.h>
#include <ydb/core/base/feature_flags_service.h>
#include <ydb/core/mon/mon.h>
#include <ydb/core/protos/feature_flags.pb.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/mon.h>
#include <google/protobuf/descriptor.pb.h>

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
                    new TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest(item));

            if (auto* mon = AppData()->Mon) {
                auto* actorsMonPage = mon->RegisterIndexPage("actors", "Actors");
                mon->RegisterActorPage(
                    actorsMonPage, "feature_flags", "Feature Flags",
                    false, TActivationContext::ActorSystem(), SelfId());
            }
        }

        void Handle(TEvConsole::TEvConfigNotificationRequest::TPtr& ev) {
            const auto& record = ev->Get()->Record;
            LOG_INFO_S(*TlsActivationContext, NKikimrServices::CMS_CONFIGS,
                "TFeatureFlagsConfigurator: new config: " << record.GetConfig().ShortDebugString());

            // Atomically replace runtime feature flags with the new config
            AppDataVerified().UpdateRuntimeFlags(record.GetConfig().GetFeatureFlags());
            Updated = true;

            Send(ev->Sender, new TEvConsole::TEvConfigNotificationResponse(record), 0, ev->Cookie);

            // Notify all subscribers about feature flags potentially changing
            for (auto& pr : Subscribers) {
                // Since feature flags are relaxed this event also establishes
                // release-acquire ordering on repeat accesses.
                Send(pr.first, new TEvFeatureFlags::TEvChanged, 0, pr.second.Cookie);
            }
        }

        void Handle(TEvFeatureFlags::TEvSubscribe::TPtr& ev) {
            auto& subscriber = Subscribers[ev->Sender];
            subscriber.Cookie = ev->Cookie;
            subscriber.Description = std::move(ev->Get()->Description);

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

        void Handle(NMon::TEvHttpInfo::TPtr& ev) {
            auto cgi = ev->Get()->Request.GetParams();
            bool showAll = false;
            if (const auto& text = cgi.Get("all")) {
                int value;
                if (TryFromString(text, value)) {
                    showAll = value;
                }
            }

            NKikimrConfig::TFeatureFlags flags = AppData()->FeatureFlags;
            const auto* d = NKikimrConfig::TFeatureFlags::descriptor();
            const auto* r = flags.GetReflection();

            TStringStream html;
            HTML(html) {
                TAG(TH4) {
                    if (showAll) {
                        html << "<a href=\"feature_flags?all=0\">";
                    }
                    html << "Modified";
                    if (showAll) {
                        html << "</a>";
                    }
                    html << " | ";
                    if (!showAll) {
                        html << "<a href=\"feature_flags?all=1\">";
                    }
                    html << "All";
                    if (!showAll) {
                        html << "</a>";
                    }
                }

                TABLE_SORTABLE_CLASS("table") {
                    TABLEHEAD() {
                        TABLER() {
                            TABLEH() { html << "Flag"; }
                            TABLEH() { html << "Value"; }
                        }
                    }
                    TABLEBODY() {
                        for (int fieldIndex = 0; fieldIndex < d->field_count(); ++fieldIndex) {
                            const auto* protoField = d->field(fieldIndex);
                            if (protoField->type() != google::protobuf::FieldDescriptor::TYPE_BOOL) {
                                continue;
                            }
                            bool hasValue = r->HasField(flags, protoField);
                            if (!hasValue && !showAll) {
                                continue;
                            }
                            bool value = r->GetBool(flags, protoField);
                            TABLER() {
                                TABLED() {
                                    if (hasValue) {
                                        html << "<b>";
                                    }
                                    html << protoField->name();
                                    if (hasValue) {
                                        html << "</b>";
                                    }
                                }
                                TABLED() {
                                    if (hasValue) {
                                        html << "<b>";
                                    }
                                    html << (value ? "true" : "false");
                                    if (hasValue) {
                                        html << "</b>";
                                    }
                                }
                            }
                        }
                    }
                }

                TAG(TH4) {
                    html << "Subscribers";
                }

                TABLE_SORTABLE_CLASS("table") {
                    TABLEHEAD() {
                        TABLER() {
                            TABLEH() { html << "ActorId"; }
                            TABLEH() { html << "Description"; }
                        }
                    }
                    TABLEBODY() {
                        for (const auto& pr : Subscribers) {
                            TABLER() {
                                TABLED() { html << pr.first; }
                                TABLED() { html << pr.second.Description; }
                            }
                        }
                    }
                }
            }

            Send(ev->Sender, new NMon::TEvHttpInfoRes(html.Str()));
        }

    private:
        STFUNC(StateWork) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvConsole::TEvConfigNotificationRequest, Handle);
                IgnoreFunc(TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse);
                hFunc(TEvFeatureFlags::TEvSubscribe, Handle);
                hFunc(TEvFeatureFlags::TEvUnsubscribe, Handle);
                hFunc(NMon::TEvHttpInfo, Handle);
            }
        }

    private:
        struct TSubscriber {
            ui64 Cookie;
            TString Description;
        };

    private:
        // ActorId -> Cookie
        THashMap<TActorId, TSubscriber> Subscribers;
        // Tracks when we updated feature flags at least once
        bool Updated = false;
    };

    IActor* CreateFeatureFlagsConfigurator() {
        return new TFeatureFlagsConfigurator();
    }

} // namespace NKikimr::NConsole

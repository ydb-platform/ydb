#pragma once
#include "defs.h"

#include <ydb/core/protos/config.pb.h>
#include <ydb/core/config/init/init.h>

#include <util/generic/vector.h>
#include <util/generic/map.h>

namespace NKikimr::NConsole {

/**
 * Configs Dispatcher is used as a broker for subscriptions for
 * config updates and as a local configs cache. It's recommended
 * to use Configs Dispatcher by all actors to spread CMS load
 * between local services and use direct subscriptions in CMS
 * for tablets only.
 *
 * Actor may have only one subscription in Configs Dispatcher.
 * Use subscription request with zero item kinds to unsusbscribe
 * from updates. Subscription response means subscription was
 * registered in dispatcher and carries no additional info.
 *
 * Subscribers receive TEvConsole::TEvConfigNotificationRequest
 * as if they were subscribed in CMS and should process this
 * event accordingly (TEvConsole::TEvConfigNotificationResponse
 * event should be send in response with proper SubscriptionId,
 * ConfigId filled in and request Cookie used for response).
 */

struct TEvConfigsDispatcher {
    enum EEv {
        EvSetConfigSubscriptionRequest = EventSpaceBegin(TKikimrEvents::ES_CONFIGS_DISPATCHER),
        EvSetConfigSubscriptionResponse,
        EvGetConfigRequest,
        EvGetConfigResponse,
        EvRemoveConfigSubscriptionRequest,
        EvRemoveConfigSubscriptionResponse,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_CONFIGS_DISPATCHER),
                  "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_CONFIGS_DISPATCHER)");

    struct TEvSetConfigSubscriptionRequest : public TEventLocal<TEvSetConfigSubscriptionRequest, EvSetConfigSubscriptionRequest> {
        TEvSetConfigSubscriptionRequest(TActorId subscriber = {})
            : Subscriber(subscriber)
        {
        }

        TEvSetConfigSubscriptionRequest(ui32 kind, TActorId subscriber = {})
            : ConfigItemKinds({kind})
            , Subscriber(subscriber)
        {
        }

        TEvSetConfigSubscriptionRequest(std::initializer_list<ui32> kinds, TActorId subscriber = {})
            : ConfigItemKinds(kinds)
            , Subscriber(subscriber)
        {
        }

        TEvSetConfigSubscriptionRequest(const TVector<ui32> &kinds, TActorId subscriber = {})
            : ConfigItemKinds(kinds)
            , Subscriber(subscriber)
        {
        }

        TVector<ui32> ConfigItemKinds;
        const TActorId Subscriber;
    };

    struct TEvSetConfigSubscriptionResponse : public TEventLocal<TEvSetConfigSubscriptionResponse, EvSetConfigSubscriptionResponse> {
    };

    struct TEvRemoveConfigSubscriptionRequest : public TEventLocal<TEvRemoveConfigSubscriptionRequest, EvRemoveConfigSubscriptionRequest> {
        TEvRemoveConfigSubscriptionRequest(TActorId subscriber = {})
            : Subscriber(subscriber)
        {
        }

        const TActorId Subscriber;
    };

    struct TEvRemoveConfigSubscriptionResponse : public TEventLocal<TEvRemoveConfigSubscriptionResponse, EvRemoveConfigSubscriptionResponse> {
    };

    struct TEvGetConfigRequest : public TEventLocal<TEvGetConfigRequest, EvGetConfigRequest> {
        TEvGetConfigRequest(ui32 kind, bool cache = true)
            : ConfigItemKinds({kind})
            , Cache(cache)
        {
        }

        TEvGetConfigRequest(TVector<ui32> kinds, bool cache = true)
            : ConfigItemKinds(std::move(kinds))
            , Cache(cache)
        {
        }

        TVector<ui32> ConfigItemKinds;
        bool Cache;
    };

    struct TEvGetConfigResponse : public TEventLocal<TEvGetConfigResponse, EvGetConfigResponse> {
        std::shared_ptr<const NKikimrConfig::TAppConfig> Config;
    };
};

/**
 * Initial config is used to initilize Configs Dispatcher. All received configs
 * are compared to the current one and notifications are not sent to local
 * subscribers if there is no config modification detected.
 */
IActor *CreateConfigsDispatcher(const NConfig::TConfigsDispatcherInitInfo& initInfo);

inline TActorId MakeConfigsDispatcherID(ui32 node = 0) {
    char x[12] = { 'c', 'o', 'n', 'f', 'i', 'g', 's', 'd', 'i', 's', 'p' };
    return TActorId(node, TStringBuf(x, 12));
}

} // namespace NKikimr::NConsole

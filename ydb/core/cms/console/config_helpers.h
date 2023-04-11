#pragma once
#include "defs.h"

namespace NKikimr::NConsole {

/**
 * Node config courier is used to get full or partial node config from
 * Console. You can specify tenant to get config for. If tenant is
 * not specified then current tenant is received from Tenant Pool.
 * Node type is always received from Tenant Pool.
 *
 * Courier forwards TEvConsole::TEvGetNodeConfigResponse event to the
 * owner.
 */
IActor *CreateNodeConfigCourier(TActorId owner,
                                ui64 cookie = 0);
IActor *CreateNodeConfigCourier(ui32 configItemKind,
                                TActorId owner,
                                ui64 cookie = 0);
IActor *CreateNodeConfigCourier(const TVector<ui32> &configItemKinds,
                                TActorId owner,
                                ui64 cookie = 0);
IActor *CreateNodeConfigCourier(const TString &tenant,
                                TActorId owner,
                                ui64 cookie = 0);
IActor *CreateNodeConfigCourier(ui32 configItemKind,
                                const TString &tenant,
                                TActorId owner,
                                ui64 cookie = 0);
IActor *CreateNodeConfigCourier(const TVector<ui32> &configItemKinds,
                                const TString &tenant,
                                TActorId owner,
                                ui64 cookie = 0);

/*
 * Config subscriber is used to add/refresh config subscription in Console
 * using TEvConsole::TEvReplaceSubscriptionsRequest. This helper actor
 * should be used by tablets and services which use only single config
 * subscription.
 * You can pass subscription tenant to subscriber or let subscriber get it
 * from Tenant Pool. Node type is always received from Tenant Pool.
 * If replace option is set TEvConsole::TEvReplaceConfigSubscriptionsRequest
 * is used for subscription and all existing subscriber's subscriptions
 * are removed. Otherwise TEvConsole::TEvAddConfigSubscriptionRequest
 * is used.
 *
 * Subscriber forwards TEvConsole::TEvAddConfigSubscriptionResponse and
 * TEvConsole::TEvReplaceConfigSubscriptionsResponse event to the owner.
 */
IActor *CreateConfigSubscriber(ui64 tabletId,
                               const TVector<ui32> &configItemKinds,
                               TActorId owner,
                               bool replace = true,
                               ui64 cookie = 0);
IActor *CreateConfigSubscriber(ui64 tabletId,
                               const TVector<ui32> &configItemKinds,
                               const TString &tenant,
                               TActorId owner,
                               bool replace = true,
                               ui64 cookie = 0);
IActor *CreateConfigSubscriber(TActorId serviceId,
                               const TVector<ui32> &configItemKinds,
                               TActorId owner,
                               bool replace = true,
                               ui64 cookie = 0);
IActor *CreateConfigSubscriber(TActorId serviceId,
                               const TVector<ui32> &configItemKinds,
                               const TString &tenant,
                               TActorId owner,
                               bool replace = true,
                               ui64 cookie = 0);

/**
 * These functions will make subscription through configs dispatcher
 * Those subscriptions handle both yaml and non-yaml configs (not in same subscriprion)
 * handle all deduplication, and reconnects
 * internally new configs dispatcher uses InMemorySubscriprion's
 */
void SubscribeViaConfigDispatcher(const TActorContext &ctx,
                                  const TVector<ui32> &configItemKinds,
                                  TActorId owner,
                                  ui64 cookie = 0);
void UnsubscribeViaConfigDispatcher(const TActorContext &ctx,
                                    TActorId owner,
                                    ui64 cookie = 0);

/**
 * Subscription eraser is used to remove config subscriptions by ID. If owner is
 * specified then TEvConsole::TEvRemoveConfigSubscriptionRepsonse event is
 * forwared to it.
 */
IActor *CreateSubscriptionEraser(ui64 subscriptionId,
                                 TActorId owner = TActorId(),
                                 ui64 cookie = 0);

} // namespace NKikimr::NConsole

#pragma once

#include "defs.h"

#include "configs_config.h"
#include "console.h"
#include "logger.h"
#include "tx_processor.h"
#include "console_configs_provider.h"
#include "configs_dispatcher.h"

#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/cms/console/util/config_index.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>

#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/interconnect/interconnect.h>

namespace NKikimr::NConsole {

using NTabletFlatExecutor::ITransaction;
using NTabletFlatExecutor::TTransactionBase;
using NTabletFlatExecutor::TTransactionContext;

class TConsole;

class TConfigsManager : public TActorBootstrapped<TConfigsManager> {
private:
    using TBase = TActorBootstrapped<TConfigsManager>;

public:
    struct TEvPrivate {
        enum EEv {
            EvStateLoaded = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
            EvCleanupSubscriptions,
            EvCleanupLog,
            EvEnd
        };

        static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_PRIVATE)");

        struct TEvStateLoaded : public TEventLocal<TEvStateLoaded, EvStateLoaded> {};

        struct TEvCleanupSubscriptions : public TEventLocal<TEvCleanupSubscriptions, EvCleanupSubscriptions> {};

        struct TEvCleanupLog : public TEventLocal<TEvCleanupLog, EvCleanupLog> {};
    };

public:
    void ClearState();
    void SetConfig(const NKikimrConsole::TConfigsConfig &config);
    // Check if specified config may be applied to configs manager.
    bool CheckConfig(const NKikimrConsole::TConfigsConfig &config,
                     Ydb::StatusIds::StatusCode &code,
                     TString &error);

    void ApplyPendingConfigModifications(const TActorContext &ctx,
                                         TAutoPtr<IEventHandle> ev = nullptr);
    void ApplyPendingSubscriptionModifications(const TActorContext &ctx,
                                               TAutoPtr<IEventHandle> ev = nullptr);

    bool MakeNewSubscriptionChecks(TSubscription::TPtr subscription,
                                   Ydb::StatusIds::StatusCode &code,
                                   TString &error);

    bool IsConfigItemScopeAllowed(TConfigItem::TPtr item);
    bool IsConfigItemScopeAllowed(TConfigItem::TPtr item,
                                  const TConfigsConfig &config);
    bool IsSupportedMergeStrategy(ui32 value) const;

    void DumpStateHTML(IOutputStream &os) const;

    // Database functions
    void DbApplyPendingConfigModifications(TTransactionContext &txc,
                                           const TActorContext &ctx) const;
    void DbApplyPendingSubscriptionModifications(TTransactionContext &txc,
                                                 const TActorContext &ctx) const;
    bool DbLoadState(TTransactionContext &txc,
                     const TActorContext &ctx);
    void DbRemoveItem(ui64 id,
                      TTransactionContext &txc,
                      const TActorContext &ctx) const;
    void DbRemoveSubscription(ui64 id,
                              TTransactionContext &txc,
                              const TActorContext &ctx) const;
    void DbUpdateItem(TConfigItem::TPtr item,
                      TTransactionContext &txc,
                      const TActorContext &ctx) const;
    void DbUpdateNextConfigItemId(TTransactionContext &txc,
                                  const TActorContext &ctx) const;
    void DbUpdateNextSubscriptionId(TTransactionContext &txc,
                                    const TActorContext &ctx) const;
    void DbUpdateSubscription(TSubscription::TPtr subscription,
                              TTransactionContext &txc,
                              const TActorContext &ctx) const;
    void DbUpdateSubscriptionLastProvidedConfig(ui64 id,
                                                const TConfigId &configId,
                                                TTransactionContext &txc,
                                                const TActorContext &ctx) const;

private:
    class TTxAddConfigSubscription;
    class TTxCleanupSubscriptions;
    class TTxConfigure;
    class TTxRemoveConfigSubscription;
    class TTxRemoveConfigSubscriptions;
    class TTxReplaceConfigSubscriptions;
    class TTxToggleConfigValidator;
    class TTxUpdateLastProvidedConfig;
    class TTxGetLogTail;
    class TTxLogCleanup;
    class TTxReplaceYamlConfig;
    class TTxDropYamlConfig;
    class TTxGetYamlConfig;
    class TTxGetYamlMetadata;

    ITransaction *CreateTxAddConfigSubscription(TEvConsole::TEvAddConfigSubscriptionRequest::TPtr &ev);
    ITransaction *CreateTxCleanupSubscriptions(TEvInterconnect::TEvNodesInfo::TPtr &ev);
    ITransaction *CreateTxConfigure(TEvConsole::TEvConfigureRequest::TPtr &ev);
    ITransaction *CreateTxRemoveConfigSubscription(TEvConsole::TEvRemoveConfigSubscriptionRequest::TPtr &ev);
    ITransaction *CreateTxRemoveConfigSubscriptions(TEvConsole::TEvRemoveConfigSubscriptionsRequest::TPtr &ev);
    ITransaction *CreateTxReplaceConfigSubscriptions(TEvConsole::TEvReplaceConfigSubscriptionsRequest::TPtr &ev);
    ITransaction *CreateTxToggleConfigValidator(TEvConsole::TEvToggleConfigValidatorRequest::TPtr &ev);
    ITransaction *CreateTxUpdateLastProvidedConfig(TEvConsole::TEvConfigNotificationResponse::TPtr &ev);
    ITransaction *CreateTxGetLogTail(TEvConsole::TEvGetLogTailRequest::TPtr &ev);
    ITransaction *CreateTxLogCleanup();
    ITransaction *CreateTxReplaceYamlConfig(TEvConsole::TEvReplaceYamlConfigRequest::TPtr &ev);
    ITransaction *CreateTxSetYamlConfig(TEvConsole::TEvSetYamlConfigRequest::TPtr &ev);
    ITransaction *CreateTxDropYamlConfig(TEvConsole::TEvDropConfigRequest::TPtr &ev);
    ITransaction *CreateTxGetYamlConfig(TEvConsole::TEvGetAllConfigsRequest::TPtr &ev);
    ITransaction *CreateTxGetYamlMetadata(TEvConsole::TEvGetAllMetadataRequest::TPtr &ev);

    void Handle(TEvConsole::TEvAddConfigSubscriptionRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvConsole::TEvConfigNotificationResponse::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvConsole::TEvConfigureRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvConsole::TEvConfigNotificationRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvConsole::TEvListConfigValidatorsRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvConsole::TEvRemoveConfigSubscriptionRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvConsole::TEvRemoveConfigSubscriptionsRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvConsole::TEvReplaceConfigSubscriptionsRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvConsole::TEvToggleConfigValidatorRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvConsole::TEvGetLogTailRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvConsole::TEvGetNodeLabelsRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvConsole::TEvResolveConfigRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvConsole::TEvResolveAllConfigRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvConsole::TEvIsYamlReadOnlyRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvConsole::TEvGetAllConfigsRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvConsole::TEvGetAllMetadataRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvConsole::TEvAddVolatileConfigRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvConsole::TEvRemoveVolatileConfigRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvInterconnect::TEvNodesInfo::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvConsole::TEvReplaceYamlConfigRequest::TPtr & ev, const TActorContext & ctx);
    void Handle(TEvConsole::TEvSetYamlConfigRequest::TPtr & ev, const TActorContext & ctx);
    void HandleUnauthorized(TEvConsole::TEvReplaceYamlConfigRequest::TPtr & ev, const TActorContext & ctx);
    void HandleUnauthorized(TEvConsole::TEvSetYamlConfigRequest::TPtr & ev, const TActorContext & ctx);
    void Handle(TEvConsole::TEvDropConfigRequest::TPtr & ev, const TActorContext & ctx);
    void Handle(TEvPrivate::TEvStateLoaded::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvPrivate::TEvCleanupSubscriptions::TPtr &ev, const TActorContext &ctx);

    static bool CheckRights(const TString& userToken);

    template <class T>
    void HandleWithRights(T &ev, const TActorContext &ctx) {
        constexpr bool HasHandleUnauthorized = requires(T &ev) {
            HandleUnauthorized(ev, ctx);
        };

        if (CheckRights(ev->Get()->Record.GetUserToken())) {
            Handle(ev, ctx);
        } else {
            if constexpr (HasHandleUnauthorized) {
                HandleUnauthorized(ev, ctx);
            }
            auto req = MakeHolder<TEvConsole::TEvUnauthorized>();
            ctx.Send(ev->Sender, req.Release());
        }
    }

    void ForwardToConfigsProvider(TAutoPtr<IEventHandle> &ev, const TActorContext &ctx);

    void ScheduleSubscriptionsCleanup(const TActorContext &ctx);
    void ScheduleLogCleanup(const TActorContext &ctx);
    void CleanupLog(const TActorContext &ctx);

    STFUNC(StateWork)
    {
        TRACE_EVENT(NKikimrServices::CMS_CONFIGS);
        switch (ev->GetTypeRewrite()) {
            HFuncTraced(TEvConsole::TEvAddConfigSubscriptionRequest, Handle);
            FFunc(TEvConsole::EvCheckConfigUpdatesRequest, ForwardToConfigsProvider);
            HFunc(TEvConsole::TEvGetLogTailRequest, Handle);
            HFuncTraced(TEvConsole::TEvConfigNotificationResponse, Handle);
            HFuncTraced(TEvConsole::TEvConfigureRequest, Handle);
            HFunc(TEvConsole::TEvResolveConfigRequest, Handle);
            HFunc(TEvConsole::TEvResolveAllConfigRequest, Handle);
            HFunc(TEvConsole::TEvConfigNotificationRequest, Handle);
            HFunc(TEvConsole::TEvIsYamlReadOnlyRequest, Handle);
            HFunc(TEvConsole::TEvGetAllConfigsRequest, HandleWithRights);
            HFunc(TEvConsole::TEvGetNodeLabelsRequest, HandleWithRights);
            HFunc(TEvConsole::TEvGetAllMetadataRequest, HandleWithRights);
            HFunc(TEvConsole::TEvAddVolatileConfigRequest, HandleWithRights);
            HFunc(TEvConsole::TEvRemoveVolatileConfigRequest, HandleWithRights);
            FFunc(TEvConsole::EvGetConfigItemsRequest, ForwardToConfigsProvider);
            HFuncTraced(TEvConsole::TEvReplaceYamlConfigRequest, HandleWithRights);
            HFuncTraced(TEvConsole::TEvSetYamlConfigRequest, HandleWithRights);
            HFuncTraced(TEvConsole::TEvDropConfigRequest, HandleWithRights);
            FFunc(TEvConsole::EvGetConfigSubscriptionRequest, ForwardToConfigsProvider);
            FFunc(TEvConsole::EvGetNodeConfigItemsRequest, ForwardToConfigsProvider);
            FFunc(TEvConsole::EvGetNodeConfigRequest, ForwardToConfigsProvider);
            FFunc(TEvConsole::EvListConfigSubscriptionsRequest, ForwardToConfigsProvider);
            HFuncTraced(TEvConsole::TEvListConfigValidatorsRequest, Handle);
            HFuncTraced(TEvConsole::TEvRemoveConfigSubscriptionRequest, Handle);
            HFuncTraced(TEvConsole::TEvRemoveConfigSubscriptionsRequest, Handle);
            HFuncTraced(TEvConsole::TEvReplaceConfigSubscriptionsRequest, Handle);
            HFuncTraced(TEvConsole::TEvToggleConfigValidatorRequest, Handle);
            HFuncTraced(TEvInterconnect::TEvNodesInfo, Handle);
            HFuncTraced(TEvPrivate::TEvCleanupSubscriptions, Handle);
            HFuncTraced(TEvPrivate::TEvStateLoaded, Handle);
            FFunc(TEvConsole::EvConfigSubscriptionRequest, ForwardToConfigsProvider);
            FFunc(TEvConsole::EvConfigSubscriptionCanceled, ForwardToConfigsProvider);
            CFunc(TEvPrivate::EvCleanupLog, CleanupLog);
            IgnoreFunc(TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse);

        default:
            Y_ABORT("TConfigsManager::StateWork unexpected event type: %" PRIx32 " event: %s",
                   ev->GetTypeRewrite(), ev->ToString().data());
        }
    }

public:
    TConfigsManager(TConsole &self)
        : Self(self)
    {
    }

    ~TConfigsManager()
    {
        ClearState();
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType()
    {
        return NKikimrServices::TActivity::CMS_CONFIGS_MANAGER;
    }

    void Bootstrap(const TActorContext &ctx);
    void Detach();

private:
    TConsole &Self;
    TConfigsConfig Config;
    // All config items by id.
    TConfigIndex ConfigIndex;
    ui64 NextConfigItemId;
    TConfigModifications PendingConfigModifications;
    // Validators state.
    THashSet<TString> DisabledValidators;
    // Subscriptions.
    TSubscriptionIndex SubscriptionIndex;
    THashMap<TSubscriberId, THashSet<ui64>> SubscriptionsBySubscriber;
    ui64 NextSubscriptionId;
    TSubscriptionModifications PendingSubscriptionModifications;
    TSchedulerCookieHolder SubscriptionsCleanupTimerCookieHolder;

    TActorId ConfigsProvider;
    TTxProcessor::TPtr TxProcessor;
    TLogger Logger;
    TSchedulerCookieHolder LogCleanupTimerCookieHolder;

    TString ClusterName;
    ui32 YamlVersion = 0;
    TString YamlConfig;
    bool YamlDropped = false;
    bool YamlReadOnly = true;
    TMap<ui64, TString> VolatileYamlConfigs;
};

} // namespace NKikimr::NConsole

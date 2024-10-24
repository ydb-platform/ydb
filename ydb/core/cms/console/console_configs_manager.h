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

    ITransaction *CreateTxAddConfigSubscription(NEvConsole::TEvAddConfigSubscriptionRequest::TPtr &ev);
    ITransaction *CreateTxCleanupSubscriptions(TEvInterconnect::TEvNodesInfo::TPtr &ev);
    ITransaction *CreateTxConfigure(NEvConsole::TEvConfigureRequest::TPtr &ev);
    ITransaction *CreateTxRemoveConfigSubscription(NEvConsole::TEvRemoveConfigSubscriptionRequest::TPtr &ev);
    ITransaction *CreateTxRemoveConfigSubscriptions(NEvConsole::TEvRemoveConfigSubscriptionsRequest::TPtr &ev);
    ITransaction *CreateTxReplaceConfigSubscriptions(NEvConsole::TEvReplaceConfigSubscriptionsRequest::TPtr &ev);
    ITransaction *CreateTxToggleConfigValidator(NEvConsole::TEvToggleConfigValidatorRequest::TPtr &ev);
    ITransaction *CreateTxUpdateLastProvidedConfig(NEvConsole::TEvConfigNotificationResponse::TPtr &ev);
    ITransaction *CreateTxGetLogTail(NEvConsole::TEvGetLogTailRequest::TPtr &ev);
    ITransaction *CreateTxLogCleanup();
    ITransaction *CreateTxReplaceYamlConfig(NEvConsole::TEvReplaceYamlConfigRequest::TPtr &ev);
    ITransaction *CreateTxSetYamlConfig(NEvConsole::TEvSetYamlConfigRequest::TPtr &ev);
    ITransaction *CreateTxDropYamlConfig(NEvConsole::TEvDropConfigRequest::TPtr &ev);
    ITransaction *CreateTxGetYamlConfig(NEvConsole::TEvGetAllConfigsRequest::TPtr &ev);
    ITransaction *CreateTxGetYamlMetadata(NEvConsole::TEvGetAllMetadataRequest::TPtr &ev);

    void Handle(NEvConsole::TEvAddConfigSubscriptionRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(NEvConsole::TEvConfigNotificationResponse::TPtr &ev, const TActorContext &ctx);
    void Handle(NEvConsole::TEvConfigureRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(NEvConsole::TEvConfigNotificationRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(NEvConsole::TEvListConfigValidatorsRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(NEvConsole::TEvRemoveConfigSubscriptionRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(NEvConsole::TEvRemoveConfigSubscriptionsRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(NEvConsole::TEvReplaceConfigSubscriptionsRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(NEvConsole::TEvToggleConfigValidatorRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(NEvConsole::TEvGetLogTailRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(NEvConsole::TEvGetNodeLabelsRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(NEvConsole::TEvResolveConfigRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(NEvConsole::TEvResolveAllConfigRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(NEvConsole::TEvIsYamlReadOnlyRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(NEvConsole::TEvGetAllConfigsRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(NEvConsole::TEvGetAllMetadataRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(NEvConsole::TEvAddVolatileConfigRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(NEvConsole::TEvRemoveVolatileConfigRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvInterconnect::TEvNodesInfo::TPtr &ev, const TActorContext &ctx);
    void Handle(NEvConsole::TEvReplaceYamlConfigRequest::TPtr & ev, const TActorContext & ctx);
    void Handle(NEvConsole::TEvSetYamlConfigRequest::TPtr & ev, const TActorContext & ctx);
    void HandleUnauthorized(NEvConsole::TEvReplaceYamlConfigRequest::TPtr & ev, const TActorContext & ctx);
    void HandleUnauthorized(NEvConsole::TEvSetYamlConfigRequest::TPtr & ev, const TActorContext & ctx);
    void Handle(NEvConsole::TEvDropConfigRequest::TPtr & ev, const TActorContext & ctx);
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
            auto req = MakeHolder<NEvConsole::TEvUnauthorized>();
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
            HFuncTraced(NEvConsole::TEvAddConfigSubscriptionRequest, Handle);
            FFunc(NEvConsole::EvCheckConfigUpdatesRequest, ForwardToConfigsProvider);
            HFunc(NEvConsole::TEvGetLogTailRequest, Handle);
            HFuncTraced(NEvConsole::TEvConfigNotificationResponse, Handle);
            HFuncTraced(NEvConsole::TEvConfigureRequest, Handle);
            HFunc(NEvConsole::TEvResolveConfigRequest, Handle);
            HFunc(NEvConsole::TEvResolveAllConfigRequest, Handle);
            HFunc(NEvConsole::TEvConfigNotificationRequest, Handle);
            HFunc(NEvConsole::TEvIsYamlReadOnlyRequest, Handle);
            HFunc(NEvConsole::TEvGetAllConfigsRequest, HandleWithRights);
            HFunc(NEvConsole::TEvGetNodeLabelsRequest, HandleWithRights);
            HFunc(NEvConsole::TEvGetAllMetadataRequest, HandleWithRights);
            HFunc(NEvConsole::TEvAddVolatileConfigRequest, HandleWithRights);
            HFunc(NEvConsole::TEvRemoveVolatileConfigRequest, HandleWithRights);
            FFunc(NEvConsole::EvGetConfigItemsRequest, ForwardToConfigsProvider);
            HFuncTraced(NEvConsole::TEvReplaceYamlConfigRequest, HandleWithRights);
            HFuncTraced(NEvConsole::TEvSetYamlConfigRequest, HandleWithRights);
            HFuncTraced(NEvConsole::TEvDropConfigRequest, HandleWithRights);
            FFunc(NEvConsole::EvGetConfigSubscriptionRequest, ForwardToConfigsProvider);
            FFunc(NEvConsole::EvGetNodeConfigItemsRequest, ForwardToConfigsProvider);
            FFunc(NEvConsole::EvGetNodeConfigRequest, ForwardToConfigsProvider);
            FFunc(NEvConsole::EvListConfigSubscriptionsRequest, ForwardToConfigsProvider);
            HFuncTraced(NEvConsole::TEvListConfigValidatorsRequest, Handle);
            HFuncTraced(NEvConsole::TEvRemoveConfigSubscriptionRequest, Handle);
            HFuncTraced(NEvConsole::TEvRemoveConfigSubscriptionsRequest, Handle);
            HFuncTraced(NEvConsole::TEvReplaceConfigSubscriptionsRequest, Handle);
            HFuncTraced(NEvConsole::TEvToggleConfigValidatorRequest, Handle);
            HFuncTraced(TEvInterconnect::TEvNodesInfo, Handle);
            HFuncTraced(TEvPrivate::TEvCleanupSubscriptions, Handle);
            HFuncTraced(TEvPrivate::TEvStateLoaded, Handle);
            FFunc(NEvConsole::EvConfigSubscriptionRequest, ForwardToConfigsProvider);
            FFunc(NEvConsole::EvConfigSubscriptionCanceled, ForwardToConfigsProvider);
            CFunc(TEvPrivate::EvCleanupLog, CleanupLog);
            IgnoreFunc(NEvConfigsDispatcher::TEvSetConfigSubscriptionResponse);

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

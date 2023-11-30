#pragma once

#include "console.h"
#include "console__scheme.h"
#include "tx_processor.h"

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/location.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/engine/minikql/flat_local_tx_factory.h>
#include <ydb/core/mind/tenant_slot_broker.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>

#include <ydb/library/yql/public/issue/protos/issue_severity.pb.h>

#include <ydb/library/actors/core/hfunc.h>

#include <util/generic/set.h>

namespace NKikimr::NConsole {

using NTabletFlatExecutor::ITransaction;
using NTabletFlatExecutor::TTransactionBase;
using NTabletFlatExecutor::TTabletExecutedFlat;
using ::NMonitoring::TDynamicCounterPtr;

class TConfigsManager;
class TTenantsManager;

class TConsole : public TActor<TConsole>
               , public TTabletExecutedFlat
               , public ITxExecutor {
    using TActorBase = TActor<TConsole>;

public:
    enum EConfigKey {
        ConfigKeyConfig = 1,
        ConfigKeyVersion,
        ConfigKeyNextConfigItemId,
        ConfigKeyNextTxId,
        ConfigKeyNextSubscriptionId,
        ConfigKeyNextLogItemId,
        ConfigKeyMinLogItemId,
    };

private:
    class TTxInitScheme;
    class TTxLoadState;
    class TTxSetConfig;

    ITransaction *CreateTxInitScheme();
    ITransaction *CreateTxLoadState();
    ITransaction *CreateTxSetConfig(TEvConsole::TEvSetConfigRequest::TPtr &ev);

    void DefaultSignalTabletActive(const TActorContext &ctx) override;
    void OnActivateExecutor(const TActorContext &ctx) override;
    void OnDetach(const TActorContext &ctx) override;
    void OnTabletDead(TEvTablet::TEvTabletDead::TPtr &ev, const TActorContext &ctx) override;
    void Enqueue(STFUNC_SIG) override;
    bool OnRenderAppHtmlPage(NMon::TEvRemoteHttpInfo::TPtr ev, const TActorContext &ctx) override;

    void Cleanup(const TActorContext &ctx);
    void Die(const TActorContext &ctx) override;

    void LoadConfigFromProto(const NKikimrConsole::TConfig &config);
    void ProcessEnqueuedEvents(const TActorContext &ctx);

    void ClearState();

    void ForwardToConfigsManager(TAutoPtr<IEventHandle> &ev, const TActorContext &ctx);
    void ForwardToTenantsManager(TAutoPtr<IEventHandle> &ev, const TActorContext &ctx);
    void Handle(TEvConsole::TEvGetConfigRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvConsole::TEvSetConfigRequest::TPtr &ev, const TActorContext &ctx);

    STFUNC(StateInit)
    {
        StateInitImpl(ev, SelfId());
    }

    STFUNC(StateWork)
    {
        TRACE_EVENT(NKikimrServices::CMS);
        switch (ev->GetTypeRewrite()) {
            FFunc(TEvConsole::EvConfigSubscriptionRequest, ForwardToConfigsManager);
            FFunc(TEvConsole::EvConfigSubscriptionCanceled, ForwardToConfigsManager);
            FFunc(TEvConsole::EvAddConfigSubscriptionRequest, ForwardToConfigsManager);
            FFunc(TEvConsole::EvAlterTenantRequest, ForwardToTenantsManager);
            FFunc(TEvConsole::EvCheckConfigUpdatesRequest, ForwardToConfigsManager);
            FFunc(TEvConsole::EvConfigNotificationResponse, ForwardToConfigsManager);
            FFunc(TEvConsole::EvIsYamlReadOnlyRequest, ForwardToConfigsManager);
            FFunc(TEvConsole::EvConfigureRequest, ForwardToConfigsManager);
            FFunc(TEvConsole::EvGetAllConfigsRequest, ForwardToConfigsManager);
            FFunc(TEvConsole::EvGetAllMetadataRequest, ForwardToConfigsManager);
            FFunc(TEvConsole::EvAddVolatileConfigRequest, ForwardToConfigsManager);
            FFunc(TEvConsole::EvRemoveVolatileConfigRequest, ForwardToConfigsManager);
            FFunc(TEvConsole::EvGetLogTailRequest, ForwardToConfigsManager);
            FFunc(TEvConsole::EvCreateTenantRequest, ForwardToTenantsManager);
            FFunc(TEvConsole::EvDescribeTenantOptionsRequest, ForwardToTenantsManager);
            FFunc(TEvConsole::EvGetConfigItemsRequest, ForwardToConfigsManager);
            HFuncTraced(TEvConsole::TEvGetConfigRequest, Handle);
            FFunc(TEvConsole::EvReplaceYamlConfigRequest, ForwardToConfigsManager);
            FFunc(TEvConsole::EvGetNodeLabelsRequest, ForwardToConfigsManager);
            FFunc(TEvConsole::EvSetYamlConfigRequest, ForwardToConfigsManager);
            FFunc(TEvConsole::EvDropConfigRequest, ForwardToConfigsManager);
            FFunc(TEvConsole::EvResolveConfigRequest, ForwardToConfigsManager);
            FFunc(TEvConsole::EvResolveAllConfigRequest, ForwardToConfigsManager);
            FFunc(TEvConsole::EvGetConfigSubscriptionRequest, ForwardToConfigsManager);
            FFunc(TEvConsole::EvGetNodeConfigItemsRequest, ForwardToConfigsManager);
            FFunc(TEvConsole::EvGetNodeConfigRequest, ForwardToConfigsManager);
            FFunc(TEvConsole::EvGetOperationRequest, ForwardToTenantsManager);
            FFunc(TEvConsole::EvGetTenantStatusRequest, ForwardToTenantsManager);
            FFunc(TEvConsole::EvListConfigSubscriptionsRequest, ForwardToConfigsManager);
            FFunc(TEvConsole::EvListConfigValidatorsRequest, ForwardToConfigsManager);
            FFunc(TEvConsole::EvListTenantsRequest, ForwardToTenantsManager);
            FFunc(TEvConsole::EvNotifyOperationCompletionRequest, ForwardToTenantsManager);
            FFunc(TEvConsole::EvRemoveConfigSubscriptionRequest, ForwardToConfigsManager);
            FFunc(TEvConsole::EvRemoveConfigSubscriptionsRequest, ForwardToConfigsManager);
            FFunc(TEvConsole::EvRemoveTenantRequest, ForwardToTenantsManager);
            FFunc(TEvConsole::EvReplaceConfigSubscriptionsRequest, ForwardToConfigsManager);
            HFuncTraced(TEvConsole::TEvSetConfigRequest, Handle);
            FFunc(TEvConsole::EvToggleConfigValidatorRequest, ForwardToConfigsManager);
            FFunc(TEvConsole::EvUpdateTenantPoolConfig, ForwardToTenantsManager);
            IgnoreFunc(TEvTabletPipe::TEvServerConnected);
            IgnoreFunc(TEvTabletPipe::TEvServerDisconnected);

        default:
            if (!HandleDefaultEvents(ev, SelfId())) {
                LOG_CRIT_S(*TlsActivationContext, NKikimrServices::CMS,
                           "TConsole::StateWork unexpected event type: " << ev->GetTypeRewrite()
                           << " event: " << ev->ToString());
            }
        }
    }

public:
    TConsole(const TActorId &tablet, TTabletStorageInfo *info)
        : TActor(&TThis::StateInit)
        , TTabletExecutedFlat(info, tablet, new NMiniKQL::TMiniKQLFactory)
        , TxProcessor(new TTxProcessor(*this, "console", NKikimrServices::CMS))
        , ConfigsManager(nullptr)
        , TenantsManager(nullptr)
    {
    }

    ~TConsole()
    {
        TxProcessor->Clear();
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::CMS_SERVICE;
    }

    void Execute(ITransaction *transaction, const TActorContext &ctx) override
    {
        TTabletExecutedFlat::Execute(transaction, ctx);
    }

    TTxProcessor::TPtr GetTxProcessor() const
    {
        return TxProcessor;
    }

    const NKikimrConsole::TConfig &GetConfig() const
    {
        return Config;
    }

private:
    TDeque<TAutoPtr<IEventHandle>> InitQueue;
    NKikimrConsole::TConfig Config;
    TTxProcessor::TPtr TxProcessor;
    TDynamicCounterPtr Counters;

    TConfigsManager* ConfigsManager;
    TTenantsManager* TenantsManager;

    TActorId NetClassifierUpdaterId;
};

} // namespace NKikimr::NConsole

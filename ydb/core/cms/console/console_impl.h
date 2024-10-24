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
    ITransaction *CreateTxSetConfig(NEvConsole::TEvSetConfigRequest::TPtr &ev);

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
    void Handle(NEvConsole::TEvGetConfigRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(NEvConsole::TEvSetConfigRequest::TPtr &ev, const TActorContext &ctx);

    STFUNC(StateInit)
    {
        StateInitImpl(ev, SelfId());
    }

    STFUNC(StateWork)
    {
        TRACE_EVENT(NKikimrServices::CMS);
        switch (ev->GetTypeRewrite()) {
            FFunc(NEvConsole::EvConfigSubscriptionRequest, ForwardToConfigsManager);
            FFunc(NEvConsole::EvConfigSubscriptionCanceled, ForwardToConfigsManager);
            FFunc(NEvConsole::EvAddConfigSubscriptionRequest, ForwardToConfigsManager);
            FFunc(NEvConsole::EvAlterTenantRequest, ForwardToTenantsManager);
            FFunc(NEvConsole::EvCheckConfigUpdatesRequest, ForwardToConfigsManager);
            FFunc(NEvConsole::EvConfigNotificationResponse, ForwardToConfigsManager);
            FFunc(NEvConsole::EvIsYamlReadOnlyRequest, ForwardToConfigsManager);
            FFunc(NEvConsole::EvConfigureRequest, ForwardToConfigsManager);
            FFunc(NEvConsole::EvGetAllConfigsRequest, ForwardToConfigsManager);
            FFunc(NEvConsole::EvGetAllMetadataRequest, ForwardToConfigsManager);
            FFunc(NEvConsole::EvAddVolatileConfigRequest, ForwardToConfigsManager);
            FFunc(NEvConsole::EvRemoveVolatileConfigRequest, ForwardToConfigsManager);
            FFunc(NEvConsole::EvGetLogTailRequest, ForwardToConfigsManager);
            FFunc(NEvConsole::EvCreateTenantRequest, ForwardToTenantsManager);
            FFunc(NEvConsole::EvDescribeTenantOptionsRequest, ForwardToTenantsManager);
            FFunc(NEvConsole::EvGetConfigItemsRequest, ForwardToConfigsManager);
            HFuncTraced(NEvConsole::TEvGetConfigRequest, Handle);
            FFunc(NEvConsole::EvReplaceYamlConfigRequest, ForwardToConfigsManager);
            FFunc(NEvConsole::EvGetNodeLabelsRequest, ForwardToConfigsManager);
            FFunc(NEvConsole::EvSetYamlConfigRequest, ForwardToConfigsManager);
            FFunc(NEvConsole::EvDropConfigRequest, ForwardToConfigsManager);
            FFunc(NEvConsole::EvResolveConfigRequest, ForwardToConfigsManager);
            FFunc(NEvConsole::EvResolveAllConfigRequest, ForwardToConfigsManager);
            FFunc(NEvConsole::EvGetConfigSubscriptionRequest, ForwardToConfigsManager);
            FFunc(NEvConsole::EvGetNodeConfigItemsRequest, ForwardToConfigsManager);
            FFunc(NEvConsole::EvGetNodeConfigRequest, ForwardToConfigsManager);
            FFunc(NEvConsole::EvGetOperationRequest, ForwardToTenantsManager);
            FFunc(NEvConsole::EvGetTenantStatusRequest, ForwardToTenantsManager);
            FFunc(NEvConsole::EvListConfigSubscriptionsRequest, ForwardToConfigsManager);
            FFunc(NEvConsole::EvListConfigValidatorsRequest, ForwardToConfigsManager);
            FFunc(NEvConsole::EvListTenantsRequest, ForwardToTenantsManager);
            FFunc(NEvConsole::EvNotifyOperationCompletionRequest, ForwardToTenantsManager);
            FFunc(NEvConsole::EvRemoveConfigSubscriptionRequest, ForwardToConfigsManager);
            FFunc(NEvConsole::EvRemoveConfigSubscriptionsRequest, ForwardToConfigsManager);
            FFunc(NEvConsole::EvRemoveTenantRequest, ForwardToTenantsManager);
            FFunc(NEvConsole::EvReplaceConfigSubscriptionsRequest, ForwardToConfigsManager);
            HFuncTraced(NEvConsole::TEvSetConfigRequest, Handle);
            FFunc(NEvConsole::EvToggleConfigValidatorRequest, ForwardToConfigsManager);
            FFunc(NEvConsole::EvUpdateTenantPoolConfig, ForwardToTenantsManager);
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

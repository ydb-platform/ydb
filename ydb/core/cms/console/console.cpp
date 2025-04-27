#include "console_impl.h"
#include "console_configs_manager.h"
#include "console_tenants_manager.h"
#include "http.h"

#include "net_classifier_updater.h"

#include <ydb/core/base/counters.h>
#include <ydb/core/base/domain.h>
#include <ydb/core/base/feature_flags.h>
#include <ydb/core/cms/console/validators/registry.h>
#include <ydb/core/protos/netclassifier.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <library/cpp/monlib/service/pages/templates.h>

namespace NKikimr::NConsole {

void TConsole::DefaultSignalTabletActive(const TActorContext &)
{
    // must be empty
}

void TConsole::OnActivateExecutor(const TActorContext &ctx)
{
    auto domains = AppData(ctx)->DomainsInfo;

    auto tabletsCounters = GetServiceCounters(AppData(ctx)->Counters, "tablets");
    tabletsCounters->RemoveSubgroup("type", "CONSOLE");
    Counters = tabletsCounters->GetSubgroup("type", "CONSOLE");

    TValidatorsRegistry::Instance()->LockValidators();

    ConfigsManager = new TConfigsManager(*this, Counters);
    ctx.RegisterWithSameMailbox(ConfigsManager);

    TenantsManager = new TTenantsManager(*this, domains->Domain,
                                         Counters,
                                         AppData()->FeatureFlags);
    ctx.RegisterWithSameMailbox(TenantsManager);

    if (AppData(ctx)->NetClassifierConfig.GetUpdaterConfig().GetNetDataSourceUrl()) {
        NetClassifierUpdaterId = ctx.Register(NNetClassifierUpdater::MakeNetClassifierUpdaterActor(SelfId()));
    }

    TxProcessor->ProcessTx(CreateTxInitScheme(), ctx);
}

void TConsole::OnDetach(const TActorContext &ctx)
{
    LOG_DEBUG(ctx, NKikimrServices::CMS, "TConsole::OnDetach");
    Die(ctx);
}

void TConsole::OnTabletDead(TEvTablet::TEvTabletDead::TPtr &, const TActorContext &ctx)
{
    LOG_INFO(ctx, NKikimrServices::CMS, "TConsole::OnTabletDead: %" PRIu64, TabletID());

    if (Counters)
        Counters->ResetCounters();

    Die(ctx);
}

void TConsole::Enqueue(TAutoPtr<IEventHandle> &ev)
{
    LOG_DEBUG(*TlsActivationContext, NKikimrServices::CMS,
              "TConsole::Enqueue: %" PRIu64 ", event type: %" PRIu32 " event: %s",
              TabletID(), ev->GetTypeRewrite(), ev->ToString().data());
    InitQueue.push_back(ev);
}

bool TConsole::OnRenderAppHtmlPage(NMon::TEvRemoteHttpInfo::TPtr ev, const TActorContext &ctx)
{
    if (!ev)
        return true;

    auto domains = AppData(ctx)->DomainsInfo;
    auto domain = domains->Domain;

    TStringStream str;
    HTML(str) {
        NHttp::OutputStyles(str);
        PRE() {
            str << "Served domain: " << domain->Name << Endl;
        }

        COLLAPSED_REF_CONTENT("tenants-div", "Tenants") {
            DIV_CLASS("tab-left") {
                TenantsManager->DumpStateHTML(str);
            }
        }
        str << "<br/>" << Endl;
        COLLAPSED_REF_CONTENT("configs-div", "Configs") {
            DIV_CLASS("tab-left") {
                ConfigsManager->DumpStateHTML(str);
            }
        }
    }
    ctx.Send(ev->Sender, new NMon::TEvRemoteHttpInfoRes(str.Str()));
    return true;
}

void TConsole::Cleanup(const TActorContext &ctx)
{
    LOG_DEBUG(ctx, NKikimrServices::CMS, "TConsole::Cleanup");

    if (ConfigsManager) {
        ConfigsManager->Detach();
        ConfigsManager = nullptr;
    }

    if (TenantsManager) {
        TenantsManager->Detach();
        TenantsManager = nullptr;
    }

    if (NetClassifierUpdaterId) {
        Send(NetClassifierUpdaterId, new TEvents::TEvPoisonPill);
        NetClassifierUpdaterId = {};
    }

    TxProcessor->Clear();
}

void TConsole::Die(const TActorContext &ctx)
{
    Cleanup(ctx);
    TActorBase::Die(ctx);
}

void TConsole::LoadConfigFromProto(const NKikimrConsole::TConfig &config)
{
    Config = config;
    TenantsManager->SetConfig(config.GetTenantsConfig());
    ConfigsManager->SetConfig(config.GetConfigsConfig());
}

void TConsole::ProcessEnqueuedEvents(const TActorContext &ctx)
{
    while (!InitQueue.empty()) {
        TAutoPtr<IEventHandle> &ev = InitQueue.front();
        LOG_DEBUG(ctx, NKikimrServices::CMS,
                  "TConsole::Dequeue: %" PRIu64 ", event type: %" PRIu32 " event: %s",
                  TabletID(), ev->GetTypeRewrite(), ev->ToString().data());
        ctx.ExecutorThread.Send(ev.Release());
        InitQueue.pop_front();
    }
}

void TConsole::ClearState()
{
    Config.Clear();
    if (ConfigsManager) {
        ConfigsManager->ClearState();
    }
    if (TenantsManager) {
        TenantsManager->ClearState();
    }

    Counters->ResetCounters();
}

void TConsole::ForwardToConfigsManager(TAutoPtr<IEventHandle> &ev, const TActorContext &ctx)
{
    ctx.Forward(ev, ConfigsManager->SelfId());
}

void TConsole::ForwardToTenantsManager(TAutoPtr<IEventHandle> &ev, const TActorContext &ctx)
{
    ctx.Forward(ev, TenantsManager->SelfId());
}

void TConsole::Handle(TEvConsole::TEvGetConfigRequest::TPtr &ev, const TActorContext &ctx)
{
    ctx.Send(ev->Sender, new TEvConsole::TEvGetConfigResponse(Config));
}

void TConsole::Handle(TEvConsole::TEvSetConfigRequest::TPtr &ev, const TActorContext &ctx)
{
    TxProcessor->ProcessTx(CreateTxSetConfig(ev), ctx);
}

IActor *CreateConsole(const TActorId &tablet, TTabletStorageInfo *info)
{
    return new TConsole(tablet, info);
}

} // namespace NKikimr::NConsole

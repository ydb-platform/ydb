#include "configs_dispatcher.h"
#include "console.h"
#include "immediate_controls_configurator.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/protos/config.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NConsole {

class TImmediateControlsConfigurator : public TActorBootstrapped<TImmediateControlsConfigurator> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType()
    {
        return NKikimrServices::TActivity::IMMEDITE_CONTROLS_CONFIGURATOR;
    }

    TImmediateControlsConfigurator(TIntrusivePtr<TControlBoard> board,
                                   const NKikimrConfig::TImmediateControlsConfig &cfg,
                                   bool allowExistingControls);

    void Bootstrap(const TActorContext &ctx);

    void Handle(TEvConsole::TEvConfigNotificationRequest::TPtr &ev,
                const TActorContext &ctx);

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvConsole::TEvConfigNotificationRequest, Handle);
            IgnoreFunc(TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse);

        default:
            Y_ABORT("unexpected event type: %" PRIx32 " event: %s",
                   ev->GetTypeRewrite(), ev->ToString().data());
            break;
        }
    }

private:
    void CreateControls(TIntrusivePtr<TControlBoard> board, bool allowExisting);
    void ApplyConfig(const NKikimrConfig::TImmediateControlsConfig &cfg,
                     TIntrusivePtr<TControlBoard> board);
};

TImmediateControlsConfigurator::TImmediateControlsConfigurator(TIntrusivePtr<TControlBoard> board,
                                                               const NKikimrConfig::TImmediateControlsConfig &cfg,
                                                               bool allowExistingControls)
{
    CreateControls(board, allowExistingControls);
    ApplyConfig(cfg, board);
}

void TImmediateControlsConfigurator::Bootstrap(const TActorContext &ctx)
{
    LOG_DEBUG_S(ctx, NKikimrServices::CMS_CONFIGS,
                "TImmediateControlsConfigurator Bootstrap");

    Become(&TThis::StateWork);

    LOG_DEBUG_S(ctx, NKikimrServices::CMS_CONFIGS,
                "TImmediateControlsConfigurator: subscribe for config updates.");

    ui32 item = (ui32)NKikimrConsole::TConfigItem::ImmediateControlsConfigItem;
    ctx.Send(MakeConfigsDispatcherID(SelfId().NodeId()),
             new TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest(item));
}

void TImmediateControlsConfigurator::Handle(TEvConsole::TEvConfigNotificationRequest::TPtr &ev,
                                            const TActorContext &ctx)
{
    auto &rec = ev->Get()->Record;

    LOG_INFO_S(ctx, NKikimrServices::CMS_CONFIGS,
               "TImmediateControlsConfigurator: got new config: "
               << rec.GetConfig().ShortDebugString());

    ApplyConfig(rec.GetConfig().GetImmediateControlsConfig(), AppData(ctx)->Icb);

    auto resp = MakeHolder<TEvConsole::TEvConfigNotificationResponse>(rec);

    LOG_TRACE_S(ctx, NKikimrServices::CMS_CONFIGS,
                "TImmediateControlsConfigurator: Send TEvConfigNotificationResponse: "
                << resp->Record.ShortDebugString());

    ctx.Send(ev->Sender, resp.Release(), 0, ev->Cookie);
}

void TImmediateControlsConfigurator::CreateControls(TIntrusivePtr<TControlBoard> board, bool allowExisting)
{
    board->CreateConfigControls(allowExisting);
}


void TImmediateControlsConfigurator::ApplyConfig(const NKikimrConfig::TImmediateControlsConfig &cfg,
                                                 TIntrusivePtr<TControlBoard> board)
{
    board->UpdateControls(cfg);
}


IActor *CreateImmediateControlsConfigurator(TIntrusivePtr<TControlBoard> board,
                                            const NKikimrConfig::TImmediateControlsConfig &cfg,
                                            bool allowExistingControls)
{
    return new TImmediateControlsConfigurator(board, cfg, allowExistingControls);
}

} // namespace NKikimr::NConsole

#include "configs_dispatcher.h"
#include "console.h"
#include "log_settings_configurator.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <util/system/file.h>
#include <util/system/fs.h>
#include <util/stream/file.h>
#include <google/protobuf/text_format.h>
#include <ydb/core/cms/console/grpc_library_helper.h>

namespace NKikimr::NConsole {

class TLogSettingsConfigurator : public TActorBootstrapped<TLogSettingsConfigurator> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType()
    {
        return NKikimrServices::TActivity::LOG_SETTINGS_CONFIGURATOR;
    }

    TLogSettingsConfigurator();
    TLogSettingsConfigurator(const TString &pathToConfigCacheFile);

    void SaveLogSettingsConfigToCache(const NKikimrConfig::TLogConfig &logConfig,
                                      const TActorContext &ctx);

    void Bootstrap(const TActorContext &ctx);

    void Handle(TEvConsole::TEvConfigNotificationRequest::TPtr &ev,
                const TActorContext &ctx);

    void ApplyLogConfig(const NKikimrConfig::TLogConfig &config,
                        const TActorContext &ctx);
    TVector<NLog::TComponentSettings>
    ComputeComponentSettings(const NKikimrConfig::TLogConfig &config,
                             const TActorContext &ctx);
    void ApplyComponentSettings(const TVector<NLog::TComponentSettings> &settings,
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
    TString PathToConfigCacheFile;
};

TLogSettingsConfigurator::TLogSettingsConfigurator()
{
}

TLogSettingsConfigurator::TLogSettingsConfigurator(const TString &pathToConfigCacheFile)
{
    PathToConfigCacheFile = pathToConfigCacheFile;
}

void TLogSettingsConfigurator::Bootstrap(const TActorContext &ctx)
{
    LOG_DEBUG_S(ctx, NKikimrServices::CMS_CONFIGS,
                "TLogSettingsConfigurator Bootstrap");

    Become(&TThis::StateWork);

    LOG_DEBUG_S(ctx, NKikimrServices::CMS_CONFIGS,
                "TLogSettingsConfigurator: subscribe for config updates.");

    ui32 item = (ui32)NKikimrConsole::TConfigItem::LogConfigItem;
    ctx.Send(MakeConfigsDispatcherID(SelfId().NodeId()),
             new TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest(item));
}

void TLogSettingsConfigurator::Handle(TEvConsole::TEvConfigNotificationRequest::TPtr &ev,
                                      const TActorContext &ctx)
{
    auto &rec = ev->Get()->Record;

    LOG_INFO_S(ctx, NKikimrServices::CMS_CONFIGS,
               "TLogSettingsConfigurator: got new config: "
               << rec.GetConfig().ShortDebugString());

    const auto& logConfig = rec.GetConfig().GetLogConfig();

    ApplyLogConfig(logConfig, ctx);

    // Save config to cache file
    if (PathToConfigCacheFile)
        SaveLogSettingsConfigToCache(logConfig, ctx);

    auto resp = MakeHolder<TEvConsole::TEvConfigNotificationResponse>(rec);

    LOG_TRACE_S(ctx, NKikimrServices::CMS_CONFIGS,
                "TLogSettingsConfigurator: Send TEvConfigNotificationResponse: "
                << resp->Record.ShortDebugString());

    ctx.Send(ev->Sender, resp.Release(), 0, ev->Cookie);
}

void TLogSettingsConfigurator::SaveLogSettingsConfigToCache(const NKikimrConfig::TLogConfig &logConfig,
                                  const TActorContext &ctx) {
    try {
        NKikimrConfig::TAppConfig appConfig;
        TFileInput cacheFile(PathToConfigCacheFile);

        if (!google::protobuf::TextFormat::ParseFromString(cacheFile.ReadAll(), &appConfig))
            ythrow yexception() << "Failed to parse config from cache file " << LastSystemError() << " " << LastSystemErrorText();

        appConfig.MutableLogConfig()->CopyFrom(logConfig);

        TString proto;
        const TString pathToTempFile = PathToConfigCacheFile + ".tmp";

        if (!google::protobuf::TextFormat::PrintToString(appConfig, &proto))
            ythrow yexception() << "Failed to print app config to string " << LastSystemError() << " " << LastSystemErrorText();

        TFileOutput tempFile(pathToTempFile);
        tempFile << proto;

        if (!NFs::Rename(pathToTempFile, PathToConfigCacheFile))
            ythrow yexception() << "Failed to rename temporary file " << LastSystemError() << " " << LastSystemErrorText();

    } catch (const yexception& ex) {
        LOG_ERROR_S(ctx, NKikimrServices::CMS_CONFIGS,
                    "TLogSettingsConfigurator: failed to save log settings config to cache file '"
                    << ex.what() << "'");
    }
}

void TLogSettingsConfigurator::ApplyLogConfig(const NKikimrConfig::TLogConfig &config,
                                              const TActorContext &ctx)
{
    auto componentSettings = ComputeComponentSettings(config, ctx);
    ApplyComponentSettings(componentSettings, ctx);

    // TODO: support update for AllowDrop, Format, ClusterName, UseLocalTimestamps.
    // Options should either become atomic or update should be done via log service.
}

TVector<NLog::TComponentSettings>
TLogSettingsConfigurator::ComputeComponentSettings(const NKikimrConfig::TLogConfig &config,
                                                   const TActorContext &ctx)
{
    auto *logSettings = static_cast<NLog::TSettings*>(ctx.LoggerSettings());
    NLog::TComponentSettings defSettings(config.GetDefaultLevel(),
                                         config.GetDefaultSamplingLevel(),
                                         config.GetDefaultSamplingRate());

    TVector<NLog::TComponentSettings> result(logSettings->MaxVal + 1, defSettings);
    for (auto &entry : config.GetEntry()) {
        auto component = logSettings->FindComponent(entry.GetComponent());

        if (component == NLog::InvalidComponent) {
            LOG_ERROR_S(ctx, NKikimrServices::CMS_CONFIGS,
                        "TLogSettingsConfigurator: ignoring entry for invalid component '"
                        << entry.GetComponent() << "'");
            continue;
        }

        if (entry.HasLevel())
            result[component].Raw.X.Level = static_cast<ui8>(entry.GetLevel());
        if (entry.HasSamplingLevel())
            result[component].Raw.X.SamplingLevel = static_cast<ui8>(entry.GetSamplingLevel());
        if (entry.HasSamplingRate())
            result[component].Raw.X.SamplingRate = static_cast<ui8>(entry.GetSamplingRate());
    }

    return result;
}

void TLogSettingsConfigurator::ApplyComponentSettings(const TVector<NLog::TComponentSettings> &settings,
                                                      const TActorContext &ctx)
{
    auto *logSettings = static_cast<NLog::TSettings*>(ctx.LoggerSettings());
    for (NLog::EComponent i = logSettings->MinVal; i <= logSettings->MaxVal; ++i) {
        if (!logSettings->IsValidComponent(i))
            continue;

        auto curSettings = logSettings->GetComponentSettings(i);

        TString msg;
        if (curSettings.Raw.X.Level != settings[i].Raw.X.Level) {
            NLog::EPriority prio = static_cast<NLog::EPriority>(settings[i].Raw.X.Level);
            auto logPrio = logSettings->SetLevel(prio, i, msg)
                ? NLog::PRI_ERROR : NLog::PRI_NOTICE;
            LOG_LOG_S(ctx, logPrio, NKikimrServices::CMS_CONFIGS,
                      "TLogSettingsConfigurator: " << msg);

            if (i == NKikimrServices::GRPC_LIBRARY) {
                NConsole::SetGRpcLibraryLogVerbosity(prio);
            }
        }
        if (curSettings.Raw.X.SamplingLevel != settings[i].Raw.X.SamplingLevel) {
            NLog::EPriority prio = static_cast<NLog::EPriority>(settings[i].Raw.X.SamplingLevel);
            auto logPrio = logSettings->SetSamplingLevel(prio, i, msg)
                ? NLog::PRI_ERROR : NLog::PRI_NOTICE;
            LOG_LOG_S(ctx, logPrio, NKikimrServices::CMS_CONFIGS,
                      "TLogSettingsConfigurator: " << msg);
        }
        if (curSettings.Raw.X.SamplingRate != settings[i].Raw.X.SamplingRate) {
            auto prio = logSettings->SetSamplingRate(settings[i].Raw.X.SamplingRate, i, msg)
                ? NLog::PRI_ERROR : NLog::PRI_NOTICE;
            LOG_LOG_S(ctx, prio, NKikimrServices::CMS_CONFIGS,
                      "TLogSettingsConfigurator: " << msg);
        }
    }
}

IActor *CreateLogSettingsConfigurator()
{
    return new TLogSettingsConfigurator();
}

IActor *CreateLogSettingsConfigurator(const TString &pathToConfigCacheFile)
{
    return new TLogSettingsConfigurator(pathToConfigCacheFile);
}

} // namespace NKikimr::NConsole

#include "configs_dispatcher.h"
#include "console.h"
#include "log_settings_configurator.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <util/system/file.h>
#include <util/system/fs.h>
#include <util/stream/file.h>
#include <google/protobuf/text_format.h>
#include <ydb/core/cms/console/grpc_library_helper.h>
#include <ydb/core/kqp/event_log/audit_event_log_writer.h>
#include <ydb/core/kqp/event_log/kqp_event_log_writer.h>
#include <ydb/core/kqp/event_log/tli_event_log_writer.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::CMS_CONFIGS

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

    static std::pair<TString, TString> GetDefaultStoreTableName(const TString& eventSource);

    static NKikimr::NKqp::NEventLog::TColumnShardLogWriter::TDatabaseSettings
    GetColumnShardDatabaseSettings(const NKikimrConfig::TLogConfig_TSink& sink);

    NStructuredLog::ILogSinkSPtr CreateColumnShardLogSink(const NKikimrConfig::TLogConfig_TSink& sink);
    NStructuredLog::ILogSinkSPtr CreateLogSink(const NKikimrConfig::TLogConfig_TSink& sink);
    void ApplyLogSinkSettings(const NKikimrConfig::TLogConfig &config, const TActorContext &ctx);

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
    YDB_LOG_DEBUG_CTX(ctx, "TLogSettingsConfigurator Bootstrap");

    Become(&TThis::StateWork);

    YDB_LOG_DEBUG_CTX(ctx, "TLogSettingsConfigurator: subscribe for config updates");

    ui32 item = (ui32)NKikimrConsole::TConfigItem::LogConfigItem;
    ctx.Send(MakeConfigsDispatcherID(SelfId().NodeId()),
             new TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest(item));
}

void TLogSettingsConfigurator::Handle(TEvConsole::TEvConfigNotificationRequest::TPtr &ev,
                                      const TActorContext &ctx)
{
    auto &rec = ev->Get()->Record;

    YDB_LOG_INFO_CTX(ctx, "TLogSettingsConfigurator: got new",
        {"config", rec.GetConfig().ShortDebugString()});

    const auto& logConfig = rec.GetConfig().GetLogConfig();

    ApplyLogConfig(logConfig, ctx);

    // Save config to cache file
    if (PathToConfigCacheFile)
        SaveLogSettingsConfigToCache(logConfig, ctx);

    auto resp = MakeHolder<TEvConsole::TEvConfigNotificationResponse>(rec);

    YDB_LOG_TRACE_CTX(ctx, "TLogSettingsConfigurator: Send",
        {"ev", resp->Record.ShortDebugString()});

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
        YDB_LOG_ERROR_CTX(ctx, "TLogSettingsConfigurator: failed to save log settings config to cache file",
            {"path", PathToConfigCacheFile},
            {"error", ex.what()});
    }
}

void TLogSettingsConfigurator::ApplyLogConfig(const NKikimrConfig::TLogConfig &config,
                                              const TActorContext &ctx)
{
    auto componentSettings = ComputeComponentSettings(config, ctx);
    ApplyComponentSettings(componentSettings, ctx);

    // TODO: support update for AllowDrop, Format, ClusterName, UseLocalTimestamps.
    // Options should either become atomic or update should be done via log service.

    ApplyLogSinkSettings(config, ctx);
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
            YDB_LOG_ERROR_CTX(ctx, "TLogSettingsConfigurator: ignoring entry for invalid component",
                {"componentId", entry.GetComponent()});
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
            YDB_LOG_CTX(ctx, logPrio, "TLogSettingsConfigurator",
                {"message", msg});

            if (i == NKikimrServices::GRPC_LIBRARY) {
                NConsole::SetGRpcLibraryLogVerbosity(prio);
            }
        }
        if (curSettings.Raw.X.SamplingLevel != settings[i].Raw.X.SamplingLevel) {
            NLog::EPriority prio = static_cast<NLog::EPriority>(settings[i].Raw.X.SamplingLevel);
            auto logPrio = logSettings->SetSamplingLevel(prio, i, msg)
                ? NLog::PRI_ERROR : NLog::PRI_NOTICE;
            YDB_LOG_CTX(ctx, logPrio, "TLogSettingsConfigurator",
                {"message", msg});
        }
        if (curSettings.Raw.X.SamplingRate != settings[i].Raw.X.SamplingRate) {
            auto prio = logSettings->SetSamplingRate(settings[i].Raw.X.SamplingRate, i, msg)
                ? NLog::PRI_ERROR : NLog::PRI_NOTICE;
            YDB_LOG_CTX(ctx, prio, "TLogSettingsConfigurator",
                {"message", msg});
        }
    }
}

std::pair<TString, TString> TLogSettingsConfigurator::GetDefaultStoreTableName(const TString& eventSource) {
    std::map<TString, std::pair<TString, TString>> defValues = {
        {"kqp-requests", {"kqp_requests", "kqp_requests"}},
        {"tli-datashard", {"tli_datashard", "tli_datashard"}},
        {"tli-session", {"tli_session", "tli_session"}},
        {"audit-schemeshard", {"audit_schemeshard", "audit_schemeshard"}},
        {"audit-grpc-proxy", {"audit_grpc_proxy", "audit_grpc_proxy"}},
        {"audit-grpc-conn", {"audit_grpc_conn", "audit_grpc_conn"}},
        {"audit-grpc-login", {"audit_grpc_login", "audit_grpc_login"}},
        {"audit-monitoring", {"audit_monitoring", "audit_monitoring"}},
        {"audit-heartbeat", {"audit_heartbeat", "audit_heartbeat"}},
        {"audit-bsc", {"audit_bsc", "audit_bsc"}},
        {"audit-distconf", {"audit_distconf", "audit_distconf"}},
        {"audit-web-login", {"audit_web_login", "audit_web_login"}},
        {"audit-console", {"audit_console", "audit_console"}}};
    auto it = defValues.find(eventSource);
    if (it == end(defValues)) {
        return {};
    }
    return it->second;
}

NKikimr::NKqp::NEventLog::TColumnShardLogWriter::TDatabaseSettings
TLogSettingsConfigurator::GetColumnShardDatabaseSettings(const NKikimrConfig::TLogConfig_TSink& sink) {
    NKikimr::NKqp::NEventLog::TColumnShardLogWriter::TDatabaseSettings settings;
    if (sink.HasDatabasePath()) {
        settings.Path = sink.GetDatabasePath();
    }
    if (sink.HasStorageName()) {
        settings.StoreName = sink.GetStorageName();
    }
    if (sink.HasTableName()) {
        settings.TableName = sink.GetTableName();
    }
    if (sink.HasMaxBatchSize()) {
        settings.MaxBatchSize = sink.GetMaxBatchSize();
    }
    if (sink.HasFlushTimeout()) {
        settings.FlushTimeout = TDuration::MilliSeconds(sink.GetFlushTimeout());
    }
    if (sink.HasStoreShardsCount()) {
        settings.StoreShardsCount = sink.GetStoreShardsCount();
    }
    if (sink.HasTableShardsCount()) {
        settings.TableShardsCount = sink.GetTableShardsCount();
    }
    return settings;
}

NStructuredLog::ILogSinkSPtr TLogSettingsConfigurator::CreateColumnShardLogSink(const NKikimrConfig::TLogConfig_TSink& sink) {
    auto eventSource = sink.GetSource();

    auto databaseSettings = GetColumnShardDatabaseSettings(sink);
    if (databaseSettings.StoreName.empty() || databaseSettings.TableName.empty()) {
        auto defaults = GetDefaultStoreTableName(eventSource);
        if (databaseSettings.StoreName.empty()) {
            databaseSettings.StoreName = defaults.first;
        }
        if (databaseSettings.TableName.empty()) {
            databaseSettings.TableName = defaults.second;
        }
    }

    using namespace NKqp::NEventLog;
    if (eventSource == "kqp-requests") {
        return std::make_shared<TKqpEventLogWriter>(databaseSettings);
    }
    else if (eventSource == "tli-datashard") {
        return std::make_shared<TDataShardTliEventLogWriter>(databaseSettings);
    }
    else if (eventSource == "tli-session") {
        return std::make_shared<TSessionTliEventLogWriter>(databaseSettings);
    }
    else if (eventSource == "audit-schemeshard") {
        return std::make_shared<NAudit::TSchemeShardEventLogWriter>(databaseSettings);
    }
    else if (eventSource == "audit-grpc-proxy") {
        return std::make_shared<NAudit::TGrpcProxyEventLogWriter>(databaseSettings);
    }
    else if (eventSource == "audit-grpc-conn") {
        return std::make_shared<NAudit::TGrpcConnEventLogWriter>(databaseSettings);
    }
    else if (eventSource == "audit-grpc-login") {
        return std::make_shared<NAudit::TGrpcLoginEventLogWriter>(databaseSettings);
    }
    else if (eventSource == "audit-monitoring") {
        return std::make_shared<NAudit::TMonitoringEventLogWriter>(databaseSettings);
    }
    else if (eventSource == "audit-heartbeat") {
        return std::make_shared<NAudit::TAuditServiceEventLogWriter>(databaseSettings);
    }
    else if (eventSource == "audit-bsc") {
        return std::make_shared<NAudit::TBscEventLogWriter>(databaseSettings);
    }
    else if (eventSource == "audit-distconf") {
        return std::make_shared<NAudit::TDistconfEventLogWriter>(databaseSettings);
    }
    else if (eventSource == "audit-web-login") {
        return std::make_shared<NAudit::TWebLoginEventLogWriter>(databaseSettings);
    }
    else if (eventSource == "audit-console") {
        return std::make_shared<NAudit::TConsoleEventLogWriter>(databaseSettings);
    }

    return nullptr;
}

NStructuredLog::ILogSinkSPtr TLogSettingsConfigurator::CreateLogSink(const NKikimrConfig::TLogConfig_TSink& sink) {
    using namespace NKikimr::NKqp::NEventLog;

    const TString destination = sink.HasDestination() ? sink.GetDestination() : TString();

    if (destination == "local_db") {
        return CreateColumnShardLogSink(sink);
    }
    return nullptr;
}

void TLogSettingsConfigurator::ApplyLogSinkSettings(const NKikimrConfig::TLogConfig &config, const TActorContext &ctx) {
    Y_UNUSED(config);

    auto *logSettings = static_cast<NLog::TSettings*>(ctx.LoggerSettings());

    NActors::NLog::TSettings::TLogSinkMap oldSinks, newSinks;

    auto oldSinksPtr = logSettings->Sinks;
    if (oldSinksPtr == nullptr) {
        oldSinks = *oldSinksPtr;
    }

    Cerr << "Start dump sinks" << Endl;

    std::map<TString, NKikimrConfig::TLogConfig_TSink> result;
    for(auto& sink: config.GetSink()) {
        auto sinkConfig = sink.ShortDebugString();
        YDB_LOG_CREATE_CONTEXT({"sinkConfig", sinkConfig});

        if (!sink.HasSource()) {
            YDB_LOG_ERROR("LogSinksConfig: Source is not specified");
            continue;
        }

        if (!sink.HasDestination()) {
            YDB_LOG_ERROR("LogSinksConfig: Destination is not specified");
            continue;
        }

        Cerr << "       sink " << sinkConfig << Endl;

        auto it = oldSinks.find(sinkConfig);
        if (it != end(oldSinks)) {
            YDB_LOG_INFO("LogSinksConfig: Don't reconfigure sink");

            newSinks[sinkConfig] = it->second;
            oldSinks.erase(it);
        } else {
            auto sinkPtr = CreateLogSink(sink);
            if (sinkPtr != nullptr) {
                YDB_LOG_INFO("LogSinksConfig: Create sink");

                newSinks[sinkConfig] = sinkPtr;
            } else {
                YDB_LOG_ERROR("LogSinksConfig: Can't create sink");
            }
        }
    }

    // (*newSinks)["1"] = std::make_shared<TKqpEventLogWriter>(GetSettings("kqp_requests"));
    logSettings->Sinks = std::make_shared<NLog::TSettings::TLogSinkMap>(newSinks);

    Cerr << "Done dump sinks" << Endl;
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

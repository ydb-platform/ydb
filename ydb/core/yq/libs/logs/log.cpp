#include "log.h"

#include <ydb/core/cms/console/console.h>
#include <ydb/core/cms/console/configs_dispatcher.h>

#include <ydb/library/yql/utils/actor_log/log.h>

#include <library/cpp/actors/core/log.h>

namespace NKikimr {

using namespace NActors;

class TYqlLogsUpdater : public TActorBootstrapped<TYqlLogsUpdater> {
public:
    static constexpr char ActorName[] = "YQ_LOGS_UPDATER";

    TYqlLogsUpdater(const NKikimrConfig::TLogConfig& logConfig)
        : YqlLoggerScope(new NYql::NLog::TTlsLogBackend(new TNullLogBackend()))
        , LogConfig(logConfig)
    { }

    void Bootstrap(const TActorContext& ctx) {
        UpdateYqlLogLevels(ctx);

        // Subscribe for Logger config changes
        ui32 logConfigKind = (ui32)NKikimrConsole::TConfigItem::LogConfigItem;
        ctx.Send(NConsole::MakeConfigsDispatcherID(ctx.SelfID.NodeId()),
            new NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest(
                {logConfigKind}),
            IEventHandle::FlagTrackDelivery);

        Become(&TYqlLogsUpdater::MainState);
    }

private:
    STFUNC(MainState) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvents::TEvUndelivered, Handle);
            HFunc(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse, Handle);
            HFunc(NConsole::TEvConsole::TEvConfigNotificationRequest, Handle);
        default:
            Y_FAIL("TYqlLogsUpdater: unexpected event type: %" PRIx32 " event: %s",
                ev->GetTypeRewrite(), ev->HasEvent() ? ev->GetBase()->ToString().data() : "serialized?");
        }
    }

    void Handle(TEvents::TEvUndelivered::TPtr &ev, const TActorContext &ctx) {
        switch (ev->Get()->SourceType) {
            case NConsole::TEvConfigsDispatcher::EvSetConfigSubscriptionRequest:
                LOG_CRIT_S(ctx, NKikimrServices::YQL_PROXY,
                    "Failed to deliver subscription request to config dispatcher.");
                break;

            case NConsole::TEvConsole::EvConfigNotificationResponse:
                LOG_ERROR_S(ctx, NKikimrServices::YQL_PROXY, "Failed to deliver config notification response.");
                break;

            default:
                LOG_ERROR_S(ctx, NKikimrServices::YQL_PROXY,
                    "Undelivered event with unexpected source type: " << ev->Get()->SourceType);
                break;
        }
    }

    void Handle(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse::TPtr &ev, const TActorContext &ctx) {
        Y_UNUSED(ev);

        LOG_DEBUG_S(ctx, NKikimrServices::YQL_PROXY, "Subscribed for config changes.");
    }

    void Handle(NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr &ev, const TActorContext &ctx) {
        auto &event = ev->Get()->Record;

        LOG_DEBUG_S(ctx, NKikimrServices::YQL_PROXY, "Updated table service config.");

        LogConfig.Swap(event.MutableConfig()->MutableLogConfig());
        UpdateYqlLogLevels(ctx);
    }

    void UpdateYqlLogLevels(const TActorContext& ctx) {
        const auto& kqpYqlName = NKikimrServices::EServiceKikimr_Name(NKikimrServices::KQP_YQL);
        for (auto &entry : LogConfig.GetEntry()) {
            if (entry.GetComponent() == kqpYqlName && entry.HasLevel()) {
                auto yqlPriority = static_cast<NActors::NLog::EPriority>(entry.GetLevel());
                NYql::NDq::SetYqlLogLevels(yqlPriority);
                LOG_DEBUG_S(ctx, NKikimrServices::YQL_PROXY, "Updated YQL logs priority: "
                    << (ui32)yqlPriority);
                return;
            }
        }

        // Set log level based on current logger settings
        ui8 currentLevel = ctx.LoggerSettings()->GetComponentSettings(NKikimrServices::KQP_YQL).Raw.X.Level;
        auto yqlPriority = static_cast<NActors::NLog::EPriority>(currentLevel);

        LOG_DEBUG_S(ctx, NKikimrServices::YQL_PROXY, "Updated YQL logs priority to current level: "
            << (ui32)yqlPriority);
        NYql::NDq::SetYqlLogLevels(yqlPriority);
    }

    NYql::NLog::YqlLoggerScope YqlLoggerScope;
    NKikimrConfig::TLogConfig LogConfig;
};

NActors::IActor* CreateYqlLogsUpdater(const NKikimrConfig::TLogConfig& logConfig) {
    return new TYqlLogsUpdater(logConfig);
}

TActorId MakeYqlLogsUpdaterId() {
    constexpr TStringBuf name = "YQLLOGSUPD";
    return NActors::TActorId(0, name);
}

} /* namespace NKikimr */

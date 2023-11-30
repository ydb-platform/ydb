#include "log.h"

#include <ydb/core/cms/console/console.h>
#include <ydb/core/cms/console/configs_dispatcher.h>

#include <ydb/library/yql/utils/actor_log/log.h>

#include <ydb/library/actors/core/log.h>

#define LOG_C(stream) LOG_CRIT_S (::NActors::TActivationContext::AsActorContext(), NKikimrServices::FQ_LOG_UPDATER, stream)
#define LOG_E(stream) LOG_ERROR_S(::NActors::TActivationContext::AsActorContext(), NKikimrServices::FQ_LOG_UPDATER, stream)
#define LOG_D(stream) LOG_DEBUG_S(::NActors::TActivationContext::AsActorContext(), NKikimrServices::FQ_LOG_UPDATER, stream)

namespace NKikimr {

using namespace NActors;

class TYqlLogsUpdater : public TActorBootstrapped<TYqlLogsUpdater> {
public:
    static constexpr char ActorName[] = "YQ_LOGS_UPDATER";

    TYqlLogsUpdater(const NKikimrConfig::TLogConfig& logConfig)
        : YqlLoggerScope(new NYql::NLog::TTlsLogBackend(new TNullLogBackend()))
        , LogConfig(logConfig)
    { }

    void Bootstrap() {
        UpdateYqlLogLevels();

        // Subscribe for Logger config changes
        ui32 logConfigKind = (ui32)NKikimrConsole::TConfigItem::LogConfigItem;
        Send(NConsole::MakeConfigsDispatcherID(SelfId().NodeId()),
            new NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest(
                {logConfigKind}),
            IEventHandle::FlagTrackDelivery);

        Become(&TYqlLogsUpdater::MainState);
    }

private:
    STATEFN(MainState) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvents::TEvUndelivered, Handle);
            hFunc(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse, Handle);
            hFunc(NConsole::TEvConsole::TEvConfigNotificationRequest, Handle);
        default:
            Y_ABORT("TYqlLogsUpdater: unexpected event type: %" PRIx32 " event: %s",
                ev->GetTypeRewrite(), ev->ToString().data());
        }
    }

    void Handle(TEvents::TEvUndelivered::TPtr& ev) {
        switch (ev->Get()->SourceType) {
            case NConsole::TEvConfigsDispatcher::EvSetConfigSubscriptionRequest:
                LOG_C("Failed to deliver subscription request to config dispatcher.");
                break;

            case NConsole::TEvConsole::EvConfigNotificationResponse:
                LOG_E("Failed to deliver config notification response.");
                break;

            default:
                LOG_E("Undelivered event with unexpected source type: " << ev->Get()->SourceType);
                break;
        }
    }

    void Handle(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse::TPtr& ev) {
        Y_UNUSED(ev);

        LOG_D("Subscribed for config changes.");
    }

    void Handle(NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr& ev) {
        auto& event = ev->Get()->Record;

        LOG_D("Updated table service config.");

        LogConfig.Swap(event.MutableConfig()->MutableLogConfig());
        UpdateYqlLogLevels();

        auto resp = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationResponse>(event);

        Send(ev->Sender, resp.Release(), 0, ev->Cookie);
    }

    void UpdateYqlLogLevels() {
        const auto& kqpYqlName = NKikimrServices::EServiceKikimr_Name(NKikimrServices::KQP_YQL);
        for (auto& entry : LogConfig.GetEntry()) {
            if (entry.GetComponent() == kqpYqlName && entry.HasLevel()) {
                auto yqlPriority = static_cast<NActors::NLog::EPriority>(entry.GetLevel());
                NYql::NDq::SetYqlLogLevels(yqlPriority);
                LOG_D("Updated YQL logs priority: " << (ui32)yqlPriority);
                return;
            }
        }

        // Set log level based on current logger settings
        ui8 currentLevel = TActivationContext::AsActorContext().LoggerSettings()->GetComponentSettings(NKikimrServices::KQP_YQL).Raw.X.Level;
        auto yqlPriority = static_cast<NActors::NLog::EPriority>(currentLevel);

        LOG_D("Updated YQL logs priority to current level: " << (ui32)yqlPriority);
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

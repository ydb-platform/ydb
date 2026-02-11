#include "consumers_advanced_monitoring_settings.h"

#include <library/cpp/json/json_reader.h>
#include <util/generic/maybe.h>

namespace NKikimr::NGRpcProxy::V1 {

    struct TCustomMonitoringSettings {
        TString MonitoringProjectId;
        TMaybe<ui32> MetricsLevel;
        bool InUse = false; // consumer are known and used
    };

    class TConsumersAdvancedMonitoringSettings::TImpl {
    public:
        bool UpdateConsumerConfig(const TStringBuf name, ::NKikimrPQ::TPQTabletConfig_TConsumer& consumer) {
            auto* monitoringSettings = ConsumersMap.FindPtr(name);
            if (!monitoringSettings) {
                return false;
            }
            monitoringSettings->InUse = true;
            if (!monitoringSettings->MonitoringProjectId.empty()) {
                consumer.SetMonitoringProjectId(monitoringSettings->MonitoringProjectId);
            }
            if (!monitoringSettings->MetricsLevel.Empty()) {
                consumer.SetMetricsLevel(*monitoringSettings->MetricsLevel);
            }
            return true;
        }

        Ydb::StatusIds::StatusCode CheckForUnknownConsumers(TString& error) const {
            for (const auto& [name, settings] : ConsumersMap) {
                if (!settings.InUse) {
                    error = std::format("Attribute advanced_monitoring contains unknown consumer name: '{}'", std::string_view(name));
                    return Ydb::StatusIds::BAD_REQUEST;
                }
            }
            return Ydb::StatusIds::SUCCESS;
        }

    public:
        THashMap<TString, TCustomMonitoringSettings> ConsumersMap;
    };

    std::expected<TConsumersAdvancedMonitoringSettings, TString> TConsumersAdvancedMonitoringSettings::FromJson(TStringBuf jsonString) {
        NJson::TJsonValue json;
        try {
            NJson::ReadJsonTree(jsonString, &json, true);
        } catch (const std::exception& e) {
            return std::unexpected(std::format("Attribute advanced_monitoring is not a valid json: {}", e.what()));
        }
        if (!json.IsMap()) {
            return std::unexpected("Attribute advanced_monitoring is not a map");
        }
        THolder<TImpl> result = MakeHolder<TImpl>();
        for (const auto& [consumerName, consumerSettings] : json.GetMap()) {
            TCustomMonitoringSettings customMonitoringSettings;
            if (const auto& v = consumerSettings["metrics_level"]; v != NJson::TJsonValue::UNDEFINED) {
                if (!v.IsInteger()) {
                    return std::unexpected(std::format("Attribute advanced_monitoring for consumer '{}' contains non-integer value for metrics_level", std::string_view(consumerName)));
                }
                customMonitoringSettings.MetricsLevel = v.GetInteger();
            }
            if (const auto& v = consumerSettings["monitoring_project_id"]; v != NJson::TJsonValue::UNDEFINED) {
                if (!v.IsString()) {
                    return std::unexpected(std::format("Attribute advanced_monitoring for consumer '{}' contains non-string value for monitoring_project_id", std::string_view(consumerName)));
                }
                customMonitoringSettings.MonitoringProjectId = v.GetString();
            }
            result->ConsumersMap.try_emplace(consumerName, std::move(customMonitoringSettings));
        }
        TConsumersAdvancedMonitoringSettings ret;
        ret.Impl_ = std::move(result);
        return ret;
    }

    TConsumersAdvancedMonitoringSettings::TConsumersAdvancedMonitoringSettings() = default;
    TConsumersAdvancedMonitoringSettings::TConsumersAdvancedMonitoringSettings(TConsumersAdvancedMonitoringSettings&&) = default;
    TConsumersAdvancedMonitoringSettings& TConsumersAdvancedMonitoringSettings::operator=(TConsumersAdvancedMonitoringSettings&&) = default;
    TConsumersAdvancedMonitoringSettings::~TConsumersAdvancedMonitoringSettings() = default;

    bool TConsumersAdvancedMonitoringSettings::UpdateConsumerConfig(const TStringBuf name, ::NKikimrPQ::TPQTabletConfig_TConsumer& consumer) const {
        if (!Impl_) {
            return false;
        }
        return Impl_->UpdateConsumerConfig(name, consumer);
    }

    Ydb::StatusIds::StatusCode TConsumersAdvancedMonitoringSettings::CheckForUnknownConsumers(TString& error) const {
        if (!Impl_) {
            return Ydb::StatusIds::SUCCESS;
        }
        return Impl_->CheckForUnknownConsumers(error);
    }

} // namespace NKikimr::NGRpcProxy::V1

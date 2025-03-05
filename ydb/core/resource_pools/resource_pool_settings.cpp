#include "resource_pool_settings.h"

#include <util/string/builder.h>


namespace NKikimr::NResourcePool {

//// TPoolSettings::TParser

void TPoolSettings::TParser::operator()(i32* setting) const {
    *setting = FromString<i32>(Value);
    if (*setting < -1) {
        throw yexception() << "Invalid integer value " << *setting << ", it is should be greater or equal -1";
    }
}

void TPoolSettings::TParser::operator()(TDuration* setting) const {
    ui64 seconds = FromString<ui64>(Value);
    if (seconds > std::numeric_limits<ui64>::max() / 1000) {
        throw yexception() << "Invalid seconds value " << seconds << ", it is should be less or equal than " << std::numeric_limits<ui64>::max() / 1000;
    }
    *setting = TDuration::Seconds(seconds);
}

void TPoolSettings::TParser::operator()(TPercent* setting) const {
    *setting = FromString<TPercent>(Value);
    if (*setting != -1 && (*setting < 0 || 100 < *setting)) {
        throw yexception() << "Invalid percent value " << *setting << ", it is should be between 0 and 100 or -1";
    }
}

//// TPoolSettings::TExtractor

TString TPoolSettings::TExtractor::operator()(i32* setting) const {
    return ToString(*setting);
}

TString TPoolSettings::TExtractor::operator()(double* setting) const {
    return ToString(*setting);
}

TString TPoolSettings::TExtractor::operator()(TDuration* setting) const {
    return ToString(setting->Seconds());
}

//// TPoolSettings

TPoolSettings::TPoolSettings(const google::protobuf::Map<TString, TString>& properties) {
    for (auto& [property, value] : GetPropertiesMap()) {
        if (auto propertyIt = properties.find(property); propertyIt != properties.end()) {
            std::visit(TPoolSettings::TParser{propertyIt->second}, value);
        }
    }
}

std::unordered_map<TString, TPoolSettings::TProperty> TPoolSettings::GetPropertiesMap(bool restricted) {
    std::unordered_map<TString, TProperty> properties = {
        {"concurrent_query_limit", &ConcurrentQueryLimit},
        {"queue_size", &QueueSize},
        {"query_memory_limit_percent_per_node", &QueryMemoryLimitPercentPerNode},
        {"database_load_cpu_threshold", &DatabaseLoadCpuThreshold},
        {"total_cpu_limit_percent_per_node", &TotalCpuLimitPercentPerNode},
        {"query_cpu_limit_percent_per_node", &QueryCpuLimitPercentPerNode},
        {"resource_weight", &ResourceWeight}
    };
    if (!restricted) {
        properties.insert({"query_cancel_after_seconds", &QueryCancelAfter});
    }
    return properties;
}

std::optional<TString> TPoolSettings::Validate() const {
    if (ConcurrentQueryLimit > POOL_MAX_CONCURRENT_QUERY_LIMIT) {
        return TStringBuilder() << "Invalid resource pool configuration, concurrent_query_limit is " << ConcurrentQueryLimit << ", that exceeds limit in " << POOL_MAX_CONCURRENT_QUERY_LIMIT;
    }
    if (QueueSize != -1 && ConcurrentQueryLimit == -1 && DatabaseLoadCpuThreshold < 0.0) {
        return "Invalid resource pool configuration, queue_size unsupported without concurrent_query_limit or database_load_cpu_threshold";
    }
    return std::nullopt;
}

}  // namespace NKikimr::NResourcePool

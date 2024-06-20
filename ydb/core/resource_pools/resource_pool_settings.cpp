#include "resource_pool_settings.h"


namespace NKikimr::NResourcePool {

//// TSettingsParser

template <>
void TSettingsParser::operator()(TDuration* setting) {
    *setting = TDuration::Seconds(FromString<ui64>(value));
}

template <>
void TSettingsParser::operator()(TPoolSettings::TRatio* setting) {
    *setting = FromString<double>(value);
    if (*setting < 0 || 100 < *setting) {
        throw yexception() << "Invalid ratio value " << *setting << ", it is should be between 0 and 100";
    }
}

//// TSettingsExtractor

template <>
TString TSettingsExtractor::operator()(TDuration* setting) {
    return ToString(setting->Seconds());
}

//// Functions

std::unordered_map<TString, TProperty> GetPropertiesMap(TPoolSettings& settings) {
    return {
        {"concurrent_query_limit", &settings.ConcurrentQueryLimit},
        {"query_count_limit", &settings.QueryCountLimit},
        {"query_cancel_after_seconds", &settings.QueryCancelAfter},
        {"query_memory_limit_ratio_per_node", &settings.QueryMemoryLimitRatioPerNode}
    };
}

}  // namespace NKikimr::NResourcePool

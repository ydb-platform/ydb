#pragma once

#include <memory>

namespace NKikimrConfig {
class TQueryServiceConfig;
}

namespace NKikimr {

struct TAppData;

std::shared_ptr<const NKikimrConfig::TQueryServiceConfig> LoadQueryServiceConfig(const TAppData& appData);
void PublishQueryServiceConfig(TAppData& appData, std::shared_ptr<const NKikimrConfig::TQueryServiceConfig> config);

} // namespace NKikimr

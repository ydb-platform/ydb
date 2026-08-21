#pragma once

#include <optional>
#include <vector>
#include <util/generic/string.h>

namespace NKikimrConfig {
class TFeatureFlags;
class TTableServiceConfig;
}

namespace NKikimr::NKqp {

std::optional<TString> ShouldInvalidateCompileCache(const NKikimrConfig::TTableServiceConfig& prev, const NKikimrConfig::TTableServiceConfig& next);
std::optional<TString> ShouldInvalidateCompileCache(const NKikimrConfig::TFeatureFlags& prev, const NKikimrConfig::TFeatureFlags& next);

}

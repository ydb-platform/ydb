#pragma once

#include <vector>
#include <util/generic/string.h>

namespace NKikimrConfig {
class TTableServiceConfig;
}

namespace NKikimr::NKqp {

std::optional<TString> ShouldInvalidateCompileCache(const NKikimrConfig::TTableServiceConfig& prev, const NKikimrConfig::TTableServiceConfig& next);

}
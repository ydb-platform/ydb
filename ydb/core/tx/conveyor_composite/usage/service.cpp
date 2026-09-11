#include "service.h"

#include <ydb/core/base/appdata.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/conclusion/result.h>

#include <optional>

namespace NKikimr::NConveyorComposite {

namespace {
std::optional<bool> ParseConfiguredPool(const bool hasField, const TString& name) {
    if (!hasField) {
        return std::nullopt;
    }
    auto parsed = NConfig::ParseActorSystemPoolName(name);
    AFL_VERIFY(parsed.IsSuccess())("error", parsed.GetErrorMessage())("name", name);
    return *parsed;
}
}

std::optional<bool> GetScanDefaultUseBatchPool() {
    if (!HasAppData()) {
        return std::nullopt;
    }
    const auto& csConfig = AppDataVerified().ColumnShardConfig;
    return ParseConfiguredPool(csConfig.HasScanDefaultPool(), csConfig.GetScanDefaultPool());
}

bool ResolveCompactionUseBatchPool() {
    if (!HasAppData()) {
        return false;
    }
    const auto& csConfig = AppDataVerified().ColumnShardConfig;
    const auto parsed = ParseConfiguredPool(csConfig.HasCompactionDefaultPool(), csConfig.GetCompactionDefaultPool());
    return parsed.value_or(false);
}

}   // namespace NKikimr::NConveyorComposite

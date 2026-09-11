#include "service.h"

#include <ydb/core/base/appdata.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/conclusion/result.h>
#include <ydb/library/services/services.pb.h>

#include <optional>

namespace NKikimr::NConveyorComposite {

namespace {
std::optional<bool> ParseConfiguredPool(const bool hasField, const TString& name) {
    if (!hasField) {
        return std::nullopt;
    }
    auto parsed = NConfig::ParseActorSystemPoolName(name);
    if (parsed.IsFail()) {
        AFL_ERROR(NKikimrServices::TX_CONVEYOR)("error", "invalid actor system pool name, using default routing")(
            "name", name)("details", parsed.GetErrorMessage());
        return std::nullopt;
    }
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

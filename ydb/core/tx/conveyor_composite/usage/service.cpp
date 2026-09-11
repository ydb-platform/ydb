#include "service.h"

#include <ydb/core/base/appdata.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/conclusion/result.h>
#include <ydb/library/services/services.pb.h>

#include <atomic>
#include <optional>

namespace NKikimr::NConveyorComposite {

namespace {
std::optional<EActorSystemPool> ParseConfiguredPool(const bool hasField, const TString& name) {
    if (!hasField) {
        return std::nullopt;
    }
    auto parsed = NConfig::ParseActorSystemPool(name);
    if (parsed.IsFail()) {
        static std::atomic<bool> logged{false};
        if (!logged.exchange(true)) {
            AFL_ERROR(NKikimrServices::TX_CONVEYOR)("error", "invalid actor system pool name, using default routing")(
                "name", name)("details", parsed.GetErrorMessage());
        }
        return std::nullopt;
    }
    return *parsed;
}
}

std::optional<EActorSystemPool> GetScanDefaultActorSystemPool() {
    if (!HasAppData()) {
        return std::nullopt;
    }
    const auto& csConfig = AppDataVerified().ColumnShardConfig;
    return ParseConfiguredPool(csConfig.HasScanDefaultPool(), csConfig.GetScanDefaultPool());
}

EActorSystemPool GetCompactionActorSystemPool() {
    if (!HasAppData()) {
        return EActorSystemPool::User;
    }
    const auto& csConfig = AppDataVerified().ColumnShardConfig;
    return ParseConfiguredPool(csConfig.HasCompactionDefaultPool(), csConfig.GetCompactionDefaultPool())
        .value_or(EActorSystemPool::User);
}

}   // namespace NKikimr::NConveyorComposite

#pragma once

#include <ydb/apps/bridge_skipper/domain/types.h>
#include <library/cpp/json/json_value.h>

namespace NBridgeSkipper {

// HTTP parses syntax only. Semantic validation belongs to the controller turn.
enum class ECommandKind { Mode, CreatePlan, Execute };

struct TControllerCommand {
    ECommandKind Kind = ECommandKind::CreatePlan;
    TString RequestId;
    EMode Mode = EMode::Observe;
    TString ExpectedBootId;
    ui64 ExpectedModeRevision = 0;
    TString SnapshotRevision;
    EOperationKind Action = EOperationKind::Failover;
    std::optional<TString> Target;
    TString PlanId;

    bool operator==(const TControllerCommand&) const = default;
};

struct TCommandResult {
    unsigned Status = 200;
    NJson::TJsonValue Body{NJson::JSON_MAP};
};

} // namespace NBridgeSkipper

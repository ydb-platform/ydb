#pragma once

#include <util/generic/string.h>
#include <util/system/types.h>

namespace NKikimr::NKqp {

enum class ERBOTransformationEventKindV1 {
    RuleApplication,
    AtomicStageCommit,
};

// One committed optimizer transformation event. Ordinals are optimizer-wide,
// one-based, and contiguous within a diagnostic capture run.
struct TRBOTransformationEventV1 {
    ui64 Ordinal = 0;
    ERBOTransformationEventKindV1 Kind = ERBOTransformationEventKindV1::RuleApplication;
    TString Stage;
    TString Name;
};

} // namespace NKikimr::NKqp

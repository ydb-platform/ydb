#pragma once

#include "schemeshard__operation_part.h"

namespace NKikimrReplication {
class TReplicationConfig;
}

namespace NKikimr::NSchemeShard {

bool ValidateReplicationDestinations(TProposeResponse& result,
    const NKikimrReplication::TReplicationConfig& config, const TOperationContext& context,
    const TPathId& selfPathId = {});

} // namespace NKikimr::NSchemeShard

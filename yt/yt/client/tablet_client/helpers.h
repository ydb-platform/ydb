#pragma once

#include "public.h"

namespace NYT::NTabletClient {

////////////////////////////////////////////////////////////////////////////////

bool IsStableReplicaMode(ETableReplicaMode mode);
bool IsStableReplicaState(ETableReplicaState state);

std::string GetTabletCellBundlePath(const std::string& name);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletClient

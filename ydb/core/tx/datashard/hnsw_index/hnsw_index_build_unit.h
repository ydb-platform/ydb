#pragma once

namespace NKikimr::NDataShard {

class TActiveTransaction;

// Called after scheme transaction details have been loaded, including recovery.
// The caller checks that the existing table still has shadow data to publish.
void AddHnswIndexBuildToExecutionPlan(TActiveTransaction& tx, bool hasHnswMetadata);

} // namespace NKikimr::NDataShard

#pragma once

namespace NKikimr::NDataShard {

class TActiveTransaction;

// Called after scheme transaction details have been loaded, including recovery.
void AddHnswIndexBuildToExecutionPlan(TActiveTransaction& tx);

} // namespace NKikimr::NDataShard

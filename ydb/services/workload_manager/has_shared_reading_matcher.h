#pragma once

#include <ydb/core/protos/kqp_physical.pb.h>


namespace NKikimr::NWorkloadManager {

///
/// Returns true if the physical query has any PQ topic source with SharedReading enabled.
///
bool UsesSharedReading(const NKqpProto::TKqpPhyQuery& phyQuery);

}  // namespace NKikimr::NWorkloadManager

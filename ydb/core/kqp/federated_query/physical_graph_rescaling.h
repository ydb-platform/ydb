#pragma once

#include <ydb/core/protos/kqp.pb.h>

#include <util/generic/vector.h>

namespace NKikimr::NKqp {

// Patches a saved physical graph to rescale PQ source stages.
// Must be called before RestoreTasksGraphInfo().
void PatchQueryPhysicalGraphForRescaling(
    NKikimrKqp::TQueryPhysicalGraph& graph,
    const TVector<NKikimrKqp::TKqpNodeResources>& resourceSnapshot);

} // namespace NKikimr::NKqp

#pragma once

#include <ydb/core/kqp/runtime/kqp_compute.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actorid.h>

namespace NKikimr {
namespace NSysView {

THolder<NActors::IActor> CreateQueryMetricsScan(const NActors::TActorId& ownerId, ui32 scanId,
    const TTableId& tableId, const TTableRange& tableRange,
    const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns);

} // NSysView
} // NKikimr

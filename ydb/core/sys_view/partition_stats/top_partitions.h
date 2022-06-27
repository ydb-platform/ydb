#pragma once

#include <ydb/core/kqp/runtime/kqp_compute.h>

namespace NKikimr::NSysView {

THolder<IActor> CreateTopPartitionsScan(const TActorId& ownerId, ui32 scanId,
    const TTableId& tableId, const TTableRange& tableRange,
    const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns);

} // NKikimr::NSysView

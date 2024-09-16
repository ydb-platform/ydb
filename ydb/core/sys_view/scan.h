#pragma once

#include <ydb/core/kqp/runtime/kqp_compute.h>

#include <ydb/library/actors/core/actor.h>

namespace NKikimr {
namespace NSysView {

THolder<NActors::IActor> CreateSystemViewScan(const NActors::TActorId& ownerId, ui32 scanId, const TTableId& tableId,
    const TString& tablePath, TVector<TSerializedTableRange> ranges, const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns);

THolder<NActors::IActor> CreateSystemViewScan(const NActors::TActorId& ownerId, ui32 scanId, const TTableId& tableId,
    const TString& tablePath, const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns);

} // NSysView
} // NKikimr

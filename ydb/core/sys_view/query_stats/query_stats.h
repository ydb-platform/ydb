#pragma once

#include <ydb/core/kqp/runtime/kqp_compute.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actorid.h>

namespace NKikimr {
namespace NSysView {

struct TQueryStatsBucketRange {
    ui64 FromBucket = 0;
    TMaybe<ui32> FromRank;

    ui64 ToBucket = std::numeric_limits<ui64>::max();
    TMaybe<ui32> ToRank;

    bool IsEmpty = false;

    explicit TQueryStatsBucketRange(const TSerializedTableRange& range, const TDuration& bucketSize);
};

THolder<NActors::IActor> CreateQueryStatsScan(const NActors::TActorId& ownerId, ui32 scanId, const TTableId& tableId,
    const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns);

} // NSysView
} // NKikimr

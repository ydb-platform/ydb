#pragma once

#include <ydb/library/yql/dq/actors/dq_events_ids.h>
#include <ydb/library/yql/dq/actors/protos/dq_events.pb.h>
#include <ydb/library/yql/dq/common/dq_common.h>

namespace NYql::NDq {

// Client           Aggregator
//   -----------------> EvUpdateCounterValue
//  <-----------------  EvOnAggregatedValueUpdated
//  <-----------------  EvOnAggregatedValueUpdated
struct TInfoAggregationActorEvents {
    struct TEvUpdateCounter : public NActors::TEventPB<TEvUpdateCounter, NDqProto::TEvUpdateCounterValue, TDqComputeEvents::EvUpdateCounterValue> {
        using TBase = NActors::TEventPB<TEvUpdateCounter, NDqProto::TEvUpdateCounterValue, TDqComputeEvents::EvUpdateCounterValue>;
        using TBase::TBase;
    };

    struct TEvOnAggregateUpdated : public NActors::TEventPB<TEvOnAggregateUpdated, NDqProto::TEvOnAggregatedValueUpdated, TDqComputeEvents::EvOnAggregatedValueUpdated> {
        using TBase = NActors::TEventPB<TEvOnAggregateUpdated, NDqProto::TEvOnAggregatedValueUpdated, TDqComputeEvents::EvOnAggregatedValueUpdated>;
        using TBase::TBase;
    };
};

// Single actor where will be calculated final aggregate.
// Expected usage: one actor on DQ graph life time, so all registered clients are not removed
NActors::IActor* CreateDqInfoAggregationActor(const TTxId& txId);

} // namespace NYql::NDq

#pragma once

#include <ydb/library/actors/core/events.h>
#include <ydb/library/yql/providers/pq/proto/dq_io.pb.h>
#include <ydb/library/yql/providers/pq/proto/dq_pq_control_plane.pb.h>

namespace NYql::NDq {

inline constexpr char PqControlPlaneActorType[] = "PqControlPlane";
inline constexpr char PqControlPlaneActorIdParam[] = "ControlPlane/PqSourceControlPlaneActorId";

struct TPqControlPlaneEvents {
    enum EEv {
        EvDescribeConsumer = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvDescribeConsumerResult,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE));

    struct TEvDescribeConsumer : NActors::TEventPB<TEvDescribeConsumer, NPq::NProto::TEvDescribeConsumer, EvDescribeConsumer> {};
    struct TEvDescribeConsumerResult : NActors::TEventPB<TEvDescribeConsumerResult, NPq::NProto::TEvDescribeConsumerResult, EvDescribeConsumerResult> {};
};

// Client           DqPqInfoAggregationActor
//   -----------------> EvUpdateCounterValue
//  <-----------------  EvOnAggregatedValueUpdated
//  <-----------------  EvOnAggregatedValueUpdated
struct TPqInfoAggregationActorEvents {
    enum EEv {
        EvUpdateCounterValue = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvOnAggregatedValueUpdated,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE)");

    struct TEvUpdateCounter : public NActors::TEventPB<TEvUpdateCounter, NPq::NProto::TEvDqPqUpdateCounterValue, EvUpdateCounterValue> {
        using TBase = NActors::TEventPB<TEvUpdateCounter, NPq::NProto::TEvDqPqUpdateCounterValue, EvUpdateCounterValue>;
        using TBase::TBase;
    };

    struct TEvOnAggregateUpdated : public NActors::TEventPB<TEvOnAggregateUpdated, NPq::NProto::TEvDqPqOnAggregatedValueUpdated, EvOnAggregatedValueUpdated> {
        using TBase = NActors::TEventPB<TEvOnAggregateUpdated, NPq::NProto::TEvDqPqOnAggregatedValueUpdated, EvOnAggregatedValueUpdated>;
        using TBase::TBase;
    };
};

} // namespace NYql::NDq

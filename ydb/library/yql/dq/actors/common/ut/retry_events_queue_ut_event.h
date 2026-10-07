#pragma once

#include <ydb/library/yql/dq/actors/dq_events_ids.h>
#include <ydb/library/yql/dq/actors/protos/dq_events.pb.h>

#include <ydb/library/actors/core/event_pb.h>

namespace NYql::NDq::TEvDqCompute {

struct TEvInjectCheckpoint : public NActors::TEventPB<TEvInjectCheckpoint,
    NDqProto::TEvInjectCheckpoint, TDqComputeEvents::EvInjectCheckpoint>
{
    TEvInjectCheckpoint() = default;
};

} // namespace NYql::NDq::TEvDqCompute

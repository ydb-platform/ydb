#pragma once

#include <ydb/library/yql/providers/common/http_gateway/yql_http_gateway.h>
#include <ydb/library/yql/dq/actors/dq_events_ids.h>

#include <library/cpp/actors/core/events.h>
#include <library/cpp/actors/core/event_local.h>

namespace NYql::NDq {
using namespace NActors;

namespace NTransformActor {
    struct TEvTransformNewData : public TEventLocal<TEvTransformNewData, TDqComputeEvents::EvTransformNewData> {
        TEvTransformNewData() {}
    };

    struct TEvTransformCompleted : public TEventLocal<TEvTransformCompleted, TDqComputeEvents::EvTransformCompleted> {
        TEvTransformCompleted() {}
    };

} // namespace NTransformActor

}
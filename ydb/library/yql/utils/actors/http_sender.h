#pragma once

#include <ydb/library/yql/providers/solomon/proto/dq_solomon_shard.pb.h>
#include <ydb/library/yql/public/udf/udf_data_type.h>

#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/core/event_local.h>
#include <library/cpp/actors/core/events.h>
#include <library/cpp/actors/http/http_proxy.h>

namespace NYql::NDq {

struct TEvHttpBase {
    // Event ids
    enum EEv : ui32 {
        EvBegin = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),

        EvSendResult = EvBegin,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE)");

    // Events

    struct TEvSendResult : public NActors::TEventLocal<TEvSendResult, EvSendResult> {
        TEvSendResult(
            const NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr& httpIncomingResponse,
            ui32 retryCount,
            bool isTerminal)
            : HttpIncomingResponse(httpIncomingResponse)
            , RetryCount(retryCount)
            , IsTerminal(isTerminal)
        { }

        NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr HttpIncomingResponse;
        ui32 RetryCount = 0;
        bool IsTerminal = false;
    };
};

}

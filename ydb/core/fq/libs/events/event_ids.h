#pragma once
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/events.h>

namespace NFq {

struct TEventIds {
    enum EEventSpaceYqlProxy {
        ES_YQL_ANALYTICS_PROXY = 4205 //TKikimrEvents::ES_YQL_ANALYTICS_PROXY
    };

    static constexpr ui32 EventSpace = ES_YQL_ANALYTICS_PROXY;

    // Event ids.
    enum EEv : ui32 {
        EvBegin = EventSpaceBegin(EventSpace),

        //YQL Internal
        EvPingTaskRequest = EvBegin,
        EvPingTaskResponse,
        EvGetTaskRequest,
        EvGetTaskResponse,
        EvWriteTaskResultRequest,
        EvWriteTaskResultResponse,

        EvNodesHealthCheckRequest,
        EvNodesHealthCheckResponse,

        EvUpdateConfig,

        // Internal events
        EvAsyncContinue,
        EvEndpointRequest,
        EvEndpointResponse,
        EvDataStreamsReadRulesCreationResult,
        EvDataStreamsReadRulesDeletionResult,
        EvQueryActionResult,
        EvForwardPingRequest,
        EvForwardPingResponse,
        EvGraphParams,
        EvRaiseTransientIssues,
        EvSchemaCreated,
        EvCallback,
        EvEffectApplicationResult,

        EvCreateRateLimiterResourceRequest,
        EvCreateRateLimiterResourceResponse,
        EvDeleteRateLimiterResourceRequest,
        EvDeleteRateLimiterResourceResponse,

        EvSchemaDeleted,
        EvSchemaUpdated,

        // Special events
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(EventSpace), "expect EvEnd < EventSpaceEnd(EventSpace)");

};

} // namespace NFq

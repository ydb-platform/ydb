#pragma once

#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_local.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

namespace NDbPool {

struct TEvents {
    enum EEv {
        EvDbFunctionRequest = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvDbFunctionResponse,
        EvEnd,
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE)");

    struct TEvDbFunctionRequest : NActors::TEventLocal<TEvDbFunctionRequest, EvDbFunctionRequest> {
        using TFunction = std::function<NYdb::TAsyncStatus(NYdb::NTable::TSession&)>;
        TFunction Handler;

        explicit TEvDbFunctionRequest(TFunction handler)
            : Handler(std::move(handler))
        {}
    };

    struct TEvDbFunctionResponse : NActors::TEventLocal<TEvDbFunctionResponse, EvDbFunctionResponse> {
        NYdb::TStatus Status;

        explicit TEvDbFunctionResponse(NYdb::TStatus status)
            : Status(status)
        {}
    };
};

} // namespace NDbPool

#pragma once

#include "private_client.h"

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event_local.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <ydb/core/fq/libs/events/event_subspace.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

#include <ydb/core/fq/libs/shared_resources/shared_resources.h>

#include <ydb/core/fq/libs/control_plane_storage/proto/yq_internal.pb.h>

namespace NFq {

struct TEvInternalService {
    // Event ids.
    enum EEv : ui32 {
        EvHealthCheckRequest = YqEventSubspaceBegin(NFq::TYqEventSubspace::InternalService),
        EvHealthCheckResponse,
        EvGetTaskRequest,
        EvGetTaskResponse,
        EvPingTaskRequest,
        EvPingTaskResponse,
        EvWriteResultRequest,
        EvWriteResultResponse,
        EvCreateRateLimiterResourceRequest,
        EvCreateRateLimiterResourceResponse,
        EvDeleteRateLimiterResourceRequest,
        EvDeleteRateLimiterResourceResponse,
        EvEnd,
    };

    static_assert(EvEnd <= YqEventSubspaceEnd(NFq::TYqEventSubspace::InternalService), "All events must be in their subspace");

    template <class TProtoRequest, ui32 TEventType>
    struct TInternalServiceRequestEvent : public NActors::TEventLocal<TInternalServiceRequestEvent<TProtoRequest, TEventType>, TEventType> {
        using TProto = TProtoRequest;
        TProtoRequest Request;
        TInstant SentAt;
        explicit TInternalServiceRequestEvent(const TProtoRequest& request)
            : Request(request), SentAt(Now())
        { }
    };

    using TEvHealthCheckRequest = TInternalServiceRequestEvent<Fq::Private::NodesHealthCheckRequest, EvHealthCheckRequest>;
    using TEvGetTaskRequest = TInternalServiceRequestEvent<Fq::Private::GetTaskRequest, EvGetTaskRequest>;
    using TEvPingTaskRequest = TInternalServiceRequestEvent<Fq::Private::PingTaskRequest, EvPingTaskRequest>;
    using TEvWriteResultRequest = TInternalServiceRequestEvent<Fq::Private::WriteTaskResultRequest, EvWriteResultRequest>;
    using TEvCreateRateLimiterResourceRequest = TInternalServiceRequestEvent<Fq::Private::CreateRateLimiterResourceRequest, EvCreateRateLimiterResourceRequest>;
    using TEvDeleteRateLimiterResourceRequest = TInternalServiceRequestEvent<Fq::Private::DeleteRateLimiterResourceRequest, EvDeleteRateLimiterResourceRequest>;

    template <class TProtoResult, ui32 TEventType>
    struct TInternalServiceResponseEvent : public NActors::TEventLocal<TInternalServiceResponseEvent<TProtoResult, TEventType>, TEventType> {
        using TProto = TProtoResult;
        NYdb::TStatus Status;
        TProtoResult Result;
        explicit TInternalServiceResponseEvent(const TProtoResultInternalWrapper<TProtoResult>& wrappedResult) : Status(wrappedResult) {
            if (wrappedResult.IsResultSet()) {
                Result = wrappedResult.GetResult();
            }
        }
        explicit TInternalServiceResponseEvent(const TString& errorMessage) : Status(NYdb::EStatus::INTERNAL_ERROR, {NYql::TIssue(errorMessage).SetCode(NYql::UNEXPECTED_ERROR, NYql::TSeverityIds::S_ERROR)}) {
        }
        explicit TInternalServiceResponseEvent(const TProtoResult& result) : Status(NYdb::EStatus::SUCCESS, NYql::TIssues()), Result(result) {
        }
        TInternalServiceResponseEvent(NYdb::EStatus statusCode, NYql::TIssues&& issues) : Status(statusCode, std::move(issues)) {
        }
    };

    using TEvHealthCheckResponse = TInternalServiceResponseEvent<Fq::Private::NodesHealthCheckResult, EvHealthCheckResponse>;
    using TEvGetTaskResponse = TInternalServiceResponseEvent<Fq::Private::GetTaskResult, EvGetTaskResponse>;
    using TEvPingTaskResponse = TInternalServiceResponseEvent<Fq::Private::PingTaskResult, EvPingTaskResponse>;
    using TEvWriteResultResponse = TInternalServiceResponseEvent<Fq::Private::WriteTaskResultResult, EvWriteResultResponse>;
    using TEvCreateRateLimiterResourceResponse = TInternalServiceResponseEvent<Fq::Private::CreateRateLimiterResourceResult, EvCreateRateLimiterResourceResponse>;
    using TEvDeleteRateLimiterResourceResponse = TInternalServiceResponseEvent<Fq::Private::DeleteRateLimiterResourceResult, EvDeleteRateLimiterResourceResponse>;
};

NActors::TActorId MakeInternalServiceActorId();

} /* NFq */

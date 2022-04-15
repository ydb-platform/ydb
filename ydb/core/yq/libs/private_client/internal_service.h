#pragma once

#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/core/event_local.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <ydb/core/yq/libs/events/event_subspace.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

#include <ydb/core/yq/libs/shared_resources/shared_resources.h>

#include <ydb/core/yq/libs/control_plane_storage/proto/yq_internal.pb.h>

namespace NYq {

struct TEvInternalService {
    // Event ids.
    enum EEv : ui32 {
        EvHealthCheckRequest = YqEventSubspaceBegin(NYq::TYqEventSubspace::InternalService),
        EvHealthCheckResponse,
        EvGetTaskRequest,
        EvGetTaskResponse,
        EvPingTaskRequest,
        EvPingTaskResponse,
        EvWriteResultRequest,
        EvWriteResultResponse,
        EvEnd,
    };

    static_assert(EvEnd <= YqEventSubspaceEnd(NYq::TYqEventSubspace::InternalService), "All events must be in their subspace");

    struct TEvHealthCheckRequest : public NActors::TEventLocal<TEvHealthCheckRequest, EvHealthCheckRequest> {
        Yq::Private::NodesHealthCheckRequest Request;
        TInstant SentAt;
        explicit TEvHealthCheckRequest(const Yq::Private::NodesHealthCheckRequest& request)
            : Request(request), SentAt(Now())
        { }
    };

    struct TEvHealthCheckResponse : public NActors::TEventLocal<TEvHealthCheckResponse, EvHealthCheckResponse>{
        bool Success;
        NYdb::EStatus Status;
        const NYql::TIssues Issues;
        Yq::Private::NodesHealthCheckResult Result;
        TEvHealthCheckResponse(bool success, NYdb::EStatus status, const NYql::TIssues& issues, const Yq::Private::NodesHealthCheckResult& result)
            : Success(success), Status(status), Issues(issues), Result(result)
        { }
    };

    struct TEvGetTaskRequest : public NActors::TEventLocal<TEvGetTaskRequest, EvGetTaskRequest> {
        Yq::Private::GetTaskRequest Request;
        TInstant SentAt;
        explicit TEvGetTaskRequest(const Yq::Private::GetTaskRequest& request)
            : Request(request), SentAt(Now())
        { }
    };

    struct TEvGetTaskResponse : public NActors::TEventLocal<TEvGetTaskResponse, EvGetTaskResponse> {
        bool Success = false;
        NYdb::EStatus Status;
        const NYql::TIssues Issues;
        const Yq::Private::GetTaskResult Result;
        TEvGetTaskResponse(bool success, NYdb::EStatus status, const NYql::TIssues& issues, const Yq::Private::GetTaskResult& result)
            : Success(success), Status(status), Issues(issues), Result(result)
        { }
    };

    struct TEvPingTaskRequest : public NActors::TEventLocal<TEvPingTaskRequest, EvPingTaskRequest> {
        Yq::Private::PingTaskRequest Request;
        TInstant SentAt;
        explicit TEvPingTaskRequest(const Yq::Private::PingTaskRequest& request)
            : Request(request), SentAt(Now())
        { }
    };

    struct TEvPingTaskResponse : public NActors::TEventLocal<TEvPingTaskResponse, EvPingTaskResponse> {
        bool Success = false;
        NYdb::EStatus Status;
        const NYql::TIssues Issues;
        const Yq::Private::PingTaskResult Result;
        bool TransportError = false;
        TEvPingTaskResponse(bool success, NYdb::EStatus status, const NYql::TIssues& issues, const Yq::Private::PingTaskResult& result, bool transportError = false)
            : Success(success), Status(status), Issues(issues), Result(result), TransportError(transportError)
        { }
        YandexQuery::QueryAction GetAction() {
            return Success ? Result.action() : YandexQuery::QUERY_ACTION_UNSPECIFIED;
        }
    };

    struct TEvWriteResultRequest : public NActors::TEventLocal<TEvWriteResultRequest, EvWriteResultRequest> {
        Yq::Private::WriteTaskResultRequest Request;
        TInstant SentAt;
        explicit TEvWriteResultRequest(const Yq::Private::WriteTaskResultRequest& request)
            : Request(request), SentAt(Now())
        { }
    };

    struct TEvWriteResultResponse : public NActors::TEventLocal<TEvWriteResultResponse, EvWriteResultResponse> {
        bool Success = false;
        NYdb::EStatus Status;
        const NYql::TIssues Issues;
        const Yq::Private::WriteTaskResultResult Result;
        TEvWriteResultResponse(bool success, NYdb::EStatus status, const NYql::TIssues& issues, const Yq::Private::WriteTaskResultResult& result)
            : Success(success), Status(status), Issues(issues), Result(result)
        { }
    };
};

NActors::TActorId MakeInternalServiceActorId();

NActors::IActor* CreateInternalServiceActor(
    const NYq::TYqSharedResources::TPtr& yqSharedResources,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    const NYq::NConfig::TPrivateApiConfig& privateApiConfig,
    const NMonitoring::TDynamicCounterPtr& counters);

} /* NYq */

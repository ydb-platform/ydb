#pragma once

#include <util/datetime/base.h>
#include <util/generic/map.h>
#include <util/generic/maybe.h>

#include <library/cpp/actors/core/event_pb.h>
#include <library/cpp/actors/core/events.h>
#include <library/cpp/actors/interconnect/events_local.h>

#include <ydb/public/api/protos/draft/yq_private.pb.h>
#include <ydb/public/api/protos/yq.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_params/params.h>

#include <ydb/library/yql/public/issue/yql_issue.h>

#include <ydb/core/yq/libs/control_plane_storage/proto/yq_internal.pb.h>
#include <ydb/core/yq/libs/events/event_subspace.h>

namespace NYq {

template<typename T>
struct TAuditDetails {
    TMaybe<T> Before;
    TMaybe<T> After;
    bool IdempotencyResult = false;
    TString CloudId;
};

struct TNodeInfo {
    ui32 NodeId;
    TString InstanceId;
    TString HostName;
};

struct TDebugItem {
    TString Query;
    NYdb::TParams Params;
    TString Plan;
    TString Ast;
    TString Error;

    TString ToString() const {
        TString result;
        result += "Query: " + Query + "\n";
        result += "Plan: " + Plan + "\n";
        result += "Ast: " + Ast + "\n";
        for (const auto& param: Params.GetValues()) {
            result += "Params: " + param.first + ", " + param.second.GetType().ToString() + "\n";
        }
        result += "Error: " + Error + "\n";
        return result;
    }
};

using TDebugInfo = TVector<TDebugItem>;
using TDebugInfoPtr = std::shared_ptr<TDebugInfo>;

struct TPermissions {
    enum TPermission {
        VIEW_PUBLIC = 0x1,
        VIEW_PRIVATE = 0x2,
        VIEW_AST = 0x4,
        MANAGE_PUBLIC = 0x8,
        MANAGE_PRIVATE = 0x10,
        CONNECTIONS_USE = 0x40,
        BINDINGS_USE = 0x80,
        QUERY_INVOKE = 0x100
    };

private:
    uint32_t Permissions = 0;

public:
    TPermissions()
    {}

    explicit TPermissions(uint32_t permissions)
        : Permissions(permissions)
    {}

    void Set(TPermission permission) {
        Permissions |= permission;
    }

    bool Check(TPermission permission) const {
        return Permissions & permission;
    }

    void SetAll() {
        Permissions = 0xFFFFFFFF;
    }
};

struct TEvControlPlaneStorage {
    // Event ids.
    enum EEv : ui32 {
        EvCreateQueryRequest = YqEventSubspaceBegin(NYq::TYqEventSubspace::ControlPlaneStorage),
        EvCreateQueryResponse,
        EvListQueriesRequest,
        EvListQueriesResponse,
        EvDescribeQueryRequest,
        EvDescribeQueryResponse,
        EvGetQueryStatusRequest,
        EvGetQueryStatusResponse,
        EvModifyQueryRequest,
        EvModifyQueryResponse,
        EvDeleteQueryRequest,
        EvDeleteQueryResponse,
        EvControlQueryRequest,
        EvControlQueryResponse,
        EvGetResultDataRequest,
        EvGetResultDataResponse,
        EvListJobsRequest,
        EvListJobsResponse,
        EvDescribeJobRequest,
        EvDescribeJobResponse,
        EvCreateConnectionRequest,
        EvCreateConnectionResponse,
        EvListConnectionsRequest,
        EvListConnectionsResponse,
        EvDescribeConnectionRequest,
        EvDescribeConnectionResponse,
        EvModifyConnectionRequest,
        EvModifyConnectionResponse,
        EvDeleteConnectionRequest,
        EvDeleteConnectionResponse,
        EvCreateBindingRequest,
        EvCreateBindingResponse,
        EvListBindingsRequest,
        EvListBindingsResponse,
        EvDescribeBindingRequest,
        EvDescribeBindingResponse,
        EvModifyBindingRequest,
        EvModifyBindingResponse,
        EvDeleteBindingRequest,
        EvDeleteBindingResponse,
        EvWriteResultDataRequest,
        EvWriteResultDataResponse,
        EvGetTaskRequest,
        EvGetTaskResponse,
        EvPingTaskRequest,
        EvPingTaskResponse,
        EvNodesHealthCheckRequest,
        EvNodesHealthCheckResponse,
        EvGetHealthNodesRequest,
        EvGetHealthNodesResponse,
        EvEnd,
    };

    static_assert(EvEnd <= YqEventSubspaceEnd(NYq::TYqEventSubspace::ControlPlaneStorage), "All events must be in their subspace");

    struct TEvCreateQueryRequest : NActors::TEventLocal<TEvCreateQueryRequest, EvCreateQueryRequest> {
        explicit TEvCreateQueryRequest(const TString& scope,
                                       const YandexQuery::CreateQueryRequest& request,
                                       const TString& user,
                                       const TString& token,
                                       const TString& cloudId,
                                       TPermissions permissions)
            : Scope(scope)
            , Request(request)
            , User(user)
            , Token(token)
            , CloudId(cloudId)
            , Permissions(permissions)
        {
        }

        TString Scope;
        YandexQuery::CreateQueryRequest Request;
        TString User;
        TString Token;
        TString CloudId;
        TPermissions Permissions;
    };

    struct TEvCreateQueryResponse : NActors::TEventLocal<TEvCreateQueryResponse, EvCreateQueryResponse> {
        explicit TEvCreateQueryResponse(const YandexQuery::CreateQueryResult& result,
                                        const TAuditDetails<YandexQuery::Query>& auditDetails)
            : Result(result)
            , AuditDetails(auditDetails)
        {
        }

        explicit TEvCreateQueryResponse(const NYql::TIssues& issues)
            : Issues(issues)
        {
        }

        YandexQuery::CreateQueryResult Result;
        TAuditDetails<YandexQuery::Query> AuditDetails;
        NYql::TIssues Issues;
        TDebugInfoPtr DebugInfo;
    };

    struct TEvListQueriesRequest : NActors::TEventLocal<TEvListQueriesRequest, EvListQueriesRequest> {
        explicit TEvListQueriesRequest(const TString& scope,
                                       const YandexQuery::ListQueriesRequest& request,
                                       const TString& user,
                                       const TString& token,
                                       TPermissions permissions)
            : Scope(scope)
            , Request(request)
            , User(user)
            , Token(token)
            , Permissions(permissions)
        {
        }

        TString Scope;
        YandexQuery::ListQueriesRequest Request;
        TString User;
        TString Token;
        TPermissions Permissions;
    };

    struct TEvListQueriesResponse : NActors::TEventLocal<TEvListQueriesResponse, EvListQueriesResponse> {
        explicit TEvListQueriesResponse(const YandexQuery::ListQueriesResult& result)
            : Result(result)
        {
        }

        explicit TEvListQueriesResponse(const NYql::TIssues& issues)
            : Issues(issues)
        {
        }

        YandexQuery::ListQueriesResult Result;
        NYql::TIssues Issues;
        TDebugInfoPtr DebugInfo;
    };

    struct TEvDescribeQueryRequest : NActors::TEventLocal<TEvDescribeQueryRequest, EvDescribeQueryRequest> {
        explicit TEvDescribeQueryRequest(const TString& scope,
                                         const YandexQuery::DescribeQueryRequest& request,
                                         const TString& user,
                                         const TString& token,
                                         TPermissions permissions)
            : Scope(scope)
            , Request(request)
            , User(user)
            , Token(token)
            , Permissions(permissions)
        {
        }

        TString Scope;
        YandexQuery::DescribeQueryRequest Request;
        TString User;
        TString Token;
        TPermissions Permissions;
    };

    struct TEvDescribeQueryResponse : NActors::TEventLocal<TEvDescribeQueryResponse, EvDescribeQueryResponse> {
        explicit TEvDescribeQueryResponse(const YandexQuery::DescribeQueryResult& result)
            : Result(result)
        {
        }

        explicit TEvDescribeQueryResponse(const NYql::TIssues& issues)
            : Issues(issues)
        {
        }

        YandexQuery::DescribeQueryResult Result;
        NYql::TIssues Issues;
        TDebugInfoPtr DebugInfo;
    };

    struct TEvGetQueryStatusRequest : NActors::TEventLocal<TEvGetQueryStatusRequest, EvGetQueryStatusRequest> {
        explicit TEvGetQueryStatusRequest(const TString& scope,
                                         const YandexQuery::GetQueryStatusRequest& request,
                                         const TString& user,
                                         const TString& token,
                                         TPermissions permissions)
            : Scope(scope)
            , Request(request)
            , User(user)
            , Token(token)
            , Permissions(permissions)
        {
        }

        TString Scope;
        YandexQuery::GetQueryStatusRequest Request;
        TString User;
        TString Token;
        TPermissions Permissions;
    };

    struct TEvGetQueryStatusResponse : NActors::TEventLocal<TEvGetQueryStatusResponse, EvGetQueryStatusResponse> {
        explicit TEvGetQueryStatusResponse(const YandexQuery::GetQueryStatusResult& result)
            : Result(result)
        {
        }

        explicit TEvGetQueryStatusResponse(const NYql::TIssues& issues)
            : Issues(issues)
        {
        }

        YandexQuery::GetQueryStatusResult Result;
        NYql::TIssues Issues;
        TDebugInfoPtr DebugInfo;
    };

    struct TEvModifyQueryRequest : NActors::TEventLocal<TEvModifyQueryRequest, EvModifyQueryRequest> {
        explicit TEvModifyQueryRequest(const TString& scope,
                                       const YandexQuery::ModifyQueryRequest& request,
                                       const TString& user,
                                       const TString& token,
                                       TPermissions permissions)
            : Scope(scope)
            , Request(request)
            , User(user)
            , Token(token)
            , Permissions(permissions)
        {
        }

        TString Scope;
        YandexQuery::ModifyQueryRequest Request;
        TString User;
        TString Token;
        TPermissions Permissions;
    };

    struct TEvModifyQueryResponse : NActors::TEventLocal<TEvModifyQueryResponse, EvModifyQueryResponse> {
        explicit TEvModifyQueryResponse(const YandexQuery::ModifyQueryResult& result,
                                        const TAuditDetails<YandexQuery::Query>& auditDetails)
            : Result(result)
            , AuditDetails(auditDetails)
        {
        }

        explicit TEvModifyQueryResponse(const NYql::TIssues& issues)
            : Issues(issues)
        {
        }

        YandexQuery::ModifyQueryResult Result;
        TAuditDetails<YandexQuery::Query> AuditDetails;
        NYql::TIssues Issues;
        TDebugInfoPtr DebugInfo;
    };

    struct TEvDeleteQueryRequest : NActors::TEventLocal<TEvDeleteQueryRequest, EvDeleteQueryRequest> {
        explicit TEvDeleteQueryRequest(const TString& scope,
                                       const YandexQuery::DeleteQueryRequest& request,
                                       const TString& user,
                                       const TString& token,
                                       TPermissions permissions)
            : Scope(scope)
            , Request(request)
            , User(user)
            , Token(token)
            , Permissions(permissions)
        {
        }

        TString Scope;
        YandexQuery::DeleteQueryRequest Request;
        TString User;
        TString Token;
        TPermissions Permissions;
    };

    struct TEvDeleteQueryResponse : NActors::TEventLocal<TEvDeleteQueryResponse, EvDeleteQueryResponse> {
        explicit TEvDeleteQueryResponse(const YandexQuery::DeleteQueryResult& result,
                                        const TAuditDetails<YandexQuery::Query>& auditDetails)
            : Result(result)
            , AuditDetails(auditDetails)
        {
        }

        explicit TEvDeleteQueryResponse(const NYql::TIssues& issues)
            : Issues(issues)
        {
        }

        YandexQuery::DeleteQueryResult Result;
        TAuditDetails<YandexQuery::Query> AuditDetails;
        NYql::TIssues Issues;
        TDebugInfoPtr DebugInfo;
    };

    struct TEvControlQueryRequest : NActors::TEventLocal<TEvControlQueryRequest, EvControlQueryRequest> {
        explicit TEvControlQueryRequest(const TString& scope,
                                        const YandexQuery::ControlQueryRequest& request,
                                        const TString& user,
                                        const TString& token,
                                        TPermissions permissions)
            : Scope(scope)
            , Request(request)
            , User(user)
            , Token(token)
            , Permissions(permissions)
        {
        }

        TString Scope;
        YandexQuery::ControlQueryRequest Request;
        TString User;
        TString Token;
        TPermissions Permissions;
    };

    struct TEvControlQueryResponse : NActors::TEventLocal<TEvControlQueryResponse, EvControlQueryResponse> {
        explicit TEvControlQueryResponse(const YandexQuery::ControlQueryResult& result,
                                         const TAuditDetails<YandexQuery::Query>& auditDetails)
            : Result(result)
            , AuditDetails(auditDetails)
        {
        }

        explicit TEvControlQueryResponse(const NYql::TIssues& issues)
            : Issues(issues)
        {
        }

        YandexQuery::ControlQueryResult Result;
        TAuditDetails<YandexQuery::Query> AuditDetails;
        NYql::TIssues Issues;
        TDebugInfoPtr DebugInfo;
    };

    struct TEvGetResultDataRequest : NActors::TEventLocal<TEvGetResultDataRequest, EvGetResultDataRequest> {
        explicit TEvGetResultDataRequest(const TString& scope,
                                         const YandexQuery::GetResultDataRequest& request,
                                         const TString& user,
                                         const TString& token,
                                         TPermissions permissions)
            : Scope(scope)
            , Request(request)
            , User(user)
            , Token(token)
            , Permissions(permissions)
        {
        }

        TString Scope;
        YandexQuery::GetResultDataRequest Request;
        TString User;
        TString Token;
        TPermissions Permissions;
    };

    struct TEvGetResultDataResponse : NActors::TEventLocal<TEvGetResultDataResponse, EvGetResultDataResponse> {
        explicit TEvGetResultDataResponse(const YandexQuery::GetResultDataResult& result)
            : Result(result)
        {
        }

        explicit TEvGetResultDataResponse(const NYql::TIssues& issues)
            : Issues(issues)
        {
        }

        YandexQuery::GetResultDataResult Result;
        NYql::TIssues Issues;
        TDebugInfoPtr DebugInfo;
    };

    struct TEvListJobsRequest : NActors::TEventLocal<TEvListJobsRequest, EvListJobsRequest> {
        explicit TEvListJobsRequest(const TString& scope,
                                    const YandexQuery::ListJobsRequest& request,
                                    const TString& user,
                                    const TString& token,
                                    TPermissions permissions)
            : Scope(scope)
            , Request(request)
            , User(user)
            , Token(token)
            , Permissions(permissions)
        {
        }

        TString Scope;
        YandexQuery::ListJobsRequest Request;
        TString User;
        TString Token;
        TPermissions Permissions;
    };

    struct TEvListJobsResponse : NActors::TEventLocal<TEvListJobsResponse, EvListJobsResponse> {
        explicit TEvListJobsResponse(const YandexQuery::ListJobsResult& result)
            : Result(result)
        {
        }

        explicit TEvListJobsResponse(const NYql::TIssues& issues)
            : Issues(issues)
        {
        }

        YandexQuery::ListJobsResult Result;
        NYql::TIssues Issues;
        TDebugInfoPtr DebugInfo;
    };

    struct TEvDescribeJobRequest : NActors::TEventLocal<TEvDescribeJobRequest, EvDescribeJobRequest> {
        explicit TEvDescribeJobRequest(const TString& scope,
                                           const YandexQuery::DescribeJobRequest& request,
                                           const TString& user,
                                           const TString& token,
                                           TPermissions permissions)
            : Scope(scope)
            , Request(request)
            , User(user)
            , Token(token)
            , Permissions(permissions)
        {
        }

        TString Scope;
        YandexQuery::DescribeJobRequest Request;
        TString User;
        TString Token;
        TPermissions Permissions;
    };

    struct TEvDescribeJobResponse : NActors::TEventLocal<TEvDescribeJobResponse, EvDescribeJobResponse> {
        explicit TEvDescribeJobResponse(const YandexQuery::DescribeJobResult& result)
            : Result(result)
        {
        }

        explicit TEvDescribeJobResponse(const NYql::TIssues& issues)
            : Issues(issues)
        {
        }

        YandexQuery::DescribeJobResult Result;
        NYql::TIssues Issues;
        TDebugInfoPtr DebugInfo;
    };

    struct TEvCreateConnectionRequest : NActors::TEventLocal<TEvCreateConnectionRequest, EvCreateConnectionRequest> {
        explicit TEvCreateConnectionRequest(const TString& scope,
                                            const YandexQuery::CreateConnectionRequest& request,
                                            const TString& user,
                                            const TString& token,
                                            const TString& cloudId,
                                            TPermissions permissions)
            : Scope(scope)
            , Request(request)
            , User(user)
            , Token(token)
            , CloudId(cloudId)
            , Permissions(permissions)
        {
        }

        TString Scope;
        YandexQuery::CreateConnectionRequest Request;
        TString User;
        TString Token;
        TString CloudId;
        TPermissions Permissions;
    };

    struct TEvCreateConnectionResponse : NActors::TEventLocal<TEvCreateConnectionResponse, EvCreateConnectionResponse> {
        explicit TEvCreateConnectionResponse(const YandexQuery::CreateConnectionResult& result,
                                             const TAuditDetails<YandexQuery::Connection>& auditDetails)
            : Result(result)
            , AuditDetails(auditDetails)
        {
        }

        explicit TEvCreateConnectionResponse(const NYql::TIssues& issues)
            : Issues(issues)
        {
        }

        YandexQuery::CreateConnectionResult Result;
        TAuditDetails<YandexQuery::Connection> AuditDetails;
        NYql::TIssues Issues;
        TDebugInfoPtr DebugInfo;
    };

    struct TEvListConnectionsRequest : NActors::TEventLocal<TEvListConnectionsRequest, EvListConnectionsRequest> {
        explicit TEvListConnectionsRequest(const TString& scope,
                                           const YandexQuery::ListConnectionsRequest& request,
                                           const TString& user,
                                           const TString& token,
                                           TPermissions permissions)
            : Scope(scope)
            , Request(request)
            , User(user)
            , Token(token)
            , Permissions(permissions)
        {
        }

        TString Scope;
        YandexQuery::ListConnectionsRequest Request;
        TString User;
        TString Token;
        TPermissions Permissions;
    };

    struct TEvListConnectionsResponse : NActors::TEventLocal<TEvListConnectionsResponse, EvListConnectionsResponse> {
        explicit TEvListConnectionsResponse(const YandexQuery::ListConnectionsResult& result)
            : Result(result)
        {
        }

        explicit TEvListConnectionsResponse(const NYql::TIssues& issues)
            : Issues(issues)
        {
        }

        YandexQuery::ListConnectionsResult Result;
        NYql::TIssues Issues;
        TDebugInfoPtr DebugInfo;
    };

    struct TEvDescribeConnectionRequest : NActors::TEventLocal<TEvDescribeConnectionRequest, EvDescribeConnectionRequest> {
        explicit TEvDescribeConnectionRequest(const TString& scope,
                                              const YandexQuery::DescribeConnectionRequest& request,
                                              const TString& user,
                                              const TString& token,
                                              TPermissions permissions)
            : Scope(scope)
            , Request(request)
            , User(user)
            , Token(token)
            , Permissions(permissions)
        {
        }

        TString Scope;
        YandexQuery::DescribeConnectionRequest Request;
        TString User;
        TString Token;
        TPermissions Permissions;
    };

    struct TEvDescribeConnectionResponse : NActors::TEventLocal<TEvDescribeConnectionResponse, EvDescribeConnectionResponse> {
        explicit TEvDescribeConnectionResponse(const YandexQuery::DescribeConnectionResult& result)
            : Result(result)
        {
        }

        explicit TEvDescribeConnectionResponse(const NYql::TIssues& issues)
            : Issues(issues)
        {
        }

        YandexQuery::DescribeConnectionResult Result;
        NYql::TIssues Issues;
        TDebugInfoPtr DebugInfo;
    };

    struct TEvModifyConnectionRequest : NActors::TEventLocal<TEvModifyConnectionRequest, EvModifyConnectionRequest> {
        explicit TEvModifyConnectionRequest(const TString& scope,
                                            const YandexQuery::ModifyConnectionRequest& request,
                                            const TString& user,
                                            const TString& token,
                                            TPermissions permissions)
            : Scope(scope)
            , Request(request)
            , User(user)
            , Token(token)
            , Permissions(permissions)
        {
        }

        TString Scope;
        YandexQuery::ModifyConnectionRequest Request;
        TString User;
        TString Token;
        TPermissions Permissions;
    };

    struct TEvModifyConnectionResponse : NActors::TEventLocal<TEvModifyConnectionResponse, EvModifyConnectionResponse> {
        explicit TEvModifyConnectionResponse(const YandexQuery::ModifyConnectionResult& result,
                                             const TAuditDetails<YandexQuery::Connection>& auditDetails)
            : Result(result)
            , AuditDetails(auditDetails)
        {
        }

        explicit TEvModifyConnectionResponse(const NYql::TIssues& issues)
            : Issues(issues)
        {
        }

        YandexQuery::ModifyConnectionResult Result;
        TAuditDetails<YandexQuery::Connection> AuditDetails;
        NYql::TIssues Issues;
        TDebugInfoPtr DebugInfo;
    };

    struct TEvDeleteConnectionRequest : NActors::TEventLocal<TEvDeleteConnectionRequest, EvDeleteConnectionRequest> {
        explicit TEvDeleteConnectionRequest(const TString& scope,
                                            const YandexQuery::DeleteConnectionRequest& request,
                                            const TString& user,
                                            const TString& token,
                                            TPermissions permissions)
            : Scope(scope)
            , Request(request)
            , User(user)
            , Token(token)
            , Permissions(permissions)
        {
        }

        TString Scope;
        YandexQuery::DeleteConnectionRequest Request;
        TString User;
        TString Token;
        TPermissions Permissions;
    };

    struct TEvDeleteConnectionResponse : NActors::TEventLocal<TEvDeleteConnectionResponse, EvDeleteConnectionResponse> {
        explicit TEvDeleteConnectionResponse(const YandexQuery::DeleteConnectionResult& result,
                                             const TAuditDetails<YandexQuery::Connection>& auditDetails)
            : Result(result)
            , AuditDetails(auditDetails)
        {
        }

        explicit TEvDeleteConnectionResponse(const NYql::TIssues& issues)
            : Issues(issues)
        {
        }

        YandexQuery::DeleteConnectionResult Result;
        TAuditDetails<YandexQuery::Connection> AuditDetails;
        NYql::TIssues Issues;
        TDebugInfoPtr DebugInfo;
    };

    struct TEvCreateBindingRequest : NActors::TEventLocal<TEvCreateBindingRequest, EvCreateBindingRequest> {
        explicit TEvCreateBindingRequest(const TString& scope,
                                         const YandexQuery::CreateBindingRequest& request,
                                         const TString& user,
                                         const TString& token,
                                         const TString& cloudId,
                                         TPermissions permissions)
            : Scope(scope)
            , Request(request)
            , User(user)
            , Token(token)
            , CloudId(cloudId)
            , Permissions(permissions)
        {
        }

        TString Scope;
        YandexQuery::CreateBindingRequest Request;
        TString User;
        TString Token;
        TString CloudId;
        TPermissions Permissions;
    };

    struct TEvCreateBindingResponse : NActors::TEventLocal<TEvCreateBindingResponse, EvCreateBindingResponse> {
        explicit TEvCreateBindingResponse(const YandexQuery::CreateBindingResult& result,
                                          const TAuditDetails<YandexQuery::Binding>& auditDetails)
            : Result(result)
            , AuditDetails(auditDetails)
        {
        }

        explicit TEvCreateBindingResponse(const NYql::TIssues& issues)
            : Issues(issues)
        {
        }

        YandexQuery::CreateBindingResult Result;
        TAuditDetails<YandexQuery::Binding> AuditDetails;
        NYql::TIssues Issues;
        TDebugInfoPtr DebugInfo;
    };

    struct TEvListBindingsRequest : NActors::TEventLocal<TEvListBindingsRequest, EvListBindingsRequest> {
        explicit TEvListBindingsRequest(const TString& scope,
                                        const YandexQuery::ListBindingsRequest& request,
                                        const TString& user,
                                        const TString& token,
                                        TPermissions permissions)
            : Scope(scope)
            , Request(request)
            , User(user)
            , Token(token)
            , Permissions(permissions)
        {
        }

        TString Scope;
        YandexQuery::ListBindingsRequest Request;
        TString User;
        TString Token;
        TPermissions Permissions;
    };

    struct TEvListBindingsResponse : NActors::TEventLocal<TEvListBindingsResponse, EvListBindingsResponse> {
        explicit TEvListBindingsResponse(const YandexQuery::ListBindingsResult& result)
            : Result(result)
        {
        }

        explicit TEvListBindingsResponse(const NYql::TIssues& issues)
            : Issues(issues)
        {
        }

        YandexQuery::ListBindingsResult Result;
        NYql::TIssues Issues;
        TDebugInfoPtr DebugInfo;
    };

    struct TEvDescribeBindingRequest : NActors::TEventLocal<TEvDescribeBindingRequest, EvDescribeBindingRequest> {
        explicit TEvDescribeBindingRequest(const TString& scope,
                                           const YandexQuery::DescribeBindingRequest& request,
                                           const TString& user,
                                           const TString& token,
                                           TPermissions permissions)
            : Scope(scope)
            , Request(request)
            , User(user)
            , Token(token)
            , Permissions(permissions)
        {
        }

        TString Scope;
        YandexQuery::DescribeBindingRequest Request;
        TString User;
        TString Token;
        TPermissions Permissions;
    };

    struct TEvDescribeBindingResponse : NActors::TEventLocal<TEvDescribeBindingResponse, EvDescribeBindingResponse> {
        explicit TEvDescribeBindingResponse(const YandexQuery::DescribeBindingResult& result)
            : Result(result)
        {
        }

        explicit TEvDescribeBindingResponse(const NYql::TIssues& issues)
            : Issues(issues)
        {
        }

        YandexQuery::DescribeBindingResult Result;
        NYql::TIssues Issues;
        TDebugInfoPtr DebugInfo;
    };

    struct TEvModifyBindingRequest : NActors::TEventLocal<TEvModifyBindingRequest, EvModifyBindingRequest> {
        explicit TEvModifyBindingRequest(const TString& scope,
                                         const YandexQuery::ModifyBindingRequest& request,
                                         const TString& user,
                                         const TString& token,
                                         TPermissions permissions)
            : Scope(scope)
            , Request(request)
            , User(user)
            , Token(token)
            , Permissions(permissions)
        {
        }

        TString Scope;
        YandexQuery::ModifyBindingRequest Request;
        TString User;
        TString Token;
        TPermissions Permissions;
    };

    struct TEvModifyBindingResponse : NActors::TEventLocal<TEvModifyBindingResponse, EvModifyBindingResponse> {
        explicit TEvModifyBindingResponse(const YandexQuery::ModifyBindingResult& result,
                                          const TAuditDetails<YandexQuery::Binding>& auditDetails)
            : Result(result)
            , AuditDetails(auditDetails)
        {
        }

        explicit TEvModifyBindingResponse(const NYql::TIssues& issues)
            : Issues(issues)
        {
        }

        YandexQuery::ModifyBindingResult Result;
        TAuditDetails<YandexQuery::Binding> AuditDetails;
        NYql::TIssues Issues;
        TDebugInfoPtr DebugInfo;
    };

    struct TEvDeleteBindingRequest : NActors::TEventLocal<TEvDeleteBindingRequest, EvDeleteBindingRequest> {
        explicit TEvDeleteBindingRequest(const TString& scope,
                                         const YandexQuery::DeleteBindingRequest& request,
                                         const TString& user,
                                         const TString& token,
                                         TPermissions permissions)
            : Scope(scope)
            , Request(request)
            , User(user)
            , Token(token)
            , Permissions(permissions)
        {
        }

        TString Scope;
        YandexQuery::DeleteBindingRequest Request;
        TString User;
        TString Token;
        TPermissions Permissions;
    };

    struct TEvDeleteBindingResponse : NActors::TEventLocal<TEvDeleteBindingResponse, EvDeleteBindingResponse> {
        explicit TEvDeleteBindingResponse(const YandexQuery::DeleteBindingResult& result,
                                          const TAuditDetails<YandexQuery::Binding>& auditDetails)
            : Result(result)
            , AuditDetails(auditDetails)
        {
        }

        explicit TEvDeleteBindingResponse(const NYql::TIssues& issues)
            : Issues(issues)
        {
        }

        YandexQuery::DeleteBindingResult Result;
        TAuditDetails<YandexQuery::Binding> AuditDetails;
        NYql::TIssues Issues;
        TDebugInfoPtr DebugInfo;
    };

    // internal messages
    struct TEvWriteResultDataRequest : NActors::TEventLocal<TEvWriteResultDataRequest, EvWriteResultDataRequest> {
        explicit TEvWriteResultDataRequest(const TString& resultId,
                                           const int32_t resultSetId,
                                           const int64_t startRowId,
                                           const TInstant& deadline,
                                           const Ydb::ResultSet& resultSet)
            : ResultId(resultId)
            , ResultSetId(resultSetId)
            , StartRowId(startRowId)
            , Deadline(deadline)
            , ResultSet(resultSet)
        {
        }

        TString ResultId;
        int32_t ResultSetId = 0;
        int64_t StartRowId = 0;
        TInstant Deadline;
        Ydb::ResultSet ResultSet;
    };

    struct TEvWriteResultDataResponse : NActors::TEventLocal<TEvWriteResultDataResponse, EvWriteResultDataResponse> {
        explicit TEvWriteResultDataResponse(const NYql::TIssues& issues)
            : Issues(issues)
        {
        }

        explicit TEvWriteResultDataResponse(
            const NYql::TIssues& issues,
            const ui64 requestId)
            : Issues(issues)
            , RequestId(requestId)
        {
        }

        NYql::TIssues Issues;
        const ui64 RequestId = 0;
        TDebugInfoPtr DebugInfo;
    };

    struct TEvGetTaskRequest : NActors::TEventLocal<TEvGetTaskRequest, EvGetTaskRequest> {
        explicit TEvGetTaskRequest(
            const TString& owner,
            const TString& hostName)
            : Owner(owner)
            , HostName(hostName)
        {
        }

        TString Owner;
        TString HostName;
    };

    struct TTask {
        TString Scope;
        TString QueryId;
        YandexQuery::Query Query;
        YandexQuery::Internal::QueryInternal Internal;
        ui64 Generation = 0;
        TInstant Deadline;
    };

    struct TEvGetTaskResponse : NActors::TEventLocal<TEvGetTaskResponse, EvGetTaskResponse> {
        explicit TEvGetTaskResponse(const TVector<TTask>& tasks, const TString& owner)
            : Tasks(tasks)
            , Owner(owner)
        {
        }

        explicit TEvGetTaskResponse(const NYql::TIssues& issues)
            : Issues(issues)
        {
        }

        NYql::TIssues Issues;
        TVector<TTask> Tasks;
        TString Owner;
        TDebugInfoPtr DebugInfo;
    };

    // Description of consumer that was created by YQ
    struct TTopicConsumer {
        TString DatabaseId;
        TString Database;
        TString TopicPath;
        TString ConsumerName;
        TString ClusterEndpoint;
        bool UseSsl = false;
        TString TokenName;
        bool AddBearerToToken = false;
    };

    struct TEvPingTaskRequest : NActors::TEventLocal<TEvPingTaskRequest, EvPingTaskRequest> {
        explicit TEvPingTaskRequest(const TString& scope, const TString& queryId, const TString& owner, const TInstant& deadline, const TString& resultId = "")
            : Scope(scope)
            , QueryId(queryId)
            , Owner(owner)
            , Deadline(deadline)
            , ResultId(resultId)
        {
        }

        const TString Scope;
        const TString QueryId;
        const TString Owner;
        const TInstant Deadline;
        TString ResultId;
        TMaybe<YandexQuery::QueryMeta::ComputeStatus> Status;
        TMaybe<NYql::TIssues> Issues;
        TMaybe<NYql::TIssues> TransientIssues;
        TMaybe<TString> Statistics;
        TMaybe<TVector<YandexQuery::ResultSetMeta>> ResultSetMetas; 
        TMaybe<TString> Ast;
        TMaybe<TString> Plan;
        TMaybe<TInstant> StartedAt;
        TMaybe<TInstant> FinishedAt;
        bool ResignQuery = false; 
        TVector<TTopicConsumer> CreatedTopicConsumers;
        TVector<TString> DqGraphs; 
        i32 DqGraphIndex = 0; 
        YandexQuery::StateLoadMode StateLoadMode = YandexQuery::STATE_LOAD_MODE_UNSPECIFIED;
        TMaybe<YandexQuery::StreamingDisposition> StreamingDisposition;
    };

    struct TEvPingTaskResponse : NActors::TEventLocal<TEvPingTaskResponse, EvPingTaskResponse> {
        explicit TEvPingTaskResponse(const YandexQuery::QueryAction& action)
            : Action(action)
        {
        }

        explicit TEvPingTaskResponse(const NYql::TIssues& issues)
            : Issues(issues)
        {
        }

        YandexQuery::QueryAction Action = YandexQuery::QUERY_ACTION_UNSPECIFIED;
        NYql::TIssues Issues;
        TDebugInfoPtr DebugInfo;
    };

    struct TEvNodesHealthCheckRequest : NActors::TEventLocal<TEvNodesHealthCheckRequest, EvNodesHealthCheckRequest> {
        explicit TEvNodesHealthCheckRequest(
            Yq::Private::NodesHealthCheckRequest&& request)
            : Request(std::move(request))
        {}
        Yq::Private::NodesHealthCheckRequest Request;
    };

    struct TEvNodesHealthCheckResponse : NActors::TEventLocal<TEvNodesHealthCheckResponse, EvNodesHealthCheckResponse> {

        explicit TEvNodesHealthCheckResponse(
            const Yq::Private::NodesHealthCheckResult& record)
            : Record(record)
        {}

        explicit TEvNodesHealthCheckResponse(
            const NYql::TIssues& issues)
            : Issues(issues)
        {}

        Yq::Private::NodesHealthCheckResult Record;
        NYql::TIssues Issues;
        TDebugInfoPtr DebugInfo;
    };

};

}

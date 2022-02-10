#pragma once

#include <ydb/core/yq/libs/control_plane_storage/events/events.h>

#include <ydb/public/api/protos/yq.pb.h>

#include <library/cpp/actors/core/event_pb.h>
#include <library/cpp/actors/core/events.h>
#include <library/cpp/actors/interconnect/events_local.h>

#include <ydb/library/yql/public/issue/yql_issue.h>

namespace NYq {

struct TEvControlPlaneProxy {
    // Event ids.
    enum EEv : ui32 {
        EvCreateQueryRequest = YqEventSubspaceBegin(NYq::TYqEventSubspace::ControlPlaneProxy), 
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
        EvTestConnectionRequest,
        EvTestConnectionResponse,
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
        EvEnd,
    };

    static_assert(EvEnd <= YqEventSubspaceEnd(NYq::TYqEventSubspace::ControlPlaneProxy), "All events must be in their subspace"); 

    struct TEvCreateQueryRequest : NActors::TEventLocal<TEvCreateQueryRequest, EvCreateQueryRequest> {
        explicit TEvCreateQueryRequest(const TString& folderId,
                                       const YandexQuery::CreateQueryRequest& request,
                                       const TString& user,
                                       const TString& token,
                                       const TVector<TString>& permissions)
            : FolderId(folderId)
            , Request(request)
            , User(user)
            , Token(token)
            , Permissions(permissions)
        {
        }

        TString FolderId;
        YandexQuery::CreateQueryRequest Request;
        TString User;
        TString Token;
        TVector<TString> Permissions;
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
    };

    struct TEvListQueriesRequest : NActors::TEventLocal<TEvListQueriesRequest, EvListQueriesRequest> {
        explicit TEvListQueriesRequest(const TString& folderId,
                                       const YandexQuery::ListQueriesRequest& request,
                                       const TString& user,
                                       const TString& token,
                                       const TVector<TString>& permissions)
            : FolderId(folderId)
            , Request(request)
            , User(user)
            , Token(token)
            , Permissions(permissions)
        {
        }

        TString FolderId;
        YandexQuery::ListQueriesRequest Request;
        TString User;
        TString Token;
        TVector<TString> Permissions;
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
    };

    struct TEvDescribeQueryRequest : NActors::TEventLocal<TEvDescribeQueryRequest, EvDescribeQueryRequest> {
        explicit TEvDescribeQueryRequest(const TString& folderId,
                                         const YandexQuery::DescribeQueryRequest& request,
                                         const TString& user,
                                         const TString& token,
                                         const TVector<TString>& permissions)
            : FolderId(folderId)
            , Request(request)
            , User(user)
            , Token(token)
            , Permissions(permissions)
        {
        }

        TString FolderId;
        YandexQuery::DescribeQueryRequest Request;
        TString User;
        TString Token;
        TVector<TString> Permissions;
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
    };

    struct TEvGetQueryStatusRequest : NActors::TEventLocal<TEvGetQueryStatusRequest, EvGetQueryStatusRequest> {
        explicit TEvGetQueryStatusRequest(const TString& folderId,
                                         const YandexQuery::GetQueryStatusRequest& request,
                                         const TString& user,
                                         const TString& token,
                                         const TVector<TString>& permissions)
            : FolderId(folderId)
            , Request(request)
            , User(user)
            , Token(token)
            , Permissions(permissions)
        {
        }

        TString FolderId;
        YandexQuery::GetQueryStatusRequest Request;
        TString User;
        TString Token;
        TVector<TString> Permissions;
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
    };

    struct TEvModifyQueryRequest : NActors::TEventLocal<TEvModifyQueryRequest, EvModifyQueryRequest> {
        explicit TEvModifyQueryRequest(const TString& folderId,
                                       const YandexQuery::ModifyQueryRequest& request,
                                       const TString& user,
                                       const TString& token,
                                       const TVector<TString>& permissions)
            : FolderId(folderId)
            , Request(request)
            , User(user)
            , Token(token)
            , Permissions(permissions)
        {
        }

        TString FolderId;
        YandexQuery::ModifyQueryRequest Request;
        TString User;
        TString Token;
        TVector<TString> Permissions;
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
    };

    struct TEvDeleteQueryRequest : NActors::TEventLocal<TEvDeleteQueryRequest, EvDeleteQueryRequest> {
        explicit TEvDeleteQueryRequest(const TString& folderId,
                                       const YandexQuery::DeleteQueryRequest& request,
                                       const TString& user,
                                       const TString& token,
                                       const TVector<TString>& permissions)
            : FolderId(folderId)
            , Request(request)
            , User(user)
            , Token(token)
            , Permissions(permissions)
        {
        }

        TString FolderId;
        YandexQuery::DeleteQueryRequest Request;
        TString User;
        TString Token;
        TVector<TString> Permissions;
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
    };

    struct TEvControlQueryRequest : NActors::TEventLocal<TEvControlQueryRequest, EvControlQueryRequest> {
        explicit TEvControlQueryRequest(const TString& folderId,
                                        const YandexQuery::ControlQueryRequest& request,
                                        const TString& user,
                                        const TString& token,
                                        const TVector<TString>& permissions)
            : FolderId(folderId)
            , Request(request)
            , User(user)
            , Token(token)
            , Permissions(permissions)
        {
        }

        TString FolderId;
        YandexQuery::ControlQueryRequest Request;
        TString User;
        TString Token;
        TVector<TString> Permissions;
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
    };

    struct TEvGetResultDataRequest : NActors::TEventLocal<TEvGetResultDataRequest, EvGetResultDataRequest> {
        explicit TEvGetResultDataRequest(const TString& folderId,
                                         const YandexQuery::GetResultDataRequest& request,
                                         const TString& user,
                                         const TString& token,
                                         const TVector<TString>& permissions)
            : FolderId(folderId)
            , Request(request)
            , User(user)
            , Token(token)
            , Permissions(permissions)
        {
        }

        TString FolderId;
        YandexQuery::GetResultDataRequest Request;
        TString User;
        TString Token;
        TVector<TString> Permissions;
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
    };

    struct TEvListJobsRequest : NActors::TEventLocal<TEvListJobsRequest, EvListJobsRequest> {
        explicit TEvListJobsRequest(const TString& folderId,
                                    const YandexQuery::ListJobsRequest& request,
                                    const TString& user,
                                    const TString& token,
                                    const TVector<TString>& permissions)
            : FolderId(folderId)
            , Request(request)
            , User(user)
            , Token(token)
            , Permissions(permissions)
        {
        }

        TString FolderId;
        YandexQuery::ListJobsRequest Request;
        TString User;
        TString Token;
        TVector<TString> Permissions;
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
    };

    struct TEvDescribeJobRequest : NActors::TEventLocal<TEvDescribeJobRequest, EvDescribeJobRequest> {
        explicit TEvDescribeJobRequest(const TString& folderId,
                                           const YandexQuery::DescribeJobRequest& request,
                                           const TString& user,
                                           const TString& token,
                                           const TVector<TString>& permissions)
            : FolderId(folderId)
            , Request(request)
            , User(user)
            , Token(token)
            , Permissions(permissions)
        {
        }

        TString FolderId;
        YandexQuery::DescribeJobRequest Request;
        TString User;
        TString Token;
        TVector<TString> Permissions;
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
    };

    struct TEvCreateConnectionRequest : NActors::TEventLocal<TEvCreateConnectionRequest, EvCreateConnectionRequest> {
        explicit TEvCreateConnectionRequest(const TString& folderId,
                                            const YandexQuery::CreateConnectionRequest& request,
                                            const TString& user,
                                            const TString& token,
                                            const TVector<TString>& permissions)
            : FolderId(folderId)
            , Request(request)
            , User(user)
            , Token(token)
            , Permissions(permissions)
        {
        }

        TString FolderId;
        YandexQuery::CreateConnectionRequest Request;
        TString User;
        TString Token;
        TVector<TString> Permissions;
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
    };

    struct TEvListConnectionsRequest : NActors::TEventLocal<TEvListConnectionsRequest, EvListConnectionsRequest> {
        explicit TEvListConnectionsRequest(const TString& folderId,
                                           const YandexQuery::ListConnectionsRequest& request,
                                           const TString& user,
                                           const TString& token,
                                           const TVector<TString>& permissions)
            : FolderId(folderId)
            , Request(request)
            , User(user)
            , Token(token)
            , Permissions(permissions)
        {
        }

        TString FolderId;
        YandexQuery::ListConnectionsRequest Request;
        TString User;
        TString Token;
        TVector<TString> Permissions;
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
    };

    struct TEvDescribeConnectionRequest : NActors::TEventLocal<TEvDescribeConnectionRequest, EvDescribeConnectionRequest> {
        explicit TEvDescribeConnectionRequest(const TString& folderId,
                                              const YandexQuery::DescribeConnectionRequest& request,
                                              const TString& user,
                                              const TString& token,
                                              const TVector<TString>& permissions)
            : FolderId(folderId)
            , Request(request)
            , User(user)
            , Token(token)
            , Permissions(permissions)
        {
        }

        TString FolderId;
        YandexQuery::DescribeConnectionRequest Request;
        TString User;
        TString Token;
        TVector<TString> Permissions;
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
    };

    struct TEvModifyConnectionRequest : NActors::TEventLocal<TEvModifyConnectionRequest, EvModifyConnectionRequest> {
        explicit TEvModifyConnectionRequest(const TString& folderId,
                                            const YandexQuery::ModifyConnectionRequest& request,
                                            const TString& user,
                                            const TString& token,
                                            const TVector<TString>& permissions)
            : FolderId(folderId)
            , Request(request)
            , User(user)
            , Token(token)
            , Permissions(permissions)
        {
        }

        TString FolderId;
        YandexQuery::ModifyConnectionRequest Request;
        TString User;
        TString Token;
        TVector<TString> Permissions;
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
    };

    struct TEvDeleteConnectionRequest : NActors::TEventLocal<TEvDeleteConnectionRequest, EvDeleteConnectionRequest> {
        explicit TEvDeleteConnectionRequest(const TString& folderId,
                                            const YandexQuery::DeleteConnectionRequest& request,
                                            const TString& user,
                                            const TString& token,
                                            const TVector<TString>& permissions)
            : FolderId(folderId)
            , Request(request)
            , User(user)
            , Token(token)
            , Permissions(permissions)
        {
        }

        TString FolderId;
        YandexQuery::DeleteConnectionRequest Request;
        TString User;
        TString Token;
        TVector<TString> Permissions;
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
    };

    struct TEvTestConnectionRequest : NActors::TEventLocal<TEvTestConnectionRequest, EvTestConnectionRequest> {
        explicit TEvTestConnectionRequest(const TString& folderId,
                                          const YandexQuery::TestConnectionRequest& request,
                                          const TString& user,
                                          const TString& token,
                                          const TVector<TString>& permissions)
            : FolderId(folderId)
            , Request(request)
            , User(user)
            , Token(token)
            , Permissions(permissions)
        {
        }

        TString FolderId;
        YandexQuery::TestConnectionRequest Request;
        TString User;
        TString Token;
        TVector<TString> Permissions;
    };

    struct TEvTestConnectionResponse : NActors::TEventLocal<TEvTestConnectionResponse, EvTestConnectionResponse> {
        explicit TEvTestConnectionResponse(const YandexQuery::TestConnectionResult& result)
            : Result(result)
        {
        }

        explicit TEvTestConnectionResponse(const NYql::TIssues& issues)
            : Issues(issues)
        {
        }

        YandexQuery::TestConnectionResult Result;
        NYql::TIssues Issues;
    };

    struct TEvCreateBindingRequest : NActors::TEventLocal<TEvCreateBindingRequest, EvCreateBindingRequest> {
        explicit TEvCreateBindingRequest(const TString& folderId,
                                         const YandexQuery::CreateBindingRequest& request,
                                         const TString& user,
                                         const TString& token,
                                         const TVector<TString>& permissions)
            : FolderId(folderId)
            , Request(request)
            , User(user)
            , Token(token)
            , Permissions(permissions)
        {
        }

        TString FolderId;
        YandexQuery::CreateBindingRequest Request;
        TString User;
        TString Token;
        TVector<TString> Permissions;
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
    };

    struct TEvListBindingsRequest : NActors::TEventLocal<TEvListBindingsRequest, EvListBindingsRequest> {
        explicit TEvListBindingsRequest(const TString& folderId,
                                        const YandexQuery::ListBindingsRequest& request,
                                        const TString& user,
                                        const TString& token,
                                        const TVector<TString>& permissions)
            : FolderId(folderId)
            , Request(request)
            , User(user)
            , Token(token)
            , Permissions(permissions)
        {
        }

        TString FolderId;
        YandexQuery::ListBindingsRequest Request;
        TString User;
        TString Token;
        TVector<TString> Permissions;
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
    };

    struct TEvDescribeBindingRequest : NActors::TEventLocal<TEvDescribeBindingRequest, EvDescribeBindingRequest> {
        explicit TEvDescribeBindingRequest(const TString& folderId,
                                           const YandexQuery::DescribeBindingRequest& request,
                                           const TString& user,
                                           const TString& token,
                                           const TVector<TString>& permissions)
            : FolderId(folderId)
            , Request(request)
            , User(user)
            , Token(token)
            , Permissions(permissions)
        {
        }

        TString FolderId;
        YandexQuery::DescribeBindingRequest Request;
        TString User;
        TString Token;
        TVector<TString> Permissions;
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
    };

    struct TEvModifyBindingRequest : NActors::TEventLocal<TEvModifyBindingRequest, EvModifyBindingRequest> {
        explicit TEvModifyBindingRequest(const TString& folderId,
                                         const YandexQuery::ModifyBindingRequest& request,
                                         const TString& user,
                                         const TString& token,
                                         const TVector<TString>& permissions)
            : FolderId(folderId)
            , Request(request)
            , User(user)
            , Token(token)
            , Permissions(permissions)
        {
        }

        TString FolderId;
        YandexQuery::ModifyBindingRequest Request;
        TString User;
        TString Token;
        TVector<TString> Permissions;
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
    };

    struct TEvDeleteBindingRequest : NActors::TEventLocal<TEvDeleteBindingRequest, EvDeleteBindingRequest> {
        explicit TEvDeleteBindingRequest(const TString& folderId,
                                         const YandexQuery::DeleteBindingRequest& request,
                                         const TString& user,
                                         const TString& token,
                                         const TVector<TString>& permissions)
            : FolderId(folderId)
            , Request(request)
            , User(user)
            , Token(token)
            , Permissions(permissions)
        {
        }

        TString FolderId;
        YandexQuery::DeleteBindingRequest Request;
        TString User;
        TString Token;
        TVector<TString> Permissions;
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
    };
};

}

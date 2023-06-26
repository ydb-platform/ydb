#pragma once

#include <ydb/core/fq/libs/control_plane_storage/events/events.h>
#include <ydb/core/fq/libs/quota_manager/events/events.h>

#include <ydb/public/api/protos/draft/fq.pb.h>

#include <library/cpp/actors/core/event_pb.h>
#include <library/cpp/actors/core/events.h>
#include <library/cpp/actors/interconnect/events_local.h>

#include <ydb/library/yql/public/issue/yql_issue.h>

namespace NFq {

struct TEvControlPlaneProxy {
    // Event ids.
    enum EEv : ui32 {
        EvCreateQueryRequest = YqEventSubspaceBegin(NFq::TYqEventSubspace::ControlPlaneProxy),
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

    static_assert(EvEnd <= YqEventSubspaceEnd(NFq::TYqEventSubspace::ControlPlaneProxy), "All events must be in their subspace");

    template<typename ProtoMessage, ui32 EventType>
    struct TControlPlaneRequest : NActors::TEventLocal<TControlPlaneRequest<ProtoMessage, EventType>, EventType> {
        TControlPlaneRequest(const TString& folderId,
                             const ProtoMessage& request,
                             const TString& user,
                             const TString& token,
                             const TVector<TString>& permissions,
                             TMaybe<TQuotaMap> quotas = Nothing(),
                             TTenantInfo::TPtr tenantInfo = nullptr)
            : FolderId(folderId)
            , Request(request)
            , User(user)
            , Token(token)
            , Permissions(permissions)
            , Quotas(std::move(quotas))
            , TenantInfo(tenantInfo)
            , ComputeYDBOperationWasPerformed(false)
        {
        }

        TString FolderId;
        TString CloudId;
        ProtoMessage Request;
        TString User;
        TString Token;
        TVector<TString> Permissions;
        TMaybe<TQuotaMap> Quotas;
        TTenantInfo::TPtr TenantInfo;
        TString SubjectType;
        bool ComputeYDBOperationWasPerformed;
    };

    template<typename TDerived, typename ProtoMessage, ui32 EventType>
    struct TControlPlaneResponse : NActors::TEventLocal<TDerived, EventType> {
        TControlPlaneResponse(const ProtoMessage& result, const TString& subjectType)
            : Result(result)
            , SubjectType(subjectType)
        {
        }

        TControlPlaneResponse(const NYql::TIssues& issues, const TString& subjectType)
            : Issues(issues)
            , SubjectType(subjectType)
        {
        }

        ProtoMessage Result;
        NYql::TIssues Issues;
        TString SubjectType;
    };

    template<typename ProtoMessage, ui32 EventType>
    struct TControlPlaneNonAuditableResponse : TControlPlaneResponse<TControlPlaneNonAuditableResponse<ProtoMessage, EventType>, ProtoMessage, EventType> {
        TControlPlaneNonAuditableResponse(const ProtoMessage& result, const TString& subjectType)
            : TControlPlaneResponse<TControlPlaneNonAuditableResponse<ProtoMessage, EventType>, ProtoMessage, EventType>(result, subjectType)
        {
        }

        TControlPlaneNonAuditableResponse(const NYql::TIssues& issues, const TString& subjectType)
            : TControlPlaneResponse<TControlPlaneNonAuditableResponse<ProtoMessage, EventType>, ProtoMessage, EventType>(issues, subjectType)
        {
        }
    };

    template<typename ProtoMessage, typename AuditMessage, ui32 EventType>
    struct TControlPlaneAuditableResponse : TControlPlaneResponse<TControlPlaneAuditableResponse<ProtoMessage, AuditMessage, EventType>, ProtoMessage, EventType> {
        TControlPlaneAuditableResponse(const ProtoMessage& result,
                                       const TAuditDetails<AuditMessage>& auditDetails,
                                       const TString& subjectType)
            : TControlPlaneResponse<TControlPlaneAuditableResponse<ProtoMessage, AuditMessage, EventType>, ProtoMessage, EventType>(result, subjectType)
            , AuditDetails(auditDetails)
        {
        }

        TControlPlaneAuditableResponse(const NYql::TIssues& issues, const TString& subjectType)
            : TControlPlaneResponse<TControlPlaneAuditableResponse<ProtoMessage, AuditMessage, EventType>, ProtoMessage, EventType>(issues, subjectType)
        {
        }

        TAuditDetails<AuditMessage> AuditDetails;
    };

    using TEvCreateQueryRequest = TControlPlaneRequest<FederatedQuery::CreateQueryRequest, EvCreateQueryRequest>;
    using TEvCreateQueryResponse = TControlPlaneAuditableResponse<FederatedQuery::CreateQueryResult, FederatedQuery::Query, EvCreateQueryResponse>;
    using TEvListQueriesRequest = TControlPlaneRequest<FederatedQuery::ListQueriesRequest, EvListQueriesRequest>;
    using TEvListQueriesResponse = TControlPlaneNonAuditableResponse<FederatedQuery::ListQueriesResult, EvListQueriesResponse>;
    using TEvDescribeQueryRequest = TControlPlaneRequest<FederatedQuery::DescribeQueryRequest, EvDescribeQueryRequest>;
    using TEvDescribeQueryResponse = TControlPlaneNonAuditableResponse<FederatedQuery::DescribeQueryResult, EvDescribeQueryResponse>;
    using TEvGetQueryStatusRequest = TControlPlaneRequest<FederatedQuery::GetQueryStatusRequest, EvGetQueryStatusRequest>;
    using TEvGetQueryStatusResponse = TControlPlaneNonAuditableResponse<FederatedQuery::GetQueryStatusResult, EvGetQueryStatusResponse>;
    using TEvModifyQueryRequest = TControlPlaneRequest<FederatedQuery::ModifyQueryRequest, EvModifyQueryRequest>;
    using TEvModifyQueryResponse = TControlPlaneAuditableResponse<FederatedQuery::ModifyQueryResult, FederatedQuery::Query, EvModifyQueryResponse>;
    using TEvDeleteQueryRequest = TControlPlaneRequest<FederatedQuery::DeleteQueryRequest, EvDeleteQueryRequest>;
    using TEvDeleteQueryResponse = TControlPlaneAuditableResponse<FederatedQuery::DeleteQueryResult, FederatedQuery::Query, EvDeleteQueryResponse>;
    using TEvControlQueryRequest = TControlPlaneRequest<FederatedQuery::ControlQueryRequest, EvControlQueryRequest>;
    using TEvControlQueryResponse = TControlPlaneAuditableResponse<FederatedQuery::ControlQueryResult, FederatedQuery::Query, EvControlQueryResponse>;
    using TEvGetResultDataRequest = TControlPlaneRequest<FederatedQuery::GetResultDataRequest, EvGetResultDataRequest>;
    using TEvGetResultDataResponse = TControlPlaneNonAuditableResponse<FederatedQuery::GetResultDataResult, EvGetResultDataResponse>;
    using TEvListJobsRequest = TControlPlaneRequest<FederatedQuery::ListJobsRequest, EvListJobsRequest>;
    using TEvListJobsResponse = TControlPlaneNonAuditableResponse<FederatedQuery::ListJobsResult, EvListJobsResponse>;
    using TEvDescribeJobRequest = TControlPlaneRequest<FederatedQuery::DescribeJobRequest, EvDescribeJobRequest>;
    using TEvDescribeJobResponse = TControlPlaneNonAuditableResponse<FederatedQuery::DescribeJobResult, EvDescribeJobResponse>;
    using TEvCreateConnectionRequest = TControlPlaneRequest<FederatedQuery::CreateConnectionRequest, EvCreateConnectionRequest>;
    using TEvCreateConnectionResponse = TControlPlaneAuditableResponse<FederatedQuery::CreateConnectionResult, FederatedQuery::Connection, EvCreateConnectionResponse>;
    using TEvListConnectionsRequest = TControlPlaneRequest<FederatedQuery::ListConnectionsRequest, EvListConnectionsRequest>;
    using TEvListConnectionsResponse = TControlPlaneNonAuditableResponse<FederatedQuery::ListConnectionsResult, EvListConnectionsResponse>;
    using TEvDescribeConnectionRequest = TControlPlaneRequest<FederatedQuery::DescribeConnectionRequest, EvDescribeConnectionRequest>;
    using TEvDescribeConnectionResponse = TControlPlaneNonAuditableResponse<FederatedQuery::DescribeConnectionResult, EvDescribeConnectionResponse>;
    using TEvModifyConnectionRequest = TControlPlaneRequest<FederatedQuery::ModifyConnectionRequest, EvModifyConnectionRequest>;
    using TEvModifyConnectionResponse = TControlPlaneAuditableResponse<FederatedQuery::ModifyConnectionResult, FederatedQuery::Connection, EvModifyConnectionResponse>;
    using TEvDeleteConnectionRequest = TControlPlaneRequest<FederatedQuery::DeleteConnectionRequest, EvDeleteConnectionRequest>;
    using TEvDeleteConnectionResponse = TControlPlaneAuditableResponse<FederatedQuery::DeleteConnectionResult, FederatedQuery::Connection, EvDeleteConnectionResponse>;
    using TEvTestConnectionRequest = TControlPlaneRequest<FederatedQuery::TestConnectionRequest, EvTestConnectionRequest>;
    using TEvTestConnectionResponse = TControlPlaneNonAuditableResponse<FederatedQuery::TestConnectionResult, EvTestConnectionResponse>;
    using TEvCreateBindingRequest = TControlPlaneRequest<FederatedQuery::CreateBindingRequest, EvCreateBindingRequest>;
    using TEvCreateBindingResponse = TControlPlaneAuditableResponse<FederatedQuery::CreateBindingResult, FederatedQuery::Binding, EvCreateBindingResponse>;
    using TEvListBindingsRequest = TControlPlaneRequest<FederatedQuery::ListBindingsRequest, EvListBindingsRequest>;
    using TEvListBindingsResponse = TControlPlaneNonAuditableResponse<FederatedQuery::ListBindingsResult, EvListBindingsResponse>;
    using TEvDescribeBindingRequest = TControlPlaneRequest<FederatedQuery::DescribeBindingRequest, EvDescribeBindingRequest>;
    using TEvDescribeBindingResponse = TControlPlaneNonAuditableResponse<FederatedQuery::DescribeBindingResult, EvDescribeBindingResponse>;
    using TEvModifyBindingRequest = TControlPlaneRequest<FederatedQuery::ModifyBindingRequest, EvModifyBindingRequest>;
    using TEvModifyBindingResponse = TControlPlaneAuditableResponse<FederatedQuery::ModifyBindingResult, FederatedQuery::Binding, EvModifyBindingResponse>;
    using TEvDeleteBindingRequest = TControlPlaneRequest<FederatedQuery::DeleteBindingRequest, EvDeleteBindingRequest>;
    using TEvDeleteBindingResponse = TControlPlaneAuditableResponse<FederatedQuery::DeleteBindingResult, FederatedQuery::Binding, EvDeleteBindingResponse>;
};

}

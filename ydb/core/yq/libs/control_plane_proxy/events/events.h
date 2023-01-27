#pragma once

#include <ydb/core/yq/libs/control_plane_storage/events/events.h>
#include <ydb/core/yq/libs/quota_manager/events/events.h>

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

    template<typename ProtoMessage, ui32 EventType>
    struct TControlPlaneRequest : NActors::TEventLocal<TControlPlaneRequest<ProtoMessage, EventType>, EventType> {
        explicit TControlPlaneRequest(const TString& folderId,
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
    };

    template<typename TDerived, typename ProtoMessage, ui32 EventType>
    struct TControlPlaneResponse : NActors::TEventLocal<TDerived, EventType> {
        explicit TControlPlaneResponse(const ProtoMessage& result)
            : Result(result)
        {
        }

        explicit TControlPlaneResponse(const NYql::TIssues& issues)
            : Issues(issues)
        {
        }

        ProtoMessage Result;
        NYql::TIssues Issues;
    };

    template<typename ProtoMessage, ui32 EventType>
    struct TControlPlaneNonAuditableResponse : TControlPlaneResponse<TControlPlaneNonAuditableResponse<ProtoMessage, EventType>, ProtoMessage, EventType> {
        explicit TControlPlaneNonAuditableResponse(const ProtoMessage& result)
            : TControlPlaneResponse<TControlPlaneNonAuditableResponse<ProtoMessage, EventType>, ProtoMessage, EventType>(result)
        {
        }

        explicit TControlPlaneNonAuditableResponse(const NYql::TIssues& issues)
            : TControlPlaneResponse<TControlPlaneNonAuditableResponse<ProtoMessage, EventType>, ProtoMessage, EventType>(issues)
        {
        }
    };

    template<typename ProtoMessage, typename AuditMessage, ui32 EventType>
    struct TControlPlaneAuditableResponse : TControlPlaneResponse<TControlPlaneAuditableResponse<ProtoMessage, AuditMessage, EventType>, ProtoMessage, EventType> {
        explicit TControlPlaneAuditableResponse(const ProtoMessage& result,
                                        const TAuditDetails<AuditMessage>& auditDetails)
            : TControlPlaneResponse<TControlPlaneAuditableResponse<ProtoMessage, AuditMessage, EventType>, ProtoMessage, EventType>(result)
            , AuditDetails(auditDetails)
        {
        }

        explicit TControlPlaneAuditableResponse(const NYql::TIssues& issues)
            : TControlPlaneResponse<TControlPlaneAuditableResponse<ProtoMessage, AuditMessage, EventType>, ProtoMessage, EventType>(issues)
        {
        }

        TAuditDetails<AuditMessage> AuditDetails;
    };

    using TEvCreateQueryRequest = TControlPlaneRequest<YandexQuery::CreateQueryRequest, EvCreateQueryRequest>;
    using TEvCreateQueryResponse = TControlPlaneAuditableResponse<YandexQuery::CreateQueryResult, YandexQuery::Query, EvCreateQueryResponse>;
    using TEvListQueriesRequest = TControlPlaneRequest<YandexQuery::ListQueriesRequest, EvListQueriesRequest>;
    using TEvListQueriesResponse = TControlPlaneNonAuditableResponse<YandexQuery::ListQueriesResult, EvListQueriesResponse>;
    using TEvDescribeQueryRequest = TControlPlaneRequest<YandexQuery::DescribeQueryRequest, EvDescribeQueryRequest>;
    using TEvDescribeQueryResponse = TControlPlaneNonAuditableResponse<YandexQuery::DescribeQueryResult, EvDescribeQueryResponse>;
    using TEvGetQueryStatusRequest = TControlPlaneRequest<YandexQuery::GetQueryStatusRequest, EvGetQueryStatusRequest>;
    using TEvGetQueryStatusResponse = TControlPlaneNonAuditableResponse<YandexQuery::GetQueryStatusResult, EvGetQueryStatusResponse>;
    using TEvModifyQueryRequest = TControlPlaneRequest<YandexQuery::ModifyQueryRequest, EvModifyQueryRequest>;
    using TEvModifyQueryResponse = TControlPlaneAuditableResponse<YandexQuery::ModifyQueryResult, YandexQuery::Query, EvModifyQueryResponse>;
    using TEvDeleteQueryRequest = TControlPlaneRequest<YandexQuery::DeleteQueryRequest, EvDeleteQueryRequest>;
    using TEvDeleteQueryResponse = TControlPlaneAuditableResponse<YandexQuery::DeleteQueryResult, YandexQuery::Query, EvDeleteQueryResponse>;
    using TEvControlQueryRequest = TControlPlaneRequest<YandexQuery::ControlQueryRequest, EvControlQueryRequest>;
    using TEvControlQueryResponse = TControlPlaneAuditableResponse<YandexQuery::ControlQueryResult, YandexQuery::Query, EvControlQueryResponse>;
    using TEvGetResultDataRequest = TControlPlaneRequest<YandexQuery::GetResultDataRequest, EvGetResultDataRequest>;
    using TEvGetResultDataResponse = TControlPlaneNonAuditableResponse<YandexQuery::GetResultDataResult, EvGetResultDataResponse>;
    using TEvListJobsRequest = TControlPlaneRequest<YandexQuery::ListJobsRequest, EvListJobsRequest>;
    using TEvListJobsResponse = TControlPlaneNonAuditableResponse<YandexQuery::ListJobsResult, EvListJobsResponse>;
    using TEvDescribeJobRequest = TControlPlaneRequest<YandexQuery::DescribeJobRequest, EvDescribeJobRequest>;
    using TEvDescribeJobResponse = TControlPlaneNonAuditableResponse<YandexQuery::DescribeJobResult, EvDescribeJobResponse>;
    using TEvCreateConnectionRequest = TControlPlaneRequest<YandexQuery::CreateConnectionRequest, EvCreateConnectionRequest>;
    using TEvCreateConnectionResponse = TControlPlaneAuditableResponse<YandexQuery::CreateConnectionResult, YandexQuery::Connection, EvCreateConnectionResponse>;
    using TEvListConnectionsRequest = TControlPlaneRequest<YandexQuery::ListConnectionsRequest, EvListConnectionsRequest>;
    using TEvListConnectionsResponse = TControlPlaneNonAuditableResponse<YandexQuery::ListConnectionsResult, EvListConnectionsResponse>;
    using TEvDescribeConnectionRequest = TControlPlaneRequest<YandexQuery::DescribeConnectionRequest, EvDescribeConnectionRequest>;
    using TEvDescribeConnectionResponse = TControlPlaneNonAuditableResponse<YandexQuery::DescribeConnectionResult, EvDescribeConnectionResponse>;
    using TEvModifyConnectionRequest = TControlPlaneRequest<YandexQuery::ModifyConnectionRequest, EvModifyConnectionRequest>;
    using TEvModifyConnectionResponse = TControlPlaneAuditableResponse<YandexQuery::ModifyConnectionResult, YandexQuery::Connection, EvModifyConnectionResponse>;
    using TEvDeleteConnectionRequest = TControlPlaneRequest<YandexQuery::DeleteConnectionRequest, EvDeleteConnectionRequest>;
    using TEvDeleteConnectionResponse = TControlPlaneAuditableResponse<YandexQuery::DeleteConnectionResult, YandexQuery::Connection, EvDeleteConnectionResponse>;
    using TEvTestConnectionRequest = TControlPlaneRequest<YandexQuery::TestConnectionRequest, EvTestConnectionRequest>;
    using TEvTestConnectionResponse = TControlPlaneNonAuditableResponse<YandexQuery::TestConnectionResult, EvTestConnectionResponse>;
    using TEvCreateBindingRequest = TControlPlaneRequest<YandexQuery::CreateBindingRequest, EvCreateBindingRequest>;
    using TEvCreateBindingResponse = TControlPlaneAuditableResponse<YandexQuery::CreateBindingResult, YandexQuery::Binding, EvCreateBindingResponse>;
    using TEvListBindingsRequest = TControlPlaneRequest<YandexQuery::ListBindingsRequest, EvListBindingsRequest>;
    using TEvListBindingsResponse = TControlPlaneNonAuditableResponse<YandexQuery::ListBindingsResult, EvListBindingsResponse>;
    using TEvDescribeBindingRequest = TControlPlaneRequest<YandexQuery::DescribeBindingRequest, EvDescribeBindingRequest>;
    using TEvDescribeBindingResponse = TControlPlaneNonAuditableResponse<YandexQuery::DescribeBindingResult, EvDescribeBindingResponse>;
    using TEvModifyBindingRequest = TControlPlaneRequest<YandexQuery::ModifyBindingRequest, EvModifyBindingRequest>;
    using TEvModifyBindingResponse = TControlPlaneAuditableResponse<YandexQuery::ModifyBindingResult, YandexQuery::Binding, EvModifyBindingResponse>;
    using TEvDeleteBindingRequest = TControlPlaneRequest<YandexQuery::DeleteBindingRequest, EvDeleteBindingRequest>;
    using TEvDeleteBindingResponse = TControlPlaneAuditableResponse<YandexQuery::DeleteBindingResult, YandexQuery::Binding, EvDeleteBindingResponse>;
};

}

#pragma once

#include "type_traits.h"

#include <util/generic/maybe.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/core/fq/libs/control_plane_storage/events/events.h>
#include <ydb/core/fq/libs/quota_manager/events/events.h>

#include <ydb/public/api/protos/draft/fq.pb.h>

#include <ydb/library/actors/core/event_pb.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/interconnect/events_local.h>

#include <ydb/library/yql/public/issue/yql_issue.h>
#include <memory>

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

    template<class Request>
    struct TResponseSelector;

    template<typename TDerived, typename ProtoMessage, ui32 EventType>
    struct TBaseControlPlaneRequest : NActors::TEventLocal<TDerived, EventType> {
        using TProxyResponse = typename TResponseSelector<TDerived>::type;

        TBaseControlPlaneRequest(const TString& scope,
                                 const ProtoMessage& request,
                                 const TString& user,
                                 const TString& token,
                                 const TVector<TString>& permissions,
                                 TMaybe<TQuotaMap> quotas     = Nothing(),
                                 TTenantInfo::TPtr tenantInfo = nullptr)
            : Scope(scope)
            , Request(request)
            , User(user)
            , Token(token)
            , Permissions(permissions)
            , Quotas(std::move(quotas))
            , TenantInfo(tenantInfo)
            , ComputeYDBOperationWasPerformed(false)
            , ControlPlaneYDBOperationWasPerformed(false) { }

        TString Scope;
        TString CloudId;
        ProtoMessage Request;
        TString User;
        TString Token;
        TVector<TString> Permissions;
        TMaybe<TQuotaMap> Quotas;
        TTenantInfo::TPtr TenantInfo;
        TString SubjectType;
        bool ComputeYDBOperationWasPerformed;
        bool ControlPlaneYDBOperationWasPerformed;
        std::unique_ptr<TProxyResponse> Response;
        std::shared_ptr<NYdb::NTable::TTableClient> YDBClient;
        TMaybe<FederatedQuery::Internal::ComputeDatabaseInternal> ComputeDatabase;
        bool RequestValidationPassed = false;
    };

    template<typename ProtoMessage, ui32 EventType>
    struct TControlPlaneRequest;

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

    template<>
    struct TResponseSelector<TEvCreateQueryRequest> {
        using type = TEvCreateQueryResponse;
    };
    template<>
    struct TResponseSelector<TEvListQueriesRequest> {
        using type = TEvListQueriesResponse;
    };
    template<>
    struct TResponseSelector<TEvDescribeQueryRequest> {
        using type = TEvDescribeQueryResponse;
    };
    template<>
    struct TResponseSelector<TEvGetQueryStatusRequest> {
        using type = TEvGetQueryStatusResponse;
    };
    template<>
    struct TResponseSelector<TEvModifyQueryRequest> {
        using type = TEvModifyQueryResponse;
    };
    template<>
    struct TResponseSelector<TEvDeleteQueryRequest> {
        using type = TEvDeleteQueryResponse;
    };
    template<>
    struct TResponseSelector<TEvControlQueryRequest> {
        using type = TEvControlQueryResponse;
    };
    template<>
    struct TResponseSelector<TEvGetResultDataRequest> {
        using type = TEvGetResultDataResponse;
    };
    template<>
    struct TResponseSelector<TEvListJobsRequest> {
        using type = TEvListJobsResponse;
    };
    template<>
    struct TResponseSelector<TEvDescribeJobRequest> {
        using type = TEvDescribeJobResponse;
    };
    template<>
    struct TResponseSelector<TEvCreateConnectionRequest> {
        using type = TEvCreateConnectionResponse;
    };
    template<>
    struct TResponseSelector<TEvListConnectionsRequest> {
        using type = TEvListConnectionsResponse;
    };
    template<>
    struct TResponseSelector<TEvDescribeConnectionRequest> {
        using type = TEvDescribeConnectionResponse;
    };
    template<>
    struct TResponseSelector<TEvModifyConnectionRequest> {
        using type = TEvModifyConnectionResponse;
    };
    template<>
    struct TResponseSelector<TEvDeleteConnectionRequest> {
        using type = TEvDeleteConnectionResponse;
    };
    template<>
    struct TResponseSelector<TEvTestConnectionRequest> {
        using type = TEvTestConnectionResponse;
    };
    template<>
    struct TResponseSelector<TEvCreateBindingRequest> {
        using type = TEvCreateBindingResponse;
    };
    template<>
    struct TResponseSelector<TEvListBindingsRequest> {
        using type = TEvListBindingsResponse;
    };
    template<>
    struct TResponseSelector<TEvDescribeBindingRequest> {
        using type = TEvDescribeBindingResponse;
    };
    template<>
    struct TResponseSelector<TEvModifyBindingRequest> {
        using type = TEvModifyBindingResponse;
    };
    template<>
    struct TResponseSelector<TEvDeleteBindingRequest> {
        using type = TEvDeleteBindingResponse;
    };

    template<typename ProtoMessage, ui32 EventType>
    struct TControlPlaneRequest :
        public TBaseControlPlaneRequest<TControlPlaneRequest<ProtoMessage, EventType>,
                                        ProtoMessage,
                                        EventType> {
        using TBaseControlPlaneRequest<TControlPlaneRequest<ProtoMessage, EventType>,
                                       ProtoMessage,
                                       EventType>::TBaseControlPlaneRequest;
    };

    enum class EEntityType : ui8 { Connection, Binding };

    template<>
    struct TControlPlaneRequest<FederatedQuery::CreateConnectionRequest,
                                EvCreateConnectionRequest> :
        public TBaseControlPlaneRequest<TEvCreateConnectionRequest,
                                        FederatedQuery::CreateConnectionRequest,
                                        EvCreateConnectionRequest> {
        using TBaseControlPlaneRequest<
            TControlPlaneRequest<FederatedQuery::CreateConnectionRequest, EvCreateConnectionRequest>,
            FederatedQuery::CreateConnectionRequest,
            EvCreateConnectionRequest>::TBaseControlPlaneRequest;

        TMaybe<EEntityType> EntityWithSameNameType;
        bool ConnectionsWithSameNameWereListed = false;
        bool BindingWithSameNameWereListed     = false;
    };

    template<>
    struct TControlPlaneRequest<FederatedQuery::ModifyConnectionRequest, EvModifyConnectionRequest> :
        public TBaseControlPlaneRequest<TEvModifyConnectionRequest,
                                        FederatedQuery::ModifyConnectionRequest,
                                        EvModifyConnectionRequest> {
        using TBaseControlPlaneRequest<
            TControlPlaneRequest<FederatedQuery::ModifyConnectionRequest, EvModifyConnectionRequest>,
            FederatedQuery::ModifyConnectionRequest,
            EvModifyConnectionRequest>::TBaseControlPlaneRequest;

        TMaybe<FederatedQuery::ConnectionContent> OldConnectionContent;
        // ListBindings
        bool OldBindingNamesDiscoveryFinished = false;
        TMaybe<TString> NextListingBindingsToken;
        std::vector<TString> OldBindingIds;
        // DescribeEachBinding
        std::vector<FederatedQuery::BindingContent> OldBindingContents;
    };

    template<>
    struct TControlPlaneRequest<FederatedQuery::DeleteConnectionRequest, EvDeleteConnectionRequest> :
        public TBaseControlPlaneRequest<TEvDeleteConnectionRequest,
                                        FederatedQuery::DeleteConnectionRequest,
                                        EvDeleteConnectionRequest> {
        using TBaseControlPlaneRequest<
            TControlPlaneRequest<FederatedQuery::DeleteConnectionRequest, EvDeleteConnectionRequest>,
            FederatedQuery::DeleteConnectionRequest,
            EvDeleteConnectionRequest>::TBaseControlPlaneRequest;

        TMaybe<FederatedQuery::ConnectionContent> ConnectionContent;
    };

    template<>
    struct TControlPlaneRequest<FederatedQuery::CreateBindingRequest, EvCreateBindingRequest> :
        public TBaseControlPlaneRequest<TEvCreateBindingRequest,
                                        FederatedQuery::CreateBindingRequest,
                                        EvCreateBindingRequest> {
        using TBaseControlPlaneRequest<
            TControlPlaneRequest<FederatedQuery::CreateBindingRequest, EvCreateBindingRequest>,
            FederatedQuery::CreateBindingRequest,
            EvCreateBindingRequest>::TBaseControlPlaneRequest;

        TMaybe<FederatedQuery::ConnectionContent> ConnectionContent;
        TMaybe<EEntityType> EntityWithSameNameType;
        bool ConnectionsWithSameNameWereListed = false;
        bool BindingWithSameNameWereListed     = false;
    };

    template<>
    struct TControlPlaneRequest<FederatedQuery::ModifyBindingRequest, EvModifyBindingRequest> :
        public TBaseControlPlaneRequest<TEvModifyBindingRequest,
                                        FederatedQuery::ModifyBindingRequest,
                                        EvModifyBindingRequest> {
        using TBaseControlPlaneRequest<
            TControlPlaneRequest<FederatedQuery::ModifyBindingRequest, EvModifyBindingRequest>,
            FederatedQuery::ModifyBindingRequest,
            EvModifyBindingRequest>::TBaseControlPlaneRequest;

        TMaybe<FederatedQuery::BindingContent> OldBindingContent;
        TMaybe<FederatedQuery::ConnectionContent> ConnectionContent;
    };

    template<>
    struct TControlPlaneRequest<FederatedQuery::DeleteBindingRequest, EvDeleteBindingRequest> :
        public TBaseControlPlaneRequest<TEvDeleteBindingRequest,
                                        FederatedQuery::DeleteBindingRequest,
                                        EvDeleteBindingRequest> {
        using TBaseControlPlaneRequest<
            TControlPlaneRequest<FederatedQuery::DeleteBindingRequest, EvDeleteBindingRequest>,
            FederatedQuery::DeleteBindingRequest,
            EvDeleteBindingRequest>::TBaseControlPlaneRequest;

        TMaybe<FederatedQuery::BindingContent> OldBindingContent;
    };
};

NActors::TActorId ControlPlaneProxyActorId();

}

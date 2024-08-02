#pragma once

#include <util/datetime/base.h>
#include <util/generic/map.h>
#include <util/generic/maybe.h>

#include <ydb/library/actors/core/event_pb.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/interconnect/events_local.h>

#include <ydb/core/fq/libs/protos/fq_private.pb.h>
#include <ydb/public/api/protos/draft/fq.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_params/params.h>

#include <ydb/library/yql/public/issue/yql_issue.h>

#include <ydb/core/fq/libs/common/debug_info.h>
#include <ydb/core/fq/libs/control_plane_config/events/events.h>
#include <ydb/core/fq/libs/control_plane_storage/proto/yq_internal.pb.h>
#include <ydb/core/fq/libs/events/event_subspace.h>
#include <ydb/core/fq/libs/quota_manager/events/events.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

namespace NFq {

template<typename T>
struct TAuditDetails {
    TMaybe<T> Before;
    TMaybe<T> After;
    bool IdempotencyResult = false;
    TString CloudId;

    size_t GetByteSize() const {
        return sizeof(*this)
                + (Before.Empty() ? 0 : Before->ByteSizeLong())
                + (After.Empty() ? 0 : After->ByteSizeLong())
                + CloudId.Size();
    }
};

struct TNodeInfo {
    ui32 NodeId;
    TString InstanceId;
    TString HostName;
};

struct TPermissions {
    enum TPermission {
        VIEW_PUBLIC = 0x1,
        VIEW_PRIVATE = 0x2,
        VIEW_AST = 0x4,
        MANAGE_PUBLIC = 0x8,
        MANAGE_PRIVATE = 0x10,
        QUERY_INVOKE = 0x40,
        VIEW_QUERY_TEXT = 0x80
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

inline size_t GetIssuesByteSize(const NYql::TIssues& issues) {
    size_t size = 0;
    for (const auto& issue: issues) {
        NYql::WalkThroughIssues(issue, false, [&size](const auto& issue, ui16) {
            size += sizeof(issue);
            size += issue.GetMessage().size();
        });
    }
    size += issues.Size() * sizeof(NYql::TIssue);
    return size;
}

inline size_t GetIssuesByteSize(const TMaybe<NYql::TIssues>& issues) {
    return issues.Empty() ? 0 : GetIssuesByteSize(*issues);
}

inline size_t GetDebugInfoByteSize(const TDebugInfoPtr& infos) {
    if (!infos) {
        return 0;
    }
    size_t size = 0;
    for (const auto& info: *infos) {
        size += info.GetByteSize();
    }
    return size;
}

struct TEvControlPlaneStorage {
    // Event ids.
    enum EEv : ui32 {
        EvCreateQueryRequest = YqEventSubspaceBegin(NFq::TYqEventSubspace::ControlPlaneStorage),
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
        EvCreateRateLimiterResourceRequest,
        EvCreateRateLimiterResourceResponse,
        EvDeleteRateLimiterResourceRequest,
        EvDeleteRateLimiterResourceResponse,
        EvDbRequestResult, // private // internal_events.h
        EvCreateDatabaseRequest,
        EvCreateDatabaseResponse,
        EvDescribeDatabaseRequest,
        EvDescribeDatabaseResponse,
        EvModifyDatabaseRequest,
        EvModifyDatabaseResponse,
        EvFinalStatusReport,
        EvEnd,
    };

    static_assert(EvEnd <= YqEventSubspaceEnd(NFq::TYqEventSubspace::ControlPlaneStorage), "All events must be in their subspace");

    template<typename TDerived, typename ProtoMessage, ui32 EventType>
    struct TBaseControlPlaneRequest : NActors::TEventLocal<TDerived, EventType> {
        using TProto = ProtoMessage;

        explicit TBaseControlPlaneRequest(
            const TString& scope,
            const ProtoMessage& request,
            const TString& user,
            const TString& token,
            const TString& cloudId,
            TPermissions permissions,
            TMaybe<TQuotaMap> quotas,
            TTenantInfo::TPtr tenantInfo,
            const FederatedQuery::Internal::ComputeDatabaseInternal& computeDatabase)
            : Scope(scope)
            , Request(request)
            , User(user)
            , Token(token)
            , CloudId(cloudId)
            , Permissions(permissions)
            , Quotas(std::move(quotas))
            , TenantInfo(tenantInfo)
            , ComputeDatabase(computeDatabase) { }

        size_t GetByteSize() const {
            return sizeof(*this)
                    + Scope.Size()
                    + Request.ByteSizeLong()
                    + User.Size()
                    + Token.Size()
                    + CloudId.Size();
        }

        TString Scope;
        ProtoMessage Request;
        TString User;
        TString Token;
        TString CloudId;
        TPermissions Permissions;
        TMaybe<TQuotaMap> Quotas;
        TTenantInfo::TPtr TenantInfo;
        FederatedQuery::Internal::ComputeDatabaseInternal ComputeDatabase;
        bool ExtractSensitiveFields = false;
    };

    template<typename TProtoMessage, ui32 EventType>
    struct TControlPlaneRequest :
        TBaseControlPlaneRequest<TControlPlaneRequest<TProtoMessage, EventType>,
                                 TProtoMessage,
                                 EventType> {
        using TBaseControlPlaneRequest<TControlPlaneRequest<TProtoMessage, EventType>,
                                       TProtoMessage,
                                       EventType>::TBaseControlPlaneRequest;
    };

    template<typename ProtoMessage, ui32 EventType>
    struct TControlPlaneListRequest :
        public TBaseControlPlaneRequest<TControlPlaneListRequest<ProtoMessage, EventType>,
                                        ProtoMessage,
                                        EventType> {
        using TProto = ProtoMessage;

        explicit TControlPlaneListRequest(
            const TString& scope,
            const ProtoMessage& request,
            const TString& user,
            const TString& token,
            const TString& cloudId,
            TPermissions permissions,
            TMaybe<TQuotaMap> quotas,
            TTenantInfo::TPtr tenantInfo,
            const FederatedQuery::Internal::ComputeDatabaseInternal& computeDatabase,
            bool isExactNameMatch = false)
            : TBaseControlPlaneRequest<TControlPlaneListRequest<ProtoMessage, EventType>,
                                       ProtoMessage,
                                       EventType>(scope,
                                                  request,
                                                  user,
                                                  token,
                                                  cloudId,
                                                  permissions,
                                                  std::move(quotas),
                                                  tenantInfo,
                                                  computeDatabase)
            , IsExactNameMatch(isExactNameMatch) { }

        bool IsExactNameMatch = false;
    };

    template<typename TDerived, typename ProtoMessage, ui32 EventType>
    struct TControlPlaneResponse : NActors::TEventLocal<TDerived, EventType> {
        using TProto = ProtoMessage;

        explicit TControlPlaneResponse(const ProtoMessage& result)
            : Result(result)
        {
        }

        explicit TControlPlaneResponse(const NYql::TIssues& issues)
            : Issues(issues)
        {
        }

        explicit TControlPlaneResponse(const ProtoMessage& result, const NYql::TIssues& issues)
            : Result(result), Issues(issues)
        {
        }

        size_t GetByteSize() const {
            return sizeof(*this)
                    + Result.ByteSizeLong()
                    + GetIssuesByteSize(Issues)
                    + GetDebugInfoByteSize(DebugInfo);
        }

        ProtoMessage Result;
        NYql::TIssues Issues;
        TDebugInfoPtr DebugInfo;
    };

    template<typename ProtoMessage, ui32 EventType>
    struct TControlPlaneNonAuditableResponse : TControlPlaneResponse<TControlPlaneNonAuditableResponse<ProtoMessage, EventType>, ProtoMessage, EventType> {
        using TProto = ProtoMessage;
        static constexpr bool Auditable = false;

        explicit TControlPlaneNonAuditableResponse(const ProtoMessage& result)
            : TControlPlaneResponse<TControlPlaneNonAuditableResponse<ProtoMessage, EventType>, ProtoMessage, EventType>(result)
        {
        }

        explicit TControlPlaneNonAuditableResponse(const NYql::TIssues& issues)
            : TControlPlaneResponse<TControlPlaneNonAuditableResponse<ProtoMessage, EventType>, ProtoMessage, EventType>(issues)
        {
        }

        explicit TControlPlaneNonAuditableResponse(const ProtoMessage& result, const NYql::TIssues& issues)
            : TControlPlaneResponse<TControlPlaneNonAuditableResponse<ProtoMessage, EventType>, ProtoMessage, EventType>(result, issues)
        {
        }
    };

    template<typename ProtoMessage, typename AuditMessage, ui32 EventType>
    struct TControlPlaneAuditableResponse : TControlPlaneResponse<TControlPlaneAuditableResponse<ProtoMessage, AuditMessage, EventType>, ProtoMessage, EventType> {
        using TProto = ProtoMessage;
        static constexpr bool Auditable = true;
        using TAuditMessage = AuditMessage;

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

        explicit TControlPlaneAuditableResponse(const ProtoMessage& result, const NYql::TIssues& issues, const TAuditDetails<AuditMessage>& auditDetails)
            : TControlPlaneResponse<TControlPlaneAuditableResponse<ProtoMessage, AuditMessage, EventType>, ProtoMessage, EventType>(result, issues)
            , AuditDetails(auditDetails)
        {
        }

        size_t GetByteSize() const {
            return TControlPlaneResponse<TControlPlaneAuditableResponse<ProtoMessage, AuditMessage, EventType>, ProtoMessage, EventType>::GetByteSize()
                    + AuditDetails.GetByteSize();
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
    using TEvListConnectionsRequest = TControlPlaneListRequest<FederatedQuery::ListConnectionsRequest, EvListConnectionsRequest>;
    using TEvListConnectionsResponse = TControlPlaneNonAuditableResponse<FederatedQuery::ListConnectionsResult, EvListConnectionsResponse>;
    using TEvDescribeConnectionRequest = TControlPlaneRequest<FederatedQuery::DescribeConnectionRequest, EvDescribeConnectionRequest>;
    using TEvDescribeConnectionResponse = TControlPlaneNonAuditableResponse<FederatedQuery::DescribeConnectionResult, EvDescribeConnectionResponse>;
    using TEvModifyConnectionRequest = TControlPlaneRequest<FederatedQuery::ModifyConnectionRequest, EvModifyConnectionRequest>;
    using TEvModifyConnectionResponse = TControlPlaneAuditableResponse<FederatedQuery::ModifyConnectionResult, FederatedQuery::Connection, EvModifyConnectionResponse>;
    using TEvDeleteConnectionRequest = TControlPlaneRequest<FederatedQuery::DeleteConnectionRequest, EvDeleteConnectionRequest>;
    using TEvDeleteConnectionResponse = TControlPlaneAuditableResponse<FederatedQuery::DeleteConnectionResult, FederatedQuery::Connection, EvDeleteConnectionResponse>;
    using TEvCreateBindingRequest = TControlPlaneRequest<FederatedQuery::CreateBindingRequest, EvCreateBindingRequest>;
    using TEvCreateBindingResponse = TControlPlaneAuditableResponse<FederatedQuery::CreateBindingResult, FederatedQuery::Binding, EvCreateBindingResponse>;
    using TEvListBindingsRequest = TControlPlaneListRequest<FederatedQuery::ListBindingsRequest, EvListBindingsRequest>;
    using TEvListBindingsResponse = TControlPlaneNonAuditableResponse<FederatedQuery::ListBindingsResult, EvListBindingsResponse>;
    using TEvDescribeBindingRequest = TControlPlaneRequest<FederatedQuery::DescribeBindingRequest, EvDescribeBindingRequest>;
    using TEvDescribeBindingResponse = TControlPlaneNonAuditableResponse<FederatedQuery::DescribeBindingResult, EvDescribeBindingResponse>;
    using TEvModifyBindingRequest = TControlPlaneRequest<FederatedQuery::ModifyBindingRequest, EvModifyBindingRequest>;
    using TEvModifyBindingResponse = TControlPlaneAuditableResponse<FederatedQuery::ModifyBindingResult, FederatedQuery::Binding, EvModifyBindingResponse>;
    using TEvDeleteBindingRequest = TControlPlaneRequest<FederatedQuery::DeleteBindingRequest, EvDeleteBindingRequest>;
    using TEvDeleteBindingResponse = TControlPlaneAuditableResponse<FederatedQuery::DeleteBindingResult, FederatedQuery::Binding, EvDeleteBindingResponse>;

    // internal messages
    struct TEvWriteResultDataRequest : NActors::TEventLocal<TEvWriteResultDataRequest, EvWriteResultDataRequest> {

        TEvWriteResultDataRequest() = default;

        explicit TEvWriteResultDataRequest(
            Fq::Private::WriteTaskResultRequest&& request)
            : Request(std::move(request))
        {}

        size_t GetByteSize() const {
            return sizeof(*this)
                    + Request.ByteSizeLong();
        }

        Fq::Private::WriteTaskResultRequest Request;
    };

    struct TEvWriteResultDataResponse : NActors::TEventLocal<TEvWriteResultDataResponse, EvWriteResultDataResponse> {
        static constexpr bool Auditable = false;

        explicit TEvWriteResultDataResponse(
            const Fq::Private::WriteTaskResultResult& record)
            : Record(record)
        {}

        explicit TEvWriteResultDataResponse(
            const NYql::TIssues& issues)
            : Issues(issues)
        {}

        size_t GetByteSize() const {
            return sizeof(*this)
                    + Record.ByteSizeLong()
                    + GetIssuesByteSize(Issues)
                    + GetDebugInfoByteSize(DebugInfo);
        }

        Fq::Private::WriteTaskResultResult Record;
        NYql::TIssues Issues;
        TDebugInfoPtr DebugInfo;
    };

    struct TEvGetTaskRequest : NActors::TEventLocal<TEvGetTaskRequest, EvGetTaskRequest> {

        TEvGetTaskRequest() = default;

        explicit TEvGetTaskRequest(
            Fq::Private::GetTaskRequest&& request)
            : Request(std::move(request))
        {}

        size_t GetByteSize() const {
            return sizeof(*this)
                    + Request.ByteSizeLong();
        }

        Fq::Private::GetTaskRequest Request;
        TTenantInfo::TPtr TenantInfo;
    };

    struct TEvGetTaskResponse : NActors::TEventLocal<TEvGetTaskResponse, EvGetTaskResponse> {
        static constexpr bool Auditable = false;

        explicit TEvGetTaskResponse(
            const Fq::Private::GetTaskResult& record)
            : Record(record)
        {}

        explicit TEvGetTaskResponse(
            const NYql::TIssues& issues)
            : Issues(issues)
        {}

        size_t GetByteSize() const {
            return sizeof(*this)
                    + Record.ByteSizeLong()
                    + GetIssuesByteSize(Issues)
                    + GetDebugInfoByteSize(DebugInfo);
        }

        Fq::Private::GetTaskResult Record;
        NYql::TIssues Issues;
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

        size_t GetByteSize() const {
            return sizeof(*this)
                    + DatabaseId.Size()
                    + Database.Size()
                    + TopicPath.Size()
                    + ConsumerName.Size()
                    + ClusterEndpoint.Size()
                    + TokenName.Size();
        }
    };

    struct TEvPingTaskRequest : NActors::TEventLocal<TEvPingTaskRequest, EvPingTaskRequest> {

        TEvPingTaskRequest() = default;

        explicit TEvPingTaskRequest(
            Fq::Private::PingTaskRequest&& request)
            : Request(std::move(request))
        {}

        size_t GetByteSize() const {
            return sizeof(*this)
                    + Request.ByteSizeLong();
        }

        Fq::Private::PingTaskRequest Request;
        TTenantInfo::TPtr TenantInfo;
    };

    struct TEvPingTaskResponse : NActors::TEventLocal<TEvPingTaskResponse, EvPingTaskResponse> {
        static constexpr bool Auditable = false;

        explicit TEvPingTaskResponse(
            const Fq::Private::PingTaskResult& record)
            : Record(record)
        {}

        explicit TEvPingTaskResponse(
            const NYql::TIssues& issues)
            : Issues(issues)
        {}

        size_t GetByteSize() const {
            return sizeof(*this)
                    + Record.ByteSizeLong()
                    + GetIssuesByteSize(Issues)
                    + GetDebugInfoByteSize(DebugInfo);
        }

        Fq::Private::PingTaskResult Record;
        NYql::TIssues Issues;
        TDebugInfoPtr DebugInfo;
    };

    struct TEvNodesHealthCheckRequest : NActors::TEventLocal<TEvNodesHealthCheckRequest, EvNodesHealthCheckRequest> {

        TEvNodesHealthCheckRequest() = default;

        explicit TEvNodesHealthCheckRequest(
            Fq::Private::NodesHealthCheckRequest&& request)
            : Request(std::move(request))
        {}

        size_t GetByteSize() const {
            return sizeof(*this)
                    + Request.ByteSizeLong();
        }

        Fq::Private::NodesHealthCheckRequest Request;
    };

    struct TEvNodesHealthCheckResponse : NActors::TEventLocal<TEvNodesHealthCheckResponse, EvNodesHealthCheckResponse> {
        static constexpr bool Auditable = false;

        explicit TEvNodesHealthCheckResponse(
            const Fq::Private::NodesHealthCheckResult& record)
            : Record(record)
        {}

        explicit TEvNodesHealthCheckResponse(
            const NYql::TIssues& issues)
            : Issues(issues)
        {}

        size_t GetByteSize() const {
            return sizeof(*this)
                    + Record.ByteSizeLong()
                    + GetIssuesByteSize(Issues)
                    + GetDebugInfoByteSize(DebugInfo);
        }

        Fq::Private::NodesHealthCheckResult Record;
        NYql::TIssues Issues;
        TDebugInfoPtr DebugInfo;
    };

    struct TEvCreateRateLimiterResourceRequest : NActors::TEventLocal<TEvCreateRateLimiterResourceRequest, EvCreateRateLimiterResourceRequest> {
        TEvCreateRateLimiterResourceRequest() = default;

        explicit TEvCreateRateLimiterResourceRequest(
            Fq::Private::CreateRateLimiterResourceRequest&& request)
            : Request(std::move(request))
        {}

        size_t GetByteSize() const {
            return sizeof(*this)
                    + Request.ByteSizeLong();
        }

        Fq::Private::CreateRateLimiterResourceRequest Request;
    };

    struct TEvCreateRateLimiterResourceResponse : NActors::TEventLocal<TEvCreateRateLimiterResourceResponse, EvCreateRateLimiterResourceResponse> {
        static constexpr bool Auditable = false;

        explicit TEvCreateRateLimiterResourceResponse(
            const Fq::Private::CreateRateLimiterResourceResult& record)
            : Record(record)
        {}

        explicit TEvCreateRateLimiterResourceResponse(
            const NYql::TIssues& issues)
            : Issues(issues)
        {}

        size_t GetByteSize() const {
            return sizeof(*this)
                    + Record.ByteSizeLong()
                    + GetIssuesByteSize(Issues)
                    + GetDebugInfoByteSize(DebugInfo);
        }

        using TProto = Fq::Private::CreateRateLimiterResourceResult;
        TProto Record;
        NYql::TIssues Issues;
        TDebugInfoPtr DebugInfo;
    };

    struct TEvDeleteRateLimiterResourceRequest : NActors::TEventLocal<TEvDeleteRateLimiterResourceRequest, EvDeleteRateLimiterResourceRequest> {
        TEvDeleteRateLimiterResourceRequest() = default;

        explicit TEvDeleteRateLimiterResourceRequest(
            Fq::Private::DeleteRateLimiterResourceRequest&& request)
            : Request(std::move(request))
        {}

        size_t GetByteSize() const {
            return sizeof(*this)
                    + Request.ByteSizeLong();
        }

        Fq::Private::DeleteRateLimiterResourceRequest Request;
    };

    struct TEvDeleteRateLimiterResourceResponse : NActors::TEventLocal<TEvDeleteRateLimiterResourceResponse, EvDeleteRateLimiterResourceResponse> {
        static constexpr bool Auditable = false;

        explicit TEvDeleteRateLimiterResourceResponse(
            const Fq::Private::DeleteRateLimiterResourceResult& record)
            : Record(record)
        {}

        explicit TEvDeleteRateLimiterResourceResponse(
            const NYql::TIssues& issues)
            : Issues(issues)
        {}

        size_t GetByteSize() const {
            return sizeof(*this)
                    + Record.ByteSizeLong()
                    + GetIssuesByteSize(Issues)
                    + GetDebugInfoByteSize(DebugInfo);
        }

        using TProto = Fq::Private::DeleteRateLimiterResourceResult;
        TProto Record;
        NYql::TIssues Issues;
        TDebugInfoPtr DebugInfo;
    };

    struct TEvCreateDatabaseRequest : NActors::TEventLocal<TEvCreateDatabaseRequest, EvCreateDatabaseRequest> {
        TEvCreateDatabaseRequest() = default;

        explicit TEvCreateDatabaseRequest(const TString& cloudId, const TString& scope, const FederatedQuery::Internal::ComputeDatabaseInternal& record)
            : CloudId(cloudId)
            , Scope(scope)
            , Record(record)
        {}

        size_t GetByteSize() const {
            return sizeof(*this)
                    + CloudId.Size()
                    + Scope.Size()
                    + Record.ByteSizeLong();
        }

        TString CloudId;
        TString Scope;
        FederatedQuery::Internal::ComputeDatabaseInternal Record;
    };

    struct TEvCreateDatabaseResponse : NActors::TEventLocal<TEvCreateDatabaseResponse, EvCreateDatabaseResponse> {
        static constexpr bool Auditable = false;

        explicit TEvCreateDatabaseResponse()
        {}

        explicit TEvCreateDatabaseResponse(const NYql::TIssues& issues)
            : Issues(issues)
        {}

        size_t GetByteSize() const {
            return sizeof(*this)
                    + GetIssuesByteSize(Issues)
                    + GetDebugInfoByteSize(DebugInfo);
        }

        NYql::TIssues Issues;
        TDebugInfoPtr DebugInfo;
    };

    struct TEvDescribeDatabaseRequest : NActors::TEventLocal<TEvDescribeDatabaseRequest, EvDescribeDatabaseRequest> {

        TEvDescribeDatabaseRequest() = default;

        explicit TEvDescribeDatabaseRequest(const TString& cloudId, const TString& scope)
            : CloudId(cloudId)
            , Scope(scope)
        {}

        size_t GetByteSize() const {
            return sizeof(*this)
                    + Request.ByteSizeLong()
                    + CloudId.Size()
                    + Scope.Size();
        }

        google::protobuf::Empty Request;
        TString CloudId;
        TString Scope;
    };

    struct TEvDescribeDatabaseResponse : NActors::TEventLocal<TEvDescribeDatabaseResponse, EvDescribeDatabaseResponse> {
        static constexpr bool Auditable = false;

        explicit TEvDescribeDatabaseResponse(const FederatedQuery::Internal::ComputeDatabaseInternal& record)
            : Record(record)
        {}

        explicit TEvDescribeDatabaseResponse(
            const NYql::TIssues& issues
            )
            : Issues(issues)
        {}

        size_t GetByteSize() const {
            return sizeof(*this)
                    + Record.ByteSizeLong()
                    + GetIssuesByteSize(Issues)
                    + GetDebugInfoByteSize(DebugInfo);
        }

        FederatedQuery::Internal::ComputeDatabaseInternal Record;
        NYql::TIssues Issues;
        TDebugInfoPtr DebugInfo;
    };

    struct TEvModifyDatabaseRequest : NActors::TEventLocal<TEvModifyDatabaseRequest, EvModifyDatabaseRequest> {
        TEvModifyDatabaseRequest() = default;

        explicit TEvModifyDatabaseRequest(const TString& cloudId, const TString& scope)
            : CloudId(cloudId)
            , Scope(scope)
        {}

        size_t GetByteSize() const {
            return sizeof(*this)
                    + CloudId.Size()
                    + Scope.Size();
        }

        TString CloudId;
        TString Scope;
        TMaybe<bool> Synchronized;
        TMaybe<TInstant> LastAccessAt;
        TMaybe<bool> WorkloadManagerSynchronized;
    };

    struct TEvModifyDatabaseResponse : NActors::TEventLocal<TEvModifyDatabaseResponse, EvModifyDatabaseResponse> {
        static constexpr bool Auditable = false;

        explicit TEvModifyDatabaseResponse()
        {}

        explicit TEvModifyDatabaseResponse(const NYql::TIssues& issues)
            : Issues(issues)
        {}

        size_t GetByteSize() const {
            return sizeof(*this)
                    + GetIssuesByteSize(Issues)
                    + GetDebugInfoByteSize(DebugInfo);
        }

        NYql::TIssues Issues;
        TDebugInfoPtr DebugInfo;
    };

    struct TEvFinalStatusReport : NActors::TEventLocal<TEvFinalStatusReport, EvFinalStatusReport> {
        TEvFinalStatusReport(
            const TString& queryId, const TString& jobId, const TString& cloudId, const TString& scope,
            std::vector<std::pair<TString, i64>>&& statistics, FederatedQuery::QueryMeta::ComputeStatus status,
            NYql::NDqProto::StatusIds::StatusCode statusCode, FederatedQuery::QueryContent::QueryType queryType,
            const NYql::TIssues& issues, const NYql::TIssues& transientIssues)
            : QueryId(queryId)
            , JobId(jobId)
            , CloudId(cloudId)
            , Scope(scope)
            , Statistics(std::move(statistics))
            , Status(status)
            , StatusCode(statusCode)
            , QueryType(queryType)
            , Issues(issues)
            , TransientIssues(transientIssues)
        {}

        TString QueryId;
        TString JobId;
        TString CloudId;
        TString Scope;
        std::vector<std::pair<TString, i64>> Statistics;
        FederatedQuery::QueryMeta::ComputeStatus Status = FederatedQuery::QueryMeta::COMPUTE_STATUS_UNSPECIFIED;
        NYql::NDqProto::StatusIds::StatusCode StatusCode = NYql::NDqProto::StatusIds::UNSPECIFIED;
        FederatedQuery::QueryContent::QueryType QueryType = FederatedQuery::QueryContent::QUERY_TYPE_UNSPECIFIED;
        NYql::TIssues Issues;
        NYql::TIssues TransientIssues;
    };
};

}

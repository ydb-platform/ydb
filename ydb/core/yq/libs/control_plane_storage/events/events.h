#pragma once

#include <util/datetime/base.h>
#include <util/generic/map.h>
#include <util/generic/maybe.h>

#include <library/cpp/actors/core/event_pb.h>
#include <library/cpp/actors/core/events.h>
#include <library/cpp/actors/interconnect/events_local.h>

#include <ydb/core/yq/libs/protos/fq_private.pb.h>
#include <ydb/public/api/protos/yq.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_params/params.h>

#include <ydb/library/yql/public/issue/yql_issue.h>

#include <ydb/core/yq/libs/common/debug_info.h>
#include <ydb/core/yq/libs/control_plane_config/events/events.h>
#include <ydb/core/yq/libs/control_plane_storage/proto/yq_internal.pb.h>
#include <ydb/core/yq/libs/events/event_subspace.h>
#include <ydb/core/yq/libs/quota_manager/events/events.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

namespace NYq {

template<typename T>
struct TAuditDetails {
    TMaybe<T> Before;
    TMaybe<T> After;
    bool IdempotencyResult = false;
    TString CloudId;

    size_t GetByteSize() const {
        return sizeof(*this)
                + Before.Empty() ? 0 : Before->ByteSizeLong()
                + After.Empty() ? 0 : After->ByteSizeLong()
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
        EvCreateRateLimiterResourceRequest,
        EvCreateRateLimiterResourceResponse,
        EvDeleteRateLimiterResourceRequest,
        EvDeleteRateLimiterResourceResponse,
        EvDbRequestResult, // private // internal_events.h
        EvEnd,
    };

    static_assert(EvEnd <= YqEventSubspaceEnd(NYq::TYqEventSubspace::ControlPlaneStorage), "All events must be in their subspace");

    template<typename ProtoMessage, ui32 EventType>
    struct TControlPlaneRequest : NActors::TEventLocal<TControlPlaneRequest<ProtoMessage, EventType>, EventType> {
        using TProto = ProtoMessage;

        explicit TControlPlaneRequest(const TString& scope,
                                             const ProtoMessage& request,
                                             const TString& user,
                                             const TString& token,
                                             const TString& cloudId,
                                             TPermissions permissions,
                                             const TQuotaMap& quotas,
                                             TTenantInfo::TPtr tenantInfo)
            : Scope(scope)
            , Request(request)
            , User(user)
            , Token(token)
            , CloudId(cloudId)
            , Permissions(permissions)
            , Quotas(quotas)
            , TenantInfo(tenantInfo)
        {
        }

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
        TQuotaMap Quotas;
        TTenantInfo::TPtr TenantInfo;
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
};

}

#pragma once

#include <util/datetime/base.h>
#include <util/generic/map.h>
#include <util/generic/maybe.h>

#include <library/cpp/actors/core/event_pb.h>
#include <library/cpp/actors/core/events.h>
#include <library/cpp/actors/interconnect/events_local.h>

#include <ydb/core/yq/libs/protos/yq_private.pb.h>
#include <ydb/public/api/protos/yq.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_params/params.h>

#include <ydb/library/yql/public/issue/yql_issue.h>

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

    size_t GetByteSize() const {
        size_t paramsSize = 0;
        for (const auto& [key, value]: Params.GetValues()) {
            paramsSize += key.Size() + NYdb::TProtoAccessor::GetProto(value).ByteSizeLong();
        }
        return sizeof(*this)
                + Query.Size()
                + paramsSize
                + Plan.Size()
                + Ast.Size()
                + Error.Size();
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

namespace {
    inline size_t GetIssuesByteSize(const NYql::TIssues& issues) {
        size_t size = 0;
        for (const auto& issue: issues) {
            NYql::WalkThroughIssues(issue, false, [&size](const auto& issue, ui16) {
                size += sizeof(issue);
                size += issue.Message.size();
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
        EvEnd,
    };

    static_assert(EvEnd <= YqEventSubspaceEnd(NYq::TYqEventSubspace::ControlPlaneStorage), "All events must be in their subspace");

    template<typename ProtoMessage, ui32 EventType>
    struct TControlPlaneRequest : NActors::TEventLocal<TControlPlaneRequest<ProtoMessage, EventType>, EventType> {
        explicit TControlPlaneRequest(const TString& scope,
                                             const ProtoMessage& request,
                                             const TString& user,
                                             const TString& token,
                                             const TString& cloudId,
                                             TPermissions permissions,
                                             const TQuotaMap& quotas)
            : Scope(scope)
            , Request(request)
            , User(user)
            , Token(token)
            , CloudId(cloudId)
            , Permissions(permissions)
            , Quotas(quotas)
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

        size_t GetByteSize() const {
            return sizeof(*this)
                    + ResultId.Size()
                    + ResultSet.ByteSizeLong();
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

        size_t GetByteSize() const {
            return sizeof(*this)
                    + GetIssuesByteSize(Issues)
                    + GetDebugInfoByteSize(DebugInfo);
        }

        NYql::TIssues Issues;
        const ui64 RequestId = 0;
        TDebugInfoPtr DebugInfo;
    };

    struct TEvGetTaskRequest : NActors::TEventLocal<TEvGetTaskRequest, EvGetTaskRequest> {
        explicit TEvGetTaskRequest(
            const TString& owner,
            const TString& hostName,
            const TString& tenantName)
            : Owner(owner)
            , HostName(hostName)
            , TenantName(tenantName)
        {
        }

        size_t GetByteSize() const {
            return sizeof(*this)
                    + Owner.Size()
                    + HostName.Size()
                    + TenantName.Size();
        }

        TString Owner;
        TString HostName;
        TString TenantName;
    };

    struct TTask {
        TString Scope;
        TString QueryId;
        YandexQuery::Query Query;
        YandexQuery::Internal::QueryInternal Internal;
        ui64 Generation = 0;
        TInstant Deadline;

        size_t GetByteSize() const {
            return sizeof(*this)
                    + Scope.Size()
                    + QueryId.Size()
                    + Query.ByteSizeLong()
                    + Internal.ByteSizeLong();
        }
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

        size_t GetByteSize() const {
            return sizeof(*this)
                    + TasksByteSizeLong()
                    + GetIssuesByteSize(Issues)
                    + Owner.Size()
                    + GetDebugInfoByteSize(DebugInfo);
        }

        size_t TasksByteSizeLong() const {
            size_t size = 0;
            for (const auto& task: Tasks) {
                size += task.GetByteSize();
            }
            size += Tasks.size() * sizeof(TTask);
            return size;
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
        explicit TEvPingTaskRequest(const TString& tenantName, const TString& cloudId, const TString& scope, const TString& queryId, const TString& owner, const TInstant& deadline, const TString& resultId = "")
            : TenantName(tenantName)
            , CloudId(cloudId)
            , Scope(scope)
            , QueryId(queryId)
            , Owner(owner)
            , Deadline(deadline)
            , ResultId(resultId)
        {
        }

        size_t GetByteSize() const {
            return sizeof(*this)
                    + TenantName.Size()
                    + CloudId.Size()
                    + Scope.Size()
                    + QueryId.Size()
                    + Owner.Size()
                    + ResultId.Size()
                    + Status.Empty() ? 0 : sizeof(*Status)
                    + GetIssuesByteSize(Issues)
                    + GetIssuesByteSize(TransientIssues)
                    + Statistics.Empty() ? 0 : Statistics->Size()
                    + ResultSetMetasByteSizeLong()
                    + Ast.Empty() ? 0 : Ast->Size()
                    + Plan.Empty() ? 0 : Plan->Size()
                    + StartedAt.Empty() ? 0 : sizeof(*StartedAt)
                    + FinishedAt.Empty() ? 0 : sizeof(*FinishedAt)
                    + CreatedTopicConsumersByteSizeLong()
                    + DqGraphByteSizeLong()
                    + StreamingDisposition.Empty() ? 0 : StreamingDisposition->ByteSizeLong();
        }

        size_t ResultSetMetasByteSizeLong() const {
            if (ResultSetMetas.Empty()) {
                return 0;
            }
            size_t size = 0;
            for (const auto& resultSet: *ResultSetMetas) {
                size += resultSet.ByteSizeLong();
            }
            size += ResultSetMetas->size() * sizeof(YandexQuery::ResultSetMeta);
            return size;
        }

        size_t CreatedTopicConsumersByteSizeLong() const {
            size_t size = 0;
            for (const auto& topic: CreatedTopicConsumers) {
                size += topic.GetByteSize();
            }
            size += CreatedTopicConsumers.size() * sizeof(YandexQuery::ResultSetMeta);
            return size;
        }

        size_t DqGraphByteSizeLong() const {
            size_t size = 0;
            for (const auto& graph: DqGraphs) {
                size += graph.Size();
            }
            size += DqGraphs.size() * sizeof(TString);
            return size;
        }

        const TString TenantName;
        const TString CloudId;
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
        ui64 StatusCode = 0;
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

        size_t GetByteSize() const {
            return sizeof(*this)
                    + GetIssuesByteSize(Issues)
                    + GetDebugInfoByteSize(DebugInfo);
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

        size_t GetByteSize() const {
            return sizeof(*this)
                    + Request.ByteSizeLong();
        }

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

        size_t GetByteSize() const {
            return sizeof(*this)
                    + Record.ByteSizeLong()
                    + GetIssuesByteSize(Issues)
                    + GetDebugInfoByteSize(DebugInfo);
        }

        Yq::Private::NodesHealthCheckResult Record;
        NYql::TIssues Issues;
        TDebugInfoPtr DebugInfo;
    };

};

}

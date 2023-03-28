#include "control_plane_storage.h"
#include "util.h"

#include <util/generic/guid.h>
#include <util/generic/set.h>

#include <library/cpp/protobuf/interop/cast.h>

#include <ydb/core/fq/libs/config/yq_issue.h>
#include <ydb/core/fq/libs/control_plane_storage/events/events.h>

namespace NFq {

class TInMemoryControlPlaneStorageActor : public NActors::TActor<TInMemoryControlPlaneStorageActor> {
    struct TKey {
        TString Scope;
        TString Id;

        bool operator<(const TKey& other) const
        {
            return tie(Scope, Id) < tie(other.Scope, other.Id);
        }
    };

    struct TConfig {
        NConfig::TControlPlaneStorageConfig Proto;
        TDuration IdempotencyKeyTtl;
        TDuration AutomaticQueriesTtl;
        TDuration ResultSetsTtl;
        TDuration AnalyticsRetryCounterUpdateTime;
        TDuration StreamingRetryCounterUpdateTime;
        TDuration TaskLeaseTtl;

        TConfig(const NConfig::TControlPlaneStorageConfig& config)
            : Proto(FillDefaultParameters(config))
            , IdempotencyKeyTtl(GetDuration(Proto.GetIdempotencyKeysTtl(), TDuration::Minutes(10)))
            , AutomaticQueriesTtl(GetDuration(Proto.GetAutomaticQueriesTtl(), TDuration::Days(1)))
            , ResultSetsTtl(GetDuration(Proto.GetResultSetsTtl(), TDuration::Days(1)))
            , TaskLeaseTtl(GetDuration(Proto.GetTaskLeaseTtl(), TDuration::Seconds(30)))
        {
        }
    };

    TConfig Config;
    TMap<TKey, FederatedQuery::Query> Queries;
    TMap<TKey, FederatedQuery::Connection> Connections;
    TMap<TString, TInstant> IdempotencyKeys; // idempotency_key -> created_at

    static constexpr int64_t InitialRevision = 1;

public:
    TInMemoryControlPlaneStorageActor(const NConfig::TControlPlaneStorageConfig& config)
        : TActor(&TThis::StateFunc)
        , Config(config)
    {
    }

    static constexpr char ActorName[] = "YQ_CONTROL_PLANE_STORAGE";

private:
    STRICT_STFUNC(StateFunc,
        hFunc(TEvControlPlaneStorage::TEvCreateQueryRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvListQueriesRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvDescribeQueryRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvModifyQueryRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvDeleteQueryRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvControlQueryRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvGetResultDataRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvListJobsRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvCreateConnectionRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvListConnectionsRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvDescribeConnectionRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvModifyConnectionRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvDeleteConnectionRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvCreateBindingRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvListBindingsRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvDescribeBindingRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvModifyBindingRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvDeleteBindingRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvDescribeJobRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvWriteResultDataRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvGetTaskRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvPingTaskRequest, Handle);
        hFunc(TEvControlPlaneStorage::TEvNodesHealthCheckRequest, Handle);
        hFunc(NActors::NMon::TEvHttpInfo, Handle);
    )

    void Handle(TEvControlPlaneStorage::TEvCreateQueryRequest::TPtr& ev)
    {
        CPS_LOG_I("CreateQueryRequest");

        const FederatedQuery::CreateQueryRequest& request = ev->Get()->Request;
        CPS_LOG_D("CreateQueryRequest: " << request.DebugString());
        CleanupIndempotencyKeys();
        auto now = TInstant::Now();
        const TString idempotencyKey = request.idempotency_key();
        if (idempotencyKey && IdempotencyKeys.contains(idempotencyKey)) {
            CPS_LOG_D("CreateQueryRequest, idempotency key already exist: " << request.DebugString());
            NYql::TIssue issue = MakeErrorIssue(TIssuesIds::BAD_REQUEST, "idempotency key already exist");
            Send(ev->Sender, new TEvControlPlaneStorage::TEvCreateQueryResponse(NYql::TIssues{issue}), 0, ev->Cookie);
            return;
        }

        NYql::TIssues issues = ValidateCreateQueryRequest(ev);
        if (issues) {
            CPS_LOG_D("CreateQueryRequest, validation failed: " << request.DebugString() << " error: " << issues.ToString());
            Send(ev->Sender, new TEvControlPlaneStorage::TEvCreateQueryResponse(issues), 0, ev->Cookie);
            return;
        }

        const TString user = ev->Get()->User;
        const TString scope = ev->Get()->Scope;
        const TString queryId = CreateGuidAsString();
        FederatedQuery::Query query;
        FederatedQuery::QueryContent& content = *query.mutable_content();
        content = request.content();
        FederatedQuery::QueryMeta& meta = *query.mutable_meta();
        FederatedQuery::CommonMeta& common = *meta.mutable_common();
        common.set_id(queryId);
        common.set_created_by(user);
        auto timestamp = NProtoInterop::CastToProto(now);
        *common.mutable_created_at() = timestamp;
        common.set_revision(InitialRevision);

        Queries[{scope, queryId}] = query;

        if (!idempotencyKey) {
            IdempotencyKeys[idempotencyKey] = now;
        }

        CPS_LOG_D("CreateQueryRequest, success: " << request.DebugString() << " query_id: " << queryId);
        FederatedQuery::CreateQueryResult result;
        result.set_query_id(queryId);
        Send(ev->Sender, new TEvControlPlaneStorage::TEvCreateQueryResponse(result, TAuditDetails<FederatedQuery::Query>{}), 0, ev->Cookie);
    }

    void Handle(TEvControlPlaneStorage::TEvListQueriesRequest::TPtr& ev)
    {
        SendEmptyResponse<
            TEvControlPlaneStorage::TEvListQueriesRequest::TPtr,
            FederatedQuery::ListQueriesResult,
            TEvControlPlaneStorage::TEvListQueriesResponse>(ev, "ListQueriesRequest");
    }

    void Handle(TEvControlPlaneStorage::TEvDescribeQueryRequest::TPtr& ev)
    {
        SendEmptyResponse<
            TEvControlPlaneStorage::TEvDescribeQueryRequest::TPtr,
            FederatedQuery::DescribeQueryResult,
            TEvControlPlaneStorage::TEvDescribeQueryResponse>(ev, "DescribeQueryRequest");
    }

    void Handle(TEvControlPlaneStorage::TEvModifyQueryRequest::TPtr& ev)
    {
        SendEmptyAuditResponse<
            TEvControlPlaneStorage::TEvModifyQueryRequest::TPtr,
            FederatedQuery::ModifyQueryResult,
            TEvControlPlaneStorage::TEvModifyQueryResponse,
            TAuditDetails<FederatedQuery::Query>>(ev, "ModifyQueryRequest");
    }

    void Handle(TEvControlPlaneStorage::TEvDeleteQueryRequest::TPtr& ev)
    {
        SendEmptyAuditResponse<
            TEvControlPlaneStorage::TEvDeleteQueryRequest::TPtr,
            FederatedQuery::DeleteQueryResult,
            TEvControlPlaneStorage::TEvDeleteQueryResponse,
            TAuditDetails<FederatedQuery::Query>>(ev, "DeleteQueryRequest");
    }

    void Handle(TEvControlPlaneStorage::TEvControlQueryRequest::TPtr& ev)
    {
        SendEmptyAuditResponse<
            TEvControlPlaneStorage::TEvControlQueryRequest::TPtr,
            FederatedQuery::ControlQueryResult,
            TEvControlPlaneStorage::TEvControlQueryResponse,
            TAuditDetails<FederatedQuery::Query>>(ev, "ControlQueryRequest");
    }

    void Handle(TEvControlPlaneStorage::TEvGetResultDataRequest::TPtr& ev)
    {
        SendEmptyResponse<
            TEvControlPlaneStorage::TEvGetResultDataRequest::TPtr,
            FederatedQuery::GetResultDataResult,
            TEvControlPlaneStorage::TEvGetResultDataResponse>(ev, "GetResultDataRequest");
    }

    void Handle(TEvControlPlaneStorage::TEvListJobsRequest::TPtr& ev)
    {
        SendEmptyResponse<
            TEvControlPlaneStorage::TEvListJobsRequest::TPtr,
            FederatedQuery::ListJobsResult,
            TEvControlPlaneStorage::TEvListJobsResponse>(ev, "ListJobsRequest");
    }

    void Handle(TEvControlPlaneStorage::TEvCreateConnectionRequest::TPtr& ev)
    {
        SendEmptyAuditResponse<
            TEvControlPlaneStorage::TEvCreateConnectionRequest::TPtr,
            FederatedQuery::CreateConnectionResult,
            TEvControlPlaneStorage::TEvCreateConnectionResponse,
            TAuditDetails<FederatedQuery::Connection>>(ev, "CreateConnectionRequest");
    }

    void Handle(TEvControlPlaneStorage::TEvListConnectionsRequest::TPtr& ev)
    {
        SendEmptyResponse<
            TEvControlPlaneStorage::TEvListConnectionsRequest::TPtr,
            FederatedQuery::ListConnectionsResult,
            TEvControlPlaneStorage::TEvListConnectionsResponse>(ev, "ListConnectionsRequest");
    }

    void Handle(TEvControlPlaneStorage::TEvDescribeConnectionRequest::TPtr& ev)
    {
        SendEmptyResponse<
            TEvControlPlaneStorage::TEvDescribeConnectionRequest::TPtr,
            FederatedQuery::DescribeConnectionResult,
            TEvControlPlaneStorage::TEvDescribeConnectionResponse>(ev, "DescribeConnectionRequest");
    }

    void Handle(TEvControlPlaneStorage::TEvModifyConnectionRequest::TPtr& ev)
    {
        SendEmptyAuditResponse<
            TEvControlPlaneStorage::TEvModifyConnectionRequest::TPtr,
            FederatedQuery::ModifyConnectionResult,
            TEvControlPlaneStorage::TEvModifyConnectionResponse,
            TAuditDetails<FederatedQuery::Connection>>(ev, "ModifyConnectionRequest");
    }

    void Handle(TEvControlPlaneStorage::TEvDeleteConnectionRequest::TPtr& ev)
    {
        SendEmptyAuditResponse<
            TEvControlPlaneStorage::TEvDeleteConnectionRequest::TPtr,
            FederatedQuery::DeleteConnectionResult,
            TEvControlPlaneStorage::TEvDeleteConnectionResponse,
            TAuditDetails<FederatedQuery::Connection>>(ev, "DeleteConnectionRequest");
    }

    void Handle(TEvControlPlaneStorage::TEvCreateBindingRequest::TPtr& ev)
    {
        SendEmptyAuditResponse<
            TEvControlPlaneStorage::TEvCreateBindingRequest::TPtr,
            FederatedQuery::CreateBindingResult,
            TEvControlPlaneStorage::TEvCreateBindingResponse,
            TAuditDetails<FederatedQuery::Binding>>(ev, "CreateBindingRequest");
    }

    void Handle(TEvControlPlaneStorage::TEvListBindingsRequest::TPtr& ev)
    {
        SendEmptyResponse<
            TEvControlPlaneStorage::TEvListBindingsRequest::TPtr,
            FederatedQuery::ListBindingsResult,
            TEvControlPlaneStorage::TEvListBindingsResponse>(ev, "ListBindingsRequest");
    }

    void Handle(TEvControlPlaneStorage::TEvDescribeBindingRequest::TPtr& ev)
    {
        SendEmptyResponse<
            TEvControlPlaneStorage::TEvDescribeBindingRequest::TPtr,
            FederatedQuery::DescribeBindingResult,
            TEvControlPlaneStorage::TEvDescribeBindingResponse>(ev, "DescribeBindingRequest");
    }

    void Handle(TEvControlPlaneStorage::TEvModifyBindingRequest::TPtr& ev)
    {
        SendEmptyAuditResponse<
            TEvControlPlaneStorage::TEvModifyBindingRequest::TPtr,
            FederatedQuery::ModifyBindingResult,
            TEvControlPlaneStorage::TEvModifyBindingResponse,
            TAuditDetails<FederatedQuery::Binding>>(ev, "ModifyBindingRequest");
    }

    void Handle(TEvControlPlaneStorage::TEvDeleteBindingRequest::TPtr& ev)
    {
        SendEmptyAuditResponse<
            TEvControlPlaneStorage::TEvDeleteBindingRequest::TPtr,
            FederatedQuery::DeleteBindingResult,
            TEvControlPlaneStorage::TEvDeleteBindingResponse,
            TAuditDetails<FederatedQuery::Binding>>(ev, "DeleteBindingRequest");
    }

    void Handle(TEvControlPlaneStorage::TEvDescribeJobRequest::TPtr& ev)
    {
        SendEmptyResponse<
            TEvControlPlaneStorage::TEvDescribeJobRequest::TPtr,
            FederatedQuery::DescribeJobResult,
            TEvControlPlaneStorage::TEvDescribeJobResponse>(ev, "DescribeJobRequest");
    }

    void Handle(TEvControlPlaneStorage::TEvWriteResultDataRequest::TPtr& ev)
    {
        SendEmptyResponse<
            TEvControlPlaneStorage::TEvWriteResultDataRequest::TPtr,
            NYql::TIssues,
            TEvControlPlaneStorage::TEvWriteResultDataResponse>(ev, "WriteResultDataRequest");
    }

    void Handle(TEvControlPlaneStorage::TEvGetTaskRequest::TPtr& ev)
    {
        CPS_LOG_I("GetTaskRequest");
        Fq::Private::GetTaskResult result;
        auto event = std::make_unique<TEvControlPlaneStorage::TEvGetTaskResponse>(result);
        NActors::TActivationContext::ActorSystem()->Send(new IEventHandle(ev->Sender, SelfId(), event.release(), 0, ev->Cookie));
    }

    void Handle(TEvControlPlaneStorage::TEvPingTaskRequest::TPtr& ev)
    {
        SendEmptyResponse<
            TEvControlPlaneStorage::TEvPingTaskRequest::TPtr,
            Fq::Private::PingTaskResult,
            TEvControlPlaneStorage::TEvPingTaskResponse>(ev, "PingTaskRequest");
    }

    void Handle(TEvControlPlaneStorage::TEvNodesHealthCheckRequest::TPtr& ev)
    {
        SendEmptyResponse<
            TEvControlPlaneStorage::TEvNodesHealthCheckRequest::TPtr,
            Fq::Private::NodesHealthCheckResult,
            TEvControlPlaneStorage::TEvNodesHealthCheckResponse>(ev, "NodesHealthCheckRequest");
    }

    void Handle(NActors::NMon::TEvHttpInfo::TPtr& ev) {
        TStringStream str;
        Send(ev->Sender, new NActors::NMon::TEvHttpInfoRes(str.Str()));
    }

    template<typename TRequest, typename TResult, typename TEvResult>
    void SendEmptyResponse(TRequest& ev, std::string logText) {
        CPS_LOG_I(logText);

        TResult result = {};
        auto event = std::make_unique<TEvResult>(result);
        NActors::TActivationContext::ActorSystem()->Send(new IEventHandle(ev->Sender, SelfId(), event.release(), 0, ev->Cookie));
    }

    template<typename TRequest, typename TResult, typename TEvResult, typename TAuditDetails>
    void SendEmptyAuditResponse(TRequest& ev, std::string logText) {
        CPS_LOG_I(logText);

        TResult result = {};
        TAuditDetails auditDetails = {};
        auto event = std::make_unique<TEvResult>(result, auditDetails);
        NActors::TActivationContext::ActorSystem()->Send(new IEventHandle(ev->Sender, SelfId(), event.release(), 0, ev->Cookie));
    }

    NYql::TIssues ValidateCreateQueryRequest(TEvControlPlaneStorage::TEvCreateQueryRequest::TPtr& ev)
    {
        NYql::TIssues issues;
        const FederatedQuery::CreateQueryRequest& request = ev->Get()->Request;
        const TString user = ev->Get()->User;
        const TString scope = ev->Get()->Scope;
        const FederatedQuery::QueryContent& query = request.content();

        TString error;
        if (!request.validate(error)) {
            issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, error));
        }

        if (query.type() == FederatedQuery::QueryContent::QUERY_TYPE_UNSPECIFIED) {
            issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "type field is not specified"));
        }

        if (query.acl().visibility() == FederatedQuery::Acl::VISIBILITY_UNSPECIFIED) {
            issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "acl.visibility field is not specified"));
        }

        if (request.ByteSize() > static_cast<int>(Config.Proto.GetMaxRequestSize())) {
            issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "Request size exceeded " + ToString(request.ByteSize()) + " out of " + ToString(Config.Proto.GetMaxRequestSize()) + " bytes"));
        }

        const uint64_t countQueries = count_if(Queries.begin(), Queries.end(), [scope](const auto& item) {
            const auto& [key, value] = item;
            return key.Scope == scope;
        });

        if (countQueries > Config.Proto.GetMaxCountQueries()) {
            issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "The count of the queries exceeds the limit of " + ToString(countQueries) + " out of " + ToString(Config.Proto.GetMaxCountQueries())));
        }

        return issues;
    }

    void CleanupIndempotencyKeys()
    {
        auto now = TInstant::Now();
        erase_if(IdempotencyKeys, [this, now](const auto& item) {
            auto const& [idempotencyKey, timestamp] = item;
            return timestamp + Config.IdempotencyKeyTtl < now;
        });
    }
};

NActors::TActorId ControlPlaneStorageServiceActorId(ui32 nodeId) {
    constexpr TStringBuf name = "CTRLSTORAGE";
    return NActors::TActorId(nodeId, name);
}

NActors::IActor* CreateInMemoryControlPlaneStorageServiceActor(const NConfig::TControlPlaneStorageConfig& config) {
    return new TInMemoryControlPlaneStorageActor(config);
}

} // NFq

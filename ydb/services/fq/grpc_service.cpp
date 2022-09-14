#include "grpc_service.h"

#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/grpc_request_proxy.h>
#include <ydb/core/grpc_services/rpc_calls.h>
#include <ydb/core/grpc_services/service_fq.h>
#include <ydb/library/protobuf_printer/security_printer.h>

namespace NKikimr::NGRpcService {

TGRpcFederatedQueryService::TGRpcFederatedQueryService(NActors::TActorSystem *system,
    TIntrusivePtr<NMonitoring::TDynamicCounters> counters, NActors::TActorId id)
    : ActorSystem_(system)
    , Counters_(counters)
    , GRpcRequestProxyId_(id) {}

void TGRpcFederatedQueryService::InitService(grpc::ServerCompletionQueue *cq, NGrpc::TLoggerPtr logger) {
    CQ_ = cq;
    SetupIncomingRequests(std::move(logger));
}

void TGRpcFederatedQueryService::SetGlobalLimiterHandle(NGrpc::TGlobalLimiter* limiter) {
    Limiter_ = limiter;
}

bool TGRpcFederatedQueryService::IncRequest() {
    return Limiter_->Inc();
}

void TGRpcFederatedQueryService::DecRequest() {
    Limiter_->Dec();
    Y_ASSERT(Limiter_->GetCurrentInFlight() >= 0);
}

void TGRpcFederatedQueryService::SetupIncomingRequests(NGrpc::TLoggerPtr logger) {
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);

    using NPerms = NKikimr::TEvTicketParser::TEvAuthorizeTicket;

    static const std::function CreateQueryPermissions{[](const FederatedQuery::CreateQueryRequest& request) {
        TVector<NPerms::TPermission> permissions{
            NPerms::Required("yq.queries.create"),
            NPerms::Optional("yq.connections.use"),
            NPerms::Optional("yq.bindings.use")
        };
        if (request.execute_mode() != FederatedQuery::SAVE) {
            permissions.push_back(NPerms::Required("yq.queries.invoke"));
        }
        if (request.content().acl().visibility() == FederatedQuery::Acl::SCOPE) {
            permissions.push_back(NPerms::Required("yq.resources.managePublic"));
        }
        return permissions;
    }};

    static const std::function ListQueriesPermissions{[](const FederatedQuery::ListQueriesRequest&) -> TVector<NPerms::TPermission> {
        return {
            NPerms::Required("yq.queries.get"),
            NPerms::Optional("yq.resources.viewPublic"),
            NPerms::Optional("yq.resources.viewPrivate")
        };
    }};

    static const std::function DescribeQueryPermissions{[](const FederatedQuery::DescribeQueryRequest&) -> TVector<NPerms::TPermission> {
        return {
            NPerms::Required("yq.queries.get"),
            NPerms::Optional("yq.queries.viewAst"),
            NPerms::Optional("yq.resources.viewPublic"),
            NPerms::Optional("yq.resources.viewPrivate")
        };
    }};

    static const std::function GetQueryStatusPermissions{[](const FederatedQuery::GetQueryStatusRequest&) -> TVector<NPerms::TPermission> {
        return {
            NPerms::Required("yq.queries.getStatus"),
            NPerms::Optional("yq.resources.viewPublic"),
            NPerms::Optional("yq.resources.viewPrivate")
        };
    }};

    static const std::function ModifyQueryPermissions{[](const FederatedQuery::ModifyQueryRequest& request) {
        TVector<NPerms::TPermission> permissions{
            NPerms::Required("yq.queries.update"),
            NPerms::Optional("yq.connections.use"),
            NPerms::Optional("yq.bindings.use"),
            NPerms::Optional("yq.resources.managePrivate")
        };
        if (request.execute_mode() != FederatedQuery::SAVE) {
            permissions.push_back(NPerms::Required("yq.queries.invoke"));
        }
        if (request.content().acl().visibility() == FederatedQuery::Acl::SCOPE) {
            permissions.push_back(NPerms::Required("yq.resources.managePublic"));
        } else {
            permissions.push_back(NPerms::Optional("yq.resources.managePublic"));
        }
        return permissions;
    }};

    static const std::function DeleteQueryPermissions{[](const FederatedQuery::DeleteQueryRequest&) -> TVector<NPerms::TPermission> {
        return {
            NPerms::Required("yq.queries.delete"),
            NPerms::Optional("yq.resources.managePublic"),
            NPerms::Optional("yq.resources.managePrivate")
        };
    }};

    static const std::function ControlQueryPermissions{[](const FederatedQuery::ControlQueryRequest&) -> TVector<NPerms::TPermission> {
        return {
            NPerms::Required("yq.queries.control"),
            NPerms::Optional("yq.resources.managePublic"),
            NPerms::Optional("yq.resources.managePrivate")
        };
    }};

    static const std::function GetResultDataPermissions{[](const FederatedQuery::GetResultDataRequest&) -> TVector<NPerms::TPermission> {
        return {
            NPerms::Required("yq.queries.getData"),
            NPerms::Optional("yq.resources.viewPublic"),
            NPerms::Optional("yq.resources.viewPrivate")
        };
    }};

    static const std::function ListJobsPermissions{[](const FederatedQuery::ListJobsRequest&) -> TVector<NPerms::TPermission> {
        return {
            NPerms::Required("yq.jobs.get"),
            NPerms::Optional("yq.resources.viewPublic"),
            NPerms::Optional("yq.resources.viewPrivate")
        };
    }};

    static const std::function DescribeJobPermissions{[](const FederatedQuery::DescribeJobRequest&) -> TVector<NPerms::TPermission> {
        return {
            NPerms::Required("yq.jobs.get"),
            NPerms::Optional("yq.resources.viewPublic"),
            NPerms::Optional("yq.resources.viewPrivate")
        };
    }};

    static const std::function CreateConnectionPermissions{[](const FederatedQuery::CreateConnectionRequest& request) {
        TVector<NPerms::TPermission> permissions{
            NPerms::Required("yq.connections.create"),
        };
        if (request.content().acl().visibility() == FederatedQuery::Acl::SCOPE) {
            permissions.push_back(NPerms::Required("yq.resources.managePublic"));
        }
        return permissions;
    }};

    static const std::function ListConnectionsPermissions{[](const FederatedQuery::ListConnectionsRequest&) -> TVector<NPerms::TPermission> {
        return {
            NPerms::Required("yq.connections.get"),
            NPerms::Optional("yq.resources.viewPublic"),
            NPerms::Optional("yq.resources.viewPrivate")
        };
    }};

    static const std::function DescribeConnectionPermissions{[](const FederatedQuery::DescribeConnectionRequest&) -> TVector<NPerms::TPermission> {
        return {
            NPerms::Required("yq.connections.get"),
            NPerms::Optional("yq.resources.viewPublic"),
            NPerms::Optional("yq.resources.viewPrivate")
        };
    }};

    static const std::function ModifyConnectionPermissions{[](const FederatedQuery::ModifyConnectionRequest& request) {
        TVector<NPerms::TPermission> permissions{
            NPerms::Required("yq.connections.update"),
            NPerms::Optional("yq.resources.managePrivate")
        };
        if (request.content().acl().visibility() == FederatedQuery::Acl::SCOPE) {
            permissions.push_back(NPerms::Required("yq.resources.managePublic"));
        } else {
            permissions.push_back(NPerms::Optional("yq.resources.managePublic"));
        }
        return permissions;
    }};

    static const std::function DeleteConnectionPermissions{[](const FederatedQuery::DeleteConnectionRequest&) -> TVector<NPerms::TPermission> {
        return {
            NPerms::Required("yq.connections.delete"),
            NPerms::Optional("yq.resources.managePublic"),
            NPerms::Optional("yq.resources.managePrivate")
        };
    }};

    static const std::function TestConnectionPermissions{[](const FederatedQuery::TestConnectionRequest&) -> TVector<NPerms::TPermission> {
        return {
            NPerms::Required("yq.connections.create")
        };
    }};

    static const std::function CreateBindingPermissions{[](const FederatedQuery::CreateBindingRequest&) {
        TVector<NPerms::TPermission> permissions{
            NPerms::Required("yq.bindings.create"),
        };
        // For use in binding links on connection with visibility SCOPE,
        // the yq.resources.managePublic permission is required. But there
        // is no information about connection visibility in this place,
        // so yq.resources.managePublic is always requested as optional
        permissions.push_back(NPerms::Optional("yq.resources.managePublic"));
        return permissions;
    }};

    static const std::function ListBindingsPermissions{[](const FederatedQuery::ListBindingsRequest&) -> TVector<NPerms::TPermission> {
        return {
            NPerms::Required("yq.bindings.get"),
            NPerms::Optional("yq.resources.viewPublic"),
            NPerms::Optional("yq.resources.viewPrivate")
        };
    }};

    static const std::function DescribeBindingPermissions{[](const FederatedQuery::DescribeBindingRequest&) -> TVector<NPerms::TPermission> {
        return {
            NPerms::Required("yq.bindings.get"),
            NPerms::Optional("yq.resources.viewPublic"),
            NPerms::Optional("yq.resources.viewPrivate")
        };
    }};

    static const std::function ModifyBindingPermissions{[](const FederatedQuery::ModifyBindingRequest&) {
        TVector<NPerms::TPermission> permissions{
            NPerms::Required("yq.bindings.update"),
            NPerms::Optional("yq.resources.managePrivate")
        };
        // For use in binding links on connection with visibility SCOPE,
        // the yq.resources.managePublic permission is required. But there
        // is no information about connection visibility in this place,
        // so yq.resources.managePublic is always requested as optional
        permissions.push_back(NPerms::Optional("yq.resources.managePublic"));
        return permissions;
    }};

    static const std::function DeleteBindingPermissions{[](const FederatedQuery::DeleteBindingRequest&) -> TVector<NPerms::TPermission> {
        return {
            NPerms::Required("yq.bindings.delete"),
            NPerms::Optional("yq.resources.managePublic"),
            NPerms::Optional("yq.resources.managePrivate")
        };
    }};

#ifdef ADD_REQUEST
#error ADD_REQUEST macro already defined
#endif
#define ADD_REQUEST(NAME, CB, PERMISSIONS)                                                                                  \
MakeIntrusive<TGRpcRequest<FederatedQuery::NAME##Request, FederatedQuery::NAME##Response, TGRpcFederatedQueryService, TSecurityTextFormatPrinter<FederatedQuery::NAME##Request>, TSecurityTextFormatPrinter<FederatedQuery::NAME##Response>>>( \
    this, &Service_, CQ_,                                                                                      \
    [this](NGrpc::IRequestContextBase *ctx) {                                                                  \
        NGRpcService::ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer());                                       \
        ActorSystem_->Send(GRpcRequestProxyId_,                                                                \
            new TGrpcFqRequestOperationCall<FederatedQuery::NAME##Request, FederatedQuery::NAME##Response>                 \
                (ctx, &CB, PERMISSIONS));                                                                                   \
    },                                                                                                         \
    &FederatedQuery::V1::FederatedQueryService::AsyncService::Request##NAME,                                  \
    #NAME, logger, getCounterBlock("fq", #NAME))                                                     \
    ->Run();                                                                                                   \

    ADD_REQUEST(CreateQuery, DoFederatedQueryCreateQueryRequest, CreateQueryPermissions)
    ADD_REQUEST(ListQueries, DoFederatedQueryListQueriesRequest, ListQueriesPermissions)
    ADD_REQUEST(DescribeQuery, DoFederatedQueryDescribeQueryRequest, DescribeQueryPermissions)
    ADD_REQUEST(GetQueryStatus, DoFederatedQueryGetQueryStatusRequest, GetQueryStatusPermissions)
    ADD_REQUEST(ModifyQuery, DoFederatedQueryModifyQueryRequest, ModifyQueryPermissions)
    ADD_REQUEST(DeleteQuery, DoFederatedQueryDeleteQueryRequest, DeleteQueryPermissions)
    ADD_REQUEST(ControlQuery, DoFederatedQueryControlQueryRequest, ControlQueryPermissions)
    ADD_REQUEST(GetResultData, DoGetResultDataRequest, GetResultDataPermissions)
    ADD_REQUEST(ListJobs, DoListJobsRequest, ListJobsPermissions)
    ADD_REQUEST(DescribeJob, DoDescribeJobRequest, DescribeJobPermissions)
    ADD_REQUEST(CreateConnection, DoCreateConnectionRequest, CreateConnectionPermissions)
    ADD_REQUEST(ListConnections, DoListConnectionsRequest, ListConnectionsPermissions)
    ADD_REQUEST(DescribeConnection, DoDescribeConnectionRequest, DescribeConnectionPermissions)
    ADD_REQUEST(ModifyConnection, DoModifyConnectionRequest, ModifyConnectionPermissions)
    ADD_REQUEST(DeleteConnection, DoDeleteConnectionRequest, DeleteConnectionPermissions)
    ADD_REQUEST(TestConnection, DoTestConnectionRequest, TestConnectionPermissions)
    ADD_REQUEST(CreateBinding, DoCreateBindingRequest, CreateBindingPermissions)
    ADD_REQUEST(ListBindings, DoListBindingsRequest, ListBindingsPermissions)
    ADD_REQUEST(DescribeBinding, DoDescribeBindingRequest, DescribeBindingPermissions)
    ADD_REQUEST(ModifyBinding, DoModifyBindingRequest, ModifyBindingPermissions)
    ADD_REQUEST(DeleteBinding, DoDeleteBindingRequest, DeleteBindingPermissions)

#undef ADD_REQUEST

}

} // namespace NKikimr::NGRpcService

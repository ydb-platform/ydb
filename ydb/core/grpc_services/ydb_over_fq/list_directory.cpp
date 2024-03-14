#include "service.h"
#include "fq_local_grpc_events.h"

#include <ydb/core/grpc_services/rpc_deferrable.h>

namespace NKikimr::NGRpcService::NYdbOverFq {

class ListDirectoryRPC
    : public TRpcOperationRequestActor<
        ListDirectoryRPC, TGrpcRequestOperationCall<Ydb::Scheme::ListDirectoryRequest, Ydb::Scheme::ListDirectoryResponse>>
    , public NLocalGrpc::TCaller {
public:
    using TBase = TRpcOperationRequestActor<
        ListDirectoryRPC,
        TGrpcRequestOperationCall<Ydb::Scheme::ListDirectoryRequest, Ydb::Scheme::ListDirectoryResponse>>;
    using TBase::Become;
    using TBase::Send;
    using TBase::PassAway;
    using TBase::Request_;
    using TBase::GetProtoRequest;

    ListDirectoryRPC(IRequestOpCtx* request, TActorId grpcProxyId)
        : TBase{request}
        , TCaller{std::move(grpcProxyId)}
    {}

    void Bootstrap(const TActorContext& ctx) {
        Become(&ListDirectoryRPC::ListBindingsState);
        ListBindings(ctx, "");
    }

private:
    STRICT_STFUNC(ListBindingsState,
        HFunc(TEvFqListBindingsResponse, HandleListBindings);
    )

    void ListBindings(const TActorContext& ctx, TString continuationToken) {
        FederatedQuery::ListBindingsRequest req;
        constexpr i32 Limit = 100;

        req.set_limit(Limit);
        req.set_page_token(std::move(continuationToken));

        LOG_DEBUG_S(ctx, NKikimrServices::FQ_INTERNAL_SERVICE,
            "pseudo ListDirectories actorId: " << SelfId().ToString() << ", listing bindings");

        MakeLocalCall(std::move(req), Request_, ctx);
    }

    void HandleListBindings(typename TEvFqListBindingsResponse::TPtr& ev, const TActorContext& ctx) {
        const auto& resp = ev->Get()->Message;
        if (HandleFailure(resp.operation(), "ListBindings", ctx)) [[unlikely]] {
            return;
        }

        FederatedQuery::ListBindingsResult result;
        resp.operation().result().UnpackTo(&result);

        for (const auto& binding : result.binding()) {
            Bindings_.push_back(binding);
        }

        if (!result.next_page_token().empty()) {
            ListBindings(ctx, result.next_page_token());
        } else {
            SendReply(ctx);
        }
    }

    // response

    void SendReply(const TActorContext& ctx) {
        Ydb::Scheme::ListDirectoryResult result;
        auto& self = *result.mutable_self();
        self.set_name("/");
        self.set_type(Ydb::Scheme::Entry::DIRECTORY);

        for (const auto& binding : Bindings_) {
            auto& destEntry = *result.add_children();
            destEntry.set_name(binding.name());
            destEntry.set_owner(binding.meta().created_by());
            destEntry.set_type(Ydb::Scheme::Entry::TABLE);
        }

        ReplyWithResult(Ydb::StatusIds::SUCCESS, result, ctx);
    }

    // helpers

    // if status is not success, replies error, returns true
    bool HandleFailure(const Ydb::Operations::Operation& operation, std::string_view opName, const TActorContext& ctx) {
        if (operation.status() == Ydb::StatusIds::SUCCESS) {
            return false;
        }

        NYql::TIssues issues;
        NYql::IssuesFromMessage(operation.issues(), issues);
        LOG_INFO_S(ctx, NKikimrServices::FQ_INTERNAL_SERVICE,
            "pseudo ListDirectory actorId: " << SelfId().ToString() << ", failed to " << opName << ", status: " <<
            Ydb::StatusIds::StatusCode_Name(operation.status()) << ", issues: " << issues.ToOneLineString());
        Reply(Ydb::StatusIds_StatusCode_INTERNAL_ERROR, operation.issues(), ctx);
        return true;
    }

private:
    std::vector<FederatedQuery::BriefBinding> Bindings_;
};

std::function<void(std::unique_ptr<IRequestOpCtx>, const IFacilityProvider&)> GetListDirectoryExecutor(NActors::TActorId grpcProxyId) {
    return [grpcProxyId = std::move(grpcProxyId)](std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
        f.RegisterActor(new ListDirectoryRPC(p.release(), grpcProxyId));
    };
}
}
#include "fq_local_grpc_events.h"
#include "rpc_base.h"
#include "service.h"

#include <ydb/core/grpc_services/rpc_deferrable.h>

namespace NKikimr::NGRpcService::NYdbOverFq {

class ListDirectoryRPC
    : public TRpcBase<
        ListDirectoryRPC, Ydb::Scheme::ListDirectoryRequest, Ydb::Scheme::ListDirectoryResponse> {
public:
    using TBase = TRpcBase<
        ListDirectoryRPC, Ydb::Scheme::ListDirectoryRequest, Ydb::Scheme::ListDirectoryResponse>;
    static constexpr std::string_view RpcName = "ListDirectory";

    using TBase::TBase;

    void Bootstrap(const TActorContext& ctx) {
        ListBindings(ctx, "");
    }

    STRICT_STFUNC(ListBindingsState,
        HFunc(TEvFqListBindingsResponse, TBase::HandleResponse<FederatedQuery::ListBindingsRequest>);
    )

    void ListBindings(const TActorContext& ctx, const TString& continuationToken) {
        FederatedQuery::ListBindingsRequest req;
        constexpr i32 Limit = 100;

        req.set_limit(Limit);
        req.set_page_token(std::move(continuationToken));

        SRC_LOG_T("listing bindings");

        Become(&ListDirectoryRPC::ListBindingsState);
        MakeLocalCall(std::move(req), ctx);
    }

    void Handle(const FederatedQuery::ListBindingsResult& result, const TActorContext& ctx) {
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
            if (binding.type() == FederatedQuery::BindingSetting::DATA_STREAMS) {
                continue;
            }

            auto& destEntry = *result.add_children();
            destEntry.set_name(binding.name());
            destEntry.set_owner(binding.meta().created_by());
            destEntry.set_type(Ydb::Scheme::Entry::TABLE);
        }

        ReplyWithResult(Ydb::StatusIds::SUCCESS, result, ctx);
    }

private:
    std::vector<FederatedQuery::BriefBinding> Bindings_;
};

std::function<void(std::unique_ptr<IRequestOpCtx>, const IFacilityProvider&)> GetListDirectoryExecutor(NActors::TActorId grpcProxyId) {
    return [grpcProxyId](std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
        f.RegisterActor(new ListDirectoryRPC(p.release(), grpcProxyId));
    };
}
}
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
        ListFilteredOutConnections(ctx, "");
    }

    STRICT_STFUNC(ListBindingsState,
        HFunc(TEvFqListBindingsResponse, TBase::HandleResponse<FederatedQuery::ListBindingsRequest>);
        HFunc(TEvFqListConnectionsResponse, TBase::HandleResponse<FederatedQuery::ListConnectionsRequest>);
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

    void ListFilteredOutConnections(const TActorContext& ctx, const TString& continuationToken) {
        FederatedQuery::ListConnectionsRequest req;
        constexpr i32 Limit = 100;

        req.set_limit(Limit);
        req.set_page_token(continuationToken);

        // Actual filter
        req.mutable_filter()->set_connection_type(FederatedQuery::ConnectionSetting::DATA_STREAMS);

        SRC_LOG_T("listing filtered out connections");

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
            CollectedBindings_ = true;
            if (ReadyToReply()) {
                SendReply(ctx);
            }
        }
    }

    void Handle(const FederatedQuery::ListConnectionsResult& result, const TActorContext& ctx) {
        // Expect to have already filtered connections
        for (const auto& connection : result.connection()) {
            FilteredOutConnections_.emplace(connection.meta().id());
        }

        if (!result.next_page_token().empty()) {
            ListFilteredOutConnections(ctx, result.next_page_token());
        } else {
            CollectedFilteredOutConnections_ = true;
            if (ReadyToReply()) {
                SendReply(ctx);
            }
        }
    }

    // response

    bool ReadyToReply() const noexcept {
        return CollectedBindings_ && CollectedFilteredOutConnections_;
    }

    void SendReply(const TActorContext& ctx) {
        Y_ABORT_UNLESS(ReadyToReply());

        Ydb::Scheme::ListDirectoryResult result;
        auto& self = *result.mutable_self();
        self.set_name("/");
        self.set_type(Ydb::Scheme::Entry::DIRECTORY);

        for (const auto& binding : Bindings_) {
            if (FilteredOutConnections_.contains(binding.connection_id())) {
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
    std::unordered_set<TString> FilteredOutConnections_;
    bool CollectedBindings_ = false;
    bool CollectedFilteredOutConnections_ = false;
};

std::function<void(std::unique_ptr<IRequestOpCtx>, const IFacilityProvider&)> GetListDirectoryExecutor(NActors::TActorId grpcProxyId) {
    return [grpcProxyId](std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
        f.RegisterActor(new ListDirectoryRPC(p.release(), grpcProxyId));
    };
}
}
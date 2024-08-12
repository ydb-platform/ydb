#include "fq_local_grpc_events.h"
#include "rpc_base.h"
#include "service.h"

#include <ydb/core/grpc_services/rpc_deferrable.h>
#include <ydb/core/grpc_services/local_grpc/local_grpc.h>

namespace NKikimr::NGRpcService::NYdbOverFq {

class DescribeTableRPC
    : public TRpcBase<
        DescribeTableRPC,Ydb::Table::DescribeTableRequest, Ydb::Table::DescribeTableResponse> {
public:
    using TBase = TRpcBase<
        DescribeTableRPC,Ydb::Table::DescribeTableRequest, Ydb::Table::DescribeTableResponse>;
    static constexpr std::string_view RpcName = "DescribeTable";

    using TBase::TBase;

    void Bootstrap(const TActorContext& ctx) {
        // find binding with given name -> take it's id -> describe binding
        BindingName_ = ExtractName(GetProtoRequest()->path());
        ListBindings(ctx, "");
    }

    static TStringBuf ExtractName(const TString& tablePath) {
        return TStringBuf{tablePath}.RNextTok('/');
    }

    // ListBindings
    STRICT_STFUNC(ListBindingsState,
        HFunc(TEvFqListBindingsResponse, TBase::HandleResponse<FederatedQuery::ListBindingsRequest>);
    )

    void ListBindings(const TActorContext& ctx, const TString& continuationToken) {
        FederatedQuery::ListBindingsRequest req;
        constexpr i32 Limit = 100;

        req.set_limit(Limit);
        req.set_page_token(continuationToken);
        auto& filter = *req.mutable_filter();
        filter.set_name(BindingName_);

        SRC_LOG_T("listing bindings");

        Become(&DescribeTableRPC::ListBindingsState);
        MakeLocalCall(std::move(req), ctx);
    }

    void Handle(const FederatedQuery::ListBindingsResult& result, const TActorContext& ctx) {
        for (const auto& binding : result.binding()) {
            if (binding.name() == BindingName_) {
                DescribeBinding(ctx, binding.meta().id());
                return;
            }
        }

        if (result.next_page_token().empty()) {
            TString errorMsg = TStringBuilder{} << "couldn't find binding with matching name for " << BindingName_;
            SRC_LOG_I("failed: " << errorMsg);
            Reply(
                Ydb::StatusIds_StatusCode_NOT_FOUND, errorMsg, NKikimrIssues::TIssuesIds::DEFAULT_ERROR, ctx);
            return;
        }

        ListBindings(ctx, result.next_page_token());
    }

    STRICT_STFUNC(DescribeBindingState,
        HFunc(TEvFqDescribeBindingResponse, TBase::HandleResponse<FederatedQuery::DescribeBindingRequest>);
    )

    void DescribeBinding(const TActorContext& ctx, const TString& bindingId) {
        FederatedQuery::DescribeBindingRequest req;
        req.set_binding_id(bindingId);

        SRC_LOG_T("describing binding: " << bindingId);

        Become(&DescribeTableRPC::DescribeBindingState);
        MakeLocalCall(std::move(req), ctx);
    }

    void Handle(const FederatedQuery::DescribeBindingResult& result, const TActorContext& ctx) {
        const auto& settings = result.binding().content().setting();

        const google::protobuf::RepeatedPtrField<Ydb::Column>* columns = nullptr;
        switch (settings.binding_case()) {
        case FederatedQuery::BindingSetting::kDataStreams: {
            columns = &settings.data_streams().schema().column();
            break;
        }
        case FederatedQuery::BindingSetting::kObjectStorage: {
            columns = &settings.object_storage().subset()[0].schema().column();
            break;
        }
        default:
            TString errorMsg = TStringBuilder{} << "binding " << result.binding().meta().id() << " got unexpected type: " <<
                static_cast<int>(settings.binding_case());
            SRC_LOG_I("failed: " << errorMsg);
            Reply(
                Ydb::StatusIds_StatusCode_INTERNAL_ERROR, errorMsg, NKikimrIssues::TIssuesIds::DEFAULT_ERROR, ctx);
            return;
        }

        Ydb::Table::DescribeTableResult response;
        auto& self = *response.mutable_self();
        self.set_name(BindingName_);
        self.set_type(Ydb::Scheme::Entry_Type::Entry_Type_TABLE);
        self.set_owner(result.binding().meta().created_by());

        *response.mutable_table_stats()->mutable_creation_time() = result.binding().meta().created_at();
        *response.mutable_table_stats()->mutable_modification_time() = result.binding().meta().modified_at();

        for (const auto& column : *columns) {
            auto& dstColumn = *response.add_columns();
            dstColumn.set_name(column.name());
            *dstColumn.mutable_type() = column.type();
        }

        ReplyWithResult(Ydb::StatusIds_StatusCode_SUCCESS, response, ctx);
    }

private:
    TString BindingName_;
};

std::function<void(std::unique_ptr<IRequestOpCtx>, const IFacilityProvider&)> GetDescribeTableExecutor(NActors::TActorId grpcProxyId) {
    return [grpcProxyId](std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
        f.RegisterActor(new DescribeTableRPC(p.release(), grpcProxyId));
    };
}

} // namespace NKikimr::NGRpcService::NYdbOverFq

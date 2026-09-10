#include "classic_grpc_service.h"

#include <ydb/core/nbs/nbs1_compat_api/cloud/blockstore/libs/service/service_method.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/context.h>

#include <ydb/core/nbs/nbs1_compat_api/cloud/storage/core/protos/request_source.pb.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>

#include <ydb/library/grpc/server/grpc_counters.h>
#include <ydb/library/grpc/server/grpc_request.h>

namespace NKikimr::NGRpcService {

    using namespace NYdb::NBS::NNbs1CompatApi::NBlockStore;

    ////////////////////////////////////////////////////////////////////////////////

    TClassicNbsGrpcService::TClassicNbsGrpcService(IBlockStorePtr blockStore)
        : BlockStore(std::move(blockStore))
    {
    }

    void TClassicNbsGrpcService::InitService(
        grpc::ServerCompletionQueue* cq,
        NYdbGrpc::TLoggerPtr logger)
    {
        SetupIncomingRequests(cq, std::move(logger));
    }

    template <typename TMethod>
    void TClassicNbsGrpcService::HandleRequest(
        NYdbGrpc::IRequestContextBase* requestContext)
    {
        using TRequest = typename TMethod::TRequest;
        using TResponse = typename TMethod::TResponse;

        const auto* typedRequest =
            static_cast<const TRequest*>(requestContext->GetRequest());
        auto request = std::make_shared<TRequest>(*typedRequest);

        if (request->GetHeaders().HasInternal()) {
            auto* response = google::protobuf::Arena::CreateMessage<TResponse>(
                requestContext->GetArena());
            *response->MutableError() = NYdb::NBS::MakeError(
                NYdb::NBS::E_ARGUMENT,
                "internal field should not be set by client");
            requestContext->Reply(response);
            return;
        }

        auto& internal = *request->MutableHeaders()->MutableInternal();
        internal.Clear();
        internal.SetRequestSource(
            NYdb::NBS::NNbs1CompatApi::NProto::SOURCE_INSECURE_CONTROL_CHANNEL);
        internal.SetPeer(requestContext->GetPeer());

        auto retainedRequestContext =
            TIntrusivePtr<NYdbGrpc::IRequestContextBase>(requestContext);
        auto future = TMethod::Execute(
            BlockStore.get(),
            MakeIntrusive<NYdb::NBS::NBlockStore::TCallContext>(
                request->GetHeaders().GetRequestId()),
            std::move(request));

        future.Subscribe(
            [requestContext = std::move(retainedRequestContext)](
                const auto& completedFuture) mutable {
                auto* response = google::protobuf::Arena::CreateMessage<TResponse>(
                    requestContext->GetArena());
                response->CopyFrom(completedFuture.GetValue());
                requestContext->Reply(response);
            });
    }

    void TClassicNbsGrpcService::SetupIncomingRequests(
        grpc::ServerCompletionQueue* cq,
        NYdbGrpc::TLoggerPtr logger)
    {
#ifdef SETUP_CLASSIC_NBS_METHOD
    #error SETUP_CLASSIC_NBS_METHOD macro already defined
#endif

        // TODO: Replace FakeCounterBlock with per-method counters on
        // enabling data-path methods.
#define SETUP_CLASSIC_NBS_METHOD(name, ...)                           \
    MakeIntrusive<NYdbGrpc::TGRpcRequest<                             \
        NProto::T##name##Request,                                     \
        NProto::T##name##Response,                                    \
        TClassicNbsGrpcService>>(                                     \
        this,                                                         \
        &Service_,                                                    \
        cq,                                                           \
        [this](NYdbGrpc::IRequestContextBase* requestContext) {       \
            HandleRequest<TBlockStore##name##Method>(requestContext); \
        },                                                            \
        &TGrpcAsyncService::Request##name,                            \
        #name,                                                        \
        logger,                                                       \
        NYdbGrpc::FakeCounterBlock())                                 \
        ->Run();

        NBS1_COMPAT_BLOCKSTORE_GRPC_SERVICE(SETUP_CLASSIC_NBS_METHOD)

#undef SETUP_CLASSIC_NBS_METHOD
    }

    ////////////////////////////////////////////////////////////////////////////////

} // namespace NKikimr::NGRpcService

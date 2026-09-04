#include "classic_grpc_service.h"

#include <ydb/core/nbs/cloud/blockstore/compat/libs/service/service_method.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/context.h>

#include <ydb/core/nbs/cloud/storage/core/compat/protos/request_source.pb.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>

#include <ydb/library/grpc/server/grpc_counters.h>
#include <ydb/library/grpc/server/grpc_request.h>

#include <library/cpp/string_utils/quote/quote.h>

namespace NKikimr::NGRpcService {

    using namespace NCloud::NBlockStore;

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
            NCloud::NProto::SOURCE_INSECURE_CONTROL_CHANNEL);
        internal.SetPeer(UrlUnescapeRet(requestContext->GetPeer()));

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

        BLOCKSTORE_GRPC_SERVICE(SETUP_CLASSIC_NBS_METHOD)

#undef SETUP_CLASSIC_NBS_METHOD
    }

    ////////////////////////////////////////////////////////////////////////////////

} // namespace NKikimr::NGRpcService

#include <util/stream/file.h>
#include <ydb/library/yql/utils/log/log.h>

#include "client.h"

namespace NYql::NConnector {
    template <class TResponse>
    class TStreamIteratorImpl: public IStreamIterator<TResponse> {
    public:
        TStreamIteratorImpl(std::shared_ptr<TStreamer<TResponse>> stream)
            : Streamer_(stream)
                  {};

        TAsyncResult<TResponse> ReadNext() {
            Y_ENSURE(!Streamer_->IsFinished(), "Attempt to read from finished stream");
            return Streamer_->ReadNext(Streamer_);
        }

    private:
        std::shared_ptr<TStreamer<TResponse>> Streamer_;
    };

    class TClientGRPC: public IClient {
    public:
        TClientGRPC() = delete;
        TClientGRPC(const TGenericConnectorConfig& config) {
            GrpcConfig_ = NYdbGrpc::TGRpcClientConfig();

            Y_ENSURE(config.GetEndpoint().host(), TStringBuilder() << "Empty host in TGenericConnectorConfig: " << config.DebugString());
            Y_ENSURE(config.GetEndpoint().port(), TStringBuilder() << "Empty port in TGenericConnectorConfig: " << config.DebugString());
            GrpcConfig_.Locator = TStringBuilder() << config.GetEndpoint().host() << ":" << config.GetEndpoint().port();
            GrpcConfig_.EnableSsl = config.GetUseSsl();
            GrpcConfig_.SslTargetNameOverride = "connector.yandex-query.cloud-preprod.yandex.net";

            YQL_CLOG(INFO, ProviderGeneric) << "Connector endpoint: " << (config.GetUseSsl() ? "grpcs" : "grpc") << "://" << GrpcConfig_.Locator;

            // Read content of CA cert
            TString rootCertData;
            if (config.GetSslCaCrt()) {
                rootCertData = TFileInput(config.GetSslCaCrt()).ReadAll();
            }

            GrpcConfig_.SslCredentials = grpc::SslCredentialsOptions{.pem_root_certs = rootCertData, .pem_private_key = "", .pem_cert_chain = ""};

            GrpcClient_ = std::make_unique<NYdbGrpc::TGRpcClientLow>();

            // FIXME: is it OK to use single connection during the client lifetime?
            GrpcConnection_ = GrpcClient_->CreateGRpcServiceConnection<NApi::Connector>(GrpcConfig_);
        }

        virtual TDescribeTableAsyncResult DescribeTable(const NApi::TDescribeTableRequest& request) override {
            return UnaryCall<NApi::TDescribeTableRequest, NApi::TDescribeTableResponse>(request, &NApi::Connector::Stub::AsyncDescribeTable);
        }

        virtual TListSplitsStreamIteratorAsyncResult ListSplits(const NApi::TListSplitsRequest& request) override {
            return ServerSideStreamingCall<NApi::TListSplitsRequest, NApi::TListSplitsResponse>(request, &NApi::Connector::Stub::AsyncListSplits);
        }

        virtual TReadSplitsStreamIteratorAsyncResult ReadSplits(const NApi::TReadSplitsRequest& request) override {
            return ServerSideStreamingCall<NApi::TReadSplitsRequest, NApi::TReadSplitsResponse>(request, &NApi::Connector::Stub::AsyncReadSplits);
        }

        ~TClientGRPC() {
            GrpcClient_->Stop(true);
        }

    private:
        template <class TService, class TRequest, class TResponse, template <typename TA, typename TB, typename TC> class TStream>
        using TStreamRpc =
            typename TStream<
                NApi::Connector::Stub,
                TRequest,
                TResponse>::TAsyncRequest;

        template <class TRequest, class TResponse>
        TAsyncResult<TResponse> UnaryCall(
            const TRequest& request,
            typename NYdbGrpc::TSimpleRequestProcessor<NApi::Connector::Stub, TRequest, TResponse>::TAsyncRequest rpc) {
            auto context = GrpcClient_->CreateContext();
            if (!context) {
                throw yexception() << "Client is being shutted down";
            }

            auto promise = NThreading::NewPromise<TResult<TResponse>>();
            auto callback = [promise, context](NYdbGrpc::TGrpcStatus&& status, TResponse&& resp) mutable {
                promise.SetValue({std::move(status), std::move(resp)});
            };

            GrpcConnection_->DoRequest<TRequest, TResponse>(
                std::move(request),
                std::move(callback),
                rpc,
                {},
                context.get());

            return promise.GetFuture();
        }

        template <class TRequest, class TResponse>
        TIteratorAsyncResult<IStreamIterator<TResponse>> ServerSideStreamingCall(
            const TRequest& request,
            TStreamRpc<NApi::Connector::Stub, TRequest, TResponse, NYdbGrpc::TStreamRequestReadProcessor> rpc) {
            using TStreamProcessorPtr = typename NYdbGrpc::IStreamRequestReadProcessor<TResponse>::TPtr;
            using TStreamInitResult = std::pair<NYdbGrpc::TGrpcStatus, TStreamProcessorPtr>;

            auto promise = NThreading::NewPromise<TStreamInitResult>();

            auto context = GrpcClient_->CreateContext();
            if (!context) {
                throw yexception() << "Client is being shutted down";
            }

            GrpcConnection_->DoStreamRequest<TRequest, TResponse>(
                request,
                [context, promise](NYdbGrpc::TGrpcStatus&& status, TStreamProcessorPtr streamProcessor) mutable {
                    promise.SetValue({std::move(status), streamProcessor});
                },
                rpc,
                {},
                context.get());

            // TODO: async handling YQ-2513
            auto result = promise.GetFuture().GetValueSync();

            auto status = result.first;
            auto streamProcessor = result.second;

            if (streamProcessor) {
                auto it = std::make_shared<TStreamIteratorImpl<TResponse>>(std::make_shared<TStreamer<TResponse>>(std::move(streamProcessor)));
                return NThreading::MakeFuture<TIteratorResult<IStreamIterator<TResponse>>>({std::move(status), std::move(it)});
            }

            return NThreading::MakeFuture<TIteratorResult<IStreamIterator<TResponse>>>({std::move(status), nullptr});
        }

    private:
        NYdbGrpc::TGRpcClientConfig GrpcConfig_;
        std::unique_ptr<NYdbGrpc::TGRpcClientLow> GrpcClient_;
        std::shared_ptr<NYdbGrpc::TServiceConnection<NApi::Connector>> GrpcConnection_;
    };

    IClient::TPtr MakeClientGRPC(const NYql::TGenericConnectorConfig& cfg) {
        return std::make_shared<TClientGRPC>(cfg);
    }
}

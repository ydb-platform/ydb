#include <util/stream/file.h>
#include <yql/essentials/utils/log/log.h>

#include "client.h"

namespace NYql::NConnector {

    template<typename T>
    class IConnectionFactory {
    public:
        virtual ~IConnectionFactory() = default;

        virtual std::shared_ptr<NYdbGrpc::TServiceConnection<T>> Create() = 0;
    };

    ///
    /// Connection Factory which always returns an exact connection
    ///
    template<typename T>
    class SingleConnectionFactory final : public IConnectionFactory<T> {
    public:
        SingleConnectionFactory(std::shared_ptr<NYdbGrpc::TGRpcClientLow> Client, const NYdbGrpc::TGRpcClientConfig & GrpcConfig )
            : GrpcConnection_(Client->CreateGRpcServiceConnection<T>(GrpcConfig))
        {
        }
        SingleConnectionFactory(
            std::shared_ptr<NYdbGrpc::TGRpcClientLow> Client,
            const NYdbGrpc::TGRpcClientConfig & GrpcConfig,
            const NYdbGrpc::TTcpKeepAliveSettings & KeepAlive
        )
            : GrpcConnection_(Client->CreateGRpcServiceConnection<T>(GrpcConfig, KeepAlive))
        {
        }

    public:
        std::shared_ptr<NYdbGrpc::TServiceConnection<T>> Create() override {
            return GrpcConnection_;
        }

    private:
        std::shared_ptr<NYdbGrpc::TServiceConnection<T>> GrpcConnection_;
    };

    ///
    /// Connection Factory which returns a new connection on each Create
    ///
    template<typename T>
    class NewOnEachRequestConnectionFactory final: public IConnectionFactory<T> {
    public:
        NewOnEachRequestConnectionFactory(
            std::shared_ptr<NYdbGrpc::TGRpcClientLow> Client,
            const NYdbGrpc::TGRpcClientConfig & GrpcConfig
        )
            : Client_(Client)
            , GrpcConfig_(GrpcConfig)
            , KeepAlive_({})
            , KeepAliveIsSet_(false)
        {
        }

        NewOnEachRequestConnectionFactory(
            std::shared_ptr<NYdbGrpc::TGRpcClientLow> Client,
            const NYdbGrpc::TGRpcClientConfig & GrpcConfig,
            const NYdbGrpc::TTcpKeepAliveSettings & KeepAlive
        )
            : Client_(Client)
            , GrpcConfig_(GrpcConfig)
            , KeepAlive_(KeepAlive)
            , KeepAliveIsSet_(true)
        {
        }

    public:
        ///
        /// Creates a new connection for each method call. It is safe to dispose
        /// connection after making request. All necessary data will be held in a
        /// request object (@sa TGRpcRequestProcessorCommon's descedants).
        ///
        /// Hence the call Create()->DoRequest(...) is absolutely legitimate.
        ///
        std::shared_ptr<NYdbGrpc::TServiceConnection<T>> Create() override {
            auto r = KeepAliveIsSet_ ?
                Client_->CreateGRpcServiceConnection<T>(GrpcConfig_, KeepAlive_)
                : Client_->CreateGRpcServiceConnection<T>(GrpcConfig_);

            return std::move(r);
        }

    private:
        std::shared_ptr<NYdbGrpc::TGRpcClientLow> Client_;
        const NYdbGrpc::TGRpcClientConfig GrpcConfig_;
        const NYdbGrpc::TTcpKeepAliveSettings KeepAlive_;
        const bool KeepAliveIsSet_;
    };

    ///
    /// TODO: PooledConnectionFactory. Factory which returns a connection from a pool
    ///

    /*

    struct TConnectorMetrics {
        explicit TConnectorMetrics(const ::NMonitoring::TDynamicCounterPtr& counters)
            : Counters(counters)
        {
            GrpcChannelCount = Counters->GetCounter("GrpChannelCount", true);
        }

        void GrpcChannelCreated() {
            GrpcChannelCount->Inc();
        }

        void GrpcChannelDestroyed() {
            GrpcChannelCount->Dec();
        }

        ::NMonitoring::TDynamicCounterPtr Counters;
        ::NMonitoring::TDynamicCounters::TCounterPtr GrpcChannelCount;
    };
    
    */

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

    TListSplitsStreamIteratorDrainer::TPtr MakeListSplitsStreamIteratorDrainer(IListSplitsStreamIterator::TPtr&& iterator) {
        return std::make_shared<TListSplitsStreamIteratorDrainer>(std::move(iterator));
    }

    TReadSplitsStreamIteratorDrainer::TPtr MakeReadSplitsStreamIteratorDrainer(IReadSplitsStreamIterator::TPtr&& iterator) {
        return std::make_shared<TReadSplitsStreamIteratorDrainer>(std::move(iterator));
    }

    class TClientGRPC: public IClient {
    public:
        TClientGRPC(const TGenericConnectorConfig& config) {
            GrpcClient_ = std::make_shared<NYdbGrpc::TGRpcClientLow>();
            auto grpcConfig = ConnectorConfigToGrpcConfig(config);

            auto keepAlive = NYdbGrpc::TTcpKeepAliveSettings {
                // TODO configure hardcoded values
                .Enabled = true,
                .Idle = 30,
                .Count = 5,
                .Interval = 10
            };

            // TODO: 
            // 1. Add config parameter to TGenericConnectorConfig to choose factory 
            // 2. Add support for multiple connector's config 
            
            // Old way of working - Single Connection
            // ConnectionFactory_ = std::make_unique<SingleConnectionFactory<NApi::Connector>>(
            //     GrpcClient_, grpcConfig, keepAlive);

            ConnectionFactory_ = std::make_unique<NewOnEachRequestConnectionFactory<NApi::Connector>>(
                GrpcClient_, grpcConfig, keepAlive);
        }

        ~TClientGRPC() {
            GrpcClient_->Stop(true);
        }

        virtual TDescribeTableAsyncResult DescribeTable(const NApi::TDescribeTableRequest& request, TDuration timeout = {}) override {
            auto kind = request.Getdata_source_instance().Getkind();
            auto promise = NThreading::NewPromise<TResult<NApi::TDescribeTableResponse>>();
            
            auto callback = [promise](NYdbGrpc::TGrpcStatus&& status, NApi::TDescribeTableResponse && resp) mutable {
                promise.SetValue({std::move(status), std::move(resp)});
            };

            GetConnection(kind)->DoRequest<NApi::TDescribeTableRequest, NApi::TDescribeTableResponse>(
                std::move(request),
                std::move(callback),
                &NApi::Connector::Stub::AsyncDescribeTable,
                { .Timeout = timeout }
            );

            return promise.GetFuture();
        }

        virtual TListSplitsStreamIteratorAsyncResult ListSplits(const NApi::TListSplitsRequest& request, TDuration timeout = {}) override {
            // The request can holds many selects, but we make assumption that all of then go to one connector.
            // Do we need to check that all parameters in a request are set ? 
            auto kind = request.Getselects().at(0).data_source_instance().kind();

            return ServerSideStreamingCall<NApi::TListSplitsRequest, NApi::TListSplitsResponse>(
                kind, request, &NApi::Connector::Stub::AsyncListSplits, timeout);
        }

        virtual TReadSplitsStreamIteratorAsyncResult ReadSplits(const NApi::TReadSplitsRequest& request, TDuration timeout = {}) override {
            // The request can holds many splits, but we make assumption that all of then go to one connector.
             // Do we need to check that all parameters in a request are set ? 
            auto kind = request.Getsplits().at(0).select().data_source_instance().kind();

            return ServerSideStreamingCall<NApi::TReadSplitsRequest, NApi::TReadSplitsResponse>(
                kind, request, &NApi::Connector::Stub::AsyncReadSplits, timeout);
        }

    private:
        std::shared_ptr<NYdbGrpc::TServiceConnection<NApi::Connector>> GetConnection(const NYql::EGenericDataSourceKind &) {
            // TODO: choose appropriate connection factory by data source kind 
            return ConnectionFactory_->Create();
        }

        template <
            typename I,
            typename O,
            typename F = typename NYdbGrpc::TStreamRequestReadProcessor<NApi::Connector::Stub, I, O>::TAsyncRequest
        >
        TIteratorAsyncResult<IStreamIterator<O>> ServerSideStreamingCall(const NYql::EGenericDataSourceKind & kind, const I & request, F rpc, TDuration timeout = {}) {
            auto promise = NThreading::NewPromise<TIteratorResult<IStreamIterator<O>>>();

            auto callback = [promise]( NYdbGrpc::TGrpcStatus && status, NYdbGrpc::IStreamRequestReadProcessor<O>::TPtr streamProcessor) mutable {
                if (!streamProcessor) {
                    promise.SetValue({std::move(status), nullptr});
                    return;
                } 
                
                auto processor = std::make_shared<TStreamer<O>>(std::move(streamProcessor));
                auto iterator = std::make_shared<TStreamIteratorImpl<O>>(processor);
                promise.SetValue({std::move(status), std::move(iterator)});
            };

            GetConnection(kind)->DoStreamRequest<I, O>(
                std::move(request),
                std::move(callback),
                rpc,
                { .Timeout = timeout }
            );

            return promise.GetFuture();
        }

        NYdbGrpc::TGRpcClientConfig ConnectorConfigToGrpcConfig(const TGenericConnectorConfig& config) {
            auto cfg = NYdbGrpc::TGRpcClientConfig();

            Y_ENSURE(config.GetEndpoint().host(), TStringBuilder() << "Empty host in TGenericConnectorConfig: " << config.DebugString());
            Y_ENSURE(config.GetEndpoint().port(), TStringBuilder() << "Empty port in TGenericConnectorConfig: " << config.DebugString());

            cfg.Locator = TStringBuilder() << config.GetEndpoint().host() << ":" << config.GetEndpoint().port();
            cfg.EnableSsl = config.GetUseSsl();

            YQL_CLOG(INFO, ProviderGeneric) << "Connector endpoint: " << (config.GetUseSsl() ? "grpcs" : "grpc") << "://" << cfg.Locator;

            // Read content of CA cert
            TString rootCertData;

            if (config.GetSslCaCrt()) {
                rootCertData = TFileInput(config.GetSslCaCrt()).ReadAll();
            }

            cfg.SslCredentials = grpc::SslCredentialsOptions{.pem_root_certs = rootCertData, .pem_private_key = "", .pem_cert_chain = ""};
            return cfg;
        }

    private:
        std::shared_ptr<NYdbGrpc::TGRpcClientLow> GrpcClient_;
        std::unique_ptr<IConnectionFactory<NApi::Connector>> ConnectionFactory_;
    };

    IClient::TPtr MakeClientGRPC(const NYql::TGenericConnectorConfig& cfg) {
        return std::make_shared<TClientGRPC>(cfg);
    }

} // namespace NYql::NConnector

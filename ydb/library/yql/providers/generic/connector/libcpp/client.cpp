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
    class TSingleConnectionFactory final : public IConnectionFactory<T> {
    public:
        TSingleConnectionFactory(std::shared_ptr<NYdbGrpc::TGRpcClientLow> Client, const NYdbGrpc::TGRpcClientConfig & GrpcConfig )
            : GrpcConnection_(Client->CreateGRpcServiceConnection<T>(GrpcConfig))
        {
        }

        TSingleConnectionFactory(
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
    class TNewOnEachRequestConnectionFactory final: public IConnectionFactory<T> {
    public:
        TNewOnEachRequestConnectionFactory(
            std::shared_ptr<NYdbGrpc::TGRpcClientLow> Client,
            const NYdbGrpc::TGRpcClientConfig & GrpcConfig
        )
            : Client_(Client)
            , GrpcConfig_(GrpcConfig)
        {
        }

        TNewOnEachRequestConnectionFactory(
            std::shared_ptr<NYdbGrpc::TGRpcClientLow> Client,
            const NYdbGrpc::TGRpcClientConfig & GrpcConfig,
            const NYdbGrpc::TTcpKeepAliveSettings & KeepAlive
        )
            : Client_(Client)
            , GrpcConfig_(GrpcConfig)
            , KeepAlive_(KeepAlive)
        {
        }

    public:
        ///
        /// Creates a new connection for each method call. It is safe to dispose
        /// connection after making request. All necessary data will be held in a
        /// request object (@sa TGRpcRequestProcessorCommon's descendants).
        ///
        /// Hence the call Create()->DoRequest(...) is absolutely legitimate.
        ///
        std::shared_ptr<NYdbGrpc::TServiceConnection<T>> Create() override {
            auto r = KeepAlive_ ?
                Client_->CreateGRpcServiceConnection<T>(GrpcConfig_, *KeepAlive_)
                : Client_->CreateGRpcServiceConnection<T>(GrpcConfig_);

            return std::move(r);
        }

    private:
        std::shared_ptr<NYdbGrpc::TGRpcClientLow> Client_;
        const NYdbGrpc::TGRpcClientConfig GrpcConfig_;
        const std::optional<NYdbGrpc::TTcpKeepAliveSettings> KeepAlive_;
    };

    ///
    ///  Experimental Pool Factory (do not use it in a prod)
    ///  which returns a connection from a pool.
    ///
    ///  TODO:
    ///  1. Ttl for connections.
    ///  2. Check if a connection in a pool is broken and replace it.
    ///  3. Need to choose balancing strategy.
    ///
    template<typename T>
    class TFixedPoolConnectionFactory final: public IConnectionFactory<T> {
    public:
        TFixedPoolConnectionFactory(
            int size,
            std::shared_ptr<NYdbGrpc::TGRpcClientLow> Client,
            const NYdbGrpc::TGRpcClientConfig & GrpcConfig
        )
            : Client_(Client)
            , GrpcConfig_(GrpcConfig)
        {
            init(size);
        }

        TFixedPoolConnectionFactory(
            int size,
            std::shared_ptr<NYdbGrpc::TGRpcClientLow> Client,
            const NYdbGrpc::TGRpcClientConfig & GrpcConfig,
            const NYdbGrpc::TTcpKeepAliveSettings & KeepAlive
        )
            : Client_(Client)
            , GrpcConfig_(GrpcConfig)
            , KeepAlive_(KeepAlive)
        {
            init(size);
        }
    private:
        void init(int size) {
            for(int i = 0; i < size; ++i) {
                auto r = KeepAlive_ ?
                    Client_->CreateGRpcServiceConnection<T>(GrpcConfig_, *KeepAlive_)
                    : Client_->CreateGRpcServiceConnection<T>(GrpcConfig_);

                Pool_.push_back(std::move(r));
            }

            Size_ = size;
        }

    public:
        std::shared_ptr<NYdbGrpc::TServiceConnection<T>> Create() override {
            // Round Robin Balancing
            auto n = NextSlot_++;
            auto r = Pool_.at(n % Size_);
            auto expectedVal = n + 1;

            NextSlot_.compare_exchange_weak(expectedVal, expectedVal % Size_);
            return r;
        }
    private:
        int Size_;
        std::atomic_long NextSlot_;
        std::shared_ptr<NYdbGrpc::TGRpcClientLow> Client_;
        const NYdbGrpc::TGRpcClientConfig GrpcConfig_;
        const std::optional<NYdbGrpc::TTcpKeepAliveSettings> KeepAlive_;
        std::vector<std::shared_ptr<NYdbGrpc::TServiceConnection<T>>> Pool_;
    };

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
            GrpcClient_ = std::make_shared<NYdbGrpc::TGRpcClientLow>(20);
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
            ConnectionFactory_ = std::make_unique<TNewOnEachRequestConnectionFactory<NApi::Connector>>(
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
            // The request can hold many selects, but we make assumption that all of them go to the same connector.
            // Do we need to check that all parameters in a request are set ? 
            auto kind = request.Getselects().at(0).data_source_instance().kind();

            return ServerSideStreamingCall<NApi::TListSplitsRequest, NApi::TListSplitsResponse>(
                kind, request, &NApi::Connector::Stub::AsyncListSplits, timeout);
        }

        virtual TReadSplitsStreamIteratorAsyncResult ReadSplits(const NApi::TReadSplitsRequest& request, TDuration timeout = {}) override {
            // The request can hold many splits, but we make assumption that all of them go to the same connector.
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

        /// 
        /// @brief Make async request to a connector with a streaming response in a Future.
        /// 
        /// @tparam TmplRequest         Type of a request, e.g.: "NApi::TListSplitsRequest"
        /// @tparam TmplResponse        Type of a data in a Response Stream, e.g.: "NApi::TListSplitsResponse" 
        /// @tparam TmplRpcCallback     Definition of a callback on the "NApi::Connector::Stub" that takes "TmplRequest" as an input arg and  
        ///                             returns "NYdbGrpc::TStreamRequestReadProcessor" with "TmplResponse" as stream's data
        ///
        /// @param[in] kind             Datasource's kind, it is a key to choose which connector will be queried 
        /// @param[in] request          Request's data 
        /// @param[in] rpc              Method on a Stub which will be executed 
        /// @param[in] timeout          How long to wait a response 
        ///
        /// @return                     Future that provides with a streaming response 
        ///
        template <
            typename TmplRequest,
            typename TmplResponse,
            typename TmplRpcCallback = typename NYdbGrpc::TStreamRequestReadProcessor<NApi::Connector::Stub, TmplRequest, TmplResponse>::TAsyncRequest
        >
        TIteratorAsyncResult<IStreamIterator<TmplResponse>> ServerSideStreamingCall(
            const NYql::EGenericDataSourceKind & kind, const TmplRequest & request, TmplRpcCallback rpc, TDuration timeout = {}) {
            auto promise = NThreading::NewPromise<TIteratorResult<IStreamIterator<TmplResponse>>>();

            auto callback = [promise](NYdbGrpc::TGrpcStatus && status, NYdbGrpc::IStreamRequestReadProcessor<TmplResponse>::TPtr streamProcessor) mutable {
                if (!streamProcessor) {
                    promise.SetValue({std::move(status), nullptr});
                    return;
                } 
                
                auto processor = std::make_shared<TStreamer<TmplResponse>>(std::move(streamProcessor));
                auto iterator = std::make_shared<TStreamIteratorImpl<TmplResponse>>(processor);
                promise.SetValue({std::move(status), std::move(iterator)});
            };

            GetConnection(kind)->DoStreamRequest<TmplRequest, TmplResponse>(
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

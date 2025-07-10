#include <util/stream/file.h>
#include <yql/essentials/utils/log/log.h>
#include <ydb/library/yql/providers/generic/connector/api/service/connector.grpc.pb.h>
#include <ydb/library/yql/providers/generic/connector/api/service/protos/connector.pb.h>

#include "client.h"

namespace NYql::NConnector {

    const size_t DEFAULT_CONNECTION_MANAGER_NUM_THREADS = 20;

    template<typename T>
    class IConnectionFactory {
    public:
        virtual ~IConnectionFactory() = default;

        virtual std::shared_ptr<NYdbGrpc::TServiceConnection<T>> Create() = 0;
    };

    ///
    /// Connection Factory which always returns an exact connection
    ///
    /// TODO: check connection healthiness
    ///
    template<typename T>
    class TSingleConnectionFactory final : public IConnectionFactory<T> {
    public:
        TSingleConnectionFactory(std::shared_ptr<NYdbGrpc::TGRpcClientLow> client, const NYdbGrpc::TGRpcClientConfig& grpcConfig)
            : GrpcConnection_(client->CreateGRpcServiceConnection<T>(grpcConfig))
        {
        }

        TSingleConnectionFactory(
            std::shared_ptr<NYdbGrpc::TGRpcClientLow> client,
            const NYdbGrpc::TGRpcClientConfig& grpcConfig,
            const NYdbGrpc::TTcpKeepAliveSettings& keepAlive
        )
            : GrpcConnection_(client->CreateGRpcServiceConnection<T>(grpcConfig, keepAlive))
        {
        }

    public:
        std::shared_ptr<NYdbGrpc::TServiceConnection<T>> Create() override {
            return GrpcConnection_;
        }

    private:
        const std::shared_ptr<NYdbGrpc::TServiceConnection<T>> GrpcConnection_;
    };

    ///
    /// Connection Factory which returns a new connection on each Create
    ///
    template<typename T>
    class TNewOnEachRequestConnectionFactory final: public IConnectionFactory<T> {
    public:
        TNewOnEachRequestConnectionFactory(
            std::shared_ptr<NYdbGrpc::TGRpcClientLow> client,
            const NYdbGrpc::TGRpcClientConfig& grpcConfig
        )
            : Client_(client)
            , GrpcConfig_(grpcConfig)
        {
        }

        TNewOnEachRequestConnectionFactory(
            std::shared_ptr<NYdbGrpc::TGRpcClientLow> client,
            const NYdbGrpc::TGRpcClientConfig& grpcConfig,
            const NYdbGrpc::TTcpKeepAliveSettings& keepAlive
        )
            : Client_(client)
            , GrpcConfig_(grpcConfig)
            , KeepAlive_(keepAlive)
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
        const std::shared_ptr<NYdbGrpc::TGRpcClientLow> Client_;
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
            std::shared_ptr<NYdbGrpc::TGRpcClientLow> client,
            const NYdbGrpc::TGRpcClientConfig& grpcConfig
        )
            : Client_(client)
            , GrpcConfig_(grpcConfig)
        {
            Init(size);
        }

        TFixedPoolConnectionFactory(
            int size,
            std::shared_ptr<NYdbGrpc::TGRpcClientLow> client,
            const NYdbGrpc::TGRpcClientConfig& grpcConfig,
            const NYdbGrpc::TTcpKeepAliveSettings& keepAlive
        )
            : Client_(client)
            , GrpcConfig_(grpcConfig)
            , KeepAlive_(keepAlive)
        {
            Init(size);
        }
    private:
        void Init(int size) {
            for (int i = 0; i < size; ++i) {
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
        const std::shared_ptr<NYdbGrpc::TGRpcClientLow> Client_;
        const NYdbGrpc::TGRpcClientConfig GrpcConfig_;
        const std::optional<NYdbGrpc::TTcpKeepAliveSettings> KeepAlive_;
        std::vector<std::shared_ptr<NYdbGrpc::TServiceConnection<T>>> Pool_;
    };

    ///
    ///  Create a factory based on a it's type @sa NYql::EConnectionFactory
    ///
    std::unique_ptr<IConnectionFactory<NApi::Connector>> CreateFactoryForConnector(
        NYql::EConnectionFactory factory,
        std::shared_ptr<NYdbGrpc::TGRpcClientLow> client,
        const NYdbGrpc::TGRpcClientConfig& grpcConfig,
        const NYdbGrpc::TTcpKeepAliveSettings& keepAlive) {

        switch (factory) {
            case SINGLE:
                return std::make_unique<TSingleConnectionFactory<NApi::Connector>>(
                    client, grpcConfig, keepAlive);

            case NEW_FOR_EACH_REQUEST:
            default:
                return std::make_unique<TNewOnEachRequestConnectionFactory<NApi::Connector>>(
                    client, grpcConfig, keepAlive);
        }
    }

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

    TListSplitsStreamIteratorDrainer::TPtr MakeListSplitsStreamIteratorDrainer(IStreamIterator<NApi::TListSplitsResponse>::TPtr&& iterator) {
        return std::make_shared<TListSplitsStreamIteratorDrainer>(std::move(iterator));
    }

    TReadSplitsStreamIteratorDrainer::TPtr MakeReadSplitsStreamIteratorDrainer(IReadSplitsStreamIterator::TPtr&& iterator) {
        return std::make_shared<TReadSplitsStreamIteratorDrainer>(std::move(iterator));
    }

    class TClientGRPC: public IClient {
    public:
        explicit TClientGRPC(const TGenericGatewayConfig& config) {
            Init(config);
        }

        ~TClientGRPC() {
            GrpcClient_->Stop(true);
        }

        virtual TDescribeTableAsyncResult DescribeTable(const NApi::TDescribeTableRequest& request, TDuration timeout = {}) override {
            auto kind = request.Getdata_source_instance().Getkind();
            auto promise = NThreading::NewPromise<TResult<NApi::TDescribeTableResponse>>();

            if (!request.has_data_source_instance()
                || !request.data_source_instance().has_kind()) {
                auto msg =  TStringBuilder() << "DescribeTable request is invalid: either data source or kind is missing";
                auto status = NYdbGrpc::TGrpcStatus(grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,msg));

                YQL_CLOG(WARN, ProviderGeneric) << msg;
                promise.SetValue({std::move(status), std::move(NApi::TDescribeTableResponse())});
                return promise.GetFuture();
            }

            auto context = CreateClientContext();

            auto callback = [context, promise](NYdbGrpc::TGrpcStatus&& status, NApi::TDescribeTableResponse&& resp) mutable {
                promise.SetValue({std::move(status), std::move(resp)});
            };

            GetConnection(kind)->DoRequest<NApi::TDescribeTableRequest, NApi::TDescribeTableResponse>(
                std::move(request),
                std::move(callback),
                &NApi::Connector::Stub::AsyncDescribeTable,
                { .Timeout = timeout },
                context.get()
            );

            return promise.GetFuture();
        }

        virtual TListSplitsStreamIteratorAsyncResult ListSplits(const NApi::TListSplitsRequest& request, TDuration timeout = {}) override {
            auto selects = request.Getselects();

            if (selects.empty()
                || !selects.at(0).has_data_source_instance()
                || !selects.at(0).data_source_instance().has_kind()) {
                auto msg =  TStringBuilder() << "ListSplits request is invalid: either selects is empty or data source or kind is missing";

                YQL_CLOG(WARN, ProviderGeneric) << msg;
                return DoEmptyStreamResponse<NApi::TListSplitsResponse>(grpc::StatusCode::INVALID_ARGUMENT, msg);
            }

            // The request can hold many selects, but we make assumption that all of them go to the same connector.
            auto kind = selects.at(0).data_source_instance().kind();

            return ServerSideStreamingCall<NApi::TListSplitsRequest, NApi::TListSplitsResponse>(
                kind, request, &NApi::Connector::Stub::AsyncListSplits, timeout);
        }

        TReadSplitsStreamIteratorAsyncResult ReadSplits(const NApi::TReadSplitsRequest& request, TDuration timeout = {}) override {
            auto splits = request.Getsplits();

            if (splits.empty()
                || !splits.at(0).has_select()
                || !splits.at(0).select().has_data_source_instance()
                || !splits.at(0).select().data_source_instance().has_kind()) {
                auto msg = TStringBuilder() << "ReadSplits request is invalid: either splits or select is empty or data source or kind is missing";

                YQL_CLOG(WARN, ProviderGeneric) << msg;
                return DoEmptyStreamResponse<NApi::TReadSplitsResponse>(grpc::StatusCode::INVALID_ARGUMENT, msg);
            }

            // The request can hold many splits, but we make assumption that all of them go to the same connector.
            auto kind = splits.at(0).select().data_source_instance().kind();

            return ServerSideStreamingCall<NApi::TReadSplitsRequest, NApi::TReadSplitsResponse>(
                kind, request, &NApi::Connector::Stub::AsyncReadSplits, timeout);
        }

    private:
        NYdbGrpc::IQueueClientContextPtr CreateClientContext() {
            auto context = GrpcClient_->CreateContext();

            if (!context) {
                throw yexception() << "Client is being shut down";
            }

            return context;
        }

        void Init(const TGenericGatewayConfig& config) {
            // TODO: place in a config file ?
            GrpcClient_ = std::make_shared<NYdbGrpc::TGRpcClientLow>(DEFAULT_CONNECTION_MANAGER_NUM_THREADS);

            auto keepAlive = NYdbGrpc::TTcpKeepAliveSettings {
                // TODO: configure hardcoded values
                .Enabled = true,
                .Idle = 30,
                .Count = 5,
                .Interval = 10
            };

            size_t count = 0;
            auto cfg = ConnectorConfigToGrpcConfig(
                config.GetConnector(), count++);

            DefaultFactory_ = CreateFactoryForConnector(
                config.GetConnector().GetFactory(), GrpcClient_, cfg, keepAlive);

            for (auto c : config.GetConnectors()) {
                auto cfg = ConnectorConfigToGrpcConfig(c, count++);
                std::shared_ptr<IConnectionFactory<NApi::Connector>> f
                    = CreateFactoryForConnector(c.GetFactory(), GrpcClient_, cfg, keepAlive);

                for (auto k : c.GetForKinds()) {
                    if (!FactoryForKind_.try_emplace(NYql::EGenericDataSourceKind(k), f).second) {
                        throw yexception()
                            << "Duplicate connector is provided for the kind: "
                            << EGenericDataSourceKind_Name(k);
                    }
                }
            }
        }

        std::shared_ptr<NYdbGrpc::TServiceConnection<NApi::Connector>> GetConnection(NYql::EGenericDataSourceKind kind) const {
            auto itr = FactoryForKind_.find(kind);

            return FactoryForKind_.end() == itr ?
                DefaultFactory_->Create() : itr->second->Create();
        }

        template<typename TResponse>
        TIteratorAsyncResult<IStreamIterator<TResponse>> DoEmptyStreamResponse(const grpc::StatusCode& code, TString msg) const {
            auto promise = NThreading::NewPromise<TIteratorResult<IStreamIterator<TResponse>>>();
            auto status = NYdbGrpc::TGrpcStatus(grpc::Status(code, msg));

            promise.SetValue({std::move(status), nullptr});
            return promise.GetFuture();
        }

        ///
        /// @brief Make async request to a connector with a streaming response in a Future.
        ///
        /// @tparam TRequest        Type of a request, e.g.: "NApi::TListSplitsRequest"
        /// @tparam TResponse       Type of a data in a Response Stream, e.g.: "NApi::TListSplitsResponse"
        /// @tparam TRpcCallback    Definition of a callback on the "NApi::Connector::Stub" that takes "TRequest" as an input arg and
        ///                         returns "NYdbGrpc::TStreamRequestReadProcessor" with "TResponse" as stream's data
        ///
        /// @param[in] kind         Datasource's kind, it is a key to choose which connector will be queried
        /// @param[in] request      Request's data
        /// @param[in] rpc          Method on a Stub which will be executed
        /// @param[in] timeout      How long to wait a response
        ///
        /// @return                 Future that provides with a streaming response
        ///
        template <
            typename TRequest,
            typename TResponse,
            typename TRpcCallback = typename NYdbGrpc::TStreamRequestReadProcessor<NApi::Connector::Stub, TRequest, TResponse>::TAsyncRequest
        >
        TIteratorAsyncResult<IStreamIterator<TResponse>> ServerSideStreamingCall(
            const NYql::EGenericDataSourceKind& kind, const TRequest& request, TRpcCallback rpc, TDuration timeout = {}) {
            auto promise = NThreading::NewPromise<TIteratorResult<IStreamIterator<TResponse>>>();
            auto context = CreateClientContext();

            auto callback = [context, promise](NYdbGrpc::TGrpcStatus&& status, NYdbGrpc::IStreamRequestReadProcessor<TResponse>::TPtr streamProcessor) mutable {
                if (!streamProcessor) {
                    promise.SetValue({std::move(status), nullptr});
                    return;
                }

                auto processor = std::make_shared<TStreamer<TResponse>>(std::move(streamProcessor));
                auto iterator = std::make_shared<TStreamIteratorImpl<TResponse>>(processor);
                promise.SetValue({std::move(status), std::move(iterator)});
            };

            GetConnection(kind)->DoStreamRequest<TRequest, TResponse>(
                std::move(request),
                std::move(callback),
                rpc,
                { .Timeout = timeout },
                context.get()
            );

            return promise.GetFuture();
        }

        NYdbGrpc::TGRpcClientConfig ConnectorConfigToGrpcConfig(const TGenericConnectorConfig& config, size_t order) const {
            auto cfg = NYdbGrpc::TGRpcClientConfig();

            // Connector's name. If order equals to zero, it means that the config belongs "TGenericGatewayConfig.Connector"
            // (default connector); otherwise, it is from "TGenericGatewayConfig.ConnectorS"
            auto name = TStringBuilder()
                << "Connector[" << (order == 0 ? TString("default") : TStringBuilder() << (order - 1)) << "]";

            Y_ENSURE(config.GetEndpoint().host(), TStringBuilder() << "Empty host in " << name << ": " << config.DebugString());
            Y_ENSURE(config.GetEndpoint().port(), TStringBuilder() << "Empty port in " << name << ": " << config.DebugString());

            cfg.Locator = TStringBuilder() << config.GetEndpoint().host() << ":" << config.GetEndpoint().port();
            cfg.EnableSsl = config.GetUseSsl();

            YQL_CLOG(INFO, ProviderGeneric) << name << " endpoint: "
                << (config.GetUseSsl() ? "grpcs" : "grpc") << "://" << cfg.Locator;

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
        std::unique_ptr<IConnectionFactory<NApi::Connector>> DefaultFactory_;
        std::unordered_map<NYql::EGenericDataSourceKind,
            std::shared_ptr<IConnectionFactory<NApi::Connector>>> FactoryForKind_;
    };

    IClient::TPtr MakeClientGRPC(const ::NYql::TGenericGatewayConfig& cfg) {
        if (!cfg.HasConnector()) {
            throw yexception()
                << "TGenericGatewayConfig.Connector is empty. "
                << "In order to create a ClientGRPC it has to be set";
        }

        return std::make_shared<TClientGRPC>(cfg);
    }

} // namespace NYql::NConnector

#include "grpc_client.h"

#include <ydb/core/protos/grpc.grpc.pb.h>

#include <util/system/thread.h>
#include <util/system/mutex.h>
#include <util/generic/maybe.h>
#include <util/generic/queue.h>
#include <util/generic/set.h>
#include <grpc++/grpc++.h>

namespace NKikimr {
    namespace NGRpcProxy {

        class TGRpcClient::TImpl : ISimpleThread {
            struct IProcessorBase {
                virtual ~IProcessorBase() = default;
                virtual void Start() = 0;
            };

            struct IRequestProcessor : public IProcessorBase
            {
                virtual void Finished() = 0;
            };

            struct IStreamRequestReadProcessor : public IProcessorBase
            {
                virtual void InvokeProcess() = 0;
                virtual void InvokeFinish() = 0;
            };

        public:
            using TStub = NKikimrClient::TGRpcServer::Stub;

        private:
            template<typename TRequest, typename TResponse>
            class TRequestProcessor
                : public IRequestProcessor
            {
            public:
                using TAsyncReaderPtr = std::unique_ptr<grpc::ClientAsyncResponseReader<TResponse>>;
                using TAsyncRequest = TAsyncReaderPtr (TStub::*)(grpc::ClientContext*, const TRequest&, grpc::CompletionQueue*);

            private:
                grpc::ClientContext Context;
                TAsyncReaderPtr Reader;
                TImpl *Impl;
                TAsyncRequest AsyncRequest;
                TRequest Params;
                const TCallback<TResponse> Callback;
                TResponse Reply;
                grpc::Status Status;
                bool Invoked = false;

            public:
                TRequestProcessor(TImpl *impl, TAsyncRequest asyncRequest, const TRequest& params,
                        TCallback<TResponse>&& callback, const TMaybe<TDuration>& timeout)
                    : Impl(impl)
                    , AsyncRequest(asyncRequest)
                    , Params(params)
                    , Callback(std::move(callback))
                {
                    if (timeout) {
                        Context.set_deadline(std::chrono::system_clock::now() +
                            std::chrono::microseconds(timeout->MicroSeconds()));
                    }
                }

                ~TRequestProcessor() {
                    if (!Invoked) {
                        TGrpcError error = {"request left unhandled", -1};
                        Callback(&error, Reply);
                    }
                }

            private:
                void Start() override {
                    Reader = (Impl->Stub.*AsyncRequest)(&Context, Params, &Impl->CQ);
                    Reader->Finish(&Reply, &Status, this);
                }

                void Finished() override {
                    Y_ABORT_UNLESS(!Invoked);
                    Invoked = true;

                    if (Status.ok()) {
                        Callback(nullptr, Reply);
                    } else {
                        const auto& msg = Status.error_message();
                        TGrpcError error = {TString(msg.data(), msg.length()), Status.error_code()};
                        Callback(&error, Reply);
                    }
                }
            };

            template<typename TRequest, typename TResponse>
            class TStreamRequestProcessor
                : public IStreamRequestReadProcessor
            {
            public:
                using TAsyncReaderPtr = std::unique_ptr<grpc::ClientAsyncReader<TResponse>>;
                using TAsyncRequest = TAsyncReaderPtr (TStub::*)(grpc::ClientContext*, const TRequest&, grpc::CompletionQueue*, void*);
                grpc::ClientContext Context;
                TAsyncReaderPtr Reader;
                TMutex ReaderLock;
                TImpl *Impl;
                TAsyncRequest AsyncRequest;
                TRequest Params;
                const TSimpleCallback<TResponse> Process;
                const TFinishCallback Finish;
                TResponse Reply;
                grpc::Status Status;
                bool Initialized;
                bool Finished;

            public:
                TStreamRequestProcessor(TImpl *impl,
                                        TAsyncRequest asyncRequest,
                                        const TRequest& params,
                                        TSimpleCallback<TResponse>&& process,
                                        TFinishCallback &&finish,
                                        const TMaybe<TDuration>& timeout)
                    : Reader(nullptr)
                    , Impl(impl)
                    , AsyncRequest(asyncRequest)
                    , Params(params)
                    , Process(std::move(process))
                    , Finish(std::move(finish))
                    , Initialized(false)
                    , Finished(false)
                {
                    if (timeout) {
                        Context.set_deadline(std::chrono::system_clock::now() +
                            std::chrono::microseconds(timeout->MicroSeconds()));
                    }
                }

                ~TStreamRequestProcessor() {
                    if (!Finished) {
                        TGrpcError error = {"request left unhandled", -1};
                        Finish(&error);
                    }
                }

                void Start() override {
                    // Stub call will cause async call to InvokeProcess. Lock reader to avoid race.
                    auto guard = Guard(ReaderLock);
                    Reader = (Impl->Stub.*AsyncRequest)(&Context, Params, &Impl->CQ, this);
                }

            private:
                void InvokeProcess() override {
                    auto guard = Guard(ReaderLock);

                    Y_ABORT_UNLESS(!Finished);
                    Y_DEBUG_ABORT_UNLESS(Reader);

                    if (Initialized)
                        Process(Reply);
                    Reader->Read(&Reply, this);
                    Initialized = true;
                }

                void InvokeFinish() override {
                    Y_ABORT_UNLESS(!Finished);
                    Finished = true;

                    if (Status.ok()) {
                        Finish(nullptr);
                    } else {
                        const auto& msg = Status.error_message();
                        TGrpcError error = {TString(msg.data(), msg.length()), Status.error_code()};
                        Finish(&error);
                    }
                }
            };

        private:
            std::shared_ptr<grpc::ChannelInterface> Channel;
            TStub Stub;
            grpc::CompletionQueue CQ;
            TMaybe<TDuration> Timeout;
            ui32 MaxInFlight = 0;
            ui32 InFlight = 0;
            TQueue<THolder<IProcessorBase>> PendingQ;
            TMutex Mutex;
            TSet<void*> StreamTags;

        public:
            TImpl(const TGRpcClientConfig& config)
                : Channel(NYdbGrpc::CreateChannelInterface(config))
                , Stub(Channel)
                , MaxInFlight(config.MaxInFlight)
            {
                if (config.Timeout != TDuration::Max()) {
                    Timeout = config.Timeout;
                }
                ISimpleThread::Start();
            }

            ~TImpl() {
                CQ.Shutdown();
                Join();
            }

            grpc_connectivity_state GetNetworkStatus() const {
                return Channel->GetState(false);
            }

            template<typename TRequest, typename TResponse>
            void Issue(const TRequest& request, TCallback<TResponse>&& callback,
                    typename TRequestProcessor<TRequest, TResponse>::TAsyncRequest asyncRequest) {
                auto processor = MakeHolder<TRequestProcessor<TRequest, TResponse>>(this, asyncRequest, request,
                    std::move(callback), Timeout);
                with_lock (Mutex) {
                    if (!MaxInFlight || InFlight < MaxInFlight) {
                        Start(std::move(processor));
                    } else {
                        PendingQ.push(std::move(processor));
                    }
                }
            }

            template<typename TRequest, typename TResponse>
            void IssueStream(const TRequest& request,
                             TSimpleCallback<TResponse>&& processCb,
                             TFinishCallback&& finishCb,
                             typename TStreamRequestProcessor<TRequest, TResponse>::TAsyncRequest streamRequest) {
                auto processor = MakeHolder<TStreamRequestProcessor<TRequest, TResponse>>
                    (this, streamRequest, request, std::move(processCb), std::move(finishCb), Timeout);
                with_lock (Mutex) {
                    StreamTags.insert(processor.Get());
                    if (!MaxInFlight || InFlight < MaxInFlight) {
                        Start(std::move(processor));
                    } else {
                        PendingQ.push(std::move(processor));
                    }
                }
            }

            bool IsStreamTag(void *tag) {
                with_lock (Mutex) {
                    return StreamTags.contains(tag);
                }
            }

            void EraseStreamTag(void *tag) {
                with_lock (Mutex) {
                    StreamTags.erase(tag);
                }
            }

            void *ThreadProc() override {
                for (;;) {
                    void *tag;
                    bool ok = false;
                    bool finished = true;
                    if (!CQ.Next(&tag, &ok)) {
                        break;
                    }
                    if (IsStreamTag(tag)) {
                        THolder<IStreamRequestReadProcessor> processor(static_cast<IStreamRequestReadProcessor*>(tag));
                        if (ok) {
                            processor->InvokeProcess();
                            Y_UNUSED(processor.Release()); // keep processor alive
                            finished = false;
                        } else {
                            processor->InvokeFinish();
                            EraseStreamTag(tag);
                        }
                    } else {
                        THolder<IRequestProcessor> processor(static_cast<IRequestProcessor*>(tag));
                        if (ok) {
                            processor->Finished();
                        }
                    }

                    if (finished) {
                        with_lock (Mutex) {
                            --InFlight;
                            while (PendingQ && InFlight < MaxInFlight) {
                                Start(std::move(PendingQ.front()));
                                PendingQ.pop();
                            }
                        }
                    }
                }
                return nullptr;
            }

            void Start(THolder<IProcessorBase> &&processor) {
                processor->Start();
                Y_UNUSED(processor.Release());
                ++InFlight;
            }
        };

        TGRpcClient::TGRpcClient(const TGRpcClientConfig& config)
            : Config(config)
            , Impl(new TImpl(Config))
        {}

        TGRpcClient::~TGRpcClient()
        {}

        const TGRpcClientConfig& TGRpcClient::GetConfig() const {
            return Config;
        }

        grpc_connectivity_state TGRpcClient::GetNetworkStatus() const {
            return Impl->GetNetworkStatus();
        }

#define IMPL_REQUEST(NAME, REQUEST, RESPONSE) \
        void TGRpcClient::NAME(const NKikimrClient::REQUEST& request, TCallback<NKikimrClient::RESPONSE> callback) { \
            Impl->Issue(request, std::move(callback), &TImpl::TStub::Async ## NAME); \
        }

        IMPL_REQUEST(Request, TRequest, TResponse)
        IMPL_REQUEST(SchemeOperation, TSchemeOperation, TResponse)
        IMPL_REQUEST(SchemeOperationStatus, TSchemeOperationStatus, TResponse)
        IMPL_REQUEST(SchemeDescribe, TSchemeDescribe, TResponse)
        IMPL_REQUEST(PersQueueRequest, TPersQueueRequest, TResponse)
        IMPL_REQUEST(SchemeInitRoot, TSchemeInitRoot, TResponse)
        IMPL_REQUEST(BlobStorageConfig, TBlobStorageConfigRequest, TResponse)
        IMPL_REQUEST(ResolveNode, TResolveNodeRequest, TResponse)
        IMPL_REQUEST(HiveCreateTablet, THiveCreateTablet, TResponse)
        IMPL_REQUEST(LocalEnumerateTablets, TLocalEnumerateTablets, TResponse)
        IMPL_REQUEST(KeyValue, TKeyValueRequest, TResponse)
        IMPL_REQUEST(RegisterNode, TNodeRegistrationRequest, TNodeRegistrationResponse)
        IMPL_REQUEST(CmsRequest, TCmsRequest, TCmsResponse)
        IMPL_REQUEST(SqsRequest, TSqsRequest, TSqsResponse)
        IMPL_REQUEST(LocalMKQL, TLocalMKQL, TResponse)
        IMPL_REQUEST(LocalSchemeTx, TLocalSchemeTx, TResponse)
        IMPL_REQUEST(TabletKillRequest, TTabletKillRequest, TResponse)
        IMPL_REQUEST(InterconnectDebug, TInterconnectDebug, TResponse)
        IMPL_REQUEST(TabletStateRequest, TTabletStateRequest, TResponse)
        IMPL_REQUEST(ChooseProxy, TChooseProxyRequest, TResponse)
        IMPL_REQUEST(ConsoleRequest, TConsoleRequest, TConsoleResponse)
        IMPL_REQUEST(FillNode, TFillNodeRequest, TResponse)
        IMPL_REQUEST(DrainNode, TDrainNodeRequest, TResponse)

        } // NGRpcProxy
} // NKikimr

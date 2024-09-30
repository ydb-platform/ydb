#include "kicli.h"

#include <ydb/public/lib/deprecated/client/msgbus_client.h>
#include <ydb/core/protos/console_config.pb.h>
#include <util/string/builder.h>

namespace NKikimr {
namespace NClient {

ui32 TKikimr::POLLING_TIMEOUT = 10000;
bool TKikimr::DUMP_REQUESTS = false;

namespace {

struct TRetryState {
    bool IsAllowedToRetry(TDuration& wait, const TRetryPolicy& policy) {
        if (RetryNumber == 0) {
           wait = policy.DoFirstRetryInstantly ? TDuration::Zero() : policy.MinRetryTime;
        } else {
            wait = TDuration::MicroSeconds(RetryDuration.GetValue() * policy.BackoffMultiplier);
            wait = Max(policy.MinRetryTime, wait);
            wait = Min(policy.MaxRetryTime, wait);
        }
        ++RetryNumber;
        RetryDuration = wait;
        return RetryNumber <= policy.RetryLimitCount;
    }

    ui32 RetryNumber = 0;
    TDuration RetryDuration;
};

}

class TKikimr::TRetryQueue {
protected:
    struct TQueueItem {
        TInstant Time;
        NThreading::TPromise<TResult> Promise;
        TAutoPtr<NBus::TBusMessage> Request;

        TQueueItem(TInstant time, NThreading::TPromise<TResult> promise, TAutoPtr<NBus::TBusMessage> request)
            : Time(time)
            , Promise(promise)
            , Request(request)
        {}

        bool operator <(const TQueueItem& other) const {
            return Time > other.Time;
        }
    };

    TKikimr::TImpl& Kikimr;
    volatile bool TimeToQuit;
    TMutex QueueMutex;
    TPriorityQueue<TQueueItem> Queue;
    TCondVar CondVar;
    TThread QueueThread;

public:
    explicit TRetryQueue(TKikimr::TImpl& kikimr);
    ~TRetryQueue();
    void RetryQueueThread();
    void QueueRequest(NThreading::TPromise<TResult> promise, TAutoPtr<NBus::TBusMessage> request, TDuration wait);
    static void* StaticRetryQueueThread(void*);
};

class TKikimr::TImpl {
protected:
    TConnectionPolicy ConnectionPolicy;
    TKikimr::TRetryQueue RetryQueue;
    TString MasterLocation;
    using TCleanupCallback = std::function<void()>;
    TCleanupCallback Cleanup;
public:
    TImpl(const TConnectionPolicy& policy)
        : ConnectionPolicy(policy)
        , RetryQueue(*this)
    {}

    void OnCall(NBus::EMessageStatus status, const TString& transportErrMessage, NThreading::TPromise<TResult> promise, TAutoPtr<NBus::TBusMessage> request, TAutoPtr<NBus::TBusMessage> reply) {
        if (status != NBus::MESSAGE_OK) {
            promise.SetValue(TResult(status, transportErrMessage));
        }
        if (status == NBus::MESSAGE_OK) {
            switch (reply->GetHeader()->Type) {
                case NMsgBusProxy::MTYPE_CLIENT_RESPONSE: {
                    const NKikimrClient::TResponse& response = static_cast<NMsgBusProxy::TBusResponse*>(reply.Get())->Record;
                    DumpResponse(response);
                    switch (response.GetStatus()) {
                        case NMsgBusProxy::MSTATUS_INPROGRESS: {
                            if (response.HasFlatTxId()) {
                                // long polling in progress
                                TAutoPtr<NMsgBusProxy::TBusSchemeOperationStatus> statusRequest = new NMsgBusProxy::TBusSchemeOperationStatus();
                                statusRequest->Record.MutableFlatTxId()->CopyFrom(response.GetFlatTxId());
                                statusRequest->Record.MutablePollOptions()->SetTimeout(POLLING_TIMEOUT);
                                auto status = ExecuteRequest(promise, statusRequest.Release());
                                if (status != NBus::MESSAGE_OK) {
                                    // retry failed - set error value and leaving
                                    promise.SetValue(TResult(status));
                                } else {
                                    // continue polling, cleanup Data for current request
                                    delete reinterpret_cast<TRetryState*>(request->Data);
                                    return;
                                }
                            }
                            break;
                        }
                        case NMsgBusProxy::MSTATUS_REJECTED: {
                            // request was rejected (overloaded?) - retrying
                            TRetryState* retryState = reinterpret_cast<TRetryState*>(request->Data);
                            TDuration wait;
                            if (retryState->IsAllowedToRetry(wait, ConnectionPolicy.RetryPolicy)) {
                                RetryQueue.QueueRequest(promise, request, wait);
                                return;
                            }
                            break;
                        }
                    }
                    break;
                }
            }
        }
        if (!promise.HasValue()) {
            promise.SetValue(TResult(reply));
        }
        delete reinterpret_cast<TRetryState*>(request->Data);
    }

    NThreading::TFuture<TResult> InternalExecute(TAutoPtr<NBus::TBusMessage> request) {
        request->Data = new TRetryState();
        auto promise = NThreading::NewPromise<TResult>();
        auto status = ExecuteRequest(promise, request);
        if (status != NBus::MESSAGE_OK)
            promise.SetValue(TResult(status));
        return promise.GetFuture();
    }

    NThreading::TFuture<TResult> Execute(TAutoPtr<NBus::TBusMessage> request) {
        if (ConnectionPolicy.ChooseProxy && MasterLocation.empty()) {
            MasterLocation = GetCurrentLocation();
            return Execute(new NMsgBusProxy::TBusChooseProxy).Apply([this, request](const NThreading::TFuture<TResult>& future) -> NThreading::TFuture<TResult> {
                const TResult& value(future.GetValue());
                if (value.GetError().Success()) {
                    const NKikimrClient::TResponse& response = value.GetResult<NKikimrClient::TResponse>();
                    TString newLocation = response.GetProxyName();
                    if (!newLocation.empty() && newLocation != MasterLocation) {
                        Cleanup = SwitchToLocation(response.GetProxyName());
                    }
                }
                return InternalExecute(request);
            });
        }
        return InternalExecute(request);
    }

    virtual ~TImpl() = default;
    virtual NBus::EMessageStatus ExecuteRequest(NThreading::TPromise<TResult> promise, TAutoPtr<NBus::TBusMessage> request) = 0;
    virtual TString GetCurrentLocation() const = 0;
    virtual TCleanupCallback SwitchToLocation(const TString& location) = 0;
};

class TKikimr::TMsgBusImpl : public TKikimr::TImpl {
    TAutoPtr<NMsgBusProxy::TMsgBusClient> MsgBusClient;
public:
    TMsgBusImpl(const NMsgBusProxy::TMsgBusClientConfig& clientConfig, const TConnectionPolicy& policy)
        : TImpl(policy)
        , MsgBusClient(new NMsgBusProxy::TMsgBusClient(clientConfig))
    {
        MsgBusClient->Init();
    }

    virtual NBus::EMessageStatus ExecuteRequest(NThreading::TPromise<TResult> promise, TAutoPtr<NBus::TBusMessage> request) override {
        return MsgBusClient->AsyncCall(request.Release(), [this, promise](
                                       NBus::EMessageStatus status,
                                       TAutoPtr<NBus::TBusMessage> request,
                                       TAutoPtr<NBus::TBusMessage> reply) mutable -> void {
            OnCall(status, "", promise, request, reply);
        });
    }

    virtual TString GetCurrentLocation() const override {
        return MsgBusClient->GetConfig().Ip;
    }

    virtual TCleanupCallback SwitchToLocation(const TString& location) override {
        NMsgBusProxy::TMsgBusClientConfig config(MsgBusClient->GetConfig());
        if (config.Ip == location) {
            return TCleanupCallback();
        }
        config.Ip = location;
        TAutoPtr<NMsgBusProxy::TMsgBusClient> msgBusClient = new NMsgBusProxy::TMsgBusClient(config);
        msgBusClient->Init();
        msgBusClient.Swap(MsgBusClient);
        // we cleanup later to avoid dead lock because we are still inside callback of current client
        return [msgBusClient]() {};
    }
};

class TKikimr::TGRpcImpl : public TKikimr::TImpl {
    TAutoPtr<NGRpcProxy::TGRpcClient> GRpcClient;
public:
    TGRpcImpl(const NGRpcProxy::TGRpcClientConfig& clientConfig, const TConnectionPolicy& policy)
        : TImpl(policy)
        , GRpcClient(new NGRpcProxy::TGRpcClient(clientConfig))
    {}

    template <typename RequestType, typename ResponseType = NMsgBusProxy::TBusResponse>
    NBus::EMessageStatus ExecuteGRpcRequest(
            void (NGRpcProxy::TGRpcClient::* func)(const typename RequestType::RecordType&, NGRpcProxy::TCallback<typename ResponseType::RecordType>),
            NThreading::TPromise<TResult> promise,
            TAutoPtr<NBus::TBusMessage> request) {
        RequestType* x = static_cast<RequestType*>(request.Get());
        const auto& proto = x->Record;
        auto callback = [this, request, promise](
                const NGRpcProxy::TGrpcError* error,
                const typename ResponseType::RecordType& proto) {
            if (error) {
                OnCall(MapGrpcStatus(error->second), error->first, promise, request, nullptr);
            } else {
                auto reply = new ResponseType;
                reply->Record = proto;
                OnCall(NBus::MESSAGE_OK, "", promise, request, reply);
            }
        };
        (GRpcClient.Get()->*func)(proto, std::move(callback));
        return NBus::MESSAGE_OK;
    }

    virtual NBus::EMessageStatus ExecuteRequest(NThreading::TPromise<TResult> promise, TAutoPtr<NBus::TBusMessage> request) override {
        const ui32 type = request->GetHeader()->Type;
        switch(type) {
        case NMsgBusProxy::MTYPE_CLIENT_REQUEST:
            return ExecuteGRpcRequest<NMsgBusProxy::TBusRequest>(&NGRpcProxy::TGRpcClient::Request, promise, request);
        case NMsgBusProxy::MTYPE_CLIENT_FLAT_TX_REQUEST:
            return ExecuteGRpcRequest<NMsgBusProxy::TBusSchemeOperation>(&NGRpcProxy::TGRpcClient::SchemeOperation, promise, request);
        case NMsgBusProxy::MTYPE_CLIENT_FLAT_TX_STATUS_REQUEST:
            return ExecuteGRpcRequest<NMsgBusProxy::TBusSchemeOperationStatus>(&NGRpcProxy::TGRpcClient::SchemeOperationStatus, promise, request);
        case NMsgBusProxy::MTYPE_CLIENT_FLAT_DESCRIBE_REQUEST:
            return ExecuteGRpcRequest<NMsgBusProxy::TBusSchemeDescribe>(&NGRpcProxy::TGRpcClient::SchemeDescribe, promise, request);
        case NMsgBusProxy::MTYPE_CLIENT_PERSQUEUE:
            return ExecuteGRpcRequest<NMsgBusProxy::TBusPersQueue>(&NGRpcProxy::TGRpcClient::PersQueueRequest, promise, request);
        case NMsgBusProxy::MTYPE_CLIENT_SCHEME_INITROOT:
            return ExecuteGRpcRequest<NMsgBusProxy::TBusSchemeInitRoot>(&NGRpcProxy::TGRpcClient::SchemeInitRoot, promise, request);
        case NMsgBusProxy::MTYPE_CLIENT_BLOB_STORAGE_CONFIG_REQUEST:
            return ExecuteGRpcRequest<NMsgBusProxy::TBusBlobStorageConfigRequest>(&NGRpcProxy::TGRpcClient::BlobStorageConfig, promise, request);
        case NMsgBusProxy::MTYPE_CLIENT_HIVE_CREATE_TABLET:
            return ExecuteGRpcRequest<NMsgBusProxy::TBusHiveCreateTablet>(&NGRpcProxy::TGRpcClient::HiveCreateTablet, promise, request);
        case NMsgBusProxy::MTYPE_CLIENT_LOCAL_ENUMERATE_TABLETS:
            return ExecuteGRpcRequest<NMsgBusProxy::TBusLocalEnumerateTablets>(&NGRpcProxy::TGRpcClient::LocalEnumerateTablets, promise, request);
        case NMsgBusProxy::MTYPE_CLIENT_KEYVALUE:
            return ExecuteGRpcRequest<NMsgBusProxy::TBusKeyValue>(&NGRpcProxy::TGRpcClient::KeyValue, promise, request);
        case NMsgBusProxy::MTYPE_CLIENT_LOCAL_MINIKQL:
            return ExecuteGRpcRequest<NMsgBusProxy::TBusTabletLocalMKQL>(&NGRpcProxy::TGRpcClient::LocalMKQL, promise, request);
        case NMsgBusProxy::MTYPE_CLIENT_LOCAL_SCHEME_TX:
            return ExecuteGRpcRequest<NMsgBusProxy::TBusTabletLocalSchemeTx>(&NGRpcProxy::TGRpcClient::LocalSchemeTx, promise, request);
        case NMsgBusProxy::MTYPE_CLIENT_TABLET_KILL_REQUEST:
            return ExecuteGRpcRequest<NMsgBusProxy::TBusTabletKillRequest>(&NGRpcProxy::TGRpcClient::TabletKillRequest, promise, request);
        case NMsgBusProxy::MTYPE_CLIENT_TABLET_STATE_REQUEST:
            return ExecuteGRpcRequest<NMsgBusProxy::TBusTabletStateRequest>(&NGRpcProxy::TGRpcClient::TabletStateRequest, promise, request);
        case NMsgBusProxy::MTYPE_CLIENT_NODE_REGISTRATION_REQUEST:
            return ExecuteGRpcRequest<NMsgBusProxy::TBusNodeRegistrationRequest, NMsgBusProxy::TBusNodeRegistrationResponse>(&NGRpcProxy::TGRpcClient::RegisterNode, promise, request);
        case NMsgBusProxy::MTYPE_CLIENT_CMS_REQUEST:
            return ExecuteGRpcRequest<NMsgBusProxy::TBusCmsRequest, NMsgBusProxy::TBusCmsResponse>(&NGRpcProxy::TGRpcClient::CmsRequest, promise, request);
        case NMsgBusProxy::MTYPE_CLIENT_CHOOSE_PROXY:
            return ExecuteGRpcRequest<NMsgBusProxy::TBusChooseProxy>(&NGRpcProxy::TGRpcClient::ChooseProxy, promise, request);
        case NMsgBusProxy::MTYPE_CLIENT_SQS_REQUEST:
            return ExecuteGRpcRequest<NMsgBusProxy::TBusSqsRequest, NMsgBusProxy::TBusSqsResponse>(&NGRpcProxy::TGRpcClient::SqsRequest, promise, request);
        case NMsgBusProxy::MTYPE_CLIENT_INTERCONNECT_DEBUG:
            return ExecuteGRpcRequest<NMsgBusProxy::TBusInterconnectDebug>(&NGRpcProxy::TGRpcClient::InterconnectDebug, promise, request);
        case NMsgBusProxy::MTYPE_CLIENT_CONSOLE_REQUEST:
            return ExecuteGRpcRequest<NMsgBusProxy::TBusConsoleRequest, NMsgBusProxy::TBusConsoleResponse>(&NGRpcProxy::TGRpcClient::ConsoleRequest, promise, request);
        case NMsgBusProxy::MTYPE_CLIENT_RESOLVE_NODE:
            return ExecuteGRpcRequest<NMsgBusProxy::TBusResolveNode, NMsgBusProxy::TBusResponse>(&NGRpcProxy::TGRpcClient::ResolveNode, promise, request);
        case NMsgBusProxy::MTYPE_CLIENT_DRAIN_NODE:
            return ExecuteGRpcRequest<NMsgBusProxy::TBusDrainNode, NMsgBusProxy::TBusResponse>(&NGRpcProxy::TGRpcClient::DrainNode, promise, request);
        case NMsgBusProxy::MTYPE_CLIENT_FILL_NODE:
            return ExecuteGRpcRequest<NMsgBusProxy::TBusFillNode, NMsgBusProxy::TBusResponse>(&NGRpcProxy::TGRpcClient::FillNode, promise, request);
        default:
            Y_ABORT("%s", (TStringBuilder() << "unexpected message type# " << type).data());
        }
    }

    virtual TString GetCurrentLocation() const override {
        TString host;
        ui32 port;
        NMsgBusProxy::TMsgBusClientConfig::CrackAddress(GRpcClient->GetConfig().Locator, host, port);
        return host;
    }

    static bool IsIPv6(const TString& host) {
        return host.find_first_not_of(":0123456789abcdef") == TString::npos;
    }

    static TString EscapeIPv6(const TString& host) {
        if (IsIPv6(host)) {
            return TStringBuilder() << '[' << host << ']';
        } else {
            return host;
        }
    }

    virtual TCleanupCallback SwitchToLocation(const TString& location) override {
        NGRpcProxy::TGRpcClientConfig config(GRpcClient->GetConfig());
        TString hostname;
        ui32 port;
        NMsgBusProxy::TMsgBusClientConfig::CrackAddress(config.Locator, hostname, port);
        TString newLocation = TStringBuilder() << EscapeIPv6(location) << ':' << port;
        if (newLocation == config.Locator) {
            return TCleanupCallback();
        }
        config.Locator = newLocation;
        TAutoPtr<NGRpcProxy::TGRpcClient> gRpcClient = new NGRpcProxy::TGRpcClient(config);
        gRpcClient.Swap(GRpcClient);
        // we cleanup later to avoid dead lock because we are still inside callback of current client
        return [gRpcClient]() {};
    }
private:
    static NBus::EMessageStatus MapGrpcStatus(int status) {
        switch (status) {
            case grpc::StatusCode::OK:
                return NBus::EMessageStatus::MESSAGE_OK;
            case grpc::StatusCode::DEADLINE_EXCEEDED:
                return NBus::EMessageStatus::MESSAGE_TIMEOUT;
            case grpc::StatusCode::RESOURCE_EXHAUSTED:
                return NBus::EMessageStatus::MESSAGE_BUSY;
            default:
                return NBus::MESSAGE_CONNECT_FAILED;
        }
    }
};

TKikimr::TRetryQueue::TRetryQueue(TKikimr::TImpl& kikimr)
    : Kikimr(kikimr)
    , TimeToQuit(false)
    , QueueThread(&TRetryQueue::StaticRetryQueueThread, this)
{}

TKikimr::TRetryQueue::~TRetryQueue() {
    AtomicStore(&TimeToQuit, true);
    if (QueueThread.Running()) {
        CondVar.Signal();
        QueueThread.Join();
    }
}

void TKikimr::TRetryQueue::RetryQueueThread() {
    while (!AtomicLoad(&TimeToQuit)) {
        TGuard<TMutex> lock(QueueMutex);
        TInstant now = TInstant::Now();
        TInstant time;
        if (!Queue.empty()) {
            time = Queue.top().Time;
        } else {
            time = TInstant::Max();
        }
        if (time < now || !CondVar.WaitD(*lock.GetMutex(), time)) {
            if (!Queue.empty()) {
                TQueueItem& item = const_cast<TQueueItem&>(Queue.top());
                auto status = Kikimr.ExecuteRequest(item.Promise, item.Request.Release());
                if (status != NBus::MESSAGE_OK) {
                    item.Promise.SetValue(TResult(status));
                }
                Queue.pop();
            }
        }
    }
}

void* TKikimr::TRetryQueue::StaticRetryQueueThread(void* param) {
    static_cast<TRetryQueue*>(param)->RetryQueueThread();
    return nullptr;
}

void TKikimr::TRetryQueue::QueueRequest(NThreading::TPromise<TResult> promise, TAutoPtr<NBus::TBusMessage> request, TDuration wait) {
    {
        TGuard<TMutex> lock(QueueMutex);
        Queue.emplace(TInstant::Now() + wait, promise, request);
        if (!QueueThread.Running()) {
            QueueThread.Start();
        }
    }
    CondVar.Signal();
}

TKikimr::TKikimr(const NMsgBusProxy::TMsgBusClientConfig& clientConfig, const TConnectionPolicy& policy)
    : Impl(new TMsgBusImpl(clientConfig, policy))
{}

TKikimr::TKikimr(const NGRpcProxy::TGRpcClientConfig& clientConfig, const TConnectionPolicy& policy)
    : Impl(new TGRpcImpl(clientConfig, policy))
{}

TString TKikimr::GetCurrentLocation() const {
    return Impl->GetCurrentLocation();
}

TKikimr::~TKikimr() = default;

TTextQuery TKikimr::Query(const TString& program) {
    return TTextQuery(*this, program);
}

TSchemaObject TKikimr::GetSchemaRoot(const TString& name) {
    return TSchemaObject(*this, "/", name, 0, TSchemaObject::EPathType::SubDomain);
}

TSchemaObject TKikimr::GetSchemaObject(const TString& name) {
    auto pos = name.rfind('/');
    TString pathObj(name.substr(0, pos));
    TString nameObj(pos != TString::npos ? name.substr(pos + 1) : "");
    return TSchemaObject(*this, pathObj, nameObj, 0, TSchemaObject::EPathType::Unknown);
}

NThreading::TFuture<TResult> TKikimr::ExecuteRequest(TAutoPtr<NBus::TBusMessage> request) {
    return Impl->Execute(request);
}

NThreading::TFuture<TQueryResult> TKikimr::ExecuteQuery(const TTextQuery& query, const NKikimrMiniKQL::TParams& parameters) {
    TAutoPtr<NMsgBusProxy::TBusRequest> request(new NMsgBusProxy::TBusRequest());
    auto* mkqlTx = request->Record.MutableTransaction()->MutableMiniKQLTransaction();
    mkqlTx->SetFlatMKQL(true);
    mkqlTx->SetMode(NKikimrTxUserProxy::TMiniKQLTransaction::COMPILE_AND_EXEC);
    mkqlTx->MutableProgram()->SetText(query.TextProgram);
    mkqlTx->MutableParams()->MutableProto()->CopyFrom(parameters);
    auto future = ExecuteRequest(request.Release());
    return future.Apply([](const NThreading::TFuture<TResult>& result) -> TQueryResult {
        return TQueryResult(result.GetValue(TDuration::Max()));
    });
}

NThreading::TFuture<TQueryResult> TKikimr::ExecuteQuery(const TTextQuery& query, const TString& parameters) {
    TAutoPtr<NMsgBusProxy::TBusRequest> request(new NMsgBusProxy::TBusRequest());
    auto* mkqlTx = request->Record.MutableTransaction()->MutableMiniKQLTransaction();
    mkqlTx->SetFlatMKQL(true);
    mkqlTx->SetMode(NKikimrTxUserProxy::TMiniKQLTransaction::COMPILE_AND_EXEC);
    mkqlTx->MutableProgram()->SetText(query.TextProgram);
    if (!parameters.empty())
        mkqlTx->MutableParams()->SetText(parameters);
    auto future = ExecuteRequest(request.Release());
    return future.Apply([](const NThreading::TFuture<TResult>& result) -> TQueryResult {
        return TQueryResult(result.GetValue(TDuration::Max()));
    });
}

NThreading::TFuture<TQueryResult> TKikimr::ExecuteQuery(const TPreparedQuery& query, const NKikimrMiniKQL::TParams& parameters) {
    TAutoPtr<NMsgBusProxy::TBusRequest> request(new NMsgBusProxy::TBusRequest());
    auto* mkqlTx = request->Record.MutableTransaction()->MutableMiniKQLTransaction();
    mkqlTx->SetFlatMKQL(true);
    mkqlTx->SetMode(NKikimrTxUserProxy::TMiniKQLTransaction::COMPILE_AND_EXEC);
    mkqlTx->MutableProgram()->SetBin(query.CompiledProgram);
    if (parameters.HasType() && parameters.GetType().HasKind()) {
        mkqlTx->MutableParams()->MutableProto()->CopyFrom(parameters);
    }
    auto future = ExecuteRequest(request.Release());
    return future.Apply([](const NThreading::TFuture<TResult>& result) -> TQueryResult {
        return TQueryResult(result.GetValue(TDuration::Max()));
    });
}

NThreading::TFuture<TQueryResult> TKikimr::ExecuteQuery(const TPreparedQuery& query, const TString& parameters) {
    TAutoPtr<NMsgBusProxy::TBusRequest> request(new NMsgBusProxy::TBusRequest());
    auto* mkqlTx = request->Record.MutableTransaction()->MutableMiniKQLTransaction();
    mkqlTx->SetFlatMKQL(true);
    mkqlTx->SetMode(NKikimrTxUserProxy::TMiniKQLTransaction::COMPILE_AND_EXEC);
    mkqlTx->MutableProgram()->SetBin(query.CompiledProgram);
    if (!parameters.empty())
        mkqlTx->MutableParams()->SetText(parameters);
    auto future = ExecuteRequest(request.Release());
    return future.Apply([](const NThreading::TFuture<TResult>& result) -> TQueryResult {
        return TQueryResult(result.GetValue(TDuration::Max()));
    });
}

NThreading::TFuture<TPrepareResult> TKikimr::PrepareQuery(const TTextQuery& query) {
    TAutoPtr<NMsgBusProxy::TBusRequest> request(new NMsgBusProxy::TBusRequest());
    auto* mkqlTx = request->Record.MutableTransaction()->MutableMiniKQLTransaction();
    mkqlTx->SetFlatMKQL(true);
    mkqlTx->SetMode(NKikimrTxUserProxy::TMiniKQLTransaction::COMPILE);
    mkqlTx->MutableProgram()->SetText(query.TextProgram);
    auto future = ExecuteRequest(request.Release());
    return future.Apply([&query](const NThreading::TFuture<TResult>& result) -> TPrepareResult {
        return TPrepareResult(result.GetValue(TDuration::Max()), query);
    });
}

NThreading::TFuture<TResult> TKikimr::DescribeObject(const TSchemaObject& object) {
    TAutoPtr<NMsgBusProxy::TBusSchemeDescribe> request(new NMsgBusProxy::TBusSchemeDescribe());
    request->Record.SetPath(object.GetPath());
    return ExecuteRequest(request.Release());
}

NThreading::TFuture<TResult> TKikimr::ModifySchema(const TModifyScheme& schema) {
    TAutoPtr<NMsgBusProxy::TBusSchemeOperation> request(new NMsgBusProxy::TBusSchemeOperation());
    request->Record.MutablePollOptions()->SetTimeout(POLLING_TIMEOUT);
    request->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(schema);
    return ExecuteRequest(request.Release());
}

NThreading::TFuture<TResult> TKikimr::MakeDirectory(const TSchemaObject& object, const TString& name) {
    TAutoPtr<NMsgBusProxy::TBusSchemeOperation> request(new NMsgBusProxy::TBusSchemeOperation());
    request->Record.MutablePollOptions()->SetTimeout(POLLING_TIMEOUT);
    auto* modifyScheme = request->Record.MutableTransaction()->MutableModifyScheme();
    modifyScheme->SetWorkingDir(object.GetPath());
    modifyScheme->SetOperationType(NKikimrSchemeOp::ESchemeOpMkDir);
    auto* makeDirectory = modifyScheme->MutableMkDir();
    makeDirectory->SetName(name);
    return ExecuteRequest(request.Release());
}

NThreading::TFuture<TResult> TKikimr::CreateTable(TSchemaObject& object, const TString& name, const TVector<TColumn>& columns,
                                                  const TTablePartitionConfig* partitionConfig)
{
    TAutoPtr<NMsgBusProxy::TBusSchemeOperation> request(new NMsgBusProxy::TBusSchemeOperation());
    request->Record.MutablePollOptions()->SetTimeout(POLLING_TIMEOUT);
    auto* modifyScheme = request->Record.MutableTransaction()->MutableModifyScheme();
    modifyScheme->SetWorkingDir(object.GetPath());
    modifyScheme->SetOperationType(NKikimrSchemeOp::ESchemeOpCreateTable);
    auto* createTable = modifyScheme->MutableCreateTable();
    createTable->SetName(name);
    for (const TColumn& column : columns) {
        auto* col = createTable->AddColumns();
        col->SetName(column.Name);
        col->SetType(column.Type.GetName());
        if (column.Key) {
            createTable->AddKeyColumnNames(column.Name);
            if (column.Partitions != 0) {
                Y_ASSERT(!createTable->HasUniformPartitionsCount());
                createTable->SetUniformPartitionsCount(column.Partitions);
            }
        }
    }
    if (partitionConfig) {
        createTable->MutablePartitionConfig()->CopyFrom(*partitionConfig);
    }
    return ExecuteRequest(request.Release());
}

void TKikimr::SetSecurityToken(const TString& securityToken) {
    SecurityToken = securityToken;
}

TPreparedQuery TKikimr::Query(const TUnbindedQuery& query) {
    return TPreparedQuery(TQuery(*this), query.CompiledProgram);
}

TKikimr::TKikimr(TKikimr&& kikimr)
    : SecurityToken(std::move(kikimr.SecurityToken))
    , Impl(std::move(kikimr.Impl))
{}

TNodeRegistrant TKikimr::GetNodeRegistrant()
{
    return TNodeRegistrant(*this);
}

TNodeConfigurator TKikimr::GetNodeConfigurator()
{
    return TNodeConfigurator(*this);
}

NThreading::TFuture<TResult> TKikimr::RegisterNode(const TString& domainPath, const TString& host, ui16 port,
                                                   const TString& address, const TString& resolveHost,
                                                   const NActors::TNodeLocation& location,
                                                   bool fixedNodeId, TMaybe<TString> path)
{
    TAutoPtr<NMsgBusProxy::TBusNodeRegistrationRequest> request = new NMsgBusProxy::TBusNodeRegistrationRequest;
    request->Record.SetHost(host);
    request->Record.SetPort(port);
    request->Record.SetAddress(address);
    request->Record.SetResolveHost(resolveHost);
    location.Serialize(request->Record.MutableLocation(), true);
    request->Record.SetDomainPath(domainPath);
    request->Record.SetFixedNodeId(fixedNodeId);
    if (path) {
        request->Record.SetPath(*path);
    }
    return ExecuteRequest(request.Release());
}

NThreading::TFuture<TResult> TKikimr::GetNodeConfig(ui32 nodeId,
                                                    const TString &host,
                                                    const TString &tenant,
                                                    const TString &nodeType,
                                                    const TString& domain,
                                                    const TString& token,
                                                    bool serveYaml,
                                                    ui64 version)
{
    TAutoPtr<NMsgBusProxy::TBusConsoleRequest> request = new NMsgBusProxy::TBusConsoleRequest;
    auto &rec = request->Record;
    rec.MutableGetNodeConfigRequest()->SetServeYaml(serveYaml);
    rec.MutableGetNodeConfigRequest()->SetYamlApiVersion(version);
    auto &node = *rec.MutableGetNodeConfigRequest()->MutableNode();
    node.SetNodeId(nodeId);
    node.SetHost(host);
    node.SetTenant(tenant);
    node.SetNodeType(nodeType);
    if (domain)
        request->Record.SetDomainName(domain);
    if (token)
        request->Record.SetSecurityToken(token);
    return ExecuteRequest(request.Release());
}


}
}

#pragma once

#include <ydb/core/fq/libs/wasm_services/transport.h>
#include <ydb/core/fq/libs/wasm_services/ut/protos/mock.grpc.pb.h>
#include <ydb/core/security/certificate_check/test_utils/test_cert_auth_utils.h>

#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/executor_thread_ctx.h>
#include <ydb/library/actors/http/http_proxy.h>
#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/library/wasm/api/function.h>
#include <ydb/services/udf_store/wasm/bridge_resident.h>
#include <ydb/services/udf_store/wasm/compile.h>
#include <ydb/services/udf_store/wasm/host.h>
#include <ydb/services/udf_store/wasm/invocation_context.h>
#include <ydb/services/udf_store/wasm/registry_helpers.h>

#include <google/protobuf/util/json_util.h>
#include <grpcpp/grpcpp.h>
#include <library/cpp/http/server/http_ex.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/resource/resource.h>
#include <library/cpp/testing/common/network.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/charset/utf8.h>
#include <util/stream/file.h>
#include <util/stream/str.h>
#include <util/stream/zlib.h>
#include <util/string/builder.h>
#include <util/system/datetime.h>
#include <util/system/tempfile.h>

#include <algorithm>
#include <condition_variable>
#include <future>
#include <mutex>

using namespace NFq::NWasmServices;
using namespace NAsync;

using namespace NKikimr::NUdfStore::NWasm;
using namespace NYdb::NWasm;

namespace NFq::NWasmServices::NTestHelpers {

inline std::string_view View(TStringBuf bytes) {
    return { bytes.data(), bytes.size() };
}

inline TString Proto(TStringBuf value) {
    google::protobuf::BytesValue message;
    message.set_value(value.data(), value.size());
    return message.SerializeAsString();
}

inline TString Value(TStringBuf bytes) {
    google::protobuf::BytesValue message;
    UNIT_ASSERT(message.ParseFromArray(bytes.data(), bytes.size()));
    return message.value();
}

template <class F>
void Eventually(F condition) {
    const auto deadline = TInstant::Now() + TDuration::Seconds(10);
    while (!condition()) {
        UNIT_ASSERT_C(TInstant::Now() < deadline, "Timed out waiting for physical transport cleanup");
        Sleep(TDuration::MilliSeconds(5));
    }
}

class TSingleUserThreadRuntime: public NActors::TTestActorRuntimeBase {
public:
    TSingleUserThreadRuntime()
        : TTestActorRuntimeBase(1, true)
    {
    }

protected:
    void InitActorSystemSetup(NActors::TActorSystemSetup& setup, TNodeDataBase*) override {
        setup.Executors[1].Reset(new NActors::TBasicExecutorPool(1, 1, 20, "User"));
    }
};

struct TExecutorFixture {
    TSingleUserThreadRuntime Runtime;
    std::shared_ptr<TTransportExecutor> Executor;

    explicit TExecutorFixture(TTransportExecutorLimits limits = {}, std::shared_ptr<TRequestMetrics> metrics = {}) {
        Runtime.Initialize();
        Executor = TTransportExecutor::Create(Runtime.GetActorSystem(0), 1, limits, std::move(metrics));
    }
};

// Hold the fixture's only user thread after admission to distinguish logical
// completion from work that still needs executor/native cleanup.
class TUserPoolGateActor final: public NActors::TActorBootstrapped<TUserPoolGateActor> {
    std::promise<void>& Entered;
    std::shared_future<void> Release;

public:
    TUserPoolGateActor(std::promise<void>& entered, std::shared_future<void> release)
        : Entered(entered)
        , Release(std::move(release))
    {
    }

    void Bootstrap() {
        Entered.set_value();
        Release.wait();
        PassAway();
    }
};

struct TUserPoolGate {
    std::promise<void> Entered, Release;

    explicit TUserPoolGate(TExecutorFixture& fixture) {
        auto entered = Entered.get_future();
        fixture.Runtime.Register(new TUserPoolGateActor(Entered, Release.get_future().share()), 0, 1);
        UNIT_ASSERT(entered.wait_for(std::chrono::seconds(10)) == std::future_status::ready);
    }

    ~TUserPoolGate() {
        Release.set_value();
    }
};

inline std::shared_ptr<TTransportExecutor> TestExecutor() {
    static TExecutorFixture fixture;
    return fixture.Executor;
}

struct TExchange {
    TString Payload;
    TString Method;
    TVector<std::pair<TString, TString>> Headers;
    TString Reply;
    TString Location;
    TString Encoding;
    int Code = 0;
    bool Ready = false;
    bool Abort = false;
    bool Cancelled = false;
};

class TMockState {
public:
    std::mutex Mutex;
    std::condition_variable Changed;
    TVector<std::shared_ptr<TExchange>> Requests;
    bool Closing = false;

    std::shared_ptr<TExchange> Record(TString payload, TString method, TVector<std::pair<TString, TString>> headers = {}) {
        std::lock_guard lock(Mutex);
        auto exchange = std::make_shared<TExchange>();
        exchange->Payload = std::move(payload);
        exchange->Method = std::move(method);
        exchange->Headers = std::move(headers);
        Requests.push_back(exchange);
        Changed.notify_all();
        return exchange;
    }

    std::shared_ptr<TExchange> Wait(size_t index = 0) {
        std::unique_lock lock(Mutex);
        UNIT_ASSERT_C(Changed.wait_for(lock, std::chrono::seconds(10), [&] {
            return Requests.size() > index;
        }), "Mock received no request");
        return Requests[index];
    }

    size_t Count() {
        std::lock_guard lock(Mutex);
        return Requests.size();
    }

    void Reply(size_t index, TString bytes, int code, bool abort = false, TString location = {}, TString encoding = {}) {
        std::lock_guard lock(Mutex);
        auto& exchange = *Requests.at(index);
        exchange.Reply = std::move(bytes);
        exchange.Code = code;
        exchange.Abort = abort;
        exchange.Location = std::move(location);
        exchange.Encoding = std::move(encoding);
        exchange.Ready = true;
        Changed.notify_all();
    }

    bool AwaitReply(const std::shared_ptr<TExchange>& exchange, const std::function<bool()>& cancelled = {}) {
        std::unique_lock lock(Mutex);
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (!Closing && !exchange->Ready) {
            if (cancelled && cancelled()) {
                exchange->Cancelled = true;
                Changed.notify_all();
                return false;
            }
            if (std::chrono::steady_clock::now() >= deadline) {
                return false;
            }
            Changed.wait_for(lock, std::chrono::milliseconds(10));
        }
        return !Closing && !exchange->Abort;
    }

    void WaitCancelled(size_t index) {
        std::unique_lock lock(Mutex);
        UNIT_ASSERT(Changed.wait_for(lock, std::chrono::seconds(10), [&] {
            return Requests.at(index)->Cancelled;
        }));
    }

    void Stop() {
        std::lock_guard lock(Mutex);
        Closing = true;
        Changed.notify_all();
    }
};

class THttpMock: public THttpServer::ICallBack {
    class TRequest: public THttpClientRequestEx {
    public:
        explicit TRequest(THttpMock& owner)
            : Owner(owner)
        {
        }

        bool Reply(void*) override {
            // The generic test-server parser does not implement HEAD.
            const bool head = RequestString.StartsWith("HEAD ");
            if (!head && !ProcessHeaders()) {
                return true;
            }
            auto exchange = Owner.State->Record(TString(Buf.AsCharPtr(), Buf.Size()), RequestString, ParsedHeaders);
            if (!Owner.State->AwaitReply(exchange)) {
                ResetConnection();
                return true;
            }
            // Tests supply already encoded bodies; do not gzip them a second time.
            Output().EnableCompressionHeader(false);
            Output() << "HTTP/1.1 " << exchange->Code << " Test\r\nContent-Length: " << exchange->Reply.size()
                     << "\r\nConnection: " << (Owner.HoldHeadConnection ? "keep-alive" : "close") << "\r\n";
            if (!exchange->Location.empty()) {
                Output() << "Location: " << exchange->Location << "\r\n";
            }
            if (!exchange->Encoding.empty()) {
                Output() << "Content-Encoding: " << exchange->Encoding << "\r\n";
            }
            Output() << "\r\n";
            if (!head) {
                Output().Write(exchange->Reply.data(), exchange->Reply.size());
            }
            Output().Finish();
            if (head && Owner.HoldHeadConnection) {
                std::unique_lock lock(Owner.State->Mutex);
                Owner.State->Changed.wait(lock, [&] {
                    return Owner.State->Closing;
                });
            }
            return true;
        }

    private:
        THttpMock& Owner;
    };

    NTesting::TPortHolder Port = NTesting::GetFreePort();
    std::unique_ptr<THttpServer> Server;

public:
    std::shared_ptr<TMockState> State = std::make_shared<TMockState>();
    const bool HoldHeadConnection;

    explicit THttpMock(bool holdHeadConnection = false)
        : HoldHeadConnection(holdHeadConnection)
    {
        THttpServerOptions options(Port);
        options.SetHost("127.0.0.1").SetThreads(4).SetMaxInputContentLength(1 << 20).SetClientTimeout(TDuration::Seconds(10));
        Server = std::make_unique<THttpServer>(this, options);
        UNIT_ASSERT(Server->Start());
    }

    ~THttpMock() {
        State->Stop();
        Server->Stop();
    }

    TClientRequest* CreateClient() override {
        return new TRequest(*this);
    }

    TString Url() const {
        return TStringBuilder() << "http://127.0.0.1:" << static_cast<ui16>(Port);
    }
};

class THttpTlsMock {
public:
    NActors::TTestActorRuntimeBase Runtime{ 1, true };
    TPortManager PortManager;
    const TIpPort Port = PortManager.GetTcpPort();
    TTempFileHandle CaFile;
    NKikimr::NCertTestUtils::TCertAndKey Ca;
    NActors::TActorId ProxyId;
    NActors::TActorId HandlerId;

    explicit THttpTlsMock(bool validHostname = true) {
        using namespace NKikimr::NCertTestUtils;
        Ca = GenerateCA(TProps::AsCA().WithValid(TDuration::Days(1)));
        auto serverProps = TProps::AsServer().WithValid(TDuration::Days(1));
        if (!validHostname) {
            serverProps.AltNames = { "DNS:wrong.test" };
        }
        const auto server = GenerateSignedCert(Ca, serverProps);
        CaFile.Write(Ca.Certificate.data(), Ca.Certificate.size());
        Runtime.Initialize();
        ProxyId = Runtime.Register(NHttp::CreateHttpProxy());
        auto* add = new NHttp::TEvHttpProxy::TEvAddListeningPort(Port);
        add->Address = "127.0.0.1";
        add->Secure = true;
        add->SslCertificatePem = server.Certificate + server.PrivateKey;
        Runtime.Send(new NActors::IEventHandle(ProxyId, Runtime.AllocateEdgeActor(), add), 0, true);
        TAutoPtr<NActors::IEventHandle> handle;
        UNIT_ASSERT(Runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvConfirmListen>(handle));
        HandlerId = Runtime.AllocateEdgeActor();
        Runtime.Send(new NActors::IEventHandle(ProxyId, HandlerId, new NHttp::TEvHttpProxy::TEvRegisterHandler("/profile", HandlerId)), 0, true);
    }

    TString Url() const {
        return TStringBuilder() << "https://localhost:" << static_cast<ui16>(Port);
    }

    void ReplyOnce(TString body) {
        TAutoPtr<NActors::IEventHandle> handle;
        auto* request = Runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingRequest>(handle);
        UNIT_ASSERT_VALUES_EQUAL(request->Request->URL, "/profile");
        const TString responseText = TStringBuilder() << "HTTP/1.1 200 OK\r\nContent-Length: " << body.size() << "\r\nConnection: close\r\n\r\n"
                                                      << body;
        auto response = request->Request->CreateResponseString(responseText);
        Runtime.Send(new NActors::IEventHandle(handle->Sender, HandlerId, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response)), 0, true);
    }
};

class TGrpcMock: public NFq::NWasmServices::NTest::MockService::Service {
    std::unique_ptr<grpc::Server> Server;
    int Port = 0;

public:
    std::shared_ptr<TMockState> State = std::make_shared<TMockState>();

    explicit TGrpcMock(std::shared_ptr<grpc::ServerCredentials> credentials = grpc::InsecureServerCredentials()) {
        grpc::ServerBuilder builder;
        builder.AddListeningPort("127.0.0.1:0", std::move(credentials), &Port);
        builder.RegisterService(this);
        Server = builder.BuildAndStart();
        UNIT_ASSERT(Server && Port);
    }

    ~TGrpcMock() {
        Stop();
    }

    void Stop() {
        if (Server) {
            State->Stop();
            Server->Shutdown(std::chrono::system_clock::now());
            Server->Wait();
            Server.reset();
        }
    }

    TString Endpoint() const {
        return TStringBuilder() << "127.0.0.1:" << Port;
    }

    grpc::Status Other(
        grpc::ServerContext* context, const google::protobuf::BytesValue* request, google::protobuf::BytesValue* response) override {
        return Call(context, request, response);
    }

    grpc::Status Call(
        grpc::ServerContext* context, const google::protobuf::BytesValue* request, google::protobuf::BytesValue* response) override {
        TVector<std::pair<TString, TString>> headers;
        for (const auto& [name, value] : context->client_metadata()) {
            headers.emplace_back(TString(name.data(), name.size()), TString(value.data(), value.size()));
        }
        auto exchange = State->Record(request->SerializeAsString(), "/NFq.NWasmServices.NTest.MockService/Call", std::move(headers));
        if (!State->AwaitReply(exchange, [context] {
                return context->IsCancelled();
            })) {
            return grpc::Status(grpc::StatusCode::CANCELLED, "Mock stopped or client cancelled");
        }
        if (exchange->Code) {
            return grpc::Status(static_cast<grpc::StatusCode>(exchange->Code), "Controlled mock error");
        }
        response->set_value(exchange->Reply.data(), exchange->Reply.size());
        return grpc::Status::OK;
    }
};

inline TVector<TBinding> Bindings(const THttpMock& http, const TGrpcMock& grpc,
    std::shared_ptr<grpc::ChannelCredentials> grpcCredentials = grpc::InsecureChannelCredentials()) {
    TBinding rest;
    rest.Endpoint = http.Url();
    rest.Headers = { { "Content-Type", "application/octet-stream" }, { "Authorization", "Bearer host-secret" } };
    TBinding rpc;
    rpc.Protocol = EProtocol::Grpc;
    rpc.Endpoint = grpc.Endpoint();
    rpc.Headers = { { "authorization", "Bearer host-secret" } };
    rpc.GrpcCredentials = grpcCredentials;
    return { std::move(rest), std::move(rpc) };
}

struct TWakeup {
    std::mutex Mutex;
    std::condition_variable Changed;
    ui64 Count = 0;
    bool CleanTls = true;
    const std::thread::id OwnerThread = std::this_thread::get_id();

    void Notify() {
        std::lock_guard lock(Mutex);
        // Wait can notify on the owner thread if completion won the poll/wait
        // race. Only notifications from other threads must have empty guest TLS.
        if (std::this_thread::get_id() != OwnerThread) {
            CleanTls &= !GetCurrentAsyncInvocation() && !GetCurrentQueryCompartment() && !GetCurrentCompartment();
        }
        ++Count;
        Changed.notify_all();
    }
};

struct TEnv {
    std::shared_ptr<TTransport> Transport;
    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc =
        std::make_shared<NKikimr::NMiniKQL::TScopedAlloc>(__LOCATION__, NKikimr::TAlignedPagePoolCounters(), false);
    std::shared_ptr<TWakeup> Wakeup = std::make_shared<TWakeup>();
    TQueryCompartmentHandle* Query = nullptr;
    std::unique_ptr<TRuntime> Runtime;
    TVector<THandle> Ready;

    explicit TEnv(TVector<TBinding> bindings, TTransportLimits limits = {}, TStringBuf resource = "/fq_transport_coroutine.wasm",
        std::shared_ptr<TTransportExecutor> executor = TestExecutor()) {
        Transport = std::make_shared<TTransport>(std::move(bindings), std::move(executor), limits);
        EnsureUdfHostIntrinsicsRegistered();
        KeepAsyncHostIntrinsicsLinked();
        auto query = std::make_unique<TQueryCompartmentHandle>();
        query->Generation = 43;
        query->BridgeNodes = std::make_unique<TWasmBridgeNodeTable>(query->Generation);
        query->Compartment = CreateRegistryCompartment({});
        const auto bytes = NResource::Find(resource);
        const auto object = CompileModuleObjectCode(bytes, EBytecodeFormat::Binary);
        AddPrecompiledModule(query->Compartment.get(), MakeModuleBytecode(bytes, object, EBytecodeFormat::Binary), "FqTransportFixture");
        query->Resident = std::make_unique<TCompartmentResidentCache>(query->Compartment.get());
        Query = query.get();
        Runtime = std::make_unique<TRuntime>(std::move(query), Alloc, Transport, TLimits{}, [wakeup = Wakeup] {
            wakeup->Notify();
        });
    }

    THandle Start(ui64 mode, ui32 a, ui32 b = 1, TString body = Proto("input"), TInstant deadline = TInstant::Now() + TDuration::Seconds(30)) {
        auto bytes = Encode(TArgumentsHeader{ mode, a, b, body.size() }, View(body));
        return Runtime->Start(TStringBuf(bytes.data(), bytes.size()), deadline);
    }

    TCallResult Resume(THandle call) {
        std::unique_lock lock(Wakeup->Mutex);
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        for (;;) {
            auto ready = Runtime->TakeReady();
            Ready.insert(Ready.end(), ready.begin(), ready.end());
            auto position = std::find(Ready.begin(), Ready.end(), call);
            if (position != Ready.end()) {
                Ready.erase(position);
                UNIT_ASSERT(Wakeup->CleanTls);
                UNIT_ASSERT(!GetCurrentAsyncInvocation() && !GetCurrentQueryCompartment() && !GetCurrentCompartment());
                lock.unlock();
                return Runtime->Poll(call);
            }
            UNIT_ASSERT(Wakeup->Changed.wait_until(lock, deadline, [&] {
                return Wakeup->Count;
            }));
            Wakeup->Count = 0;
        }
    }

    TCallResult AwaitTerminal(THandle call) {
        auto result = Runtime->Poll(call);
        while (result.Status == ECallStatus::Waiting || result.Status == ECallStatus::Runnable) {
            result = Resume(call);
        }
        return result;
    }

    void Clean(THandle call) {
        Runtime->Drop(call);
        UNIT_ASSERT_VALUES_EQUAL(Runtime->Stats().Calls, 0);
        UNIT_ASSERT_VALUES_EQUAL(Runtime->Stats().Operations, 0);
        UNIT_ASSERT_VALUES_EQUAL(Runtime->Stats().BufferedBytes, 0);
        {
            auto guard = Guard(*Alloc);
            TCurrentCompartmentGuard compartmentGuard(Query->Compartment.get());
            TCompartmentFunction<ui64()> live(Query->Compartment.get(), "WasmAsyncLiveObjects");
            UNIT_ASSERT_VALUES_EQUAL(live(), 0);
        }
        UNIT_ASSERT_VALUES_EQUAL(Query->BridgeNodes->DebugSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(Query->BridgeNodes->DebugRunScopeDepth(), 0);
        UNIT_ASSERT(!Alloc->IsAttached());
        UNIT_ASSERT(!Runtime->IsPoisoned());
        Eventually([&] {
            return Transport->Stats().Operations == 0;
        });
        UNIT_ASSERT_VALUES_EQUAL(Transport->Stats().ReservedBytes, 0);
    }
};

inline TString ReplyBody(const TCallResult& result, size_t index, int code) {
    TResultHeader header;
    std::string_view responses;
    UNIT_ASSERT(Decode(View(result.Data), header, responses));
    UNIT_ASSERT_VALUES_EQUAL(header.Version, WireVersion);
    UNIT_ASSERT(index < header.Count);
    UNIT_ASSERT(header.FirstBytes <= responses.size());
    UNIT_ASSERT_VALUES_EQUAL(header.FirstBytes + header.SecondBytes, responses.size());
    auto bytes = index == 0 ? responses.substr(0, header.FirstBytes) : responses.substr(header.FirstBytes);
    TResponseHeader response;
    TStringBuf payload;
    UNIT_ASSERT(ParseResponse(TStringBuf(bytes.data(), bytes.size()), response, payload));
    UNIT_ASSERT_VALUES_EQUAL(response.Code, code);
    return TString(payload);
}

inline TResponseHeader ReplyHeader(const TCallResult& result, size_t index) {
    TResultHeader header;
    std::string_view responses;
    UNIT_ASSERT(Decode(View(result.Data), header, responses));
    UNIT_ASSERT_VALUES_EQUAL(header.Version, WireVersion);
    UNIT_ASSERT(index < header.Count);
    UNIT_ASSERT(header.FirstBytes <= responses.size());
    UNIT_ASSERT_VALUES_EQUAL(header.FirstBytes + header.SecondBytes, responses.size());
    const auto bytes = index == 0 ? responses.substr(0, header.FirstBytes) : responses.substr(header.FirstBytes);
    TResponseHeader response;
    TStringBuf payload;
    UNIT_ASSERT(ParseResponse(TStringBuf(bytes.data(), bytes.size()), response, payload));
    return response;
}

struct TDirectReply {
    std::mutex Mutex;
    std::condition_variable Changed;
    bool Done = false;
    EOperationStatus Status = EOperationStatus::Failed;
    TString Bytes;
};

inline std::shared_ptr<TDirectReply> StartDirectHttp(TTransport& transport, const std::shared_ptr<TDirectReply>& reply) {
    auto payload = TString("{\"id\":42}");
    transport.Start(1, EOperationKind::Request, MakeRequest(0, payload, "POST", "/profile"), TInstant::Now() + TDuration::Seconds(5),
        [reply](EOperationStatus status, TString bytes) {
            std::lock_guard lock(reply->Mutex);
            reply->Status = status;
            reply->Bytes = std::move(bytes);
            reply->Done = true;
            reply->Changed.notify_all();
            return true;
        });
    return reply;
}

inline bool HasHeader(const TExchange& exchange, TStringBuf name, TStringBuf value) {
    return std::find(exchange.Headers.begin(), exchange.Headers.end(), std::pair<TString, TString>{ TString(name), TString(value) }) !=
           exchange.Headers.end();
}

struct TCompletions {
    std::mutex Mutex;
    std::condition_variable Changed;
    TVector<EOperationStatus> Statuses;

    void Notify(EOperationStatus status) {
        std::lock_guard lock(Mutex);
        Statuses.push_back(status);
        Changed.notify_all();
    }

    void Wait(size_t count) {
        std::unique_lock lock(Mutex);
        UNIT_ASSERT(Changed.wait_for(lock, std::chrono::seconds(10), [&] {
            return Statuses.size() >= count;
        }));
    }
};

}   // namespace NFq::NWasmServices::NTestHelpers

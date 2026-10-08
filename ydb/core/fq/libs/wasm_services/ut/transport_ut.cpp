#include <ydb/core/fq/libs/wasm_services/transport.h>
#include <ydb/core/fq/libs/wasm_services/ut/protos/mock.grpc.pb.h>

#include <ydb/services/udf_store/wasm/bridge_resident.h>
#include <ydb/services/udf_store/wasm/compile.h>
#include <ydb/services/udf_store/wasm/host.h>
#include <ydb/services/udf_store/wasm/invocation_context.h>
#include <ydb/services/udf_store/wasm/registry_helpers.h>

#include <ydb/library/wasm/api/function.h>

#include <library/cpp/http/server/http_ex.h>
#include <library/cpp/resource/resource.h>
#include <library/cpp/testing/common/network.h>
#include <library/cpp/testing/unittest/registar.h>

#include <grpcpp/grpcpp.h>

#include <util/string/builder.h>
#include <util/stream/str.h>
#include <util/stream/zlib.h>
#include <util/system/datetime.h>

#include <condition_variable>
#include <algorithm>
#include <mutex>

using namespace NFq::NWasmServices;
using namespace NAsync;
using namespace NKikimr::NUdfStore::NWasm;
using namespace NYdb::NWasm;

namespace {

std::string_view View(TStringBuf bytes) {
    return {bytes.data(), bytes.size()};
}

TString Proto(TStringBuf value) {
    google::protobuf::BytesValue message;
    message.set_value(value.data(), value.size());
    return message.SerializeAsString();
}

TString Value(TStringBuf bytes) {
    google::protobuf::BytesValue message;
    UNIT_ASSERT(message.ParseFromArray(bytes.data(), bytes.size()));
    return message.value();
}

template <class F> void Eventually(F condition) {
    const auto deadline = TInstant::Now() + TDuration::Seconds(10);
    while (!condition()) {
        UNIT_ASSERT_C(TInstant::Now() < deadline, "Timed out waiting for physical transport cleanup");
        Sleep(TDuration::MilliSeconds(5));
    }
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
        UNIT_ASSERT_C(Changed.wait_for(lock, std::chrono::seconds(10), [&] { return Requests.size() > index; }),
                      "Mock received no request");
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
        UNIT_ASSERT(Changed.wait_for(lock, std::chrono::seconds(10), [&] { return Requests.at(index)->Cancelled; }));
    }

    void Stop() {
        std::lock_guard lock(Mutex);
        Closing = true;
        Changed.notify_all();
    }
};

class THttpMock : public THttpServer::ICallBack {
    class TRequest : public THttpClientRequestEx {
    public:
        explicit TRequest(THttpMock& owner)
            : Owner(owner)
        {}
        bool Reply(void*) override {
            if (!ProcessHeaders()) {
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
                     << "\r\nConnection: close\r\n";
            if (!exchange->Location.empty()) {
                Output() << "Location: " << exchange->Location << "\r\n";
            }
            if (!exchange->Encoding.empty()) {
                Output() << "Content-Encoding: " << exchange->Encoding << "\r\n";
            }
            Output() << "\r\n";
            Output().Write(exchange->Reply.data(), exchange->Reply.size());
            Output().Finish();
            return true;
        }

    private:
        THttpMock& Owner;
    };

    NTesting::TPortHolder Port = NTesting::GetFreePort();
    std::unique_ptr<THttpServer> Server;

public:
    std::shared_ptr<TMockState> State = std::make_shared<TMockState>();
    THttpMock() {
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
        return TStringBuilder() << "http://127.0.0.1:" << static_cast<ui16>(Port) << "/call";
    }
};

class TGrpcMock : public NFq::NWasmServices::NTest::MockService::Service {
    std::unique_ptr<grpc::Server> Server;
    int Port = 0;

public:
    std::shared_ptr<TMockState> State = std::make_shared<TMockState>();
    TGrpcMock() {
        grpc::ServerBuilder builder;
        builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &Port);
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
    grpc::Status Call(grpc::ServerContext* context, const google::protobuf::BytesValue* request,
                      google::protobuf::BytesValue* response) override {
        TVector<std::pair<TString, TString>> headers;
        for (const auto& [name, value] : context->client_metadata()) {
            headers.emplace_back(TString(name.data(), name.size()), TString(value.data(), value.size()));
        }
        auto exchange = State->Record(request->SerializeAsString(), "/NFq.NWasmServices.NTest.MockService/Call", std::move(headers));
        if (!State->AwaitReply(exchange, [context] { return context->IsCancelled(); })) {
            return grpc::Status(grpc::StatusCode::CANCELLED, "Mock stopped or client cancelled");
        }
        if (exchange->Code) {
            return grpc::Status(static_cast<grpc::StatusCode>(exchange->Code), "Controlled mock error");
        }
        response->set_value(exchange->Reply.data(), exchange->Reply.size());
        return grpc::Status::OK;
    }
};

TVector<TBinding> Bindings(const THttpMock& http, const TGrpcMock& grpc) {
    TBinding rest;
    rest.Endpoint = http.Url();
    rest.Headers = {{"Content-Type", "application/octet-stream"}, {"Authorization", "Bearer host-secret"}};
    TBinding rpc;
    rpc.Protocol = EProtocol::Grpc;
    rpc.Endpoint = grpc.Endpoint();
    rpc.Method = "/NFq.NWasmServices.NTest.MockService/Call";
    rpc.Headers = {{"authorization", "Bearer host-secret"}};
    rpc.GrpcCredentials = grpc::InsecureChannelCredentials();
    return {std::move(rest), std::move(rpc)};
}

struct TWakeup {
    std::mutex Mutex;
    std::condition_variable Changed;
    ui64 Count = 0;
    bool CleanTls = true;
    void Notify() {
        std::lock_guard lock(Mutex);
        CleanTls &= !GetCurrentAsyncInvocation() && !GetCurrentQueryCompartment() && !GetCurrentCompartment();
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

    explicit TEnv(TVector<TBinding> bindings, TTransportLimits limits = {}) {
        Transport = std::make_shared<TTransport>(std::move(bindings), limits);
        EnsureUdfHostIntrinsicsRegistered();
        KeepAsyncHostIntrinsicsLinked();
        auto query = std::make_unique<TQueryCompartmentHandle>();
        query->Generation = 43;
        query->BridgeNodes = std::make_unique<TWasmBridgeNodeTable>(query->Generation);
        query->Compartment = CreateRegistryCompartment({});
        const auto bytes = NResource::Find("/fq_transport_coroutine.wasm");
        const auto object = CompileModuleObjectCode(bytes, EBytecodeFormat::Binary);
        AddPrecompiledModule(query->Compartment.get(), MakeModuleBytecode(bytes, object, EBytecodeFormat::Binary), "FqTransportFixture");
        query->Resident = std::make_unique<TCompartmentResidentCache>(query->Compartment.get());
        Query = query.get();
        Runtime = std::make_unique<TRuntime>(std::move(query), Alloc, Transport, TLimits{}, [wakeup = Wakeup] { wakeup->Notify(); });
    }

    THandle Start(ui64 mode, ui32 a, ui32 b = 1, TString body = Proto("input"),
                  TInstant deadline = TInstant::Now() + TDuration::Seconds(30)) {
        auto bytes = Encode(TArgumentsHeader{mode, a, b, body.size()}, View(body));
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
                lock.unlock();
                return Runtime->Poll(call);
            }
            UNIT_ASSERT(Wakeup->Changed.wait_until(lock, deadline, [&] { return Wakeup->Count; }));
            Wakeup->Count = 0;
        }
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
        Eventually([&] { return Transport->Stats().Operations == 0; });
        UNIT_ASSERT_VALUES_EQUAL(Transport->Stats().ReservedBytes, 0);
    }
};

TString ReplyBody(const TCallResult& result, size_t index, int code) {
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

bool HasHeader(const TExchange& exchange, TStringBuf name, TStringBuf value) {
    return std::find(exchange.Headers.begin(), exchange.Headers.end(), std::pair<TString, TString>{TString(name), TString(value)}) !=
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
        UNIT_ASSERT(Changed.wait_for(lock, std::chrono::seconds(10), [&] { return Statuses.size() >= count; }));
    }
};

} // namespace

Y_UNIT_TEST_SUITE(TFqWasmTransportTest) {
    Y_UNIT_TEST(HttpPostThroughWasmAndNativeHeaders) {
        THttpMock http;
        TGrpcMock grpc;
        TEnv env(Bindings(http, grpc));
        const auto call = env.Start(0, 0);
        UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
        auto request = http.State->Wait();
        UNIT_ASSERT(request->Method.StartsWith("POST /call "));
        UNIT_ASSERT_VALUES_EQUAL(Value(request->Payload), "input");
        UNIT_ASSERT(HasHeader(*request, "Authorization", "Bearer host-secret"));
        http.State->Reply(0, Proto("answer"), 200);
        auto result = env.Resume(call);
        UNIT_ASSERT(result.Status == ECallStatus::Completed);
        UNIT_ASSERT_VALUES_EQUAL(Value(ReplyBody(result, 0, 200)), "answer");
        env.Clean(call);
    }

    Y_UNIT_TEST(GenericGrpcThroughWasmAndNativeCredentials) {
        THttpMock http;
        TGrpcMock grpc;
        TEnv env(Bindings(http, grpc));
        const auto call = env.Start(0, 1);
        UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
        auto request = grpc.State->Wait();
        UNIT_ASSERT_VALUES_EQUAL(Value(request->Payload), "input");
        UNIT_ASSERT(HasHeader(*request, "authorization", "Bearer host-secret"));
        grpc.State->Reply(0, "answer", 0);
        auto result = env.Resume(call);
        UNIT_ASSERT(result.Status == ECallStatus::Completed);
        UNIT_ASSERT_VALUES_EQUAL(Value(ReplyBody(result, 0, 0)), "answer");
        env.Clean(call);
    }

    Y_UNIT_TEST(SequentialHttpThenGrpcUsesFirstResponse) {
        THttpMock http;
        TGrpcMock grpc;
        TEnv env(Bindings(http, grpc));
        const auto call = env.Start(1, 0, 1);
        UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
        http.State->Wait();
        UNIT_ASSERT_VALUES_EQUAL(grpc.State->Count(), 0);
        http.State->Reply(0, Proto("intermediate"), 200);
        UNIT_ASSERT(env.Resume(call).Status == ECallStatus::Waiting);
        UNIT_ASSERT_VALUES_EQUAL(Value(grpc.State->Wait()->Payload), "intermediate");
        grpc.State->Reply(0, "final", 0);
        auto result = env.Resume(call);
        UNIT_ASSERT(result.Status == ECallStatus::Completed);
        UNIT_ASSERT_VALUES_EQUAL(Value(ReplyBody(result, 0, 200)), "intermediate");
        UNIT_ASSERT_VALUES_EQUAL(Value(ReplyBody(result, 1, 0)), "final");
        env.Clean(call);
    }

    Y_UNIT_TEST(SequentialGrpcThenHttpUsesFirstResponse) {
        THttpMock http;
        TGrpcMock grpc;
        TEnv env(Bindings(http, grpc));
        const auto call = env.Start(1, 1, 0);
        UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
        grpc.State->Wait();
        grpc.State->Reply(0, "intermediate", 0);
        UNIT_ASSERT(env.Resume(call).Status == ECallStatus::Waiting);
        UNIT_ASSERT_VALUES_EQUAL(Value(http.State->Wait()->Payload), "intermediate");
        http.State->Reply(0, Proto("final"), 200);
        auto result = env.Resume(call);
        UNIT_ASSERT(result.Status == ECallStatus::Completed);
        UNIT_ASSERT_VALUES_EQUAL(Value(ReplyBody(result, 0, 0)), "intermediate");
        UNIT_ASSERT_VALUES_EQUAL(Value(ReplyBody(result, 1, 200)), "final");
        env.Clean(call);
    }

    Y_UNIT_TEST(ParallelHttpAndGrpcCompleteOutOfOrder) {
        THttpMock http;
        TGrpcMock grpc;
        TEnv env(Bindings(http, grpc));
        const auto call = env.Start(2, 0, 1);
        UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
        http.State->Wait();
        grpc.State->Wait();
        grpc.State->Reply(0, "second", 0);
        UNIT_ASSERT(env.Resume(call).Status == ECallStatus::Waiting);
        http.State->Reply(0, Proto("first"), 200);
        auto result = env.Resume(call);
        UNIT_ASSERT(result.Status == ECallStatus::Completed);
        UNIT_ASSERT_VALUES_EQUAL(Value(ReplyBody(result, 0, 200)), "first");
        UNIT_ASSERT_VALUES_EQUAL(Value(ReplyBody(result, 1, 0)), "second");
        env.Clean(call);
    }

    Y_UNIT_TEST(PendingHttpDoesNotBlockAnotherCall) {
        THttpMock http;
        TGrpcMock grpc;
        TEnv env(Bindings(http, grpc));
        const auto slow = env.Start(0, 0);
        const auto fast = env.Start(0, 1);
        UNIT_ASSERT(env.Runtime->Poll(slow).Status == ECallStatus::Waiting);
        UNIT_ASSERT(env.Runtime->Poll(fast).Status == ECallStatus::Waiting);
        http.State->Wait();
        grpc.State->Wait();
        grpc.State->Reply(0, "fast", 0);
        UNIT_ASSERT(env.Resume(fast).Status == ECallStatus::Completed);
        UNIT_ASSERT(env.Runtime->Poll(slow).Status == ECallStatus::Waiting);
        env.Runtime->Drop(fast);
        env.Runtime->Cancel(slow);
        env.Clean(slow);
    }

    Y_UNIT_TEST(HttpErrorsDoNotRetryOrPoisonGuest) {
        THttpMock http;
        TGrpcMock grpc;
        TEnv env(Bindings(http, grpc));
        size_t index = 0;
        for (const auto code : {429, 503}) {
            const auto call = env.Start(0, 0);
            UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
            http.State->Wait(index);
            http.State->Reply(index++, "controlled error", code);
            auto result = env.Resume(call);
            UNIT_ASSERT(result.Status == ECallStatus::Failed);
            UNIT_ASSERT_VALUES_EQUAL(ReplyBody(result, 0, code), "controlled error");
            env.Clean(call);
            UNIT_ASSERT_VALUES_EQUAL(http.State->Count(), index);
        }
    }

    Y_UNIT_TEST(GrpcUnavailableDoesNotRetry) {
        THttpMock http;
        TGrpcMock grpc;
        TEnv env(Bindings(http, grpc));
        const auto call = env.Start(0, 1);
        UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
        grpc.State->Wait();
        grpc.State->Reply(0, {}, grpc::StatusCode::UNAVAILABLE);
        auto result = env.Resume(call);
        UNIT_ASSERT(result.Status == ECallStatus::Failed);
        ReplyBody(result, 0, grpc::StatusCode::UNAVAILABLE);
        env.Clean(call);
        UNIT_ASSERT_VALUES_EQUAL(grpc.State->Count(), 1);
    }

    Y_UNIT_TEST(HttpDisconnectIsAnOperationError) {
        THttpMock http;
        TGrpcMock grpc;
        TEnv env(Bindings(http, grpc));
        const auto call = env.Start(0, 0);
        UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
        http.State->Wait();
        http.State->Reply(0, {}, 0, true);
        UNIT_ASSERT(env.Resume(call).Status == ECallStatus::Failed);
        env.Clean(call);
    }

    Y_UNIT_TEST(GrpcServerStopsBeforeReply) {
        THttpMock http;
        TGrpcMock grpc;
        TEnv env(Bindings(http, grpc));
        const auto call = env.Start(0, 1);
        UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
        grpc.State->Wait();
        grpc.Stop();
        UNIT_ASSERT(env.Resume(call).Status == ECallStatus::Failed);
        env.Clean(call);
    }

    Y_UNIT_TEST(MalformedApplicationPayloadIsNotReinterpretedByTransport) {
        THttpMock http;
        TGrpcMock grpc;
        TEnv env(Bindings(http, grpc));
        const auto call = env.Start(0, 0);
        UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
        http.State->Wait();
        http.State->Reply(0, "not protobuf", 200);
        auto result = env.Resume(call);
        UNIT_ASSERT(result.Status == ECallStatus::Completed);
        auto bytes = ReplyBody(result, 0, 200);
        google::protobuf::BytesValue message;
        UNIT_ASSERT(!message.ParseFromArray(bytes.data(), bytes.size()));
        env.Clean(call);
        UNIT_ASSERT_VALUES_EQUAL(http.State->Count(), 1);
    }

    Y_UNIT_TEST(GrpcBytesResultCanCarryMalformedApplicationPayload) {
        THttpMock http;
        TGrpcMock grpc;
        TEnv env(Bindings(http, grpc));
        const auto call = env.Start(0, 1);
        UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
        grpc.State->Wait();
        grpc.State->Reply(0, "not protobuf", 0);
        auto result = env.Resume(call);
        UNIT_ASSERT(result.Status == ECallStatus::Completed);
        const auto payload = Value(ReplyBody(result, 0, 0));
        UNIT_ASSERT_VALUES_EQUAL(payload, "not protobuf");
        google::protobuf::BytesValue application;
        UNIT_ASSERT(!application.ParseFromArray(payload.data(), payload.size()));
        env.Clean(call);
        UNIT_ASSERT_VALUES_EQUAL(grpc.State->Count(), 1);
    }

    Y_UNIT_TEST(CancelHttpReleasesPhysicalBuffersBeforeLateServerReply) {
        THttpMock http;
        TGrpcMock grpc;
        TEnv env(Bindings(http, grpc));
        const auto call = env.Start(1, 0, 1);
        UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
        http.State->Wait();
        UNIT_ASSERT(env.Transport->Stats().ReservedBytes > 0);
        env.Runtime->Cancel(call);
        UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Cancelled);
        env.Clean(call);
        http.State->Reply(0, Proto("late"), 200);
        UNIT_ASSERT_VALUES_EQUAL(grpc.State->Count(), 0);
    }

    Y_UNIT_TEST(CancelGrpcPropagatesToServer) {
        THttpMock http;
        TGrpcMock grpc;
        TEnv env(Bindings(http, grpc));
        const auto call = env.Start(0, 1);
        UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
        grpc.State->Wait();
        env.Runtime->Cancel(call);
        grpc.State->WaitCancelled(0);
        env.Clean(call);
    }

    Y_UNIT_TEST(DeadlineCancelsBothProtocols) {
        for (const auto binding : {0u, 1u}) {
            THttpMock http;
            TGrpcMock grpc;
            TEnv env(Bindings(http, grpc));
            const auto deadline = TInstant::Now() + TDuration::Seconds(30);
            const auto call = env.Start(0, binding, 1, Proto("input"), deadline);
            UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
            (binding ? grpc.State : http.State)->Wait();
            UNIT_ASSERT_VALUES_EQUAL(env.Runtime->NextDeadline(), deadline);
            env.Runtime->Expire(deadline);
            UNIT_ASSERT(env.Resume(call).Status == ECallStatus::Cancelled);
            env.Clean(call);
        }
    }

    Y_UNIT_TEST(OversizedHttpResponseIsBounded) {
        THttpMock http;
        TGrpcMock grpc;
        TTransportLimits limits;
        limits.MaxPayloadBytes = 128;
        TEnv env(Bindings(http, grpc), limits);
        const auto call = env.Start(0, 0);
        UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
        http.State->Wait();
        http.State->Reply(0, TString(8192, 'x'), 200);
        UNIT_ASSERT(env.Resume(call).Status == ECallStatus::Failed);
        env.Clean(call);
    }

    Y_UNIT_TEST(OversizedGrpcResponseIsBounded) {
        THttpMock http;
        TGrpcMock grpc;
        TTransportLimits limits;
        limits.MaxPayloadBytes = 128;
        TEnv env(Bindings(http, grpc), limits);
        const auto call = env.Start(0, 1);
        UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
        grpc.State->Wait();
        grpc.State->Reply(0, TString(8192, 'x'), 0);
        UNIT_ASSERT(env.Resume(call).Status == ECallStatus::Failed);
        env.Clean(call);
    }

    Y_UNIT_TEST(CompressedHttpResponseIsBoundedAfterDecompression) {
        THttpMock http;
        TGrpcMock grpc;
        TTransportLimits limits;
        limits.MaxPayloadBytes = 128;
        TEnv env(Bindings(http, grpc), limits);
        const auto call = env.Start(0, 0);
        UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
        http.State->Wait();
        TString compressed;
        TStringOutput output(compressed);
        TZLibCompress gzip(&output, ZLib::GZip);
        gzip.Write(TString(8192, 'x'));
        gzip.Finish();
        UNIT_ASSERT(compressed.size() < limits.MaxPayloadBytes);
        http.State->Reply(0, std::move(compressed), 200, false, {}, "gzip");
        UNIT_ASSERT(env.Resume(call).Status == ECallStatus::Failed);
        env.Clean(call);
    }

    Y_UNIT_TEST(SequentialCallsKeepTheOriginalDeadline) {
        THttpMock http;
        TGrpcMock grpc;
        TEnv env(Bindings(http, grpc));
        const auto deadline = TInstant::Now() + TDuration::Seconds(30);
        const auto call = env.Start(1, 0, 1, Proto("input"), deadline);
        UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
        http.State->Wait();
        http.State->Reply(0, Proto("intermediate"), 200);
        UNIT_ASSERT(env.Resume(call).Status == ECallStatus::Waiting);
        grpc.State->Wait();
        UNIT_ASSERT_VALUES_EQUAL(env.Runtime->NextDeadline(), deadline);
        env.Runtime->Expire(deadline);
        UNIT_ASSERT(env.Resume(call).Status == ECallStatus::Cancelled);
        grpc.State->WaitCancelled(0);
        env.Clean(call);
    }

    Y_UNIT_TEST(OperationQuotaRejectsBeforeSendingAnotherRpc) {
        THttpMock http;
        TGrpcMock grpc;
        TTransportLimits limits;
        limits.MaxOperations = 1;
        TEnv env(Bindings(http, grpc), limits);
        const auto first = env.Start(0, 0);
        UNIT_ASSERT(env.Runtime->Poll(first).Status == ECallStatus::Waiting);
        http.State->Wait();
        const auto second = env.Start(0, 1);
        UNIT_ASSERT(env.Runtime->Poll(second).Status == ECallStatus::Failed);
        UNIT_ASSERT_VALUES_EQUAL(grpc.State->Count(), 0);
        env.Runtime->Drop(second);
        env.Runtime->Cancel(first);
        env.Clean(first);
    }

    Y_UNIT_TEST(BufferQuotaRejectsBeforeNetworkDispatch) {
        THttpMock http;
        TGrpcMock grpc;
        TTransportLimits limits;
        limits.MaxReservedBytes = 1;
        TEnv env(Bindings(http, grpc), limits);
        const auto call = env.Start(0, 0);
        UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Failed);
        UNIT_ASSERT_VALUES_EQUAL(http.State->Count(), 0);
        env.Clean(call);
    }

    Y_UNIT_TEST(GuestCannotSelectAnUnboundEndpoint) {
        THttpMock http;
        TGrpcMock grpc;
        TEnv env(Bindings(http, grpc));
        const auto call = env.Start(0, 99);
        UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Failed);
        UNIT_ASSERT_VALUES_EQUAL(http.State->Count(), 0);
        UNIT_ASSERT_VALUES_EQUAL(grpc.State->Count(), 0);
        env.Clean(call);
    }

    Y_UNIT_TEST(HttpRedirectIsNotFollowed) {
        THttpMock http;
        TGrpcMock grpc;
        TEnv env(Bindings(http, grpc));
        const auto call = env.Start(0, 0);
        UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
        http.State->Wait();
        http.State->Reply(0, {}, 302, false, http.Url());
        UNIT_ASSERT(env.Resume(call).Status == ECallStatus::Failed);
        env.Clean(call);
        UNIT_ASSERT_VALUES_EQUAL(http.State->Count(), 1);
    }

    Y_UNIT_TEST(ForcedOwnerTeardownCancelsPendingFanout) {
        THttpMock http;
        TGrpcMock grpc;
        TEnv env(Bindings(http, grpc));
        const auto call = env.Start(2, 0, 1);
        UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
        http.State->Wait();
        grpc.State->Wait();
        env.Runtime.reset();
        grpc.State->WaitCancelled(0);
        Eventually([&] { return env.Transport->Stats().Operations == 0; });
        UNIT_ASSERT_VALUES_EQUAL(env.Transport->Stats().ReservedBytes, 0);
        http.State->Reply(0, Proto("late"), 200);
    }

    Y_UNIT_TEST(RepeatedNetworkCallsReturnToBaseline) {
        THttpMock http;
        TGrpcMock grpc;
        TEnv env(Bindings(http, grpc));
        for (size_t i = 0; i < 10; ++i) {
            const auto call = env.Start(0, 1);
            UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
            grpc.State->Wait(i);
            grpc.State->Reply(i, "reply", 0);
            UNIT_ASSERT(env.Resume(call).Status == ECallStatus::Completed);
            env.Clean(call);
        }
    }

    Y_UNIT_TEST(NativeTimerUsesTheSharedWorker) {
        TTransport transport({});
        const ui64 delay = 1000;
        auto completions = std::make_shared<TCompletions>();
        auto cancel = transport.Start(1, EOperationKind::Timer, TString(reinterpret_cast<const char*>(&delay), sizeof(delay)),
                                      TInstant::Now() + TDuration::Seconds(10), [completions](EOperationStatus status, TString) {
                                          completions->Notify(status);
                                          return true;
                                      });
        completions->Wait(1);
        UNIT_ASSERT(completions->Statuses[0] == EOperationStatus::Ready);
        cancel();
        Eventually([&] { return transport.Stats().Operations == 0; });
        UNIT_ASSERT_VALUES_EQUAL(transport.Stats().ReservedBytes, 0);
    }

    Y_UNIT_TEST(MaximumNativeGrpcDeadlineDoesNotOverflow) {
        THttpMock http;
        TGrpcMock grpc;
        TTransport transport(Bindings(http, grpc));
        auto completions = std::make_shared<TCompletions>();
        transport.Start(1, EOperationKind::Request, MakeRequest(1, Proto("input")), TInstant::Max(),
                        [completions](EOperationStatus status, TString) {
                            completions->Notify(status);
                            return true;
                        });
        grpc.State->Wait();
        grpc.State->Reply(0, "answer", 0);
        completions->Wait(1);
        UNIT_ASSERT(completions->Statuses[0] == EOperationStatus::Ready);
        Eventually([&] { return transport.Stats().Operations == 0; });
        UNIT_ASSERT_VALUES_EQUAL(transport.Stats().ReservedBytes, 0);
    }

    Y_UNIT_TEST(NativeDeadlineStopsBothClientsWithoutOwnerPolling) {
        THttpMock http;
        TGrpcMock grpc;
        TTransport transport(Bindings(http, grpc));
        auto completions = std::make_shared<TCompletions>();
        const auto deadline = TInstant::Now() + TDuration::Seconds(1);
        for (ui32 binding = 0; binding < 2; ++binding) {
            transport.Start(binding + 1, EOperationKind::Request, MakeRequest(binding, Proto("input")), deadline,
                            [completions](EOperationStatus status, TString) {
                                completions->Notify(status);
                                return true;
                            });
        }
        completions->Wait(2);
        for (const auto status : completions->Statuses) {
            UNIT_ASSERT(status == EOperationStatus::Failed);
        }
        Eventually([&] { return transport.Stats().Operations == 0; });
        UNIT_ASSERT_VALUES_EQUAL(transport.Stats().ReservedBytes, 0);
        UNIT_ASSERT(http.State->Count() <= 1 && grpc.State->Count() <= 1);
    }

    Y_UNIT_TEST(TransportShutdownCancelsAndDrainsBothClients) {
        THttpMock http;
        TGrpcMock grpc;
        auto transport = std::make_unique<TTransport>(Bindings(http, grpc));
        auto completions = std::make_shared<TCompletions>();
        for (ui32 binding = 0; binding < 2; ++binding) {
            transport->Start(binding + 1, EOperationKind::Request, MakeRequest(binding, Proto("input")),
                             TInstant::Now() + TDuration::Seconds(30), [completions](EOperationStatus status, TString) {
                                 completions->Notify(status);
                                 return true;
                             });
        }
        http.State->Wait();
        grpc.State->Wait();
        transport.reset();
        completions->Wait(2);
        UNIT_ASSERT_VALUES_EQUAL(completions->Statuses.size(), 2);
        for (const auto status : completions->Statuses) {
            UNIT_ASSERT(status == EOperationStatus::Cancelled);
        }
    }
}

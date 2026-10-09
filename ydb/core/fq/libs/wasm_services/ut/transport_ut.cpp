#include <ydb/core/fq/libs/wasm_services/transport.h>
#include <ydb/udfs/wasm/profile/profile.h>
#include <ydb/udfs/wasm/profile/contract/service_methods.h>
#include <ydb/udfs/wasm/echo/contract/service_methods.h>
#include <ydb/core/fq/libs/wasm_services/query/manifest.h>
#include <ydb/core/fq/libs/wasm_services/query/query.h>
#include <ydb/core/fq/libs/common/external_service.h>
#include <ydb/library/protobuf_printer/security_printer.h>
#include <ydb/core/fq/libs/wasm_services/ut/protos/mock.grpc.pb.h>
#include <ydb/udfs/wasm/profile/proto/schema/profile.pb.h>
#include <ydb/core/security/certificate_check/test_utils/test_cert_auth_utils.h>
#include <ydb/library/actors/http/http_proxy.h>
#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/library/yql/providers/common/ut_helpers/dq_fake_ca.h>
#include <ydb/library/yql/providers/function/proto/dq_function.pb.h>
#include <yql/essentials/minikql/mkql_node_serialization.h>

#include <ydb/services/udf_store/wasm/bridge_resident.h>
#include <ydb/services/udf_store/wasm/compile.h>
#include <ydb/services/udf_store/wasm/host.h>
#include <ydb/services/udf_store/wasm/invocation_context.h>
#include <ydb/services/udf_store/wasm/registry_helpers.h>

#include <ydb/library/wasm/api/function.h>

#include <library/cpp/http/server/http_ex.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/resource/resource.h>
#include <library/cpp/testing/common/network.h>
#include <library/cpp/testing/unittest/registar.h>

#include <grpcpp/grpcpp.h>
#include <google/protobuf/util/json_util.h>

#include <util/charset/utf8.h>
#include <util/stream/str.h>
#include <util/stream/file.h>
#include <util/stream/zlib.h>
#include <util/string/builder.h>
#include <util/system/datetime.h>
#include <util/system/tempfile.h>

#include <algorithm>
#include <condition_variable>
#include <mutex>

using namespace NFq::NWasmServices;
using namespace NYdb::NWasm::NServices::NProfile;
using namespace NAsync;

static constexpr TStringBuf ProfileTransformType = "WASM_PROFILE";
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
        {
        }
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

class THttpTlsMock {
  public:
    NActors::TTestActorRuntimeBase Runtime{1, true};
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
        if (!validHostname)
            serverProps.AltNames = {"DNS:wrong.test"};
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
        Runtime.Send(new NActors::IEventHandle(ProxyId, HandlerId, new NHttp::TEvHttpProxy::TEvRegisterHandler("/profile", HandlerId)), 0,
                     true);
    }

    TString Url() const {
        return TStringBuilder() << "https://localhost:" << static_cast<ui16>(Port) << "/profile";
    }

    void ReplyOnce(TString body) {
        TAutoPtr<NActors::IEventHandle> handle;
        auto* request = Runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpIncomingRequest>(handle);
        UNIT_ASSERT_VALUES_EQUAL(request->Request->URL, "/profile");
        const TString responseText = TStringBuilder()
                                     << "HTTP/1.1 200 OK\r\nContent-Length: " << body.size() << "\r\nConnection: close\r\n\r\n"
                                     << body;
        auto response = request->Request->CreateResponseString(responseText);
        Runtime.Send(new NActors::IEventHandle(handle->Sender, HandlerId, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response)), 0,
                     true);
    }
};

class TGrpcMock : public NFq::NWasmServices::NTest::MockService::Service {
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

    grpc::Status Lookup(grpc::ServerContext* context, const NFq::NWasmServices::NTest::ProfileRequest* request,
                        NFq::NWasmServices::NTest::ProfileReply* response) override {
        TVector<std::pair<TString, TString>> headers;
        for (const auto& [name, value] : context->client_metadata()) {
            headers.emplace_back(TString(name.data(), name.size()), TString(value.data(), value.size()));
        }
        auto exchange = State->Record(request->SerializeAsString(), "/NFq.NWasmServices.NTest.MockService/Lookup", std::move(headers));
        if (!State->AwaitReply(exchange, [context] { return context->IsCancelled(); })) {
            return grpc::Status(grpc::StatusCode::CANCELLED, "Mock stopped or client cancelled");
        }
        if (exchange->Code) {
            return grpc::Status(static_cast<grpc::StatusCode>(exchange->Code), "Controlled mock error");
        }
        response->set_payload(exchange->Reply.data(), exchange->Reply.size());
        return grpc::Status::OK;
    }

    grpc::Status LookupBatch(grpc::ServerContext* context, const NFq::NWasmServices::NTest::ProfileBatchRequest* request,
                             NFq::NWasmServices::NTest::ProfileBatchReply* response) override {
        TVector<std::pair<TString, TString>> headers;
        for (const auto& [name, value] : context->client_metadata())
            headers.emplace_back(TString(name.data(), name.size()), TString(value.data(), value.size()));
        auto exchange = State->Record(request->SerializeAsString(), "/NFq.NWasmServices.NTest.MockService/LookupBatch", std::move(headers));
        if (!State->AwaitReply(exchange, [context] { return context->IsCancelled(); }))
            return grpc::Status(grpc::StatusCode::CANCELLED, "Mock stopped or client cancelled");
        if (exchange->Code)
            return grpc::Status(static_cast<grpc::StatusCode>(exchange->Code), "Controlled mock error");
        response->set_payload(exchange->Reply.data(), exchange->Reply.size());
        return grpc::Status::OK;
    }
};

TVector<TBinding> Bindings(const THttpMock& http, const TGrpcMock& grpc,
                           std::shared_ptr<grpc::ChannelCredentials> grpcCredentials = grpc::InsecureChannelCredentials()) {
    TBinding rest;
    rest.Endpoint = http.Url();
    rest.Headers = {{"Content-Type", "application/octet-stream"}, {"Authorization", "Bearer host-secret"}};
    TBinding rpc;
    rpc.Protocol = EProtocol::Grpc;
    rpc.Endpoint = grpc.Endpoint();
    rpc.Method = "/NFq.NWasmServices.NTest.MockService/Call";
    rpc.Headers = {{"authorization", "Bearer host-secret"}};
    rpc.GrpcCredentials = grpcCredentials;
    TBinding profileHttp = rest;
    profileHttp.Headers = {{"Content-Type", "application/json"}, {"Authorization", "Bearer host-secret"}};
    TBinding profileGrpc = rpc;
    profileGrpc.Method = "/NFq.NWasmServices.NTest.MockService/Lookup";
    return {std::move(rest), std::move(rpc), std::move(profileHttp), std::move(profileGrpc)};
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

    explicit TEnv(TVector<TBinding> bindings, TTransportLimits limits = {}, TStringBuf resource = "/fq_transport_coroutine.wasm") {
        Transport = std::make_shared<TTransport>(std::move(bindings), limits);
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
        Runtime = std::make_unique<TRuntime>(std::move(query), Alloc, Transport, TLimits{}, [wakeup = Wakeup] { wakeup->Notify(); });
    }

    THandle Start(ui64 mode, ui32 a, ui32 b = 1, TString body = Proto("input"),
                  TInstant deadline = TInstant::Now() + TDuration::Seconds(30)) {
        auto bytes = Encode(TArgumentsHeader{mode, a, b, body.size()}, View(body));
        return Runtime->Start(TStringBuf(bytes.data(), bytes.size()), deadline);
    }

    THandle StartProfile(EProfileMode mode, ui64 id = 42, ui32 a = 2, ui32 b = 3,
                         TInstant deadline = TInstant::Now() + TDuration::Seconds(30)) {
        auto bytes = Encode(TArgumentsHeader{static_cast<ui64>(mode), a, b, sizeof(id)},
                            std::string_view(reinterpret_cast<const char*>(&id), sizeof(id)));
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

    TCallResult AwaitTerminal(THandle call) {
        auto result = Runtime->Poll(call);
        while (result.Status == ECallStatus::Waiting || result.Status == ECallStatus::Runnable)
            result = Resume(call);
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

TResponseHeader ReplyHeader(const TCallResult& result, size_t index) {
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

TProfileResults ProfileResult(const TCallResult& result) {
    UNIT_ASSERT_VALUES_EQUAL(result.Data.size(), sizeof(TProfileResults));
    TProfileResults results;
    std::memcpy(&results, result.Data.data(), sizeof(results));
    UNIT_ASSERT_VALUES_EQUAL(results.Version, ProfileVersion);
    UNIT_ASSERT(results.Count >= 1 && results.Count <= 2);
    return results;
}

TString GrpcProfilePayload(ui64 id = 42, TString name = "Ada", ui32 score = 97, ui32 version = ProfileVersion) {
    NFq::NWasmServices::NTest::Profile profile;
    profile.set_id(id);
    profile.set_name(name);
    profile.set_score(score);
    profile.set_version(version);
    return profile.SerializeAsString();
}

TString GrpcBatchPayload(const TVector<ui64>& ids, TString name = "Ada", ui32 score = 97) {
    NFq::NWasmServices::NTest::ProfileBatchPayload payload;
    for (const auto id : ids) {
        auto* profile = payload.add_profiles();
        profile->set_id(id);
        profile->set_name(name);
        profile->set_score(score);
        profile->set_version(ProfileVersion);
    }
    return payload.SerializeAsString();
}

TString HttpBatchPayload(const TVector<ui64>& ids) {
    TStringBuilder text;
    text << "[";
    for (size_t i = 0; i < ids.size(); ++i) {
        if (i)
            text << ",";
        text << "{\"id\":" << ids[i] << ",\"name\":\"Ada\",\"score\":97,\"version\":1}";
    }
    text << "]";
    return text;
}

struct TDirectReply {
    std::mutex Mutex;
    std::condition_variable Changed;
    bool Done = false;
    EOperationStatus Status = EOperationStatus::Failed;
    TString Bytes;
};

std::shared_ptr<TDirectReply> StartDirectHttp(TTransport& transport, const std::shared_ptr<TDirectReply>& reply) {
    auto payload = TString("{\"id\":42}");
    transport.Start(1, EOperationKind::Request, MakeRequest(0, payload), TInstant::Now() + TDuration::Seconds(5),
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

struct TQueryOutput {
    std::mutex Mutex;
    TVector<TProfile> Rows;
    std::atomic<bool> Blocked = false;
    std::atomic<bool> Finished = false;
    std::atomic<ui32> BlockAfterRows = 0;
};

class TProfileConsumer final : public NYql::NDq::IDqOutputConsumer {
  public:
    explicit TProfileConsumer(std::shared_ptr<TQueryOutput> output)
        : Output(std::move(output))
    {
    }
    NYql::NDq::EDqFillLevel GetFillLevel() const override {
        return Output->Blocked ? NYql::NDq::HardLimit : NYql::NDq::NoLimit;
    }
    void Consume(NYql::NUdf::TUnboxedValue&& row) override {
        TProfile profile;
        profile.Id = row.GetElement(0).Get<ui64>();
        const auto nameValue = row.GetElement(1);
        const auto name = nameValue.AsStringRef();
        profile.NameBytes = name.Size();
        std::memcpy(profile.Name, name.Data(), name.Size());
        profile.Score = row.GetElement(2).Get<ui32>();
        std::lock_guard lock(Output->Mutex);
        Output->Rows.push_back(profile);
        if (Output->BlockAfterRows && Output->Rows.size() >= Output->BlockAfterRows)
            Output->Blocked = true;
    }
    void WideConsume(NYql::NUdf::TUnboxedValue[], ui32) override {
        UNIT_FAIL("Unexpected wide Profile output");
    }
    void Consume(NYql::NDqProto::TCheckpoint&&) override {
        UNIT_FAIL("Unexpected checkpoint");
    }
    void Consume(NYql::NDqProto::TWatermark&&) override {
        UNIT_FAIL("Unexpected watermark");
    }
    void Finish() override {
        Output->Finished = true;
    }
    void Flush() override {
    }
    bool IsFinished() const override {
        return Output->Finished;
    }
    bool IsEarlyFinished() const override {
        return false;
    }

  private:
    std::shared_ptr<TQueryOutput> Output;
};

FederatedQuery::Connection TestServiceConnection(const TString& name = "service") {
    FederatedQuery::Connection connection;
    connection.mutable_meta()->set_id(name);
    connection.mutable_content()->set_name(name);
    auto* service = connection.mutable_content()->mutable_setting()->mutable_external_service();
    service->set_protocol(FederatedQuery::ExternalService::HTTP);
    service->set_endpoint("http://unused");
    service->set_insecure(true);
    service->mutable_auth()->mutable_none();
    return connection;
}

THashMap<TString, FederatedQuery::Connection> TestServiceConnections(const TString& name = "service") {
    return {{name, TestServiceConnection(name)}};
}

struct TQueryTransformEnv {
    TTempFile Module;
    TTempFile Manifest;
    NYql::NDq::TDqAsyncIoFactory Factory;
    std::shared_ptr<TQueryOutput> Output = std::make_shared<TQueryOutput>();
    NYql::NDq::TFakeCASetup Setup;

    TQueryTransformEnv(const TBinding& binding, ui32 timeoutMs = 5000, ui32 batchRows = 1, ui32 batchBytes = 0,
                       ui32 maxBufferedRows = 65536, ui64 maxBufferedBytes = 0, bool grpcInsecure = true)
        : Module(MakeTempName()), Manifest(MakeTempName())
    {
        TFileOutput(Module.Name()).Write(NResource::Find("/fq_transport_coroutine.wasm"));
        TFileOutput(Manifest.Name()).Write(NResource::Find("/ydb/udfs/wasm/profile/manifest.json"));
        NFq::NConfig::TWasmServicesConfig config;
        config.SetEnabled(true);
        auto* module = config.AddModules();
        module->SetModulePath(Module.Name());
        module->SetManifestPath(Manifest.Name());
        config.SetCallTimeoutMs(timeoutMs);
        config.SetMaxBatchRows(batchRows);
        config.SetMaxBatchBytes(batchBytes);
        config.SetMaxBufferedRows(maxBufferedRows);
        config.SetMaxBufferedBytes(maxBufferedBytes);
        auto connection = TestServiceConnection("profiles");
        auto* entry = connection.mutable_content()->mutable_setting()->mutable_external_service();
        entry->set_endpoint(binding.Endpoint);
        entry->set_method(binding.Method);
        entry->set_ca_certificate(binding.CaCertificate);
        if (!binding.CaFile.empty())
            entry->set_ca_certificate(TFileInput(binding.CaFile).ReadAll());
        entry->set_insecure(binding.Protocol == EProtocol::Grpc ? grpcInsecure : binding.Endpoint.StartsWith("http://"));
        for (const auto& [key, value] : binding.Headers) {
            if (key == "Authorization" || key == "authorization")
                entry->mutable_auth()->mutable_token()->set_token(value.substr(7));
            else
                (*entry->mutable_headers())[key] = value;
        }
        if (binding.Protocol == EProtocol::Grpc) {
            entry->set_protocol(FederatedQuery::ExternalService::GRPC);
            if (batchRows > 1)
                entry->set_method(binding.Method + "Batch");
        }
        auto gateway = CreateServiceGatewayFactory(config, {{"profiles", connection}});
        auto description =
            gateway->CreateDqFunctionGateway(TString(ProfileTransformType), {}, "profiles")->ResolveFunction({}, "Profile").GetValueSync();
        UNIT_ASSERT_STRING_CONTAINS(description.InvokeUrl, "profiles");
        UNIT_ASSERT_EXCEPTION(gateway->CreateDqFunctionGateway(TString(ProfileTransformType), {}, "missing"), yexception);
        RegisterServiceTransforms(Factory, config);
        Setup.Execute([&](NYql::NDq::TFakeActor& actor) {
            using namespace NKikimr::NMiniKQL;
            auto* input =
                TStructTypeBuilder(actor.TypeEnv).Add("id", TDataType::Create(NYql::NUdf::TDataType<ui64>::Id, actor.TypeEnv)).Build();
            auto* output = TStructTypeBuilder(actor.TypeEnv)
                               .Add("id", TDataType::Create(NYql::NUdf::TDataType<ui64>::Id, actor.TypeEnv))
                               .Add("name", TDataType::Create(NYql::NUdf::TDataType<NYql::NUdf::TUtf8>::Id, actor.TypeEnv))
                               .Add("score", TDataType::Create(NYql::NUdf::TDataType<ui32>::Id, actor.TypeEnv))
                               .Build();
            NYql::NDqProto::TTaskOutput desc;
            desc.MutableTransform()->SetType(TString(ProfileTransformType));
            desc.MutableTransform()->SetInputType(SerializeNode(input, actor.TypeEnv));
            desc.MutableTransform()->SetOutputType(SerializeNode(output, actor.TypeEnv));
            NYql::NProto::TFunctionTransform settings;
            settings.SetInvokeUrl(description.InvokeUrl);
            desc.MutableTransform()->MutableSettings()->PackFrom(settings);
            THashMap<TString, TString> params;
            params[ServiceConnectionKey("profiles")] = PrepareServiceConnection(connection, {});
            auto [sink, sinkActor] = Factory.CreateDqOutputTransform(
                {.OutputDesc = desc, .OutputIndex = 0, .StatsLevel = NYql::NDq::None, .TxId = {}, .TaskId = 1,
                 .TransformOutput = new TProfileConsumer(Output),
                 .Callback = &actor.GetAsyncOutputCallbacks(),
                 .SecureParams = params,
                 .TaskParams = params,
                 .TypeEnv = actor.TypeEnv,
                 .HolderFactory = actor.HolderFactory,
                 .Alloc = std::shared_ptr<TScopedAlloc>(&actor.Alloc, [](auto*) {}),
                 .TraceId = {}});
            actor.InitAsyncOutput(sink, sinkActor);
        });
    }

    void Start(TVector<ui64> ids = {42}, bool finished = true) {
        Setup.AsyncOutputWrite([&](NKikimr::NMiniKQL::THolderFactory& holders) {
            NKikimr::NMiniKQL::TUnboxedValueBatch batch;
            for (const auto id : ids) {
                NYql::NUdf::TUnboxedValue* members;
                auto row = holders.CreateDirectArrayHolder(1, members);
                members[0] = NYql::NUdf::TUnboxedValuePod(id);
                batch.emplace_back(std::move(row));
            }
            return batch;
        }, Nothing(), finished);
    }
};

} // namespace

Y_UNIT_TEST_SUITE(TFqWasmTransportTest) {
    Y_UNIT_TEST(EchoModuleLinksAndRejectsUnknownMethod) {
        TEnv env({}, {}, "/echo_service.wasm");
        const NYdb::NWasm::NServices::TServiceRequest request{
            NYdb::NWasm::NServices::ServiceMagic, NYdb::NWasm::NServices::ServiceVersion, 999, 0, 0, 1, 32768, 0};
        const auto call = env.Runtime->Start(TStringBuf(reinterpret_cast<const char*>(&request), sizeof(request)),
                                             TInstant::Now() + TDuration::Seconds(30));
        const auto result = env.AwaitTerminal(call);
        UNIT_ASSERT(result.Status == ECallStatus::Completed);
        NYdb::NWasm::NServices::TRowReader reader(View(result.Data));
        NYdb::NWasm::NServices::TServiceResult reply;
        UNIT_ASSERT(reader.Get(reply));
        UNIT_ASSERT_VALUES_EQUAL(reply.Error, 1);
        UNIT_ASSERT(reader.Remaining().empty());
        env.Clean(call);
    }

    Y_UNIT_TEST(EchoModuleHttpBatchDecodesDifferentSchemas) {
        using namespace NYdb::NWasm::NServices;
        THttpMock http;
        TGrpcMock grpc;
        TEnv env(Bindings(http, grpc), {}, "/echo_service.wasm");
        const std::string_view messages[] = {std::string_view("a\0b", 3), std::string_view()};
        size_t exchange = 0;
        using namespace NYdb::NWasm::NServices::NGenerated::NModuleEcho;
        for (const ui32 method : {MethodEcho, MethodLength}) {
            char arguments[256];
            TRowWriter writer(arguments, sizeof(arguments));
            UNIT_ASSERT(writer.Put(TServiceRequest{ServiceMagic, ServiceVersion, method, 0, 0, 2, 32768, 1}));
            for (const auto message : messages)
                UNIT_ASSERT(writer.String(message));
            const auto call = env.Runtime->Start(TStringBuf(arguments, writer.Size()), TInstant::Now() + TDuration::Seconds(30));
            UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
            const auto request = http.State->Wait(exchange);
            http.State->Reply(exchange++, request->Payload, 200);
            const auto result = env.AwaitTerminal(call);
            UNIT_ASSERT(result.Status == ECallStatus::Completed);
            TRowReader reader(View(result.Data));
            TServiceResult reply;
            UNIT_ASSERT(reader.Get(reply));
            UNIT_ASSERT_VALUES_EQUAL(reply.Version, ServiceVersion);
            UNIT_ASSERT_VALUES_EQUAL(reply.Count, 2);
            UNIT_ASSERT_VALUES_EQUAL(reply.Error, 0);
            for (const auto expected : messages) {
                if (method == MethodEcho) {
                    std::string_view value;
                    ui64 length;
                    ui8 empty;
                    i64 delta;
                    UNIT_ASSERT(reader.String(value, 1024) && reader.Get(length) && reader.Get(empty) && reader.Get(delta));
                    UNIT_ASSERT(value == expected);
                    UNIT_ASSERT_VALUES_EQUAL(length, expected.size());
                    UNIT_ASSERT_VALUES_EQUAL(empty, expected.empty());
                    UNIT_ASSERT_VALUES_EQUAL(delta, -static_cast<i64>(expected.size()));
                } else {
                    ui32 length;
                    UNIT_ASSERT(reader.Get(length));
                    UNIT_ASSERT_VALUES_EQUAL(length, expected.size());
                }
            }
            UNIT_ASSERT(reader.Remaining().empty());
            env.Clean(call);
        }
    }

    Y_UNIT_TEST(ServiceManifestDescribesDifferentModules) {
        const auto profile = ParseServiceManifest(NResource::Find("/ydb/udfs/wasm/profile/manifest.json"));
        const auto echo = ParseServiceManifest(NResource::Find("/ydb/udfs/wasm/echo/manifest.json"));
        UNIT_ASSERT_VALUES_EQUAL(profile.Name, "WASM_PROFILE");
        UNIT_ASSERT_VALUES_EQUAL(profile.Methods.at("Profile").Input[0].Name, "id");
        UNIT_ASSERT_VALUES_EQUAL(echo.Name, "Echo");
        UNIT_ASSERT_VALUES_EQUAL(echo.Methods.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(echo.Methods.at("Echo").Id, NYdb::NWasm::NServices::NGenerated::NModuleEcho::MethodEcho);
        UNIT_ASSERT_VALUES_EQUAL(echo.Methods.at("Length").Id, NYdb::NWasm::NServices::NGenerated::NModuleEcho::MethodLength);
        UNIT_ASSERT_VALUES_EQUAL(profile.Methods.at("Profile").Id, NYdb::NWasm::NServices::NGenerated::NModuleWASM_PROFILE::MethodProfile);
        UNIT_ASSERT_VALUES_EQUAL(echo.Methods.at("Echo").Output.size(), 4);
        UNIT_ASSERT_VALUES_EQUAL(echo.Methods.at("Length").Output.size(), 1);
    }

    Y_UNIT_TEST(ServiceGeneratedDispatchIgnoresManifestOrder) {
        NJson::TJsonValue root;
        UNIT_ASSERT(NJson::ReadJsonTree(NResource::Find("/ydb/udfs/wasm/echo/manifest.json"), &root, true));
        for (const auto& method : root["service_methods"].GetArraySafe())
            UNIT_ASSERT(!method.Has("id"));
        auto& methods = root["service_methods"].GetArraySafe();
        std::swap(methods[0], methods[1]);
        auto additional = methods[0];
        additional["name"] = "Additional";
        methods.push_back(std::move(additional));
        const auto changed = ParseServiceManifest(NJson::WriteJson(root, false));
        UNIT_ASSERT_VALUES_EQUAL(changed.Methods.at("Echo").Id, NYdb::NWasm::NServices::NGenerated::NModuleEcho::MethodEcho);
        UNIT_ASSERT_VALUES_EQUAL(changed.Methods.at("Length").Id, NYdb::NWasm::NServices::NGenerated::NModuleEcho::MethodLength);
    }

    Y_UNIT_TEST(ServiceRowContractRejectsOldDispatchVersion) {
        using namespace NYdb::NWasm::NServices;
        TServiceRequest request;
        request.Count = 1;
        request.Version = 1;
        const auto bytes = Encode(request, {});
        TServiceRequest decoded;
        std::string_view rows;
        UNIT_ASSERT(!ReadServiceRequest({bytes.data(), bytes.size()}, decoded, rows));
    }

    Y_UNIT_TEST(ServiceManifestRejectsInvalidContracts) {
        const TVector<std::function<void(NJson::TJsonValue&)>> mutations{
            [](auto& root) { root["service_abi_version"] = 1; },
            [](auto& root) { root["service_methods"][1]["id"] = 7; },
            [](auto& root) { root["service_methods"][1]["name"] = "Echo"; },
            [](auto& root) { root["service_methods"][0]["input"][0]["type"] = "Optional<String>"; },
            [](auto& root) { root["service_methods"][0]["input"][0]["max_bytes"] = 0; },
            [](auto& root) { root["service_methods"][0]["max_output_row_bytes"] = 1; },
            [](auto& root) { root["service_methods"][0]["max_batch_rows"] = 65; },
            [](auto& root) { root["service_methods"][0]["max_batch_rows"] = 0; },
            [](auto& root) { root["service_methods"][0]["batch"] = false; },
            [](auto& root) { root["service_methods"][0]["output"][1]["name"] = "value"; },
            [](auto& root) {
                root["service_methods"][0]["name"] = "M15119";
                root["service_methods"][1]["name"] = "M203802";
            },
        };
        for (const auto& mutate : mutations) {
            NJson::TJsonValue root;
            UNIT_ASSERT(NJson::ReadJsonTree(NResource::Find("/ydb/udfs/wasm/echo/manifest.json"), &root, true));
            mutate(root);
            UNIT_ASSERT_EXCEPTION(ParseServiceManifest(NJson::WriteJson(root, false)), yexception);
        }
    }

    Y_UNIT_TEST(ServiceRegistrySelectsModuleAndMethod) {
        TTempFile profile(MakeTempName()), echo(MakeTempName());
        TFileOutput(profile.Name()).Write(NResource::Find("/ydb/udfs/wasm/profile/manifest.json"));
        TFileOutput(echo.Name()).Write(NResource::Find("/ydb/udfs/wasm/echo/manifest.json"));
        NFq::NConfig::TWasmServicesConfig config;
        config.SetEnabled(true);
        for (const auto& manifest : {profile.Name(), echo.Name()}) {
            auto* module = config.AddModules();
            module->SetModulePath("unused");
            module->SetManifestPath(manifest);
        }
        const auto factory = CreateServiceGatewayFactory(config, TestServiceConnections());
        auto gateway = factory->CreateDqFunctionGateway("Echo", {}, "service");
        const auto description = gateway->ResolveFunction({}, "Length").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(description.Type, "Echo");
        UNIT_ASSERT_STRING_CONTAINS(description.InvokeUrl, "Length");
        UNIT_ASSERT_EXCEPTION(gateway->ResolveFunction({}, "Profile"), yexception);
        UNIT_ASSERT_EXCEPTION(factory->CreateDqFunctionGateway("Echo", {}, "missing"), yexception);
        const auto hidden = CreateServiceGatewayFactory(config, {});
        UNIT_ASSERT_EXCEPTION(hidden->CreateDqFunctionGateway("Echo", {}, "service"), yexception);
        auto wrongType = TestServiceConnections();
        wrongType.at("service").mutable_content()->mutable_setting()->mutable_monitoring();
        UNIT_ASSERT_EXCEPTION(CreateServiceGatewayFactory(config, wrongType)->CreateDqFunctionGateway("Echo", {}, "service"), yexception);
        UNIT_ASSERT(!description.InvokeUrl.Contains("http://"));
        config.MutableModules(1)->SetManifestPath(profile.Name());
        UNIT_ASSERT_EXCEPTION(CreateServiceGatewayFactory(config, TestServiceConnections()), yexception);
        config.MutableModules(1)->SetManifestPath(echo.Name());
        config.SetMaxBufferedBytes(1);
        UNIT_ASSERT_EXCEPTION(
            CreateServiceGatewayFactory(config, TestServiceConnections())->CreateDqFunctionGateway("Echo", {}, "service")->ResolveFunction({}, "Echo"), yexception);
        config.SetMaxBufferedBytes(0);
        config.SetMaxBatchBytes(592);
        const auto limited = CreateServiceGatewayFactory(config, TestServiceConnections());
        UNIT_ASSERT(limited->CreateDqFunctionGateway("WASM_PROFILE", {}, "service")->ResolveFunction({}, "Profile").GetValueSync().Type ==
                    "WASM_PROFILE");
        UNIT_ASSERT_EXCEPTION(limited->CreateDqFunctionGateway("Echo", {}, "service")->ResolveFunction({}, "Echo"), yexception);
        config.SetMaxBatchBytes(0);
        config.SetModulePath("ambiguous");
        UNIT_ASSERT_EXCEPTION(CreateServiceGatewayFactory(config, TestServiceConnections()), yexception);
    }

    Y_UNIT_TEST(ExternalServiceValidationAndCredentialIsolation) {
        auto connection = TestServiceConnection("profiles");
        auto& service = *connection.mutable_content()->mutable_setting()->mutable_external_service();
        UNIT_ASSERT(NFq::ValidateExternalService(service, false).Empty());
        service.set_insecure(false);
        UNIT_ASSERT(!NFq::ValidateExternalService(service, false).Empty());
        service.set_endpoint("https://service.test/profile");
        UNIT_ASSERT(NFq::ValidateExternalService(service, false).Empty());
        for (const auto& url : {"https://user:secret@service.test/profile", "https:///profile", "file:///etc/passwd",
                                "https://service.test/#fragment", "https://service.test/\r\n"}) {
            service.set_endpoint(url);
            UNIT_ASSERT_C(!NFq::ValidateExternalService(service, false).Empty(), url);
        }
        service.set_endpoint("https://service.test/profile");
        service.mutable_auth()->mutable_current_iam();
        UNIT_ASSERT(!NFq::ValidateExternalService(service, true).Empty());
        UNIT_ASSERT_EXCEPTION(PrepareServiceConnection(connection, {}), yexception);
        FederatedQuery::ExternalService resolved;
        service.set_ca_certificate(TString(4096, 'A'));
        const auto prepared = PrepareServiceConnection(connection, "host-secret");
        UNIT_ASSERT(IsUtf(prepared));
        google::protobuf::StringValue envelope;
        envelope.set_value(prepared);
        google::protobuf::StringValue received;
        UNIT_ASSERT(received.ParseFromString(envelope.SerializeAsString()));
        UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(received.value(), &resolved).ok());
        UNIT_ASSERT_VALUES_EQUAL(resolved.ca_certificate(), service.ca_certificate());
        UNIT_ASSERT_VALUES_EQUAL(resolved.auth().token().token(), "host-secret");
        UNIT_ASSERT(service.auth().has_current_iam());
        service.mutable_auth()->mutable_token()->set_token("host-secret");
        (*service.mutable_headers())["X-Api-Key"] = "header-secret";
        const auto printed = NKikimr::SecureDebugString(connection);
        UNIT_ASSERT(!printed.Contains("host-secret"));
        UNIT_ASSERT(!printed.Contains("header-secret"));
        (*service.mutable_headers())["Authorization"] = "override";
        UNIT_ASSERT(!NFq::ValidateExternalService(service, false).Empty());
        service.clear_headers();
        service.mutable_auth()->mutable_service_account()->set_id("service-account");
        UNIT_ASSERT(!NFq::ValidateExternalService(service, false).Empty());
        service.mutable_auth()->mutable_none();
        service.set_protocol(FederatedQuery::ExternalService::GRPC);
        service.set_endpoint("localhost:443");
        service.set_method("/package.Service/Call");
        UNIT_ASSERT(NFq::ValidateExternalService(service, false).Empty());
        service.set_endpoint("dns:///localhost:443");
        UNIT_ASSERT(!NFq::ValidateExternalService(service, false).Empty());
    }

    Y_UNIT_TEST(ServiceRowWireBounds) {
        using namespace NYdb::NWasm::NServices;
        char buffer[128];
        TRowWriter writer(buffer, sizeof(buffer));
        UNIT_ASSERT(writer.Put(ui64(Max<ui64>())));
        UNIT_ASSERT(writer.Put(i64(-7)));
        UNIT_ASSERT(writer.String(std::string_view("a\0b", 3)));
        UNIT_ASSERT(writer.String({}));
        TRowReader reader({buffer, writer.Size()});
        ui64 unsignedValue;
        i64 signedValue;
        std::string_view text;
        UNIT_ASSERT(reader.Get(unsignedValue) && unsignedValue == Max<ui64>());
        UNIT_ASSERT(reader.Get(signedValue) && signedValue == -7);
        UNIT_ASSERT(reader.String(text, 3) && text == std::string_view("a\0b", 3));
        UNIT_ASSERT(reader.String(text, 3) && text.empty());
        UNIT_ASSERT(reader.Remaining().empty());
        UNIT_ASSERT(!reader.Get(unsignedValue));
        TRowReader truncated({buffer, 1});
        UNIT_ASSERT(!truncated.Get(unsignedValue));
        TRowWriter shortWriter(buffer, 3);
        UNIT_ASSERT(!shortWriter.String("x"));
        TServiceRequest header;
        header.Count = 1;
        auto bytes = Encode(header, std::string_view(buffer, 8));
        std::string_view rows;
        UNIT_ASSERT(ReadServiceRequest(bytes, header, rows));
        header.Count = MaxServiceBatchRows + 1;
        UNIT_ASSERT(!ReadServiceRequest(Encode(header), header, rows));
    }

    Y_UNIT_TEST(ServiceSingleModuleShorthandUsesAdjacentManifest) {
        TTempFile artifact(MakeTempName());
        TTempFile manifest(artifact.Name() + ".manifest.json");
        TFileOutput(manifest.Name()).Write(NResource::Find("/ydb/udfs/wasm/profile/manifest.json"));
        NFq::NConfig::TWasmServicesConfig config;
        config.SetEnabled(true);
        config.SetModulePath(artifact.Name());
        const auto factory = CreateServiceGatewayFactory(config, TestServiceConnections());
        UNIT_ASSERT_VALUES_EQUAL(
            factory->CreateDqFunctionGateway("WASM_PROFILE", {}, "service")->ResolveFunction({}, "Profile").GetValueSync().Type,
            "WASM_PROFILE");
    }

    Y_UNIT_TEST(DqBatchByteQuotaReservesOutputRows) {
        THttpMock http;
        TGrpcMock grpc;
        TQueryTransformEnv env(Bindings(http, grpc)[2], 5000, 2, 0, 10, 2 * sizeof(TProfileResult));
        env.Output->BlockAfterRows = 1;
        env.Setup.Execute(
            [](NYql::NDq::TFakeActor& actor) { UNIT_ASSERT_VALUES_EQUAL(actor.DqAsyncOutput->GetFreeSpace(), 2 * sizeof(ui64)); });
        env.Start({42, 43}, false);
        http.State->Wait();
        http.State->Reply(0, HttpBatchPayload({42, 43}), 200);
        Eventually([&] { return env.Output->Blocked.load(); });
        env.Setup.Execute(
            [](NYql::NDq::TFakeActor& actor) { UNIT_ASSERT_VALUES_EQUAL(actor.DqAsyncOutput->GetFreeSpace(), sizeof(ui64)); });
        env.Start({44});
        env.Output->BlockAfterRows = 0;
        env.Output->Blocked = false;
        env.Setup.Execute([](NYql::NDq::TFakeActor& actor) { actor.DqAsyncOutput->OnOutputConsumerReady(); });
        UNIT_ASSERT_VALUES_EQUAL(http.State->Wait(1)->Payload, "{\"ids\":[44]}");
        http.State->Reply(1, HttpBatchPayload({44}), 200);
        Eventually([&] { return env.Output->Finished.load(); });
        std::lock_guard lock(env.Output->Mutex);
        UNIT_ASSERT_VALUES_EQUAL(env.Output->Rows.size(), 3);
    }

    Y_UNIT_TEST(DqBatchQuotaCountsActiveAndUndeliveredRows) {
        THttpMock http;
        TGrpcMock grpc;
        TQueryTransformEnv env(Bindings(http, grpc)[2], 5000, 2, 0, 5);
        env.Output->BlockAfterRows = 1;
        env.Start({42, 43, 44}, false);
        UNIT_ASSERT_VALUES_EQUAL(http.State->Wait()->Payload, "{\"ids\":[42,43]}");
        env.Setup.Execute(
            [](NYql::NDq::TFakeActor& actor) { UNIT_ASSERT_VALUES_EQUAL(actor.DqAsyncOutput->GetFreeSpace(), 2 * sizeof(ui64)); });
        http.State->Reply(0, HttpBatchPayload({42, 43}), 200);
        Eventually([&] { return env.Output->Blocked.load(); });
        env.Setup.Execute(
            [](NYql::NDq::TFakeActor& actor) { UNIT_ASSERT_VALUES_EQUAL(actor.DqAsyncOutput->GetFreeSpace(), 3 * sizeof(ui64)); });
        env.Start({45, 46, 47});
        UNIT_ASSERT_VALUES_EQUAL(http.State->Count(), 1);
        env.Output->BlockAfterRows = 0;
        env.Output->Blocked = false;
        env.Setup.Execute([](NYql::NDq::TFakeActor& actor) { actor.DqAsyncOutput->OnOutputConsumerReady(); });
        UNIT_ASSERT_VALUES_EQUAL(http.State->Wait(1)->Payload, "{\"ids\":[44,45]}");
        http.State->Reply(1, HttpBatchPayload({44, 45}), 200);
        UNIT_ASSERT_VALUES_EQUAL(http.State->Wait(2)->Payload, "{\"ids\":[46,47]}");
        http.State->Reply(2, HttpBatchPayload({46, 47}), 200);
        Eventually([&] { return env.Output->Finished.load(); });
        std::lock_guard lock(env.Output->Mutex);
        UNIT_ASSERT_VALUES_EQUAL(env.Output->Rows.size(), 6);
        for (size_t i = 0; i < env.Output->Rows.size(); ++i)
            UNIT_ASSERT_VALUES_EQUAL(env.Output->Rows[i].Id, 42 + i);
        UNIT_ASSERT_VALUES_EQUAL(http.State->Count(), 3);
    }

    Y_UNIT_TEST(DqHttpBatchSplitsAndHonorsBackpressure) {
        THttpMock http;
        TGrpcMock grpc;
        TQueryTransformEnv env(Bindings(http, grpc)[2], 5000, 2);
        env.Output->BlockAfterRows = 1;
        env.Start({42, 42, 43, 44, 45});
        auto request = http.State->Wait();
        UNIT_ASSERT_VALUES_EQUAL(request->Payload, "{\"ids\":[42,42]}");
        UNIT_ASSERT(HasHeader(*request, "Authorization", "Bearer host-secret"));
        http.State->Reply(0, HttpBatchPayload({42, 42}), 200);
        Eventually([&] { return env.Output->Blocked.load(); });
        {
            std::lock_guard lock(env.Output->Mutex);
            UNIT_ASSERT_VALUES_EQUAL(env.Output->Rows.size(), 1);
        }
        UNIT_ASSERT_VALUES_EQUAL(http.State->Count(), 1);
        env.Output->BlockAfterRows = 0;
        env.Output->Blocked = false;
        env.Setup.Execute([](NYql::NDq::TFakeActor& actor) { actor.DqAsyncOutput->OnOutputConsumerReady(); });
        UNIT_ASSERT_VALUES_EQUAL(http.State->Wait(1)->Payload, "{\"ids\":[43,44]}");
        http.State->Reply(1, HttpBatchPayload({43, 44}), 200);
        UNIT_ASSERT_VALUES_EQUAL(http.State->Wait(2)->Payload, "{\"ids\":[45]}");
        http.State->Reply(2, HttpBatchPayload({45}), 200);
        Eventually([&] { return env.Output->Finished.load(); });
        std::lock_guard lock(env.Output->Mutex);
        UNIT_ASSERT_VALUES_EQUAL(env.Output->Rows.size(), 5);
        for (size_t i = 0; i < 5; ++i)
            UNIT_ASSERT_VALUES_EQUAL(env.Output->Rows[i].Id, (TVector<ui64>{42, 42, 43, 44, 45})[i]);
        UNIT_ASSERT_VALUES_EQUAL(http.State->Count(), 3);
    }

    Y_UNIT_TEST(DqGrpcBatchByteLimitAndFinalPartial) {
        THttpMock http;
        TGrpcMock grpc;
        TQueryTransformEnv env(Bindings(http, grpc)[3], 5000, 64, sizeof(TProfileBatchHeader) + 2 * sizeof(TProfileResult));
        env.Start({42, 43, 44, 45, 46});
        const TVector<TVector<ui64>> batches{{42, 43}, {44, 45}, {46}};
        for (size_t i = 0; i < batches.size(); ++i) {
            auto request = grpc.State->Wait(i);
            UNIT_ASSERT(HasHeader(*request, "authorization", "Bearer host-secret"));
            NFq::NWasmServices::NTest::ProfileBatchRequest parsed;
            UNIT_ASSERT(parsed.ParseFromString(request->Payload));
            UNIT_ASSERT_VALUES_EQUAL(parsed.ids_size(), batches[i].size());
            for (size_t j = 0; j < batches[i].size(); ++j)
                UNIT_ASSERT_VALUES_EQUAL(parsed.ids(j), batches[i][j]);
            grpc.State->Reply(i, GrpcBatchPayload(batches[i]), 0);
        }
        Eventually([&] { return env.Output->Finished.load(); });
        std::lock_guard lock(env.Output->Mutex);
        UNIT_ASSERT_VALUES_EQUAL(env.Output->Rows.size(), 5);
        UNIT_ASSERT_VALUES_EQUAL(grpc.State->Count(), 3);
    }

    Y_UNIT_TEST(DqHttpInvalidBatchDoesNotPublishPartialRows) {
        const TVector<TString> replies{
            HttpBatchPayload({42}),
            HttpBatchPayload({42, 43, 44}),
            HttpBatchPayload({43, 42}),
            "[{\"id\":42,\"name\":\"Ada\",\"score\":97,\"version\":1},"
            "{\"id\":43,\"name\":\"Ada\",\"score\":101,\"version\":1}]",
            "[{\"id\":42,\"name\":\"Ada\",\"score\":97,\"version\":1},{}]",
            TString(40000, 'x'),
        };
        for (const auto& reply : replies) {
            THttpMock http;
            TGrpcMock grpc;
            TQueryTransformEnv env(Bindings(http, grpc)[2], 5000, 2);
            const auto error = env.Setup.AsyncOutputPromises->Issue.GetFuture();
            env.Start({42, 43});
            http.State->Wait();
            http.State->Reply(0, reply, 200);
            UNIT_ASSERT(error.Wait(TDuration::Seconds(10)));
            std::lock_guard lock(env.Output->Mutex);
            UNIT_ASSERT(env.Output->Rows.empty());
            UNIT_ASSERT(!env.Output->Finished);
        }
    }

    Y_UNIT_TEST(DqGrpcInvalidBatchDoesNotPublishPartialRows) {
        const TVector<TString> replies{
            GrpcBatchPayload({42}),
            GrpcBatchPayload({42, 43, 44}),
            GrpcBatchPayload({43, 42}),
            GrpcBatchPayload({42, 43}, "Ada", 101),
            GrpcBatchPayload({42, 43}, TString("\xc0\x80", 2)),
            "not protobuf",
        };
        for (const auto& reply : replies) {
            THttpMock http;
            TGrpcMock grpc;
            TQueryTransformEnv env(Bindings(http, grpc)[3], 5000, 2);
            const auto error = env.Setup.AsyncOutputPromises->Issue.GetFuture();
            env.Start({42, 43});
            grpc.State->Wait();
            grpc.State->Reply(0, reply, 0);
            UNIT_ASSERT(error.Wait(TDuration::Seconds(10)));
            std::lock_guard lock(env.Output->Mutex);
            UNIT_ASSERT(env.Output->Rows.empty());
        }
    }

    Y_UNIT_TEST(DqBatchMaximumSizeAndLargeIds) {
        for (const auto binding : {2, 3}) {
            THttpMock http;
            TGrpcMock grpc;
            TQueryTransformEnv env(Bindings(http, grpc)[binding], 5000, MaxProfileBatchRows);
            TVector<ui64> ids;
            for (ui32 i = 0; i < MaxProfileBatchRows; ++i)
                ids.push_back(Max<ui64>() - i);
            env.Start(ids);
            auto state = binding == 2 ? http.State : grpc.State;
            state->Wait();
            state->Reply(0, binding == 2 ? HttpBatchPayload(ids) : GrpcBatchPayload(ids), binding == 2 ? 200 : 0);
            Eventually([&] { return env.Output->Finished.load(); });
            std::lock_guard lock(env.Output->Mutex);
            UNIT_ASSERT_VALUES_EQUAL(env.Output->Rows.size(), MaxProfileBatchRows);
            UNIT_ASSERT_VALUES_EQUAL(env.Output->Rows.back().Id, ids.back());
            UNIT_ASSERT_VALUES_EQUAL(state->Count(), 1);
        }
    }

    Y_UNIT_TEST(DqBatchDeadlineAndCancellation) {
        THttpMock http;
        TGrpcMock grpc;
        {
            TQueryTransformEnv env(Bindings(http, grpc)[2], 100, 2);
            const auto error = env.Setup.AsyncOutputPromises->Issue.GetFuture();
            env.Start({42, 43});
            http.State->Wait();
            UNIT_ASSERT(error.Wait(TDuration::Seconds(10)));
            UNIT_ASSERT(!env.Output->Finished);
        }
        {
            TQueryTransformEnv env(Bindings(http, grpc)[3], 5000, 2);
            env.Start({42, 43});
            grpc.State->Wait();
            env.Setup.Terminate();
            Eventually([&] {
                std::lock_guard lock(grpc.State->Mutex);
                return grpc.State->Requests[0]->Cancelled;
            });
            UNIT_ASSERT(!env.Output->Finished);
        }
    }

    Y_UNIT_TEST(DqBatchRejectsInvalidConfig) {
        TTempFile manifest(MakeTempName());
        TFileOutput(manifest.Name()).Write(NResource::Find("/ydb/udfs/wasm/profile/manifest.json"));
        NFq::NConfig::TWasmServicesConfig config;
        config.SetEnabled(true);
        auto* module = config.AddModules();
        module->SetModulePath("unused");
        module->SetManifestPath(manifest.Name());
        config.SetMaxBatchRows(MaxProfileBatchRows + 1);
        UNIT_ASSERT_EXCEPTION(CreateServiceGatewayFactory(config, TestServiceConnections("profiles")), yexception);
        config.SetMaxBatchRows(2);
        config.SetMaxBatchBytes(sizeof(TProfileBatchHeader) + sizeof(TProfileResult) - 1);
        UNIT_ASSERT_EXCEPTION(
            CreateServiceGatewayFactory(config, TestServiceConnections("profiles"))->CreateDqFunctionGateway("WASM_PROFILE", {}, "profiles")->ResolveFunction({}, "Profile"),
            yexception);
        config.SetMaxBatchBytes(MaxProfileBatchBytes + 1);
        UNIT_ASSERT_EXCEPTION(CreateServiceGatewayFactory(config, TestServiceConnections("profiles")), yexception);
    }

    Y_UNIT_TEST(DqProfileHttpBackpressureAndTypedRows) {
        THttpMock http;
        TGrpcMock grpc;
        TQueryTransformEnv env(Bindings(http, grpc)[2]);
        env.Output->BlockAfterRows = 1;
        env.Start({42, 43});
        UNIT_ASSERT_VALUES_EQUAL(http.State->Wait()->Payload, "{\"id\":42}");
        http.State->Reply(0, "{\"id\":42,\"name\":\"Ada\",\"score\":97,\"version\":1}", 200);
        Sleep(TDuration::MilliSeconds(50));
        UNIT_ASSERT_VALUES_EQUAL(http.State->Count(), 1);
        env.Output->BlockAfterRows = 0;
        env.Output->Blocked = false;
        env.Setup.Execute([](NYql::NDq::TFakeActor& actor) { actor.DqAsyncOutput->OnOutputConsumerReady(); });
        UNIT_ASSERT_VALUES_EQUAL(http.State->Wait(1)->Payload, "{\"id\":43}");
        http.State->Reply(1, "{\"id\":43,\"name\":\"Grace\",\"score\":99,\"version\":1}", 200);
        Eventually([&] { return env.Output->Finished.load(); });
        std::lock_guard lock(env.Output->Mutex);
        UNIT_ASSERT_VALUES_EQUAL(env.Output->Rows.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(env.Output->Rows[1].Id, 43);
    }

    Y_UNIT_TEST(DqProfileGrpcTypedRow) {
        THttpMock http;
        TGrpcMock grpc;
        TQueryTransformEnv env(Bindings(http, grpc)[3]);
        env.Start();
        auto request = grpc.State->Wait();
        UNIT_ASSERT(HasHeader(*request, "authorization", "Bearer host-secret"));
        grpc.State->Reply(0, GrpcProfilePayload(), 0);
        Eventually([&] { return env.Output->Finished.load(); });
        std::lock_guard lock(env.Output->Mutex);
        UNIT_ASSERT_VALUES_EQUAL(env.Output->Rows.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(env.Output->Rows[0].Id, 42);
    }

    Y_UNIT_TEST(DqProfileDeadlineAndMalformedResponse) {
        THttpMock http;
        TGrpcMock grpc;
        {
            TQueryTransformEnv env(Bindings(http, grpc)[2], 100);
            const auto error = env.Setup.AsyncOutputPromises->Issue.GetFuture();
            env.Start();
            http.State->Wait();
            UNIT_ASSERT(error.Wait(TDuration::Seconds(10)));
            UNIT_ASSERT(!error.GetValue().Empty());
        }
        {
            TQueryTransformEnv env(Bindings(http, grpc)[2]);
            const auto error = env.Setup.AsyncOutputPromises->Issue.GetFuture();
            env.Start();
            http.State->Wait(1);
            http.State->Reply(1, "not json", 200);
            UNIT_ASSERT(error.Wait(TDuration::Seconds(10)));
            UNIT_ASSERT_STRING_CONTAINS(error.GetValue().ToString(), "service failed");
            UNIT_ASSERT(!env.Output->Finished);
        }
    }

    Y_UNIT_TEST(DqProfileCancellationCancelsNativeGrpc) {
        THttpMock http;
        TGrpcMock grpc;
        TQueryTransformEnv env(Bindings(http, grpc)[3]);
        env.Start();
        grpc.State->Wait();
        env.Setup.Terminate();
        Eventually([&] {
            std::lock_guard lock(grpc.State->Mutex);
            return grpc.State->Requests[0]->Cancelled;
        });
        UNIT_ASSERT(!env.Output->Finished);
    }

    Y_UNIT_TEST(HttpTlsTrustedUntrustedAndHostnameVerification) {
        auto perform = [](THttpTlsMock& server, bool trust, bool serve) {
            TBinding binding;
            binding.Endpoint = server.Url();
            binding.Headers = {{"Content-Type", "application/json"}};
            if (trust)
                binding.CaFile = server.CaFile.Name();
            TTransport transport({binding});
            auto reply = StartDirectHttp(transport, std::make_shared<TDirectReply>());
            if (serve)
                server.ReplyOnce("{\"id\":42,\"name\":\"Ada\",\"score\":97,\"version\":1}");
            std::unique_lock lock(reply->Mutex);
            UNIT_ASSERT(reply->Changed.wait_for(lock, std::chrono::seconds(10), [&] { return reply->Done; }));
            TResponseHeader header;
            TStringBuf payload;
            UNIT_ASSERT(ParseResponse(reply->Bytes, header, payload));
            return std::make_pair(reply->Status, header);
        };

        THttpTlsMock trustedServer;
        auto [trustedStatus, trustedHeader] = perform(trustedServer, true, true);
        UNIT_ASSERT(trustedStatus == EOperationStatus::Ready);
        UNIT_ASSERT(trustedHeader.Error == EClientError::None);

        THttpTlsMock untrustedServer;
        auto [untrustedStatus, untrustedHeader] = perform(untrustedServer, false, false);
        UNIT_ASSERT(untrustedStatus == EOperationStatus::Failed);
        UNIT_ASSERT(untrustedHeader.Error == EClientError::Tls);

        THttpTlsMock wrongHostnameServer(false);
        auto [hostnameStatus, hostnameHeader] = perform(wrongHostnameServer, true, false);
        UNIT_ASSERT(hostnameStatus == EOperationStatus::Failed);
        UNIT_ASSERT(hostnameHeader.Error == EClientError::Tls);
    }

    Y_UNIT_TEST(DqConnectionHttpTlsPemRoots) {
        THttpTlsMock http;
        TBinding binding;
        binding.Endpoint = http.Url();
        binding.CaCertificate = http.Ca.Certificate;
        TQueryTransformEnv env(binding);
        env.Start();
        http.ReplyOnce("{\"id\":42,\"name\":\"Ada\",\"score\":97,\"version\":1}");
        Eventually([&] { return env.Output->Finished.load(); });
        UNIT_ASSERT_VALUES_EQUAL(env.Output->Rows.size(), 1);
    }

    Y_UNIT_TEST(DqConnectionGrpcTlsPemRoots) {
        using namespace NKikimr::NCertTestUtils;
        const auto ca = GenerateCA(TProps::AsCA().WithValid(TDuration::Days(1)));
        const auto cert = GenerateSignedCert(ca, TProps::AsServer().WithValid(TDuration::Days(1)));
        grpc::SslServerCredentialsOptions options;
        grpc::SslServerCredentialsOptions::PemKeyCertPair keyCert;
        keyCert.private_key = cert.PrivateKey;
        keyCert.cert_chain = cert.Certificate;
        options.pem_key_cert_pairs.push_back(std::move(keyCert));
        TGrpcMock grpc(grpc::SslServerCredentials(options));
        THttpMock http;
        auto binding = Bindings(http, grpc)[3];
        binding.Endpoint = TStringBuilder() << "localhost:" << TStringBuf(binding.Endpoint).RNextTok(':');
        binding.CaCertificate = ca.Certificate;
        TQueryTransformEnv env(binding, 5000, 1, 0, 65536, 0, false);
        env.Start();
        grpc.State->Wait();
        grpc.State->Reply(0, GrpcProfilePayload(), 0);
        Eventually([&] { return env.Output->Finished.load(); });
        UNIT_ASSERT_VALUES_EQUAL(env.Output->Rows.size(), 1);
    }

    Y_UNIT_TEST(TypedGrpcTlsTrustAndUntrustedCa) {
        using namespace NKikimr::NCertTestUtils;
        const auto ca = GenerateCA(TProps::AsCA().WithValid(TDuration::Days(1)));
        const auto serverCert = GenerateSignedCert(ca, TProps::AsServer().WithValid(TDuration::Days(1)));
        grpc::SslServerCredentialsOptions serverOptions;
        grpc::SslServerCredentialsOptions::PemKeyCertPair keyCertPair;
        keyCertPair.private_key = serverCert.PrivateKey;
        keyCertPair.cert_chain = serverCert.Certificate;
        serverOptions.pem_key_cert_pairs.push_back(std::move(keyCertPair));
        TGrpcMock grpc(grpc::SslServerCredentials(serverOptions));
        THttpMock http;

        grpc::SslCredentialsOptions trustedOptions;
        trustedOptions.pem_root_certs = ca.Certificate;
        TEnv env(Bindings(http, grpc, grpc::SslCredentials(trustedOptions)));
        const auto call = env.StartProfile(EProfileMode::Grpc, 42, 3, 3);
        UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
        grpc.State->Wait();
        grpc.State->Reply(0, GrpcProfilePayload(), 0);
        auto result = env.Resume(call);
        UNIT_ASSERT(result.Status == ECallStatus::Completed);
        auto decoded = ProfileResult(result);
        UNIT_ASSERT(decoded.Items[0].Error == EServiceError::None);
        env.Clean(call);

        TGrpcMock untrustedGrpc(grpc::SslServerCredentials(serverOptions));
        THttpMock untrustedHttp;
        TEnv untrustedEnv(Bindings(untrustedHttp, untrustedGrpc, grpc::SslCredentials(grpc::SslCredentialsOptions{})));
        const auto failedCall = untrustedEnv.StartProfile(EProfileMode::Grpc, 42, 3, 3);
        const auto failed = untrustedEnv.AwaitTerminal(failedCall);
        UNIT_ASSERT(failed.Status == ECallStatus::Completed);
        decoded = ProfileResult(failed);
        UNIT_ASSERT(decoded.Items[0].Error == EServiceError::Transport);
        UNIT_ASSERT(decoded.Items[0].ClientError == EClientError::Connection);
        UNIT_ASSERT_VALUES_EQUAL(untrustedGrpc.State->Count(), 0);
        untrustedEnv.Clean(failedCall);
    }

    Y_UNIT_TEST(TypedHttpProfileDecoding) {
        THttpMock http;
        TGrpcMock grpc;
        TEnv env(Bindings(http, grpc));
        const auto call = env.StartProfile(EProfileMode::Http);
        UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
        auto request = http.State->Wait();
        UNIT_ASSERT(request->Method.StartsWith("POST /call "));
        UNIT_ASSERT_VALUES_EQUAL(request->Payload, "{\"id\":42}");
        UNIT_ASSERT(HasHeader(*request, "Content-Type", "application/json"));
        http.State->Reply(0, "{\"id\":42,\"name\":\"Ada\",\"score\":97,\"version\":1}", 200);
        auto result = env.Resume(call);
        UNIT_ASSERT(result.Status == ECallStatus::Completed);
        const auto decoded = ProfileResult(result);
        UNIT_ASSERT_VALUES_EQUAL(decoded.Count, 1);
        UNIT_ASSERT(decoded.Items[0].Error == EServiceError::None);
        UNIT_ASSERT_VALUES_EQUAL(decoded.Items[0].Profile.Id, 42);
        UNIT_ASSERT_VALUES_EQUAL(decoded.Items[0].Profile.Score, 97);
        UNIT_ASSERT_VALUES_EQUAL(TString(decoded.Items[0].Profile.Name, decoded.Items[0].Profile.NameBytes), "Ada");
        env.Clean(call);
    }

    Y_UNIT_TEST(TypedGrpcProfileDecodingAndMalformedPayload) {
        THttpMock http;
        TGrpcMock grpc;
        TEnv env(Bindings(http, grpc));
        const auto call = env.StartProfile(EProfileMode::Grpc, 42, 3, 3);
        UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
        auto request = grpc.State->Wait();
        NFq::NWasmServices::NTest::ProfileRequest parsedRequest;
        UNIT_ASSERT(parsedRequest.ParseFromString(request->Payload));
        UNIT_ASSERT_VALUES_EQUAL(parsedRequest.id(), 42);
        grpc.State->Reply(0, GrpcProfilePayload(), 0);
        auto result = env.Resume(call);
        UNIT_ASSERT(result.Status == ECallStatus::Completed);
        auto decoded = ProfileResult(result);
        UNIT_ASSERT(decoded.Items[0].Error == EServiceError::None);
        UNIT_ASSERT_VALUES_EQUAL(decoded.Items[0].Profile.Id, 42);
        env.Clean(call);

        THttpMock badHttp;
        TGrpcMock badGrpc;
        TEnv badEnv(Bindings(badHttp, badGrpc));
        const auto badCall = badEnv.StartProfile(EProfileMode::Http);
        UNIT_ASSERT(badEnv.Runtime->Poll(badCall).Status == ECallStatus::Waiting);
        badHttp.State->Wait();
        badHttp.State->Reply(0, "{\"id\":42,\"name\":\"Ada\",\"score\":101,\"version\":1}", 200);
        const auto badResult = badEnv.Resume(badCall);
        UNIT_ASSERT(badResult.Status == ECallStatus::Completed);
        decoded = ProfileResult(badResult);
        UNIT_ASSERT(decoded.Items[0].Error == EServiceError::InvalidProfile);
        UNIT_ASSERT(!badEnv.Runtime->IsPoisoned());
        badEnv.Clean(badCall);

        THttpMock malformedHttp;
        TGrpcMock malformedGrpc;
        TEnv malformedEnv(Bindings(malformedHttp, malformedGrpc));
        const auto malformedCall = malformedEnv.StartProfile(EProfileMode::Grpc, 42, 3, 3);
        UNIT_ASSERT(malformedEnv.Runtime->Poll(malformedCall).Status == ECallStatus::Waiting);
        malformedGrpc.State->Wait();
        malformedGrpc.State->Reply(0, "not-protobuf", 0);
        const auto malformedResult = malformedEnv.Resume(malformedCall);
        UNIT_ASSERT(malformedResult.Status == ECallStatus::Completed);
        decoded = ProfileResult(malformedResult);
        UNIT_ASSERT(decoded.Items[0].Error == EServiceError::Decode);
        malformedEnv.Clean(malformedCall);
    }

    Y_UNIT_TEST(TypedProfileSequentialAndParallelModes) {
        THttpMock sequentialHttp;
        TGrpcMock sequentialGrpc;
        TEnv sequentialEnv(Bindings(sequentialHttp, sequentialGrpc));
        const auto sequentialCall = sequentialEnv.StartProfile(EProfileMode::HttpThenGrpc);
        UNIT_ASSERT(sequentialEnv.Runtime->Poll(sequentialCall).Status == ECallStatus::Waiting);
        sequentialHttp.State->Wait();
        sequentialHttp.State->Reply(0, "{\"id\":42,\"name\":\"Ada\",\"score\":97,\"version\":1}", 200);
        UNIT_ASSERT(sequentialEnv.Resume(sequentialCall).Status == ECallStatus::Waiting);
        auto grpcRequest = sequentialGrpc.State->Wait();
        NFq::NWasmServices::NTest::ProfileRequest request;
        UNIT_ASSERT(request.ParseFromString(grpcRequest->Payload));
        UNIT_ASSERT_VALUES_EQUAL(request.id(), 42);
        sequentialGrpc.State->Reply(0, GrpcProfilePayload(), 0);
        auto sequentialResult = sequentialEnv.Resume(sequentialCall);
        UNIT_ASSERT(sequentialResult.Status == ECallStatus::Completed);
        auto sequentialProfiles = ProfileResult(sequentialResult);
        UNIT_ASSERT_VALUES_EQUAL(sequentialProfiles.Count, 2);
        UNIT_ASSERT(sequentialProfiles.Items[0].Error == EServiceError::None);
        UNIT_ASSERT(sequentialProfiles.Items[1].Error == EServiceError::None);
        sequentialEnv.Clean(sequentialCall);

        THttpMock parallelHttp;
        TGrpcMock parallelGrpc;
        TEnv parallelEnv(Bindings(parallelHttp, parallelGrpc));
        const auto parallelCall = parallelEnv.StartProfile(EProfileMode::Parallel);
        UNIT_ASSERT(parallelEnv.Runtime->Poll(parallelCall).Status == ECallStatus::Waiting);
        parallelHttp.State->Wait();
        parallelGrpc.State->Wait();
        parallelGrpc.State->Reply(0, GrpcProfilePayload(), 0);
        parallelHttp.State->Reply(0, "{\"id\":42,\"name\":\"Ada\",\"score\":97,\"version\":1}", 200);
        auto parallelResult = parallelEnv.AwaitTerminal(parallelCall);
        UNIT_ASSERT(parallelResult.Status == ECallStatus::Completed);
        auto parallelProfiles = ProfileResult(parallelResult);
        UNIT_ASSERT_VALUES_EQUAL(parallelProfiles.Count, 2);
        UNIT_ASSERT(parallelProfiles.Items[0].Error == EServiceError::None);
        UNIT_ASSERT(parallelProfiles.Items[1].Error == EServiceError::None);
        parallelEnv.Clean(parallelCall);
    }

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
            UNIT_ASSERT(ReplyHeader(result, 0).Error == EClientError::HttpStatus);
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
        UNIT_ASSERT(ReplyHeader(result, 0).Error == EClientError::Connection);
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
        const auto result = env.Resume(call);
        UNIT_ASSERT(result.Status == ECallStatus::Failed);
        UNIT_ASSERT(ReplyHeader(result, 0).Error == EClientError::ResourceLimit);
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
        const auto result = env.Resume(call);
        UNIT_ASSERT(result.Status == ECallStatus::Failed);
        UNIT_ASSERT(ReplyHeader(result, 0).Error == EClientError::ResourceLimit);
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

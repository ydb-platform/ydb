#include <ydb/services/nbs/classic_grpc_service.h>

#include <ydb/core/nbs/cloud/blockstore/compat/libs/service/service_method.h>
#include <ydb/core/nbs/cloud/blockstore/libs/nbs_frontend/blockstore_facade.h>

#include <ydb/core/nbs/cloud/storage/core/compat/protos/request_source.pb.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>

#include <ydb/library/grpc/server/grpc_server.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <grpcpp/create_channel.h>
#include <grpcpp/generic/generic_stub.h>

#include <chrono>
#include <mutex>

namespace NKikimr::NGRpcService {

    namespace {

        using namespace NCloud::NBlockStore;

        ////////////////////////////////////////////////////////////////////////////////

        // Runs the classic service on a real YDB gRPC server for transport tests.
        class TClassicNbsGrpcTestServer final {
        public:
            explicit TClassicNbsGrpcTestServer(IBlockStorePtr blockStore)
                : Port(PortManager.GetPort())
            {
                NYdbGrpc::TServerOptions options;
                options.SetHost("localhost");
                options.SetPort(Port);

                Server = std::make_unique<NYdbGrpc::TGRpcServer>(options);
                Server->AddService(
                    new TClassicNbsGrpcService(std::move(blockStore)));
                Server->Start();

                Channel = grpc::CreateChannel(
                    TStringBuilder() << "localhost:" << Port,
                    grpc::InsecureChannelCredentials());
            }

            ~TClassicNbsGrpcTestServer()
            {
                Server->Stop();
            }

            // Returns a control-service stub connected to the test server.
            std::unique_ptr<NProto::TBlockStoreService::Stub> CreateControlStub() const {
                return NProto::TBlockStoreService::NewStub(Channel);
            }

            // Returns a type-agnostic stub connected to the test server.
            std::unique_ptr<grpc::GenericStub> CreateGenericStub() const {
                return std::make_unique<grpc::GenericStub>(Channel);
            }

        private:
            TPortManager PortManager;
            ui16 Port;
            std::unique_ptr<NYdbGrpc::TGRpcServer> Server;
            std::shared_ptr<grpc::Channel> Channel;
        };

        struct TRecordedRequest {
            NProto::THeaders Headers;
            ui32 CallCount = 0;
        };

        // Records adapted headers received through the classic IBlockStore boundary.
        class TRecordingBlockStore final
            : public TBlockStoreImpl<TRecordingBlockStore, IBlockStore> {
        public:
            // This test facade has no lifecycle state.
            void Start() override {
            }

            // This test facade has no lifecycle state.
            void Stop() override {
            }

            // This test facade does not allocate data-path buffers.
            TStorageBuffer AllocateBuffer(size_t bytesCount) override {
                Y_UNUSED(bytesCount);
                return nullptr;
            }

            // Records the request headers and returns a successful response.
            template <typename TMethod>
            NThreading::TFuture<typename TMethod::TResponse> Execute(
                NYdb::NBS::NBlockStore::TCallContextPtr callContext,
                std::shared_ptr<typename TMethod::TRequest> request)
            {
                Y_UNUSED(callContext);

                {
                    std::lock_guard guard(Mutex);
                    LastRequest.Headers.CopyFrom(request->GetHeaders());
                    ++LastRequest.CallCount;
                }

                return NThreading::MakeFuture<typename TMethod::TResponse>();
            }

            // Returns a synchronized snapshot of the last recorded request.
            TRecordedRequest GetLastRequest() const {
                std::lock_guard guard(Mutex);
                return LastRequest;
            }

        private:
            mutable std::mutex Mutex;
            TRecordedRequest LastRequest;
        };

        void SetDeadline(grpc::ClientContext* context)
        {
            context->set_deadline(
                std::chrono::system_clock::now() + std::chrono::seconds(5));
        }

        ////////////////////////////////////////////////////////////////////////////////

        Y_UNIT_TEST_SUITE(TClassicNbsGrpcServiceTest) {
            Y_UNIT_TEST(ShouldReflectFacadeLifecycleThroughTransport) {
                auto blockStore = NYdb::NBS::NBlockStore::CreateNbsFrontendBlockStore();
                TClassicNbsGrpcTestServer server(blockStore);
                auto stub = server.CreateControlStub();

                NProto::TPingRequest request;
                NProto::TPingResponse response;
                grpc::ClientContext contextBeforeStart;
                SetDeadline(&contextBeforeStart);
                auto status = stub->Ping(&contextBeforeStart, request, &response);
                UNIT_ASSERT(status.ok());
                UNIT_ASSERT_VALUES_EQUAL(
                    response.GetError().GetCode(),
                    NYdb::NBS::E_REJECTED);

                blockStore->Start();
                response.Clear();
                grpc::ClientContext contextWhileStarted;
                SetDeadline(&contextWhileStarted);
                status = stub->Ping(&contextWhileStarted, request, &response);
                UNIT_ASSERT(status.ok());
                UNIT_ASSERT(!NYdb::NBS::HasError(response));

                blockStore->Stop();
                response.Clear();
                grpc::ClientContext contextAfterStop;
                SetDeadline(&contextAfterStop);
                status = stub->Ping(&contextAfterStop, request, &response);
                UNIT_ASSERT(status.ok());
                UNIT_ASSERT_VALUES_EQUAL(
                    response.GetError().GetCode(),
                    NYdb::NBS::E_REJECTED);
            }

            Y_UNIT_TEST(ShouldRegisterEverySupportedMethod) {
                auto blockStore = NYdb::NBS::NBlockStore::CreateNbsFrontendBlockStore();
                blockStore->Start();
                TClassicNbsGrpcTestServer server(blockStore);
                auto stub = server.CreateControlStub();

#define TEST_METHOD(name, ...)                                                \
    if (TStringBuf(#name) != "Ping") {                                        \
        NProto::T##name##Request request;                                     \
        NProto::T##name##Response response;                                   \
        grpc::ClientContext context;                                          \
        SetDeadline(&context);                                                \
        const auto status = stub->name(&context, request, &response);         \
        UNIT_ASSERT_C(status.ok(), status.error_message());                   \
        UNIT_ASSERT_VALUES_EQUAL(                                             \
            response.GetError().GetCode(),                                    \
            NYdb::NBS::E_NOT_IMPLEMENTED);                                    \
        UNIT_ASSERT_STRING_CONTAINS(response.GetError().GetMessage(), #name); \
    }

                BLOCKSTORE_GRPC_SERVICE(TEST_METHOD)

#undef TEST_METHOD
            }

            Y_UNIT_TEST(ShouldAdaptHeadersAndRejectClientInternalHeaders) {
                auto blockStore = std::make_shared<TRecordingBlockStore>();
                TClassicNbsGrpcTestServer server(blockStore);
                auto stub = server.CreateControlStub();

                NProto::TPingRequest request;
                request.MutableHeaders()->SetTraceId("trace-id");
                request.MutableHeaders()->SetClientId("client-id");
                request.MutableHeaders()->SetRequestId(42);
                NProto::TPingResponse response;
                grpc::ClientContext context;
                SetDeadline(&context);

                const auto status = stub->Ping(&context, request, &response);
                UNIT_ASSERT(status.ok());
                UNIT_ASSERT(!NYdb::NBS::HasError(response));

                const auto recorded = blockStore->GetLastRequest();
                UNIT_ASSERT_VALUES_EQUAL(recorded.CallCount, 1);
                UNIT_ASSERT_VALUES_EQUAL(recorded.Headers.GetTraceId(), "trace-id");
                UNIT_ASSERT_VALUES_EQUAL(recorded.Headers.GetClientId(), "client-id");
                UNIT_ASSERT_VALUES_EQUAL(recorded.Headers.GetRequestId(), 42);
                UNIT_ASSERT(recorded.Headers.HasInternal());
                UNIT_ASSERT_VALUES_EQUAL(
                    static_cast<ui32>(
                        recorded.Headers.GetInternal().GetRequestSource()),
                    static_cast<ui32>(
                        NCloud::NProto::SOURCE_INSECURE_CONTROL_CHANNEL));
                UNIT_ASSERT(!recorded.Headers.GetInternal().GetPeer().empty());

                request.MutableHeaders()->MutableInternal()->SetPeer("client-peer");
                response.Clear();
                grpc::ClientContext invalidContext;
                SetDeadline(&invalidContext);
                const auto invalidStatus =
                    stub->Ping(&invalidContext, request, &response);
                UNIT_ASSERT(invalidStatus.ok());
                UNIT_ASSERT_VALUES_EQUAL(
                    response.GetError().GetCode(),
                    NYdb::NBS::E_ARGUMENT);
                UNIT_ASSERT_VALUES_EQUAL(
                    response.GetError().GetMessage(),
                    "internal field should not be set by client");
                UNIT_ASSERT_VALUES_EQUAL(
                    blockStore->GetLastRequest().CallCount,
                    1);
            }

            Y_UNIT_TEST(ShouldRejectUnknownMethodAtTransportLevel) {
                auto blockStore = NYdb::NBS::NBlockStore::CreateNbsFrontendBlockStore();
                blockStore->Start();
                TClassicNbsGrpcTestServer server(blockStore);
                auto stub = server.CreateGenericStub();

                grpc::ClientContext context;
                SetDeadline(&context);
                grpc::ByteBuffer request;
                grpc::ByteBuffer response;
                grpc::CompletionQueue completionQueue;
                auto call = stub->PrepareUnaryCall(
                    &context,
                    "/NCloud.NBlockStore.NProto.TBlockStoreService/DescribeVolume",
                    request,
                    &completionQueue);
                UNIT_ASSERT(call);

                call->StartCall();
                grpc::Status status;
                void* const expectedTag = reinterpret_cast<void*>(1);
                call->Finish(&response, &status, expectedTag);

                void* actualTag = nullptr;
                bool ok = false;
                UNIT_ASSERT(completionQueue.Next(&actualTag, &ok));
                UNIT_ASSERT(ok);
                UNIT_ASSERT(actualTag == expectedTag);
                completionQueue.Shutdown();

                UNIT_ASSERT_VALUES_EQUAL(
                    status.error_code(),
                    grpc::StatusCode::UNIMPLEMENTED);
            }
        } // Y_UNIT_TEST_SUITE(TClassicNbsGrpcServiceTest)

        ////////////////////////////////////////////////////////////////////////////////

    } // namespace

} // namespace NKikimr::NGRpcService

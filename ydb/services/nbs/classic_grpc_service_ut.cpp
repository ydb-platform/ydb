#include <ydb/services/nbs/classic_grpc_service_adapter.h>
#include <ydb/services/nbs/classic_grpc_service_factory.h>

#include <ydb/core/nbs/nbs1_compat_api/cloud/blockstore/public/api/grpc/service.pb.h>
#include <ydb/core/nbs/nbs1_compat_api/cloud/blockstore/libs/service/service_method.h>
#include <ydb/core/nbs/cloud/blockstore/libs/nbs_frontend/blockstore_facade.h>
#include <ydb/core/nbs/cloud/blockstore/libs/nbs_frontend/frontend_test.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/storage_test.h>
#include <ydb/core/nbs/cloud/storage/core/protos/media.pb.h>
#include <ydb/core/protos/blockstore_config.pb.h>

#include <ydb/core/nbs/nbs1_compat_api/cloud/storage/core/protos/request_source.pb.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>

#include <ydb/library/grpc/server/grpc_server.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <grpcpp/create_channel.h>
#include <grpcpp/generic/generic_stub.h>
#include <grpcpp/impl/client_unary_call.h>

#include <atomic>
#include <chrono>
#include <mutex>

namespace NKikimr::NGRpcService {

    namespace {

        using namespace NYdb::NBS::NNbs1CompatApi::NBlockStore;

        ////////////////////////////////////////////////////////////////////////////////

        // Calls literal classic paths independently of server registration and
        // the renamed service.proto. This must not use the adapter's name.
        class TClassicNbsTestClient final {
        public:
            explicit TClassicNbsTestClient(std::shared_ptr<grpc::Channel> channel)
                : Channel(std::move(channel))
            {
            }

#define TEST_CLIENT_METHOD(name)                                        \
    grpc::Status name(grpc::ClientContext* context,                     \
                      const NProto::T##name##Request& request,          \
                      NProto::T##name##Response* response) {            \
        return grpc::internal::BlockingUnaryCall(                       \
            Channel.get(),                                              \
            grpc::internal::RpcMethod(                                  \
                "/NCloud.NBlockStore.NProto.TBlockStoreService/" #name, \
                grpc::internal::RpcMethod::NORMAL_RPC),                 \
            context, request, response);                                \
    }

            // Deliberately independent of the server's supported-method macro.
            TEST_CLIENT_METHOD(Ping)
            TEST_CLIENT_METHOD(MountVolume)
            TEST_CLIENT_METHOD(UnmountVolume)
            TEST_CLIENT_METHOD(ReadBlocks)
            TEST_CLIENT_METHOD(WriteBlocks)

#undef TEST_CLIENT_METHOD

        private:
            const std::shared_ptr<grpc::Channel> Channel;
        };

        // Runs the classic service on a real YDB gRPC server for transport tests.
        class TClassicNbsGrpcTestServer final {
        public:
            explicit TClassicNbsGrpcTestServer(
                IBlockStorePtr blockStore,
                size_t maxMessageSize = NYdb::NGrpc::DEFAULT_GRPC_MESSAGE_SIZE_LIMIT)
                : Port(PortManager.GetPort())
            {
                NYdbGrpc::TServerOptions options;
                options.SetHost("localhost");
                options.SetPort(Port);
                options.SetMaxMessageSize(maxMessageSize);

                Server = std::make_unique<NYdbGrpc::TGRpcServer>(options);
                Server->AddService(
                    CreateClassicNbsGrpcService(std::move(blockStore)));
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
            std::unique_ptr<TClassicNbsTestClient> CreateControlStub() const {
                return std::make_unique<TClassicNbsTestClient>(Channel);
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

        namespace NNative = NYdb::NBS::NBlockStore;

        // Exercises classic RPC paths against a real frontend and a memory backend.
        class TClassicNbsGrpcPathTestEnv final {
        public:
            TClassicNbsGrpcPathTestEnv()
            {
                Storage->WriteBlocksLocalHandler = [this](NNative::TCallContextPtr context, auto request) {
                    Y_UNUSED(context);
                    ++IoCalls;
                    const auto guard = request->Sglist.Acquire();
                    UNIT_ASSERT(guard);
                    UNIT_ASSERT_VALUES_EQUAL(
                        NYdb::NBS::SgListCopy(guard.Get(), NYdb::NBS::TBlockDataRef(Data.data(), Data.size())),
                        Data.size());
                    return NThreading::MakeFuture<NNative::TWriteBlocksLocalResponse>();
                };
                Storage->ReadBlocksLocalHandler = [this](NNative::TCallContextPtr context, auto request) {
                    Y_UNUSED(context);
                    ++IoCalls;
                    const auto guard = request->Sglist.Acquire();
                    UNIT_ASSERT(guard);
                    UNIT_ASSERT_VALUES_EQUAL(
                        NYdb::NBS::SgListCopy(NYdb::NBS::TBlockDataRef(Data.data(), Data.size()), guard.Get()),
                        Data.size());
                    return NThreading::MakeFuture<NNative::TReadBlocksLocalResponse>();
                };
                UNIT_ASSERT(!NYdb::NBS::HasError(FrontendEnv.RegisterVolume(
                    Config, Storage, NNative::NTests::MakeTestIoConfig(Config))));
                FrontendEnv.Facade->Start();
                Server = std::make_unique<TClassicNbsGrpcTestServer>(FrontendEnv.Facade);
                Stub = Server->CreateControlStub();
            }

            // Mounts the client as a remote vhost endpoint would.
            NProto::TMountVolumeResponse Mount()
            {
                auto request = NNative::NTests::MakeTestMountRequest();
                request.SetDiskId(Config.GetDiskId());
                request.SetVolumeMountMode(NProto::VOLUME_MOUNT_REMOTE);
                request.SetForceRemoteBinding(true);
                request.SetIpcType(NProto::IPC_VHOST);
                NProto::TMountVolumeResponse response;
                grpc::ClientContext context;
                SetDeadline(&context);
                const auto status = Stub->MountVolume(&context, request, &response);
                UNIT_ASSERT_C(status.ok(), status.error_message());
                UNIT_ASSERT(!NYdb::NBS::HasError(response));
                return response;
            }

            // Reads one block with the supplied session, including revoked sessions.
            NProto::TReadBlocksResponse Read(const TString& sessionId)
            {
                NProto::TReadBlocksRequest request;
                request.SetDiskId(Config.GetDiskId());
                request.MutableHeaders()->SetClientId(NNative::NTests::TestClientId);
                request.SetSessionId(sessionId);
                request.SetBlocksCount(1);
                NProto::TReadBlocksResponse response;
                grpc::ClientContext context;
                SetDeadline(&context);
                const auto status = Stub->ReadBlocks(&context, request, &response);
                UNIT_ASSERT_C(status.ok(), status.error_message());
                return response;
            }

            // Writes the pattern using the supplied session.
            NProto::TWriteBlocksResponse Write(const TString& sessionId, const TString& pattern)
            {
                NProto::TWriteBlocksRequest request;
                request.SetDiskId(Config.GetDiskId());
                request.MutableHeaders()->SetClientId(NNative::NTests::TestClientId);
                request.SetSessionId(sessionId);
                request.MutableBlocks()->AddBuffers(pattern);
                NProto::TWriteBlocksResponse response;
                grpc::ClientContext context;
                SetDeadline(&context);
                const auto status = Stub->WriteBlocks(&context, request, &response);
                UNIT_ASSERT_C(status.ok(), status.error_message());
                return response;
            }

            // Revokes the supplied session over its classic RPC path.
            void Unmount(const TString& sessionId)
            {
                NProto::TUnmountVolumeRequest request;
                request.SetDiskId(Config.GetDiskId());
                request.MutableHeaders()->SetClientId(NNative::NTests::TestClientId);
                request.SetSessionId(sessionId);
                NProto::TUnmountVolumeResponse response;
                grpc::ClientContext context;
                SetDeadline(&context);
                const auto status = Stub->UnmountVolume(&context, request, &response);
                UNIT_ASSERT_C(status.ok(), status.error_message());
                UNIT_ASSERT(!NYdb::NBS::HasError(response));
            }

            const NKikimrBlockStore::TVolumeConfig Config = NNative::NTests::MakeTestVolumeConfig();
            std::atomic<ui32> IoCalls = 0;

        private:
            TString Data = TString(Config.GetBlockSize(), '\0');
            std::shared_ptr<NNative::TTestStorage> Storage = std::make_shared<NNative::TTestStorage>();
            NNative::NTests::TFrontendTestEnv FrontendEnv;
            std::unique_ptr<TClassicNbsGrpcTestServer> Server;
            std::unique_ptr<TClassicNbsTestClient> Stub;
        };

        Y_UNIT_TEST_SUITE(TClassicNbsGrpcPathsTest) {
            Y_UNIT_TEST(ShouldMountIdempotently) {
                TClassicNbsGrpcPathTestEnv env;
                const auto first = env.Mount();
                UNIT_ASSERT_VALUES_EQUAL(first.GetVolume().GetDiskId(), env.Config.GetDiskId());
                UNIT_ASSERT_VALUES_EQUAL(first.GetVolume().GetBlockSize(), env.Config.GetBlockSize());
                UNIT_ASSERT_VALUES_EQUAL(first.GetVolume().GetBlocksCount(), env.Config.GetPartitions(0).GetBlockCount());
                UNIT_ASSERT(!first.GetSessionId().empty());
                UNIT_ASSERT_VALUES_EQUAL(first.GetInactiveClientsTimeout(), 0);
                UNIT_ASSERT_VALUES_EQUAL(env.Mount().GetSessionId(), first.GetSessionId());
            }

            Y_UNIT_TEST(ShouldWriteAndRead) {
                TClassicNbsGrpcPathTestEnv env;
                const auto sessionId = env.Mount().GetSessionId();
                const TString pattern(env.Config.GetBlockSize(), 'w');
                UNIT_ASSERT(!NYdb::NBS::HasError(env.Write(sessionId, pattern)));
                const auto response = env.Read(sessionId);
                UNIT_ASSERT(!NYdb::NBS::HasError(response));
                UNIT_ASSERT_VALUES_EQUAL(response.GetBlocks().BuffersSize(), 1);
                UNIT_ASSERT_VALUES_EQUAL(response.GetBlocks().GetBuffers(0), pattern);
                UNIT_ASSERT_VALUES_EQUAL(env.IoCalls.load(), 2);
            }

            Y_UNIT_TEST(ShouldRejectIoAfterUnmount) {
                TClassicNbsGrpcPathTestEnv env;
                const auto first = env.Mount().GetSessionId();
                env.Unmount(first);
                // Direct RPCs do not remount automatically after session revocation.
                UNIT_ASSERT_VALUES_EQUAL(env.Read(first).GetError().GetCode(), NYdb::NBS::E_BS_INVALID_SESSION);
                UNIT_ASSERT_VALUES_EQUAL(
                    env.Write(first, TString(env.Config.GetBlockSize(), 'w')).GetError().GetCode(),
                    NYdb::NBS::E_BS_INVALID_SESSION);
                UNIT_ASSERT_VALUES_EQUAL(env.IoCalls.load(), 0);
                UNIT_ASSERT(env.Mount().GetSessionId() != first);
            }
        }

        Y_UNIT_TEST_SUITE(TClassicNbsGrpcServiceTest) {
            Y_UNIT_TEST(ShouldRejectOversizedTransportMessageBeforeFacade) {
                constexpr ui32 blockSize = NNative::DefaultBlockSize;
                constexpr size_t maxMessageSize = 2 * blockSize;
                auto blockStore = std::make_shared<TRecordingBlockStore>();
                TClassicNbsGrpcTestServer server(blockStore, maxMessageSize);
                auto stub = server.CreateControlStub();

                NProto::TWriteBlocksRequest request;
                request.SetDiskId(NNative::NTests::TestDiskId);
                request.MutableHeaders()->SetClientId(NNative::NTests::TestClientId);
                request.SetBlockSize(blockSize);
                auto* buffer = request.MutableBlocks()->AddBuffers();
                *buffer = TString(blockSize, 'w');
                UNIT_ASSERT(request.ByteSizeLong() < maxMessageSize);

                NProto::TWriteBlocksResponse response;
                grpc::ClientContext acceptedContext;
                SetDeadline(&acceptedContext);
                const auto accepted = stub->WriteBlocks(&acceptedContext, request, &response);
                UNIT_ASSERT_C(accepted.ok(), accepted.error_message());
                UNIT_ASSERT(!NYdb::NBS::HasError(response));
                UNIT_ASSERT_VALUES_EQUAL(blockStore->GetLastRequest().CallCount, 1);

                // The payload fits the transport budget, but its protobuf does not.
                // Both payloads are well below the frontend's 32 MiB ceiling.
                buffer->resize(maxMessageSize, 'w');
                UNIT_ASSERT(request.ByteSizeLong() > maxMessageSize);
                grpc::ClientContext rejectedContext;
                SetDeadline(&rejectedContext);
                response.Clear();
                const auto rejected = stub->WriteBlocks(&rejectedContext, request, &response);
                UNIT_ASSERT_VALUES_EQUAL_C(
                    rejected.error_code(), grpc::StatusCode::RESOURCE_EXHAUSTED,
                    rejected.error_message());
                UNIT_ASSERT_VALUES_EQUAL(blockStore->GetLastRequest().CallCount, 1);
            }

            Y_UNIT_TEST(ShouldIsolateProtobufDescriptors) {
                const auto* file = google::protobuf::DescriptorPool::generated_pool()
                                       ->FindFileByName(
                                           "ydb/core/nbs/nbs1_compat_api/cloud/blockstore/public/api/grpc/service.proto");
                UNIT_ASSERT(file);
                const auto* service = file->FindServiceByName("TBlockStoreService");
                UNIT_ASSERT(service);
                UNIT_ASSERT_VALUES_EQUAL(service->full_name(),
                                         "NYdb.NBS.NNbs1CompatApi.NBlockStore.NProto.TBlockStoreService");
                UNIT_ASSERT_VALUES_EQUAL(service->method_count(), 5);
                UNIT_ASSERT_VALUES_EQUAL(NProto::TPingRequest::descriptor()->full_name(),
                                         "NYdb.NBS.NNbs1CompatApi.NBlockStore.NProto.TPingRequest");
                UNIT_ASSERT_VALUES_EQUAL(TClassicNbsGrpcServiceAdapter::service_full_name(),
                                         "NCloud.NBlockStore.NProto.TBlockStoreService");
            }

            Y_UNIT_TEST(ShouldReflectFacadeLifecycleThroughTransport) {
                auto blockStore = NYdb::NBS::NBlockStore::CreateNbsBlockStoreFacade(
                    TLog{});
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
                auto blockStore = NYdb::NBS::NBlockStore::CreateNbsBlockStoreFacade(
                    TLog{});
                blockStore->Start();
                TClassicNbsGrpcTestServer server(blockStore);
                auto stub = server.CreateControlStub();

#define TEST_METHOD(name)                                                     \
    {                                                                         \
        NProto::T##name##Request request;                                     \
        NProto::T##name##Response response;                                   \
        grpc::ClientContext context;                                          \
        SetDeadline(&context);                                                \
        const auto status = stub->name(&context, request, &response);         \
        UNIT_ASSERT_C(status.ok(), status.error_message());                   \
        UNIT_ASSERT_VALUES_EQUAL(                                             \
            response.GetError().GetCode(),                                    \
            NYdb::NBS::E_NOT_FOUND);                                          \
    }

                // Keep the expected API independent of registration macros.
                // Ping is exercised separately by the lifecycle test.
                TEST_METHOD(MountVolume)
                TEST_METHOD(UnmountVolume)
                TEST_METHOD(ReadBlocks)
                TEST_METHOD(WriteBlocks)

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
                        NYdb::NBS::NNbs1CompatApi::NProto::SOURCE_INSECURE_CONTROL_CHANNEL));
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

            Y_UNIT_TEST(ShouldRejectUnknownAndPrivateMethodPaths) {
                auto blockStore = NYdb::NBS::NBlockStore::CreateNbsBlockStoreFacade(
                    TLog{});
                blockStore->Start();
                TClassicNbsGrpcTestServer server(blockStore);
                auto stub = server.CreateGenericStub();

                // The isolated protobuf package is not another public API.
                TVector<TString> notSupportedMethods = {
                    "/NCloud.NBlockStore.NProto.TBlockStoreService/DescribeVolume",
                    "/NYdb.NBS.NNbs1CompatApi.NBlockStore.NProto.TBlockStoreService/Ping"};
                for (TString &path : notSupportedMethods) {
                    grpc::ClientContext context;
                    SetDeadline(&context);
                    grpc::ByteBuffer request;
                    grpc::ByteBuffer response;
                    grpc::CompletionQueue completionQueue;
                    auto call = stub->PrepareUnaryCall(
                        &context,
                        path.c_str(),
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
            }
        } // Y_UNIT_TEST_SUITE(TClassicNbsGrpcServiceTest)

        ////////////////////////////////////////////////////////////////////////////////

    } // namespace

} // namespace NKikimr::NGRpcService

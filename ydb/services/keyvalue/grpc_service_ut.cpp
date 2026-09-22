#include "grpc_service_v1.h"
#include "grpc_service_v2.h"

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/keyvalue/keyvalue.h>
#include <ydb/core/keyvalue/keyvalue_events.h>
#include <ydb/core/protos/blob_depot_config.pb.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/s3_settings.pb.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/wrappers/ut_helpers/s3_mock.h>
#include <ydb/library/aws_init/aws.h>

#include <ydb/public/api/grpc/ydb_scheme_v1.grpc.pb.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/resources/ydb_resources.h>

#include <ydb/public/sdk/cpp/src/library/grpc/client/grpc_client_low.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/hook/hook.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/logger/backend.h>

#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>

#include <atomic>
#include <util/string/builder.h>
#include <util/system/datetime.h>


#define UNIT_ASSERT_STATUS_EQUALS(got, exp) \
    UNIT_ASSERT_C((got) == (exp), "exp# " << Ydb::StatusIds::StatusCode_Name(exp) \
            << " got# " << Ydb::StatusIds::StatusCode_Name(got)) \
// UNIT_ASSERT_STATUS_EQUALS

#define UNIT_ASSERT_CHECK_STATUS(got, exp) \
    UNIT_ASSERT_C(got.status() == exp, "exp# " << Ydb::StatusIds::StatusCode_Name(exp) \
            << " got# " << Ydb::StatusIds::StatusCode_Name(got.status()) << " issues# "  << got.issues()) \
// UNIT_ASSERT_CHECK_STATUS


namespace NKikimr::NGRpcService {


namespace {
#ifndef KIKIMR_DISABLE_S3_OPS
    Y_TEST_HOOK_BEFORE_RUN(InitAwsAPI) {
        NKikimr::InitAwsAPI();
    }

    Y_TEST_HOOK_AFTER_RUN(ShutdownAwsAPI) {
        NKikimr::ShutdownAwsAPI();
    }
#endif

    enum class Version {
        V1,
        V2
    };

    enum class MetaType {
        Request,
        Result,
        Response
    };

    template <Version StubVersion>
    using Stub = std::conditional_t<StubVersion == Version::V1, Ydb::KeyValue::V1::KeyValueService::Stub, Ydb::KeyValue::V2::KeyValueService::Stub>;

    template <Version StubVersion>
    using AcquireLockResultType = Ydb::KeyValue::AcquireLockResult;
    template <Version StubVersion>
    using AcquireLockResponseType = std::conditional_t<StubVersion == Version::V1, Ydb::KeyValue::AcquireLockResponse, Ydb::KeyValue::AcquireLockResult>;

    template <typename Type>
    struct TypeHolder {
        using type = Type;
    };

    template <Version StubVersion>
    struct VersionHolder {
        static constexpr Version version = StubVersion;
    };

    template <Version StubVersion, typename Request>
    struct RequestToResponse {
        static_assert(false);
    };
    template <Version StubVersion, typename Response>
    struct ResponseToResult {
        static_assert(false);
    };

    template <typename Response>
    struct ResponseToVersion {
        static_assert(false);
    };

    // ResponseToResult V1
    template<> struct ResponseToResult<Version::V1, Ydb::KeyValue::AcquireLockResponse> : TypeHolder<Ydb::KeyValue::AcquireLockResult> {};
    template<> struct ResponseToResult<Version::V1, Ydb::KeyValue::ExecuteTransactionResponse> : TypeHolder<Ydb::KeyValue::ExecuteTransactionResult> {};
    template<> struct ResponseToResult<Version::V1, Ydb::KeyValue::ReadResponse> : TypeHolder<Ydb::KeyValue::ReadResult> {};
    template<> struct ResponseToResult<Version::V1, Ydb::KeyValue::ReadRangeResponse> : TypeHolder<Ydb::KeyValue::ReadRangeResult> {};
    template<> struct ResponseToResult<Version::V1, Ydb::KeyValue::ListRangeResponse> : TypeHolder<Ydb::KeyValue::ListRangeResult> {};
    template<> struct ResponseToResult<Version::V1, Ydb::KeyValue::GetStorageChannelStatusResponse> : TypeHolder<Ydb::KeyValue::GetStorageChannelStatusResult> {};
    // ResponseToResult V2
    template <typename Response> struct ResponseToResult<Version::V2, Response> : TypeHolder<Response> {};

    // RequestToResponse V1
    template<> struct RequestToResponse<Version::V1, Ydb::KeyValue::AcquireLockRequest> : TypeHolder<Ydb::KeyValue::AcquireLockResponse> {};
    template<> struct RequestToResponse<Version::V1, Ydb::KeyValue::ExecuteTransactionRequest> : TypeHolder<Ydb::KeyValue::ExecuteTransactionResponse> {};
    template<> struct RequestToResponse<Version::V1, Ydb::KeyValue::ReadRequest> : TypeHolder<Ydb::KeyValue::ReadResponse> {};
    template<> struct RequestToResponse<Version::V1, Ydb::KeyValue::ReadRangeRequest> : TypeHolder<Ydb::KeyValue::ReadRangeResponse> {};
    template<> struct RequestToResponse<Version::V1, Ydb::KeyValue::ListRangeRequest> : TypeHolder<Ydb::KeyValue::ListRangeResponse> {};
    template<> struct RequestToResponse<Version::V1, Ydb::KeyValue::GetStorageChannelStatusRequest> : TypeHolder<Ydb::KeyValue::GetStorageChannelStatusResponse> {};
    // RequestToResponse V2
    template<> struct RequestToResponse<Version::V2, Ydb::KeyValue::AcquireLockRequest> : TypeHolder<Ydb::KeyValue::AcquireLockResult> {};
    template<> struct RequestToResponse<Version::V2, Ydb::KeyValue::ExecuteTransactionRequest> : TypeHolder<Ydb::KeyValue::ExecuteTransactionResult> {};
    template<> struct RequestToResponse<Version::V2, Ydb::KeyValue::ReadRequest> : TypeHolder<Ydb::KeyValue::ReadResult> {};
    template<> struct RequestToResponse<Version::V2, Ydb::KeyValue::ReadRangeRequest> : TypeHolder<Ydb::KeyValue::ReadRangeResult> {};
    template<> struct RequestToResponse<Version::V2, Ydb::KeyValue::ListRangeRequest> : TypeHolder<Ydb::KeyValue::ListRangeResult> {};
    template<> struct RequestToResponse<Version::V2, Ydb::KeyValue::GetStorageChannelStatusRequest> : TypeHolder<Ydb::KeyValue::GetStorageChannelStatusResult> {};

    // ResponseToVersion V1
    template<> struct ResponseToVersion<Ydb::KeyValue::AcquireLockResponse> : VersionHolder<Version::V1> {};
    template<> struct ResponseToVersion<Ydb::KeyValue::ExecuteTransactionResponse> : VersionHolder<Version::V1> {};
    template<> struct ResponseToVersion<Ydb::KeyValue::ReadResponse> : VersionHolder<Version::V1> {};
    template<> struct ResponseToVersion<Ydb::KeyValue::ReadRangeResponse> : VersionHolder<Version::V1> {};
    template<> struct ResponseToVersion<Ydb::KeyValue::ListRangeResponse> : VersionHolder<Version::V1> {};
    template<> struct ResponseToVersion<Ydb::KeyValue::GetStorageChannelStatusResponse> : VersionHolder<Version::V1> {};
    // ResponseToVersion V2
    template<> struct ResponseToVersion<Ydb::KeyValue::AcquireLockResult> : VersionHolder<Version::V2> {};
    template<> struct ResponseToVersion<Ydb::KeyValue::ExecuteTransactionResult> : VersionHolder<Version::V2> {};
    template<> struct ResponseToVersion<Ydb::KeyValue::ReadResult> : VersionHolder<Version::V2> {};
    template<> struct ResponseToVersion<Ydb::KeyValue::ReadRangeResult> : VersionHolder<Version::V2> {};
    template<> struct ResponseToVersion<Ydb::KeyValue::ListRangeResult> : VersionHolder<Version::V2> {};
    template<> struct ResponseToVersion<Ydb::KeyValue::GetStorageChannelStatusResult> : VersionHolder<Version::V2> {};

    template <Version StubVersion, typename Request>
    struct RequestToResult : ResponseToResult<StubVersion, typename RequestToResponse<StubVersion, Request>::type> {};

    template <Version StubVersion>
    struct StubHelper {
        template <typename TResponse>
        static auto GetResult(const TResponse &response) {
            if constexpr (StubVersion == Version::V1) {
                using TResult = typename ResponseToResult<StubVersion, TResponse>::type;
                TResult result;
                response.operation().result().UnpackTo(&result);
                return result;
            } else {
                return response;
            }
        }

        template <typename TResponse>
        static Ydb::StatusIds::StatusCode GetStatus(const TResponse &response) {
            if constexpr (StubVersion == Version::V1) {
                return response.operation().status();
            } else {
                return response.status();
            }
        }
    };
}



class TKikimrWithGrpcAndRootSchema {
public:
    TKikimrWithGrpcAndRootSchema(
            NKikimrConfig::TAppConfig appConfig = {},
            TAutoPtr<TLogBackend> logBackend = {},
            const NKikimrBlobDepot::TS3BackendSettings* s3Settings = nullptr)
    {
        ui16 port = PortManager.GetPort(2134);
        ui16 grpc = PortManager.GetPort(2135);
        ServerSettings = new Tests::TServerSettings(port);
        ServerSettings->SetGrpcPort(grpc);
        ServerSettings->SetLogBackend(logBackend);
        ServerSettings->SetDomainName("Root");
        ServerSettings->SetDynamicNodeCount(1);
        if (appConfig.HasImmediateControlsConfig()) {
            ServerSettings->SetControls(appConfig.GetImmediateControlsConfig());
        }
        ServerSettings->AddStoragePool("ssd", "ssd-pool");
        ServerSettings->AddStoragePool("hdd", "hdd-pool");
        ServerSettings->AddStoragePool("hdd1", "hdd1-pool");
        ServerSettings->AddStoragePool("hdd2", "hdd2-pool");
        if (s3Settings) {
            ServerSettings->AddStoragePool("s3", "s3-pool", 0);
        }
        ServerSettings->Formats = new TFormatFactory;
        ServerSettings->FeatureFlags = appConfig.GetFeatureFlags();
        ServerSettings->FeatureFlags.SetAllowUpdateChannelsBindingOfSolomonPartitions(true);
        ServerSettings->RegisterGrpcService<NKikimr::NGRpcService::TKeyValueGRpcServiceV1>("keyvalue");
        ServerSettings->RegisterGrpcService<NKikimr::NGRpcService::TKeyValueGRpcServiceV2>("keyvalue");

        Server_.Reset(new Tests::TServer(*ServerSettings));
        Tenants_.Reset(new Tests::TTenants(Server_));


        //Server_->GetRuntime()->SetLogPriority(NKikimrServices::TX_PROXY_SCHEME_CACHE, NActors::NLog::PRI_DEBUG);
        //Server_->GetRuntime()->SetLogPriority(NKikimrServices::SCHEME_BOARD_REPLICA, NActors::NLog::PRI_DEBUG);
        //Server_->GetRuntime()->SetLogPriority(NKikimrServices::SCHEME_BOARD_SUBSCRIBER, NActors::NLog::PRI_TRACE);
        //Server_->GetRuntime()->SetLogPriority(NKikimrServices::SCHEME_BOARD_POPULATOR, NActors::NLog::PRI_DEBUG);
        //Server_->GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_DEBUG);
        //Server_->GetRuntime()->SetLogPriority(NKikimrServices::TX_PROXY, NActors::NLog::PRI_DEBUG);
        Server_->GetRuntime()->SetLogPriority(NKikimrServices::GRPC_SERVER, NActors::NLog::PRI_DEBUG);
        Server_->GetRuntime()->SetLogPriority(NKikimrServices::GRPC_PROXY, NActors::NLog::PRI_DEBUG);
        Server_->GetRuntime()->SetLogPriority(NKikimrServices::KEYVALUE, NActors::NLog::PRI_DEBUG);
        //Server_->GetRuntime()->SetLogPriority(NKikimrServices::BOOTSTRAPPER, NActors::NLog::PRI_DEBUG);
        //Server_->GetRuntime()->SetLogPriority(NKikimrServices::STATESTORAGE, NActors::NLog::PRI_DEBUG);
        //Server_->GetRuntime()->SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_DEBUG);
        //Server_->GetRuntime()->SetLogPriority(NKikimrServices::SAUSAGE_BIO, NActors::NLog::PRI_DEBUG);
        //Server_->GetRuntime()->SetLogPriority(NKikimrServices::TABLET_FLATBOOT, NActors::NLog::PRI_DEBUG);
        //Server_->GetRuntime()->SetLogPriority(NKikimrServices::TABLET_OPS_HOST, NActors::NLog::PRI_DEBUG);
        //Server_->GetRuntime()->SetLogPriority(NKikimrServices::TABLET_SAUSAGECACHE, NActors::NLog::PRI_DEBUG);
        //Server_->GetRuntime()->SetLogPriority(NKikimrServices::TX_OLAPSHARD, NActors::NLog::PRI_DEBUG);
        //Server_->GetRuntime()->SetLogPriority(NKikimrServices::TX_COLUMNSHARD, NActors::NLog::PRI_DEBUG);

        NYdbGrpc::TServerOptions grpcOption;
        grpcOption.SetPort(grpc);
        Server_->EnableGRpc(grpcOption);

        Tests::TClient annoyingClient(*ServerSettings);
        if (ServerSettings->AppConfig->GetDomainsConfig().GetSecurityConfig().GetEnforceUserTokenRequirement()) {
            annoyingClient.SetSecurityToken("root@builtin");
        }
        annoyingClient.InitRootScheme("Root");
        if (s3Settings) {
            auto* runtime = Server_->GetRuntime();
            auto request = MakeHolder<TEvBlobStorage::TEvControllerConfigRequest>();
            auto* config = request->Record.MutableRequest();
            config->AddCommand()->MutableDefineStoragePool()->CopyFrom(ServerSettings->StoragePoolTypes.at("s3"));
            auto* group = config->AddCommand()->MutableAllocateVirtualGroup();
            group->SetName("kv-s3-group");
            group->SetHiveId(runtime->GetAppData().DomainsInfo->GetHive());
            group->SetStoragePoolName("s3-pool");
            auto* profile = group->AddChannelProfiles();
            profile->SetStoragePoolName("ssd-pool");
            profile->SetCount(2);
            profile = group->AddChannelProfiles();
            profile->SetStoragePoolName("ssd-pool");
            profile->SetChannelKind(NKikimrBlobDepot::TChannelKind::Data);
            group->MutableS3BackendSettings()->CopyFrom(*s3Settings);

            const auto edge = runtime->AllocateEdgeActor();
            NTabletPipe::TClientConfig pipeConfig;
            pipeConfig.RetryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();
            runtime->SendToPipe(MakeBSControllerID(), edge, request.Release(), 0, pipeConfig);
            TAutoPtr<IEventHandle> handle;
            auto* response = runtime->GrabEdgeEvent<TEvBlobStorage::TEvControllerConfigResponse>(handle);
            UNIT_ASSERT_C(response->Record.GetResponse().GetSuccess(), response->Record.DebugString());
            const ui32 groupId = response->Record.GetResponse().GetStatus(1).GetGroupId(0);
            const TInstant deadline = TInstant::Now() + TDuration::Seconds(30);
            for (;;) {
                auto query = MakeHolder<TEvBlobStorage::TEvControllerConfigRequest>();
                query->Record.MutableRequest()->AddCommand()->MutableQueryBaseConfig();
                runtime->SendToPipe(MakeBSControllerID(), edge, query.Release(), 0, pipeConfig);
                response = runtime->GrabEdgeEvent<TEvBlobStorage::TEvControllerConfigResponse>(handle);
                UNIT_ASSERT_C(response->Record.GetResponse().GetSuccess(), response->Record.DebugString());
                bool ready = false;
                for (const auto& item : response->Record.GetResponse().GetStatus(0).GetBaseConfig().GetGroup()) {
                    if (item.GetGroupId() == groupId) {
                        const auto& info = item.GetVirtualGroupInfo();
                        UNIT_ASSERT_C(info.GetState() != NKikimrBlobStorage::EVirtualGroupState::CREATE_FAILED,
                            info.DebugString());
                        ready = info.GetState() == NKikimrBlobStorage::EVirtualGroupState::WORKING;
                    }
                }
                if (ready) {
                    break;
                }
                UNIT_ASSERT_C(TInstant::Now() < deadline, response->Record.DebugString());
                Sleep(TDuration::MilliSeconds(10));
            }
            runtime->Send(CreateEventForBSProxy(edge, groupId, new TEvBlobStorage::TEvStatus(deadline), 0));
            auto* status = runtime->GrabEdgeEvent<TEvBlobStorage::TEvStatusResult>(handle);
            UNIT_ASSERT_VALUES_EQUAL_C(status->Status, NKikimrProto::OK, status->ToString());
        }
        GRpcPort_ = grpc;
    }

    ui16 GetPort() {
        return GRpcPort_;
    }

    TPortManager& GetPortManager() {
        return PortManager;
    }

    void ResetSchemeCache(TString path, ui32 nodeIndex = 0) {
        TTestActorRuntime* runtime = Server_->GetRuntime();
        Tests::TClient annoyingClient(*ServerSettings);
        annoyingClient.RefreshPathCache(runtime, path, nodeIndex);
    }

    TTestActorRuntime* GetRuntime() {
        return Server_->GetRuntime();
    }

    Tests::TServer& GetServer() {
        return *Server_;
    }

    Tests::TServerSettings::TPtr ServerSettings;
    Tests::TServer::TPtr Server_;
    THolder<Tests::TTenants> Tenants_;
private:
    TPortManager PortManager;
    ui16 GRpcPort_;
};


Y_UNIT_TEST_SUITE(KeyValueGRPCService) {

    void InitTablet(TKikimrWithGrpcAndRootSchema &server, ui64 tabletId) {
        server.GetRuntime()->SetScheduledLimit(100);
        CreateTestBootstrapper(*server.GetRuntime(),
            CreateTestTabletInfo(tabletId, TTabletTypes::KeyValue),
            &CreateKeyValueFlat);
        NanoSleep(3'000'000'000);
    }

    void CmdWrite(ui64 tabletId, const TDeque<TString> &keys, const TDeque<TString> &values, TKikimrWithGrpcAndRootSchema &server)
    {
        Y_ABORT_UNLESS(keys.size() == values.size());
        TAutoPtr<IEventHandle> handle;
        TEvKeyValue::TEvResponse *result;
        THolder<TEvKeyValue::TEvRequest> request;
        TActorId edgeActor = server.GetRuntime()->AllocateEdgeActor();
        for (i32 retriesLeft = 2; retriesLeft > 0; --retriesLeft) {
            try {
                server.GetRuntime()->ResetScheduledCount();
                request.Reset(new TEvKeyValue::TEvRequest);
                for (ui64 idx = 0; idx < keys.size(); ++idx) {
                    auto write = request->Record.AddCmdWrite();
                    write->SetKey(keys[idx]);
                    write->SetValue(values[idx]);
                    write->SetStorageChannel(NKikimrClient::TKeyValueRequest::MAIN);
                    write->SetPriority(NKikimrClient::TKeyValueRequest::REALTIME);
                }
                server.GetRuntime()->SendToPipe(tabletId, edgeActor, request.Release(), 0, GetPipeConfigWithRetries());
                result = server.GetRuntime()->GrabEdgeEvent<TEvKeyValue::TEvResponse>(handle);
                UNIT_ASSERT(result);
                UNIT_ASSERT(result->Record.HasStatus());
                UNIT_ASSERT_EQUAL(result->Record.GetStatus(), NMsgBusProxy::MSTATUS_OK);
                UNIT_ASSERT_VALUES_EQUAL(result->Record.WriteResultSize(), values.size());
                for (ui64 idx = 0; idx < values.size(); ++idx) {
                    const auto &writeResult = result->Record.GetWriteResult(idx);
                    UNIT_ASSERT(writeResult.HasStatus());
                    UNIT_ASSERT_EQUAL(writeResult.GetStatus(), NKikimrProto::OK);
                    UNIT_ASSERT(writeResult.HasStatusFlags());
                    if (values[idx].size()) {
                        UNIT_ASSERT(writeResult.GetStatusFlags() & ui32(NKikimrBlobStorage::StatusIsValid));
                    }
                }
                retriesLeft = 0;
            } catch (NActors::TSchedulingLimitReachedException) {
                UNIT_ASSERT(retriesLeft == 2);
            }
        }
    }

    template <typename TCtx>
    void AdjustCtxForDB(TCtx &ctx) {
        ctx.AddMetadata(NYdb::YDB_DATABASE_HEADER, "/Root");
        ctx.AddMetadata(NYdb::YDB_AUTH_TICKET_HEADER, "root@builtin");
    }

    void MakeDirectory(auto &channel, const TString &path) {
        std::unique_ptr<Ydb::Scheme::V1::SchemeService::Stub> stub;
        stub = Ydb::Scheme::V1::SchemeService::NewStub(channel);

        Ydb::Scheme::MakeDirectoryRequest makeDirectoryRequest;
        makeDirectoryRequest.set_path(path);
        Ydb::Scheme::MakeDirectoryResponse makeDirectoryResponse;
        grpc::ClientContext makeDirectoryCtx;
        AdjustCtxForDB(makeDirectoryCtx);
        stub->MakeDirectory(&makeDirectoryCtx, makeDirectoryRequest, &makeDirectoryResponse);
        UNIT_ASSERT_CHECK_STATUS(makeDirectoryResponse.operation(), Ydb::StatusIds::SUCCESS);
    }

    void MakeTable(auto &channel, const TString &path, const TString &dataMedia = "ssd") {
        std::unique_ptr<Ydb::KeyValue::V1::KeyValueService::Stub> stub;
        stub = Ydb::KeyValue::V1::KeyValueService::NewStub(channel);

        Ydb::KeyValue::CreateVolumeRequest createVolumeRequest;
        createVolumeRequest.set_path(path);
        createVolumeRequest.set_partition_count(1);
        auto *storage_config = createVolumeRequest.mutable_storage_config();
        storage_config->add_channel()->set_media("ssd");
        storage_config->add_channel()->set_media("ssd");
        storage_config->add_channel()->set_media(dataMedia);

        Ydb::KeyValue::CreateVolumeResponse createVolumeResponse;
        Ydb::KeyValue::CreateVolumeResult createVolumeResult;

        grpc::ClientContext createVolumeCtx;
        AdjustCtxForDB(createVolumeCtx);
        stub->CreateVolume(&createVolumeCtx, createVolumeRequest, &createVolumeResponse);
        UNIT_ASSERT_CHECK_STATUS(createVolumeResponse.operation(), Ydb::StatusIds::SUCCESS);
        createVolumeResponse.operation().result().UnpackTo(&createVolumeResult);
    }

    void AlterVolume(auto &channel, const TString &path, ui32 partition_count = 1, std::optional<Ydb::KeyValue::StorageConfig> storage_config = {}) {
        std::unique_ptr<Ydb::KeyValue::V1::KeyValueService::Stub> stub;
        stub = Ydb::KeyValue::V1::KeyValueService::NewStub(channel);

        Ydb::KeyValue::AlterVolumeRequest alterVolumeRequest;
        alterVolumeRequest.set_path(path);
        alterVolumeRequest.set_alter_partition_count(partition_count);
        if (storage_config) {
            auto *storageConfig = alterVolumeRequest.mutable_storage_config();
            for (const auto &channel : storage_config->channel()) {
                auto *channelBind = storageConfig->add_channel();
                channelBind->set_media(channel.media());
            }
        }

        Ydb::KeyValue::AlterVolumeResponse alterVolumeResponse;
        Ydb::KeyValue::AlterVolumeResult alterVolumeResult;

        grpc::ClientContext alterVolumeCtx;
        AdjustCtxForDB(alterVolumeCtx);
        stub->AlterVolume(&alterVolumeCtx, alterVolumeRequest, &alterVolumeResponse);
        UNIT_ASSERT_CHECK_STATUS(alterVolumeResponse.operation(), Ydb::StatusIds::SUCCESS);
        alterVolumeResponse.operation().result().UnpackTo(&alterVolumeResult);
    }

    void DropVolume(auto &channel, const TString &path) {
        std::unique_ptr<Ydb::KeyValue::V1::KeyValueService::Stub> stub;
        stub = Ydb::KeyValue::V1::KeyValueService::NewStub(channel);

        Ydb::KeyValue::DropVolumeRequest dropVolumeRequest;
        dropVolumeRequest.set_path(path);

        Ydb::KeyValue::DropVolumeResponse dropVolumeResponse;
        Ydb::KeyValue::DropVolumeResult dropVolumeResult;

        grpc::ClientContext dropVolumeCtx;
        AdjustCtxForDB(dropVolumeCtx);
        stub->DropVolume(&dropVolumeCtx, dropVolumeRequest, &dropVolumeResponse);
        UNIT_ASSERT_CHECK_STATUS(dropVolumeResponse.operation(), Ydb::StatusIds::SUCCESS);
        dropVolumeResponse.operation().result().UnpackTo(&dropVolumeResult);
    }

    Ydb::KeyValue::DescribeVolumeResult DescribeVolume(auto &channel, const TString &path) {
        std::unique_ptr<Ydb::KeyValue::V1::KeyValueService::Stub> stub;
        stub = Ydb::KeyValue::V1::KeyValueService::NewStub(channel);

        Ydb::KeyValue::DescribeVolumeRequest describeVolumeRequest;
        describeVolumeRequest.set_path(path);

        Ydb::KeyValue::DescribeVolumeResponse describeVolumeResponse;
        Ydb::KeyValue::DescribeVolumeResult describeVolumeResult;

        grpc::ClientContext describeVolumeCtx;
        AdjustCtxForDB(describeVolumeCtx);
        stub->DescribeVolume(&describeVolumeCtx, describeVolumeRequest, &describeVolumeResponse);
        UNIT_ASSERT_CHECK_STATUS(describeVolumeResponse.operation(), Ydb::StatusIds::SUCCESS);
        describeVolumeResponse.operation().result().UnpackTo(&describeVolumeResult);
        return describeVolumeResult;
    }


    Ydb::Scheme::ListDirectoryResult ListDirectory(auto &channel, const TString &path) {
        std::unique_ptr<Ydb::Scheme::V1::SchemeService::Stub> stub;
        stub = Ydb::Scheme::V1::SchemeService::NewStub(channel);
        Ydb::Scheme::ListDirectoryRequest listDirectoryRequest;
        listDirectoryRequest.set_path(path);

        Ydb::Scheme::ListDirectoryResult listDirectoryResult;
        Ydb::Scheme::ListDirectoryResponse listDirectoryResponse;

        grpc::ClientContext listDirectoryCtx;
        AdjustCtxForDB(listDirectoryCtx);
        stub->ListDirectory(&listDirectoryCtx, listDirectoryRequest, &listDirectoryResponse);

        UNIT_ASSERT_CHECK_STATUS(listDirectoryResponse.operation(), Ydb::StatusIds::SUCCESS);
        listDirectoryResponse.operation().result().UnpackTo(&listDirectoryResult);
        return listDirectoryResult;
    }

    ui64 AcquireLock( const TString &path, ui64 partitionId, auto &stub) {
        Ydb::KeyValue::AcquireLockRequest request;
        request.set_path(path);
        request.set_partition_id(partitionId);

        Ydb::KeyValue::AcquireLockResponse response;
        Ydb::KeyValue::AcquireLockResult result;

        grpc::ClientContext ctx;
        AdjustCtxForDB(ctx);
        stub->AcquireLock(&ctx, request, &response);
        UNIT_ASSERT_CHECK_STATUS(response.operation(), Ydb::StatusIds::SUCCESS);
        response.operation().result().UnpackTo(&result);
        return result.lock_generation();
    }

    void WaitTableCreation(TKikimrWithGrpcAndRootSchema &server, const TString &path) {
        bool again = true;
        for (ui32 i = 0; i < 10 && again; ++i) {
            Cerr << "Wait iteration# " << i << Endl;
            auto req = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
            auto& entry = req->ResultSet.emplace_back();
            entry.Path = SplitPath(path);
            entry.RequestType = NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByPath;
            entry.ShowPrivatePath = true;
            entry.SyncVersion = false;
            req->UserToken = new NACLib::TUserToken("root@builtin", {});
            UNIT_ASSERT(req->UserToken);
            TActorId edgeActor = server.GetRuntime()->AllocateEdgeActor();
            auto ev = new TEvTxProxySchemeCache::TEvNavigateKeySet(req.Release());
            UNIT_ASSERT(ev->Request->UserToken);
            auto schemeCache = MakeSchemeCacheID();
            server.GetRuntime()->Send(new IEventHandle(schemeCache, edgeActor, ev));

            TAutoPtr<IEventHandle> handle;
            auto *result = server.GetRuntime()->GrabEdgeEvent<TEvTxProxySchemeCache::TEvNavigateKeySetResult>(handle);
            UNIT_ASSERT_VALUES_EQUAL(result->Request->ResultSet.size(), 1);
            again = result->Request->ResultSet[0].Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok;
        }
    }

    template <Version StubVersion>
    void MakeSimpleTest(const TString &tablePath,
            std::function<void(const std::unique_ptr<Stub<StubVersion>>&)> func)
    {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;

        ////////////////////////////////////////////////////////////////////////

        std::shared_ptr<grpc::Channel> channel;
        channel = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        MakeDirectory(channel, "/Root/mydb");
        MakeTable(channel, tablePath);
        auto pr = SplitPath(tablePath);
        Ydb::Scheme::ListDirectoryResult listDirectoryResult = ListDirectory(channel, "/Root/mydb");
        UNIT_ASSERT_VALUES_EQUAL(listDirectoryResult.self().name(), "mydb");
        UNIT_ASSERT_VALUES_EQUAL(listDirectoryResult.children(0).name(), pr.back());

        WaitTableCreation(server, tablePath);
        if constexpr (StubVersion == Version::V1) {
            std::unique_ptr<Ydb::KeyValue::V1::KeyValueService::Stub> stub;
            stub = Ydb::KeyValue::V1::KeyValueService::NewStub(channel);
            func(stub);
        } else {
            std::unique_ptr<Ydb::KeyValue::V2::KeyValueService::Stub> stub;
            stub = Ydb::KeyValue::V2::KeyValueService::NewStub(channel);
            func(stub);
        }
    }

    template <typename TFunc>
    auto WithRetry(ui64 count, TFunc func) {
        Ydb::StatusIds::StatusCode status = Ydb::StatusIds::UNAVAILABLE;
        std::decay_t<decltype(func())> response;
        for (ui32 i = 0; i < count && status == Ydb::StatusIds::UNAVAILABLE; ++i) {
            response = func();
            status = StubHelper<ResponseToVersion<decltype(response)>::version>::GetStatus(response);
        }
        return response;
    }

    template <Version StubVersion, Ydb::StatusIds::StatusCode ExpectedCode = Ydb::StatusIds::SUCCESS>
    Ydb::KeyValue::AcquireLockResult AcquireLock(const Ydb::KeyValue::AcquireLockRequest &acquireLockRequest, const std::unique_ptr<Stub<StubVersion>> &stub) {
        auto response = WithRetry(5, [&] {
            using TResponse = RequestToResponse<StubVersion, Ydb::KeyValue::AcquireLockRequest>::type;
            TResponse response;
            grpc::ClientContext ctx;
            AdjustCtxForDB(ctx);
            grpc::Status status = stub->AcquireLock(&ctx, acquireLockRequest, &response);
            UNIT_ASSERT_C(status.ok(), status.error_code());
            return response;
        });
        UNIT_ASSERT_STATUS_EQUALS(StubHelper<StubVersion>::GetStatus(response), ExpectedCode);
        return StubHelper<StubVersion>::GetResult(response);
    }

    template <Version StubVersion, Ydb::StatusIds::StatusCode ExpectedCode = Ydb::StatusIds::SUCCESS>
    Ydb::KeyValue::ExecuteTransactionResult ExecuteTransaction(const Ydb::KeyValue::ExecuteTransactionRequest &request, const std::unique_ptr<Stub<StubVersion>> &stub)
    {
        auto response = WithRetry(5, [&] {
            using TResponse = RequestToResponse<StubVersion, Ydb::KeyValue::ExecuteTransactionRequest>::type;
            TResponse response;
            grpc::ClientContext ctx;
            AdjustCtxForDB(ctx);
            grpc::Status status = stub->ExecuteTransaction(&ctx, request, &response);
            UNIT_ASSERT_C(status.ok(), status.error_code());
            return response;
        });
        UNIT_ASSERT_STATUS_EQUALS(StubHelper<StubVersion>::GetStatus(response), ExpectedCode);
        return StubHelper<StubVersion>::GetResult(response);
    }

    template <Version StubVersion, Ydb::StatusIds::StatusCode ExpectedCode = Ydb::StatusIds::SUCCESS>
    Ydb::KeyValue::ReadResult Read(const Ydb::KeyValue::ReadRequest &request, const std::unique_ptr<Stub<StubVersion>> &stub)
    {
        auto response = WithRetry(5, [&] {
            using TResponse = RequestToResponse<StubVersion, Ydb::KeyValue::ReadRequest>::type;
            TResponse response;
            grpc::ClientContext ctx;
            AdjustCtxForDB(ctx);
            grpc::Status status = stub->Read(&ctx, request, &response);
            UNIT_ASSERT_C(status.ok(), status.error_code());
            return response;
        });
        UNIT_ASSERT_STATUS_EQUALS(StubHelper<StubVersion>::GetStatus(response), ExpectedCode);
        return StubHelper<StubVersion>::GetResult(response);
    }

    template <Version StubVersion, Ydb::StatusIds::StatusCode ExpectedCode = Ydb::StatusIds::SUCCESS>
    Ydb::KeyValue::ReadRangeResult ReadRange(const Ydb::KeyValue::ReadRangeRequest &request, const std::unique_ptr<Stub<StubVersion>> &stub)
    {
        auto response = WithRetry(5, [&] {
            using TResponse = RequestToResponse<StubVersion, Ydb::KeyValue::ReadRangeRequest>::type;
            TResponse response;
            grpc::ClientContext ctx;
            AdjustCtxForDB(ctx);
            grpc::Status status = stub->ReadRange(&ctx, request, &response);
            UNIT_ASSERT_C(status.ok(), status.error_code());
            return response;
        });
        UNIT_ASSERT_STATUS_EQUALS(StubHelper<StubVersion>::GetStatus(response), ExpectedCode);
        return StubHelper<StubVersion>::GetResult(response);
    }

    template <Version StubVersion, Ydb::StatusIds::StatusCode ExpectedCode = Ydb::StatusIds::SUCCESS>
    Ydb::KeyValue::ListRangeResult ListRange(const Ydb::KeyValue::ListRangeRequest &request, const std::unique_ptr<Stub<StubVersion>> &stub)
    {
        auto response = WithRetry(5, [&] {
            using TResponse = RequestToResponse<StubVersion, Ydb::KeyValue::ListRangeRequest>::type;
            TResponse response;
            grpc::ClientContext ctx;
            AdjustCtxForDB(ctx);
            grpc::Status status = stub->ListRange(&ctx, request, &response);
            UNIT_ASSERT_C(status.ok(), status.error_code());
            return response;
        });
        UNIT_ASSERT_STATUS_EQUALS(StubHelper<StubVersion>::GetStatus(response), ExpectedCode);
        return StubHelper<StubVersion>::GetResult(response);
    }

    template <Version StubVersion, Ydb::StatusIds::StatusCode ExpectedCode = Ydb::StatusIds::SUCCESS>
    Ydb::KeyValue::GetStorageChannelStatusResult GetStorageChannelStatus(const Ydb::KeyValue::GetStorageChannelStatusRequest &request, const std::unique_ptr<Stub<StubVersion>> &stub)
    {
        auto response = WithRetry(5, [&] {
            using TResponse = RequestToResponse<StubVersion, Ydb::KeyValue::GetStorageChannelStatusRequest>::type;
            TResponse response;
            grpc::ClientContext ctx;
            AdjustCtxForDB(ctx);
            grpc::Status status = stub->GetStorageChannelStatus(&ctx, request, &response);
            UNIT_ASSERT_C(status.ok(), status.error_code());
            return response;
        });
        UNIT_ASSERT_STATUS_EQUALS(StubHelper<StubVersion>::GetStatus(response), ExpectedCode);
        return StubHelper<StubVersion>::GetResult(response);
    }

#define Y_UNIT_TEST_BOTH_VERSION(Name) \
    template <Version StubVersion> void Test ## Name (); \
    Y_UNIT_TEST(Name ## V1) { Test ## Name <Version::V1>(); } \
    Y_UNIT_TEST(Name ## V2) { Test ## Name <Version::V2>(); } \
    template <Version StubVersion> void Test ## Name ()
// Y_UNIT_TEST_BOTH_VERSION

    Y_UNIT_TEST_BOTH_VERSION(TestSimpleExecuteTransaction) {
        TString tablePath = "/Root/mydb/kvtable";
        MakeSimpleTest<StubVersion>(tablePath, [tablePath](const std::unique_ptr<Stub<StubVersion>> &stub){
            Ydb::KeyValue::ExecuteTransactionRequest request;
            request.set_path(tablePath);
            request.set_partition_id(0);
            ExecuteTransaction<StubVersion>(request, stub);
        });
    }

    Y_UNIT_TEST_BOTH_VERSION(SimpleExecuteTransactionWithWrongGeneration) {
        TString tablePath = "/Root/mydb/kvtable";
        MakeSimpleTest<StubVersion>(tablePath, [tablePath](const std::unique_ptr<Stub<StubVersion>> &stub){
            Ydb::KeyValue::ExecuteTransactionRequest request;
            request.set_path(tablePath);
            request.set_partition_id(0);
            request.set_lock_generation(42);
            ExecuteTransaction<StubVersion, Ydb::StatusIds::PRECONDITION_FAILED>(request, stub);
        });
    }

    template <Version StubVersion>
    Ydb::KeyValue::ExecuteTransactionResult Write(const TString &path, ui64 partitionId, const TString &key, const TString &value, ui64 storageChannel,
            const std::unique_ptr<Stub<StubVersion>> &stub)
    {
        Ydb::KeyValue::ExecuteTransactionRequest request;
        request.set_path(path);
        request.set_partition_id(partitionId);
        auto *cmd = request.add_commands();
        auto *write = cmd->mutable_write();
        write->set_key(key);
        write->set_value(value);
        write->set_storage_channel(storageChannel);
        return ExecuteTransaction<StubVersion>(request, stub);
    }

    template <Version StubVersion>
    Ydb::KeyValue::ExecuteTransactionResult Rename(const TString &path, ui64 partitionId, const TString &oldKey, const TString &newKey,
            const std::unique_ptr<Stub<StubVersion>> &stub)
    {
        Ydb::KeyValue::ExecuteTransactionRequest request;
        request.set_path(path);
        request.set_partition_id(partitionId);
        auto *cmd = request.add_commands();
        auto *rename = cmd->mutable_rename();
        rename->set_old_key(oldKey);
        rename->set_new_key(newKey);
        return ExecuteTransaction<StubVersion>(request, stub);
    }

    Y_UNIT_TEST_BOTH_VERSION(SimpleAcquireLock) {
        TString tablePath = "/Root/mydb/kvtable";
        MakeSimpleTest<StubVersion>(tablePath, [tablePath](const std::unique_ptr<Stub<StubVersion>> &stub){
            Ydb::KeyValue::AcquireLockRequest request;
            request.set_path(tablePath);
            request.set_partition_id(0);

            auto result1 = AcquireLock<StubVersion>(request, stub);
            UNIT_ASSERT(result1.lock_generation() == 1);

            auto result2 = AcquireLock<StubVersion>(request, stub);
            UNIT_ASSERT(result2.lock_generation() == 2);
        });
    };


    Y_UNIT_TEST_BOTH_VERSION(SimpleRenameUnexistedKey) {
        TString tablePath = "/Root/mydb/kvtable";
        MakeSimpleTest<StubVersion>(tablePath, [tablePath](const std::unique_ptr<Stub<StubVersion>> &stub){
            Ydb::KeyValue::ExecuteTransactionRequest request;
            request.set_path(tablePath);
            request.set_partition_id(0);
            auto *cmd = request.add_commands();
            auto *rename = cmd->mutable_rename();
            rename->set_old_key("key1");
            rename->set_new_key("key2");
            ExecuteTransaction<StubVersion, Ydb::StatusIds::NOT_FOUND>(request, stub);
        });
    }

    Y_UNIT_TEST_BOTH_VERSION(SimpleConcatUnexistedKey) {
        TString tablePath = "/Root/mydb/kvtable";
        MakeSimpleTest<StubVersion>(tablePath, [tablePath](const std::unique_ptr<Stub<StubVersion>> &stub){
            Ydb::KeyValue::ExecuteTransactionRequest request;
            request.set_path(tablePath);
            request.set_partition_id(0);
            auto *cmd = request.add_commands();
            auto *concat = cmd->mutable_concat();
            concat->add_input_keys("key1");
            concat->add_input_keys("key2");
            concat->set_output_key("key3");
            Ydb::KeyValue::ExecuteTransactionResponse response;
            ExecuteTransaction<StubVersion, Ydb::StatusIds::NOT_FOUND>(request, stub);
        });
    }

    Y_UNIT_TEST_BOTH_VERSION(SimpleCopyUnexistedKey) {
        TString tablePath = "/Root/mydb/kvtable";
        MakeSimpleTest<StubVersion>(tablePath, [tablePath](const std::unique_ptr<Stub<StubVersion>> &stub){
            Ydb::KeyValue::ExecuteTransactionRequest request;
            request.set_path(tablePath);
            request.set_partition_id(0);
            auto *cmd = request.add_commands();
            auto *rename = cmd->mutable_copy_range();
            auto *range = rename->mutable_range();
            range->set_from_key_inclusive("key1");
            range->set_to_key_inclusive("key2");
            rename->set_prefix_to_add("A");
            ExecuteTransaction<StubVersion>(request, stub);
        });
    }

    Y_UNIT_TEST_BOTH_VERSION(SimpleWriteRead) {
        TString tablePath = "/Root/mydb/kvtable";
        MakeSimpleTest<StubVersion>(tablePath, [tablePath](const std::unique_ptr<Stub<StubVersion>> &stub){
            Write<StubVersion>(tablePath, 0, "key", "value", 0, stub);

            Ydb::KeyValue::ReadRequest readRequest;
            readRequest.set_path(tablePath);
            readRequest.set_partition_id(0);
            readRequest.set_key("key");
            Ydb::KeyValue::ReadResult readResult = Read<StubVersion>(readRequest, stub);

            UNIT_ASSERT(!readResult.is_overrun());
            UNIT_ASSERT_VALUES_EQUAL(readResult.requested_key(), "key");
            UNIT_ASSERT_VALUES_EQUAL(readResult.value(), "value");
            UNIT_ASSERT_VALUES_EQUAL(readResult.requested_offset(), 0);
            UNIT_ASSERT_VALUES_EQUAL(readResult.requested_size(), 5);
        });
    }

    Y_UNIT_TEST_BOTH_VERSION(TabletErrorInIssues) {
        TKikimrWithGrpcAndRootSchema server;
        const TString tablePath = "/Root/mydb/kvtable";
        auto channel = grpc::CreateChannel("localhost:" + ToString(server.GetPort()), grpc::InsecureChannelCredentials());
        MakeDirectory(channel, "/Root/mydb");
        MakeTable(channel, tablePath);
        WaitTableCreation(server, tablePath);
        auto stub = std::make_unique<Stub<StubVersion>>(channel);
        Write<StubVersion>(tablePath, 0, "key", "value", 0, stub);

        auto checkResponse = [](const auto &response) {
            UNIT_ASSERT_STATUS_EQUALS(StubHelper<StubVersion>::GetStatus(response), Ydb::StatusIds::PRECONDITION_FAILED);
            const auto &issues = [&]() -> const auto& {
                if constexpr (StubVersion == Version::V1) {
                    return response.operation().issues();
                } else {
                    return response.issues();
                }
            }();
            UNIT_ASSERT_VALUES_EQUAL(issues.size(), 1);
            UNIT_ASSERT_STRING_CONTAINS(issues.Get(0).message(), "Generation mismatch! Requested# 42");
        };

        {
            Ydb::KeyValue::ReadRequest request;
            request.set_path(tablePath);
            request.set_lock_generation(42);
            request.set_key("key");
            typename RequestToResponse<StubVersion, Ydb::KeyValue::ReadRequest>::type response;
            grpc::ClientContext ctx;
            AdjustCtxForDB(ctx);
            UNIT_ASSERT(stub->Read(&ctx, request, &response).ok());
            checkResponse(response);
        }
        {
            Ydb::KeyValue::ReadRangeRequest request;
            request.set_path(tablePath);
            request.set_lock_generation(42);
            request.mutable_range()->set_from_key_inclusive("key");
            request.mutable_range()->set_to_key_inclusive("key");
            typename RequestToResponse<StubVersion, Ydb::KeyValue::ReadRangeRequest>::type response;
            grpc::ClientContext ctx;
            AdjustCtxForDB(ctx);
            UNIT_ASSERT(stub->ReadRange(&ctx, request, &response).ok());
            checkResponse(response);
        }
        {
            Ydb::KeyValue::ExecuteTransactionRequest request;
            request.set_path(tablePath);
            request.set_lock_generation(42);
            auto *write = request.add_commands()->mutable_write();
            write->set_key("key");
            write->set_value("value");
            typename RequestToResponse<StubVersion, Ydb::KeyValue::ExecuteTransactionRequest>::type response;
            grpc::ClientContext ctx;
            AdjustCtxForDB(ctx);
            UNIT_ASSERT(stub->ExecuteTransaction(&ctx, request, &response).ok());
            checkResponse(response);
        }
    }

#ifndef KIKIMR_DISABLE_S3_OPS
    Y_UNIT_TEST_BOTH_VERSION(S3ErrorInIssues) {
        using TS3Mock = NWrappers::NTestHelpers::TS3Mock;
        const TString errorMessage = "Access to KV test objects is denied by S3";
        const TString errorBody = TStringBuilder()
            << "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
            << "<Error><Code>AccessDenied</Code><Message>" << errorMessage << "</Message></Error>";
        const TString errorResponse = TStringBuilder()
            << "HTTP/1.1 403 Forbidden\r\nContent-Type: application/xml\r\n"
            << "Content-Length: " << errorBody.size() << "\r\nConnection: close\r\n\r\n" << errorBody;
        std::atomic<bool> failPuts{true};
        std::atomic<bool> failGets{false};
        std::atomic<ui32> failedPuts{0};
        std::atomic<ui32> failedGets{0};
        TPortManager portManager;
        const ui16 s3Port = portManager.GetPort();
        TS3Mock::TSettings mockSettings(s3Port);
        mockSettings.ErrorResponse = [&](TStringBuf method, TStringBuf path) -> TString {
            // Leave bucket listing and garbage collection available to BlobDepot.
            if (path.StartsWith("/kv-test/objects/")) {
                if (method == "PUT" && failPuts.load()) {
                    ++failedPuts;
                    return errorResponse;
                }
                if (method == "GET" && failGets.load()) {
                    ++failedGets;
                    return errorResponse;
                }
            }
            return {};
        };
        TS3Mock s3(mockSettings);
        UNIT_ASSERT_C(s3.Start(), s3.GetError());

        NKikimrBlobDepot::TS3BackendSettings s3Settings;
        s3Settings.MutableSyncMode();
        auto* settings = s3Settings.MutableSettings();
        settings->SetEndpoint(TStringBuilder() << "localhost:" << s3Port);
        settings->SetScheme(NKikimrSchemeOp::TS3Settings::HTTP);
        settings->SetBucket("kv-test");
        settings->SetObjectKeyPattern("objects");
        settings->SetAccessKey("test-access-key");
        settings->SetSecretKey("test-secret-key");
        settings->SetRegion("us-east-1");
        settings->SetUseVirtualAddressing(false);
        TKikimrWithGrpcAndRootSchema server({}, {}, &s3Settings);
        auto channel = grpc::CreateChannel("localhost:" + ToString(server.GetPort()), grpc::InsecureChannelCredentials());
        const TString tablePath = "/Root/mydb/kvtable";
        MakeDirectory(channel, "/Root/mydb");
        MakeTable(channel, tablePath, "s3");
        WaitTableCreation(server, tablePath);
        auto stub = std::make_unique<Stub<StubVersion>>(channel);

        auto checkError = [&](const auto& response) {
            UNIT_ASSERT_STATUS_EQUALS(StubHelper<StubVersion>::GetStatus(response), Ydb::StatusIds::INTERNAL_ERROR);
            const auto& issues = [&]() -> const auto& {
                if constexpr (StubVersion == Version::V1) {
                    return response.operation().issues();
                } else {
                    return response.issues();
                }
            }();
            UNIT_ASSERT_VALUES_EQUAL(issues.size(), 1);
            UNIT_ASSERT_STRING_CONTAINS(issues.Get(0).message(), errorMessage);
        };

        const TString value(16 * 1024, 'x');
        {
            Ydb::KeyValue::ExecuteTransactionRequest request;
            request.set_path(tablePath);
            request.set_partition_id(0);
            auto* write = request.add_commands()->mutable_write();
            write->set_key("key");
            write->set_value(value);
            write->set_storage_channel(2);
            typename RequestToResponse<StubVersion, Ydb::KeyValue::ExecuteTransactionRequest>::type response;
            grpc::ClientContext ctx;
            AdjustCtxForDB(ctx);
            UNIT_ASSERT(stub->ExecuteTransaction(&ctx, request, &response).ok());
            UNIT_ASSERT_GT_C(failedPuts.load(), 0, response.ShortDebugString());
            checkError(response);
        }

        failPuts = false;
        Write<StubVersion>(tablePath, 0, "key", value, 2, stub);

        Ydb::KeyValue::ReadRequest readRequest;
        readRequest.set_path(tablePath);
        readRequest.set_partition_id(0);
        readRequest.set_key("key");
        UNIT_ASSERT_VALUES_EQUAL(Read<StubVersion>(readRequest, stub).value(), value);

        failGets = true;
        {
            typename RequestToResponse<StubVersion, Ydb::KeyValue::ReadRequest>::type response;
            grpc::ClientContext ctx;
            AdjustCtxForDB(ctx);
            UNIT_ASSERT(stub->Read(&ctx, readRequest, &response).ok());
            UNIT_ASSERT_GT(failedGets.load(), 0);
            checkError(response);
        }
        {
            const ui32 previousFailedGets = failedGets.load();
            Ydb::KeyValue::ReadRangeRequest request;
            request.set_path(tablePath);
            request.set_partition_id(0);
            request.mutable_range()->set_from_key_inclusive("key");
            request.mutable_range()->set_to_key_inclusive("key");
            typename RequestToResponse<StubVersion, Ydb::KeyValue::ReadRangeRequest>::type response;
            grpc::ClientContext ctx;
            AdjustCtxForDB(ctx);
            UNIT_ASSERT(stub->ReadRange(&ctx, request, &response).ok());
            UNIT_ASSERT_GT(failedGets.load(), previousFailedGets);
            checkError(response);
        }

        failGets = false;
        UNIT_ASSERT_VALUES_EQUAL(Read<StubVersion>(readRequest, stub).value(), value);
    }
#endif

    Y_UNIT_TEST(SimpleWriteReadRangeV2WithUsePayloadControl) {
        TString tablePath = "/Root/mydb/kvtable";
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableImmediateControlsConfig()->MutableKeyValueVolumeControls()->SetUsePayload(1);

        TKikimrWithGrpcAndRootSchema server(appConfig);
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> channel;
        channel = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        MakeDirectory(channel, "/Root/mydb");
        MakeTable(channel, tablePath);
        WaitTableCreation(server, tablePath);

        std::unique_ptr<Ydb::KeyValue::V2::KeyValueService::Stub> stub;
        stub = Ydb::KeyValue::V2::KeyValueService::NewStub(channel);

        Write<Version::V2>(tablePath, 0, "key1", "value1", 1, stub);
        Write<Version::V2>(tablePath, 0, "key2", "value22", 2, stub);

        Ydb::KeyValue::ReadRangeRequest readRangeRequest;
        readRangeRequest.set_path(tablePath);
        readRangeRequest.set_partition_id(0);
        auto *range = readRangeRequest.mutable_range();
        range->set_from_key_inclusive("key1");
        range->set_to_key_inclusive("key3");

        Ydb::KeyValue::ReadRangeResult readRangeResult = ReadRange<Version::V2>(readRangeRequest, stub);

        UNIT_ASSERT_VALUES_EQUAL(readRangeResult.pair_size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(readRangeResult.pair(0).key(), "key1");
        UNIT_ASSERT_VALUES_EQUAL(readRangeResult.pair(0).value(), "value1");
        UNIT_ASSERT_VALUES_EQUAL(readRangeResult.pair(1).key(), "key2");
        UNIT_ASSERT_VALUES_EQUAL(readRangeResult.pair(1).value(), "value22");
    }

    Y_UNIT_TEST(SimpleWriteReadRangeV2WithUsePayloadAndCustomSerializationControl) {
        TString tablePath = "/Root/mydb/kvtable";
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableImmediateControlsConfig()->MutableKeyValueVolumeControls()->SetUsePayload(1);
        appConfig.MutableImmediateControlsConfig()->MutableKeyValueVolumeControls()->SetUseCustomSerialization(1);

        TKikimrWithGrpcAndRootSchema server(appConfig);
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> channel;
        channel = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        MakeDirectory(channel, "/Root/mydb");
        MakeTable(channel, tablePath);
        WaitTableCreation(server, tablePath);

        std::unique_ptr<Ydb::KeyValue::V2::KeyValueService::Stub> stub;
        stub = Ydb::KeyValue::V2::KeyValueService::NewStub(channel);

        Write<Version::V2>(tablePath, 0, "key1", "value1", 1, stub);
        Write<Version::V2>(tablePath, 0, "key2", "value22", 2, stub);

        Ydb::KeyValue::ReadRangeRequest readRangeRequest;
        readRangeRequest.set_path(tablePath);
        readRangeRequest.set_partition_id(0);
        auto *range = readRangeRequest.mutable_range();
        range->set_from_key_inclusive("key1");
        range->set_to_key_inclusive("key3");

        Ydb::KeyValue::ReadRangeResult readRangeResult = ReadRange<Version::V2>(readRangeRequest, stub);

        UNIT_ASSERT_VALUES_EQUAL(readRangeResult.pair_size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(readRangeResult.pair(0).key(), "key1");
        UNIT_ASSERT_VALUES_EQUAL(readRangeResult.pair(0).value(), "value1");
        UNIT_ASSERT_VALUES_EQUAL(readRangeResult.pair(1).key(), "key2");
        UNIT_ASSERT_VALUES_EQUAL(readRangeResult.pair(1).value(), "value22");
    }

    Y_UNIT_TEST(SimpleWriteReadV2WithUsePayloadAndCustomSerializationControl) {
        TString tablePath = "/Root/mydb/kvtable";
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableImmediateControlsConfig()->MutableKeyValueVolumeControls()->SetUsePayload(1);
        appConfig.MutableImmediateControlsConfig()->MutableKeyValueVolumeControls()->SetUseCustomSerialization(1);

        TKikimrWithGrpcAndRootSchema server(appConfig);
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> channel;
        channel = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        MakeDirectory(channel, "/Root/mydb");
        MakeTable(channel, tablePath);
        WaitTableCreation(server, tablePath);

        std::unique_ptr<Ydb::KeyValue::V2::KeyValueService::Stub> stub;
        stub = Ydb::KeyValue::V2::KeyValueService::NewStub(channel);

        Write<Version::V2>(tablePath, 0, "key", "value", 1, stub);

        Ydb::KeyValue::ReadRequest readRequest;
        readRequest.set_path(tablePath);
        readRequest.set_partition_id(0);
        readRequest.set_key("key");
        Ydb::KeyValue::ReadResult readResult = Read<Version::V2>(readRequest, stub);

        UNIT_ASSERT_VALUES_EQUAL(readResult.value(), "value");
    }

    Y_UNIT_TEST(SimpleExecuteWriteV2WithUsePayloadControl) {
        TString tablePath = "/Root/mydb/kvtable";
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableImmediateControlsConfig()->MutableKeyValueVolumeControls()->SetUsePayload(1);

        TKikimrWithGrpcAndRootSchema server(appConfig);
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> channel;
        channel = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        MakeDirectory(channel, "/Root/mydb");
        MakeTable(channel, tablePath);
        WaitTableCreation(server, tablePath);

        std::unique_ptr<Ydb::KeyValue::V2::KeyValueService::Stub> stub;
        stub = Ydb::KeyValue::V2::KeyValueService::NewStub(channel);

        Ydb::KeyValue::ExecuteTransactionRequest txRequest;
        txRequest.set_path(tablePath);
        txRequest.set_partition_id(0);
        auto *cmd = txRequest.add_commands();
        auto *write = cmd->mutable_write();
        write->set_key("payload_key");
        write->set_value("payload_value");
        write->set_storage_channel(2);
        ExecuteTransaction<Version::V2>(txRequest, stub);

        Ydb::KeyValue::ReadRequest readRequest;
        readRequest.set_path(tablePath);
        readRequest.set_partition_id(0);
        readRequest.set_key("payload_key");
        Ydb::KeyValue::ReadResult readResult = Read<Version::V2>(readRequest, stub);

        UNIT_ASSERT_VALUES_EQUAL(readResult.value(), "payload_value");
    }

    Y_UNIT_TEST_BOTH_VERSION(SimpleWriteReadWithIncorreectPath) {
        TString tablePath = "/Root/mydb/kvtable";
        MakeSimpleTest<StubVersion>(tablePath, [tablePath](const std::unique_ptr<Stub<StubVersion>> &stub){
            Write<StubVersion>(tablePath, 0, "key", "value", 0, stub);

            Ydb::KeyValue::ReadRequest readRequest;
            readRequest.set_path("/Root/mydb/table");
            readRequest.set_partition_id(0);
            readRequest.set_key("key");

            Read<StubVersion, Ydb::StatusIds::SCHEME_ERROR>(readRequest, stub);
        });
    }

    Y_UNIT_TEST_BOTH_VERSION(SimpleWriteReadWithoutToken) {
        TString tablePath = "/Root/mydb/kvtable";
        MakeSimpleTest<StubVersion>(tablePath, [tablePath](const std::unique_ptr<Stub<StubVersion>> &stub){
            return;
            Write<StubVersion>(tablePath, 0, "key", "value", 0, stub);

            Ydb::KeyValue::ReadRequest readRequest;
            readRequest.set_path("/Root/mydb/kvtable");
            readRequest.set_partition_id(0);
            readRequest.set_key("key");
            using TResponse = RequestToResponse<StubVersion, Ydb::KeyValue::ReadRequest>::type;
            TResponse readResponse;

            grpc::ClientContext readCtx;
            //AdjustCtxForDB(readCtx);
            stub->Read(&readCtx, readRequest, &readResponse);
            UNIT_ASSERT_STATUS_EQUALS(StubHelper<StubVersion>::GetStatus(readResponse), Ydb::StatusIds::SCHEME_ERROR);
        });
    }

    Y_UNIT_TEST_BOTH_VERSION(SimpleWriteReadWithoutLockGeneration1) {
        TString tablePath = "/Root/mydb/kvtable";
        MakeSimpleTest<StubVersion>(tablePath, [tablePath](const std::unique_ptr<Stub<StubVersion>> &stub){
            Ydb::KeyValue::AcquireLockRequest lockReq;
            lockReq.set_path(tablePath);
            lockReq.set_partition_id(0);
            AcquireLock<StubVersion>(lockReq, stub);

            Write<StubVersion>(tablePath, 0, "key", "value", 0, stub);

            Ydb::KeyValue::ReadRequest readRequest;
            readRequest.set_path(tablePath);
            readRequest.set_partition_id(0);
            readRequest.set_key("key");
            Read<StubVersion>(readRequest, stub);
        });
    }

    Y_UNIT_TEST_BOTH_VERSION(SimpleWriteReadWithoutLockGeneration2) {
        TString tablePath = "/Root/mydb/kvtable";
        MakeSimpleTest<StubVersion>(tablePath, [tablePath](const std::unique_ptr<Stub<StubVersion>> &stub){
            Write<StubVersion>(tablePath, 0, "key", "value", 0, stub);

            Ydb::KeyValue::AcquireLockRequest lockReq;
            lockReq.set_path(tablePath);
            lockReq.set_partition_id(0);
            AcquireLock<StubVersion>(lockReq, stub);

            Ydb::KeyValue::ReadRequest readRequest;
            readRequest.set_path(tablePath);
            readRequest.set_partition_id(0);
            readRequest.set_key("key");
            Read<StubVersion>(readRequest, stub);
        });
    }

    Y_UNIT_TEST_BOTH_VERSION(SimpleWriteReadWithGetChannelStatus) {
        TString tablePath = "/Root/mydb/kvtable";
        MakeSimpleTest<StubVersion>(tablePath, [tablePath](const std::unique_ptr<Stub<StubVersion>> &stub){
            Ydb::KeyValue::GetStorageChannelStatusRequest getStatusRequest;
            getStatusRequest.set_path(tablePath);
            getStatusRequest.add_storage_channel(0);
            GetStorageChannelStatus<StubVersion>(getStatusRequest, stub);

            Write<StubVersion>(tablePath, 0, "key", "value", 0, stub);

            Ydb::KeyValue::GetStorageChannelStatusRequest getStatusRequest2;
            getStatusRequest2.set_path(tablePath);
            getStatusRequest2.add_storage_channel(0);
            GetStorageChannelStatus<StubVersion>(getStatusRequest2, stub);

            Ydb::KeyValue::ReadRequest readRequest;
            readRequest.set_path(tablePath);
            readRequest.set_partition_id(0);
            readRequest.set_key("key");
            Read<StubVersion>(readRequest, stub);
        });
    }

    Y_UNIT_TEST_BOTH_VERSION(SimpleWriteReadOverrun) {
        TString tablePath = "/Root/mydb/kvtable";
        MakeSimpleTest<StubVersion>(tablePath, [tablePath](const std::unique_ptr<Stub<StubVersion>> &stub){
            Write<StubVersion>(tablePath, 0, "key", "value", 0, stub);

            Ydb::KeyValue::ReadRequest readRequest;
            readRequest.set_path(tablePath);
            readRequest.set_partition_id(0);
            readRequest.set_key("key");
            ui64 limitBytes = 1 + 5 + 3
                    + 1 + 5 + 1
                    + 1 + 8
                    + 1 + 8
                    + 1 + 1
                    ;
            readRequest.set_limit_bytes(limitBytes);

            Ydb::KeyValue::ReadResult readResult = Read<StubVersion>(readRequest, stub);
            UNIT_ASSERT(readResult.is_overrun());
            UNIT_ASSERT_VALUES_EQUAL(readResult.requested_key(), "key");
            UNIT_ASSERT_VALUES_EQUAL(readResult.value(), "v");
            UNIT_ASSERT_VALUES_EQUAL(readResult.requested_offset(), 0);
            UNIT_ASSERT_VALUES_EQUAL(readResult.requested_size(), 5);
        });
    }

    Y_UNIT_TEST_BOTH_VERSION(SimpleWriteReadRange) {
        TString tablePath = "/Root/mydb/kvtable";
        MakeSimpleTest<StubVersion>(tablePath, [tablePath](const std::unique_ptr<Stub<StubVersion>> &stub){
            Write<StubVersion>(tablePath, 0, "key1", "value1", 1, stub);
            Write<StubVersion>(tablePath, 0, "key2", "value12", 2, stub);

            Ydb::KeyValue::ReadRangeRequest readRangeRequest;
            readRangeRequest.set_path(tablePath);
            readRangeRequest.set_partition_id(0);
            auto *r = readRangeRequest.mutable_range();
            r->set_from_key_inclusive("key1");
            r->set_to_key_inclusive("key3");

            Ydb::KeyValue::ReadRangeResult readRangeResult = ReadRange<StubVersion>(readRangeRequest, stub);

            UNIT_ASSERT_VALUES_EQUAL(readRangeResult.pair(0).key(), "key1");
            UNIT_ASSERT_VALUES_EQUAL(readRangeResult.pair(1).key(), "key2");

            UNIT_ASSERT_VALUES_EQUAL(readRangeResult.pair(0).value(), "value1");
            UNIT_ASSERT_VALUES_EQUAL(readRangeResult.pair(1).value(), "value12");

            UNIT_ASSERT_VALUES_EQUAL(readRangeResult.pair(0).storage_channel(), 1);
            UNIT_ASSERT_VALUES_EQUAL(readRangeResult.pair(1).storage_channel(), 2);
        });
    }


    Y_UNIT_TEST_BOTH_VERSION(SimpleWriteListRange) {
        TString tablePath = "/Root/mydb/kvtable";
        MakeSimpleTest<StubVersion>(tablePath, [tablePath](const std::unique_ptr<Stub<StubVersion>> &stub){
            Write<StubVersion>(tablePath, 0, "key1", "value1", 1, stub);
            Write<StubVersion>(tablePath, 0, "key2", "value12", 2, stub);

            Ydb::KeyValue::ListRangeRequest listRangeRequest;
            listRangeRequest.set_path(tablePath);
            listRangeRequest.set_partition_id(0);
            auto *r = listRangeRequest.mutable_range();
            r->set_from_key_inclusive("key1");
            r->set_to_key_inclusive("key3");

            Ydb::KeyValue::ListRangeResult listRangeResult = ListRange<StubVersion>(listRangeRequest, stub);

            UNIT_ASSERT_VALUES_EQUAL(listRangeResult.key(0).key(), "key1");
            UNIT_ASSERT_VALUES_EQUAL(listRangeResult.key(1).key(), "key2");

            UNIT_ASSERT_VALUES_EQUAL(listRangeResult.key(0).value_size(), 6);
            UNIT_ASSERT_VALUES_EQUAL(listRangeResult.key(1).value_size(), 7);

            UNIT_ASSERT_VALUES_EQUAL(listRangeResult.key(0).storage_channel(), 1);
            UNIT_ASSERT_VALUES_EQUAL(listRangeResult.key(1).storage_channel(), 2);
        });
    }


    Y_UNIT_TEST_BOTH_VERSION(SimpleGetStorageChannelStatus) {
        TString tablePath = "/Root/mydb/kvtable";
        MakeSimpleTest<StubVersion>(tablePath, [tablePath](const std::unique_ptr<Stub<StubVersion>> &stub){
            Ydb::KeyValue::GetStorageChannelStatusRequest getStatusRequest;
            getStatusRequest.set_path(tablePath);
            getStatusRequest.set_partition_id(0);
            getStatusRequest.add_storage_channel(1);

            Ydb::KeyValue::GetStorageChannelStatusResult getStatusResult = GetStorageChannelStatus<StubVersion>(getStatusRequest, stub);
            UNIT_ASSERT_VALUES_EQUAL(getStatusResult.storage_channel_info_size(), 1);
        });
    }

    Y_UNIT_TEST(SimpleCreateAlterDropVolume) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;

        std::shared_ptr<grpc::Channel> channel;
        channel = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());

        TString path = "/Root/mydb/";
        TString tablePath = "/Root/mydb/mytable";
        MakeDirectory(channel, path);
        MakeTable(channel, tablePath);

        Ydb::Scheme::ListDirectoryResult listDirectoryResult = ListDirectory(channel, path);
        UNIT_ASSERT_VALUES_EQUAL(listDirectoryResult.self().name(), "mydb");
        UNIT_ASSERT_VALUES_EQUAL(listDirectoryResult.children(0).name(), "mytable");

        auto describeVolumeResult = DescribeVolume(channel, tablePath);
        UNIT_ASSERT_VALUES_EQUAL(1, describeVolumeResult.partition_count());
        UNIT_ASSERT(describeVolumeResult.has_storage_config());
        UNIT_ASSERT_VALUES_EQUAL(describeVolumeResult.storage_config().channel_size(), 3);
        for (const auto& channel : describeVolumeResult.storage_config().channel()) {
            UNIT_ASSERT_VALUES_EQUAL(channel.media(), "ssd");
        }

        AlterVolume(channel, tablePath, 2);
        listDirectoryResult = ListDirectory(channel, path);
        UNIT_ASSERT_VALUES_EQUAL(listDirectoryResult.self().name(), "mydb");
        UNIT_ASSERT_VALUES_EQUAL(listDirectoryResult.children(0).name(), "mytable");

        describeVolumeResult = DescribeVolume(channel, tablePath);
        UNIT_ASSERT_VALUES_EQUAL(2, describeVolumeResult.partition_count());
        UNIT_ASSERT(describeVolumeResult.has_storage_config());
        UNIT_ASSERT_VALUES_EQUAL(describeVolumeResult.storage_config().channel_size(), 3);
        for (const auto& channel : describeVolumeResult.storage_config().channel()) {
            UNIT_ASSERT_VALUES_EQUAL(channel.media(), "ssd");
        }

        ui32 currentChannelCount = describeVolumeResult.storage_config().channel_size();
        Ydb::KeyValue::StorageConfig storageConfig = describeVolumeResult.storage_config();
        auto newChannel = storageConfig.add_channel();
        newChannel->set_media("ssd");

        AlterVolume(channel, tablePath, 2, storageConfig);
        describeVolumeResult = DescribeVolume(channel, tablePath);
        UNIT_ASSERT_VALUES_EQUAL(2, describeVolumeResult.partition_count());
        UNIT_ASSERT(describeVolumeResult.has_storage_config());
        UNIT_ASSERT_VALUES_EQUAL(describeVolumeResult.storage_config().channel_size(), currentChannelCount + 1);
        for (const auto& channel : describeVolumeResult.storage_config().channel()) {
            UNIT_ASSERT_VALUES_EQUAL(channel.media(), "ssd");
        }

        DropVolume(channel, tablePath);
        listDirectoryResult = ListDirectory(channel, path);
        UNIT_ASSERT_VALUES_EQUAL(listDirectoryResult.self().name(), "mydb");
        UNIT_ASSERT_VALUES_EQUAL(listDirectoryResult.children_size(), 0);
    }

    Y_UNIT_TEST(SimpleListPartitions) {
        return; // delete it after adding ydb_token to requests in tests
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;

        std::shared_ptr<grpc::Channel> channel;
        channel = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());

        TString path = "/Root/mydb/";
        TString tablePath = "/Root/mydb/mytable";
        MakeDirectory(channel, path);
        MakeTable(channel, tablePath);

        std::unique_ptr<Ydb::KeyValue::V1::KeyValueService::Stub> stub;
        stub = Ydb::KeyValue::V1::KeyValueService::NewStub(channel);

        Write<Version::V1>(tablePath, 0, "key1", "value1", 1, stub);

        Ydb::KeyValue::ListLocalPartitionsRequest enumerateRequest;
        enumerateRequest.set_path(tablePath);
        enumerateRequest.set_node_id(2);

        Ydb::KeyValue::ListLocalPartitionsResult enumerateResult;
        Ydb::KeyValue::ListLocalPartitionsResponse eumerateResponse;

        grpc::ClientContext enumerateCtx;
        AdjustCtxForDB(enumerateCtx);
        stub->ListLocalPartitions(&enumerateCtx, enumerateRequest, &eumerateResponse);

        UNIT_ASSERT_CHECK_STATUS(eumerateResponse.operation(), Ydb::StatusIds::SUCCESS);
        eumerateResponse.operation().result().UnpackTo(&enumerateResult);
        UNIT_ASSERT_VALUES_EQUAL(enumerateResult.partition_ids_size(), 1);

        auto writeRes = Write<Version::V1>(tablePath, enumerateResult.partition_ids(0), "key2", "value2", 1, stub);
        UNIT_ASSERT_VALUES_EQUAL(writeRes.node_id(), 2);
    }

} // Y_UNIT_TEST_SUITE(KeyValueGRPCService)

} // NKikimr::NGRpcService

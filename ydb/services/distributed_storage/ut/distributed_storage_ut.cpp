#include <ydb/services/distributed_storage/grpc_service.h>

#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/blobstorage/nodewarden/node_warden_events.h>
#include <ydb/core/formats/factory.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/core/grpc_services/service_distributed_storage.h>
#include <ydb/core/protos/stream.pb.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/public/api/grpc/draft/ydb_distributed_storage_v1.grpc.pb.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>

#include <util/datetime/base.h>
#include <util/string/builder.h>
#include <util/generic/vector.h>

#include <chrono>
#include <utility>

namespace NKikimr::NGRpcService {
namespace {

class TStartChangeDataCenterState : public TActorBootstrapped<TStartChangeDataCenterState>, public IFacilityProvider {
    std::unique_ptr<IRequestOpCtx> Request;

public:
    explicit TStartChangeDataCenterState(std::unique_ptr<IRequestOpCtx> request)
        : Request(std::move(request))
    {}

    void Bootstrap() {
        DoChangeDataCenterState(std::move(Request), *this);
        PassAway();
    }

    ui64 GetChannelBufferSize() const override { return 0; }
    TActorId RegisterActor(IActor* actor) const override { return Register(actor); }
};

class TDistributedStorageTestServer {
public:
    TDistributedStorageTestServer() {
        const ui16 kikimrPort = PortManager.GetPort(2134);
        const ui16 grpcPort = PortManager.GetPort(2135);

        ServerSettings = new Tests::TServerSettings(kikimrPort);
        ServerSettings->SetGrpcPort(grpcPort);
        ServerSettings->SetDomainName("Root");
        ServerSettings->SetDynamicNodeCount(1);
        ServerSettings->AddStoragePool("ssd", TString{}, 2);
        ServerSettings->Formats = new TFormatFactory;
        ServerSettings->RegisterGrpcService<TDistributedStorageGRpcService>("distributed_storage");

        Server.Reset(new Tests::TServer(*ServerSettings));
        Tenants.Reset(new Tests::TTenants(Server));

        NYdbGrpc::TServerOptions grpcOptions;
        grpcOptions.SetHost("localhost").SetPort(grpcPort);
        Server->EnableGRpc(grpcOptions);

        Tests::TClient client(*ServerSettings);
        client.InitRootScheme("Root");

        Channel = grpc::CreateChannel(TStringBuilder() << "localhost:" << grpcPort, grpc::InsecureChannelCredentials());
    }

    const std::shared_ptr<grpc::Channel>& GetChannel() const {
        return Channel;
    }

    NActors::TTestActorRuntime* GetRuntime() const {
        return Server->GetRuntime();
    }

    void AddSparePDisk() {
        auto& runtime = *GetRuntime();
        const TActorId sender = runtime.AllocateEdgeActor();

        auto readRequest = MakeHolder<TEvBlobStorage::TEvControllerConfigRequest>();
        readRequest->Record.MutableRequest()->AddCommand()->MutableReadHostConfig();
        runtime.SendToPipe(MakeBSControllerID(), sender, readRequest.Release());

        TAutoPtr<IEventHandle> readHandle;
        const auto readResponse = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvControllerConfigResponse>(readHandle);
        const auto& response = readResponse->Record.GetResponse();
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        UNIT_ASSERT_VALUES_EQUAL(response.StatusSize(), 1);
        UNIT_ASSERT_C(response.GetStatus(0).GetSuccess(), response.GetStatus(0).GetErrorDescription());

        const ui32 nodeId = runtime.GetNodeId(0);
        NKikimrBlobStorage::TDefineHostConfig hostConfig;
        for (const auto& config : response.GetStatus(0).GetHostConfig()) {
            if (config.GetHostConfigId() == nodeId) {
                hostConfig.CopyFrom(config);
                break;
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(hostConfig.GetHostConfigId(), nodeId);
        hostConfig.AddDrive()->SetPath(TStringBuilder() << runtime.GetTempDir() << "pdisk_2.dat");

        auto defineRequest = MakeHolder<TEvBlobStorage::TEvControllerConfigRequest>();
        defineRequest->Record.MutableRequest()->AddCommand()->MutableDefineHostConfig()->CopyFrom(hostConfig);
        runtime.SendToPipe(MakeBSControllerID(), sender, defineRequest.Release());

        TAutoPtr<IEventHandle> defineHandle;
        const auto defineResponse = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvControllerConfigResponse>(defineHandle);
        const auto& define = defineResponse->Record.GetResponse();
        UNIT_ASSERT_C(define.GetSuccess(), define.GetErrorDescription());
        UNIT_ASSERT_VALUES_EQUAL(define.StatusSize(), 1);
        UNIT_ASSERT_C(define.GetStatus(0).GetSuccess(), define.GetStatus(0).GetErrorDescription());
    }

private:
    TPortManager PortManager;
    Tests::TServerSettings::TPtr ServerSettings;
    Tests::TServer::TPtr Server;
    THolder<Tests::TTenants> Tenants;
    std::shared_ptr<grpc::Channel> Channel;
};

struct TStorageStateOptions {
    bool StoragePools = false;
    bool Groups = false;
    bool VDisks = false;
    bool PDisks = false;
    bool Nodes = false;
    bool Devices = false;
    bool Settings = false;

    static TStorageStateOptions All() {
        return {
            .StoragePools = true,
            .Groups = true,
            .VDisks = true,
            .PDisks = true,
            .Nodes = true,
            .Devices = true,
            .Settings = true,
        };
    }
};

struct TStorageStateResult {
    Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::SUCCESS;
    size_t Parts = 0;
    size_t MaxPartSize = 0;
    google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage> Issues;
    TVector<Ydb::DistributedStorage::StoragePool> StoragePools;
    TVector<Ydb::DistributedStorage::Group> Groups;
    TVector<Ydb::DistributedStorage::VDisk> VDisks;
    TVector<Ydb::DistributedStorage::PDisk> PDisks;
    TVector<Ydb::DistributedStorage::Node> Nodes;
    TVector<Ydb::DistributedStorage::Device> Devices;
    Ydb::DistributedStorage::ClusterSettings Settings;
    bool HasSettings = false;
};

TStorageStateResult StreamStorageState(const std::shared_ptr<grpc::Channel>& channel, const TStorageStateOptions& options = {}) {
    auto stub = Ydb::DistributedStorage::V1::DistributedStorageService::NewStub(channel);

    Ydb::DistributedStorage::StorageStateRequest request;
    request.set_include_storage_pools(options.StoragePools);
    request.set_include_groups(options.Groups);
    request.set_include_vdisks(options.VDisks);
    request.set_include_pdisks(options.PDisks);
    request.set_include_nodes(options.Nodes);
    request.set_include_devices(options.Devices);
    request.set_include_settings(options.Settings);

    grpc::ClientContext context;
    auto reader = stub->StreamStorageState(&context, request);

    TStorageStateResult result;
    Ydb::DistributedStorage::StorageStateResponse part;
    while (reader->Read(&part)) {
        ++result.Parts;
        result.MaxPartSize = Max(result.MaxPartSize, part.ByteSizeLong());
        result.Status = part.status();
        result.Issues.MergeFrom(part.issues());

        if (part.has_result()) {
            const auto& partResult = part.result();
            result.StoragePools.insert(result.StoragePools.end(), partResult.storage_pools().begin(), partResult.storage_pools().end());
            result.Groups.insert(result.Groups.end(), partResult.groups().begin(), partResult.groups().end());
            result.VDisks.insert(result.VDisks.end(), partResult.vdisks().begin(), partResult.vdisks().end());
            result.PDisks.insert(result.PDisks.end(), partResult.pdisks().begin(), partResult.pdisks().end());
            result.Nodes.insert(result.Nodes.end(), partResult.nodes().begin(), partResult.nodes().end());
            result.Devices.insert(result.Devices.end(), partResult.devices().begin(), partResult.devices().end());
            if (partResult.has_settings()) {
                UNIT_ASSERT(!result.HasSettings);
                result.Settings.CopyFrom(partResult.settings());
                result.HasSettings = true;
            }
        }
    }

    const grpc::Status status = reader->Finish();
    UNIT_ASSERT_C(status.ok(), status.error_message());
    return result;
}

Ydb::DistributedStorage::ReassignVDiskResponse ReassignVDisk(const std::shared_ptr<grpc::Channel>& channel, const Ydb::DistributedStorage::ReassignVDiskRequest& request) {
    auto stub = Ydb::DistributedStorage::V1::DistributedStorageService::NewStub(channel);

    grpc::ClientContext context;
    Ydb::DistributedStorage::ReassignVDiskResponse response;
    const grpc::Status status = stub->ReassignVDisk(&context, request, &response);
    UNIT_ASSERT_C(status.ok(), status.error_message());
    return response;
}

template <typename TResult, typename TResponse>
TResult ExtractMutationResult(const TResponse& response) {
    UNIT_ASSERT_C(response.operation().ready(), response.operation().DebugString());
    UNIT_ASSERT_VALUES_EQUAL_C(response.operation().status(), Ydb::StatusIds::SUCCESS, response.operation().DebugString());

    TResult result;
    UNIT_ASSERT_C(response.operation().result().UnpackTo(&result), response.operation().DebugString());
    return result;
}

Ydb::DistributedStorage::ChangeDataCenterStateResponse ChangeDataCenterState(const std::shared_ptr<grpc::Channel>& channel,
        Ydb::DistributedStorage::ChangeDataCenterStateRequest::Action action, const TString& name, bool dryRun = false) {
    auto stub = Ydb::DistributedStorage::V1::DistributedStorageService::NewStub(channel);
    Ydb::DistributedStorage::ChangeDataCenterStateRequest request;
    request.set_action(action);
    request.set_datacenter_name(name);
    request.set_dry_run(dryRun);

    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(15));
    Ydb::DistributedStorage::ChangeDataCenterStateResponse response;
    grpc::Status status = stub->ChangeDataCenterState(&context, request, &response);
    UNIT_ASSERT_C(status.ok(), status.error_message());
    return response;
}

bool SameVDiskPosition(const Ydb::DistributedStorage::VDiskId& lhs, const Ydb::DistributedStorage::VDiskId& rhs) {
    return lhs.group_id() == rhs.group_id()
        && lhs.fail_realm_idx() == rhs.fail_realm_idx()
        && lhs.fail_domain_idx() == rhs.fail_domain_idx()
        && lhs.vdisk_idx() == rhs.vdisk_idx();
}

bool SameVDiskId(const Ydb::DistributedStorage::VDiskId& lhs, const Ydb::DistributedStorage::VDiskId& rhs) {
    return SameVDiskPosition(lhs, rhs)
        && lhs.group_generation() == rhs.group_generation();
}

bool SameVSlotId(const Ydb::DistributedStorage::VSlotId& lhs, const Ydb::DistributedStorage::VSlotId& rhs) {
    return lhs.node_id() == rhs.node_id()
        && lhs.pdisk_id() == rhs.pdisk_id()
        && lhs.vslot_id() == rhs.vslot_id();
}

const Ydb::DistributedStorage::VDisk* FindVDisk(const TStorageStateResult& storage, bool isStatic) {
    for (const auto& vdisk : storage.VDisks) {
        if (vdisk.is_static() == isStatic) {
            return &vdisk;
        }
    }
    return nullptr;
}

const Ydb::DistributedStorage::VDisk* FindVDisk(const TStorageStateResult& storage, const Ydb::DistributedStorage::VDiskId& id) {
    for (const auto& vdisk : storage.VDisks) {
        if (SameVDiskId(vdisk.id(), id)) {
            return &vdisk;
        }
    }
    return nullptr;
}

void IgnoreAllSafetyChecks(Ydb::DistributedStorage::SafetyOptions& safety) {
    safety.set_ignore_degraded_groups(true);
    safety.set_ignore_group_failure_model(true);
    safety.set_ignore_group_layout_checks(true);
}

} // namespace

Y_UNIT_TEST_SUITE(DistributedStorageGRpcService) {
    Y_UNIT_TEST(StorageStateContainsAllSectionsIncludingStaticStorage) {
        TDistributedStorageTestServer server;
        const auto result = StreamStorageState(server.GetChannel(), TStorageStateOptions::All());

        UNIT_ASSERT_VALUES_EQUAL(result.Status, Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT(!result.StoragePools.empty());
        UNIT_ASSERT(!result.Groups.empty());
        UNIT_ASSERT(!result.VDisks.empty());
        UNIT_ASSERT(!result.PDisks.empty());
        UNIT_ASSERT(!result.Nodes.empty());
        UNIT_ASSERT(result.HasSettings);

        bool hasStaticGroup = false;
        for (const auto& group : result.Groups) {
            hasStaticGroup |= group.is_static();
        }
        UNIT_ASSERT(hasStaticGroup);

        bool hasStaticVDisk = false;
        for (const auto& vdisk : result.VDisks) {
            hasStaticVDisk |= vdisk.is_static();
        }
        UNIT_ASSERT(hasStaticVDisk);
    }

    Y_UNIT_TEST(StorageStateContainsOnlyRequestedSections) {
        TDistributedStorageTestServer server;

        const auto groups = StreamStorageState(server.GetChannel(), {.Groups = true});
        UNIT_ASSERT(!groups.Groups.empty());
        UNIT_ASSERT(groups.StoragePools.empty());
        UNIT_ASSERT(groups.VDisks.empty());
        UNIT_ASSERT(groups.PDisks.empty());
        UNIT_ASSERT(groups.Nodes.empty());
        UNIT_ASSERT(groups.Devices.empty());
        UNIT_ASSERT(!groups.HasSettings);

        const auto pools = StreamStorageState(server.GetChannel(), {.StoragePools = true});
        UNIT_ASSERT(!pools.StoragePools.empty());
        UNIT_ASSERT(pools.Groups.empty());
        UNIT_ASSERT(pools.VDisks.empty());
        UNIT_ASSERT(pools.PDisks.empty());
        UNIT_ASSERT(pools.Nodes.empty());
        UNIT_ASSERT(pools.Devices.empty());
        UNIT_ASSERT(!pools.HasSettings);
    }

    Y_UNIT_TEST(StorageStateIncludesSettings) {
        TDistributedStorageTestServer server;

        const auto result = StreamStorageState(server.GetChannel(), {.Settings = true});
        UNIT_ASSERT(result.HasSettings);
        UNIT_ASSERT(result.Settings.default_max_slots() > 0);
        UNIT_ASSERT(result.Settings.pdisk_space_color_border() != Ydb::DistributedStorage::SPACE_COLOR_UNSPECIFIED);
        UNIT_ASSERT(result.StoragePools.empty());
        UNIT_ASSERT(result.Groups.empty());
        UNIT_ASSERT(result.VDisks.empty());
        UNIT_ASSERT(result.PDisks.empty());
        UNIT_ASSERT(result.Nodes.empty());
        UNIT_ASSERT(result.Devices.empty());
    }

    Y_UNIT_TEST(EmptyRequestIsSuccessfulPing) {
        TDistributedStorageTestServer server;
        const auto result = StreamStorageState(server.GetChannel());

        UNIT_ASSERT_VALUES_EQUAL(result.Status, Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT(result.StoragePools.empty());
        UNIT_ASSERT(result.Groups.empty());
        UNIT_ASSERT(result.VDisks.empty());
        UNIT_ASSERT(result.PDisks.empty());
        UNIT_ASSERT(result.Nodes.empty());
        UNIT_ASSERT(result.Devices.empty());
        UNIT_ASSERT(!result.HasSettings);
    }

    Y_UNIT_TEST(StorageStateSnapshotIsSplitByConfiguredMessageLimit) {
        TDistributedStorageTestServer server;

        const auto initial = StreamStorageState(server.GetChannel(), {.VDisks = true});
        UNIT_ASSERT(initial.VDisks.size() > 1);

        size_t messageSizeLimit = 0;
        for (const auto& vdisk : initial.VDisks) {
            Ydb::DistributedStorage::StorageStateResponse part;
            part.set_status(Ydb::StatusIds::SUCCESS);
            part.mutable_result()->add_vdisks()->CopyFrom(vdisk);
            messageSizeLimit = Max(messageSizeLimit, part.ByteSizeLong());
        }
        server.GetRuntime()->GetAppData(0).StreamingConfig.MutableOutputStreamConfig()->SetMessageSizeLimit(messageSizeLimit);

        const auto result = StreamStorageState(server.GetChannel(), {.VDisks = true});
        UNIT_ASSERT_VALUES_EQUAL(result.Status, Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_C(result.Parts > 1, result.Parts);
        UNIT_ASSERT_C(result.MaxPartSize <= messageSizeLimit, result.MaxPartSize);
        UNIT_ASSERT(!result.VDisks.empty());
        UNIT_ASSERT(result.Groups.empty());
        UNIT_ASSERT(result.PDisks.empty());
    }

    Y_UNIT_TEST(RejectNonAdministrator) {
        TDistributedStorageTestServer server;
        server.GetRuntime()->GetAppData(0).AdministrationAllowedSIDs = {"admin@builtin"};

        const auto response = StreamStorageState(server.GetChannel(), TStorageStateOptions::All());
        UNIT_ASSERT_VALUES_EQUAL(response.Status, Ydb::StatusIds::UNAUTHORIZED);
    }

    Y_UNIT_TEST(DynamicReassign) {
        TDistributedStorageTestServer server;
        server.AddSparePDisk();

        size_t dynamicPDiskCount = 0;
        size_t activeDynamicPDiskCount = 0;
        const TInstant deadline = TInstant::Now() + TDuration::Seconds(10);
        do {
            const auto storage = StreamStorageState(server.GetChannel(), {.PDisks = true});
            UNIT_ASSERT_VALUES_EQUAL(storage.Status, Ydb::StatusIds::SUCCESS);
            dynamicPDiskCount = 0;
            activeDynamicPDiskCount = 0;
            for (const auto& pdisk : storage.PDisks) {
                if (!pdisk.is_static()) {
                    ++dynamicPDiskCount;
                    activeDynamicPDiskCount += pdisk.status() == Ydb::DistributedStorage::PDISK_STATUS_ACTIVE;
                }
            }
            if (activeDynamicPDiskCount >= 2) {
                break;
            }
            Sleep(TDuration::MilliSeconds(100));
        } while (TInstant::Now() < deadline);
        UNIT_ASSERT_C(activeDynamicPDiskCount >= 2, "dynamic# " << dynamicPDiskCount << " active# " << activeDynamicPDiskCount);

        const auto before = StreamStorageState(server.GetChannel(), {.VDisks = true});

        const auto* selected = FindVDisk(before, false);
        UNIT_ASSERT(selected);

        Ydb::DistributedStorage::ReassignVDiskRequest request;
        request.mutable_vdisk_id()->CopyFrom(selected->id());
        request.set_dry_run(true);
        IgnoreAllSafetyChecks(*request.mutable_options()->mutable_safety());
        request.mutable_options()->set_allow_existing_ineligible_pdisks(true);
        request.mutable_options()->set_ignore_target_space_check(true);

        const auto result = ExtractMutationResult<Ydb::DistributedStorage::ReassignVDiskResult>(ReassignVDisk(server.GetChannel(), request));
        UNIT_ASSERT_C(SameVDiskId(result.vdisk_id(), selected->id()), result.DebugString());
        UNIT_ASSERT_C(SameVSlotId(result.source_slot_id(), selected->slot_id()), result.DebugString());
        UNIT_ASSERT_C(result.source_slot_id().node_id() != result.target_slot_id().node_id()
                          || result.source_slot_id().pdisk_id() != result.target_slot_id().pdisk_id(),
                      result.DebugString());

        Ydb::DistributedStorage::ReassignVDiskRequest targetedRequest = request;
        const ui32 targetNodeId = result.target_slot_id().node_id();
        const ui32 targetPDiskId = result.target_slot_id().pdisk_id();
        auto* target = targetedRequest.mutable_target_pdisk_id();
        target->set_node_id(targetNodeId);
        target->set_pdisk_id(targetPDiskId);

        const auto targetedResult = ExtractMutationResult<Ydb::DistributedStorage::ReassignVDiskResult>(ReassignVDisk(server.GetChannel(), targetedRequest));
        UNIT_ASSERT_VALUES_EQUAL(targetedResult.target_slot_id().node_id(), targetNodeId);
        UNIT_ASSERT_VALUES_EQUAL(targetedResult.target_slot_id().pdisk_id(), targetPDiskId);

        const auto afterDryRun = StreamStorageState(server.GetChannel(), {.VDisks = true});
        const auto* afterDryRunVDisk = FindVDisk(afterDryRun, selected->id());
        UNIT_ASSERT(afterDryRunVDisk);
        UNIT_ASSERT_C(SameVSlotId(afterDryRunVDisk->slot_id(), selected->slot_id()), afterDryRunVDisk->DebugString());

        targetedRequest.set_dry_run(false);
        const auto committedResult = ExtractMutationResult<Ydb::DistributedStorage::ReassignVDiskResult>(ReassignVDisk(server.GetChannel(), targetedRequest));
        UNIT_ASSERT_C(SameVDiskId(committedResult.vdisk_id(), selected->id()), committedResult.DebugString());
        UNIT_ASSERT_C(SameVSlotId(committedResult.source_slot_id(), selected->slot_id()), committedResult.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(committedResult.target_slot_id().node_id(), targetNodeId);
        UNIT_ASSERT_VALUES_EQUAL(committedResult.target_slot_id().pdisk_id(), targetPDiskId);

        bool moved = false;
        Ydb::DistributedStorage::VDisk lastVDisk;
        const TInstant moveDeadline = TInstant::Now() + TDuration::Seconds(10);
        do {
            const auto storage = StreamStorageState(server.GetChannel(), {.VDisks = true});
            for (const auto& vdisk : storage.VDisks) {
                if (SameVDiskPosition(vdisk.id(), selected->id())) {
                    lastVDisk.CopyFrom(vdisk);
                    moved = vdisk.id().group_generation() > selected->id().group_generation() && SameVSlotId(vdisk.slot_id(), committedResult.target_slot_id());
                    if (moved) {
                        break;
                    }
                }
            }
            if (!moved) {
                Sleep(TDuration::MilliSeconds(100));
            }
        } while (!moved && TInstant::Now() < moveDeadline);
        UNIT_ASSERT_C(moved, "result# " << committedResult.ShortDebugString() << " lastVDisk# " << lastVDisk.ShortDebugString());
    }

    Y_UNIT_TEST(RejectInvalidMutationRequests) {
        TDistributedStorageTestServer server;

        Ydb::DistributedStorage::ReassignVDiskRequest reassignRequest;
        const auto reassignResponse = ReassignVDisk(server.GetChannel(), reassignRequest);
        UNIT_ASSERT_VALUES_EQUAL_C(reassignResponse.operation().status(), Ydb::StatusIds::BAD_REQUEST, reassignResponse.operation().DebugString());

        reassignRequest.mutable_vdisk_id()->set_group_id(1);
        reassignRequest.mutable_vdisk_id()->set_group_generation(1);
        reassignRequest.mutable_target_pdisk_id()->set_node_id(1);
        const auto invalidTargetResponse = ReassignVDisk(server.GetChannel(), reassignRequest);
        UNIT_ASSERT_VALUES_EQUAL_C(invalidTargetResponse.operation().status(), Ydb::StatusIds::BAD_REQUEST, invalidTargetResponse.operation().DebugString());

        using TRequest = Ydb::DistributedStorage::ChangeDataCenterStateRequest;
        UNIT_ASSERT_VALUES_EQUAL(ChangeDataCenterState(server.GetChannel(), TRequest::ACTION_UNSPECIFIED, "dc1").operation().status(), Ydb::StatusIds::BAD_REQUEST);
        UNIT_ASSERT_VALUES_EQUAL(ChangeDataCenterState(server.GetChannel(), TRequest::TAKEDOWN, "").operation().status(), Ydb::StatusIds::BAD_REQUEST);
        UNIT_ASSERT_VALUES_EQUAL(ChangeDataCenterState(server.GetChannel(), TRequest::REJOIN, "dc1", true).operation().status(), Ydb::StatusIds::BAD_REQUEST);
    }

    Y_UNIT_TEST(ChangeDataCenterStateChecksDistconfAcknowledgement) {
        using TRequest = Ydb::DistributedStorage::ChangeDataCenterStateRequest;
        using TRpc = TGrpcRequestOperationCall<TRequest, Ydb::DistributedStorage::ChangeDataCenterStateResponse>;
        using TResult = NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult;
        TTestBasicRuntime runtime;
        runtime.Initialize(TAppPrepare().Unwrap());
        TActorId nodeWarden = runtime.AllocateEdgeActor();
        runtime.RegisterService(MakeBlobStorageNodeWardenID(runtime.GetNodeId(0)), nodeWarden);
        TResult reply;
        auto run = [&](TRequest::Action action, bool dryRun = false) {
            TRequest request;
            request.set_action(action);
            request.set_datacenter_name("dc1");
            request.set_dry_run(dryRun);
            NRpcService::SetRequestSyncOperationMode(request);
            auto promise = NThreading::NewPromise<TRpc::TResponse>();
            using TCallback = NRpcService::TPromiseWrapper<TRpc::TResponse>;
            auto ctx = std::make_unique<NRpcService::TLocalRpcCtx<TRpc, TCallback>>(
                std::move(request), TCallback(promise), "", Nothing(), Nothing(), false);
            runtime.Register(new TStartChangeDataCenterState(std::move(ctx)));

            const TDuration timeout = TDuration::Seconds(5);
            auto query = runtime.GrabEdgeEventIf<TEvNodeWardenQueryStorageConfig>(nodeWarden,
                [](const auto& ev) { return !ev->Get()->Subscribe; }, timeout);
            UNIT_ASSERT_C(query, "RPC did not query NodeWarden for the storage configuration");
            runtime.Send(new IEventHandle(query->Sender, nodeWarden, new TEvNodeWardenStorageConfig(nullptr, true, nullptr)));
            auto invoke = runtime.GrabEdgeEvent<NStorage::TEvNodeConfigInvokeOnRoot>(nodeWarden, timeout);
            UNIT_ASSERT_C(invoke && invoke->Get()->Record.HasChangeDataCenterState(), "RPC did not forward the DC change");
            const auto& command = invoke->Get()->Record.GetChangeDataCenterState();
            UNIT_ASSERT_VALUES_EQUAL(command.GetName(), "dc1");
            UNIT_ASSERT_VALUES_EQUAL(command.GetTakeDown(), action == TRequest::TAKEDOWN);
            UNIT_ASSERT_VALUES_EQUAL(command.GetDryRun(), dryRun);
            auto result = std::make_unique<NStorage::TEvNodeConfigInvokeOnRootResult>();
            result->Record = reply;
            runtime.Send(new IEventHandle(invoke->Sender, nodeWarden, result.release(), 0, invoke->Cookie));
            return runtime.WaitFuture(promise.GetFuture(), timeout);
        };

        reply.SetStatus(TResult::OK);
        UNIT_ASSERT_VALUES_EQUAL(run(TRequest::TAKEDOWN).operation().status(), Ydb::StatusIds::INTERNAL_ERROR);
        ExtractMutationResult<Ydb::DistributedStorage::ChangeDataCenterStateResult>(run(TRequest::TAKEDOWN, true));
        reply.MutableAppliedDataCenterStates(); // unlisted DCs are included
        UNIT_ASSERT_VALUES_EQUAL(run(TRequest::TAKEDOWN).operation().status(), Ydb::StatusIds::INTERNAL_ERROR);
        ExtractMutationResult<Ydb::DistributedStorage::ChangeDataCenterStateResult>(run(TRequest::REJOIN));
        auto* dc = reply.MutableAppliedDataCenterStates()->AddDataCenter();
        dc->SetName("dc1");
        dc->SetState(NKikimrBlobStorage::TDataCenterState::EXCLUDED);
        ExtractMutationResult<Ydb::DistributedStorage::ChangeDataCenterStateResult>(run(TRequest::TAKEDOWN));
        UNIT_ASSERT_VALUES_EQUAL(run(TRequest::REJOIN).operation().status(), Ydb::StatusIds::INTERNAL_ERROR);

        reply.SetStatus(TResult::NO_QUORUM);
        reply.SetErrorReason("Lost quorum");
        auto failure = run(TRequest::TAKEDOWN);
        UNIT_ASSERT_VALUES_EQUAL(failure.operation().status(), Ydb::StatusIds::UNAVAILABLE);
        UNIT_ASSERT_STRING_CONTAINS(failure.operation().issues(0).message(), "Lost quorum");
    }

    Y_UNIT_TEST(RejectUnsupportedStaticDryRun) {
        TDistributedStorageTestServer server;
        const auto storage = StreamStorageState(server.GetChannel(), {.VDisks = true});

        const auto* selected = FindVDisk(storage, true);
        UNIT_ASSERT(selected);
        UNIT_ASSERT_VALUES_EQUAL(selected->id().group_id(), 0);

        Ydb::DistributedStorage::ReassignVDiskRequest request;
        request.mutable_vdisk_id()->CopyFrom(selected->id());
        request.set_dry_run(true);
        const auto response = ReassignVDisk(server.GetChannel(), request);
        UNIT_ASSERT_VALUES_EQUAL_C(response.operation().status(), Ydb::StatusIds::UNSUPPORTED, response.operation().DebugString());
    }

    Y_UNIT_TEST(StaticReassignmentRequiresDistconfV2) {
        TDistributedStorageTestServer server;
        const auto storage = StreamStorageState(server.GetChannel(), {.VDisks = true});

        const auto* selected = FindVDisk(storage, true);
        UNIT_ASSERT(selected);

        Ydb::DistributedStorage::ReassignVDiskRequest request;
        request.mutable_vdisk_id()->CopyFrom(selected->id());
        const auto response = ReassignVDisk(server.GetChannel(), request);
        UNIT_ASSERT_VALUES_EQUAL_C(response.operation().status(), Ydb::StatusIds::UNSUPPORTED, response.operation().DebugString());
    }

    Y_UNIT_TEST(RejectStaleVDiskGeneration) {
        TDistributedStorageTestServer server;
        const auto storage = StreamStorageState(server.GetChannel(), {.VDisks = true});

        const auto* selected = FindVDisk(storage, false);
        UNIT_ASSERT(selected);

        Ydb::DistributedStorage::ReassignVDiskRequest request;
        request.mutable_vdisk_id()->CopyFrom(selected->id());
        request.mutable_vdisk_id()->set_group_generation(selected->id().group_generation() + 1);
        request.set_dry_run(true);

        const auto response = ReassignVDisk(server.GetChannel(), request);
        UNIT_ASSERT_VALUES_EQUAL_C(response.operation().status(), Ydb::StatusIds::PRECONDITION_FAILED, response.operation().DebugString());
    }

    Y_UNIT_TEST(RejectMutationFromNonAdministrator) {
        TDistributedStorageTestServer server;
        server.GetRuntime()->GetAppData(0).AdministrationAllowedSIDs = {"admin@builtin"};

        Ydb::DistributedStorage::ReassignVDiskRequest reassignRequest;
        reassignRequest.mutable_vdisk_id()->set_group_id(1);
        reassignRequest.mutable_vdisk_id()->set_group_generation(1);
        const auto reassignResponse = ReassignVDisk(server.GetChannel(), reassignRequest);
        UNIT_ASSERT_VALUES_EQUAL_C(reassignResponse.operation().status(), Ydb::StatusIds::UNAUTHORIZED, reassignResponse.operation().DebugString());
        using TRequest = Ydb::DistributedStorage::ChangeDataCenterStateRequest;
        UNIT_ASSERT_VALUES_EQUAL(ChangeDataCenterState(server.GetChannel(), TRequest::TAKEDOWN, "dc1").operation().status(), Ydb::StatusIds::UNAUTHORIZED);
    }
}

} // namespace NKikimr::NGRpcService

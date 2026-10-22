#include "env.h"

#include <ydb/core/mind/bscontroller/config.h>
#include <ydb/core/protos/blobstorage_pdisk_kind.pb.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/library/yaml_config/yaml_config_compress.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NBsController {
namespace {

using namespace NKikimrBlobStorage;

class TPDiskKindsEnvironment : public TEnvironmentSetup {
public:
    TStorageConfig Config;

    TPDiskKindsEnvironment()
        : TEnvironmentSetup(1, 1)
    {
        bool activeZone = false;
        Prepare("pdisk kinds", [](TTestActorRuntime&) {}, activeZone);
        DisableLogging();
        const auto ready = Invoke(TConfigRequest{});
        UNIT_ASSERT_C(ready.GetSuccess(), ready.DebugString());

        auto* bs = Config.MutableBlobStorageConfig();
        bs->MutableServiceSet();
        auto* host = bs->AddDefineHostConfig();
        host->SetHostConfigId(1);
        auto* drive = host->AddDrive();
        drive->SetPath("/dev/nvme0n1");
        drive->SetType(NVME);
        bs->MutableDefineBox()->SetBoxId(1);
        for (const auto& [fqdn, port, nodeId] : GetNodes()) {
            auto* node = Config.AddAllNodes();
            node->SetHost(fqdn);
            node->SetPort(port);
            node->SetNodeId(nodeId);
            auto* boxHost = bs->MutableDefineBox()->AddHost();
            boxHost->SetHostConfigId(1);
            boxHost->SetEnforcedNodeId(nodeId);
        }
        Config.MutableSelfManagementConfig()->SetEnabled(true);
        Config.MutableSelfManagementConfig()->SetAutomaticBoxManagement(true);

        // Enable the real distconf path before validating the first candidate.
        // The initial notification contains node identities, but no catalog or drives.
        auto initial = Config;
        initial.ClearBlobStorageConfig();
        Publish(initial);
    }

    TDefineHostConfig& Host() {
        return *Config.MutableBlobStorageConfig()->MutableDefineHostConfig(0);
    }

    THostConfigDrive& Drive(int index = 0) {
        return *Host().MutableDrive(index);
    }

    TDefinePDiskKind& AddKind(const TString& name) {
        auto* kind = Config.MutableBlobStorageConfig()->AddPDiskKinds();
        kind->SetName(name);
        return *kind;
    }

    TConfigResponse DefineKind(const TString& name, ui64 generation = 0) {
        TConfigRequest request;
        auto* cmd = request.AddCommand()->MutableDefinePDiskKind();
        cmd->SetName(name);
        cmd->SetItemConfigGeneration(generation);
        return Invoke(request);
    }

    TConfigResponse DeleteKind(const TString& name, ui64 generation) {
        TConfigRequest request;
        auto* cmd = request.AddCommand()->MutableDeletePDiskKind();
        cmd->SetName(name);
        cmd->SetItemConfigGeneration(generation);
        return Invoke(request);
    }

    NKikimrBlobStorage::TPDiskConfig& Profile(int index = 0) {
        return *Config.MutableBlobStorageConfig()->MutablePDiskKinds(index)->MutablePDiskConfig();
    }

    void UseKind(const TString& name = "qlc") {
        AddKind(name);
        Drive().SetKindName(name);
    }

    THostConfigDrive& AddDrive(const TString& path, ui64 kind = 0, const TString& name = {}) {
        auto* drive = Host().AddDrive();
        drive->SetPath(path);
        drive->SetType(NVME);
        drive->SetKind(kind);
        drive->SetKindName(name);
        return *drive;
    }

    TEvControllerDistconfResponse Validate() {
        const auto edge = Runtime->AllocateEdgeActor();
        auto ev = MakeHolder<TEvBlobStorage::TEvControllerDistconfRequest>();
        ev->Record.SetOperation(TEvControllerDistconfRequest::ValidateConfig);
        // Distconf supplies both the YAML envelope and its derived StorageConfig.
        ev->Record.SetCompressedMainConfig(NYamlConfig::CompressYamlString("{}"));
        ev->Record.MutableStorageConfig()->PackFrom(Config);
        Runtime->SendToPipe(TabletId, edge, ev.Release(), NodeId, GetPipeConfigWithRetries());
        return Runtime->GrabEdgeEventRethrow<TEvBlobStorage::TEvControllerDistconfResponse>(edge)->Get()->Record;
    }

    void Apply() {
        const auto response = Validate();
        UNIT_ASSERT_C(response.GetStatus() == TEvControllerDistconfResponse::OK, response.DebugString());
        Publish(Config);
    }

    TConfigResponse Read() {
        TConfigRequest request;
        request.AddCommand()->MutableReadHostConfig();
        request.AddCommand()->MutableReadStoragePool()->SetBoxId(1);
        request.AddCommand()->MutableQueryBaseConfig();
        auto response = Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(response.StatusSize(), 3);
        return response;
    }

    TBaseConfig Base() {
        return Read().GetStatus(2).GetBaseConfig();
    }

    TDefineHostConfig SourceHost() {
        TConfigRequest request;
        auto* cmd = request.AddCommand()->MutableReadHostConfig();
        cmd->AddHostConfigId(1);
        cmd->SetReturnSource(true);
        const auto response = Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.DebugString());
        return response.GetStatus(0).GetHostConfig(0);
    }

    TDefineStoragePool SourcePool() {
        TConfigRequest request;
        auto* cmd = request.AddCommand()->MutableReadStoragePool();
        cmd->SetBoxId(1);
        cmd->AddStoragePoolId(1);
        cmd->SetReturnSource(true);
        const auto response = Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.DebugString());
        return response.GetStatus(0).GetStoragePool(0);
    }

    void RegisterSerialDrive(const TString& serial, const TString& path) {
        const ui32 nodeId = std::get<2>(GetNodes().front());
        const ui32 nodeIndex = nodeId - Runtime->GetNodeId(0);
        const auto edge = Runtime->AllocateEdgeActor(nodeIndex);
        NPDisk::TDriveData drive;
        drive.Path = path;
        drive.SerialNumber = serial;
        drive.DeviceType = NPDisk::DEVICE_TYPE_NVME;
        Runtime->SendToPipe(TabletId, edge, new TEvBlobStorage::TEvControllerRegisterNode(
            nodeId, TVector<ui32>{}, TVector<ui32>{}, TVector<NPDisk::TDriveData>{drive}),
            nodeIndex, GetPipeConfigWithRetries());
        Runtime->GrabEdgeEventRethrow<TEvBlobStorage::TEvControllerNodeServiceSetUpdate>(edge);
        Read();
    }

    TBaseConfig::TPDisk Disk(const TString& path = "/dev/nvme0n1") {
        const auto base = Base();
        for (const auto& disk : base.GetPDisk()) {
            if (disk.GetPath() == path) {
                return disk;
            }
        }
        UNIT_FAIL("PDisk not found: " << path);
        return {};
    }

    TString Snapshot() {
        const auto response = Read();
        // Exclude transient node connection information; retain generations,
        // effective configs, pool filters, and placement.
        TBaseConfig base = response.GetStatus(2).GetBaseConfig();
        base.ClearNode();
        return response.GetStatus(0).SerializeAsString()
            + response.GetStatus(1).SerializeAsString() + base.SerializeAsString();
    }

    void AssertRejected() {
        const TString before = Snapshot();
        const auto response = Validate();
        UNIT_ASSERT_C(response.GetStatus() == TEvControllerDistconfResponse::Error, response.DebugString());
        UNIT_ASSERT_C(!response.GetErrorReason().empty(), response.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(Snapshot(), before);
    }

    void Reboot() {
        RebootTablet(*Runtime, TabletId, Runtime->AllocateEdgeActor());
        // The shared NodeWarden mock sends an empty configuration after restart.
        // Thus reads below verify tablet persistence, not reapplication of Config.
    }

    void AssertKindResolves(const TString& name, ui64 kind) {
        TConfigRequest request;
        auto* host = request.AddCommand()->MutableDefineHostConfig();
        host->SetHostConfigId(2);
        auto* drive = host->AddDrive();
        drive->SetPath("/dev/nvme2n1");
        drive->SetType(NVME);
        drive->SetKindName(name);
        request.AddCommand()->MutableReadHostConfig()->AddHostConfigId(2);
        const auto response = Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.DebugString());
        const auto& stored = response.GetStatus(1).GetHostConfig(0).GetDrive(0);
        UNIT_ASSERT_VALUES_EQUAL(stored.GetKind(), kind);
        UNIT_ASSERT_VALUES_EQUAL(stored.GetKindName(), name);
    }

    TConfigRequest PoolRequest(ui64 poolId, TMaybe<ui64> kind = {}, const TString& kindName = {},
            ui32 numGroups = 1) {
        TConfigRequest request;
        request.SetIgnoreGroupReserve(true);
        DefineStoragePool(1, poolId, TStringBuilder() << "pool-" << poolId, numGroups,
            NVME, {}, request, "none");
        auto* pool = request.MutableCommand(0)->MutableDefineStoragePool();
        pool->SetKind("capacity"); // Pool Kind is independent of the PDisk class.
        auto* filter = pool->MutablePDiskFilter(0);
        if (kind) {
            filter->AddProperty()->SetKind(*kind);
        }
        if (kindName) {
            filter->AddProperty()->SetKindName(kindName);
        }
        return request;
    }

    void CreatePool(ui64 poolId, TMaybe<ui64> kind = {}, const TString& kindName = {}, ui32 numGroups = 1) {
        const auto response = Invoke(PoolRequest(poolId, kind, kindName, numGroups));
        UNIT_ASSERT_C(response.GetSuccess(), response.DebugString());
    }

    void AssertPoolKind(ui64 poolId, ui64 kind, EPDiskType type = NVME) {
        const auto base = Base();
        ui32 matchedSlots = 0;
        for (const auto& group : base.GetGroup()) {
            if (group.GetStoragePoolId() != poolId) {
                continue;
            }
            for (const auto& slot : group.GetVSlotId()) {
                bool found = false;
                for (const auto& disk : base.GetPDisk()) {
                    if (disk.GetNodeId() == slot.GetNodeId() && disk.GetPDiskId() == slot.GetPDiskId()) {
                        UNIT_ASSERT_VALUES_EQUAL(disk.GetType(), type);
                        UNIT_ASSERT_VALUES_EQUAL(disk.GetKind(), kind);
                        found = true;
                        ++matchedSlots;
                        break;
                    }
                }
                UNIT_ASSERT(found);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(matchedSlots, 1);
    }

private:
    void Publish(const TStorageConfig& config) {
        const auto published = std::make_shared<TStorageConfig>(config);
        bool delivered = false;
        auto observer = Runtime->AddObserver<TEvNodeWardenStorageConfig>(
            [&](TEvNodeWardenStorageConfig::TPtr& ev) { delivered |= ev->Get()->Config == published; });
        const auto edge = Runtime->AllocateEdgeActor();
        Runtime->SendToPipe(TabletId, edge,
            new TEvNodeWardenStorageConfig(published, true, nullptr),
            NodeId, GetPipeConfigWithRetries());
        Runtime->WaitFor("storage configuration notification", [&] { return delivered; }, Timeout);
        observer.Remove();
        // The notification enqueues the controller's configuration transaction.
        // This subsequent request also waits for that transaction to complete.
        Read();
    }
};

} // namespace

Y_UNIT_TEST_SUITE(BsControllerPDiskKinds) {
    Y_UNIT_TEST(LegacyNumericKindsSurviveRestart) {
        TPDiskKindsEnvironment env;
        env.AddDrive("/dev/nvme1n1", 1);
        env.AddDrive("/dev/nvme2n1", 17);
        env.Apply();

        UNIT_ASSERT_VALUES_EQUAL(env.Disk().GetKind(), 0);
        UNIT_ASSERT_VALUES_EQUAL(env.Disk("/dev/nvme1n1").GetKind(), 1);
        UNIT_ASSERT_VALUES_EQUAL(env.Disk("/dev/nvme2n1").GetKind(), 17);
        env.Reboot();
        UNIT_ASSERT_VALUES_EQUAL(env.Disk().GetKind(), 0);
        UNIT_ASSERT_VALUES_EQUAL(env.Disk("/dev/nvme1n1").GetKind(), 1);
        UNIT_ASSERT_VALUES_EQUAL(env.Disk("/dev/nvme2n1").GetKind(), 17);
    }

    Y_UNIT_TEST(NumericFiltersSeparateNVMePoolsIncludingKindZero) {
        TPDiskKindsEnvironment env;
        env.AddDrive("/dev/nvme1n1", 1);
        env.AddDrive("/dev/nvme2n1", 2);
        env.Apply();
        env.CreatePool(1, 1);
        env.CreatePool(2, 2);
        env.CreatePool(3, 0);
        env.AssertPoolKind(1, 1);
        env.AssertPoolKind(2, 2);
        env.AssertPoolKind(3, 0);
    }

    Y_UNIT_TEST(TypeOnlyFilterAcceptsNonzeroNumericKind) {
        TPDiskKindsEnvironment env;
        env.Drive().SetKind(17);
        env.Apply();
        env.CreatePool(1);
        env.AssertPoolKind(1, 17);
    }

    Y_UNIT_TEST(PropertiesAreConjoinedAndFiltersAreDisjoined) {
        TPDiskKindsEnvironment env;
        env.Drive().SetKind(1);
        env.AddDrive("/dev/ssd1", 2).SetType(SSD);
        env.Apply();
        const TString accepted = env.Snapshot();

        // NVME exists and Kind 2 exists, but not on the same disk.
        auto request = env.PoolRequest(1, 2);
        const auto rejected = env.Invoke(request);
        UNIT_ASSERT_C(!rejected.GetSuccess(), rejected.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(env.Snapshot(), accepted);

        // A second filter provides an alternative that does match.
        auto* filter = request.MutableCommand(0)->MutableDefineStoragePool()->AddPDiskFilter();
        filter->AddProperty()->SetType(SSD);
        filter->AddProperty()->SetKind(2);
        const auto acceptedResponse = env.Invoke(request);
        UNIT_ASSERT_C(acceptedResponse.GetSuccess(), acceptedResponse.DebugString());
        env.AssertPoolKind(1, 2, SSD);
    }

    Y_UNIT_TEST(UnnamedDriveConfigReplacesHostDefaultAsAWhole) {
        TPDiskKindsEnvironment env;
        auto* defaults = env.Host().MutableDefaultHostPDiskConfig();
        defaults->SetLogWeight(8);
        defaults->SetIoPieceSizeBytes(65536);
        env.AddDrive("/dev/nvme1n1").MutablePDiskConfig();
        env.AddDrive("/dev/nvme2n1").MutablePDiskConfig()->SetLogWeight(4);
        env.Apply();

        const auto inherited = env.Disk().GetPDiskConfig();
        UNIT_ASSERT_VALUES_EQUAL(inherited.GetLogWeight(), 8);
        UNIT_ASSERT_VALUES_EQUAL(inherited.GetIoPieceSizeBytes(), 65536);
        const auto empty = env.Disk("/dev/nvme1n1").GetPDiskConfig();
        UNIT_ASSERT(!empty.HasLogWeight());
        UNIT_ASSERT(!empty.HasIoPieceSizeBytes());
        const auto overridden = env.Disk("/dev/nvme2n1").GetPDiskConfig();
        UNIT_ASSERT_VALUES_EQUAL(overridden.GetLogWeight(), 4);
        UNIT_ASSERT(!overridden.HasIoPieceSizeBytes());
    }

    Y_UNIT_TEST(EmptyPoolFilterKindNameIsRejectedOnCreate) {
        TPDiskKindsEnvironment env;
        env.Apply();
        const TString before = env.Snapshot();

        auto request = env.PoolRequest(1);
        auto* filter = request.MutableCommand(0)->MutableDefineStoragePool()->MutablePDiskFilter(0);
        // PoolRequest omits empty names, so explicitly select the KindName oneof here.
        filter->AddProperty()->SetKindName("");
        const auto rejected = env.Invoke(request);
        UNIT_ASSERT_C(!rejected.GetSuccess(), rejected.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(rejected.StatusSize(), 1);
        UNIT_ASSERT_C(rejected.GetStatus(0).GetErrorDescription().Contains("PDisk kind name is empty"),
            rejected.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(env.Snapshot(), before);

        // The same request without a KindName property remains valid.
        filter->MutableProperty()->RemoveLast();
        const auto accepted = env.Invoke(request);
        UNIT_ASSERT_C(accepted.GetSuccess(), accepted.DebugString());
        env.AssertPoolKind(1, 0);
    }

    Y_UNIT_TEST(EmptyPoolFilterKindNameDoesNotReplaceAcceptedPool) {
        TPDiskKindsEnvironment env;
        env.Drive().SetKind(17);
        env.Apply();
        env.CreatePool(1, 17);
        const TString before = env.Snapshot();

        for (bool includeNumericKind : {false, true}) {
            auto request = env.PoolRequest(1);
            auto* pool = request.MutableCommand(0)->MutableDefineStoragePool();
            pool->SetItemConfigGeneration(1);
            auto* filter = pool->MutablePDiskFilter(0);
            if (includeNumericKind) {
                filter->AddProperty()->SetKind(17);
            }
            filter->AddProperty()->SetKindName("");

            const auto response = env.Invoke(request);
            UNIT_ASSERT_C(!response.GetSuccess(), response.DebugString());
            UNIT_ASSERT_VALUES_EQUAL(response.StatusSize(), 1);
            UNIT_ASSERT_C(response.GetStatus(0).GetErrorDescription().Contains("PDisk kind name is empty"),
                response.DebugString());
            UNIT_ASSERT_VALUES_EQUAL(env.Snapshot(), before);
        }
        env.AssertPoolKind(1, 17);
    }

    Y_UNIT_TEST(UnknownNamedPoolFilterDoesNotReplaceAcceptedPool) {
        TPDiskKindsEnvironment env;
        env.Apply();
        env.CreatePool(1, 0);
        const TString before = env.Snapshot();
        auto request = env.PoolRequest(1, {}, "missing");
        request.MutableCommand(0)->MutableDefineStoragePool()->SetItemConfigGeneration(1);
        const auto response = env.Invoke(request);
        UNIT_ASSERT_C(!response.GetSuccess(), response.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(env.Snapshot(), before);
        env.AssertPoolKind(1, 0);
    }
}

Y_UNIT_TEST_SUITE(BsControllerPDiskKindCommands) {
    Y_UNIT_TEST(UnchangedNamedDriveKeepsHostConfigGeneration) {
        TPDiskKindsEnvironment env;
        env.UseKind();
        env.Apply();
        const auto host = env.Read().GetStatus(0).GetHostConfig(0);
        const ui64 generation = host.GetItemConfigGeneration();
        const ui64 kind = host.GetDrive(0).GetKind();
        UNIT_ASSERT(kind != 0);

        env.Apply();
        UNIT_ASSERT_VALUES_EQUAL(env.Read().GetStatus(0).GetHostConfig(0).GetItemConfigGeneration(), generation);

        env.Drive().SetKind(kind);
        env.Apply();
        UNIT_ASSERT_VALUES_EQUAL(env.Read().GetStatus(0).GetHostConfig(0).GetItemConfigGeneration(), generation);

        env.Drive().SetKind(kind + 1);
        env.AssertRejected();
    }

    Y_UNIT_TEST(NameChangeWithUnchangedNumericKindIsValidated) {
        TPDiskKindsEnvironment env;
        env.UseKind();
        env.Apply();
        env.Drive().SetKind(env.Disk().GetKind());

        env.Drive().SetKindName("missing");
        env.AssertRejected();

        env.AddKind("tlc");
        env.Drive().SetKindName("tlc");
        env.AssertRejected();
    }

    Y_UNIT_TEST(ClearingNameKeepsExplicitNumericKind) {
        TPDiskKindsEnvironment env;
        env.UseKind();
        env.Apply();
        const auto host = env.Read().GetStatus(0).GetHostConfig(0);
        const ui64 kind = host.GetDrive(0).GetKind();

        env.Drive().SetKind(kind);
        env.Drive().ClearKindName();
        env.Apply();
        const auto updated = env.Read().GetStatus(0).GetHostConfig(0);
        UNIT_ASSERT_VALUES_EQUAL(updated.GetItemConfigGeneration(), host.GetItemConfigGeneration() + 1);
        UNIT_ASSERT_VALUES_EQUAL(updated.GetDrive(0).GetKind(), kind);
        UNIT_ASSERT_VALUES_EQUAL(updated.GetDrive(0).GetKindName(), "");
    }

    Y_UNIT_TEST(ReadEditWritePreservesKindNameAfterRestart) {
        TPDiskKindsEnvironment env;
        env.UseKind();
        env.Apply();
        env.Reboot();
        const auto host = env.Read().GetStatus(0).GetHostConfig(0);
        UNIT_ASSERT_VALUES_EQUAL(host.GetDrive(0).GetKindName(), "qlc");

        TConfigRequest request;
        auto* updated = request.AddCommand()->MutableDefineHostConfig();
        updated->CopyFrom(host);
        updated->SetName("edited host config");
        const auto response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.DebugString());

        const auto stored = env.Read().GetStatus(0).GetHostConfig(0);
        UNIT_ASSERT_VALUES_EQUAL(stored.GetName(), "edited host config");
        UNIT_ASSERT_VALUES_EQUAL(stored.GetDrive(0).GetKind(), host.GetDrive(0).GetKind());
        UNIT_ASSERT_VALUES_EQUAL(stored.GetDrive(0).GetKindName(), "qlc");
    }

    Y_UNIT_TEST(DeleteChecksGenerationAndSurvivesRestart) {
        TPDiskKindsEnvironment env;
        auto response = env.DefineKind("qlc");
        UNIT_ASSERT_C(response.GetSuccess(), response.DebugString());

        response = env.DeleteKind("missing", 0);
        UNIT_ASSERT_C(!response.GetSuccess(), response.DebugString());
        UNIT_ASSERT_C(response.GetStatus(0).GetErrorDescription().Contains("not found"), response.DebugString());

        response = env.DeleteKind("qlc", 0);
        UNIT_ASSERT_C(!response.GetSuccess(), response.DebugString());
        UNIT_ASSERT_C(response.GetStatus(0).GetFailReason() == TConfigResponse::TStatus::kItemConfigGenerationMismatch,
            response.DebugString());

        response = env.DefineKind("qlc", 1);
        UNIT_ASSERT_C(response.GetSuccess(), response.DebugString());
        response = env.DeleteKind("qlc", 1);
        UNIT_ASSERT_C(!response.GetSuccess(), response.DebugString());

        response = env.DeleteKind("qlc", Max<ui64>());
        UNIT_ASSERT_C(response.GetSuccess(), response.DebugString());
        env.Reboot();
        response = env.DefineKind("qlc");
        UNIT_ASSERT_C(response.GetSuccess(), response.DebugString());
    }

    Y_UNIT_TEST(FailedBatchAndDryRunKeepTheKind) {
        TPDiskKindsEnvironment env;
        auto response = env.DefineKind("qlc");
        UNIT_ASSERT_C(response.GetSuccess(), response.DebugString());

        TConfigRequest request;
        auto* del = request.AddCommand()->MutableDeletePDiskKind();
        del->SetName("qlc");
        del->SetItemConfigGeneration(1);
        request.AddCommand()->MutableDeletePDiskKind()->SetName("missing");
        response = env.Invoke(request);
        UNIT_ASSERT_C(!response.GetSuccess(), response.DebugString());

        // Updating with generation 1 proves the failed batch retained the catalog entry.
        response = env.DefineKind("qlc", 1);
        UNIT_ASSERT_C(response.GetSuccess(), response.DebugString());

        request.Clear();
        request.SetRollback(true);
        del = request.AddCommand()->MutableDeletePDiskKind();
        del->SetName("qlc");
        del->SetItemConfigGeneration(2);
        response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetRollbackSuccess(), response.DebugString());

        env.Reboot();
        response = env.DeleteKind("qlc", 2);
        UNIT_ASSERT_C(response.GetSuccess(), response.DebugString());
    }

    Y_UNIT_TEST(DeleteRejectsPersistedHostReferenceUntilHostConfigIsRemoved) {
        TPDiskKindsEnvironment env;
        auto response = env.DefineKind("qlc");
        UNIT_ASSERT_C(response.GetSuccess(), response.DebugString());

        TConfigRequest request;
        auto* host = request.AddCommand()->MutableDefineHostConfig();
        host->SetHostConfigId(2);
        auto* drive = host->AddDrive();
        drive->SetPath("/dev/nvme2n1");
        drive->SetType(NVME);
        drive->SetKind(1);
        request.AddCommand()->MutableReadHostConfig()->AddHostConfigId(2);
        response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(response.GetStatus(1).GetHostConfig(0).GetDrive(0).GetKind(), 1);

        env.Reboot();
        response = env.DeleteKind("qlc", 1);
        UNIT_ASSERT_C(!response.GetSuccess(), response.DebugString());
        UNIT_ASSERT_C(response.GetStatus(0).GetErrorDescription().Contains("used by drive"), response.DebugString());

        request.Clear();
        auto* delHost = request.AddCommand()->MutableDeleteHostConfig();
        delHost->SetHostConfigId(2);
        delHost->SetItemConfigGeneration(1);
        auto* delKind = request.AddCommand()->MutableDeletePDiskKind();
        delKind->SetName("qlc");
        delKind->SetItemConfigGeneration(1);
        response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.DebugString());
    }

    Y_UNIT_TEST(DeleteRejectsNumericPoolFilterUntilPoolIsRemoved) {
        TPDiskKindsEnvironment env;
        env.Apply();
        auto response = env.DefineKind("qlc");
        UNIT_ASSERT_C(response.GetSuccess(), response.DebugString());
        env.CreatePool(1, 1, {}, 0);

        response = env.DeleteKind("qlc", 1);
        UNIT_ASSERT_C(!response.GetSuccess(), response.DebugString());
        UNIT_ASSERT_C(response.GetStatus(0).GetErrorDescription().Contains("used by pool filter"), response.DebugString());

        TConfigRequest request;
        auto* delPool = request.AddCommand()->MutableDeleteStoragePool();
        delPool->SetBoxId(1);
        delPool->SetStoragePoolId(1);
        delPool->SetItemConfigGeneration(1);
        auto* delKind = request.AddCommand()->MutableDeletePDiskKind();
        delKind->SetName("qlc");
        delKind->SetItemConfigGeneration(1);
        response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.DebugString());
    }

    Y_UNIT_TEST(CatalogRemovalRunsAfterClearingTheLastHostReference) {
        TPDiskKindsEnvironment env;
        env.AddKind("qlc");
        env.Apply();
        env.Drive().SetKind(1);
        env.Apply();
        env.Config.MutableBlobStorageConfig()->ClearPDiskKinds();
        env.AssertRejected();

        env.Drive().SetKind(0);
        env.Apply();
        env.Reboot();
        const auto response = env.DefineKind("qlc");
        UNIT_ASSERT_C(response.GetSuccess(), response.DebugString());
    }
}

// RFC acceptance tests. Named-kind catalog processing is still being implemented;
// these assertions deliberately describe the required behavior, not today's gaps.
Y_UNIT_TEST_SUITE(BsControllerNamedPDiskKinds) {
    Y_UNIT_TEST(IdsStartAtOneAndZeroWithNameIsUnspecified) {
        TPDiskKindsEnvironment env;
        env.UseKind();
        env.Drive().SetKind(0);
        env.Apply();
        const auto disk = env.Disk();
        UNIT_ASSERT_VALUES_EQUAL(disk.GetKind(), 1);
        UNIT_ASSERT_VALUES_EQUAL(disk.GetKindName(), "qlc");
        UNIT_ASSERT_VALUES_EQUAL(disk.GetType(), NVME);
        env.Reboot();
        UNIT_ASSERT_VALUES_EQUAL(env.Disk().GetKind(), 1);
        UNIT_ASSERT_VALUES_EQUAL(env.Disk().GetKindName(), "qlc");
    }

    Y_UNIT_TEST(CatalogResolvesNewReferencesAfterRestartWithoutBeingResubmitted) {
        TPDiskKindsEnvironment env;
        env.UseKind();
        env.Apply();
        UNIT_ASSERT_VALUES_EQUAL(env.Disk().GetKind(), 1);
        env.Reboot();
        env.AssertKindResolves("qlc", 1);
    }

    Y_UNIT_TEST(LegacyNumericIdsAreNotAssignedToNamedKinds) {
        TPDiskKindsEnvironment env;
        env.UseKind();
        env.AddDrive("/dev/nvme1n1", 1);
        env.Apply();
        UNIT_ASSERT_VALUES_EQUAL(env.Disk().GetKind(), 2);
        UNIT_ASSERT_VALUES_EQUAL(env.Disk("/dev/nvme1n1").GetKind(), 1);
    }

    Y_UNIT_TEST(CatalogOrderAndProfileChangesPreserveIds) {
        TPDiskKindsEnvironment env;
        env.UseKind();
        env.AddKind("tlc");
        env.AddDrive("/dev/nvme1n1", 0, "tlc");
        env.Apply();
        const ui64 qlc = env.Disk().GetKind();
        const ui64 tlc = env.Disk("/dev/nvme1n1").GetKind();
        UNIT_ASSERT(qlc != 0 && tlc != 0 && qlc != tlc);

        auto* kinds = env.Config.MutableBlobStorageConfig()->MutablePDiskKinds();
        kinds->SwapElements(0, 1);
        env.Profile(1).SetIoPieceSizeBytes(65536);
        env.Apply();
        UNIT_ASSERT_VALUES_EQUAL(env.Disk().GetKind(), qlc);
        UNIT_ASSERT_VALUES_EQUAL(env.Disk("/dev/nvme1n1").GetKind(), tlc);
        env.Reboot();
        UNIT_ASSERT_VALUES_EQUAL(env.Disk().GetKind(), qlc);
        UNIT_ASSERT_VALUES_EQUAL(env.Disk("/dev/nvme1n1").GetKind(), tlc);
    }

    Y_UNIT_TEST(NamesAreCaseSensitive) {
        TPDiskKindsEnvironment env;
        env.UseKind("qlc");
        env.AddKind("QLC");
        env.AddDrive("/dev/nvme1n1", 0, "QLC");
        env.Apply();
        const auto lower = env.Disk();
        const auto upper = env.Disk("/dev/nvme1n1");
        UNIT_ASSERT(lower.GetKind() != 0 && upper.GetKind() != 0);
        UNIT_ASSERT(lower.GetKind() != upper.GetKind());
        UNIT_ASSERT_VALUES_EQUAL(lower.GetKindName(), "qlc");
        UNIT_ASSERT_VALUES_EQUAL(upper.GetKindName(), "QLC");
    }

    Y_UNIT_TEST(EmptyCatalogNameIsRejected) {
        TPDiskKindsEnvironment env;
        env.Apply();
        env.AddKind("");
        env.AssertRejected();
    }

    Y_UNIT_TEST(DuplicateCatalogNameIsRejected) {
        TPDiskKindsEnvironment env;
        env.Apply();
        env.AddKind("qlc");
        env.AddKind("qlc");
        env.AssertRejected();
    }

    Y_UNIT_TEST(UnknownDriveKindDoesNotReplaceAcceptedConfiguration) {
        TPDiskKindsEnvironment env;
        env.Drive().SetKind(17);
        env.Drive().MutablePDiskConfig()->SetIoPieceSizeBytes(65536);
        env.Apply();
        const TString accepted = env.Snapshot();

        env.Drive().SetKind(0);
        env.Drive().SetKindName("missing");
        env.Drive().MutablePDiskConfig()->SetIoPieceSizeBytes(32768);
        env.AssertRejected();
        env.Reboot();
        UNIT_ASSERT_VALUES_EQUAL(env.Snapshot(), accepted);
    }

    Y_UNIT_TEST(HostConfigCommandRejectsUnknownKindName) {
        TPDiskKindsEnvironment env;
        env.Apply();
        const TString accepted = env.Snapshot();
        TConfigRequest request;
        auto* host = request.AddCommand()->MutableDefineHostConfig();
        host->CopyFrom(env.Read().GetStatus(0).GetHostConfig(0));
        host->MutableDrive(0)->SetKindName("missing");
        const auto response = env.Invoke(request);
        UNIT_ASSERT_C(!response.GetSuccess(), response.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(env.Snapshot(), accepted);
    }

    Y_UNIT_TEST(KindReferencedByADriveCannotBeDeleted) {
        TPDiskKindsEnvironment env;
        env.UseKind();
        env.Apply();
        env.Config.MutableBlobStorageConfig()->ClearPDiskKinds();
        env.AssertRejected();
        env.Reboot();
        env.AssertKindResolves("qlc", 1);
    }

    Y_UNIT_TEST(KindAndItsLastDriveReferenceCanBeRemovedTogether) {
        TPDiskKindsEnvironment env;
        env.UseKind();
        env.Apply();
        UNIT_ASSERT_VALUES_EQUAL(env.Disk().GetKind(), 1);

        env.Config.MutableBlobStorageConfig()->ClearPDiskKinds();
        env.Drive().ClearKindName();
        env.Drive().SetKind(0);
        env.Apply();
        UNIT_ASSERT_VALUES_EQUAL(env.Disk().GetKind(), 0);
        UNIT_ASSERT(env.Disk().GetKindName().empty());

        // A later reference must not resurrect the deleted catalog entry.
        env.Drive().SetKindName("qlc");
        env.AssertRejected();
    }

    Y_UNIT_TEST(RenameInCatalogAndEveryDrivePreservesId) {
        TPDiskKindsEnvironment env;
        env.UseKind();
        env.AddDrive("/dev/nvme1n1", 0, "qlc");
        env.Apply();
        const ui64 kind = env.Disk().GetKind();
        UNIT_ASSERT_VALUES_EQUAL(kind, 1);

        env.Config.MutableBlobStorageConfig()->MutablePDiskKinds(0)->SetName("capacity");
        env.Config.MutableBlobStorageConfig()->MutablePDiskKinds(0)->SetPreviousName("qlc");
        env.Drive(0).SetKindName("capacity");
        env.Drive(1).SetKindName("capacity");
        env.Apply();
        for (const auto& path : {"/dev/nvme0n1", "/dev/nvme1n1"}) {
            UNIT_ASSERT_VALUES_EQUAL(env.Disk(path).GetKind(), kind);
            UNIT_ASSERT_VALUES_EQUAL(env.Disk(path).GetKindName(), "capacity");
        }
        env.Reboot();
        UNIT_ASSERT_VALUES_EQUAL(env.Disk().GetKind(), kind);
        UNIT_ASSERT_VALUES_EQUAL(env.Disk("/dev/nvme1n1").GetKindName(), "capacity");
    }

    Y_UNIT_TEST(RenameLeavingAnOldDriveReferenceIsRejected) {
        TPDiskKindsEnvironment env;
        env.UseKind();
        env.AddDrive("/dev/nvme1n1", 0, "qlc");
        env.Apply();

        env.Config.MutableBlobStorageConfig()->MutablePDiskKinds(0)->SetName("capacity");
        env.Config.MutableBlobStorageConfig()->MutablePDiskKinds(0)->SetPreviousName("qlc");
        env.Drive(0).SetKindName("capacity");
        // The second drive still references qlc.
        env.AssertRejected();
        env.Reboot();
        env.AssertKindResolves("qlc", 1);
    }

    Y_UNIT_TEST(ReplacingClassWithoutPreviousNameAllocatesNewId) {
        TPDiskKindsEnvironment env;
        env.UseKind();
        env.Apply();
        const auto id = env.Disk().GetKind();
        env.Config.MutableBlobStorageConfig()->MutablePDiskKinds(0)->SetName("tlc");
        env.Drive().SetKindName("tlc");
        env.Apply();
        UNIT_ASSERT(env.Disk().GetKind() != id);
        UNIT_ASSERT_VALUES_EQUAL(env.Base().PDiskKindSize(), 1);
        env.Reboot();
        UNIT_ASSERT_VALUES_EQUAL(env.Disk().GetKindName(), "tlc");
        UNIT_ASSERT(env.Disk().GetKind() != id);
    }

    Y_UNIT_TEST(MatchingExplicitNumericKindIsAccepted) {
        TPDiskKindsEnvironment env;
        env.UseKind();
        env.Apply();
        UNIT_ASSERT_VALUES_EQUAL(env.Disk().GetKind(), 1);

        env.Drive().SetKind(1);
        env.Apply();
        UNIT_ASSERT_VALUES_EQUAL(env.Disk().GetKind(), 1);
        UNIT_ASSERT_VALUES_EQUAL(env.Disk().GetKindName(), "qlc");
    }

    Y_UNIT_TEST(ConflictingExplicitNumericKindIsRejected) {
        TPDiskKindsEnvironment env;
        env.UseKind();
        env.Apply();
        // In an otherwise empty cluster, qlc owns ID 1.
        env.Drive().SetKind(2);
        env.AssertRejected();
    }

    Y_UNIT_TEST(ProfileDefaultsAndDriveOverridesIgnoreHostDefault) {
        TPDiskKindsEnvironment env;
        env.UseKind();
        env.Profile().SetExpectedSlotSize(600000000000ULL);
        env.Profile().SetMaxSlots(8);
        env.Profile().SetIoPieceSizeBytes(65536);
        auto* hostDefault = env.Host().MutableDefaultHostPDiskConfig();
        hostDefault->SetLogWeight(42);
        hostDefault->SetIoPieceSizeBytes(131072);
        env.Drive().MutablePDiskConfig()->SetMaxSlots(4);
        env.Drive().MutablePDiskConfig()->SetIoPieceSizeBytes(32768);
        env.Apply();

        const auto effective = env.Disk().GetPDiskConfig();
        UNIT_ASSERT_VALUES_EQUAL(effective.GetExpectedSlotSize(), 600000000000ULL);
        UNIT_ASSERT_VALUES_EQUAL(effective.GetMaxSlots(), 4);
        UNIT_ASSERT_VALUES_EQUAL(effective.GetIoPieceSizeBytes(), 32768);
        UNIT_ASSERT(!effective.HasLogWeight());
    }

    Y_UNIT_TEST(EmptyDriveOverrideDoesNotClearProfile) {
        TPDiskKindsEnvironment env;
        env.UseKind();
        env.Profile().SetExpectedSlotSize(600000000000ULL);
        env.Profile().SetMaxSlots(8);
        env.Profile().SetIoPieceSizeBytes(65536);
        env.Drive().MutablePDiskConfig();
        env.Apply();

        const auto effective = env.Disk().GetPDiskConfig();
        UNIT_ASSERT_VALUES_EQUAL(effective.GetMaxSlots(), 8);
        UNIT_ASSERT_VALUES_EQUAL(effective.GetIoPieceSizeBytes(), 65536);
    }

    Y_UNIT_TEST(ExplicitZeroAndFalseOverrideProfileValues) {
        TPDiskKindsEnvironment env;
        env.UseKind();
        env.Profile().SetLogWeight(13);
        env.Profile().SetUseNoopScheduler(true);
        env.Profile().SetIoPieceSizeBytes(65536);
        env.Drive().MutablePDiskConfig()->SetLogWeight(0);
        env.Drive().MutablePDiskConfig()->SetUseNoopScheduler(false);
        env.Apply();

        const auto effective = env.Disk().GetPDiskConfig();
        UNIT_ASSERT(effective.HasLogWeight());
        UNIT_ASSERT_VALUES_EQUAL(effective.GetLogWeight(), 0);
        UNIT_ASSERT(effective.HasUseNoopScheduler());
        UNIT_ASSERT(!effective.GetUseNoopScheduler());
        UNIT_ASSERT_VALUES_EQUAL(effective.GetIoPieceSizeBytes(), 65536);
    }

    Y_UNIT_TEST(DriveCanCompleteRequiredProfileSettings) {
        TPDiskKindsEnvironment env;
        env.UseKind();
        env.Profile().SetExpectedSlotSize(600000000000ULL);
        env.Drive().MutablePDiskConfig()->SetMaxSlots(4);
        env.Apply();

        const auto effective = env.Disk().GetPDiskConfig();
        UNIT_ASSERT_VALUES_EQUAL(effective.GetExpectedSlotSize(), 600000000000ULL);
        UNIT_ASSERT_VALUES_EQUAL(effective.GetMaxSlots(), 4);
    }

    Y_UNIT_TEST(ConflictingInheritedSlotPoliciesAreRejected) {
        TPDiskKindsEnvironment env;
        env.UseKind();
        env.Profile().SetExpectedSlotSize(600000000000ULL);
        env.Profile().SetMaxSlots(8);
        env.Apply();

        env.Drive().MutablePDiskConfig()->SetExpectedSlotCount(4);
        env.AssertRejected();
    }

    Y_UNIT_TEST(ProfileUpdatesRecomputeFromOriginalDriveOverrides) {
        TPDiskKindsEnvironment env;
        env.UseKind();
        env.Profile().SetIoPieceSizeBytes(65536);
        env.Profile().SetLogWeight(13);
        env.Drive().MutablePDiskConfig()->SetBytesSchedulerWeight(4);
        env.AddDrive("/dev/nvme1n1", 0, "qlc").MutablePDiskConfig()->SetIoPieceSizeBytes(32768);
        env.Apply();
        UNIT_ASSERT_VALUES_EQUAL(env.Disk().GetPDiskConfig().GetIoPieceSizeBytes(), 65536);

        // Neither the old inherited value nor the host default may become an override.
        env.Profile().ClearIoPieceSizeBytes();
        env.Profile().SetLogWeight(42);
        env.Host().MutableDefaultHostPDiskConfig()->SetIoPieceSizeBytes(131072);
        env.Apply();
        const auto effective = env.Disk().GetPDiskConfig();
        UNIT_ASSERT(!effective.HasIoPieceSizeBytes());
        UNIT_ASSERT_VALUES_EQUAL(effective.GetLogWeight(), 42);
        UNIT_ASSERT_VALUES_EQUAL(effective.GetBytesSchedulerWeight(), 4);
        UNIT_ASSERT_VALUES_EQUAL(env.Disk("/dev/nvme1n1").GetPDiskConfig().GetIoPieceSizeBytes(), 32768);
        UNIT_ASSERT_VALUES_EQUAL(env.Disk("/dev/nvme1n1").GetPDiskConfig().GetLogWeight(), 42);
        env.Reboot();
        UNIT_ASSERT_VALUES_EQUAL(env.Disk().GetPDiskConfig().SerializeAsString(), effective.SerializeAsString());

        // Original overrides must survive restart, independently of the effective config.
        env.Profile().SetLogWeight(77);
        env.Apply();
        UNIT_ASSERT_VALUES_EQUAL(env.Disk().GetPDiskConfig().GetLogWeight(), 77);
        UNIT_ASSERT_VALUES_EQUAL(env.Disk("/dev/nvme1n1").GetPDiskConfig().GetLogWeight(), 77);
        UNIT_ASSERT_VALUES_EQUAL(env.Disk("/dev/nvme1n1").GetPDiskConfig().GetIoPieceSizeBytes(), 32768);
    }

    Y_UNIT_TEST(NamedPoolFilterUsesTheSameNumericIdAsTheDisk) {
        TPDiskKindsEnvironment env;
        env.UseKind();
        env.AddDrive("/dev/nvme1n1"); // Legacy kind 0 must not enter this pool.
        env.Apply();
        env.CreatePool(1, {}, "qlc");
        env.AssertPoolKind(1, 1);

        const auto response = env.Read();
        const auto& pool = response.GetStatus(1).GetStoragePool(0);
        UNIT_ASSERT_VALUES_EQUAL(pool.GetKind(), "capacity");
        bool hasNumericKind = false;
        for (const auto& filter : pool.GetPDiskFilter()) {
            for (const auto& property : filter.GetProperty()) {
                if (property.HasKind()) {
                    UNIT_ASSERT_VALUES_EQUAL(property.GetKind(), 1);
                    hasNumericKind = true;
                }
            }
        }
        UNIT_ASSERT(hasNumericKind);
    }

    Y_UNIT_TEST(NamedPoolFilterPreservesNameAndNumericKindAfterRestart) {
        TPDiskKindsEnvironment env;
        env.UseKind();
        env.Apply();
        env.CreatePool(1, {}, "qlc");

        auto readFilter = [&] {
            env.Read(); // Wait for initialization and pending configuration transactions after restart.
            const auto actorId = ResolveTablet(*env.Runtime, env.TabletId);
            auto* controller = dynamic_cast<TBlobStorageController*>(env.Runtime->FindActor(actorId));
            UNIT_ASSERT(controller);

            // Inspect the actual controller state: ReadStoragePool currently serializes only numeric Kind.
            const TBlobStorageController::TConfigState state(*controller,
                std::make_shared<THostRecordMapImpl>(env.Config), TInstant::Zero(), TMonotonic::Zero());
            const auto& pools = state.StoragePools.Get();
            const auto it = pools.find({1, 1});
            UNIT_ASSERT(it != pools.end());
            UNIT_ASSERT_VALUES_EQUAL(it->second.PDiskFilters.size(), 1);
            return *it->second.PDiskFilters.begin();
        };

        const auto before = readFilter();
        UNIT_ASSERT(before.Kind);
        UNIT_ASSERT_VALUES_EQUAL(*before.Kind, 1);
        UNIT_ASSERT(before.KindName);
        UNIT_ASSERT_VALUES_EQUAL(*before.KindName, "qlc");

        env.Reboot();
        const auto restored = readFilter();
        UNIT_ASSERT(restored.Kind);
        UNIT_ASSERT_VALUES_EQUAL(*restored.Kind, *before.Kind);
        UNIT_ASSERT_C(restored.KindName, "Named pool filter lost KindName after controller restart");
        UNIT_ASSERT_VALUES_EQUAL(*restored.KindName, *before.KindName);
    }

    Y_UNIT_TEST(RenameUpdatesPoolReferenceAndPreservesNumericKind) {
        TPDiskKindsEnvironment env;
        env.UseKind();
        env.Profile().SetLogWeight(8);
        env.Apply();
        env.CreatePool(1, {}, "qlc");
        const auto id = env.Disk().GetKind();
        auto host = env.SourceHost();
        auto pool = env.SourcePool();
        host.MutableDrive(0)->SetKindName("capacity");
        for (auto& property : *pool.MutablePDiskFilter(0)->MutableProperty()) {
            if (property.HasKindName()) {
                property.SetKindName("capacity");
            }
        }
        TConfigRequest request;
        auto* kind = request.AddCommand()->MutableDefinePDiskKind();
        kind->SetName("capacity");
        kind->SetPreviousName("qlc");
        kind->SetItemConfigGeneration(1);
        kind->MutablePDiskConfig()->SetLogWeight(16);
        request.AddCommand()->MutableDefineHostConfig()->CopyFrom(host);
        request.AddCommand()->MutableDefineStoragePool()->CopyFrom(pool);
        const auto response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.DebugString());
        env.Reboot();
        UNIT_ASSERT_VALUES_EQUAL(env.Disk().GetKind(), id);
        UNIT_ASSERT_VALUES_EQUAL(env.Disk().GetKindName(), "capacity");
        UNIT_ASSERT_VALUES_EQUAL(env.Disk().GetPDiskConfig().GetLogWeight(), 16);
        env.AssertPoolKind(1, id);
        UNIT_ASSERT_VALUES_EQUAL(env.SourcePool().GetPDiskFilter(0).SerializeAsString(),
            pool.GetPDiskFilter(0).SerializeAsString());

        const auto actorId = ResolveTablet(*env.Runtime, env.TabletId);
        auto* controller = dynamic_cast<TBlobStorageController*>(env.Runtime->FindActor(actorId));
        UNIT_ASSERT(controller);
        const TBlobStorageController::TConfigState state(*controller,
            std::make_shared<THostRecordMapImpl>(env.Config), TInstant::Zero(), TMonotonic::Zero());
        const auto& filters = state.StoragePools.Get().at({1, 1}).PDiskFilters;
        UNIT_ASSERT_VALUES_EQUAL(filters.size(), 1); // The row with the old name must be deleted.
        const auto& filter = *filters.begin();
        UNIT_ASSERT_VALUES_EQUAL(filter.Kind.GetRef(), id);
        UNIT_ASSERT_VALUES_EQUAL(filter.KindName.GetRef(), "capacity");
    }

    Y_UNIT_TEST(RenameLeavingAnOldPoolReferenceRollsBack) {
        TPDiskKindsEnvironment env;
        env.UseKind();
        env.Apply();
        env.CreatePool(1, {}, "qlc");
        const auto before = env.Snapshot();
        TConfigRequest request;
        auto* kind = request.AddCommand()->MutableDefinePDiskKind();
        kind->SetName("capacity");
        kind->SetPreviousName("qlc");
        kind->SetItemConfigGeneration(1);
        auto* host = request.AddCommand()->MutableDefineHostConfig();
        host->CopyFrom(env.SourceHost());
        host->MutableDrive(0)->SetKindName("capacity");
        const auto response = env.Invoke(request);
        UNIT_ASSERT_C(!response.GetSuccess(), response.DebugString());
        env.Reboot();
        UNIT_ASSERT_VALUES_EQUAL(env.Snapshot(), before);
    }

    Y_UNIT_TEST(CatalogReadReturnsProfilesGenerationsAndStableIds) {
        TPDiskKindsEnvironment env;
        env.AddKind("qlc").MutablePDiskConfig()->SetLogWeight(8);
        env.AddKind("tlc");
        env.Apply();
        env.Reboot();
        TConfigRequest request;
        auto* query = request.AddCommand()->MutableQueryBaseConfig();
        query->SetSuppressPDisks(true);
        query->SetSuppressVSlots(true);
        query->SetSuppressGroups(true);
        const auto response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.DebugString());
        const auto& base = response.GetStatus(0).GetBaseConfig();
        UNIT_ASSERT_VALUES_EQUAL(base.PDiskKindSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(base.PDiskSize(), 0);
        const auto& kind = base.GetPDiskKind(0);
        UNIT_ASSERT_VALUES_EQUAL(kind.GetKind(), 1);
        UNIT_ASSERT_VALUES_EQUAL(kind.GetConfig().GetName(), "qlc");
        UNIT_ASSERT_VALUES_EQUAL(kind.GetConfig().GetItemConfigGeneration(), 1);
        UNIT_ASSERT_VALUES_EQUAL(kind.GetConfig().GetPDiskConfig().GetLogWeight(), 8);
    }

    Y_UNIT_TEST(ProfileUpdateSendsEffectiveConfigWithoutRequestingRestart) {
        TPDiskKindsEnvironment env;
        env.UseKind();
        env.Profile().SetLogWeight(8);
        env.Drive().MutablePDiskConfig()->SetIoPieceSizeBytes(32768);
        env.Apply();
        // Register on the actual runtime node, establishing the notification pipe.
        env.RegisterSerialDrive("serial-0", "/dev/nvme0n1");
        TVector<TNodeWardenServiceSet::TPDisk> updates;
        auto observer = env.Runtime->AddObserver<TEvBlobStorage::TEvControllerNodeServiceSetUpdate>(
            [&](TEvBlobStorage::TEvControllerNodeServiceSetUpdate::TPtr& ev) {
                for (const auto& disk : ev->Get()->Record.GetServiceSet().GetPDisks()) {
                    updates.push_back(disk);
                }
            });
        TConfigRequest request;
        auto* kind = request.AddCommand()->MutableDefinePDiskKind();
        kind->SetName("qlc");
        kind->SetItemConfigGeneration(1);
        kind->MutablePDiskConfig()->SetLogWeight(16);
        const auto response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.DebugString());
        env.Runtime->WaitFor("updated PDisk configuration", [&] { return !updates.empty(); }, env.Timeout);
        observer.Remove();
        bool found = false;
        for (const auto& disk : updates) {
            UNIT_ASSERT(!disk.HasEntityStatus() || disk.GetEntityStatus() == INITIAL);
            if (disk.GetPath() == "/dev/nvme0n1") {
                UNIT_ASSERT_VALUES_EQUAL(disk.GetPDiskConfig().GetLogWeight(), 16);
                UNIT_ASSERT_VALUES_EQUAL(disk.GetPDiskConfig().GetIoPieceSizeBytes(), 32768);
                UNIT_ASSERT_VALUES_EQUAL(TPDiskCategory(disk.GetPDiskCategory()).Kind(), 1);
                found = true;
            }
        }
        UNIT_ASSERT(found);
    }

    Y_UNIT_TEST(ReplacementAndVDiskMovementRespectNamedPoolFilter) {
        for (bool populate : {false, true}) {
            TPDiskKindsEnvironment env;
            env.UseKind();
            env.AddDrive("/dev/nvme1n1", 0, "qlc");
            env.AddKind("tlc");
            env.AddDrive("/dev/nvme2n1", 0, "tlc");
            env.Apply();
            env.CreatePool(1, {}, "qlc");
            const auto base = env.Base();
            const auto& slot = base.GetVSlot(0);
            auto requestFor = [&](const TBaseConfig::TPDisk& target) {
                TConfigRequest request;
                request.SetIgnoreGroupReserve(true);
                request.SetIgnoreGroupFailModelChecks(true); // This fixture does not run VDisks.
                if (populate) {
                    auto* cmd = request.AddCommand()->MutablePopulatePDisk();
                    auto* id = cmd->MutableDestinationPDisk()->MutableTargetPDiskId();
                    id->SetNodeId(target.GetNodeId());
                    id->SetPDiskId(target.GetPDiskId());
                    VDiskIDFromVDiskID(TVDiskID(slot.GetGroupId(), slot.GetGroupGeneration(),
                        slot.GetFailRealmIdx(), slot.GetFailDomainIdx(), slot.GetVDiskIdx()), cmd->AddVDiskId());
                } else {
                    auto* cmd = request.AddCommand()->MutableReassignGroupDisk();
                    cmd->SetGroupId(slot.GetGroupId());
                    cmd->SetGroupGeneration(slot.GetGroupGeneration());
                    cmd->SetFailRealmIdx(slot.GetFailRealmIdx());
                    cmd->SetFailDomainIdx(slot.GetFailDomainIdx());
                    cmd->SetVDiskIdx(slot.GetVDiskIdx());
                    cmd->MutableTargetPDiskId()->SetNodeId(target.GetNodeId());
                    cmd->MutableTargetPDiskId()->SetPDiskId(target.GetPDiskId());
                }
                return request;
            };
            const auto before = env.Snapshot();
            auto response = env.Invoke(requestFor(env.Disk("/dev/nvme2n1")));
            UNIT_ASSERT_C(!response.GetSuccess(), response.DebugString());
            UNIT_ASSERT_VALUES_EQUAL(env.Snapshot(), before);
            bool moved = false;
            for (const auto& disk : base.GetPDisk()) {
                if (disk.GetKind() == 1 && disk.GetPDiskId() != slot.GetVSlotId().GetPDiskId()) {
                    response = env.Invoke(requestFor(disk));
                    UNIT_ASSERT_C(response.GetSuccess(), response.DebugString());
                    moved = true;
                    break;
                }
            }
            UNIT_ASSERT(moved);
            env.AssertPoolKind(1, 1);
        }
    }

    Y_UNIT_TEST(MergingBoxesPreservesOriginalNamedDriveOverrides) {
        TPDiskKindsEnvironment env;
        env.UseKind();
        env.Profile().SetLogWeight(8);
        env.Drive().MutablePDiskConfig()->SetIoPieceSizeBytes(32768);
        env.Apply();
        TConfigRequest request;
        auto* host = request.AddCommand()->MutableDefineHostConfig();
        host->SetHostConfigId(2);
        auto* drive = host->AddDrive();
        drive->SetPath("/dev/nvme1n1");
        drive->SetType(NVME);
        drive->SetKindName("qlc");
        drive->MutablePDiskConfig()->SetLogWeight(2);
        auto* box = request.AddCommand()->MutableDefineBox();
        box->SetBoxId(2);
        auto* boxHost = box->AddHost();
        boxHost->SetHostConfigId(2);
        boxHost->MutableKey()->SetNodeId(std::get<2>(env.GetNodes().front()));
        auto response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.DebugString());
        request.Clear();
        auto* merge = request.AddCommand()->MutableMergeBoxes();
        merge->SetOriginBoxId(2);
        merge->SetOriginBoxGeneration(1);
        merge->SetTargetBoxId(1);
        merge->SetTargetBoxGeneration(1);
        response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.DebugString());
        env.Reboot();
        request.Clear();
        auto* kind = request.AddCommand()->MutableDefinePDiskKind();
        kind->SetName("qlc");
        kind->SetItemConfigGeneration(1);
        kind->MutablePDiskConfig()->SetLogWeight(16);
        response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(env.Disk().GetPDiskConfig().GetLogWeight(), 16);
        UNIT_ASSERT_VALUES_EQUAL(env.Disk().GetPDiskConfig().GetIoPieceSizeBytes(), 32768);
        UNIT_ASSERT_VALUES_EQUAL(env.Disk("/dev/nvme1n1").GetPDiskConfig().GetLogWeight(), 2);
        UNIT_ASSERT(!env.Disk().GetSourcePDiskConfig().HasLogWeight());
    }

    Y_UNIT_TEST(MaterializedHostAndPoolConfigWorksWithoutCatalog) {
        TPDiskKindsEnvironment env;
        env.UseKind();
        env.Profile().SetLogWeight(8);
        env.Drive().MutablePDiskConfig()->SetIoPieceSizeBytes(32768);
        env.Apply();
        env.CreatePool(1, {}, "qlc", 0);
        const auto read = env.Read();
        auto host = read.GetStatus(0).GetHostConfig(0);
        auto pool = read.GetStatus(1).GetStoragePool(0);
        host.MutableDrive(0)->ClearKindName();
        UNIT_ASSERT_VALUES_EQUAL(host.GetDrive(0).GetKind(), 1);
        UNIT_ASSERT_VALUES_EQUAL(host.GetDrive(0).GetPDiskConfig().GetLogWeight(), 8);
        for (const auto& property : pool.GetPDiskFilter(0).GetProperty()) {
            UNIT_ASSERT(!property.HasKindName());
        }

        TPDiskKindsEnvironment restored;
        host.SetItemConfigGeneration(0);
        restored.Config.MutableBlobStorageConfig()->MutableDefineHostConfig(0)->CopyFrom(host);
        restored.Apply();
        pool.SetItemConfigGeneration(0);
        TConfigRequest request;
        request.AddCommand()->MutableDefineStoragePool()->CopyFrom(pool);
        auto response = restored.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.DebugString());
        restored.Reboot();
        UNIT_ASSERT_VALUES_EQUAL(restored.Base().PDiskKindSize(), 0);
        const auto disk = restored.Disk();
        UNIT_ASSERT_VALUES_EQUAL(disk.GetKind(), 1);
        UNIT_ASSERT(disk.GetKindName().empty());
        UNIT_ASSERT_VALUES_EQUAL(disk.GetPDiskConfig().GetLogWeight(), 8);
        UNIT_ASSERT_VALUES_EQUAL(disk.GetPDiskConfig().GetIoPieceSizeBytes(), 32768);
    }

    Y_UNIT_TEST(MaterializedConfigCanReplaceNamedSourcesBeforeDowngrade) {
        TPDiskKindsEnvironment env;
        env.UseKind();
        env.Profile().SetLogWeight(8);
        env.Drive().MutablePDiskConfig()->SetIoPieceSizeBytes(32768);
        env.Apply();
        env.RegisterSerialDrive("serial-1", "/dev/nvme1n1");
        TConfigRequest request;
        auto* serial = request.AddCommand()->MutableAddDriveSerial();
        serial->SetSerial("serial-1");
        serial->SetBoxId(1);
        serial->SetKindName("qlc");
        auto response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.DebugString());
        env.CreatePool(1, {}, "qlc", 0);

        const auto effective = env.Read();
        request.Clear();
        auto* host = request.AddCommand()->MutableDefineHostConfig();
        host->CopyFrom(effective.GetStatus(0).GetHostConfig(0));
        host->MutableDrive(0)->ClearKindName();
        serial = request.AddCommand()->MutableAddDriveSerial();
        serial->SetSerial("serial-1");
        serial->SetBoxId(1);
        serial->SetUpdateExisting(true);
        const auto disk = env.Disk("/dev/nvme1n1");
        serial->SetKind(disk.GetKind());
        serial->MutablePDiskConfig()->CopyFrom(disk.GetPDiskConfig());
        request.AddCommand()->MutableDefineStoragePool()->CopyFrom(effective.GetStatus(1).GetStoragePool(0));
        response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.DebugString());
        env.Reboot();

        // Both definition tables now contain complete configs that an old BSC can use without the catalog.
        UNIT_ASSERT_VALUES_EQUAL(env.SourceHost().GetDrive(0).GetPDiskConfig().GetLogWeight(), 8);
        UNIT_ASSERT(env.SourceHost().GetDrive(0).GetKindName().empty());
        for (const auto& path : {"/dev/nvme0n1", "/dev/nvme1n1"}) {
            UNIT_ASSERT_VALUES_EQUAL(env.Disk(path).GetPDiskConfig().GetLogWeight(), 8);
            UNIT_ASSERT_VALUES_EQUAL(env.Disk(path).GetSourcePDiskConfig().GetLogWeight(), 8);
            UNIT_ASSERT_VALUES_EQUAL(env.Disk(path).GetKindName(), "qlc");
        }

        // A later profile update/rename changes the displayed name, but not these materialized definitions.
        request.Clear();
        auto* kind = request.AddCommand()->MutableDefinePDiskKind();
        kind->SetName("capacity");
        kind->SetPreviousName("qlc");
        kind->SetItemConfigGeneration(1);
        kind->MutablePDiskConfig()->SetLogWeight(16);
        request.AddCommand()->MutableQueryBaseConfig();
        response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.DebugString());
        for (const auto& pdisk : response.GetStatus(1).GetBaseConfig().GetPDisk()) {
            UNIT_ASSERT_VALUES_EQUAL(pdisk.GetKindName(), "capacity");
        }
        env.Reboot();
        UNIT_ASSERT_VALUES_EQUAL(env.Disk().GetPDiskConfig().GetLogWeight(), 8);
        UNIT_ASSERT_VALUES_EQUAL(env.Disk("/dev/nvme1n1").GetPDiskConfig().GetLogWeight(), 8);
        UNIT_ASSERT_VALUES_EQUAL(env.Disk().GetKindName(), "capacity");
        UNIT_ASSERT_VALUES_EQUAL(env.Disk("/dev/nvme1n1").GetKindName(), "capacity");
    }

    Y_UNIT_TEST(LegacyNumericKindSurvivesRestartAndEditing) {
        TPDiskKindsEnvironment env;
        env.Drive().SetKind(17);
        env.Drive().MutablePDiskConfig()->SetLogWeight(8);
        env.Apply();
        env.CreatePool(1, 17, {}, 0);
        env.Reboot();
        UNIT_ASSERT_VALUES_EQUAL(env.SourceHost().GetDrive(0).GetKind(), 17);
        UNIT_ASSERT_VALUES_EQUAL(env.SourceHost().GetDrive(0).GetPDiskConfig().GetLogWeight(), 8);
        UNIT_ASSERT_VALUES_EQUAL(env.SourcePool().GetStoragePoolId(), 1);
        TConfigRequest request;
        auto* host = request.AddCommand()->MutableDefineHostConfig();
        host->CopyFrom(env.SourceHost());
        host->MutableDrive(0)->MutablePDiskConfig()->SetLogWeight(16);
        const auto edited = env.Invoke(request);
        UNIT_ASSERT_C(edited.GetSuccess(), edited.DebugString());
        env.Reboot();
        UNIT_ASSERT_VALUES_EQUAL(env.Disk().GetPDiskConfig().GetLogWeight(), 16);
        UNIT_ASSERT_VALUES_EQUAL(env.Disk().GetKind(), 17);
    }

    Y_UNIT_TEST(ProfileOnlyCommandUpdatesAllDisksAndSurvivesRestart) {
        TPDiskKindsEnvironment env;
        env.UseKind();
        env.Profile().SetLogWeight(8);
        env.Drive().MutablePDiskConfig()->SetIoPieceSizeBytes(32768);
        env.AddDrive("/dev/nvme1n1", 0, "qlc").MutablePDiskConfig()->SetLogWeight(2);
        env.Apply();

        TConfigRequest request;
        auto* kind = request.AddCommand()->MutableDefinePDiskKind();
        kind->SetName("qlc");
        kind->SetItemConfigGeneration(1);
        kind->MutablePDiskConfig()->SetLogWeight(16);
        kind->MutablePDiskConfig()->SetIoPieceSizeBytes(65536);
        const auto response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.DebugString());
        for (bool restart : {false, true}) {
            if (restart) {
                env.Reboot();
            }
            const auto first = env.Disk().GetPDiskConfig();
            const auto second = env.Disk("/dev/nvme1n1").GetPDiskConfig();
            UNIT_ASSERT_VALUES_EQUAL(first.GetLogWeight(), 16);
            UNIT_ASSERT_VALUES_EQUAL(first.GetIoPieceSizeBytes(), 32768);
            UNIT_ASSERT_VALUES_EQUAL(second.GetLogWeight(), 2);
            UNIT_ASSERT_VALUES_EQUAL(second.GetIoPieceSizeBytes(), 65536);
        }
    }

    Y_UNIT_TEST(ProfileUpdateFailureRollsBackEveryDiskAndCatalogGeneration) {
        TPDiskKindsEnvironment env;
        env.UseKind();
        env.Profile().SetLogWeight(8);
        env.AddDrive("/dev/nvme1n1", 0, "qlc").MutablePDiskConfig()->SetExpectedSlotCount(4);
        env.Apply();
        const TString before = env.Snapshot();

        TConfigRequest request;
        auto* kind = request.AddCommand()->MutableDefinePDiskKind();
        kind->SetName("qlc");
        kind->SetItemConfigGeneration(1);
        kind->MutablePDiskConfig()->SetExpectedSlotSize(600000000000ULL);
        kind->MutablePDiskConfig()->SetMaxSlots(8);
        const auto rejected = env.Invoke(request);
        UNIT_ASSERT_C(!rejected.GetSuccess(), rejected.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(env.Snapshot(), before);
        env.Reboot();
        UNIT_ASSERT_VALUES_EQUAL(env.Snapshot(), before);
        const auto response = env.DefineKind("qlc", 1);
        UNIT_ASSERT_C(response.GetSuccess(), response.DebugString());
    }

    Y_UNIT_TEST(ProfileAndDriveOverridesCanChangeInOneRequest) {
        TPDiskKindsEnvironment env;
        env.UseKind();
        env.Drive().MutablePDiskConfig()->SetExpectedSlotCount(4);
        env.Apply();

        TConfigRequest request;
        auto* kind = request.AddCommand()->MutableDefinePDiskKind();
        kind->SetName("qlc");
        kind->SetItemConfigGeneration(1);
        kind->MutablePDiskConfig()->SetExpectedSlotSize(600000000000ULL);
        kind->MutablePDiskConfig()->SetMaxSlots(8);
        auto* host = request.AddCommand()->MutableDefineHostConfig();
        host->CopyFrom(env.SourceHost());
        host->MutableDrive(0)->ClearPDiskConfig();
        const auto response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.DebugString());
        const auto config = env.Disk().GetPDiskConfig();
        UNIT_ASSERT_VALUES_EQUAL(config.GetExpectedSlotSize(), 600000000000ULL);
        UNIT_ASSERT_VALUES_EQUAL(config.GetMaxSlots(), 8);
        UNIT_ASSERT(!config.HasExpectedSlotCount());
    }

    Y_UNIT_TEST(HostDriveRowsKeepNamedOverridesAndExpandHostDefaultsWithoutAKindName) {
        TPDiskKindsEnvironment env;
        env.UseKind();
        env.Profile().SetLogWeight(8);
        env.Drive().MutablePDiskConfig()->SetIoPieceSizeBytes(32768);
        env.Host().MutableDefaultHostPDiskConfig()->SetLogWeight(99);
        env.AddDrive("/dev/nvme1n1");
        env.AddDrive("/dev/nvme2n1").MutablePDiskConfig(); // Explicit empty config suppresses the host default.
        env.Apply();
        env.Reboot();
        env.Read();

        const auto actorId = ResolveTablet(*env.Runtime, env.TabletId);
        auto* controller = dynamic_cast<TBlobStorageController*>(env.Runtime->FindActor(actorId));
        UNIT_ASSERT(controller);
        const TBlobStorageController::TConfigState state(*controller,
            std::make_shared<THostRecordMapImpl>(env.Config), TInstant::Zero(), TMonotonic::Zero());
        const auto& host = state.HostConfigs.Get().at(1);
        for (const auto& [key, drive] : host.Drives) {
            UNIT_ASSERT(drive.PDiskConfig);
            NKikimrBlobStorage::TPDiskConfig config;
            UNIT_ASSERT(config.ParseFromString(*drive.PDiskConfig));
            if (key.Path == "/dev/nvme1n1") {
                UNIT_ASSERT_VALUES_EQUAL(config.GetLogWeight(), 99);
            } else {
                UNIT_ASSERT(!config.HasLogWeight());
                if (key.Path == "/dev/nvme0n1") {
                    UNIT_ASSERT_VALUES_EQUAL(config.GetIoPieceSizeBytes(), 32768);
                }
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(env.Disk().GetPDiskConfig().GetLogWeight(), 8);
        UNIT_ASSERT_VALUES_EQUAL(env.Disk("/dev/nvme1n1").GetPDiskConfig().GetLogWeight(), 99);
        UNIT_ASSERT(!env.Disk("/dev/nvme2n1").GetPDiskConfig().HasLogWeight());
    }

    Y_UNIT_TEST(SourceReadEditWriteDoesNotTurnInheritedFieldsIntoOverrides) {
        TPDiskKindsEnvironment env;
        env.UseKind();
        env.Profile().SetLogWeight(8);
        env.Drive().MutablePDiskConfig()->SetIoPieceSizeBytes(32768);
        env.Host().MutableDefaultHostPDiskConfig()->SetLogWeight(99);
        env.AddDrive("/dev/nvme1n1");
        env.Apply();
        env.Reboot();
        const auto source = env.SourceHost();
        UNIT_ASSERT(!source.GetDrive(0).GetPDiskConfig().HasLogWeight());
        UNIT_ASSERT(!source.HasDefaultHostPDiskConfig());
        UNIT_ASSERT_VALUES_EQUAL(source.GetDrive(1).GetPDiskConfig().GetLogWeight(), 99);

        TConfigRequest request;
        auto* host = request.AddCommand()->MutableDefineHostConfig();
        host->CopyFrom(source);
        host->SetName("edited");
        auto* kind = request.AddCommand()->MutableDefinePDiskKind();
        kind->SetName("qlc");
        kind->SetItemConfigGeneration(1);
        kind->MutablePDiskConfig()->SetLogWeight(16);
        const auto response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(env.Disk().GetPDiskConfig().GetLogWeight(), 16);
        UNIT_ASSERT_VALUES_EQUAL(env.Disk().GetPDiskConfig().GetIoPieceSizeBytes(), 32768);
        UNIT_ASSERT_VALUES_EQUAL(env.Disk("/dev/nvme1n1").GetPDiskConfig().GetLogWeight(), 99);
    }

    Y_UNIT_TEST(SerialDriveProfilesPersistAndReAddingClearsTheName) {
        TPDiskKindsEnvironment env;
        env.AddKind("qlc");
        env.Profile().SetLogWeight(8);
        env.Apply();
        env.RegisterSerialDrive("serial-1", "/dev/nvme1n1");
        TConfigRequest request;
        auto* drive = request.AddCommand()->MutableAddDriveSerial();
        drive->SetSerial("serial-1");
        drive->SetBoxId(1);
        drive->SetKindName("qlc");
        drive->MutablePDiskConfig()->SetIoPieceSizeBytes(32768);
        auto response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.DebugString());
        env.Reboot();
        UNIT_ASSERT_VALUES_EQUAL(env.Disk("/dev/nvme1n1").GetKindName(), "qlc");
        UNIT_ASSERT_VALUES_EQUAL(env.Disk("/dev/nvme1n1").GetPDiskConfig().GetLogWeight(), 8);

        {
            const auto actorId = ResolveTablet(*env.Runtime, env.TabletId);
            auto* controller = dynamic_cast<TBlobStorageController*>(env.Runtime->FindActor(actorId));
            UNIT_ASSERT(controller);
            const TBlobStorageController::TConfigState state(*controller,
                std::make_shared<THostRecordMapImpl>(env.Config), TInstant::Zero(), TMonotonic::Zero());
            const auto* stored = state.DrivesSerials.Find(TBlobStorageController::TSerial("serial-1"));
            UNIT_ASSERT(stored && stored->PDiskConfig);
            NKikimrBlobStorage::TPDiskConfig overrides;
            UNIT_ASSERT(overrides.ParseFromString(*stored->PDiskConfig));
            UNIT_ASSERT(!overrides.HasLogWeight());
            UNIT_ASSERT_VALUES_EQUAL(overrides.GetIoPieceSizeBytes(), 32768);
        }

        request.Clear();
        auto* kind = request.AddCommand()->MutableDefinePDiskKind();
        kind->SetName("qlc");
        kind->SetItemConfigGeneration(1);
        kind->MutablePDiskConfig()->SetLogWeight(16);
        response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(env.Disk("/dev/nvme1n1").GetPDiskConfig().GetLogWeight(), 16);
        UNIT_ASSERT_VALUES_EQUAL(env.Disk("/dev/nvme1n1").GetPDiskConfig().GetIoPieceSizeBytes(), 32768);

        request.Clear();
        request.AddCommand()->MutableRemoveDriveSerial()->SetSerial("serial-1");
        drive = request.AddCommand()->MutableAddDriveSerial();
        drive->SetSerial("serial-1");
        drive->SetBoxId(1);
        response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.DebugString());
        env.Reboot();
        const auto disk = env.Disk("/dev/nvme1n1");
        UNIT_ASSERT_VALUES_EQUAL(disk.GetKind(), 0);
        UNIT_ASSERT(disk.GetKindName().empty());
        UNIT_ASSERT(!disk.GetPDiskConfig().HasLogWeight());
    }

    Y_UNIT_TEST(OccupiedSerialDriveCanBeRenamedWithItsKindAndPool) {
        TPDiskKindsEnvironment env;
        env.AddKind("qlc").MutablePDiskConfig()->SetLogWeight(8);
        env.Apply();
        env.RegisterSerialDrive("serial-1", "/dev/nvme1n1");
        TConfigRequest request;
        auto* serial = request.AddCommand()->MutableAddDriveSerial();
        serial->SetSerial("serial-1");
        serial->SetBoxId(1);
        serial->SetKindName("qlc");
        serial->MutablePDiskConfig()->SetIoPieceSizeBytes(32768);
        auto response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.DebugString());
        env.CreatePool(1, {}, "qlc");
        const auto disk = env.Disk("/dev/nvme1n1");
        auto pool = env.SourcePool();
        for (auto& property : *pool.MutablePDiskFilter(0)->MutableProperty()) {
            if (property.HasKindName()) {
                property.SetKindName("capacity");
            }
        }
        TAddDriveSerial updated = *serial;
        updated.SetKindName("capacity");
        updated.SetUpdateExisting(true);
        request.Clear();
        auto* kind = request.AddCommand()->MutableDefinePDiskKind();
        kind->SetName("capacity");
        kind->SetPreviousName("qlc");
        kind->SetItemConfigGeneration(1);
        kind->MutablePDiskConfig()->SetLogWeight(16);
        request.AddCommand()->MutableAddDriveSerial()->CopyFrom(updated);
        request.AddCommand()->MutableDefineStoragePool()->CopyFrom(pool);
        response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.DebugString());
        env.Reboot();
        const auto renamed = env.Disk("/dev/nvme1n1");
        UNIT_ASSERT_VALUES_EQUAL(renamed.GetPDiskId(), disk.GetPDiskId());
        UNIT_ASSERT_VALUES_EQUAL(renamed.GetGuid(), disk.GetGuid());
        UNIT_ASSERT_VALUES_EQUAL(renamed.GetKind(), disk.GetKind());
        UNIT_ASSERT_VALUES_EQUAL(renamed.GetKindName(), "capacity");
        UNIT_ASSERT_VALUES_EQUAL(renamed.GetPDiskConfig().GetLogWeight(), 16);
        UNIT_ASSERT_VALUES_EQUAL(renamed.GetPDiskConfig().GetIoPieceSizeBytes(), 32768);
        UNIT_ASSERT(!renamed.GetSourcePDiskConfig().HasLogWeight());
        UNIT_ASSERT_VALUES_EQUAL(renamed.GetSourcePDiskConfig().GetIoPieceSizeBytes(), 32768);
        env.AssertPoolKind(1, disk.GetKind());
    }

    Y_UNIT_TEST(RenameUpdatesHostAndSerialReferencesInTheSameBox) {
        TPDiskKindsEnvironment env;
        env.UseKind();
        env.Profile().SetLogWeight(8);
        env.Apply();
        env.RegisterSerialDrive("serial-1", "/dev/nvme1n1");
        TConfigRequest request;
        auto* serial = request.AddCommand()->MutableAddDriveSerial();
        serial->SetSerial("serial-1");
        serial->SetBoxId(1);
        serial->SetKindName("qlc");
        auto response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.DebugString());

        request.Clear();
        auto* kind = request.AddCommand()->MutableDefinePDiskKind();
        kind->SetName("capacity");
        kind->SetPreviousName("qlc");
        kind->SetItemConfigGeneration(1);
        kind->MutablePDiskConfig()->SetLogWeight(16);
        auto* host = request.AddCommand()->MutableDefineHostConfig();
        host->CopyFrom(env.SourceHost());
        host->MutableDrive(0)->SetKindName("capacity");
        serial = request.AddCommand()->MutableAddDriveSerial();
        serial->SetSerial("serial-1");
        serial->SetBoxId(1);
        serial->SetKindName("capacity");
        serial->SetUpdateExisting(true);
        response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.DebugString());
        env.Reboot();
        for (const auto& path : {"/dev/nvme0n1", "/dev/nvme1n1"}) {
            const auto disk = env.Disk(path);
            UNIT_ASSERT_VALUES_EQUAL(disk.GetKindName(), "capacity");
            UNIT_ASSERT_VALUES_EQUAL(disk.GetKind(), 1);
            UNIT_ASSERT_VALUES_EQUAL(disk.GetPDiskConfig().GetLogWeight(), 16);
        }
    }

    Y_UNIT_TEST(SerialDriveRejectsUnknownNamesAndConflictingIds) {
        TPDiskKindsEnvironment env;
        env.AddKind("qlc");
        env.Apply();
        env.RegisterSerialDrive("serial-1", "/dev/nvme1n1");
        const TString before = env.Snapshot();
        for (const TString name : {TString("missing"), TString("qlc")}) {
            TConfigRequest request;
            auto* drive = request.AddCommand()->MutableAddDriveSerial();
            drive->SetSerial("serial-1");
            drive->SetBoxId(1);
            drive->SetKindName(name);
            drive->SetKind(2);
            const auto response = env.Invoke(request);
            UNIT_ASSERT_C(!response.GetSuccess(), response.DebugString());
            UNIT_ASSERT_VALUES_EQUAL(env.Snapshot(), before);
        }
    }

    Y_UNIT_TEST(NamedPoolFilterPropertiesAreOrderIndependent) {
        TPDiskKindsEnvironment env;
        env.UseKind();
        env.Apply();
        for (bool reverse : {false, true}) {
            auto request = env.PoolRequest(reverse ? 2 : 1, 1, "qlc", 0);
            auto* properties = request.MutableCommand(0)->MutableDefineStoragePool()
                ->MutablePDiskFilter(0)->MutableProperty();
            if (reverse) {
                properties->SwapElements(1, 2);
            }
            const auto response = env.Invoke(request);
            UNIT_ASSERT_C(response.GetSuccess(), response.DebugString());
        }
    }

    Y_UNIT_TEST(SourcePoolReadEditWritePreservesTheNamedReference) {
        TPDiskKindsEnvironment env;
        env.UseKind();
        env.Apply();
        env.CreatePool(1, {}, "qlc");
        env.Reboot();
        auto pool = env.SourcePool();
        bool found = false;
        for (const auto& property : pool.GetPDiskFilter(0).GetProperty()) {
            if (property.HasKindName()) {
                UNIT_ASSERT_VALUES_EQUAL(property.GetKindName(), "qlc");
                found = true;
            }
            UNIT_ASSERT(!property.HasKind());
        }
        UNIT_ASSERT(found);
        pool.SetName("renamed pool");
        TConfigRequest request;
        request.AddCommand()->MutableDefineStoragePool()->CopyFrom(pool);
        const auto response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.DebugString());
        env.Reboot();
        UNIT_ASSERT_VALUES_EQUAL(env.SourcePool().GetPDiskFilter(0).SerializeAsString(),
            pool.GetPDiskFilter(0).SerializeAsString());
    }

    Y_UNIT_TEST(SourcePoolReadReconstructsNamedAndNumericFiltersAfterRestart) {
        TPDiskKindsEnvironment env;
        env.UseKind();
        env.Apply();
        auto request = env.PoolRequest(1, 1, "qlc", 0);
        auto* pool = request.MutableCommand(0)->MutableDefineStoragePool();
        auto* named = pool->MutablePDiskFilter(0);
        named->AddProperty()->SetSharedWithOs(false);
        named->AddProperty()->SetReadCentric(true);
        named->MutableProperty()->SwapElements(0, 2); // Input order does not define the stored representation.
        auto* numeric = pool->AddPDiskFilter();
        numeric->AddProperty()->SetType(NVME);
        numeric->AddProperty()->SetKind(0);
        auto response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.DebugString());
        env.Reboot();

        const auto source = env.SourcePool();
        UNIT_ASSERT_VALUES_EQUAL(source.PDiskFilterSize(), 2);
        ui32 namedFilters = 0;
        ui32 numericFilters = 0;
        for (const auto& filter : source.GetPDiskFilter()) {
            bool hasName = false;
            bool hasKind = false;
            bool hasSharedWithOs = false;
            bool hasReadCentric = false;
            for (const auto& property : filter.GetProperty()) {
                if (property.HasKindName()) {
                    hasName = true;
                    UNIT_ASSERT_VALUES_EQUAL(property.GetKindName(), "qlc");
                } else if (property.HasKind()) {
                    hasKind = true;
                    UNIT_ASSERT_VALUES_EQUAL(property.GetKind(), 0);
                } else if (property.HasSharedWithOs()) {
                    hasSharedWithOs = true;
                    UNIT_ASSERT(!property.GetSharedWithOs());
                } else if (property.HasReadCentric()) {
                    hasReadCentric = true;
                    UNIT_ASSERT(property.GetReadCentric());
                }
            }
            if (hasName) {
                ++namedFilters;
                UNIT_ASSERT(!hasKind);
                UNIT_ASSERT(hasSharedWithOs && hasReadCentric);
            } else {
                ++numericFilters;
                UNIT_ASSERT(hasKind);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(namedFilters, 1);
        UNIT_ASSERT_VALUES_EQUAL(numericFilters, 1);

        const auto effective = env.Read().GetStatus(1).GetStoragePool(0);
        request.Clear();
        request.AddCommand()->MutableDefineStoragePool()->CopyFrom(source);
        response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.DebugString());
        env.Reboot();
        const auto roundTrip = env.Read().GetStatus(1).GetStoragePool(0);
        UNIT_ASSERT_VALUES_EQUAL(roundTrip.PDiskFilterSize(), effective.PDiskFilterSize());
        for (int i = 0; i < effective.PDiskFilterSize(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(roundTrip.GetPDiskFilter(i).SerializeAsString(),
                effective.GetPDiskFilter(i).SerializeAsString());
        }
    }

    Y_UNIT_TEST(ExplicitNumericKindInNamedFilterMustMatchIncludingZero) {
        TPDiskKindsEnvironment env;
        env.UseKind();
        env.Apply();
        env.CreatePool(1, 1, "qlc");
        const TString accepted = env.Snapshot();
        for (ui64 conflictingKind : {0ULL, 2ULL}) {
            auto request = env.PoolRequest(1, conflictingKind, "qlc");
            request.MutableCommand(0)->MutableDefineStoragePool()->SetItemConfigGeneration(1);
            const auto response = env.Invoke(request);
            UNIT_ASSERT_C(!response.GetSuccess(), response.DebugString());
            UNIT_ASSERT_VALUES_EQUAL(env.Snapshot(), accepted);
        }
    }

    Y_UNIT_TEST(TypeOnlyFilterAlsoAcceptsNamedDisks) {
        TPDiskKindsEnvironment env;
        env.UseKind();
        env.Apply();
        env.CreatePool(1);
        env.AssertPoolKind(1, 1);
    }

    Y_UNIT_TEST(KindReferencedOnlyByAPoolCannotBeDeleted) {
        TPDiskKindsEnvironment env;
        env.AddKind("qlc"); // The disk itself has no named kind.
        env.Apply();
        env.CreatePool(1, {}, "qlc", 0);
        env.Config.MutableBlobStorageConfig()->ClearPDiskKinds();
        env.AssertRejected();
    }

    Y_UNIT_TEST(OccupiedDiskCannotChangeToAKindExcludedByItsPool) {
        TPDiskKindsEnvironment env;
        env.UseKind();
        env.AddKind("tlc");
        env.Apply();
        env.CreatePool(1, 1);
        env.AssertPoolKind(1, 1);

        env.Drive().SetKindName("tlc");
        env.AssertRejected();
        env.AssertPoolKind(1, 1);
    }

    Y_UNIT_TEST(OccupiedLegacyDiskKeepsNumericKindChangeBehavior) {
        TPDiskKindsEnvironment env;
        env.AddKind("unused");
        env.Drive().SetKind(17);
        env.Apply();
        env.CreatePool(1, 17);
        const auto pdiskId = env.Disk().GetPDiskId();
        env.Drive().SetKind(18);
        env.Apply();
        UNIT_ASSERT_VALUES_EQUAL(env.Disk().GetKind(), 18);
        UNIT_ASSERT_VALUES_EQUAL(env.Disk().GetPDiskId(), pdiskId);
        UNIT_ASSERT_VALUES_EQUAL(env.Base().GetVSlot(0).GetVSlotId().GetPDiskId(), pdiskId);
    }
}

} // namespace NKikimr::NBsController

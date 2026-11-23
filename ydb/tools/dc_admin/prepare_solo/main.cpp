#include <ydb/core/blobstorage/base/data_center_states.h>
#include <ydb/core/blobstorage/nodewarden/distconf.h>
#include <ydb/core/config/validation/validators.h>
#include <ydb/core/protos/blobstorage_distributed_config.pb.h>

#include <google/protobuf/util/json_util.h>
#include <util/generic/algorithm.h>
#include <util/stream/file.h>
#include <util/stream/output.h>

namespace NKikimr {

static void SetReplicas(NKikimrConfig::TStateStorageConfig& config, const THashSet<ui32>& nodes, const char* name) {
    Y_ENSURE(config.HasRing() ? !config.RingGroupsSize() : config.RingGroupsSize() == 1, name << ": expected one ring group");
    auto* group = config.HasRing() ? config.MutableRing() : config.MutableRingGroups(0);
    Y_ENSURE(!group->NodeSize() && !group->HasBridgePileId(), name << ": expected ordinary replica rings");
    const size_t originalRingCount = group->RingSize();
    THashSet<ui32> kept;
    for (int i = group->RingSize(); i-- > 0;) {
        const auto& ring = group->GetRing(i);
        Y_ENSURE(ring.NodeSize() == 1 && !ring.GetIsDisabled(), name << ": expected one active node per ring");
        const ui32 node = ring.GetNode(0);
        if (nodes.contains(node)) {
            Y_ENSURE(kept.insert(node).second, name << ": duplicate replica node");
        } else {
            group->MutableRing()->DeleteSubrange(i, 1);
        }
    }
    if (kept == nodes
            && group->GetNToSelect() == nodes.size() && originalRingCount == group->RingSize()) {
        return;
    }
    TVector<ui32> missing;
    for (ui32 node : nodes) {
        if (!kept.contains(node)) {
            missing.push_back(node);
        }
    }
    Sort(missing);
    for (ui32 node : missing) {
        group->AddRing()->AddNode(node);
    }
    group->SetNToSelect(nodes.size());
    Y_ENSURE(config.GetStateStorageVersion() < Max<ui32>(), name << ": version overflow");
    config.SetStateStorageVersion(config.GetStateStorageVersion() + 1);
    config.ClearCompatibleVersions();
    const TString error = NConfig::ValidateStateStorageConfig(name, {}, config);
    Y_ENSURE(error.empty(), error);
}

static void Prepare(const TString& mode, const TString& path, int argc, char** argv) {
    using TKeeper = NStorage::TDistributedConfigKeeper;
    NKikimrBlobStorage::TPDiskMetadataRecord record;
    const auto parsed = google::protobuf::util::JsonStringToMessage(
        std::string(TUnbufferedFileInput(path).ReadAll()), &record);
    Y_ENSURE(parsed.ok() && record.HasCommittedStorageConfig(), "Invalid committed metadata");
    const auto before = record.GetCommittedStorageConfig();
    Y_ENSURE(TKeeper::CheckFingerprint(before), "Invalid committed fingerprint");
    if (const auto error = NStorage::ValidateConfig(before)) {
        ythrow yexception() << *error;
    }
    auto* next = record.MutableCommittedStorageConfig();
    if (mode == "group") {
        Y_ENSURE(argc == 5, "Expected excluded and surviving DCs");
        const TString excludeDC(argv[3]), survivor(argv[4]);
        const auto& groups = before.GetBlobStorageConfig().GetServiceSet().GetGroups();
        using TState = NKikimrBlobStorage::TGroupState;
        Y_ENSURE(groups.size() == 1 && groups[0].GetGroupID() == 0, "Expected static group 0");
        const auto state = groups[0].GetMirror3dcState();
        Y_ENSURE(state == TState::DC_FAILURE_INV1 || state == TState::DC_FAILURE_INV2,
            "Expected a confirmed DC_FAILURE_INV1 or DC_FAILURE_INV2 state");
        const auto soloState = state == TState::DC_FAILURE_INV1 ? TState::DC_SOLO_INV1 : TState::DC_SOLO_INV2;
        auto states = ParseDataCenterStates(before.GetDataCenterStates());
        Y_ENSURE(states, states.error());
        (*states)[excludeDC] = NKikimrBlobStorage::TDataCenterState::EXCLUDED;
        Y_ENSURE(NStorage::ApplyStaticDataCenterStates(next, SerializeDataCenterStates(*states))
            && next->GetBlobStorageConfig().GetServiceSet().GetGroups(0).GetMirror3dcState()
                == soloState, "Expected Solo DC at the confirmed redundancy level");
        THashSet<ui32> nodes;
        for (const auto& node : next->GetAllNodes()) {
            const TString& dc = node.GetLocation().GetDataCenter();
            const auto it = states->find(dc);
            const bool excluded = it != states->end() && it->second == NKikimrBlobStorage::TDataCenterState::EXCLUDED;
            Y_ENSURE(excluded == (dc != survivor), "Unexpected DC policy for " << dc);
            if (dc == survivor) {
                nodes.insert(node.GetNodeId());
            }
        }
        Y_ENSURE(nodes.size() == 3, "Expected three static nodes in surviving DC");
    } else {
        Y_ENSURE(mode == "state-storage", "Unknown operation");
        THashSet<TString> selected(argv + 3, argv + argc);
        THashSet<ui32> nodes;
        for (const auto& node : before.GetAllNodes()) {
            if (selected.contains(node.GetLocation().GetDataCenter())) {
                nodes.insert(node.GetNodeId());
            }
        }
        Y_ENSURE(nodes.size() == 3 * selected.size(), "Expected three nodes in each selected DC");
        SetReplicas(*next->MutableStateStorageConfig(), nodes, "StateStorage");
        SetReplicas(*next->MutableStateStorageBoardConfig(), nodes, "StateStorageBoard");
        SetReplicas(*next->MutableSchemeBoardConfig(), nodes, "SchemeBoard");
        if (next->SerializeAsString() == before.SerializeAsString()) {
            Cout << TUnbufferedFileInput(path).ReadAll();
            return;
        }
    }
    Y_ENSURE(before.GetGeneration() < Max<ui64>(), "Storage generation overflow");
    next->SetGeneration(before.GetGeneration() + 1);
    next->MutablePrevConfig()->CopyFrom(before);
    next->MutablePrevConfig()->ClearPrevConfig();
    TKeeper::UpdateFingerprint(next);
    if (const auto error = NStorage::ValidateConfigUpdate(before, *next)) {
        ythrow yexception() << *error;
    }
    record.ClearProposedStorageConfig();
    google::protobuf::util::JsonPrintOptions opts;
    opts.preserve_proto_field_names = true;
    TString json;
    const auto status = google::protobuf::util::MessageToJsonString(record, &json, opts);
    Y_ENSURE(status.ok(), "Failed to serialize prepared metadata");
    Cout << json << Endl;
}

} // namespace NKikimr

int main(int argc, char** argv) {
    if (argc < 4 || argc > 6) {
        Cerr << "Usage: dc-admin-prepare-solo <group|state-storage> <metadata.json> <DCs...>" << Endl;
        return 1;
    }
    try {
        NKikimr::Prepare(argv[1], argv[2], argc, argv);
        return 0;
    } catch (const std::exception& e) {
        Cerr << e.what() << Endl;
        return 1;
    }
}

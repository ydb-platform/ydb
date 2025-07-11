#include "statestorage.h"
#include "tabletid.h"
#include <ydb/core/blobstorage/base/utility.h>
#include <util/generic/xrange.h>
#include <util/generic/mem_copy.h>
#include <util/generic/algorithm.h>
#include "statestorage_ringwalker.h"

namespace NKikimr {

constexpr ui64 MaxRingCount = 1024;
constexpr ui64 MaxNodeCount = 1024;

TString TEvStateStorage::TSignature::ToString() const {
    TStringStream str;
    str << "{ Size: " << Size();
    if (!ReplicasSignature.empty()) {
        str << " Signature: {";
        ui32 i = 0;
        for (auto& [id, sig] : ReplicasSignature) {
            if(i++ > 0)
                str << ", ";
            str << "{" << id.ToString() <<" : " << sig << "}";
        }
        str << "}";
    }
    str << "}";
    return str.Str();
}

ui32 TEvStateStorage::TSignature::Size() const {
    return ReplicasSignature.size();
}

void TEvStateStorage::TSignature::Merge(const TEvStateStorage::TSignature& signature) {
    for(auto [replicaId, sig] : signature.ReplicasSignature) {
        SetReplicaSignature(replicaId, sig);
    }
}

bool TEvStateStorage::TSignature::HasReplicaSignature(const TActorId &replicaId) const {
    return ReplicasSignature.contains(replicaId);
}

ui64 TEvStateStorage::TSignature::GetReplicaSignature(const TActorId &replicaId) const {
    auto it = ReplicasSignature.find(replicaId);
    return it == ReplicasSignature.end() ? 0 : it->second;
}

void TEvStateStorage::TSignature::SetReplicaSignature(const TActorId &replicaId, ui64 signature) {
    ReplicasSignature[replicaId] = signature;
}

void TStateStorageInfo::SelectReplicas(ui64 tabletId, TSelection *selection, ui32 ringGroupIdx) const {
    const ui32 hash = StateStorageHashFromTabletID(tabletId);

    Y_ABORT_UNLESS(ringGroupIdx < RingGroups.size());

    auto& ringGroup = RingGroups[ringGroupIdx];
    const ui32 total = ringGroup.Rings.size();

    Y_ABORT_UNLESS(ringGroup.NToSelect <= total);

    if (selection->Sz < ringGroup.NToSelect) {
        selection->Status.Reset(new TStateStorageInfo::TSelection::EStatus[ringGroup.NToSelect]);
        selection->SelectedReplicas.Reset(new TActorId[ringGroup.NToSelect]);
    }

    selection->Sz = ringGroup.NToSelect;

    Fill(selection->Status.Get(), selection->Status.Get() + ringGroup.NToSelect, TStateStorageInfo::TSelection::StatusUnknown);

    if (ringGroup.NToSelect == total) {
        for (ui32 idx : xrange(total)) {
            selection->SelectedReplicas[idx] = ringGroup.Rings[idx].SelectReplica(hash);
        }
    } else { // NToSelect < total, first - select rings with walker, then select concrete node
        TStateStorageRingWalker walker(hash, total);
        for (ui32 idx : xrange(ringGroup.NToSelect))
            selection->SelectedReplicas[idx] = ringGroup.Rings[walker.Next()].SelectReplica(hash);
    }
}

TActorId TStateStorageInfo::TRing::SelectReplica(ui32 hash) const {
    if (Replicas.size() == 1)
        return Replicas[0];

    Y_ABORT_UNLESS(!Replicas.empty());
    if (UseRingSpecificNodeSelection) {
        return Replicas[CombineHashes(hash, ContentHash()) % Replicas.size()];
    } else {
        return Replicas[hash % Replicas.size()];
    }
}

TList<TActorId> TStateStorageInfo::SelectAllReplicas() const {
// TODO: we really need this method in such way?
    TList<TActorId> replicas;
    for (auto &ringGroup : RingGroups) {
        for (auto &ring : ringGroup.Rings) {
            for (TActorId replica : ring.Replicas)
                replicas.push_back(replica);
        }
    }

    return replicas;
}

ui32 TStateStorageInfo::RingGroupsSelectionSize() const {
    ui32 res = 0;
    for (auto &ringGroup : RingGroups)
        res += ringGroup.NToSelect;
    return res;
}

ui32 TStateStorageInfo::TRing::ContentHash() const {
    ui64 hash = 17;
    for (TActorId replica : Replicas) {
        hash = Hash64to32((hash << 32) | replica.Hash32());
    }
    return static_cast<ui32>(hash);
}

ui32 TStateStorageInfo::ContentHash() const {
    ui64 hash = RelaxedLoad<ui64>(&Hash);
    if (Y_UNLIKELY(hash == Max<ui64>())) {
        hash = 37;
        for (auto &ringGroup : RingGroups) {
            for (const TRing &ring : ringGroup.Rings) {
                hash = Hash64to32((hash << 32) | ring.ContentHash());
            }
        }
        RelaxedStore<ui64>(&Hash, static_cast<ui32>(hash));
    }
    return static_cast<ui32>(hash);
}

TString TStateStorageInfo::TRingGroup::ToString() const {
    TStringStream s;
    s << "{";
    s << "NToSelect# " << NToSelect;
    s << " Rings# [";
    for (size_t ring = 0; ring < Rings.size(); ++ring) {
        if (ring) {
            s << ' ';
        }
        s << ring << ":{";
        const auto& r = Rings[ring];
        s << FormatList(r.Replicas);
        if (r.IsDisabled) {
            s << " Disabled";
        }
        if (r.UseRingSpecificNodeSelection) {
            s << " UseRingSpecificNodeSelection";
        }
        s << '}';
    }
    s << '}';
    return s.Str();
}

TString TStateStorageInfo::ToString() const {
    TStringStream s;
    s << '{';
    s << "RingGroups# [";
    for (size_t ringGroupIdx = 0; ringGroupIdx < RingGroups.size(); ++ringGroupIdx) {
        auto& ringGroup = RingGroups[ringGroupIdx];
        if (ringGroupIdx) {
            s << ' ';
        }
        s << ringGroupIdx << ":" << ringGroup.ToString();
    }
    s << "] StateStorageVersion# " << StateStorageVersion;
    s << " CompatibleVersions# " << FormatList(CompatibleVersions);
    s << " ClusterStateGeneration# " << ClusterStateGeneration;
    s << " ClusterStateGuid# " << ClusterStateGuid;
    s << '}';
    return s.Str();
}

void TStateStorageInfo::TSelection::MergeReply(EStatus status, EStatus *owner, ui64 targetCookie, bool resetOld) {
    ui32 unknown = 0;
    ui32 ok = 0;
    ui32 outdated = 0;
    ui32 unavailable = 0;

    const ui32 majority = Sz / 2 + 1;

    ui32 cookie = 0;
    for (ui32 i = 0; i < Sz; ++i) {
        EStatus &st = Status[i];
        if (resetOld && st != StatusUnknown && st != StatusUnavailable)
            st = StatusOutdated;

        if (cookie == targetCookie)
            st = status;

        ++cookie;

        switch (st) {
        case StatusUnknown:
            ++unknown;
            break;
        case StatusOk:
            ++ok;
            break;
        case StatusNoInfo:
            break;
        case StatusOutdated:
            ++outdated;
            break;
        case StatusUnavailable:
            ++unavailable;
            break;
        }
    }

    if (owner) {
        if (ok >= majority) {
            *owner = StatusOk;
        } else if (ok + unknown < majority) {
            if (unavailable > (Sz - majority))
                *owner = StatusUnavailable;
            else if (outdated)
                *owner = StatusOutdated;
            else
                *owner = StatusNoInfo;
        }
    }
}

bool TStateStorageInfo::TRingGroup::SameConfiguration(const TStateStorageInfo::TRingGroup& rg) {
    return NToSelect == rg.NToSelect && Rings == rg.Rings && State == rg.State;
}

bool operator==(const TStateStorageInfo::TRing& lhs, const TStateStorageInfo::TRing& rhs) {
    return lhs.IsDisabled == rhs.IsDisabled && lhs.UseRingSpecificNodeSelection == rhs.UseRingSpecificNodeSelection && lhs.Replicas == rhs.Replicas;
}

bool operator==(const TStateStorageInfo::TRingGroup& lhs, const TStateStorageInfo::TRingGroup& rhs) {
    return lhs.WriteOnly == rhs.WriteOnly && lhs.NToSelect == rhs.NToSelect && lhs.Rings == rhs.Rings && lhs.State == rhs.State;
}

bool operator!=(const TStateStorageInfo::TRing& lhs, const TStateStorageInfo::TRing& rhs) {
    return !operator==(lhs, rhs);
}

bool operator!=(const TStateStorageInfo::TRingGroup& lhs, const TStateStorageInfo::TRingGroup& rhs) {
    return !operator==(lhs, rhs);
}

static void CopyStateStorageRingInfo(
    const NKikimrConfig::TDomainsConfig::TStateStorage::TRing &source,
    TStateStorageInfo::TRingGroup& ringGroup,
    char *serviceId,
    ui32 depth,
    ui32 ringGroupActorIdOffset
) {

    const bool hasRings = source.RingSize() > 0;
    const bool hasNodes = source.NodeSize() > 0;

    if (hasRings) { // has explicitely defined rings, use them as info rings
        Y_ABORT_UNLESS(!hasNodes);
        Y_ABORT_UNLESS(source.RingSize() < MaxRingCount);
        ringGroup.Rings.resize(source.RingSize());

        for (ui32 iring = 0, ering = source.RingSize(); iring != ering; ++iring) {
            serviceId[depth] = (iring + 1);

            const NKikimrConfig::TDomainsConfig::TStateStorage::TRing &ring = source.GetRing(iring);
            ringGroup.Rings[iring].UseRingSpecificNodeSelection = ring.GetUseRingSpecificNodeSelection();
            ringGroup.Rings[iring].IsDisabled = ring.GetIsDisabled();

            if (ring.GetUseSingleNodeActorId()) {
                Y_ABORT_UNLESS(ring.NodeSize() == 1);
                serviceId[depth + 1] = ringGroupActorIdOffset;
                const TActorId replicaActorID = TActorId(ring.GetNode(0), TStringBuf(serviceId, serviceId + 12));
                ringGroup.Rings[iring].Replicas.push_back(replicaActorID);
            }
            else {
               Y_ABORT_UNLESS(ring.NodeSize() > 0);

               for (ui32 inode = 0, enode = ring.NodeSize(); inode != enode; ++inode) {
                    serviceId[depth + 1] = (ringGroupActorIdOffset + inode + 1);
                    const TActorId replicaActorID = TActorId(ring.GetNode(inode), TStringBuf(serviceId, serviceId + 12));
                    ringGroup.Rings[iring].Replicas.push_back(replicaActorID);
               }
            }
            // reset for next ring
            serviceId[depth + 1] = char();
        }
        // reset for next ring group
        serviceId[depth] = char();
        return;
    }

    if (hasNodes) { // has explicitely defined replicas, use nodes as 1-node rings
        Y_ABORT_UNLESS(!hasRings);
        Y_ABORT_UNLESS(source.NodeSize() < MaxNodeCount);

        ringGroup.Rings.resize(source.NodeSize());
        for (ui32 inode = 0, enode = source.NodeSize(); inode != enode; ++inode) {
            serviceId[depth] = (ringGroupActorIdOffset + inode + 1);

            const TActorId replicaActorID = TActorId(source.GetNode(inode), TStringBuf(serviceId, serviceId + 12));
            ringGroup.Rings[inode].Replicas.push_back(replicaActorID);
        }
        // reset for next ring group
        serviceId[depth] = char();
        return;
    }

    Y_ABORT("must have rings or legacy node config");
}

ERingGroupState GetRingGroupState(const NKikimrConfig::TDomainsConfig::TStateStorage::TRing &ringGroup) {
    if (!ringGroup.HasPileState()) {
        return ERingGroupState::PRIMARY;
    }
    switch (ringGroup.GetPileState()) {
        case NKikimrConfig::TDomainsConfig::TStateStorage::PRIMARY:
        case NKikimrConfig::TDomainsConfig::TStateStorage::PROMOTED:
            return ERingGroupState::PRIMARY;
        case NKikimrConfig::TDomainsConfig::TStateStorage::SYNCHRONIZED:
        case NKikimrConfig::TDomainsConfig::TStateStorage::DEMOTED:
            return ERingGroupState::SYNCHRONIZED;
        case NKikimrConfig::TDomainsConfig::TStateStorage::NOT_SYNCHRONIZED:
            return ERingGroupState::NOT_SYNCHRONIZED;
        case NKikimrConfig::TDomainsConfig::TStateStorage::DISCONNECTED:
            return ERingGroupState::DISCONNECTED;
        default:
            Y_ABORT("Unsupported ring group pile state");
    }
}
TIntrusivePtr<TStateStorageInfo> BuildStateStorageInfoImpl(const char* namePrefix,
        const NKikimrConfig::TDomainsConfig::TStateStorage& config) {
    char name[TActorId::MaxServiceIDLength];
    strcpy(name, namePrefix);
    TIntrusivePtr<TStateStorageInfo> info = new TStateStorageInfo();
    info->ClusterStateGeneration = config.GetClusterStateGeneration();
    info->ClusterStateGuid = config.GetClusterStateGuid();
    Y_ABORT_UNLESS(config.GetSSId() == 1);
    Y_ABORT_UNLESS(config.HasRing() != (config.RingGroupsSize() > 0));
    info->StateStorageVersion = config.GetStateStorageVersion();

    info->CompatibleVersions.reserve(config.CompatibleVersionsSize());
    for (ui32 version : config.GetCompatibleVersions()) {
        info->CompatibleVersions.push_back(version);
    }

    const size_t offset = FindIndex(name, char()) + sizeof(ui32);
    Y_ABORT_UNLESS(offset != NPOS && (offset) < TActorId::MaxServiceIDLength);
    const ui32 stateStorageGroup = 1;
    memcpy(name + offset - sizeof(ui32), reinterpret_cast<const char *>(&stateStorageGroup), sizeof(ui32));
    memset(name + offset, 0, TActorId::MaxServiceIDLength - offset);
    for (size_t i = 0; i < config.RingGroupsSize(); i++) {
        auto& ringGroup = config.GetRingGroups(i);
        info->RingGroups.push_back({GetRingGroupState(ringGroup), ringGroup.GetWriteOnly(), ringGroup.GetNToSelect(), {}});
        CopyStateStorageRingInfo(ringGroup, info->RingGroups.back(), name, offset, ringGroup.GetRingGroupActorIdOffset());
        memset(name + offset, 0, TActorId::MaxServiceIDLength - offset);
    }
    if (config.HasRing()) {
        auto& ring = config.GetRing();
        info->RingGroups.push_back({ERingGroupState::PRIMARY, false, ring.GetNToSelect(), {}});
        CopyStateStorageRingInfo(ring, info->RingGroups.back(), name, offset, ring.GetRingGroupActorIdOffset());
    }
    return info;
}

TIntrusivePtr<TStateStorageInfo> BuildStateStorageInfo(const NKikimrConfig::TDomainsConfig::TStateStorage& config) {
    return BuildStateStorageInfoImpl("ssr", config);
}

TIntrusivePtr<TStateStorageInfo> BuildStateStorageBoardInfo(const NKikimrConfig::TDomainsConfig::TStateStorage& config) {
    return BuildStateStorageInfoImpl("ssb", config);
}

TIntrusivePtr<TStateStorageInfo> BuildSchemeBoardInfo(const NKikimrConfig::TDomainsConfig::TStateStorage& config) {
    return BuildStateStorageInfoImpl("sbr", config);
}

void BuildStateStorageInfos(const NKikimrConfig::TDomainsConfig::TStateStorage& config,
    TIntrusivePtr<TStateStorageInfo> &stateStorageInfo,
    TIntrusivePtr<TStateStorageInfo> &boardInfo,
    TIntrusivePtr<TStateStorageInfo> &schemeBoardInfo)
{
    stateStorageInfo = BuildStateStorageInfo(config);
    boardInfo = BuildStateStorageBoardInfo(config);
    schemeBoardInfo = BuildSchemeBoardInfo(config);
}

}

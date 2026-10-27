#pragma once

#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_mirror3dc_state.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_sets.h>

namespace NKikimr::NSyncer {

// Blob copy guarantees do not constrain where GUID witnesses were published.
// Collect every normal source before declaring the recovered history complete.
inline bool HasAllNormalGuidSources(const TBlobStorageGroupInfo& info,
        const TBlobStorageGroupInfo::TGroupVDisks& sources) {
    using TFailRealm = NKikimrBlobStorage::TGroupInfo::TFailRealm;
    const auto& topology = info.GetTopology();
    if (info.Type.GetErasure() != TBlobStorageGroupType::ErasureMirror3dc
            || topology.GetTotalFailRealmsNum() != 3 || sources.GetTopology() != &topology) {
        return false;
    }
    if (!GetMirror3dcGroupState(info.GetFailRealmState(0), info.GetFailRealmState(1), info.GetFailRealmState(2))) {
        return false;
    }
    bool hasNormalSource = false;
    for (ui32 realm = 0; realm < 3; ++realm) {
        const auto state = info.GetFailRealmState(realm);
        if (state == TFailRealm::F || state == TFailRealm::R) {
            continue;
        }
        hasNormalSource = true;
        for (const auto& domain : topology.FailRealms[realm].FailDomains) {
            for (const auto& disk : domain.VDisks) {
                if (!sources[disk.OrderNumber]) {
                    return false;
                }
            }
        }
    }
    return hasNormalSource;
}

// One Final observation per owner is sufficient to remember that the owner existed.
// Keep its original witness when forwarding; this is not a fresh quorum vote.
inline bool MergeGuidWitness(const TBlobStorageGroupInfo& info,
        const NKikimrBlobStorage::TSyncGuidWitness& witness,
        NKikimrBlobStorage::TSyncGuidHistory* history) {
    if (!witness.HasOwnerVDiskID() || !witness.HasWitnessVDiskID() || !witness.GetGuid()) {
        return false;
    }
    const auto owner = VDiskIDFromVDiskID(witness.GetOwnerVDiskID());
    const auto observer = VDiskIDFromVDiskID(witness.GetWitnessVDiskID());
    auto belongsToGroup = [&info](const TVDiskID& disk) {
        if (disk.GroupID != info.GroupID || disk.GroupGeneration > info.GroupGeneration) {
            return false;
        }
        for (ui32 index = 0; index < info.GetTotalVDisksNum(); ++index) {
            if (TVDiskIdShort(disk) == TVDiskIdShort(info.GetVDiskId(index))) {
                return true;
            }
        }
        return false;
    };
    if (!belongsToGroup(owner) || !belongsToGroup(observer)) {
        return false;
    }
    for (const auto& saved : history->GetWitnesses()) {
        if (TVDiskIdShort(VDiskIDFromVDiskID(saved.GetOwnerVDiskID())) == TVDiskIdShort(owner)) {
            return saved.GetGuid() == witness.GetGuid();
        }
    }
    history->AddWitnesses()->CopyFrom(witness);
    return true;
}

inline NKikimrBlobStorage::TSyncGuidWitness MakeGuidWitness(const TVDiskID& owner, ui64 guid,
        const TVDiskID& observer) {
    NKikimrBlobStorage::TSyncGuidWitness witness;
    VDiskIDFromVDiskID(owner, witness.MutableOwnerVDiskID());
    witness.SetGuid(guid);
    VDiskIDFromVDiskID(observer, witness.MutableWitnessVDiskID());
    return witness;
}

} // namespace NKikimr::NSyncer

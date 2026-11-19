#pragma once

#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_mirror3dc_state.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_mirror3dc_write_quorum.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_sets.h>

namespace NKikimr::NSyncer {

// History must intersect every possible Final publication quorum. Requiring a
// publication quorum of readers would unnecessarily reduce the failure budget.
// This preserves quorum-backed history across modern recoveries; it cannot
// reconstruct witnesses already lost before history-capable recovery was enabled.
inline bool HasGuidHistoryCoverage(const TBlobStorageGroupInfo& info,
        TBlobStorageGroupInfo::TGroupVDisks sources, const TVDiskIdShort& self, bool hasLocalFinal) {
    if (!hasLocalFinal) {
        sources &= ~TBlobStorageGroupInfo::TGroupVDisks(&info.GetTopology(), self);
    }
    if (info.Type.GetErasure() == TBlobStorageGroupType::ErasureMirror3dc
            && info.GetTopology().GetTotalFailRealmsNum() == 3) {
        return !TMirror3dcWriteQuorumChecker::CheckQuorumForFullSync(info, ~sources);
    }
    return !info.GetTopology().GetQuorumChecker().CheckQuorumForGroup(~sources);
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

#include "blobstorage_anubis_algo.h"
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_sets.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TBlobsStatusMngr
    ////////////////////////////////////////////////////////////////////////////
    TBlobsStatusMngr::TBlobsStatusMngr(const TBlobStorageGroupInfo::TTopology *top)
        : Top(top)
    {}

    void TBlobsStatusMngr::SetupCandidates(TVector<TLogoBlobID> &&candidates) {
        Candidates = std::move(candidates);
    }

    TVector<TLogoBlobID> TBlobsStatusMngr::GetLogoBlobIdsToCheck(const TVDiskIdShort &vd) const {
        TVector<TLogoBlobID> checkIds;
        // find out candidates that belong to this 'vd'
        for (const auto &c : Candidates) {
            if (Top->BelongsToSubgroup(vd, c.Hash())) {
                checkIds.push_back(c);
            }
        }
        return checkIds;
    }

    void TBlobsStatusMngr::UpdateStatusForVDisk(const TVDiskIdShort &vd,
                                                const NKikimrBlobStorage::TEvVGetResult &record) {
        const TVDiskID vdiskId = VDiskIDFromVDiskID(record.GetVDiskID());
        Y_ABORT_UNLESS(TVDiskIdShort(vdiskId) == vd);

        if (record.GetStatus() == NKikimrProto::OK) {
            // empty subGroup
            TBlobStorageGroupInfo::TSubgroupVDisks emptySubGroup(Top);

            const auto size = record.ResultSize();
            // process every result we got
            for (ui32 i = 0; i < size; ++i) {
                const auto &res = record.GetResult(i);
                if (res.GetStatus() == NKikimrProto::NODATA) {
                    // if we got NODATA, than we are sure, that the given VDisk
                    // definitely does not contain the LogoBlobID, we mark this fact
                    // in corresponding TSubgroupVDisks structure
                    const TLogoBlobID id = LogoBlobIDFromLogoBlobID(res.GetBlobID());
                    const auto idx = Top->GetIdxInSubgroup(vd, id.Hash());

                    auto p = BlobsStatus.emplace(id, emptySubGroup);
                    auto &it = p.first;
                    it->second |= TBlobStorageGroupInfo::TSubgroupVDisks(Top, idx);
                }
            }
        }
    }

    // returns LogoBlobIDs to remove
    TVector<TLogoBlobID> TBlobsStatusMngr::BlobsToRemove() const {
        // get a reference to quorum checker helper class instance
        const auto& checker = Top->GetQuorumChecker();
        // filling in this vector
        TVector<TLogoBlobID> forRemoval;
        for (const auto &x : BlobsStatus) {
            const TBlobStorageGroupInfo::TSubgroupVDisks &subgroupDisks = x.second;
            const TBlobStorageGroupInfo::TSubgroupVDisks noneNodata = ~subgroupDisks;
            bool haveQuorum = checker.CheckQuorumForSubgroup(noneNodata);
            if (!haveQuorum) {
                // e don't have quorum of 'POSSIBLY HAVE' (Ok, Error, etc) answers;
                // it means we can't have this blob! Remove it.
                const TLogoBlobID id = x.first;
                forRemoval.push_back(id);
            }
        }
        return forRemoval;
    }

} // NKikimr

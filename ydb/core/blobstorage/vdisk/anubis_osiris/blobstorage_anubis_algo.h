#pragma once

#include "defs.h"
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TBlobsToRemove
    ////////////////////////////////////////////////////////////////////////////
    class TBlobsToRemove {
    public:
        TBlobsToRemove() = default;
        TBlobsToRemove(const TBlobsToRemove &) = default;
        TBlobsToRemove &operator =(const TBlobsToRemove &) = default;
        TBlobsToRemove(TBlobsToRemove &&) = default;
        TBlobsToRemove &operator =(TBlobsToRemove &&) = default;

        TBlobsToRemove(TVector<TLogoBlobID> &&blobs)
            : Blobs(std::move(blobs))
            , It(Blobs.end())
        {}

        void SeekToFirst() {
            It = Blobs.begin();
        }

        bool Valid() const {
            return It != Blobs.end();
        }

        void Next() {
            ++It;
        }

        const TLogoBlobID &Get() const {
            return *It;
        }

    private:
        TVector<TLogoBlobID> Blobs;
        TVector<TLogoBlobID>::const_iterator It;
    };

    ////////////////////////////////////////////////////////////////////////////
    // TBlobsStatusMngr
    ////////////////////////////////////////////////////////////////////////////
    class TBlobsStatusMngr {
    public:
        TBlobsStatusMngr(const TBlobStorageGroupInfo::TTopology *top);
        void SetupCandidates(TVector<TLogoBlobID> &&candidates);
        TVector<TLogoBlobID> GetLogoBlobIdsToCheck(const TVDiskIdShort &vd) const;
        void UpdateStatusForVDisk(const TVDiskIdShort &vd,
                                  const NKikimrBlobStorage::TEvVGetResult &record);
        // returns LogoBlobIDs to remove
        TVector<TLogoBlobID> BlobsToRemove() const;

    private:
        const TBlobStorageGroupInfo::TTopology *Top = nullptr;
        // LogoBlobIDs we got from Finder; we check these ones for removing
        TVector<TLogoBlobID> Candidates;
        // This structure maps every TLogoBlobID from Candidates field to
        // TSubgroupVDisks object. TSubgroupVDisks contains bit mask of
        // VDisks that definitely have NODATA for this TLogoBlobID. We set
        // this object via communication with peers.
        THashMap<TLogoBlobID, TBlobStorageGroupInfo::TSubgroupVDisks> BlobsStatus;
    };

} // NKikimr

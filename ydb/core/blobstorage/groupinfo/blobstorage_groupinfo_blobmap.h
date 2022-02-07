#pragma once

#include "defs.h"
#include "blobstorage_groupinfo.h"

namespace NKikimr {

    // disk to blob mapper -- a special class that controls distribution of blobs over VDisks in their group
    struct IBlobToDiskMapper {
        virtual ~IBlobToDiskMapper() = default;

        // function selects where to put disk blob; hash = TLogoBlobID::Hash() for a specific blob, fromRing is a ring
        // number of entity calling this function; vdisks and services array are filled (if not nullptr) with VDisk
        // identifier and service actor ids; output arrays must be empty on entry and they are filled with BlobSubgroupSize
        // items on exit
        virtual void PickSubgroup(ui32 hash, TBlobStorageGroupInfo::TOrderNums &orderNums) = 0;

        // function checks if a blob with provided hash is a replica for specific vdisk
        virtual bool BelongsToSubgroup(const TVDiskIdShort& vdisk, ui32 hash) = 0;

        // function returns "vdisk" index into "vdisks" array of select replicas, i.e. it is equivalent to
        //
        // TVDiskIds ids;
        // PickSubgroup(hash, vdisk.Ring, &vdisks, nullptr);
        // return std::find(ids.begin(), ids.end(), vdisk) - ids.begin();
        //
        // It returns BlobSubgroupSize if this vdisk is not designated for provided blob
        virtual ui32 GetIdxInSubgroup(const TVDiskIdShort& vdisk, ui32 hash) = 0;

        // function returns idxInSubgroup-th element of vdisks array from PickSubgroup
        virtual TVDiskIdShort GetVDiskInSubgroup(ui32 idxInSubgroup, ui32 hash) = 0;

        static IBlobToDiskMapper *CreateBasicMapper(const TBlobStorageGroupInfo::TTopology *topology);
        static IBlobToDiskMapper *CreateMirror3dcMapper(const TBlobStorageGroupInfo::TTopology *topology);
    };

} // NKikimr

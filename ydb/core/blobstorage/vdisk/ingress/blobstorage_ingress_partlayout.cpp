#include "blobstorage_ingress.h"

#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_partlayout.h>

namespace NKikimr {

    // These two TSubgroupPartLayout factories are declared in
    // ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_partlayout.h, but defined here
    // rather than in `groupinfo`: they are the only place where groupinfo would need TIngress,
    // which belongs to this (higher) layer. Defining them in `groupinfo` would make it depend
    // back on `vdisk/ingress` and close a dependency cycle.

    ui32 TSubgroupPartLayout::CountEffectiveReplicas(TIngress ingress, TBlobStorageGroupType gtype) {
        return CreateFromIngress(ingress, gtype).CountEffectiveReplicas(gtype);
    }

    TSubgroupPartLayout TSubgroupPartLayout::CreateFromIngress(TIngress ingress, const TBlobStorageGroupType &gtype) {
        TSubgroupPartLayout res;
        const ui8 subgroupSize = gtype.BlobSubgroupSize();
        for (ui8 i = 0; i < subgroupSize; ++i) {
            const NMatrix::TVectorType parts = ingress.KnownParts(gtype, i);
            for (ui8 j = parts.FirstPosition(); j != parts.GetSize(); j = parts.NextPosition(j)) {
                res.AddItem(i, j, gtype);
            }
        }
        return res;
    }

} // NKikimr

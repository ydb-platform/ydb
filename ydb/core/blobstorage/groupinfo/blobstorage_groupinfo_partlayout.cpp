#include "blobstorage_groupinfo_partlayout.h"
#include "blobstorage_groupinfo_sets.h"
#include <ydb/core/blobstorage/vdisk/ingress/blobstorage_ingress.h>

namespace NKikimr {

    template<typename Iterator>
    ui32 CountEffectiveReplicasGeneric(Iterator begin, Iterator end, ui32 usedDiskMask = 0) {
        if (begin == end) {
            return PopCount(usedDiskMask);
        }
        ui32 value = *begin++ & ~usedDiskMask;
        ui32 res = CountEffectiveReplicasGeneric(begin, end, usedDiskMask); // try skipping this disk
        while (value) {
            const ui32 leastDiskMask = 1 << CountTrailingZeroBits(value); // obtain the least set bit as a bitmask
            res = Max(res, CountEffectiveReplicasGeneric(begin, end, usedDiskMask | leastDiskMask)); // try this disk for current part
            value &= ~leastDiskMask;
        }
        return res;
    }

    ui32 TSubgroupPartLayout::CountEffectiveReplicas(const TBlobStorageGroupType &gtype) const {
        switch (gtype.GetErasure()) {
            case TBlobStorageGroupType::ErasureMirror3dc:
            case TBlobStorageGroupType::ErasureMirror3of4:
                Y_ABORT("inapplicable operation for erasure %s", TBlobStorageGroupType::ErasureSpeciesName(gtype.GetErasure()).data());
            default: {
                /*******************************************************************************************************
                 * PerPartStatus is a matrix with bits indicating if the some part is present at specific disk; it looks
                 * like:
                 *
                 * M 0 0 0 0 0 H1 H2
                 * 0 M 0 0 0 0 H1 H2
                 * 0 0 M 0 0 0 H1 H2
                 * 0 0 0 M 0 0 H1 H2
                 * 0 0 0 0 M 0 H1 H2
                 * 0 0 0 0 0 M H1 H2
                 *
                 * where M is the main disk for each part idx and the H[i] is the i-th handoff disk
                 */
                const ui32 totalPartCount = gtype.TotalPartCount();
                const ui32 mainMask = (1 << totalPartCount) - 1;

                // calculate mask of main parts in place
                ui32 total = 0;
                for (ui32 i = 0; i < totalPartCount; ++i) {
                    total |= GetDisksWithPart(i);
                }
                total &= mainMask;

                // count them as effective replicas
                ui32 res = PopCount(total);

                if (total != mainMask) {
                    // here we filter out handoff disks containing at least one of required parts (which are indicated
                    // by bits in total); each row in 'handoffs' table contains bitmask of handoff disks containing
                    // specific part
                    TStackVec<ui32, MaxTotalPartCount> handoffDiskByPart;
                    for (ui32 i = 0; i < totalPartCount; ++i) {
                        const ui32 value = GetDisksWithPart(i) >> totalPartCount;
                        if (~total & 1 << i && value) { // if there is no such part at main disk
                            handoffDiskByPart.push_back(value);
                        }
                    }
                    switch (handoffDiskByPart.size()) {
                        case 0: // no handoff disks contain required parts
                            break;

                        case 1: // only one handoff disks contains at least one part -- we can use it
                            ++res;
                            break;

                        case 2: { // two handoff disks contain parts
                            const ui32 a = handoffDiskByPart[0], b = handoffDiskByPart[1];
                            res += Min(2U, PopCount(a | b)); // if both disks contain the same part, we can use only one disk
                            break;
                        }

                        default: // use generic algorithm
                            res += CountEffectiveReplicasGeneric(handoffDiskByPart.begin(), handoffDiskByPart.end());
                            break;
                    }
                }

                return res;
            }
        }
    }

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

    TBlobStorageGroupInfo::TSubgroupVDisks TSubgroupPartLayout::GetInvolvedDisks(const TBlobStorageGroupInfo::TTopology *top) const {
        const ui32 totalPartCount = top->GType.TotalPartCount();
        ui32 mask = 0;
        for (ui32 i = 0; i < totalPartCount; ++i) {
            mask |= GetDisksWithPart(i);
        }
        return TBlobStorageGroupInfo::TSubgroupVDisks::CreateFromMask(top, mask);
    }

} // NKikimr

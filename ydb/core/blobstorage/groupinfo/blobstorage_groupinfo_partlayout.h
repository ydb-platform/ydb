#pragma once

#include "defs.h"
#include "blobstorage_groupinfo.h"

namespace NKikimr {

    class TIngress;

    // A special class that contains actual layout of some blob across its subgroup. It contains a number of rows
    // matching the number of parts for the specific erasure type, whereas each cell in a row contains a bit indicating
    // whether a specific part (identified by row number) is present on a subgroup's disk (identified by column number)
    class TSubgroupPartLayout {
        using TRowBitMask = ui16;
        static_assert(sizeof(TRowBitMask) * CHAR_BIT >= MaxNodesPerBlob, "incorrect size of row mask");
        using TTable = std::array<TRowBitMask, MaxTotalPartCount>;

        TTable PerPartStatus;

    public:
        TSubgroupPartLayout() {
            // clear all states to zero
            std::fill(PerPartStatus.begin(), PerPartStatus.end(), 0);
        }

        void AddItem(ui32 nodeId, ui32 partIdx, const TBlobStorageGroupType &gtype) {
            Y_VERIFY(nodeId < gtype.BlobSubgroupSize() && partIdx < gtype.TotalPartCount());
            PerPartStatus[partIdx] |= TRowBitMask(1) << nodeId;
        }

        void ClearItem(ui32 nodeId, ui32 partIdx, const TBlobStorageGroupType& gtype) {
            Y_VERIFY(nodeId < gtype.BlobSubgroupSize() && partIdx < gtype.TotalPartCount());
            PerPartStatus[partIdx] &= ~(TRowBitMask(1) << nodeId);
        }

        void Merge(const TSubgroupPartLayout& other, const TBlobStorageGroupType& type) {
            for (ui32 partIdx = 0; partIdx < type.TotalPartCount(); ++partIdx) {
                PerPartStatus[partIdx] |= other.PerPartStatus[partIdx];
            }
        }

        // Count number of effective replicas (that is, the number of replicas written on distinct disks) using the
        // part-to-node mask; items in that mask are indexes by part index and contain bitmask of subgroup's disks
        // containing these parts
        ui32 CountEffectiveReplicas(const TBlobStorageGroupType &gtype) const;

        // Create part layout from ingress
        static TSubgroupPartLayout CreateFromIngress(TIngress ingress, const TBlobStorageGroupType &gtype);

        // Count effective replicas based on ingress.
        static ui32 CountEffectiveReplicas(TIngress ingress, TBlobStorageGroupType gtype);

        // Return a set of subgroup's disk contaning any replicas
        TBlobStorageGroupInfo::TSubgroupVDisks GetInvolvedDisks(const TBlobStorageGroupInfo::TTopology *top) const;

        void Output(IOutputStream& str, const TBlobStorageGroupType &gtype) const {
            const ui32 totalPartCount = gtype.TotalPartCount();
            str << "{";
            for (ui32 i = 0; i < totalPartCount; ++i) {
                if (i) {
                    str << " ";
                }
                for (ui32 j = 0; j < gtype.BlobSubgroupSize(); ++j) {
                    str << ((PerPartStatus[i] >> (gtype.BlobSubgroupSize() - j - 1)) & 1);
                }
            }
            str << "}";
        }

        TString ToString(const TBlobStorageGroupType& gtype) const {
            TStringStream s;
            Output(s, gtype);
            return s.Str();
        }

        ui32 GetDisksWithPart(ui32 partIdx) const {
            return PerPartStatus[partIdx];
        }

        std::pair<ui32, ui32> GetMirror3of4State() const {
            const ui32 data = PerPartStatus[0] | PerPartStatus[1];
            const ui32 meta = PerPartStatus[2];
            return std::make_tuple(PopCount(data), PopCount(data | meta));
        }

        template<typename F>
        void ForEachPartOfDisk(const TBlobStorageGroupType& gtype, F&& callback) const {
            for (ui32 partIdx = 0; partIdx < gtype.TotalPartCount(); ++partIdx) {
                ui32 mask = PerPartStatus[partIdx];
                while (mask) {
                    const ui32 idxInSubgroup = CountTrailingZeroBits(mask);
                    mask &= ~(1 << idxInSubgroup);
                    callback(partIdx, idxInSubgroup);
                }
            }
        }

        template<typename F>
        static void GeneratePossibleLayouts(const TBlobStorageGroupType& gtype, ui32 maxHandoffBits, F&& callback) {
            std::vector<std::vector<ui32>> perDiskPartMasks(gtype.BlobSubgroupSize());
            for (ui32 idxInSubgroup = 0; idxInSubgroup < perDiskPartMasks.size(); ++idxInSubgroup) {
                auto& partMasks = perDiskPartMasks[idxInSubgroup];
                partMasks.push_back(0);
                switch (gtype.GetErasure()) {
                    case TBlobStorageGroupType::ErasureMirror3dc:
                        partMasks.push_back(1 << (idxInSubgroup % 3));
                        break;

                    case TBlobStorageGroupType::ErasureMirror3of4:
                        partMasks.push_back(1 << (idxInSubgroup & 1)); // data part only
                        partMasks.push_back(1 << (idxInSubgroup & 1) | 1 << 2); // data part + metadata part
                        partMasks.push_back(1 << 2); // metadata part only
                        break;

                    default:
                        if (idxInSubgroup < gtype.TotalPartCount()) {
                            partMasks.push_back(1 << idxInSubgroup);
                        } else {
                            for (ui32 j = 1; j < 1 << gtype.TotalPartCount(); ++j) {
                                if (PopCount(j) <= maxHandoffBits) {
                                    partMasks.push_back(j);
                                }
                            }
                        }
                        break;
                }
            }

            std::vector<ui8> state(perDiskPartMasks.size(), 0);
            for (;;) {
                TSubgroupPartLayout layout;

                for (ui32 idxInSubgroup = 0; idxInSubgroup < state.size(); ++idxInSubgroup) {
                    ui32 diskMask = perDiskPartMasks[idxInSubgroup][state[idxInSubgroup]];
                    while (diskMask) {
                        const ui32 partIdx = CountTrailingZeroBits(diskMask);
                        diskMask &= ~(1 << partIdx);
                        layout.AddItem(idxInSubgroup, partIdx, gtype);
                    }
                }

                callback(layout);

                for (ui32 idxInSubgroup = 0; idxInSubgroup < state.size(); ++idxInSubgroup) {
                    if (++state[idxInSubgroup] != perDiskPartMasks[idxInSubgroup].size()) {
                        break;
                    } else if (idxInSubgroup + 1 != state.size()) {
                        state[idxInSubgroup] = 0;
                    } else {
                        return;
                    }
                }
            }
        }

        friend bool operator ==(const TSubgroupPartLayout& x, const TSubgroupPartLayout& y) {
            return x.PerPartStatus == y.PerPartStatus;
        }

        friend bool operator !=(const TSubgroupPartLayout& x, const TSubgroupPartLayout& y) {
            return x.PerPartStatus != y.PerPartStatus;
        }
    };

} // NKikimr

#pragma once

#include "defs.h"

#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_partlayout.h>

namespace NKikimr {

    class TBlobStatusTracker {
        TLogoBlobID FullId;

        TSubgroupPartLayout PresentParts;
        TSubgroupPartLayout PresentUnconditionally;

        // a mask of faulty disks in subgroup
        TBlobStorageGroupInfo::TSubgroupVDisks FaultyDisks;

        TIngress Ingress;
        bool HasIngress = true;

    public:
        TBlobStatusTracker(const TLogoBlobID& fullId, const TBlobStorageGroupInfo *info)
            : FullId(fullId)
            , FaultyDisks(&info->GetTopology())
        {}

        void Output(IOutputStream& str, const TBlobStorageGroupInfo *info) const {
            str << "{FullId# " << FullId.ToString() << " PresentParts# ";
            PresentParts.Output(str, info->Type);
            str << " FaultyDisks# ";
            FaultyDisks.Output(str);
            if (HasIngress) {
                str << " Ingress# " << Ingress.Raw();
            }
            str << "}";
        }

        void UpdateFromResponseData(const NKikimrBlobStorage::TQueryResult& result, const TVDiskID& vdisk,
                const TBlobStorageGroupInfo *info) {
            // ensure that we have blob id set in reply and that it matches stored one, which we are processing
            Y_ABORT_UNLESS(result.HasBlobID());
            const TLogoBlobID id = LogoBlobIDFromLogoBlobID(result.GetBlobID());
            Y_ABORT_UNLESS(id.FullID() == FullId);

            // check the status
            Y_ABORT_UNLESS(result.HasStatus());
            const NKikimrProto::EReplyStatus status = result.GetStatus();

            // get the node id for this blob and check the part index
            ui32 nodeId = info->GetTopology().GetIdxInSubgroup(vdisk, FullId.Hash());

            // merge the ingress
            Ingress.Merge(TIngress(result.GetIngress()));
            HasIngress = HasIngress && result.HasIngress();

            if (result.HasIngress() && !id.PartId()) { // extract local parts from ingress when there is no specific part reply
                TIngress ingress(result.GetIngress());
                NMatrix::TVectorType parts = ingress.LocalParts(info->Type);
                for (ui8 partIdx = parts.FirstPosition(); partIdx != parts.GetSize(); partIdx = parts.NextPosition(partIdx)) {
                    PresentUnconditionally.AddItem(nodeId, partIdx, info->Type);
                    if (info->Type.PartFits(partIdx + 1, nodeId)) {
                        PresentParts.AddItem(nodeId, partIdx, info->Type);
                    }
                }
            }

            switch (status) {
                case NKikimrProto::OK:
                    if (const ui32 partId = id.PartId()) {
                        PresentUnconditionally.AddItem(nodeId, partId - 1, info->Type);
                        if (info->Type.PartFits(partId, nodeId)) {
                            PresentParts.AddItem(nodeId, partId - 1, info->Type);
                        }
                    }
                    break;

                case NKikimrProto::NODATA:
                    break;

                case NKikimrProto::NOT_YET:
                case NKikimrProto::ERROR:
                case NKikimrProto::VDISK_ERROR_STATE:
                    FaultyDisks += TBlobStorageGroupInfo::TSubgroupVDisks(&info->GetTopology(), nodeId);
                    break;

                default:
                    Y_ABORT("unexpected blob status# %s", NKikimrProto::EReplyStatus_Name(status).data());
            }
        }

        TBlobStorageGroupInfo::EBlobState GetBlobState(const TBlobStorageGroupInfo *info, bool *lostByIngress) const {
            const auto& checker = info->GetQuorumChecker();
            TBlobStorageGroupInfo::EBlobState state = checker.GetBlobState(PresentParts, FaultyDisks);

            if (state == TBlobStorageGroupInfo::EBS_UNRECOVERABLE_FRAGMENTARY || state == TBlobStorageGroupInfo::EBS_DISINTEGRATED) {
                // if the blob seems lost/never written according to fitting parts, fall back to all seen parts
                state = checker.GetBlobState(PresentUnconditionally, FaultyDisks);
                if (state == TBlobStorageGroupInfo::EBS_FULL) { // but restore this blob's layout correctly then
                    state = TBlobStorageGroupInfo::EBS_RECOVERABLE_FRAGMENTARY;
                }
            }

            // check if the blob was completely written according to returned Ingress information
            const bool fullByIngress = HasIngress &&
                checker.GetBlobState(TSubgroupPartLayout::CreateFromIngress(Ingress, info->Type),
                TBlobStorageGroupInfo::TSubgroupVDisks(&info->GetTopology())) == TBlobStorageGroupInfo::EBS_FULL;

            // in case we have an ingress and we don't have all the replicas, we can update the state accoring to ingress
            // by finding out if the blob was seen on all disks
            if (state == TBlobStorageGroupInfo::EBS_RECOVERABLE_FRAGMENTARY && !fullByIngress) {
                state = TBlobStorageGroupInfo::EBS_RECOVERABLE_DOUBTED;
            }

            if (lostByIngress) {
                *lostByIngress = state == TBlobStorageGroupInfo::EBS_UNRECOVERABLE_FRAGMENTARY && fullByIngress;
            }

            return state;
        }
    };

} // NKikimr

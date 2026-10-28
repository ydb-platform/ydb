#pragma once

#include "defs.h"

#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_sets.h>

namespace NKikimr {

    class TGroupQuorumTracker {
        const TBlobStorageGroupInfo *Info;

        // a set of disks that replied successfully (with OK)
        TBlobStorageGroupInfo::TGroupVDisks SuccessfulDisks;

        // a set of disks that replied with error (not OK, suddenly)
        TBlobStorageGroupInfo::TGroupVDisks ErroneousDisks;

        // per-vdisk record for VDisks that replied with success
        struct TVDiskInfo {
            TInstant ExpireTimestamp; // when the disk successful response expires
            std::optional<ui64> IncarnationGuid;
        };
        std::map<TVDiskID, TVDiskInfo> VDiskMap;
        ui32 NumResendsRemain;
        ui32 NumStatusRemain;

    public:
        TGroupQuorumTracker(const TBlobStorageGroupInfo *info)
            : Info(info)
            , SuccessfulDisks(&Info->GetTopology())
            , ErroneousDisks(&Info->GetTopology())
            , NumResendsRemain(Info->GetTotalVDisksNum())
            , NumStatusRemain(2 * Info->GetTotalVDisksNum())
        {}

        void Output(IOutputStream &out) {
            out << "{Erroneous# ";
            ErroneousDisks.Output(out);
            out << " Successful# ";
            SuccessfulDisks.Output(out);
            out << "}";
        }

        TString ToString() {
            TStringStream str;
            Output(str);
            return str.Str();
        }

        NKikimrProto::EReplyStatus ProcessReply(const TVDiskID& from, NKikimrProto::EReplyStatus status) {
            // should be handled in CheckForTermErrors
            Y_ABORT_UNLESS(status != NKikimrProto::RACE && status != NKikimrProto::BLOCKED && status != NKikimrProto::DEADLINE);

            Y_ABORT_UNLESS(status == NKikimrProto::OK || status == NKikimrProto::ERROR ||
                status == NKikimrProto::VDISK_ERROR_STATE || status == NKikimrProto::OUT_OF_SPACE,
                "unexpected status# %s", NKikimrProto::EReplyStatus_Name(status).data());

            const auto& mask = TBlobStorageGroupInfo::TGroupVDisks(&Info->GetTopology(), from);
            if (status == NKikimrProto::OK) {
                SuccessfulDisks |= mask;
                ErroneousDisks -= mask;
            } else {
                ErroneousDisks |= mask;
                SuccessfulDisks -= mask;
            }

            return CalculateStatus();
        }

        NKikimrProto::EReplyStatus CalculateStatus() {
            const auto& checker = Info->GetQuorumChecker();
            return !checker.CheckFailModelForGroup(ErroneousDisks) ? NKikimrProto::ERROR  :
                   checker.CheckQuorumForGroup(SuccessfulDisks)    ? NKikimrProto::OK     :
                                                                     NKikimrProto::UNKNOWN;
        }

        NKikimrProto::EReplyStatus ProcessReplyWithCooldown(const TVDiskID& from, NKikimrProto::EReplyStatus status,
                TInstant now, ui64 incarnationGuid, std::vector<TVDiskID>& queryStatus, std::vector<TVDiskID>& resend) {
            if (status == NKikimrProto::OK) {
                auto& info = VDiskMap[from];
                info.ExpireTimestamp = now + VDiskCooldownTimeoutOnProxy;
                if (!info.IncarnationGuid) {
                    info.IncarnationGuid = incarnationGuid;
                } else if (*info.IncarnationGuid != incarnationGuid) {
                    if (NumResendsRemain) {
                        --NumResendsRemain;
                        resend.push_back(from);
                        VDiskMap.erase(from);
                        return NKikimrProto::UNKNOWN;
                    } else {
                        status = NKikimrProto::ERROR;
                    }
                }
            }

            status = ProcessReply(from, status);
            if (status == NKikimrProto::OK) {
                for (auto& [vdiskId, info] : VDiskMap) {
                    if (info.ExpireTimestamp <= now) {
                        const auto& mask = TBlobStorageGroupInfo::TGroupVDisks(&Info->GetTopology(), vdiskId);
                        SuccessfulDisks -= mask;
                        info.ExpireTimestamp = TInstant::Max();
                        status = NKikimrProto::UNKNOWN;
                        if (NumStatusRemain) {
                            --NumStatusRemain;
                            queryStatus.push_back(vdiskId);
                        } else {
                            ErroneousDisks += mask;
                        }
                    }
                }
                if (status == NKikimrProto::UNKNOWN) {
                    status = CalculateStatus();
                }
            }
            return status;
        }
    };

} // NKikimr

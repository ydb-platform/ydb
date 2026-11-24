#include "util.h"

#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_mirror3dc_write_quorum.h>

namespace NKikimr::NBsController {

    NKikimrBlobStorage::TUpdateSettings FromBscConfig(const NKikimrBlobStorage::TBscConfig &config) {
        NKikimrBlobStorage::TUpdateSettings result;

        if (config.HasDefaultMaxSlots()) {
            result.AddDefaultMaxSlots(config.GetDefaultMaxSlots());
        }
        if (config.HasEnableSelfHeal()) {
            result.AddEnableSelfHeal(config.GetEnableSelfHeal());
        }
        if (config.HasEnableDonorMode()) {
            result.AddEnableDonorMode(config.GetEnableDonorMode());
        }
        if (config.HasScrubPeriodicitySeconds()) {
            result.AddScrubPeriodicitySeconds(config.GetScrubPeriodicitySeconds());
        }
        if (config.HasPDiskSpaceMarginPromille()) {
            result.AddPDiskSpaceMarginPromille(config.GetPDiskSpaceMarginPromille());
        }
        if (config.HasGroupReserveMin()) {
            result.AddGroupReserveMin(config.GetGroupReserveMin());
        }
        if (config.HasGroupReservePartPPM()) {
            result.AddGroupReservePartPPM(config.GetGroupReservePartPPM());
        }
        if (config.HasMaxScrubbedDisksAtOnce()) {
            result.AddMaxScrubbedDisksAtOnce(config.GetMaxScrubbedDisksAtOnce());
        }
        if (config.HasPDiskSpaceColorBorder()) {
            result.AddPDiskSpaceColorBorder(config.GetPDiskSpaceColorBorder());
        }
        if (config.HasDatabaseSpaceBlockColor()) {
            result.AddDatabaseSpaceBlockColor(config.GetDatabaseSpaceBlockColor());
        }
        if (config.HasDatabaseSpaceUnblockColor()) {
            result.AddDatabaseSpaceUnblockColor(config.GetDatabaseSpaceUnblockColor());
        }
        if (config.HasBridgeSyncTotalRateBytesPerSecond()) {
            result.AddBridgeSyncTotalRateBytesPerSecond(config.GetBridgeSyncTotalRateBytesPerSecond());
        }
        if (config.HasBridgeSyncMaxSyncers()) {
            result.AddBridgeSyncMaxSyncers(config.GetBridgeSyncMaxSyncers());
        }
        if (config.HasEnableGroupLayoutSanitizer()) {
            result.AddEnableGroupLayoutSanitizer(config.GetEnableGroupLayoutSanitizer());
        }
        if (config.HasAllowMultipleRealmsOccupation()) {
            result.AddAllowMultipleRealmsOccupation(config.GetAllowMultipleRealmsOccupation());
        }
        if (config.HasUseSelfHealLocalPolicy()) {
            result.AddUseSelfHealLocalPolicy(config.GetUseSelfHealLocalPolicy());
        }
        if (config.HasTryToRelocateBrokenDisksLocallyFirst()) {
            result.AddTryToRelocateBrokenDisksLocallyFirst(config.GetTryToRelocateBrokenDisksLocallyFirst());
        }

        return result;
    }

    bool CheckGroupReassignmentFailModel(const TMirror3dcGroupConfiguration& config,
            const TBlobStorageGroupInfo::TGroupVDisks& failed) {
        if (config == TMirror3dcGroupConfiguration{}) {
            return failed.GetTopology()->GetQuorumChecker().CheckFailModelForGroup(failed);
        }
        return TMirror3dcWriteQuorumChecker::CheckFailModelForGroup(config, failed);
    }

    TBlobStorageGroupInfo::TGroupVDisks GetExcludedVDisks(const TMirror3dcGroupConfiguration& config,
            const TBlobStorageGroupInfo::TTopology& topology) {
        TBlobStorageGroupInfo::TGroupVDisks excluded(&topology);
        if (config != TMirror3dcGroupConfiguration{} && topology.GetTotalFailRealmsNum() == config.Realms.size()) {
            for (ui32 order = 0; order < topology.GetTotalVDisksNum(); ++order) {
                TVDiskIdShort id = topology.GetVDiskId(order);
                if (IsExcluded(config.Realms[id.FailRealm])) {
                    excluded |= {&topology, id};
                }
            }
        }
        return excluded;
    }

}  // NKikimr::NBsController

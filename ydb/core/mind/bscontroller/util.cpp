#include "util.h"

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


}  // NKikimr::NBsController

#pragma once

#include <Disks/IVolume.h>

namespace DB
{

VolumePtr createVolumeFromReservation(const ReservationPtr & reservation, VolumePtr other_volume);

VolumePtr createVolumeFromConfig(
    String name_,
    const DBPoco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    DiskSelectorPtr disk_selector
);

VolumePtr updateVolumeFromConfig(
    VolumePtr volume,
    const DBPoco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    DiskSelectorPtr & disk_selector
);

}

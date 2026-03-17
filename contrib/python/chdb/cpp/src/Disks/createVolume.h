#pragma once

#include <Disks/IVolume.h>

namespace DB_CHDB
{

VolumePtr createVolumeFromReservation(const ReservationPtr & reservation, VolumePtr other_volume);

VolumePtr createVolumeFromConfig(
    String name_,
    const CHDBPoco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    DiskSelectorPtr disk_selector
);

VolumePtr updateVolumeFromConfig(
    VolumePtr volume,
    const CHDBPoco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    DiskSelectorPtr & disk_selector
);

}

#include "utils.h"

namespace NKikimr::NPQ {

ui64 TopicPartitionReserveSize(const NKikimrPQ::TPQTabletConfig& config) {
    if (NKikimrPQ::TPQTabletConfig::METERING_MODE_REQUEST_UNITS == config.GetMeteringMode()) {
        return 0;
    }
    if (config.GetPartitionConfig().HasStorageLimitBytes()) {
        return config.GetPartitionConfig().GetStorageLimitBytes();
    }
    return config.GetPartitionConfig().GetLifetimeSeconds() * config.GetPartitionConfig().GetWriteSpeedInBytesPerSecond();
}

ui64 TopicPartitionReserveThroughput(const NKikimrPQ::TPQTabletConfig& config) {
    if (NKikimrPQ::TPQTabletConfig::METERING_MODE_REQUEST_UNITS == config.GetMeteringMode()) {
        return 0;
    }
    return config.GetPartitionConfig().GetWriteSpeedInBytesPerSecond();
}

} // NKikimr::NPQ

#pragma once

#include <ydb/core/protos/pqconfig.pb.h>

namespace NKikimr {
namespace NSchemeShard {

class PQGroupReserve {
public:
    PQGroupReserve(const ::NKikimrPQ::TPQTabletConfig& tabletConfig, ui64 partitions, ui64 currentStorageUsage = 0) {
        Storage = NKikimrPQ::TPQTabletConfig::METERING_MODE_REQUEST_UNITS == tabletConfig.GetMeteringMode()
            ? currentStorageUsage : partitions * NPQ::TopicPartitionReserveSize(tabletConfig);
        Throughput = partitions * NPQ::TopicPartitionReserveThroughput(tabletConfig);
    }

    ui64 Storage;
    ui64 Throughput;
};

} // NSchemeShard
} // NKikimr

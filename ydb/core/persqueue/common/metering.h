#pragma once

namespace NKikimr::NPQ {

enum class EMeteringJson {
    PutEventsV1 = 1,
    ResourcesReservedV1 = 2,
    ThroughputV1 = 3,
    ReadThroughputV1 = 4,
    StorageV1 = 5,
    UsedStorageV1 = 6,
};

}

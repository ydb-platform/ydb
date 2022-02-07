#pragma once
#include "defs.h"

namespace NKikimr {
namespace NKeyValue {

#pragma pack(push, 1)
struct TKeyHeader {
    ui8 ItemType;
    ui8 Checksum;

    TKeyHeader()
        : ItemType(0)
        , Checksum(1)
    {}
};
#pragma pack(pop)

} // NKeyValue
} // NKikimr

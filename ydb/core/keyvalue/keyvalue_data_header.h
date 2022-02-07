#pragma once
#include "defs.h"

namespace NKikimr {
namespace NKeyValue {

#pragma pack(push, 1)
struct TDataHeader {
    ui8 ItemType;
    ui8 Checksum;

    TDataHeader()
        : ItemType(0)
        , Checksum(1)
    {}
};
#pragma pack(pop)

} // NKeyValue
} // NKikimr

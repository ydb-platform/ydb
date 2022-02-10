#pragma once
#include "defs.h"
#include "keyvalue_data_header.h"
#include <ydb/core/base/logoblob.h>

namespace NKikimr {
namespace NKeyValue {

#pragma pack(push, 1)
struct TKeyValueData1 {
    TDataHeader DataHeader;
    TLogoBlobID FirstLogoBlobId;
    ui64 CreationUnixTime;
    TLogoBlobID ExtraIds[];

    TKeyValueData1();
    void UpdateChecksum(ui32 numItems);
    bool CheckChecksum(ui32 numItems) const;
    static ui32 GetRecordSize(ui32 numItems);
    static ui32 GetNumItems(ui32 recordSize);
};

struct TKeyValueData2 {
    TDataHeader DataHeader;
    ui64 CreationUnixTime;
    char Serialized[];  // Mixed series of {(TLogoBlobID)id} or {(ui64)0, (ui32)size, (ui8[size])data}

    TKeyValueData2();
    void UpdateChecksum(ui64 size);
    bool CheckChecksum(ui64 size) const;
};
#pragma pack(pop)

} // NKeyValue
} // NKikimr

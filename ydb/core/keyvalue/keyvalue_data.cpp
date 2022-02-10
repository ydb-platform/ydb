#include "keyvalue_data.h"
#include "keyvalue_item_type.h"
#include "keyvalue_helpers.h"

namespace NKikimr {
namespace NKeyValue {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TKeyValueData1
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

TKeyValueData1::TKeyValueData1()
    : CreationUnixTime(0)
{
    DataHeader.ItemType = EIT_KEYVALUE_1;
}

void TKeyValueData1::UpdateChecksum(ui32 numItems) {
    DataHeader.Checksum = 0;
    DataHeader.Checksum = THelpers::Checksum(0, GetRecordSize(numItems), (const ui8*)this);
}

bool TKeyValueData1::CheckChecksum(ui32 numItems) const {
    ui8 sum = THelpers::Checksum(0, GetRecordSize(numItems), (const ui8*)this);
    return (sum == 0);
}

ui32 TKeyValueData1::GetRecordSize(ui32 numItems) {
    return sizeof(TKeyValueData1) + (numItems > 0 ? sizeof(TLogoBlobID) * (numItems - 1) : 0);
}

ui32 TKeyValueData1::GetNumItems(ui32 recordSize) {
    return 1 + (recordSize - sizeof(TKeyValueData1)) / sizeof(TLogoBlobID);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TKeyValueData2
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

TKeyValueData2::TKeyValueData2()
    : CreationUnixTime(0)
{
    DataHeader.ItemType = EIT_KEYVALUE_2;
}

void TKeyValueData2::UpdateChecksum(ui64 size) {
    DataHeader.Checksum = 0;
    DataHeader.Checksum = THelpers::Checksum(0, size, (const ui8*)this);
}

bool TKeyValueData2::CheckChecksum(ui64 size) const {
    ui8 sum = THelpers::Checksum(0, size, (const ui8*)this);
    return (sum == 0);
}

} // NKeyValue
} // NKikimr

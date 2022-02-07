#include "keyvalue_stored_state_data.h"
#include "keyvalue_helpers.h"
#include "keyvalue_const.h"

namespace NKikimr {
namespace NKeyValue {

TKeyValueStoredStateData::TKeyValueStoredStateData() {
    Clear();
}

void TKeyValueStoredStateData::Clear() {
    DataHeader.ItemType = EIT_STATE;
    DataHeader.Checksum = 1;
    Version = KEYVALUE_VERSION;
    UserGeneration = 0;

    CollectGeneration = 0;
    CollectStep = 0;
    ChannelGeneration = 0;
    ChannelStep = 0;
}

void TKeyValueStoredStateData::UpdateChecksum() {
    DataHeader.Checksum = 0;
    DataHeader.Checksum = THelpers::Checksum(0, sizeof(TKeyValueStoredStateData), (const ui8*)this);
}

bool TKeyValueStoredStateData::CheckChecksum() const {
    ui8 sum = THelpers::Checksum(0, sizeof(TKeyValueStoredStateData), (const ui8*)this);
    return (sum == 0);
}

} // NKeyValue
} // NKikimr

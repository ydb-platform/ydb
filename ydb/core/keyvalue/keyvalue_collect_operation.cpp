#include "keyvalue_collect_operation.h"
#include "keyvalue_helpers.h"

namespace NKikimr {
namespace NKeyValue {

TCollectOperationHeader::TCollectOperationHeader(ui64 collectGeneration, ui64 collectStep,
        TVector<TLogoBlobID> &keep, TVector<TLogoBlobID> &doNotKeep)
    : CollectGeneration(collectGeneration)
    , CollectStep(collectStep)
    , KeepCount(keep.size())
    , DoNotKeepCount(doNotKeep.size())
{
    DataHeader.ItemType = EIT_COLLECT;
    DataHeader.Checksum = 0;
    ui8 sum = 0;
    sum = THelpers::Checksum(sum, sizeof(TCollectOperationHeader), (const ui8*)this);
    if (KeepCount) {
        sum = THelpers::Checksum(sum, sizeof(TLogoBlobID) * KeepCount, (const ui8 *) &keep[0]);
    }
    if (DoNotKeepCount) {
        sum = THelpers::Checksum(sum, sizeof(TLogoBlobID) * DoNotKeepCount, (const ui8 *) &doNotKeep[0]);
    }
    DataHeader.Checksum = sum;
}


} // NKeyValue
} // NKikimr

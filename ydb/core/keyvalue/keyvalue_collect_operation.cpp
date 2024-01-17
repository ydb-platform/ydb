#include "keyvalue_collect_operation.h"
#include "keyvalue_helpers.h"

namespace NKikimr {
namespace NKeyValue {

TCollectOperationHeader::TCollectOperationHeader(ui32 collectGeneration, ui32 collectStep)
    : CollectGeneration(collectGeneration)
    , CollectStep(collectStep)
{} 

} // NKeyValue
} // NKikimr

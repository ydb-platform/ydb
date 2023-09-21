#include "common.h"
#include <util/generic/refcount.h>

namespace NKikimr::NOlap {

namespace {
static TAtomicCounter ActionIdCounter = 0;
}
ICommonBlobsAction::ICommonBlobsAction(const TString& storageId)
    : StorageId(storageId)
    , ActionId(ActionIdCounter.Inc())
{
}

}

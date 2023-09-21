#pragma once
#include "common.h"
#include <util/generic/string.h>

namespace NKikimr::NColumnShard {
class TColumnShard;
class TBlobManagerDb;
}

namespace NKikimr::NOlap {

class IBlobsGCAction: public ICommonBlobsAction {
private:
    using TBase = ICommonBlobsAction;
protected:

    virtual void DoOnExecuteTxAfterCleaning(NColumnShard::TColumnShard& self, NColumnShard::TBlobManagerDb& dbBlobs) = 0;
    virtual void DoOnCompleteTxAfterCleaning(NColumnShard::TColumnShard& self, const std::shared_ptr<IBlobsGCAction>& taskAction) = 0;
public:
    void OnExecuteTxAfterCleaning(NColumnShard::TColumnShard& self, NColumnShard::TBlobManagerDb& dbBlobs) {
        return DoOnExecuteTxAfterCleaning(self, dbBlobs);
    }
    void OnCompleteTxAfterCleaning(NColumnShard::TColumnShard& self, const std::shared_ptr<IBlobsGCAction>& taskAction);

    IBlobsGCAction(const TString& storageId)
        : TBase(storageId)
    {

    }
};

}

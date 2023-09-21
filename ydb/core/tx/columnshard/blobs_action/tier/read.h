#pragma once

#include <ydb/core/tx/columnshard/blobs_action/abstract/read.h>
#include <ydb/core/wrappers/abstract.h>

namespace NKikimr::NOlap::NBlobOperations::NTier {

class TReadingAction: public IBlobsReadingAction {
private:
    using TBase = IBlobsReadingAction;
    const NWrappers::NExternalStorage::IExternalStorageOperator::TPtr ExternalStorageOperator;
protected:
    virtual void DoStartReading(const THashMap<TUnifiedBlobId, THashSet<TBlobRange>>& ranges) override;
public:

    TReadingAction(const TString& storageId, const NWrappers::NExternalStorage::IExternalStorageOperator::TPtr& storageOperator)
        : TBase(storageId)
        , ExternalStorageOperator(storageOperator) {

    }
};

}

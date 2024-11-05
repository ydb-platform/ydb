#pragma once

#include <ydb/core/tx/columnshard/blobs_action/abstract/read.h>
#include <ydb/core/wrappers/abstract.h>

namespace NKikimr::NOlap::NBlobOperations::NTier {

class TReadingAction: public IBlobsReadingAction {
private:
    using TBase = IBlobsReadingAction;
    const NWrappers::NExternalStorage::IExternalStorageOperator::TPtr ExternalStorageOperator;
protected:
    virtual void DoStartReading(THashSet<TBlobRange>&& ranges) override;
    virtual THashMap<TBlobRange, std::vector<TBlobRange>> GroupBlobsForOptimization(std::vector<TBlobRange>&& ranges) const override {
        return TBlobsGlueing::GroupRanges(std::move(ranges), TBlobsGlueing::TBlobGluePolicy(8LLU << 20));
    }
public:

    TReadingAction(const TString& storageId, const NWrappers::NExternalStorage::IExternalStorageOperator::TPtr& storageOperator)
        : TBase(storageId)
        , ExternalStorageOperator(storageOperator) {

    }
};

}

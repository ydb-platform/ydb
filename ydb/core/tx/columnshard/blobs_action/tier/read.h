#pragma once

#include "common.h"

#include <ydb/core/tx/columnshard/blobs_action/abstract/read.h>
#include <ydb/core/wrappers/abstract.h>

namespace NKikimr::NOlap::NBlobOperations::NTier {

class TReadingAction: public IBlobsReadingAction {
private:
    using TBase = IBlobsReadingAction;
    // The holder is shared with the storage operator: every (re)try of a read uses the current external storage operator,
    // so reads started while the tier was unavailable succeed as soon as the tier is configured
    const std::shared_ptr<TExternalStorageOperatorHolder> ExternalStorageOperator;

protected:
    virtual void DoStartReading(THashSet<TBlobRange>&& ranges) override;
    virtual void DoRetryRead(const TBlobRange& range) override;

    virtual THashMap<TBlobRange, std::vector<TBlobRange>> GroupBlobsForOptimization(std::vector<TBlobRange>&& ranges) const override {
        return TBlobsGlueing::GroupRanges(std::move(ranges), TBlobsGlueing::TBlobGluePolicy(8LLU << 20));
    }

public:
    TReadingAction(const TString& storageId, const std::shared_ptr<TExternalStorageOperatorHolder>& storageOperator)
        : TBase(storageId)
        , ExternalStorageOperator(storageOperator)
    {
        AFL_VERIFY(ExternalStorageOperator);
    }
};

}   // namespace NKikimr::NOlap::NBlobOperations::NTier

#pragma once

#include <ydb/core/tx/columnshard/blobs_action/abstract/read.h>

namespace NKikimr::NOlap::NBlobOperations::NBlobStorage {

class TReadingAction: public IBlobsReadingAction {
private:
    using TBase = IBlobsReadingAction;
    const TActorId BlobCacheActorId;
protected:
    virtual void DoStartReading(THashSet<TBlobRange>&& ranges) override;
    virtual THashMap<TBlobRange, std::vector<TBlobRange>> GroupBlobsForOptimization(std::vector<TBlobRange>&& ranges) const override {
        return TBlobsGlueing::GroupRanges(std::move(ranges), TBlobsGlueing::TSequentialGluePolicy());
    }
public:

    TReadingAction(const TString& storageId, const TActorId& blobCacheActorId)
        : TBase(storageId)
        , BlobCacheActorId(blobCacheActorId)
    {

    }
};

}

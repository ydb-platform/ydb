#pragma once

#include <ydb/core/tx/columnshard/blobs_action/abstract/read.h>

namespace NKikimr::NOlap::NBlobOperations::NBlobStorage {

class TReadingAction: public IBlobsReadingAction {
private:
    using TBase = IBlobsReadingAction;
    const TActorId BlobCacheActorId;
protected:
    virtual void DoStartReading(THashSet<TBlobRange>&& ranges) override;
public:

    TReadingAction(const TString& storageId, const TActorId& blobCacheActorId)
        : TBase(storageId)
        , BlobCacheActorId(blobCacheActorId)
    {

    }
};

}

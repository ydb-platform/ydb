#include "remove.h"
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NOlap {

void IBlobsDeclareRemovingAction::DeclareRemove(const TTabletId tabletId, const TUnifiedBlobId& blobId) {
    if (DeclaredBlobs.Add(tabletId, blobId)) {
        ACFL_DEBUG("event", "DeclareRemove")("blob_id", blobId)("tablet_id", (ui64)tabletId);
        Counters->OnRequest(blobId.BlobSize());
        return DoDeclareRemove(tabletId, blobId);
    }
}

void IBlobsDeclareRemovingAction::DeclareSelfRemove(const TUnifiedBlobId& blobId) {
    DeclareRemove(SelfTabletId, blobId);
}

}

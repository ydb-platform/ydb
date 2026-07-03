#include "remove.h"

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NOlap {

void IBlobsDeclareRemovingAction::DeclareRemove(const TTabletId tabletId, const TUnifiedBlobId& blobId) {
    if (DeclaredBlobs.Add(tabletId, blobId)) {
        YDB_LOG_DEBUG_COMP(NActors::NStructuredLog::TLogStack::GetComponent(), "Dump event, blobId, tabletId",
            {"event", "DeclareRemove"},
            {"blobId", blobId},
            {"tabletId", (ui64)tabletId});
        Counters->OnRequest(blobId.BlobSize());
        return DoDeclareRemove(tabletId, blobId);
    }
}

void IBlobsDeclareRemovingAction::DeclareSelfRemove(const TUnifiedBlobId& blobId) {
    DeclareRemove(SelfTabletId, blobId);
}

}   // namespace NKikimr::NOlap

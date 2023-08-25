#pragma once

#include "blob.h"
#include <ydb/core/tx/columnshard/engines/indexed_read_data.h>

namespace NKikimr::NColumnShard {

class IBlobInUseTracker {
protected:
    ~IBlobInUseTracker() = default;

public:
    // Marks the blob as "in use (or no longer in use) by an in-flight request", increments (or decrements)
    // it's ref count. This will prevent the blob from beeing physically deleted when DeleteBlob() is called
    // until all the references are released.
    // NOTE: this ref counts are in-memory only, so the blobs can be deleted if tablet restarts
    virtual bool SetBlobInUse(const NOlap::TUnifiedBlobId& blobId, bool inUse) = 0;
    virtual bool BlobInUse(const NOlap::TUnifiedBlobId& blobId) const = 0;
};

using NOlap::TReadMetadata;

class TInFlightReadsTracker {
public:
    // Returns a unique cookie associated with this request
    ui64 AddInFlightRequest(NOlap::TReadMetadataBase::TConstPtr readMeta, IBlobInUseTracker& blobTracker) {
        const ui64 cookie = NextCookie++;
        AddToInFlightRequest(cookie, readMeta, blobTracker);
        return cookie;
    }

    // Returns a unique cookie associated with this request
    template <class TReadMetadataList>
    ui64 AddInFlightRequest(const TReadMetadataList& readMetaList, IBlobInUseTracker& blobTracker) {
        const ui64 cookie = NextCookie++;
        for (const auto& readMetaPtr : readMetaList) {
            AddToInFlightRequest(cookie, readMetaPtr, blobTracker);
        }
        return cookie;
    }

    // Forget completed request
    THashSet<NOlap::TUnifiedBlobId> RemoveInFlightRequest(ui64 cookie, IBlobInUseTracker& blobTracker) {
        Y_VERIFY(RequestsMeta.contains(cookie), "Unknown request cookie %" PRIu64, cookie);
        const auto& readMetaList = RequestsMeta[cookie];

        THashSet<NOlap::TUnifiedBlobId> freedBlobs;

        for (const auto& readMetaBase : readMetaList) {
            NOlap::TReadMetadata::TConstPtr readMeta = std::dynamic_pointer_cast<const NOlap::TReadMetadata>(readMetaBase);

            if (!readMeta) {
                continue;
            }

            for (const auto& portion : readMeta->SelectInfo->Portions) {
                const ui64 portionId = portion.Records[0].Portion;
                auto it = PortionUseCount.find(portionId);
                Y_VERIFY(it != PortionUseCount.end(), "Portion id %" PRIu64 " not found in request %" PRIu64, portionId, cookie);
                if (it->second == 1) {
                    PortionUseCount.erase(it);
                } else {
                    it->second--;
                }
                for (auto& rec : portion.Records) {
                    if (blobTracker.SetBlobInUse(rec.BlobRange.BlobId, false)) {
                        freedBlobs.emplace(rec.BlobRange.BlobId);
                    }
                }
            }

            for (const auto& committedBlob : readMeta->CommittedBlobs) {
                if (blobTracker.SetBlobInUse(committedBlob.GetBlobId(), false)) {
                    freedBlobs.emplace(committedBlob.GetBlobId());
                }
            }
        }

        RequestsMeta.erase(cookie);
        return freedBlobs;
    }

    // Checks if the portion is in use by any in-flight request
    bool IsPortionUsed(ui64 portionId) const {
        return PortionUseCount.contains(portionId);
    }

    NOlap::TSelectInfo::TStats GetSelectStatsDelta() {
        auto delta = SelectStatsDelta;
        SelectStatsDelta = NOlap::TSelectInfo::TStats();
        return delta;
    }

private:
    void AddToInFlightRequest(const ui64 cookie, NOlap::TReadMetadataBase::TConstPtr readMetaBase, IBlobInUseTracker& blobTracker) {
        RequestsMeta[cookie].push_back(readMetaBase);

        NOlap::TReadMetadata::TConstPtr readMeta = std::dynamic_pointer_cast<const NOlap::TReadMetadata>(readMetaBase);

        if (!readMeta) {
            return;
        }

        auto selectInfo = readMeta->SelectInfo;
        Y_VERIFY(selectInfo);
        SelectStatsDelta += selectInfo->Stats();

        for (const auto& portion : readMeta->SelectInfo->Portions) {
            const ui64 portionId = portion.Records[0].Portion;
            PortionUseCount[portionId]++;
            for (auto& rec : portion.Records) {
                blobTracker.SetBlobInUse(rec.BlobRange.BlobId, true);
            }
        }

        for (const auto& committedBlob : readMeta->CommittedBlobs) {
            blobTracker.SetBlobInUse(committedBlob.GetBlobId(), true);
        }
    }

private:
    ui64 NextCookie{1};
    THashMap<ui64, TList<NOlap::TReadMetadataBase::TConstPtr>> RequestsMeta;
    THashMap<ui64, ui64> PortionUseCount;
    NOlap::TSelectInfo::TStats SelectStatsDelta;
};

}

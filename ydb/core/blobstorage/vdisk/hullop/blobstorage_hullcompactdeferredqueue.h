#pragma once

#include "defs.h"
#include <ydb/core/blobstorage/vdisk/common/disk_part.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/blobstorage_blob.h>
#include <ydb/core/blobstorage/vdisk/ingress/blobstorage_ingress_matrix.h>

#include <util/generic/queue.h>

namespace NKikimr {

    template<typename TDerived>
    class TDeferredItemQueueBase {
        struct TItem {
            ui64 Id;
            ui32 NumReads;
            TDiskPart PreallocatedLocation;
            TDiskBlobMergerWithMask Merger;
            NMatrix::TVectorType PartsToStore;
            TLogoBlobID BlobId;

            TItem(ui64 id, ui32 numReads, const TDiskPart& preallocatedLocation, const TDiskBlobMerger& merger,
                    NMatrix::TVectorType partsToStore, const TLogoBlobID& blobId)
                : Id(id)
                , NumReads(numReads)
                , PreallocatedLocation(preallocatedLocation)
                , Merger(merger, partsToStore)
                , PartsToStore(partsToStore)
                , BlobId(blobId)
            {}
        };

        TQueue<TItem> ItemQueue;
        bool Started = false;
        TRopeArena& Arena;
        const TBlobStorageGroupType GType;
        const bool AddHeader;

    public:
        TDeferredItemQueueBase(TRopeArena& arena, TBlobStorageGroupType gtype, bool addHeader)
            : Arena(arena)
            , GType(gtype)
            , AddHeader(addHeader)
        {}

        template<typename... TArgs>
        void Put(TArgs&&... args) {
            Y_ABORT_UNLESS(!Started);
            ItemQueue.emplace(std::forward<TArgs>(args)...);
        }

        template<typename... TArgs>
        void Start(TArgs&&... args) {
            Y_ABORT_UNLESS(!Started);
            Started = true;
            static_cast<TDerived&>(*this).StartImpl(std::forward<TArgs>(args)...);
            ProcessItemQueue();
        }

        void AddReadDiskBlob(ui64 id, TRope&& buffer, NMatrix::TVectorType expectedParts) {
            Y_ABORT_UNLESS(Started);
            Y_ABORT_UNLESS(ItemQueue);
            TItem& item = ItemQueue.front();
            Y_ABORT_UNLESS(item.Id == id);
            item.Merger.Add(TDiskBlob(&buffer, expectedParts, GType, item.BlobId));
            Y_ABORT_UNLESS(item.NumReads > 0);
            if (!--item.NumReads) {
                ProcessItemQueue();
            }
        }

        bool AllProcessed() {
            return ItemQueue.empty();
        }

        void Finish() {
            Y_ABORT_UNLESS(Started);
            Y_ABORT_UNLESS(ItemQueue.empty());
            Started = false;
            static_cast<TDerived&>(*this).FinishImpl();
        }

    private:
        void ProcessItemQueue() {
            while (ItemQueue && !ItemQueue.front().NumReads) {
                ProcessItem(ItemQueue.front());
                ItemQueue.pop();
            }
        }

        void ProcessItem(TItem& item) {
            // ensure that we have all the parts we must have
            Y_ABORT_UNLESS(item.Merger.GetDiskBlob().GetParts() == item.PartsToStore);

            // get newly generated blob raw content and put it into writer queue
            static_cast<TDerived&>(*this).ProcessItemImpl(item.PreallocatedLocation, item.Merger.CreateDiskBlob(Arena,
                AddHeader));
        }
    };

} // NKikimr

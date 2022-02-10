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

    public:
        TDeferredItemQueueBase(TRopeArena& arena, TBlobStorageGroupType gtype)
            : Arena(arena)
            , GType(gtype)
        {}

        template<typename... TArgs>
        void Put(TArgs&&... args) {
            Y_VERIFY(!Started);
            ItemQueue.emplace(std::forward<TArgs>(args)...);
        }

        template<typename... TArgs>
        void Start(TArgs&&... args) {
            Y_VERIFY(!Started);
            Started = true;
            static_cast<TDerived&>(*this).StartImpl(std::forward<TArgs>(args)...);
            ProcessItemQueue();
        }

        void AddReadDiskBlob(ui64 id, TRope&& buffer, NMatrix::TVectorType expectedParts) {
            Y_VERIFY(Started);
            Y_VERIFY(ItemQueue);
            TItem& item = ItemQueue.front();
            Y_VERIFY(item.Id == id);
            item.Merger.Add(TDiskBlob(&buffer, expectedParts, GType, item.BlobId));
            Y_VERIFY(item.NumReads > 0);
            if (!--item.NumReads) {
                ProcessItemQueue();
            }
        }

        bool AllProcessed() {
            return ItemQueue.empty();
        }

        void Finish() {
            Y_VERIFY(Started);
            Y_VERIFY(ItemQueue.empty());
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
            Y_VERIFY(item.Merger.GetDiskBlob().GetParts() == item.PartsToStore);

            // get newly generated blob raw content and put it into writer queue
            static_cast<TDerived&>(*this).ProcessItemImpl(item.PreallocatedLocation, item.Merger.CreateDiskBlob(Arena));
        }
    };

} // NKikimr

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
            TDiskBlobMerger Merger;
            TLogoBlobID BlobId;
            bool IsInline;

            TItem(ui64 id, ui32 numReads, const TDiskPart& preallocatedLocation, const TDiskBlobMerger& merger,
                    const TLogoBlobID& blobId, bool isInline)
                : Id(id)
                , NumReads(numReads)
                , PreallocatedLocation(preallocatedLocation)
                , Merger(merger)
                , BlobId(blobId)
                , IsInline(isInline)
            {}
        };

    public:
        const TString VDiskLogPrefix;

    private:
        TQueue<TItem> ItemQueue;
        bool Started = false;
        TRopeArena& Arena;
        const TBlobStorageGroupType GType;
        const bool AddHeader;

    public:
        TDeferredItemQueueBase(const TString& prefix, TRopeArena& arena, TBlobStorageGroupType gtype, bool addHeader)
            : VDiskLogPrefix(prefix)
            , Arena(arena)
            , GType(gtype)
            , AddHeader(addHeader)
        {}

        template<typename... TArgs>
        void Put(TArgs&&... args) {
            Y_VERIFY_S(!Started, VDiskLogPrefix);
            ItemQueue.emplace(std::forward<TArgs>(args)...);
        }

        template<typename... TArgs>
        void Start(TArgs&&... args) {
            Y_VERIFY_S(!Started, VDiskLogPrefix);
            Started = true;
            static_cast<TDerived&>(*this).StartImpl(std::forward<TArgs>(args)...);
            ProcessItemQueue();
        }

        void AddReadDiskBlob(ui64 id, TRope&& buffer, ui8 partIdx) {
            Y_VERIFY_S(Started, VDiskLogPrefix);
            Y_VERIFY_S(ItemQueue, VDiskLogPrefix);
            TItem& item = ItemQueue.front();
            Y_VERIFY_S(item.Id == id, VDiskLogPrefix);
            item.Merger.AddPart(std::move(buffer), GType, TLogoBlobID(item.BlobId, partIdx + 1));
            Y_VERIFY_S(item.NumReads > 0, VDiskLogPrefix);
            if (!--item.NumReads) {
                ProcessItemQueue();
            }
        }

        bool AllProcessed() {
            return ItemQueue.empty();
        }

        void Finish() {
            Y_VERIFY_S(Started, VDiskLogPrefix);
            Y_VERIFY_S(ItemQueue.empty(), VDiskLogPrefix);
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
            // get newly generated blob raw content and put it into writer queue
            static_cast<TDerived&>(*this).ProcessItemImpl(item.PreallocatedLocation, item.Merger.CreateDiskBlob(Arena,
                AddHeader), item.IsInline);
        }
    };

} // NKikimr

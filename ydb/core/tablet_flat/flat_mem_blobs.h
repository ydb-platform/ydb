#pragma once

#include "flat_sausage_solid.h"
#include "flat_sausage_fetch.h"
#include "util_store.h"

#include <ydb/library/actors/util/shared_data.h>

#include <util/generic/ptr.h>

namespace NKikimr {
namespace NTable {
namespace NMem {

    class TBlobs : private TNonCopyable {
        using TStore = NUtil::TConcurrentStore<NPageCollection::TMemGlob>;

    public:
        explicit TBlobs(ui64 head)
            : Head(head)
        { }

        ui64 GetBytes() const noexcept
        {
            return Bytes;
        }

        size_t Size() const noexcept
        {
            return Store.size();
        }

        TStore::TConstIterator Iterator() const noexcept
        {
            return Store.Iterator();
        }

        ui64 Tail() const noexcept
        {
            return Head + Store.size();
        }

        const NPageCollection::TMemGlob& GetRaw(ui64 index) const noexcept
        {
            return Store[index];
        }

        const NPageCollection::TMemGlob& Get(ui64 ref) const noexcept
        {
            Y_ABORT_UNLESS(ref >= Head && ref < Tail(), "ELargeObj ref is out of cache");

            return Store[ref - Head];
        }

        ui64 Push(const NPageCollection::TMemGlob &glob) noexcept
        {
            return Push(glob.GId, glob.Data);
        }

        ui64 Push(const NPageCollection::TGlobId& glob, TSharedData data) noexcept
        {
            Y_ABORT_UNLESS(glob.Logo.BlobSize(), "Blob cannot have zero bytes");

            Store.emplace_back(glob, std::move(data));
            Bytes += glob.Logo.BlobSize();

            const ui64 ref = Head + (Store.size() - 1);

            return ref;
        }

        void Assign(TArrayRef<NPageCollection::TLoadedPage> pages) noexcept
        {
            for (auto &one : pages) {
                Y_ABORT_UNLESS(one.PageId < Store.size());

                Store[one.PageId].Data = std::move(one.Data);
            }
        }

        void Commit(size_t count, TArrayRef<const NPageCollection::TMemGlob> pages) noexcept
        {
            if (count > 0) {
                size_t currentSize = Store.size();
                Y_ABORT_UNLESS(count <= currentSize);
                Store.Enumerate(currentSize - count, currentSize, [pages](size_t, NPageCollection::TMemGlob& blob) {
                    Y_ABORT_UNLESS(blob.GId.Logo.TabletID() == 0);
                    const ui32 ref = blob.GId.Logo.Cookie();
                    const auto& fixedBlob = pages.at(ref);
                    Y_ABORT_UNLESS(fixedBlob.GId.Logo.TabletID() != 0);
                    blob.GId = fixedBlob.GId;
                });
            }
        }

        void Rollback(size_t count) noexcept
        {
            if (count > 0) {
                size_t currentSize = Store.size();
                Y_ABORT_UNLESS(count <= currentSize);
                Store.Enumerate(currentSize - count, currentSize, [this](size_t, NPageCollection::TMemGlob& blob) {
                    Bytes -= blob.GId.Logo.BlobSize();
                });
                Store.truncate(currentSize - count);
            }
        }

    public:
        const ui64 Head;

    private:
        ui64 Bytes = 0;
        TStore Store;
    };

}
}
}

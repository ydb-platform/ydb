#pragma once

#include "flat_sausage_solid.h"
#include "flat_sausage_fetch.h"
#include "util_store.h" 
 
#include <ydb/core/base/shared_data.h>
 
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
            Y_VERIFY(ref >= Head && ref < Tail(), "ELargeObj ref is out of cache");

            return Store[ref - Head]; 
        }

        ui64 Push(const NPageCollection::TMemGlob &glob) noexcept
        {
            return Push(glob.GId, glob.Data);
        }

        ui64 Push(const NPageCollection::TGlobId& glob, TSharedData data) noexcept
        {
            Y_VERIFY(glob.Logo.BlobSize(), "Blob cannot have zero bytes");

            Store.emplace_back(glob, std::move(data)); 
            Bytes += glob.Logo.BlobSize(); 

            return Head + (Store.size() - 1); 
        }

        void Assign(TArrayRef<NPageCollection::TLoadedPage> pages) noexcept
        {
            for (auto &one : pages) {
                Y_VERIFY(one.PageId < Store.size()); 

                Store[one.PageId].Data = std::move(one.Data); 
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

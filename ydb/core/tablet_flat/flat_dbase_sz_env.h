#pragma once

#include "flat_page_iface.h"
#include "flat_part_store.h"
#include <util/generic/hash_set.h>
#include <util/generic/cast.h>

namespace NKikimr {
namespace NTable {

    struct TSizeEnv : public IPages {
        using TPageCollection = NTabletFlatExecutor::TPrivatePageCache::TPageCollection;

        TSizeEnv(IPages* env)
            : Env(env)
        {
        }

        TResult Locate(const TMemTable*, ui64, ui32) override
        {
            Y_TABLET_ERROR("IPages::Locate(TMemTable*, ...) shouldn't be used here");
        }

        TResult Locate(const TPart *part, ui64 ref, ELargeObj lob) override
        {
            auto *partStore = CheckedCast<const NTable::TPartStore*>(part);

            auto *info = partStore->Locate(lob, ref);
            AddPageSize(info->PageCollection.Get(), info->PageCollection->GetLocation(ref));

            return { true, nullptr };
        }

        const TSharedData* TryGetPage(const TPart* part, TPageLocation location, TGroupId groupId) override
        {
            auto *partStore = CheckedCast<const NTable::TPartStore*>(part);
            auto *collection = partStore->PageCollections.at(groupId.Index).Get();

            switch (location.Type) {
                case EPage::FlatIndex:
                case EPage::BTreeIndex:
                    // need index pages to continue counting
                    // do not count index
                    // if these pages are not in memory, data won't be counted in precharge
                    return Env->TryGetPage(part, location, groupId);
                default:
                    AddPageSize(collection->PageCollection.Get(), location);
                    return nullptr;
            }
        }

        ui64 GetSize() const {
            return Bytes;
        }

    private:
        void AddPageSize(const NPageCollection::IPageCollection *collection, TPageLocation location)
        {
            if (Touched[collection].insert(location.Offset).second) {
                Pages++;
                Bytes += location.Size;
            }
        }

    private:
        IPages* Env;
        THashMap<const NPageCollection::IPageCollection*, THashSet<TPageOffset>> Touched;
        ui64 Pages = 0;
        ui64 Bytes = 0;
    };

}
}

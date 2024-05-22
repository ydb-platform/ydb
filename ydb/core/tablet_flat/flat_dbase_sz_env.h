#pragma once

#include "flat_page_iface.h"
#include "flat_part_store.h"
#include <util/generic/hash_set.h>
#include <util/generic/cast.h>

namespace NKikimr {
namespace NTable {

    struct TSizeEnv : public IPages {
        using TInfo = NTabletFlatExecutor::TPrivatePageCache::TInfo;

        TSizeEnv(IPages* env)
            : Env(env)
        {
        }

        TResult Locate(const TMemTable*, ui64, ui32) noexcept override
        {
            Y_ABORT("IPages::Locate(TMemTable*, ...) shouldn't be used here");
        }

        TResult Locate(const TPart *part, ui64 ref, ELargeObj lob) noexcept override
        {
            auto *partStore = CheckedCast<const NTable::TPartStore*>(part);

            AddPageSize(partStore->Locate(lob, ref), ref);

            return { true, nullptr };
        }

        const TSharedData* TryGetPage(const TPart* part, TPageId pageId, TGroupId groupId) override
        {
            auto *partStore = CheckedCast<const NTable::TPartStore*>(part);

            auto info = partStore->PageCollections.at(groupId.Index).Get();
            auto type = EPage(info->PageCollection->Page(pageId).Type);
            
            switch (type) {
                case EPage::FlatIndex:
                case EPage::BTreeIndex:
                    // need index pages to continue counting
                    // do not count index
                    // if these pages are not in memory, data won't be counted in precharge
                    return Env->TryGetPage(part, pageId, groupId);
                default:
                    AddPageSize(partStore->PageCollections.at(groupId.Index).Get(), pageId);
                    return nullptr;
            }
        }

        ui64 GetSize() const {
            return Bytes;
        }

    private:
        void AddPageSize(TInfo *info, TPageId page) noexcept
        {
            if (Touched[info].insert(page).second) {
                Pages++;
                Bytes += info->PageCollection->Page(page).Size;
            }
        }

    private:
        IPages* Env;
        THashMap<const void*, THashSet<TPageId>> Touched;
        ui64 Pages = 0;
        ui64 Bytes = 0;
    };

}
}

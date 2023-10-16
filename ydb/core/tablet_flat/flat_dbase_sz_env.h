#pragma once

#include "flat_page_iface.h"
#include "flat_part_store.h"
#include <util/generic/hash_set.h>
#include <util/generic/cast.h>

namespace NKikimr {
namespace NTable {

    struct TSizeEnv : public IPages {
        using TInfo = NTabletFlatExecutor::TPrivatePageCache::TInfo;

        TResult Locate(const TMemTable*, ui64, ui32) noexcept override
        {
            Y_ABORT("IPages::Locate(TMemTable*, ...) shouldn't be used here");
        }

        TResult Locate(const TPart *part, ui64 ref, ELargeObj lob) noexcept override
        {
            auto *partStore = CheckedCast<const NTable::TPartStore*>(part);

            return { true, Touch(partStore->Locate(lob, ref), ref) };
        }

        const TSharedData* TryGetPage(const TPart* part, TPageId page, TGroupId groupId) override
        {
            auto *partStore = CheckedCast<const NTable::TPartStore*>(part);

            return Touch(partStore->PageCollections.at(groupId.Index).Get(), page);
        }

        ui64 GetSize() const {
            return Bytes;
        }

    private:
        const TSharedData* Touch(TInfo *info, TPageId page) noexcept
        {
            if (Touched[info].insert(page).second) {
                Pages++;
                Bytes += info->PageCollection->Page(page).Size;
            }

            return nullptr;
        }

    private:
        THashMap<const void*, THashSet<TPageId>> Touched;
        ui64 Pages = 0;
        ui64 Bytes = 0;
    };

}
}

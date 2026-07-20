#pragma once
#include "flat_sausage_gut.h"
#include "flat_page_iface.h"
#include "shared_handle.h"
#include "shared_cache_events.h"
#include <ydb/core/util/page_map.h>

namespace NKikimr {
namespace NTabletFlatExecutor {

using namespace NSharedCache;

class TPrivatePageCache {
public:
    // TODO: provide a way to get TSharedData from used TSharedPageRef directly
    struct TPinnedPage {
        TSharedPageRef SharedBody;
        TSharedData PinnedBody;

        explicit TPinnedPage(TSharedPageRef sharedBody)
            : SharedBody(std::move(sharedBody))
            , PinnedBody(TPinnedPageRef(SharedBody).GetData())
        {}
    };

    struct TPageCollection;

    struct TStats {
        ui64 PageCollections = 0;
        ui64 SharedBodyBytes = 0;
        ui64 StickyBytes = 0;
        ui64 TryKeepInMemoryBytes = 0;
    };

    struct TPage : TNonCopyable {
        const TPageOffset Offset;
        const size_t Size;
        const TSharedPageRef SharedBody;
        const TPageCollection* const PageCollection;

        TPage(TPageOffset offset, size_t size, TSharedPageRef sharedBody, TPageCollection* pageCollection);

        bool IsSticky() const noexcept {
            // Note: because this method doesn't use TPage flags
            // it may be called from multiple threads later
            // also it doesn't affect offloading, only touched memory counting
            return PageCollection->IsStickyPage(Offset);
        }
    };

    struct TPageCollection : public TThrRefBase {
        TPage* FindPage(TPageId pageId) const noexcept {
            return FindPage(PageCollection->GetLocation(pageId).Offset);
        }

        TPage* FindPage(NTable::NPage::TPageOffset offset) const noexcept {
            auto it = PageMap.find(offset);
            return it != PageMap.end() ? it->Get() : nullptr;
        }

        EPage GetPageType(TPageId pageId) const noexcept {
            return EPage(PageCollection->Page(pageId).Type);
        }

        ui64 GetPageSize(TPageId pageId) const noexcept {
            return PageCollection->Page(pageId).Size;
        }

        const auto& GetPageMap() const noexcept {
            return PageMap;
        }

        bool IsStickyPage(TPageId pageId) const noexcept {
            return IsStickyPage(PageCollection->GetLocation(pageId).Offset);
        }

        bool IsStickyPage(TPageOffset offset) const noexcept {
            return StickyPages.contains(offset);
        }

        ECacheMode GetCacheMode() const noexcept {
            return CacheMode;
        }

        // Mutable methods can be only called on a construction stage or from a Private Cache
        // Otherwise stat counters would be out of sync

        bool AddPage(TPageId pageId, TSharedPageRef sharedBody) {
            auto location = PageCollection->GetLocation(pageId);
            return AddPage(location, std::move(sharedBody));
        }

        bool AddPage(TPageLocation location, TSharedPageRef sharedBody) {
            return PageMap.emplace(MakeHolder<TPage>(
                location.Offset, location.Size, std::move(sharedBody), this)).second;
        }

        bool AddStickyPage(TPageId pageId, TSharedPageRef sharedBody) {
            return AddStickyPage(PageCollection->GetLocation(pageId), std::move(sharedBody));
        }

        bool AddStickyPage(TPageLocation location, TSharedPageRef sharedBody) {
            Y_ENSURE(sharedBody.IsUsed());
            AddPage(location, sharedBody);
            return StickyPages.emplace(location.Offset, std::move(sharedBody)).second;
        }

        bool DropPage(TPageId pageId) {
            return DropPage(PageCollection->GetLocation(pageId).Offset);
        }

        bool DropPage(NTable::NPage::TPageOffset offset, ui64 *size = nullptr) {
            auto it = PageMap.find(offset);
            if (it == PageMap.end()) return false;
            if (size) *size = it->Get()->Size;
            PageMap.erase(it);
            return true;
        }

        void Clear() {
            PageMap.clear();
            StickyPages.clear();
        }

        void SetCacheMode(ECacheMode cacheMode) {
            CacheMode = cacheMode;
        }

        const TLogoBlobID Id;
        const TIntrusiveConstPtr<NPageCollection::IPageCollection> PageCollection;

        explicit TPageCollection(TIntrusiveConstPtr<NPageCollection::IPageCollection> pageCollection);
        TPageCollection(const TPageCollection &pageCollection);

    private:
        struct TPageByOffsetHash {
            size_t operator()(const THolder<TPage>& page) const { return THash<TPageOffset>()(page->Offset); }
            size_t operator()(TPageOffset offset) const { return THash<TPageOffset>()(offset); }
        };

        struct TPageByOffsetEq {
            bool operator()(const THolder<TPage>& a, const THolder<TPage>& b) const { return a->Offset == b->Offset; }
            bool operator()(const THolder<TPage>& a, TPageOffset b) const { return a->Offset == b; }
            bool operator()(TPageOffset a, const THolder<TPage>& b) const { return a == b->Offset; }
        };

        // all pages in PageMap have valid unused shared body
        THashSet<THolder<TPage>, TPageByOffsetHash, TPageByOffsetEq> PageMap;

        // storing sticky pages used refs guarantees that they won't be offload from Shared Cache
        THashMap<TPageOffset, TSharedPageRef> StickyPages;
        ECacheMode CacheMode = ECacheMode::Regular;
    };

public:
    TPageCollection* FindPageCollection(const TLogoBlobID &id) const;
    TPageCollection* GetPageCollection(const TLogoBlobID &id) const;
    THashMap<TLogoBlobID, THashSet<TPageOffset>> AddPageCollection(TIntrusivePtr<TPageCollection> pageCollection);
    void DropPageCollection(TPageCollection *pageCollection);

    const TStats& GetStats() const { return Stats; }

    TSharedPageRef TryGetPage(TPageId pageId, TPageCollection *pageCollection) {
        return TryGetPage(pageCollection->PageCollection->GetLocation(pageId).Offset, pageCollection);
    }
    TSharedPageRef TryGetPage(TPageOffset offset, TPageCollection *pageCollection);

    void DropPage(TPageId pageId, TPageCollection *pageCollection) {
        DropPage(pageCollection->PageCollection->GetLocation(pageId).Offset, pageCollection);
    }
    void DropPage(TPageOffset offset, TPageCollection *pageCollection);
    void AddPage(TPageId pageId, TSharedPageRef sharedBody, TPageCollection *pageCollection) {
        AddPage(pageCollection->PageCollection->GetLocation(pageId), std::move(sharedBody), pageCollection);
    }
    void AddPage(TPageLocation location, TSharedPageRef sharedBody, TPageCollection *pageCollection);
    void AddStickyPage(TPageId pageId, TSharedPageRef sharedBody, TPageCollection *pageCollection) {
        AddStickyPage(pageCollection->PageCollection->GetLocation(pageId), std::move(sharedBody), pageCollection);
    }
    void AddStickyPage(TPageLocation location, TSharedPageRef sharedBody, TPageCollection *pageCollection);
    bool UpdateCacheMode(ECacheMode newCacheMode, TPageCollection *pageCollection);

    THashMap<TLogoBlobID, TIntrusivePtr<TPageCollection>> DetachPrivatePageCache();

private:
    THashMap<TLogoBlobID, TIntrusivePtr<TPageCollection>> PageCollections;

    TStats Stats;
};

}}

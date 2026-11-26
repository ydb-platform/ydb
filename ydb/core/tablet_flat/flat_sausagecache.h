#pragma once
#include "flat_sausage_gut.h"
#include "flat_page_iface.h"
#include "flat_transaction_pages.h"
#include "shared_handle.h"
#include "shared_cache_events.h"
#include <ydb/core/util/page_map.h>

namespace NKikimr::NSharedCache {
class TSharedCachePages;
}

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

    using TStats = TPageCacheStats;

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

        bool AddPage(TPageOffset offset, size_t size, TSharedPageRef sharedBody) {
            return PageMap.emplace(MakeHolder<TPage>(
                offset, size, std::move(sharedBody), this)).second;
        }

        bool AddStickyPage(TPageOffset offset, size_t size, TSharedPageRef sharedBody) {
            Y_ENSURE(sharedBody.IsUsed());
            AddPage(offset, size, sharedBody);
            return StickyPages.emplace(offset, std::move(sharedBody)).second;
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

        NTable::NPage::TPageLocation GetLocation(ui32 pageId) const {
            return PageCollection->GetLocation(pageId);
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

    TSharedPageRef TryGetPage(TPageOffset offset, TPageCollection *pageCollection);

    void DropPage(TPageOffset offset, TPageCollection *pageCollection);
    void AddPage(TPageOffset offset, size_t size, const TSharedPageRef& sharedBody, TPageCollection *pageCollection);
    void AddStickyPage(TPageOffset offset, size_t size, TSharedPageRef sharedBody, TPageCollection *pageCollection);
    bool UpdateCacheMode(ECacheMode newCacheMode, TPageCollection *pageCollection);

    THashMap<TLogoBlobID, TIntrusivePtr<TPageCollection>> DetachPrivatePageCache();

private:
    THashMap<TLogoBlobID, TIntrusivePtr<TPageCollection>> PageCollections;

    TStats Stats;
};

struct TPrivatePageCacheRestartState final : public IPageCacheRestartState {
    THashMap<TLogoBlobID, TIntrusivePtr<TPrivatePageCache::TPageCollection>> PageCollections;
};

struct TPrivateLoadedCollections final : public IPageCacheLoadedCollections {
    TVector<TIntrusivePtr<TPrivatePageCache::TPageCollection>> PageCollections;
};

TWrittenPageCache MakePrivateWrittenPageCache(NSharedCache::TSharedCachePages& pages) noexcept;

// The holder stays stable when boot recreates the private cache
class TPrivatePageCacheBackend final : public IExecutorPageCache {
public:
    explicit TPrivatePageCacheBackend(THolder<TPrivatePageCache>& cache) noexcept
        : Cache_(cache)
    {}

    bool PrepareRegistration() override;
    void CaptureForRestart(THolder<IPageCacheRestartState>& restartState) const override;
    void CaptureForRestart(
        const NTable::TPartStore& partStore, THolder<IPageCacheLoadedCollections>& loaded) const override;
    void MergeForRestart(
        THolder<IPageCacheRestartState>& restartState, const IPageCacheLoadedCollections* loaded) const override;
    THolder<NTable::IPartLoaderEnv> MakeLoaderEnvironment(
        TConstArrayRef<NPageCollection::TLargeGlobId> collections = {},
        const IPageCacheRestartState* restartState = nullptr) const override;
    void PrepareCollectionModes(
        const NTable::TPartStore& partStore, TVector<ECacheMode>& modes, const TVector<bool>& stickyGroups) override;
    TVector<bool> UpdateCollectionModes(
        const NTable::TPartStore& partStore, TVector<ECacheMode>& modes, const TVector<bool>& stickyGroups) override;
    void ReadCollectionModes(const NTable::TPartStore& partStore, TVector<ECacheMode>& modes,
        const TVector<bool>& stickyGroups) const override;
    void RegisterCollection(const NTable::TPartStore& partStore, ui32 slot, ECacheMode cacheMode,
        TVector<NPageCollection::TBtreeSeed> btreeSeeds, bool replayStickyWalk, TPageCacheEventSender send) override;
    void RegisterPseudoCollection(const NTable::TPartStore& partStore, TPageCacheEventSender send) override;
    THolder<NActors::IEventBase> DetachCollection(const TLogoBlobID& collectionId) override;
    std::unique_ptr<TSeat> MakeSeat(ui64 uniqueId, TAutoPtr<ITransaction> transaction) override;
    void WithTransactionPages(NTable::TDatabase& database, TSeat& seat,
        TFunctionRef<void(ITransactionPages&, TTransactionEnv&)> execute) override;
    EPageCacheEvent EventKind(ui32 eventType) const noexcept override;
    TPageCacheResult ReadResult(NActors::IEventBase& event) const override;
    THolder<NActors::IEventBase> MakePageRequest(NBlockIO::EPriority priority,
        TIntrusiveConstPtr<NPageCollection::IPageCollection> collection,
        TVector<NTable::NPage::TPageLocation> locations, ui64 cookie = 0) const override;
    THolder<NActors::IEventBase> MakeCollectionUpdate(TIntrusiveConstPtr<NPageCollection::IPageCollection> collection,
        ECacheMode mode, TVector<NPageCollection::TBtreeSeed> btreeSeeds = {}) const override;
    THolder<NActors::IEventBase> MakeUnregisterRequest() const override;
    THolder<NActors::IEventBase> MakeTransactionRequest(const TLogoBlobID& collectionId,
        TVector<NTable::NPage::TPageLocation> locations, TIntrusivePtr<NPageCollection::TPagesWaitPad> waitPad,
        NWilson::TTraceId traceId) override;
    void ProcessCacheUpdate(const NActors::IEventBase& update) override;
    THolder<NActors::IEventBase> MakeStickyPageRequest(const NActors::IEventBase& notification) override;
    EPageResultStatus ProcessLoadedPages(NActors::IEventBase& result, bool sticky) override;
    bool AdoptLoadedPages(TSeat& seat, NActors::IEventBase* result) override;
    void ReleasePins(TSeat& seat) override;
    bool ReadStats(TPageCacheStats& stats) const noexcept override;

private:
    void RegisterCollection(const TIntrusivePtr<TPrivatePageCache::TPageCollection>& pageCollection,
        ECacheMode cacheMode, TVector<NPageCollection::TBtreeSeed> btreeSeeds, bool replayStickyWalk,
        TPageCacheEventSender send);

    THolder<TPrivatePageCache>& Cache_;
};
}}

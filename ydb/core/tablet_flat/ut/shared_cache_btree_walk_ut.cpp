#include <ydb/core/tablet_flat/test/libs/rows/layout.h>

#include <library/cpp/testing/unittest/registar.h>

#include <flat_page_btree_index_writer.h>
#include <shared_cache_btree_walk.h>
#include <shared_cache_pages.h>
#include <shared_sausagecache_state.h>

namespace NKikimr::NSharedCache {
namespace {

    class TPageCollectionStub final : public NPageCollection::IPageCollection {
    public:
        explicit TPageCollectionStub(TLogoBlobID id)
            : Id(std::move(id))
        {
        }

        const TLogoBlobID& Label() const noexcept override {
            return Id;
        }

        ui32 Total() const noexcept override {
            return 1;
        }

        NPageCollection::TInfo Page(ui32) const override {
            return {};
        }

        NPageCollection::TBorder Bounds(ui32) const override {
            return {};
        }

        NPageCollection::TBorder Bounds(const TPageLocation&) const override {
            return {};
        }

        NPageCollection::TGlobId Glob(ui32) const override {
            return {};
        }

        bool Verify(ui32, TArrayRef<const char>) const override {
            return false;
        }

        bool Verify(const TPageLocation&, TArrayRef<const char>) const override {
            return false;
        }

        size_t BackingSize() const noexcept override {
            return 0;
        }

        TPageLocation GetLocation(ui32) const override {
            return {};
        }

    private:
        TLogoBlobID Id;
    };

    class TWalkHostMock final : public ICacheBTreeWalkHost {
    public:
        TCollection Collection;
        TPendingInMemoryPages PendingPages;
        TIntrusivePtr<TSharedCachePages> CachePages = new TSharedCachePages;
        TVector<TVector<TPageLocation>> StickyBatches;
        ui32 CancelledRequests = 0;
        ui32 ScheduledContinuations = 0;
        ui32 ExpiredChecks = 0;
        ui32 CollectionLookups = 0;
        TLogoBlobID FetchWalkCollectionId;
        bool AllowFetch = false;

        TWalkHostMock() {
            Collection.Id = TLogoBlobID(1, 1, 1);
            Collection.PageCollection = MakeIntrusiveConst<TPageCollectionStub>(Collection.Id);
        }

        TCollection* FindWalkCollection(const TLogoBlobID& id) override {
            ++CollectionLookups;
            return id == Collection.Id ? &Collection : nullptr;
        }

        TPendingInMemoryPages& PendingWalkPages() override {
            return PendingPages;
        }

        TSharedCachePages* WalkCachePages() override {
            return CachePages.Get();
        }

        void FetchWalkIndexLevel(TCollection&, TVector<TPageLocation>&&, const TLogoBlobID& walkCollectionId) override {
            UNIT_ASSERT_C(AllowFetch, "Unexpected index fetch");
            FetchWalkCollectionId = walkCollectionId;
        }

        void SendWalkStickyPages(TCollection&, const TActorId&, const TVector<TPageLocation>& locations) override {
            StickyBatches.push_back(locations);
        }

        void CancelQueuedWalkRequestsAndPump(const TLogoBlobID&) override {
            ++CancelledRequests;
        }

        void TryDropExpiredCollection(TCollection&) override {
            ++ExpiredChecks;
        }

        void ScheduleWalkContinuation() override {
            ++ScheduledContinuations;
        }

        void AddLoadedNode(const TPageLocation& location, TSharedData body) {
            UNIT_ASSERT_VALUES_EQUAL(location.Size, body.size());
            auto page =
                MakeIntrusive<TPage>(location.Offset, location.Size, location.Type, location.Crc32, &Collection);
            page->ProvideBody(std::move(body));
            UNIT_ASSERT(Collection.PageSet.insert(std::move(page)).second);
        }
    };

    NTable::NPage::TBtreeIndexNode::TChildV2 MakeChild(const TPageLocation& location) {
        return { location.Offset, location.Size, location.Crc32, 1, location.Size, 0, 0 };
    }

    TSharedData MakeNode(const TPageLocation& first, const TPageLocation& second) {
        NTable::NTest::TLayoutCook layout;
        layout.Col(0, 0, NScheme::NTypeIds::Uint32).Key({ 0 });
        NTable::NPage::TBtreeIndexNodeWriter<NTable::NPage::TBtreeIndexNode::TChildV2> writer(
            new NTable::TPartScheme(layout.RowScheme()->Cols), {});
        const TCell key = TCell::Make(ui32(100));
        writer.AddChild(MakeChild(first));
        writer.AddKey(TArrayRef<const TCell>(&key, 1));
        writer.AddChild(MakeChild(second));
        return writer.Finish();
    }

    TEvAttach::TBtreeSeed MakeSeed(const TWalkHostMock& host, ui32 pageId) {
        TEvAttach::TBtreeSeed seed;
        seed.IndexCollectionId = TLogoBlobID(1, 1, 2);
        seed.DataCollectionId = host.Collection.Id;
        seed.Root = TPageLocation::FromPageIndex(pageId, 10, NTable::NPage::EPage::BTreeIndexV2, pageId + 1);
        seed.LevelCount = 1;
        return seed;
    }

} // namespace

Y_UNIT_TEST_SUITE(TCacheBTreeWalkController) {
    Y_UNIT_TEST(MissingInMemoryIndexUsesPendingQueue) {
        TWalkHostMock host;
        TCacheBTreeWalkController walks(host);
        const TActorId owner(1, TStringBuf("owner"));
        auto seed = MakeSeed(host, 0);
        seed.IndexCollectionId = host.Collection.Id;
        host.Collection.InMemoryOwners.insert(owner);

        walks.UpdateSeeds(host.Collection, owner, { seed });
        walks.Advance();
        walks.Advance();
        const auto& pending = host.PendingPages.at(host.Collection.Id);
        UNIT_ASSERT_VALUES_EQUAL(pending.size(), 1);
        UNIT_ASSERT(pending.contains(seed.Root));
        // Capacity-blocked index reads wait for loader progress without spinning self-wakeups.
        UNIT_ASSERT_VALUES_EQUAL(host.ScheduledContinuations, 0);

        walks.UpdateSeeds(host.Collection, owner, {});
        UNIT_ASSERT(pending.contains(seed.Root));
        UNIT_ASSERT(walks.IsIdle(host.Collection.Id));
    }

    Y_UNIT_TEST(BlockedIndexAssociationPreventsExpiry) {
        TWalkHostMock host;
        TCacheBTreeWalkController walks(host);
        const TActorId owner(1, TStringBuf("owner"));
        const auto seed = MakeSeed(host, 0);

        walks.UpdateSeeds(host.Collection, owner, { seed });
        walks.DropForIndexCollection(seed.IndexCollectionId);
        UNIT_ASSERT(!walks.IsIdle(host.Collection.Id));

        walks.UpdateSeeds(host.Collection, owner, {});
        UNIT_ASSERT(walks.IsIdle(host.Collection.Id));
        walks.EraseCollection(host.Collection.Id);
    }

    Y_UNIT_TEST(IdenticalSeedsDoNotRestartWalk) {
        TWalkHostMock host;
        TCacheBTreeWalkController walks(host);
        const TActorId owner(1, TStringBuf("owner"));
        const auto current = MakeSeed(host, 0);
        const auto historic = MakeSeed(host, 1);

        walks.UpdateSeeds(host.Collection, owner, { current, historic });
        walks.UpdateSeeds(host.Collection, owner, { current, historic });
        UNIT_ASSERT_VALUES_EQUAL(host.CancelledRequests, 0);
    }

    Y_UNIT_TEST(ReorderedSeedsRestartWalk) {
        TWalkHostMock host;
        TCacheBTreeWalkController walks(host);
        const TActorId owner(1, TStringBuf("owner"));
        const auto current = MakeSeed(host, 0);
        const auto historic = MakeSeed(host, 1);

        walks.UpdateSeeds(host.Collection, owner, { current, historic });
        walks.UpdateSeeds(host.Collection, owner, { historic, current });
        UNIT_ASSERT_VALUES_EQUAL(host.CancelledRequests, 1);
    }

    Y_UNIT_TEST(MultiLevelWalkSplitsDataPageBatches) {
        TWalkHostMock host;
        TCacheBTreeWalkController walks(host);
        const TActorId owner(1, TStringBuf("owner"));
        constexpr ui64 dataPageSize = 3 * 1024 * 1024;
        const auto dataPage1 = TPageLocation::FromByteOffset(4000, dataPageSize, EPage::DataPage, 1);
        const auto dataPage2 = TPageLocation::FromByteOffset(5000, dataPageSize, EPage::DataPage, 2);
        const auto dataPage3 = TPageLocation::FromByteOffset(6000, dataPageSize, EPage::DataPage, 3);
        const auto dataPage4 = TPageLocation::FromByteOffset(7000, dataPageSize, EPage::DataPage, 4);

        auto firstBody = MakeNode(dataPage1, dataPage2);
        auto secondBody = MakeNode(dataPage3, dataPage4);
        const auto first = TPageLocation::FromByteOffset(2000, firstBody.size(), EPage::BTreeIndexV2, 5);
        const auto second = TPageLocation::FromByteOffset(3000, secondBody.size(), EPage::BTreeIndexV2, 6);
        auto rootBody = MakeNode(first, second);
        const auto root = TPageLocation::FromByteOffset(1000, rootBody.size(), EPage::BTreeIndexV2, 7);
        host.AddLoadedNode(root, std::move(rootBody));
        host.AddLoadedNode(first, std::move(firstBody));
        host.AddLoadedNode(second, std::move(secondBody));

        auto seed = MakeSeed(host, 0);
        seed.IndexCollectionId = host.Collection.Id;
        seed.Root = root;
        seed.LevelCount = 2;
        seed.QueueDataPages = true;
        seed.Sticky = true;
        host.Collection.InMemoryOwners.insert(owner);
        const auto blocker = TPageLocation::FromByteOffset(8000, 10, EPage::DataPage, 8);
        auto& pending = host.PendingPages[host.Collection.Id];
        pending.emplace(blocker);
        walks.UpdateSeeds(host.Collection, owner, { seed });
        walks.Advance();
        walks.Advance();
        walks.Advance();
        walks.FinishReady();

        UNIT_ASSERT_VALUES_EQUAL(host.StickyBatches.size(), 3);
        UNIT_ASSERT(host.StickyBatches[0] == TVector<TPageLocation>({ root, first, second }));
        UNIT_ASSERT(host.StickyBatches[1] == TVector<TPageLocation>({ dataPage1, dataPage2 }));
        UNIT_ASSERT(host.StickyBatches[2] == TVector<TPageLocation>({ dataPage3, dataPage4 }));
        UNIT_ASSERT_VALUES_EQUAL(pending.size(), 5);
        UNIT_ASSERT(pending.contains(dataPage1) && pending.contains(dataPage2));
        UNIT_ASSERT(pending.contains(dataPage3) && pending.contains(dataPage4));
        UNIT_ASSERT_VALUES_EQUAL(host.ScheduledContinuations, 2);
        UNIT_ASSERT_VALUES_EQUAL(host.ExpiredChecks, 1);

        // Queued pages remain collection preload work after the walk is withdrawn.
        walks.UpdateSeeds(host.Collection, owner, {});
        UNIT_ASSERT_VALUES_EQUAL(pending.size(), 5);
        UNIT_ASSERT(pending.contains(blocker));
        UNIT_ASSERT_VALUES_EQUAL(host.ExpiredChecks, 1);
    }

    Y_UNIT_TEST(QueuedDataPagesIgnoreExistingInMemoryPages) {
        TWalkHostMock host;
        TCacheBTreeWalkController walks(host);
        const TActorId owner(1, TStringBuf("owner"));
        const auto blocker = TPageLocation::FromByteOffset(2000, 10, EPage::DataPage, 1);
        auto seed = MakeSeed(host, 0);
        seed.IndexCollectionId = host.Collection.Id;
        seed.Root = TPageLocation::FromByteOffset(3000, 10, EPage::DataPage, 2);
        seed.LevelCount = 0;
        host.Collection.InMemoryOwners.insert(owner);
        auto& pending = host.PendingPages[host.Collection.Id];
        pending.emplace(blocker);

        walks.UpdateSeeds(host.Collection, owner, { seed });
        walks.Advance();
        UNIT_ASSERT_VALUES_EQUAL(pending.size(), 2);
        UNIT_ASSERT(pending.contains(seed.Root));
        UNIT_ASSERT_VALUES_EQUAL(host.ScheduledContinuations, 1);

        walks.Advance();
        walks.FinishReady();
        UNIT_ASSERT_VALUES_EQUAL(host.ExpiredChecks, 1);
        pending.erase(seed.Root);
        walks.FinishReady();
        UNIT_ASSERT_VALUES_EQUAL(host.ExpiredChecks, 1);
    }

    Y_UNIT_TEST(CompletionOnlyChecksChangedRuns) {
        TWalkHostMock host;
        TCacheBTreeWalkController walks(host);
        const TActorId owner(1, TStringBuf("owner"));
        auto seed = MakeSeed(host, 0);
        seed.IndexCollectionId = host.Collection.Id;
        seed.Root = TPageLocation::FromByteOffset(3000, 10, EPage::DataPage, 2);
        seed.LevelCount = 0;
        seed.QueueDataPages = true;
        host.Collection.InMemoryOwners.insert(owner);

        walks.UpdateSeeds(host.Collection, owner, { seed });
        walks.Advance();
        walks.Advance();
        walks.FinishReady();
        UNIT_ASSERT(!walks.HasActiveWalks());
        UNIT_ASSERT_VALUES_EQUAL(host.ExpiredChecks, 1);
        UNIT_ASSERT(host.PendingPages.at(host.Collection.Id).contains(seed.Root));

        // The final completion consumes its bookkeeping; later calls have no host side effects.
        host.CollectionLookups = 0;
        const ui32 continuations = host.ScheduledContinuations;
        walks.Advance();
        walks.FinishReady();
        UNIT_ASSERT_VALUES_EQUAL(host.CollectionLookups, 0);
        UNIT_ASSERT_VALUES_EQUAL(host.ExpiredChecks, 1);
        UNIT_ASSERT_VALUES_EQUAL(host.ScheduledContinuations, continuations);
    }

    Y_UNIT_TEST(CancelledRunWaitsForDispatchedFetch) {
        TWalkHostMock host;
        host.AllowFetch = true;
        TCacheBTreeWalkController walks(host);
        const TActorId owner(1, TStringBuf("owner"));
        auto seed = MakeSeed(host, 0);
        seed.IndexCollectionId = host.Collection.Id;

        walks.UpdateSeeds(host.Collection, owner, { seed });
        walks.Advance();
        UNIT_ASSERT_VALUES_EQUAL(host.FetchWalkCollectionId, host.Collection.Id);
        walks.FetchStarted(host.FetchWalkCollectionId);

        walks.UpdateSeeds(host.Collection, owner, {});
        UNIT_ASSERT(walks.HasActiveWalks());
        UNIT_ASSERT_VALUES_EQUAL(host.ExpiredChecks, 0);

        walks.FinishFetch(host.FetchWalkCollectionId);
        UNIT_ASSERT(!walks.HasActiveWalks());
        UNIT_ASSERT_VALUES_EQUAL(host.ExpiredChecks, 1);
    }

    Y_UNIT_TEST(CancelledWalkKeepsQueuedDataPages) {
        TWalkHostMock host;
        TCacheBTreeWalkController walks(host);
        const TActorId owner(1, TStringBuf("owner"));
        auto seed = MakeSeed(host, 0);
        seed.IndexCollectionId = host.Collection.Id;
        seed.Root = TPageLocation::FromByteOffset(3000, 10, EPage::DataPage, 2);
        seed.LevelCount = 0;
        host.Collection.InMemoryOwners.insert(owner);
        auto& pending = host.PendingPages[host.Collection.Id];
        pending.emplace(TPageLocation::FromByteOffset(2000, 10, EPage::DataPage, 1));

        walks.UpdateSeeds(host.Collection, owner, { seed });
        walks.Advance();
        walks.UpdateSeeds(host.Collection, owner, {});
        UNIT_ASSERT(!walks.HasActiveWalks());
        UNIT_ASSERT_VALUES_EQUAL(pending.size(), 2);
        UNIT_ASSERT(pending.contains(seed.Root));
        UNIT_ASSERT_VALUES_EQUAL(host.ExpiredChecks, 1);
    }
}

} // namespace NKikimr::NSharedCache

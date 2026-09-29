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

        TWalkHostMock() {
            Collection.Id = TLogoBlobID(1, 1, 1);
            Collection.PageCollection = MakeIntrusiveConst<TPageCollectionStub>(Collection.Id);
        }

        TCollection* FindWalkCollection(const TLogoBlobID& id) override {
            return id == Collection.Id ? &Collection : nullptr;
        }

        TPendingInMemoryPages& PendingWalkPages() override {
            return PendingPages;
        }

        TSharedCachePages* WalkCachePages() override {
            return CachePages.Get();
        }

        void FetchWalkIndexLevel(TCollection&, TVector<TPageLocation>&&, ui64) override {
            UNIT_FAIL("Unexpected index fetch");
        }

        void SendWalkStickyPages(TCollection&, const TActorId&, const TVector<TPageLocation>& locations) override {
            StickyBatches.push_back(locations);
        }

        void CancelQueuedWalkRequestsAndPump(ui64) override {
            ++CancelledRequests;
        }

        void TryDropExpiredCollection(TCollection&) override {
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

    Y_UNIT_TEST(MultiLevelWalkSplitsLeafBatches) {
        TWalkHostMock host;
        TCacheBTreeWalkController walks(host);
        const TActorId owner(1, TStringBuf("owner"));
        constexpr ui64 leafSize = 3 * 1024 * 1024;
        const auto leaf1 = TPageLocation::FromByteOffset(4000, leafSize, EPage::DataPage, 1);
        const auto leaf2 = TPageLocation::FromByteOffset(5000, leafSize, EPage::DataPage, 2);
        const auto leaf3 = TPageLocation::FromByteOffset(6000, leafSize, EPage::DataPage, 3);
        const auto leaf4 = TPageLocation::FromByteOffset(7000, leafSize, EPage::DataPage, 4);

        auto firstBody = MakeNode(leaf1, leaf2);
        auto secondBody = MakeNode(leaf3, leaf4);
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
        seed.QueueLeaves = false;
        seed.Sticky = true;
        walks.UpdateSeeds(host.Collection, owner, { seed });
        walks.Advance();

        UNIT_ASSERT_VALUES_EQUAL(host.StickyBatches.size(), 3);
        UNIT_ASSERT(host.StickyBatches[0] == TVector<TPageLocation>({ root, first, second }));
        UNIT_ASSERT(host.StickyBatches[1] == TVector<TPageLocation>({ leaf1, leaf2 }));
        UNIT_ASSERT(host.StickyBatches[2] == TVector<TPageLocation>({ leaf3, leaf4 }));
    }

    Y_UNIT_TEST(QueuedLeavesWaitForExistingInMemoryPages) {
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
        pending.emplace(blocker, 0);

        walks.UpdateSeeds(host.Collection, owner, { seed });
        walks.Advance();
        UNIT_ASSERT_VALUES_EQUAL(pending.size(), 1);
        UNIT_ASSERT(!pending.contains(seed.Root));

        pending.clear();
        walks.Advance();
        UNIT_ASSERT(pending.contains(seed.Root));
    }
}

} // namespace NKikimr::NSharedCache

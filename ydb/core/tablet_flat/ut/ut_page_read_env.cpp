#include <ydb/core/tablet_flat/flat_executor_tx_env.h>
#include <ydb/core/tablet_flat/flat_database.h>
#include <ydb/core/tablet_flat/test/libs/table/test_writer.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NTabletFlatExecutor {
namespace {

    class TReadTransaction final : public ITransaction {
        bool Execute(TTransactionContext&, const NActors::TActorContext&) override {
            return true;
        }

        void Complete(const NActors::TActorContext&) override {}
    };

    class TReadPageCollection final : public NTable::NTest::TStorePageCollection {
    public:
        TReadPageCollection(TIntrusiveConstPtr<NTable::NTest::TStore> store, ui32 room, TLogoBlobID label)
            : TStorePageCollection(std::move(store), room)
            , Label_(label)
        {}

        const TLogoBlobID& Label() const noexcept override {
            return Label_;
        }

    private:
        const TLogoBlobID Label_;
    };

    struct TReadFixture {
        TReadFixture()
            : Seat(1, TAutoPtr<ITransaction>(new TReadTransaction))
            , Env(Cache, Seat)
        {
            NTable::NTest::TLayoutCook layout;
            layout.Col(0, 0, NScheme::NTypeIds::Uint32).Col(0, 1, NScheme::NTypeIds::String).Key({ 0 });
            NTable::NPage::TConf conf;
            conf.Group(0).PageSize = 128;
            conf.SmallEdge = 8;
            conf.LargeEdge = 128;
            NTable::NTest::TPartCook cook(layout, conf);
            for (ui32 key : xrange(32)) {
                cook.AddN(key, TString(32, 'x'));
            }
            Source = cook.Finish().Lone();
            NTable::TPart::TParams params{
                .Epoch = Source->Epoch,
                .Scheme = Source->Scheme,
                .IndexPages = Source->IndexPages,
                .Blobs = Source->Blobs,
                .ByKeyPrefixes = Source->ByKeyPrefixes,
                .Large = Source->Large,
                .Small = Source->Small,
                .IndexesRawSize = Source->IndexesRawSize,
                .MinRowVersion = Source->MinRowVersion,
                .MaxRowVersion = Source->MaxRowVersion,
                .GarbageStats = Source->GarbageStats,
                .TxIdStats = Source->TxIdStats,
                .Layout = Source->Slots.GetLayout(),
            };
            Part = MakeHolder<NTable::TPartStore>(Source->Label, std::move(params), Source->Stat);
            for (ui32 room : xrange(Source->PageColls.size())) {
                const auto& label = Source->Label;
                auto descriptor = MakeIntrusive<TReadPageCollection>(
                    Source->Store, room, TLogoBlobID(label.TabletID(), label.Generation(), label.Step(),
                                             label.Channel(), label.BlobSize(), label.Cookie() + room));
                auto collection = MakeIntrusive<TPrivatePageCache::TPageCollection>(std::move(descriptor));
                Cache.AddPageCollection(collection);
                Part->PageCollections.push_back(std::move(collection));
            }
            const ui32 slot = Part->DataSlot({});
            for (ui32 id : xrange(Source->Store->PageCollectionPagesCount(slot))) {
                if (Source->Store->GetPageType(slot, id) == NTable::NPage::EPage::DataPage) {
                    Locations.push_back(Source->Store->GetPageLocation(slot, id));
                }
            }
            UNIT_ASSERT(Locations.size() >= 3);
            UNIT_ASSERT(Part->Small && Part->OuterSlot() < Part->PageCollections.size());
        }

        TSharedPageRef Load(const NTable::TPageLocation& location, bool sticky = false) {
            const ui32 slot = Part->DataSlot({});
            auto page = TSharedPageRef::MakePrivate(*Source->Store->GetPage(slot, location.Offset));
            auto* collection = Part->PageCollections[slot].Get();
            if (sticky) {
                Cache.AddStickyPage(location.Offset, location.Size, page, collection);
            } else {
                Cache.AddPage(location.Offset, location.Size, page, collection);
            }
            return page;
        }

        TPrivatePageCache Cache;
        TSeat Seat;
        TIntrusiveConstPtr<NTable::NTest::TPartStore> Source;
        THolder<NTable::TPartStore> Part;
        TVector<NTable::TPageLocation> Locations;
        TPageCollectionReadEnv Env;
    };

} // namespace

Y_UNIT_TEST_SUITE(TLegacyPageReadEnv) {
    Y_UNIT_TEST(BackendSupportsPrebootSeatsAndSynchronousFrames) {
        THolder<TPrivatePageCache> cache;
        TPrivatePageCacheBackend backend(cache);
        IExecutorPageCache& pageCache = backend;
        auto seat = pageCache.MakeSeat(1, TAutoPtr<ITransaction>(new TReadTransaction));
        UNIT_ASSERT(seat && !cache);
        pageCache.ReleasePins(*seat);
        UNIT_ASSERT(!cache);

        NTable::TDatabase database;
        ui32 calls = 0;
        for (ui32 attempt : xrange(2)) {
            cache = MakeHolder<TPrivatePageCache>();
            pageCache.WithTransactionPages(database, *seat, [&](ITransactionPages& pages, TTransactionEnv& txEnv) {
                ++calls;
                database.Begin(1, pages);
                IExecuting& executing = txEnv;
                executing.CleanupLoan(TLogoBlobID(1, 2, 3), 7);
                UNIT_ASSERT(txEnv.HasChanges());
                database.RollbackChanges();
                UNIT_ASSERT(!txEnv.HasChanges());
                database.Commit(1, false);
                if (attempt == 1) {
                    seat.reset();
                }
            });
        }
        UNIT_ASSERT_VALUES_EQUAL(calls, 2);
        UNIT_ASSERT(!seat);
    }

    Y_UNIT_TEST(PinsAreReusedWithoutExtraHeatOrAccounting) {
        TReadFixture f;
        const auto& location = f.Locations.front();
        auto owner = f.Load(location);
        const auto frequency = owner.GetHandle()->GetFrequency();
        NTable::IPages& pages = f.Env;
        const auto* first = pages.TryGetPage(f.Part.Get(), location, {});
        UNIT_ASSERT(first);
        UNIT_ASSERT_VALUES_EQUAL(first->size(), location.Size);
        const auto pinnedFrequency = owner.GetHandle()->GetFrequency();
        UNIT_ASSERT(pinnedFrequency == frequency || pinnedFrequency == frequency + 1);
        UNIT_ASSERT(pages.TryGetPage(f.Part.Get(), location, {}) == first);
        UNIT_ASSERT_VALUES_EQUAL(owner.GetHandle()->GetFrequency(), pinnedFrequency);
        UNIT_ASSERT_VALUES_EQUAL(f.Env.GetStats().NewlyPinnedPages, 1);
        UNIT_ASSERT_VALUES_EQUAL(f.Env.GetStats().NewlyPinnedBytes, location.Size);

        auto* collection = f.Part->PageCollections[f.Part->DataSlot({})].Get();
        UNIT_ASSERT_VALUES_EQUAL(f.Seat.Pinned.at(collection->Id).size(), 1);
        f.Cache.DropPage(location.Offset, collection);
        TPageCollectionReadEnv retry(f.Cache, f.Seat);
        NTable::IPages& retryPages = retry;
        UNIT_ASSERT(retryPages.TryGetPage(f.Part.Get(), location, {}) == first);
        UNIT_ASSERT_VALUES_EQUAL(retry.GetStats().NewlyPinnedPages, 0);
        UNIT_ASSERT_VALUES_EQUAL(retry.GetStats().NewlyPinnedBytes, 0);
        UNIT_ASSERT_VALUES_EQUAL(owner.GetHandle()->GetFrequency(), pinnedFrequency);
    }

    Y_UNIT_TEST(StickyPinsDoNotChargeTransactionBytes) {
        TReadFixture f;
        const auto& location = f.Locations.front();
        auto owner = f.Load(location, true);
        NTable::IPages& pages = f.Env;
        UNIT_ASSERT(pages.TryGetPage(f.Part.Get(), location, {}));
        UNIT_ASSERT_VALUES_EQUAL(f.Env.GetStats().NewlyPinnedPages, 1);
        UNIT_ASSERT_VALUES_EQUAL(f.Env.GetStats().NewlyPinnedBytes, 0);
        UNIT_ASSERT_VALUES_EQUAL(f.Env.GetStats().ToLoadPages, 0);
    }

    Y_UNIT_TEST(MissingPagesAreDeduplicatedAndSorted) {
        TReadFixture f;
        ITransactionPages& pages = f.Env;
        for (ui32 index : { 2, 0, 1, 2, 0 }) {
            UNIT_ASSERT(!pages.TryGetPage(f.Part.Get(), f.Locations[index], {}));
        }
        const ui64 bytes = f.Locations[0].Size + f.Locations[1].Size + f.Locations[2].Size;
        UNIT_ASSERT_VALUES_EQUAL(pages.GetStats().ToLoadPages, 3);
        UNIT_ASSERT_VALUES_EQUAL(pages.GetStats().ToLoadBytes, bytes);
        const auto requests = pages.ObtainToLoad();
        UNIT_ASSERT_VALUES_EQUAL(requests.size(), 1);
        const auto& locations = requests.at(f.Part->GetPageCollection(f.Part->DataSlot({}))->Label());
        UNIT_ASSERT_VALUES_EQUAL(locations.size(), 3);
        for (ui32 index : xrange(3)) {
            UNIT_ASSERT(locations[index].Offset == f.Locations[index].Offset);
        }
        UNIT_ASSERT(pages.ObtainToLoad().empty());
        UNIT_ASSERT(!pages.TryGetPage(f.Part.Get(), f.Locations[0], {}));
        UNIT_ASSERT_VALUES_EQUAL(pages.GetStats().ToLoadPages, 4);
    }

    Y_UNIT_TEST(TransactionRollbackClearsEffectsAndPreservesReadState) {
        TReadFixture f;
        const auto& location = f.Locations.front();
        auto owner = f.Load(location);
        NTable::TDatabase database;
        TPageCollectionTxEnv env(database, f.Cache, f.Seat);
        IExecuting& executing = env;
        ITransactionPages& pages = env;
        database.Begin(1, pages);
        const auto* data = pages.TryGetPage(f.Part.Get(), location, {});
        UNIT_ASSERT(data);
        executing.CleanupLoan(f.Part->Label, 7);
        UNIT_ASSERT(env.HasChanges());
        executing.EnableReadMissingReferences();
        const auto missing = pages.Locate(f.Part.Get(), 0, NTable::ELargeObj::Outer);
        UNIT_ASSERT(!missing.Need && !missing.Page);
        const auto missingBytes = executing.MissingReferencesSize();
        UNIT_ASSERT(missingBytes > 0);

        database.RollbackChanges();
        UNIT_ASSERT(!env.HasChanges());
        UNIT_ASSERT(pages.TryGetPage(f.Part.Get(), location, {}) == data);
        UNIT_ASSERT_VALUES_EQUAL(pages.GetStats().NewlyPinnedPages, 1);
        UNIT_ASSERT_VALUES_EQUAL(pages.GetStats().NewlyPinnedBytes, location.Size);
        UNIT_ASSERT_VALUES_EQUAL(pages.GetStats().ToLoadPages, 1);
        UNIT_ASSERT_VALUES_EQUAL(executing.MissingReferencesSize(), missingBytes);
        executing.DisableReadMissingReferences();
        UNIT_ASSERT_VALUES_EQUAL(pages.MissingReferencesSize(), 0);
        database.Commit(1, false);
    }

    Y_UNIT_TEST(MissingReferenceControlsPreserveNeedAndResetBytes) {
        TReadFixture f;
        ITransactionPages& pages = f.Env;
        pages.EnableReadMissingReferences();
        const ui64 bytes = Max<ui64>(1, f.Part->GetPageSize(NTable::ELargeObj::Outer, 0));
        for (ui32 call = 1; call <= 2; ++call) {
            const auto result = pages.Locate(f.Part.Get(), 0, NTable::ELargeObj::Outer);
            UNIT_ASSERT(!result.Need && !result.Page);
            UNIT_ASSERT_VALUES_EQUAL(pages.MissingReferencesSize(), call * bytes);
        }
        UNIT_ASSERT_VALUES_EQUAL(pages.GetStats().ToLoadPages, 1);
        pages.DisableReadMissingReferences();
        UNIT_ASSERT_VALUES_EQUAL(pages.MissingReferencesSize(), 0);
        const auto result = pages.Locate(f.Part.Get(), 0, NTable::ELargeObj::Outer);
        UNIT_ASSERT(result.Need && !result.Page);
        UNIT_ASSERT_VALUES_EQUAL(pages.MissingReferencesSize(), 0);
    }
}

} // namespace NKikimr::NTabletFlatExecutor

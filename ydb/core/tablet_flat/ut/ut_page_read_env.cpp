#include <ydb/core/tablet_flat/flat_executor_tx_env.h>
#include <ydb/core/tablet_flat/flat_database.h>
#include <ydb/core/tablet_flat/flat_dbase_sz_env.h>
#include <ydb/core/tablet_flat/flat_fwd_iface.h>
#include <ydb/core/tablet_flat/flat_fwd_conf.h>
#include <ydb/core/tablet_flat/flat_fwd_sieve.h>
#include <ydb/core/tablet_flat/flat_part_index_iter_iface.h>
#include <ydb/core/tablet_flat/flat_writer_blocks.h>
#include <ydb/core/tablet_flat/shared_cache_pages.h>
#include <ydb/core/tablet_flat/test/libs/table/test_writer.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/str.h>

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
            Scheme = layout.RowScheme();
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
            Part = MakeHolder<NTable::TPrivatePartStore>(Source->Label, std::move(params), Source->Stat);
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
        TPrivateSeat Seat;
        TIntrusiveConstPtr<NTable::TRowScheme> Scheme;
        TIntrusiveConstPtr<NTable::NTest::TPartStore> Source;
        THolder<NTable::TPrivatePartStore> Part;
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
        UNIT_ASSERT(dynamic_cast<TPrivateSeat*>(seat.get()));
        pageCache.ReleasePins(*seat);
        UNIT_ASSERT(!cache);
        TPageCacheStats stats;
        UNIT_ASSERT(!pageCache.ReadStats(stats));
        UNIT_ASSERT(!pageCache.Protocol().MakeReadyRequest());
        UNIT_ASSERT(!pageCache.MakeMetadataWorkingSet());

        NTable::TDatabase database;
        ui32 calls = 0;
        for (ui32 attempt : xrange(2)) {
            UNIT_ASSERT_VALUES_EQUAL(pageCache.PrepareRegistration(), attempt != 0);
            UNIT_ASSERT(pageCache.ReadStats(stats));
            UNIT_ASSERT_VALUES_EQUAL(stats.PageCollections, 0);
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

    Y_UNIT_TEST(SizeCountingDeduplicatesSharedDescriptors) {
        TReadFixture f;
        const ui32 slot = f.Part->DataSlot({});
        const auto& location = f.Locations.front();
        const NTable::TPartStore& common = *f.Part;
        const auto* descriptor = common.GetPageCollection(slot);
        UNIT_ASSERT(descriptor == common.GetPageCollectionRef(slot).Get());
        NTable::TSizeEnv env(&f.Env);
        UNIT_ASSERT(!env.TryGetPage(f.Part.Get(), location, {}));
        UNIT_ASSERT(!env.TryGetPage(f.Part.Get(), location, {}));
        UNIT_ASSERT_VALUES_EQUAL(env.GetSize(), location.Size);

        auto clone = common.CloneWithEpoch(common.Epoch);
        UNIT_ASSERT(!env.TryGetPage(clone.Get(), location, {}));
        UNIT_ASSERT_VALUES_EQUAL(env.GetSize(), location.Size);
        TReadFixture separatePart;
        auto separate = MakeIntrusive<TPrivatePageCache::TPageCollection>(common.GetPageCollectionRef(slot));
        separatePart.Part->PageCollections[slot] = separate;
        UNIT_ASSERT(separate->PageCollection.Get() == descriptor);
        UNIT_ASSERT(!env.TryGetPage(separatePart.Part.Get(), location, {}));
        UNIT_ASSERT_VALUES_EQUAL(env.GetSize(), location.Size);
    }

    Y_UNIT_TEST(PartCloneKeepsLegacyOwnersBehindCommonDescriptor) {
        TReadFixture f;
        auto regular = f.Load(f.Locations[0]);
        auto sticky = f.Load(f.Locations[1], true);
        const NTable::TPartStore& common = *f.Part;
        const NTable::TEpoch epoch(i64(42));
        auto clone = common.CloneWithEpoch(epoch);
        const auto* privateClone = CheckedCast<const NTable::TPrivatePartStore*>(clone.Get());
        const NTable::TPartStore& clonedDescriptor = *privateClone;
        UNIT_ASSERT(clone->Epoch == epoch);
        UNIT_ASSERT(clonedDescriptor.BundleId() == common.BundleId());
        UNIT_ASSERT_VALUES_EQUAL(clonedDescriptor.BackingSize(), common.BackingSize());
        UNIT_ASSERT_VALUES_EQUAL(clonedDescriptor.GetPageCollectionCount(), common.GetPageCollectionCount());
        for (ui32 slot : xrange(common.GetPageCollectionCount())) {
            UNIT_ASSERT(privateClone->PageCollections[slot].Get() == f.Part->PageCollections[slot].Get());
            UNIT_ASSERT(clonedDescriptor.GetPageCollectionRef(slot).Get() == common.GetPageCollectionRef(slot).Get());
        }
        UNIT_ASSERT(privateClone->Pseudo.Get() == f.Part->Pseudo.Get());
        UNIT_ASSERT(clonedDescriptor.GetPseudoPageCollection() == common.GetPseudoPageCollection());
        const auto* collection = privateClone->PageCollections[privateClone->DataSlot({})].Get();
        UNIT_ASSERT(collection->FindPage(f.Locations[0].Offset)->SharedBody.GetHandle() == regular.GetHandle());
        UNIT_ASSERT(collection->IsStickyPage(f.Locations[1].Offset));
        UNIT_ASSERT(collection->FindPage(f.Locations[1].Offset)->SharedBody.GetHandle() == sticky.GetHandle());
    }

    Y_UNIT_TEST(BackendCapturesBootOwnersWithoutChangingActiveCache) {
        TReadFixture f;
        THolder<TPrivatePageCache> cache;
        TPrivatePageCacheBackend backend(cache);
        IExecutorPageCache& pageCache = backend;
        TExecutorCaches captured;
        const TLogoBlobID statusId(2, 3, 4, 1, 0, 0);
        captured.TxStatusCaches[statusId] = TSharedData::Copy("tx", 2);
        const auto* txData = captured.TxStatusCaches.at(statusId).data();
        pageCache.CaptureForRestart(captured.PageCollections);
        UNIT_ASSERT(!captured.PageCollections);
        UNIT_ASSERT_VALUES_EQUAL(captured.TxStatusCaches.size(), 1);
        UNIT_ASSERT(captured.TxStatusCaches.at(statusId).data() == txData);

        auto regular = f.Load(f.Locations[0]);
        auto sticky = f.Load(f.Locations[1], true);
        const auto& collection = f.Part->PageCollections[f.Part->DataSlot({})];
        f.Cache.UpdateCacheMode(ECacheMode::TryKeepInMemory, collection.Get());
        cache = MakeHolder<TPrivatePageCache>();
        cache->AddPageCollection(collection);
        pageCache.CaptureForRestart(captured.PageCollections);
        const auto& saved =
            CheckedCast<TPrivatePageCacheRestartState*>(captured.PageCollections.Get())->PageCollections;
        UNIT_ASSERT_VALUES_EQUAL(saved.size(), 1);
        const auto& capturedCollection = saved.at(collection->Id);
        UNIT_ASSERT(capturedCollection.Get() != collection.Get());
        UNIT_ASSERT(capturedCollection->PageCollection.Get() == collection->PageCollection.Get());
        UNIT_ASSERT(capturedCollection->GetCacheMode() == ECacheMode::TryKeepInMemory);
        UNIT_ASSERT(capturedCollection->FindPage(f.Locations[0].Offset)->SharedBody.GetHandle() == regular.GetHandle());
        UNIT_ASSERT(capturedCollection->IsStickyPage(f.Locations[1].Offset));
        UNIT_ASSERT(cache->FindPageCollection(collection->Id) == collection.Get());
        UNIT_ASSERT_VALUES_EQUAL(cache->GetStats().PageCollections, 1);
        UNIT_ASSERT(captured.TxStatusCaches.at(statusId).data() == txData);

        UNIT_ASSERT(pageCache.PrepareRegistration());
        f.Cache.DropPageCollection(collection.Get());
        regular.Drop();
        sticky.Drop();
        auto restored = capturedCollection->FindPage(f.Locations[0].Offset)->SharedBody;
        UNIT_ASSERT(restored.Use());
        UNIT_ASSERT_VALUES_EQUAL(TPinnedPageRef(restored).GetData().size(), f.Locations[0].Size);
        UNIT_ASSERT(capturedCollection->IsStickyPage(f.Locations[1].Offset));
    }

    Y_UNIT_TEST(LoaderEnvironmentKeepsRestartOwnersAcrossMetadataIO) {
        TReadFixture f;
        THolder<TPrivatePageCache> cache;
        TPrivatePageCacheBackend backend(cache);
        IExecutorPageCache& pageCache = backend;
        TExecutorCaches captured;
        auto empty = pageCache.MakeLoaderEnvironment();
        UNIT_ASSERT_VALUES_EQUAL(empty->GetCollectionCount(), 0);
        UNIT_ASSERT(!cache);

        auto regular = f.Load(f.Locations[0]);
        const ui32 slot = f.Part->DataSlot({});
        const auto& collection = f.Part->PageCollections[slot];
        cache = MakeHolder<TPrivatePageCache>();
        cache->AddPageCollection(collection);
        pageCache.CaptureForRestart(captured.PageCollections);
        TVector<NPageCollection::TLargeGlobId> ids(2);
        ids[0].Lead = collection->Id;
        ids[1].Lead = TLogoBlobID(collection->Id.TabletID() + 1, 0, 0, 0, 0, 0);
        auto environment = pageCache.MakeLoaderEnvironment(ids, captured.PageCollections.Get());
        UNIT_ASSERT_VALUES_EQUAL(environment->GetCollectionCount(), 2);
        UNIT_ASSERT(environment->HasCollection(0));
        UNIT_ASSERT(!environment->HasCollection(1));
        UNIT_ASSERT(environment->GetCollection(0).Get() == collection->PageCollection.Get());

        captured.PageCollections.Reset();
        cache.Reset();
        f.Cache.DropPageCollection(collection.Get());
        regular.Drop();
        environment->Initialize(NTable::TPartSlots(1));
        const auto* page = environment->TryGetSlotPage(0, f.Locations[0]);
        UNIT_ASSERT(page);
        UNIT_ASSERT_VALUES_EQUAL(page->size(), f.Locations[0].Size);
    }

    Y_UNIT_TEST(BackendMergesBootOwnersWithoutConsumingThem) {
        TReadFixture f;
        THolder<TPrivatePageCache> cache;
        TPrivatePageCacheBackend backend(cache);
        IExecutorPageCache& pageCache = backend;
        TExecutorCaches captured;
        pageCache.MergeForRestart(captured.PageCollections, nullptr);
        UNIT_ASSERT(!captured.PageCollections);
        THolder<IPageCacheLoadedCollections> loaded;
        const auto& label = f.Part->Label;
        auto pseudo = MakeIntrusive<TReadPageCollection>(
            f.Source->Store, ui32{ 0 }, TLogoBlobID(label.TabletID(), label.Generation(), label.Step(), label.Channel(),
                                            label.BlobSize(), label.Cookie() + 2048));
        f.Part->Pseudo = MakeIntrusive<TPrivatePageCache::TPageCollection>(std::move(pseudo));
        pageCache.CaptureForRestart(*f.Part, loaded);
        const auto& retained = CheckedCast<TPrivateLoadedCollections*>(loaded.Get())->PageCollections;
        UNIT_ASSERT_VALUES_EQUAL(retained.size(), f.Part->PageCollections.size() + 1);
        for (size_t slot : xrange(f.Part->PageCollections.size())) {
            UNIT_ASSERT(retained[slot].Get() == f.Part->PageCollections[slot].Get());
        }
        UNIT_ASSERT(retained.back().Get() == f.Part->Pseudo.Get());
        const auto firstId = retained.front()->Id;
        auto replacement = MakeIntrusive<TPrivatePageCache::TPageCollection>(retained.front()->PageCollection);
        auto& appended = CheckedCast<TPrivateLoadedCollections*>(loaded.Get())->PageCollections;
        appended.push_back(replacement);
        const auto count = retained.size();
        pageCache.MergeForRestart(captured.PageCollections, loaded.Get());
        const auto& saved =
            CheckedCast<TPrivatePageCacheRestartState*>(captured.PageCollections.Get())->PageCollections;
        UNIT_ASSERT(saved.at(firstId).Get() == replacement.Get());
        UNIT_ASSERT(saved.at(f.Part->Pseudo->Id).Get() == f.Part->Pseudo.Get());
        UNIT_ASSERT_VALUES_EQUAL(retained.size(), count);
        cache.Reset();
        const auto* restartState = captured.PageCollections.Get();
        pageCache.CaptureForRestart(captured.PageCollections);
        UNIT_ASSERT(captured.PageCollections.Get() == restartState);
    }

    Y_UNIT_TEST(BackendModesPreserveLegacyAccountingAndOuterMode) {
        TReadFixture f;
        auto cache = MakeHolder<TPrivatePageCache>();
        TPrivatePageCacheBackend backend(cache);
        IExecutorPageCache& pageCache = backend;
        const ui32 dataSlot = f.Part->DataSlot({});
        const ui32 outerSlot = f.Part->OuterSlot();
        const ui32 count = f.Part->GetPageCollectionCount();
        f.Part->PageCollections[outerSlot]->SetCacheMode(ECacheMode::TryKeepInMemory);
        TVector<bool> stickyGroups(f.Part->GroupsCount, true);
        TVector<ECacheMode> modes(count, ECacheMode::Regular);
        modes[dataSlot] = ECacheMode::TryKeepInMemory;
        pageCache.ReadCollectionModes(*f.Part, modes, stickyGroups);
        UNIT_ASSERT(modes[dataSlot] == ECacheMode::Regular);
        UNIT_ASSERT(modes[outerSlot] == ECacheMode::TryKeepInMemory);
        modes[dataSlot] = ECacheMode::TryKeepInMemory;
        modes[outerSlot] = ECacheMode::Regular;
        pageCache.PrepareCollectionModes(*f.Part, modes, stickyGroups);
        UNIT_ASSERT(modes[dataSlot] == ECacheMode::TryKeepInMemory);
        UNIT_ASSERT(modes[outerSlot] == ECacheMode::TryKeepInMemory);
        TPageCacheStats stats;
        UNIT_ASSERT(pageCache.ReadStats(stats));
        UNIT_ASSERT_VALUES_EQUAL(stats.TryKeepInMemoryBytes, 0);
        const auto send = [](THolder<IEventBase>) {
        };
        for (ui32 slot : xrange(count)) {
            pageCache.RegisterCollection(*f.Part, slot, modes[slot], {}, false, send);
        }
        const ui64 dataBytes = f.Part->GetPageCollection(dataSlot)->BackingSize();
        const ui64 outerBytes = f.Part->GetPageCollection(outerSlot)->BackingSize();
        UNIT_ASSERT(dataBytes > 0 && outerBytes > 0);
        UNIT_ASSERT(pageCache.ReadStats(stats));
        UNIT_ASSERT_VALUES_EQUAL(stats.TryKeepInMemoryBytes, dataBytes + outerBytes);

        modes.assign(count, ECacheMode::Regular);
        auto changed = pageCache.UpdateCollectionModes(*f.Part, modes, stickyGroups);
        UNIT_ASSERT_VALUES_EQUAL(changed.size(), count);
        for (ui32 slot : xrange(count)) {
            UNIT_ASSERT_VALUES_EQUAL(changed[slot], slot == dataSlot);
        }
        UNIT_ASSERT(modes[dataSlot] == ECacheMode::Regular);
        UNIT_ASSERT(modes[outerSlot] == ECacheMode::TryKeepInMemory);
        UNIT_ASSERT(pageCache.ReadStats(stats));
        UNIT_ASSERT_VALUES_EQUAL(stats.TryKeepInMemoryBytes, outerBytes);
        modes.assign(count, ECacheMode::Regular);
        changed = pageCache.UpdateCollectionModes(*f.Part, modes, stickyGroups);
        for (bool value : changed) {
            UNIT_ASSERT(!value);
        }
        UNIT_ASSERT(pageCache.ReadStats(stats));
        UNIT_ASSERT_VALUES_EQUAL(stats.TryKeepInMemoryBytes, outerBytes);
    }

    Y_UNIT_TEST(BackendRegistersBeforeSendingAttachAndSync) {
        TReadFixture f;
        auto cache = MakeHolder<TPrivatePageCache>();
        TPrivatePageCacheBackend backend(cache);
        IExecutorPageCache& pageCache = backend;
        const ui32 slot = f.Part->DataSlot({});
        auto regular = f.Load(f.Locations[0]);
        auto sticky = f.Load(f.Locations[1], true);
        const auto& collection = f.Part->PageCollections[slot];
        collection->SetCacheMode(ECacheMode::TryKeepInMemory);
        TVector<NPageCollection::TBtreeSeed> seeds{ {
            .IndexCollectionId = collection->Id,
            .DataCollectionId = collection->Id,
            .Root = f.Locations.front(),
            .Sticky = true,
        } };
        const auto expectedSeeds = seeds;
        const auto* seedStorage = seeds.data();
        TVector<THolder<IEventBase>> events;
        const auto send = [&](THolder<IEventBase> event) {
            UNIT_ASSERT(cache->FindPageCollection(collection->Id) == collection.Get());
            events.push_back(std::move(event));
        };
        pageCache.RegisterCollection(*f.Part, slot, ECacheMode::TryKeepInMemory, std::move(seeds), true, send);
        UNIT_ASSERT_VALUES_EQUAL(events.size(), 2);
        const auto& attach = *CheckedCast<NSharedCache::TEvAttach*>(events[0].Get());
        UNIT_ASSERT(attach.PageCollection.Get() == collection->PageCollection.Get());
        UNIT_ASSERT(attach.CacheMode == ECacheMode::TryKeepInMemory);
        UNIT_ASSERT(attach.BtreeSeeds == expectedSeeds);
        UNIT_ASSERT(attach.BtreeSeeds.data() == seedStorage);
        UNIT_ASSERT(attach.ReplayStickyWalk);
        const auto& sync = *CheckedCast<NSharedCache::TEvSync*>(events[1].Get());
        UNIT_ASSERT_VALUES_EQUAL(sync.Pages.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sync.Pages.at(collection->Id).size(), 2);
        UNIT_ASSERT(sync.Pages.at(collection->Id).contains(f.Locations[0].Offset));
        UNIT_ASSERT(sync.Pages.at(collection->Id).contains(f.Locations[1].Offset));
        TPageCacheStats stats;
        UNIT_ASSERT(pageCache.ReadStats(stats));
        UNIT_ASSERT_VALUES_EQUAL(stats.PageCollections, 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.SharedBodyBytes, f.Locations[0].Size + f.Locations[1].Size);
        UNIT_ASSERT_VALUES_EQUAL(stats.StickyBytes, f.Locations[1].Size);
        UNIT_ASSERT_VALUES_EQUAL(stats.TryKeepInMemoryBytes, collection->PageCollection->BackingSize());
        UNIT_ASSERT(collection->FindPage(f.Locations[0].Offset)->SharedBody.GetHandle() == regular.GetHandle());
        UNIT_ASSERT(collection->FindPage(f.Locations[1].Offset)->SharedBody.GetHandle() == sticky.GetHandle());
        UNIT_ASSERT(collection->IsStickyPage(f.Locations[1].Offset));

        TReadFixture empty;
        UNIT_ASSERT(pageCache.PrepareRegistration());
        events.clear();
        const auto collect = [&](THolder<IEventBase> event) {
            events.push_back(std::move(event));
        };
        pageCache.RegisterCollection(*empty.Part, empty.Part->DataSlot({}), ECacheMode::Regular, {}, false, collect);
        UNIT_ASSERT_VALUES_EQUAL(events.size(), 1);
        const auto& emptyAttach = *CheckedCast<NSharedCache::TEvAttach*>(events.front().Get());
        UNIT_ASSERT(emptyAttach.BtreeSeeds.empty() && !emptyAttach.ReplayStickyWalk);
    }

    Y_UNIT_TEST(WrittenPagesKeepOwnersAndReuseBuffers) {
        TReadFixture f;
        auto cachePages = MakeIntrusive<NSharedCache::TSharedCachePages>();
        auto cache = MakePrivateWrittenPageCache(*cachePages);
        TVector<NWriter::TWrittenPageCollection> written(2);
        const ui32 dataSlot = f.Part->DataSlot({});
        const ui32 outerSlot = f.Part->OuterSlot();
        written[0].PageCollection = f.Part->GetPageCollectionRef(dataSlot);
        written[1].PageCollection = f.Part->GetPageCollectionRef(outerSlot);
        const auto& stickyLocation = f.Locations[0];
        const auto& regularLocation = f.Locations[1];
        const auto outerLocation = f.Source->Store->GetPageLocation(outerSlot, 0);
        written[0].StickyPages.emplace_back(stickyLocation, *f.Source->Store->GetPage(dataSlot, stickyLocation.Offset));
        written[0].RegularPages.emplace_back(
            regularLocation, *f.Source->Store->GetPage(dataSlot, regularLocation.Offset));
        written[1].RegularPages.emplace_back(outerLocation, *f.Source->Store->GetPage(outerSlot, outerLocation.Offset));
        const auto* stickyBytes = written[0].StickyPages.front().Data.data();
        const auto* regularBytes = written[0].RegularPages.front().Data.data();
        TVector<THolder<IEventBase>> events;
        auto environment = cache.AddWrittenPages(written, [&](THolder<IEventBase> event) {
            UNIT_ASSERT_VALUES_EQUAL(event->Type(), NSharedCache::EvSaveCompactedPages);
            const auto& saved = *CheckedCast<NSharedCache::TEvSaveCompactedPages*>(event.Get());
            UNIT_ASSERT(events.size() < written.size());
            UNIT_ASSERT(saved.PageCollection.Get() == written[events.size()].PageCollection.Get());
            if (events.empty()) {
                UNIT_ASSERT_VALUES_EQUAL(saved.Pages.size(), 2);
                UNIT_ASSERT_VALUES_EQUAL(saved.Pages[0]->Offset, stickyLocation.Offset);
                UNIT_ASSERT_VALUES_EQUAL(saved.Pages[1]->Offset, regularLocation.Offset);
                UNIT_ASSERT_VALUES_EQUAL(saved.Pages[0]->Size, stickyLocation.Size);
                UNIT_ASSERT_VALUES_EQUAL(saved.Pages[1]->Size, regularLocation.Size);
                // The shared-cache handoff holds one use; Sticky retains another
                UNIT_ASSERT_VALUES_EQUAL(saved.Pages[0]->UseCount(), 2);
                UNIT_ASSERT_VALUES_EQUAL(saved.Pages[1]->UseCount(), 1);
                UNIT_ASSERT(saved.Pages[0]->Type == stickyLocation.Type);
                UNIT_ASSERT(saved.Pages[1]->Type == regularLocation.Type);
                UNIT_ASSERT_VALUES_EQUAL(saved.Pages[0]->Crc32, stickyLocation.Crc32);
                UNIT_ASSERT_VALUES_EQUAL(saved.Pages[1]->Crc32, regularLocation.Crc32);
                UNIT_ASSERT(!written[0].StickyPages.front().Data);
                UNIT_ASSERT(!written[0].RegularPages.front().Data);
                UNIT_ASSERT(written[1].RegularPages.front().Data);
            } else {
                UNIT_ASSERT_VALUES_EQUAL(saved.Pages.size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(saved.Pages.front()->Offset, outerLocation.Offset);
                UNIT_ASSERT(!written[1].RegularPages.front().Data);
            }
            events.push_back(std::move(event));
        });
        UNIT_ASSERT_VALUES_EQUAL(events.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(environment->GetCollectionCount(), 2);
        UNIT_ASSERT(environment->GetCollection(0).Get() == written[0].PageCollection.Get());
        UNIT_ASSERT(environment->GetCollection(1).Get() == written[1].PageCollection.Get());
        events.clear();
        environment->Initialize(NTable::TPartSlots(1));
        const auto* sticky = environment->TryGetSlotPage(0, stickyLocation);
        const auto* regular = environment->TryGetSlotPage(0, regularLocation);
        UNIT_ASSERT(sticky && regular);
        UNIT_ASSERT(sticky->data() == stickyBytes);
        UNIT_ASSERT(regular->data() == regularBytes);
    }

    Y_UNIT_TEST(ForwardEnvironmentPreservesFetchAndResetLifetimes) {
        TReadFixture f;
        TIntrusiveConstPtr<NTable::TPrivatePartStore> part(f.Part.Release());
        NTable::TSubset subset(
            NTable::TEpoch::Max(), f.Scheme, TVector<NTable::TPartView>{ { part, nullptr, NTable::TSlices::All() } });
        NTable::NFwd::TConf conf;
        conf.AheadLo = 256;
        conf.AheadHi = 512;
        auto environment = NTable::NFwd::MakeEnv(conf, subset);
        const auto& target = f.Locations.front();
        auto index = NTable::CreateIndexIter(part.Get(), environment.Get(), {});
        UNIT_ASSERT(index->Seek(0) == NTable::EReady::Page);
        auto firstFetch = environment->GetFetch();
        UNIT_ASSERT(firstFetch);
        auto staleRequest = environment->MakeRequest(NBlockIO::EPriority::Bulk, std::move(firstFetch));
        auto makeReply = [&](NActors::IEventBase& event) {
            const auto& request = *CheckedCast<NSharedCache::TEvRequest*>(&event);
            ui32 slot = 0;
            while (slot < part->GetPageCollectionCount() &&
                   part->GetPageCollection(slot)->Label() != request.PageCollection->Label()) {
                ++slot;
            }
            UNIT_ASSERT(slot < part->GetPageCollectionCount());
            auto reply = MakeHolder<NSharedCache::TEvResult>(request.PageCollection, NKikimrProto::OK, request.Cookie);
            for (const auto& location : request.Pages) {
                auto page = TSharedPageRef::MakePrivate(*f.Source->Store->GetPage(slot, location.Offset));
                auto type = location.Type;
                if (type == NTable::NPage::EPage::Undef) {
                    const auto pageId = location.Offset.IsPageIndex()
                                            ? location.Offset.AsPageIndex()
                                            : f.Source->Store->ResolveByteOffset(slot, location.Offset.AsByteOffset());
                    type = f.Source->Store->GetPageType(slot, pageId);
                }
                page.SetType(type);
                reply->Pages.emplace_back(location.Offset, location.Size, std::move(page));
            }
            return reply;
        };
        auto stale = makeReply(*staleRequest);
        UNIT_ASSERT(!environment->MayProgress());
        UNIT_ASSERT(environment->Reset() == environment.Get());
        index = NTable::CreateIndexIter(part.Get(), environment.Get(), {});
        UNIT_ASSERT(environment->MayProgress());
        const auto beforeStale = environment->Stats();
        environment->Save(*stale);
        UNIT_ASSERT(environment->MayProgress());
        UNIT_ASSERT(environment->Stats() == beforeStale);

        const TSharedData* page = nullptr;
        for (ui32 attempt = 0; attempt < 32 && !page; ++attempt) {
            if (index->Seek(0) == NTable::EReady::Data) {
                UNIT_ASSERT(index->GetLocation().Offset == target.Offset);
                page = environment->TryGetPage(part.Get(), target, {});
            }
            if (page) {
                break;
            }
            while (auto fetch = environment->GetFetch()) {
                auto request = environment->MakeRequest(NBlockIO::EPriority::Bulk, std::move(fetch));
                auto reply = makeReply(*request);
                environment->Save(*reply);
            }
        }
        UNIT_ASSERT(page);
        UNIT_ASSERT_VALUES_EQUAL(page->size(), target.Size);
        UNIT_ASSERT(page->data() == f.Source->Store->GetPage(part->DataSlot({}), target.Offset)->data());
        UNIT_ASSERT(environment->MayProgress());
    }

    Y_UNIT_TEST(ForwardLoaderAndProtocolOutliveForwardEnvironment) {
        TReadFixture f;
        NTable::TSubset subset(NTable::TEpoch::Max(), f.Scheme);
        NTable::NFwd::TConf conf;
        conf.AheadLo = 0;
        conf.AheadHi = 0;
        auto environment = NTable::NFwd::MakeEnv(conf, subset);
        const auto* protocol = &environment->Protocol();
        auto loader = environment->MakeLoaderEnvironment();
        UNIT_ASSERT(&loader->Protocol() == protocol);
        THolder<TPrivatePageCache> cache;
        {
            TPrivatePageCacheBackend backend(cache);
            UNIT_ASSERT(&backend.Protocol() == protocol);
        }
        environment.Reset();
        UNIT_ASSERT(protocol->EventKind(NSharedCache::EvResult) == EPageCacheEvent::Result);
        auto fetch = NTable::TLoaderFetch{
            .PageCollection = f.Part->GetPageCollectionRef(f.Part->DataSlot({})),
            .Pages = f.Locations,
        };
        const auto* locations = fetch.Pages.data();
        auto request = loader->MakeRequest(NBlockIO::EPriority::Fast, std::move(fetch), 91);
        const auto& typed = *CheckedCast<NSharedCache::TEvRequest*>(request.Get());
        UNIT_ASSERT(typed.Pages.data() == locations);
        UNIT_ASSERT(typed.Pages == f.Locations);
        UNIT_ASSERT_VALUES_EQUAL(typed.Cookie, 91);
        UNIT_ASSERT(typed.Priority == NBlockIO::EPriority::Fast);
        UNIT_ASSERT(!cache);
    }

    Y_UNIT_TEST(BackendClassifiesEventsAndBorrowsResultMetadata) {
        TReadFixture f;
        THolder<TPrivatePageCache> cache;
        TPrivatePageCacheBackend backend(cache);
        const auto& protocol = backend.Protocol();
        UNIT_ASSERT(protocol.EventKind(NSharedCache::EvResult) == EPageCacheEvent::Result);
        UNIT_ASSERT(protocol.EventKind(NSharedCache::EvUpdated) == EPageCacheEvent::Updated);
        UNIT_ASSERT(protocol.EventKind(NSharedCache::EvStickyCollectionPages) == EPageCacheEvent::StickyPages);
        UNIT_ASSERT(protocol.EventKind(TEvents::TSystem::Poison) == EPageCacheEvent::Unhandled);
        const ui32 slot = f.Part->DataSlot({});
        const auto& descriptor = f.Part->GetPageCollectionRef(slot);
        constexpr ui64 cookie = 135;
        for (auto status : { NKikimrProto::OK, NKikimrProto::NODATA, NKikimrProto::RACE }) {
            auto result = MakeHolder<NSharedCache::TEvResult>(descriptor, status, cookie);
            result->WaitPad = MakeIntrusive<NPageCollection::TPagesWaitPad>();
            const auto* waitPad = result->WaitPad.Get();
            const auto& location = f.Locations.front();
            result->Pages.emplace_back(location.Offset, location.Size,
                TSharedPageRef::MakePrivate(*f.Source->Store->GetPage(slot, location.Offset)));
            NActors::IEventHandle envelope(
                TActorId(), TActorId(), result.Release(), 0, ui64(ERequestTypeCookie::Transaction));
            auto& typed = *CheckedCast<NSharedCache::TEvResult*>(envelope.GetBase());
            const auto view = protocol.ReadResult(typed);
            UNIT_ASSERT(view.Status == status);
            UNIT_ASSERT(view.PageCollection == descriptor.Get());
            UNIT_ASSERT(&view.WaitPad == &typed.WaitPad);
            UNIT_ASSERT(view.WaitPad.Get() == waitPad);
            UNIT_ASSERT_VALUES_EQUAL(view.Cookie, cookie);
            UNIT_ASSERT_VALUES_EQUAL(view.PageCount, 1);
            UNIT_ASSERT(view.Kind == EPageCacheResultKind::Pages);
            UNIT_ASSERT_VALUES_EQUAL(envelope.Cookie, ui64(ERequestTypeCookie::Transaction));
            TStringStream originalText;
            TStringStream commonText;
            typed.Describe(originalText);
            view.Describe(commonText);
            UNIT_ASSERT_VALUES_EQUAL(commonText.Str(), originalText.Str());
            auto moved = std::move(view.WaitPad);
            UNIT_ASSERT(moved.Get() == waitPad);
            UNIT_ASSERT(!typed.WaitPad);
        }
        UNIT_ASSERT(!cache);
    }

    Y_UNIT_TEST(BackendBuildsLoaderAndCollectionRequests) {
        TReadFixture f;
        THolder<TPrivatePageCache> cache;
        TPrivatePageCacheBackend backend(cache);
        IExecutorPageCache& pageCache = backend;
        const auto& protocol = pageCache.Protocol();
        const auto& descriptor = f.Part->GetPageCollectionRef(f.Part->DataSlot({}));
        for (auto priority : { NBlockIO::EPriority::Fast, NBlockIO::EPriority::Bkgr }) {
            auto locations = f.Locations;
            const auto* storage = locations.data();
            auto event = protocol.MakePageRequest(priority, descriptor, std::move(locations), 73);
            UNIT_ASSERT_VALUES_EQUAL(event->Type(), NSharedCache::EvRequest);
            const auto& request = *CheckedCast<NSharedCache::TEvRequest*>(event.Get());
            UNIT_ASSERT(request.Priority == priority);
            UNIT_ASSERT(request.PageCollection.Get() == descriptor.Get());
            UNIT_ASSERT(request.Pages.data() == storage);
            UNIT_ASSERT(request.Pages == f.Locations);
            UNIT_ASSERT_VALUES_EQUAL(request.Cookie, 73);
            UNIT_ASSERT(!request.WaitPad);
        }
        TVector<NPageCollection::TBtreeSeed> seeds{ {
            .IndexCollectionId = descriptor->Label(),
            .DataCollectionId = descriptor->Label(),
            .Root = f.Locations.front(),
            .LevelCount = 2,
            .Sticky = true,
        } };
        const auto expectedSeeds = seeds;
        const auto* seedStorage = seeds.data();
        auto update = protocol.MakeCollectionUpdate(descriptor, ECacheMode::TryKeepInMemory, std::move(seeds));
        UNIT_ASSERT_VALUES_EQUAL(update->Type(), NSharedCache::EvAttach);
        const auto& attach = *CheckedCast<NSharedCache::TEvAttach*>(update.Get());
        UNIT_ASSERT(attach.PageCollection.Get() == descriptor.Get());
        UNIT_ASSERT(attach.CacheMode == ECacheMode::TryKeepInMemory);
        UNIT_ASSERT(attach.BtreeSeeds == expectedSeeds);
        UNIT_ASSERT(attach.BtreeSeeds.data() == seedStorage);
        UNIT_ASSERT(!attach.ReplayStickyWalk);
        auto unregister = pageCache.MakeUnregisterRequest();
        UNIT_ASSERT_VALUES_EQUAL(unregister->Type(), NSharedCache::EvUnregister);
        UNIT_ASSERT(!cache);
        static_assert(ui64(ERequestTypeCookie::Transaction) == 1);
        static_assert(ui64(ERequestTypeCookie::BootLogic) == 4);
        static_assert(ui64(NSharedCache::ERequestTypeCookie::TryKeepInMemPages) == 5);
    }

    Y_UNIT_TEST(BackendBuildsRequestsFromCurrentCache) {
        TReadFixture f;
        THolder<TPrivatePageCache> cache;
        TPrivatePageCacheBackend backend(cache);
        IExecutorPageCache& pageCache = backend;
        const ui32 slot = f.Part->DataSlot({});
        const auto& collectionId = f.Part->PageCollections[slot]->Id;
        auto waitPad = MakeIntrusive<NPageCollection::TPagesWaitPad>();
        waitPad->PendingRequests = 7;
        const auto traceId = NWilson::TTraceId::NewTraceId(3, 100);
        THolder<NActors::IEventBase> previous;
        for (ui32 attempt = 0; attempt < 2; ++attempt) {
            cache = MakeHolder<TPrivatePageCache>();
            auto descriptor = MakeIntrusive<TReadPageCollection>(f.Source->Store, slot, collectionId);
            cache->AddPageCollection(MakeIntrusive<TPrivatePageCache::TPageCollection>(descriptor));
            auto locations = f.Locations;
            const auto* storage = locations.data();
            auto event = pageCache.MakeTransactionRequest(collectionId, std::move(locations), waitPad, traceId.Clone());
            const auto& request = *CheckedCast<NSharedCache::TEvRequest*>(event.Get());
            UNIT_ASSERT(request.PageCollection.Get() == descriptor.Get());
            UNIT_ASSERT(request.Pages.data() == storage);
            UNIT_ASSERT(request.Pages == f.Locations);
            UNIT_ASSERT(request.Priority == NBlockIO::EPriority::Fast);
            UNIT_ASSERT_VALUES_EQUAL(request.Cookie, 0);
            UNIT_ASSERT(request.WaitPad.Get() == waitPad.Get());
            UNIT_ASSERT(request.TraceId == traceId);
            UNIT_ASSERT_VALUES_EQUAL(waitPad->PendingRequests, 7);
            if (previous) {
                const auto& oldRequest = *CheckedCast<NSharedCache::TEvRequest*>(previous.Get());
                UNIT_ASSERT(oldRequest.PageCollection.Get() != descriptor.Get());
                UNIT_ASSERT_VALUES_EQUAL(oldRequest.PageCollection->Label(), collectionId);
            }
            previous = std::move(event);
        }
    }

    Y_UNIT_TEST(BackendDropsCachedPagesWithoutReleasingTransactionPins) {
        TReadFixture f;
        auto cache = MakeHolder<TPrivatePageCache>();
        TPrivatePageCacheBackend backend(cache);
        IExecutorPageCache& pageCache = backend;
        const ui32 slot = f.Part->DataSlot({});
        auto collection = MakeIntrusive<TPrivatePageCache::TPageCollection>(f.Part->GetPageCollectionRef(slot));
        cache->AddPageCollection(collection);
        TEvResult result(collection->PageCollection, NKikimrProto::OK, 0);
        for (ui32 index : xrange(2)) {
            const auto& location = f.Locations[index];
            result.Pages.emplace_back(location.Offset, location.Size,
                TSharedPageRef::MakePrivate(*f.Source->Store->GetPage(slot, location.Offset)));
        }
        UNIT_ASSERT(pageCache.ProcessLoadedPages(result, false) == EPageResultStatus::Ready);
        UNIT_ASSERT(pageCache.AdoptLoadedPages(f.Seat, &result));
        const auto& pinned = f.Seat.Pinned.at(collection->Id);
        const auto* bytes = pinned.at(f.Locations[0].Offset).PinnedBody.data();
        NSharedCache::TEvUpdated update;
        update.DroppedPages[collection->Id] = { f.Locations[0].Offset, f.Locations[2].Offset };
        const TLogoBlobID missingCollectionId(collection->Id.TabletID() + 1, 0, 0, 0, 0, 0);
        update.DroppedPages[missingCollectionId].insert(f.Locations[1].Offset);
        for (ui32 attempt = 0; attempt < 2; ++attempt) {
            pageCache.ProcessCacheUpdate(update);
            UNIT_ASSERT(!collection->FindPage(f.Locations[0].Offset));
            UNIT_ASSERT(collection->FindPage(f.Locations[1].Offset));
            UNIT_ASSERT_VALUES_EQUAL(cache->GetStats().SharedBodyBytes, f.Locations[1].Size);
            UNIT_ASSERT_VALUES_EQUAL(pinned.size(), 2);
            UNIT_ASSERT(pinned.at(f.Locations[0].Offset).PinnedBody.data() == bytes);
            UNIT_ASSERT_VALUES_EQUAL(update.DroppedPages.at(collection->Id).size(), 2);
        }
        auto event = pageCache.DetachCollection(collection->Id);
        UNIT_ASSERT(event);
        const auto& detach = *CheckedCast<NSharedCache::TEvDetach*>(event.Get());
        UNIT_ASSERT_VALUES_EQUAL(detach.PageCollectionId, collection->Id);
        UNIT_ASSERT(!cache->FindPageCollection(collection->Id));
        pageCache.ProcessCacheUpdate(update);
        UNIT_ASSERT_VALUES_EQUAL(cache->GetStats().PageCollections, 0);
        UNIT_ASSERT_VALUES_EQUAL(cache->GetStats().SharedBodyBytes, 0);
        UNIT_ASSERT_VALUES_EQUAL(pinned.size(), 2);
        UNIT_ASSERT(pinned.at(f.Locations[0].Offset).PinnedBody.data() == bytes);
    }

    Y_UNIT_TEST(BackendBuildsStickyRequestsOnlyForRegisteredCollections) {
        TReadFixture f;
        auto cache = MakeHolder<TPrivatePageCache>();
        TPrivatePageCacheBackend backend(cache);
        IExecutorPageCache& pageCache = backend;
        auto collection =
            MakeIntrusive<TPrivatePageCache::TPageCollection>(f.Part->GetPageCollectionRef(f.Part->DataSlot({})));
        NSharedCache::TEvStickyCollectionPages notification(collection->Id, f.Locations);
        UNIT_ASSERT(!pageCache.MakeStickyPageRequest(notification));
        cache->AddPageCollection(collection);
        auto event = pageCache.MakeStickyPageRequest(notification);
        UNIT_ASSERT(event);
        const auto& request = *CheckedCast<NSharedCache::TEvRequest*>(event.Get());
        UNIT_ASSERT(request.PageCollection.Get() == collection->PageCollection.Get());
        UNIT_ASSERT(request.Priority == NBlockIO::EPriority::Bkgr);
        UNIT_ASSERT(request.Pages == f.Locations);
        UNIT_ASSERT(request.Pages.data() != notification.Locations.data());
        UNIT_ASSERT(notification.Locations == f.Locations);
        UNIT_ASSERT_VALUES_EQUAL(request.Cookie, 0);
        UNIT_ASSERT(!request.WaitPad && !request.TraceId);
        cache->DropPageCollection(collection.Get());
        UNIT_ASSERT(!pageCache.MakeStickyPageRequest(notification));
    }

    Y_UNIT_TEST(BackendRegistersRegularAndStickyResults) {
        TReadFixture f;
        auto cache = MakeHolder<TPrivatePageCache>();
        TPrivatePageCacheBackend backend(cache);
        IExecutorPageCache& pageCache = backend;
        const ui32 slot = f.Part->DataSlot({});
        auto collection = MakeIntrusive<TPrivatePageCache::TPageCollection>(f.Part->GetPageCollectionRef(slot));
        cache->AddPageCollection(collection);
        const auto& regularLocation = f.Locations[0];
        TEvResult regular(collection->PageCollection, NKikimrProto::OK, 0);
        regular.Pages.emplace_back(regularLocation.Offset, regularLocation.Size,
            TSharedPageRef::MakePrivate(*f.Source->Store->GetPage(slot, regularLocation.Offset)));
        const auto* regularHandle = regular.Pages.front().Page.GetHandle().Get();
        UNIT_ASSERT(pageCache.ProcessLoadedPages(regular, false) == EPageResultStatus::Ready);
        UNIT_ASSERT(regular.Pages.front().Page.GetHandle().Get() == regularHandle);
        const auto* regularPage = collection->FindPage(regularLocation.Offset);
        UNIT_ASSERT(regularPage && regularPage->SharedBody.GetHandle().Get() == regularHandle);
        UNIT_ASSERT(!collection->IsStickyPage(regularLocation.Offset));

        const auto& stickyLocation = f.Locations[1];
        TEvResult sticky(collection->PageCollection, NKikimrProto::OK, 0);
        sticky.Pages.emplace_back(stickyLocation.Offset, stickyLocation.Size,
            TSharedPageRef::MakePrivate(*f.Source->Store->GetPage(slot, stickyLocation.Offset)));
        const auto* stickyHandle = sticky.Pages.front().Page.GetHandle().Get();
        UNIT_ASSERT(pageCache.ProcessLoadedPages(sticky, true) == EPageResultStatus::Ready);
        UNIT_ASSERT(!sticky.Pages.front().Page);
        const auto* stickyPage = collection->FindPage(stickyLocation.Offset);
        UNIT_ASSERT(stickyPage && stickyPage->SharedBody.GetHandle().Get() == stickyHandle);
        UNIT_ASSERT(collection->IsStickyPage(stickyLocation.Offset));
        TPageCacheStats stats;
        UNIT_ASSERT(pageCache.ReadStats(stats));
        UNIT_ASSERT_VALUES_EQUAL(stats.PageCollections, 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.SharedBodyBytes, regularLocation.Size + stickyLocation.Size);
        UNIT_ASSERT_VALUES_EQUAL(stats.StickyBytes, stickyLocation.Size);
        UNIT_ASSERT_VALUES_EQUAL(stats.TryKeepInMemoryBytes, 0);
        UNIT_ASSERT(f.Seat.Pinned.empty());
    }

    Y_UNIT_TEST(BackendIgnoresRemovedCollectionsBeforeCheckingErrors) {
        TReadFixture f;
        auto cache = MakeHolder<TPrivatePageCache>();
        TPrivatePageCacheBackend backend(cache);
        const ui32 slot = f.Part->DataSlot({});
        auto collection = MakeIntrusive<TPrivatePageCache::TPageCollection>(f.Part->GetPageCollectionRef(slot));
        const auto& location = f.Locations.front();
        TEvResult error(collection->PageCollection, NKikimrProto::NODATA, 0);
        error.Pages.emplace_back(location.Offset, location.Size,
            TSharedPageRef::MakePrivate(*f.Source->Store->GetPage(slot, location.Offset)));
        const auto* handle = error.Pages.front().Page.GetHandle().Get();
        UNIT_ASSERT(backend.ProcessLoadedPages(error, true) == EPageResultStatus::Ignored);
        cache->AddPageCollection(collection);
        UNIT_ASSERT(backend.ProcessLoadedPages(error, true) == EPageResultStatus::Error);
        UNIT_ASSERT(error.Pages.front().Page.GetHandle().Get() == handle);
        UNIT_ASSERT(!collection->FindPage(location.Offset));
        UNIT_ASSERT(!collection->IsStickyPage(location.Offset));
        UNIT_ASSERT_VALUES_EQUAL(cache->GetStats().SharedBodyBytes, 0);
        UNIT_ASSERT_VALUES_EQUAL(cache->GetStats().StickyBytes, 0);

        cache->DropPageCollection(collection.Get());
        TEvResult late(collection->PageCollection, NKikimrProto::OK, 0);
        late.Pages.emplace_back(location.Offset, location.Size, std::move(error.Pages.front().Page));
        UNIT_ASSERT(backend.ProcessLoadedPages(late, false) == EPageResultStatus::Ignored);
        UNIT_ASSERT(late.Pages.front().Page.GetHandle().Get() == handle);
        UNIT_ASSERT(!collection->FindPage(location.Offset));
        UNIT_ASSERT_VALUES_EQUAL(cache->GetStats().SharedBodyBytes, 0);
    }

    Y_UNIT_TEST(BackendMovesLoadedPagesIntoExistingPins) {
        TReadFixture f;
        auto cache = MakeHolder<TPrivatePageCache>();
        auto collection = f.Part->PageCollections[f.Part->DataSlot({})];
        cache->AddPageCollection(collection);
        TPrivatePageCacheBackend backend(cache);
        const auto& location = f.Locations.front();
        auto page = TSharedPageRef::MakePrivate(*f.Source->Store->GetPage(f.Part->DataSlot({}), location.Offset));
        const auto* bytes = TPinnedPageRef(page).GetData().data();
        TEvResult result(collection->PageCollection, NKikimrProto::OK, 0);
        result.Pages.emplace_back(location.Offset, location.Size, std::move(page));
        NTable::IPages& pages = f.Env;
        UNIT_ASSERT(!pages.TryGetPage(f.Part.Get(), location, {}));
        UNIT_ASSERT(!backend.AdoptLoadedPages(f.Seat, nullptr));
        UNIT_ASSERT(backend.AdoptLoadedPages(f.Seat, &result));
        UNIT_ASSERT(!result.Pages.front().Page);
        UNIT_ASSERT_VALUES_EQUAL(f.Seat.Pinned.at(collection->Id).size(), 1);
        UNIT_ASSERT(f.Seat.Pinned.at(collection->Id).at(location.Offset).PinnedBody.data() == bytes);
        UNIT_ASSERT(backend.AdoptLoadedPages(f.Seat, nullptr));
        backend.ReleasePins(f.Seat);
        UNIT_ASSERT(!backend.AdoptLoadedPages(f.Seat, nullptr));
        cache->DropPageCollection(collection.Get());
        result.Pages.clear();
        result.Pages.emplace_back(location.Offset, location.Size,
            TSharedPageRef::MakePrivate(*f.Source->Store->GetPage(f.Part->DataSlot({}), location.Offset)));
        UNIT_ASSERT(!backend.AdoptLoadedPages(f.Seat, &result));
        UNIT_ASSERT(result.Pages.front().Page);
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

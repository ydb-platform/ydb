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
        TPageCacheStats stats;
        UNIT_ASSERT(!pageCache.ReadStats(stats));

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

    Y_UNIT_TEST(BackendCapturesBootOwnersWithoutChangingActiveCache) {
        TReadFixture f;
        THolder<TPrivatePageCache> cache;
        TPrivatePageCacheBackend backend(cache);
        IExecutorPageCache& pageCache = backend;
        TExecutorCaches captured;
        const TLogoBlobID statusId(2, 3, 4, 1, 0, 0);
        captured.TxStatusCaches[statusId] = TSharedData::Copy("tx", 2);
        const auto* txData = captured.TxStatusCaches.at(statusId).data();
        pageCache.CaptureForRestart(captured);
        UNIT_ASSERT(captured.PageCollections.empty());
        UNIT_ASSERT_VALUES_EQUAL(captured.TxStatusCaches.size(), 1);
        UNIT_ASSERT(captured.TxStatusCaches.at(statusId).data() == txData);

        auto regular = f.Load(f.Locations[0]);
        auto sticky = f.Load(f.Locations[1], true);
        const auto& collection = f.Part->PageCollections[f.Part->DataSlot({})];
        f.Cache.UpdateCacheMode(ECacheMode::TryKeepInMemory, collection.Get());
        cache = MakeHolder<TPrivatePageCache>();
        cache->AddPageCollection(collection);
        pageCache.CaptureForRestart(captured);
        UNIT_ASSERT_VALUES_EQUAL(captured.PageCollections.size(), 1);
        const auto& snapshot = captured.PageCollections.at(collection->Id);
        UNIT_ASSERT(snapshot.Get() != collection.Get());
        UNIT_ASSERT(snapshot->PageCollection.Get() == collection->PageCollection.Get());
        UNIT_ASSERT(snapshot->GetCacheMode() == ECacheMode::TryKeepInMemory);
        UNIT_ASSERT(snapshot->FindPage(f.Locations[0].Offset)->SharedBody.GetHandle() == regular.GetHandle());
        UNIT_ASSERT(snapshot->IsStickyPage(f.Locations[1].Offset));
        UNIT_ASSERT(cache->FindPageCollection(collection->Id) == collection.Get());
        UNIT_ASSERT_VALUES_EQUAL(cache->GetStats().PageCollections, 1);
        UNIT_ASSERT(captured.TxStatusCaches.at(statusId).data() == txData);

        UNIT_ASSERT(pageCache.PrepareRegistration());
        f.Cache.DropPageCollection(collection.Get());
        regular.Drop();
        sticky.Drop();
        auto restored = snapshot->FindPage(f.Locations[0].Offset)->SharedBody;
        UNIT_ASSERT(restored.Use());
        UNIT_ASSERT_VALUES_EQUAL(TPinnedPageRef(restored).GetData().size(), f.Locations[0].Size);
        UNIT_ASSERT(snapshot->IsStickyPage(f.Locations[1].Offset));
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

#include "flat_executor_tx_env.h"
#include "flat_database.h"
#include "flat_part_loader.h"
#include "flat_writer_blocks.h"
#include "shared_cache_pages.h"

#include <ydb/core/base/appdata.h>

namespace NKikimr {
namespace NTabletFlatExecutor {
    namespace {

        class TPrivatePageCacheProtocol final : public IPageCacheProtocol {
        public:
            EPageCacheEvent EventKind(ui32 eventType) const noexcept override;
            TPageCacheResult ReadResult(NActors::IEventBase& event) const override;
            THolder<NActors::IEventBase> MakePageRequest(NBlockIO::EPriority priority,
                TIntrusiveConstPtr<NPageCollection::IPageCollection> collection,
                TVector<NTable::NPage::TPageLocation> locations, ui64 cookie) const override;
            THolder<NActors::IEventBase> MakeCollectionUpdate(
                TIntrusiveConstPtr<NPageCollection::IPageCollection> collection, ECacheMode mode,
                TVector<NPageCollection::TBtreeSeed> btreeSeeds) const override;
            THolder<NActors::IEventBase> MakeUnregisterRequest() const override;
        };

        THolder<NTable::IPartLoaderEnv> AddPrivateWrittenPages(
            void* context, TArrayRef<NWriter::TWrittenPageCollection> collections, TPageCacheEventSender send) {
            auto* cachePages = static_cast<NSharedCache::TSharedCachePages*>(context);
            TVector<TIntrusivePtr<TPrivatePageCache::TPageCollection>> prebuilt;
            for (auto& collection : collections) {
                auto cached = MakeIntrusive<TPrivatePageCache::TPageCollection>(collection.PageCollection);
                auto event = MakeHolder<NSharedCache::TEvSaveCompactedPages>(collection.PageCollection);
                auto gcList = cachePages->GCList;
                const auto addPage = [&](NPageCollection::TLoadedPage& loaded, bool sticky) {
                    const auto& location = loaded.Location;
                    auto page = MakeIntrusive<NSharedCache::TPage>(
                        location.Offset, location.Size, location.Type, location.Crc32, nullptr);
                    page->ProvideBody(std::move(loaded.Data));
                    event->Pages.push_back(page);
                    auto ref = NSharedCache::TSharedPageRef::MakeUsed(std::move(page), gcList, location.Type);
                    if (sticky) {
                        cached->AddStickyPage(location.Offset, location.Size, std::move(ref));
                    } else {
                        cached->AddPage(location.Offset, location.Size, std::move(ref));
                    }
                };
                for (auto& page : collection.StickyPages) {
                    addPage(page, true);
                }
                for (auto& page : collection.RegularPages) {
                    addPage(page, false);
                }
                send(std::move(event));
                prebuilt.push_back(std::move(cached));
            }
            return MakeHolder<NTable::TLoader::TLoaderEnv>(std::move(prebuilt));
        }

        const NTable::TPrivatePartStore& PrivatePart(const NTable::TPartStore& partStore) {
            return *CheckedCast<const NTable::TPrivatePartStore*>(&partStore);
        }

    } // namespace

    const IPageCacheProtocol& GetPrivatePageCacheProtocol() noexcept {
        static const TPrivatePageCacheProtocol protocol;
        return protocol;
    }

    THolder<NTable::IPartLoaderEnv> TWrittenPageCache::AddWrittenPages(
        TArrayRef<NWriter::TWrittenPageCollection> collections, TPageCacheEventSender send) const {
        return AddWrittenPages_(Context_, collections, send);
    }

    TWrittenPageCache MakePrivateWrittenPageCache(NSharedCache::TSharedCachePages& pages) noexcept {
        return TWrittenPageCache(&pages, AddPrivateWrittenPages);
    }

    TWrittenPageCache GetWrittenPageCache() {
        return MakePrivateWrittenPageCache(*AppData()->SharedCachePages);
    }

    bool TPrivatePageCacheBackend::PrepareRegistration() {
        const bool replayStickyWalks = bool(Cache_);
        Cache_ = MakeHolder<TPrivatePageCache>();
        return replayStickyWalks;
    }

    void TPrivatePageCacheBackend::CaptureForRestart(THolder<IPageCacheRestartState>& restartState) const {
        if (Cache_) {
            auto captured = MakeHolder<TPrivatePageCacheRestartState>();
            captured->PageCollections = Cache_->DetachPrivatePageCache();
            restartState = std::move(captured);
        }
    }

    void TPrivatePageCacheBackend::CaptureForRestart(
        const NTable::TPartStore& partStore, THolder<IPageCacheLoadedCollections>& loaded) const {
        const auto& part = PrivatePart(partStore);
        if (!loaded) {
            loaded = MakeHolder<TPrivateLoadedCollections>();
        }
        auto& retained = CheckedCast<TPrivateLoadedCollections*>(loaded.Get())->PageCollections;
        for (auto& pageCollection : part.PageCollections) {
            retained.push_back(pageCollection);
        }
        if (const auto& pageCollection = part.Pseudo) {
            retained.push_back(pageCollection);
        }
    }

    void TPrivatePageCacheBackend::MergeForRestart(
        THolder<IPageCacheRestartState>& restartState, const IPageCacheLoadedCollections* loaded) const {
        if (!loaded) {
            return;
        }
        if (!restartState) {
            restartState = MakeHolder<TPrivatePageCacheRestartState>();
        }
        auto& saved = CheckedCast<TPrivatePageCacheRestartState*>(restartState.Get())->PageCollections;
        for (const auto& collection : CheckedCast<const TPrivateLoadedCollections*>(loaded)->PageCollections) {
            saved[collection->Id] = collection;
        }
    }

    THolder<NTable::IPartLoaderEnv> TPrivatePageCacheBackend::MakeLoaderEnvironment(
        TConstArrayRef<NPageCollection::TLargeGlobId> collections, const IPageCacheRestartState* restartState) const {
        TVector<TIntrusivePtr<TPrivatePageCache::TPageCollection>> prebuilt(collections.size());
        if (restartState) {
            const auto& saved = CheckedCast<const TPrivatePageCacheRestartState*>(restartState)->PageCollections;
            for (size_t slot : xrange(collections.size())) {
                if (const auto* collection = saved.FindPtr(collections[slot].Lead)) {
                    prebuilt[slot] = *collection;
                }
            }
        }
        return MakeHolder<NTable::TLoader::TLoaderEnv>(std::move(prebuilt));
    }

    void TPrivatePageCacheBackend::PrepareCollectionModes(
        const NTable::TPartStore& partStore, TVector<ECacheMode>& modes, const TVector<bool>& stickyGroups) {
        const auto& collections = PrivatePart(partStore).PageCollections;
        for (ui32 slot : xrange(partStore.Slots.SlotsCount())) {
            collections[slot]->SetCacheMode(modes[slot]);
        }
        ReadCollectionModes(partStore, modes, stickyGroups);
    }

    TVector<bool> TPrivatePageCacheBackend::UpdateCollectionModes(
        const NTable::TPartStore& partStore, TVector<ECacheMode>& modes, const TVector<bool>& stickyGroups) {
        TVector<bool> changed(partStore.GetPageCollectionCount(), false);
        const auto& collections = PrivatePart(partStore).PageCollections;
        for (ui32 slot : xrange(partStore.Slots.SlotsCount())) {
            changed[slot] = Cache_->UpdateCacheMode(modes[slot], collections[slot].Get());
        }
        ReadCollectionModes(partStore, modes, stickyGroups);
        return changed;
    }

    void TPrivatePageCacheBackend::ReadCollectionModes(
        const NTable::TPartStore& partStore, TVector<ECacheMode>& modes, const TVector<bool>&) const {
        const auto& collections = PrivatePart(partStore).PageCollections;
        for (size_t slot = 0; slot < modes.size(); ++slot) {
            modes[slot] = collections[slot]->GetCacheMode();
        }
    }

    void TPrivatePageCacheBackend::RegisterCollection(const NTable::TPartStore& partStore, ui32 slot,
        ECacheMode cacheMode, TVector<NPageCollection::TBtreeSeed> btreeSeeds, bool replayStickyWalk,
        TPageCacheEventSender send) {
        RegisterCollection(
            PrivatePart(partStore).PageCollections[slot], cacheMode, std::move(btreeSeeds), replayStickyWalk, send);
    }

    void TPrivatePageCacheBackend::RegisterPseudoCollection(
        const NTable::TPartStore& partStore, TPageCacheEventSender send) {
        const auto& pseudo = PrivatePart(partStore).Pseudo;
        RegisterCollection(pseudo, pseudo->GetCacheMode(), {}, false, send);
    }

    void TPrivatePageCacheBackend::RegisterCollection(
        const TIntrusivePtr<TPrivatePageCache::TPageCollection>& pageCollection, ECacheMode cacheMode,
        TVector<NPageCollection::TBtreeSeed> btreeSeeds, bool replayStickyWalk, TPageCacheEventSender send) {
        auto syncPages = Cache_->AddPageCollection(pageCollection);
        send(MakeHolder<NSharedCache::TEvAttach>(
            pageCollection->PageCollection, cacheMode, std::move(btreeSeeds), replayStickyWalk));
        if (syncPages) {
            send(MakeHolder<NSharedCache::TEvSync>(std::move(syncPages)));
        }
    }

    THolder<NActors::IEventBase> TPrivatePageCacheBackend::DetachCollection(const TLogoBlobID& collectionId) {
        auto* pageCollection = Cache_->GetPageCollection(collectionId);
        Cache_->DropPageCollection(pageCollection);
        return MakeHolder<NSharedCache::TEvDetach>(collectionId);
    }

    std::unique_ptr<TSeat> TPrivatePageCacheBackend::MakeSeat(ui64 uniqueId, TAutoPtr<ITransaction> transaction) {
        return std::make_unique<TPrivateSeat>(uniqueId, std::move(transaction));
    }

    void TPrivatePageCacheBackend::WithTransactionPages(
        NTable::TDatabase& database, TSeat& seat, TFunctionRef<void(ITransactionPages&, TTransactionEnv&)> execute) {
        TPageCollectionTxEnv frame(database, *Cache_, *CheckedCast<TPrivateSeat*>(&seat));
        execute(frame, frame);
        // The callback may destroy the seat; do not access it afterward
    }

    void TPageCacheResult::Describe(IOutputStream& out) const {
        out << "TEvResult{" << PageCount << " pages"
            << " " << PageCollection->Label() << " " << (Status == NKikimrProto::OK ? "ok" : "fail") << " "
            << NKikimrProto::EReplyStatus_Name(Status) << "}";
    }

    EPageCacheEvent TPrivatePageCacheProtocol::EventKind(ui32 eventType) const noexcept {
        switch (eventType) {
            case NSharedCache::EvResult:
                return EPageCacheEvent::Result;
            case NSharedCache::EvUpdated:
                return EPageCacheEvent::Updated;
            case NSharedCache::EvStickyCollectionPages:
                return EPageCacheEvent::StickyPages;
            default:
                return EPageCacheEvent::Unhandled;
        }
    }

    TPageCacheResult TPrivatePageCacheProtocol::ReadResult(NActors::IEventBase& event) const {
        auto& result = *CheckedCast<NSharedCache::TEvResult*>(&event);
        return {
            .Status = result.Status,
            .PageCollection = result.PageCollection.Get(),
            .WaitPad = result.WaitPad,
            .Cookie = result.Cookie,
            .PageCount = result.Pages.size(),
        };
    }

    THolder<NActors::IEventBase> TPrivatePageCacheProtocol::MakePageRequest(NBlockIO::EPriority priority,
        TIntrusiveConstPtr<NPageCollection::IPageCollection> collection,
        TVector<NTable::NPage::TPageLocation> locations, ui64 cookie) const {
        return MakeHolder<NSharedCache::TEvRequest>(priority, std::move(collection), std::move(locations), cookie);
    }

    THolder<NActors::IEventBase> TPrivatePageCacheProtocol::MakeCollectionUpdate(
        TIntrusiveConstPtr<NPageCollection::IPageCollection> collection, ECacheMode mode,
        TVector<NPageCollection::TBtreeSeed> btreeSeeds) const {
        return MakeHolder<NSharedCache::TEvAttach>(std::move(collection), mode, std::move(btreeSeeds));
    }

    THolder<NActors::IEventBase> TPrivatePageCacheProtocol::MakeUnregisterRequest() const {
        return MakeHolder<NSharedCache::TEvUnregister>();
    }

    THolder<NActors::IEventBase> TPrivatePageCacheBackend::MakeTransactionRequest(const TLogoBlobID& collectionId,
        TVector<NTable::NPage::TPageLocation> locations, TIntrusivePtr<NPageCollection::TPagesWaitPad> waitPad,
        NWilson::TTraceId traceId) {
        auto request = MakeHolder<NSharedCache::TEvRequest>(
            NBlockIO::EPriority::Fast, Cache_->GetPageCollection(collectionId)->PageCollection, std::move(locations));
        request->TraceId = std::move(traceId);
        request->WaitPad = std::move(waitPad);
        return request;
    }

    void TPrivatePageCacheBackend::ProcessCacheUpdate(const NActors::IEventBase& event) {
        const auto* msg = CheckedCast<const NSharedCache::TEvUpdated*>(&event);
        for (auto& kv : msg->DroppedPages) {
            if (auto* pageCollection = Cache_->FindPageCollection(kv.first)) {
                for (auto offset : kv.second) {
                    Cache_->DropPage(offset, pageCollection);
                }
            }
        }
    }

    THolder<NActors::IEventBase> TPrivatePageCacheBackend::MakeStickyPageRequest(const NActors::IEventBase& event) {
        const auto* msg = CheckedCast<const NSharedCache::TEvStickyCollectionPages*>(&event);
        if (auto* pageCollection = Cache_->FindPageCollection(msg->CollectionId)) {
            return MakeHolder<NSharedCache::TEvRequest>(
                NBlockIO::EPriority::Bkgr, pageCollection->PageCollection, msg->Locations);
        }
        return {};
    }

    EPageResultStatus TPrivatePageCacheBackend::ProcessLoadedPages(NActors::IEventBase& event, bool sticky) {
        auto& result = *CheckedCast<NSharedCache::TEvResult*>(&event);
        auto* pageCollection = Cache_->FindPageCollection(result.PageCollection->Label());
        if (!pageCollection) {
            return EPageResultStatus::Ignored;
        }
        if (result.Status != NKikimrProto::OK) {
            return EPageResultStatus::Error;
        }
        if (sticky) {
            for (auto& loaded : result.Pages) {
                Cache_->AddStickyPage(loaded.Offset, loaded.Size, std::move(loaded.Page), pageCollection);
            }
        } else {
            for (auto& loaded : result.Pages) {
                Cache_->AddPage(loaded.Offset, loaded.Size, loaded.Page, pageCollection);
            }
        }
        return EPageResultStatus::Ready;
    }

    bool TPrivatePageCacheBackend::AdoptLoadedPages(TSeat& seat, NActors::IEventBase* event) {
        auto& pinned = CheckedCast<TPrivateSeat*>(&seat)->Pinned;
        if (event) {
            auto& result = *CheckedCast<NSharedCache::TEvResult*>(event);
            if (auto* collection = Cache_->FindPageCollection(result.PageCollection->Label())) {
                auto& pinnedCollection = pinned[collection->Id];
                for (auto& loaded : result.Pages) {
                    auto inserted = pinnedCollection.insert(
                        std::make_pair(loaded.Offset, TPrivatePageCache::TPinnedPage(std::move(loaded.Page))));
                    Y_ENSURE(inserted.second);
                }
                if (!pinnedCollection.empty()) {
                    return true;
                }
            }
        }
        for (const auto& [id, pages] : pinned) {
            if (!pages.empty()) {
                return true;
            }
        }
        return false;
    }

    void TPrivatePageCacheBackend::ReleasePins(TSeat& seat) {
        CheckedCast<TPrivateSeat*>(&seat)->Pinned.clear();
    }

    bool TPrivatePageCacheBackend::ReadStats(TPageCacheStats& stats) const noexcept {
        if (!Cache_) {
            return false;
        }
        stats = Cache_->GetStats();
        return true;
    }

    void TTransactionEnv::MakeSnapshot(TIntrusivePtr<TTableSnapshotContext> snap)
    {
        auto tables = snap->TablesToSnapshot();
        Y_ENSURE(tables);

        for (ui32 table : tables) {
            auto& entry = MakeSnap[table];
            entry.Context.push_back(snap);
            auto epoch = DB.TxSnapTable(table);
            if (entry.Epoch) {
                Y_ENSURE(*entry.Epoch == epoch, "Table snapshot changed unexpectedly");
            } else {
                entry.Epoch.emplace(epoch);
            }
        }
    }

} // namespace NTabletFlatExecutor
} // namespace NKikimr

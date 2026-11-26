#include "flat_executor_tx_env.h"
#include "flat_database.h"

namespace NKikimr {
namespace NTabletFlatExecutor {

    bool TPrivatePageCacheBackend::PrepareRegistration() {
        const bool replayStickyWalks = bool(Cache_);
        Cache_ = MakeHolder<TPrivatePageCache>();
        return replayStickyWalks;
    }

    void TPrivatePageCacheBackend::CaptureForRestart(TExecutorCaches& caches) const {
        if (Cache_) {
            caches.PageCollections = Cache_->DetachPrivatePageCache();
        }
    }

    void TPrivatePageCacheBackend::PrepareCollectionModes(
        const NTable::TPartStore& partStore, TVector<ECacheMode>& modes, const TVector<bool>& stickyGroups) {
        for (ui32 slot : xrange(partStore.Slots.SlotsCount())) {
            partStore.PageCollections[slot]->SetCacheMode(modes[slot]);
        }
        ReadCollectionModes(partStore, modes, stickyGroups);
    }

    TVector<bool> TPrivatePageCacheBackend::UpdateCollectionModes(
        const NTable::TPartStore& partStore, TVector<ECacheMode>& modes, const TVector<bool>& stickyGroups) {
        TVector<bool> changed(partStore.GetPageCollectionCount(), false);
        for (ui32 slot : xrange(partStore.Slots.SlotsCount())) {
            changed[slot] = Cache_->UpdateCacheMode(modes[slot], partStore.PageCollections[slot].Get());
        }
        ReadCollectionModes(partStore, modes, stickyGroups);
        return changed;
    }

    void TPrivatePageCacheBackend::ReadCollectionModes(
        const NTable::TPartStore& partStore, TVector<ECacheMode>& modes, const TVector<bool>&) const {
        for (size_t slot = 0; slot < modes.size(); ++slot) {
            modes[slot] = partStore.PageCollections[slot]->GetCacheMode();
        }
    }

    void TPrivatePageCacheBackend::RegisterCollection(const NTable::TPartStore& partStore, ui32 slot,
        ECacheMode cacheMode, TVector<NPageCollection::TBtreeSeed> btreeSeeds, bool replayStickyWalk,
        TPageCacheEventSender send) {
        RegisterCollection(partStore.PageCollections[slot], cacheMode, std::move(btreeSeeds), replayStickyWalk, send);
    }

    void TPrivatePageCacheBackend::RegisterPseudoCollection(
        const NTable::TPartStore& partStore, TPageCacheEventSender send) {
        RegisterCollection(partStore.Pseudo, partStore.Pseudo->GetCacheMode(), {}, false, send);
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
        return std::make_unique<TSeat>(uniqueId, std::move(transaction));
    }

    void TPrivatePageCacheBackend::WithTransactionPages(
        NTable::TDatabase& database, TSeat& seat, TFunctionRef<void(ITransactionPages&, TTransactionEnv&)> execute) {
        TPageCollectionTxEnv frame(database, *Cache_, seat);
        execute(frame, frame);
        // The callback may destroy the seat; do not access it afterward
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
        if (event) {
            auto& result = *CheckedCast<NSharedCache::TEvResult*>(event);
            if (auto* collection = Cache_->FindPageCollection(result.PageCollection->Label())) {
                auto& pinnedCollection = seat.Pinned[collection->Id];
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
        for (const auto& [id, pages] : seat.Pinned) {
            if (!pages.empty()) {
                return true;
            }
        }
        return false;
    }

    void TPrivatePageCacheBackend::ReleasePins(TSeat& seat) {
        seat.Pinned.clear();
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

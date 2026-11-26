#pragma once

#include "flat_part_iface.h"

#include <util/generic/function_ref.h>
#include <util/generic/hash.h>
#include <util/generic/ptr.h>
#include <util/generic/vector.h>

#include <memory>

namespace NWilson {
class TTraceId;
}

namespace NKikimr::NPageCollection {
struct TPagesWaitPad;
struct TBtreeSeed;
}

namespace NKikimr::NTable {
class TDatabase;
class TPartStore;
}

namespace NKikimr::NTabletFlatExecutor {

class ITransaction;
struct TSeat;
struct TTransactionEnv;
struct TExecutorCaches;

struct TPageReadStats {
    size_t NewlyPinnedPages = 0;
    ui64 NewlyPinnedBytes = 0;
    size_t ToLoadPages = 0;
    ui64 ToLoadBytes = 0;
};

using TPageRequests = THashMap<TLogoBlobID, TVector<NTable::NPage::TPageLocation>>;

class ITransactionPages : public NTable::IPages {
public:
    virtual const TPageReadStats& GetStats() const = 0;
    virtual TPageRequests ObtainToLoad() = 0;
    virtual void EnableReadMissingReferences() = 0;
    virtual void DisableReadMissingReferences() = 0;
    virtual ui64 MissingReferencesSize() const = 0;
};

struct TPageCacheStats {
    ui64 PageCollections = 0;
    ui64 SharedBodyBytes = 0;
    ui64 StickyBytes = 0;
    ui64 TryKeepInMemoryBytes = 0;
};

using TPageCacheEventSender = TFunctionRef<void(THolder<NActors::IEventBase>)>;

enum class EPageResultStatus {
    Ignored,
    Error,
    Ready,
};

// Backend operations shared by the executor; concrete pin owners stay private
class IExecutorPageCache {
public:
    virtual ~IExecutorPageCache() = default;

    // Returns whether previously registered Sticky walks need replay
    virtual bool PrepareRegistration() = 0;
    virtual void CaptureForRestart(TExecutorCaches& caches) const = 0;
    // Resolve this executor's requested modes in place using backend-specific Sticky rules
    virtual void PrepareCollectionModes(const NTable::TPartStore& partStore, TVector<NTable::NPage::ECacheMode>& modes,
        const TVector<bool>& stickyGroups) = 0;
    virtual TVector<bool> UpdateCollectionModes(const NTable::TPartStore& partStore,
        TVector<NTable::NPage::ECacheMode>& modes, const TVector<bool>& stickyGroups) = 0;
    virtual void ReadCollectionModes(const NTable::TPartStore& partStore, TVector<NTable::NPage::ECacheMode>& modes,
        const TVector<bool>& stickyGroups) const = 0;

    // Sends attachment and optional synchronization events synchronously, in that order
    virtual void RegisterCollection(const NTable::TPartStore& partStore, ui32 slot, NTable::NPage::ECacheMode cacheMode,
        TVector<NPageCollection::TBtreeSeed> btreeSeeds, bool replayStickyWalk, TPageCacheEventSender send) = 0;
    virtual void RegisterPseudoCollection(const NTable::TPartStore& partStore, TPageCacheEventSender send) = 0;
    virtual THolder<NActors::IEventBase> DetachCollection(const TLogoBlobID& collectionId) = 0;

    virtual std::unique_ptr<TSeat> MakeSeat(ui64 uniqueId, TAutoPtr<ITransaction> transaction) = 0;

    // Calls execute once synchronously; frame references must not escape
    virtual void WithTransactionPages(
        NTable::TDatabase& database, TSeat& seat, TFunctionRef<void(ITransactionPages&, TTransactionEnv&)> execute) = 0;

    // Prepares the fresh wait pad before the executor sends any page requests
    virtual TPageRequests PreparePageRequests(
        const TSeat&, ITransactionPages& pages, NPageCollection::TPagesWaitPad&) const {
        return pages.ObtainToLoad();
    }

    virtual THolder<NActors::IEventBase> MakeTransactionRequest(const TLogoBlobID& collectionId,
        TVector<NTable::NPage::TPageLocation> locations, TIntrusivePtr<NPageCollection::TPagesWaitPad> waitPad,
        NWilson::TTraceId traceId) = 0;

    virtual void ProcessCacheUpdate(const NActors::IEventBase& update) = 0;
    virtual THolder<NActors::IEventBase> MakeStickyPageRequest(const NActors::IEventBase& notification) = 0;

    virtual EPageResultStatus ProcessLoadedPages(NActors::IEventBase& result, bool sticky) = 0;
    virtual bool AdoptLoadedPages(TSeat& seat, NActors::IEventBase* result) = 0;
    virtual void ReleasePins(TSeat& seat) = 0;
    virtual bool ReadStats(TPageCacheStats& stats) const noexcept = 0;
};

} // namespace NKikimr::NTabletFlatExecutor

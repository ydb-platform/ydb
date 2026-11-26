#pragma once

#include "flat_part_iface.h"
#include "flat_bio_eggs.h"
#include "flat_sausage_gut.h"

#include <ydb/core/protos/base.pb.h>

#include <util/generic/array_ref.h>
#include <util/generic/function_ref.h>
#include <util/generic/hash.h>
#include <util/generic/ptr.h>
#include <util/generic/strbuf.h>
#include <util/generic/vector.h>

#include <memory>

namespace NActors {
class IEventHandle;
class TActorSystem;
} // namespace NActors

namespace NWilson {
class TTraceId;
}

namespace NKikimr::NPageCollection {
struct TPagesWaitPad;
}

namespace NKikimr::NTable {
class TDatabase;
class TPartStore;
struct IPartLoaderEnv;
enum class EMetadataLoadResult;
}

namespace NKikimr::NTabletFlatExecutor {

class ITransaction;
struct TSeat;
struct TTransactionEnv;

namespace NWriter {
    struct TWrittenPageCollection;
}

class IPageCacheRestartState {
public:
    virtual ~IPageCacheRestartState() = default;
};

class IPageCacheLoadedCollections {
public:
    virtual ~IPageCacheLoadedCollections() = default;
};

struct TExecutorCaches {
    THolder<IPageCacheRestartState> PageCollections;
    THashMap<TLogoBlobID, TSharedData> TxStatusCaches;
};

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

// Borrows the application's cache state for the writer's lifetime
class TWrittenPageCache {
    using TAddWrittenPages = THolder<NTable::IPartLoaderEnv> (*)(
        void*, TArrayRef<NWriter::TWrittenPageCollection>, TPageCacheEventSender);

public:
    TWrittenPageCache() = default;

    TWrittenPageCache(void* context, TAddWrittenPages addWrittenPages) noexcept
        : Context_(context)
        , AddWrittenPages_(addWrittenPages)
    {}

    // The sender is called synchronously and is not retained
    THolder<NTable::IPartLoaderEnv> AddWrittenPages(
        TArrayRef<NWriter::TWrittenPageCollection> collections, TPageCacheEventSender send) const;

private:
    void* Context_ = nullptr;
    TAddWrittenPages AddWrittenPages_ = nullptr;
};

TWrittenPageCache GetWrittenPageCache();

enum class ERequestTypeCookie : ui64 {
    Undefined = 0,
    Transaction = 1,
    StickyPages,
    PendingInit,
    BootLogic,
    TryKeepInMemPages,
};

enum class EPageCacheEvent {
    Unhandled,
    Result,
    Updated,
    StickyPages,
};

enum class EPageCacheResultKind {
    Pages,
    WorkingSetTooLarge,
    ResourcesReady,
    ResourcePressure,
};

// Borrows from the live source event and must not escape its dispatch
struct TPageCacheResult {
    void Describe(IOutputStream& out) const;

    NKikimrProto::EReplyStatus Status;
    const NPageCollection::IPageCollection* PageCollection;
    TIntrusivePtr<NPageCollection::TPagesWaitPad>& WaitPad;
    ui64 Cookie;
    size_t PageCount;
    // Control results precede normal descriptor and storage-status handling
    EPageCacheResultKind Kind = EPageCacheResultKind::Pages;
};

struct TPageCacheMetadataResult {
    // The source event keeps this identity alive through dispatch
    const void* Request = nullptr;
    NKikimrProto::EReplyStatus Status = NKikimrProto::OK;
    TStringBuf Error;
};

class IPageMetadataLoadGroup : public TThrRefBase {
public:
    virtual ~IPageMetadataLoadGroup() = default;
};

enum class EPageResultStatus {
    Ignored,
    Error,
    Ready,
};

// Immutable protocol instances outlive all actors and environments that use them
class IPageCacheProtocol {
public:
    virtual ~IPageCacheProtocol() = default;

    virtual EPageCacheEvent EventKind(ui32 eventType) const noexcept = 0;
    virtual TPageCacheResult ReadResult(NActors::IEventBase& event) const = 0;

    virtual bool ReadMetadataResult(NActors::IEventHandle&, TPageCacheMetadataResult&) const {
        return false;
    }
    virtual THolder<NActors::IEventBase> MakePageRequest(NBlockIO::EPriority priority,
        TIntrusiveConstPtr<NPageCollection::IPageCollection> collection,
        TVector<NTable::NPage::TPageLocation> locations, ui64 cookie = 0) const = 0;
    virtual THolder<NActors::IEventBase> MakeCollectionUpdate(
        TIntrusiveConstPtr<NPageCollection::IPageCollection> collection, NTable::NPage::ECacheMode mode,
        TVector<NPageCollection::TBtreeSeed> btreeSeeds = {}) const = 0;
    virtual THolder<NActors::IEventBase> MakeUnregisterRequest() const = 0;
};

// Backend operations shared by the executor; concrete pin owners stay private
class IExecutorPageCache {
public:
    virtual ~IExecutorPageCache() = default;

    // Returns whether previously registered Sticky walks need replay
    virtual bool PrepareRegistration() = 0;
    virtual void CaptureForRestart(THolder<IPageCacheRestartState>& restartState) const = 0;
    virtual void CaptureForRestart(
        const NTable::TPartStore& partStore, THolder<IPageCacheLoadedCollections>& loaded) const = 0;
    virtual void MergeForRestart(
        THolder<IPageCacheRestartState>& restartState, const IPageCacheLoadedCollections* loaded) const = 0;
    // The environment owns its prebuilt collections independently of the restart state
    virtual THolder<NTable::IPartLoaderEnv> MakeLoaderEnvironment(
        TConstArrayRef<NPageCollection::TLargeGlobId> collections = {},
        const IPageCacheRestartState* restartState = nullptr) const = 0;
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

    virtual const IPageCacheProtocol& Protocol() const noexcept = 0;

    THolder<NActors::IEventBase> MakeUnregisterRequest() const {
        return Protocol().MakeUnregisterRequest();
    }

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

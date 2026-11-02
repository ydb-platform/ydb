#pragma once

#include "flat_bio_events.h"
#include "shared_cache_events.h"
#include "shared_page.h"
#include "shared_cache_memory.h"

#include <util/generic/set.h>

namespace NKikimr::NSharedCache {

struct TCollection;
class TCollections;

struct TMetadataRequestTag {};

struct TRequest : public TSimpleRefCount<TRequest>, public TIntrusiveListItem<TRequest>,
                  public TIntrusiveListItem<TRequest, TMetadataRequestTag> {
    ~TRequest();
    void ReleaseMetadata();
    void RetireQueuedWalk();
    bool IsResponded() const {
        return !Sender;
    }

    void MarkResponded() {
        Sender = {};
    }

    TLogoBlobID Label;
    TActorId Sender;
    NTabletFlatExecutor::NBlockIO::EPriority Priority;
    TIntrusiveConstPtr<NPageCollection::IPageCollection> PageCollection;
    TCollection* MetadataOwner = nullptr;
    ui64 EventCookie = 0;
    ui64 RequestCookie = 0;
    ui64 PendingBlocks = 0;
    TVector<TEvResult::TLoaded> ReadyPages;
    TDeque<TPageOffset> QueuePagesToRequest; // FIXME: store first pending page
    TIntrusivePtr<NPageCollection::TPagesWaitPad> WaitPad;
    NWilson::TTraceId TraceId;
    TLogoBlobID WalkCollectionId;
};

// pending request, index in ready blocks for page
using TPendingRequests = THashMap<TIntrusivePtr<TRequest>, ui32>;

struct TPageByOffsetHash {
    size_t operator()(const TIntrusivePtr<TPage>& ptr) const {
        return THash<TPageOffset>()(ptr->Offset);
    }

    size_t operator()(TPageOffset offset) const {
        return THash<TPageOffset>()(offset);
    }
};

struct TPageByOffsetEq {
    bool operator()(const TIntrusivePtr<TPage>& a, const TIntrusivePtr<TPage>& b) const {
        return a->Offset == b->Offset;
    }

    bool operator()(const TIntrusivePtr<TPage>& a, TPageOffset b) const {
        return a->Offset == b;
    }

    bool operator()(TPageOffset a, const TIntrusivePtr<TPage>& b) const {
        return a == b->Offset;
    }
};

using TPageSetBase = THashSet<TIntrusivePtr<TPage>, TPageByOffsetHash, TPageByOffsetEq>;

class TPageSet : public TPageSetBase {
public:
    using TPageSetBase::TPageSetBase;

    TPage* FindPage(TPageOffset offset) const {
        auto it = find(offset);
        return it != end() ? it->Get() : nullptr;
    }

    bool ErasePage(TPageOffset offset) {
        auto it = find(offset);
        if (it == end()) {
            return false;
        }
        erase(it);
        return true;
    }
};

struct TCollection {
    TCollection() = default;
    ~TCollection();
    TCollection(const TCollection&) = delete;
    TCollection& operator=(const TCollection&) = delete;
    TCollection(TCollection&&) = delete;
    TCollection& operator=(TCollection&&) = delete;

    struct TMetadataReference {
        const NPageCollection::IPageCollection* Descriptor;
        size_t References;
    };
    TLogoBlobID Id;
    TIntrusiveConstPtr<NPageCollection::IPageCollection> PageCollection;
    TCollections* CollectionSet = nullptr;
    // Descriptor objects and Steps; allocation tags account for shared Raw buffers.
    i64 MetadataBytes = 0;
    size_t MetadataRequestCount = 0; // Requests retaining PageCollection
    THolder<TVector<TMetadataReference>> RetiredMetadata;
    TIntrusiveList<TRequest, TMetadataRequestTag> MetadataRequests;
    TSet<TActorId> InMemoryOwners;
    TSet<TActorId> Owners;
    TPageSet PageSet;
    ui64 TotalSize;
    ui64 AliveBytes; // Include sizes of all pages presented in the PageSet (Active + Passive + InFly)
    ui64 TotalPages = 0;
    THashMap<TPageOffset, TPendingRequests> PendingRequests;
    TDeque<TPageOffset> DroppedPages;

    void SetPageCollection(TIntrusiveConstPtr<NPageCollection::IPageCollection> collection);
    void AddMetadataRequest(TRequest& request);
    void RemoveMetadataRequest(TRequest& request);
    void RefMetadata(const NPageCollection::IPageCollection& collection);
    void UnrefMetadata(const NPageCollection::IPageCollection& collection);
    bool HasSharedMeta(
        const NPageCollection::TMeta* meta, const NPageCollection::IPageCollection* except = nullptr) const;

    ECacheMode GetCacheMode() {
        return InMemoryOwners ? ECacheMode::TryKeepInMemory : ECacheMode::Regular;
    }
};

class TCollections {
public:
    TCollections() = default;
    TCollections(const TCollections&) = delete;
    TCollections& operator=(const TCollections&) = delete;
    TCollections(TCollections&&) = delete;
    TCollections& operator=(TCollections&&) = delete;

    TCollection& operator[](const TLogoBlobID& id) {
        auto& collection = Entries[id];
        collection.CollectionSet = this;
        return collection;
    }

    TCollection* FindPtr(const TLogoBlobID& id) {
        return Entries.FindPtr(id);
    }

    bool contains(const TLogoBlobID& id) const {
        return Entries.contains(id);
    }

    void erase(const TLogoBlobID& id) {
        Entries.erase(id);
    }

    i64 GetMetadataMemoryUsage() const noexcept {
        return MetadataBytes + MetadataBuffers.GetMemoryUsage();
    }

    TMetadataBuffers MetadataBuffers;

private:
    friend struct TCollection;
    i64 MetadataBytes = 0;
    // Destroy entries before their shared accounting state
    THashMap<TLogoBlobID, TCollection> Entries;
};

} // namespace NKikimr::NSharedCache

#include "shared_cache_memory.h"
#include "shared_sausagecache_state.h"
#include "flat_sausage_meta.h"

#include <util/generic/algorithm.h>

#include <utility>

namespace NKikimr::NSharedCache {

// Tags may outlive the cache, so they hold this counter rather than an actor pointer.
class TMetadataBuffers::TAccount : public TAtomicRefCount<TAccount> {
public:
    std::atomic<i64> MetadataBytes{ 0 };
};

class TMetadataBuffers::TBufferTag {
    struct TCharge {
        TIntrusivePtr<TAccount> Account;
        TCharge* Next = nullptr;
        // These describe cache charges, not TSharedData's allocation reference count.
        // Only the owning cache actor updates this entry.
        i64 MetadataReferences = 0;
        i64 PageBytes = 0;

        i64 GetMetadataBytes(i64 allocationBytes) const noexcept {
            if (!MetadataReferences) {
                return 0;
            }
            return Max<i64>(0, allocationBytes - PageBytes);
        }
    };

public:
    ~TBufferTag() {
        auto* charge = Charges.load(std::memory_order_relaxed);
        while (charge) {
            auto* next = charge->Next;
            Y_DEBUG_ABORT_UNLESS(!charge->MetadataReferences);
            delete charge;
            charge = next;
        }
    }

    void Update(const TIntrusivePtr<TAccount>& account, i64 allocationBytes, i64 referencesDelta, i64 pageBytesDelta) {
        auto& charge = GetCharge(account);
        const i64 previous = charge.GetMetadataBytes(allocationBytes);
        charge.MetadataReferences += referencesDelta;
        charge.PageBytes += pageBytesDelta;
        Y_ENSURE(charge.MetadataReferences >= 0 && charge.PageBytes >= 0);
        account->MetadataBytes.fetch_add(
            charge.GetMetadataBytes(allocationBytes) - previous, std::memory_order_relaxed);
    }

private:
    TCharge& GetCharge(const TIntrusivePtr<TAccount>& account) {
        auto* head = Charges.load(std::memory_order_acquire);
        THolder<TCharge> candidate;
        for (;;) {
            for (auto* charge = head; charge; charge = charge->Next) {
                if (charge->Account == account) {
                    return *charge;
                }
            }
            if (!candidate) {
                candidate = MakeHolder<TCharge>();
                candidate->Account = account;
            }
            candidate->Next = head;
            if (Charges.compare_exchange_weak(
                    head, candidate.Get(), std::memory_order_acq_rel, std::memory_order_acquire)) {
                return *candidate.Release();
            }
        }
    }

    // Entries live with the allocation; another cache may register the same buffer.
    // Keeping the account alive prevents stale actor pointers and address reuse.
    std::atomic<TCharge*> Charges{ nullptr };
};

TMetadataBuffers::TMetadataBuffers()
    : Account(MakeIntrusive<TAccount>())
{}

TMetadataBuffers::~TMetadataBuffers() = default;

i64 TMetadataBuffers::GetMemoryUsage() const noexcept {
    return Account->MetadataBytes.load(std::memory_order_relaxed);
}

void TMetadataBuffers::Update(const TSharedData& data, i64 referencesDelta, i64 pageBytesDelta) {
    Y_ENSURE(data);
    auto& tag = data.GetOrCreateAllocationTag<TBufferTag>();
    tag.Update(Account, data.GetAllocationSize(), referencesDelta, pageBytesDelta);
}

void TMetadataBuffers::UpdateMetadataReferences(const TSharedData& buffer, i64 delta) {
    Update(buffer, delta, 0);
}

void TMetadataBuffers::UpdatePageCharge(const TSharedData& buffer, i64 delta) {
    Update(buffer, 0, delta);
}

TRequest::~TRequest() {
    ReleaseMetadata();
}

void TRequest::ReleaseMetadata() {
    if (MetadataOwner) {
        MetadataOwner->RemoveMetadataRequest(*this);
    }
}

void TRequest::RetireQueuedWalk() {
    Y_ENSURE(WalkCollectionId);
    ReleaseMetadata();
    PageCollection.Reset();
    MarkResponded();
}

TCollection::~TCollection() {
    while (!MetadataRequests.Empty()) {
        MetadataRequests.Front()->ReleaseMetadata();
    }
    if (CollectionSet && PageCollection) {
        UnrefMetadata(*PageCollection);
    }
    Y_DEBUG_ABORT_UNLESS(!MetadataRequestCount && !RetiredMetadata && !MetadataBytes);
}

bool TCollection::HasSharedMeta(
    const NPageCollection::TMeta* meta, const NPageCollection::IPageCollection* except) const {
    if (PageCollection && PageCollection.Get() != except && PageCollection->GetSharedMeta().Get() == meta) {
        return true;
    }
    if (RetiredMetadata) {
        for (const auto& reference : *RetiredMetadata) {
            if (reference.Descriptor != except && reference.Descriptor->GetSharedMeta().Get() == meta) {
                return true;
            }
        }
    }
    return false;
}

void TCollection::RefMetadata(const NPageCollection::IPageCollection& collection) {
    const auto* raw = collection.GetMetadataBuffer();
    const bool hasRaw = raw && *raw;
    i64 bytes = collection.GetMetadataMemoryUsage();
    if (const auto meta = collection.GetSharedMeta(); meta && HasSharedMeta(meta.Get())) {
        bytes -= meta->GetMemoryUsage();
    } else if (hasRaw) {
        bytes -= raw->GetAllocationSize();
    }
    Y_ENSURE(bytes >= 0);
    if (hasRaw) {
        CollectionSet->MetadataBuffers.UpdateMetadataReferences(*raw, 1);
    }
    MetadataBytes += bytes;
    CollectionSet->MetadataBytes += bytes;
}

void TCollection::UnrefMetadata(const NPageCollection::IPageCollection& collection) {
    const auto* raw = collection.GetMetadataBuffer();
    const bool hasRaw = raw && *raw;
    i64 bytes = collection.GetMetadataMemoryUsage();
    if (const auto meta = collection.GetSharedMeta(); meta && HasSharedMeta(meta.Get(), &collection)) {
        bytes -= meta->GetMemoryUsage();
    } else if (hasRaw) {
        bytes -= raw->GetAllocationSize();
    }
    if (hasRaw) {
        CollectionSet->MetadataBuffers.UpdateMetadataReferences(*raw, -1);
    }
    MetadataBytes -= bytes;
    CollectionSet->MetadataBytes -= bytes;
    Y_ENSURE(MetadataBytes >= 0 && CollectionSet->MetadataBytes >= 0);
}

void TCollection::SetPageCollection(TIntrusiveConstPtr<NPageCollection::IPageCollection> collection) {
    Y_ENSURE(CollectionSet);
    Y_ENSURE(!collection || collection->Label() == Id);
    if (collection.Get() == PageCollection.Get()) {
        return;
    }
    THolder<TVector<TMetadataReference>> newRetired;
    auto* retired = RetiredMetadata.Get();
    if (MetadataRequestCount) {
        if (!retired) {
            newRetired = MakeHolder<TVector<TMetadataReference>>();
            retired = newRetired.Get();
        }
        retired->reserve(retired->size() + 1);
    }
    size_t retiredIndex = 0;
    while (retired && retiredIndex < retired->size() && (*retired)[retiredIndex].Descriptor != collection.Get()) {
        ++retiredIndex;
    }
    const bool wasRetired = retired && retiredIndex < retired->size();
    const size_t nextRequestCount = wasRetired ? (*retired)[retiredIndex].References : 0;
    if (collection && !wasRetired) {
        RefMetadata(*collection);
    }
    if (newRetired) {
        RetiredMetadata = std::move(newRetired);
    }
    if (wasRetired) {
        retired->erase(retired->begin() + retiredIndex);
    }
    if (MetadataRequestCount) {
        retired->push_back({ PageCollection.Get(), MetadataRequestCount });
    }
    auto previous = std::exchange(PageCollection, std::move(collection));
    const bool releasePrevious = previous && !MetadataRequestCount;
    MetadataRequestCount = nextRequestCount;
    if (releasePrevious) {
        UnrefMetadata(*previous);
    }
    if (RetiredMetadata && RetiredMetadata->empty()) {
        RetiredMetadata.Reset();
    }
}

void TCollection::AddMetadataRequest(TRequest& request) {
    Y_ENSURE(CollectionSet && !request.MetadataOwner && request.PageCollection == PageCollection);
    ++MetadataRequestCount;
    request.MetadataOwner = this;
    MetadataRequests.PushBack(&request);
}

void TCollection::RemoveMetadataRequest(TRequest& request) {
    Y_ENSURE(request.MetadataOwner == this);
    request.MetadataOwner = nullptr;
    request.TIntrusiveListItem<TRequest, TMetadataRequestTag>::Unlink();
    if (request.PageCollection == PageCollection) {
        Y_ENSURE(MetadataRequestCount);
        --MetadataRequestCount;
        return;
    }
    Y_ENSURE(RetiredMetadata);
    for (auto it = RetiredMetadata->begin(); it != RetiredMetadata->end(); ++it) {
        if (it->Descriptor == request.PageCollection.Get()) {
            Y_ENSURE(it->References);
            if (!--it->References) {
                UnrefMetadata(*request.PageCollection);
                RetiredMetadata->erase(it);
                if (RetiredMetadata->empty()) {
                    RetiredMetadata.Reset();
                }
            }
            return;
        }
    }
    Y_ABORT("Unregistered collection metadata reference");
}

} // namespace NKikimr::NSharedCache

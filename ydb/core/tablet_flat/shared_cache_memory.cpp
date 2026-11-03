#include "shared_cache_memory.h"
#include "shared_sausagecache_state.h"

#include <utility>

namespace NKikimr::NSharedCache {

// The shared allocator owner outlives the cache whenever a caller still holds its buffer
class TMetadataAccount final : public TAtomicRefCount<TMetadataAccount>, public TSharedData::IOwner {
public:
    size_t GetAllocationSize(const char* data) const noexcept override {
        return BuiltinAllocationSize(data);
    }

    void* FindAllocationTag(const void* key) const noexcept override {
        return key == TSharedData::AllocationTagKey<TMetadataAccount>() ? const_cast<TMetadataAccount*>(this) : nullptr;
    }

    void Deallocate(char* data) noexcept override {
        const ui64 bytes = BuiltinAllocationSize(data);
        DeallocateBuiltin(data);
        Bytes.fetch_sub(bytes, std::memory_order_relaxed);
        UnRef();
    }

    std::atomic<ui64> Bytes{ 0 };
};

// One record per physical object/account; a losing unpublished candidate has no charge
class TMetadataRecord {
public:
    explicit TMetadataRecord(TIntrusivePtr<TMetadataAccount> account) noexcept
        : Account(std::move(account))
    {}

    ~TMetadataRecord() {
        Account->Bytes.fetch_sub(Bytes, std::memory_order_relaxed);
    }

    void Charge(ui64 bytes) noexcept {
        Account->Bytes.fetch_add(bytes, std::memory_order_relaxed);
        Bytes = bytes;
    }

    const TIntrusivePtr<TMetadataAccount> Account;
    TMetadataRecord* Next = nullptr;
    ui64 Bytes = 0;
};

static void DestroyMetadataRecords(TMetadataRecord* record) noexcept {
    while (record) {
        auto* next = record->Next;
        delete record;
        record = next;
    }
}

namespace {

    // Only pre-existing custom owners and buffers shared between caches need this fallback
    struct TMetadataBufferAccounts {
        ~TMetadataBufferAccounts() {
            DestroyMetadataRecords(Records.load(std::memory_order_relaxed));
        }

        std::atomic<TMetadataRecord*> Records{ nullptr };
    };

} // namespace

TMetadataAccounting::TMetadataAccounting()
    : Account_(MakeIntrusive<TMetadataAccount>())
{}

TMetadataAccounting::TMetadataAccounting(TIntrusivePtr<TMetadataAccount> account) noexcept
    : Account_(std::move(account))
{}

TMetadataAccounting::TMetadataAccounting(TMetadataAccounting&&) noexcept = default;
TMetadataAccounting& TMetadataAccounting::operator=(TMetadataAccounting&&) noexcept = default;
TMetadataAccounting::~TMetadataAccounting() = default;

TMetadataAccounting TMetadataAccounting::Share() const noexcept {
    return TMetadataAccounting(Account_);
}

ui64 TMetadataAccounting::GetMemoryUsage() const noexcept {
    return Account_->Bytes.load(std::memory_order_relaxed);
}

void TMetadataAccounting::AccountBuffer(const TSharedData& buffer) const {
    if (!buffer.data() || buffer.FindAllocationTag<TMetadataAccount>() == Account_.Get()) {
        return;
    }
    const ui64 bytes = buffer.GetAllocationSize();
    if (buffer.TrySetBuiltinOwner(*Account_)) {
        // The facade and input buffer protect the owner until its lifetime reference exists
        Account_->Ref();
        Account_->Bytes.fetch_add(bytes, std::memory_order_relaxed);
        return;
    }
    if (buffer.FindAllocationTag<TMetadataAccount>() == Account_.Get()) {
        return;
    }
    AccountObject(buffer.GetOrCreateAllocationTag<TMetadataBufferAccounts>().Records, bytes);
}

void TMetadataAccounting::AccountMetadata(const NPageCollection::IPageCollection& collection) const {
    const i64 footprint = collection.GetMetadataMemoryUsage();
    Y_ENSURE(footprint >= 0, "Negative metadata footprint");
    const auto* raw = collection.GetMetadataBuffer();
    ui64 sharedBytes = raw ? raw->GetAllocationSize() : 0;
    if (raw) {
        AccountBuffer(*raw);
    }
    if (const auto meta = collection.GetSharedMeta()) {
        sharedBytes = meta->GetMemoryUsage();
        const ui64 rawBytes = meta->Raw.GetAllocationSize();
        Y_ENSURE(sharedBytes >= rawBytes, "Metadata footprint omits its backing allocation");
        AccountBuffer(meta->Raw);
        AccountObject(meta->MetadataAccounts_, sharedBytes - rawBytes);
    }
    Y_ENSURE(ui64(footprint) >= sharedBytes, "Descriptor footprint omits its shared metadata");
    AccountObject(collection.MetadataAccounts_, ui64(footprint) - sharedBytes);
}

void TMetadataAccounting::AccountObject(std::atomic<TMetadataRecord*>& records, ui64 bytes) const {
    auto* head = records.load(std::memory_order_acquire);
    THolder<TMetadataRecord> candidate;
    for (;;) {
        for (auto* record = head; record; record = record->Next) {
            if (record->Account == Account_) {
                return;
            }
        }
        if (!candidate) {
            candidate = MakeHolder<TMetadataRecord>(Account_);
        }
        candidate->Next = head;
        if (records.compare_exchange_weak(
                head, candidate.Get(), std::memory_order_acq_rel, std::memory_order_acquire)) {
            candidate.Release()->Charge(bytes);
            return;
        }
    }
}

TRequest::~TRequest() {
    ReleaseCollection();
}

void TRequest::ReleaseCollection() {
    if (CollectionOwner) {
        CollectionOwner->RemoveRequest(*this);
    }
}

void TRequest::RetireQueuedWalk() {
    Y_ENSURE(WalkCollectionId);
    ReleaseCollection();
    PageCollection.Reset();
    MarkResponded();
}

TCollection::~TCollection() {
    while (!Requests.Empty()) {
        Requests.Front()->ReleaseCollection();
    }
}

void TCollection::SetPageCollection(TIntrusiveConstPtr<NPageCollection::IPageCollection> collection) {
    Y_ENSURE(CollectionSet);
    Y_ENSURE(!collection || collection->Label() == Id);
    if (collection) {
        CollectionSet->Accounting.AccountMetadata(*collection);
    }
    PageCollection = std::move(collection);
}

void TCollection::AddRequest(TRequest& request) {
    Y_ENSURE(!request.CollectionOwner && request.PageCollection);
    request.CollectionOwner = this;
    Requests.PushBack(&request);
}

void TCollection::RemoveRequest(TRequest& request) {
    Y_ENSURE(request.CollectionOwner == this);
    request.CollectionOwner = nullptr;
    request.TIntrusiveListItem<TRequest, TCollectionRequestTag>::Unlink();
}

} // namespace NKikimr::NSharedCache

namespace NKikimr::NPageCollection {

IPageCollection::~IPageCollection() {
    NSharedCache::DestroyMetadataRecords(MetadataAccounts_.load(std::memory_order_relaxed));
}

TMeta::~TMeta() {
    NSharedCache::DestroyMetadataRecords(MetadataAccounts_.load(std::memory_order_relaxed));
}

} // namespace NKikimr::NPageCollection

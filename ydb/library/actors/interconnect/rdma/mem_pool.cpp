#include "mem_pool.h"
#include "link_manager.h"

#ifndef MEM_POOL_DISABLE_RDMA_SUPPORT
#include "ctx.h"
#else
extern "C" {

struct ibv_mr {
    struct ibv_context     *context;
    struct ibv_pd	       *pd;
    void		       *addr;
    size_t			length;
    ui32		handle;
    ui32		lkey;
    ui32		rkey;
};

}

#endif

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/thread/lfstack.h>
#include <util/stream/output.h>
#include <util/system/align.h>

#include <bit>
#include <vector>
#include <list>

#include <mutex>
#include <thread>

#include <set>

#include <cstdlib>
#include <cstring>
#include <cerrno>

#if defined(_win_)
#include <malloc.h> // _aligned_malloc, _aligned_free
#else
#include <sys/mman.h>   // madvise
#include <unistd.h>
#include <sys/syscall.h>
#endif

static constexpr size_t HPageSz = (1 << 21);

using ::NMonitoring::TDynamicCounters;

struct TMemRegCompare {
    using is_transparent = void;
    bool operator ()(const TIntrusivePtr<NInterconnect::NRdma::TMemRegion>& a, const TIntrusivePtr<NInterconnect::NRdma::TMemRegion>& b) const noexcept {
        return a->Chunk.Get() < b->Chunk.Get();
    }
    bool operator ()(const NInterconnect::NRdma::TChunkPtr& a, const TIntrusivePtr<NInterconnect::NRdma::TMemRegion>& b) const noexcept {
        return a.Get() < b->Chunk.Get();
    }
    bool operator ()(const TIntrusivePtr<NInterconnect::NRdma::TMemRegion>& a, const NInterconnect::NRdma::TChunkPtr& b) const noexcept {
        return a->Chunk.Get() < b.Get();
    }
};

namespace NInterconnect::NRdma {

    // Cross-platform memory management
    static void* allocateMemory(size_t size, size_t alignment, bool hp);
    static void freeMemory(void* ptr) noexcept;

    class TChunk: public NNonCopyable::TMoveOnly, public TAtomicRefCount<TChunk>, public TIntrusiveListItem<TChunk> {
        friend class TMemPoolBase;
    public:

    TChunk(std::vector<ibv_mr*>&& mrs, IMemPool* pool) noexcept
        : MRs(std::move(mrs))
        , MemPool(pool)
    {
    }

    ~TChunk() {
        MemPool->DealocateMr(this);
    }

    ibv_mr* GetMr(size_t deviceIndex) noexcept {
        if (Y_UNLIKELY(deviceIndex >= MRs.size())) {
            return nullptr;
        }
        return MRs[deviceIndex];
    }

    void Free(TMemRegion&& mr) noexcept {
        MemPool->Free(std::move(mr), *this);
    }

    bool Empty() const noexcept {
        return MRs.empty();
    }

    TMemRegionPtr AllocMr(int size, ui32 flags) noexcept {
        return MemPool->Alloc(size, flags);
    }

    bool TryReclaim() noexcept {
        return ReclaimSemaphore.TryReclaim();
    }

    void DoRelease() noexcept {
        ReclaimSemaphore.Release();
    }

    bool TryAcquire(ui64 expGeneration) noexcept {
        return ReclaimSemaphore.TryAcquire(expGeneration);
    }

    ui64 GetGeneration() const noexcept {
        return ReclaimSemaphore.GetGeneration();
    }

    size_t Size() const noexcept {
        Y_ABORT_UNLESS(!MRs.empty());
        return MRs[0]->length;
    }

    private:
        std::vector<ibv_mr*> MRs;
        IMemPool* MemPool;
        class TChunkUseLock {
            // Prevent reclaming just after new chunk allocation
            std::atomic<ui64> Lock = ReclaimingBitMask;
            static constexpr ui64 ReclaimingBitMask = 1ul << 63;
            static constexpr size_t GenerationBits = 46u;
            static constexpr size_t UseCountBits = 17u;
            static constexpr ui64 UseCountMask = (1ul << UseCountBits) - 1u;
        public:
            void Release() noexcept {
                //TODO: remove this verify it is a bit expensive
                Y_ABORT_UNLESS(static_cast<i64>(Lock.load() & UseCountMask) > 0);
                //Just decrease use count, no need to be synchronized with other threads
                Lock.fetch_sub(1, std::memory_order_relaxed);
            }

            bool TryAcquire(ui64 expectedGeneration) noexcept {
                ui64 x = Lock.fetch_add(1, std::memory_order_acq_rel);
                Y_ABORT_UNLESS((x & UseCountMask) < UseCountMask);
                if (Y_LIKELY(((x & (~ReclaimingBitMask)) >> UseCountBits) == expectedGeneration)) {
                    // Check we need to unset reclaiming bit
                    if (Y_UNLIKELY(x & ReclaimingBitMask)) {
                        // In most cases (x & UseCountMask) == 0 here but there is a chanse that parallel TryAcquire
                        // will increase UseCount in moment (the time between Lock.fetch_add and Lock.fetch_sub) but
                        // there is only one thread which call TryAcquire with aim to unset Reclaiming bit.
                        // All other threads have not seen the updated generation yiet so they can't be in this part of code.
                        // That all mean we can set UseCount 1 and unset Reclaiming bit
                        const ui64 newX = (x & (~(ReclaimingBitMask | UseCountMask))) | 1ul; //generation without reclaiming bit and witn
                        x = (x & (~UseCountMask)) | 1ul; // value already incremented in the memory - expect single use count
                        while (!Lock.compare_exchange_strong(x, newX,
                            std::memory_order_release, std::memory_order_acquire)) {
                                // The only way to fail compare exchange here is parallel TryAcuire - see comment above
                                // So verify everething is as expected and try again
                                Y_ABORT_UNLESS(x & ReclaimingBitMask); //still reclaiming
                                Y_ABORT_UNLESS(((x & (~ReclaimingBitMask)) >> UseCountBits) == expectedGeneration); //still expected generation
                                Y_ABORT_UNLESS((x & UseCountMask) > 1ul); //it is the reson of CAS fail

                                x = (x & (~UseCountMask)) | 1ul; // It is what we expect in the memory
                            }
                    }
                    return true;
                } else {
                    Lock.fetch_sub(1, std::memory_order_release);
                    return false;
                }
            }

            bool TryReclaim() noexcept {
                ui64 oldX = Lock.load(std::memory_order_relaxed);
                ui64 newX;
                do {
                    // Check the chunk is currently not in progress of reclaim
                    if (oldX & ReclaimingBitMask) {
                        return false;
                    }
                    // Check use count
                    if ((oldX & UseCountMask) != 0) {
                        return false;
                    }
                    // Bump generation. We have 46 bits for generation.
                    // 1 << 46 is 70368744177664, which mean even if we have 1000000 reclaimations per second
                    // we need ((1 << 46) / 1000000 ) / 3600 / 24 / 365 = 2.23 yaar of continuous reclaimations
                    // to rollower this counter.
                    // Also set Reclaimint bit to prevent possible multiple reclaim of same chunk (the time
                    // between Reclaim and first Acquire)
                    newX = (((oldX >> UseCountBits) + 1ul) << UseCountBits) | ReclaimingBitMask;
                } while (!Lock.compare_exchange_weak(oldX, newX,
                    std::memory_order_release, std::memory_order_relaxed));

                return true;
            }

            ui64 GetGeneration() const noexcept {
                return (Lock.load(std::memory_order_relaxed) & ~ReclaimingBitMask) >> UseCountBits;
            }
        } ReclaimSemaphore;
    };

    TMemRegion::TMemRegion(TChunkPtr chunk, uint32_t offset, uint32_t size) noexcept
        : Chunk(std::move(chunk))
        , Offset(offset)
        , Size(size)
        , OrigSize(size)
    {
        Y_ABORT_UNLESS(Chunk);
        Y_ABORT_UNLESS(!Chunk->Empty(), "Chunk is empty");
    }

    TMemRegion::~TMemRegion() {
        if (Chunk)
            Chunk->Free(std::move(*this));
    }

    void* TMemRegion::GetAddr() const {
        auto* mr = Chunk->GetMr(0);
        if (Y_UNLIKELY(!mr)) {
            return nullptr;
        }
        return static_cast<char*>(mr->addr) + Offset;
    }
    uint32_t TMemRegion::GetSize() const {
        return Size;
    }

    uint32_t TMemRegion::GetLKey(size_t deviceIndex) const {
        auto* mr = Chunk->GetMr(deviceIndex);
        if (Y_UNLIKELY(!mr)) {
            return 0;
        }
        return mr->lkey;
    }
    uint32_t TMemRegion::GetRKey(size_t deviceIndex) const {
        auto* mr = Chunk->GetMr(deviceIndex);
        if (Y_UNLIKELY(!mr)) {
            return 0;
        }
        return mr->rkey;
    }

    void TMemRegion::Resize(uint32_t newSize) noexcept {
        Y_ABORT_UNLESS(newSize <= OrigSize, "Cannot resize to larger size");
        Size = newSize;
    }

    TContiguousSpan TMemRegion::GetData() const {
        return TContiguousSpan(static_cast<const char*>(GetAddr()), GetSize());
    }

    TMutableContiguousSpan TMemRegion::UnsafeGetDataMut() {
        return TMutableContiguousSpan(static_cast<char*>(GetAddr()), GetSize());
    }

    size_t TMemRegion::GetOccupiedMemorySize() const {
        return GetSize();
    }

    IContiguousChunk::EInnerType TMemRegion::GetInnerType() const noexcept {
        return EInnerType::RDMA_MEM_REG;
    }

    IContiguousChunk::TPtr TMemRegion::Clone() noexcept {
        static const ui64 pageAlign = NSystemInfo::GetPageSize() - 1;
        const IMemPool::Flags flag = (((ui64)GetAddr() & pageAlign) == 0) ? IMemPool::PAGE_ALIGNED : IMemPool::EMPTY;
        TMemRegionPtr newRegion = Chunk->AllocMr(GetSize(), flag);
        auto span = newRegion->UnsafeGetDataMut();
        ::memcpy(span.GetData(), GetAddr(), GetSize());
        return newRegion;
    }

    TMemRegionSlice::TMemRegionSlice(TIntrusivePtr<TMemRegion> memRegion, uint32_t offset, uint32_t size) noexcept
        : MemRegion(std::move(memRegion))
        , Offset(offset)
        , Size(size)
    {
        Y_ABORT_UNLESS(MemRegion);
        Y_ABORT_UNLESS(Offset + Size <= MemRegion->GetSize(), "Invalid slice size or offset");
    }

    void* TMemRegionSlice::GetAddr() const {
        return static_cast<char*>(MemRegion->GetAddr()) + Offset;
    }
    uint32_t TMemRegionSlice::GetSize() const {
        return Size;
    }

    uint32_t TMemRegionSlice::GetLKey(size_t deviceIndex) const {
        return MemRegion->GetLKey(deviceIndex);
    }
    uint32_t TMemRegionSlice::GetRKey(size_t deviceIndex) const {
        return MemRegion->GetRKey(deviceIndex);
    }

    TMemRegionSlice TryExtractFromRcBuf(const TRcBuf& rcBuf) noexcept {
        std::optional<IContiguousChunk::TPtr> underlying = rcBuf.ExtractFullUnderlyingContainer<IContiguousChunk::TPtr>();
        if (!underlying || !*underlying || underlying->Get()->GetInnerType() != IContiguousChunk::EInnerType::RDMA_MEM_REG) {
            return {};
        }
        auto memReg = dynamic_cast<NInterconnect::NRdma::TMemRegion*>(underlying->Get());
        if (!memReg) {
            return {};
        }
        return TMemRegionSlice(
            TIntrusivePtr<TMemRegion>(memReg),
            rcBuf.GetData() - memReg->GetData().data(),
            rcBuf.GetSize()
        );
    }

    static void* allocateMemory(size_t size, size_t alignment, bool hp) {
        if (size % alignment != 0) {
            return nullptr;
        }

        void* buf = nullptr;

#if defined(_win_)
        // Windows: use _aligned_malloc
        buf = _aligned_malloc(size, alignment);
        if (!buf) {
            fprintf(stderr, "Failed to allocate aligned memory on Windows\n");
            return nullptr;
        }
#else
        // POSIX/C++: std::aligned_alloc (C++17)
        buf = std::aligned_alloc(alignment, size);
        if (!buf) {
            fprintf(stderr, "Failed to allocate aligned memory on Unix\n");
            return nullptr;
        }
#endif

        if (hp) {
#if defined(_linux_)
            if (madvise(buf, size, MADV_HUGEPAGE) < 0) {
                fprintf(stderr, "Unable to madvice to use THP, %d (%d)",
                    strerror(errno), errno);
            }
#endif
            for (size_t i = 0; i < size; i += HPageSz) {
                // We use THP right now. We need to touch each page to promote it to HUGE.
                static_cast<char*>(buf)[i] = 0;
            }
        }
        return buf;
    }

    static void freeMemory(void* ptr) noexcept {
        if (!ptr) {
            return;
        }
#if defined(_win_)
        _aligned_free(ptr);
#else
        std::free(ptr);
#endif
    }

    std::vector<ibv_mr*> registerMemory(void* addr, size_t size, const NInterconnect::NRdma::NLinkMgr::TCtxsMap& ctxs) {
        std::vector<ibv_mr*> res;

// In case of windows windows we can't use ibv_dereg_mr - compile error
// In case of linux but absent librray we can't register memory, but we need mem pool for tests - emulate registration
#ifndef MEM_POOL_DISABLE_RDMA_SUPPORT
        if (!ctxs.empty()) {
            res.reserve(ctxs.size());
            for (const auto& [_, ctx]: ctxs) {
                ibv_mr* mr = ibv_reg_mr(
                    ctx->GetProtDomain(), addr, size,
                    IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE
                );
                if (!mr) {
                    for (ibv_mr* tmp : res) {
                        ibv_dereg_mr(tmp);
                    }
                    return {};
                }
                res.push_back(mr);
            }
        } else
#endif
        {
#ifdef MEM_POOL_DISABLE_RDMA_SUPPORT
        Y_UNUSED(ctxs);
#endif
            struct ibv_mr* dummy = (struct ibv_mr*)calloc(1, sizeof(struct ibv_mr));
            dummy->addr = addr;
            dummy->length = size;
            res.push_back(dummy);
        }
        return res;
    }

    TMemRegionPtr IMemPool::Alloc(int size, ui32 flags) noexcept {
        TMemRegionPtr region = AllocImpl(size, flags);
        if (!region) {
            return nullptr;
        }
        return region;
    }

    std::optional<TRcBuf> IMemPool::AllocRcBuf(int size, ui32 flags) noexcept {
        TIntrusivePtr<TMemRegion>  region = AllocImpl(size, flags);
        if (!region) {
            return {};
        }
        return TRcBuf(region);
    }

    static NMonitoring::TDynamicCounterPtr MakeCounters(TDynamicCounters* counters) {
        if (!counters) {
                static NMonitoring::TDynamicCounterPtr dummy(new TDynamicCounters());
                return dummy;
        }
        return counters;
    }

    static const NInterconnect::NRdma::NLinkMgr::TCtxsMap& GetAllCtxs() {
        NInterconnect::NRdma::NLinkMgr::Init();
        return NInterconnect::NRdma::NLinkMgr::GetAllCtxs();
    }

    class TMemPoolBase: public IMemPool {
    public:
        TMemPoolBase(size_t maxChunk, NMonitoring::TDynamicCounterPtr counter)
            : Ctxs(GetAllCtxs())
            , MaxChunk(maxChunk)
            , Alignment(NSystemInfo::GetPageSize())
        {
            AllocatedCounter = counter->GetCounter("RdmaPoolAllocatedUserBytes", false);
            AllocatedChunksCounter = counter->GetCounter("RdmaPoolAllocatedChunks", false);
            MaxAllocated.Counter = counter->GetCounter("RdmaPoolAllocatedUserMaxBytes", false);
            ReclaimationRunCounter = counter->GetCounter("RdmaPoolReclaimationRunCounter", true);
            ReclaimationFailCounter = counter->GetCounter("RdmaPoolReclaimationFailCounter", true);

            Y_ABORT_UNLESS((Alignment & Alignment - 1) == 0, "Alignment must be a power of two %zu", Alignment);
        }

        size_t GetAllocatedChunksCount() const noexcept {
            return AllocatedChunks.load(std::memory_order_relaxed);
        }

        size_t GetMaxChunks() const noexcept {
            return MaxChunk;
        }

    protected:
        TChunkPtr AllocNewChunk(size_t size, bool hp) noexcept {
            const std::lock_guard<std::mutex> lock(Mutex);
            Y_ABORT_UNLESS(AllocatedChunks <= MaxChunk);

            size = AlignUp(size, Alignment);

            if (Y_UNLIKELY(AllocatedChunks == MaxChunk)) {
                //return nullptr;
                return TryReclaim(size);
            }

            void* ptr = allocateMemory(size, Alignment, hp);
            if (!ptr) {
                return nullptr;
            }

            auto mrs = registerMemory(ptr, size, Ctxs);
            if (mrs.empty()) {
                freeMemory(ptr);
                return nullptr;
            }

            AllocatedChunksCounter->Inc();
            AllocatedChunks++;

            auto chunk = MakeIntrusive<TChunk>(std::move(mrs), this);
            Chunks.PushBack(chunk.Get());

            if (ReclaimIt.Item() == nullptr) {
                ReclaimIt = Chunks.Begin();
            }

            return chunk;
        }

        void DealocateMr(TChunk* chunk) noexcept override {
            std::vector<ibv_mr*>& mrs = chunk->MRs;
            {
                const std::lock_guard<std::mutex> lock(Mutex);
                chunk->Unlink();
                AllocatedChunks--;
                AllocatedChunksCounter->Dec();
            }
            if (mrs.empty()) {
                return;
            }
            auto addr = mrs.front()->addr;
#ifndef MEM_POOL_DISABLE_RDMA_SUPPORT
            if (!Ctxs.empty()) {
                for (auto& m: mrs) {
                    ibv_dereg_mr(m);
                }
            } else
#endif
            {
                free(mrs.front());
            }
            freeMemory(addr);
            mrs.clear();
        }

        void TrackPeakAlloc(i64 newVal) noexcept {
            i64 curVal = MaxAllocated.Val.load(std::memory_order_relaxed);
            while (newVal > curVal) {
                if (MaxAllocated.Val.compare_exchange_weak(curVal, newVal,
                    std::memory_order_release, std::memory_order_relaxed)) {
                    break;
                }
            }
        }
    private:
        TChunkPtr TryReclaim(size_t size) noexcept {
            ReclaimationRunCounter->Inc();

            auto it = ReclaimIt;

            for (; it!= Chunks.End(); it++) {
                if (it->Size() == size && it->TryReclaim()) {
                    TChunkPtr ptr = &(*it);
                    ReclaimIt = ++it;
                    return ptr;
                }
            }

            it = Chunks.Begin();
            for (; it != ReclaimIt; it++) {
                if (it->Size() == size && it->TryReclaim()) {
                    TChunkPtr ptr = &(*it);
                    ReclaimIt = ++it;
                    return ptr;
                }
            }

            ReclaimationFailCounter->Inc();
            return nullptr;
        }

    protected:

        const NInterconnect::NRdma::NLinkMgr::TCtxsMap Ctxs;
        const size_t MaxChunk;
        const size_t Alignment;
        ::NMonitoring::TDynamicCounters::TCounterPtr AllocatedCounter;
        ::NMonitoring::TDynamicCounters::TCounterPtr AllocatedChunksCounter;
        ::NMonitoring::TDynamicCounters::TCounterPtr ReclaimationRunCounter;
        ::NMonitoring::TDynamicCounters::TCounterPtr ReclaimationFailCounter;
        struct {
           NMonotonic::TMonotonic Time;
           std::atomic<i64> Val = 0;
           ::NMonitoring::TDynamicCounters::TCounterPtr Counter;
        } MaxAllocated;
        std::atomic<size_t> AllocatedChunks = 0;
    private:
        std::mutex Mutex;
        TIntrusiveList<TChunk> Chunks;
        TIntrusiveList<TChunk>::TIterator ReclaimIt;

        void Tick(NMonotonic::TMonotonic time) noexcept override {
            constexpr TDuration holdTime = TDuration::Seconds(15);

            if (time - MaxAllocated.Time > holdTime) {
                MaxAllocated.Counter->Set(MaxAllocated.Val.load());
                MaxAllocated.Val.store(AllocatedCounter->Val());
                MaxAllocated.Time = time;
            }
        }
    };

    class TDummyMemPool: public TMemPoolBase {
    public:
        TDummyMemPool()
            : TMemPoolBase(-1, MakeCounters(nullptr))
        {}

        TMemRegionPtr AllocImpl(int size, ui32) noexcept override {
            auto chunk = AllocNewChunk(size, false);
            if (!chunk) {
                return nullptr;
            }
            return new TMemRegion(chunk , 0, size);
        }

        void Free(TMemRegion&&, TChunk&) noexcept override {}

        int GetMaxAllocSz() const noexcept override {
            return 2048 << 20;
        }

        TString GetName() const noexcept override {
            return "DummyMemPool";
        }
    };

    constexpr ui32 GetPowerOfTwo(ui32 size) noexcept {
        return std::bit_width(std::bit_ceil(size)) - 1;
    }
    static_assert((1 << GetPowerOfTwo(512)) == 512, "GetPowerOfTwo(512) must return 9");

    constexpr ui32 GetNumChains(ui32 minAllocSz, ui32 maxAllocSz) noexcept {
        return GetPowerOfTwo(maxAllocSz) - GetPowerOfTwo(minAllocSz) + 1;
    }

    class TSlotMemPool: public TMemPoolBase {
        struct TChain {
            using TMemRegcontainer = std::multiset<TIntrusivePtr<TMemRegion>, TMemRegCompare>; 
            TChain() = default;
            void Init(ui32 slotSize) {
                SlotSize = slotSize;
                SlotsInBatch = GetSlotsInBatch(slotSize);
            }
            TMemRegionPtr TryGetSlot() noexcept {
                if (Slots.empty()) {
                    return nullptr;
                }
                auto it = Slots.begin();
                TIntrusivePtr<TMemRegion> slot = *it;
                const TChunk* const curChunkPtr = slot->Chunk.Get();
                const ui64 curGeneration = slot->Generation;
                if (Y_LIKELY(slot->Chunk->TryAcquire(curGeneration))) {
                    Slots.erase(it);
                    return slot;
                } else {
                    // Search for slots with same chunk and same (important!) generation to remove it from local cache
                    // The generation check is mandatory here because the cache can contain slots with same parent chunk but with different generation
                    // we need to delete only slots with generation we check in TryAcquire
                    while ((it != Slots.end()) && ((*it)->Chunk.Get() == curChunkPtr) && ((*it)->Generation == curGeneration)) {
                        (*it)->Chunk.Reset();
                        Slots.erase(it++);
                    }
                    return TryGetSlot();
                }
            }
            void PutSlot(TIntrusivePtr<TMemRegion> slot) noexcept {
                Slots.insert(slot);
            }
            TMemRegcontainer::iterator PutSlot(TIntrusivePtr<TMemRegion> slot, TMemRegcontainer::iterator hint) noexcept {
                return Slots.insert(hint, slot);
            }
            TMemRegcontainer::iterator FindHint(const TChunkPtr& p) noexcept {
                return Slots.find(p);
            }
            void PutSlotsBatch(std::list<TIntrusivePtr<TMemRegion>>&& slots) noexcept {
                for (auto& x : slots) {
                    PutSlot(x);
                }
            }
            std::list<TIntrusivePtr<TMemRegion>> GetSlotsBatch(ui32 batchSize) {
                Y_ABORT_UNLESS(Slots.size() >= batchSize, "Not enough slots in chain");
                std::list<TIntrusivePtr<TMemRegion>> res;
                auto it = Slots.begin();
                for (size_t i = 0; i < batchSize; i++) {
                    res.insert(res.end(), *it);
                    it = Slots.erase(it);
                }
                return res;
            }
            //TODO: It shoult be possible to improve it using more optimised container and may be using raw pointer to store TMemRegion
            // Requirements:
            // * We need to group all regions with same Chunk
            // * We need to be able to delete only slots with given chunk + generation
            // * We need some way to prevent cal "Free" during deletion
            //
            // multiset here is a bit naive approach but it works
            // unordered_multiset works much worse due to div op inside hash calculation.
            //
            // Some ideas to think about:
            //  - The size of chunk relative large (32Mb+) - so 25 low bits in addres are same
            //  - The user address space less than 64 bits
            //  - Probably we can reserve address on startup to get linear space
            TMemRegcontainer Slots;

            ui32 SlotSize;
            ui32 SlotsInBatch;
        };

        struct TLockFreeChain {
            TLockFreeChain() = default;
            void Init(ui32 slotSize) noexcept {
                SlotSize = slotSize;
                SlotsInBatch = GetSlotsInBatch(slotSize);
            }
            std::optional<std::list<TIntrusivePtr<TMemRegion>>> GetSlotsBatch(bool allowIncompleted) {
                std::list<TIntrusivePtr<TMemRegion>> res;
                if (FullBatchesSlots.Dequeue(&res)) {
                    return res;
                }
                if (allowIncompleted) {
                    {
                        std::lock_guard<std::mutex> lock(IncompleteBatchMutex);
                        res.swap(IncompleteBatch);
                    }
                    if (res.size()) {
                        return res;
                    }
                }
                return std::nullopt;
            }
            void PutSlotsBatches(std::list<TIntrusivePtr<TMemRegion>>&& slots) {
                Y_DEBUG_ABORT_UNLESS(slots.size() == SlotsInBatch, "Invalid slots size: %zu, expected: %u", slots.size(), SlotsInBatch);
                FullBatchesSlots.Enqueue(std::move(slots));
            }
            void PutSlotsBatches(std::list<std::list<TIntrusivePtr<TMemRegion>>>&& slots) {
                for (auto& batch : slots) {
                    Y_DEBUG_ABORT_UNLESS(batch.size() == SlotsInBatch, "Invalid slots size: %zu, expected: %u", batch.size(), SlotsInBatch);
                    FullBatchesSlots.Enqueue(std::move(batch));
                }
            }
            void PutSlotUnderLock(TIntrusivePtr<TMemRegion> slot) noexcept {
                std::lock_guard<std::mutex> lock(IncompleteBatchMutex);
                IncompleteBatch.push_back(std::move(slot));
                if (IncompleteBatch.size() >= SlotsInBatch) {
                    FullBatchesSlots.Enqueue(std::move(IncompleteBatch));
                    IncompleteBatch.clear();
                }
            }

            TLockFreeStack<std::list<TIntrusivePtr<TMemRegion>>> FullBatchesSlots;
            ui32 SlotSize;
            ui32 SlotsInBatch;

            std::mutex IncompleteBatchMutex;
            std::list<TIntrusivePtr<TMemRegion>> IncompleteBatch;
        };

        static constexpr ui32 MinAllocSz = 512;
        static constexpr ui32 MaxAllocSz = 32 * 1024 * 1024;
        static_assert((MinAllocSz & (MinAllocSz - 1)) == 0, "MinAllocSz must be a power of 2");
        static_assert((MaxAllocSz & (MaxAllocSz - 1)) == 0, "MaxAllocSz must be a power of 2");
        static constexpr ui32 ChainsNum = GetNumChains(MinAllocSz, MaxAllocSz);
        static constexpr ui64 BatchSizeBytes = 32 * 1024 * 1024; // 32 MB

        static constexpr ui32 GetChainIndex(ui32 size) noexcept {
            return GetPowerOfTwo(std::max(size, MinAllocSz)) - GetPowerOfTwo(MinAllocSz);
        }
        static ui32 GetSlotsInBatch(ui32 size) noexcept {
            Y_ABORT_UNLESS(BatchSizeBytes % size == 0, "BatchSizeBytes must be divisible by size");
            return BatchSizeBytes / size;
        }

        struct TSlotMemPoolCache {
            TSlotMemPoolCache() {
                for (ui32 i = GetPowerOfTwo(MinAllocSz); i <= GetPowerOfTwo(MaxAllocSz); ++i) {
                    Chains[GetChainIndex(1 << i)].Init(1 << i);
                }
            }
            ~TSlotMemPoolCache() {
                Stopped = true;
            }

            bool AllocAndSplitNewChunk(TChain& chain, TSlotMemPool& pool) {
                TChunkPtr chunk = pool.AllocNewChunk(BatchSizeBytes, true);
                if (!chunk) {
                    return false;
                }
                TChain::TMemRegcontainer::iterator it = chain.FindHint(chunk);
                const ui64 generation = chunk->GetGeneration();
                for (ui32 i = 0; i < chain.SlotsInBatch; ++i) {
                    auto x = MakeIntrusive<TMemRegion>(chunk, i * chain.SlotSize, chain.SlotSize);
                    //TODO: Use Y_DEBUG_ABORT_UNLESS instead
                    Y_ABORT_UNLESS(generation == chunk->GetGeneration());
                    x->Generation = generation;
                    it = chain.PutSlot(x, it);
                }
                return true;
            }

            TMemRegionPtr AllocImpl(int size, ui32 flags, TSlotMemPool& pool) noexcept {
                if (flags & IMemPool::PAGE_ALIGNED && static_cast<size_t>(size) < pool.Alignment) {
                    size = pool.Alignment;
                }

                if (Y_UNLIKELY(size > static_cast<int>(MaxAllocSz))) {
                    return nullptr;
                }
                ui32 chainIndex = GetChainIndex(size);
                Y_ABORT_UNLESS(chainIndex < ChainsNum, "Invalid chain index: %u", chainIndex);
                // Try to get slot from local cache
                auto& localChain = Chains[chainIndex];
                TMemRegionPtr slot = localChain.TryGetSlot();
                if (slot) {
                    return slot;
                }

                const bool allChunksAllocated = pool.GetAllocatedChunksCount() == pool.GetMaxChunks();

                // If no slot in local cache, try to get from global pool
                for (;;) {
                    auto batch = pool.Chains[chainIndex].GetSlotsBatch(allChunksAllocated);
                    if (batch) {
                        localChain.PutSlotsBatch(std::move(*batch));
                        if (auto memRegion = localChain.TryGetSlot()) {
                            return memRegion;
                        }
                    } else {
                        break;
                    }
                }
                // If no slots in global pool, allocate new chunk
                if (!AllocAndSplitNewChunk(localChain, pool)) {
                    return nullptr;
                }

                return localChain.TryGetSlot();
            }

            void Free(TMemRegion&& mr, TSlotMemPool& pool) noexcept {
                ui32 chainIndex = GetChainIndex(mr.GetSize());
                if (Y_UNLIKELY(Stopped)) {
                    // current thread is stopped, return mr to global pool
                    pool.Chains[chainIndex].PutSlotUnderLock(MakeIntrusive<TMemRegion>(std::move(mr)));
                    return;
                }

                mr.Chunk->DoRelease();

                auto& chain = Chains[chainIndex];
                Y_ABORT_UNLESS(chainIndex < ChainsNum, "Invalid chain index: %u", chainIndex);
                chain.PutSlot(MakeIntrusive<TMemRegion>(std::move(mr)));

                if (chain.Slots.size() >= 2 * chain.SlotsInBatch) { // TODO: replace constant 2
                    // If we have too much slots in local cache, put them back to global pool
                    auto batch = chain.GetSlotsBatch(chain.SlotsInBatch);
                    pool.Chains[chainIndex].PutSlotsBatches(std::move(batch));
                }
            }

            std::array<TChain, ChainsNum> Chains;
            bool Stopped = false;
        };
        friend struct TSlotMemPoolCache;

        static size_t CalcChunksLimit(const std::optional<TMemPoolSettings>& settings) noexcept {
            if (!settings.has_value())
                return 128; //default

            const ui64 sizeLimit = settings->SizeLimitMb * (1ull << 20);

            if (sizeLimit < BatchSizeBytes)
                return 1; //Do not allow zerro limit

            return sizeLimit / BatchSizeBytes;
        }
    public:
        TSlotMemPool(NMonitoring::TDynamicCounterPtr counter, const std::optional<TMemPoolSettings>& settings)
            : TMemPoolBase(CalcChunksLimit(settings), counter)
        {
            for (ui32 i = GetPowerOfTwo(MinAllocSz); i <= GetPowerOfTwo(MaxAllocSz); ++i) {
                Chains[GetChainIndex(1 << i)].Init(1 << i);
            }
        }

        ~TSlotMemPool() {
            for (size_t i = 0; i < Chains.size(); i++) {
                TLockFreeChain& chain = Chains[i];
                for (auto& x : chain.IncompleteBatch) {
                    x->Chunk.Reset();
                }

                std::list<TIntrusivePtr<TMemRegion>> tmp;
                while (chain.FullBatchesSlots.Dequeue(&tmp)) {
                    for (auto& x : tmp) {
                        x->Chunk.Reset();
                    }
                    tmp.clear();
                }
            }
        }

        int GetMaxAllocSz() const noexcept override {
            return MaxAllocSz;
        }

        TString GetName() const noexcept override {
            return "SlotMemPool";
        }

    protected:
        TMemRegionPtr AllocImpl(int size, ui32 flags) noexcept override {
            if (auto memReg = LocalCache.AllocImpl(size, flags, *this)) {
                memReg->Resize(size);
                i64 newVal = AllocatedCounter->Add(size);

                TrackPeakAlloc(newVal);

                return memReg;
            }
            return nullptr;
        }
        void Free(TMemRegion&& mr, TChunk&) noexcept override {
            AllocatedCounter->Sub(mr.GetSize());
            LocalCache.Free(std::move(mr), *this);
        }

    private:
        std::array<TLockFreeChain, ChainsNum> Chains;

    private:
        static thread_local TSlotMemPoolCache LocalCache;
    };

    thread_local TSlotMemPool::TSlotMemPoolCache TSlotMemPool::LocalCache;

    std::shared_ptr<IMemPool> CreateDummyMemPool() noexcept {
        auto* pool = Singleton<TDummyMemPool>();
        return std::shared_ptr<TDummyMemPool>(pool, [](TDummyMemPool*) {});
    }

    std::shared_ptr<IMemPool> CreateSlotMemPool(TDynamicCounters* counters, std::optional<TMemPoolSettings> settings) noexcept {
        static TSlotMemPool pool(MakeCounters(counters), settings);
        return std::shared_ptr<TSlotMemPool>(&pool, [](TSlotMemPool*) {});
    }

    // Just for UT
    std::shared_ptr<IMemPool> CreateNonStaticSlotMemPool(TDynamicCounters* counters, std::optional<TMemPoolSettings> settings) noexcept {
        return std::make_shared<TSlotMemPool>(MakeCounters(counters), settings);
    }
}

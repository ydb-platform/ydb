#include "mem_pool.h"
#include "link_manager.h"
#include "ctx.h"

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/thread/lfstack.h>
#include <util/stream/output.h>
#include <util/system/align.h>

#include <bit>
#include <vector>
#include <list>

#include <unistd.h>
#include <sys/syscall.h>
#include <mutex>
#include <thread>

#include <sys/mman.h>

static constexpr size_t HPageSz = (1 << 21);

using ::NMonitoring::TDynamicCounters;

namespace NInterconnect::NRdma {

    class TChunk: public NNonCopyable::TMoveOnly, public TAtomicRefCount<TChunk> {
    public:

    TChunk(std::vector<ibv_mr*>&& mrs, IMemPool* pool) noexcept
        : MRs(std::move(mrs))
        , MemPool(pool)
    {
    }

    ~TChunk() {
        MemPool->NotifyDealocated();
        if (Empty()) {
            return;
        }
        auto addr = MRs.front()->addr;
        for (auto& m: MRs) {
            ibv_dereg_mr(m);
        }
        std::free(addr);
        MRs.clear();
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

    private:
        std::vector<ibv_mr*> MRs;
        IMemPool* MemPool;
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
    TMutableContiguousSpan TMemRegion::GetDataMut() {
        return TMutableContiguousSpan(static_cast<char*>(GetAddr()), GetSize());
    }
    size_t TMemRegion::GetOccupiedMemorySize() const {
        return GetSize();
    }
    IContiguousChunk::EInnerType TMemRegion::GetInnerType() const noexcept {
        return EInnerType::RDMA_MEM_REG;
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

    void* allocateMemory(size_t size, size_t alignment, bool hp) {
        if (size % alignment != 0) {
            return nullptr;
        }
        void* buf = std::aligned_alloc(alignment, size);
        if (hp) {
            if (madvise(buf, size, MADV_HUGEPAGE) < 0) {
                fprintf(stderr, "Unable to madvice to use THP, %d (%d)",
                    strerror(errno), errno);
            }
            for (size_t i = 0; i < size; i += HPageSz) {
                // We use THP right now. We need to touch each page to promote it to HUGE.
                ((char*)buf)[i] = 0;
            }
        }
        return buf;
    }

    std::vector<ibv_mr*> registerMemory(void* addr, size_t size, const NInterconnect::NRdma::NLinkMgr::TCtxsMap& ctxs) {
        std::vector<ibv_mr*> res;
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
        return res;
    }

    TMemRegionPtr IMemPool::Alloc(int size, ui32 flags) noexcept {
        TMemRegion* region = AllocImpl(size, flags);
        if (!region) {
            return nullptr;
        }
        return TMemRegionPtr(region);
    }

    std::optional<TRcBuf> IMemPool::AllocRcBuf(int size, ui32 flags) noexcept {
        TMemRegion* region = AllocImpl(size, flags);
        if (!region) {
            return {};
        }
        return TRcBuf(IContiguousChunk::TPtr(region));
    }

    static NMonitoring::TDynamicCounterPtr MakeCounters(TDynamicCounters* counters) {
        if (!counters) {
                static NMonitoring::TDynamicCounterPtr dummy(new TDynamicCounters());
                return dummy;
        }
        return counters;
    }

    class TMemPoolBase: public IMemPool {
    public:
        TMemPoolBase(size_t maxChunk, NMonitoring::TDynamicCounterPtr counter)
            : Ctxs(NInterconnect::NRdma::NLinkMgr::GetAllCtxs())
            , MaxChunk(maxChunk)
            , Alignment(NSystemInfo::GetPageSize())
        {
            AllocatedCounter = counter->GetCounter("RdmaPoolAllocatedUserBytes", false);
            AllocatedChunksCounter = counter->GetCounter("RdmaPoolAllocatedChunks", false);

            Y_ABORT_UNLESS((Alignment & Alignment - 1) == 0, "Alignment must be a power of two %zu", Alignment);
        }
    protected:
        TChunkPtr AllocNewChunk(size_t size, bool hp) noexcept {
            const std::lock_guard<std::mutex> lock(Mutex);
            Y_ABORT_UNLESS(AllocatedChunks <= MaxChunk);

            if (AllocatedChunks == MaxChunk) {
                return nullptr;
            }

            size = AlignUp(size, Alignment);

            void* ptr = allocateMemory(size, Alignment, hp);
            if (!ptr) {
                return nullptr;
            }

            auto mrs = registerMemory(ptr, size, Ctxs);
            if (mrs.empty()) {
                std::free(ptr);
                return nullptr;
            }

            AllocatedChunksCounter->Inc();
            AllocatedChunks++;

            return MakeIntrusive<TChunk>(std::move(mrs), this);
        }

        void NotifyDealocated() noexcept override {
            const std::lock_guard<std::mutex> lock(Mutex);
            AllocatedChunks--;
            AllocatedChunksCounter->Dec();
        }

        const NInterconnect::NRdma::NLinkMgr::TCtxsMap Ctxs;
        const size_t MaxChunk;
        const size_t Alignment;
        ::NMonitoring::TDynamicCounters::TCounterPtr AllocatedCounter;
        ::NMonitoring::TDynamicCounters::TCounterPtr AllocatedChunksCounter;
        size_t AllocatedChunks = 0;
        std::mutex Mutex;
    };

    class TDummyMemPool: public TMemPoolBase {
    public:
        TDummyMemPool()
            : TMemPoolBase(-1, MakeCounters(nullptr))
        {}

        TMemRegion* AllocImpl(int size, ui32) noexcept override {
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
            TChain() = default;
            void Init(ui32 slotSize) {
                SlotSize = slotSize;
                SlotsInBatch = GetSlotsInBatch(slotSize);
            }
            TMemRegion* TryGetSlot() noexcept {
                if (Slots.empty()) {
                    return nullptr;
                }
                // TMemRegion* slot = Slots.front();
                TMemRegion* slot = Slots.front().release();
                Slots.pop_front();
                return slot;
            }
            void PutSlot(std::unique_ptr<TMemRegion>&& slot) noexcept {
                Slots.push_back(std::move(slot));
            }
            void PutSlotsBatch(std::list<std::unique_ptr<TMemRegion>>&& slots) noexcept {
                Slots.splice(Slots.end(), slots);
            }
            std::list<std::unique_ptr<TMemRegion>> GetSlotsBatch(ui32 batchSize) {
                Y_ABORT_UNLESS(Slots.size() >= batchSize, "Not enough slots in chain");
                std::list<std::unique_ptr<TMemRegion>> res;
                auto it = Slots.begin();
                std::advance(it, batchSize);
                res.splice(res.end(), Slots, Slots.begin(), it);
                return res;
            }
            std::list<std::unique_ptr<TMemRegion>> Slots;
            ui32 SlotSize;
            ui32 SlotsInBatch;
        };

        struct TLockFreeChain {
            TLockFreeChain() = default;
            void Init(ui32 slotSize) {
                SlotSize = slotSize;
                SlotsInBatch = GetSlotsInBatch(slotSize);
            }
            std::optional<std::list<std::unique_ptr<TMemRegion>>> GetSlotsBatch() {
                std::list<std::unique_ptr<TMemRegion>> res;
                if (FullBatchesSlots.Dequeue(&res)) {
                    return res;
                }
                return std::nullopt;
            }
            void PutSlotsBatches(std::list<std::unique_ptr<TMemRegion>>&& slots) {
                Y_DEBUG_ABORT_UNLESS(slots.size() == SlotsInBatch, "Invalid slots size: %zu, expected: %u", slots.size(), SlotsInBatch);
                FullBatchesSlots.Enqueue(std::move(slots));
            }
            void PutSlotsBatches(std::list<std::list<std::unique_ptr<TMemRegion>>>&& slots) {
                for (auto& batch : slots) {
                    Y_DEBUG_ABORT_UNLESS(batch.size() == SlotsInBatch, "Invalid slots size: %zu, expected: %u", batch.size(), SlotsInBatch);
                    FullBatchesSlots.Enqueue(std::move(batch));
                }
            }
            void PutSlot(std::unique_ptr<TMemRegion>&& slot) noexcept {
                std::lock_guard<std::mutex> lock(IncompleteBatchMutex);
                IncompleteBatch.push_back(std::move(slot));
                if (IncompleteBatch.size() >= SlotsInBatch) {
                    FullBatchesSlots.Enqueue(std::move(IncompleteBatch));
                    IncompleteBatch.clear();
                }
            }

            TLockFreeStack<std::list<std::unique_ptr<TMemRegion>>> FullBatchesSlots;
            ui32 SlotSize;
            ui32 SlotsInBatch;

            std::mutex IncompleteBatchMutex;
            std::list<std::unique_ptr<TMemRegion>> IncompleteBatch;
        };

        static constexpr ui32 MinAllocSz = 512;
        static constexpr ui32 MaxAllocSz = 8 * 1024 * 1024;
        static_assert((MinAllocSz & (MinAllocSz - 1)) == 0, "MinAllocSz must be a power of 2");
        static_assert((MaxAllocSz & (MaxAllocSz - 1)) == 0, "MaxAllocSz must be a power of 2");
        static constexpr ui32 ChainsNum = GetNumChains(MinAllocSz, MaxAllocSz);
        static constexpr ui32 BatchSizeBytes = 32 * 1024 * 1024; // 32 MB

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
            TMemRegion* AllocImpl(int size, ui32 flags, TSlotMemPool& pool) noexcept {
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
                TMemRegion* slot = localChain.TryGetSlot();
                if (slot) {
                    return slot;
                }
                // If no slot in local cache, try to get from global pool
                auto batch = pool.Chains[chainIndex].GetSlotsBatch();
                if (batch) {
                    localChain.PutSlotsBatch(std::move(*batch));
                    return localChain.TryGetSlot();
                }
                // If no slots in global pool, allocate new chunk
                auto chunk = pool.AllocNewChunk(BatchSizeBytes, true);
                if (!chunk) {
                    return nullptr;
                }
                for (size_t i = 0; i < localChain.SlotsInBatch; ++i) {
                    auto region = std::make_unique<TMemRegion>(chunk, i * localChain.SlotSize, localChain.SlotSize);
                    localChain.PutSlot(std::move(region));
                }
                return localChain.TryGetSlot();
            }

            void Free(TMemRegion&& mr, TSlotMemPool& pool) noexcept {
                ui32 chainIndex = GetChainIndex(mr.GetSize());
                if (Stopped) {
                    // current thread is stopped, return mr to global pool
                    pool.Chains[chainIndex].PutSlot(std::make_unique<TMemRegion>(std::move(mr)));
                    return;
                }

                auto& chain = Chains[chainIndex];
                Y_ABORT_UNLESS(chainIndex < ChainsNum, "Invalid chain index: %u", chainIndex);
                chain.PutSlot(std::make_unique<TMemRegion>(std::move(mr)));

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

    public:
        TSlotMemPool(NMonitoring::TDynamicCounterPtr counter)
            : TMemPoolBase(128, counter)
        {
            for (ui32 i = GetPowerOfTwo(MinAllocSz); i <= GetPowerOfTwo(MaxAllocSz); ++i) {
                Chains[GetChainIndex(1 << i)].Init(1 << i);
            }
        }

        int GetMaxAllocSz() const noexcept override {
            return MaxAllocSz;
        }

        TString GetName() const noexcept override {
            return "SlotMemPool";
        }

    protected:
        TMemRegion* AllocImpl(int size, ui32 flags) noexcept override {
            if (auto memReg = LocalCache.AllocImpl(size, flags, *this)) {
                memReg->Resize(size);
                AllocatedCounter->Add(size);
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

    std::shared_ptr<IMemPool> CreateSlotMemPool(TDynamicCounters* counters) noexcept {
        //auto* pool = HugeSingleton<TSlotMemPool>(MakeCounters(counters));
        static TSlotMemPool pool(MakeCounters(counters));
        return std::shared_ptr<TSlotMemPool>(&pool, [](TSlotMemPool*) {});
    }
}

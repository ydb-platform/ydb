#include "mem_pool.h"
#include "link_manager.h"
#include "ctx.h"

#include <ydb/library/actors/interconnect/rdma/ibdrv/include/infiniband/verbs.h>

#include <util/thread/lfstack.h>
#include <util/stream/output.h>
#include <util/system/align.h>

#include <vector>

#include <unistd.h>
#include <sys/syscall.h>
#include <mutex>
#include <thread>

#include <sys/mman.h>

static constexpr size_t CacheLineSz = 64;
static constexpr size_t HPageSz = (1 << 21);

namespace NInterconnect::NRdma {

    class TMemPoolImpl {
    public:
        static TMemRegion* DoAlloc(IMemPool* pool, int size, ui32 flags) {
            TMemRegion* region = pool->AllocImpl(size, flags);
            if (flags & IMemPool::BLOCK_MODE) {
                while (!region) {
                    std::this_thread::yield();
                    region = pool->AllocImpl(size, flags);
                }
            }
            return region;
        }
    };

    class TChunk: public NNonCopyable::TMoveOnly, public TAtomicRefCount<TChunk> {
    public:

    TChunk(std::vector<ibv_mr*>&& mrs, std::weak_ptr<IMemPool> pool, void* auxData) noexcept
        : MRs(std::move(mrs))
        , MemPool(pool)
        , AuxData(auxData)
    {
    }

    ~TChunk() {
        if (auto memPool = MemPool.lock()) {
            memPool->NotifyDealocated();
        }
        std::free(AuxData);
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
        if (auto memPool = MemPool.lock()) {
            memPool->Free(std::move(mr), *this);
        }
    }

    bool Empty() const noexcept {
        return MRs.empty();
    }
    
    void* GetAuxData() noexcept {
        return AuxData;
    }

    private:
        std::vector<ibv_mr*> MRs;
        std::weak_ptr<IMemPool> MemPool;
        void* AuxData;
    };

    TMemRegion::TMemRegion(TChunkPtr chunk, uint32_t offset, uint32_t size) noexcept 
        : Chunk(std::move(chunk))
        , Offset(offset)
        , Size(size)
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
        TMemRegion* region = TMemPoolImpl::DoAlloc(this, size, flags);
        if (!region) {
            return nullptr;
        }
        return TMemRegionPtr(region);
    }

    std::optional<TRcBuf> IMemPool::AllocRcBuf(int size, ui32 flags) noexcept {
        TMemRegion* region = TMemPoolImpl::DoAlloc(this, size, flags);
        if (!region) {
            return {};
        }
        return TRcBuf(IContiguousChunk::TPtr(region));
    }

    class TMemPoolBase: public IMemPool, public std::enable_shared_from_this<TMemPoolBase> {
    public:
        TMemPoolBase(size_t maxChunk)
            : Ctxs(NInterconnect::NRdma::NLinkMgr::GetAllCtxs())
            , MaxChunk(maxChunk)
            , Alignment(NSystemInfo::GetPageSize())
        {
        }
    protected:
        template<typename TAuxData>
        TChunkPtr AllocNewChunk(size_t size, bool hp) noexcept {
            static_assert(sizeof(TAuxData) < CacheLineSz, "AuxData too big");

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

            void* auxPtr = std::aligned_alloc(CacheLineSz, CacheLineSz);
            auxPtr = new (auxPtr)TAuxData;

            AllocatedChunks++;

            return MakeIntrusive<TChunk>(std::move(mrs), shared_from_this(), auxPtr);
        }

        void NotifyDealocated() noexcept {
            const std::lock_guard<std::mutex> lock(Mutex);
            AllocatedChunks--;
        }

        const NInterconnect::NRdma::NLinkMgr::TCtxsMap Ctxs;
        const size_t MaxChunk;
        const size_t Alignment;
        size_t AllocatedChunks = 0;
        std::mutex Mutex;
    };

    class TDummyMemPool: public TMemPoolBase {
    public:
        TDummyMemPool()
            : TMemPoolBase(-1)
        {}

        TMemRegion* AllocImpl(int size, ui32) noexcept override {
            struct TDummy {};
            auto chunk = AllocNewChunk<TDummy>(size, false);
            if (!chunk) {
                return nullptr; 
            }
            return new TMemRegion(chunk , 0, size);
        }

        void Free(TMemRegion&&, TChunk&) noexcept override {}

        int GetMaxAllocSz() const noexcept override {
            return 2048 << 20;
        }
    };

    class TIncrementalMemPool: public TMemPoolBase {
    public:
        TIncrementalMemPool()
            : TMemPoolBase(MaxChunks)
        {
            for (auto& x : ActiveAndFree) {
                x.store(nullptr);
            }
            for (auto& x : Inactive) {
                x.store(nullptr);
            }
        }

        ~TIncrementalMemPool()
        {
#if defined(__clang__)
            #pragma nounroll
#endif
            for (size_t i = 0; i < MaxChunks; i++) {
                {
                    TChunk* p = ActiveAndFree[i].exchange(nullptr);
                    if (p) {
                        TChunkPtr(p)->DecRef();

                    }
                }
                {
                    TChunk* p = Inactive[i].exchange(nullptr);
                    if (p) {
                        TChunkPtr(p)->DecRef();
                    }
                }
            }
        }

        struct TAuxChunkData {
            std::atomic<ui32> Allocated = 0; //not atomic modified only from alloc while not in shared array
            std::atomic<int> Freed;
            std::atomic<int> InactivePos = -1;
            bool IsInactive() const noexcept {
                return InactivePos.load(std::memory_order_acquire) >= 0;
            }
            //static TChunk* MarkActive(TChunk* chunk) {
            //    return reinterpret_cast<TChunk*>(reinterpret_cast<ui64>(chunk) | (((1ul << 15) - 1) << 48));
            //}
        };

        TMemRegion* AllocImpl(int size, ui32 flags) noexcept override {
            if (size > (int)ChunkSize)
                return nullptr;

            size_t startPos = GetStartPos();
            constexpr size_t maxAttempts = 7;
            size_t attempt = maxAttempts;
            TChunkPtr chunk;

            // we need to consider up to one page gap during allocation;
            const size_t alignAwareChunkSize = ChunkSize - Alignment;
            do {
                TChunk* cur = PopChunk(startPos, ActiveAndFree);
                if (!cur) {
                    if (attempt == maxAttempts) {
                        // May be all chunks are inactive 
                        ReclaimInactive();
                        continue;
                    } else {
                        // No chunks - try to alloc new one
                        break;
                    }
                }

                TAuxChunkData* aux = CastToAuxChunkData(cur);

                // We have chunk, check can we use it to allock region
                if (aux->Allocated.load() + (size_t)size > alignAwareChunkSize) {
                    Y_ABORT_UNLESS(!aux->IsInactive());
                    // No more space - put chunk in to inactive to wait deallocation regions
                    int pos = -1;
                    do {
                        pos = PushChunk(startPos, Inactive, cur);
                        if (pos == -1) {
                            ReclaimInactive();
                        }
                    } while (pos == -1);
                    aux->InactivePos.store(pos);
                } else {
                    Y_ABORT_UNLESS(!aux->IsInactive());
                    chunk = cur;
                    Y_ABORT_UNLESS(!CastToAuxChunkData(chunk.Get())->IsInactive());
                    break;
                }
            } while (attempt--);

            if (!chunk) {
                chunk = AllocNewChunk<TAuxChunkData>(ChunkSize, true);
                if (!chunk) {
                    return nullptr; 
                }
                chunk->Ref();
            }

            auto aux = CastToAuxChunkData(chunk.Get());
            size_t offset = aux->Allocated.load();
            size_t allignmentOffset = 0;
            if (flags & Flags::PAGE_ALIGNED) {
                allignmentOffset = Alignment - (offset % Alignment); 
            }
            aux->Allocated.store(offset + size + allignmentOffset);

            //size_t offset = CastToAuxChunkData(chunk.Get())->Allocated.fetch_add(size);
            Y_ABORT_UNLESS(!CastToAuxChunkData(chunk.Get())->IsInactive());

            while (PushChunk(startPos, ActiveAndFree, chunk.Get()) == -1) {
                std::this_thread::yield();
            }
            return new TMemRegion(chunk, offset + allignmentOffset, size);
        }

        void Free(TMemRegion&&, TChunk& chunk) noexcept override {
            TAuxChunkData* auxData = CastToAuxChunkData(&chunk);
            if (auxData->IsInactive() && chunk.RefCount() == (1 + 1)) { // last MemRegion for chunk: 1 ref from TMemRegion and 1 is "manual" during allocation 
                Y_ABORT_UNLESS(auxData->InactivePos < (int)Inactive.size());
                Y_ABORT_UNLESS(Inactive[auxData->InactivePos].load() == &chunk, "chunk: %p, expected: %p",
                    (void*)&chunk, Inactive[auxData->InactivePos].load());
                Inactive[auxData->InactivePos].store(nullptr);
                auxData->Allocated.store(0);
                auxData->InactivePos.store(-1);
                int ret = PushChunk(0, ActiveAndFree, &chunk);
                if (ret == -1) {
                    chunk.UnRef();
                }
            }
        }

        int GetMaxAllocSz() const noexcept override {
            return ChunkSize - Alignment;
        }

    private:
        static constexpr size_t ChunkSize = 32 << 20;
        static constexpr size_t MaxChunks = 1 << 5; //must be power of two
        static constexpr size_t ChunkGap = CacheLineSz / sizeof(TChunk*); // Distance between elemets to prevent cache line sharing
        static_assert(MaxChunks % ChunkGap == 0);

        using TChunkContainer = std::array<std::atomic<TChunk*>, MaxChunks>; 

        static size_t WrapPos(size_t x) noexcept {
            return x % MaxChunks;
        }

        static size_t GetStartPos() noexcept {
            static thread_local size_t id = ((size_t)syscall(SYS_gettid)) * ChunkGap % MaxChunks;
            return id;
        }

        static TAuxChunkData* CastToAuxChunkData(TChunk* chunk) noexcept {
            return reinterpret_cast<TAuxChunkData*>(chunk->GetAuxData());
        }

        static TChunk* PopChunk(size_t startPos, TChunkContainer& cont) noexcept {
#if defined(__clang__)
            #pragma nounroll
#endif
            for (size_t i = 0, j = startPos; i < MaxChunks; i++, j++) {
                size_t pos = WrapPos(j);
                TChunk* p = cont[pos].exchange(nullptr, std::memory_order_seq_cst);
                if (p) {
                    return p;
                }

               /*
                TChunk* p = cont[pos].load(std::memory_order_relaxed);
                if (p == nullptr) {
                    continue;
                }
                if (cont[pos].compare_exchange_strong(p, nullptr, std::memory_order_seq_cst)) {
                    return p;
                }
                */
            }
            return nullptr;
        }

        static int PushChunk(size_t startPos, TChunkContainer& cont, TChunk* chunk) noexcept {
#if defined(__clang__)
            #pragma nounroll
#endif
            for (size_t i = 0, j = startPos; i < MaxChunks; i++, j++) {
                size_t pos = WrapPos(j);
                TChunk* p = cont[pos].load(std::memory_order_relaxed);
                if (p != nullptr) {
                    continue;
                }
                if (cont[pos].compare_exchange_strong(p, chunk, std::memory_order_seq_cst)) {
                    return pos;
                }
            }
            return -1;
        }

        void ReclaimInactive() noexcept {
            if (!ReclaimMutex.try_lock()) {
                return;
            }
#if defined(__clang__)
            #pragma nounroll
#endif
            for (size_t i = 0; i < MaxChunks; i++) {
                TChunk* p = Inactive[i].load(std::memory_order_seq_cst);
                if (p == nullptr || !CastToAuxChunkData(p)->IsInactive()) {
                    continue;
                }
                if (p->RefCount() == 1) {
                    if (Inactive[i].compare_exchange_strong(p, nullptr, std::memory_order_seq_cst)) {
                        Y_ABORT_UNLESS(CastToAuxChunkData(p)->IsInactive());
                        Y_ABORT_UNLESS(p->RefCount() == 1);
                        //if (!CastToAuxChunkData(p)->IsInactive()) {
                        //    //if (PushChunk(0, Inactive, p) == -1) {
                        //    //    Y_ABORT_UNLESS(expr, ...)
                        //   // }
                        //    continue;
                        //}
                        auto aux = CastToAuxChunkData(p);
                        aux->Allocated.store(0);
                        aux->InactivePos.store(-1);
                        
                        if (PushChunk(0, ActiveAndFree, p) == -1) {
                            TChunkPtr(p)->UnRef();
                        }
                    }
                }
            }
            ReclaimMutex.unlock();
        }
        
        alignas(64) TChunkContainer ActiveAndFree;
        alignas(64) TChunkContainer Inactive; 
        std::mutex ReclaimMutex;
    };

    std::shared_ptr<IMemPool> CreateDummyMemPool() noexcept {
        return std::make_shared<TDummyMemPool>();
    }

    std::shared_ptr<IMemPool> CreateIncrementalMemPool() noexcept {
        return std::make_shared<TIncrementalMemPool>();
    }
}

#include "mem_pool.h"
#include "rdma_link_manager.h"
#include "rdma_ctx.h"

#include <ydb/library/actors/interconnect/rdma/ibdrv/include/infiniband/verbs.h>

#include <util/thread/lfstack.h>
#include <util/stream/output.h>

#include <vector>


namespace NInterconnect::NRdma {
    TChunk::TChunk(std::vector<ibv_mr*>&& mrs, std::weak_ptr<IMemPool> pool) noexcept
        : MRs(std::move(mrs))
        , MemPool(pool)
    {
    }

    TChunk::~TChunk() {
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

    ibv_mr* TChunk::GetMr(size_t deviceIndex) {
        if (Y_UNLIKELY(deviceIndex >= MRs.size())) {
            return nullptr;
        }
        return MRs[deviceIndex];
    }

    void TChunk::Free(TMemRegion&& mr) noexcept {
        if (auto memPool = MemPool.lock()) {
            memPool->Free(std::move(mr), *this);
        }
    }

    bool TChunk::Empty() const {
        return MRs.empty();
    }


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

    void* allocateMemory(size_t size, size_t alignment) {
        if (size % alignment != 0) {
            return nullptr;
        }
        return std::aligned_alloc(alignment, size);
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
                std::free(addr);
                return {};
            }
            res.push_back(mr);
        }
        return res;
    }

    class TMemPoolBase: public IMemPool, public std::enable_shared_from_this<TMemPoolBase> {
        static constexpr size_t ALIGNMENT = 4096;
    public:
        TMemPoolBase()
            : Ctxs(NInterconnect::NRdma::NLinkMgr::GetAllCtxs())
        {
        }
    protected:
        TMemRegionPtr AllocNewPage(int size) {
            void* ptr = allocateMemory(size, ALIGNMENT);
            if (!ptr) {
                return nullptr;
            }
            auto mrs = registerMemory(ptr, size, Ctxs);
            TChunkPtr chunk = std::make_shared<TChunk>(std::move(mrs), shared_from_this());
            return std::make_unique<TMemRegion>(chunk, 0, size);
        }

        const NInterconnect::NRdma::NLinkMgr::TCtxsMap Ctxs;
    };

    class TDummyMemPool: public TMemPoolBase {
        static constexpr size_t ALIGNMENT = 4096;
    public:
        using TMemPoolBase::TMemPoolBase;

        TMemRegionPtr Alloc(int size) override {
            return AllocNewPage(size);
        }

        void Free(TMemRegion&&, TChunk&) noexcept override {}
    };

    std::shared_ptr<IMemPool> CreateDummyMemPool() {
        return std::make_shared<TDummyMemPool>();
    }
}

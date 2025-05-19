#include "mem_pool.h"
#include "rdma_link_manager.h"
#include "rdma_ctx.h"

#include <ydb/library/actors/interconnect/rdma/ibdrv/include/infiniband/verbs.h>

#include <util/thread/lfstack.h>
#include <util/stream/output.h>

#include <vector>


namespace NInterconnect::NRdma {
    TMemRegion::TMemRegion(std::vector<ibv_mr*>&& mrs, std::weak_ptr<IMemPool> memPool) noexcept
        : MRs(std::move(mrs))
        , MemPool(memPool)
    {
    }

    TMemRegion::~TMemRegion() {
        if (MRs.empty()) {
            return;
        }
        if (auto memPool = MemPool.lock()) {
            memPool->Free(std::move(*this));
            return;
        } else {
            std::move(*this).Free();
        }
    }

    void TMemRegion::Free() && {
        if (MRs.empty()) {
            return;
        }
        auto addr = MRs.front()->addr;
        for (auto& m: MRs) {
            ibv_dereg_mr(m);
        }
        std::free(addr);
        MRs.clear();
    }

    bool TMemRegion::IsEmpty() const {
        return MRs.empty();
    }

    ibv_mr* TMemRegion::GetMr(size_t deviceIndex) {
        return MRs[deviceIndex];
    }

    void* TMemRegion::GetAddr() const {
        if (MRs.empty()) {
            return nullptr;
        }
        return MRs.front()->addr;
    }

    class TDummyMemPool: public IMemPool, public std::enable_shared_from_this<TDummyMemPool> {
        static constexpr size_t ALIGNMENT = 4096;
    public:
        TDummyMemPool()
            : Ctxs(NInterconnect::NRdma::NLinkMgr::GetAllCtxs())
        {
        }

        TMemRegionPtr Alloc(int size) override {
            return AllocNewPage(size);
        }

        void Free(TMemRegion&& mr) override {
            std::move(mr).Free();
        }
    
    private:
    TMemRegionPtr AllocNewPage(int size) {
            if (size % ALIGNMENT != 0) {
                return {};
            }
            void* ptr = std::aligned_alloc(ALIGNMENT, size);
            if (!ptr) {
                return {};
            }
            std::vector<ibv_mr*> res;
            res.reserve(Ctxs.size());
            for (const auto& [_, ctx]: Ctxs) {
                ibv_mr* mr = ibv_reg_mr(
                    ctx->GetProtDomain(), ptr, size,
                    IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE
                );
                if (!mr) {
                    std::free(ptr);
                    return {};
                }
                res.push_back(mr);
            }
            return std::make_shared<TMemRegion>(std::move(res), shared_from_this());
        }

    private:
        const NInterconnect::NRdma::NLinkMgr::TCtxsMap Ctxs;
        std::weak_ptr<IMemPool> Self;
    };

    std::shared_ptr<IMemPool> CreateDummyMemPool() {
        return std::make_shared<TDummyMemPool>();
    }
}

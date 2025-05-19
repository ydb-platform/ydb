#pragma once

#include <util/generic/noncopyable.h>
#include <util/generic/vector.h>

#include <memory>

struct ibv_mr;

namespace NInterconnect::NRdma {

    class IMemPool;

    class TMemRegion: public NNonCopyable::TMoveOnly {
    public:
        friend class IMemPool;

        using NNonCopyable::TMoveOnly::TMoveOnly;

        TMemRegion() noexcept = default;
        TMemRegion(std::vector<ibv_mr*>&& mrs, std::weak_ptr<IMemPool> memPool) noexcept;
        ~TMemRegion();
        void Free() &&;

        bool IsEmpty() const;
        ibv_mr* GetMr(size_t deviceIndex);
        void* GetAddr() const;
    private:
        std::vector<ibv_mr*> MRs;
        std::weak_ptr<IMemPool> MemPool;
    };

    using TMemRegionPtr = std::shared_ptr<TMemRegion>;


    class IMemPool {
    public:
        friend class TMemRegion;

        virtual ~IMemPool() = default;
        virtual TMemRegionPtr Alloc(int size) = 0;
    protected:
        virtual void Free(TMemRegion&& mr) = 0;
    };

    std::shared_ptr<IMemPool> CreateDummyMemPool();

}

#pragma once

#include <util/generic/noncopyable.h>
#include <util/generic/vector.h>

#include <memory>

struct ibv_mr;

namespace NInterconnect::NRdma {

    class IMemPool;
    class TMemRegion;

    class TChunk: public NNonCopyable::TMoveOnly {
    public:
        TChunk(std::vector<ibv_mr*>&& mrs, std::weak_ptr<IMemPool> pool) noexcept;
        ~TChunk();

        ibv_mr* GetMr(size_t deviceIndex);
        void Free(TMemRegion&& mr) noexcept;
        bool Empty() const;
    private:
        std::vector<ibv_mr*> MRs;
        std::weak_ptr<IMemPool> MemPool;
    };

    using TChunkPtr = std::shared_ptr<TChunk>;

    class TMemRegion: public NNonCopyable::TMoveOnly {
    public:
        TMemRegion(TChunkPtr chunk, uint32_t size, uint32_t offset) noexcept;
        ~TMemRegion();

        void*    GetAddr() const;
        uint32_t GetSize() const;

        uint32_t GetLKey(size_t deviceIndex) const;
        uint32_t GetRKey(size_t deviceIndex) const;
    protected:
        TChunkPtr Chunk;
        uint32_t Offset;
        uint32_t Size;
    };

    using TMemRegionPtr = std::unique_ptr<TMemRegion>;

    class IMemPool {
    public:
        friend class TChunk;

        virtual ~IMemPool() = default;
        virtual TMemRegionPtr Alloc(int size) = 0;
    protected:
        virtual void Free(TMemRegion&& mr, TChunk& chunk) noexcept = 0;
    };

    std::shared_ptr<IMemPool> CreateDummyMemPool();

}

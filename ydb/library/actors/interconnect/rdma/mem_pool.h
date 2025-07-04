#pragma once

#include <ydb/library/actors/util/rc_buf.h>

#include <util/generic/noncopyable.h>
#include <util/generic/vector.h>

#include <memory>

struct ibv_mr;

namespace NInterconnect::NRdma {

    class IMemPool;
    class TMemRegion;
    class TChunk;

    using TChunkPtr = TIntrusivePtr<TChunk>;

    class TMemRegion: public NNonCopyable::TMoveOnly, public IContiguousChunk {
    public:
        TMemRegion(TChunkPtr chunk, uint32_t offset, uint32_t size) noexcept;
        ~TMemRegion();

        void*    GetAddr() const;
        uint32_t GetSize() const;

        uint32_t GetLKey(size_t deviceIndex) const;
        uint32_t GetRKey(size_t deviceIndex) const;

    public: // IContiguousChunk
        TContiguousSpan GetData() const override;
        TMutableContiguousSpan GetDataMut() override;
        size_t GetOccupiedMemorySize() const override;
        EInnerType GetInnerType() const noexcept override;
    protected:
        const TChunkPtr Chunk;
        const uint32_t Offset;
        const uint32_t Size;
    };

    class TMemRegionSlice {
    public:
        TMemRegionSlice() = default;
        TMemRegionSlice(TIntrusivePtr<TMemRegion> memRegion, uint32_t offset, uint32_t size) noexcept;

        bool Empty() const {
            return !MemRegion;
        }
        void*    GetAddr() const;
        uint32_t GetSize() const;

        uint32_t GetLKey(size_t deviceIndex) const;
        uint32_t GetRKey(size_t deviceIndex) const;
    private:
        TIntrusivePtr<TMemRegion> MemRegion;
        uint32_t Offset;
        uint32_t Size;
    };

    using TMemRegionPtr = std::unique_ptr<TMemRegion>;

    TMemRegionSlice TryExtractFromRcBuf(const TRcBuf& rcBuf) noexcept;

    class IMemPool {
    public:
        friend class TChunk;

        virtual ~IMemPool() = default;

        TMemRegionPtr Alloc(int size) noexcept;
        TRcBuf AllocRcBuf(int size) noexcept;
        virtual int GetMaxAllocSz() const noexcept = 0;

    protected:
        virtual TMemRegion* AllocImpl(int size) noexcept = 0;
        virtual void Free(TMemRegion&& mr, TChunk& chunk) noexcept = 0;
        virtual void NotifyDealocated() noexcept = 0;
    };

    std::shared_ptr<IMemPool> CreateDummyMemPool() noexcept;
    std::shared_ptr<IMemPool> CreateIncrementalMemPool() noexcept;
}

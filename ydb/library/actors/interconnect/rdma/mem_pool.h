#pragma once

#include <ydb/library/actors/util/rc_buf.h>

#include <util/generic/noncopyable.h>
#include <util/generic/vector.h>

#include <memory>

struct ibv_mr;

namespace NMonitoring {
    struct TDynamicCounters;
}

namespace NInterconnect::NRdma {

    class IMemPool;
    class TMemRegion;
    class TChunk;
    class TMemPoolImpl;

    using TChunkPtr = TIntrusivePtr<TChunk>;

    class TMemRegion: public NNonCopyable::TMoveOnly, public IContiguousChunk {
    public:
        TMemRegion(TChunkPtr chunk, uint32_t offset, uint32_t size) noexcept;
        TMemRegion(TMemRegion&& other) noexcept = default;
        ~TMemRegion();

        void*    GetAddr() const;
        uint32_t GetSize() const;

        uint32_t GetLKey(size_t deviceIndex) const;
        uint32_t GetRKey(size_t deviceIndex) const;

        void Resize(uint32_t newSize) noexcept;

    public: // IContiguousChunk
        TContiguousSpan GetData() const override;
        TMutableContiguousSpan UnsafeGetDataMut() override;
        size_t GetOccupiedMemorySize() const override;
        EInnerType GetInnerType() const noexcept override;
        IContiguousChunk::TPtr Clone() noexcept override;
    protected:
        TChunkPtr Chunk;
        const uint32_t Offset;
        uint32_t Size;
        const uint32_t OrigSize;
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

    using TMemRegionPtr = TIntrusivePtr<TMemRegion>;

    TMemRegionSlice TryExtractFromRcBuf(const TRcBuf& rcBuf) noexcept;

    class IMemPool {
    public:
        // WARN: In case of addition new flags consider "Clone()" method implementation
        enum Flags : ui32 {
            EMPTY = 0,
            PAGE_ALIGNED = 1,    // Page alignment allocation
        };
        
        friend class TChunk;
        friend class TMemPoolImpl;

        virtual ~IMemPool() = default;

        TMemRegionPtr Alloc(int size, ui32 flags) noexcept;
        std::optional<TRcBuf> AllocRcBuf(int size, ui32 flags) noexcept;
        virtual int GetMaxAllocSz() const noexcept = 0;
        virtual TString GetName() const noexcept = 0;

    protected:
        virtual TMemRegion* AllocImpl(int size, ui32 flags) noexcept = 0;
        virtual void Free(TMemRegion&& mr, TChunk& chunk) noexcept = 0;
        virtual void DealocateMr(std::vector<ibv_mr*>& mrs) noexcept = 0;
    };

    std::shared_ptr<IMemPool> CreateDummyMemPool() noexcept;
    std::shared_ptr<IMemPool> CreateSlotMemPool(NMonitoring::TDynamicCounters* counters) noexcept;
}

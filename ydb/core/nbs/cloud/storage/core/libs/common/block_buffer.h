#pragma once

#include "public.h"

#include "block_data_ref.h"

#include <util/generic/noncopyable.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/memory/alloc.h>

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

class TBlockBuffer: private TMoveOnly
{
private:
    IAllocator* Allocator;
    size_t BytesCount = 0;
    TVector<TBlockDataRef> Blocks;

public:
    TBlockBuffer(IAllocator* allocator = TDefaultAllocator::Instance()) noexcept
        : Allocator(allocator)
    {}

    TBlockBuffer(TBlockBuffer&& other) noexcept
        : Allocator(other.Allocator)
        , BytesCount(other.BytesCount)
        , Blocks(other.DetachBlocks())
    {}

    ~TBlockBuffer() noexcept
    {
        Clear();
    }

    TBlockBuffer& operator=(TBlockBuffer&& other) noexcept
    {
        TBlockBuffer temp(std::move(other));
        Swap(temp);
        return *this;
    }

    explicit operator bool() const noexcept
    {
        return !Blocks.empty();
    }

    size_t GetBlocksCount() const noexcept
    {
        return Blocks.size();
    }

    size_t GetBytesCount() const noexcept
    {
        return BytesCount;
    }

    TBlockDataRef GetBlock(size_t index) const noexcept
    {
        Y_ABORT_UNLESS(index < Blocks.size());
        return Blocks[index];
    }

    const TVector<TBlockDataRef>& GetBlocks() const noexcept
    {
        return Blocks;
    }

    TBlockBuffer Clone() const
    {
        TBlockBuffer clone(Allocator);
        for (const auto& block: Blocks) {
            if (block.Data()) {
                clone.AddBlock(block);
            } else {
                clone.AddZeroBlock(block.Size());
            }
        }
        return clone;
    }

    TString AsString() const
    {
        TString result;
        result.ReserveAndResize(BytesCount);

        char* head = result.begin();
        for (const auto& block: Blocks) {
            const auto size = block.Size();
            if (size == 0) {
                continue;
            }

            Y_ABORT_UNLESS(head < result.data() + result.size());

            if (block.Data()) {
                memcpy((void*)head, (void*)block.Data(), size);
            } else {
                memset((void*)head, 0, size);
            }

            head += size;
        }
        return result;
    }

    void AddBlock(size_t size, char fill)
    {
        Y_ABORT_UNLESS(size);

        auto b = Allocator->Allocate(size);
        memset(b.Data, fill, size);
        AttachBlock({(char*)b.Data, size});
    }

    void AddBlock(TBlockDataRef block)
    {
        Y_ABORT_UNLESS(block);

        auto b = Allocator->Allocate(block.Size());
        memcpy(b.Data, block.Data(), block.Size());

        AttachBlock({(char*)b.Data, block.Size()});
    }

    void AddZeroBlock(size_t size)
    {
        AttachBlock(TBlockDataRef::CreateZeroBlock(size));
    }

    void AttachBlock(TBlockDataRef block)
    {
        BytesCount += block.Size();
        Blocks.push_back(block);
    }

    TVector<TBlockDataRef> DetachBlocks() noexcept
    {
        BytesCount = 0;
        return std::move(Blocks);
    }

    void Clear() noexcept
    {
        for (const auto& block: Blocks) {
            ReleaseBlock(block);
        }

        BytesCount = 0;
        Blocks.clear();
    }

    void Swap(TBlockBuffer& other) noexcept
    {
        DoSwap(Allocator, other.Allocator);
        DoSwap(BytesCount, other.BytesCount);
        DoSwap(Blocks, other.Blocks);
    }

private:
    void ReleaseBlock(TBlockDataRef block)
    {
        if (block.Data()) {
            Allocator->Release({(void*)block.Data(), block.Size()});
        }
    }
};

}   // namespace NYdb::NBS

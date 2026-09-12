#include "block_range_field.h"

#include "block_range_field_bitmask.h"
#include "block_range_field_flat_set.h"
#include "block_range_field_set.h"
#include "block_range_field_std_set.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/memory/arena_allocator.h>

#include <utility>

namespace NYdb::NBS::NBlockStore {

namespace {

//////////////////////////////////////////////////////////////////////////////

void CopyRanges(
    const TNodeBasedBlockRangeField& source,
    IBlockRangeFieldImpl* target)
{
    source.Enumerate(
        [&](TBlockRange16 range)
        {
            bool changed = false;
            Y_ABORT_UNLESS(target->TryAdd(range, &changed));
            return TNodeBasedBlockRangeField::EEnumerateContinuation::Continue;
        });
}

std::unique_ptr<TNodeBasedBlockRangeField> MakeImpl(
    IBlockRangeFieldImpl::EBackend backend,
    ui16 maxBlockCount,
    IArenaAllocatorPtr allocator)
{
    const size_t memUsageLimit =
        TBlockRangeFieldBitMask::CalcMemoryUsage(maxBlockCount);

    switch (backend) {
        case IBlockRangeFieldImpl::EBackend::StdSet: {
            auto result = std::make_unique<TBlockRangeFieldStdSet>(
                allocator,
                memUsageLimit);
            return result;
        }
        case IBlockRangeFieldImpl::EBackend::Set: {
            auto result =
                std::make_unique<TBlockRangeFieldSet>(allocator, memUsageLimit);
            return result;
        }
        case IBlockRangeFieldImpl::EBackend::FlatSet: {
            auto result = std::make_unique<TBlockRangeFieldFlatSet>(
                allocator,
                memUsageLimit);
            return result;
        }
        case IBlockRangeFieldImpl::EBackend::Bitmask:
        case IBlockRangeFieldImpl::EBackend::Simple: {
            return nullptr;
        }
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TBlockRangeField::TBlockRangeField(
    IArenaAllocatorPtr arenaAllocator,
    ui16 maxBlockCount,
    EBackend preferredBackend)
    : MaxBlockCount(maxBlockCount)
    , PreferredBackend(preferredBackend)
    , ArenaAllocator(std::move(arenaAllocator))
    , SimpleImpl(TBlockRangeFieldSimple{})
{
    Y_ABORT_UNLESS(ArenaAllocator);
    Y_ABORT_UNLESS(MaxBlockCount && !(MaxBlockCount & (MaxBlockCount - 1)));
}

TBlockRangeField::~TBlockRangeField() = default;

TBlockRangeField::TBlockRangeField(TBlockRangeField&& other) noexcept
    : MaxBlockCount(other.MaxBlockCount)
    , PreferredBackend(other.PreferredBackend)
    , ArenaAllocator(std::move(other.ArenaAllocator))
    , SimpleImpl(std::move(other.SimpleImpl))
    , NodeBasedImpl(std::move(other.NodeBasedImpl))
    , BitMaskBasedImpl(std::move(other.BitMaskBasedImpl))
{}

TBlockRangeField& TBlockRangeField::operator=(TBlockRangeField&& other) noexcept
{
    MaxBlockCount = other.MaxBlockCount;
    PreferredBackend = other.PreferredBackend;
    ArenaAllocator = std::move(other.ArenaAllocator);
    SimpleImpl = std::move(other.SimpleImpl);
    NodeBasedImpl = std::move(other.NodeBasedImpl);
    BitMaskBasedImpl = std::move(other.BitMaskBasedImpl);
    return *this;
}

////////////////////////////////////////////////////////////////////////////////

bool TBlockRangeField::Add(TBlockRange16 range)
{
    bool changed = false;
    if (GetImpl()->TryAdd(range, &changed)) {
        return changed;
    }
    // Retry with next backend.
    Upgrade();
    return Add(range);
}

void TBlockRangeField::Add(const TBlockRangeField& field)
{
    if (this == &field) {
        return;
    }

    if (field.IsBitmapBased()) {
        UpgradeToBitmapBackend();
        BitMaskBasedImpl->Add(*field.BitMaskBasedImpl);
        return;
    }

    field.GetNodeBasedImpl()->Enumerate(
        [&](TBlockRange16 range)
        {
            Add(range);
            return EEnumerateContinuation::Continue;
        });
}

bool TBlockRangeField::Remove(TBlockRange16 range)
{
    bool changed = false;
    if (GetImpl()->TryRemove(range, &changed)) {
        DowngradeToSimpleBackendIfEmpty();
        return changed;
    }
    // Retry with next backend.
    Upgrade();
    return Remove(range);
}

void TBlockRangeField::Remove(const TBlockRangeField& field)
{
    if (field.Empty() || Empty()) {
        return;
    }

    if (this == &field) {
        Clear();
        return;
    }

    if (field.IsBitmapBased()) {
        UpgradeToBitmapBackend();
        BitMaskBasedImpl->Remove(*field.BitMaskBasedImpl);
        return;
    }

    field.GetNodeBasedImpl()->Enumerate(
        [&](TBlockRange16 range)
        {
            Remove(range);
            return Empty() ? EEnumerateContinuation::Stop
                           : EEnumerateContinuation::Continue;
        });
}

bool TBlockRangeField::Clear()
{
    if (Empty()) {
        return false;
    }
    GetImpl()->Clear();
    DowngradeToSimpleBackendIfEmpty();
    return true;
}

bool TBlockRangeField::Overlaps(TBlockRange16 other) const
{
    if (Empty()) {
        return false;
    }
    return GetImpl()->Overlaps(other);
}

bool TBlockRangeField::Overlaps(const TBlockRangeField& other) const
{
    if (Empty() || other.Empty()) {
        return false;
    }

    if (!IsBitmapBased()) {
        bool overlaps = false;
        GetNodeBasedImpl()->Enumerate(
            [&](TBlockRange16 range)
            {
                overlaps = other.Overlaps(range);
                return overlaps ? EEnumerateContinuation::Stop
                                : EEnumerateContinuation::Continue;
            });
        return overlaps;
    }
    if (!other.IsBitmapBased()) {
        bool overlaps = false;
        other.GetNodeBasedImpl()->Enumerate(
            [&](TBlockRange16 range)
            {
                overlaps = Overlaps(range);
                return overlaps ? EEnumerateContinuation::Stop
                                : EEnumerateContinuation::Continue;
            });
        return overlaps;
    }

    return BitMaskBasedImpl->OverlapsWithBitMask(*other.BitMaskBasedImpl);
}

bool TBlockRangeField::Empty() const
{
    return GetImpl()->Empty();
}

size_t TBlockRangeField::GetBlockCount() const
{
    return GetImpl()->GetBlockCount();
}

std::optional<TBlockRange16> TBlockRangeField::GetFirstRange() const
{
    return GetImpl()->GetFirstRange();
}

TString TBlockRangeField::Print() const
{
    return GetImpl()->Print();
}

bool TBlockRangeField::IsBitmapBased() const
{
    return BitMaskBasedImpl != nullptr;
}

TString TBlockRangeField::Serialize() const
{
    return GetImpl()->Save();
}

void TBlockRangeField::DeserializeFromBitmap(const TString& source)
{
    if (source.size() > TBlockRangeFieldBitMask::CalcMemoryUsage(MaxBlockCount))
    {
        Y_DEBUG_ABORT_UNLESS(
            false,
            "Bitmap exceeds the configured block count");
        return;
    }
    UpgradeToBitmapBackend();
    BitMaskBasedImpl->Clear();
    BitMaskBasedImpl->DeserializeFromBitmap(source);
}

void TBlockRangeField::DeserializeFromRLE(const TString& source)
{
    const bool valid = TNodeBasedBlockRangeField::DeserializeFromRLE(
        source,
        MaxBlockCount,
        [](TBlockRange16) { return EEnumerateContinuation::Continue; });
    if (!valid) {
        Y_DEBUG_ABORT_UNLESS(false, "Malformed block range RLE");
        return;
    }

    Clear();

    TNodeBasedBlockRangeField::DeserializeFromRLE(
        source,
        MaxBlockCount,
        [&](TBlockRange16 range)
        {
            Add(range);
            return EEnumerateContinuation::Continue;
        });
}

size_t TBlockRangeField::GetAllocatedSize() const
{
    return GetImpl()->GetAllocatedSize();
}

size_t TBlockRangeField::GetUsedSize() const
{
    return GetImpl()->GetUsedSize();
}

IBlockRangeFieldImpl::EBackend TBlockRangeField::GetBackend() const
{
    return GetImpl()->GetBackend();
}

void TBlockRangeField::Upgrade()
{
    if (SimpleImpl) {
        UpgradeToPreferredBackend();
        return;
    }
    if (NodeBasedImpl) {
        UpgradeToBitmapBackend();
        return;
    }
    if (BitMaskBasedImpl) {
        Y_ABORT_UNLESS(false, "Can't upgrade from bitmask based backend");
        return;
    }
}

void TBlockRangeField::UpgradeToPreferredBackend()
{
    if (!SimpleImpl) {
        return;
    }

    if (PreferredBackend == IBlockRangeFieldImpl::EBackend::Bitmask) {
        UpgradeToBitmapBackend();
        return;
    }

    auto newBackend = MakeImpl(PreferredBackend, MaxBlockCount, ArenaAllocator);
    CopyRanges(*GetNodeBasedImpl(), newBackend.get());

    BitMaskBasedImpl.reset();
    NodeBasedImpl = std::move(newBackend);
    SimpleImpl.reset();
}

void TBlockRangeField::UpgradeToBitmapBackend()
{
    if (IsBitmapBased()) {
        return;
    }

    auto newBackend = std::make_unique<TBlockRangeFieldBitMask>(
        ArenaAllocator,
        MaxBlockCount);
    CopyRanges(*GetNodeBasedImpl(), newBackend.get());

    BitMaskBasedImpl = std::move(newBackend);
    NodeBasedImpl.reset();
    SimpleImpl.reset();
}

void TBlockRangeField::DowngradeToSimpleBackendIfEmpty()
{
    if (!Empty()) {
        return;
    }

    BitMaskBasedImpl.reset();
    NodeBasedImpl.reset();
    SimpleImpl = TBlockRangeFieldSimple{};
}

IBlockRangeFieldImpl* TBlockRangeField::GetImpl()
{
    if (SimpleImpl) {
        return &*SimpleImpl;
    }
    if (NodeBasedImpl) {
        return NodeBasedImpl.get();
    }
    return BitMaskBasedImpl.get();
}

const IBlockRangeFieldImpl* TBlockRangeField::GetImpl() const
{
    if (SimpleImpl) {
        return &*SimpleImpl;
    }
    if (NodeBasedImpl) {
        return NodeBasedImpl.get();
    }
    return BitMaskBasedImpl.get();
}

TNodeBasedBlockRangeField* TBlockRangeField::GetNodeBasedImpl()
{
    Y_ABORT_UNLESS(!IsBitmapBased());
    return SimpleImpl ? &*SimpleImpl : NodeBasedImpl.get();
}

const TNodeBasedBlockRangeField* TBlockRangeField::GetNodeBasedImpl() const
{
    Y_ABORT_UNLESS(!IsBitmapBased());
    return SimpleImpl ? &*SimpleImpl : NodeBasedImpl.get();
}

//////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore

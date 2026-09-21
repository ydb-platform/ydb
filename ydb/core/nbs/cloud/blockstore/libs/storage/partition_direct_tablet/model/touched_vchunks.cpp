#include "touched_vchunks.h"

#include <util/generic/bitmap.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

namespace {

////////////////////////////////////////////////////////////////////////////////

TString MakeEmptyMask()
{
    return TString(TTouchedVChunks::MaskSize, 0);
}

size_t CountBits(TStringBuf mask)
{
    size_t result = 0;
    for (const char byte: mask) {
        result += ::NBitMapPrivate::CountBitsPrivate(static_cast<ui8>(byte));
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace

////////////////////////////////////////////////////////////////////////////////

size_t TTouchedVChunks::GetCount() const
{
    return Count;
}

bool TTouchedVChunks::Get(ui32 vChunkIndex) const
{
    const ui32 maskIndex = GetMaskIndex(vChunkIndex);
    if (maskIndex >= Masks.size()) {
        return false;
    }

    const ui32 bitIndex = vChunkIndex % VChunksPerMask;
    return static_cast<ui8>(Masks[maskIndex][bitIndex / 8]) &
           (1u << (bitIndex % 8));
}

TRegionVChunks TTouchedVChunks::GetTouchedVChunks(ui32 regionIndex) const
{
    static_assert(VChunkPerRegionCount % 8 == 0);
    constexpr size_t RegionByteCount = VChunkPerRegionCount / 8;
    static_assert(RegionByteCount == sizeof(ui32));
    static_assert(MaskSize % RegionByteCount == 0);
    constexpr size_t RegionsPerMask = MaskSize / RegionByteCount;

    const size_t maskIndex = regionIndex / RegionsPerMask;
    if (maskIndex >= Masks.size()) {
        return {};
    }

    const size_t regionIndexInMask = regionIndex % RegionsPerMask;
    const size_t byteIndex = regionIndexInMask * RegionByteCount;
    ui32 bits = 0;
    // Persisted mask bytes store lower VChunk indices in less significant bits.
    for (size_t i = 0; i < RegionByteCount; ++i) {
        bits |= ui32(static_cast<ui8>(Masks[maskIndex][byteIndex + i]))
                << (8 * i);
    }
    return TRegionVChunks(bits);
}

bool TTouchedVChunks::Add(ui32 vChunkIndex, TPersistResultPromise promise)
{
    if (Get(vChunkIndex)) {
        const ui32 maskIndex = GetMaskIndex(vChunkIndex);
        if (PendingMasks.contains(maskIndex)) {
            PendingPromises.push_back(std::move(promise));
        } else if (SavingMasks.contains(maskIndex)) {
            SavingPromises.push_back(std::move(promise));
        } else {
            promise.TrySetValue(EPersistResult::Success);
        }
        return false;
    }

    const ui32 maskIndex = GetMaskIndex(vChunkIndex);
    if (Masks.size() <= maskIndex) {
        Masks.resize(maskIndex + 1, MakeEmptyMask());
    }

    const ui32 bitIndex = vChunkIndex % VChunksPerMask;
    const ui32 byteIndex = bitIndex / 8;
    const ui8 byte = static_cast<ui8>(Masks[maskIndex][byteIndex]);
    Masks[maskIndex][byteIndex] =
        static_cast<char>(byte | (1u << (bitIndex % 8)));
    ++Count;
    PendingMasks.insert(maskIndex);
    PendingPromises.push_back(std::move(promise));

    return !IsSaveInProgress();
}

void TTouchedVChunks::Load(TChunk chunk)
{
    Y_ABORT_UNLESS(chunk.VChunkStartIndex % VChunksPerMask == 0);
    Y_ABORT_UNLESS(chunk.Mask.size() == MaskSize);

    const ui32 maskIndex = GetMaskIndex(chunk.VChunkStartIndex);
    if (Masks.size() <= maskIndex) {
        Masks.resize(maskIndex + 1, MakeEmptyMask());
    }
    Count -= CountBits(Masks[maskIndex]);
    Count += CountBits(chunk.Mask);
    Masks[maskIndex] = std::move(chunk.Mask);
}

TVector<TTouchedVChunks::TChunk> TTouchedVChunks::BeginSave()
{
    Y_ABORT_UNLESS(!PendingMasks.empty());
    Y_ABORT_UNLESS(SavingMasks.empty());

    SavingMasks.swap(PendingMasks);
    SavingPromises = std::move(PendingPromises);
    PendingPromises.clear();

    TVector<TChunk> result;
    result.reserve(SavingMasks.size());
    for (const ui32 maskIndex: SavingMasks) {
        result.push_back({
            .VChunkStartIndex = GetMaskStartVChunkIndex(maskIndex),
            .Mask = Masks[maskIndex],
        });
    }
    return result;
}

bool TTouchedVChunks::IsSaveInProgress() const
{
    return !SavingMasks.empty();
}

bool TTouchedVChunks::HasPendingChanges() const
{
    return !PendingMasks.empty();
}

void TTouchedVChunks::OnSaveCompleted()
{
    Y_ABORT_UNLESS(!SavingMasks.empty());

    for (auto& promise: SavingPromises) {
        promise.TrySetValue(EPersistResult::Success);
    }
    SavingPromises.clear();
    SavingMasks.clear();
}

void TTouchedVChunks::OnSaveInterrupted()
{
    for (auto& promise: SavingPromises) {
        promise.TrySetValue(EPersistResult::Cancelled);
    }
    SavingPromises.clear();

    for (auto& promise: PendingPromises) {
        promise.TrySetValue(EPersistResult::Cancelled);
    }
    PendingPromises.clear();
}

ui32 TTouchedVChunks::GetMaskIndex(ui32 vChunkIndex)
{
    return vChunkIndex / VChunksPerMask;
}

ui32 TTouchedVChunks::GetMaskStartVChunkIndex(ui32 maskIndex)
{
    return maskIndex * VChunksPerMask;
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect

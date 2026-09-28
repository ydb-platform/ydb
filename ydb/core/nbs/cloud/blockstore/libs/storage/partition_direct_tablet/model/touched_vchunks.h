#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/public.h>

#include <util/generic/bitmap.h>
#include <util/generic/set.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/types.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

// Stores touched-vchunk bits and tracks mask chunks that need persistence.
class TTouchedVChunks final: public ITouchedProvider
{
public:
    // Number of bytes in one persisted touched-vchunk mask.
    static constexpr size_t MaskSize = 128;
    static constexpr ui32 VChunksPerMask = MaskSize * 8;

    struct TChunk
    {
        ui32 VChunkStartIndex;
        TString Mask;
    };

    // Returns the number of touched vchunks.
    [[nodiscard]] size_t GetCount() const;

    // Implemented ITouchedProvider.
    [[nodiscard]] bool Get(ui32 vChunkIndex) const override;

    [[nodiscard]] TRegionVChunks GetTouchedVChunks(
        ui32 regionIndex) const override;

    // Sets the touched bit and returns true if ready to start transaction.
    bool Add(ui32 vChunkIndex, TPersistResultPromise promise);

    // Loads one persisted mask chunk. VChunkIndex must start a mask.
    void Load(TChunk chunk);

    // Starts persisting all pending chunks and returns their current values.
    [[nodiscard]] TVector<TChunk> BeginSave();

    // Returns whether a persistence transaction is in progress.
    [[nodiscard]] bool IsSaveInProgress() const;

    // Returns whether any changed mask chunks await persistence.
    [[nodiscard]] bool HasPendingChanges() const;

    // Acknowledges a successful persistence transaction.
    void OnSaveCompleted();
    void OnSaveInterrupted();

private:
    [[nodiscard]] static ui32 GetMaskIndex(ui32 vChunkIndex);
    [[nodiscard]] static ui32 GetMaskStartVChunkIndex(ui32 maskIndex);

    TVector<TString> Masks;
    TSet<ui32> PendingMasks;
    TSet<ui32> SavingMasks;
    TVector<TPersistResultPromise> SavingPromises;
    TVector<TPersistResultPromise> PendingPromises;
    size_t Count = 0;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect

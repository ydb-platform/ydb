#pragma once

#include "public.h"

#include <library/cpp/yt/farmhash/farm_hash.h>

#include <library/cpp/yt/memory/range.h>
#include <library/cpp/yt/memory/ref.h>
#include <library/cpp/yt/memory/ref_counted.h>

#include <array>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TXorFilter
{
public:
    TXorFilter() = default;

    bool IsInitialized() const;
    void Initialize(TSharedRef data);

    bool Contains(TFingerprint key) const;

    static int ComputeByteSize(int keyCount, int bitsPerKey);

    static TSharedRef Build(TRange<TFingerprint> keys, int bitsPerKey, int trialCount = 10);

private:
    constexpr static int WordSize = 64;
    static_assert(WordSize % sizeof(ui64) == 0);

    constexpr static double LoadFactor = 1.23;
    constexpr static int LoadFactorIncrement = 32;

    constexpr static int FormatVersionSize = sizeof(i32);
    static_assert(FormatVersionSize == 4);

    constexpr static int FormatVersion = 1;

    // First three salts are used for computing slots of a certain key.
    // The fourth one is used to generate the expected fingerprint of the key.
    std::array<ui64, 4> Salts_;
    int BitsPerKey_;
    int SlotCount_;

    constexpr static int MetaSize = sizeof(BitsPerKey_) + sizeof(Salts_) + sizeof(SlotCount_);
    static_assert(MetaSize == 40, "Consider changing FormatVersion");

    TSharedRef Data_;


    //! Used when building filter.
    TXorFilter(int bitsPerKey, int slotCount);

    void LoadMeta();
    void SaveMeta(TMutableRef data) const;

    ui64 GetUi64Word(TRef data, int index) const;
    void SetUi64Word(TMutableRef data, int index, ui64 value) const;

    ui64 GetEntry(TRef data, int index) const;
    void SetEntry(TMutableRef data, int index, ui64 value) const;

    ui64 GetHash(ui64 key, int hashIndex) const;

    int GetSlot(ui64 key, int hashIndex) const;

    ui64 GetExpectedXorFingerprint(ui64 key) const;

    static TSharedRef DoBuild(TRange<TFingerprint> keys, int bitsPerKey);
    static int ComputeSlotCount(int keyCount);
    int ComputeAllocationSize() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

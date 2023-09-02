#include "xor_filter.h"

#include <yt/yt/core/misc/numeric_helpers.h>
#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/serialize.h>

#include <library/cpp/iterator/enumerate.h>

#include <util/digest/multi.h>

#include <queue>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

bool TXorFilter::IsInitialized() const
{
    return static_cast<bool>(Data_);
}

void TXorFilter::Initialize(TSharedRef data)
{
    YT_VERIFY(!IsInitialized());
    Data_ = std::move(data);
    LoadMeta();
}

bool TXorFilter::Contains(TFingerprint key) const
{
    YT_ASSERT(IsInitialized());

    ui64 actualXorFingerprint = 0;
    for (int hashIndex = 0; hashIndex < 3; ++hashIndex) {
        actualXorFingerprint ^= GetEntry(Data_, GetSlot(key, hashIndex));
    }
    return actualXorFingerprint == GetExpectedXorFingerprint(key);
}

int TXorFilter::ComputeSlotCount(int keyCount)
{
    int slotCount = keyCount * LoadFactor + LoadFactorIncrement;

    // Make slotCount a multiple of 3.
    slotCount = slotCount / 3 * 3;

    return slotCount;
}

int TXorFilter::ComputeByteSize(int keyCount, int bitsPerKey)
{
    return DivCeil(ComputeSlotCount(keyCount) * bitsPerKey, WordSize) * sizeof(ui64);
}

int TXorFilter::ComputeAllocationSize() const
{
    int dataSize = DivCeil(SlotCount_ * BitsPerKey_, WordSize) * sizeof(ui64);
    return FormatVersionSize + dataSize + MetaSize;
}

ui64 TXorFilter::GetUi64Word(TRef data, int index) const
{
    ui64 result;
    std::memcpy(
        &result,
        data.begin() + index * sizeof(result) + FormatVersionSize,
        sizeof(result));
    return result;
}

void TXorFilter::SetUi64Word(TMutableRef data, int index, ui64 value) const
{
    std::memcpy(
        data.begin() + index * sizeof(value) + FormatVersionSize,
        &value,
        sizeof(value));
}

ui64 TXorFilter::GetHash(ui64 key, int hashIndex) const
{
    ui64 hash = Salts_[hashIndex];
    HashCombine(hash, key);
    return hash;
}

ui64 TXorFilter::GetEntry(TRef data, int index) const
{
    // Fast path.
    if (BitsPerKey_ == 8) {
        return static_cast<ui8>(data[index + FormatVersionSize]);
    }

    int startBit = index * BitsPerKey_;
    int wordIndex = startBit / WordSize;
    int offset = startBit % WordSize;

    auto loWord = GetUi64Word(data, wordIndex);
    auto result = loWord >> offset;

    if (offset + BitsPerKey_ > WordSize) {
        auto hiWord = GetUi64Word(data, wordIndex + 1);
        result |= hiWord << (WordSize - offset);
    }

    return result & MaskLowerBits(BitsPerKey_);
}

void TXorFilter::SetEntry(TMutableRef data, int index, ui64 value) const
{
    // Fast path.
    if (BitsPerKey_ == 8) {
        data[index + FormatVersionSize] = static_cast<ui8>(value);
    }

    int startBit = index * BitsPerKey_;
    int wordIndex = startBit / WordSize;
    int offset = startBit % WordSize;

    auto loWord = GetUi64Word(data, wordIndex);
    loWord &= ~(MaskLowerBits(BitsPerKey_) << offset);
    loWord ^= value << offset;
    SetUi64Word(data, wordIndex, loWord);

    if (offset + BitsPerKey_ > WordSize) {
        auto hiWord = GetUi64Word(data, wordIndex + 1);
        hiWord &= ~(MaskLowerBits(BitsPerKey_) >> (WordSize - offset));
        hiWord ^= value >> (WordSize - offset);
        SetUi64Word(data, wordIndex + 1, hiWord);
    }
}

int TXorFilter::GetSlot(ui64 key, int hashIndex) const
{
    auto hash = GetHash(key, hashIndex);

    // A faster way to generate an almost uniform integer in [0, SlotCount_ / 3).
    // Note the "hash >> 32" part. Somehow higher 32 bits are distributed much
    // better than lower ones, and that turned out to be critical for the filter
    // building success probability.
    auto res = static_cast<ui64>((hash >> 32) * (SlotCount_ / 3)) >> 32;

    return res + (SlotCount_ / 3 * hashIndex);
}

ui64 TXorFilter::GetExpectedXorFingerprint(ui64 key) const
{
    return GetHash(key, 3) & MaskLowerBits(BitsPerKey_);
}

void TXorFilter::SaveMeta(TMutableRef data) const
{
    {
        char* ptr = data.begin();
        WritePod(ptr, static_cast<i32>(FormatVersion));
    }

    {
        char* ptr = data.end() - MetaSize;
        WritePod(ptr, Salts_);
        WritePod(ptr, BitsPerKey_);
        WritePod(ptr, SlotCount_);
        YT_VERIFY(ptr == data.end());
    }
}

void TXorFilter::LoadMeta()
{
    YT_ASSERT(IsInitialized());

    int formatVersion;
    {
        const char* ptr = Data_.begin();
        ReadPod(ptr, formatVersion);
    }

    if (formatVersion != 1) {
        THROW_ERROR_EXCEPTION("Invalid XOR filter format version %v",
            formatVersion);
    }

    {
        const char* ptr = Data_.end() - MetaSize;
        ReadPod(ptr, Salts_);
        ReadPod(ptr, BitsPerKey_);
        ReadPod(ptr, SlotCount_);
        YT_VERIFY(ptr == Data_.end());
    }
}

TXorFilter::TXorFilter(int bitsPerKey, int slotCount)
    : BitsPerKey_(bitsPerKey)
    , SlotCount_(slotCount)
{
    if (bitsPerKey >= WordSize) {
        THROW_ERROR_EXCEPTION("Cannot create xor filter: expected bits_per_key < %v, got %v",
            WordSize,
            bitsPerKey);
    }

    for (int i = 0; i < 4; ++i) {
        Salts_[i] = RandomNumber<ui64>();
    }
}

TSharedRef TXorFilter::Build(TRange<TFingerprint> keys, int bitsPerKey, int trialCount)
{
    for (int trialIndex = 0; trialIndex < trialCount; ++trialIndex) {
        if (auto data = DoBuild(keys, bitsPerKey)) {
            return data;
        }
    }

    THROW_ERROR_EXCEPTION("Failed to build XOR filter in %v attempts",
        trialCount);
}

TSharedRef TXorFilter::DoBuild(TRange<TFingerprint> keys, int bitsPerKey)
{
    int slotCount = ComputeSlotCount(std::ssize(keys));

    TXorFilter filter(bitsPerKey, slotCount);
    auto data = TSharedMutableRef::Allocate(filter.ComputeAllocationSize());

    std::vector<int> assignedKeysXor(slotCount);
    std::vector<int> hitCount(slotCount);

    for (auto [keyIndex, key] : Enumerate(keys)) {
        for (int hashIndex = 0; hashIndex < 3; ++hashIndex) {
            int slot = filter.GetSlot(key, hashIndex);
            assignedKeysXor[slot] ^= keyIndex;
            ++hitCount[slot];
        }
    }

    std::vector<char> inQueue(slotCount);
    std::queue<int> queue;
    for (int slot = 0; slot < slotCount; ++slot) {
        if (hitCount[slot] == 1) {
            queue.push(slot);
            inQueue[slot] = true;
        }
    }

    std::vector<std::pair<int, int>> order;
    order.reserve(keys.Size());

    while (!queue.empty()) {
        int candidateSlot = queue.front();
        queue.pop();

        if (hitCount[candidateSlot] == 0) {
            continue;
        }

        YT_VERIFY(hitCount[candidateSlot] == 1);

        int keyIndex = assignedKeysXor[candidateSlot];
        YT_VERIFY(keyIndex != -1);
        order.emplace_back(keyIndex, candidateSlot);

        auto key = keys[keyIndex];
        for (int hashIndex = 0; hashIndex < 3; ++hashIndex) {
            int slot = filter.GetSlot(key, hashIndex);
            assignedKeysXor[slot] ^= keyIndex;
            if (--hitCount[slot] == 1 && !inQueue[slot]) {
                inQueue[slot] = true;
                queue.push(slot);
            }
        }
    }

    if (std::ssize(order) < std::ssize(keys)) {
        return {};
    }

    std::reverse(order.begin(), order.end());

    for (auto [keyIndex, candidateSlot] : order) {
        auto key = keys[keyIndex];

        YT_VERIFY(filter.GetEntry(data, candidateSlot) == 0);

        ui64 expectedXor = filter.GetExpectedXorFingerprint(key);
        ui64 actualXor = 0;

        for (int hashIndex = 0; hashIndex < 3; ++hashIndex) {
            actualXor ^= filter.GetEntry(data, filter.GetSlot(key, hashIndex));
        }

        filter.SetEntry(data, candidateSlot, actualXor ^ expectedXor);
    }

    filter.SaveMeta(data);

    return data;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

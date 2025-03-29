#pragma once

#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/public/udf/udf_data_type.h>
#include <yql/essentials/public/udf/udf_types.h>
#include <yql/essentials/public/udf/udf_value.h>

#include <util/generic/buffer.h>

#include <ydb/library/yql/utils/simd/simd.h>

#include "tuple.h"

namespace NKikimr {
namespace NMiniKQL {
namespace NPackedTuple {

/*
 * Class TNeumannHashTable provides hash table implementation for packed tuples.
 *
 * TNeumannHashTable has the following layout:
 * HashTable:    [[Directory 1][Directory 2]...[Directory N]]
 * Directory:    [[Buffer Index][Bloom Filter]]
 * Buffer:       [[Tuple 1][Tuple 2]...[Tuple M]]
 */

template <class T> class TBloomFilterMasks {

    static constexpr size_t BinomCoeff(size_t n, size_t k) noexcept {
        return k == 0 ? 1 : BinomCoeff(n - 1, k - 1) * n / k;
    }

    static constexpr size_t Pow2Round(size_t n) {
        return n == 0 ? 0
                      : 1ul << (sizeof(size_t) * 8 - std::countl_zero(n - 1));
    }

    template <size_t N, size_t K>
    static consteval size_t GenRec(auto &arr, T mask, size_t i, size_t j) {
        if constexpr (K == 0) {
            arr[i] = mask;
            return i + 1;
        } else {
            for (size_t ind = j; ind <= N - K; ++ind) {
                i = GenRec<N, K - 1>(arr, mask | (T(1) << ind), i, ind + 1);
            }
            return i;
        }
    }

  public:
    template <unsigned BloomBits, unsigned BloomMaskBits>
    static constexpr auto Gen() {
        constexpr size_t BloomMasksNum = BinomCoeff(BloomBits, BloomMaskBits);
        constexpr size_t BloomMasksNumRounded = Pow2Round(BloomMasksNum);

        auto result = std::array<T, BloomMasksNumRounded>{};
        GenRec<BloomBits, BloomMaskBits>(result, 0, 0, 0);
        std::copy(result.data(),
                  result.data() + BloomMasksNumRounded - BloomMasksNum,
                  result.data() + BloomMasksNum);

        return result;
    }
};

class TNeumannHashTable {
    /// hash = [...] [directory_bits] [...] [bloom_filter_bits]
    using Hash = ui32;
    using TBloom = ui16;

    static constexpr unsigned kBloomBits = 16;
    static constexpr unsigned kBloomMaskBits = 4;

    alignas(64) static constexpr auto kBloomTags =
        TBloomFilterMasks<TBloom>::template Gen<kBloomBits, kBloomMaskBits>();
    static constexpr unsigned kBloomHashBits =
        std::countr_zero(kBloomTags.size());

    static_assert(kBloomBits != 0 && kBloomMaskBits != 0 &&
                  kBloomMaskBits < kBloomBits &&
                  kBloomBits <= sizeof(TBloom) * 8);

    struct TDirectory {
        using T = ui64;

        T &operator*() { return reinterpret_cast<T &>(*this); }
        const T &operator*() const {
            return reinterpret_cast<const T &>(*this);
        }

        static constexpr size_t kBufferSlotShift = kBloomBits;

        T BloomFilter : kBloomBits = 0;
        T BufferSlot : sizeof(T) * 8 - kBloomBits = 0;
    };
    static_assert(sizeof(TDirectory) == sizeof(typename TDirectory::T));

    struct THash {
        using T = Hash;

        T &operator*() { return reinterpret_cast<T &>(*this); }
        const T &operator*() const {
            return reinterpret_cast<const T &>(*this);
        }

        T BloomTagSlot : kBloomHashBits;
        T : sizeof(T) * 8 - kBloomHashBits;
    };
    static_assert(sizeof(THash) == sizeof(typename THash::T));

    Hash getDirectorySlot(THash thash) const {
        return *thash >> DirectoryHashShift_;
    }

    struct TOutplace {
        Hash Hash;
        ui32 Index;
    };

  public:
    using OnMatchCallback = void(const ui8 *tuple);

    TNeumannHashTable() = default;

    explicit TNeumannHashTable(const TTupleLayout *layout) {
        SetTupleLayout(layout);
    }

    void SetTupleLayout(const TTupleLayout *layout) {
        Layout_ = layout;
        IsInplace_ = layout->TotalRowSize <= 16;
        BufferSlotSize_ = IsInplace_ ? layout->TotalRowSize : sizeof(TOutplace);

        Clear();
    }

    /// TODO: memory management
    TNeumannHashTable(const TNeumannHashTable &) = delete;
    TNeumannHashTable &operator=(const TNeumannHashTable &) = delete;

    void Build(const ui8 *const tuples, const ui8 *const overflow, ui32 nItems,
               ui32 estimatedLogSize = 0) {
        Y_ASSERT(Layout_ != nullptr);
        Y_ASSERT(Directories_.empty() && Buffer_.empty() &&
                 Tuples_ == nullptr && Overflow_ == nullptr);

        if (estimatedLogSize == 0) {
            estimatedLogSize = 32 - std::countl_zero<ui32>(nItems);
            estimatedLogSize = estimatedLogSize > 2 ? estimatedLogSize - 2 : estimatedLogSize;
        }

        Tuples_ = tuples;
        Overflow_ = overflow;

        DirectoryHashBits_ = estimatedLogSize;
        DirectoryHashShift_ = sizeof(Hash) * 8 - DirectoryHashBits_;
        Directories_.resize((1ul << DirectoryHashBits_) + 1);

        for (ui32 ind = 0; ind != nItems; ++ind) {
            const THash thash =
                ReadUnaligned<THash>(tuples + Layout_->TotalRowSize * ind);
            auto &dir = *Directories_[getDirectorySlot(thash)];
            dir += 1ul << TDirectory::kBufferSlotShift;
            dir |= kBloomTags[thash.BloomTagSlot];
        }

        {
            ui32 accum = 0;
            for (auto &directory : Directories_) {
                accum += directory.BufferSlot;
                directory.BufferSlot = accum;

                directory.BloomFilter = ~directory.BloomFilter;
            }
        }

        /// TODO: maybe speed up buffer init

        Buffer_.resize(BufferSlotSize_ * nItems);
        for (ui32 ind = 0; ind != nItems; ++ind) {
            const ui8 *const row = tuples + Layout_->TotalRowSize * ind;
            const THash thash = ReadUnaligned<THash>(row);

            auto &dir = *Directories_[getDirectorySlot(thash)];
            dir -= 1ul << TDirectory::kBufferSlotShift;
            const ui32 dataSlot = dir >> TDirectory::kBufferSlotShift;

            if (IsInplace_) {
                std::memcpy(Buffer_.data() + BufferSlotSize_ * dataSlot, row,
                            BufferSlotSize_);
            } else {
                auto *const outRow = reinterpret_cast<TOutplace *>(
                    Buffer_.data() + BufferSlotSize_ * dataSlot);
                outRow->Hash = *thash;
                outRow->Index = ind;
            }
        }
    }

    void Apply(const ui8 *const row, const ui8 *const overflow,
               auto &&onMatch) {
        Y_ASSERT(Layout_ != nullptr);
        Y_ASSERT(!Directories_.empty() && Tuples_ != nullptr);

        const THash &thash = reinterpret_cast<const THash &>(row[0]);
        const TBloom hashBloomTag = kBloomTags[thash.BloomTagSlot];

        const Hash dirSlot = getDirectorySlot(thash);
        const TDirectory dir = Directories_[dirSlot];
        const TBloom dirBloomFilter = dir.BloomFilter;

        if (hashBloomTag & dirBloomFilter) {
            return;
        }

        const ui8 *const begin =
            Buffer_.data() + BufferSlotSize_ * dir.BufferSlot;
        const ui8 *const end =
            Buffer_.data() +
            BufferSlotSize_ * Directories_[dirSlot + 1].BufferSlot;

        /// TODO: key ordering

        for (auto it = begin; it != end; it += BufferSlotSize_) {
            const ui8 *matchRow;
            if (IsInplace_) {
                const Hash matchRowHash = ReadUnaligned<Hash>(it);
                if (matchRowHash != *thash) {
                    continue;
                }
                matchRow = it;
            } else {
                const auto *const outRow =
                    reinterpret_cast<const TOutplace *>(it);
                if (outRow->Hash != *thash) {
                    continue;
                }
                matchRow = Tuples_ + Layout_->TotalRowSize * outRow->Index;
            }

            if (Layout_->KeysEqual(row, overflow, matchRow, Overflow_)) {
                onMatch(matchRow);
            }
        }
    }

    void Clear() {
        Directories_.clear();
        Buffer_.clear();
        Tuples_ = nullptr;
        Overflow_ = nullptr;
    }

  private:
    const TTupleLayout * Layout_ = nullptr;

    unsigned DirectoryHashBits_;
    unsigned DirectoryHashShift_;
    std::vector<TDirectory, TMKQLAllocator<TDirectory>> Directories_;

    std::vector<ui8, TMKQLAllocator<ui8>> Buffer_;
    
    bool IsInplace_;
    ui32 BufferSlotSize_;

    const ui8 *Tuples_ = nullptr;
    const ui8 *Overflow_ = nullptr;
};

} // namespace NPackedTuple
} // namespace NMiniKQL
} // namespace NKikimr

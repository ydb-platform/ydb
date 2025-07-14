#pragma once

#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/public/udf/udf_data_type.h>
#include <yql/essentials/public/udf/udf_types.h>
#include <yql/essentials/public/udf/udf_value.h>

#include <yql/essentials/utils/prefetch.h>

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

    static consteval size_t BinomCoeff(size_t n, size_t k) noexcept {
        return k == 0 ? 1 : BinomCoeff(n - 1, k - 1) * n / k;
    }

    static consteval size_t Pow2Round(size_t n) {
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
    static consteval auto Gen() {
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

template <bool ConsecutiveDuplicates = false, bool Prefetch = true,
          typename TAllocator = TMKQLAllocator<ui8>>
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
        T DirSlotHash : sizeof(T) * 8 - kBloomHashBits;
    };
    static_assert(sizeof(THash) == sizeof(typename THash::T));

    Hash getDirectorySlot(THash thash) const {
        return (*thash >> DirectoryHashShift_) & DirectoryHashMask_;
    }

    static constexpr ui32 kEmbeddedSize = 16;

    template <bool>
    struct TOutplaceImpl {
        Hash Hash;
        ui32 Index;
    };
    template <>
    struct TOutplaceImpl<true> : TOutplaceImpl<false> {
    };

    using TOutplace = TOutplaceImpl<ConsecutiveDuplicates>;
    static_assert(sizeof(TOutplace) <= kEmbeddedSize);


    template<bool> struct TIteratorImpl;
    template<> struct TIteratorImpl<false> {
        friend TNeumannHashTable;

      public:
        TIteratorImpl(): It_(nullptr), End_(nullptr) {}

      private:
        const ui8 *Row_;
        const ui8 *It_;
        const ui8 *End_;
    };
    template<> struct TIteratorImpl<true> {
        friend TNeumannHashTable;

      public:
        TIteratorImpl(): It_(nullptr), Len_(0) {}

      private:
        const ui8 *It_;
        ui32 Len_;
    };


  public:
    using TIterator = TIteratorImpl<ConsecutiveDuplicates>;
  
    TNeumannHashTable()
        : DirectoriesData_(Allocator_)
        , Buffer_(Allocator_) 
    {}

    explicit TNeumannHashTable(const TTupleLayout *layout): TNeumannHashTable() {
        SetTupleLayout(layout);
    }

    void SetTupleLayout(const TTupleLayout *layout) {
        Clear();

        Layout_ = layout;
        IsInplace_ = layout->TotalRowSize <= kEmbeddedSize;
        RowIndexSize_ = IsInplace_ ? layout->TotalRowSize : sizeof(TOutplace);
        BufferSlotSize_ = RowIndexSize_;
        if constexpr (ConsecutiveDuplicates) {
            BufferSlotSize_ += sizeof(ui32);
        }
    }

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
        estimatedLogSize = std::max(1u, std::min(24u, estimatedLogSize));

        Tuples_ = tuples;
        Overflow_ = overflow;

        DirectoryHashBits_ = estimatedLogSize;
        DirectoryHashShift_ = sizeof(Hash) * 8 - kBloomHashBits >= DirectoryHashBits_
                                ? kBloomHashBits
                                : sizeof(Hash) * 8 - DirectoryHashBits_;
        DirectoryHashMask_ = (1ul << DirectoryHashBits_) - 1;
        
        const ui32 dirsSize = (1ul << DirectoryHashBits_) + 1;
        DirectoriesData_.resize(dirsSize * sizeof(TDirectory));
        Directories_ = std::span((TDirectory *)DirectoriesData_.data(), dirsSize);
        for (auto& directory : Directories_) {
            directory = {};
        }

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

        Buffer_.resize(BufferSlotSize_ * nItems);
        for (ui32 ind = 0; ind != nItems; ++ind) {
            const ui8 *const row = tuples + Layout_->TotalRowSize * ind;
            const THash thash = ReadUnaligned<THash>(row);

            auto &dir = *Directories_[getDirectorySlot(thash)];
            dir -= 1ul << TDirectory::kBufferSlotShift;
            const ui32 dataSlot = dir >> TDirectory::kBufferSlotShift;
            ui8 *const bufRow = Buffer_.data() + BufferSlotSize_ * dataSlot;

            if (IsInplace_) {
                std::memcpy(bufRow, row,
                            RowIndexSize_);
            } else {
                auto *const outRow = reinterpret_cast<TOutplace *>(bufRow);
                outRow->Hash = *thash;
                outRow->Index = ind;
            }
            
            if constexpr (ConsecutiveDuplicates) {
                WriteUnaligned<ui32>(bufRow + RowIndexSize_, 0); 
            }
        }

        if constexpr (ConsecutiveDuplicates) {
            for (size_t dirSlot = 0; dirSlot != Directories_.size() - 1; ++dirSlot) {
                ui8 *begin = Buffer_.data() + BufferSlotSize_ * Directories_[dirSlot].BufferSlot;
                ui8 *const end = Buffer_.data() + BufferSlotSize_ * Directories_[dirSlot + 1].BufferSlot;
                const ui8* matchedRow;

                while (begin != end) {
                    const ui8 *const row = GetRow(begin);

                    ui8 *left = begin + BufferSlotSize_;
                    ui8 *right = end;
                    ui32 size = 1;

                    bool checkLeft = true;
                    while (left != right) {
                        while (left != right && checkLeft && GetRowMatch(left, row, overflow, matchedRow)) {
                            ++size;
                            left += BufferSlotSize_;
                        }
                        checkLeft = false;

                        if (left == right) {
                            break;
                        }
                        right -= BufferSlotSize_;

                        if (GetRowMatch(right, row, overflow, matchedRow)) {
                            ui8 buf[kEmbeddedSize];
                            std::memcpy(buf, left, RowIndexSize_);
                            std::memcpy(left, right, RowIndexSize_);
                            std::memcpy(right, buf, RowIndexSize_);

                            ++size;
                            left += BufferSlotSize_;
                            checkLeft = true;
                        }
                    }
    
                    WriteUnaligned<ui32>(begin + RowIndexSize_, size);
                    begin = left;
                }
            }
        }
    }

    const ui8 *NextMatch(TIterator &iter, const ui8 *overflow) const {
        const ui8 *result = nullptr;

        if constexpr (ConsecutiveDuplicates) {
            if (iter.Len_) { /// allow multiple false calls
                result = GetRow(iter.It_);
                iter.It_ += BufferSlotSize_;
                iter.Len_--;
            }
        } else {
            for (auto& it = iter.It_; it != iter.End_; it += BufferSlotSize_) { /// allow multiple false calls
                if (GetRowMatch(it, iter.Row_, overflow, result)) {
                    it += BufferSlotSize_;
                    break;
                }
                result = nullptr;
            }
        }

        return result;
    }

    TIterator Find(const ui8 *const row, const ui8 *const overflow) const {
        Y_ASSERT(Layout_ != nullptr);
        Y_ASSERT(!Directories_.empty() && Tuples_ != nullptr);

        TIterator iter;

        const THash &thash = reinterpret_cast<const THash &>(row[0]);
        const TBloom hashBloomTag = kBloomTags[thash.BloomTagSlot];

        const Hash dirSlot = getDirectorySlot(thash);
        const TDirectory dir = Directories_[dirSlot];
        const TBloom dirBloomFilter = dir.BloomFilter;

        if (hashBloomTag & dirBloomFilter) {
            return iter;
        }

        const ui8 *const begin =
            Buffer_.data() + BufferSlotSize_ * dir.BufferSlot;
        const ui8 *const end =
            Buffer_.data() +
            BufferSlotSize_ * Directories_[dirSlot + 1].BufferSlot;

        if constexpr (!ConsecutiveDuplicates) {
            iter.Row_ = row;
            iter.It_ = begin;
            iter.End_ = end;
        } else {
            for (auto it = begin; it != end; ) {
                const ui8 *matchedRow;
                auto size  = ReadUnaligned<ui32>(it + RowIndexSize_);

                if (GetRowMatch(it, row, overflow, matchedRow)) {
                    iter.It_ = it;
                    iter.Len_ = size;
                    break;
                } else {
                    it += size * BufferSlotSize_;
                }
            }
        }

        return iter;
    }

    template <size_t Size>
    std::array<TIterator, Size> FindBatch(const std::array<const ui8 *, Size> &rows,
                                          const ui8 *const overflow) const {
        if constexpr (Prefetch) {
            if (Directories_.size_bytes() > 1024 * 1024) {
                for (ui32 index = 0; index < Size && rows[index]; ++index) {
                    const THash &thash = reinterpret_cast<const THash &>(rows[index][0]);
                    const Hash dirSlot = getDirectorySlot(thash);
                    NYql::PrefetchForRead(&Directories_[dirSlot]);
                }    
            } else {
                for (ui32 index = 0; index < Size && rows[index]; ++index) {
                    const THash &thash = reinterpret_cast<const THash &>(rows[index][0]);
                    const Hash dirSlot = getDirectorySlot(thash);
                    const TDirectory dir = Directories_[dirSlot]; 
                    const ui8 *ptr = Buffer_.data() + BufferSlotSize_ * dir.BufferSlot;
                    NYql::PrefetchForRead(ptr);
                }
            }
        }

        std::array<TIterator, Size> iters;
        for (ui32 index = 0; index < Size && rows[index]; ++index) {
            iters[index] = Find(rows[index], overflow);
        }

        return iters;
    }

    void Apply(const ui8 *const row, const ui8 *const overflow,
               auto &&onMatch) {
        auto iter = Find(row, overflow);
        while (auto matched = NextMatch(iter, overflow)) {
            onMatch(matched);
        }
    }

    void Clear() {
        Directories_ = {};
        DirectoriesData_.clear();
        Buffer_.clear();

        Tuples_ = nullptr;
        Overflow_ = nullptr;
    }

  private:
    const ui8* GetRow(const ui8* it) const {
        if (IsInplace_) {
            return it;
        } else {
            const auto *const outRow =
                reinterpret_cast<const TOutplace *>(it);
            it = Tuples_ + Layout_->TotalRowSize * outRow->Index;
            return it;
        }
    } 

    bool GetRowMatch(const ui8 *const it, const ui8 *const row, const ui8 *const overflow, const ui8 *&matchedRow) const {
        const THash &thash = reinterpret_cast<const THash &>(row[0]);

        if (IsInplace_) {
            const Hash matchRowHash = ReadUnaligned<Hash>(it);
            if (matchRowHash != *thash) {
                return false;
            }
            matchedRow = it;
        } else {
            const auto *const outRow =
                reinterpret_cast<const TOutplace *>(it);
            if (outRow->Hash != *thash) {
                return false;
            }
            matchedRow = Tuples_ + Layout_->TotalRowSize * outRow->Index;
        }

        if (Layout_->KeysEqual(row, overflow, matchedRow, Overflow_)) {
            return true;
        }
        return false;
    }

  private:
    const TTupleLayout * Layout_ = nullptr;

    const ui8 *Tuples_ = nullptr;
    const ui8 *Overflow_ = nullptr;

    bool IsInplace_;
    ui32 BufferSlotSize_;
    ui32 RowIndexSize_;

    unsigned DirectoryHashBits_;
    unsigned DirectoryHashShift_;
    Hash DirectoryHashMask_;

    TAllocator Allocator_;
    std::span<TDirectory> Directories_;
    std::vector<ui8, TAllocator> DirectoriesData_;
    std::vector<ui8, TAllocator> Buffer_;
};

} // namespace NPackedTuple
} // namespace NMiniKQL
} // namespace NKikimr

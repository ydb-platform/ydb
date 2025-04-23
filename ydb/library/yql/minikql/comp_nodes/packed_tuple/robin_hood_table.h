#pragma once

#include <util/generic/bitops.h>
#include <util/generic/yexception.h>
#include <util/system/compiler.h>
#include <util/system/types.h>

#include <yql/essentials/minikql/mkql_rh_hash_utils.h>
#include <yql/essentials/utils/prefetch.h>

#include <ydb/library/yql/utils/simd/simd.h>

#include <util/digest/city.h>
#include <util/generic/scope.h>

#include "tuple.h"

namespace NKikimr {
namespace NMiniKQL {
namespace NPackedTuple {

struct TTupleEqual {
    bool operator()(const TTupleLayout *layout, const ui8 *lhsRow,
                    const ui8 *lhsOverflow, const ui8 *rhsRow,
                    const ui8 *rhsOverflow) const {
        return layout->KeysEqual(lhsRow, lhsOverflow, rhsRow, rhsOverflow);
    }
};

struct TTupleHash {
    ui32 operator()(const TTupleLayout * /*layout*/, const ui8 *tuple,
                    const ui8 * /*overflow*/) const {
        return ((const ui32 *)tuple)[0];
    }
};

template <bool ConsecutiveDuplicates = true, bool Prefetch = true,
          typename TAllocator = TMKQLAllocator<ui8>>
class TRobinHoodHashBase {
    using TEqual = TTupleEqual;
    using THash = TTupleHash;

    struct TCellStatus {
        /// TODO: default: ui32
        ///       ui16 for testing embedded case of map[ui32] -> ui32
        using T = ui16; 

        static constexpr T kInvalid = 0;
        static constexpr T kStart = 2;

        bool IsValid() const { return value; }
        bool IsList() const { return value & 1; }
        void ToList() { value = value | 1; }
        void operator++() { value += 2; }
        bool operator<(TCellStatus rhs) const { return value < rhs.value; }

      public:
      T value = kStart;
    };

    struct TPSLStorage {
        TCellStatus CellStatus;

        TPSLStorage() = default;
        TPSLStorage(ui32 /*hash*/) : CellStatus(TCellStatus::kStart) {}
    };

    struct TPSLOutStorage : TPSLStorage, TIndexedTuple {
        ui32 DupIndex;

        TPSLOutStorage() = default;
        TPSLOutStorage(ui32 hash, ui32 outIndex)
            : TPSLStorage(hash), TIndexedTuple{hash, outIndex} {}
    };

    static constexpr ui32 kEmbeddedSize = 16;
    static_assert(sizeof(TPSLOutStorage) <= kEmbeddedSize);

    static constexpr ui32 RoundUp(ui32 n, ui32 r) {
        return ((n + r - 1) / r) * r;
    }

  public:
    struct TIterator {
        friend TRobinHoodHashBase;

      public:
        TIterator(): Matched_(nullptr), Len_(0) {}

      private:
        const ui8 *Matched_;
        ui32 Len_;
    };

  public:
    TRobinHoodHashBase() : SelfHash_(GetSelfHash(this)), DupSeq_(Allocator_) {
        // Init(256);
    }

    explicit TRobinHoodHashBase(const TTupleLayout *layout)
        : TRobinHoodHashBase() {
        SetTupleLayout(layout);
    }

    ~TRobinHoodHashBase() {
        if (Data_) {
            Allocator_.deallocate(Data_, DataEnd_ - Data_);
        }
    }

    void SetTupleLayout(const TTupleLayout *layout) {
        Clear();

        Layout_ = layout;
        /// whole tuple and tuple key with index can be embedded
        IsInplace_ = sizeof(TPSLStorage) + layout->TotalRowSize <= kEmbeddedSize && 
                     sizeof(TPSLStorage) + layout->PayloadOffset + sizeof(ui32) <= kEmbeddedSize;
        // CellSize_ = IsInplace_
        //     ? RoundUp(sizeof(TPSLStorage) + std::max<ui32>(layout->TotalRowSize,
        //                                                    layout->PayloadOffset + sizeof(ui32)),
        //               sizeof(ui32))
        //     : sizeof(TPSLOutStorage);
        DupPayloadSize_ = IsInplace_ ? Layout_->TotalRowSize : sizeof(ui32);

        /// size may be stored in dup cell
        Y_ASSERT(DupPayloadSize_ >= sizeof(ui32));
    }

    TRobinHoodHashBase(const TRobinHoodHashBase &) = delete;
    TRobinHoodHashBase(TRobinHoodHashBase &&) = delete;
    void operator=(const TRobinHoodHashBase &) = delete;
    void operator=(TRobinHoodHashBase &&) = delete;

    Y_FORCE_INLINE const ui8 *NextMatch(TIterator &iter, const ui8 * /*overflow*/) const {
        const ui8 *result = nullptr;

        if constexpr (ConsecutiveDuplicates) {
            if (iter.Len_) { /// allow multiple false calls
                result = iter.Matched_;
                iter.Matched_ += DupPayloadSize_;
                iter.Len_--;
            }
        } else {
            if (iter.Matched_) { /// allow multiple false calls
                result = iter.Matched_;
                if (!iter.Len_) {
                    iter.Matched_ = nullptr;
                } else {
                    ui32 dupIndex = ReadUnaligned<ui32>(iter.Matched_ + DupPayloadSize_);
                    iter.Matched_ = dupIndex == -1u
                                    ? nullptr
                                    : &DupSeq_[dupIndex * (DupPayloadSize_ + sizeof(ui32))];
                }
            }
        }

        if (!IsInplace_ && result) {
            result = GetTupleOut(ReadUnaligned<ui32>(result));
        }

        return result;
    }

    Y_FORCE_INLINE TIterator Find(const ui8 *const tuple, const ui8 *const overflow) const {
        const ui32 hash = TupleHash_(Layout_, tuple, overflow);
        auto ptr = GetPtr(hash, Data_, CapacityShift_);
        return FindImpl(hash, ptr, tuple, overflow);
    }

    template <size_t Size>
    Y_FORCE_INLINE std::array<TIterator, Size>
    FindBatch(const std::array<const ui8 *, Size> &tuples,
              const ui8 *const overflow) const {
        if constexpr (Prefetch) {
            for (ui32 index = 0; index < Size && tuples[index]; ++index) {
                const ui32 hash = TupleHash_(Layout_, tuples[index], overflow);
                auto ptr = GetPtr(hash, Data_, CapacityShift_);
                NYql::PrefetchForRead(ptr);
            }
        }

        std::array<TIterator, Size> iters;
        for (ui32 index = 0; index < Size && tuples[index]; ++index) {
            iters[index] = Find(tuples[index], overflow);
        }

        return iters;
    }

    Y_FORCE_INLINE void Apply(const ui8 *const tuple, const ui8 *const overflow,
                              auto &&onMatch) {
        auto iter = Find(tuple, overflow);
        while (auto matched = NextMatch(iter, overflow)) {
            onMatch(matched);
        }
    }

    void Build(const ui8 *const tuples, const ui8 *const overflow,
               ui32 nItems) {
        Y_ASSERT(Size_ == 0 && Listed_ == 0 && ListedUnique_ == 0);

        Tuples_ = tuples;
        Overflow_ = overflow;

        /// heuristic to decrease number of growths
        constexpr ui32 preallocFactor = 16;
        if (RHHashTableNeedsGrow(nItems / preallocFactor, Capacity_)) {
            Grow(nItems / preallocFactor);
        }

        auto duplicates = TVector<ui8, TAllocator>(Allocator_);

        constexpr ui32 prefetchInAdvance = 16;

        if constexpr (Prefetch) {
            for (ui32 index = 0; index < std::min(prefetchInAdvance, nItems); ++index) {
                const ui8 *const tuple = Tuples_ + index * Layout_->TotalRowSize;
                const auto hash = TupleHash_(Layout_, tuple, Overflow_);
                const auto ptr = GetPtr(hash, Data_, CapacityShift_);
                NYql::PrefetchForWrite(ptr);
            }
        }

        for (ui32 index = 0; index < nItems; ++index) {
            if (RHHashTableNeedsGrow(Size_, Capacity_)) {
                Grow(Size_);
            }

            if constexpr (Prefetch) {
                if (index + prefetchInAdvance < nItems) {
                    const ui8 *const tuple = Tuples_ + (index + prefetchInAdvance) * Layout_->TotalRowSize;
                    const auto hash = TupleHash_(Layout_, tuple, Overflow_);
                    const auto ptr = GetPtr(hash, Data_, CapacityShift_);
                    NYql::PrefetchForWrite(ptr);
                }
            }    

            const ui8 *const tuple = Tuples_ + index * Layout_->TotalRowSize;
            const auto hash = TupleHash_(Layout_, tuple, Overflow_);
            const auto ptr = GetPtr(hash, Data_, CapacityShift_);

            ui8 cell[kEmbeddedSize];
            if (IsInplace_) {
                new (cell) TPSLStorage(hash);
                std::memcpy(GetTuple(cell), tuple, Layout_->TotalRowSize);
            } else {
                new (cell) TPSLOutStorage(hash, index);
            }

            Size_ += InsertImpl(cell, hash, ptr, Data_, DataEnd_, duplicates,
                               EqualInsertHandler);
        }

        if (Listed_ == 0) {
            return;
        }

        if constexpr (!ConsecutiveDuplicates) {
            DupSeq_ = std::move(duplicates);
            return;
        }

        DupSeq_.resize((Listed_ + ListedUnique_) * DupPayloadSize_);
        ui32 dupSeqIndex = 0;

        for (auto ptr = Data_; ptr != DataEnd_; ptr += CellSize_) {
            auto &ptrPsl = GetPSL(ptr);
            if (!ptrPsl.CellStatus.IsValid() || !ptrPsl.CellStatus.IsList()) {
                continue;
            }
            
            ui32 &ptrDupIndex = IsInplace_ ? GetDupIndex(ptr) : GetPSLOut(ptr).DupIndex;
            ui32 dupIndex = ptrDupIndex; 
            ptrDupIndex = dupSeqIndex;

            ui32 &ptrSize = *(ui32 *)(DupSeq_.data() + dupSeqIndex * DupPayloadSize_);
            ptrSize = 0;
            ui8 *ptrDupSeq = DupSeq_.data() + (dupSeqIndex + 1) * DupPayloadSize_;
            ++dupSeqIndex;

            while (dupIndex != -1u) {
                const auto *const ptrDup =
                    &duplicates[dupIndex * (DupPayloadSize_ + sizeof(ui32))];
                std::memcpy(ptrDupSeq, ptrDup, DupPayloadSize_);
                
                ptrDupSeq += DupPayloadSize_;
                ++ptrSize;
                dupIndex = ReadUnaligned<ui32>(ptrDup + DupPayloadSize_);
                ++dupSeqIndex;
            }
        }
    }

    void Clear() {
        if (Size_ != 0) {
            ui8 *ptr = Data_;
            for (ui64 i = 0; i < Capacity_; ++i, ptr += CellSize_) {
                GetPSL(ptr).CellStatus.value = TCellStatus::kInvalid;
            }
        }

        Size_ = 0;
        Listed_ = 0;
        ListedUnique_ = 0;

        Tuples_ = nullptr;
        Overflow_ = nullptr;
    }

  private:
    ui8 *GetPtr(const ui64 hash, ui8 *data, ui64 capacityShift) const {
        // https://probablydance.com/2018/06/16/fibonacci-hashing-the-optimization-that-the-world-forgot-or-a-better-alternative-to-integer-modulo/
        ui64 bucket = ((SelfHash_ ^ hash) * 11400714819323198485llu) >> capacityShift;
        ui8 *ptr = data + CellSize_ * bucket;
        return ptr;
    }

    static ui8 *GetTuple(ui8 *ptr) { return ptr + sizeof(TPSLStorage); }

    const ui8 *GetTupleOut(ui32 index) const {
        return Tuples_ + index * Layout_->TotalRowSize;
    }

    static TPSLStorage &GetPSL(ui8 *ptr) { return *(TPSLStorage *)ptr; }

    static TPSLOutStorage &GetPSLOut(ui8 *ptr) {
        return *(TPSLOutStorage *)ptr;
    }

    ui32 &GetDupIndex(ui8 *ptr) const {
        return *(ui32 *)(ptr + CellSize_ - sizeof(ui32));
    }

    void CopyCell(ui8 *dst, const ui8 *src) {
        std::memcpy(dst, src, CellSize_);
    }

    void SwapCells(ui8 *lhs, ui8 *rhs) {
        ui8 buf[kEmbeddedSize];
        std::memcpy(buf, lhs, CellSize_);
        std::memcpy(lhs, rhs, CellSize_);
        std::memcpy(rhs, buf, CellSize_);
    }

    Y_FORCE_INLINE TIterator FindImpl(ui32 hash, ui8* ptr, const ui8 *const tuple, const ui8 *const overflow) const {
        TCellStatus currCellStatus;
        TIterator iter;

        for (;;) {
            auto &ptrPsl = GetPSL(ptr);
            if (ptrPsl.CellStatus < currCellStatus) {
                return iter;
            }

            auto onCell = [&](const ui8 *matched) { 
                iter.Matched_ = matched;
                if constexpr (ConsecutiveDuplicates) {
                    iter.Len_ = 1;
                }
            };
            auto onList = [&](const ui8 *matched) {
                if constexpr (ConsecutiveDuplicates) {
                    iter.Len_ = ReadUnaligned<ui32>(matched);
                    iter.Matched_ = matched + DupPayloadSize_;
                    if constexpr (Prefetch) {
                        NYql::PrefetchForRead(iter.Matched_);
                    }
                } else {
                    iter.Len_ = true; /// used as flag
                    iter.Matched_ = matched;
                }
            };

            if (OnEqual<!ConsecutiveDuplicates>(
                    hash, ptr, DupSeq_, tuple, overflow,
                    onCell, onList, onCell, onList)) {
                return iter;
            }

            ++currCellStatus;
            AdvancePointer(ptr, Data_, DataEnd_);
        }
    }

    template <bool Offseted>
    Y_FORCE_INLINE bool
    OnEqual(ui32 hash, ui8 *ptr, const TVector<ui8, TAllocator> &duplicates,
            const ui8 *tuple, const ui8 *overflow, auto FInplaceCell,
            auto FInplaceList, auto FOutplaceCell, auto FOutplaceList) const {
        static constexpr ui32 dupOffset = Offseted ? sizeof(ui32) : 0;

        auto &ptrPsl = GetPSL(ptr);

        if (IsInplace_) {
            auto *const ptrTuple = GetTuple(ptr);

            if (TuplesEqual_(Layout_, ptrTuple, Overflow_, tuple, overflow)) {
                if (!ptrPsl.CellStatus.IsList()) {
                    FInplaceCell(ptrTuple);
                } else {
                    const ui32 dupIndex = GetDupIndex(ptr);
                    auto *const ptrDup =
                        &duplicates[dupIndex * (DupPayloadSize_ + dupOffset)];
                    FInplaceList(ptrDup);
                }

                return true;
            }
        } else {
            auto &ptrPsl = GetPSLOut(ptr);
            auto *const ptrTuple = GetTupleOut(ptrPsl.Index);

            if (ptrPsl.Hash != hash) {
                return false;
            }

            if (TuplesEqual_(Layout_, ptrTuple, Overflow_, tuple, overflow)) {
                if (!ptrPsl.CellStatus.IsList()) {
                    FOutplaceCell((ui8 *)&ptrPsl.Index);
                } else {
                    auto *const ptrDup =
                        &duplicates[ptrPsl.DupIndex * (DupPayloadSize_ + dupOffset)];
                    FOutplaceList(ptrDup);
                }

                return true;
            }
        }

        return false;
    }

    Y_FORCE_INLINE bool InsertImpl(ui8 *const newRow, const ui32 hash, ui8 *ptr,
                                   ui8 *const data, ui8 *const dataEnd,
                                   TVector<ui8, TAllocator> &duplicates,
                                   auto EqualHandler) {
        auto &psl = GetPSL(newRow);

        for (;;) {
            auto &ptrPsl = GetPSL(ptr);
            if (!ptrPsl.CellStatus.IsValid()) {
                CopyCell(ptr, newRow);
                return true;
            }

            if (EqualHandler(*this, newRow, hash, ptr, duplicates)) {
                return false;
            }

            if (ptrPsl.CellStatus < psl.CellStatus) {
                SwapCells(ptr, newRow);
                ++psl.CellStatus;
                AdvancePointer(ptr, data, dataEnd);
                break;
            }

            ++psl.CellStatus;
            AdvancePointer(ptr, data, dataEnd);
        }

        for (;;) {
            auto &ptrPsl = GetPSL(ptr);
            if (!ptrPsl.CellStatus.IsValid()) {
                CopyCell(ptr, newRow);
                return true;
            }

            if (ptrPsl.CellStatus < psl.CellStatus) {
                SwapCells(ptr, newRow);
            }

            ++psl.CellStatus;
            AdvancePointer(ptr, data, dataEnd);
        }

        return true;
    }

    Y_FORCE_INLINE static bool
    EqualInsertHandler(TRobinHoodHashBase &self, ui8 *const newRow,
                       const ui32 hash, ui8 *ptr,
                       TVector<ui8, TAllocator> &duplicates) {
        const ui8 *const tuple =
            self.IsInplace_ ? GetTuple(newRow)
                            : self.GetTupleOut(GetPSLOut(newRow).Index);

        return self.OnEqual<true>(
            hash, ptr, duplicates, tuple, self.Overflow_,
            [&](const ui8 * /*matched*/) {
                ui8 *ptrDup = self.GetDuplicatesData(duplicates, self.Listed_);
                std::memcpy(ptrDup, GetTuple(ptr), self.DupPayloadSize_);
                WriteUnaligned<ui32>(ptrDup + self.DupPayloadSize_, -1u);

                ++self.Listed_;

                ptrDup = self.GetDuplicatesData(duplicates, self.Listed_);
                std::memcpy(ptrDup, tuple, self.DupPayloadSize_);
                WriteUnaligned<ui32>(ptrDup + self.DupPayloadSize_, self.Listed_ - 1);

                auto &ptrPsl = GetPSL(ptr);
                self.GetDupIndex(ptr) = self.Listed_;
                ptrPsl.CellStatus.ToList();

                ++self.Listed_;
                ++self.ListedUnique_;
            },
            [&](const ui8 * /*matched*/) {
                auto &dupIndex = self.GetDupIndex(ptr);

                ui8 *ptrDup = self.GetDuplicatesData(duplicates, self.Listed_);
                std::memcpy(ptrDup, tuple, self.DupPayloadSize_);
                WriteUnaligned<ui32>(ptrDup + self.DupPayloadSize_, dupIndex);

                dupIndex = self.Listed_;

                ++self.Listed_;
            },
            [&](const ui8 * /*matched*/) {
                auto &ptrPsl = GetPSLOut(ptr);

                ui8 *ptrDup = self.GetDuplicatesData(duplicates, self.Listed_);
                WriteUnaligned<ui32>(ptrDup, ptrPsl.Index);
                WriteUnaligned<ui32>(ptrDup + self.DupPayloadSize_, -1u);

                ++self.Listed_;

                ptrDup = self.GetDuplicatesData(duplicates, self.Listed_);
                WriteUnaligned<ui32>(ptrDup, GetPSLOut(newRow).Index);
                WriteUnaligned<ui32>(ptrDup + self.DupPayloadSize_, self.Listed_ - 1);

                ptrPsl.DupIndex = self.Listed_;
                ptrPsl.CellStatus.ToList();

                ++self.Listed_;
                ++self.ListedUnique_;
            },
            [&](const ui8 * /*matched*/) {
                auto &ptrPsl = GetPSLOut(ptr);

                ui8 *ptrDup = self.GetDuplicatesData(duplicates, self.Listed_);
                WriteUnaligned<ui32>(ptrDup, GetPSLOut(newRow).Index);
                WriteUnaligned<ui32>(ptrDup + self.DupPayloadSize_, ptrPsl.DupIndex);

                ptrPsl.DupIndex = self.Listed_;

                ++self.Listed_;
            });
    }

    Y_FORCE_INLINE ui8 *GetDuplicatesData(TVector<ui8, TAllocator> &duplicates, ui32 ind) {
        const ui32 dupCellSize = DupPayloadSize_ + sizeof(ui32);
        if (duplicates.size() <= ind * dupCellSize) {
            duplicates.resize_uninitialized(
                std::max(64ul * dupCellSize, duplicates.size() * 2));
        }

        return duplicates.data() + ind * dupCellSize;
    }

    Y_FORCE_INLINE void AdvancePointer(ui8 *&ptr, ui8 *begin, ui8 *end) const {
        ptr += CellSize_;
        ptr = (ptr == end) ? begin : ptr;
    }

    static ui64 GetSelfHash(void *self) {
        char buf[sizeof(void *)];
        *(void **)buf = self;
        return CityHash64(buf, sizeof(buf));
    }

    void Init(ui64 capacity) {
        Y_ASSERT((capacity & (capacity - 1)) == 0);
        Capacity_ = capacity;
        CapacityShift_ = 64 - MostSignificantBit(capacity);
        Allocate(Capacity_, Data_, DataEnd_);
    }

    void Grow(ui64 size) {
        auto newCapacity = CalculateRHHashTableCapacity(size);
        Y_ASSERT((newCapacity & (newCapacity - 1)) == 0);
        auto newCapacityShift = 64 - MostSignificantBit(newCapacity);
        ui8 *newData, *newDataEnd;
        Allocate(newCapacity, newData, newDataEnd);

        for (auto cell = Data_; cell != DataEnd_; cell += CellSize_) {
            auto &psl = GetPSL(cell);
            if (!psl.CellStatus.IsValid()) {
                continue;
            }

            const ui32 hash =
                IsInplace_ ? TupleHash_(Layout_, GetTuple(cell), Overflow_)
                           : GetPSLOut(cell).Hash;
            const auto ptr = GetPtr(hash, newData, newCapacityShift);

            if (psl.CellStatus.IsList()) {
                psl.CellStatus.value = TCellStatus::kStart;
                psl.CellStatus.ToList();
            } else {
                psl.CellStatus.value = TCellStatus::kStart;
            }

            InsertImpl(cell, hash, ptr, newData, newDataEnd, DupSeq_,
                       [](auto &&...) { return false; });
        }

        Capacity_ = newCapacity;
        CapacityShift_ = newCapacityShift;
        std::swap(Data_, newData);
        std::swap(DataEnd_, newDataEnd);
        if (newData) {
            Allocator_.deallocate(newData, newDataEnd - newData);
        }
    }

    void Allocate(ui64 capacity, ui8 *&data, ui8 *&dataEnd) {
        ui64 bytes = capacity * CellSize_;
        data = Allocator_.allocate(bytes);
        dataEnd = data + bytes;
        ui8 *ptr = data;
        for (ui64 i = 0; i < capacity; ++i) {
            GetPSL(ptr).CellStatus.value = TCellStatus::kInvalid;
            ptr += CellSize_;
        }
    }

  private:
    static constexpr ui32 CellSize_ = kEmbeddedSize;

    const ui64 SelfHash_;

    THash TupleHash_;
    TEqual TuplesEqual_;

    const TTupleLayout * Layout_ = nullptr;

    const ui8 *Tuples_ = nullptr;
    const ui8 *Overflow_ = nullptr;

    bool IsInplace_;
    ui32 DupPayloadSize_;

    ui64 Size_ = 0;
    ui64 Listed_ = 0;
    ui64 ListedUnique_ = 0;
    ui64 Capacity_ = 0;
    ui64 CapacityShift_;

    TAllocator Allocator_;
    ui8 *Data_ = nullptr;
    ui8 *DataEnd_ = nullptr;
    TVector<ui8, TAllocator> DupSeq_;
};

} // namespace NPackedTuple
} // namespace NMiniKQL
} // namespace NKikimr

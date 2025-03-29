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
                    const ui8 *rhsOverflow) {
        return layout->KeysEqual(lhsRow, lhsOverflow, rhsRow, rhsOverflow);
    }
};

struct TTupleHash {
    ui32 operator()(const TTupleLayout * /*layout*/, const ui8 *tuple,
                    const ui8 * /*overflow*/) {
        return ((const ui32 *)tuple)[0];
    }
};

constexpr ui32 PrefetchBatchSize = 64; /// TODO

template <bool ConsecutiveDuplicates = true,
          typename TEqual = TTupleEqual,
          typename THash = TTupleHash,
          typename TAllocator = TMKQLAllocator<ui8>>
class TRobinHoodHashBase {
    struct TCellStatus {
        static constexpr ui32 kInvalid = 0;
        static constexpr ui32 kStart = 2;

        bool IsValid() const { return value; }
        bool IsList() const { return value & 1; }
        void ToList() { value = value | 1; }
        void operator++() { value += 2; }
        bool operator<(TCellStatus rhs) const { return value < rhs.value; }

      public:
        ui32 value = kStart;
    };

    struct TListCell {
        ui32 Index;
        ui32 Size_;
    };

    struct TPSLStorage {
        TCellStatus CellStatus;

        TPSLStorage() = default;
        TPSLStorage(const ui32 /*hash*/) : CellStatus(TCellStatus::kStart) {}
    };

    struct TPSLOutStorage : TPSLStorage {
        ui32 Hash;
        TListCell ListCell;

        TPSLOutStorage() = default;
        TPSLOutStorage(ui32 outIndex, const ui32 hash)
            : TPSLStorage(hash), Hash(hash), ListCell{outIndex, 1} {}
    };

    static constexpr ui32 kEmbeddedSize = 16;
    static_assert(sizeof(TPSLOutStorage) <= kEmbeddedSize);

  public:
    TRobinHoodHashBase() : SelfHash_(GetSelfHash(this)) {
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
        IsInplace_ = sizeof(TPSLStorage) + layout->TotalRowSize <= kEmbeddedSize;
        DupPayloadSize_ = IsInplace_ ? Layout_->TotalRowSize : sizeof(ui32);
    }

    TRobinHoodHashBase(const TRobinHoodHashBase &) = delete;
    TRobinHoodHashBase(TRobinHoodHashBase &&) = delete;
    void operator=(const TRobinHoodHashBase &) = delete;
    void operator=(TRobinHoodHashBase &&) = delete;

    Y_FORCE_INLINE void Apply(const ui8 *const tuple, const ui8 *const overflow,
                              auto &&onMatch) {
        const ui32 hash = HashLocal_(Layout_, tuple, overflow);
        auto ptr = MakeIterator(hash, Data_, CapacityShift_);

        TCellStatus currCellStatus;
        for (;;) {
            auto &ptrPsl = GetPSL(ptr);
            if (ptrPsl.CellStatus < currCellStatus) {
                return;
            }

            if (OnEqual<!ConsecutiveDuplicates>(
                    hash, ptr, DupSeq_, tuple, overflow,
                    [&](const ui8 *matched) { onMatch(matched); },
                    [&](const ui8 *matched) {
                        auto &ptrPsl = GetPSLOut(ptr);
                        for (ui32 i = 0; i != ptrPsl.ListCell.Size_; ++i) {
                            onMatch(matched);
                            if constexpr (ConsecutiveDuplicates) {
                                matched += DupPayloadSize_;
                            } else {
                                ui32 dupCellIndex = ReadUnaligned<ui32>(matched + DupPayloadSize_);
                                matched = &DupSeq_[dupCellIndex *
                                    (DupPayloadSize_ + sizeof(ui32))];
                            }
                        }
                    },
                    [&](const ui8 *matched) { onMatch(matched); },
                    [&](const ui8 *matched) {
                        auto &ptrPsl = GetPSLOut(ptr);
                        for (ui32 i = 0; i != ptrPsl.ListCell.Size_; ++i) {
                            onMatch(GetTupleOut(ReadUnaligned<ui32>(matched)));
                            if constexpr (ConsecutiveDuplicates) {
                                matched += DupPayloadSize_;
                            } else {
                                ui32 dupCellIndex = ReadUnaligned<ui32>(matched + DupPayloadSize_);
                                matched = &DupSeq_[dupCellIndex *
                                    (DupPayloadSize_ + sizeof(ui32))];
                            }
                        }
                    })) {
                return;
            }

            ++currCellStatus;
            AdvancePointer(ptr, Data_, DataEnd_);
        }
    }

    void Build(const ui8 *const tuples, const ui8 *const overflow,
               ui32 nItems) {
        Y_ASSERT(Size_ == 0);
        Y_ASSERT(Listed_ == 0);

        Tuples_ = tuples;
        Overflow_ = overflow;

        /// heuristic to decrease number of growths
        if (RHHashTableNeedsGrow(nItems / 16, Capacity_)) {
            Grow(nItems / 16);
        }

        auto duplicates = TVector<ui8, TAllocator>(Allocator_);

        for (ui32 index = 0; index < nItems; ++index) {
            if (RHHashTableNeedsGrow(Size_, Capacity_)) {
                Grow(Size_);
            }

            const ui8 *const tuple = tuples + index * Layout_->TotalRowSize;
            const auto hash = HashLocal_(Layout_, tuple, overflow);
            const auto ptr = MakeIterator(hash, Data_, CapacityShift_);

            ui8 cell[kEmbeddedSize];
            if (IsInplace_) {
                new (cell) TPSLStorage(hash);
                std::memcpy(GetTuple(cell), tuple, Layout_->TotalRowSize);
            } else {
                new (cell) TPSLOutStorage(index, hash);
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

        DupSeq_.resize(Listed_ * DupPayloadSize_);
        ui32 dupSeqIndex = 0;

        for (auto ptr = Data_; ptr != DataEnd_; ptr += CellSize_) {
            auto &ptrPsl = GetPSL(ptr);
            if (!ptrPsl.CellStatus.IsValid() || !ptrPsl.CellStatus.IsList()) {
                continue;
            }

            TListCell &ptrListCell = GetPSLOut(ptr).ListCell;
            ui32 dupCellIndex = ptrListCell.Index;
            ptrListCell.Index = dupSeqIndex;
            ui8 *ptrDupSeq = DupSeq_.data() + dupSeqIndex * DupPayloadSize_;

            while (dupCellIndex != -1u) {
                const auto *const ptrDup =
                    &duplicates[dupCellIndex *
                                       (DupPayloadSize_ + sizeof(ui32))];
                std::memcpy(ptrDupSeq, ptrDup, DupPayloadSize_);
                ptrDupSeq += DupPayloadSize_;
                dupCellIndex = ReadUnaligned<ui32>(ptrDup + DupPayloadSize_);
            }

            dupSeqIndex += ptrListCell.Size_;
        }
    }

    void Clear() {
        if (Size_ == 0) {
            return;
        }

        ui8 *ptr = Data_;
        for (ui64 i = 0; i < Capacity_; ++i, ptr += CellSize_) {
            GetPSL(ptr).CellStatus.value = TCellStatus::kInvalid;
        }
        Size_ = 0;
        Listed_ = 0;
    }

  private:
    static ui8 *GetTuple(ui8 *ptr) { return ptr + sizeof(TPSLStorage); }

    const ui8 *GetTupleOut(ui32 index) const {
        return Tuples_ + index * Layout_->TotalRowSize;
    }

    static TPSLStorage &GetPSL(ui8 *ptr) { return *(TPSLStorage *)ptr; }

    static TPSLOutStorage &GetPSLOut(ui8 *ptr) {
        return *(TPSLOutStorage *)ptr;
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

    template <bool Build>
    Y_FORCE_INLINE bool
    OnEqual(ui32 hash, ui8 *ptr, const TVector<ui8, TAllocator> &duplicates,
            const ui8 *tuple, const ui8 *overflow, auto FInplaceCell,
            auto FInplaceList, auto FOutplaceCell, auto FOutplaceList) {
        static constexpr ui32 buildOffset = Build ? sizeof(ui32) : 0;

        auto &ptrPsl = GetPSL(ptr);

        if (IsInplace_) {
            if (!ptrPsl.CellStatus.IsList()) {
                auto *const ptrTuple = GetTuple(ptr);
                if (EqualLocal_(Layout_, ptrTuple, Overflow_, tuple, overflow)) {
                    FInplaceCell(ptrTuple);
                    return true;
                }
            } else {
                auto &ptrPsl = GetPSLOut(ptr);
                auto *const ptrDup =
                    &duplicates[ptrPsl.ListCell.Index *
                                       (DupPayloadSize_ + buildOffset)];
                if (EqualLocal_(Layout_, ptrDup, Overflow_, tuple, overflow)) {
                    FInplaceList(ptrDup);
                    return true;
                }
            }
        } else {
            auto &ptrPsl = GetPSLOut(ptr);
            if (ptrPsl.Hash != hash) {
                return false;
            }

            if (!ptrPsl.CellStatus.IsList()) {
                auto *const ptrTuple = GetTupleOut(ptrPsl.ListCell.Index);
                if (EqualLocal_(Layout_, ptrTuple, Overflow_, tuple, overflow)) {
                    FOutplaceCell(ptrTuple);
                    return true;
                }
            } else {
                auto *const ptrDup =
                    &duplicates[ptrPsl.ListCell.Index *
                                       (DupPayloadSize_ + buildOffset)];
                auto *const ptrTuple = GetTupleOut(ReadUnaligned<ui32>(ptrDup));
                if (EqualLocal_(Layout_, ptrTuple, Overflow_, tuple, overflow)) {
                    FOutplaceList(ptrDup);
                    return true;
                }
            }
        }

        return false;
    }

    Y_FORCE_INLINE ui8 *MakeIterator(const ui64 hash, ui8 *data,
                                     ui64 capacityShift) {
        // https://probablydance.com/2018/06/16/fibonacci-hashing-the-optimization-that-the-world-forgot-or-a-better-alternative-to-integer-modulo/
        ui64 bucket =
            ((SelfHash_ ^ hash) * 11400714819323198485llu) >> capacityShift;
        ui8 *ptr = data + CellSize_ * bucket;
        return ptr;
    }

    Y_FORCE_INLINE bool InsertImpl(ui8 *const cell, const ui32 hash, ui8 *ptr,
                                   ui8 *const data, ui8 *const dataEnd,
                                   TVector<ui8, TAllocator> &duplicates,
                                   auto EqualHandler) {
        auto &psl = GetPSL(cell);

        for (;;) {
            auto &ptrPsl = GetPSL(ptr);
            if (!ptrPsl.CellStatus.IsValid()) {
                CopyCell(ptr, cell);
                return true;
            }

            if (EqualHandler(*this, cell, hash, ptr, duplicates)) {
                return false;
            }

            if (ptrPsl.CellStatus < psl.CellStatus) {
                SwapCells(ptr, cell);
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
                CopyCell(ptr, cell);
                return true;
            }

            if (ptrPsl.CellStatus < psl.CellStatus) {
                SwapCells(ptr, cell);
            }

            ++psl.CellStatus;
            AdvancePointer(ptr, data, dataEnd);
        }

        return true;
    }

    Y_FORCE_INLINE static bool
    EqualInsertHandler(TRobinHoodHashBase &self, ui8 *const cell,
                       const ui32 hash, ui8 *ptr,
                       TVector<ui8, TAllocator> &duplicates) {
        const ui8 *const tuple =
            self.IsInplace_ ? GetTuple(cell)
                            : self.Tuples_ + GetPSLOut(cell).ListCell.Index *
                                                 self.Layout_->TotalRowSize;

        return self.OnEqual<true>(
            hash, ptr, duplicates, tuple, self.Overflow_,
            [&](const ui8 * /*matched*/) {
                auto &ptrPsl = GetPSLOut(ptr);

                ui8 *ptrDup = self.GetDuplicatesData(duplicates, self.Listed_);
                std::memcpy(ptrDup, GetTuple(ptr), self.DupPayloadSize_);
                WriteUnaligned<ui32>(ptrDup + self.DupPayloadSize_, -1u);

                ++self.Listed_;

                ptrDup = self.GetDuplicatesData(duplicates, self.Listed_);
                std::memcpy(ptrDup, tuple, self.DupPayloadSize_);
                WriteUnaligned<ui32>(ptrDup + self.DupPayloadSize_,
                                     self.Listed_ - 1);

                ptrPsl.Hash = hash;
                ptrPsl.ListCell.Index = self.Listed_;
                ptrPsl.ListCell.Size_ = 2;
                ptrPsl.CellStatus.ToList();
                ++self.Listed_;
            },
            [&](const ui8 * /*matched*/) {
                auto &ptrPsl = GetPSLOut(ptr);

                ui8 *ptrDup = self.GetDuplicatesData(duplicates, self.Listed_);
                std::memcpy(ptrDup, tuple, self.DupPayloadSize_);
                WriteUnaligned<ui32>(ptrDup + self.DupPayloadSize_,
                                     ptrPsl.ListCell.Index);

                ptrPsl.ListCell.Index = self.Listed_;
                ptrPsl.ListCell.Size_++;
                ++self.Listed_;
            },
            [&](const ui8 * /*matched*/) {
                auto &ptrPsl = GetPSLOut(ptr);

                ui8 *ptrDup = self.GetDuplicatesData(duplicates, self.Listed_);
                WriteUnaligned<ui32>(ptrDup, ptrPsl.ListCell.Index);
                WriteUnaligned<ui32>(ptrDup + self.DupPayloadSize_, -1u);

                ++self.Listed_;

                ptrDup = self.GetDuplicatesData(duplicates, self.Listed_);
                WriteUnaligned<ui32>(ptrDup, GetPSLOut(cell).ListCell.Index);
                WriteUnaligned<ui32>(ptrDup + self.DupPayloadSize_,
                                     self.Listed_ - 1);

                ptrPsl.ListCell.Index = self.Listed_;
                ptrPsl.ListCell.Size_ = 2;
                ptrPsl.CellStatus.ToList();
                ++self.Listed_;
            },
            [&](const ui8 * /*matched*/) {
                auto &ptrPsl = GetPSLOut(ptr);

                ui8 *ptrDup = self.GetDuplicatesData(duplicates, self.Listed_);
                WriteUnaligned<ui32>(ptrDup, GetPSLOut(cell).ListCell.Index);
                WriteUnaligned<ui32>(ptrDup + self.DupPayloadSize_,
                                     ptrPsl.ListCell.Index);

                ptrPsl.ListCell.Index = self.Listed_;
                ptrPsl.ListCell.Size_++;
                ++self.Listed_;
            });
    }

    ui8 *GetDuplicatesData(TVector<ui8, TAllocator> &duplicates, ui32 ind) {
        const ui32 dupCellSize = DupPayloadSize_ + sizeof(ui32);
        if (duplicates.size() <= ind * dupCellSize) {
            duplicates.resize_uninitialized(
                std::max(64ul * dupCellSize, duplicates.size() * 2));
        }

        return duplicates.data() + ind * dupCellSize;
    }

    void AdvancePointer(ui8 *&ptr, ui8 *begin, ui8 *end) const {
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

    Y_NO_INLINE void Grow(ui64 size) {
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
                IsInplace_ && !psl.CellStatus.IsList()
                    ? HashLocal_(Layout_, GetTuple(cell), Overflow_)
                    : GetPSLOut(cell).Hash;
            const auto ptr = MakeIterator(hash, newData, newCapacityShift);

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

    THash HashLocal_;
    TEqual EqualLocal_;

    const TTupleLayout * Layout_ = nullptr;

    const ui8 *Tuples_ = nullptr;
    const ui8 *Overflow_ = nullptr;

    bool IsInplace_;
    ui32 DupPayloadSize_;

    ui64 Size_ = 0;
    ui64 Listed_ = 0;
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

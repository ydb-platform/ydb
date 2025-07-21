
#pragma once

#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/public/udf/udf_data_type.h>
#include <yql/essentials/public/udf/udf_types.h>

#include <util/generic/buffer.h>

#include <util/system/cpu_id.h>
#include <ydb/library/yql/utils/simd/simd.h>

namespace NKikimr {
namespace NMiniKQL {

template <typename TData>
struct TCastPtrTransform {
    using T = TData;
    static T *ToPtr(ui8 *ptr) { return reinterpret_cast<T *>(ptr); }
    static T &ToRef(ui8 *ptr) { return *reinterpret_cast<T *>(ptr); }
};

template <typename TData, typename TPtrTransform = TCastPtrTransform<TData>>
class TPaddedPtr
    : public std::iterator<std::random_access_iterator_tag,
                           typename TPtrTransform::T, int64_t,
                           decltype(TPtrTransform::ToPtr(nullptr)),
                           decltype(TPtrTransform::ToRef(nullptr))> {
    using T = std::iterator_traits<TPaddedPtr>::value_type;
    using TPtr = std::iterator_traits<TPaddedPtr>::pointer;
    using TRef = std::iterator_traits<TPaddedPtr>::reference;

  public:
    TPaddedPtr(TData *ptr)
        : Ptr_(reinterpret_cast<ui8 *>(ptr)), Step_(sizeof(TData)) {}

    TPaddedPtr(TData *ptr, ui32 step)
        : Ptr_(reinterpret_cast<ui8 *>(ptr)), Step_(step) {}

    TPaddedPtr(ui8 *ptr, ui32 step)
        requires(!std::same_as<ui8, TData>)
        : Ptr_(reinterpret_cast<ui8 *>(ptr)), Step_(step) {}

    ui32 Step() const {
        return Step_;
    }

    TRef operator[](int64_t ind) const {
        return TPtrTransform::ToRef(Ptr_ + Step_ * ind);
    }

    TPtr operator->() const {
        return TPtrTransform::ToPtr(Ptr_);
    }

    TRef operator*() const {
        return TPtrTransform::ToRef(Ptr_);
    }
        
    TPaddedPtr& operator+=(int64_t n) {
        Ptr_ += n * Step_;
        return *this;
    }
    TPaddedPtr& operator++() {
        return (*this) += 1;
    }
    TPaddedPtr& operator-=(int64_t n) {
        return (*this) += -n;
    }
    TPaddedPtr& operator--() {
        return (*this) -= 1;
    }

    TPaddedPtr operator+(int64_t n) const {
        auto ptr = *this;
        ptr += n;
        return ptr;
    }
    TPaddedPtr operator-(int64_t n) const {
        return (*this) + (-n);
    }

    int64_t operator-(const TPaddedPtr& rhs) const {
        return (Ptr_ - rhs.Ptr_) / Step_;
    }

    bool operator==(const TPaddedPtr& rhs) const {
        return Ptr_ == rhs.Ptr_;
    }
    bool operator!=(const TPaddedPtr& rhs) const {
        return Ptr_ != rhs.Ptr_;
    }

    bool operator<(const TPaddedPtr& rhs) const {
        return Ptr_ < rhs.Ptr_;
    }
    bool operator<=(const TPaddedPtr& rhs) const {
        return Ptr_ <= rhs.Ptr_;
    }
    bool operator>(const TPaddedPtr& rhs) const {
        return Ptr_ > rhs.Ptr_;
    }
    bool operator>=(const TPaddedPtr& rhs) const {
        return Ptr_ >= rhs.Ptr_;
    }

  private:
    ui8* Ptr_;
    i64 Step_;
};

namespace NPackedTuple {

// Defines if data type of particular column variable or fixed
enum class EColumnSizeType { Fixed, Variable };

// Defines if particular column is key column or payload column
enum class EColumnRole { Key, Payload };

// Describes layout and size of particular column
struct TColumnDesc {
    ui32 ColumnIndex = 0;   // Index of the column in particular layout
    ui32 OriginalColumnIndex = 0; // Index of the column in input representation
    ui32 OriginalIndex = 0; // Index of the buffer in input representation
    EColumnRole Role = EColumnRole::Payload; // Role of the particular column in
                                             // tuple (Key or Payload)
    EColumnSizeType SizeType =
        EColumnSizeType::Fixed; // Fixed size or variable size column
    ui32 DataSize = 0; // Size of the column in bytes for fixed size part
                       // Must be same for matching key columns
    ui32 Offset =
        0; // Offset in bytes for column value from the beginning of tuple
};

// Defines in memory layout of tuple.
struct TTupleLayout {
    std::vector<TColumnDesc> OrigColumns; // Columns description and order as
                                          // passed during layout construction
    std::vector<TColumnDesc> Columns; // Vector describing all columns in order
                                      // corresponding to tuple layout
    std::vector<TColumnDesc> KeyColumns; // Vector describing key columns
    std::vector<TColumnDesc>
        PayloadColumns;      // Vector describing payload columns
    std::vector<TColumnDesc> VariableColumns;  // Variable-size columns only
    ui32 KeyColumnsNum;      // Total number of key columns
    ui32 KeyColumnsSize;     // Total size of all key columns in bytes
    ui32 KeyColumnsOffset;   // Start of row-packed keys data
    ui32 KeyColumnsFixedEnd; // Offset in row-packed keys data of first variable
                             // key (can be same as KeyColumnsEnd, if there are
                             // none)
    ui32 KeyColumnsFixedNum; // Number of fixed-size columns
    ui32 KeyColumnsEnd; // First byte after key columns. Start of bitmask for
                        // row-based columns
    ui32 KeySizeTag_;   // Simple byte key fast path tag
    ui32 BitmaskSize;   // Size of bitmask for null values flag in columns
    ui32 BitmaskOffset; // Offset of nulls bitmask. = KeyColumnsEnd
    ui32 BitmaskEnd;    // First byte after bitmask. = PayloadOffset
    ui32 PayloadSize;   // Total size in bytes of the payload columns
    ui32 PayloadOffset; // Offset of payload values. = BitmaskEnd.
    ui32 PayloadEnd;    // First byte after payload
    ui32 TotalRowSize;  // Total size of bytes for packed row

    // Creates new tuple layout based on provided columns description.
    static THolder<TTupleLayout>
    Create(const std::vector<TColumnDesc> &columns);

    TTupleLayout(const std::vector<TColumnDesc> &columns)
        : OrigColumns(columns) {}
    virtual ~TTupleLayout() {}

    // Takes array of pointer to columns, array of validity bitmaps,
    // outputs packed rows
    virtual void Pack(const ui8 **columns, const ui8 **isValidBitmask, ui8 *res,
                      std::vector<ui8, TMKQLAllocator<ui8>> &overflow,
                      ui32 start, ui32 count) const = 0;

    // Takes packed rows,
    // outputs array of pointer to columns, array of validity bitmaps
    virtual void Unpack(ui8 **columns, ui8 **isValidBitmask, const ui8 *res,
                        const std::vector<ui8, TMKQLAllocator<ui8>> &overflow,
                        ui32 start, ui32 count) const = 0;

    virtual void
    BucketPack(const ui8 **columns, const ui8 **isValidBitmask,
                 TPaddedPtr<std::vector<ui8, TMKQLAllocator<ui8>>> reses,
                 TPaddedPtr<std::vector<ui8, TMKQLAllocator<ui8>>> overflows,
                 ui32 start, ui32 count, ui32 bucketsLogNum) const = 0;

    // Takes packed rows,
    // outputs vector of column sizes in bytes
    void CalculateColumnSizes(
        const ui8* res, ui32 count, std::vector<ui64, TMKQLAllocator<ui64>>& bytes) const ;
    
    void TupleDeepCopy(
        const ui8* inTuple, const ui8* inOverflow,
        ui8* outTuple, ui8* outOverflow, ui64& outOverflowSize) const;

    void Join(
        std::vector<ui8, TMKQLAllocator<ui8>>& dst,
        std::vector<ui8, TMKQLAllocator<ui8>>& dstOverflow,
        ui32 dstCount,
        const ui8 *src, const ui8 *srcOverflow, ui32 srcCount, ui32 srcOverflowSize) const;
    
    ui32 GetTupleVarSize(const ui8* inTuple) const;

    bool KeysEqual(const ui8 *lhsRow, const ui8 *lhsOverflow, const ui8 *rhsRow, const ui8 *rhsOverflow) const;
    bool KeysLess(const ui8 *lhsRow, const ui8 *lhsOverflow, const ui8 *rhsRow, const ui8 *rhsOverflow) const;
};

struct TTupleLayoutFallback : public TTupleLayout {

    explicit TTupleLayoutFallback(const std::vector<TColumnDesc> &columns);

    void Pack(const ui8 **columns, const ui8 **isValidBitmask, ui8 *res,
              std::vector<ui8, TMKQLAllocator<ui8>> &overflow, ui32 start,
              ui32 count) const override;

    void Unpack(ui8 **columns, ui8 **isValidBitmask, const ui8 *res,
                const std::vector<ui8, TMKQLAllocator<ui8>> &overflow,
                ui32 start, ui32 count) const override;

    void
    BucketPack(const ui8 **columns, const ui8 **isValidBitmask,
                 TPaddedPtr<std::vector<ui8, TMKQLAllocator<ui8>>> reses,
                 TPaddedPtr<std::vector<ui8, TMKQLAllocator<ui8>>> overflows,
                 ui32 start, ui32 count, ui32 bucketsLogNum) const override;

  protected:
    std::array<std::vector<TColumnDesc>, 5>
        FixedPOTColumns_; // Fixed-size columns for power-of-two sizes from 1 to
                          // 16 bytes
    std::vector<TColumnDesc> FixedNPOTColumns_; // Remaining fixed-size columns
};


template <typename TTraits> struct TTupleLayoutSIMD : public TTupleLayoutFallback {

    explicit TTupleLayoutSIMD(const std::vector<TColumnDesc> &columns);

    void Pack(const ui8 **columns, const ui8 **isValidBitmask, ui8 *res,
        std::vector<ui8, TMKQLAllocator<ui8>> &overflow, ui32 start,
        ui32 count) const override;

    void Unpack(ui8 **columns, ui8 **isValidBitmask, const ui8 *res,
            const std::vector<ui8, TMKQLAllocator<ui8>> &overflow,
            ui32 start, ui32 count) const override;

    void
    BucketPack(const ui8 **columns, const ui8 **isValidBitmask,
                 TPaddedPtr<std::vector<ui8, TMKQLAllocator<ui8>>> reses,
                 TPaddedPtr<std::vector<ui8, TMKQLAllocator<ui8>>> overflows,
                 ui32 start, ui32 count, ui32 bucketsLogNum) const override;

  private:
    using TSimdI8 = typename TTraits::TSimdI8;
    template <class T> using TSimd = typename TTraits::template TSimd8<T>;

    static constexpr ui8 kSIMDMaxCols = 4;
    static constexpr ui8 kSIMDMaxInnerLoopSize = 4;

    size_t BlockRows_; // Estimated rows per cache block
    std::vector<size_t> BlockColsOffsets_;
    std::vector<size_t> BlockFixedColsSizes_;
    std::vector<size_t> BlockColumnsOrigInds_;

    struct SIMDSmallTupleDesc {
        ui8 Cols = 0;
        ui8 InnerLoopIters;
        size_t SmallTupleSize;
        size_t RowOffset;
    };
    SIMDSmallTupleDesc SIMDSmallTuple_;
    std::vector<TSimd<ui8>> SIMDPermMasks_; // small tuple precomputed masks

    struct SIMDTransposeDesc {
        ui8 Cols = 0;
        size_t RowOffset;
    };
    // [1, 2, 4, 8] bytes x [Key, Payload]
    std::array<SIMDTransposeDesc, 8> SIMDTranspositions_;
    static constexpr std::array<size_t, 4> SIMDTranspositionsColSizes_ = {1, 2,
                                                                          4, 8};
};

bool TupleKeysEqual(const TTupleLayout *layout,
    const ui8 *lhsRow, const ui8 *lhsOverflow,
    const ui8 *rhsRow, const ui8 *rhsOverflow);

Y_FORCE_INLINE
bool TTupleLayout::KeysEqual(const ui8 *lhsRow, const ui8 *lhsOverflow,
                             const ui8 *rhsRow, const ui8 *rhsOverflow) const {
    const ui8 keyNullMask = (1u << KeyColumnsNum) - 1;

    switch (KeySizeTag_) {
    case 0:
        return ReadUnaligned<ui8>(lhsRow + KeyColumnsOffset) ==
                   ReadUnaligned<ui8>(rhsRow + KeyColumnsOffset) &&
               (ReadUnaligned<ui8>(lhsRow + BitmaskOffset) &
                ReadUnaligned<ui8>(rhsRow + BitmaskOffset) & keyNullMask) ==
                   keyNullMask;
    case 1:
        return ReadUnaligned<ui16>(lhsRow + KeyColumnsOffset) ==
                   ReadUnaligned<ui16>(rhsRow + KeyColumnsOffset) &&
               (ReadUnaligned<ui8>(lhsRow + BitmaskOffset) &
                ReadUnaligned<ui8>(rhsRow + BitmaskOffset) & keyNullMask) ==
                   keyNullMask;

    case 2:
        return ReadUnaligned<ui32>(lhsRow + KeyColumnsOffset) ==
                   ReadUnaligned<ui32>(rhsRow + KeyColumnsOffset) &&
               (ReadUnaligned<ui8>(lhsRow + BitmaskOffset) &
                ReadUnaligned<ui8>(rhsRow + BitmaskOffset) & keyNullMask) ==
                   keyNullMask;

    case 3:
        return ReadUnaligned<ui64>(lhsRow + KeyColumnsOffset) ==
                   ReadUnaligned<ui64>(rhsRow + KeyColumnsOffset) &&
               (ReadUnaligned<ui8>(lhsRow + BitmaskOffset) &
                ReadUnaligned<ui8>(rhsRow + BitmaskOffset) & keyNullMask) ==
                   keyNullMask;

    case 4:
        return ReadUnaligned<ui64>(lhsRow + KeyColumnsOffset) ==
                   ReadUnaligned<ui64>(rhsRow + KeyColumnsOffset) &&
               ReadUnaligned<ui64>(lhsRow + KeyColumnsOffset + 8) ==
                   ReadUnaligned<ui64>(rhsRow + KeyColumnsOffset + 8) &&
               (ReadUnaligned<ui8>(lhsRow + BitmaskOffset) &
                ReadUnaligned<ui8>(rhsRow + BitmaskOffset) & keyNullMask) ==
                   keyNullMask;

    default:
        return TupleKeysEqual(this, lhsRow, lhsOverflow, rhsRow, rhsOverflow);
    }
}

template <size_t Size>
struct TEmbeddedTupleRowRef {
    TEmbeddedTupleRowRef(ui8* tuple): Ptr(tuple) {}

    TEmbeddedTupleRowRef(const TEmbeddedTupleRowRef&) = default;
    
    TEmbeddedTupleRowRef& operator=(const TEmbeddedTupleRowRef& rhs) {
        std::memcpy(Ptr, rhs.Ptr, Size);
        return *this;
    }
    
    friend void swap(TEmbeddedTupleRowRef lhs, TEmbeddedTupleRowRef rhs) {
        ui8 buf[Size];
        std::memcpy(buf, lhs.Ptr, Size);
        std::memcpy(lhs.Ptr, rhs.Ptr, Size);
        std::memcpy(rhs.Ptr, buf, Size);
    }

  public:
    ui8* Ptr;
};

template <size_t Size>
struct TEmbeddedTuple {
    TEmbeddedTuple() {}

    TEmbeddedTuple(const TEmbeddedTuple& row) {
        std::memcpy(Ptr, row.Ptr, Size);
    };

    TEmbeddedTuple& operator=(const TEmbeddedTuple &rhs) {
        std::memcpy(Ptr, rhs.Ptr, Size);
        return *this;
    }
    
  public:
    ui8 Ptr[Size];
};

template <size_t Size = 16>
[[maybe_unused]] void SortEmbeddedTuples(TPaddedPtr<TEmbeddedTuple<Size>> first, size_t n, const TTupleLayout* layout, const ui8* overflow) {
    auto cmpr = [layout, overflow](const TEmbeddedTuple<Size> &lhs, const TEmbeddedTuple<Size> &rhs) {
        return layout->KeysLess(lhs.Ptr, overflow, rhs.Ptr, overflow);
    };
    std::sort(first, first + n, cmpr);
}

struct TIndexedTuple {
    ui32 Hash;
    ui32 Index;
};

[[maybe_unused]] inline void SortIndexedTuples(TPaddedPtr<TIndexedTuple> first, size_t n, const TTupleLayout* layout, const ui8* tuples, const ui8* overflow) {
    auto cmpr = [layout, tuples, overflow](TIndexedTuple lhs, TIndexedTuple rhs) {
        if (lhs.Hash != rhs.Hash) {
            return lhs.Hash < rhs.Hash;
        }
        return layout->KeysLess(tuples + layout->TotalRowSize * lhs.Index, overflow, tuples + layout->TotalRowSize * rhs.Index, overflow);
    };
    std::sort(first, first + n, cmpr);
}

} // namespace NPackedTuple
} // namespace NMiniKQL
} // namespace NKikimr

